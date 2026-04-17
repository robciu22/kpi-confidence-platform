#!/usr/bin/env python3
"""
scripts/load_new_detectors_tgz_to_staging_v1_1.py

NEW-Loader: detektoren_YYYY_MM.tgz -> staging.stg_new_detector_hourly

Korrekturen:
- Erkennt echte Header: "Datum (Ortszeit)", "Stunde des Tages (Ortszeit)", "Vollständigkeit"
- Dedup innerhalb einer CSV-Datei auf Key:
    (det_name_alt, det_index, datum_ortszeit, stunde_ortszeit)
  Begründung: In NEW-Dateien tauchen Stunden teils doppelt auf (erst NaN-Block, später echte Werte).
  staging hat PK/UNIQUE auf diesen Spalten -> COPY würde sonst abbrechen und alles rollbacken.

Idempotenz:
- Ersetzt Monatsslice per DELETE + COPY (schnell und stabil).

Team-Hinweis:
- Dieses Script kann jedes Teammitglied lokal ausführen, solange .env und FILE_STORAGE_ROOT korrekt gesetzt sind.

Standalone:
  python scripts/load_new_detectors_tgz_to_staging_v1_1.py \
    --month-key 2024_04 \
    --tgz "data/source/verkehrsdetektion/monthly/2024_04/new_detectors/detektoren_2024_04.tgz" \
    --env-file "./.env" \
    --replace-month-slice
"""

from __future__ import annotations

import argparse
import csv
import gzip
import os
import re
import tarfile
from dataclasses import dataclass
from datetime import date, datetime, time
from io import TextIOWrapper
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Optional, Sequence, Tuple

from dotenv import load_dotenv
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row


_RE_DET_INDEX = re.compile(r"Det(\d+)$", re.IGNORECASE)


def _clean_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return None
    return s


def _parse_int(x: Any) -> Optional[int]:
    s = _clean_str(x)
    if s is None:
        return None
    s = s.replace(",", ".")
    try:
        if re.fullmatch(r"-?\d+", s):
            return int(s)
        if re.fullmatch(r"-?\d+\.0+", s):
            return int(float(s))
        return None
    except Exception:
        return None


def _parse_float(x: Any) -> Optional[float]:
    s = _clean_str(x)
    if s is None:
        return None
    s = s.replace(",", ".")
    try:
        v = float(s)
        if v != v:  # NaN
            return None
        return v
    except Exception:
        return None


def _parse_date(x: Any) -> Optional[date]:
    s = _clean_str(x)
    if s is None:
        return None
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            return datetime.strptime(s, "%Y-%m-%d").date()
        if re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", s):
            return datetime.strptime(s, "%d.%m.%Y").date()
        return None
    except Exception:
        return None


def _parse_dt(x: Any) -> Optional[datetime]:
    s = _clean_str(x)
    if s is None:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _sniff_delimiter(header_line: str) -> str:
    candidates = [",", ";", "\t"]
    counts = {c: header_line.count(c) for c in candidates}
    return max(counts, key=counts.get)


def _norm_header(name: str) -> str:
    return name.strip().lower().replace(" ", "").replace("_", "")


def _resolve_idx(idx_map: Dict[str, int], candidates: Sequence[str]) -> Optional[int]:
    # 1) exakter Treffer auf normalisiertem Header
    for c in candidates:
        key = _norm_header(c)
        if key in idx_map:
            return idx_map[key]
    # 2) Präfix-Treffer (Datum -> Datum(Ortszeit))
    for c in candidates:
        key = _norm_header(c)
        for hk, i in idx_map.items():
            if hk.startswith(key):
                return i
    return None


def month_key_to_range(month_key: str) -> Tuple[date, date, int]:
    y_s, m_s = month_key.split("_", 1)
    y, m = int(y_s), int(m_s)
    start = date(y, m, 1)
    end = date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)
    return start, end, m


@dataclass(frozen=True)
class NewRow:
    det_name_alt: str
    det_index: int
    datum_ortszeit: date
    stunde_ortszeit: int
    vollstaendigkeit: Optional[float]
    zscore_det0: Optional[float]
    zscore_det1: Optional[float]
    zscore_det2: Optional[float]
    hist_cor: Optional[float]
    local_time: Optional[datetime]
    month: int
    qkfz: Optional[int]
    qlkw: Optional[int]
    qpkw: Optional[int]
    utc: Optional[datetime]
    vkfz: Optional[int]
    vlkw: Optional[int]
    vpkw: Optional[int]
    source_file: str
    ingested_at: datetime


def _row_score(r: NewRow) -> int:
    """Bewertet eine Zeile anhand der Anzahl nicht-None Messwerte.

    Wird für Dedup verwendet: Bei doppelten (Datum, Stunde)-Einträgen gewinnt die vollständigere Zeile.
    """
    fields = [
        r.vollstaendigkeit,
        r.zscore_det0,
        r.zscore_det1,
        r.zscore_det2,
        r.hist_cor,
        r.qkfz,
        r.qlkw,
        r.qpkw,
        r.vkfz,
        r.vlkw,
        r.vpkw,
    ]
    return sum(1 for x in fields if x is not None)


def load_env_file(env_file: str) -> Path:
    p = Path(env_file).expanduser().resolve()
    if not p.exists():
        raise FileNotFoundError(f"env_file nicht gefunden: {p}")
    load_dotenv(p, override=False)
    return p


def _as_int(value: Any, default: int) -> int:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except Exception:
        return default


def db_connect() -> psycopg.Connection:
    host = os.getenv("PGHOST", "localhost")
    port = _as_int(os.getenv("PGPORT"), 5432)
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    dbname = os.getenv("PGDATABASE")

    missing = [
        k
        for k, v in [("PGUSER", user), ("PGPASSWORD", password), ("PGDATABASE", dbname)]
        if not v
    ]
    if missing:
        raise RuntimeError(f"Fehlende Umgebungsvariablen: {missing}. .env / --env-file prüfen.")

    conn = psycopg.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname,
        row_factory=dict_row,
    )
    conn.autocommit = False
    return conn


def iter_new_rows_from_tgz(
    tgz_path: Path,
    *,
    month_key: str,
    timezone: str,
    only_detectors: Sequence[str],
    max_rows: Optional[int],
) -> Iterator[NewRow]:
    """Liest alle Zeilen aus einem NEW-Detektor-TGZ-Archiv (detektoren_YYYY_MM.tgz).

    Jede enthaltene Datei entspricht einem Detektor (TEU00002_Det0.csv oder TEU00002_Det0.csv.gz).
    Beide Formate werden unterstützt: ältere Archive enthalten .csv, neuere (ab ca. 2025_01) .csv.gz.
    Pro (Datum, Stunde) wird die beste Zeile behalten (Dedup nach _row_score).
    Zeilen außerhalb des Monatsbereichs werden verworfen.
    Timestamps werden in lokale Zeit (Europe/Berlin) und UTC konvertiert.
    """
    start_date, end_date, month_int = month_key_to_range(month_key)
    tz = ZoneInfo(timezone)
    utc_tz = ZoneInfo("UTC")

    only_set = {s.strip() for s in only_detectors if s and s.strip()}
    produced = 0
    ingested_at = datetime.now(tz=utc_tz)

    with tarfile.open(tgz_path, mode="r:*") as tf:
        members = [
            m for m in tf.getmembers()
            if m.isfile() and (
                m.name.lower().endswith(".csv") or m.name.lower().endswith(".csv.gz")
            )
        ]

        for m in members:
            base = Path(m.name).name
            # Beide Endungen abschneiden: TEU00002_Det0.csv.gz -> TEU00002_Det0
            if base.lower().endswith(".csv.gz"):
                det_name_alt = Path(Path(base).stem).stem
            else:
                det_name_alt = Path(base).stem  # TEU00002_Det0

            if only_set and not any(
                det_name_alt.startswith(prefix) for prefix in only_set
            ):
                continue

            det_index = 0
            mm = _RE_DET_INDEX.search(det_name_alt)
            if mm:
                det_index = int(mm.group(1))

            f = tf.extractfile(m)
            if f is None:
                continue

            # .csv.gz innerhalb des TAR: zusätzliche GZip-Dekomprimierung nötig
            if base.lower().endswith(".csv.gz"):
                txt = gzip.open(f, "rt", encoding="utf-8", errors="replace", newline="")
            else:
                txt = TextIOWrapper(f, encoding="utf-8", errors="replace", newline="")
            header = txt.readline()
            if not header:
                continue

            delim = _sniff_delimiter(header)
            header_cols = [h.strip() for h in header.strip().split(delim)]
            idx_map: Dict[str, int] = {
                _norm_header(h): i for i, h in enumerate(header_cols)
            }

            i_date = _resolve_idx(
                idx_map, ["Datum (Ortszeit)", "Datum", "date", "datum"]
            )
            i_hour = _resolve_idx(
                idx_map,
                [
                    "Stunde des Tages (Ortszeit)",
                    "Stunde des Tages",
                    "Stunde",
                    "hour",
                    "stunde",
                ],
            )
            i_voll = _resolve_idx(
                idx_map,
                [
                    "Vollständigkeit",
                    "Vollstaendigkeit",
                    "Datapoints_Rel",
                    "vollstaendigkeit",
                ],
            )
            i_z0 = _resolve_idx(idx_map, ["ZScore_Det0", "zscore_det0"])
            i_z1 = _resolve_idx(idx_map, ["ZScore_Det1", "zscore_det1"])
            i_z2 = _resolve_idx(idx_map, ["ZScore_Det2", "zscore_det2"])
            i_hc = _resolve_idx(idx_map, ["hist_cor", "HIST_COR", "Hist_Cor"])
            i_local = _resolve_idx(idx_map, ["localTime", "local_time", "localtime"])
            i_utc = _resolve_idx(idx_map, ["utc", "UTC"])

            i_qkfz = _resolve_idx(idx_map, ["qkfz", "kfz"])
            i_qlkw = _resolve_idx(idx_map, ["qlkw"])
            i_qpkw = _resolve_idx(idx_map, ["qpkw"])
            i_vkfz = _resolve_idx(idx_map, ["vkfz"])
            i_vlkw = _resolve_idx(idx_map, ["vlkw"])
            i_vpkw = _resolve_idx(idx_map, ["vpkw"])

            reader = csv.reader(txt, delimiter=delim)

            # Dedup pro (Datum, Stunde) innerhalb dieser Datei
            keep: Dict[Tuple[date, int], NewRow] = {}

            for row in reader:
                if not row:
                    continue

                def get(i: Optional[int]) -> Optional[str]:
                    if i is None or i >= len(row):
                        return None
                    return row[i]

                d = _parse_date(get(i_date))
                h = _parse_int(get(i_hour))
                if d is None or h is None:
                    continue
                if not (start_date <= d < end_date):
                    continue
                if not (0 <= h <= 23):
                    continue

                utc_dt = _parse_dt(get(i_utc))
                local_dt = _parse_dt(get(i_local))

                if utc_dt is not None and utc_dt.tzinfo is None:
                    utc_dt = utc_dt.replace(tzinfo=utc_tz)
                if local_dt is not None and local_dt.tzinfo is None:
                    local_dt = local_dt.replace(tzinfo=tz)
                if local_dt is None and utc_dt is not None:
                    try:
                        local_dt = utc_dt.astimezone(tz)
                    except Exception:
                        local_dt = None
                if local_dt is None:
                    try:
                        naive = datetime.combine(d, time(int(h), 0, 0))
                        local_dt = naive.replace(tzinfo=tz)
                    except Exception:
                        local_dt = None

                cand = NewRow(
                    det_name_alt=det_name_alt,
                    det_index=det_index,
                    datum_ortszeit=d,
                    stunde_ortszeit=int(h),
                    vollstaendigkeit=_parse_float(get(i_voll)),
                    zscore_det0=_parse_float(get(i_z0)),
                    zscore_det1=_parse_float(get(i_z1)),
                    zscore_det2=_parse_float(get(i_z2)),
                    hist_cor=_parse_float(get(i_hc)),
                    local_time=local_dt,
                    month=month_int,  # aus month_key
                    qkfz=_parse_int(get(i_qkfz)),
                    qlkw=_parse_int(get(i_qlkw)),
                    qpkw=_parse_int(get(i_qpkw)),
                    utc=utc_dt,
                    vkfz=_parse_int(get(i_vkfz)),
                    vlkw=_parse_int(get(i_vlkw)),
                    vpkw=_parse_int(get(i_vpkw)),
                    source_file=f"{tgz_path}#{m.name}",
                    ingested_at=ingested_at,
                )

                key = (d, int(h))
                prev = keep.get(key)
                if prev is None or _row_score(cand) >= _row_score(prev):
                    keep[key] = cand

            # stabile Ausgabe (damit Tests reproduzierbar sind)
            for d, h in sorted(keep.keys()):
                produced += 1
                yield keep[(d, h)]
                if max_rows is not None and produced >= max_rows:
                    return


STG_COLUMNS = (
    "det_name_alt",
    "det_index",
    "datum_ortszeit",
    "stunde_ortszeit",
    "vollstaendigkeit",
    "zscore_det0",
    "zscore_det1",
    "zscore_det2",
    "hist_cor",
    "local_time",
    "month",
    "qkfz",
    "qlkw",
    "qpkw",
    "utc",
    "vkfz",
    "vlkw",
    "vpkw",
    "source_file",
    "ingested_at",
)


def replace_month_slice(cur: psycopg.Cursor, month_key: str) -> int:
    """Löscht alle bestehenden Zeilen des Monats aus staging.stg_new_detector_hourly.

    Stellt Idempotenz sicher: Bei Wiederholung werden keine Duplikate erzeugt.
    Gibt die Anzahl der gelöschten Zeilen zurück.
    """
    start, end, _ = month_key_to_range(month_key)
    cur.execute(
        "DELETE FROM staging.stg_new_detector_hourly WHERE datum_ortszeit >= %s AND datum_ortszeit < %s;",
        (start, end),
    )
    return cur.rowcount


def copy_rows(cur: psycopg.Cursor, rows: Iterable[NewRow]) -> int:
    """Schreibt NewRow-Objekte per COPY-Protokoll in staging.stg_new_detector_hourly.

    COPY ist deutlich schneller als INSERT für große Datenmengen.
    Gibt die Anzahl der geschriebenen Zeilen zurück.
    """
    sql = f"COPY staging.stg_new_detector_hourly ({', '.join(STG_COLUMNS)}) FROM STDIN"
    n = 0
    with cur.copy(sql) as cp:
        for r in rows:
            cp.write_row(
                (
                    r.det_name_alt,
                    r.det_index,
                    r.datum_ortszeit,
                    r.stunde_ortszeit,
                    r.vollstaendigkeit,
                    r.zscore_det0,
                    r.zscore_det1,
                    r.zscore_det2,
                    r.hist_cor,
                    r.local_time,
                    r.month,
                    r.qkfz,
                    r.qlkw,
                    r.qpkw,
                    r.utc,
                    r.vkfz,
                    r.vlkw,
                    r.vpkw,
                    r.source_file,
                    r.ingested_at,
                )
            )
            n += 1
    return n


def main() -> None:
    ap = argparse.ArgumentParser(
        description="NEW detektoren_YYYY_MM.tgz in staging.stg_new_detector_hourly laden (v1.1)"
    )
    ap.add_argument("--month-key", required=True, help="YYYY_MM (z.B. 2024_04)")
    ap.add_argument("--tgz", required=True, help="Pfad zur detektoren_YYYY_MM.tgz")
    ap.add_argument(
        "--env-file", default="./.env", help="Pfad zur .env (Standard: ./.env)"
    )
    ap.add_argument(
        "--timezone",
        default="Europe/Berlin",
        help="Lokale Zeitzone (Standard: Europe/Berlin)",
    )
    ap.add_argument(
        "--replace-month-slice",
        action="store_true",
        help="Bestehenden Monatsslice vor dem Laden löschen",
    )
    ap.add_argument(
        "--only-detector",
        action="append",
        default=[],
        help="Präfix-Filter, z.B. TEU00002 (wiederholbar)",
    )
    ap.add_argument(
        "--max-rows", type=int, default=None, help="Nach N Zeilen abbrechen (Test)"
    )
    args = ap.parse_args()

    load_env_file(args.env_file)

    tgz_path = Path(args.tgz).expanduser().resolve()
    if not tgz_path.exists():
        raise FileNotFoundError(f"TGZ nicht gefunden: {tgz_path}")

    conn = db_connect()
    try:
        with conn.cursor() as cur:
            if args.replace_month_slice:
                replace_month_slice(cur, args.month_key)

            rows_iter = iter_new_rows_from_tgz(
                tgz_path,
                month_key=args.month_key,
                timezone=args.timezone,
                only_detectors=args.only_detector,
                max_rows=args.max_rows,
            )
            inserted = copy_rows(cur, rows_iter)

        conn.commit()
        print(
            f"[OK] {inserted} Zeilen in staging.stg_new_detector_hourly geladen aus {tgz_path}"
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
