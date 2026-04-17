#!/usr/bin/env python3
"""
scripts/load_old_detectors_gz_to_staging_v1_2.py

Plan-basierter OLD-Loader (old_detectors/*.csv.gz) -> staging.stg_old_det_val_hr

- Liest config/pipeline_ingestion.yaml (oder --config)
- Baut Storage-Root + MonthPlan über src/config/pipeline_config_v1_0.py
- Wenn --month-key gesetzt ist, wird dieser Monat geladen (auch wenn er NICHT in YAML selection steht)
- Expandiert old_detectors glob
- Optional: replace-month-slice (DELETE) + COPY
- Robust: delimiter sniffing, NaN/leer -> NULL, Komma-Dezimal
"""

from __future__ import annotations

import argparse
import csv
import gzip
import os
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row


# ----------------------------
# Bootstrap-Imports (robust)
# ----------------------------


def find_project_root(start: Path) -> Path:
    """Sucht das Projektverzeichnis nach oben, bis ein Ordner mit src/ gefunden wird."""
    p = start.resolve()
    for parent in [p] + list(p.parents):
        if (parent / "src").is_dir():
            return parent
    raise RuntimeError(
        f"Projektverzeichnis nicht gefunden ab {start} (erwartet ein übergeordnetes Verzeichnis mit 'src/')."
    )


def bootstrap_import_path() -> Path:
    """Fügt das Projektverzeichnis zum sys.path hinzu und gibt es zurück.

    Wird beim Modulstart ausgeführt, damit src/config/... importierbar ist.
    """
    here = Path(__file__).resolve()
    project_root = find_project_root(here.parent)
    pr = str(project_root)
    if pr not in sys.path:
        sys.path.insert(0, pr)
    return project_root


PROJECT_ROOT = bootstrap_import_path()
from src.config.pipeline_config_v1_0 import (  # noqa: E402
    load_pipeline_config,
    build_plan,
    build_month_plan,
)  # noqa: E402


# ----------------------------
# Parser-Hilfsfunktionen
# ----------------------------

_MONTH_RE = re.compile(r"^\d{4}_(0[1-9]|1[0-2])$")


def month_key_to_range(month_key: str) -> Tuple[date, date]:
    if not _MONTH_RE.match(month_key):
        raise ValueError(f"Ungültiger month_key: {month_key}")
    y_s, m_s = month_key.split("_", 1)
    y, m = int(y_s), int(m_s)
    start = date(y, m, 1)
    end = date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)
    return start, end


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
        if v != v:
            return None
        return v
    except Exception:
        return None


def _parse_date(x: Any) -> Optional[date]:
    s = _clean_str(x)
    if s is None:
        return None
    try:
        if re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", s):
            return datetime.strptime(s, "%d.%m.%Y").date()
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            return datetime.strptime(s, "%Y-%m-%d").date()
        return None
    except Exception:
        return None


def _sniff_delimiter(header_line: str) -> str:
    candidates = [";", ",", "\t"]
    counts = {c: header_line.count(c) for c in candidates}
    return max(counts, key=counts.get)


def _norm_header(h: str) -> str:
    return h.strip().lower().replace(" ", "").replace("_", "")


def _idx(idx_map: Dict[str, int], *candidates: str) -> Optional[int]:
    for c in candidates:
        k = _norm_header(c)
        if k in idx_map:
            return idx_map[k]
    for c in candidates:
        k = _norm_header(c)
        for hk, i in idx_map.items():
            if hk.startswith(k):
                return i
    return None


@dataclass(frozen=True)
class OldDetRow:
    detid_15: Optional[int]
    tag: date
    stunde: int
    qualitaet: Optional[float]
    q_kfz_det_hr: Optional[int]
    v_kfz_det_hr: Optional[int]
    q_pkw_det_hr: Optional[int]
    v_pkw_det_hr: Optional[int]
    q_lkw_det_hr: Optional[int]
    v_lkw_det_hr: Optional[int]
    source_file: str
    ingested_at: datetime


def _row_score(r: OldDetRow) -> int:
    fields = [
        r.qualitaet,
        r.q_kfz_det_hr,
        r.v_kfz_det_hr,
        r.q_pkw_det_hr,
        r.v_pkw_det_hr,
        r.q_lkw_det_hr,
        r.v_lkw_det_hr,
    ]
    return sum(1 for x in fields if x is not None)


# ----------------------------
# DB-Hilfsfunktionen
# ----------------------------


def load_env_file(env_file: Path) -> None:
    if env_file.exists():
        load_dotenv(env_file, override=False)


def _as_int(v: Any, default: int) -> int:
    if v is None or v == "":
        return default
    try:
        return int(v)
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
        for k, val in [
            ("PGUSER", user),
            ("PGPASSWORD", password),
            ("PGDATABASE", dbname),
        ]
        if not val
    ]
    if missing:
        raise RuntimeError("Fehlende " + "/".join(missing) + " in .env")

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


def replace_month_slice(cur: psycopg.Cursor, start: date, end: date) -> int:
    """Löscht alle bestehenden Zeilen des Monats aus staging.stg_old_det_val_hr (Idempotenz)."""
    cur.execute(
        "DELETE FROM staging.stg_old_det_val_hr WHERE tag >= %s AND tag < %s;",
        (start, end),
    )
    return cur.rowcount


def copy_rows(cur: psycopg.Cursor, rows: Iterable[OldDetRow]) -> int:
    """Schreibt OldDetRow-Objekte per COPY-Protokoll in staging.stg_old_det_val_hr."""
    sql = """
    COPY staging.stg_old_det_val_hr (
      detid_15, tag, stunde, qualitaet,
      q_kfz_det_hr, v_kfz_det_hr,
      q_pkw_det_hr, v_pkw_det_hr,
      q_lkw_det_hr, v_lkw_det_hr,
      source_file, ingested_at
    ) FROM STDIN
    """
    n = 0
    with cur.copy(sql) as cp:
        for r in rows:
            cp.write_row(
                (
                    r.detid_15,
                    r.tag,
                    r.stunde,
                    r.qualitaet,
                    r.q_kfz_det_hr,
                    r.v_kfz_det_hr,
                    r.q_pkw_det_hr,
                    r.v_pkw_det_hr,
                    r.q_lkw_det_hr,
                    r.v_lkw_det_hr,
                    r.source_file,
                    r.ingested_at,
                )
            )
            n += 1
    return n


# ----------------------------
# Datei-Iteration
# ----------------------------


def expand_glob(glob_path: Path) -> List[Path]:
    parent = glob_path.parent
    pattern = glob_path.name
    if not parent.exists():
        return []
    return sorted(parent.glob(pattern))


def iter_rows_from_gz(gz_path: Path, *, start: date, end: date) -> Iterator[OldDetRow]:
    """Liest Detektor-Messwerte aus einer komprimierten CSV.GZ-Datei (OLD-Format).

    Erkennt automatisch den Delimiter (;/,/Tab) und normalisiert Spaltennamen.
    Zeilen außerhalb des Monatsbereichs [start, end) werden verworfen.
    Dedup: Pro (detid_15, Datum, Stunde) wird die vollständigere Zeile behalten.
    """
    ingested_at = datetime.now(timezone.utc)

    with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace", newline="") as f:
        header = f.readline()
        if not header:
            return
        delim = _sniff_delimiter(header)
        headers = [h.strip() for h in header.strip().split(delim)]
        idx_map = {_norm_header(h): i for i, h in enumerate(headers)}

        i_tag = _idx(idx_map, "tag", "datum", "date")
        i_hr = _idx(idx_map, "stunde", "hour")
        if i_tag is None or i_hr is None:
            raise RuntimeError(
                f"Pflichtpalten nicht gefunden in {gz_path.name}. Headers={headers}"
            )

        i_det = _idx(idx_map, "detid_15", "det_id15", "detid15", "detid")

        i_q = _idx(idx_map, "qualitaet", "qualität", "quality")
        i_qk = _idx(idx_map, "q_kfz_det_hr", "qkfz_det_hr", "q_kfz")
        i_vk = _idx(idx_map, "v_kfz_det_hr", "vkfz_det_hr", "v_kfz")
        i_qp = _idx(idx_map, "q_pkw_det_hr", "qpkw_det_hr", "q_pkw")
        i_vp = _idx(idx_map, "v_pkw_det_hr", "vpkw_det_hr", "v_pkw")
        i_ql = _idx(idx_map, "q_lkw_det_hr", "qlkw_det_hr", "q_lkw")
        i_vl = _idx(idx_map, "v_lkw_det_hr", "vlkw_det_hr", "v_lkw")

        reader = csv.reader(f, delimiter=delim)

        keep: Dict[Tuple[Optional[int], date, int], OldDetRow] = {}

        for row in reader:
            if not row:
                continue

            def get(i: Optional[int]) -> Optional[str]:
                if i is None or i >= len(row):
                    return None
                return row[i]

            tag = _parse_date(get(i_tag))
            hr = _parse_int(get(i_hr))
            if tag is None or hr is None:
                continue
            if not (start <= tag < end):
                continue
            if not (0 <= hr <= 23):
                continue

            detid_15 = _parse_int(get(i_det))

            cand = OldDetRow(
                detid_15=detid_15,
                tag=tag,
                stunde=int(hr),
                qualitaet=_parse_float(get(i_q)),
                q_kfz_det_hr=_parse_int(get(i_qk)),
                v_kfz_det_hr=_parse_int(get(i_vk)),
                q_pkw_det_hr=_parse_int(get(i_qp)),
                v_pkw_det_hr=_parse_int(get(i_vp)),
                q_lkw_det_hr=_parse_int(get(i_ql)),
                v_lkw_det_hr=_parse_int(get(i_vl)),
                source_file=str(gz_path),
                ingested_at=ingested_at,
            )

            k = (cand.detid_15, cand.tag, cand.stunde)
            prev = keep.get(k)
            if prev is None or _row_score(cand) >= _row_score(prev):
                keep[k] = cand

        for k in sorted(
            keep.keys(), key=lambda x: (x[1], x[2], x[0] if x[0] is not None else -1)
        ):
            yield keep[k]


# ----------------------------
# main
# ----------------------------


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/pipeline_ingestion.yaml")
    ap.add_argument("--env-file", default=None, help="Pfad zur .env-Datei (überschreibt config env_file)")
    ap.add_argument(
        "--month-key",
        default=None,
        help="YYYY_MM (wird auch geladen, wenn nicht in YAML-Auswahl)",
    )
    ap.add_argument("--replace-month-slice", action="store_true")
    ap.add_argument("--max-files", type=int, default=None)
    args = ap.parse_args()

    cfg_path = (
        (PROJECT_ROOT / args.config).resolve()
        if not Path(args.config).is_absolute()
        else Path(args.config).resolve()
    )
    cfg = load_pipeline_config(cfg_path)
    # HINWEIS: cfg ist ein frozen dataclass (pipeline_config_v1_1), daher darf es nicht verändert werden.
    # CLI-Überschreibung ohne Änderung von cfg ermöglichen.
    env_file = args.env_file or cfg.env_file

    env_path = (
        (PROJECT_ROOT / env_file).resolve()
        if not Path(env_file).is_absolute()
        else Path(env_file).resolve()
    )
    load_env_file(env_path)
    if env_path.exists():
        print(f"Umgebung geladen aus: {env_path}")

    # Plan(s) aufbauen
    project_root, storage_root, plans = build_plan(cfg, project_root=PROJECT_ROOT)

    # Monatsauswahl überschreiben: einmaligen Plan erstellen, auch wenn YAML ihn nicht enthält
    if args.month_key:
        plans = [
            build_month_plan(
                cfg,
                project_root=project_root,
                storage_root=storage_root,
                month_key=args.month_key,
            )
        ]

    if not plans:
        raise SystemExit("Keine Monate ausgewählt.")

    conn = db_connect()
    try:
        grand_total = 0
        for plan in plans:
            if not plan.old_enabled or plan.old_detectors_glob_abs is None:
                print(f"[SKIP] month={plan.month_key} old_detectors deaktiviert")
                continue

            start, end = month_key_to_range(plan.month_key)
            files = expand_glob(plan.old_detectors_glob_abs)
            if args.max_files is not None:
                files = files[: args.max_files]

            if not files:
                print(
                    f"[SKIP] month={plan.month_key} keine Dateien gefunden: {plan.old_detectors_glob_abs}"
                )
                continue

            with conn.cursor() as cur:
                if args.replace_month_slice:
                    replace_month_slice(cur, start, end)

                total = 0
                for f in files:
                    total += copy_rows(cur, iter_rows_from_gz(f, start=start, end=end))

            conn.commit()
            grand_total += total
            print(
                f"[OK] month={plan.month_key} inserted={total} files={len(files)} -> staging.stg_old_det_val_hr"
            )

        print(f"[DONE] total_inserted={grand_total}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
