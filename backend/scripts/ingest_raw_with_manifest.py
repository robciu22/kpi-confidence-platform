#!/usr/bin/env python3
"""
Ingestion von Berlin-Verkehrsdetektion-Dateien in Postgres (raw.traffic_rows) mit Manifest-Tracking.

Ziele werden in einer YAML-Konfigurationsdatei definiert (Schlüssel: targets). Jedes Ziel muss enthalten:
  - month_key: "YYYY_MM" (z.B. "2023_03")
  - dataset_version: "old" | "new"
  - source_type: "detectors" | "cross_sections"
  - url: Remote-URL (https://...) ODER local_path: lokaler Dateipfad
Optional:
  - delimiter: CSV-Trennzeichen (Standard ';')
  - encoding: CSV-Zeichenkodierung (Standard 'utf-8')
  - skip_header: true/false (Standard true)
  - tgz_member: wenn url eine .tgz/.tar.gz mit mehreren Dateien ist, Member per Substring-Match auswählen

Umgebung:
  Liest DB-Zugangsdaten aus einer .env-Datei (Standard: .env im Projektroot) oder OS-Umgebungsvariablen:
    PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE

Ausführungssemantik (Failfast + sichere Commits):
  - Einen Run starten (ingestion.ingestion_runs) und sofort committen (run_id bleibt erhalten).
  - Für jedes Ziel:
      * ingestion.file_manifest aktualisieren/einfügen (last_seen + Prüfsumme + Bytes).
      * Wenn unverändert und zuvor erfolgreich => SKIP protokollieren und committen.
      * Sonst: Roh-Monatsslice löschen, Zeilen einfügen, Manifest-Status auf success setzen, run_files protokollieren, committen.
  - Beim ersten Fehler: Rollback des aktuellen Ziels, best-effort Fehlermarkierung in Datei/Run, dann abbrechen.

WICHTIGE Schema-Ausrichtung:
  raw.traffic_rows verwendete Spalten: run_id, source_url, month_key, dataset_version, source_type, row_number, payload
  (payload ist JSONB). Keine "row_data"-Spalte wird verwendet.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import os
import re
import tarfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import requests
import yaml
from dotenv import load_dotenv

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


# ----------------------------
# Hilfsfunktionen
# ----------------------------


def sha256_hex(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def load_yaml(path: str | Path) -> dict:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Konfigurationsdatei nicht gefunden: {p}")
    with p.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _as_int(value: Any, default: int) -> int:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except Exception:
        return default


# ----------------------------
# DB-Verbindung
# ----------------------------


def _looks_like_header(row: List[str]) -> bool:
    """Heuristik: Wenn mindestens ein Feld Buchstaben enthält, ist es sehr wahrscheinlich ein Header."""
    for cell in row:
        if cell is None:
            continue
        if re.search(r"[A-Za-zÄÖÜäöü]", str(cell)):
            return True
        # klassische Header-Patterns
        if "_" in str(cell) or " " in str(cell):
            return True
    return False


def _is_tgz_source(source_id: str) -> bool:
    s = (source_id or "").lower()
    return s.endswith(".tgz") or s.endswith(".tar.gz") or s.endswith(".tar.gzip")


def _iter_rows_from_csv_fileobj(
    fobj,
    *,
    delimiter: str,
    encoding: str,
    skip_header: bool,
) -> Iterator[Dict[str, Any]]:
    """Liest CSV aus einem File-Objekt und liefert jede Zeile als Dict.

    - Wenn skip_header=True und die erste Zeile wie ein Header aussieht -> nutze sie als Spaltennamen.
    - Sonst -> generiere Spaltennamen col_1..col_n (headerlose CSV).
    """
    text = io.TextIOWrapper(fobj, encoding=encoding, newline="")
    reader = csv.reader(text, delimiter=delimiter)

    first = next(reader, None)
    if first is None:
        return

    # Leerzeichen trimmen
    first = [c.strip() if isinstance(c, str) else c for c in first]

    if skip_header and _looks_like_header(first):
        headers = [(h if h else f"col_{i+1}") for i, h in enumerate(first)]
        for row in reader:
            row = [c.strip() if isinstance(c, str) else c for c in row]
            # auffüllen / abschneiden
            if len(row) < len(headers):
                row = row + [None] * (len(headers) - len(row))
            elif len(row) > len(headers):
                row = row[: len(headers)]
            yield {headers[i]: row[i] for i in range(len(headers))}
        return

    # ohne Header: erste Zeile ist bereits Daten
    ncols = len(first)
    headers = [f"col_{i+1}" for i in range(ncols)]
    # erste Zeile ausgeben
    first_row = first + [None] * (ncols - len(first))
    yield {headers[i]: first_row[i] for i in range(ncols)}
    for row in reader:
        row = [c.strip() if isinstance(c, str) else c for c in row]
        if len(row) < ncols:
            row = row + [None] * (ncols - len(row))
        elif len(row) > ncols:
            row = row[:ncols]
        yield {headers[i]: row[i] for i in range(ncols)}


def _iter_rows_from_csv_bytes(
    blob: bytes,
    *,
    delimiter: str,
    encoding: str,
    skip_header: bool,
) -> Iterator[Dict[str, Any]]:
    return _iter_rows_from_csv_fileobj(
        io.BytesIO(blob),
        delimiter=delimiter,
        encoding=encoding,
        skip_header=skip_header,
    )


def _iter_rows_from_tgz_bytes(
    tgz_blob: bytes,
    *,
    delimiter: str,
    encoding: str,
    skip_header: bool,
    member_name_contains: Optional[str] = None,
    max_members: Optional[int] = None,
) -> Iterator[Dict[str, Any]]:
    """Iteriert alle CSV-Dateien innerhalb einer .tgz/.tar.gz und yieldet deren Zeilen."""
    with tarfile.open(fileobj=io.BytesIO(tgz_blob), mode="r:gz") as tf:
        members = [
            m for m in tf.getmembers() if m.isfile() and m.name.lower().endswith(".csv")
        ]
        # optionaler Filter
        if member_name_contains:
            members = [m for m in members if member_name_contains in m.name]

        # stabile Reihenfolge: nach Name
        members.sort(key=lambda m: m.name)

        if max_members is not None:
            members = members[:max_members]

        for m_idx, m in enumerate(members, start=1):
            f = tf.extractfile(m)
            if not f:
                continue

            # Heuristik für Detektor-ID
            base = os.path.basename(m.name)
            stem = base[:-4] if base.lower().endswith(".csv") else base
            parts = stem.split("_")
            detector_id = parts[0] if parts else None
            detector_variant = "_".join(parts[1:]) if len(parts) > 1 else None

            row_in_member = 0
            for row in _iter_rows_from_csv_fileobj(
                f, delimiter=delimiter, encoding=encoding, skip_header=skip_header
            ):
                row_in_member += 1
                # minimale Metadaten anhängen (keine Schema-Änderung nötig)
                row["__meta"] = {
                    "archive_member": m.name,
                    "member_index": m_idx,
                    "row_in_member": row_in_member,
                    "detector_id": detector_id,
                    "detector_variant": detector_variant,
                }
                # ----------------------------
                # DB-Verbindung
                # ----------------------------

                yield row


def load_env(env_file: Optional[str]) -> Optional[Path]:
    """Umgebungsvariablen aus env_file laden, falls angegeben, sonst aus .env im aktuellen Verzeichnis."""
    if env_file:
        p = Path(env_file).expanduser().resolve()
        if not p.exists():
            raise FileNotFoundError(f"--env-file nicht gefunden: {p}")
        load_dotenv(p, override=False)
        return p

    p = Path(".env").resolve()
    if p.exists():
        load_dotenv(p, override=False)
        return p
    return None


# ----------------------------
# DB-Verbindung
# ----------------------------


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
        raise RuntimeError(
            "Fehlende " + "/".join(missing) + " in Umgebung/.env. "
            "Erwartete Schlüssel: PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE"
        )

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


# ----------------------------
# DB-Operationen (ingestion-Schema)
# ----------------------------


def run_start(cur: psycopg.Cursor, triggered_by: str, payload: dict) -> str:
    cur.execute(
        """
        INSERT INTO ingestion.ingestion_runs (triggered_by, status, started_at, payload)
        VALUES (%s, %s, now(), %s::jsonb)
        RETURNING run_id
        """,
        (triggered_by, "running", json.dumps(payload)),
    )
    return cur.fetchone()["run_id"]


def run_end(cur: psycopg.Cursor, run_id: str, status: str, notes: str) -> None:
    cur.execute(
        """
        UPDATE ingestion.ingestion_runs
        SET status = %s,
            ended_at = now(),
            notes = %s
        WHERE run_id = %s
        """,
        (status, notes, run_id),
    )


def run_files_log(
    cur: psycopg.Cursor,
    run_id: str,
    source_url: str,
    action: str,
    row_count: Optional[int],
    message: Optional[str],
) -> None:
    cur.execute(
        """
        INSERT INTO ingestion.run_files (run_id, source_url, action, row_count, message, created_at)
        VALUES (%s, %s, %s, %s, %s, now())
        """,
        (run_id, source_url, action, row_count, message),
    )


def manifest_get(cur: psycopg.Cursor, source_url: str) -> Optional[dict]:
    cur.execute(
        """
        SELECT source_url, checksum_sha256, last_status
        FROM ingestion.file_manifest
        WHERE source_url = %s
        """,
        (source_url,),
    )
    return cur.fetchone()


def manifest_upsert(
    cur: psycopg.Cursor,
    *,
    source_url: str,
    month_key: str,
    dataset_version: str,
    source_type: str,
    checksum: str,
    bytes_len: int,
    last_modified: Optional[str],
    metadata: dict,
) -> None:
    cur.execute(
        """
        INSERT INTO ingestion.file_manifest
            (source_url, month_key, dataset_version, source_type,
             checksum_sha256, bytes, last_modified, first_seen_at, last_seen_at,
             last_status, metadata)
        VALUES
            (%s, %s, %s, %s,
             %s, %s, %s, now(), now(),
             COALESCE((SELECT last_status FROM ingestion.file_manifest WHERE source_url=%s), 'unknown'),
             %s::jsonb)
        ON CONFLICT (source_url)
        DO UPDATE SET
            month_key = EXCLUDED.month_key,
            dataset_version = EXCLUDED.dataset_version,
            source_type = EXCLUDED.source_type,
            checksum_sha256 = EXCLUDED.checksum_sha256,
            bytes = EXCLUDED.bytes,
            last_modified = EXCLUDED.last_modified,
            last_seen_at = now(),
            metadata = EXCLUDED.metadata
        """,
        (
            source_url,
            month_key,
            dataset_version,
            source_type,
            checksum,
            bytes_len,
            last_modified,
            source_url,
            json.dumps(metadata),
        ),
    )


def manifest_set_status(
    cur: psycopg.Cursor, source_url: str, run_id: str, status: str
) -> None:
    cur.execute(
        """
        UPDATE ingestion.file_manifest
        SET last_status = %s,
            last_ingestion_run_id = %s,
            last_ingested_at = CASE WHEN %s = 'success' THEN now() ELSE last_ingested_at END
        WHERE source_url = %s
        """,
        (status, run_id, status, source_url),
    )


# ----------------------------
# RAW-Operationen
# ----------------------------


def raw_delete_month(
    cur: psycopg.Cursor, month_key: str, dataset_version: str, source_type: str
) -> None:
    cur.execute(
        """
        DELETE FROM raw.traffic_rows
        WHERE month_key = %s AND dataset_version = %s AND source_type = %s
        """,
        (month_key, dataset_version, source_type),
    )


def raw_insert_rows(
    cur: psycopg.Cursor,
    run_id: str,
    source_url: str,
    month_key: str,
    dataset_version: str,
    source_type: str,
    rows_iter: Iterable[Dict[str, Any]],
    batch_size: int = 1000,
) -> int:
    """Geparste Zeilen in raw.traffic_rows einfügen.

    WICHTIG: Das Schema erwartet eine JSONB-Spalte namens 'payload' (NICHT 'row_data').
    """

    sql = """
        INSERT INTO raw.traffic_rows
            (run_id, source_url, month_key, dataset_version, source_type, row_number, payload, loaded_at)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, now())
    """

    batch: List[Tuple[Any, ...]] = []
    total = 0
    row_number = 0

    for row in rows_iter:
        row_number += 1
        batch.append(
            (
                run_id,
                source_url,
                month_key,
                dataset_version,
                source_type,
                row_number,
                Jsonb(row),
            )
        )
        if len(batch) >= batch_size:
            cur.executemany(sql, batch)
            total += len(batch)
            batch.clear()

    if batch:
        cur.executemany(sql, batch)
        total += len(batch)

    return total


# ----------------------------
# Ziele lesen
# ----------------------------


@dataclass
class Target:
    month_key: str
    dataset_version: str
    source_type: str
    url: Optional[str] = None
    local_path: Optional[str] = None
    delimiter: str = ";"
    encoding: str = "utf-8"
    skip_header: bool = True

    # Optional: Wenn die Quelle eine .tgz/.tar.gz ist:
    # - tgz_member: Filter (Substring), um nur bestimmte Dateien im Archiv zu laden
    # - max_archive_members: harte Begrenzung für einen schnellen Smoke-Test
    tgz_member: Optional[str] = None
    max_archive_members: Optional[int] = None

    @staticmethod
    def from_dict(d: dict) -> "Target":
        return Target(
            month_key=str(d["month_key"]),
            dataset_version=str(d["dataset_version"]),
            source_type=str(d["source_type"]),
            url=d.get("url") or d.get("source_url"),
            local_path=d.get("local_path"),
            delimiter=d.get("delimiter") or ";",
            encoding=d.get("encoding") or "utf-8",
            skip_header=bool(d.get("skip_header", True)),
            tgz_member=d.get("tgz_member"),
            max_archive_members=(_as_int(d.get("max_archive_members"), 0) or None),
        )


def read_bytes_from_target(t: Target) -> Tuple[bytes, str, Dict[str, Any]]:
    """Lädt Bytes von URL oder local_path.

    Wichtig:
    - .gz (z.B. *.csv.gz) wird entpackt (gzip), ABER:
    - .tgz/.tar.gz bleibt als TAR+GZIP-Archiv *unentpackt* und wird später in iter_rows_from_blob verarbeitet,
      damit wir ALLE CSV-Dateien im Archiv ingestieren können (oder gefiltert via tgz_member).
    """
    http_meta: Dict[str, Any] = {}

    if t.url:
        r = requests.get(t.url, timeout=60)
        r.raise_for_status()
        blob = r.content
        source_id = t.url
        http_meta = {
            "status_code": r.status_code,
            "content_length": int(r.headers.get("Content-Length") or len(blob)),
            "etag": r.headers.get("ETag"),
            "last_modified": r.headers.get("Last-Modified"),
        }
    elif t.local_path:
        p = Path(t.local_path)
        blob = p.read_bytes()
        source_id = str(p)
        http_meta = {"local_path": str(p), "content_length": len(blob)}
    else:
        raise ValueError("Ziel muss entweder 'url' oder 'local_path' enthalten.")

    # Gzip-Dekomprimierung nur für einfache .gz-Dateien (NICHT für .tgz/.tar.gz)
    if source_id.lower().endswith(".gz") and not _is_tgz_source(source_id):
        blob = gzip.decompress(blob)

    return blob, source_id, http_meta


def iter_rows_from_blob(
    blob: bytes,
    delimiter: str,
    encoding: str,
    skip_header: bool,
    *,
    source_id: str,
    member_name_contains: Optional[str] = None,
    max_members: Optional[int] = None,
) -> Iterator[Dict[str, Any]]:
    """Liest 'blob' und yieldet Zeilen als Dict.

    - Normales CSV/CSV-bytes: wird direkt geparst.
    - .tgz/.tar.gz: alle CSV-Dateien im Archiv werden geparst (optional gefiltert).
    """
    if _is_tgz_source(source_id):
        yield from _iter_rows_from_tgz_bytes(
            blob,
            delimiter=delimiter,
            encoding=encoding,
            skip_header=skip_header,
            member_name_contains=member_name_contains,
            max_members=max_members,
        )
        return

    yield from _iter_rows_from_csv_bytes(
        blob,
        delimiter=delimiter,
        encoding=encoding,
        skip_header=skip_header,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Berlin-Verkehrsdetektion-Dateien in Postgres (raw.traffic_rows) mit Manifest-Tracking einlesen."
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Pfad zur YAML-Konfigurationsdatei (z.B. config/ingestion_targets.yaml)",
    )
    parser.add_argument(
        "--triggered-by",
        default=None,
        help="Freitext für ingestion_runs.triggered_by (z.B. manual, n8n)",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Pfad zur .env-Datei. Standard: .env im aktuellen Verzeichnis.",
    )
    args = parser.parse_args()

    cfg = load_yaml(args.config)
    targets_raw = cfg.get("targets") or []
    if not targets_raw:
        raise SystemExit("Config enthält keine Ziele. Erwarteter YAML-Schlüssel: targets:")

    targets = [Target.from_dict(d) for d in targets_raw]
    triggered_by = args.triggered_by or cfg.get("triggered_by") or "manual"

    loaded_env = load_env(args.env_file)
    if loaded_env:
        print(f"Umgebung geladen aus: {loaded_env}")

    conn: Optional[psycopg.Connection] = None
    run_id: Optional[str] = None

    try:
        conn = db_connect()

        # 1) Run starten + sofort committen
        with conn.cursor() as cur:
            run_id = run_start(
                cur, triggered_by, {"targets": [t.__dict__ for t in targets]}
            )
            conn.commit()
            print("run_id:", run_id)

        # 2) Ziele nacheinander verarbeiten
        for t in targets:
            source_url: Optional[str] = None

            try:
                with conn.cursor() as cur:
                    blob, source_url, http_meta = read_bytes_from_target(t)
                    checksum = sha256_hex(blob)

                    prev = manifest_get(cur, source_url)

                    manifest_upsert(
                        cur,
                        source_url=source_url,
                        month_key=t.month_key,
                        dataset_version=t.dataset_version,
                        source_type=t.source_type,
                        checksum=checksum,
                        bytes_len=len(blob),
                        last_modified=http_meta.get("last_modified_dt"),
                        metadata={"note": "python ingest", **http_meta},
                    )

                    # Unveränderte Datei überspringen
                    if (
                        prev
                        and prev.get("checksum_sha256") == checksum
                        and prev.get("last_status") == "success"
                    ):
                        manifest_set_status(
                            cur, source_url, run_id, "skipped_unchanged"
                        )
                        run_files_log(
                            cur,
                            run_id,
                            source_url,
                            "skipped_unchanged",
                            None,
                            "Prüfsumme unverändert",
                        )
                        conn.commit()
                        print(
                            f"SKIP   {t.month_key} {t.dataset_version} {t.source_type} (unverändert)"
                        )
                        continue

                    raw_delete_month(cur, t.month_key, t.dataset_version, t.source_type)

                    rows_iter = iter_rows_from_blob(
                        blob,
                        t.delimiter,
                        t.encoding,
                        t.skip_header,
                        source_id=source_url,
                        member_name_contains=t.tgz_member,
                        max_members=t.max_archive_members,
                    )

                    row_count = raw_insert_rows(
                        cur,
                        run_id,
                        source_url,
                        t.month_key,
                        t.dataset_version,
                        t.source_type,
                        rows_iter,
                    )

                    manifest_set_status(cur, source_url, run_id, "success")
                    run_files_log(cur, run_id, source_url, "ingested", row_count, None)

                    conn.commit()
                    print(
                        f"INGEST {t.month_key} {t.dataset_version} {t.source_type} -> {row_count} Zeilen"
                    )

            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass

                # best-effort Fehler-Logging pro Datei
                try:
                    if run_id and source_url:
                        with conn.cursor() as cur:
                            manifest_set_status(cur, source_url, run_id, "failed")
                            run_files_log(
                                cur, run_id, source_url, "failed", None, str(e)[:500]
                            )
                        conn.commit()
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:
                        pass

                raise

        # 3) Run erfolgreich abschließen
        if run_id:
            with conn.cursor() as cur:
                run_end(cur, run_id, "success", "python ingest abgeschlossen")
            conn.commit()

    except Exception as e:
        if conn is not None and run_id is not None:
            try:
                with conn.cursor() as cur:
                    run_end(cur, run_id, "failed", str(e)[:500])
                conn.commit()
            except Exception:
                pass
        raise

    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
