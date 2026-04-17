#!/usr/bin/env python3
"""
scripts/register_file_manifest_v1_0.py

Manifest/Register-Schritt (DB-Feedback), unabhängig von raw.traffic_rows.

Wozu?
- ingestion.file_manifest ist die zentrale "Wahrheit" über Dateien:
  sha256, bytes, last_status, last_ingested_at, metadata (z.B. local_path), etc.
- n8n kann später ETag/sha256 über die DB vergleichen, ohne Datei erneut zu laden.
- Das Script ist absichtlich eigenständig: Datei kann lokal schon existieren
  ODER es kann (optional) von einer URL in den Storage geladen werden.

Wichtige Fakten (passt zur DB/Phase-A Semantik):
- ingestion.file_manifest ist eindeutig über source_url (Primary Key).
- Upsert per source_url: checksum_sha256/bytes/metadata/last_seen werden aktualisiert.
- last_ingested_at wird nur bei status=success gesetzt.
- Run-Tracking: schreibt in ingestion.ingestion_runs und ingestion.run_files.

Status-Konzept (vereinfacht):
- success: Datei erfolgreich geprüft/registriert (und ggf. heruntergeladen)
- skipped_unchanged: sha256 identisch zum letzten Stand -> keine Änderung
- error_*: irgendein Fehler (Download/IO/DB)

Beispiele:
  # lokale Datei schon vorhanden:
  python scripts/register_file_manifest_v1_0.py \
    --source-url "data/source/verkehrsdetektion/monthly/2024_04/new_detectors/detektoren_2024_04.tgz" \
    --month-key "2024_04" --dataset-version "new" --source-type "detectors" \
    --local-path "data/source/verkehrsdetektion/monthly/2024_04/new_detectors/detektoren_2024_04.tgz" \
    --triggered-by "manual" --env-file "./.env"

  # Remote-URL (optional, Download in storage_root):
  python scripts/register_file_manifest_v1_0.py \
    --source-url "https://.../detektoren_2024_04.tgz" \
    --month-key "2024_04" --dataset-version "new" --source-type "detectors" \
    --triggered-by "n8n" --storage-root "./data/source"
"""


from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests
from dotenv import load_dotenv

import psycopg
from psycopg.rows import dict_row


# ----------------------------
# Hilfsfunktionen
# ----------------------------


def _as_int(v: Any, default: int) -> int:
    if v is None or v == "":
        return default
    try:
        return int(v)
    except Exception:
        return default


# -----------------------------------------------------------------------------
# .env-Verarbeitung
# -----------------------------------------------------------------------------

def load_env(env_file: Optional[str]) -> Optional[Path]:
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


# -----------------------------------------------------------------------------
# DB-Verbindung (psycopg)
# -----------------------------------------------------------------------------

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
        raise RuntimeError("Fehlende " + "/".join(missing) + " in Umgebung/.env")

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


# -----------------------------------------------------------------------------
# SHA256-Berechnung (zweite Validierung neben ETag)
# -----------------------------------------------------------------------------

def sha256_file(path: Path, chunk_size: int) -> str:
    import hashlib

    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def parse_http_date(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt = parsedate_to_datetime(s)
        # Zeitzonen-Bewusstsein sicherstellen
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def derive_local_path(
    *,
    storage_root: Path,
    month_key: str,
    dataset_version: str,
    source_type: str,
    source_url: str,
) -> Path:
    """
    Stabilen lokalen Pfad ableiten, wenn nur eine Remote-URL angegeben wurde.
    Einfache Struktur: <root>/verkehrsdetektion/monthly/<YYYY_MM>/<dataset>_<type>/<basename>
    """
    basename = Path(source_url.split("?")[0]).name or "download.bin"
    rel = (
        Path("verkehrsdetektion")
        / "monthly"
        / month_key
        / f"{dataset_version}_{source_type}"
        / basename
    )
    return (storage_root / rel).resolve()


def download_file(
    url: str, dest: Path
) -> Tuple[int, Optional[str], Optional[str], Optional[int]]:
    """
    Lädt URL nach dest herunter. Gibt (status_code, etag, last_modified, content_length) zurück.
    """
    ensure_dir(dest.parent)
    tmp = dest.with_suffix(dest.suffix + ".tmp")

    with requests.get(url, stream=True, timeout=60) as r:
        status = r.status_code
        r.raise_for_status()
        etag = r.headers.get("ETag")
        last_modified = r.headers.get("Last-Modified")
        cl = r.headers.get("Content-Length")
        content_length = int(cl) if cl and cl.isdigit() else None

        with tmp.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    tmp.replace(dest)
    return status, etag, last_modified, content_length


# ----------------------------
# DB-Operationen (ingestion-Schema)
# Spiegelt Phase-A-Semantik, aber ohne raw.traffic_rows.
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
    last_modified_dt: Optional[datetime],
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
            last_modified_dt,
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
# Main
# ----------------------------


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Eine Datei in ingestion.file_manifest registrieren (sha256-Wahrheit) und Feedback in DB protokollieren."
    )
    ap.add_argument(
        "--env-file", default=None, help="Pfad zur .env (Standard: .env im aktuellen Verzeichnis)"
    )
    ap.add_argument(
        "--triggered-by",
        default="manual",
        help="Wert für ingestion_runs.triggered_by (z.B. n8n)",
    )
    ap.add_argument(
        "--storage-root",
        default=None,
        help="Lokales Storage-Verzeichnis (Standard: FILE_STORAGE_ROOT env oder ./data/source)",
    )

    ap.add_argument(
        "--source-url",
        required=True,
        help="Eindeutiger Schlüssel für file_manifest (PK). URL oder lokaler Pfad-String.",
    )
    ap.add_argument("--month-key", required=True, help="YYYY_MM")
    ap.add_argument(
        "--dataset-version", required=True, choices=["old", "new"], help="old|new"
    )
    ap.add_argument(
        "--source-type",
        required=True,
        choices=["detectors", "cross_sections"],
        help="detectors|cross_sections",
    )

    ap.add_argument(
        "--local-path",
        default=None,
        help="Falls angegeben, wird diese lokale Datei gehasht. Falls nicht angegeben und source-url ist lokal, wird source-url verwendet.",
    )
    ap.add_argument(
        "--download",
        action="store_true",
        help="Falls source-url remote ist, vor dem Hashen in den Storage herunterladen.",
    )

    ap.add_argument("--etag", default=None, help="ETag von n8n HEAD/GET (optional)")
    ap.add_argument(
        "--last-modified", default=None, help="Last-Modified HTTP-Header (optional)"
    )
    ap.add_argument(
        "--content-length",
        type=int,
        default=None,
        help="Content-Length aus HTTP (optional)",
    )
    ap.add_argument(
        "--status-code", type=int, default=None, help="HTTP-Statuscode (optional)"
    )

    args = ap.parse_args()

    loaded = load_env(args.env_file)
    if loaded:
        print(f"Umgebung geladen aus: {loaded}")

    chunk_size = _as_int(os.getenv("SHA256_CHUNK_SIZE_BYTES"), 8 * 1024 * 1024)

    storage_root = (
        Path(args.storage_root or os.getenv("FILE_STORAGE_ROOT") or "./data/source")
        .expanduser()
        .resolve()
    )

    src = args.source_url

    # Lokalen Dateipfad bestimmen
    local_path: Optional[Path] = None
    if args.local_path:
        local_path = Path(args.local_path).expanduser().resolve()
    else:
        # wenn source_url wie ein lokaler Pfad aussieht, direkt verwenden
        if re.match(r"^(/|\.{1,2}/|data/)", src):
            local_path = Path(src).expanduser().resolve()

    # Download, wenn angefordert und source_url ist http(s)
    http_meta: Dict[str, Any] = {}
    downloaded = False
    if args.download and src.lower().startswith(("http://", "https://")):
        if local_path is None:
            local_path = derive_local_path(
                storage_root=storage_root,
                month_key=args.month_key,
                dataset_version=args.dataset_version,
                source_type=args.source_type,
                source_url=src,
            )
        status, etag, lm, cl = download_file(src, local_path)
        downloaded = True
        http_meta.update(
            {
                "status_code": status,
                "etag": etag,
                "last_modified": lm,
                "content_length": cl,
            }
        )

    # Von n8n übergebene Header überlagern (falls vorhanden)
    if args.status_code is not None:
        http_meta["status_code"] = args.status_code
    if args.etag:
        http_meta["etag"] = args.etag
    if args.last_modified:
        http_meta["last_modified"] = args.last_modified
    if args.content_length is not None:
        http_meta["content_length"] = args.content_length

    if local_path is None:
        raise SystemExit(
            "Kein lokaler Dateipfad verfügbar. --local-path angeben oder --download für Remote-URLs verwenden."
        )

    if not local_path.exists():
        raise SystemExit(f"Lokale Datei nicht gefunden: {local_path}")

    # Lokale Datei hashen WIE GESPEICHERT (komprimiert)
    checksum = sha256_file(local_path, chunk_size=chunk_size)
    bytes_len = local_path.stat().st_size
    last_modified_dt = parse_http_date(http_meta.get("last_modified"))

    metadata = {
        "note": "python manifest register",
        "local_path": str(local_path),
        "downloaded": downloaded,
        **http_meta,
    }

    conn = db_connect()
    run_id: Optional[str] = None

    try:
        with conn.cursor() as cur:
            run_id = run_start(
                cur,
                args.triggered_by,
                {
                    "source_url": args.source_url,
                    "month_key": args.month_key,
                    "dataset_version": args.dataset_version,
                    "source_type": args.source_type,
                    "local_path": str(local_path),
                    "download": args.download,
                },
            )
            conn.commit()

        with conn.cursor() as cur:
            prev = manifest_get(cur, args.source_url)
            prev_checksum = prev["checksum_sha256"] if prev else None
            prev_status = prev["last_status"] if prev else None

            manifest_upsert(
                cur,
                source_url=args.source_url,
                month_key=args.month_key,
                dataset_version=args.dataset_version,
                source_type=args.source_type,
                checksum=checksum,
                bytes_len=bytes_len,
                last_modified_dt=last_modified_dt,
                metadata=metadata,
            )

            unchanged = (prev_checksum == checksum) and (prev_status == "success")
            if unchanged:
                manifest_set_status(cur, args.source_url, run_id, "skipped_unchanged")
                run_files_log(
                    cur,
                    run_id,
                    args.source_url,
                    "skip",
                    None,
                    "sha256 unverändert (vorheriger Status: success)",
                )
                run_end(cur, run_id, "success", "skipped_unchanged")
                conn.commit()
                print(
                    f"[OK] run_id={run_id} status=skipped_unchanged sha256={checksum} bytes={bytes_len}"
                )
            else:
                manifest_set_status(cur, args.source_url, run_id, "success")
                run_files_log(
                    cur,
                    run_id,
                    args.source_url,
                    "register",
                    None,
                    "sha256 berechnet / Manifest aktualisiert",
                )
                run_end(cur, run_id, "success", "success")
                conn.commit()
                print(
                    f"[OK] run_id={run_id} status=success sha256={checksum} bytes={bytes_len}"
                )

    except Exception as e:
        conn.rollback()
        if run_id:
            try:
                with conn.cursor() as cur:
                    manifest_set_status(cur, args.source_url, run_id, "failed")
                    run_files_log(
                        cur,
                        run_id,
                        args.source_url,
                        "error",
                        None,
                        f"{type(e).__name__}: {e}",
                    )
                    run_end(cur, run_id, "failed", f"{type(e).__name__}: {e}")
                    conn.commit()
            except Exception:
                conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
