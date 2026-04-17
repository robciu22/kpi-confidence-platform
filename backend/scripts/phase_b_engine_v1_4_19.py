#!/usr/bin/env python3
"""
scripts/phase_b_engine_v1_4_19.py

KPI-Confidence-Scoring – Phase B Engine (v1.4.18 + BI-Allowlist hotfix8 (month-key override))

Ziel dieser Version:
- Orchestriert Stage-Loader (NEW tgz + OLD csv.gz) über run_stage_loaders_v1_1.py
- Baut Core Facts aus staging.* (kein raw.traffic_rows mehr)
- Rechnet QA-Features, KPI-Values und KPI-Confidence (snapshot-style upserts)
- ML bleibt bewusst als Platzhalter (sensor-spezifische Anomalie optional/extern)

Wichtig:
- Diese Engine nutzt die flexible YAML-Konfiguration (config/pipeline_ingestion.yaml)
  über src/config/pipeline_config_v1_0.py
- Die DB-Schemata/Tables werden NICHT „neu erfunden“ – nur IF NOT EXISTS abgesichert.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, Iterable, List, Optional, Tuple

def _resolve_policy_allowed_layouts(month_key: str) -> set[str]:
    """Gibt die erlaubten source_layouts gemäß Projekt-Policy zurück.

    Policy:
    - 2017–2022: {'old'}
    - ab 2023:   {'new'}
    """
    try:
        year = int(month_key.split("_", 1)[0])
    except Exception:
        year = 9999  # im Zweifel NEW-only
    return {"old"} if year <= 2022 else {"new"}


def _apply_source_layout_policy(plan, month_key: str):
    """Erzwingt die Projekt-Policy (OLD/NEW) auf dem MonthPlan."""
    allowed = _resolve_policy_allowed_layouts(month_key)
    old_before = getattr(plan, "old_enabled", None)
    new_before = getattr(plan, "new_enabled", None)

    if hasattr(plan, "old_enabled"):
        plan.old_enabled = "old" in allowed
    if hasattr(plan, "new_enabled"):
        plan.new_enabled = "new" in allowed

    if old_before is not None and new_before is not None:
        if old_before != plan.old_enabled or new_before != plan.new_enabled:
            print(
                f"[POLICY] Override LayoutPlan: month={month_key} "
                f"old_enabled {old_before}→{plan.old_enabled}, "
                f"new_enabled {new_before}→{plan.new_enabled}"
            )
    return plan


def _purge_disallowed_staging(cur, start_date: date, end_date: date, tz: str, allowed_layouts: set[str]) -> None:
    """Löscht (nur) disallowed Staging-Daten für den Monat, um Vermischung zu verhindern.

    Hintergrund: Wenn OLD/NEW vorher mal geladen wurde, kann in staging.* noch Altbestand liegen.
    Wenn wir z.B. ab 2023 strikt NEW wollen, muss staging.stg_old_* für diesen Monat leer sein.
    """
    # OLD disallowed?
    if "old" not in allowed_layouts:
        # OLD detector values (tag basiert auf Ortszeit; wir löschen per tag-range)
        if regclass_exists(cur, "staging.stg_old_det_val_hr"):
            cur.execute(
                "DELETE FROM staging.stg_old_det_val_hr WHERE tag >= %s AND tag < %s;",
                (start_date, end_date),
            )
        if regclass_exists(cur, "staging.stg_old_mq_hr"):
            cur.execute(
                "DELETE FROM staging.stg_old_mq_hr WHERE tag >= %s AND tag < %s;",
                (start_date, end_date),
            )

    # NEW disallowed?
    if "new" not in allowed_layouts:
        if regclass_exists(cur, "staging.stg_new_detector_hourly"):
            cur.execute(
                "DELETE FROM staging.stg_new_detector_hourly WHERE datum_ortszeit >= %s AND datum_ortszeit < %s;",
                (start_date, end_date),
            )

# ---------------------------------------------------------------------------
# DB-Library Kompatibilität
# ---------------------------------------------------------------------------
#
# In eurem Projekt wurde bereits an mehreren Stellen **psycopg v3** genutzt.
# Manche Umgebungen haben jedoch nur **psycopg** (v3) installiert – nicht
# automatisch auch **psycopg2**.
#
# Damit das Script in beiden Welten stabil läuft, nutzen wir hier eine kleine
# Kompatibilitätsschicht:
#
# - Wenn `psycopg2` verfügbar ist → verwende `RealDictCursor`.
# - Sonst → verwende `psycopg` (v3) mit `dict_row`.
#
# So vermeiden wir, dass Teammitglieder an einem fehlenden Paket hängen bleiben.

try:
    import psycopg2  # type: ignore
    from psycopg2.extras import RealDictCursor  # type: ignore

    _DB_DRIVER = "psycopg2"

    def _connect(dsn: str):
        return psycopg2.connect(dsn)

    def _dict_cursor(conn):
        return conn.cursor(cursor_factory=RealDictCursor)

except ModuleNotFoundError:  # pragma: no cover
    import psycopg  # type: ignore
    from psycopg.rows import dict_row  # type: ignore

    _DB_DRIVER = "psycopg"

    def _connect(dsn: str):
        return psycopg.connect(dsn)

    def _dict_cursor(conn):
        return conn.cursor(row_factory=dict_row)
from dotenv import load_dotenv


# -----------------------------
# Bootstrap / Project Root
# -----------------------------
def find_project_root(start: Path) -> Path:
    """
    Finds the project root by walking up until a folder containing "src" is found.
    """
    cur = start.resolve()
    for _ in range(12):
        if (cur / "src").exists() and (cur / "src").is_dir():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    # Fallback: assume current working dir is project root
    return start.resolve()


def ensure_sys_path(project_root: Path) -> None:
    """Fügt das Projektverzeichnis zum Python-Suchpfad hinzu, falls noch nicht vorhanden.

    Notwendig, damit src/config/... und andere lokale Module importiert werden können.
    """
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))



def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def import_pipeline_config(project_root: Path):
    """
    Robust import for src/config/pipeline_config_v1_0.py.
    Works even when VS Code / Pylance doesn't resolve imports nicely.
    """
    try:
        from src.config.pipeline_config_v1_0 import load_pipeline_config, build_plan, build_month_plan  # type: ignore
        return load_pipeline_config, build_plan, build_month_plan
    except Exception:
        # Fallback: import via file path
        import importlib.util

        mod_path = project_root / "src" / "config" / "pipeline_config_v1_0.py"
        spec = importlib.util.spec_from_file_location("pipeline_config_v1_0", mod_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot import pipeline_config from {mod_path}")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore
        return mod.load_pipeline_config, mod.build_plan, mod.build_month_plan


# -----------------------------
# DB Helpers
# -----------------------------
def db_connect() -> "psycopg2.extensions.connection":
    """
    Öffnet eine DB-Verbindung.

    Priorität:
      1) PG_DSN (voller DSN-String)
      2) libpq-Umgebungsvariablen (PGHOST, PGPORT, PGUSER, PGDATABASE, PGPASSWORD)
      3) Fallback defaults (localhost:5432, user=postgres, db=kpi_cs_partition)

    Hinweis:
    - Wenn kein Passwort gesetzt ist, kann libpq trotzdem verbinden (z. B. via ~/.pgpass).
    """
    dsn = os.getenv("PG_DSN")
    if dsn:
        return psycopg2.connect(dsn)

    host = os.getenv("PGHOST", "127.0.0.1")
    port = int(os.getenv("PGPORT", "5432"))
    user = os.getenv("PGUSER", "postgres")
    dbname = os.getenv("PGDATABASE") or os.getenv("DB_NAME") or "kpi_cs_partition"
    password = os.getenv("PGPASSWORD")  # optional

    kwargs = dict(host=host, port=port, user=user, dbname=dbname)
    if password:
        kwargs["password"] = password

    return psycopg2.connect(**kwargs)

def regclass_exists(cur: Any, qualified_name: str) -> bool:
    """Prüft, ob eine Tabelle/View in der DB existiert (via to_regclass).

    Wird vor jedem bedingten DELETE/INSERT verwendet, um Fehler bei fehlenden Tabellen zu vermeiden.
    """
    cur.execute("SELECT to_regclass(%s) IS NOT NULL AS exists;", (qualified_name,))
    return bool(cur.fetchone()["exists"])


def _row_get(row, idx: int, key: str):
    """Helper: robustly get values from tuple-rows *or* dict-rows."""
    try:
        return row[idx]
    except Exception:
        # psycopg3 dict_row / psycopg2 RealDictCursor etc.
        return row[key]


def get_columns_info(cur, schema: str, table: str):
    """Gibt alle Spalten-Metadaten einer Tabelle zurück (Name, Nullable, Default).

    Wird genutzt, um dynamisch auf unterschiedliche DB-Schemas reagieren zu können
    (z.B. welche Spalten in dim_time_hour tatsächlich existieren).
    """
    cur.execute(
        """
        SELECT column_name, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table),
    )
    return cur.fetchall()

def month_key_to_dates(month_key: str) -> tuple[date, date]:
    """Converts YYYY_MM (e.g. 2024_04) into (start_date, end_date).

    end_date is exclusive (first day of next month).
    """
    try:
        y, m = month_key.split("_")
        year = int(y)
        month = int(m)
    except Exception as e:
        raise ValueError(f"Invalid month_key '{month_key}'. Expected YYYY_MM (e.g. 2024_04).") from e

    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1)
    else:
        end = date(year, month + 1, 1)
    return start, end




def ensure_required_schemas(cur) -> None:
    """
    Stellt sicher, dass die benötigten Schemas existieren.

    Warum:
    - Die Engine arbeitet schema-qualifiziert (core.*, staging.*, kpi.*, ...)
    - Auf einer frischen/Team-DB können Schemas fehlen
    - IF NOT EXISTS ist idempotent (safe mehrfach ausführbar)
    """
    schemas = [
        "staging",
        "core",
        "kpi",
        "analytics",
        "ml",
        "monitoring",
        "bi",
        "ingestion",
    ]
    for s in schemas:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {s};")


def preflight_check(cur, start_date, end_date, tz: str) -> None:
    """Optional preflight check (no writes).

    This runs *read-only* checks against the current DB to catch common mismatches
    (missing tables/columns, missing time-dim coverage) *before* you run the engine.
    """

    def _cols(schema: str, table: str) -> set[str]:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
            ORDER BY ordinal_position;
            """,
            (schema, table),
        )
        rows = cur.fetchall()
        return { _row_get(r, 0, "column_name") for r in rows }

    required_tables = [
        ("core", "dim_time_hour"),
        ("core", "dim_detector"),
        ("core", "dim_vehicle_class"),
        ("core", "fact_detector_hourly"),
        ("core", "fact_mq_hourly"),
        ("kpi", "kpi_definition"),
        ("kpi", "kpi_value"),
        ("kpi", "kpi_confidence"),
        ("analytics", "qa_features_hourly"),
        ("monitoring", "pipeline_run"),
    ]

    print("\n[PRECHECK] Database sanity checks")
    for sch, tab in required_tables:
        ok = regclass_exists(cur, f"{sch}.{tab}")
        print(f"  - {sch}.{tab:22s} exists: {'OK' if ok else 'MISSING'}")

    if regclass_exists(cur, "core.dim_time_hour"):
        print(f"  - core.dim_time_hour cols: {sorted(_cols('core', 'dim_time_hour'))}")

    if regclass_exists(cur, "core.fact_mq_hourly"):
        print(f"  - core.fact_mq_hourly cols: {sorted(_cols('core', 'fact_mq_hourly'))}")


    if regclass_exists(cur, "analytics.qa_features_hourly"):
        print(f"  - analytics.qa_features_hourly cols: {sorted(_cols('analytics', 'qa_features_hourly'))}")

    if regclass_exists(cur, "monitoring.pipeline_run"):
        print(f"  - monitoring.pipeline_run cols: {sorted(_cols('monitoring', 'pipeline_run'))}")

    # Month slice bounds (timestamps interpreted in local TZ, converted to UTC instant)
    cur.execute(
        """
        SELECT
          (%s::timestamp AT TIME ZONE %s) AS start_ts_utc,
          (%s::timestamp AT TIME ZONE %s) AS end_ts_utc
        """,
        (start_date, tz, end_date, tz),
    )
    bounds = cur.fetchone()
    s = _row_get(bounds, 0, "start_ts_utc")
    e = _row_get(bounds, 1, "end_ts_utc")
    print(f"[PRECHECK] month slice UTC bounds: start={s} end={e} (tz={tz})")

    # Optional: check whether the time dimension already covers the month slice.
    # (The engine will populate it on run anyway, but this helps explain FK issues early.)
    if regclass_exists(cur, "core.dim_time_hour"):
        try:
            cur.execute(
                "SELECT COUNT(*) AS cnt FROM core.dim_time_hour WHERE ts_utc >= %s AND ts_utc < %s",
                (s, e),
            )
            cnt_row = cur.fetchone()
            cnt = int(_row_get(cnt_row, 0, "cnt") or 0)
            expected = int((e - s).total_seconds() // 3600) if (s is not None and e is not None) else None
            if expected is not None:
                print(f"[PRECHECK] core.dim_time_hour rows in slice: {cnt} (expected ~ {expected})")
            else:
                print(f"[PRECHECK] core.dim_time_hour rows in slice: {cnt}")
        except Exception as ex:
            print(f"[PRECHECK] core.dim_time_hour slice check skipped (error): {ex}")

    print("[PRECHECK] done (no changes applied).")


def ensure_time_dim_hour(cur, start_date: date, end_date: date, tz: str) -> None:
    """Stellt sicher, dass `core.dim_time_hour` alle Stunden für den Monatsslice enthält.

    Hintergrund:
    - `core.fact_*_hourly` referenziert `core.dim_time_hour(ts_utc)` via Foreign Key.
    - Wenn wir einen Monat neu einspielen (`--replace-month-slice`), müssen diese Keys *vor* dem
      Insert in Facts existieren, sonst knallt der FK-Check.

    Umsetzung:
    - Wir erzeugen die Stundenliste in Python (UTC-Iteration, dann Ableitung Local-Felder),
      und füllen die Spalten, die in der DB tatsächlich vorhanden sind.
    - Das ist robuster als reine SQL-Generierung, weil wir so keine Cursor-Typ-/Row-Typ
      Überraschungen haben und die Werte 1:1 zu dem passen, was wir später beim Facts-Load nutzen.
    """
    cols = { _row_get(r, 0, 'column_name') for r in get_columns_info(cur, 'core', 'dim_time_hour') }

    # minimaler Vertragsumfang (muss existieren)
    required = {"ts_utc"}
    missing_required = required - cols
    if missing_required:
        raise RuntimeError(
            "core.dim_time_hour hat nicht die erwarteten Pflichtspalten: "
            f"{sorted(missing_required)}"
        )

    # Wir iterieren in UTC, damit DST (Sommer-/Winterzeit) sauber abgebildet wird.
    from datetime import datetime, time, timedelta, timezone
    try:
        from zoneinfo import ZoneInfo
        tzinfo = ZoneInfo(tz)
    except Exception as e:
        raise RuntimeError(f"Ungültige Zeitzone '{tz}': {e}") from e

    start_local = datetime.combine(start_date, time(0, 0)).replace(tzinfo=tzinfo)
    end_local = datetime.combine(end_date, time(0, 0)).replace(tzinfo=tzinfo)
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)

    # Welche Spalten sollen wir befüllen? (nur, was wirklich existiert)
    insert_cols = ["ts_utc"]
    if "date_local" in cols:
        insert_cols.append("date_local")
    if "hour_local" in cols:
        insert_cols.append("hour_local")
    if "month_local" in cols:
        insert_cols.append("month_local")

    # (optional) UTC-Derivate – falls ein älteres/anderes Schema diese Spalten hat
    for opt in ["d_utc", "year_utc", "month_utc", "day_utc", "hour_utc"]:
        if opt in cols:
            insert_cols.append(opt)

    placeholders = ", ".join(["%s"] * len(insert_cols))
    insert_sql = (
        "INSERT INTO core.dim_time_hour("
        + ", ".join(insert_cols)
        + ") VALUES ("
        + placeholders
        + ") ON CONFLICT (ts_utc) DO NOTHING"
    )

    params = []
    cur_utc = start_utc
    while cur_utc < end_utc:
        local = cur_utc.astimezone(tzinfo)

        row = {"ts_utc": cur_utc}
        if "date_local" in cols:
            row["date_local"] = local.date()
        if "hour_local" in cols:
            row["hour_local"] = int(local.hour)
        if "month_local" in cols:
            row["month_local"] = int(local.month)

        if "d_utc" in cols:
            row["d_utc"] = cur_utc.date()
        if "year_utc" in cols:
            row["year_utc"] = int(cur_utc.year)
        if "month_utc" in cols:
            row["month_utc"] = int(cur_utc.month)
        if "day_utc" in cols:
            row["day_utc"] = int(cur_utc.day)
        if "hour_utc" in cols:
            row["hour_utc"] = int(cur_utc.hour)

        params.append(tuple(row.get(c) for c in insert_cols))
        cur_utc += timedelta(hours=1)

    cur.executemany(insert_sql, params)

    # Sanity-Check: erste Stunde muss existieren (sonst kommt später ein FK-Fehler)
    cur.execute(
        "SELECT 1 AS ok FROM core.dim_time_hour WHERE ts_utc=%s",
        (start_utc,),
    )
    if cur.fetchone() is None:
        raise RuntimeError(
            "dim_time_hour wurde nicht korrekt befüllt (Start-Stunde fehlt). "
            f"Start={start_utc} (tz={tz})"
        )
def ensure_kpi_tables(cur) -> None:
    """Erstellt die KPI-Tabellen falls nicht vorhanden (idempotent).

    Tabellen:
    - kpi.kpi_definition: Stammdaten der KPI-Definitionen (Name, Formel, Grain)
    - kpi.kpi_value:      Berechnete KPI-Werte pro Entität und Zeitstempel
    - kpi.kpi_confidence: Konfidenz-Scores pro KPI-Wert (Freshness, Volume, Null, Anomaly, Drift)
    """
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS kpi.kpi_definition (
          kpi_id serial PRIMARY KEY,
          kpi_name text NOT NULL UNIQUE,
          description text,
          grain text NOT NULL,
          owner text,
          formula text,
          is_active boolean NOT NULL DEFAULT true,
          version integer NOT NULL DEFAULT 1,
          created_at timestamptz NOT NULL DEFAULT now()
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS kpi.kpi_value (
          kpi_id integer NOT NULL REFERENCES kpi.kpi_definition(kpi_id),
          ts_utc timestamptz NOT NULL,
          entity_type text NOT NULL,        -- 'detector' | 'mq' | ...
          entity_id bigint NOT NULL,
          value numeric,
          run_id uuid,
          calculated_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (kpi_id, ts_utc, entity_type, entity_id)
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS kpi.kpi_confidence (
          kpi_id integer NOT NULL REFERENCES kpi.kpi_definition(kpi_id),
          ts_utc timestamptz NOT NULL,
          entity_type text NOT NULL,
          entity_id bigint NOT NULL,
          confidence_score numeric,
          confidence_label text,
          freshness_score numeric,
          volume_score numeric,
          null_score numeric,
          anomaly_score numeric,
          drift_score numeric,
          run_id uuid,
          calculated_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (kpi_id, ts_utc, entity_type, entity_id)
        );
        """
    )



def ensure_qa_table(cur) -> None:
    """Create analytics.qa_features_hourly if missing (matches current DB schema).

    Current schema (partitioned by ts_utc):
      det_id15 bigint NOT NULL
      ts_utc   timestamptz NOT NULL
      run_id   uuid NOT NULL
      row_count integer
      missing_rate numeric(18,6)
      duplicate_rate numeric(18,6)
      freshness_lag_h integer
      created_at timestamptz NOT NULL DEFAULT now()
      PK (det_id15, ts_utc, run_id)
    """

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS analytics.qa_features_hourly (
          det_id15 bigint NOT NULL,
          ts_utc timestamptz NOT NULL,
          run_id uuid NOT NULL,
          row_count integer,
          missing_rate numeric(18,6),
          duplicate_rate numeric(18,6),
          freshness_lag_h integer,
          created_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (det_id15, ts_utc, run_id)
        )
        PARTITION BY RANGE (ts_utc);
        """
    )


# -----------------------------
# KPI Catalog


# -----------------------------
# KPI Catalog (6 KPIs)
# -----------------------------


def ensure_vehicle_classes(cur) -> None:
    """Stellt sicher, dass in core.dim_vehicle_class die drei Fahrzeugklassen vorhanden sind.

    Hintergrund:
    - In den Facts (core.fact_*_hourly) wird vehicle_class_id als Foreign Key verwendet.
    - Unsere KPI-Definitionen nutzen vehicle_class_id=1/2/3 (KFZ/PKW/LKW).

    Die Funktion ist idempotent.
    """
    rows = [
        (1, 'kfz', 'KFZ'),
        (2, 'pkw', 'PKW'),
        (3, 'lkw', 'LKW'),
    ]
    for vehicle_class_id, code, label in rows:
        cur.execute(
            """
            INSERT INTO core.dim_vehicle_class (vehicle_class_id, code, label)
            VALUES (%s, %s, %s)
            ON CONFLICT (vehicle_class_id) DO UPDATE
            SET code = EXCLUDED.code,
                label = EXCLUDED.label;
            """,
            (vehicle_class_id, code, label),
        )
# -----------------------------
# BI Scope / Allowlist
# -----------------------------
# Basierend auf den PowerBI-Dashboard-Überlegungen rechnen wir gezielt nur die KPIs,
# die in den Visuals benötigt werden (Volume/Speed × KFZ/PKW/LKW).
# Die Unterscheidung Detector vs. MQ erfolgt in kpi_value/kpi_confidence über entity_type.
BI_KPI_ALLOWLIST = {
    "flow_kfz_hourly",
    "speed_kfz_hourly",
    "flow_pkw_hourly",
    "speed_pkw_hourly",
    "flow_lkw_hourly",
    "speed_lkw_hourly",
}

KPI_SPECS = [
    ("flow_kfz_hourly", "KFZ flow (q) per hour", 1, "flow_q"),
    ("speed_kfz_hourly", "KFZ speed (v) per hour", 1, "speed_v"),
    ("flow_pkw_hourly", "PKW flow (q) per hour", 2, "flow_q"),
    ("speed_pkw_hourly", "PKW speed (v) per hour", 2, "speed_v"),
    ("flow_lkw_hourly", "LKW flow (q) per hour", 3, "flow_q"),
    ("speed_lkw_hourly", "LKW speed (v) per hour", 3, "speed_v"),
]


def ensure_kpi_definitions(cur) -> Dict[str, int]:
    """Inserts KPI definitions if missing and returns a BI-scoped mapping.

    BI-Scope (wichtig):
    - Wir erzeugen die KPI-Definitionen aus KPI_SPECS idempotent.
    - Für den Lauf selbst geben wir aber *nur* die BI_KPI_ALLOWLIST zurück,
      damit der Confidence-Step nicht versehentlich weitere (alte/aktive) KPIs rechnet.

    Returns mapping: kpi_name -> kpi_id
    """
    for kpi_name, desc, vehicle_class_id, metric_col in KPI_SPECS:
        cur.execute(
            """
            INSERT INTO kpi.kpi_definition (kpi_name, description, grain, owner, formula, is_active, version)
            VALUES (%s, %s, 'hourly', 'team', %s, true, 1)
            ON CONFLICT (kpi_name) DO NOTHING;
            """,
            (
                kpi_name,
                desc,
                f"core.fact_*_hourly({metric_col}) WHERE vehicle_class_id={vehicle_class_id}",
            ),
        )

    allow = sorted(BI_KPI_ALLOWLIST)
    cur.execute(
        """
        SELECT kpi_id, kpi_name
        FROM kpi.kpi_definition
        WHERE is_active = true
          AND kpi_name = ANY(%s)
        """,
        (allow,),
    )
    rows = cur.fetchall()
    return {r["kpi_name"]: int(r["kpi_id"]) for r in rows}



# -----------------------------
# Stage Orchestration (Loader)
# -----------------------------
def run_stage_loaders(
    project_root: Path,
    cfg_path: Path,
    month_key: str,
    replace_month_slice: bool,
    only_detector: Optional[List[str]] = None,
    max_rows: Optional[int] = None,
) -> None:
    """Ruft run_stage_loaders_v1_1.py als Subprozess auf (Stage-Loader-Orchestrierung).

    Lädt Quelldateien (OLD CSV.GZ / NEW TGZ) in staging.*.
    Die Entscheidung OLD/NEW wird weiterhin durch den MonthPlan aus der YAML-Config getroffen.
    """
    cmd = [sys.executable, str(project_root / "scripts" / "run_stage_loaders_v1_1.py"), "--config", str(cfg_path), "--month-key", month_key]
    # Run both OLD+NEW stage loaders (they still respect cfg.old_enabled/cfg.new_enabled)
    cmd += ["--with-old", "--with-new"]
    if replace_month_slice:
        cmd.append("--replace-month-slice")
    if only_detector:
        for d in only_detector:
            cmd += ["--only-detector", d]
    if max_rows is not None:
        cmd += ["--max-rows", str(max_rows)]

    print("\n[STAGE] " + " ".join(cmd))
    proc = subprocess.run(cmd, cwd=str(project_root))
    if proc.returncode != 0:
        raise RuntimeError(f"Stage loaders failed (rc={proc.returncode}).")


# -----------------------------
# Core Facts Load (from staging.*)
#   (copied/adapted from phase_b_engine_v1_2_3, raw removed)
# -----------------------------
def delete_month_slice(cur, start_date: date, end_date: date, month_key: str, tz: str, run_id: uuid.UUID) -> None:
    """
    Deletes (best-effort) all rows for the month slice [start_date, end_date) from the main fact/KPI tables.

    Important:
    - We do NOT delete core.dim_time_hour. The time dimension is shared and referenced by FKs.
    - We avoid hard failures if some optional tables do not exist (e.g., early development stage).

    Implementation detail:
    - We pre-check existence via to_regclass() to avoid exceptions that would otherwise
      force a transaction rollback (which could undo earlier "ensure_*" steps).
    """
    # Convert local month boundaries to UTC timestamps for consistent filtering on ts_utc
    cur.execute(
        "SELECT (%s::timestamp AT TIME ZONE %s) AS start_ts_utc, (%s::timestamp AT TIME ZONE %s) AS end_ts_utc;",
        (start_date, tz, end_date, tz),
    )
    row = cur.fetchone()
    start_ts_utc = _row_get(row, 0, "start_ts_utc")
    end_ts_utc = _row_get(row, 1, "end_ts_utc")

    def _exists(fqname: str) -> bool:
        # to_regclass(...) liefert NULL, wenn das Objekt nicht existiert
        cur.execute("SELECT to_regclass(%s) AS reg;", (fqname,))
        row2 = cur.fetchone()
        return _row_get(row2, 0, "reg") is not None

    # Tables that contain ts_utc and are safe to slice-delete
    tables = [
        "core.fact_detector_hourly",
        "core.fact_mq_hourly",
        "core.fact_quality_hourly",
        "analytics.qa_features_hourly",
        "kpi.kpi_value",
        "kpi.kpi_confidence",
        "ml.ml_anomaly_score_hourly",
    ]

    for fq in tables:
        if not _exists(fq):
            continue
        cur.execute(
            f"DELETE FROM {fq} WHERE ts_utc >= %s AND ts_utc < %s;",
            (start_ts_utc, end_ts_utc),
        )

    # Small metadata entry (optional)
    # monitoring.pipeline_run: best-effort Logging (Schema kann je nach DB-Stand variieren)
    if _exists("monitoring.pipeline_run"):
        try:
            cols = get_columns_info(cur, "monitoring", "pipeline_run")
            colset = {c["column_name"] for c in cols}

            # Variante 1: Struktur mit month_key/stage/status/message
            if {"month_key", "stage", "status", "message"}.issubset(colset):
                cur.execute(
                    """
                    INSERT INTO monitoring.pipeline_run(month_key, stage, status, message, created_at)
                    VALUES (%s, 'phase_b', 'success', %s, now())
                    """,
                    (month_key, f"Deleted month slice {month_key} (best-effort)."),
                )

            # Variante 2: Struktur aus unserem DB-Script (run_id/started_at/finished_at/status/source_year/source_month/source_layout/notes)
            elif {"run_id", "started_at", "status", "source_year", "source_month"}.issubset(colset):
                y_str, m_str = month_key.split("_")
                y, m = int(y_str), int(m_str)
                # optional: source_layout/notes, falls vorhanden
                insert_cols = ["run_id", "started_at", "finished_at", "status", "source_year", "source_month", "source_layout", "notes"]
                insert_cols = [c for c in insert_cols if c in colset]

                values = {
                    "run_id": str(run_id),
                    "status": "success",
                    "source_year": y,
                    "source_month": m,
                    "source_layout": None,
                    "notes": f"Deleted month slice {month_key} (best-effort).",
                }

                col_sql = ", ".join(insert_cols)
                val_sql = []
                params = []
                for c in insert_cols:
                    if c in ("started_at", "finished_at"):
                        val_sql.append("now()")
                    else:
                        val_sql.append("%s")
                        params.append(values.get(c))

                cur.execute(
                    f"INSERT INTO monitoring.pipeline_run({col_sql}) VALUES ({', '.join(val_sql)});",
                    tuple(params),
                )
            else:
                print("[WARN] monitoring.pipeline_run exists, but schema is unknown -> skip logging.")
        except Exception as e:
            print(f"[WARN] monitoring.pipeline_run logging failed (ignored): {e}")


def load_core_facts(cur, start_date: date, end_date: date, tz: str, allowed_layouts: Optional[set[str]] = None) -> Dict[str, int]:
    """Lädt Core-Fakten aus Staging-Tabellen in core.fact_*.

    Verarbeitet je nach allowed_layouts:
    - 'old': staging.stg_old_mq_hr -> core.fact_mq_hourly
             staging.stg_old_det_val_hr -> core.fact_detector_hourly
    - 'new': staging.stg_new_detector_hourly -> core.fact_detector_hourly
    Zusätzlich wird core.fact_quality_hourly aus beiden Quellen befüllt.

    Gibt ein Dict mit Zeilenzählern je Zieltabelle zurück.
    """
    counts: Dict[str, int] = {}

    allowed_layouts = allowed_layouts or {"old", "new"}
    allow_old = "old" in allowed_layouts
    allow_new = "new" in allowed_layouts

    # OLD MQ
    if regclass_exists(cur, "staging.stg_old_mq_hr"):
        cur.execute(
            """
            INSERT INTO core.fact_mq_hourly (mq_id15, ts_utc, vehicle_class_id, flow_q, speed_v, source_layout, created_at)
            WITH base AS (
              SELECT
                m.mq_id15 AS mq_id15,
                ((s.tag::timestamp + make_interval(hours => s.stunde)) AT TIME ZONE %s) AS ts_utc,
                s.q_kfz_mq_hr AS q_kfz, s.v_kfz_mq_hr AS v_kfz,
                s.q_pkw_mq_hr AS q_pkw, s.v_pkw_mq_hr AS v_pkw,
                s.q_lkw_mq_hr AS q_lkw, s.v_lkw_mq_hr AS v_lkw
              FROM staging.stg_old_mq_hr s
              LEFT JOIN core.dim_mq m
                     ON m.mq_kurzname = s.mq_name
              WHERE s.tag >= %s AND s.tag < %s
                AND m.mq_id15 IS NOT NULL
            ),
            rows AS (

              SELECT mq_id15, ts_utc, 1::smallint AS vehicle_class_id, q_kfz AS flow_q, v_kfz AS speed_v, 'old'::text AS source_layout FROM base
              UNION ALL
              SELECT mq_id15, ts_utc, 2::smallint, q_pkw, v_pkw, 'old' FROM base
              UNION ALL
              SELECT mq_id15, ts_utc, 3::smallint, q_lkw, v_lkw, 'old' FROM base
            )
            SELECT mq_id15, ts_utc, vehicle_class_id, flow_q, speed_v, source_layout, now()
            FROM rows
            WHERE ts_utc IS NOT NULL
            ON CONFLICT (mq_id15, ts_utc, vehicle_class_id)
            DO UPDATE SET
              flow_q = EXCLUDED.flow_q,
              speed_v = EXCLUDED.speed_v,
              source_layout = EXCLUDED.source_layout,
              created_at = now();
            """,
            (tz, start_date, end_date),
        )
        counts["fact_mq_hourly_old_upsert"] = cur.rowcount

    elif (not allow_old) and regclass_exists(cur, "staging.stg_old_mq_hr"):
        print("[POLICY] Skip OLD MQ (staging.stg_old_mq_hr) – ab 2023 nur NEW")

    # OLD detector
    if regclass_exists(cur, "staging.stg_old_det_val_hr"):
        cur.execute(
            """
            INSERT INTO core.fact_detector_hourly (det_id15, ts_utc, vehicle_class_id, flow_q, speed_v, source_layout, created_at)
            WITH base AS (
              SELECT
                detid_15 AS det_id15,
                ((tag::timestamp + make_interval(hours => stunde)) AT TIME ZONE %s) AS ts_utc,
                q_kfz_det_hr AS q_kfz, v_kfz_det_hr AS v_kfz,
                q_pkw_det_hr AS q_pkw, v_pkw_det_hr AS v_pkw,
                q_lkw_det_hr AS q_lkw, v_lkw_det_hr AS v_lkw
              FROM staging.stg_old_det_val_hr
              WHERE tag >= %s AND tag < %s
                AND detid_15 IS NOT NULL
            ),
            rows AS (

              SELECT det_id15, ts_utc, 1::smallint AS vehicle_class_id, q_kfz AS flow_q, v_kfz AS speed_v, 'old'::text AS source_layout FROM base
              UNION ALL
              SELECT det_id15, ts_utc, 2::smallint, q_pkw, v_pkw, 'old' FROM base
              UNION ALL
              SELECT det_id15, ts_utc, 3::smallint, q_lkw, v_lkw, 'old' FROM base
            )
            SELECT det_id15, ts_utc, vehicle_class_id, flow_q, speed_v, source_layout, now()
            FROM rows
            WHERE ts_utc IS NOT NULL
            ON CONFLICT (det_id15, ts_utc, vehicle_class_id)
            DO UPDATE SET
              flow_q = EXCLUDED.flow_q,
              speed_v = EXCLUDED.speed_v,
              source_layout = EXCLUDED.source_layout,
              created_at = now();
            """,
            (tz, start_date, end_date),
        )
        counts["fact_detector_hourly_old_upsert"] = cur.rowcount

        # OLD quality (detector)
        if regclass_exists(cur, "core.fact_quality_hourly"):
            cur.execute(
                """
                INSERT INTO core.fact_quality_hourly (det_id15, ts_utc, quality_old, completeness_new, zscore_det0, zscore_det1, zscore_det2, hist_cor, source_layout, created_at)
                SELECT
                  detid_15 AS det_id15,
                  ((tag::timestamp + make_interval(hours => stunde)) AT TIME ZONE %s) AS ts_utc,
                  qualitaet AS quality_old,
                  NULL::numeric(12,6) AS completeness_new,
                  NULL::numeric(12,6), NULL::numeric(12,6), NULL::numeric(12,6), NULL::numeric(12,6),
                  'old'::text AS source_layout,
                  now() AS created_at
                FROM staging.stg_old_det_val_hr
                WHERE tag >= %s AND tag < %s
                  AND detid_15 IS NOT NULL
                ON CONFLICT (det_id15, ts_utc)
                DO UPDATE SET
                  quality_old = EXCLUDED.quality_old,
                  source_layout = EXCLUDED.source_layout,
                  created_at = now();
                """,
                (tz, start_date, end_date),
            )
            counts["fact_quality_hourly_old_upsert"] = cur.rowcount

    elif (not allow_old) and regclass_exists(cur, "staging.stg_old_det_val_hr"):
        print("[POLICY] Skip OLD detector/quality (staging.stg_old_det_val_hr) – ab 2023 nur NEW")

    # NEW detector hourly
    if regclass_exists(cur, "staging.stg_new_detector_hourly"):
        cur.execute(
            """
            WITH joined AS (
              -- Rohdaten: Join staging -> dim_detector (kann mehrere det_id15 pro det_name_alt liefern)
              SELECT
                d.det_id15 AS det_id15,
                COALESCE(s.utc, ((s.datum_ortszeit::timestamp + make_interval(hours => s.stunde_ortszeit)) AT TIME ZONE %s)) AS ts_utc,
                s.qkfz AS q_kfz, s.vkfz AS v_kfz,
                s.qpkw AS q_pkw, s.vpkw AS v_pkw,
                s.qlkw AS q_lkw, s.vlkw AS v_lkw,
                s.vollstaendigkeit AS completeness_new,
                s.zscore_det0, s.zscore_det1, s.zscore_det2, s.hist_cor
              FROM staging.stg_new_detector_hourly s
              LEFT JOIN core.dim_detector d
                ON d.det_name_alt = s.det_name_alt
              WHERE s.datum_ortszeit >= %s AND s.datum_ortszeit < %s
            ),
            base AS (
              -- Dedup: pro (det_id15, ts_utc) nur eine Zeile behalten (verhindert CardinalityViolation beim UPSERT)
              SELECT DISTINCT ON (det_id15, ts_utc)
                det_id15, ts_utc,
                q_kfz, v_kfz, q_pkw, v_pkw, q_lkw, v_lkw,
                completeness_new, zscore_det0, zscore_det1, zscore_det2, hist_cor
              FROM joined
              ORDER BY det_id15, ts_utc
            ),
            rows AS (
              SELECT det_id15, ts_utc, 1::smallint AS vehicle_class_id, q_kfz AS flow_q, v_kfz AS speed_v, 'new'::text AS source_layout FROM base
              UNION ALL
              SELECT det_id15, ts_utc, 2::smallint, q_pkw, v_pkw, 'new' FROM base
              UNION ALL
              SELECT det_id15, ts_utc, 3::smallint, q_lkw, v_lkw, 'new' FROM base
            )
            INSERT INTO core.fact_detector_hourly (det_id15, ts_utc, vehicle_class_id, flow_q, speed_v, source_layout, created_at)
            SELECT det_id15, ts_utc, vehicle_class_id, flow_q, speed_v, source_layout, now()
            FROM rows
            WHERE det_id15 IS NOT NULL AND ts_utc IS NOT NULL
            ON CONFLICT (det_id15, ts_utc, vehicle_class_id)
            DO UPDATE SET
              flow_q = EXCLUDED.flow_q,
              speed_v = EXCLUDED.speed_v,
              source_layout = EXCLUDED.source_layout,
              created_at = now();
            """,
            (tz, start_date, end_date),
        )
        counts["fact_detector_hourly_new_upsert"] = cur.rowcount

        # NEW quality
        if regclass_exists(cur, "core.fact_quality_hourly"):
            cur.execute(
                """
                WITH joined AS (
                  -- Rohdaten inkl. berechneter ts_utc
                  SELECT
                    d.det_id15 AS det_id15,
                    COALESCE(s.utc, ((s.datum_ortszeit::timestamp + make_interval(hours => s.stunde_ortszeit)) AT TIME ZONE %s)) AS ts_utc,
                    s.vollstaendigkeit AS completeness_new,
                    s.zscore_det0, s.zscore_det1, s.zscore_det2, s.hist_cor
                  FROM staging.stg_new_detector_hourly s
                  LEFT JOIN core.dim_detector d
                    ON d.det_name_alt = s.det_name_alt
                  WHERE s.datum_ortszeit >= %s AND s.datum_ortszeit < %s
                    AND d.det_id15 IS NOT NULL
                ),
                deduped AS (
                  -- Dedup: pro (det_id15, ts_utc) nur eine Zeile (verhindert CardinalityViolation)
                  SELECT DISTINCT ON (det_id15, ts_utc)
                    det_id15, ts_utc, completeness_new,
                    zscore_det0, zscore_det1, zscore_det2, hist_cor
                  FROM joined
                  ORDER BY det_id15, ts_utc
                )
                INSERT INTO core.fact_quality_hourly (det_id15, ts_utc, quality_old, completeness_new, zscore_det0, zscore_det1, zscore_det2, hist_cor, source_layout, created_at)
                SELECT
                  det_id15, ts_utc,
                  NULL::numeric(12,6) AS quality_old,
                  completeness_new,
                  zscore_det0, zscore_det1, zscore_det2, hist_cor,
                  'new'::text AS source_layout,
                  now() AS created_at
                FROM deduped
                ON CONFLICT (det_id15, ts_utc)
                DO UPDATE SET
                  completeness_new = EXCLUDED.completeness_new,
                  zscore_det0 = EXCLUDED.zscore_det0,
                  zscore_det1 = EXCLUDED.zscore_det1,
                  zscore_det2 = EXCLUDED.zscore_det2,
                  hist_cor = EXCLUDED.hist_cor,
                  source_layout = EXCLUDED.source_layout,
                  created_at = now();
                """,
                (tz, start_date, end_date),
            )
            counts["fact_quality_hourly_new_upsert"] = cur.rowcount

    elif (not allow_new) and regclass_exists(cur, "staging.stg_new_detector_hourly"):
        print("[POLICY] Skip NEW detector/quality (staging.stg_new_detector_hourly) – bis 2022 nur OLD")

    return counts


# -----------------------------
# QA Features (detector + mq)
# -----------------------------

def upsert_qa_features_hourly(cur, start_date: date, end_date: date, run_id: uuid.UUID) -> int:
    """
    Creates per-hour QA features for detectors (core.fact_detector_hourly) in the month slice.

    Target table (current DB):
      analytics.qa_features_hourly(det_id15, ts_utc, run_id, row_count, missing_rate,
                                  duplicate_rate, freshness_lag_h, created_at)
    PK: (det_id15, ts_utc, run_id)
    """

    cur.execute(
        """
        WITH det_base AS (
          SELECT
            det_id15::bigint AS det_id15,
            ts_utc,
            COUNT(*)::int AS row_count,
            AVG(CASE WHEN flow_q IS NULL OR speed_v IS NULL THEN 1 ELSE 0 END)::numeric AS missing_rate,
            0::numeric AS duplicate_rate,
            0::int AS freshness_lag_h
          FROM core.fact_detector_hourly
          WHERE ts_utc >= %s AND ts_utc < %s
          GROUP BY det_id15, ts_utc
        )
        INSERT INTO analytics.qa_features_hourly
          (det_id15, ts_utc, run_id, row_count, missing_rate, duplicate_rate, freshness_lag_h, created_at)
        SELECT
          det_id15, ts_utc, %s::uuid, row_count, missing_rate, duplicate_rate, freshness_lag_h, now()
        FROM det_base
        ON CONFLICT (det_id15, ts_utc, run_id)
        DO UPDATE SET
          row_count = EXCLUDED.row_count,
          missing_rate = EXCLUDED.missing_rate,
          duplicate_rate = EXCLUDED.duplicate_rate,
          freshness_lag_h = EXCLUDED.freshness_lag_h,
          created_at = now();
        """,
        (start_date, end_date, str(run_id)),
    )
    return cur.rowcount


# -----------------------------
# KPI Values + Confidence


# -----------------------------
# KPI Values + Confidence
# -----------------------------
def upsert_kpi_values(cur, start_date: date, end_date: date, run_id: uuid.UUID, kpi_ids: Dict[str, int]) -> int:
    """
    Creates kpi.kpi_value for detectors + MQs.
    Uses core.fact_*_hourly (vehicle_class_id 1/2/3).
    """
    total = 0

    def do_insert(entity_type: str, fact_table: str, id_col: str):
        nonlocal total
        for kpi_name, _desc, vehicle_class_id, metric_col in KPI_SPECS:
            kpi_id = kpi_ids.get(kpi_name)
            if not kpi_id:
                continue
            cur.execute(
                f"""
                INSERT INTO kpi.kpi_value (kpi_id, ts_utc, entity_type, entity_id, value, run_id, calculated_at)
                SELECT
                  %s AS kpi_id,
                  f.ts_utc,
                  %s AS entity_type,
                  f.{id_col}::bigint AS entity_id,
                  f.{metric_col}::numeric AS value,
                  %s::uuid AS run_id,
                  now() AS calculated_at
                FROM {fact_table} f
                WHERE f.ts_utc >= %s AND f.ts_utc < %s
                  AND f.vehicle_class_id = %s
                  AND f.{id_col} IS NOT NULL
                ON CONFLICT (kpi_id, ts_utc, entity_type, entity_id)
                DO UPDATE SET
                  value = EXCLUDED.value,
                  run_id = EXCLUDED.run_id,
                  calculated_at = now();
                """,
                (kpi_id, entity_type, str(run_id), start_date, end_date, vehicle_class_id),
            )
            total += cur.rowcount

    if regclass_exists(cur, "core.fact_detector_hourly"):
        do_insert("detector", "core.fact_detector_hourly", "det_id15")
    if regclass_exists(cur, "core.fact_mq_hourly"):
        do_insert("mq", "core.fact_mq_hourly", "mq_id15")

    return total


def _confidence_label(score: float) -> str:
    if score >= 0.85:
        return "high"
    if score >= 0.60:
        return "medium"
    return "low"


def upsert_kpi_confidence(cur, start_date: date, end_date: date, run_id: uuid.UUID, kpi_ids: Dict[str, int]) -> int:
    """
    Computes confidence per KPI/hour/entity based on:
    - missing_rate + duplicate_rate + freshness_lag_h from analytics.qa_features_hourly
    - volume_score only for flow_* KPIs (others get 1.0, based on KPI run p95)
    - null_score based on kpi_value.value
    - anomaly/drift currently placeholders (1.0)
    """
    total = 0

    # Compute confidence for each KPI separately (simple + explicit).
    for kpi_name, kpi_id in kpi_ids.items():
        t_kpi = perf_counter()
        log(f'kpi_confidence KPI start: {kpi_name} (kpi_id={kpi_id})')
        cur.execute("SET LOCAL work_mem = '256MB';")
        if kpi_name not in {k[0] for k in KPI_SPECS}:
            continue

        is_flow = kpi_name.startswith("flow_")

        cur.execute(
            """
            WITH qv AS (
              SELECT kpi_id, ts_utc, entity_type, entity_id, value
              FROM kpi.kpi_value
              WHERE kpi_id = %s
                AND ts_utc >= %s AND ts_utc < %s
                AND run_id = %s::uuid
            ),
            qf AS (
              -- qa_features_hourly is detector-only in the current schema
              SELECT 'detector'::text AS entity_type,
                     det_id15       AS entity_id,
                     ts_utc,
                     missing_rate,
                     duplicate_rate,
                     freshness_lag_h
              FROM analytics.qa_features_hourly
              WHERE ts_utc >= %s AND ts_utc < %s
                AND run_id = %s::uuid
            ),
            p95 AS (
              SELECT entity_type,
                     percentile_cont(0.95) WITHIN GROUP (ORDER BY value) AS value_p95
              FROM qv
              WHERE value IS NOT NULL
              GROUP BY entity_type
            ),
            joined AS (
              SELECT
                qv.kpi_id,
                qv.ts_utc,
                qv.entity_type,
                qv.entity_id,
                COALESCE(qf.missing_rate, 0)::numeric     AS missing_rate,
                COALESCE(qf.duplicate_rate, 0)::numeric   AS duplicate_rate,
                COALESCE(qf.freshness_lag_h, 0)::numeric  AS freshness_lag_h,
                COALESCE(p95.value_p95, 0)::numeric       AS value_p95,
                qv.value
              FROM qv
              LEFT JOIN qf
                ON qf.entity_type = qv.entity_type
               AND qf.entity_id   = qv.entity_id
               AND qf.ts_utc      = qv.ts_utc
              LEFT JOIN p95
                ON p95.entity_type = qv.entity_type
            )
            INSERT INTO kpi.kpi_confidence (
              kpi_id, ts_utc, entity_type, entity_id,
              confidence_score, confidence_label,
              freshness_score, volume_score, null_score,
              anomaly_score, drift_score,
              run_id, calculated_at
            )
            SELECT
              j.kpi_id,
              j.ts_utc,
              j.entity_type,
              j.entity_id,
              -- confidence_score: basic penalty model (MVP)
              LEAST(1.0, GREATEST(0.0,
                1.0
                - COALESCE(j.missing_rate, 0)
                - COALESCE(j.duplicate_rate, 0)
                - LEAST(0.2, COALESCE(j.freshness_lag_h, 0) * 0.02)
              )) AS confidence_score,
              NULL::text AS confidence_label,  -- set in UPDATE below
              -- freshness_score:
              LEAST(1.0, GREATEST(0.0, 1.0 - LEAST(0.2, COALESCE(j.freshness_lag_h, 0) * 0.02))) AS freshness_score,
              -- volume_score:
              CASE
                WHEN %s THEN
                  CASE
                    WHEN j.value IS NULL THEN 0.0
                    WHEN COALESCE(j.value_p95, 0) <= 0 THEN 1.0
                    ELSE LEAST(1.0, GREATEST(0.0, (j.value / NULLIF(j.value_p95, 0))))
                  END
                ELSE 1.0
              END AS volume_score,
              CASE WHEN j.value IS NULL THEN 0.0 ELSE 1.0 END AS null_score,
              1.0::numeric AS anomaly_score,
              1.0::numeric AS drift_score,
              %s::uuid AS run_id,
              now() AS calculated_at
            FROM joined j
            ON CONFLICT (kpi_id, ts_utc, entity_type, entity_id)
            DO UPDATE SET
              confidence_score = EXCLUDED.confidence_score,
              freshness_score  = EXCLUDED.freshness_score,
              volume_score     = EXCLUDED.volume_score,
              null_score       = EXCLUDED.null_score,
              anomaly_score    = EXCLUDED.anomaly_score,
              drift_score      = EXCLUDED.drift_score,
              run_id           = EXCLUDED.run_id,
              calculated_at    = now();
            """,
            (kpi_id, start_date, end_date, str(run_id), start_date, end_date, str(run_id), is_flow, str(run_id)),
        )
        total += cur.rowcount
        log(f'kpi_confidence KPI upsert done: {kpi_name} in {perf_counter()-t_kpi:.1f}s')

        # Update labels deterministically
        cur.execute(
            """
            UPDATE kpi.kpi_confidence
            SET confidence_label = CASE
              WHEN confidence_score >= 0.85 THEN 'high'
              WHEN confidence_score >= 0.60 THEN 'medium'
              ELSE 'low'
            END
            WHERE kpi_id = %s AND ts_utc >= %s AND ts_utc < %s AND run_id = %s::uuid;
            """,
            (kpi_id, start_date, end_date, str(run_id)),
        )
        log(f'kpi_confidence KPI label done: {kpi_name} in {perf_counter()-t_kpi:.1f}s')

    return total


# -----------------------------
# Engine Run
# -----------------------------
def run_one_month(
    conn,
    project_root: Path,
    cfg_path: Path,
    month_key: str,
    tz: str,
    replace_month_slice: bool,
    skip_stage: bool,
    only_detector: Optional[List[str]],
    max_rows: Optional[int],
) -> None:
    start_date, end_date = month_key_to_dates(month_key)

    run_id = uuid.uuid4()

    t_total = perf_counter()
    log(f"RUN start month={month_key} run_id={run_id} replace_month_slice={replace_month_slice} skip_stage={skip_stage}")

    with _dict_cursor(conn) as cur:
        ensure_required_schemas(cur)
        ensure_vehicle_classes(cur)
        ensure_time_dim_hour(cur, start_date, end_date, tz)
        ensure_kpi_tables(cur)
        ensure_qa_table(cur)

        log("Ensures done")

        if replace_month_slice:
            t = perf_counter()
            log("delete_month_slice start")
            delete_month_slice(cur, start_date, end_date, month_key, tz, run_id)
            log(f"delete_month_slice done in {perf_counter()-t:.1f}s")

        conn.commit()
        log("Committed ensure/delete")

    # Stage loaders (external scripts)
    if skip_stage:
        log("Stage loaders skipped")
    else:
        log("Stage loaders start")
    if not skip_stage:
        run_stage_loaders(
            project_root=project_root,
            cfg_path=cfg_path,
            month_key=month_key,
            replace_month_slice=replace_month_slice,
            only_detector=only_detector,
            max_rows=max_rows,
        )

    if not skip_stage:
        log("Stage loaders done")

    with _dict_cursor(conn) as cur:
        # Core facts
        t = perf_counter()
        log("load_core_facts start")
        allowed_layouts = _resolve_policy_allowed_layouts(month_key)
        _purge_disallowed_staging(cur, start_date, end_date, tz, allowed_layouts)
        counts = load_core_facts(cur, start_date, end_date, tz, allowed_layouts=allowed_layouts)
        log(f"load_core_facts done in {perf_counter()-t:.1f}s")

        # QA features
        t = perf_counter()
        log("upsert_qa_features_hourly start")
        qa_rows = upsert_qa_features_hourly(cur, start_date, end_date, run_id)
        log(f"upsert_qa_features_hourly done in {perf_counter()-t:.1f}s")
        counts["qa_features_upsert"] = int(qa_rows)

        # KPI definitions
        t = perf_counter()
        log("ensure_kpi_definitions start")
        kpi_ids = ensure_kpi_definitions(cur)
        log(f"ensure_kpi_definitions done in {perf_counter()-t:.1f}s")

        # KPI values + confidence
        t = perf_counter()
        log("upsert_kpi_values start")
        val_rows = upsert_kpi_values(cur, start_date, end_date, run_id, kpi_ids)
        log(f"upsert_kpi_values done in {perf_counter()-t:.1f}s")
        conn.commit()
        log('Committed kpi_values (BI scope)')
        t = perf_counter()
        log("upsert_kpi_confidence start")
        conf_rows = upsert_kpi_confidence(cur, start_date, end_date, run_id, kpi_ids)
        log(f"upsert_kpi_confidence done in {perf_counter()-t:.1f}s")
        counts["kpi_value_upsert"] = int(val_rows)
        counts["kpi_confidence_upsert"] = int(conf_rows)

        conn.commit()

    print(f"\n[OK] month={month_key} run_id={run_id}")
    for k, v in counts.items():
        print(f"  - {k}: {v}")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Phase B Engine v1.4.4 (loader-based)")
    ap.add_argument("--config", default="config/pipeline_ingestion.yaml", help="Path to pipeline_ingestion.yaml")
    ap.add_argument("--env-file", default=None, help="Override env file path (defaults to cfg.env_file in YAML)")
    ap.add_argument("--timezone", default="Europe/Berlin", help="Local timezone (used only as fallback)")
    ap.add_argument("--month-key", default=None, help="Run a single month_key YYYY_MM (overrides config selection)")
    ap.add_argument("--replace-month-slice", action="store_true", help="Delete month slice before upserts (snapshot rerun)")
    ap.add_argument("--skip-stage", action="store_true", help="Skip calling stage loaders (assume staging tables already filled)")
    ap.add_argument("--preflight", action="store_true", help="Nur Vorab-Checks ausführen und danach beenden (keine Writes)")
    ap.add_argument("--only-detector", action="append", default=None, help="Prefix filter for NEW loader (repeatable)")
    ap.add_argument("--max-rows", type=int, default=None, help="Stop after N rows (test)")
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    project_root = find_project_root(Path.cwd())
    ensure_sys_path(project_root)

    cfg_path = Path(args.config)
    if not cfg_path.is_absolute():
        cfg_path = (project_root / cfg_path).resolve()

    load_pipeline_config, build_plan, build_month_plan = import_pipeline_config(project_root)
    cfg = load_pipeline_config(cfg_path)

    # load env early
    env_path = Path(args.env_file) if args.env_file else Path(cfg.env_file)
    if not env_path.is_absolute():
        env_path = (project_root / env_path).resolve()
    if env_path.exists():
        load_dotenv(env_path, override=False)
        print(f"Loaded env from: {env_path}")
    else:
        print(f"[WARN] env file not found: {env_path} (continuing; PG_DSN must still be set)")

    log("DB connect...")
    conn = db_connect()
    log("DB connected")

    try:
        _project_root, _storage_root, plans = build_plan(cfg, project_root=project_root)

        # override single month
        # NOTE: build_plan() respects cfg.selection. For CLI --month-key we want a true override,
        # even if the month isn't listed in the YAML selection.
        if args.month_key:
            try:
                mp = build_month_plan(cfg, project_root=project_root, storage_root=_storage_root, month_key=args.month_key)
            except Exception as e:
                raise SystemExit(f"Invalid --month-key={args.month_key}: {e}")
            plans = [mp]

        if not plans:
            raise SystemExit("No months selected. Check config selection (mode/months/range/years/discover).")


        if getattr(args, 'preflight', False):
            with _dict_cursor(conn) as cur:
                for p in plans:
                    print(f"[PRECHECK] month_key={p.month_key}")
                    start_date, end_date = month_key_to_dates(p.month_key)
                    preflight_check(cur, start_date, end_date, args.timezone)
            print("[PRECHECK] done (no changes applied).")
            return

        for p in plans:
            run_one_month(
                conn=conn,
                project_root=project_root,
                cfg_path=cfg_path,
                month_key=p.month_key,
                tz=args.timezone,
                replace_month_slice=args.replace_month_slice,
                skip_stage=args.skip_stage,
                only_detector=args.only_detector,
                max_rows=args.max_rows,
            )

    finally:
        conn.close()


if __name__ == "__main__":
    main()
