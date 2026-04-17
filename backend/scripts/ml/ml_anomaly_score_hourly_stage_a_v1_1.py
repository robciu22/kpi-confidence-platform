#!/usr/bin/env python3
"""ml_anomaly_score_hourly_stage_a_v1_1.py

KPI-CS – ML Stage A (MVP): Robuster Z-Score (Median/MAD) Anomalie-Signal pro Detektor-Stunde.

Eingabe
  - analytics.qa_features_hourly
    (det_id15, ts_utc, row_count, missing_rate, duplicate_rate, freshness_lag_h, run_id, created_at)

Ausgabe
  - ml.ml_anomaly_score_hourly
    (det_id15, ts_utc, run_id, model_name, anomaly_score, is_anomaly, top_driver, driver_value, created_at)

Verhalten
  - Erkennt automatisch die aktuellste run_id für den angeforderten Monat.
  - Berechnet robuste Z-Scores pro (Detektor, Stunde_des_Tages_lokal) mit Median + MAD.
  - Schreibt nur den angeforderten Monatsslice (Historie wird nur für Baselines genutzt).

Beispiel
  python scripts/ml_anomaly_score_hourly_stage_a_v1_1.py \
    --config config/pipeline_ingestion_e2e_selected_months.yaml \
    --month-key 2025_05 \
    --replace-month-slice
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

# psycopg (v3) optional – Fallback auf psycopg2
try:
    import psycopg
    from psycopg.rows import dict_row

    _DB_DRIVER = "psycopg"

    def _connect(dsn: str):
        return psycopg.connect(dsn)

    def _dict_cursor(conn):
        return conn.cursor(row_factory=dict_row)

except ModuleNotFoundError:  # pragma: no cover
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore

    _DB_DRIVER = "psycopg2"

    def _connect(dsn: str):
        return psycopg2.connect(dsn)

    def _dict_cursor(conn):
        return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

from dotenv import load_dotenv


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


def find_project_root(start: Path) -> Path:
    """Projektverzeichnis suchen, indem nach oben gegangen wird, bis 'config/' gefunden wird.

    Macht das Skript robust gegenüber dem Aufruf aus beliebigem Arbeitsverzeichnis
    und widerstandsfähig gegenüber unterschiedlichen Skript-Layouts (z.B. scripts/ vs python/scripts/).
    """

    p = start if start.is_dir() else start.parent

    # Verzeichnisse bevorzugen, die wie unsere Projektstruktur aussehen
    for cand in [p, *p.parents]:
        if (cand / "config").is_dir() and (cand / "src").exists():
            return cand

    # Fallback: beliebiger Ordner, der config/ enthält
    for cand in [p, *p.parents]:
        if (cand / "config").is_dir():
            return cand

    return p


def load_env_file(project_root: Path, env_file: str) -> None:
    """Lädt die .env-Datei relativ zum Projektverzeichnis (oder absolut, falls angegeben)."""
    env_path = (project_root / env_file).resolve() if not os.path.isabs(env_file) else Path(env_file)
    load_dotenv(env_path)
    log(f"Umgebung geladen aus: {env_path}")


def db_connect():
    """DB-Verbindung (gleiche Priorität wie bei den Engine-Skripten)."""
    dsn = os.getenv("PG_DSN")
    if dsn:
        return _connect(dsn)

    host = os.getenv("PGHOST", "127.0.0.1")
    port = int(os.getenv("PGPORT", "5432"))
    user = os.getenv("PGUSER", "postgres")
    dbname = os.getenv("PGDATABASE") or os.getenv("DB_NAME") or "kpi_cs_partition"
    password = os.getenv("PGPASSWORD")

    if password:
        dsn = f"host={host} port={port} dbname={dbname} user={user} password={password}"
    else:
        dsn = f"host={host} port={port} dbname={dbname} user={user}"

    return _connect(dsn)


def parse_month_key(month_key: str) -> Tuple[int, int]:
    y, m = month_key.split("_")
    return int(y), int(m)


def month_bounds_utc(month_key: str, tz_name: str) -> Tuple[datetime, datetime]:
    """Gibt die UTC-Grenzen [start_ts_utc, end_ts_utc) für einen Monatsschlüssel zurück.

    Konvertiert lokale Monatsanfang/-ende in UTC, damit Zeitzonensprünge (DST) korrekt behandelt werden.
    """
    year, month = parse_month_key(month_key)
    tz = ZoneInfo(tz_name)

    start_local = datetime(year, month, 1, 0, 0, 0, tzinfo=tz)
    if month == 12:
        end_local = datetime(year + 1, 1, 1, 0, 0, 0, tzinfo=tz)
    else:
        end_local = datetime(year, month + 1, 1, 0, 0, 0, tzinfo=tz)

    start_utc = start_local.astimezone(ZoneInfo("UTC"))
    end_utc = end_local.astimezone(ZoneInfo("UTC"))
    return start_utc, end_utc


def _policy_allowed_layout(month_key: str) -> str:
    """Ermittelt das erlaubte source_layout gemäß Projekt-Policy."""
    try:
        year = int(month_key.split('_', 1)[0])
    except Exception:
        year = 9999
    return 'old' if year <= 2022 else 'new'


def ensure_partitions(cur, month_key: str) -> None:
    """Best-effort monatliche Partitions-Erstellung.

    Das Erstellen einer neuen Monatspartition kann fehlschlagen, wenn die *_default-Partition bereits
    Zeilen für diesen Monat enthält (Daten, die vor der Monatspartition eingefügt wurden).

    ML Stage A benötigt keine monatlichen Partitionen zwingend, daher wird dies als
    best-effort behandelt: Bei Fehler wird die abgebrochene Transaktion zurückgerollt und fortgefahren.
    """
    year, month = parse_month_key(month_key)
    month_date = datetime(year, month, 1).date()
    try:
        cur.execute("SELECT monitoring.ensure_monthly_partitions(%s)", (month_date,))
    except Exception as e:
        try:
            cur.connection.rollback()
        except Exception:
            pass
        log(
            "[WARN] ensure_monthly_partitions fehlgeschlagen für month="
            f"{month_key}: {e}. Fortfahren ohne Partitions-Erstellung."
        )


def detect_score_ts_column(cur) -> str | None:
    """Eine nutzbare Timestamp-Spalte in ml.ml_anomaly_score_hourly ermitteln.

    Manche DB-Snapshots haben 'calculated_at', andere verwenden 'created_at'.
    """
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='ml'
          AND table_name='ml_anomaly_score_hourly'
          AND column_name IN ('calculated_at','created_at','scored_at','inserted_at','updated_at')
    """)
    # Wir verwenden psycopgs dict_row row_factory (siehe connect_db), also sind rows dict-ähnlich.
    rows = cur.fetchall()
    cols: set[str] = set()
    for r in rows:
        if isinstance(r, dict):
            v = r.get('column_name')
            if v is not None:
                cols.add(str(v))
        else:
            # Fallback für tuple/namedtuple row factories
            try:
                cols.add(str(r[0]))
            except Exception:
                cols.add(str(r['column_name']))

    # Den Spaltennamen bevorzugen, der tatsächlich im DB-Schema existiert.
    # In unserer aktuellen KPI-CS-DB ist die Spalte 'created_at' (DEFAULT now()).
    if 'created_at' in cols:
        return 'created_at'
    if 'calculated_at' in cols:
        return 'calculated_at'

    for cand in ('scored_at','inserted_at','updated_at'):
        if cand in cols:
            return cand
    return None


def pick_latest_run_id(cur, start_ts, end_ts) -> Optional[str]:
    """Ermittelt die neueste run_id in analytics.qa_features_hourly für das Monats-Zeitfenster.

    Wird verwendet, wenn --run-id nicht explizit übergeben wurde.
    Sortiert nach created_at DESC, dann ts_utc DESC.
    """
    sql = """
        WITH params AS (
          SELECT %s::timestamptz AS start_ts, %s::timestamptz AS end_ts
        )
        SELECT q.run_id
        FROM analytics.qa_features_hourly q
        JOIN params p
          ON q.ts_utc >= p.start_ts
         AND q.ts_utc <  p.end_ts
        GROUP BY q.run_id
        ORDER BY MAX(q.created_at) DESC NULLS LAST,
                 MAX(q.ts_utc)      DESC
        LIMIT 1;
    """
    cur.execute(sql, (start_ts, end_ts))
    row = cur.fetchone()
    if not row:
        return None
    return row["run_id"] if isinstance(row, dict) else row[0]


@dataclass
class ModelParams:
    tz_name: str
    lookback_days: int
    min_samples_per_group: int
    z_threshold: float
    threshold_mode: str  # fixed|quantile
    quantile: float
    eps: float


def robust_stats(series: pd.Series, eps: float) -> Tuple[float, float]:
    """Gibt (Median, mad_skaliert) mit 1.4826-Skalierung zurück."""
    x = series.dropna().to_numpy()
    if x.size == 0:
        return np.nan, np.nan
    med = float(np.median(x))
    mad = float(np.median(np.abs(x - med)))
    mad_scaled = max(mad * 1.4826, eps)
    return med, mad_scaled


def compute_scores(df: pd.DataFrame, params: ModelParams) -> pd.DataFrame:
    """Berechnet Anomalie-Scores (Robust Z-Score / MAD) pro Detektor und Stunde.

    Eingabe: DataFrame mit Spalten det_id15, ts_utc, row_count, missing_rate, duplicate_rate, freshness_lag_h.

    Vorgehen:
    1. Statistiken (Median + MAD) pro (Detektor, lokale Stunde) – primäre Baseline.
    2. Fallback auf Detektor-Gesamt, dann globale Statistik.
    3. Z-Score = |x - Median| / (MAD * 1.4826).
    4. anomaly_score = Maximum der Z-Scores über alle Features.
    5. is_anomaly: True wenn anomaly_score >= Schwellenwert (fixed oder Quantil).
    6. top_driver: Feature mit dem höchsten Z-Score.
    """
    if df.empty:
        return df

    df = df.copy()
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)

    tz = ZoneInfo(params.tz_name)
    df["hour_local"] = df["ts_utc"].dt.tz_convert(tz).dt.hour.astype(int)

    feature_cols = ["row_count", "missing_rate", "duplicate_rate", "freshness_lag_h"]
    for c in feature_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # pro (Detektor, Stunde)
    stats_rows = []
    for (det, h), g in df.groupby(["det_id15", "hour_local"], sort=False):
        if len(g) < params.min_samples_per_group:
            continue
        row = {"det_id15": det, "hour_local": h}
        for c in feature_cols:
            med, mad = robust_stats(g[c], params.eps)
            row[f"{c}__med"] = med
            row[f"{c}__mad"] = mad
        stats_rows.append(row)
    stats_h = pd.DataFrame(stats_rows)

    # Detektor-Fallback
    stats_det_rows = []
    for det, g in df.groupby("det_id15", sort=False):
        if len(g) < params.min_samples_per_group:
            continue
        row = {"det_id15": det}
        for c in feature_cols:
            med, mad = robust_stats(g[c], params.eps)
            row[f"{c}__med_det"] = med
            row[f"{c}__mad_det"] = mad
        stats_det_rows.append(row)
    stats_det = pd.DataFrame(stats_det_rows)

    if not stats_h.empty:
        df = df.merge(stats_h, on=["det_id15", "hour_local"], how="left")
    if not stats_det.empty:
        df = df.merge(stats_det, on="det_id15", how="left")

    # globaler Fallback
    global_stats = {}
    for c in feature_cols:
        med, mad = robust_stats(df[c], params.eps)
        global_stats[f"{c}__med_glob"] = med
        global_stats[f"{c}__mad_glob"] = mad

    z_cols = []
    for c in feature_cols:
        base_med = df[f"{c}__med"] if f"{c}__med" in df.columns else pd.Series(np.nan, index=df.index)
        base_mad = df[f"{c}__mad"] if f"{c}__mad" in df.columns else pd.Series(np.nan, index=df.index)

        if f"{c}__med_det" in df.columns:
            base_med = base_med.fillna(df[f"{c}__med_det"])
            base_mad = base_mad.fillna(df[f"{c}__mad_det"])

        base_med = base_med.fillna(global_stats[f"{c}__med_glob"])
        base_mad = base_mad.fillna(global_stats[f"{c}__mad_glob"])

        x = df[c]
        z = (x - base_med).abs() / base_mad

        near_zero = base_mad <= params.eps * 1.0001
        z = z.mask(near_zero & (x == base_med), 0.0)
        z = z.mask(near_zero & (x != base_med), 999.0)

        zc = f"z_{c}"
        df[zc] = z.astype(float)
        z_cols.append(zc)

    df["anomaly_score"] = df[z_cols].max(axis=1)

    # top driver
    def _top_driver(row) -> Tuple[str, float]:
        best_feature = ""
        best_val = -1.0
        for zc in z_cols:
            v = row[zc]
            if pd.isna(v):
                continue
            if float(v) > best_val:
                best_val = float(v)
                best_feature = zc.replace("z_", "")
        return best_feature, best_val

    td = df.apply(_top_driver, axis=1, result_type="expand")
    df["top_driver"] = td[0]
    df["driver_value"] = td[1]

    if params.threshold_mode == "quantile":
        thr = float(df["anomaly_score"].quantile(params.quantile))
    else:
        thr = float(params.z_threshold)

    df["is_anomaly"] = df["anomaly_score"] >= thr
    df["threshold_used"] = thr

    return df


def delete_month_slice(cur, start_ts, end_ts, run_id: str, model_name: str) -> int:
    """Löscht bestehende ML-Scores für den Monat (Idempotenz bei --replace-month-slice).

    Filtert nach run_id, model_name und dem UTC-Zeitfenster des Monats.
    """
    sql = """
        DELETE FROM ml.ml_anomaly_score_hourly
        WHERE run_id = %s
          AND model_name = %s
          AND ts_utc >= %s::timestamptz
          AND ts_utc <  %s::timestamptz;
    """
    cur.execute(sql, (run_id, model_name, start_ts, end_ts))
    return int(cur.rowcount or 0)


def write_results(cur, out_df: pd.DataFrame, run_id: str, model_name: str) -> int:
    """Schreibt berechnete ML-Anomalie-Scores in ml.ml_anomaly_score_hourly (Upsert).

    Erkennt automatisch die verfügbare Timestamp-Spalte (created_at oder calculated_at),
    da das DB-Schema je nach Snapshot leicht abweichen kann.
    Nutzt psycopg2.execute_values für hohe Schreibgeschwindigkeit (Batch-Insert),
    mit Fallback auf executemany für psycopg v3.
    """
    if out_df is None or out_df.empty:
        return 0

    ts_col = detect_score_ts_column(cur)
    if ts_col:
        log(f"[INFO] Score-Timestamp-Spalte erkannt: {ts_col}")
    else:
        log("[WARN] Keine Score-Timestamp-Spalte erkannt; Schreiben ohne Timestamp-Feld.")

    now_ts = dt.datetime.now(dt.timezone.utc)

    # Erwartete df-Spalten: det_id15, ts_utc, anomaly_score, is_anomaly, top_driver, driver_value
    rows = []
    for r in out_df.itertuples(index=False):
        base = [
            getattr(r, 'det_id15'),
            getattr(r, 'ts_utc'),
            run_id,
            model_name,
            float(getattr(r, 'anomaly_score')),
            bool(getattr(r, 'is_anomaly')),
            getattr(r, 'top_driver'),
            float(getattr(r, 'driver_value')),
        ]
        if ts_col:
            base.append(now_ts)
        rows.append(tuple(base))

    base_cols = [
        'det_id15', 'ts_utc', 'run_id', 'model_name',
        'anomaly_score', 'is_anomaly',
        'top_driver', 'driver_value',
    ]
    if ts_col:
        base_cols.append(ts_col)

    col_sql = ', '.join(base_cols)

    set_cols = [
        'anomaly_score = EXCLUDED.anomaly_score',
        'is_anomaly    = EXCLUDED.is_anomaly',
        'top_driver    = EXCLUDED.top_driver',
        'driver_value  = EXCLUDED.driver_value',
    ]
    if ts_col:
        set_cols.append(f"{ts_col} = EXCLUDED.{ts_col}")

    # psycopg2 execute_values bevorzugen, wenn verfügbar (schnell). Sonst Fallback auf executemany.
    try:
        import psycopg2.extras  # type: ignore
        values_sql = (
            f"INSERT INTO ml.ml_anomaly_score_hourly ({col_sql}) VALUES %s\n"
            "ON CONFLICT (det_id15, ts_utc, run_id, model_name) DO UPDATE SET\n"
            + ",\n".join(set_cols)
            + ";"
        )
        psycopg2.extras.execute_values(cur, values_sql, rows, page_size=5000)
        return len(rows)
    except Exception:
        placeholders = ','.join(['%s'] * len(base_cols))
        one_sql = (
            f"INSERT INTO ml.ml_anomaly_score_hourly ({col_sql})\n"
            f"VALUES ({placeholders})\n"
            "ON CONFLICT (det_id15, ts_utc, run_id, model_name) DO UPDATE SET\n"
            + ",\n".join(set_cols)
            + ";"
        )
        try:
            cur.executemany(one_sql, rows)
            return len(rows)
        except Exception as e:
            # Häufiger Fallstrick: Die DB verwendet 'created_at', aber älterer Code schreibt 'calculated_at'.
            # Falls das passiert, einmal ohne die optionale Timestamp-Spalte wiederholen.
            msg = str(e)
            if ts_col and (ts_col in msg) and ('does not exist' in msg):
                try:
                    cur.connection.rollback()
                except Exception:
                    pass
                log(f"[WARN] Spalte '{ts_col}' nicht in ml.ml_anomaly_score_hourly gefunden; Wiederholung ohne sie.")

                base_cols2 = base_cols[:-1]
                rows2 = [r[:-1] for r in rows]

                placeholders2 = ','.join(['%s'] * len(base_cols2))
                col_sql2 = ', '.join(base_cols2)
                set_cols2 = set_cols[:-1]

                one_sql2 = (
                    f"INSERT INTO ml.ml_anomaly_score_hourly ({col_sql2})\n"
                    f"VALUES ({placeholders2})\n"
                    "ON CONFLICT (det_id15, ts_utc, run_id, model_name) DO UPDATE SET\n"
                    + ",\n".join(set_cols2)
                    + ";"
                )

                cur.executemany(one_sql2, rows2)
                return len(rows2)

            raise


def main() -> int:


    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/pipeline_ingestion_e2e_selected_months.yaml")
    ap.add_argument("--month-key", required=True)
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--model-name", default="robust_zscore_mad_v1_0")
    ap.add_argument("--lookback-days", type=int, default=14)
    ap.add_argument("--min-samples-per-group", type=int, default=10)
    ap.add_argument("--threshold-mode", choices=["fixed", "quantile"], default="fixed")
    ap.add_argument("--z-threshold", type=float, default=4.0)
    ap.add_argument("--quantile", type=float, default=0.99)
    ap.add_argument("--eps", type=float, default=1e-6)
    ap.add_argument("--replace-month-slice", action="store_true")
    ap.add_argument("--ensure-partitions", action="store_true", help="Vor der Ausführung monatliche Partitionen erstellen (optional).")
    ap.add_argument(
        "--min-rows",
        type=int,
        default=5000,
        help="Monat überspringen, wenn Zeilen im Monatsslice unter diesem Schwellenwert liegen.",
    )
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    # Config-Pfad robust auflösen (funktioniert unabhängig vom aktuellen Arbeitsverzeichnis)
    project_root = find_project_root(Path(__file__).resolve())
    cfg_arg = Path(args.config)
    tried: list[str] = []

    if cfg_arg.is_absolute():
        config_path = cfg_arg
        tried.append(str(config_path))
    else:
        # 1) relativ zum aktuellen Arbeitsverzeichnis
        config_path = (Path.cwd() / cfg_arg)
        tried.append(str(config_path))
        if not config_path.exists():
            # 2) relativ zum Projektverzeichnis (bevorzugt)
            config_path = (project_root / cfg_arg)
            tried.append(str(config_path))

    config_path = config_path.resolve()
    if not config_path.exists():
        raise SystemExit(
            f"Config nicht gefunden: {args.config}. Gesucht in: " + ", ".join(tried)
        )

    sys.path.insert(0, str(project_root))

    # Vorhandenen YAML-Loader verwenden
    from src.config.pipeline_config_v1_1 import load_pipeline_config  # type: ignore

    cfg = load_pipeline_config(config_path)
    tz_name = (getattr(getattr(cfg, "options", None), "timezone", None) or "Europe/Berlin")
    env_file = (getattr(cfg, "env_file", None) or "./.env")

    load_env_file(project_root, env_file)

    start_ts, end_ts = month_bounds_utc(args.month_key, tz_name)
    hist_start = start_ts - timedelta(days=int(args.lookback_days))

    log(f"month={args.month_key} tz={tz_name}")
    log(f"window_utc=[{start_ts.isoformat()} .. {end_ts.isoformat()}) lookback_days={args.lookback_days}")

    conn = db_connect()
    conn.autocommit = False

    try:
        cur = _dict_cursor(conn)
        if args.ensure_partitions:
            ensure_partitions(cur, args.month_key)
        else:
            log('[INFO] ensure_monthly_partitions übersprungen (Standard). Mit --ensure-partitions aktivieren.')
        conn.commit()

        run_id = args.run_id or pick_latest_run_id(cur, start_ts, end_ts)
        if not run_id:
            log("Keine run_id für Monatsfenster gefunden -> beenden")
            conn.rollback()
            return 0
        log(f"Verwende run_id={run_id}")

        allowed_layout = _policy_allowed_layout(args.month_key)
        log(f"[POLICY] erlaubtes source_layout={allowed_layout} (2017–2022=old, ab 2023=new)")

        fetch_sql = """
            WITH allowed AS (
                SELECT DISTINCT det_id15, ts_utc
                FROM core.fact_detector_hourly
                WHERE ts_utc >= %s::timestamptz
                  AND ts_utc <  %s::timestamptz
                  AND source_layout = %s
            )
            SELECT q.det_id15, q.ts_utc, q.row_count, q.missing_rate, q.duplicate_rate, q.freshness_lag_h
            FROM analytics.qa_features_hourly q
            JOIN allowed a
              ON a.det_id15 = q.det_id15
             AND a.ts_utc  = q.ts_utc
            WHERE q.run_id = %s
              AND q.ts_utc >= %s::timestamptz
              AND q.ts_utc <  %s::timestamptz
            ORDER BY q.det_id15, q.ts_utc;
        """
        cur.execute(fetch_sql, (hist_start, end_ts, allowed_layout, run_id, hist_start, end_ts))
        rows = cur.fetchall()
        if not rows:
            log("Keine qa_features-Zeilen im Bereich -> beenden")
            conn.rollback()
            return 0

        df = pd.DataFrame(rows)
        if args.limit:
            df = df.head(args.limit)

        # Guardrail: Wenn die Monats-Slice zu klein ist, frühzeitig überspringen.
        ts_series = pd.to_datetime(df["ts_utc"], utc=True, errors="coerce")
        month_rows = int(((ts_series >= pd.Timestamp(start_ts)) & (ts_series < pd.Timestamp(end_ts))).sum())
        if month_rows < int(args.min_rows):
            log(f"Monat überspringen month={args.month_key}: month_rows={month_rows} < min_rows={args.min_rows}")
            conn.rollback()
            return 0

        mp = ModelParams(
            tz_name=tz_name,
            lookback_days=int(args.lookback_days),
            min_samples_per_group=int(args.min_samples_per_group),
            z_threshold=float(args.z_threshold),
            threshold_mode=str(args.threshold_mode),
            quantile=float(args.quantile),
            eps=float(args.eps),
        )

        scored = compute_scores(df, mp)
        scored = scored[(scored["ts_utc"] >= pd.Timestamp(start_ts)) & (scored["ts_utc"] < pd.Timestamp(end_ts))]

        if scored.empty:
            log("Keine Zeilen im angeforderten Monatsslice nach Filterung -> beenden")
            conn.rollback()
            return 0

        if args.replace_month_slice:
            deleted = delete_month_slice(cur, start_ts, end_ts, run_id, args.model_name)
            log(f"Bestehende ML-Zeilen gelöscht: {deleted}")
            conn.commit()

        inserted = write_results(cur, scored, run_id, args.model_name)
        conn.commit()

        anom = int(scored["is_anomaly"].sum())
        thr = float(scored["threshold_used"].iloc[0])
        log(f"Eingefügt/aktualisiert: {inserted} | Anomalien: {anom}/{len(scored)} | threshold_used={thr}")
        return 0

    except Exception:
        conn.rollback()
        raise

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
