#!/usr/bin/env python3
"""Guardrail-Validierung für einen einzelnen Monat (MVP).

Zweck
-----
Offensichtliche Datenprobleme frühzeitig erkennen (besonders für NEW-Detektor-TGZ-Monate), z.B.:
- Monatsslice enthält verdächtig wenige Detektoren/Zeilen (typisches Symptom bei Inhalts-Monats-Diskrepanz)
- det_name_alt-Mapping-Abdeckung in core.dim_detector ist zu gering (würde Daten downstream still verwerfen)

Verhalten
---------
- PASS  -> Exit-Code 0
- SKIP  -> Exit-Code 42 (absichtlich nicht fatal; Batch-Wrapper soll mit nächstem Monat fortfahren)
- FEHLER -> Exit-Code 2

MVP-Philosophie
---------------
Keine Schema-Änderungen. Reine SQL-Prüfungen + klare Log-Ausgaben.
v1_0 (gepatcht: rollback-sicherer Autofix)
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, replace as dc_replace
from datetime import date
from pathlib import Path
from typing import Any, Optional, Tuple

def _apply_source_layout_policy(plan, month_key: str):
    """Erzwingt die Projekt-Policy (OLD/NEW) unabhängig vom Auto-Layout.

    Policy:
    - 2017–2022: nur OLD
    - ab 2023:   nur NEW
    """
    try:
        year = int(month_key.split("_", 1)[0])
    except Exception:
        year = 9999  # im Zweifel NEW-only

    want_old = year <= 2022
    want_new = year >= 2023

    old_before = getattr(plan, "old_enabled", None)
    new_before = getattr(plan, "new_enabled", None)

    kwargs = {}
    if hasattr(plan, "old_enabled"):
        kwargs["old_enabled"] = bool(want_old)
    if hasattr(plan, "new_enabled"):
        kwargs["new_enabled"] = bool(want_new)
    if kwargs:
        plan = dc_replace(plan, **kwargs)

    if old_before is not None and new_before is not None:
        if old_before != plan.old_enabled or new_before != plan.new_enabled:
            print(
                f"[POLICY] Override LayoutPlan: month={month_key} "
                f"old_enabled {old_before}→{plan.old_enabled}, "
                f"new_enabled {new_before}→{plan.new_enabled}"
            )
    return plan

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# DB-Kompatibilität (psycopg2 vs. psycopg v3)
# ---------------------------------------------------------------------------
try:
    import psycopg2  # type: ignore
    from psycopg2.extras import RealDictCursor  # type: ignore

    def _connect_dsn(dsn: str):
        return psycopg2.connect(dsn)

    def _connect_kwargs(**kwargs):
        return psycopg2.connect(**kwargs)

    def _dict_cursor(conn):
        return conn.cursor(cursor_factory=RealDictCursor)

except ModuleNotFoundError:  # pragma: no cover
    import psycopg  # type: ignore
    from psycopg.rows import dict_row  # type: ignore

    def _connect_dsn(dsn: str):
        return psycopg.connect(dsn)

    def _connect_kwargs(**kwargs):
        return psycopg.connect(**kwargs)

    def _dict_cursor(conn):
        return conn.cursor(row_factory=dict_row)


_SKIP_RC = 42
_ERROR_RC = 2


_MONTH_RE = __import__("re").compile(r"^\d{4}_(0[1-9]|1[0-2])$")


def month_key_to_range(month_key: str) -> Tuple[date, date]:
    """Wandelt einen Monatsschlüssel (YYYY_MM) in ein halboffenes Datumsintervall [start, end) um."""
    if not _MONTH_RE.match(month_key):
        raise ValueError(f"Ungültiger month_key '{month_key}'. Erwartet 'YYYY_MM'.")
    y_s, m_s = month_key.split("_")
    y, m = int(y_s), int(m_s)
    start = date(y, m, 1)
    if m == 12:
        end = date(y + 1, 1, 1)
    else:
        end = date(y, m + 1, 1)
    return start, end


def db_connect() -> Any:
    """DB-Verbindung öffnen. Priorität:
    1) PG_DSN
    2) libpq-Umgebungsvariablen (PGHOST/PGPORT/PGUSER/PGDATABASE/DB_NAME/PGPASSWORD)
    """
    dsn = os.getenv("PG_DSN")
    if dsn:
        return _connect_dsn(dsn)

    host = os.getenv("PGHOST", "127.0.0.1")
    port = int(os.getenv("PGPORT", "5432"))
    user = os.getenv("PGUSER", "postgres")
    dbname = os.getenv("PGDATABASE") or os.getenv("DB_NAME") or "kpi_cs_partition"
    password = os.getenv("PGPASSWORD")  # optional

    kwargs = dict(host=host, port=port, user=user, dbname=dbname)
    if password:
        kwargs["password"] = password
    return _connect_kwargs(**kwargs)


def to_regclass(cur: Any, qualified_name: str) -> Optional[str]:
    cur.execute("SELECT to_regclass(%s) AS r;", (qualified_name,))
    row = cur.fetchone()
    if not row:
        return None
    # psycopg2 gibt dict mit Schlüssel 'r' zurück; psycopg v3 dict_row analog
    return row.get("r")  # type: ignore[return-value]


def _safe_rollback(cur: Any) -> None:
    """Aktuelle Transaktion zurückrollen, wenn möglich (psycopg2/psycopg v3)."""
    try:
        conn = getattr(cur, "connection", None)
        if conn is not None:
            conn.rollback()
    except Exception:
        pass


def _relation_has_columns(cur: Any, qualified_name: str, required_cols: Tuple[str, ...]) -> bool:
    """Prüft, ob eine Relation existiert und alle geforderten Spalten enthält.

    Wird verwendet, um optional verfügbare Stammdaten-Relationen sicher zu erkennen.
    """
    if to_regclass(cur, qualified_name) is None:
        return False
    if "." not in qualified_name:
        return False
    schema, name = qualified_name.split(".", 1)
    cur.execute(
        """
        SELECT lower(column_name) AS c
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, name),
    )
    cols = {r.get("c") for r in (cur.fetchall() or [])}
    req = {c.lower() for c in required_cols}
    return req.issubset(cols)



@dataclass
class GuardrailThresholds:
    # absolute Mindestwerte (sehr niedrig; Verhältnisprüfungen leisten die Hauptarbeit)
    abs_det_names_min: int = 5
    abs_rows_min: int = 5_000

    # Baseline-Verhältnisprüfungen (Baseline = maximaler beobachteter Monat im staging)
    baseline_factor_det_names: float = 0.20
    baseline_factor_rows: float = 0.20

    # Mapping-Abdeckung (dim-Join)
    mapping_ratio_min: float = 0.99


def _find_stammdaten_detector_relation(cur: Any) -> Optional[str]:
    """Sucht eine verfügbare Stammdaten-Detektor-Relation in staging.

    Benötigt mindestens die Spalten det_id15, det_name_alt, mq_id15.
    Wird für den Auto-Fix verwendet, wenn core.dim_detector unvollständig ist.
    Gibt den qualifizierten Relationsnamen zurück oder None, falls nicht vorhanden.
    """
    required = ("det_id15", "det_name_alt", "mq_id15")
    for rel in (
        "staging.stg_stammdaten_detector",
        "staging.vw_stg_stammdaten_detector",
        # breiteren Fallback nur berücksichtigen, wenn er die erforderlichen Spalten enthält:
        "staging.stg_stammdaten_verkehrsdetektion",
    ):
        if _relation_has_columns(cur, rel, required):
            return rel
    return None


def _autofix_new_dim_detector_from_stammdaten(
    *,
    cur: Any,
    d1: date,
    d2: date,
) -> Tuple[int, str]:
    """Upsert fehlender NEW det_name_alt aus staging-Stammdaten in core.dim_detector.

    Hinweise
    --------
    core.dim_detector erfordert mq_id15 (NOT NULL) und referenziert core.dim_mq.
    Für MVP wird:
      - sichergestellt, dass ein UNKNOWN-MQ-Eintrag existiert (mq_id15=0)
      - fehlende MQ-Einträge aus staging.stg_stammdaten_mq eingefügt, wenn möglich
      - fehlende Detektoren aus staging.stg_stammdaten_detector geupserted
        (mq_id15 fällt auf 0 zurück, wenn in Stammdaten nicht vorhanden)
    """
    stam_rel = _find_stammdaten_detector_relation(cur)
    if stam_rel is None:
        return 0, "[AUTO-FIX][NEW] keine staging-Stammdaten-Detektor-Relation gefunden"

    # 0) Sicherstellen, dass UNKNOWN-MQ existiert (mq_id15=0) als sicherer Fallback
    try:
        cur.execute(
            "INSERT INTO core.dim_mq (mq_id15, mq_kurzname) VALUES (0, 'UNKNOWN') "
            "ON CONFLICT (mq_id15) DO NOTHING;"
        )
    except Exception as e:
        _safe_rollback(cur)
        return 0, f"[AUTO-FIX][NEW] UNKNOWN dim_mq konnte nicht sichergestellt werden: {e}"

    # 1) Fehlende MQ-Einträge einfügen, die von einzufügenden Detektoren referenziert werden (best effort)
    if to_regclass(cur, "staging.stg_stammdaten_mq") is not None:
        sql_mq = f"""
        WITH missing_det AS (
          SELECT DISTINCT s.det_name_alt
          FROM staging.stg_new_detector_hourly s
          LEFT JOIN core.dim_detector d
            ON d.det_name_alt = s.det_name_alt
          WHERE s.datum_ortszeit >= %s AND s.datum_ortszeit < %s
            AND d.det_id15 IS NULL
        ), needed_mq AS (
          SELECT DISTINCT md.mq_id15
          FROM {stam_rel} md
          JOIN missing_det m
            ON m.det_name_alt = md.det_name_alt
          WHERE md.mq_id15 IS NOT NULL
        ), missing_mq AS (
          SELECT n.mq_id15
          FROM needed_mq n
          LEFT JOIN core.dim_mq dm
            ON dm.mq_id15 = n.mq_id15
          WHERE dm.mq_id15 IS NULL
        )
        INSERT INTO core.dim_mq (
          mq_id15, mq_kurzname, strasse, "position", pos_detail, richtung, lon_wgs84, lat_wgs84
        )
        SELECT
          m.mq_id15, sm.mq_kurzname, sm.strasse, sm."position", sm.pos_detail, sm.richtung, sm.lon_wgs84, sm.lat_wgs84
        FROM missing_mq m
        JOIN staging.stg_stammdaten_mq sm
          ON sm.mq_id15 = m.mq_id15
        ON CONFLICT (mq_id15) DO NOTHING;
        """
        try:
            cur.execute(sql_mq, (d1, d2))
        except Exception as e:
            _safe_rollback(cur)
            # Nicht fatal; wir können trotzdem auf mq_id15=0 für Detektoren zurückfallen.
            # Meldung für Fehlerbehebung beibehalten.
            pass

    # 2) Fehlende Detektoren in dim_detector upserten (mq_id15 fällt auf 0 zurück)
    sql_det = f"""
    WITH missing_det AS (
      SELECT DISTINCT s.det_name_alt
      FROM staging.stg_new_detector_hourly s
      LEFT JOIN core.dim_detector d
        ON d.det_name_alt = s.det_name_alt
      WHERE s.datum_ortszeit >= %s AND s.datum_ortszeit < %s
        AND d.det_id15 IS NULL
    ), src AS (
      SELECT DISTINCT
        md.det_id15,
        COALESCE(md.mq_id15, 0) AS mq_id15,
        md.det_name_alt,
        md.det_name_neu,
        md.spur,
        md.annotation,
        md.kommentar,
        md.inbetriebnahme,
        md.abbaudatum,
        md.deinstalliert,
        md.lon_wgs84,
        md.lat_wgs84
      FROM {stam_rel} md
      JOIN missing_det m
        ON m.det_name_alt = md.det_name_alt
      WHERE md.det_id15 IS NOT NULL
    )
    INSERT INTO core.dim_detector (
      det_id15, mq_id15, det_name_alt, det_name_neu, spur, annotation, kommentar,
      inbetriebnahme, abbaudatum, deinstalliert, lon_wgs84, lat_wgs84
    )
    SELECT
      det_id15, mq_id15, det_name_alt, det_name_neu, spur, annotation, kommentar,
      inbetriebnahme, abbaudatum, deinstalliert, lon_wgs84, lat_wgs84
    FROM src
    ON CONFLICT (det_id15) DO UPDATE
      SET
        mq_id15      = EXCLUDED.mq_id15,
        det_name_alt = COALESCE(EXCLUDED.det_name_alt, core.dim_detector.det_name_alt),
        det_name_neu = COALESCE(EXCLUDED.det_name_neu, core.dim_detector.det_name_neu),
        spur         = COALESCE(EXCLUDED.spur, core.dim_detector.spur),
        annotation   = COALESCE(EXCLUDED.annotation, core.dim_detector.annotation),
        kommentar    = COALESCE(EXCLUDED.kommentar, core.dim_detector.kommentar),
        inbetriebnahme = COALESCE(EXCLUDED.inbetriebnahme, core.dim_detector.inbetriebnahme),
        abbaudatum     = COALESCE(EXCLUDED.abbaudatum, core.dim_detector.abbaudatum),
        deinstalliert  = COALESCE(EXCLUDED.deinstalliert, core.dim_detector.deinstalliert),
        lon_wgs84    = COALESCE(EXCLUDED.lon_wgs84, core.dim_detector.lon_wgs84),
        lat_wgs84    = COALESCE(EXCLUDED.lat_wgs84, core.dim_detector.lat_wgs84);
    """
    try:
        cur.execute(sql_det, (d1, d2))
        affected = int(getattr(cur, "rowcount", 0) or 0)
        return affected, f"[AUTO-FIX][NEW] dim_detector aus {stam_rel} geupserted (affected={affected})"
    except Exception as e:
        _safe_rollback(cur)
        return 0, f"[AUTO-FIX][NEW] Upsert aus {stam_rel} fehlgeschlagen: {e}"


def _autofix_old_dim_detector_from_staging(*, cur: Any) -> Tuple[int, str]:
    """Stellt sicher, dass alle det_id15-Werte aus dem OLD-Staging in core.dim_detector existieren.

    Verhindert FK-Fehler beim späteren Load von core.fact_detector_hourly.
    OLD-Staging nutzt die Spalte detid_15 (nicht det_id15).

    Vorgehen:
    1. Fehlende det_id15 werden als Platzhalter (mq_id15=0, det_name_alt='UNKNOWN_<id>') eingefügt.
    2. Optional: Anreicherung mit det_name_alt aus Stammdaten (best effort).
    """
    if to_regclass(cur, "staging.stg_old_det_val_hr") is None:
        return 0, "[AUTO-FIX][OLD] staging.stg_old_det_val_hr nicht gefunden -> nichts zu reparieren"

    # Sicherstellen, dass UNKNOWN-MQ existiert (mq_id15=0) für sichere Platzhalter
    try:
        cur.execute(
            "INSERT INTO core.dim_mq (mq_id15, mq_kurzname) VALUES (0, 'UNKNOWN') "
            "ON CONFLICT (mq_id15) DO NOTHING;"
        )
    except Exception as e:
        _safe_rollback(cur)
        return 0, f"[AUTO-FIX][OLD] UNKNOWN dim_mq konnte nicht sichergestellt werden: {e}"

    missing_cnt_row = _fetch_one(
        cur,
        """
        WITH missing AS (
          SELECT DISTINCT s.detid_15 AS det_id15
          FROM staging.stg_old_det_val_hr s
          LEFT JOIN core.dim_detector d
            ON d.det_id15 = s.detid_15
          WHERE d.det_id15 IS NULL
            AND s.detid_15 IS NOT NULL
        )
        SELECT COUNT(*)::bigint AS missing_cnt FROM missing;
        """,
        tuple(),
    )
    missing_cnt = int(missing_cnt_row.get("missing_cnt") or 0)
    if missing_cnt == 0:
        return 0, "[AUTO-FIX][OLD] keine fehlenden det_id15 in dim_detector"

    affected_total = 0
    msgs: list[str] = [f"[AUTO-FIX][OLD] fehlende det_id15 in dim_detector: {missing_cnt}"]

    # MVP: Platzhalter einfügen (FK-sicher). Falls Stammdaten vorhanden, danach det_name_alt anreichern.
    try:
        cur.execute(
            """
            WITH missing AS (
              SELECT DISTINCT s.detid_15 AS det_id15
              FROM staging.stg_old_det_val_hr s
              LEFT JOIN core.dim_detector d
                ON d.det_id15 = s.detid_15
              WHERE d.det_id15 IS NULL
                AND s.detid_15 IS NOT NULL
            )
            INSERT INTO core.dim_detector (det_id15, mq_id15, det_name_alt)
            SELECT det_id15, 0::bigint, ('UNKNOWN_' || det_id15::text)
            FROM missing
            ON CONFLICT (det_id15) DO UPDATE
              SET det_name_alt = COALESCE(core.dim_detector.det_name_alt, EXCLUDED.det_name_alt);
            """
        )
        aff = int(getattr(cur, "rowcount", 0) or 0)
        affected_total += aff
        msgs.append(f"[AUTO-FIX][OLD] Platzhalter eingefügt/aktualisiert (affected={aff})")
    except Exception as e:
        _safe_rollback(cur)
        msgs.append(f"[AUTO-FIX][OLD] Platzhalter-Insert fehlgeschlagen: {e}")

    # Optionale Anreicherung aus Stammdaten, falls vorhanden (best effort, blockiert nicht)
    stam_rel = _find_stammdaten_detector_relation(cur)
    if stam_rel is not None:
        try:
            cur.execute(
                f"""
                WITH src AS (
                  SELECT DISTINCT md.det_id15, md.det_name_alt
                  FROM {stam_rel} md
                  JOIN staging.stg_old_det_val_hr s
                    ON s.detid_15 = md.det_id15
                  WHERE md.det_name_alt IS NOT NULL
                )
                UPDATE core.dim_detector d
                SET det_name_alt = COALESCE(src.det_name_alt, d.det_name_alt)
                FROM src
                WHERE d.det_id15 = src.det_id15;
                """
            )
            msgs.append(f"[AUTO-FIX][OLD] det_name_alt aus {stam_rel} angereichert")
        except Exception as e:
            _safe_rollback(cur)
            msgs.append(f"[AUTO-FIX][OLD] Anreicherung aus {stam_rel} fehlgeschlagen: {e}")

    # Verbleibende Fehlzählungen nach dem Fix
    missing_cnt_row2 = _fetch_one(
        cur,
        """
        WITH missing AS (
          SELECT DISTINCT s.detid_15 AS det_id15
          FROM staging.stg_old_det_val_hr s
          LEFT JOIN core.dim_detector d
            ON d.det_id15 = s.detid_15
          WHERE d.det_id15 IS NULL
            AND s.detid_15 IS NOT NULL
        )
        SELECT COUNT(*)::bigint AS missing_cnt FROM missing;
        """,
        tuple(),
    )
    msgs.append(f"[AUTO-FIX][OLD] noch fehlende nach Fix: {int(missing_cnt_row2.get('missing_cnt') or 0)}")

    return affected_total, " | ".join(msgs)


def _fetch_one(cur: Any, sql: str, params: Tuple[Any, ...]) -> dict:
    cur.execute(sql, params)
    row = cur.fetchone()
    return row or {}


def validate_new_detectors(
    *,
    cur: Any,
    month_key: str,
    d1: date,
    d2: date,
    th: GuardrailThresholds,
    auto_fix_dim: bool,
) -> Tuple[bool, str]:
    """Validiert den NEW-Detektor-Monatsslice in staging.stg_new_detector_hourly.

    Prüft:
    - Mindestanzahl Zeilen und Detectornamen (absolute Schwellenwerte)
    - Verhältnis zum besten bekannten Monat (Baseline-Checks)
    - Mapping-Coverage: Anteil der Detectornamen in core.dim_detector

    Gibt (True, msg) bei PASS oder (False, msg) bei FAIL zurück.
    Bei auto_fix_dim=True wird ein Upsert aus Stammdaten versucht, bevor die Mapping-Rate geprüft wird.
    """
    if to_regclass(cur, "staging.stg_new_detector_hourly") is None:
        return True, "[NEW] staging.stg_new_detector_hourly nicht gefunden -> PASS (nichts zu validieren)."

    # Monatsslice-Statistiken
    stats = _fetch_one(
        cur,
        """
        SELECT
          COUNT(*)::bigint AS rows,
          COUNT(DISTINCT det_name_alt)::bigint AS det_names,
          MIN(datum_ortszeit) AS min_datum,
          MAX(datum_ortszeit) AS max_datum
        FROM staging.stg_new_detector_hourly
        WHERE datum_ortszeit >= %s AND datum_ortszeit < %s;
        """,
        (d1, d2),
    )
    rows = int(stats.get("rows") or 0)
    det_names = int(stats.get("det_names") or 0)

    # Baseline über alle gestagetem NEW-Monate
    baseline = _fetch_one(
        cur,
        """
        WITH per_month AS (
          SELECT
            date_trunc('month', datum_ortszeit) AS m,
            COUNT(*)::bigint AS rows,
            COUNT(DISTINCT det_name_alt)::bigint AS det_names
          FROM staging.stg_new_detector_hourly
          GROUP BY 1
        )
        SELECT
          COALESCE(MAX(rows), 0)::bigint AS max_rows,
          COALESCE(MAX(det_names), 0)::bigint AS max_det_names
        FROM per_month;
        """,
        tuple(),
    )
    base_rows = int(baseline.get("max_rows") or 0)
    base_det = int(baseline.get("max_det_names") or 0)

    def _mapping_stats() -> Tuple[int, int, float]:
        ms = _fetch_one(
            cur,
            """
            SELECT
              COUNT(DISTINCT s.det_name_alt)::bigint AS staging_det_names,
              COUNT(DISTINCT d.det_name_alt)::bigint AS matched_dim_names
            FROM staging.stg_new_detector_hourly s
            LEFT JOIN core.dim_detector d
              ON d.det_name_alt = s.det_name_alt
            WHERE s.datum_ortszeit >= %s AND s.datum_ortszeit < %s;
            """,
            (d1, d2),
        )
        st = int(ms.get("staging_det_names") or 0)
        mt = int(ms.get("matched_dim_names") or 0)
        rr = (mt / st) if st else 1.0
        return st, mt, rr

    staging_det_names, matched_dim_names, mapping_ratio = _mapping_stats()

    autofix_note = ""
    if auto_fix_dim and staging_det_names > 0 and mapping_ratio < th.mapping_ratio_min:
        _aff, af_msg = _autofix_new_dim_detector_from_stammdaten(cur=cur, d1=d1, d2=d2)
        autofix_note = " | " + af_msg
        staging_det_names, matched_dim_names, mapping_ratio = _mapping_stats()

    # Verhältnisse zur Baseline berechnen (Division durch Null vermeiden)
    ratio_det = (det_names / base_det) if base_det else 1.0
    ratio_rows = (rows / base_rows) if base_rows else 1.0

    reasons: list[str] = []

    if rows == 0 or det_names == 0:
        reasons.append("no_data_in_month_slice")

    if det_names < th.abs_det_names_min:
        reasons.append(f"det_names<{th.abs_det_names_min} (det_names={det_names})")

    if rows < th.abs_rows_min:
        reasons.append(f"rows<{th.abs_rows_min} (rows={rows})")

    # Baseline-Verhältnisprüfungen nur wenn Baseline sinnvoll erscheint
    if base_det >= th.abs_det_names_min and ratio_det < th.baseline_factor_det_names:
        reasons.append(
            f"det_names_ratio<{th.baseline_factor_det_names:.2f} (det_names={det_names}, baseline_det_names={base_det})"
        )

    if base_rows >= th.abs_rows_min and ratio_rows < th.baseline_factor_rows:
        reasons.append(
            f"rows_ratio<{th.baseline_factor_rows:.2f} (rows={rows}, baseline_rows={base_rows})"
        )

    if mapping_ratio < th.mapping_ratio_min:
        reasons.append(
            f"mapping_ratio<{th.mapping_ratio_min:.2f} (matched={matched_dim_names}, staging={staging_det_names})"
        )

    # Meldung aufbauen (immer ausgeben für Transparenz)
    msg = (
        f"[NEW] month={month_key} rows={rows} det_names={det_names} "
        f"baseline_rows={base_rows} baseline_det_names={base_det} "
        f"rows_ratio={ratio_rows:.4f} det_ratio={ratio_det:.4f} "
        f"mapping_ratio={mapping_ratio:.4f}"
    ) + autofix_note

    if reasons:
        return False, msg + " | FAIL: " + "; ".join(reasons)

    return True, msg + " | PASS"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/pipeline_ingestion.yaml")
    ap.add_argument("--month-key", required=True)

    ap.add_argument("--env-file", default=None, help="Optionaler .env-Pfad (Standard: YAML env_file)")
    ap.add_argument("--skip-new", action="store_true", help="NEW-Staging-Prüfungen überspringen")
    ap.add_argument(
        "--no-auto-fix-dim",
        action="store_true",
        help="Automatische dim_detector-Korrekturen deaktivieren (Stammdaten-Upsert / Platzhalter)",
    )

    # Schwellenwerte (nur bei Bedarf überschreiben)
    ap.add_argument("--abs-det-names-min", type=int, default=GuardrailThresholds.abs_det_names_min)
    ap.add_argument("--abs-rows-min", type=int, default=GuardrailThresholds.abs_rows_min)
    ap.add_argument("--baseline-factor-det-names", type=float, default=GuardrailThresholds.baseline_factor_det_names)
    ap.add_argument("--baseline-factor-rows", type=float, default=GuardrailThresholds.baseline_factor_rows)
    ap.add_argument("--mapping-ratio-min", type=float, default=GuardrailThresholds.mapping_ratio_min)

    args = ap.parse_args()

    # Config laden, um env_file + Auto-Layout-Entscheidung (NEW aktiv oder nicht) zu ermitteln
    project_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(project_root))

    from src.config.pipeline_config_v1_1 import load_pipeline_config, build_month_plan, build_plan  # type: ignore

    cfg_path = Path(args.config)
    if not cfg_path.is_absolute():
        cfg_path = (project_root / cfg_path)
    cfg_path = cfg_path.resolve()

    cfg = load_pipeline_config(cfg_path)

    used_project_root, used_storage_root, _plans = build_plan(cfg, project_root=project_root)
    mp = build_month_plan(cfg, project_root=used_project_root, storage_root=used_storage_root, month_key=args.month_key)
    mp = _apply_source_layout_policy(mp, args.month_key)

    env_file = Path(args.env_file) if args.env_file else (used_project_root / cfg.env_file)
    if env_file.exists():
        load_dotenv(env_file)
    else:
        # Verbindungsversuch über libpq-Umgebungsvariablen
        pass

    d1, d2 = month_key_to_range(args.month_key)

    th = GuardrailThresholds(
        abs_det_names_min=args.abs_det_names_min,
        abs_rows_min=args.abs_rows_min,
        baseline_factor_det_names=args.baseline_factor_det_names,
        baseline_factor_rows=args.baseline_factor_rows,
        mapping_ratio_min=args.mapping_ratio_min,
    )

    try:
        conn = db_connect()
        # psycopg2: autocommit standardmäßig aus; psycopg v3: ebenfalls autocommit=False
        cur = _dict_cursor(conn)

        print("=" * 80)
        print(f"[GUARDRAIL] month={args.month_key} env_file={env_file}")
        print(f"[GUARDRAIL] plan: old_enabled={mp.old_enabled} new_enabled={mp.new_enabled}")
        print("=" * 80)

        auto_fix_dim = not args.no_auto_fix_dim

        if mp.new_enabled and not args.skip_new:
            ok, msg = validate_new_detectors(
                cur=cur,
                month_key=args.month_key,
                d1=d1,
                d2=d2,
                th=th,
                auto_fix_dim=auto_fix_dim,
            )
            print(msg)
            if auto_fix_dim:
                # Auto-Fixes persistieren (Stammdaten-Upsert)
                try:
                    conn.commit()
                except Exception:
                    pass
            if not ok:
                print(f"[GUARDRAIL] SKIP month={args.month_key} (rc={_SKIP_RC})")
                if auto_fix_dim:
                    try:
                        conn.commit()
                    except Exception:
                        pass
                try:
                    conn.close()
                except Exception:
                    pass
                return _SKIP_RC
        else:
            print("[GUARDRAIL] NEW-Prüfungen übersprungen (nicht durch Plan aktiviert oder --skip-new gesetzt).")

        # OLD: sicherstellen, dass dim_detector alle det_id15 aus dem OLD-Staging enthält
        # (verhindert FK-Verletzung beim Laden von core.fact_detector_hourly)
        if mp.old_enabled and auto_fix_dim:
            _aff, msg = _autofix_old_dim_detector_from_staging(cur=cur)
            print(msg)
            try:
                conn.commit()
            except Exception:
                pass

        try:
            conn.close()
        except Exception:
            pass

        print(f"[GUARDRAIL] PASS month={args.month_key}")
        return 0

    except Exception as e:
        print(f"[GUARDRAIL] FEHLER: {e}")
        return _ERROR_RC


if __name__ == "__main__":
    raise SystemExit(main())
