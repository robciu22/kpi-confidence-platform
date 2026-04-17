#!/usr/bin/env python3
"""Batch-Runner für die KPI-CS-Pipeline (Stage + Phase-B-Engine) mit MVP-Guardrails.

Was dieser Wrapper pro month_key macht
---------------------------------------
1) Stage-Loader (Dateien -> staging.*)  [optional über --steps]
2) Guardrail-Validierung (staging Sanity + Dim-Abdeckung)  [optional über --skip-guardrail]
   - Wenn guardrail FEHLSCHLÄGT: Diesen Monat ÜBERSPRINGEN und Batch fortsetzen
3) Phase-B-Engine (staging -> core/analytics/kpi + BI-Allowlist)  [optional über --steps]

Warum Guardrails?
-----------------
Sie verhindern das stille Einlesen "falscher" Monate (Inhalts-Monats-Diskrepanz) oder den
Datenverlust durch fehlende Dim-Mappings.

Exit-Code-Konvention
--------------------
guardrail_validate_month_v1_1.py gibt zurück:
- 0  PASS
- 42 SKIP (nicht fatal; wir fahren mit dem nächsten Monat fort)
- 2  FEHLER

v1_2
"""

from __future__ import annotations

import argparse
from dataclasses import replace as dc_replace
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional


def _apply_source_layout_policy(plan, month_key: str):
    """Erzwingt die Projekt-Policy (OLD/NEW) unabhängig vom Auto-Layout.

    Policy:
    - 2017–2022: nur OLD  (alte Detektoren-CSV.GZ Format)
    - ab 2023:   nur NEW  (neue TGZ-Pakete mit Det0/Det1/Det2-CSVs)

    Da MonthPlan ein frozen dataclass ist, muss dataclasses.replace() verwendet
    werden – direkte Zuweisung würde einen FrozenInstanceError auslösen.
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
            log(
                f"[POLICY] Override LayoutPlan: month={month_key} "
                f"old_enabled {old_before}→{plan.old_enabled}, "
                f"new_enabled {new_before}→{plan.new_enabled}"
            )
    return plan

def find_project_root(start: Path) -> Path:
    """Sucht das Projektverzeichnis, indem es nach oben durch das Dateisystem geht.

    Erkennung über bekannte Marker-Verzeichnisse/-Dateien (config/, scripts/, .env, src/).
    Wird benötigt, damit das Skript aus beliebigem Arbeitsverzeichnis aufrufbar ist.
    """
    markers = ["config", "scripts", ".env", "src"]
    cur = start.resolve()
    for _ in range(25):
        if any((cur / m).exists() for m in markers):
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    return start.resolve()


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _run(cmd: list[str], cwd: Path, dry_run: bool) -> int:
    print(f"[{_ts()}] $ {' '.join(cmd)}")
    if dry_run:
        return 0
    proc = subprocess.run(cmd, cwd=str(cwd))
    return proc.returncode


def _import_plan_tools(project_root: Path):
    sys.path.insert(0, str(project_root))
    try:
        from src.config.pipeline_config_v1_1 import build_plan, load_pipeline_config  # type: ignore
        return load_pipeline_config, build_plan
    except Exception:
        from src.config.pipeline_config_v1_0 import build_plan, load_pipeline_config  # type: ignore
        return load_pipeline_config, build_plan


def _months_from_yaml(project_root: Path, cfg_path: Path) -> list[str]:
    """Liest die zu verarbeitenden Monate aus der YAML-Config (selection.months).

    Gibt eine stabile, deduplizierte Liste zurück (Reihenfolge aus YAML bleibt erhalten).
    """
    load_pipeline_config, build_plan = _import_plan_tools(project_root)
    cfg = load_pipeline_config(str(cfg_path))
    res = build_plan(cfg, project_root=project_root)

    if isinstance(res, tuple) and len(res) == 3:
        _used_project_root, _used_storage_root, plans = res
    else:
        plans = res

    months: list[str] = []
    for p in plans:
        mk = getattr(p, "month_key", None)
        if mk is None:
            raise TypeError(
                f"Unerwarteter Plan-Typ: {type(p).__name__}; erwartet MonthPlan mit .month_key"
            )
        months.append(mk)

    # stabile Deduplizierung
    seen: set[str] = set()
    out: list[str] = []
    for m in months:
        if m not in seen:
            out.append(m)
            seen.add(m)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()

    ap.add_argument("--config", default="config/pipeline_ingestion.yaml")

    # Auswahl
    ap.add_argument("--month-key", dest="month_key", default=None,
                    help="Genau einen Monat verarbeiten (überschreibt YAML-Auswahl)")
    ap.add_argument("--months", nargs="*", default=None,
                    help="Liste von Monaten verarbeiten (überschreibt YAML-Auswahl)")

    # Schritte
    ap.add_argument("--steps", default="stage,engine",
                    help="Kommasepariert: stage,engine (Standard: stage,engine)")

    # Skripte
    ap.add_argument("--stage-script", default=None,
                    help="Pfad zum Stage-Skript (Standard: scripts/run_stage_loaders_v1_3.py falls vorhanden, sonst v1_1)")
    ap.add_argument("--engine-script", default="scripts/phase_b_engine_v1_4_19.py",
                    help="Pfad zum Phase-B-Engine-Skript")

    # Guardrails
    ap.add_argument("--skip-guardrail", action="store_true",
                    help="Guardrail-Validierung deaktivieren (nicht empfohlen)")
    ap.add_argument("--guardrail-script", default="scripts/guardrail_validate_month_v1_1.py",
                    help="Pfad zum Guardrail-Skript")

    # Durchgereichte Optionen
    ap.add_argument("--replace-month-slice", action="store_true")
    ap.add_argument("--with-old", action="store_true")
    ap.add_argument("--with-new", action="store_true")
    ap.add_argument("--skip-stage", action="store_true",
                    help="Bei Engine-Aufruf --skip-stage übergeben (empfohlen, wenn Stage im selben Batch lief)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--continue-on-error", action="store_true")

    args = ap.parse_args()

    project_root = find_project_root(Path(__file__).parent)
    cfg_path = (project_root / args.config).resolve()

    # Monatsliste bestimmen
    if args.month_key:
        months = [args.month_key]
        source = "cli:month-key"
    elif args.months is not None and len(args.months) > 0:
        months = list(args.months)
        source = "cli:months"
    else:
        months = _months_from_yaml(project_root, cfg_path)
        source = "yaml:selection"

    if not months:
        print(f"[{_ts()}] [ERROR] Keine Monate aufgelöst ({source}).")
        return 2

    steps = [s.strip().lower() for s in args.steps.split(",") if s.strip()]
    do_stage = "stage" in steps
    do_engine = "engine" in steps

    if args.stage_script is None:
        if (project_root / "scripts/run_stage_loaders_v1_3.py").exists():
            stage_script = "scripts/run_stage_loaders_v1_3.py"
        else:
            stage_script = "scripts/run_stage_loaders_v1_1.py"
    else:
        stage_script = args.stage_script

    py = sys.executable

    # Standardmäßig beide aktivieren, wenn nichts angegeben wurde.
    with_old = args.with_old
    with_new = args.with_new
    if do_stage and not with_old and not with_new:
        with_old = True
        with_new = True

    # Wenn Stage und Engine im selben Batch laufen, ist --skip-stage für die Engine fast immer korrekt.
    if do_stage and do_engine and not args.skip_stage:
        args.skip_stage = True

    print("=" * 80)
    print(f"[{_ts()}] Batch runner v1_1 (mit Guardrails)")
    print(f"[{_ts()}] project_root: {project_root}")
    print(f"[{_ts()}] config: {cfg_path}")
    print(f"[{_ts()}] months ({source}): {', '.join(months)}")
    print(f"[{_ts()}] steps: {steps}")
    print(f"[{_ts()}] stage_script: {stage_script}")
    print(f"[{_ts()}] engine_script: {args.engine_script}")
    print(f"[{_ts()}] guardrail: {'AUS' if args.skip_guardrail else 'AN'} ({args.guardrail_script})")
    print("=" * 80)

    # ---------------------------------------------------------------------------
    # Hauptschleife: pro Monat Stage -> Guardrail -> Engine
    # ---------------------------------------------------------------------------
    for mk in months:
        print("\n" + "#" * 80)
        print(f"[{_ts()}] MONTH: {mk}")

        # --- Stage-Loader: Quelldateien -> staging.* ---
        if do_stage:
            cmd = [
                py,
                stage_script,
                "--config",
                str(cfg_path),
                "--month-key",
                mk,
            ]
            if with_old:
                cmd.append("--with-old")
            if with_new:
                cmd.append("--with-new")
            if args.replace_month_slice:
                cmd.append("--replace-month-slice")

            rc = _run(cmd, cwd=project_root, dry_run=args.dry_run)
            if rc != 0 and not args.continue_on_error:
                print(f"[{_ts()}] [ERROR] Stage fehlgeschlagen für month={mk} (rc={rc}).")
                return rc
            elif rc != 0:
                print(f"[{_ts()}] [WARN] Stage fehlgeschlagen für month={mk} (rc={rc}) -> continue-on-error")

        # --- Guardrail: Sanity-Check vor der Engine ---
        # rc=42 -> Monat überspringen (z.B. falscher Inhalt), rc=0 -> weiter
        if do_engine and not args.skip_guardrail:
            gcmd = [
                py,
                args.guardrail_script,
                "--config",
                str(cfg_path),
                "--month-key",
                mk,
            ]
            grc = _run(gcmd, cwd=project_root, dry_run=args.dry_run)

            if grc == 42:
                print(f"[{_ts()}] [SKIP] Guardrail-Diskrepanz für month={mk} -> Batch läuft weiter")
                continue
            if grc != 0 and not args.continue_on_error:
                print(f"[{_ts()}] [ERROR] Guardrail-Fehler für month={mk} (rc={grc}).")
                return grc
            if grc != 0:
                print(f"[{_ts()}] [WARN] Guardrail-Fehler für month={mk} (rc={grc}) -> continue-on-error")

        # --- Phase-B-Engine: staging.* -> core/analytics/kpi/BI ---
        if do_engine:
            cmd = [
                py,
                args.engine_script,
                "--config",
                str(cfg_path),
                "--month-key",
                mk,
            ]
            if args.replace_month_slice:
                cmd.append("--replace-month-slice")
            if args.skip_stage:
                cmd.append("--skip-stage")

            rc = _run(cmd, cwd=project_root, dry_run=args.dry_run)
            if rc != 0 and not args.continue_on_error:
                print(f"[{_ts()}] [ERROR] Engine fehlgeschlagen für month={mk} (rc={rc}).")
                return rc
            elif rc != 0:
                print(f"[{_ts()}] [WARN] Engine fehlgeschlagen für month={mk} (rc={rc}) -> continue-on-error")

    print("\n" + "=" * 80)
    print(f"[{_ts()}] [DONE] Batch abgeschlossen.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
