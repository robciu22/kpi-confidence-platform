#!/usr/bin/env python
"""Stage-Loader (OLD + NEW) für einen einzelnen Monat.

Dieses Skript wird vom Batch-Wrapper (run_batch_pipeline_v1_0.py) verwendet.

Wichtige Hinweise
-----------------
- Liest config/pipeline_ingestion.yaml (oder einen benutzerdefinierten Pfad via --config)
- Kann für einen bestimmten Monat via --month-key (ad-hoc) ausgeführt werden, auch wenn die YAML-Auswahl abweicht
- Respektiert die Auto-Layout-Policy (old/new/beide) aus pipeline_config_v1_1
- Registriert Quelldateien in ingestion.file_manifest
- Lädt Quelldaten über die zugehörigen Loader-Skripte in das staging-Schema

CLI-Beispiele
-------------
python scripts/run_stage_loaders_v1_3.py \
  --config config/pipeline_ingestion.yaml \
  --month-key 2022_06 \
  --with-old --with-new \
  --replace-month-slice
"""

from __future__ import annotations

import argparse
from dataclasses import replace as dc_replace
import os
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List


def _apply_source_layout_policy(plan, month_key: str):
    """Erzwingt die Projekt-Policy (OLD/NEW) unabhängig vom Auto-Layout.

    Policy:
    - 2017–2022: nur OLD  (alte Detektoren-CSV.GZ Format)
    - ab 2023:   nur NEW  (neue TGZ-Pakete)

    Hinweis: Für 2023/2024 liegen in der Quelle OLD+NEW parallel vor. Für konsistente Ergebnisse
    wird ab 2023 ausschließlich NEW verwendet.

    Da MonthPlan ein frozen dataclass ist, wird dc_replace() statt direkter Zuweisung verwendet.
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


def _find_project_root() -> Path:
    """Sucht das Projektverzeichnis nach oben, bis src/ und scripts/ gefunden werden.

    Wird benötigt, damit das Skript aus beliebigem Arbeitsverzeichnis aufrufbar ist.
    """
    cur = Path(__file__).resolve()
    for parent in [cur.parent, *cur.parents]:
        if (parent / "src").is_dir() and (parent / "scripts").is_dir():
            return parent
    # Fallback: 2 Ebenen nach oben
    return Path(__file__).resolve().parents[1]


def _expand_glob(pattern: str) -> List[Path]:
    # Glob auf dem Elternverzeichnis ausführen (unterstützt Muster wie /a/b/*.csv.gz)
    p = Path(pattern)
    parent = p.parent
    return sorted(parent.glob(p.name))


def _run(cmd: List[str], dry_run: bool) -> int:
    if dry_run:
        print("$ " + " ".join(cmd))
        return 0
    return subprocess.call(cmd)


def _register_file(
    *,
    python_bin: str,
    project_root: Path,
    source_url: str,
    month_key: str,
    dataset_version: str,
    source_type: str,
    local_path: Path,
    triggered_by: str,
    env_file: Path,
    dry_run: bool,
) -> int:
    """Registriert eine Quelldatei im File-Manifest (ingestion.file_manifest).

    Schreibt Metadaten wie Pfad, Monat, Quelle und Prüfsumme in die DB.
    Dient zur Rückverfolgbarkeit: Welche Datei wurde wann geladen?
    """
    script = project_root / "scripts" / "register_file_manifest_v1_0.py"
    cmd = [
        python_bin,
        str(script),
        "--source-url",
        source_url,
        "--month-key",
        month_key,
        "--dataset-version",
        dataset_version,
        "--source-type",
        source_type,
        "--local-path",
        str(local_path),
        "--triggered-by",
        triggered_by,
        "--env-file",
        str(env_file),
    ]
    return _run(cmd, dry_run)


def _run_new_loader(
    *,
    python_bin: str,
    project_root: Path,
    month_key: str,
    tgz_path: Path,
    env_file: Path,
    timezone: str,
    replace_month_slice: bool,
    dry_run: bool,
) -> int:
    """Startet den NEW-Loader (TGZ -> staging.stg_new_detector_hourly).

    Ruft load_new_detectors_tgz_to_staging_v1_1.py als Subprozess auf.
    """
    script = project_root / "scripts" / "load_new_detectors_tgz_to_staging_v1_1.py"
    cmd = [
        python_bin,
        str(script),
        "--month-key",
        month_key,
        "--tgz",
        str(tgz_path),
        "--env-file",
        str(env_file),
        "--timezone",
        timezone,
    ]
    if replace_month_slice:
        cmd.append("--replace-month-slice")
    return _run(cmd, dry_run)


def _run_old_detectors_loader(
    *,
    python_bin: str,
    project_root: Path,
    month_key: str,
    cfg_path: Path,
    env_file: Path,
    replace_month_slice: bool,
    dry_run: bool,
) -> int:
    """Startet den OLD-Detektoren-Loader (CSV.GZ -> staging.stg_old_det_val_hr).

    Ruft load_old_detectors_gz_to_staging_v1_2.py als Subprozess auf.
    cfg_path wird explizit übergeben, damit nicht der veraltete Default-Pfad genutzt wird.
    """
    script = project_root / "scripts" / "load_old_detectors_gz_to_staging_v1_2.py"
    cmd = [
        python_bin,
        str(script),
        "--month-key",
        month_key,
        "--config",
        str(cfg_path),
        "--env-file",
        str(env_file),
    ]
    if replace_month_slice:
        cmd.append("--replace-month-slice")
    return _run(cmd, dry_run)


def _run_old_cross_sections_loader(
    *,
    python_bin: str,
    project_root: Path,
    month_key: str,
    cfg_path: Path,
    env_file: Path,
    replace_month_slice: bool,
    dry_run: bool,
) -> int:
    """Startet den OLD-Querschnitt-Loader (CSV.GZ -> staging.stg_old_mq_hr).

    Ruft load_old_cross_sections_gz_to_staging_v1_2.py als Subprozess auf.
    cfg_path wird explizit übergeben, damit nicht der veraltete Default-Pfad genutzt wird.
    """
    script = project_root / "scripts" / "load_old_cross_sections_gz_to_staging_v1_2.py"
    cmd = [
        python_bin,
        str(script),
        "--month-key",
        month_key,
        "--config",
        str(cfg_path),
        "--env-file",
        str(env_file),
    ]
    if replace_month_slice:
        cmd.append("--replace-month-slice")
    return _run(cmd, dry_run)


def _iter_existing(files: Iterable[Path]) -> List[Path]:
    out: List[Path] = []
    for f in files:
        if f.exists():
            out.append(f)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/pipeline_ingestion.yaml")
    ap.add_argument("--month-key", default=None)
    ap.add_argument("--with-old", action="store_true")
    ap.add_argument("--with-new", action="store_true")
    ap.add_argument("--replace-month-slice", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    project_root = _find_project_root()
    sys.path.insert(0, str(project_root))

    # Konfigurationsmodul (v1_1)
    from src.config.pipeline_config_v1_1 import (  # type: ignore
        build_month_plan,
        build_plan,
        load_pipeline_config,
    )

    # Config-Pfad auflösen (relativ oder absolut)
    cfg_path = Path(args.config)
    if not cfg_path.is_absolute():
        cfg_path = project_root / cfg_path
    cfg_path = cfg_path.resolve()

    cfg = load_pipeline_config(cfg_path)
    used_project_root, used_storage_root, yaml_plans = build_plan(
        cfg, project_root=project_root
    )

    if args.month_key:
        plans = [
            build_month_plan(
                cfg,
                month_key=args.month_key,
                project_root=used_project_root,
                storage_root=used_storage_root,
            )
        ]
    else:
        plans = yaml_plans

    python_bin = sys.executable
    env_file = (used_project_root / cfg.env_file).resolve()
    timezone = cfg.options.timezone

    print("=" * 80)
    print(f"project_root: {used_project_root}")
    print(f"storage_root: {used_storage_root}")
    print(f"config: {cfg_path}")
    print(f"months: {len(plans)}")
    print("=" * 80)

    # Standardflags: wenn der Nutzer weder with-old noch with-new angegeben hat, beide ausführen
    # (aber plan-seitige enabled-Flags weiterhin respektieren)
    run_old = args.with_old or (not args.with_old and not args.with_new)
    run_new = args.with_new or (not args.with_old and not args.with_new)

    for p in plans:
        print("\n" + "=" * 80)
        print(f"MONTH: {p.month_key}")

        # NEW
        if run_new:
            if p.new_enabled:
                print(f"[NEW] registrieren: {p.new_source_url}")
                if (
                    _register_file(
                        python_bin=python_bin,
                        project_root=used_project_root,
                        source_url=p.new_source_url,
                        month_key=p.month_key,
                        dataset_version="new",
                        source_type="detectors",
                        local_path=p.new_tgz_abs,
                        triggered_by="python_orchestrator",
                        env_file=env_file,
                        dry_run=args.dry_run,
                    )
                    != 0
                ):
                    return 10

                print("[NEW] stage loader")
                if (
                    _run_new_loader(
                        python_bin=python_bin,
                        project_root=used_project_root,
                        month_key=p.month_key,
                        tgz_path=p.new_tgz_abs,
                        env_file=env_file,
                        timezone=timezone,
                        replace_month_slice=args.replace_month_slice,
                        dry_run=args.dry_run,
                    )
                    != 0
                ):
                    return 11
            else:
                print(
                    f"[SKIP] NEW: durch Auto-Layout/Config deaktiviert für month={p.month_key}"
                )
        else:
            print("[SKIP] NEW: durch CLI-Flags deaktiviert")

        # OLD (Detektoren + Querschnitte)
        if run_old:
            if p.old_enabled:
                detectors_glob_rel = (
                    "data/source/"
                    + cfg.sources.old.detectors_glob_relpath.format(
                        dataset_root=cfg.file_storage.dataset_root,
                        month_key=p.month_key,
                    )
                )
                old_det_files: List[Path] = []
                if p.old_detectors_glob_abs is not None:
                    old_det_files = _iter_existing(
                        _expand_glob(str(p.old_detectors_glob_abs))
                    )

                print(f"[OLD detectors] registrieren: {detectors_glob_rel}")
                for f in old_det_files:
                    if (
                        _register_file(
                            python_bin=python_bin,
                            project_root=used_project_root,
                            source_url=str(Path(detectors_glob_rel).parent / f.name),
                            month_key=p.month_key,
                            dataset_version="old",
                            source_type="detectors",
                            local_path=f,
                            triggered_by="python_orchestrator",
                            env_file=env_file,
                            dry_run=args.dry_run,
                        )
                        != 0
                    ):
                        return 20
            else:
                print(
                    f"[SKIP] OLD: durch Auto-Layout/Config deaktiviert für month={p.month_key}"
                )

            # OLD Querschnitte
            if p.old_enabled:
                mq_glob_rel = (
                    "data/source/"
                    + cfg.sources.old.cross_sections_glob_relpath.format(
                        dataset_root=cfg.file_storage.dataset_root,
                        month_key=p.month_key,
                    )
                )
                old_mq_files: List[Path] = []
                if p.old_cross_sections_glob_abs is not None:
                    old_mq_files = _iter_existing(
                        _expand_glob(str(p.old_cross_sections_glob_abs))
                    )

                print(f"[OLD cross_sections] registrieren: {mq_glob_rel}")
                for f in old_mq_files:
                    if (
                        _register_file(
                            python_bin=python_bin,
                            project_root=used_project_root,
                            source_url=str(Path(mq_glob_rel).parent / f.name),
                            month_key=p.month_key,
                            dataset_version="old",
                            source_type="cross_sections",
                            local_path=f,
                            triggered_by="python_orchestrator",
                            env_file=env_file,
                            dry_run=args.dry_run,
                        )
                        != 0
                    ):
                        return 21

            # OLD-Loader ausführen (laden aus Standard-Pfaden abgeleitet vom month_key)
            if p.old_enabled:
                if (
                    _run_old_detectors_loader(
                        python_bin=python_bin,
                        project_root=used_project_root,
                        month_key=p.month_key,
                        cfg_path=cfg_path,
                        env_file=env_file,
                        replace_month_slice=args.replace_month_slice,
                        dry_run=args.dry_run,
                    )
                    != 0
                ):
                    return 30

            if p.old_enabled:
                if (
                    _run_old_cross_sections_loader(
                        python_bin=python_bin,
                        project_root=used_project_root,
                        month_key=p.month_key,
                        cfg_path=cfg_path,
                        env_file=env_file,
                        replace_month_slice=args.replace_month_slice,
                        dry_run=args.dry_run,
                    )
                    != 0
                ):
                    return 31

        else:
            print("[SKIP] OLD: durch CLI-Flags deaktiviert")

    print("\n[DONE] run_stage_loaders_v1_2 abgeschlossen.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
