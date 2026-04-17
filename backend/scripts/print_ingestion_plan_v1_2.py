#!/usr/bin/env python3
"""print_ingestion_plan_v1_2.py

Gibt den aufgelösten Ingestion-Plan basierend auf config/pipeline_ingestion.yaml aus.

Hauptfunktionen:
  - Nutzt die YAML-Auswahl (mode: months | range | years | discover) über build_plan().
  - Optionale Überschreibung: --month-key <YYYY_MM> um einen einzelnen Monat zu prüfen, auch wenn er nicht in der Auswahl ist.
  - Optional: --check-exists um zu prüfen, ob referenzierte Dateien unter storage_root vorhanden sind.

Dieses Skript ist auf src/config/pipeline_config_v1_1.py abgestimmt.
"""

from __future__ import annotations

import argparse
import glob
from pathlib import Path
from typing import Tuple
import sys


def _db_key(storage_root: Path, abs_path: Path) -> str:
    """Absoluten Storage-Pfad in einen DB-freundlichen Schlüssel umwandeln (data/source/...)."""
    try:
        rel = abs_path.relative_to(storage_root)
        return f"data/source/{rel.as_posix()}"
    except Exception:
        return str(abs_path)


def _glob_exists(pattern: Path) -> Tuple[bool, int]:
    matches = glob.glob(str(pattern))
    return (len(matches) > 0, len(matches))


def _glob_parent_exists(pattern: Path) -> bool:
    """Für Muster wie /pfad/zu/verz/*.csv.gz prüfen, ob /pfad/zu/verz existiert."""
    return pattern.parent.exists()


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Ingestion-Plan aus pipeline_ingestion.yaml ausgeben (v1_2).",
    )
    ap.add_argument("--config", required=True, help="Pfad zur config/pipeline_ingestion.yaml")
    ap.add_argument(
        "--month-key",
        help="Optionale Einzelmonat-Überschreibung (YYYY_MM). Falls angegeben, wird die YAML-Auswahl für die Planung ignoriert.",
    )
    ap.add_argument("--check-exists", action="store_true", help="Lokale Dateiexistenz unter storage_root prüfen")
    args = ap.parse_args()

    # Sicherstellen, dass das Repo-Root in sys.path ist (damit `import src...` unabhängig vom cwd funktioniert).
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    from src.config.pipeline_config_v1_1 import (  # type: ignore
        build_month_plan,
        build_plan,
        load_pipeline_config,
    )

    cfg = load_pipeline_config(args.config)

    config_path = Path(args.config).resolve()
    # Wenn config-Pfad .../<project_root>/config/pipeline_ingestion.yaml ist,
    # liegt project_root eine Ebene über dem config-Ordner.
    project_root_guess = config_path.parent.parent

    used_project_root, used_storage_root, selected_plans = build_plan(cfg, project_root=project_root_guess)

    if args.month_key:
        plans = [
            build_month_plan(
                cfg,
                args.month_key,
                project_root=used_project_root,
                storage_root=used_storage_root,
            )
        ]
    else:
        plans = selected_plans

    print(f"project_root: {used_project_root}")
    print(f"storage_root: {used_storage_root}")
    print(f"months: {len(plans)}")
    print("" + "-" * 80)

    for p in plans:
        print(f"month_key: {p.month_key}")

        # NEW
        if p.new_enabled:
            print(f"  NEW tgz_abs: {p.new_tgz_abs}")
            print(f"  NEW source_url (DB-Schlüssel): {_db_key(used_storage_root, p.new_tgz_abs)}")
            if args.check_exists:
                print(f"    exists: {p.new_tgz_abs.exists()}")
        else:
            print("  NEW: deaktiviert")

        # OLD
        if p.old_enabled:
            print(f"  OLD detectors_glob_abs: {p.old_detectors_glob_abs}")
            print(f"  OLD cross_sections_glob_abs: {p.old_cross_sections_glob_abs}")
            if args.check_exists:
                det_exists, det_n = _glob_exists(p.old_detectors_glob_abs)
                mq_exists, mq_n = _glob_exists(p.old_cross_sections_glob_abs)
                # MonthPlan (v1_1) enthält keine *dir_exists-Flags, daher bei Bedarf berechnen.
                print(f"    old_detectors_dir_exists: {_glob_parent_exists(p.old_detectors_glob_abs)}")
                print(f"    old_cross_sections_dir_exists: {_glob_parent_exists(p.old_cross_sections_glob_abs)}")
                print(f"    old_detectors_files: {det_n} (exists={det_exists})")
                print(f"    old_cross_sections_files: {mq_n} (exists={mq_exists})")
        else:
            print("  OLD: deaktiviert")

        print("" + "-" * 80)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
