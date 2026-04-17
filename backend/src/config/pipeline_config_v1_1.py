#!/usr/bin/env python3
"""
src/config/pipeline_config_v1_1.py

v1_1 ergänzt **AUTO-LAYOUT** für Verkehrsdetektion-Quellen:

Hintergrund (Berlin Verkehrsdetektion API Layout):
- 2017–2022: NUR OLD (old_detectors + old_cross_sections)
- 2023–2024: BEIDE (OLD + NEW)
- 2025: NUR NEW (new_detectors tgz)

Ziel:
- Auswahl eines einzelnen Monats, vieler Monate oder ganzer Jahre in EINEM Lauf.
- Für gemischte Monatslisten (z.B. 2022_06 + 2025_03) pro Monat entscheiden, ob OLD/NEW gilt.
- Rückwärtskompatibilität: wenn layout_mode fehlt oder "manual" ist, Verhalten identisch zu v1_0.

YAML-Ergänzungen (optional):
sources:
  layout_mode: "auto"          # "manual" (Standard) | "auto"
  auto_layout:
    old_only_years: [2017, 2018, 2019, 2020, 2021, 2022]
    both_years:     [2023, 2024]
    new_only_years: [2025]
    unknown_year_policy: "both"   # both | old | new | none
  overrides:
    "2022_06": { old: true,  new: false }   # optionale Überschreibung pro Monat
    "2025_03": { old: false, new: true  }

Wichtig:
- Globale Gates gelten weiterhin:
    sources.new.enabled / sources.old.enabled
  Auto-Layout berechnet eine pro-Monat (old,new)-Entscheidung und verknüpft sie per AND mit den globalen Gates.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import yaml


_MONTH_RE = re.compile(r"^\d{4}_(0[1-9]|1[0-2])$")


def _must_month_key(s: str) -> str:
    s = str(s).strip()
    if not _MONTH_RE.match(s):
        raise ValueError(f"Ungültiger month_key '{s}'. Erwartet 'YYYY_MM'.")
    return s


def months_between_inclusive(start_month: str, end_month: str) -> List[str]:
    start_month = _must_month_key(start_month)
    end_month = _must_month_key(end_month)

    ys, ms = start_month.split("_")
    ye, me = end_month.split("_")
    y, m = int(ys), int(ms)
    y_end, m_end = int(ye), int(me)

    if (y, m) > (y_end, m_end):
        raise ValueError(f"range.start_month > range.end_month: {start_month} > {end_month}")

    out: List[str] = []
    while True:
        out.append(f"{y:04d}_{m:02d}")
        if (y, m) == (y_end, m_end):
            break
        m += 1
        if m == 13:
            m = 1
            y += 1
    return out


def months_for_years(years: Sequence[int]) -> List[str]:
    out: List[str] = []
    for y in years:
        for m in range(1, 13):
            out.append(f"{int(y):04d}_{m:02d}")
    return out


def safe_relpath(path: Path, base: Path) -> str:
    """Relativen POSIX-Pfad zurückgeben, wenn möglich, sonst absoluten."""
    try:
        return path.relative_to(base).as_posix()
    except Exception:
        return path.as_posix()


@dataclass(frozen=True)
class FileStorageConfig:
    root_env: str = "FILE_STORAGE_ROOT"
    root_fallback: str = "./data/source"
    auto_create: bool = True
    dataset_root: str = "verkehrsdetektion/monthly"

    def resolve_root(self, project_root: Path) -> Path:
        val = os.getenv(self.root_env) or self.root_fallback
        p = Path(val).expanduser()
        if not p.is_absolute():
            p = (project_root / p).resolve()
        else:
            p = p.resolve()
        if self.auto_create:
            p.mkdir(parents=True, exist_ok=True)
        return p


@dataclass(frozen=True)
class SelectionDiscover:
    month_glob: str = "*"


@dataclass(frozen=True)
class SelectionRange:
    start_month: str
    end_month: str


@dataclass(frozen=True)
class SelectionConfig:
    mode: str
    months: Tuple[str, ...] = ()
    range: Optional[SelectionRange] = None
    years: Tuple[int, ...] = ()
    discover: Optional[SelectionDiscover] = None


@dataclass(frozen=True)
class NewSourceConfig:
    enabled: bool = True
    tgz_relpath: str = "{dataset_root}/{month_key}/new_detectors/detektoren_{month_key}.tgz"


@dataclass(frozen=True)
class OldSourceConfig:
    enabled: bool = True
    detectors_glob_relpath: str = "{dataset_root}/{month_key}/old_detectors/*.csv.gz"
    cross_sections_glob_relpath: str = "{dataset_root}/{month_key}/old_cross_sections/*.csv.gz"


@dataclass(frozen=True)
class AutoLayoutConfig:
    layout_mode: str = "manual"  # manual | auto

    # Standard-Regeln (wie im Projekt besprochen):
    old_only_years: Tuple[int, ...] = (2017, 2018, 2019, 2020, 2021, 2022)
    both_years: Tuple[int, ...] = (2023, 2024)
    new_only_years: Tuple[int, ...] = (2025,)

    # Bei unbekannten Jahren:
    unknown_year_policy: str = "both"  # both | old | new | none

    # Überschreibungen pro Monat:
    overrides: Dict[str, Dict[str, bool]] = field(default_factory=dict)


@dataclass(frozen=True)
class SourcesConfig:
    new: NewSourceConfig
    old: OldSourceConfig
    auto_layout: AutoLayoutConfig = AutoLayoutConfig()


@dataclass(frozen=True)
class LimitsConfig:
    new_only_detectors: Tuple[str, ...] = ()
    max_rows: Optional[int] = None


@dataclass(frozen=True)
class OptionsConfig:
    timezone: str = "Europe/Berlin"
    limits: LimitsConfig = LimitsConfig()


@dataclass(frozen=True)
class HashingConfig:
    algorithm: str = "sha256"
    chunk_size_bytes: int = 8 * 1024 * 1024


@dataclass(frozen=True)
class ManifestConfig:
    enabled: bool = True


@dataclass(frozen=True)
class PipelineConfig:
    env_file: str
    file_storage: FileStorageConfig
    selection: SelectionConfig
    sources: SourcesConfig
    options: OptionsConfig
    hashing: HashingConfig
    manifest: ManifestConfig


@dataclass(frozen=True)
class MonthPlan:
    month_key: str

    # NEW
    new_enabled: bool
    new_tgz_abs: Optional[Path]
    new_source_url: Optional[str]

    # OLD
    old_enabled: bool
    old_detectors_glob_abs: Optional[Path]
    old_cross_sections_glob_abs: Optional[Path]


def _fmt(template: str, *, dataset_root: str, month_key: str) -> str:
    return template.format(dataset_root=dataset_root, month_key=month_key)


def _parse_overrides(raw: object) -> Dict[str, Dict[str, bool]]:
    out: Dict[str, Dict[str, bool]] = {}
    if not isinstance(raw, dict):
        return out
    for mk, cfg in raw.items():
        mk2 = _must_month_key(mk)
        if not isinstance(cfg, dict):
            continue
        # Schlüssel akzeptieren: old/new (bevorzugt)
        ov: Dict[str, bool] = {}
        if "old" in cfg:
            ov["old"] = bool(cfg["old"])
        if "new" in cfg:
            ov["new"] = bool(cfg["new"])
        if ov:
            out[mk2] = ov
    return out


def _parse_cfg(d: dict) -> PipelineConfig:
    env_file = str(d.get("env_file") or "./.env")

    fs = d.get("file_storage") or {}
    file_storage = FileStorageConfig(
        root_env=str(fs.get("root_env") or "FILE_STORAGE_ROOT"),
        root_fallback=str(fs.get("root_fallback") or "./data/source"),
        auto_create=bool(fs.get("auto_create", True)),
        dataset_root=str(fs.get("dataset_root") or "verkehrsdetektion/monthly"),
    )

    sel = d.get("selection") or {}
    mode = str(sel.get("mode") or "months").strip()

    months = tuple(_must_month_key(m) for m in (sel.get("months") or []))

    sel_range = None
    if sel.get("range"):
        r = sel["range"]
        sel_range = SelectionRange(
            start_month=_must_month_key(r["start_month"]),
            end_month=_must_month_key(r["end_month"]),
        )

    years = tuple(int(y) for y in (sel.get("years") or []))

    sel_discover = None
    if sel.get("discover"):
        dd = sel["discover"]
        sel_discover = SelectionDiscover(month_glob=str(dd.get("month_glob") or "*"))

    selection = SelectionConfig(
        mode=mode,
        months=months,
        range=sel_range,
        years=years,
        discover=sel_discover,
    )

    src = d.get("sources") or {}
    new = src.get("new") or {}
    old = src.get("old") or {}

    # Auto-Layout-Parsing (optional)
    layout_mode = str(src.get("layout_mode") or "manual").strip().lower()
    auto = src.get("auto_layout") or {}
    overrides = _parse_overrides(src.get("overrides"))

    auto_cfg = AutoLayoutConfig(
        layout_mode=layout_mode,
        old_only_years=tuple(int(x) for x in (auto.get("old_only_years") or AutoLayoutConfig.old_only_years)),
        both_years=tuple(int(x) for x in (auto.get("both_years") or AutoLayoutConfig.both_years)),
        new_only_years=tuple(int(x) for x in (auto.get("new_only_years") or AutoLayoutConfig.new_only_years)),
        unknown_year_policy=str(auto.get("unknown_year_policy") or AutoLayoutConfig.unknown_year_policy).strip().lower(),
        overrides=overrides,
    )

    sources = SourcesConfig(
        new=NewSourceConfig(
            enabled=bool(new.get("enabled", True)),
            tgz_relpath=str(new.get("tgz_relpath") or NewSourceConfig.tgz_relpath),
        ),
        old=OldSourceConfig(
            enabled=bool(old.get("enabled", True)),
            detectors_glob_relpath=str(old.get("detectors_glob_relpath") or OldSourceConfig.detectors_glob_relpath),
            cross_sections_glob_relpath=str(old.get("cross_sections_glob_relpath") or OldSourceConfig.cross_sections_glob_relpath),
        ),
        auto_layout=auto_cfg,
    )

    opt = d.get("options") or {}
    lim = opt.get("limits") or {}
    options = OptionsConfig(
        timezone=str(opt.get("timezone") or "Europe/Berlin"),
        limits=LimitsConfig(
            new_only_detectors=tuple(str(x) for x in (lim.get("new_only_detectors") or [])),
            max_rows=lim.get("max_rows", None),
        ),
    )

    hashing = d.get("hashing") or {}
    hashing_cfg = HashingConfig(
        algorithm=str(hashing.get("algorithm") or "sha256"),
        chunk_size_bytes=int(hashing.get("chunk_size_bytes") or 8 * 1024 * 1024),
    )

    manifest = d.get("manifest") or {}
    manifest_cfg = ManifestConfig(enabled=bool(manifest.get("enabled", True)))

    return PipelineConfig(
        env_file=env_file,
        file_storage=file_storage,
        selection=selection,
        sources=sources,
        options=options,
        hashing=hashing_cfg,
        manifest=manifest_cfg,
    )


def load_pipeline_config(path: str | Path) -> PipelineConfig:
    p = Path(path).expanduser().resolve()
    if not p.exists():
        raise FileNotFoundError(f"Pipeline-YAML nicht gefunden: {p}")
    with p.open("r", encoding="utf-8") as f:
        d = yaml.safe_load(f) or {}
    return _parse_cfg(d)


def resolve_month_keys(cfg: PipelineConfig, *, storage_root: Path) -> List[str]:
    mode = cfg.selection.mode.lower().strip()

    if mode == "months":
        if not cfg.selection.months:
            raise ValueError("selection.mode=months erfordert selection.months")
        return list(cfg.selection.months)

    if mode == "range":
        if not cfg.selection.range:
            raise ValueError("selection.mode=range erfordert selection.range")
        return months_between_inclusive(cfg.selection.range.start_month, cfg.selection.range.end_month)

    if mode == "years":
        if not cfg.selection.years:
            raise ValueError("selection.mode=years erfordert selection.years")
        return months_for_years(cfg.selection.years)

    if mode == "discover":
        dd = cfg.selection.discover or SelectionDiscover("*")
        dataset_root = (storage_root / cfg.file_storage.dataset_root).resolve()
        if not dataset_root.exists():
            return []
        months: List[str] = []
        for p in sorted(dataset_root.glob(dd.month_glob)):
            if p.is_dir() and _MONTH_RE.match(p.name):
                months.append(p.name)
        return months

    raise ValueError(f"Unbekannter selection.mode: {cfg.selection.mode}")


def _auto_layout_for_month(cfg: PipelineConfig, month_key: str) -> Tuple[bool, bool]:
    """
    Gibt (old_enabled, new_enabled) VOR Anwendung der globalen Gates zurück.
    """
    mk = _must_month_key(month_key)
    year = int(mk.split("_", 1)[0])
    a = cfg.sources.auto_layout

    # Basis nach Jahr
    if year in a.old_only_years:
        old, new = True, False
    elif year in a.both_years:
        old, new = True, True
    elif year in a.new_only_years:
        old, new = False, True
    else:
        pol = a.unknown_year_policy
        if pol == "old":
            old, new = True, False
        elif pol == "new":
            old, new = False, True
        elif pol == "none":
            old, new = False, False
        else:  # "both" oder unbekannt
            old, new = True, True

    # Pro-Monat-Überschreibungen
    ov = a.overrides.get(mk)
    if ov:
        if "old" in ov:
            old = bool(ov["old"])
        if "new" in ov:
            new = bool(ov["new"])

    return old, new


def _enabled_for_month(cfg: PipelineConfig, month_key: str) -> Tuple[bool, bool]:
    """
    Gibt (old_enabled, new_enabled) NACH Anwendung der globalen Gates zurück.
    """
    mode = cfg.sources.auto_layout.layout_mode.lower().strip()
    if mode != "auto":
        old, new = cfg.sources.old.enabled, cfg.sources.new.enabled
    else:
        old, new = _auto_layout_for_month(cfg, month_key)

        # Globale Gates weiterhin anwenden (erlaubt Nutzern, old oder new komplett zu deaktivieren)
        old = old and cfg.sources.old.enabled
        new = new and cfg.sources.new.enabled

    return old, new


def build_month_plan(
    cfg: PipelineConfig,
    *,
    project_root: Path,
    storage_root: Path,
    month_key: str,
) -> MonthPlan:
    old_enabled, new_enabled = _enabled_for_month(cfg, month_key)

    # NEW tgz
    new_abs: Optional[Path] = None
    new_source_url: Optional[str] = None
    if new_enabled:
        rel = _fmt(cfg.sources.new.tgz_relpath, dataset_root=cfg.file_storage.dataset_root, month_key=month_key)
        new_abs = (storage_root / rel).resolve()
        new_source_url = safe_relpath(new_abs, project_root)
        if cfg.file_storage.auto_create:
            new_abs.parent.mkdir(parents=True, exist_ok=True)

    # OLD-Globs (als Paths mit Glob-Pattern)
    old_det_abs: Optional[Path] = None
    old_mq_abs: Optional[Path] = None
    if old_enabled:
        det_rel = _fmt(cfg.sources.old.detectors_glob_relpath, dataset_root=cfg.file_storage.dataset_root, month_key=month_key)
        mq_rel = _fmt(cfg.sources.old.cross_sections_glob_relpath, dataset_root=cfg.file_storage.dataset_root, month_key=month_key)
        old_det_abs = (storage_root / det_rel).resolve()
        old_mq_abs = (storage_root / mq_rel).resolve()

        if cfg.file_storage.auto_create:
            (storage_root / cfg.file_storage.dataset_root / month_key).mkdir(parents=True, exist_ok=True)

    return MonthPlan(
        month_key=month_key,
        new_enabled=new_enabled,
        new_tgz_abs=new_abs,
        new_source_url=new_source_url,
        old_enabled=old_enabled,
        old_detectors_glob_abs=old_det_abs,
        old_cross_sections_glob_abs=old_mq_abs,
    )


def build_plan(cfg: PipelineConfig, *, project_root: Optional[Path] = None) -> Tuple[Path, Path, List[MonthPlan]]:
    project_root = (project_root or Path.cwd()).resolve()
    storage_root = cfg.file_storage.resolve_root(project_root)
    month_keys = resolve_month_keys(cfg, storage_root=storage_root)

    plans = [
        build_month_plan(cfg, project_root=project_root, storage_root=storage_root, month_key=mk)
        for mk in month_keys
    ]
    return project_root, storage_root, plans
