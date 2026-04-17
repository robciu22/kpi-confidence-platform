from __future__ import annotations

import numpy as np
import pandas as pd


def make_demo_kpi_hourly(days: int = 14, n_detectors: int = 30, seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    hours = pd.date_range(end=pd.Timestamp.utcnow().floor("H"), periods=days * 24, freq="H", tz="UTC")

    kpis = [
        ("flow", "kfz", "hourly"),
        ("flow", "pkw", "hourly"),
        ("flow", "lkw", "hourly"),
        ("speed", "kfz", "hourly"),
        ("speed", "pkw", "hourly"),
        ("speed", "lkw", "hourly"),
    ]

    det_ids = np.arange(1, n_detectors + 1)
    rows = []
    for kpi_family, vehicle_class, grain in kpis:
        for det in det_ids:
            base = rng.normal(1200, 200) if kpi_family == "flow" else rng.normal(45, 6)
            seasonal = np.sin(np.linspace(0, 5 * np.pi, len(hours))) * (120 if kpi_family == "flow" else 4)
            noise = rng.normal(0, 80 if kpi_family == "flow" else 2.5, len(hours))
            value = np.maximum(0, base + seasonal + noise)

            # Confidence mechanics
            anomaly = np.clip(rng.normal(0.15, 0.12, len(hours)), 0, 1)
            drift = np.clip(rng.normal(0.10, 0.10, len(hours)) + np.linspace(0, 0.15, len(hours)), 0, 1)
            null = np.clip(rng.normal(0.08, 0.08, len(hours)), 0, 1)
            fresh = np.clip(rng.normal(0.07, 0.06, len(hours)), 0, 1)
            volume = np.clip(rng.normal(0.06, 0.05, len(hours)), 0, 1)

            conf = np.clip(1 - (0.35 * anomaly + 0.25 * drift + 0.15 * null + 0.15 * fresh + 0.10 * volume), 0, 1)
            label = np.where(conf >= 0.85, "high", np.where(conf >= 0.70, "medium", "low"))

            run_id = "00000000-0000-0000-0000-000000000000"

            for i, ts in enumerate(hours):
                rows.append(
                    {
                        "kpi_id": 0,
                        "kpi_name": f"{kpi_family}_{vehicle_class}_{grain}",
                        "entity_type": "detector",
                        "entity_id": int(det),
                        "ts_utc": ts,
                        "d_utc": ts.date(),
                        "year_utc": ts.year,
                        "month_utc": ts.month,
                        "day_utc": ts.day,
                        "hour_utc": ts.hour,
                        "value": float(value[i]),
                        "confidence_score": float(conf[i]),
                        "confidence_label": str(label[i]),
                        "freshness_score": float(1 - fresh[i]),
                        "volume_score": float(1 - volume[i]),
                        "null_score": float(1 - null[i]),
                        "anomaly_score": float(anomaly[i]),
                        "drift_score": float(drift[i]),
                        "value_run_id": run_id,
                        "confidence_run_id": run_id,
                        "kpi_family": kpi_family,
                        "vehicle_class": vehicle_class,
                        "grain": grain,
                    }
                )

    df = pd.DataFrame(rows)

    # Add a global aggregate
    g = (
        df.groupby(["kpi_name", "ts_utc", "d_utc", "year_utc", "month_utc", "day_utc", "hour_utc", "kpi_family", "vehicle_class", "grain"], as_index=False)
        .agg(
            value=("value", "sum"),
            confidence_score=("confidence_score", "mean"),
            anomaly_score=("anomaly_score", "mean"),
            drift_score=("drift_score", "mean"),
            null_score=("null_score", "mean"),
            freshness_score=("freshness_score", "mean"),
            volume_score=("volume_score", "mean"),
        )
    )
    g["entity_type"] = "global"
    g["entity_id"] = 0
    g["confidence_label"] = np.where(g["confidence_score"] >= 0.85, "high", np.where(g["confidence_score"] >= 0.70, "medium", "low"))
    g["value_run_id"] = "00000000-0000-0000-0000-000000000000"
    g["confidence_run_id"] = "00000000-0000-0000-0000-000000000000"

    # Align columns
    for col in df.columns:
        if col not in g.columns:
            g[col] = np.nan
    g = g[df.columns]

    return pd.concat([df, g], ignore_index=True)


def make_demo_file_history(n_runs: int = 10, seed: int = 5) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    run_ids = [f"11111111-1111-1111-1111-{i:012d}" for i in range(n_runs)]
    decisions = ["ingested", "skip", "failed"]
    rows = []
    for run in run_ids:
        for j in range(rng.integers(8, 20)):
            decision = rng.choice(decisions, p=[0.6, 0.3, 0.1])
            rows.append(
                {
                    "run_id": run,
                    "source_url": f"https://example.com/file_{j}.csv.gz",
                    "month_key": "2024_04",
                    "dataset_version": rng.choice(["old", "new"]),
                    "source_type": rng.choice(["detectors", "cross_sections"]),
                    "decision": decision,
                    "reason": None if decision != "failed" else "connection/reset",
                    "rows_loaded": int(rng.integers(10_000, 200_000)) if decision == "ingested" else 0,
                    "duration_ms": int(rng.integers(200, 5000)),
                    "error_message": None if decision != "failed" else "timeout",
                    "created_at": pd.Timestamp.utcnow() - pd.Timedelta(hours=int(rng.integers(1, 240))),
                }
            )
    return pd.DataFrame(rows)
