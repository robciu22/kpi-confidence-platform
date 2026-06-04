#!/usr/bin/env python3
"""run_azure_ml_all.py

Führt alle 8 MLflow-Experimente (Z-Score + Isolation Forest, 4 Monate)
direkt in Azure ML aus. Läuft im selben Prozess wie die Authentifizierung,
daher kein Subprocess-Auth-Problem.

Verwendung:
    python backend/scripts/ml/run_azure_ml_all.py

Azure ML Studio: https://ml.azure.com → robert-ml-workspace → Aufträge
"""

import sys
import os
from pathlib import Path

# Projektverzeichnis in Python-Pfad einfügen
project_root = Path(__file__).resolve().parents[3]
os.chdir(project_root)
sys.path.insert(0, str(project_root))

from azure.ai.ml import MLClient
from azure.identity import InteractiveBrowserCredential
import mlflow

# ── Konfiguration ────────────────────────────────────────────────────────────
SUBSCRIPTION_ID = "97ca38db-a1ae-4c66-ae90-f3a7891c8194"
RESOURCE_GROUP  = "rg-mlflow"
WORKSPACE_NAME  = "robert-ml-workspace"
TENANT_ID       = "207d1b83-d438-4e06-9aca-e9117a06624a"

CONFIG_FILE     = "config/pipeline_ingestion_e2e_selected_months_policy.yaml"

RUNS = [
    ("2025_05", "robust_zscore_mad_v1_0", {}),
    ("2024_04", "robust_zscore_mad_v1_0", {}),
    ("2023_03", "robust_zscore_mad_v1_0", {}),
    ("2022_06", "robust_zscore_mad_v1_0", {}),
    ("2025_05", "isolation_forest",        {"contamination": 0.1}),
    ("2024_04", "isolation_forest",        {"contamination": 0.1}),
    ("2023_03", "isolation_forest",        {"contamination": 0.1}),
    ("2022_06", "isolation_forest",        {"contamination": 0.1}),
]
# ─────────────────────────────────────────────────────────────────────────────


def get_tracking_uri() -> str:
    print("Authentifizierung gegen Azure ML (Browser öffnet sich)...")
    credential = InteractiveBrowserCredential(tenant_id=TENANT_ID)
    ml_client  = MLClient(
        credential=credential,
        subscription_id=SUBSCRIPTION_ID,
        resource_group_name=RESOURCE_GROUP,
        workspace_name=WORKSPACE_NAME,
    )
    uri = ml_client.workspaces.get(WORKSPACE_NAME).mlflow_tracking_uri
    print(f"Azure ML Tracking URI gesetzt.\n")
    return uri


if __name__ == "__main__":
    tracking_uri = get_tracking_uri()

    # Tracking-URI im aktuellen Prozess setzen — alle folgenden mlflow-Calls
    # gehen direkt zu Azure ML
    os.environ["MLFLOW_TRACKING_URI"] = tracking_uri
    mlflow.set_tracking_uri(tracking_uri)

    # Jetzt die Scoring-Funktion direkt importieren und aufrufen
    from backend.scripts.ml.ml_anomaly_score_hourly_stage_a_v1_1 import main as score_main

    print(f"Starte {len(RUNS)} Runs in Azure ML...\n")
    success = 0

    for month, model, extra in RUNS:
        prefix = "iforest" if "isolation" in model else "zscore"
        label  = f"{prefix}_{month}"
        print(f"▶ {label} ...", end=" ", flush=True)

        # sys.argv temporär setzen (scoring script liest via argparse)
        argv_extra = []
        if extra.get("contamination"):
            argv_extra = ["--contamination", str(extra["contamination"])]

        sys.argv = [
            "ml_anomaly_score_hourly_stage_a_v1_1.py",
            "--config",          CONFIG_FILE,
            "--month-key",       month,
            "--model-name",      model,
            "--replace-month-slice",
        ] + argv_extra

        try:
            ret = score_main()
            if ret == 0:
                print("✓")
                success += 1
            else:
                print(f"✗ (exit {ret})")
        except SystemExit as e:
            print(f"✗ (exit {e.code})")
        except Exception as e:
            print(f"✗ FEHLER: {e}")

    print(f"\nFertig: {success}/{len(RUNS)} erfolgreich.")
    print("Azure ML Studio: https://ml.azure.com → Aufträge → kpi-anomaly-detection")
