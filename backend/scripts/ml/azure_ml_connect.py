#!/usr/bin/env python3
"""azure_ml_connect.py

Verbindet das lokale MLflow-Experiment 'kpi-anomaly-detection' mit
Azure Machine Learning. Öffnet einmalig den Browser zur Authentifizierung,
danach werden alle MLflow-Runs in Azure ML Studio sichtbar.

Verwendung:
    python backend/scripts/ml/azure_ml_connect.py

Nach erfolgreicher Verbindung: https://ml.azure.com → robert-ml-workspace
→ Experiments → kpi-anomaly-detection
"""

import mlflow
from azure.ai.ml import MLClient
from azure.identity import InteractiveBrowserCredential

# ── Azure ML Workspace-Konfiguration ────────────────────────────────────────
SUBSCRIPTION_ID    = "97ca38db-a1ae-4c66-ae90-f3a7891c8194"
RESOURCE_GROUP     = "rg-mlflow"
WORKSPACE_NAME     = "robert-ml-workspace"
EXPERIMENT_NAME    = "kpi-anomaly-detection"
TENANT_ID          = "207d1b83-d438-4e06-9aca-e9117a06624a"
# ─────────────────────────────────────────────────────────────────────────────


def get_azure_ml_tracking_uri() -> str:
    """Authentifiziert via Browser und gibt den Azure-ML-MLflow-Tracking-URI zurück."""
    print("Öffne Browser zur Azure-Authentifizierung...")
    credential = InteractiveBrowserCredential(tenant_id=TENANT_ID)

    ml_client = MLClient(
        credential=credential,
        subscription_id=SUBSCRIPTION_ID,
        resource_group_name=RESOURCE_GROUP,
        workspace_name=WORKSPACE_NAME,
    )

    workspace = ml_client.workspaces.get(WORKSPACE_NAME)
    tracking_uri = workspace.mlflow_tracking_uri
    print(f"Azure ML Tracking URI: {tracking_uri}")
    return tracking_uri


def run_test_experiment(tracking_uri: str) -> None:
    """Schreibt einen Test-Run in Azure ML zur Verifikation der Verbindung."""
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(EXPERIMENT_NAME)

    with mlflow.start_run(run_name="azure_connection_test"):
        mlflow.log_param("source", "azure_ml_connect.py")
        mlflow.log_param("workspace", WORKSPACE_NAME)
        mlflow.log_metric("connection_status", 1.0)
        print("Test-Run erfolgreich in Azure ML geloggt.")
        print(f"Azure ML Studio: https://ml.azure.com")
        print(f"Workspace: {WORKSPACE_NAME} → Experiments → {EXPERIMENT_NAME}")


if __name__ == "__main__":
    tracking_uri = get_azure_ml_tracking_uri()
    run_test_experiment(tracking_uri)
