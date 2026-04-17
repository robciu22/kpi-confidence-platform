
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import tempfile
import subprocess
import sys
import os

from pathlib import Path
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# Konfiguration
# ---------------------------------------------------------------------------

# Projektverzeichnis: main_Fast_API.py liegt im Project-Root
PROJECT_ROOT = Path(__file__).resolve().parent

# Standard-Pipeline-Config (kann pro Request überschrieben werden)
DEFAULT_CONFIG = "config/pipeline_ingestion_e2e_selected_months_policy.yaml"

# Guardrail-Skip Exit-Code (nicht fatal, Pipeline läuft weiter)
_RC_GUARDRAIL_SKIP = 42

app = FastAPI(title="KPI Confidence Ingest Service")


# ---------------------------------------------------------------------------
# Modelle
# ---------------------------------------------------------------------------

class Target(BaseModel):
    month_key: str
    dataset_version: str
    source_type: str
    url: str

class IngestRequest(BaseModel):
    run_id: str
    triggered_by: str = "n8n"
    targets: List[Target]

class PipelineRequest(BaseModel):
    """Wird von n8n Workflow 2 gesendet, nachdem Workflow 1 neue Daten ingested hat.

    n8n liest die relevanten month_keys selbstständig aus ingestion.file_history
    (decision = 'ingested') und übergibt sie hier.
    """
    month_keys: List[str]
    steps: List[str] = ["stage", "engine", "ml"]
    config: Optional[str] = None  # None → DEFAULT_CONFIG


# ---------------------------------------------------------------------------
# Endpunkt 1: /ingest  (n8n Workflow 1 – Ingestion & Manifest)
# ---------------------------------------------------------------------------

@app.post("/ingest")
def ingest(req: IngestRequest):
    """Lädt Quelldateien herunter, prüft SHA256 und schreibt in raw.traffic_rows.

    Ablauf intern (ingest_raw_with_manifest.py):
      HEAD request → ETag-Vergleich vs. file_manifest
      Download (Azure Blob) → SHA256-Verifikation
      → raw.traffic_rows, file_manifest, file_history
    """
    # 1) Temporäre YAML-Datei für ingest_raw_with_manifest.py erstellen
    fd, yaml_path = tempfile.mkstemp(suffix=".yaml")
    os.close(fd)

    try:
        yaml_lines = [f'triggered_by: "{req.triggered_by}"', "targets:"]
        for t in req.targets:
            yaml_lines.append(f'  - month_key: "{t.month_key}"')
            yaml_lines.append(f'    dataset_version: "{t.dataset_version}"')
            yaml_lines.append(f'    source_type: "{t.source_type}"')
            yaml_lines.append(f'    url: "{t.url}"')

        with open(yaml_path, "w", encoding="utf-8") as f:
            f.write("\n".join(yaml_lines) + "\n")

        script_path = (PROJECT_ROOT / "scripts" / "ingest_raw_with_manifest.py").resolve()
        if not script_path.exists():
            raise HTTPException(
                status_code=500,
                detail={"error": "Ingestion-Skript nicht gefunden", "path": str(script_path)},
            )

        # 2) Ingestion-Script ausführen
        proc = subprocess.run(
            [sys.executable, str(script_path), "--config", yaml_path,
             "--triggered-by", req.triggered_by],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
            env=os.environ.copy(),
        )

        if proc.returncode != 0:
            raise HTTPException(
                status_code=500,
                detail={"stderr": proc.stderr[-2000:], "stdout": proc.stdout[-2000:]},
            )

        return {"status": "success", "run_id": req.run_id, "stdout": proc.stdout[-2000:]}

    finally:
        try:
            os.remove(yaml_path)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Endpunkt 2: /run-pipeline  (n8n Workflow 2 – Stage → Engine → ML)
# ---------------------------------------------------------------------------

@app.post("/run-pipeline")
def run_pipeline(req: PipelineRequest):
    """Führt Stage-Load, Phase-B-Engine und ML-Scoring für die angegebenen Monate aus.

    Wird von n8n Workflow 2 aufgerufen, nachdem Workflow 1 neue Daten ingested hat.
    n8n liest die month_keys selbst aus ingestion.file_history (decision='ingested').

    Fehlerverhalten: Ein fehlgeschlagener Monat stoppt nicht den Batch –
    der Fehler wird im Ergebnis-Array protokolliert (continue-on-error).
    Guardrail-Skip (rc=42) gilt als nicht-fatal.
    """
    config_path = (
        PROJECT_ROOT / (req.config or DEFAULT_CONFIG)
    ).resolve()

    if not config_path.exists():
        raise HTTPException(
            status_code=500,
            detail={"error": "Pipeline-Config nicht gefunden", "path": str(config_path)},
        )

    pipeline_script = (PROJECT_ROOT / "scripts" / "run_batch_pipeline_v1_2.py").resolve()
    ml_script       = (PROJECT_ROOT / "scripts" / "ml" / "ml_anomaly_score_hourly_stage_a_v1_1.py").resolve()

    results = []
    overall_ok = True

    for month_key in req.month_keys:
        month_result: dict = {"month_key": month_key, "pipeline": None, "ml": None}

        # ---- Stage + Engine ------------------------------------------------
        run_pipeline_step = "stage" in req.steps or "engine" in req.steps
        if run_pipeline_step:
            steps_arg = ",".join(s for s in ["stage", "engine"] if s in req.steps)
            proc = subprocess.run(
                [
                    sys.executable, str(pipeline_script),
                    "--config",              str(config_path),
                    "--month-key",           month_key,
                    "--steps",               steps_arg,
                    "--replace-month-slice",
                ],
                capture_output=True,
                text=True,
                cwd=str(PROJECT_ROOT),
                env=os.environ.copy(),
            )
            fatal = proc.returncode not in (0, _RC_GUARDRAIL_SKIP)
            month_result["pipeline"] = {
                "rc":     proc.returncode,
                "status": "skipped_guardrail" if proc.returncode == _RC_GUARDRAIL_SKIP
                          else ("ok" if proc.returncode == 0 else "error"),
                "stdout": proc.stdout[-1000:],
                "stderr": proc.stderr[-500:] if fatal else "",
            }
            if fatal:
                overall_ok = False
                results.append(month_result)
                continue  # ML für diesen Monat überspringen

            # Guardrail-Skip → ML ebenfalls überspringen
            if proc.returncode == _RC_GUARDRAIL_SKIP:
                results.append(month_result)
                continue

        # ---- ML-Scoring ----------------------------------------------------
        if "ml" in req.steps:
            proc_ml = subprocess.run(
                [
                    sys.executable, str(ml_script),
                    "--config",              str(config_path),
                    "--month-key",           month_key,
                    "--model-name",          "robust_zscore_mad_v1_0",
                    "--min-rows",            "5000",
                    "--replace-month-slice",
                ],
                capture_output=True,
                text=True,
                cwd=str(PROJECT_ROOT),
                env=os.environ.copy(),
            )
            ml_ok = proc_ml.returncode == 0
            month_result["ml"] = {
                "rc":     proc_ml.returncode,
                "status": "ok" if ml_ok else "error",
                "stdout": proc_ml.stdout[-1000:],
                "stderr": proc_ml.stderr[-500:] if not ml_ok else "",
            }
            if not ml_ok:
                overall_ok = False

        results.append(month_result)

    return {
        "status":     "completed" if overall_ok else "completed_with_errors",
        "month_keys": req.month_keys,
        "results":    results,
    }
