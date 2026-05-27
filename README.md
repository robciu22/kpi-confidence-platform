# KPI Confidence Platform

**"Can we trust this KPI?"** — Ein Data Engineering Projekt zur automatisierten Bewertung der Zuverlässigkeit von Verkehrs-KPIs auf Basis Berliner Detektordaten.

Entwickelt als Abschlussprojekt am Data Science Institute (DSI) — 3-köpfiges Team.  
**Live Demo:** [kpi-confidence.streamlit.app](https://kpi-confidence.streamlit.app)  
Dornas Repository (Streamlit Demo live): [github.com/DornaPoursoheil/kpi-confidence-platform](https://github.com/DornaPoursoheil/kpi-confidence-platform)

---

## Problemstellung

Rohe Verkehrsdaten aus Berliner Induktionsschleifen enthalten Ausfälle, Messfehler und Anomalien.
Klassische Dashboards zeigen KPIs — aber nicht ob diesen KPIs zu vertrauen ist.

Dieses System berechnet automatisch einen **Confidence Score (0–1)** für jeden KPI:
- 🟢 **≥ 0.8** — hohe Verlässlichkeit
- 🟡 **0.5–0.8** — eingeschränkt verlässlich
- 🔴 **< 0.5** — fragwürdig, Anomalie erkannt

---

## Tech Stack

| Schicht | Technologie |
|---|---|
| Daten-Ingestion | Python, FastAPI, SHA-256-Verifikation |
| Orchestrierung | n8n (Workflow-Automatisierung) |
| Datenbank | PostgreSQL 16 (8 Schemas) |
| ML / Anomalieerkennung | Python, MAD (Median Absolute Deviation), **MLflow** |
| Diagnose-Dashboard | Streamlit |
| Business Dashboard | Power BI |
| Monitoring | Systemd Service, Slack-Alerts |

---

## Architektur

```
Rohdaten (TGZ/GZ)
    ↓
Ingest & Manifest (SHA-256, ETag-Erkennung)
    ↓
Staging (PostgreSQL: raw → staging → core)
    ↓
KPI-Engine (phase_b_engine) → Confidence Score
    ↓
ML Anomalie-Scoring (MAD-basiert)
    ↓
BI-Views → Streamlit Dashboard / Power BI
```

---

## Ordnerstruktur

```
kpi-confidence-platform/
├── backend/               FastAPI-Service + Pipeline-Skripte
│   ├── main_fast_api.py   HTTP-Ingest-Endpunkt
│   ├── scripts/           Pipeline-Orchestrierung + ML
│   ├── src/               Config-Parser, Utilities
│   └── config/            YAML-Pipeline-Konfiguration
├── streamlit_app/         Diagnose-Dashboard (standalone)
├── database/
│   ├── schema.sql         Vollständiges PostgreSQL-Schema (8 Schemas)
│   └── eda/               Explorative Analysen, ERD-Modelle
├── n8n_workflows/         Workflow-Definitionen (JSON) + systemd Service
└── docs/                  Architektur-Dokumentation, Präsentation
```

---

## Lokales Setup

### Voraussetzungen
- Python 3.11+
- PostgreSQL 16
- n8n (optional, für Workflow-Automatisierung)

### 1. Repository klonen
```bash
git clone https://github.com/robciu22/kpi-confidence-platform.git
cd kpi-confidence-platform
```

### 2. Datenbank einrichten
```bash
psql -U postgres -c "CREATE DATABASE kpi_cs_partition;"
psql -U postgres -d kpi_cs_partition -f database/schema.sql
```

### 3. Umgebungsvariablen konfigurieren
```bash
cp .env.template .env
# .env mit eigenen Werten befüllen
```

### 4. Python-Abhängigkeiten installieren
```bash
pip install fastapi uvicorn psycopg[binary] python-dotenv pandas numpy
```

### 5. FastAPI-Service starten
```bash
uvicorn backend.main_fast_api:app --reload
```

### 6. Streamlit-Dashboard starten
```bash
cd streamlit_app
pip install -r requirements.txt
streamlit run project.py
```

Das Dashboard läuft automatisch im **Demo-Modus** wenn keine DB-Verbindung vorhanden ist.

---

## MLflow Experiment Tracking

Das ML-Anomalie-Scoring-Modul (`backend/scripts/ml/ml_anomaly_score_hourly_stage_a_v1_1.py`) ist mit **MLflow** integriert, um Runs systematisch zu vergleichen und zu reproduzieren.

### Getrackter Experiment: `kpi-anomaly-detection`

Zwei Modelle werden verglichen — beide auf denselben 4 Monaten Berliner Detektordaten:

| Modell | `--model-name` | Ansatz |
|---|---|---|
| Robust Z-Score / MAD | `robust_zscore_mad_v1_0` (Default) | Statistisch: lokale Ausreißer pro Detektor-Stunde |
| Isolation Forest | `isolation_forest` | Baumbasiert: globale Verteilungsstruktur |

**Parameter (pro Run):** `month_key`, `model_name`, `z_threshold`, `threshold_mode`, `lookback_days`, `contamination` (IF)  
**Metriken (pro Run):** `anomaly_count`, `anomaly_rate`, `rows_scored`, `rows_inserted`, `threshold_used`  
**Tags:** `data_quality` (`ok` / `known_naming_error`), `stage`, `script`

### Modellvergleich — 4 Monate × 2 Modelle

| Monat | Z-Score Rate | Isolation Forest Rate | data_quality | Befund |
|---|---|---|---|---|
| Mai 2025 | 11,1 % | 7,9 % | ok | Beide moderat, IF konservativer |
| Apr 2024 | 14,7 % | **0,0 %** | ok | **Divergenz** — systematische vs. lokale Anomalien |
| Mrz 2023 | 10,9 % | 2,0 % | ok | IF deutlich konservativer |
| Jun 2022 | **0,0 %** | **0,0 %** | known_naming_error | **Beide einig** — struktureller Datenfehler bestätigt |

**Kernerkenntnisse aus dem Vergleich:**

- **2022_06 (0 % / 0 %):** Zwei fundamental verschiedene Algorithmen bestätigen unabhängig denselben Befund — die strukturell fehlerhaften Datenbezeichner erzeugen ein so durchgängiges Muster, dass weder statistisches noch baumbasiertes Scoring Ausreißer erkennt. Validation in Aktion.

- **2024_04 (14,7 % vs. 0 %):** Die größte Diskrepanz zeigt den methodischen Unterschied: Der Z-Score erkennt *lokale* Ausreißer innerhalb jeder Detektor-Stunden-Gruppe. Isolation Forest bewertet die *globale Verteilung* — wenn alle Detektoren eines Monats gleichförmig degradierte Werte liefern, erscheint das dem IF als "normal". Dieser Befund motiviert den Multi-Modell-Ansatz.

### MLflow UI lokal starten

```bash
pip install mlflow
mlflow ui --port 5000
# → http://127.0.0.1:5000  →  "Model training"  →  kpi-anomaly-detection
```

### Run ausführen

```bash
# Z-Score (Default)
python backend/scripts/ml/ml_anomaly_score_hourly_stage_a_v1_1.py \
  --config config/pipeline_ingestion_e2e_selected_months_policy.yaml \
  --month-key 2025_05 \
  --replace-month-slice

# Isolation Forest
python backend/scripts/ml/ml_anomaly_score_hourly_stage_a_v1_1.py \
  --config config/pipeline_ingestion_e2e_selected_months_policy.yaml \
  --month-key 2025_05 \
  --model-name isolation_forest \
  --contamination 0.1 \
  --replace-month-slice
```

---

## Team

Abschlussprojekt am Data Science Institute (DSI) — entwickelt von einem 3-köpfigen Team.

| Rolle | Schwerpunkt |
|---|---|
| Robert Legatzki | Backend-Pipeline, FastAPI, ML-Anomalieerkennung, PostgreSQL-Schema, Systemarchitektur, Explorative Datenanalyse |
| Dorna Poursoheil | Streamlit-Dashboard, n8n-Orchestrierung |
| Christian Jessen | Power BI Dashboard |
