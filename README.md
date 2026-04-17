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
| ML / Anomalieerkennung | Python, MAD (Median Absolute Deviation) |
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

## Team

Abschlussprojekt am Data Science Institute (DSI) — entwickelt von einem 3-köpfigen Team.

| Rolle | Schwerpunkt |
|---|---|
| Robert Legatzki | Backend-Pipeline, FastAPI, ML-Anomalieerkennung, PostgreSQL-Schema, Systemarchitektur, Explorative Datenanalyse |
| Dorna Poursoheil | Streamlit-Dashboard, n8n-Orchestrierung |
| Christian Jessen | Power BI Dashboard |
