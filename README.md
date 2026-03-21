# FDA Pharma Supply Chain Analytics Platform

> End-to-end data engineering pipeline ingesting FDA drug shortage, recall, and approval data daily — transforming it into actionable supply chain intelligence via a medallion architecture.

## 🚀 Live Demo

**Dashboard:** [FDA Pharma Supply Chain Intelligence](https://fda-supply-chain-analytics-ywfuutnsqcjdjpn5yrmwqc.streamlit.app/)

> Pipeline runs automatically every day at 6am UTC via GitHub Actions.

---

## Problem Statement

Hospital procurement managers and supply chain analysts lack a unified view of:
- Which drugs are currently in shortage
- Which manufacturers are reliable alternatives
- Which manufacturers have high recall risk

This platform solves that by pulling directly from FDA APIs daily, transforming raw data into risk scores and alternative drug recommendations.

---

## Architecture

```
FDA APIs (3 endpoints)
    ↓
Python Ingestion (concurrent.futures + Pydantic)
    ↓
Bronze Layer — Supabase PostgreSQL (raw, as-is)
    ↓
dbt Staging Models — Silver Layer (cleaned, normalized, typed)
    ↓
dbt Mart Models — Gold Layer (risk scores, alternatives, manufacturer reliability)
    ↓
Streamlit Dashboard (interactive, public URL)
    ↓
GitHub Actions (daily schedule, 6am UTC)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python, Requests, concurrent.futures |
| Validation | Pydantic |
| Storage | Supabase (cloud PostgreSQL) |
| Transformation | dbt (dbt-postgres 1.10.0) |
| Orchestration | GitHub Actions (daily) + Apache Airflow (Docker) |
| Dashboard | Streamlit |
| Data Quality | dbt tests (15 passing) |
| Version Control | Git + GitHub |
| Package Manager | uv |

---

## Data Sources

All data pulled from the [FDA openFDA API](https://open.fda.gov/apis/):

| Endpoint | Records Ingested | Bronze Table |
|---|---|---|
| `/drug/shortages.json` | ~1,664 | `bronze.raw_fda_shortages` |
| `/drug/enforcement.json` | ~17,521 | `bronze.raw_fda_recalls` |
| `/drug/drugsfda.json` | 25,100 | `bronze.raw_fda_approvals` |

---

## Project Structure

```
pharma_supply_chain/
├── .env                          ← API keys, DB credentials (not committed)
├── .github/workflows/
│   └── fda_pipeline.yml          ← Daily GitHub Actions pipeline
├── ingestion/
│   ├── fda_shortages.py          ← Bronze ingestion: shortages
│   ├── fda_recalls.py            ← Bronze ingestion: recalls
│   └── fda_approvals.py          ← Bronze ingestion: approvals
├── FDA_Pipeline/                 ← dbt project
│   ├── dbt_project.yml
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── models/
│       ├── staging/              ← Silver layer (9 models)
│       │   ├── sources.yml
│       │   ├── schema.yml        ← 15 dbt data quality tests
│       │   └── stg_*.sql
│       └── gold/                 ← Gold layer (3 models)
│           ├── mart_alternatives.sql
│           ├── mart_manufacturer_risk.sql
│           └── mart_shortage_risk.sql
├── airflow/                      ← Local Airflow Docker setup
│   ├── docker-compose.yaml
│   └── dags/
│       └── fda_pipeline_dag.py
├── dashboard/
│   └── app.py                    ← Streamlit dashboard
└── notebooks/
```

---

## Data Models

### Silver Layer — 9 Models

| Model | Rows | Description |
|---|---|---|
| `stg_shortages` | ~1,664 | Cleaned shortage records, dates typed, availability typos fixed |
| `stg_shortage_manufacturers` | ~1,539 | Exploded manufacturer names per shortage |
| `stg_shortage_substances` | ~2,113 | Exploded substance names per shortage |
| `stg_recalls` | ~17,521 | Cleaned recall records, YYYYMMDD dates converted |
| `stg_recall_manufacturers` | ~3,099 | Exploded manufacturer names per recall |
| `stg_recall_substances` | ~3,852 | Exploded substance names per recall |
| `stg_approvals` | ~10,660 | FDA approved drugs (filtered nulls) |
| `stg_approval_manufacturers` | ~16,406 | Exploded manufacturer names per approval |
| `stg_approval_substances` | ~11,876 | Exploded substance names per approval |

### Gold Layer — 3 Models

| Model | Rows | Description |
|---|---|---|
| `mart_alternatives` | ~9,527 | FDA approved manufacturers NOT in shortage, matched by substance name |
| `mart_manufacturer_risk` | ~1,666 | Risk scores combining shortage count + recall history by severity |
| `mart_shortage_risk` | ~1,138 | Composite drug-level risk score with risk level labels |

### Manufacturer Risk Score Formula

```
risk_score =
    (active_shortage_count × 3) +
    (Class I recalls × 5)       +
    (Class II recalls × 2)      +
    (Class III recalls × 1)     +
    (ongoing recalls × 3)
```

### Shortage Risk Score Formula

```
shortage_risk_score =
    (shortage_days > 365 ? 3 : 1) +
    (manufacturer_risk: High=3, Medium=2, Low=1) +
    (alternatives=0 ? 3 : alternatives<3 ? 2 : 1)

Critical = 9, High = 7-8, Medium = 5-6, Low < 5
```

---

## Dashboard Pages

**Page 1 — Drug Shortage Risk**
- 1,138 current shortages ranked by composite risk score
- Filter by risk level and search by drug name

**Page 2 — Alternatives Finder**
- Find FDA approved manufacturers not currently in shortage
- Search by drug name or substance name

**Page 3 — Manufacturer Risk Scorecard**
- 1,666 manufacturers ranked by combined shortage and recall risk
- Breakdown by recall classification and ongoing recalls

---

## Key Engineering Decisions

**Normalization in Silver:** Manufacturer and substance names exploded into child tables for many-to-many matching across shortage, recall, and approval datasets.

**Custom dbt Schema Macro:** Overrides default dbt behavior that appends schemas (e.g. `silver_gold`). Ensures Gold models land in `gold` schema exactly.

**Concurrent API Fetching:** `concurrent.futures.ThreadPoolExecutor` fetches paginated FDA results in parallel across all 3 ingestion scripts.

**URL Encoding:** Supabase password contains special characters. Used `urllib.parse.quote_plus()` to encode password in SQLAlchemy connection string.

**Two Orchestration Layers:** Apache Airflow (Docker) for local development. GitHub Actions for production — no always-on infrastructure required.

**Known Limitation — Manufacturer Deduplication:** FDA data has inconsistent naming (e.g. `Fresenius Kabi USA, LLC` vs `fresenius kabi usa llc`). Current pipeline uses `LOWER(TRIM(REPLACE(...)))`. Production would use `pg_trgm` or `rapidfuzz`.

---

## Setup & Running Locally

### Prerequisites
- Python 3.12+
- PostgreSQL with `bronze`, `silver`, `gold` schemas
- uv package manager

### Environment Variables (`.env`)
```
FDA_BASE_URL=https://api.fda.gov/drug
FDA_API_KEY=your_api_key_here
DB_HOST=localhost
DB_PORT=5432
DB_NAME=pharma_db
DB_USER=postgres
DB_PASSWORD=your_password
```

### Run Ingestion
```bash
set UV_LINK_MODE=copy
set UV_CACHE_DIR=C:\uvcache
.venv\Scripts\activate

python ingestion/fda_shortages.py
python ingestion/fda_recalls.py
python ingestion/fda_approvals.py
```

### Run dbt
```bash
cd FDA_Pipeline
dbt debug
dbt run
dbt test
dbt docs generate && dbt docs serve
```

### Run Dashboard
```bash
streamlit run dashboard/app.py
```

---

## Automated Pipeline

Runs every day at 6am UTC via GitHub Actions:

```
Install dependencies
    ↓
Run 3 ingestion scripts → Supabase Bronze
    ↓
dbt run → rebuild Silver + Gold (12 models)
    ↓
dbt test → validate 15 data quality checks
```

---

## Current Status

```
✅ Bronze ingestion          3 scripts, ~44,000 records daily
✅ Silver layer              9 dbt staging models
✅ Gold layer                3 mart models
✅ dbt tests                 15 passing
✅ Airflow DAG               Docker, local orchestration
✅ Streamlit dashboard       3 pages, live public URL
✅ Supabase                  cloud PostgreSQL
✅ GitHub Actions            daily automated pipeline at 6am UTC
✅ Streamlit Cloud           public deployment
```

---

## Author

**Ali Khan** — S&OP Planner / Data Engineer  
MS Data Science, Indiana University Bloomington  
[GitHub](https://github.com/ali-khan9991) | [Live Dashboard](https://fda-supply-chain-analytics-ywfuutnsqcjdjpn5yrmwqc.streamlit.app/)