# FDA Pharma Supply Chain Analytics Platform

> End-to-end data engineering pipeline ingesting FDA drug shortage, recall, and approval data — transforming it into actionable supply chain intelligence via a medallion architecture.

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
Bronze Layer — PostgreSQL (raw, as-is)
    ↓
dbt Staging Models — Silver Layer (cleaned, normalized, typed)
    ↓
dbt Mart Models — Gold Layer (risk scores, alternatives, manufacturer reliability)
    ↓
Streamlit Dashboard (interactive, public URL)
    ↓
Airflow DAG (daily schedule)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python, Requests, concurrent.futures |
| Validation | Pydantic |
| Storage | PostgreSQL (local) → Supabase (cloud) |
| Transformation | dbt (dbt-postgres 1.10.0) |
| Orchestration | Apache Airflow → GitHub Actions |
| Dashboard | Streamlit |
| Data Quality | dbt tests + Great Expectations |
| Version Control | Git + GitHub |
| Package Manager | uv |

---

## Data Sources

All data pulled from the [FDA openFDA API](https://open.fda.gov/apis/):

| Endpoint | Records Ingested | Bronze Table |
|---|---|---|
| `/drug/shortages.json` | 1,659 | `bronze.raw_fda_shortages` |
| `/drug/enforcement.json` | 17,416 | `bronze.raw_fda_recalls` |
| `/drug/drugsfda.json` | 25,100 | `bronze.raw_fda_approvals` |

---

## Project Structure

```
pharma_supply_chain/
├── .env                          ← API keys, DB credentials (not committed)
├── ingestion/
│   ├── fda_shortages.py          ← Bronze ingestion: shortages
│   ├── fda_recalls.py            ← Bronze ingestion: recalls
│   └── fda_approvals.py          ← Bronze ingestion: approvals
├── logs/                         ← Daily pipeline logs
├── FDA_Pipeline/                 ← dbt project
│   ├── dbt_project.yml
│   ├── macros/
│   │   └── generate_schema_name.sql  ← Custom schema routing macro
│   └── models/
│       ├── staging/              ← Silver layer (9 models)
│       │   ├── sources.yml
│       │   ├── stg_shortages.sql
│       │   ├── stg_shortage_manufacturers.sql
│       │   ├── stg_shortage_substances.sql
│       │   ├── stg_recalls.sql
│       │   ├── stg_recall_manufacturers.sql
│       │   ├── stg_recall_substances.sql
│       │   ├── stg_approvals.sql
│       │   ├── stg_approval_manufacturers.sql
│       │   └── stg_approval_substances.sql
│       └── gold/                 ← Gold layer (3 models)
│           ├── mart_alternatives.sql
│           ├── mart_manufacturer_risk.sql
│           └── mart_shortage_risk.sql        ← in progress
└── notebooks/
```

---

## Data Models

### Silver Layer — 9 Models

| Model | Rows | Description |
|---|---|---|
| `stg_shortages` | 1,656 | Cleaned shortage records, dates typed, availability typos fixed |
| `stg_shortage_manufacturers` | 1,539 | Exploded manufacturer names per shortage |
| `stg_shortage_substances` | 2,113 | Exploded substance names per shortage |
| `stg_recalls` | 17,416 | Cleaned recall records, YYYYMMDD dates converted |
| `stg_recall_manufacturers` | 3,099 | Exploded manufacturer names per recall |
| `stg_recall_substances` | 3,852 | Exploded substance names per recall |
| `stg_approvals` | 10,660 | FDA approved drugs (filtered nulls) |
| `stg_approval_manufacturers` | 16,406 | Exploded manufacturer names per approval |
| `stg_approval_substances` | 11,876 | Exploded substance names per approval |

### Gold Layer — 3 Models

| Model | Rows | Description |
|---|---|---|
| `mart_alternatives` | 9,527 | FDA approved manufacturers NOT in shortage, matched by substance name |
| `mart_manufacturer_risk` | 1,659 | Risk scores combining shortage count + recall history by severity |
| `mart_shortage_risk` | — | Composite drug-level risk score (in progress) |

#### Manufacturer Risk Score Formula

```
risk_score =
    (active_shortage_count × 3) +
    (Class I recalls × 5)       +   ← most serious
    (Class II recalls × 2)      +
    (Class III recalls × 1)     +
    (ongoing recalls × 3)

risk_level:
    score ≥ 20  → High
    score ≥ 10  → Medium
    score < 10  → Low
```

---

## Key Engineering Decisions

**Normalization in Silver:**
Manufacturer names and substance names are exploded into child tables because alternatives logic requires many-to-many matching across shortage, recall, and approval datasets. Brand names and routes are flattened with `ARRAY_TO_STRING` for display only.

**Custom dbt Schema Macro:**
Default dbt behavior appends custom schema to profile default (e.g. `silver_gold`). A `generate_schema_name` macro override ensures Gold models land in `gold` schema exactly.

**Concurrent API Fetching:**
All three ingestion scripts use `concurrent.futures.ThreadPoolExecutor` to fetch paginated API results in parallel, reducing ingestion time significantly.

**FDA Pagination Limit:**
FDA enforces a hard limit at `skip=25,000` for the drugsfda endpoint. Only 25,100 of 28,900 approval records are fetchable. Acknowledged as platform limitation, not a pipeline bug.

**Manufacturer Name Deduplication (Known Limitation):**
FDA data contains inconsistent manufacturer naming (e.g. `Fresenius Kabi USA, LLC` vs `fresenius kabi usa  llc`). Current pipeline uses `LOWER(TRIM(REPLACE(...)))` for fuzzy-ish matching. Production solution would use `pg_trgm` extension or `rapidfuzz` Python library for proper deduplication.

---

## Setup & Running

### Prerequisites
- Python 3.12+
- PostgreSQL with `pharma_db` database and `bronze`, `silver`, `gold` schemas
- uv package manager
- dbt-postgres

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
# Set uv environment variables (Windows)
set UV_LINK_MODE=copy
set UV_CACHE_DIR=C:\uvcache

# Activate virtual environment
.venv\Scripts\activate

# Run ingestion scripts
python ingestion/fda_shortages.py
python ingestion/fda_recalls.py
python ingestion/fda_approvals.py
```

### Run dbt Transformations
```bash
cd FDA_Pipeline

# Test connection
dbt debug

# Run all models
dbt run

# Run specific model
dbt run --select mart_alternatives

# Run tests
dbt test

# View lineage graph
dbt docs generate && dbt docs serve
```

---

## Current Status

```
✅ Bronze ingestion          3 scripts, 3 tables, ~44,000 records
✅ Silver layer              9 dbt staging models
✅ Gold — mart_alternatives  9,527 alternative drug-manufacturer pairs
✅ Gold — mart_manufacturer_risk  1,659 manufacturers with risk scores
⏳ Gold — mart_shortage_risk      in progress
⏳ dbt tests                      pending
⏳ Airflow DAG                    pending
⏳ Streamlit dashboard            pending
⏳ Cloud deployment (Supabase + Streamlit Cloud)   pending
⏳ README final version           pending
```

---

## Roadmap

### Phase 1 — Finish Gold + Tests
- [ ] `mart_shortage_risk` model
- [ ] dbt schema tests (unique, not_null, accepted_values)
- [ ] `dbt docs serve` lineage screenshot

### Phase 2 — Orchestration
- [ ] Airflow DAG with daily schedule
- [ ] End-to-end pipeline test

### Phase 3 — Dashboard
- [ ] Streamlit app — Drug shortage search page
- [ ] Streamlit app — Alternatives finder page
- [ ] Streamlit app — Manufacturer risk table page

### Phase 4 — Cloud Deployment
- [ ] Migrate PostgreSQL to Supabase (free tier)
- [ ] Deploy Streamlit to Streamlit Cloud (public URL)
- [ ] Replace local Airflow with GitHub Actions

### Phase 5 — Polish
- [ ] Architecture diagram
- [ ] Final README
- [ ] LinkedIn announcement post

---

## Author

**Ali Khan** 
[GitHub](https://github.com/ali-khan9991)
