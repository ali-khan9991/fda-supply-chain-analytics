import requests
import pandas as pd
import json
import os
import logging
from datetime import datetime
from pathlib import Path
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

# ── logging setup ──────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

log_filename = LOG_DIR / f"fda_shortages_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"

formatter = logging.Formatter(
    fmt="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

file_handler = logging.FileHandler(log_filename, mode="a", encoding="utf-8")
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

# ── config ──────────────────────────────────────────
BASE_URL = os.getenv("FDA_BASE_URL")
DB_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

BRONZE_COLUMNS = [
    "package_ndc", "generic_name", "company_name", "dosage_form",
    "presentation", "status", "availability", "update_type",
    "initial_posting_date", "update_date", "discontinued_date",
    "shortage_reason", "therapeutic_category", "related_info",
    "contact_info", "openfda.substance_name", "openfda.manufacturer_name",
    "openfda.brand_name", "openfda.application_number",
    "openfda.route", "openfda.product_ndc"
]

LIST_COLS = [
    "openfda.substance_name", "openfda.manufacturer_name",
    "openfda.brand_name", "openfda.application_number",
    "openfda.route", "openfda.product_ndc", "therapeutic_category"
]

# ── extract ──────────────────────────────────────────
def fetch_all_shortages():
    all_results = []
    skip = 0
    total = None

    while True:
        try:
            response = requests.get(
                f"{BASE_URL}/shortages.json",
                params={"limit": 100, "skip": skip},
                timeout=10
            )
            response.raise_for_status()
            data = response.json()

            if total is None:
                total = data["meta"]["results"]["total"]
                logger.info(f"Total records: {total}")

            all_results.extend(data["results"])
            logger.info(f"Fetched: {len(all_results)}/{total}")
            skip += 100

            if skip >= total:
                break
        except Exception as e:
            logger.error(f"Request failed: {e}")
            break

    return all_results

# ── transform ──────────────────────────────────────────
def build_bronze_df(results):
    df = pd.json_normalize(results)

    existing = [c for c in BRONZE_COLUMNS if c in df.columns]
    bronze_df = df[existing].copy()

    for col in LIST_COLS:
        if col in bronze_df.columns:
            bronze_df[col] = bronze_df[col].apply(
                lambda x: json.dumps(x) if isinstance(x, list) else x
            )

    bronze_df["ingested_at"] = pd.Timestamp.now()
    bronze_df = bronze_df.drop_duplicates(subset=["package_ndc"])

    logger.info(f"Built bronze_df with shape: {bronze_df.shape}")
    return bronze_df

# ── load ──────────────────────────────────────────
def load_to_bronze(bronze_df):
    engine = create_engine(DB_URL)

    bronze_df.to_sql(
        name="raw_fda_shortages",
        schema="bronze",
        con=engine,
        if_exists="replace",
        index=False
    )

    logger.info(f"Loaded {len(bronze_df)} records into bronze.raw_fda_shortages")

# ── main ──────────────────────────────────────────
if __name__ == "__main__":
    logger.info("Starting FDA shortages ingestion...")
    results = fetch_all_shortages()
    bronze_df = build_bronze_df(results)
    load_to_bronze(bronze_df)
    logger.info("Done.")