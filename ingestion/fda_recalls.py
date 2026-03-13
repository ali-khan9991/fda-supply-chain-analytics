import requests
import pandas as pd
import json
import os
import logging
import concurrent.futures
from datetime import datetime
from pathlib import Path
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

# ── logging setup ──────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

log_filename = LOG_DIR / f"fda_recalls_{datetime.now().strftime('%Y-%m-%d')}.log"

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
    "recall_number",
    "recalling_firm",
    "product_description",
    "reason_for_recall",
    "classification",
    "status",
    "voluntary_mandated",
    "initial_firm_notification",
    "distribution_pattern",
    "recall_initiation_date",
    "center_classification_date",
    "termination_date",
    "report_date",
    "product_quantity",
    "city",
    "state",
    "country",
    "code_info",
    "openfda.brand_name",
    "openfda.generic_name",
    "openfda.manufacturer_name",
    "openfda.substance_name",
    "openfda.application_number",
    "openfda.product_ndc",
]

LIST_COLS = [
    "openfda.brand_name",
    "openfda.generic_name",
    "openfda.manufacturer_name",
    "openfda.substance_name",
    "openfda.application_number",
    "openfda.product_ndc",
]

# ── extract ──────────────────────────────────────────
def get_total_records():
    try:
        response = requests.get(
            f"{BASE_URL}/enforcement.json",
            params={"limit": 1, "api_key": os.getenv("FDA_API_KEY")},
            timeout=10
        )
        response.raise_for_status() # if there is an HTTP error, this will raise an exception which we can catch and log
        total = response.json()["meta"]["results"]["total"]
        logger.info(f"Total records to fetch: {total}")
        return total
    except Exception as e:
        logger.error(f"Failed to get total: {type(e).__name__} - {e}") # type is used to log the type of exception that occurred, which can be helpful for debugging and understanding the nature of the error.
        return 0

def fetch_page(skip):
    try:
        response = requests.get(
            f"{BASE_URL}/enforcement.json",
            params={"limit": 100, "skip": skip, "api_key": os.getenv("FDA_API_KEY")},
            timeout=10
        )
        response.raise_for_status()
        results = response.json()["results"]
        logger.info(f"Fetched page at skip={skip} — {len(results)} records")
        return results
    except Exception as e:
        logger.error(f"Error at skip={skip}: {type(e).__name__} - {e}")
        return []

def fetch_all_recalls():
    total = get_total_records()
    if total == 0:
        return []

    skips = range(0, total, 100)

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        pages = list(executor.map(fetch_page, skips))
    # ThreadPoolExecutor is used here to fetch multiple pages concurrently, which can significantly reduce the total time taken to fetch all records compared to sequential requests. The number of workers can be adjusted based on the expected load and system capabilities.
    # ThreadPoolExecutor is chosen over ProcessPoolExecutor because the task is I/O-bound (network requests), and using threads can be more efficient for such tasks without the overhead of inter-process communication.
    # ProcessPoolExecutor is generally better for CPU-bound tasks, while ThreadPoolExecutor is more suitable for I/O-bound tasks like network requests. In this case, since we are making HTTP requests, ThreadPoolExecutor is the appropriate choice.
    # pages is a list of lists (each inner list contains the records fetched from one or one ping)  to flatten this list of lists into a single list of records.
    all_results = [record for page in pages for record in page]
    logger.info(f"Total fetched: {len(all_results)}/{total}")
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
    bronze_df = bronze_df.drop_duplicates(subset=["recall_number"])

    logger.info(f"Built bronze_df with shape: {bronze_df.shape}")
    return bronze_df

# ── load ──────────────────────────────────────────
def load_to_bronze(bronze_df):
    engine = create_engine(DB_URL)

    bronze_df.to_sql(
        name="raw_fda_recalls",
        schema="bronze",
        con=engine,
        if_exists="replace",
        index=False
    )

    logger.info(f"Loaded {len(bronze_df)} records into bronze.raw_fda_recalls")

def fda_recalls():
    logger.info("Starting FDA recalls ingestion...")
    results = fetch_all_recalls()
    bronze_df = build_bronze_df(results)
    load_to_bronze(bronze_df)
    logger.info("Done.")

# ── main ──────────────────────────────────────────
if __name__ == "__main__":
    logger.info("Starting FDA recalls ingestion...")
    results = fetch_all_recalls()
    bronze_df = build_bronze_df(results)
    load_to_bronze(bronze_df)
    logger.info("Done.")