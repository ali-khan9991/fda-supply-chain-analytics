import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pathlib import Path
import os
from urllib.parse import quote_plus

# load environment variables
load_dotenv(Path(__file__).parent.parent / '.env')

# database connection
if hasattr(st, 'secrets') and 'DB_HOST' in st.secrets:
    DB_URL = (
        f"postgresql://{st.secrets['DB_USER']}:{quote_plus(st.secrets['DB_PASSWORD'])}"
        f"@{st.secrets['DB_HOST']}:{st.secrets['DB_PORT']}/{st.secrets['DB_NAME']}"
    )
else:
    load_dotenv(Path(__file__).parent.parent / '.env')
    DB_URL = (
        f"postgresql://{os.getenv('DB_USER')}:{quote_plus(os.getenv('DB_PASSWORD'))}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )



@st.cache_data
def load_data(query):
    engine = create_engine(DB_URL)
    return pd.read_sql(query, engine)

# page config
st.set_page_config(
    page_title="FDA Pharma Supply Chain Intelligence",
    page_icon="💊",
    layout="wide"
)

# sidebar navigation
st.sidebar.title("💊 FDA Supply Chain")
page = st.sidebar.radio(
    "Navigate",
    ["Drug Shortage Risk", "Alternatives Finder", "Manufacturer Risk"]
)

# ── Page 1 — Drug Shortage Risk ──────────────────────────
if page == "Drug Shortage Risk":
    st.title("🚨 Drug Shortage Risk Dashboard")
    st.caption("FDA drug shortages ranked by composite risk score")

    df = load_data("SELECT * FROM gold.mart_shortage_risk ORDER BY shortage_risk_score DESC")

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Current Shortages", len(df))
    col2.metric("Critical Risk", len(df[df['shortage_risk_level'] == 'Critical']))
    col3.metric("High Risk", len(df[df['shortage_risk_level'] == 'High']))

    risk_filter = st.selectbox("Filter by Risk Level", ["All", "Critical", "High", "Medium", "Low"])
    if risk_filter != "All":
        df = df[df['shortage_risk_level'] == risk_filter]

    search = st.text_input("Search by drug name")
    if search:
        df = df[df['generic_name'].str.contains(search, case=False, na=False)]

    st.dataframe(
        df[['generic_name', 'brand_name', 'route', 'shortage_days',
            'alternatives_available', 'manufacturer_risk_level', 'shortage_risk_level']],
        use_container_width=True
    )

# ── Page 2 — Alternatives Finder ──────────────────────────
elif page == "Alternatives Finder":
    st.title("🔍 Drug Alternatives Finder")
    st.caption("Find FDA approved manufacturers not currently in shortage")

    df = load_data("SELECT * FROM gold.mart_alternatives ORDER BY shortage_days DESC")

    search = st.text_input("Search by drug name or substance")
    if search:
        df = df[
            df['generic_name'].str.contains(search, case=False, na=False) |
            df['substance_name'].str.contains(search, case=False, na=False)
        ]

    st.metric("Alternative Manufacturer Options Found", len(df))
    st.dataframe(
        df[['generic_name', 'substance_name', 'shortage_days', 'alternative_manufacturer']],
        use_container_width=True
    )

# ── Page 3 — Manufacturer Risk ──────────────────────────
elif page == "Manufacturer Risk":
    st.title("🏭 Manufacturer Risk Scorecard")
    st.caption("Manufacturers ranked by combined shortage and recall risk")

    df = load_data("SELECT * FROM gold.mart_manufacturer_risk ORDER BY risk_score DESC")

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Manufacturers", len(df))
    col2.metric("High Risk", len(df[df['risk_level'] == 'High']))
    col3.metric("Medium Risk", len(df[df['risk_level'] == 'Medium']))

    risk_filter = st.selectbox("Filter by Risk Level", ["All", "High", "Medium", "Low"])
    if risk_filter != "All":
        df = df[df['risk_level'] == risk_filter]

    search = st.text_input("Search manufacturer")
    if search:
        df = df[df['manufacturer_name'].str.contains(search, case=False, na=False)]

    st.dataframe(
        df[['manufacturer_name', 'active_shortage_count', 'total_recalls',
            'class1_recalls', 'ongoing_recalls', 'risk_score', 'risk_level']],
        use_container_width=True
    )