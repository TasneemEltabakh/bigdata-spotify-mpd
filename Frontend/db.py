import os
from databricks import sql
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# 🔹 LOAD ENV VARIABLES HERE
load_dotenv()

def _get_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing environment variable: {name}")
    return v

@st.cache_resource
def get_connection():
    hostname = _get_env("DATABRICKS_SERVER_HOSTNAME")
    http_path = _get_env("DATABRICKS_HTTP_PATH")
    token = _get_env("DATABRICKS_TOKEN")

    return sql.connect(
        server_hostname=hostname,
        http_path=http_path,
        access_token=token
    )

@st.cache_data(ttl=300)
def run_query(query: str, params: tuple = ()):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    return pd.DataFrame(rows, columns=cols)
