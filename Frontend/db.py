from databricks import sql
import pandas as pd
import streamlit as st

# ================================
# Databricks Connection (Cloud-safe)
# ================================

@st.cache_resource
def get_connection():
    hostname = st.secrets["DATABRICKS_SERVER_HOSTNAME"]
    http_path = st.secrets["DATABRICKS_HTTP_PATH"]
    token = st.secrets["DATABRICKS_TOKEN"]

    return sql.connect(
        server_hostname=hostname,
        http_path=http_path,
        access_token=token
    )


# ================================
# Query Runner
# ================================

@st.cache_data(ttl=300)
def run_query(query: str, params: tuple = ()):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

    return pd.DataFrame(rows, columns=cols)
