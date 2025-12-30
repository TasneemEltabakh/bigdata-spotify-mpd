import streamlit as st
import pandas as pd
import altair as alt
from db import run_query
from queries import DATA_QUALITY
from ui import apply_spotify_ui, add_now_playing_footer, render_sidebar

st.set_page_config(
    page_title="Data Quality",
    layout="wide"
)


apply_spotify_ui()
render_sidebar()


# =====================================================
# HEADER
# =====================================================
st.title("Data Health")
st.caption("Pipeline freshness and processing status")
# =====================================================
# LOAD DATA
# =====================================================
df = run_query(DATA_QUALITY)

if df.empty:
    st.warning("No data quality information available.")
    st.stop()

row = df.iloc[0]

# =====================================================
# FORMAT SNAPSHOT DATE
# =====================================================
snapshot_date = pd.to_datetime(
    row["latest_snapshot"],
    unit="s",
    errors="coerce"
)

snapshot_str = (
    snapshot_date.strftime("%b %d, %Y")
    if pd.notna(snapshot_date)
    else "Unknown"
)

# =====================================================
# PIPELINE STATUS (DERIVED)
# =====================================================
pipeline_status = "Healthy" if pd.notna(snapshot_date) else "Unknown"

# =====================================================
# KPI CARDS
# =====================================================
c1, c2, c3, c4 = st.columns(4)

c1.metric("🕒 Last Snapshot", snapshot_str)
c2.metric("📂 Playlists Processed", f"{int(row['total_playlists']):,}")
c3.metric("🎵 Tracks Processed", f"{int(row['total_tracks']):,}")
c4.metric("✅ Pipeline Status", pipeline_status)

st.divider()

# =====================================================
# DATA VOLUME CHART
# =====================================================
volume_df = pd.DataFrame({
    "Metric": ["Playlists", "Tracks"],
    "Count": [row["total_playlists"], row["total_tracks"]]
})

volume_chart = (
    alt.Chart(volume_df)
    .mark_bar()
    .encode(
        x=alt.X("Metric:N", title=""),
        y=alt.Y("Count:Q", title="Records Processed"),
        tooltip=["Metric:N", "Count:Q"]
    )
    .properties(title="Processed Data Volume")
)

st.altair_chart(volume_chart, width='stretch')

st.divider()

# =====================================================
# AUDIT TABLE
# =====================================================
st.subheader("Pipeline Snapshot Details")

st.dataframe(
    pd.DataFrame([{
        "Snapshot Date": snapshot_str,
        "Total Playlists": row["total_playlists"],
        "Total Tracks": row["total_tracks"],
        "Pipeline Status": pipeline_status
    }]),
    width='stretch'
)