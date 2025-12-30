import streamlit as st
import pandas as pd
import altair as alt
from db import run_query
from queries import EXEC_KPIS, KPI_DAILY_TREND
from ui import apply_spotify_ui, add_now_playing_footer, render_sidebar

st.set_page_config(
    page_title="Executive Overview",
    layout="wide"
)




# =====================================================
# HEADER
# =====================================================
apply_spotify_ui()
render_sidebar()

st.title("Overview")
st.caption("Platform scale and growth signals")

# =====================================================
# KPI CARDS
# =====================================================
kpi_df = run_query(EXEC_KPIS)

if kpi_df.empty:
    st.warning("No KPI data available.")
    st.stop()

kpis = kpi_df.iloc[0]

c1, c2, c3, c4 = st.columns(4)

c1.metric("🎵 Total Tracks", f"{int(kpis['total_tracks']):,}")
c2.metric("📂 Total Playlists", f"{int(kpis['total_playlists']):,}")
c3.metric("🎤 Total Artists", f"{int(kpis['total_artists']):,}")
c4.metric("⭐ Avg Playlist Followers", f"{kpis['avg_playlist_followers']:.1f}")

# Format last refresh date (Unix timestamp → readable date)
last_refresh = pd.to_datetime(
    kpis["last_data_date"],
    unit="s",
    errors="coerce"
)

if pd.notna(last_refresh):
    st.caption(f"Last data refresh: {last_refresh.strftime('%b %d, %Y')}")
else:
    st.caption("Last data refresh: N/A")

st.divider()

# =====================================================
# LOAD TREND DATA (FULL HISTORY)
# =====================================================
trend_df = run_query(KPI_DAILY_TREND)

if trend_df.empty:
    st.warning("No trend data available.")
    st.stop()

trend_df["event_date"] = pd.to_datetime(
    trend_df["event_date"], unit="s", errors="coerce"
)

trend_df = trend_df.dropna(subset=["event_date"])

# =====================================================
# CHARTS
# =====================================================
left, right = st.columns(2)

with left:
    tracks_chart = (
        alt.Chart(trend_df)
        .mark_line(point=False)
        .encode(
            x=alt.X("event_date:T", title="Date"),
            y=alt.Y("tracks_added:Q", title="Tracks Added"),
            tooltip=[
                alt.Tooltip("event_date:T", title="Date"),
                alt.Tooltip("tracks_added:Q", title="Tracks Added")
            ]
        )
        .properties(title="Tracks Added Over Time")
    )
    st.altair_chart(tracks_chart, width='stretch')

with right:
    playlists_chart = (
        alt.Chart(trend_df)
        .mark_line(point=False, color="#1DB954")
        .encode(
            x=alt.X("event_date:T", title="Date"),
            y=alt.Y("active_playlists:Q", title="Active Playlists"),
            tooltip=[
                alt.Tooltip("event_date:T", title="Date"),
                alt.Tooltip("active_playlists:Q", title="Active Playlists")
            ]
        )
        .properties(title="Active Playlists Over Time")
    )
    st.altair_chart(playlists_chart, width='stretch')