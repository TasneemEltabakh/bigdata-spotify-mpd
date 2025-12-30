import streamlit as st
import pandas as pd
import altair as alt
from db import run_query
from queries import TRACK_KPIS, TOP_TRACKS, TRACK_DISTRIBUTION, TOP_TRACKS_BY_APPEARANCES, DURATION_VS_REACH, TOP_TRACKS_TABLE
from ui import apply_spotify_ui, add_now_playing_footer, render_sidebar

st.set_page_config(
    page_title="Product Analysis",
    layout="wide"
)


apply_spotify_ui()
render_sidebar()

st.title("Content")
st.caption("Tracks, playlists, and catalog structure")


# =====================================================
# HEADER
# =====================================================
st.title("Product Analysis")
st.caption("Track-level performance across playlists")

# =====================================================
# KPI CARDS
# =====================================================
kpi_df = run_query(TRACK_KPIS)

if kpi_df.empty:
    st.warning("No product KPI data available.")
    st.stop()

kpis = kpi_df.iloc[0]

c1, c2, c3, c4 = st.columns(4)

c1.metric("🎵 Total Tracks", f"{int(kpis['total_tracks']):,}")
c2.metric("📂 Avg Playlists / Track", f"{kpis['avg_playlists_per_track']:.1f}")
c3.metric("⏱ Avg Track Duration (min)", f"{kpis['avg_track_duration']:.2f}")
c4.metric("🚀 Max Playlist Reach", f"{int(kpis['max_playlist_reach']):,}")

st.divider()

# =====================================================
# TOP TRACKS (RANKING)
# =====================================================
top_tracks_df = run_query(TOP_TRACKS)

st.subheader("Top Tracks by Playlist Coverage")

bar_chart = (
    alt.Chart(top_tracks_df)
    .mark_bar()
    .encode(
        y=alt.Y("track_name:N", sort="-x", title="Track"),
        x=alt.X("playlists_count:Q", title="Playlists Count"),
        tooltip=[
            "track_name:N",
            "playlists_count:Q",
            "total_appearances:Q",
            "avg_duration_min:Q"
        ]
    )
)

st.altair_chart(bar_chart, width='stretch')

st.divider()

# =====================================================
# DISTRIBUTIONS
# =====================================================
dist_df = run_query(TRACK_DISTRIBUTION)

left, right = st.columns(2)

with left:
    playlists_hist = (
        alt.Chart(dist_df)
        .mark_bar()
        .encode(
            x=alt.X("playlists_count:Q", bin=alt.Bin(maxbins=30), title="Playlists Count"),
            y=alt.Y("count()", title="Number of Tracks")
        )
        .properties(title="Track Distribution by Playlist Coverage")
    )
    st.altair_chart(playlists_hist, width='stretch')

with right:
    duration_hist = (
        alt.Chart(dist_df)
        .mark_bar(color="#1DB954")
        .encode(
            x=alt.X("avg_duration_min:Q", bin=alt.Bin(maxbins=30), title="Duration (min)"),
            y=alt.Y("count()", title="Number of Tracks")
        )
        .properties(title="Track Duration Distribution")
    )
    st.altair_chart(duration_hist, width='stretch')


st.divider()
# =====================================================
# TOP TRACKS (TOTAL APPEARANCES)
# =====================================================
st.subheader("Top Tracks by Total Appearances")

@st.cache_data(ttl=1800)
def load_top_appear_df():
    return run_query(TOP_TRACKS_BY_APPEARANCES)

top_appear_df = load_top_appear_df()

appear_chart = (
    alt.Chart(top_appear_df)
    .mark_bar(color="#1DB954")
    .encode(
        y=alt.Y("track_name:N", sort="-x", title="Track"),
        x=alt.X("total_appearances:Q", title="Total Appearances"),
        tooltip=["track_name", "total_appearances", "playlists_count", "avg_duration_min"],
    )
    .properties(height=350)
)
st.altair_chart(appear_chart, width="stretch")


# =====================================================
# DURATION vs REACH (FAST: BINNED)
# =====================================================
st.subheader("Duration vs Playlist Coverage")

@st.cache_data(ttl=1800)
def load_dur_df():
    return run_query(DURATION_VS_REACH)

dur_df = load_dur_df()

dur_scatter = (
    alt.Chart(dur_df)
    .mark_circle(opacity=0.55, color="#1DB954")
    .encode(
        x=alt.X("duration_bin_min:Q", title="Duration Bin (min)"),
        y=alt.Y("median_playlists_count:Q", title="Median Playlists Count", scale=alt.Scale(type="log")),
        size=alt.Size("tracks:Q", title="Tracks in Bin"),
        tooltip=["duration_bin_min", "tracks", "median_playlists_count", "median_total_appearances"],
    )
    .properties(height=380)
)
st.altair_chart(dur_scatter, width="stretch")


st.divider()


# =====================================================
# TABLE VIEW (FAST: LIMIT + CACHE)
# =====================================================
st.subheader("Track Performance Table")
@st.cache_data(ttl=1800)
def load_top_tracks_table():
    return run_query(TOP_TRACKS_TABLE)

top_tracks_df = load_top_tracks_table()

st.dataframe(top_tracks_df, width="stretch")
