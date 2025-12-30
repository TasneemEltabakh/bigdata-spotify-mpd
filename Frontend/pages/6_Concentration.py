import streamlit as st
import altair as alt
from db import run_query
from ui import apply_spotify_ui, render_sidebar, add_now_playing_footer
from queries import (
    PARETO_ARTISTS,
    PARETO_TRACKS,
    TRACK_REACH_BINS,
    ARTIST_REACH_BINS,
)

st.set_page_config(page_title="Concentration & Long Tail", layout="wide")

apply_spotify_ui()
render_sidebar()

st.title("Concentration & Long Tail")
st.caption("Server-side Pareto curves and log-binned reach distributions (computed in Databricks)")

# =====================================================
# PARETO CURVES
# =====================================================
left, right = st.columns(2)

with left:
    st.subheader("Artist Reach Concentration (Top 500)")
    pareto_a = run_query(PARETO_ARTISTS)

    chart_a = (
        alt.Chart(pareto_a)
        .mark_line(color="#1DB954")
        .encode(
            x=alt.X("rnk:Q", title="Artist Rank (1 = highest reach)"),
            y=alt.Y("cum_share:Q", title="Cumulative Share of Total Reach", axis=alt.Axis(format="%")),
            tooltip=[
                alt.Tooltip("rnk:Q", title="Rank"),
                alt.Tooltip("reach:Q", title="Reach"),
                alt.Tooltip("cum_share:Q", title="Cum Share", format=".2%")
            ]
        )
        .properties(height=360)
    )
    st.altair_chart(chart_a, width='stretch')

with right:
    st.subheader("Track Reach Concentration (Top 500)")
    pareto_t = run_query(PARETO_TRACKS)

    chart_t = (
        alt.Chart(pareto_t)
        .mark_line(color="#1DB954")
        .encode(
            x=alt.X("rnk:Q", title="Track Rank (1 = highest reach)"),
            y=alt.Y("cum_share:Q", title="Cumulative Share of Total Reach", axis=alt.Axis(format="%")),
            tooltip=[
                alt.Tooltip("rnk:Q", title="Rank"),
                alt.Tooltip("reach:Q", title="Reach"),
                alt.Tooltip("cum_share:Q", title="Cum Share", format=".2%")
            ]
        )
        .properties(height=360)
    )
    st.altair_chart(chart_t, width='stretch')

st.divider()

# =====================================================
# LONG TAIL (LOG BINS)
# =====================================================
left, right = st.columns(2)

with left:
    st.subheader("Track Reach Long-Tail (log10 buckets)")
    bins_t = run_query(TRACK_REACH_BINS)
    # bucket label like 10^k .. 10^(k+1)
    bins_t["bucket_label"] = bins_t["log10_bucket"].apply(lambda k: f"10^{k} .. 10^{k+1}")

    bins_chart_t = (
        alt.Chart(bins_t)
        .mark_bar(color="#1DB954")
        .encode(
            x=alt.X("bucket_label:N", title="Reach Bucket (playlists_count)", sort=None),
            y=alt.Y("n_tracks:Q", title="Number of Tracks"),
            tooltip=["log10_bucket", "n_tracks", "min_reach", "max_reach"]
        )
        .properties(height=360)
    )
    st.altair_chart(bins_chart_t, width='stretch')

with right:
    st.subheader("Artist Reach Long-Tail (log10 buckets)")
    bins_a = run_query(ARTIST_REACH_BINS)
    bins_a["bucket_label"] = bins_a["log10_bucket"].apply(lambda k: f"10^{k} .. 10^{k+1}")

    bins_chart_a = (
        alt.Chart(bins_a)
        .mark_bar(color="#1DB954")
        .encode(
            x=alt.X("bucket_label:N", title="Reach Bucket (playlists_count)", sort=None),
            y=alt.Y("n_artists:Q", title="Number of Artists"),
            tooltip=["log10_bucket", "n_artists", "min_reach", "max_reach"]
        )
        .properties(height=360)
    )
    st.altair_chart(bins_chart_a, width='stretch')
