import streamlit as st
import altair as alt
import pandas as pd

from db import run_query
from ui import apply_spotify_ui, add_now_playing_footer, render_sidebar
import queries as q

st.set_page_config(page_title="Trends", layout="wide")
apply_spotify_ui()
render_sidebar()

st.title("Trends")
st.caption("Velocity-based views: what is rising or falling in playlist coverage over time.")


def _safe_query(sql: str):
    try:
        return run_query(sql)
    except Exception as e:
        st.error("This page expects gold_track_trend_velocity to exist in your Databricks schema.")
        st.code(str(e))
        return None


@st.cache_data(ttl=1800)
def _cached(sql: str):
    return _safe_query(sql)


# =====================================================
# Top risers / fallers (latest week)
# =====================================================
riser_df = _cached(q.TOP_RISERS_LATEST_WEEK)
faller_df = _cached(q.TOP_FALLERS_LATEST_WEEK)

c1, c2 = st.columns(2)

with c1:
    st.subheader("Top risers (latest week)")
    if riser_df is not None and not riser_df.empty:
        st.dataframe(riser_df, width="stretch", height=420)
    else:
        st.info("Single-week dataset: risers require at least 2 weeks. Showing baseline coverage below.")

with c2:
    st.subheader("Top fallers (latest week)")
    if faller_df is not None and not faller_df.empty:
        st.dataframe(faller_df, width="stretch", height=420)
    else:
        st.info("Single-week dataset: fallers require at least 2 weeks. Showing baseline coverage below.")


# =====================================================
# Recent weekly changes (Top 200)
# =====================================================
st.subheader("Recent weekly changes (Top 200 rows)")

trend_df = _cached(q.TRENDING_TRACKS_WEEKLY)

# -----------------------------------------------------
# CASE 1: No trend data at all
# -----------------------------------------------------
if trend_df is None or trend_df.empty:
    st.info("No weekly trend data returned.")

# -----------------------------------------------------
# CASE 2: Only ONE week exists → BASELINE MODE
# -----------------------------------------------------
elif trend_df["week_start"].nunique(dropna=True) <= 1:
    st.info(
        "Only one week of data is currently available. "
        "Week-over-week trends require at least two weeks. "
        "Showing baseline playlist coverage instead."
    )

    baseline_df = _cached(q.TOP_TRACKS_BY_APPEARANCES)

    if baseline_df is not None and not baseline_df.empty:
        baseline_df = baseline_df.head(25)

        baseline_chart = (
            alt.Chart(baseline_df)
            .mark_bar()
            .encode(
                y=alt.Y("track_name:N", sort="-x", title=None),
                x=alt.X("playlists_count:Q", title="Playlist coverage"),
                tooltip=[
                    alt.Tooltip("track_name:N", title="Track"),
                    alt.Tooltip("playlists_count:Q", title="Playlists"),
                    alt.Tooltip("total_appearances:Q", title="Total appearances"),
                    alt.Tooltip("avg_duration_min:Q", title="Avg duration (min)"),
                ],
            )
            .properties(height=520)
        )

        st.altair_chart(baseline_chart, width="stretch")
    else:
        st.warning("Baseline coverage data is unavailable.")

# -----------------------------------------------------
# CASE 3: MULTI-WEEK MODE → real trends
# -----------------------------------------------------
else:
    trend_df["week_start"] = pd.to_datetime(trend_df["week_start"], errors="coerce")
    trend_df = trend_df.dropna(subset=["week_start"])

    latest_week = trend_df["week_start"].max()

    latest_df = trend_df[trend_df["week_start"] == latest_week].copy()
    latest_df["abs_change"] = latest_df["net_change"].abs()
    latest_df = latest_df.sort_values("abs_change", ascending=False).head(25)

    st.markdown(f"**Top movers in latest week: {latest_week.date()}**")

    movers_chart = (
        alt.Chart(latest_df)
        .mark_bar()
        .encode(
            y=alt.Y("track_name:N", sort="-x", title=None),
            x=alt.X("net_change:Q", title="Net change (adds − removes)"),
            tooltip=[
                alt.Tooltip("track_name:N", title="Track"),
                alt.Tooltip("net_change:Q", title="Net change"),
            ],
        )
        .properties(height=520)
    )

    st.altair_chart(movers_chart, width="stretch")


