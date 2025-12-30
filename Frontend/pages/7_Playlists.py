import streamlit as st
import pandas as pd
import altair as alt

from db import run_query
from ui import apply_spotify_ui, add_now_playing_footer, render_sidebar
from queries import (
    PLAYLIST_GROWTH_DIST,
    TOP_DYNAMIC_PLAYLISTS,
    ARTIST_CONCENTRATION_DIST,
    TOP_DOMINATED_PLAYLISTS,
    TRENDING_TRACKS_WEEKLY
)

st.set_page_config(page_title="Playlists", layout="wide")
apply_spotify_ui()
render_sidebar()

st.title("Playlists")
st.caption("How playlists evolve, and how diverse (or concentrated) they are.")

def _safe_query(sql: str):
    try:
        return run_query(sql)
    except Exception as e:
        st.error("This page expects additional gold tables that are not available yet in your Databricks schema.")
        st.code(str(e))
        st.info(
            "Create these tables on Databricks: gold_playlist_lifecycle, gold_playlist_artist_concentration. "
            "I included a ready-to-edit SQL template in the repository zip under Databricks/new_gold_tables.sql."
        )
        return None

# =========================
# Playlist growth / churn
# =========================
# st.subheader("Lifecycle and change intensity")

# growth_df = _safe_query("""
# SELECT CAST(track_growth AS BIGINT) AS net_growth
# FROM gold_playlist_lifecycle
# WHERE track_growth IS NOT NULL
# """)
# top_dyn_df = _safe_query(TOP_DYNAMIC_PLAYLISTS)

# if growth_df is not None and len(growth_df) > 0:
#     c1, c2 = st.columns([2, 1])

#     with c1:
#         chart = (
#             alt.Chart(growth_df)
#             .mark_bar()
#             .encode(
#                 x=alt.X("net_growth:Q", bin=alt.Bin(maxbins=60), title="Net Growth (tracks)"),
#                 y=alt.Y("count():Q", title="Playlists"),
#                 tooltip=[alt.Tooltip("net_growth:Q"), alt.Tooltip("count():Q")]
#             )
#         )
#         st.altair_chart(chart, width="stretch")
#     with c2:
#         st.markdown("**Top dynamic playlists**")
#         if top_dyn_df is not None:
#             st.dataframe(top_dyn_df, width='stretch', height=360)

growth_df = _safe_query(PLAYLIST_GROWTH_DIST)

if growth_df is None or growth_df.empty:
    st.warning("No lifecycle data returned.")
else:
    # If everything is 0 (your screenshot case), show a clear message + still render a single bar
    if growth_df["net_growth"].nunique() == 1 and float(growth_df["net_growth"].iloc[0]) == 0.0:
        st.info(
            "All playlists have net_growth = 0 based on the current gold_playlist_lifecycle data. "
            "This usually means the lifecycle table was built from a single snapshot (no day-to-day history)."
        )

    chart = (
        alt.Chart(growth_df)
        .mark_bar()
        .encode(
            x=alt.X("net_growth:Q", bin=alt.Bin(maxbins=60), title="Net Growth (tracks)"),
            y=alt.Y("n_playlists:Q", title="Playlists"),
            tooltip=[alt.Tooltip("net_growth:Q"), alt.Tooltip("n_playlists:Q")]
        )
        .properties(height=320)
    )
    st.altair_chart(chart, width="stretch")
    
    
# =========================
# Artist concentration
# =========================
st.subheader("Artist concentration inside playlists")

conc_df = _safe_query(ARTIST_CONCENTRATION_DIST)
dom_df = _safe_query(TOP_DOMINATED_PLAYLISTS)

if conc_df is not None and len(conc_df) > 0:
    c3, c4 = st.columns([2, 1])

    with c3:
        st.markdown("**Gini concentration distribution** (0 = diverse, 1 = dominated)")
        chart2 = (
            alt.Chart(conc_df)
            .mark_bar()
            .encode(
                x=alt.X("gini_bin:Q", title="Gini Bin"),
                y=alt.Y("n_playlists:Q", title="Playlists"),
                tooltip=[alt.Tooltip("gini_bin:Q"), alt.Tooltip("n_playlists:Q"), alt.Tooltip("avg_top_artist_share:Q")]
            )
        )
        st.altair_chart(chart2, width='stretch')

    with c4:
        st.markdown("**Most dominated playlists**")
        if dom_df is not None:
            st.dataframe(dom_df, width='stretch', height=360)
