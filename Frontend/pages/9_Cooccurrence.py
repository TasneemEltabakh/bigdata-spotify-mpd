import streamlit as st
import altair as alt

from db import run_query
from ui import apply_spotify_ui, add_now_playing_footer, render_sidebar
import queries as q

st.set_page_config(page_title="Co-occurrence", layout="wide")
apply_spotify_ui()
render_sidebar()

st.title("Co-occurrence")
st.caption("Tracks that commonly appear together in playlists. Great for playlist DNA insights.")

def _safe_query(sql: str):
    try:
        return run_query(sql)
    except Exception as e:
        st.error("This page expects an additional gold table that is not available yet: gold_track_pairs.")
        st.code(str(e))
        st.info("I included a ready-to-edit SQL template in the repository zip under Databricks/new_gold_tables.sql.")
        return None

pairs_df = _safe_query(q.TOP_TRACK_PAIRS)

if pairs_df is not None and len(pairs_df) > 0:
    st.subheader("Top 50 track pairs")
    st.dataframe(pairs_df, width='stretch', height=420)

    st.subheader("Co-occurrence bar chart (top 25)")
    top25 = pairs_df.head(25).copy()
    top25["pair"] = top25["track_name_1"] + " × " + top25["track_name_2"]

    chart = (
        alt.Chart(top25)
        .mark_bar()
        .encode(
            y=alt.Y("pair:N", sort='-x', title="Pair"),
            x=alt.X("co_occurrence_count:Q", title="Co-occurrence"),
            tooltip=["pair:N", "co_occurrence_count:Q"]
        )
        .properties(height=520)
    )
    st.altair_chart(chart, width='stretch')
