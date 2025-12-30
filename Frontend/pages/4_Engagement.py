import streamlit as st
import pandas as pd
import altair as alt

from db import run_query
from ui import apply_spotify_ui, add_now_playing_footer, render_sidebar
from queries import (
    ENGAGEMENT_SUMMARY,
    ENGAGEMENT_TOTALS,
    ENGAGEMENT_BINS,
)

st.set_page_config(
    page_title="Engagement Segments",
    layout="wide",
)

apply_spotify_ui()
render_sidebar()

st.title("Engagement")
st.caption("Playlist popularity tiers and follower behavior")

# =====================================================
# LOAD DATA
# =====================================================
summary_df = run_query(ENGAGEMENT_SUMMARY)
totals_df = run_query(ENGAGEMENT_TOTALS)
bins_df = run_query(ENGAGEMENT_BINS)

total_playlists = totals_df.iloc[0]["total_playlists"]

# =====================================================
# KPI CARDS
# =====================================================
avg_median = summary_df["avg_median_followers"].mean()
avg_p90 = summary_df["avg_p90_followers"].mean()

top_bin_playlists = summary_df.iloc[0]["total_playlists"]
top_bin_share = (top_bin_playlists / total_playlists) * 100 if total_playlists else 0

c1, c2, c3, c4 = st.columns(4)

c1.metric("📂 Total Playlists", f"{int(total_playlists):,}")
c2.metric("👥 Avg Median Followers", f"{avg_median:.1f}")
c3.metric("🚀 Avg P90 Followers", f"{avg_p90:.1f}")
c4.metric("🏆 Top Segment Share", f"{top_bin_share:.1f}%")

st.divider()

# =====================================================
# SEGMENT SIZE
# =====================================================
segment_chart = (
    alt.Chart(summary_df)
    .mark_bar()
    .encode(
        x=alt.X("tracks_bin:N", title="Engagement Segment"),
        y=alt.Y("total_playlists:Q", title="Number of Playlists"),
        tooltip=[
            "tracks_bin:N",
            "total_playlists:Q"
        ]
    )
    .properties(title="Playlists by Engagement Segment")
)

st.altair_chart(segment_chart, width='stretch')

st.divider()

# =====================================================
# FOLLOWER DEPTH
# =====================================================
depth_df = summary_df.melt(
    id_vars="tracks_bin",
    value_vars=["avg_median_followers", "avg_p90_followers"],
    var_name="Metric",
    value_name="Followers"
)

depth_chart = (
    alt.Chart(depth_df)
    .mark_line(point=True)
    .encode(
        x=alt.X("tracks_bin:N", title="Engagement Segment"),
        y=alt.Y("Followers:Q", title="Followers"),
        color="Metric:N",
        tooltip=["Metric:N", "Followers:Q"]
    )
    .properties(title="Follower Depth by Engagement Segment")
)

st.altair_chart(depth_chart, width='stretch')

st.divider()

# =====================================================
# POWER VS REST
# =====================================================
power_df = pd.DataFrame({
    "Group": ["Top Engagement Segment", "All Other Segments"],
    "Playlists": [
        top_bin_playlists,
        total_playlists - top_bin_playlists
    ]
})

power_chart = (
    alt.Chart(power_df)
    .mark_bar()
    .encode(
        x=alt.X("Playlists:Q", title="Number of Playlists"),
        y=alt.Y("Group:N", title=""),
        color="Group:N"
    )
    .properties(title="Engagement Concentration")
)

st.altair_chart(power_chart, width='stretch')

st.divider()

# =====================================================
# TABLE VIEW
# =====================================================
st.subheader("Engagement Segment Details")

st.dataframe(
    bins_df,
    width='stretch'
)

