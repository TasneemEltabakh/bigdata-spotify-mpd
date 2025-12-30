import streamlit as st
import pandas as pd
import altair as alt
from db import run_query
from ui import apply_spotify_ui, add_now_playing_footer, render_sidebar

st.set_page_config(
    page_title="Creator Power",
    layout="wide"
)


apply_spotify_ui()
render_sidebar()


from queries import (
    ARTIST_BASE,
    ARTIST_KPIS,
    TOP_10_EXPOSURE,
    TOTAL_EXPOSURE,
    TOP_TRACKS_TABLE
)


# =====================================================
# HEADER
# =====================================================
st.title("Creators")
st.caption("Artist concentration and reach")
# =====================================================
# LOAD DATA
# =====================================================
artist_df = run_query(ARTIST_BASE)
kpi_df = run_query(ARTIST_KPIS)

top10_exp = run_query(TOP_10_EXPOSURE).iloc[0, 0]
total_exp = run_query(TOTAL_EXPOSURE).iloc[0, 0]

# =====================================================
# KPI CARDS
# =====================================================
kpis = kpi_df.iloc[0]

top10_share = (top10_exp / total_exp) * 100 if total_exp else 0

c1, c2, c3, c4 = st.columns(4)

c1.metric("🎤 Total Artists", f"{int(kpis['total_artists']):,}")
c2.metric("📂 Avg Playlists / Artist", f"{kpis['avg_playlists_per_artist']:.1f}")
c3.metric("🏆 Top 10 Exposure Share", f"{top10_share:.1f}%")
c4.metric("🚀 Max Playlist Reach", f"{int(kpis['max_playlist_reach']):,}")

st.divider()

# =====================================================
# PARETO CURVE (EXPOSURE CONCENTRATION)
# =====================================================
artist_df = artist_df.sort_values("playlists_count", ascending=False)
artist_df["cum_exposure"] = artist_df["playlists_count"].cumsum()
artist_df["cum_exposure_pct"] = artist_df["cum_exposure"] / artist_df["playlists_count"].sum()
artist_df["artist_rank"] = range(1, len(artist_df) + 1)

pareto_chart = (
    alt.Chart(artist_df)
    .mark_line()
    .encode(
        x=alt.X("artist_rank:Q", title="Artists Ranked by Exposure"),
        y=alt.Y("cum_exposure_pct:Q", title="Cumulative Share of Playlist Exposure", axis=alt.Axis(format="%")),
        tooltip=["artist_rank", alt.Tooltip("cum_exposure_pct:Q", format=".2%")]
    )
    .properties(title="Creator Exposure Pareto Curve")
)

st.altair_chart(pareto_chart, width='stretch')

st.divider()

# =====================================================
# TOP 10 VS REST
# =====================================================
comparison_df = pd.DataFrame({
    "Group": ["Top 10 Artists", "All Other Artists"],
    "Playlist Exposure": [top10_exp, total_exp - top10_exp]
})

stacked_bar = (
    alt.Chart(comparison_df)
    .mark_bar()
    .encode(
        x=alt.X("Playlist Exposure:Q", title="Total Playlist Exposure"),
        y=alt.Y("Group:N", title=""),
        color="Group:N"
    )
    .properties(title="Exposure Concentration: Top 10 vs Rest")
)

st.altair_chart(stacked_bar, width='stretch')

st.divider()

# =====================================================
# TRACKS PER ARTIST DISTRIBUTION
# =====================================================
track_dist = (
    alt.Chart(artist_df)
    .mark_bar()
    .encode(
        x=alt.X("track_count:Q", bin=alt.Bin(maxbins=30), title="Tracks per Artist"),
        y=alt.Y("count()", title="Number of Artists")
    )
    .properties(title="Artist Productivity Distribution")
)

st.altair_chart(track_dist, width='stretch')

st.divider()

# =====================================================
# STRATEGIC TABLE
# =====================================================
st.subheader("High-Exposure, Low-Volume Artists")

# Drop nulls to keep quantiles valid
artist_df_clean = artist_df.dropna(subset=["track_count", "playlists_count"])

# Thresholds
p90_playlists = artist_df_clean["playlists_count"].quantile(0.90)
p25_tracks = artist_df_clean["track_count"].quantile(0.25)

strategic_df = (
    artist_df_clean[
        (artist_df_clean["playlists_count"] >= p90_playlists) &
        (artist_df_clean["track_count"] <= p25_tracks)
    ][["artist_name", "track_count", "playlists_count"]]
    .sort_values(by=["playlists_count", "track_count"], ascending=[False, True])
    .head(50)
)
