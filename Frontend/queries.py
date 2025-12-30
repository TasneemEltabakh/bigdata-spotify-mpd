## ======================

## Executive Overview

## ======================


EXEC_KPIS = """
SELECT
    total_tracks,
    total_playlists,
    total_artists,
    avg_playlist_followers,
    last_data_date
FROM gold_exec_kpis
"""

KPI_DAILY_TREND = """
SELECT
    event_date,
    tracks_added,
    active_playlists
FROM gold_kpi_daily
ORDER BY event_date
"""

## ======================

## Product (Track) Analysis

## ======================


TRACK_KPIS = """
SELECT
    COUNT(DISTINCT track_uri) AS total_tracks,
    AVG(playlists_count) AS avg_playlists_per_track,
    AVG(avg_duration_min) AS avg_track_duration,
    MAX(playlists_count) AS max_playlist_reach
FROM gold_track_summary
"""

TOP_TRACKS = """
SELECT
    track_name,
    playlists_count,
    total_appearances,
    avg_duration_min
FROM gold_track_summary
ORDER BY playlists_count DESC
LIMIT 10
"""

TRACK_DISTRIBUTION = """
SELECT
    playlists_count,
    avg_duration_min
FROM gold_track_summary
"""

TOP_TRACKS_BY_APPEARANCES = """
SELECT
    track_name,
    total_appearances,
    playlists_count,
    avg_duration_min
FROM gold_track_summary
ORDER BY total_appearances DESC
LIMIT 10
"""

DURATION_VS_REACH = """
SELECT
    avg_duration_min,
    playlists_count,
    total_appearances
FROM gold_track_summary
WHERE avg_duration_min IS NOT NULL
"""

## ======================

## Artist Performance

## ======================


ARTIST_KPIS = """
SELECT
    COUNT(DISTINCT artist_name) AS total_artists,
    AVG(track_count) AS avg_tracks_per_artist,
    AVG(playlists_count) AS avg_playlists_per_artist,
    MAX(playlists_count) AS max_playlist_reach
FROM gold_artist_summary
"""

TOP_ARTISTS = """
SELECT
    artist_name,
    track_count,
    total_appearances,
    playlists_count
FROM gold_artist_summary
ORDER BY playlists_count DESC
LIMIT 10
"""

ARTIST_DISTRIBUTION = """
SELECT
    track_count,
    playlists_count
FROM gold_artist_summary
"""

## ======================

## Power Concentration (Pareto / Long Tail)

## ======================


PARETO_ARTISTS = """
WITH ranked AS (
  SELECT
    artist_name,
    playlists_count AS reach,
    ROW_NUMBER() OVER (ORDER BY playlists_count DESC) AS rnk,
    SUM(playlists_count) OVER () AS total_reach,
    SUM(playlists_count) OVER (
      ORDER BY playlists_count DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_reach
  FROM gold_artist_summary
)
SELECT
  rnk,
  reach,
  cum_reach,
  total_reach,
  cum_reach / total_reach AS cum_share
FROM ranked
WHERE rnk <= 500
ORDER BY rnk
"""

PARETO_TRACKS = """
WITH ranked AS (
  SELECT
    track_name,
    playlists_count AS reach,
    ROW_NUMBER() OVER (ORDER BY playlists_count DESC) AS rnk,
    SUM(playlists_count) OVER () AS total_reach,
    SUM(playlists_count) OVER (
      ORDER BY playlists_count DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_reach
  FROM gold_track_summary
)
SELECT
  rnk,
  reach,
  cum_reach,
  total_reach,
  cum_reach / total_reach AS cum_share
FROM ranked
WHERE rnk <= 500
ORDER BY rnk
"""

## ======================

## Engagement Segments

## ======================


ENGAGEMENT_SUMMARY = """
SELECT
    tracks_bin,
    total_playlists,
    avg_median_followers,
    avg_p90_followers
FROM gold_engagement_summary
ORDER BY total_playlists DESC
"""

ENGAGEMENT_BINS = """
SELECT
    tracks_bin,
    n_playlists,
    median_followers,
    p90_followers
FROM gold_engagement_bins
ORDER BY n_playlists DESC
"""

## ======================

## Data Quality

## ======================


DATA_QUALITY = """
SELECT
    latest_snapshot,
    total_playlists,
    total_tracks
FROM gold_data_quality
"""

## ======================

## ✅ Playlist Lifecycle (FIXED)

## ======================


PLAYLIST_GROWTH_DIST = """
SELECT
  track_growth,
  COUNT(*) AS n_playlists
FROM gold_playlist_lifecycle
GROUP BY track_growth
ORDER BY track_growth
"""

TOP_DYNAMIC_PLAYLISTS = """
SELECT
  playlist_id,
  active_days,
  min_tracks,
  max_tracks,
  avg_tracks,
  track_growth
FROM gold_playlist_lifecycle
ORDER BY ABS(track_growth) DESC
LIMIT 25
"""


## ======================

## Track Lifecycle

## ======================


TRACK_LONGEVITY_DIST = """
SELECT
  active_days,
  COUNT(*) AS n_tracks
FROM gold_track_lifecycle
GROUP BY active_days
ORDER BY active_days
"""

TOP_EVERGREEN_TRACKS = """
SELECT
  track_name,
  artist_name,
  active_days,
  avg_concurrent_playlists
FROM gold_track_lifecycle
ORDER BY active_days DESC
LIMIT 25
"""

## ======================

## Trend Velocity

## ======================


TRENDING_TRACKS_WEEKLY = """
WITH base AS (
  SELECT
    DATE_TRUNC(
      'week',
      TO_DATE(CAST(event_date AS STRING), 'yyyyMMdd')
    ) AS week_start,
    track_uri,
    track_name,
    CAST(playlists_containing_track AS BIGINT)
      - COALESCE(CAST(prev_playlists_containing_track AS BIGINT), 0) AS net_change
  FROM gold_track_trend_velocity
)
SELECT
  week_start,
  track_uri,
  track_name,
  net_change
FROM base
ORDER BY ABS(net_change) DESC
LIMIT 200
"""



## ======================

## Artist Concentration

## ======================


ARTIST_CONCENTRATION_DIST = """
SELECT
  CAST(FLOOR(gini_artist_index * 20) / 20 AS DOUBLE) AS gini_bin,
  COUNT(*) AS n_playlists,
  AVG(top_artist_share) AS avg_top_artist_share
FROM gold_playlist_artist_concentration
GROUP BY CAST(FLOOR(gini_artist_index * 20) / 20 AS DOUBLE)
ORDER BY gini_bin
"""

TOP_DOMINATED_PLAYLISTS = """
SELECT
  playlist_id,
  total_artists,
  top_artist_name,
  top_artist_share,
  gini_artist_index
FROM gold_playlist_artist_concentration
ORDER BY top_artist_share DESC
LIMIT 25
"""


## ======================

## Track Co-Occurrence

## ======================


TOP_TRACK_PAIRS = """
SELECT
  track_name_1,
  artist_name_1,
  track_name_2,
  artist_name_2,
  co_occurrence_count
FROM gold_track_pairs
ORDER BY co_occurrence_count DESC
LIMIT 50
"""
# ======================
# Creators (missing in some zips)
# ======================

ARTIST_BASE = r"""
SELECT
  artist_name,
  track_count,
  playlists_count
FROM gold_artist_summary
ORDER BY playlists_count DESC
"""

TOTAL_EXPOSURE = r"""
SELECT
  SUM(playlists_count) AS total_exposure
FROM gold_artist_summary
"""

TOP_10_EXPOSURE = r"""
WITH ranked AS (
  SELECT
    playlists_count,
    ROW_NUMBER() OVER (ORDER BY playlists_count DESC) AS rnk
  FROM gold_artist_summary
)
SELECT
  SUM(playlists_count) AS top10_exposure
FROM ranked
WHERE rnk <= 10
"""

# ======================
# Engagement (missing in some zips)
# ======================

ENGAGEMENT_TOTALS = r"""
SELECT
  COUNT(DISTINCT playlist_id) AS total_playlists
FROM silver_playlists
"""

# ======================
# Concentration long-tail bins (missing in some zips)
# ======================

TRACK_REACH_BINS = r"""
WITH base AS (
  SELECT
    playlists_count AS reach
  FROM gold_track_summary
  WHERE playlists_count IS NOT NULL AND playlists_count > 0
),
binned AS (
  SELECT
    CAST(FLOOR(LOG10(reach)) AS INT) AS log10_bucket,
    reach
  FROM base
)
SELECT
  log10_bucket,
  CONCAT(
    CAST(POWER(10, log10_bucket) AS BIGINT),
    ' - ',
    CAST(POWER(10, log10_bucket + 1) - 1 AS BIGINT)
  ) AS bucket_label,
  COUNT(*) AS n_tracks,
  MIN(reach) AS min_reach,
  MAX(reach) AS max_reach
FROM binned
GROUP BY log10_bucket
ORDER BY log10_bucket
"""

ARTIST_REACH_BINS = r"""
WITH base AS (
  SELECT
    playlists_count AS reach
  FROM gold_artist_summary
  WHERE playlists_count IS NOT NULL AND playlists_count > 0
),
binned AS (
  SELECT
    CAST(FLOOR(LOG10(reach)) AS INT) AS log10_bucket,
    reach
  FROM base
)
SELECT
  log10_bucket,
  CONCAT(
    CAST(POWER(10, log10_bucket) AS BIGINT),
    ' - ',
    CAST(POWER(10, log10_bucket + 1) - 1 AS BIGINT)
  ) AS bucket_label,
  COUNT(*) AS n_artists,
  MIN(reach) AS min_reach,
  MAX(reach) AS max_reach
FROM binned
GROUP BY log10_bucket
ORDER BY log10_bucket
"""

# ======================
# Trends (missing in some zips)
# ======================



TOP_RISERS_LATEST_WEEK = """
WITH base AS (
  SELECT
    TO_DATE(CAST(event_date AS STRING), 'yyyyMMdd') AS event_date,
    track_uri,
    track_name,
    CAST(playlists_containing_track AS BIGINT) AS playlists_containing_track,
    CAST(prev_playlists_containing_track AS BIGINT) AS prev_playlists_containing_track
  FROM gold_track_trend_velocity
),
latest AS (
  SELECT MAX(event_date) AS max_date FROM base
)
SELECT
  b.event_date,
  b.track_uri,
  b.track_name,
  COALESCE(b.prev_playlists_containing_track, 0) AS prev_playlists_containing_track,
  b.playlists_containing_track,
  (b.playlists_containing_track - COALESCE(b.prev_playlists_containing_track, 0)) AS net_change
FROM base b
JOIN latest l ON b.event_date = l.max_date
WHERE (b.playlists_containing_track - COALESCE(b.prev_playlists_containing_track, 0)) >= 0
ORDER BY net_change DESC
LIMIT 50
"""

TOP_FALLERS_LATEST_WEEK = """
WITH base AS (
  SELECT
    TO_DATE(CAST(event_date AS STRING), 'yyyyMMdd') AS event_date,
    track_uri,
    track_name,
    CAST(playlists_containing_track AS BIGINT) AS playlists_containing_track,
    CAST(prev_playlists_containing_track AS BIGINT) AS prev_playlists_containing_track
  FROM gold_track_trend_velocity
),
latest AS (
  SELECT MAX(event_date) AS max_date FROM base
)
SELECT
  b.event_date,
  b.track_uri,
  b.track_name,
  COALESCE(b.prev_playlists_containing_track, 0) AS prev_playlists_containing_track,
  b.playlists_containing_track,
  (b.playlists_containing_track - COALESCE(b.prev_playlists_containing_track, 0)) AS net_change
FROM base b
JOIN latest l ON b.event_date = l.max_date
WHERE (b.playlists_containing_track - COALESCE(b.prev_playlists_containing_track, 0)) < 0
ORDER BY net_change ASC
LIMIT 50
"""



DURATION_VS_COVERAGE_BINS = """
WITH base AS (
  SELECT
    CAST(avg_duration_min AS DOUBLE) AS duration_min,
    CAST(playlists_count AS BIGINT) AS playlists_count
  FROM gold_track_summary
  WHERE avg_duration_min IS NOT NULL AND playlists_count IS NOT NULL
),
binned AS (
  SELECT
    CAST(FLOOR(duration_min) AS BIGINT) AS duration_bin_min,  -- 0,1,2,3...
    COUNT(*) AS tracks,
    AVG(playlists_count) AS avg_playlists_count,
    PERCENTILE_APPROX(playlists_count, 0.5) AS median_playlists_count
  FROM base
  GROUP BY CAST(FLOOR(duration_min) AS BIGINT)
)
SELECT *
FROM binned
ORDER BY duration_bin_min
"""

TOP_TRACKS_TABLE = """
SELECT
  track_name,
  track_uri,
  total_appearances,
  playlists_count,
  avg_duration_min
FROM gold_track_summary
ORDER BY total_appearances DESC
LIMIT 1000
"""


DURATION_VS_REACH = """
WITH base AS (
  SELECT
    CAST(avg_duration_min AS DOUBLE) AS avg_duration_min,
    CAST(playlists_count AS BIGINT) AS playlists_count,
    CAST(total_appearances AS BIGINT) AS total_appearances
  FROM gold_track_summary
  WHERE avg_duration_min IS NOT NULL
    AND playlists_count IS NOT NULL
    AND total_appearances IS NOT NULL
),
binned AS (
  SELECT
    CAST(FLOOR(avg_duration_min) AS BIGINT) AS duration_bin_min,
    COUNT(*) AS tracks,
    AVG(playlists_count) AS avg_playlists_count,
    PERCENTILE_APPROX(playlists_count, 0.5) AS median_playlists_count,
    AVG(total_appearances) AS avg_total_appearances,
    PERCENTILE_APPROX(total_appearances, 0.5) AS median_total_appearances
  FROM base
  GROUP BY CAST(FLOOR(avg_duration_min) AS BIGINT)
)
SELECT *
FROM binned
ORDER BY duration_bin_min
"""

ARTIST_BASE = """
SELECT
  artist_name,
  CAST(track_count AS BIGINT) AS track_count,
  CAST(playlists_count AS BIGINT) AS playlists_count,
  CAST(total_appearances AS BIGINT) AS total_appearances
FROM gold_artist_summary
WHERE artist_name IS NOT NULL
"""


HIGH_EXPOSURE_LOW_VOLUME_ARTISTS = """
WITH base AS (
  SELECT *
  FROM ({ARTIST_BASE}) b
  WHERE exposure_score IS NOT NULL
    AND tracks IS NOT NULL
    AND tracks > 0
),
p AS (
  SELECT
    PERCENTILE_APPROX(exposure_score, 0.90) AS exp_p90,
    PERCENTILE_APPROX(tracks, 0.25) AS tracks_p25
  FROM base
)
SELECT
  b.artist_name,
  b.tracks,
  b.playlists_reached,
  b.avg_playlists_per_track,
  b.exposure_score
FROM base b
CROSS JOIN p
WHERE b.exposure_score >= p.exp_p90
  AND b.tracks <= p.tracks_p25
ORDER BY b.exposure_score DESC, b.tracks ASC
LIMIT 50
""".format(ARTIST_BASE=ARTIST_BASE)

PLAYLIST_GROWTH_DIST = """
WITH base AS (
  SELECT
    CAST(max_tracks AS BIGINT) AS max_tracks,
    CAST(min_tracks AS BIGINT) AS min_tracks
  FROM gold_playlist_lifecycle
  WHERE max_tracks IS NOT NULL AND min_tracks IS NOT NULL
)
SELECT
  (max_tracks - min_tracks) AS net_growth,
  COUNT(*) AS n_playlists
FROM base
GROUP BY (max_tracks - min_tracks)
ORDER BY net_growth
"""

TOP_DYNAMIC_PLAYLISTS = """
WITH base AS (
  SELECT
    playlist_id,
    CAST(active_days AS BIGINT) AS active_days,
    CAST(max_tracks AS BIGINT)  AS max_tracks,
    CAST(min_tracks AS BIGINT)  AS min_tracks,
    CAST(followers AS BIGINT)   AS followers
  FROM gold_playlist_lifecycle
  WHERE active_days IS NOT NULL AND active_days > 0
    AND max_tracks IS NOT NULL AND min_tracks IS NOT NULL
),
scored AS (
  SELECT
    playlist_id,
    active_days,
    (max_tracks - min_tracks) AS net_growth,
    ABS(CAST(max_tracks - min_tracks AS DOUBLE)) / NULLIF(CAST(active_days AS DOUBLE), 0) AS change_intensity,
    followers,
    max_tracks
  FROM base
)
SELECT
  playlist_id,
  active_days,
  net_growth,
  change_intensity,
  followers,
  max_tracks
FROM scored
ORDER BY change_intensity DESC
LIMIT 50
"""
ARTIST_CONCENTRATION_DIST = """
WITH base AS (
  SELECT
    CAST(gini_artist_index AS DOUBLE) AS gini,
    CAST(unique_artists AS BIGINT)    AS unique_artists,
    CAST(total_tracks AS BIGINT)      AS total_tracks
  FROM gold_playlist_artist_concentration
  WHERE gini_artist_index IS NOT NULL
)
SELECT
  ROUND(gini, 2) AS gini_bin,
  COUNT(*) AS n_playlists,
  AVG(unique_artists) AS avg_unique_artists,
  AVG(total_tracks) AS avg_total_tracks
FROM base
GROUP BY ROUND(gini, 2)
ORDER BY gini_bin
"""

TOP_DOMINATED_PLAYLISTS = """
SELECT
  playlist_id,
  CAST(gini_artist_index AS DOUBLE) AS gini_artist_index,
  CAST(unique_artists AS BIGINT)    AS unique_artists,
  CAST(total_tracks AS BIGINT)      AS total_tracks,
  CAST(total_artists AS BIGINT)     AS total_artists
FROM gold_playlist_artist_concentration
WHERE gini_artist_index IS NOT NULL
ORDER BY gini_artist_index DESC
LIMIT 50
"""

# TRENDING_TRACKS_WEEKLY = """
# WITH base AS (
#   SELECT
#     CAST(event_date AS DATE) AS d,
#     track_uri,
#     track_name,
#     CAST(prev_playlists_containing_track AS BIGINT) AS prev_playlists_containing_track,
#     CAST(playlists_containing_track AS BIGINT) AS playlists_containing_track,
#     (CAST(playlists_containing_track AS BIGINT) - CAST(prev_playlists_containing_track AS BIGINT)) AS net_change
#   FROM gold_track_trend_velocity
#   WHERE event_date IS NOT NULL
# ),
# weekly AS (
#   SELECT
#     DATE_TRUNC('week', d) AS week_start,
#     track_uri,
#     track_name,
#     SUM(net_change) AS net_change
#   FROM base
#   GROUP BY DATE_TRUNC('week', d), track_uri, track_name
# )
# SELECT
#   week_start,
#   track_uri,
#   track_name,
#   net_change
# FROM weekly
# ORDER BY week_start DESC, ABS(net_change) DESC
# LIMIT 200
# """