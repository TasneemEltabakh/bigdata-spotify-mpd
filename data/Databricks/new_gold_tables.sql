-- =========================================================
-- Gold tables for the Streamlit dashboard (Databricks SQL)
--
-- IMPORTANT: This version matches the silver tables you provided:
--   - silver_playlist_tracks.playlist_modified_at (STRING)
--   - silver_playlist_tracks.playlist_followers (LONG)
--   - silver_playlists.playlist_modified_ts exists, but these gold tables
--     are derived from silver_playlist_tracks snapshots.
-- =========================================================

-- 1) Playlist lifecycle (min/max/avg tracks across snapshot days)
CREATE OR REPLACE TABLE gold_playlist_lifecycle AS
WITH per_day AS (
  SELECT
    playlist_id,
    TO_DATE(playlist_modified_at) AS snapshot_date,
    COUNT(*) AS tracks_in_snapshot
  FROM silver_playlist_tracks
  GROUP BY playlist_id, TO_DATE(playlist_modified_at)
)
SELECT
  playlist_id,
  MIN(snapshot_date) AS first_snapshot_date,
  MAX(snapshot_date) AS last_snapshot_date,
  COUNT(DISTINCT snapshot_date) AS active_days,
  MIN(tracks_in_snapshot) AS min_tracks,
  MAX(tracks_in_snapshot) AS max_tracks,
  AVG(tracks_in_snapshot) AS avg_tracks,
  (MAX(tracks_in_snapshot) - MIN(tracks_in_snapshot)) AS track_growth
FROM per_day
GROUP BY playlist_id
;

-- 2) Track lifecycle (how long a track persists across snapshot days)
CREATE OR REPLACE TABLE gold_track_lifecycle AS
WITH per_day_track AS (
  SELECT
    TO_DATE(playlist_modified_at) AS snapshot_date,
    track_uri,
    MAX(track_name) AS track_name,
    MAX(artist_name) AS artist_name,
    COUNT(DISTINCT playlist_id) AS playlists_containing_track
  FROM silver_playlist_tracks
  GROUP BY TO_DATE(playlist_modified_at), track_uri
)
SELECT
  track_uri,
  MAX(track_name) AS track_name,
  MAX(artist_name) AS artist_name,
  MIN(snapshot_date) AS first_snapshot_date,
  MAX(snapshot_date) AS last_snapshot_date,
  COUNT(*) AS active_days,
  AVG(playlists_containing_track) AS avg_concurrent_playlists
FROM per_day_track
GROUP BY track_uri
;

-- 3) Track trend velocity (week-to-week or snapshot-to-snapshot delta)
-- event_date is stored as yyyymmdd (BIGINT) to match your CSV schema.
CREATE OR REPLACE TABLE gold_track_trend_velocity AS
WITH per_day_track AS (
  SELECT
    CAST(DATE_FORMAT(TO_DATE(playlist_modified_at), 'yyyyMMdd') AS BIGINT) AS event_date,
    track_uri,
    COUNT(DISTINCT playlist_id) AS playlists_containing_track
  FROM silver_playlist_tracks
  GROUP BY CAST(DATE_FORMAT(TO_DATE(playlist_modified_at), 'yyyyMMdd') AS BIGINT), track_uri
),
with_prev AS (
  SELECT
    event_date,
    track_uri,
    playlists_containing_track,
    LAG(playlists_containing_track) OVER (PARTITION BY track_uri ORDER BY event_date) AS prev_playlists_containing_track
  FROM per_day_track
)
SELECT
  event_date,
  track_uri,
  playlists_containing_track,
  prev_playlists_containing_track,
  (playlists_containing_track - COALESCE(prev_playlists_containing_track, playlists_containing_track)) AS delta_playlists
FROM with_prev
;

-- 4) Playlist artist concentration (top artist share + Gini index)
CREATE OR REPLACE TABLE gold_playlist_artist_concentration AS
WITH artist_counts AS (
  SELECT
    playlist_id,
    artist_name,
    COUNT(*) AS artist_tracks
  FROM silver_playlist_tracks
  GROUP BY playlist_id, artist_name
),
playlist_totals AS (
  SELECT
    playlist_id,
    SUM(artist_tracks) AS total_tracks,
    COUNT(*) AS total_artists,
    MAX(artist_tracks) AS top_artist_tracks,
    MAX_BY(artist_name, artist_tracks) AS top_artist_name
  FROM artist_counts
  GROUP BY playlist_id
),
ranked AS (
  SELECT
    ac.playlist_id,
    ac.artist_name,
    ac.artist_tracks,
    pt.total_tracks,
    pt.total_artists,
    pt.top_artist_tracks,
    pt.top_artist_name,
    ROW_NUMBER() OVER (PARTITION BY ac.playlist_id ORDER BY ac.artist_tracks ASC, ac.artist_name ASC) AS rnk
  FROM artist_counts ac
  JOIN playlist_totals pt ON pt.playlist_id = ac.playlist_id
),
gini_parts AS (
  SELECT
    playlist_id,
    total_tracks,
    total_artists,
    top_artist_name,
    top_artist_tracks,
    SUM(rnk * artist_tracks) AS sum_rnk_x
  FROM ranked
  GROUP BY playlist_id, total_tracks, total_artists, top_artist_name, top_artist_tracks
)
SELECT
  playlist_id,
  total_artists,
  top_artist_name,
  (top_artist_tracks / total_tracks) AS top_artist_share,
  CASE
    WHEN total_artists <= 1 OR total_tracks = 0 THEN 0.0
    ELSE ( (2.0 * sum_rnk_x) / (total_artists * total_tracks) ) - ( (total_artists + 1.0) / total_artists )
  END AS gini_artist_index
FROM gini_parts
;

-- 5) Track co-occurrence pairs (count of playlists where the pair appears)
CREATE OR REPLACE TABLE gold_track_pairs AS
WITH playlist_tracks AS (
  SELECT DISTINCT
    playlist_id,
    track_uri
  FROM silver_playlist_tracks
),
pairs AS (
  SELECT
    a.track_uri AS track_uri_1,
    b.track_uri AS track_uri_2,
    COUNT(DISTINCT a.playlist_id) AS co_occurrence_count
  FROM playlist_tracks a
  JOIN playlist_tracks b
    ON a.playlist_id = b.playlist_id
   AND a.track_uri < b.track_uri
  GROUP BY a.track_uri, b.track_uri
),
track_dim AS (
  SELECT
    track_uri,
    MAX(track_name) AS track_name,
    MAX(artist_name) AS artist_name
  FROM silver_playlist_tracks
  GROUP BY track_uri
)
SELECT
  p.track_uri_1,
  d1.track_name AS track_name_1,
  d1.artist_name AS artist_name_1,
  p.track_uri_2,
  d2.track_name AS track_name_2,
  d2.artist_name AS artist_name_2,
  p.co_occurrence_count
FROM pairs p
LEFT JOIN track_dim d1 ON d1.track_uri = p.track_uri_1
LEFT JOIN track_dim d2 ON d2.track_uri = p.track_uri_2
;
