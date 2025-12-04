# Databricks notebook source
# MAGIC %md
# MAGIC # ETL Pipeline
# MAGIC This notebook implements the Extract, Transform, Load (ETL) pipeline for the Spotify Million Playlist Dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, count, avg, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# NOTE: SparkSession is pre-initialized in Databricks as 'spark'
# For local development, uncomment the following:
# spark = SparkSession.builder.appName("SpotifyMPD-ETL").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extract

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Define Source Paths

# COMMAND ----------

# TODO: Define source data paths
# source_path = "/mnt/data/spotify_mpd/"
# delta_source_path = "/mnt/delta/spotify_mpd_clean/"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Load Source Data

# COMMAND ----------

# TODO: Extract data from Delta Lake
# df_source = spark.read.format("delta").load(delta_source_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Transform

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Playlist Transformations

# COMMAND ----------

# TODO: Create playlist dimension table
# df_playlists = df_source.select(
#     col("pid").alias("playlist_id"),
#     col("name").alias("playlist_name"),
#     col("num_tracks"),
#     col("num_followers"),
#     col("collaborative"),
#     col("modified_at")
# ).distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Track Transformations

# COMMAND ----------

# TODO: Create track dimension table
# df_tracks = df_source.select(
#     explode(col("tracks")).alias("track")
# ).select(
#     col("track.track_uri").alias("track_id"),
#     col("track.track_name"),
#     col("track.artist_uri").alias("artist_id"),
#     col("track.artist_name"),
#     col("track.album_uri").alias("album_id"),
#     col("track.album_name"),
#     col("track.duration_ms")
# ).distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Artist Transformations

# COMMAND ----------

# TODO: Create artist dimension table
# df_artists = df_tracks.select(
#     col("artist_id"),
#     col("artist_name")
# ).distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Album Transformations

# COMMAND ----------

# TODO: Create album dimension table
# df_albums = df_tracks.select(
#     col("album_id"),
#     col("album_name"),
#     col("artist_id")
# ).distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 Playlist-Track Fact Table

# COMMAND ----------

# TODO: Create fact table linking playlists and tracks
# df_playlist_tracks = df_source.select(
#     col("pid").alias("playlist_id"),
#     explode(col("tracks")).alias("track")
# ).select(
#     col("playlist_id"),
#     col("track.track_uri").alias("track_id"),
#     col("track.pos").alias("position")
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Load

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Define Target Paths

# COMMAND ----------

# TODO: Define target Delta Lake paths
# target_base_path = "/mnt/delta/spotify_mpd_warehouse/"
# playlists_path = f"{target_base_path}/dim_playlists"
# tracks_path = f"{target_base_path}/dim_tracks"
# artists_path = f"{target_base_path}/dim_artists"
# albums_path = f"{target_base_path}/dim_albums"
# fact_path = f"{target_base_path}/fact_playlist_tracks"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Write to Delta Lake

# COMMAND ----------

# TODO: Write dimension tables
# df_playlists.write.format("delta").mode("overwrite").save(playlists_path)
# df_tracks.write.format("delta").mode("overwrite").save(tracks_path)
# df_artists.write.format("delta").mode("overwrite").save(artists_path)
# df_albums.write.format("delta").mode("overwrite").save(albums_path)

# TODO: Write fact table
# df_playlist_tracks.write.format("delta").mode("overwrite").save(fact_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validation

# COMMAND ----------

# TODO: Validate loaded data
# print("Playlists count:", spark.read.format("delta").load(playlists_path).count())
# print("Tracks count:", spark.read.format("delta").load(tracks_path).count())
# print("Artists count:", spark.read.format("delta").load(artists_path).count())
# print("Albums count:", spark.read.format("delta").load(albums_path).count())
# print("Playlist-Track associations:", spark.read.format("delta").load(fact_path).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create Delta Tables (Optional)

# COMMAND ----------

# TODO: Register as managed tables in the metastore
# spark.sql("CREATE DATABASE IF NOT EXISTS spotify_mpd")
# 
# spark.sql(f"CREATE TABLE IF NOT EXISTS spotify_mpd.dim_playlists USING DELTA LOCATION '{playlists_path}'")
# spark.sql(f"CREATE TABLE IF NOT EXISTS spotify_mpd.dim_tracks USING DELTA LOCATION '{tracks_path}'")
# spark.sql(f"CREATE TABLE IF NOT EXISTS spotify_mpd.dim_artists USING DELTA LOCATION '{artists_path}'")
# spark.sql(f"CREATE TABLE IF NOT EXISTS spotify_mpd.dim_albums USING DELTA LOCATION '{albums_path}'")
# spark.sql(f"CREATE TABLE IF NOT EXISTS spotify_mpd.fact_playlist_tracks USING DELTA LOCATION '{fact_path}'")
