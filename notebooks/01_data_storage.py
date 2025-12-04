# Databricks notebook source
# MAGIC %md
# MAGIC # Data Storage
# MAGIC This notebook handles loading and storing the Spotify Million Playlist Dataset (MPD) into Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Configuration

# COMMAND ----------

from pyspark.sql import SparkSession

# NOTE: SparkSession is pre-initialized in Databricks as 'spark'
# For local development, uncomment the following:
# spark = SparkSession.builder.appName("SpotifyMPD-DataStorage").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Raw Data
# MAGIC 
# MAGIC Load the JSON files from the Spotify MPD dataset.

# COMMAND ----------

# TODO: Define path to raw data files
# raw_data_path = "/mnt/data/spotify_mpd/"

# TODO: Load JSON files into DataFrame
# df_raw = spark.read.json(raw_data_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Save to Delta Lake
# MAGIC 
# MAGIC Store the data in Delta Lake format for efficient querying.

# COMMAND ----------

# TODO: Define Delta table path
# delta_path = "/mnt/delta/spotify_mpd/"

# TODO: Write DataFrame to Delta Lake
# df_raw.write.format("delta").mode("overwrite").save(delta_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify Data Storage

# COMMAND ----------

# TODO: Read back and verify data
# df_verify = spark.read.format("delta").load(delta_path)
# df_verify.printSchema()
# print(f"Total records: {df_verify.count()}")
