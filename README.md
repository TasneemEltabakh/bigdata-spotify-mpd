# Big Data Analytics Project — Spotify Million Playlist Dataset (MPD)

## Project Overview

This project implements a comprehensive big data analytics pipeline for analyzing the Spotify Million Playlist Dataset (MPD) using Databricks and PySpark. The goal is to demonstrate scalable data processing techniques including data storage, cleaning, exploratory data analysis, and ETL pipeline development.

## Dataset

### Spotify Million Playlist Dataset (MPD)

The Spotify Million Playlist Dataset contains **1 million playlists** created by Spotify users, featuring:

- **1,000,000** unique playlists
- **66+ million** unique tracks
- **295,860** unique artists
- **734,684** unique albums

Each playlist includes:
- Playlist metadata (name, number of tracks, number of followers, collaborative status)
- Track information (track URI, name, artist, album, duration)
- Position of each track within the playlist

**Note**: The dataset is not included in this repository due to its size. Download it from the [Spotify Million Playlist Dataset Challenge](https://www.aicrowd.com/challenges/spotify-million-playlist-dataset-challenge).

## Project Structure

```
bigdata-spotify-mpd/
├── notebooks/                    # Databricks notebooks
│   ├── 01_data_storage.py       # Data ingestion and storage
│   ├── 02_data_cleaning_eda.py  # Data cleaning and EDA
│   └── 03_etl_pipeline.py       # ETL pipeline implementation
├── docs/                         # Documentation
│   └── report.tex               # LaTeX project report
├── data/                         # Data directory (empty)
│   └── .gitkeep
├── .gitignore
├── LICENSE
└── README.md
```

## Notebooks

### 1. Data Storage (`01_data_storage.py`)
- Load raw JSON data from the Spotify MPD
- Store data in Delta Lake format for efficient querying
- Verify data integrity after storage

### 2. Data Cleaning & EDA (`02_data_cleaning_eda.py`)
- Assess data quality (missing values, duplicates)
- Clean and transform data
- Exploratory analysis:
  - Playlist statistics
  - Track analysis
  - Artist analysis
  - Album analysis

### 3. ETL Pipeline (`03_etl_pipeline.py`)
- **Extract**: Load data from Delta Lake
- **Transform**: Create dimension and fact tables
  - `dim_playlists`: Playlist dimension
  - `dim_tracks`: Track dimension
  - `dim_artists`: Artist dimension
  - `dim_albums`: Album dimension
  - `fact_playlist_tracks`: Playlist-Track associations
- **Load**: Write to Delta Lake data warehouse

## Technologies

- **[Databricks](https://databricks.com/)**: Unified analytics platform
- **[Apache Spark](https://spark.apache.org/)**: Distributed computing framework
- **[PySpark](https://spark.apache.org/docs/latest/api/python/)**: Python API for Spark
- **[Delta Lake](https://delta.io/)**: Open-source storage layer for data lakes

## Getting Started

### Prerequisites

1. Databricks workspace access
2. Spotify Million Playlist Dataset downloaded
3. Data uploaded to cloud storage (DBFS, S3, Azure Blob, etc.)

### Setup

1. Clone this repository
2. Import notebooks into your Databricks workspace
3. Update file paths in notebooks to match your data location
4. Run notebooks in sequence: `01_data_storage` → `02_data_cleaning_eda` → `03_etl_pipeline`

## Deliverables

- [ ] Data storage implementation in Delta Lake
- [ ] Cleaned dataset with quality documentation
- [ ] Exploratory Data Analysis with visualizations
- [ ] Star schema data warehouse
- [ ] ETL pipeline for data transformation
- [ ] LaTeX report documenting methodology and findings

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Spotify Research](https://research.spotify.com/) for providing the Million Playlist Dataset
- [Databricks](https://databricks.com/) for the unified analytics platform
