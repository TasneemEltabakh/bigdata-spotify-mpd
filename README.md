
Big Data Analytics Project — Spotify Million Playlist Dataset (MPD)
==================================================================

End-to-End Big Data Architecture, Analytics Platform & Recommendation System

==================================================================
1. PROJECT DESCRIPTION
==================================================================
This repository contains a complete, production-style big data analytics
platform built on the Spotify Million Playlist Dataset (MPD). The project
implements a scalable data engineering pipeline, analytical data warehouse,
interactive dashboard, and a bonus recommendation system.

The focus of this project is not only analytics, but also:
- Real-world cloud constraints handling
- Scalable architecture design
- Data governance and reliability
- Deployment-ready engineering practices

==================================================================
2. DATASET OVERVIEW
==================================================================
Dataset: Spotify Million Playlist Dataset (MPD)

Key characteristics:
- ~1,000,000 playlists
- ~2.2M unique tracks
- ~295K unique artists
- Tens of millions of playlist–track interactions after normalization

Storage details:
- Original ZIP size: ~5.5 GB
- Uncompressed size: ~32–34 GB
- 1000 multi-line JSON files

Due to Databricks Free Edition limitations, the dataset was:
- Unzipped externally
- Uploaded once to Amazon S3 as immutable raw data
- Read directly by Spark using multiline JSON ingestion

==================================================================
3. SYSTEM ARCHITECTURE (END-TO-END)
==================================================================
The platform follows a Medallion Architecture with a clear separation
between ingestion, processing, storage, and consumption layers.

-------------------- SYSTEM BLOCK DIAGRAM --------------------

   Spotify MPD (Raw JSON Files)
              |
              v
     Amazon S3 (Raw Storage)
              |
              v
     Bronze Delta Tables
              |
              v
     Silver Delta Tables
     (Cleaned & Normalized)
              |
              v
     Gold Delta Tables
     (Star Schema + KPIs)
              |
              v
     Databricks SQL Warehouse
              |
              v
     Streamlit Analytics Dashboard
              |
              v
     Bonus ML Recommender System

--------------------------------------------------------------

This architecture ensures:
- Scalability for large datasets
- Minimal recomputation
- Clear governance boundaries
- Analytics and ML reuse

==================================================================
4. GOLD LAYER STAR SCHEMA
==================================================================
The Gold layer is modeled using a star schema optimized for analytics.

-------------------- STAR SCHEMA --------------------

                dim_artist
                    |
                    |
              +-----+-----+
              |           |
        dim_track     dim_playlist
              \           //
               \         //
                \       //
             fact_playlist_track

--------------------------------------------------

Fact Table:
- fact_playlist_track
  Grain: One track occurrence in one playlist
  Primary Key:
    - playlist_id
    - track_uri
    - track_position
  Measures:
    - duration_ms
    - track_position

Dimension Tables:
- dim_playlist  : playlist metadata, followers, sizes, timestamps
- dim_track     : track identifiers and titles
- dim_artist    : artist-level attributes

The schema is designed to:
- Minimize join complexity
- Support aggregation-heavy analytics
- Enable ML use cases without redesign

==================================================================
5. DATA PROCESSING PIPELINE
==================================================================
Phase 1 — Data Storage, Cleaning & EDA
- Ingest raw JSON from S3 into Bronze Delta tables
- Flatten nested playlist and track structures
- Clean invalid records and duplicates
- Perform exploratory data analysis

Phase 2 — Analytical Modeling
- Design and implement Gold-layer star schema
- Persist fact and dimension tables
- Validate referential integrity

Phase 3 — Optimization & Governance
- Persist Silver and Gold tables to avoid recomputation
- Handle Databricks rate limits and credit exhaustion
- Secure access using IAM roles and access tokens
- Serve analytics via SQL Warehouse

==================================================================
6. DASHBOARD
==================================================================
A production-style Streamlit dashboard is deployed and publicly accessible.

Dashboard URL:
https://bigdata-spotify-mpd-hcvgyjweudpvaqiubgrkrd.streamlit.app/

Dashboard characteristics:
- Self-explanatory storytelling design
- General-to-specific analytical flow
- Read-only access to Gold tables
- Optimized for Databricks Free Edition limits

Main sections:
- Executive Overview & KPIs
- Data Health Monitoring
- Playlist-Level Analysis
- Content Analysis
- User Engagement Insights
- Creator-Level Insights

==================================================================
7. BONUS: PLAYLIST CONTINUATION RECOMMENDER
==================================================================
An optional recommender system is implemented on top of the Gold layer.

Features:
- Reuses existing star schema
- Popularity-based baseline model
- Item-to-item co-occurrence model
- Evaluated using ranking metrics:
  Precision@K, Recall@K, NDCG@K

This demonstrates a seamless transition from analytics to ML.

==================================================================
8. CONTAINERIZATION & DEPLOYMENT
==================================================================
All components are fully containerized.

Deployment artifacts:
- Dockerfile
- docker-compose.yml

Benefits:
- One-command startup
- Environment consistency
- Easy local or cloud deployment

==================================================================
9. CI/CD PIPELINE
==================================================================
A GitHub Actions CI/CD pipeline is provided to:
- Build Docker images
- Validate configurations
- Support automated deployments

==================================================================
10. REPOSITORY STRUCTURE
==================================================================
.
├── Backend/                Backend services and data access logic
├── Frontend/               Streamlit dashboard
├── Documentation/          Reports, diagrams, presentations
├── data/                   Configuration and metadata
├── docker-compose.yml      Service orchestration
├── Dockerfile              Container definition
├── .github/workflows/      CI/CD pipelines
├── README.txt              Project documentation
└── LICENSE

==================================================================
11. AUTHORS
==================================================================
Tasneem Muhammad
Nada Nabil
Rghda Salah

==================================================================
12. ACKNOWLEDGMENTS
==================================================================
Spotify Research — Million Playlist Dataset
Databricks — Unified analytics platform

==================================================================
END OF DOCUMENTATION
==================================================================
