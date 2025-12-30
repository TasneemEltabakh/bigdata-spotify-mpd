# Spotify Analytics Dashboard - Docker Setup

## Quick Start

1. **Create environment file:**
   ```bash
   cp .env.example .env
   ```
   Then edit `.env` with your Databricks credentials.

2. **Build and run:**
   ```bash
   docker-compose up --build
   ```

3. **Access the dashboard:**
   Open http://localhost:8501 in your browser

## Commands

- **Start (detached):** `docker-compose up -d`
- **Stop:** `docker-compose down`
- **View logs:** `docker-compose logs -f`
- **Rebuild:** `docker-compose up --build`
- **Remove volumes:** `docker-compose down -v`

## Production Deployment

For production, comment out the volume mounts in `docker-compose.yml` to avoid live code reloading.

## Environment Variables

Required Databricks credentials in `.env`:
- `DATABRICKS_SERVER_HOSTNAME`
- `DATABRICKS_HTTP_PATH`
- `DATABRICKS_TOKEN`
