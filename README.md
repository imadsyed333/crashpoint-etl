# CrashPoint ETL

Geocodes Toronto KSI collision records against municipal address and intersection points, then writes the result to PostGIS.

Exploration work lives in [crashpoint-notebook](https://github.com/imadsyed333/crashpoint-notebook).

## Production (GitHub Action)

The pipeline runs weekly (Monday 06:00 UTC) and on demand via **Actions → etl → Run workflow**.

You need:

1. A Postgres database **with PostGIS**, reachable from GitHub-hosted runners (a managed host such as Neon, Supabase, or RDS — not a laptop).
2. `CREATE EXTENSION IF NOT EXISTS postgis;`
3. Repo secret `DATABASE_URL` (include `sslmode=require` for hosted databases).

Each run downloads the City of Toronto CSVs, geocodes collisions, **replaces** the `geocoded_collisions` table, and deletes intermediate parquet files.

## Local run

```sh
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export DATABASE_URL="postgresql://user:pass@host:5432/dbname?sslmode=require"
python -m src.run
```

`python -m src.run` fetches the three Open Data dumps, runs the pipeline, and loads PostGIS. No manual CSV download.

Smoke check (no database, no CKAN):

```sh
python tests/check.py
```

## Optional: Airflow UI

Docker Compose still starts a local Airflow UI if you want to trigger the DAG by hand. The production path does not use it.

```sh
docker compose up --build
```

Open [http://localhost:8080](http://localhost:8080) and trigger `geocode_pipeline`. Raw CSVs must already be in `data/raw/` for that path (`collisions.csv`, `addresses.csv`, `intersections.csv`).
