#!/usr/bin/env bash
# ==============================================================================
# Spotify Analytics DWH — one-command setup.
# Starts Postgres (Docker), applies the schema, loads the dataset, generates
# synthetic listening events, derives weighted/unweighted metrics, and runs
# the ML layer. Safe to re-run: it always rebuilds the warehouse from scratch.
# ==============================================================================
set -euo pipefail
cd "$(dirname "$0")"

echo "== 1/7  Checking prerequisites =="
command -v docker >/dev/null 2>&1 || { echo "ERROR: Docker is not installed. See README.md prerequisites."; exit 1; }
docker compose version >/dev/null 2>&1 || { echo "ERROR: 'docker compose' (v2) not found."; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "ERROR: python3 is not installed."; exit 1; }

set -a; source .env; set +a

echo "== 2/7  Starting Postgres (Docker) =="
docker compose up -d
echo -n "Waiting for Postgres to be healthy"
for i in $(seq 1 30); do
  status=$(docker inspect --format='{{.State.Health.Status}}' spotify_dwh_postgres 2>/dev/null || echo "starting")
  if [ "$status" = "healthy" ]; then echo " -> healthy"; break; fi
  echo -n "."; sleep 2
  if [ "$i" -eq 30 ]; then echo " -> TIMEOUT waiting for Postgres"; exit 1; fi
done

echo "== 3/7  Setting up Python environment =="
if [ ! -d ".venv" ]; then python3 -m venv .venv; fi
source .venv/bin/activate
pip install -q --upgrade pip
pip install -q -r requirements.txt

echo "== 4/7  Applying star-schema DDL =="
docker compose exec -T postgres psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -f /sql/01_star_schema.sql

echo "== 5/7  Running incremental ETL (catalog load + SCD2 + bridge table) =="
python3 etl/load_dwh.py

echo "== 6/7  Generating 60 days of synthetic listening events =="
python3 etl/generate_events.py
docker compose exec -T postgres psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -f /sql/02_derive_weighted_metrics.sql

echo "== 7/7  Running ML layer (clustering, classification, trend, recommender, temporal) =="
python3 ml/run_ml.py

echo ""
echo "=============================================================="
echo " Setup complete. Verify with:"
echo "   docker compose exec postgres psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -c \"SELECT count(*) FROM dwh.fact_stream;\""
echo " Dashboard: open dashboard/spotify_dashboard.html in a browser"
echo "=============================================================="
