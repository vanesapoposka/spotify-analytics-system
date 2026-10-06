# Spotify Analytics Data Warehouse

Everything needed to run this from zero: the dataset (already included, no
download needed), a Dockerized Postgres, all ETL/SCD2/OLAP/ML code, and the
interactive dashboard.

## What's inside
```
spotify_dwh/
├── .env                    <- DB connection settings (edit if needed)
├── docker-compose.yml      <- Postgres 16, one command to start
├── requirements.txt        <- pinned Python dependencies
├── setup.sh                <- one-command automated setup (Linux/Mac/WSL)
├── data/spotify_songs.csv  <- song catalog (32,833 Spotify tracks) — already included
├── data/spotify_listening_stats.csv <- fixed daily listening-statistics demo data
├── sql/                    <- Bronze/Silver/Gold + warehouse DDL, metrics, and OLAP queries
├── etl/                    <- raw ingestion, Silver cleaning, Gold loading, DQ, and DB config
├── ml/                     <- listener clustering, trend prediction, and personalized recommendations
├── dashboard/               <- published interactive dashboard (open directly in a browser)
├── data_vault/              <- alternative Data Vault 2.0 model (DDL only, optional)
└── airflow_dags/            <- Airflow orchestration (optional, see bottom)
```

## Prerequisites
- **Docker Desktop** (or Docker Engine + the `docker compose` plugin) — https://docs.docker.com/get-docker/
- **Python 3.10–3.12** with the Windows `py` launcher or `python3 -m venv` available (the pinned pandas 2.1.4 dependency does not support Python 3.13)
- On Windows: use **WSL2** or **Git Bash** to run `setup.sh` (native PowerShell won't run the `.sh` script — see the manual steps below if you'd rather not use either)

---

## Datasets
The checked-in song catalog is based on the TidyTuesday Spotify playlist
dataset: https://github.com/rfordatascience/tidytuesday. The listening
statistics CSV is a fixed demo dataset included in `data/`; it supplies the
daily user/track activity used by the warehouse, dashboard, and ML steps.

## Medallion data flow
The PostgreSQL database has explicit `bronze`, `silver`, and `gold` schemas.
Bronze stores each original CSV record as JSONB with its source filename and
row number. Silver stores typed and validated catalog/listening rows; invalid
records and the rule that rejected them are retained in `silver.dq_rejects`.
The existing dimensional warehouse is loaded internally into `dwh`, and
read-only curated views in `gold` expose its dimensions and facts to OLAP, ML,
login, and recommendation queries. Thus consumers read Gold while ETL loaders
write the core warehouse.

## Quickest start (recommended)

```bash
cd spotify_dwh
chmod +x setup.sh
./setup.sh
```

This single command will:
1. Start a Postgres 16 container (Docker Compose)
2. Create a Python virtual environment and install pinned dependencies
3. Create the Bronze, Silver, Gold, and dimensional warehouse schemas
4. Preserve both source CSVs in Bronze and clean/validate them into Silver
5. Load the Gold-facing dimensional warehouse, listening facts, and weighted artist metrics
6. Run OLAP queries and the ML layer (listener clustering, trending-song prediction, weekday totals)

It takes 2-5 minutes depending on your machine. You'll see progress printed for every step — if anything fails, the script stops immediately and shows the error.

---

## Manual steps (equivalent, if you don't want to use setup.sh)

Run the commands from the project directory. In **PowerShell**, use the Windows
Python launcher (`py`) and activate the PowerShell script:

```powershell
# 1. Start Postgres
docker compose up -d
docker compose ps        # wait until "healthy"

# 2. Python environment
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt

# 3. Create medallion and dimensional schemas
docker compose exec -T postgres psql -U postgres -d spotify_dwh -f /sql/00_medallion_layers.sql
docker compose exec -T postgres psql -U postgres -d spotify_dwh -f /sql/01_star_schema.sql

# 4. Ingest Bronze, validate and clean into Silver
python etl/prepare_medallion.py

# 5. Load the Gold dimensional warehouse (SCD2 and artist bridge)
python etl/load_dwh.py

# 6. Load Silver listening statistics + derive Gold metrics and OLAP results
python etl/load_listening_stats.py
docker compose exec -T postgres psql -U postgres -d spotify_dwh -f /sql/02_derive_weighted_metrics.sql
docker compose exec -T postgres psql -U postgres -d spotify_dwh -f /sql/03_olap_queries.sql

# 7. Run ML layer
python ml/run_ml.py
```

In **bash** (Linux, macOS, WSL, or Git Bash), use:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Then run the schema, medallion preparation, Gold loaders, and ML commands above, replacing
`python` with `python3` if that is how Python is installed on your system.

---

## How to check it worked

**1. Row counts** — should roughly match:
```bash
docker compose exec postgres psql -U postgres -d spotify_dwh -c "
SELECT 'dim_song' t, count(*) FROM gold.dim_song WHERE is_current
UNION ALL SELECT 'dim_artist', count(*) FROM gold.dim_artist WHERE is_current
UNION ALL SELECT 'dim_user', count(*) FROM gold.dim_user
UNION ALL SELECT 'fact_stream', count(*) FROM gold.fact_stream
UNION ALL SELECT 'fact_song_artist_daily', count(*) FROM gold.fact_song_artist_daily;"
```
Expect: ~28,000 songs · ~10,800 artists · 4,000 users · ~375,000 stream rows · a similar number of weighted-metric rows.

**2. The weighted vs. unweighted logic** — pick any multi-artist song and confirm the weighted streams sum back to the true total:
```bash
docker compose exec postgres psql -U postgres -d spotify_dwh -c "
SELECT ds.track_name, da.artist_name, fsad.unweighted_streams, fsad.weighted_streams
FROM gold.fact_song_artist_daily fsad
JOIN gold.dim_song ds ON ds.song_sk = fsad.song_sk
JOIN gold.dim_artist da ON da.artist_sk = fsad.artist_sk
WHERE fsad.song_sk IN (SELECT song_sk FROM gold.bridge_song_artist GROUP BY song_sk HAVING count(*) > 1)
ORDER BY ds.track_name LIMIT 6;"
```
Each artist's `weighted_streams` should be the song's true stream count divided by the number of credited artists.

**3. OLAP queries** — run the rollup/drilldown/slice-dice/KPI examples:
```bash
docker compose exec postgres psql -U postgres -d spotify_dwh -f /sql/03_olap_queries.sql
```

**4. ML results** — run `python3 ml/run_ml.py`. It writes `ml/ml_results.json` with listener clusters, top predicted trending songs, and streams by weekday.

**5. Dashboard** — on Windows, run `.\dashboard\serve_dashboard.ps1` from the project directory and open the URL it prints. The Machine Learning section shows listener clusters, trending-song predictions, and weekday stream totals from `ml/ml_results.json`. Run `python ml/run_ml.py` to refresh those results, then reload the page. The other dashboard charts remain a static snapshot and do not refresh automatically from the database. Press Ctrl+C in the PowerShell window to stop the local server.

### Demo login and personalized recommendations

Aggregate warehouse and ML analytics are public. Sign in to see song, album,
and artist recommendations based on that
listener's play history from the supplied listening-statistics dataset and similar listeners. The hybrid recommender
blends collaborative filtering with similarity across song audio features.

The listening-statistics dataset contains demo usernames `U000000` through `U003999`.
For example, try `U000001`, `U000042`, or `U000123`; each uses the demo password
`password123`. Login checks the username against the current `gold.dim_user`
records, so the warehouse must be running and populated. All demo accounts use
the same password as requested. To change it for this local server, set
`SPOTIFY_DEMO_PASSWORD` before starting the dashboard. Sessions are held by the
local server and expire after eight hours. The launcher uses port 8000 when it
is free; if another server is already using it, it selects the next free port
and prints the URL to open.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `docker compose ps` never shows "healthy" | Run `docker compose logs postgres` — usually a port conflict on 5432. Change `POSTGRES_PORT` in `.env` and re-run. |
| `psql: FATAL: password authentication failed` | Make sure `.env` matches what's in `docker-compose.yml` (they read the same file) — if you edited one, edit both, or better, only edit `.env`. |
| DataGrip connects but shows no project tables | The project data is in `bronze`, `silver`, `dwh`, and `gold`, not `public`. In Database Explorer, enable those schemas in the schema selector, then refresh with Ctrl+F5. |
| Python reports password authentication failed for `localhost` (`::1`) | Set `POSTGRES_HOST=127.0.0.1` in `.env` so Windows connects to Docker's IPv4-published port, then retry. |
| `source` is not recognized | `source` is a bash command. In PowerShell, activate with ` .\\.venv\\Scripts\\Activate.ps1`; in Command Prompt use `.venv\\Scripts\\activate.bat`. |
| `ModuleNotFoundError` running any `.py` script | Activate the venv for your shell (`.\\.venv\\Scripts\\Activate.ps1` in PowerShell, `source .venv/bin/activate` in bash), then run `python -m pip install -r requirements.txt`. |
| ETL script errors on `to_sql` / SQLAlchemy | Confirm `requirements.txt` installed the **pinned** versions — pandas ≥3.0 breaks against SQLAlchemy 1.4.x, which this project (and Airflow, if you add it) requires. |
| Want a clean slate | `docker compose down -v` (deletes the DB volume) then re-run `./setup.sh`. |

---

## Optional: Airflow orchestration
**Windows users: install and run Airflow inside WSL2, not from native PowerShell.** Apache Airflow does not support native Windows installs. The distribution name to install is `apache-airflow` (not `airflow`). Use the constraints file that matches Python inside WSL.

The active DAG is `airflow_dags/spotify_dwh_dag_RUNNABLE_VERSION.py`. It runs seven steps: create schemas, prepare Bronze and Silver, load catalog dimensions, load listening facts, derive weighted metrics, run OLAP, and refresh ML results. `airflow_dags/spotify_dwh_dag.py` is a commented design sketch, not a runnable DAG.

To run it from an Ubuntu WSL terminal (this project is at `/mnt/c/Desktop/spotify_dwh_complete` on this machine):
```bash
sudo apt update
sudo apt install -y python3-pip python3-venv postgresql-client
cd /mnt/c/Desktop/spotify_dwh_complete
python3 -m venv ~/spotify-airflow-venv
source ~/spotify-airflow-venv/bin/activate
python -m pip install --upgrade pip
export AIRFLOW_HOME=~/airflow
export SPOTIFY_DWH_PROJECT_DIR=/mnt/c/Desktop/spotify_dwh_complete
AIRFLOW_VERSION=2.10.3
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
python -m pip install "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-postgres --constraint "$CONSTRAINT_URL"
python -m pip install -r "$SPOTIFY_DWH_PROJECT_DIR/requirements.txt"
bash airflow_wsl.sh db migrate
bash airflow_wsl.sh dags list-import-errors
# Run only when you intend to rebuild the warehouse from the CSV:
bash airflow_wsl.sh dags test spotify_dwh_daily "$(date +%F)"
```
This WSL venv is separate from the Windows `.venv`. The constraints URL is built from the venv's actual Python version. `airflow_wsl.sh` sets the project and Airflow paths and copies the active DAG into Airflow's DAG folder each time. The runnable DAG is manual-only because its schema task drops and recreates the medallion/warehouse schemas; replace the source CSVs before running it if you want to load a new snapshot.
