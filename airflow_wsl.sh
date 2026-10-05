#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
AIRFLOW_VENV="${HOME}/spotify-airflow-venv"

if [[ ! -x "${AIRFLOW_VENV}/bin/airflow" ]]; then
  echo "Airflow is not installed at ${AIRFLOW_VENV}. Run the WSL setup steps in README.md first." >&2
  exit 1
fi

export AIRFLOW_HOME="${HOME}/airflow"
export SPOTIFY_DWH_PROJECT_DIR="${PROJECT_ROOT}"
export PATH="${AIRFLOW_VENV}/bin:${PATH}"

mkdir -p "${AIRFLOW_HOME}/dags"
cp "${PROJECT_ROOT}/airflow_dags/spotify_dwh_dag_RUNNABLE_VERSION.py" "${AIRFLOW_HOME}/dags/"

exec "${AIRFLOW_VENV}/bin/airflow" "$@"
