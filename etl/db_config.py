"""
Centralized Postgres connection for the whole project.
Reads from environment variables (populated from .env via python-dotenv),
so the SAME code works whether Postgres is a local Docker container,
a cloud instance, or a bare-metal install — just change .env.
"""
import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Load .env from the project root regardless of which script imports this
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

DB_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "spotify_dwh")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
PSQL_CONN_STRING = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def get_engine():
    return create_engine(DATABASE_URL)
