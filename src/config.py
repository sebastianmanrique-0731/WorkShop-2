"""Central configuration. Everything comes from environment variables (see .env.example)."""
from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name} (see .env.example)")
    return value


def db_params(database_env: str) -> dict:
    """psycopg2 connection kwargs for the data server. `database_env` is SOURCE_DB_NAME or DW_DB_NAME."""
    return {
        "host": _env("DATA_DB_HOST", "localhost"),
        "port": int(_env("DATA_DB_PORT", "5433")),
        "user": _env("DATA_DB_USER"),
        "password": _env("DATA_DB_PASSWORD"),
        "dbname": _env(database_env),
    }


def source_db_params() -> dict:
    return db_params("SOURCE_DB_NAME")


def dw_db_params() -> dict:
    return db_params("DW_DB_NAME")


def spotify_csv_path() -> Path:
    return Path(_env("SPOTIFY_CSV_PATH", str(PROJECT_ROOT / "data/raw/spotify_dataset.csv")))


def grammy_csv_path() -> Path:
    return Path(_env("GRAMMY_CSV_PATH", str(PROJECT_ROOT / "data/raw/the_grammy_awards.csv")))
