"""Extraction layer (Workshop §6.7): acquire each source, stage it untouched, return compact metadata.

    Spotify  <- CSV file                          (data/raw/spotify_dataset.csv)
    Grammy   <- PostgreSQL table grammy_awards    (NEVER the Grammy CSV: the CSV only prepares the source DB)

Rules
  * No premature cleaning: nothing is trimmed, cased, de-duplicated or imputed, so the raw gate sees the real
    source quality.
  * Each extract is staged as Parquet under data/staging/<batch_id>/ and described by a compact dict
    (path, rows, columns, SHA-256, origin, batch id). Tasks exchange that dict, never data frames.
  * Staging is atomic (tmp + rename) and keyed by batch_id: a rerun of the same batch overwrites the same file.
  * Contract problems (missing file, empty/missing table, unknown fault, checksum mismatch) raise
    ExtractionContractError: deterministic, never retried. Connection errors are NOT caught here, so Airflow
    can retry them (transient operational failures).
  * Controlled fault injection (Test B) changes ONE value after a clean extraction (or, for the prepared fault, after
    a clean transformation) and records it in the metadata.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from pathlib import Path

import pandas as pd
import psycopg2

from src.config import PROJECT_ROOT, grammy_csv_path, source_db_params, spotify_csv_path

LOG = logging.getLogger("extract")

STAGING_ROOT = PROJECT_ROOT / "data" / "staging"

GRAMMY_QUERY = (
    "SELECT grammy_row_id, year, title, published_at, updated_at, category, nominee, artist, workers, img, winner "
    "FROM grammy_awards ORDER BY grammy_row_id"
)

# fault name -> (source, rule violated). Ids must match src/validation.py (checked by tests/test_extract.py).
FAULTS = {
    "spotify_popularity_out_of_range": ("spotify", "DQ-03"),
    "grammy_year_out_of_range": ("grammy", "DQ-08"),
    # applied by transform_and_integrate after a clean transformation: proves that validate_prepared gates load_dw
    "prepared_popularity_out_of_range": ("prepared", "DQ-12"),
}


class ExtractionContractError(RuntimeError):
    """Deterministic extraction failure. Retrying cannot fix it."""


def batch_id_from_run_id(run_id: str) -> str:
    """Airflow run_id (e.g. manual__2026-10-08T12:00:00+00:00) -> filesystem-safe batch id."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with Path(path).open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _stage(df: pd.DataFrame, source: str, batch_id: str, extra: dict | None = None) -> dict:
    """Write `df` atomically to data/staging/<batch_id>/raw_<source>.parquet and return the compact metadata."""
    out_dir = STAGING_ROOT / batch_id
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{source}_raw.parquet"
    tmp = path.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, index=False)
    os.replace(tmp, path)
    meta = {
        "source": source,
        "batch_id": batch_id,
        "staged_path": str(path),
        "staged_sha256": sha256_file(path),
        "rows": int(len(df)),
        "columns": list(df.columns),
        **(extra or {}),
    }
    meta_path = out_dir / f"{source}_raw.meta.json"                  # kept next to the data so the batch can be investigated
    tmp_meta = meta_path.with_suffix(".json.tmp")
    tmp_meta.write_text(json.dumps(meta, indent=2, default=str), encoding="utf-8")
    os.replace(tmp_meta, meta_path)
    return meta


def load_staged(meta: dict) -> pd.DataFrame:
    """Re-open a staged raw file and verify it is the one that was extracted."""
    path = Path(meta["staged_path"])
    if not path.is_file():
        raise ExtractionContractError(f"Staged file {path} is missing; rerun the extract task.")
    if sha256_file(path) != meta["staged_sha256"]:
        raise ExtractionContractError(f"Staged file {path} changed after extraction (checksum mismatch); rerun the extract task.")
    return pd.read_parquet(path)


# --------------------------------------------------------------------------------------
# Controlled faults (Test B)
# --------------------------------------------------------------------------------------
def apply_fault(df: pd.DataFrame, source: str, fault: str | None) -> tuple[pd.DataFrame, dict]:
    """Inject one Critical violation for `source` when `fault` targets it. Returns (df, metadata)."""
    if fault in (None, "", "none"):
        return df, {"fault_injected": None, "fault_rule": None}
    if fault not in FAULTS:
        raise ExtractionContractError(f"Unknown fault_injection {fault!r}; expected one of {['none', *FAULTS]}")
    target, rule = FAULTS[fault]
    if target != source:
        return df, {"fault_injected": None, "fault_rule": None}      # the other branch is untouched
    df = df.copy()
    if fault == "spotify_popularity_out_of_range":
        df.loc[df.index[5], "popularity"] = 150
    elif fault == "grammy_year_out_of_range":
        df.loc[df.index[3], "year"] = 1850
    else:                                                      # prepared_track_artist after transformation
        df.loc[df.index[0], "popularity"] = 150.0
    LOG.warning("CONTROLLED FAULT INJECTED: %s -> will violate %s (source=%s)", fault, rule, source)
    return df, {"fault_injected": fault, "fault_rule": rule}


# --------------------------------------------------------------------------------------
# Extractors
# --------------------------------------------------------------------------------------
def read_spotify_csv(path: Path | None = None) -> pd.DataFrame:
    path = Path(path or spotify_csv_path())
    if not path.is_file():
        raise ExtractionContractError(f"Spotify CSV not found at {path} (check SPOTIFY_CSV_PATH).")
    df = pd.read_csv(path)
    if df.empty:
        raise ExtractionContractError(f"Spotify CSV {path} has no rows.")
    return df.rename(columns={df.columns[0]: "csv_index"})       # the unnamed pandas index column of the file


def extract_spotify(batch_id: str, fault: str | None = None) -> dict:
    df = read_spotify_csv()
    df, fault_meta = apply_fault(df, "spotify", fault)
    meta = _stage(df, "spotify", batch_id, {"origin": "csv_file", "origin_file": spotify_csv_path().name, **fault_meta})
    LOG.info("[extract_spotify] batch=%s rows=%d columns=%d origin=%s fault=%s",
             batch_id, meta["rows"], len(meta["columns"]), meta["origin"], fault_meta["fault_injected"])
    return meta


def read_grammy_table() -> tuple[pd.DataFrame, int]:
    """Read the Grammy table from the relational source database (not from the CSV). Returns (frame, count(*))."""
    with psycopg2.connect(**source_db_params()) as conn:          # OperationalError propagates -> Airflow may retry
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('grammy_awards')")
            if cur.fetchone()[0] is None:
                raise ExtractionContractError(
                    "Table grammy_awards does not exist in the source database. Run: docker compose --profile setup run --rm prepare-source")
            cur.execute("SELECT count(*) FROM grammy_awards")
            db_count = int(cur.fetchone()[0])
            cur.execute(GRAMMY_QUERY)
            df = pd.DataFrame(cur.fetchall(), columns=[c.name for c in cur.description])
    if df.empty:
        raise ExtractionContractError("Table grammy_awards is empty. Run the prepare-source job.")
    if len(df) != db_count:
        raise ExtractionContractError(f"Extracted {len(df)} rows but the table reports count(*) = {db_count}.")
    return df, db_count


def extract_grammys(batch_id: str, fault: str | None = None) -> dict:
    df, db_count = read_grammy_table()
    df, fault_meta = apply_fault(df, "grammy", fault)
    db = source_db_params()["dbname"]
    meta = _stage(df, "grammy", batch_id,
                  {"origin": "postgres_table", "origin_database": db, "origin_table": "grammy_awards",
                   "db_row_count": db_count, **fault_meta})
    LOG.info("[extract_grammys] batch=%s rows=%d (db count=%d) from %s.grammy_awards fault=%s",
             batch_id, meta["rows"], db_count, db, fault_meta["fault_injected"])
    return meta
