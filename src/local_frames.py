"""Raw frames built from the preserved CSVs, for DESIGN-TIME evidence and tests only.

The operational pipeline never uses this module: Spotify comes from the CSV through src/extract.py and Grammy comes
from the PostgreSQL source table. These frames reproduce the same shape (same columns, same dtypes) so the quality
rules can be exercised without a database.
"""
from __future__ import annotations

import pandas as pd

from src.config import grammy_csv_path, spotify_csv_path


def spotify_raw() -> pd.DataFrame:
    df = pd.read_csv(spotify_csv_path())
    return df.rename(columns={df.columns[0]: "csv_index"})


def grammy_raw() -> pd.DataFrame:
    df = pd.read_csv(grammy_csv_path())
    df.insert(0, "grammy_row_id", range(1, len(df) + 1))
    df["published_at"] = pd.to_datetime(df["published_at"], utc=True)
    df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True)
    return df


def frames_for_gate(gate: str) -> dict[str, pd.DataFrame]:
    if gate == "raw_spotify":
        return {"spotify_raw": spotify_raw()}
    if gate == "raw_grammy":
        return {"grammy_raw": grammy_raw()}
    raise ValueError("Only the raw gates can run from the CSVs; the prepared gate needs transform_and_integrate output.")
