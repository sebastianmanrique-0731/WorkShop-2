"""Load layer (Workshop §6.10 and §7.3): validated prepared datasets -> star schema in music_dw.

Strategy for safe reruns: ONE transaction per load.
    1. pg_advisory_xact_lock      -> two loads can never interleave.
    2. sql/dw_schema.sql          -> idempotent DDL (IF NOT EXISTS), so a fresh database also works.
    3. COPY every prepared dataset into TEMP staging tables (dropped at commit).
    4. Dimensions: controlled upsert by BUSINESS key (UPDATE only the rows whose attributes changed, INSERT the new
       ones). Surrogate keys of existing members never change, so a rerun keeps every key stable.
    5. Facts and bridges: replaced (TRUNCATE + INSERT) with keys resolved through the business keys. The DW
       represents the latest validated batch; the primary key of every fact is its grain, so a duplicate is
       physically impossible even if this code were wrong.
    6. Dimension members that the batch no longer contains are deleted (the facts that used them are gone).
    7. Reconciliation: DW row counts must equal the prepared row counts. Any difference raises LoadContractError
       and the whole transaction rolls back.
    8. One row in etl_load_audit (batch id, counts, measures) is committed together with the data.

Failure semantics
    * Any error before COMMIT rolls back everything: the DW keeps the previous batch, never a half-loaded one.
      That is why the Airflow task may retry transient database errors (psycopg2.OperationalError) safely.
    * LoadContractError and integrity errors are deterministic: the task fails without retry.
"""
from __future__ import annotations

import io
import json
import logging
import os
from datetime import datetime, timezone

import pandas as pd
import psycopg2

from src.config import PROJECT_ROOT, dw_db_params

LOG = logging.getLogger("load")

SCHEMA_SQL = PROJECT_ROOT / "sql" / "dw_schema.sql"
EVIDENCE_DIR = PROJECT_ROOT / "docs" / "evidence" / "load"
LOCK_KEY = 2_026_002                      # any constant: identifies "the DW load" for pg_advisory_xact_lock

DW_TABLES = ["dim_artist", "dim_track", "dim_genre", "dim_category", "dim_year",
             "fact_track_artist", "fact_grammy_award", "bridge_artist_genre", "bridge_award_track"]

# Measures compared before and after every load (§7.3: "selected measures before and after the rerun")
MEASURES_SQL = {
    "fact_track_artist_avg_popularity": "SELECT ROUND(AVG(popularity), 4) FROM fact_track_artist",
    "fact_grammy_award_total_wins": "SELECT COALESCE(SUM(win_count), 0) FROM fact_grammy_award",
    "fact_grammy_award_matched_rows": "SELECT COUNT(*) FILTER (WHERE is_track_matched) FROM fact_grammy_award",
    "dim_artist_grammy_winners": "SELECT COUNT(*) FILTER (WHERE is_grammy_winner) FROM dim_artist",
}


class LoadContractError(RuntimeError):
    """Deterministic load failure (gate not passed, reconciliation mismatch). Retrying cannot fix it."""


# --------------------------------------------------------------------------------------
# Staging tables: prepared dataset -> TEMP table (column order = COPY order)
# --------------------------------------------------------------------------------------
STAGING = {
    "stg_artist": ("prepared_artist", [
        ("artist_norm", "TEXT"), ("artist_name", "TEXT"), ("in_spotify", "BOOLEAN"), ("is_grammy_winner", "BOOLEAN"),
        ("grammy_win_count", "INTEGER"), ("first_grammy_year", "INTEGER")]),
    "stg_track_artist": ("prepared_track_artist", [
        ("track_id", "TEXT"), ("artist_norm", "TEXT"), ("artist_position", "SMALLINT"), ("is_primary_artist", "BOOLEAN"),
        ("popularity", "NUMERIC(5,2)"), ("is_high_popularity", "SMALLINT"), ("danceability", "REAL"), ("energy", "REAL"),
        ("valence", "REAL"), ("loudness_db", "REAL"), ("tempo_bpm", "REAL"), ("duration_ms", "INTEGER"),
        ("track_name", "TEXT"), ("album_name", "TEXT"), ("explicit", "BOOLEAN")]),
    "stg_category": ("prepared_category", [("category_key", "TEXT"), ("category_name", "TEXT")]),
    "stg_year": ("prepared_year", [("year", "INTEGER"), ("decade", "INTEGER"), ("ceremony_no", "INTEGER"),
                                   ("ceremony_title", "TEXT")]),
    "stg_award": ("prepared_grammy_award", [
        ("grammy_row_id", "INTEGER"), ("year", "INTEGER"), ("category_key", "TEXT"), ("artist_norm", "TEXT"),
        ("nominee", "TEXT"), ("win_count", "SMALLINT"), ("is_matchable", "BOOLEAN"), ("is_track_matched", "BOOLEAN"),
        ("matched_track_count", "INTEGER")]),
    "stg_award_track": ("prepared_award_track", [("grammy_row_id", "INTEGER"), ("track_id", "TEXT"), ("match_type", "TEXT")]),
    "stg_track_genre": ("prepared_track_genre", [("track_id", "TEXT"), ("track_genre", "TEXT")]),
}
_INT_TYPES = {"INTEGER", "SMALLINT"}


def _copy(cur, table: str, df: pd.DataFrame, columns: list[tuple[str, str]]) -> None:
    cols = [c for c, _ in columns]
    cur.execute(f"CREATE TEMP TABLE {table} ({', '.join(f'{c} {t}' for c, t in columns)}) ON COMMIT DROP")
    out = df[cols].copy()
    for c, t in columns:
        if t in _INT_TYPES:                               # 210000.0 -> 210000; NaN stays NULL
            out[c] = pd.to_numeric(out[c]).round().astype("Int64")
    buf = io.StringIO()
    out.to_csv(buf, index=False, header=False, na_rep="")
    buf.seek(0)
    cur.copy_expert(f"COPY {table} ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv, NULL '')", buf)


# --------------------------------------------------------------------------------------
# Load statements (executed in this order inside one transaction)
# --------------------------------------------------------------------------------------
TRUNCATE_FACTS = "TRUNCATE bridge_award_track, bridge_artist_genre, fact_grammy_award, fact_track_artist"

UPSERT_DIMENSIONS = {
    "dim_artist": (
        """UPDATE dim_artist d SET artist_name = s.artist_name, in_spotify = s.in_spotify,
                  is_grammy_winner = s.is_grammy_winner, grammy_win_count = s.grammy_win_count,
                  first_grammy_year = s.first_grammy_year
           FROM stg_artist s
           WHERE d.artist_norm = s.artist_norm
             AND (d.artist_name, d.in_spotify, d.is_grammy_winner, d.grammy_win_count, d.first_grammy_year)
                 IS DISTINCT FROM (s.artist_name, s.in_spotify, s.is_grammy_winner, s.grammy_win_count, s.first_grammy_year)""",
        """INSERT INTO dim_artist (artist_norm, artist_name, in_spotify, is_grammy_winner, grammy_win_count, first_grammy_year)
           SELECT s.artist_norm, s.artist_name, s.in_spotify, s.is_grammy_winner, s.grammy_win_count, s.first_grammy_year
           FROM stg_artist s
           WHERE NOT EXISTS (SELECT 1 FROM dim_artist d WHERE d.artist_norm = s.artist_norm)
           ORDER BY s.artist_norm"""),
    "dim_track": (
        """UPDATE dim_track d SET track_name = s.track_name, album_name = s.album_name, explicit = s.explicit
           FROM (SELECT DISTINCT track_id, track_name, album_name, explicit FROM stg_track_artist) s
           WHERE d.track_id = s.track_id
             AND (d.track_name, d.album_name, d.explicit) IS DISTINCT FROM (s.track_name, s.album_name, s.explicit)""",
        """INSERT INTO dim_track (track_id, track_name, album_name, explicit)
           SELECT DISTINCT s.track_id, s.track_name, s.album_name, s.explicit
           FROM stg_track_artist s
           WHERE NOT EXISTS (SELECT 1 FROM dim_track d WHERE d.track_id = s.track_id)
           ORDER BY s.track_id"""),
    "dim_genre": (
        None,
        """INSERT INTO dim_genre (genre_name)
           SELECT DISTINCT s.track_genre FROM stg_track_genre s
           WHERE NOT EXISTS (SELECT 1 FROM dim_genre d WHERE d.genre_name = s.track_genre)
           ORDER BY s.track_genre"""),
    "dim_category": (
        """UPDATE dim_category d SET category_name = s.category_name
           FROM stg_category s
           WHERE d.category_norm = s.category_key AND d.category_name IS DISTINCT FROM s.category_name""",
        """INSERT INTO dim_category (category_norm, category_name)
           SELECT s.category_key, s.category_name FROM stg_category s
           WHERE NOT EXISTS (SELECT 1 FROM dim_category d WHERE d.category_norm = s.category_key)
           ORDER BY s.category_key"""),
    "dim_year": (
        """UPDATE dim_year d SET decade = s.decade, ceremony_no = s.ceremony_no, ceremony_title = s.ceremony_title
           FROM stg_year s
           WHERE d.year_key = s.year
             AND (d.decade, d.ceremony_no, d.ceremony_title) IS DISTINCT FROM (s.decade, s.ceremony_no, s.ceremony_title)""",
        """INSERT INTO dim_year (year_key, decade, ceremony_no, ceremony_title)
           SELECT s.year, s.decade, s.ceremony_no, s.ceremony_title FROM stg_year s
           WHERE NOT EXISTS (SELECT 1 FROM dim_year d WHERE d.year_key = s.year)"""),
}

INSERT_FACTS = {
    "fact_track_artist": """
        INSERT INTO fact_track_artist (track_key, artist_key, artist_position, is_primary_artist, popularity,
                                       is_high_popularity, danceability, energy, valence, loudness_db, tempo_bpm, duration_ms)
        SELECT t.track_key, a.artist_key, s.artist_position, s.is_primary_artist, s.popularity, s.is_high_popularity,
               s.danceability, s.energy, s.valence, s.loudness_db, s.tempo_bpm, s.duration_ms
        FROM stg_track_artist s
        JOIN dim_track  t ON t.track_id    = s.track_id
        JOIN dim_artist a ON a.artist_norm = s.artist_norm""",
    "fact_grammy_award": """
        INSERT INTO fact_grammy_award (grammy_row_id, year_key, category_key, artist_key, nominee, win_count,
                                       is_matchable, is_track_matched, matched_track_count)
        SELECT s.grammy_row_id, s.year, c.category_key, COALESCE(a.artist_key, 0), s.nominee, s.win_count,
               s.is_matchable, s.is_track_matched, s.matched_track_count
        FROM stg_award s
        JOIN dim_category  c ON c.category_norm = s.category_key
        LEFT JOIN dim_artist a ON a.artist_norm = s.artist_norm""",
    "bridge_artist_genre": """
        INSERT INTO bridge_artist_genre (artist_key, genre_key, track_count)
        SELECT a.artist_key, g.genre_key, COUNT(DISTINCT s.track_id)
        FROM stg_track_genre tg
        JOIN stg_track_artist s ON s.track_id    = tg.track_id
        JOIN dim_artist       a ON a.artist_norm = s.artist_norm
        JOIN dim_genre        g ON g.genre_name  = tg.track_genre
        GROUP BY a.artist_key, g.genre_key""",
    "bridge_award_track": """
        INSERT INTO bridge_award_track (grammy_row_id, track_key, match_type)
        SELECT s.grammy_row_id, t.track_key, s.match_type
        FROM stg_award_track s
        JOIN dim_track t ON t.track_id = s.track_id""",
}

DELETE_STALE_MEMBERS = {
    "dim_artist": "DELETE FROM dim_artist d WHERE d.artist_key <> 0 AND NOT EXISTS (SELECT 1 FROM stg_artist s WHERE s.artist_norm = d.artist_norm)",
    "dim_track": "DELETE FROM dim_track d WHERE NOT EXISTS (SELECT 1 FROM stg_track_artist s WHERE s.track_id = d.track_id)",
    "dim_genre": "DELETE FROM dim_genre d WHERE NOT EXISTS (SELECT 1 FROM stg_track_genre s WHERE s.track_genre = d.genre_name)",
    "dim_category": "DELETE FROM dim_category d WHERE NOT EXISTS (SELECT 1 FROM stg_category s WHERE s.category_key = d.category_norm)",
    "dim_year": "DELETE FROM dim_year d WHERE NOT EXISTS (SELECT 1 FROM stg_year s WHERE s.year = d.year_key)",
}


def expected_counts(frames: dict[str, pd.DataFrame]) -> dict[str, int]:
    """Row counts the DW must hold after loading `frames` (computed from the prepared data, not from the DW)."""
    ta, tg = frames["prepared_track_artist"], frames["prepared_track_genre"]
    artist_genre = tg.merge(ta[["track_id", "artist_norm"]], on="track_id")[["artist_norm", "track_genre"]].drop_duplicates()
    return {
        "dim_artist": len(frames["prepared_artist"]) + 1,                 # + unknown member artist_key = 0
        "dim_track": ta["track_id"].nunique(),
        "dim_genre": tg["track_genre"].nunique(),
        "dim_category": len(frames["prepared_category"]),
        "dim_year": len(frames["prepared_year"]),
        "fact_track_artist": len(ta),
        "fact_grammy_award": len(frames["prepared_grammy_award"]),
        "bridge_artist_genre": len(artist_genre),
        "bridge_award_track": len(frames["prepared_award_track"]),
    }


def _snapshot(cur) -> dict:
    """Row counts and selected measures of the DW (tables may not exist yet on the very first load)."""
    cur.execute("SELECT to_regclass('fact_grammy_award') IS NOT NULL")
    if not cur.fetchone()[0]:
        return {"rows": {t: None for t in DW_TABLES}, "measures": {m: None for m in MEASURES_SQL}}
    rows = {}
    for t in DW_TABLES:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        rows[t] = int(cur.fetchone()[0])
    measures = {}
    for name, sql in MEASURES_SQL.items():
        cur.execute(sql)
        value = cur.fetchone()[0]
        measures[name] = float(value) if value is not None else None
    return {"rows": rows, "measures": measures}


def load_dw(meta: dict, dag_run_id: str | None = None) -> dict:
    """Load the validated prepared batch described by `meta` (output of validate_prepared). Returns a compact summary."""
    from src.transform import load_prepared                    # verifies path + checksum of every prepared file

    gate = meta.get("prepared_gate") or {}
    if not gate.get("gate_passed"):
        raise LoadContractError(f"Batch {meta.get('batch_id')!r} has no passed prepared gate; refusing to load.")

    batch_id = meta["batch_id"]
    frames = load_prepared(meta)
    expected = expected_counts(frames)
    started = datetime.now(timezone.utc)
    changes: dict[str, dict] = {}

    conn = psycopg2.connect(**dw_db_params())              # OperationalError propagates -> transient, Airflow retries
    try:
        with conn:                                          # one transaction: COMMIT on success, ROLLBACK on any error
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_xact_lock(%s)", (LOCK_KEY,))
                before = _snapshot(cur)
                cur.execute(SCHEMA_SQL.read_text(encoding="utf-8"))

                for table, (dataset, columns) in STAGING.items():
                    _copy(cur, table, frames[dataset], columns)

                cur.execute(TRUNCATE_FACTS)
                for dim, (update_sql, insert_sql) in UPSERT_DIMENSIONS.items():
                    updated = 0
                    if update_sql:
                        cur.execute(update_sql)
                        updated = cur.rowcount
                    cur.execute(insert_sql)
                    changes[dim] = {"updated": updated, "inserted": cur.rowcount}
                for fact, sql in INSERT_FACTS.items():
                    cur.execute(sql)
                    changes[fact] = {"inserted": cur.rowcount}
                for dim, sql in DELETE_STALE_MEMBERS.items():
                    cur.execute(sql)
                    changes[dim]["deleted_stale"] = cur.rowcount

                after = _snapshot(cur)
                mismatches = {t: {"expected": expected[t], "loaded": after["rows"][t]}
                              for t in DW_TABLES if after["rows"][t] != expected[t]}
                if mismatches:
                    raise LoadContractError(f"DW reconciliation failed for batch {batch_id}: {mismatches}. Transaction rolled back.")

                cur.execute(
                    """INSERT INTO etl_load_audit (batch_id, dag_run_id, started_at, finished_at, rows_loaded, measures, changes)
                       VALUES (%s, %s, %s, now(), %s, %s, %s) RETURNING load_id""",
                    (batch_id, dag_run_id, started, json.dumps(after["rows"]), json.dumps(after["measures"]), json.dumps(changes)))
                load_id = int(cur.fetchone()[0])
    finally:
        conn.close()

    report = {
        "batch_id": batch_id,
        "dag_run_id": dag_run_id,
        "load_id": load_id,
        "strategy": "single transaction; dimensions upserted by business key (stable surrogate keys); "
                    "facts and bridges replaced; reconciliation before COMMIT",
        "started_at": started.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "prepared_gate": gate,
        "expected_rows": expected,
        "before": before,
        "after": after,
        "changes": changes,
        "reconciled": True,
    }
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    path = EVIDENCE_DIR / f"load__{batch_id}.json"
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    os.replace(tmp, path)

    LOG.info("[load_dw] batch=%s load_id=%s rows=%s | before=%s | measures=%s | changes=%s | evidence=%s",
             batch_id, load_id, after["rows"], before["rows"], after["measures"], changes, path)
    return {"batch_id": batch_id, "load_id": load_id, "rows": after["rows"], "measures": after["measures"],
            "evidence_path": str(path)}

