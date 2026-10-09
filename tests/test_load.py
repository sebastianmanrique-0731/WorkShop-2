"""Prepared gate (§6.9) and DW load (§6.10, §7.3).

The gate and contract tests need only the CSVs. The load tests need a reachable PostgreSQL (the data server of
docker-compose, host port 5433 by default); they create and use their own database `music_dw_pytest`, never music_dw.
"""
from __future__ import annotations

import os

import pandas as pd
import psycopg2
import pytest

os.environ.setdefault("DATA_DB_USER", "x")
os.environ.setdefault("DATA_DB_PASSWORD", "x")
os.environ.setdefault("SOURCE_DB_NAME", "x")
os.environ.setdefault("DW_DB_NAME", "x")

from src import config, extract, load, transform, validation  # noqa: E402
from src.local_frames import grammy_raw, spotify_raw  # noqa: E402

TEST_DB = "music_dw_pytest"


@pytest.fixture(scope="module")
def staged(tmp_path_factory):
    """Real files -> staged raw -> run_transform, in a temporary staging area (shared by the module)."""
    root = tmp_path_factory.mktemp("load")
    mp = pytest.MonkeyPatch()
    for mod, attr, sub in ((extract, "STAGING_ROOT", "staging"), (transform, "STAGING_ROOT", "staging"),
                           (transform, "EVIDENCE_DIR", "evidence"), (validation, "EVIDENCE_DIR", "evidence"),
                           (load, "EVIDENCE_DIR", "evidence")):
        mp.setattr(mod, attr, root / sub)
    sp_meta = extract._stage(spotify_raw(), "spotify", "t1", {"origin": "test"})
    gr_meta = extract._stage(grammy_raw(), "grammy", "t1", {"origin": "test"})
    yield {"root": root, "sp": sp_meta, "gr": gr_meta, "prepared": transform.run_transform("t1", sp_meta, gr_meta)}
    mp.undo()


# --------------------------------------------------------------------------------------
# validate_prepared
# --------------------------------------------------------------------------------------
def test_prepared_gate_accepts_the_real_batch_and_hands_load_its_metadata(staged):
    accepted = validation.validate_staged_prepared(staged["prepared"])
    gate = accepted["prepared_gate"]
    assert gate["gate_passed"] and gate["failed_critical"] == []
    assert gate["failed_warning"] == []                             # DQ-17 tolerates the 4 album box sets (README §6)
    assert accepted["datasets"] == staged["prepared"]["datasets"]    # load_dw receives the same verified files


def test_prepared_fault_blocks_the_gate_with_exactly_dq12(staged):
    meta = transform.run_transform("t1", staged["sp"], staged["gr"], fault="prepared_popularity_out_of_range")
    assert meta["fault_rule"] == "DQ-12"
    with pytest.raises(validation.DataQualityError) as err:
        validation.validate_staged_prepared(meta)
    assert err.value.report["failed_critical"] == ["DQ-12"]
    transform.run_transform("t1", staged["sp"], staged["gr"])        # restore the clean prepared files for other tests


def test_load_refuses_a_batch_without_a_passed_prepared_gate(staged):
    with pytest.raises(load.LoadContractError):
        load.load_dw(staged["prepared"])                               # no prepared_gate in the metadata
    with pytest.raises(load.LoadContractError):
        load.load_dw({**staged["prepared"], "prepared_gate": {"gate_passed": False}})


def test_expected_counts_follow_the_grain():
    frames = {
        "prepared_artist": pd.DataFrame({"artist_norm": ["a", "b"]}),
        "prepared_track_artist": pd.DataFrame({"track_id": ["t1", "t1", "t2"], "artist_norm": ["a", "b", "a"]}),
        "prepared_track_genre": pd.DataFrame({"track_id": ["t1", "t2", "t2"], "track_genre": ["pop", "pop", "rock"]}),
        "prepared_category": pd.DataFrame({"category_key": ["c"]}),
        "prepared_year": pd.DataFrame({"year": [1990, 1991]}),
        "prepared_grammy_award": pd.DataFrame({"grammy_row_id": [1, 2, 3]}),
        "prepared_award_track": pd.DataFrame({"grammy_row_id": [1], "track_id": ["t1"]}),
    }
    c = load.expected_counts(frames)
    assert c["dim_artist"] == 3                                        # + unknown member 0
    assert c["dim_track"] == 2 and c["dim_genre"] == 2
    assert c["bridge_artist_genre"] == 3                               # (a,pop) (b,pop) (a,rock): t2 does not double (a,pop)
    assert c["fact_track_artist"] == 3 and c["fact_grammy_award"] == 3 and c["bridge_award_track"] == 1


# --------------------------------------------------------------------------------------
# load_dw against PostgreSQL
# --------------------------------------------------------------------------------------
def _server_params() -> dict | None:
    try:
        params = {**config.db_params("DW_DB_NAME"), "dbname": "postgres", "connect_timeout": 3}
        psycopg2.connect(**params).close()
        return params
    except Exception:
        return None


@pytest.fixture(scope="module")
def dw(staged):
    params = _server_params()
    if params is None:
        pytest.skip("Data PostgreSQL server is not reachable (docker compose up -d; DATA_DB_HOST=localhost DATA_DB_PORT=5433)")
    conn = psycopg2.connect(**params)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB}")
        cur.execute(f"CREATE DATABASE {TEST_DB}")
    conn.close()
    mp = pytest.MonkeyPatch()
    mp.setattr(load, "dw_db_params", lambda: {**params, "dbname": TEST_DB})
    yield {**params, "dbname": TEST_DB}
    mp.undo()


def _keys(params) -> dict:
    with psycopg2.connect(**params) as conn, conn.cursor() as cur:
        cur.execute("SELECT md5(string_agg(artist_norm || ':' || artist_key, ',' ORDER BY artist_norm)) FROM dim_artist")
        artists = cur.fetchone()[0]
        cur.execute("SELECT md5(string_agg(track_id || ':' || track_key, ',' ORDER BY track_id)) FROM dim_track")
        return {"artists": artists, "tracks": cur.fetchone()[0]}


def test_first_load_reconciles_and_a_rerun_changes_nothing(staged, dw):
    accepted = validation.validate_staged_prepared(staged["prepared"])
    first = load.load_dw(accepted, dag_run_id="pytest_first")
    keys_first = _keys(dw)
    second = load.load_dw(accepted, dag_run_id="pytest_rerun")       # same batch again
    assert second["rows"] == first["rows"] == load.expected_counts(transform.load_prepared(accepted))
    assert second["measures"] == first["measures"]
    assert _keys(dw) == keys_first                                     # surrogate keys are stable across reruns
    report = __import__("json").loads(open(second["evidence_path"], encoding="utf-8").read())
    assert report["before"]["rows"] == report["after"]["rows"]
    assert all(v.get("updated", 0) == 0 and v.get("deleted_stale", 0) == 0 for v in report["changes"].values())
    with psycopg2.connect(**dw) as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM etl_load_audit")
        assert cur.fetchone()[0] == 2


def test_a_failed_load_rolls_back_and_leaves_the_previous_state(staged, dw, monkeypatch):
    accepted = validation.validate_staged_prepared(staged["prepared"])
    before = load.load_dw(accepted, dag_run_id="pytest_before")
    monkeypatch.setattr(load, "expected_counts", lambda frames: {t: -1 for t in load.DW_TABLES})   # force a mismatch
    with pytest.raises(load.LoadContractError):
        load.load_dw(accepted, dag_run_id="pytest_broken")
    with psycopg2.connect(**dw) as conn, conn.cursor() as cur:
        rows = {}
        for t in load.DW_TABLES:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            rows[t] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM etl_load_audit WHERE dag_run_id = 'pytest_broken'")
        assert cur.fetchone()[0] == 0
    assert rows == before["rows"]
