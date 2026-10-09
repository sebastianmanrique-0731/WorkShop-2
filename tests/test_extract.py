"""Tests for extraction and the raw validation gates as the DAG runs them (Workshop §6.7).

Spotify tests need only the preserved CSV. Grammy staging is exercised from the CSV-shaped frame; the one test that
reads the real PostgreSQL source table is skipped when the database is not reachable.

Run:  pytest -q tests/test_extract.py
"""
from __future__ import annotations

import os

import pandas as pd
import pytest

for _k in ("DATA_DB_USER", "DATA_DB_PASSWORD", "SOURCE_DB_NAME", "DW_DB_NAME"):
    os.environ.setdefault(_k, "x")

from src import extract, validation  # noqa: E402
from src.local_frames import grammy_raw  # noqa: E402


@pytest.fixture()
def staging(tmp_path, monkeypatch):
    monkeypatch.setattr(extract, "STAGING_ROOT", tmp_path / "staging")
    monkeypatch.setattr(validation, "EVIDENCE_DIR", tmp_path / "evidence")
    return tmp_path


def _stage_grammy(batch_id, fault=None):
    df, fm = extract.apply_fault(grammy_raw(), "grammy", fault)
    return extract._stage(df, "grammy", batch_id, {"origin": "test", **fm})


# --------------------------------------------------------------------------------------
# Extraction behaviour
# --------------------------------------------------------------------------------------
def test_batch_id_is_filesystem_safe():
    assert extract.batch_id_from_run_id("manual__2026-10-08T12:00:00+00:00") == "manual__2026-10-08T12_00_00_00_00"


def test_spotify_extract_does_no_cleaning(staging):
    meta = extract.extract_spotify("t1")
    df = extract.load_staged(meta)
    assert meta["rows"] == len(df) == 114_000 and meta["fault_injected"] is None
    assert df.duplicated(["track_id", "track_genre"]).sum() == 450          # duplicates stay visible (TR-S02 removes them later)
    assert (df["tempo"] == 0).sum() == 157                                  # placeholder zeros stay visible (TR-S05)
    assert "csv_index" in df.columns


def test_staged_file_integrity_is_verified(staging):
    meta = extract.extract_spotify("t2")
    with open(meta["staged_path"], "ab") as fh:
        fh.write(b"x")
    with pytest.raises(extract.ExtractionContractError):
        extract.load_staged(meta)


def test_rerun_of_the_same_batch_overwrites_the_same_file(staging):
    first = extract.extract_spotify("t3")
    second = extract.extract_spotify("t3")
    assert first["staged_path"] == second["staged_path"] and first["staged_sha256"] == second["staged_sha256"]
    assert sorted(p.name for p in (staging / "staging" / "t3").iterdir()) == ["spotify_raw.meta.json", "spotify_raw.parquet"]   # no temp leftovers


def test_missing_spotify_csv_is_a_contract_error(staging, monkeypatch):
    monkeypatch.setattr(extract, "spotify_csv_path", lambda: staging / "nope.csv")
    with pytest.raises(extract.ExtractionContractError):
        extract.extract_spotify("t4")


def test_unknown_fault_is_a_contract_error():
    with pytest.raises(extract.ExtractionContractError):
        extract.apply_fault(pd.DataFrame({"a": [1]}), "spotify", "not_a_fault")


def test_faults_target_critical_rules_that_exist():
    for fault, (_, rule_id) in extract.FAULTS.items():
        assert validation.RULES_BY_ID[rule_id].severity == "critical", fault


def test_fault_only_touches_its_own_branch():
    gr = grammy_raw()
    same, meta = extract.apply_fault(gr, "grammy", "spotify_popularity_out_of_range")
    assert same is gr and meta["fault_injected"] is None


# --------------------------------------------------------------------------------------
# Raw gates as the DAG tasks run them
# --------------------------------------------------------------------------------------
def test_clean_spotify_batch_passes_the_gate_and_leaves_evidence(staging):
    out = validation.validate_staged_raw("raw_spotify", extract.extract_spotify("t5"))
    assert out["gate_passed"] and out["failed_critical"] == [] and out["rules_passed"] == out["rules_evaluated"] == 5
    assert (staging / "evidence" / "raw_spotify__t5.json").is_file()


def test_injected_spotify_fault_blocks_with_exactly_dq03_and_keeps_evidence(staging):
    meta = extract.extract_spotify("t6", "spotify_popularity_out_of_range")
    assert meta["fault_rule"] == "DQ-03"
    with pytest.raises(validation.DataQualityError) as exc:
        validation.validate_staged_raw("raw_spotify", meta)
    assert exc.value.report["failed_critical"] == ["DQ-03"]
    assert (staging / "evidence" / "raw_spotify__t6.json").is_file()          # report written BEFORE the policy is applied


def test_clean_grammy_batch_passes_the_gate(staging):
    out = validation.validate_staged_raw("raw_grammy", _stage_grammy("t7"))
    assert out["gate_passed"] and out["rules_passed"] == out["rules_evaluated"] == 5


def test_injected_grammy_fault_blocks_with_exactly_dq08_and_keeps_evidence(staging):
    meta = _stage_grammy("t8", "grammy_year_out_of_range")
    assert meta["fault_rule"] == "DQ-08"
    with pytest.raises(validation.DataQualityError) as exc:
        validation.validate_staged_raw("raw_grammy", meta)
    assert exc.value.report["failed_critical"] == ["DQ-08"]
    assert (staging / "evidence" / "raw_grammy__t8.json").is_file()


def test_gate_detects_a_staged_file_modified_after_extraction(staging):
    meta = _stage_grammy("t9")
    with open(meta["staged_path"], "ab") as fh:
        fh.write(b"x")
    with pytest.raises(extract.ExtractionContractError):
        validation.validate_staged_raw("raw_grammy", meta)


def test_a_failed_spotify_gate_does_not_affect_the_grammy_branch(staging):
    bad = extract.extract_spotify("t10", "spotify_popularity_out_of_range")
    good = _stage_grammy("t10")
    with pytest.raises(validation.DataQualityError):
        validation.validate_staged_raw("raw_spotify", bad)
    assert validation.validate_staged_raw("raw_grammy", good)["gate_passed"]


# --------------------------------------------------------------------------------------
# Real source database (skipped when it is not up)
# --------------------------------------------------------------------------------------
def _db_up() -> bool:
    try:
        import psycopg2
        from src.config import source_db_params
        psycopg2.connect(connect_timeout=2, **source_db_params()).close()
        return True
    except Exception:
        return False


@pytest.mark.skipif(not _db_up(), reason="Grammy source database is not reachable")
def test_grammy_extract_reads_the_database_not_the_csv(staging):
    meta = extract.extract_grammys("t11")
    assert meta["origin"] == "postgres_table" and meta["rows"] == meta["db_row_count"] == 4810
