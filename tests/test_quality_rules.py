"""Mutation tests of the quality rules (Workshop §6.5 / §6.6).

Each rule is proven twice on the REAL data: (1) a clean batch does not trigger it, (2) a minimal mutation that
violates exactly what the rule protects makes it fail with the documented severity, and the gate decision follows the
severity policy (Critical blocks, Warning / Informational do not).

Run:  pytest -q tests/test_quality_rules.py      (no database or Airflow needed)
"""
from __future__ import annotations

import os

import numpy as np
import pandas as pd
import pytest

for _k in ("DATA_DB_USER", "DATA_DB_PASSWORD", "SOURCE_DB_NAME", "DW_DB_NAME"):
    os.environ.setdefault(_k, "x")

from src import transform, validation  # noqa: E402
from src.local_frames import grammy_raw, spotify_raw  # noqa: E402


@pytest.fixture(scope="module")
def sp():
    return spotify_raw()


@pytest.fixture(scope="module")
def gr():
    return grammy_raw()


@pytest.fixture(scope="module")
def prepared(sp, gr):
    frames, _ = transform.transform_and_integrate(sp, gr)
    out = {k: frames[k].copy() for k in validation.GATES["prepared"]}
    # The real batch carries 4 award entries that match long albums / box sets (110, 110, 21, 18 tracks). DQ-17 tolerates
    # up to 1% of rows above 15, so the real batch is a clean baseline as it is (see the test below).
    return out, len(gr)


def _run(gate, frames, raw_rows=None):
    return validation.run_gate(gate, frames, batch_id="pytest", write_report=False,
                               runtime={"raw_grammy_rows": raw_rows} if raw_rows is not None else None)


def _assert_fails(report, rule_id):
    sev = validation.RULES_BY_ID[rule_id].severity
    key = {"critical": "failed_critical", "warning": "failed_warning", "informational": "failed_informational"}[sev]
    assert rule_id in report[key], f"{rule_id} should fail as {sev}; got {report[key]}"
    assert report["gate_passed"] == (sev != "critical")          # only Critical blocks the gate


# --------------------------------------------------------------------------------------
# Clean baselines
# --------------------------------------------------------------------------------------
def test_registry_is_small_consistent_and_covers_every_severity():
    ids = [r.rule_id for r in validation.RULES]
    assert ids == [f"DQ-{i:02d}" for i in range(1, 18)]
    assert {r.severity for r in validation.RULES} == {"critical", "warning", "informational"}
    assert all(r.dataset in validation.DATASET_GATE and r.justification for r in validation.RULES)
    for dataset in validation.DATASET_GATE:                      # every suite has at least one expectation
        assert validation.expectations_for(dataset, {"raw_grammy_rows": 1})


def test_clean_raw_spotify_passes_all_rules(sp):
    r = _run("raw_spotify", {"spotify_raw": sp})
    assert r["rules_evaluated"] == 5 and r["failed_critical"] == [] and r["failed_warning"] == [] and r["gate_passed"]


def test_clean_raw_grammy_passes_all_rules(gr):
    r = _run("raw_grammy", {"grammy_raw": gr})
    assert r["rules_evaluated"] == 5 and r["failed_critical"] == [] and r["failed_warning"] == [] and r["gate_passed"]


def test_clean_prepared_passes_all_rules(prepared):
    frames, raw_rows = prepared
    r = _run("prepared", frames, raw_rows)
    assert r["rules_evaluated"] == 7 and r["failed_critical"] == [] and r["failed_warning"] == [] and r["gate_passed"]


# --------------------------------------------------------------------------------------
# Raw Spotify: DQ-01 .. DQ-05
# --------------------------------------------------------------------------------------
def _sp_mut(sp, fn):
    df = sp.copy()
    fn(df)
    return {"spotify_raw": df}


SPOTIFY_MUTATIONS = {
    "DQ-01": lambda d: d.drop(columns=["valence"], inplace=True),
    "DQ-02": lambda d: d.__setitem__("track_id", d["track_id"].where(d.index != 0, "bad-id")),
    "DQ-03": lambda d: d.__setitem__("popularity", d["popularity"].where(d.index != 5, 150)),
    "DQ-04": lambda d: d.__setitem__("artists", d["artists"].mask(d.index % 25 == 0)),          # 4% without artists
    "DQ-05": lambda d: d.__setitem__("tempo", d["tempo"].mask(d.index % 40 == 0, 0.0)),         # 2.5% zero tempo
}


@pytest.mark.parametrize("rule_id", sorted(SPOTIFY_MUTATIONS))
def test_raw_spotify_rule_fails_when_violated(sp, rule_id):
    _assert_fails(_run("raw_spotify", _sp_mut(sp, SPOTIFY_MUTATIONS[rule_id])), rule_id)


def test_spotify_volume_floor_is_part_of_dq01(sp):
    _assert_fails(_run("raw_spotify", {"spotify_raw": sp.head(100)}), "DQ-01")


def test_spotify_null_genre_is_part_of_dq04(sp):
    df = sp.copy(); df.loc[0, "track_genre"] = None
    _assert_fails(_run("raw_spotify", {"spotify_raw": df}), "DQ-04")


# --------------------------------------------------------------------------------------
# Raw Grammy: DQ-06 .. DQ-10
# --------------------------------------------------------------------------------------
GRAMMY_MUTATIONS = {
    "DQ-06": lambda d: d.drop(columns=["category"], inplace=True),
    "DQ-07": lambda d: d.__setitem__("grammy_row_id", d["grammy_row_id"].where(d.index != 1, 1)),
    "DQ-08": lambda d: d.__setitem__("year", d["year"].where(d.index != 3, 1850)),
    "DQ-09": lambda d: d.__setitem__("artist", d["artist"].mask(d.index % 10 < 6)),             # 60% without artist
    "DQ-10": lambda d: d.__setitem__("winner", d["winner"].where(d.index != 0, False)),
}


@pytest.mark.parametrize("rule_id", sorted(GRAMMY_MUTATIONS))
def test_raw_grammy_rule_fails_when_violated(gr, rule_id):
    df = gr.copy()
    GRAMMY_MUTATIONS[rule_id](df)
    _assert_fails(_run("raw_grammy", {"grammy_raw": df}), rule_id)


def test_grammy_future_year_violates_dq08(gr):
    df = gr.copy(); df.loc[0, "year"] = 2999
    _assert_fails(_run("raw_grammy", {"grammy_raw": df}), "DQ-08")


def test_grammy_truncated_extract_violates_dq06(gr):
    _assert_fails(_run("raw_grammy", {"grammy_raw": gr.head(50)}), "DQ-06")


def test_informational_and_warning_rules_never_block(gr):
    df = gr.copy()
    df.loc[0, "winner"] = False                                  # DQ-10 informational
    df["artist"] = df["artist"].mask(df.index % 10 < 6)          # DQ-09 warning
    r = _run("raw_grammy", {"grammy_raw": df})
    assert {"DQ-09"} == set(r["failed_warning"]) and {"DQ-10"} == set(r["failed_informational"])
    assert r["gate_passed"] and validation.evaluate_gate(r) is r  # evaluate_gate does not raise


def test_critical_failure_raises_a_non_retryable_data_quality_error(gr):
    df = gr.copy(); df.loc[3, "year"] = 1850
    with pytest.raises(validation.DataQualityError) as exc:
        validation.evaluate_gate(_run("raw_grammy", {"grammy_raw": df}))
    assert "DQ-08" in str(exc.value)


# --------------------------------------------------------------------------------------
# Prepared: DQ-11 .. DQ-17
# --------------------------------------------------------------------------------------
def _p(prepared):
    frames, raw_rows = prepared
    return {k: v.copy() for k, v in frames.items()}, raw_rows


def _mut_dq11(f):
    f["prepared_track_artist"] = pd.concat([f["prepared_track_artist"], f["prepared_track_artist"].head(1)], ignore_index=True)


def _mut_dq12(f):
    i = f["prepared_track_artist"]["tempo_bpm"].first_valid_index()
    f["prepared_track_artist"].loc[i, "tempo_bpm"] = 0.0


def _mut_dq13(f):
    f["prepared_artist"].loc[f["prepared_artist"].index[0], "artist_norm"] = "(various artists)"


def _mut_dq14(f):
    f["prepared_grammy_award"] = f["prepared_grammy_award"].iloc[1:].copy()            # one entry lost


def _mut_dq15(f):
    f["prepared_artist"]["spotify_winner"] = 0


def _mut_dq16(f):
    f["prepared_grammy_award"]["is_track_matched"] = False


def _mut_dq17(f):
    # 60 rows (1.2% of 4,810) fan out to 40 tracks: a systemic over-match, above the 1% tolerance
    a = f["prepared_grammy_award"]; a.loc[a.index[:60], "matched_track_count"] = 40


PREPARED_MUTATIONS = {"DQ-11": _mut_dq11, "DQ-12": _mut_dq12, "DQ-13": _mut_dq13, "DQ-14": _mut_dq14,
                      "DQ-15": _mut_dq15, "DQ-16": _mut_dq16, "DQ-17": _mut_dq17}


@pytest.mark.parametrize("rule_id", sorted(PREPARED_MUTATIONS))
def test_prepared_rule_fails_when_violated(prepared, rule_id):
    frames, raw_rows = _p(prepared)
    PREPARED_MUTATIONS[rule_id](frames)
    _assert_fails(_run("prepared", frames, raw_rows), rule_id)


def test_dq17_tolerates_the_few_album_box_sets_of_the_real_batch(prepared):
    frames, raw_rows = _p(prepared)
    counts = frames["prepared_grammy_award"]["matched_track_count"]
    assert 0 < (counts > 15).sum() <= 0.01 * len(counts)          # the real batch DOES contain rows above 15 (box sets)
    assert "DQ-17" not in _run("prepared", frames, raw_rows)["failed_warning"]


def test_gx_ids_are_stable_with_and_without_the_runtime_expectation():
    """The committed ./gx suite (no runtime value) and the executed suite must give the same id to every rule."""
    static = {e.meta["gx_id"]: e.meta["rule_id"] for e in validation.expectations_for("prepared_grammy_award")}
    runtime = {e.meta["gx_id"]: e.meta["rule_id"] for e in validation.expectations_for("prepared_grammy_award", {"raw_grammy_rows": 4810})}
    assert all(runtime[k] == v for k, v in static.items())
    assert set(runtime) - set(static) == {"GX-506"}                 # the DQ-14 row-count expectation, added at run time


def test_dq14_detects_a_wrong_runtime_row_count(prepared):
    frames, raw_rows = _p(prepared)
    _assert_fails(_run("prepared", frames, raw_rows + 1), "DQ-14")                      # raw had one more row than prepared
