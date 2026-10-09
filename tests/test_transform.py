"""Tests for transformation and integration (Workshop §6.8).

Unit tests use tiny hand-made frames so each rule (TR-xx) is proven on its own. The last block runs the real CSVs
end to end: row reconciliation, determinism of reruns, and the prepared gate (§6.9 rules) on the real output.

Run:  pytest -q tests/test_transform.py      (no database or Airflow needed)
"""
from __future__ import annotations

import os

import numpy as np
import pandas as pd
import pytest

os.environ.setdefault("DATA_DB_USER", "x")
os.environ.setdefault("DATA_DB_PASSWORD", "x")
os.environ.setdefault("SOURCE_DB_NAME", "x")
os.environ.setdefault("DW_DB_NAME", "x")

from src import extract, transform, validation  # noqa: E402
from src.config import grammy_csv_path, spotify_csv_path  # noqa: E402


# --------------------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------------------
def _sp_row(track_id, artists="Artist One", genre="pop", popularity=50, tempo=120.0, duration_ms=200_000,
            track_name="Song", album_name="Album", **kw):
    row = dict(csv_index=0, track_id=track_id, artists=artists, album_name=album_name, track_name=track_name,
               popularity=popularity, duration_ms=duration_ms, explicit=False, danceability=0.5, energy=0.5, key=1,
               loudness=-7.0, mode=1, speechiness=0.1, acousticness=0.1, instrumentalness=0.0, liveness=0.1,
               valence=0.5, tempo=tempo, time_signature=4, track_genre=genre)
    row.update(kw)
    return row


def _gr_row(rid, year=1990, category="Record Of The Year", nominee="Song", artist="Artist One", workers=None,
            winner=True, title=None):
    return dict(grammy_row_id=rid, year=year, title=title or f"{year - 1957}th Annual GRAMMY Awards  ({year})",
                published_at=None, updated_at=None, category=category, nominee=nominee, artist=artist,
                workers=workers, img=None, winner=winner)


def _tid(n: int) -> str:
    return f"{n:022d}"


# --------------------------------------------------------------------------------------
# normalization and artist resolution (integration contract)
# --------------------------------------------------------------------------------------
def test_norm_key_standardizes_accents_case_brackets_and_punctuation():
    assert transform.norm_key("  Beyoncé ") == "beyonce"
    assert transform.norm_key("H.E.R.") == "h e r"
    assert transform.norm_key("Bon Iver (live)") == "bon iver"
    assert transform.norm_key("Guns N' Roses") == transform.norm_key("guns n roses")
    assert transform.norm_key("!!!") == "!!!"             # never silently disappears
    assert transform.norm_key(None) is None and transform.norm_key("   ") is None


def test_resolve_artist_exact_split_unmatched_and_placeholder():
    keys = {"mumford sons", "dolly parton", "beyonce", "eve"}
    assert transform.resolve_artist("Mumford & Sons", keys) == ("mumford sons", ["mumford sons"], "exact")
    primary, credited, strategy = transform.resolve_artist("Dolly Parton & Someone Unknown", keys)
    assert (primary, credited, strategy) == ("dolly parton", ["dolly parton"], "split")
    assert transform.resolve_artist("Nobody Famous", keys)[2] == "unmatched"
    assert transform.resolve_artist("(Various Artists)", keys) == (None, [], "no_artist")
    assert transform.resolve_artist(None, keys) == (None, [], "no_artist")


def test_split_parts_shorter_than_four_characters_never_match_but_whole_short_names_can():
    keys = {"eve", "joy"}
    assert transform.resolve_artist("Sam & Eve", keys)[2] == "unmatched"       # 'eve' (3) is not accepted as a part
    assert transform.resolve_artist("Eve", keys) == ("eve", ["eve"], "exact")   # whole string is an explicit name


# --------------------------------------------------------------------------------------
# Spotify rules TR-S01..S08
# --------------------------------------------------------------------------------------
def test_spotify_rules_on_a_small_frame():
    raw = pd.DataFrame([
        _sp_row(_tid(1), "Alpha; Beta ; alpha", genre="pop", popularity=60, tempo=0.0, duration_ms=0),   # TR-S05, S08
        _sp_row(_tid(1), "Alpha; Beta ; alpha", genre="rock", popularity=80, tempo=0.0, duration_ms=0),  # TR-S03 mean 70
        _sp_row(_tid(1), "Alpha; Beta ; alpha", genre="rock", popularity=80, tempo=0.0, duration_ms=0),  # TR-S02 duplicate
        _sp_row(_tid(2), None, track_name=None, album_name=None),                                          # TR-S01
        _sp_row(_tid(3), "Various Artists", popularity=0),                                                 # TR-S08 placeholder only
        _sp_row(_tid(4), "Gamma", popularity=0),                                                           # TR-S06
    ])
    out, rec = transform.transform_spotify(raw)
    credits = out["credits"]

    assert rec["TR-S01_dropped_no_artist"] == 1
    assert rec["TR-S02_exact_duplicates_removed"] == 1
    assert rec["TR-S03_tracks_with_different_popularity_by_genre"] == 1
    assert rec["TR-S08_placeholder_credits_removed"] == 1 and rec["TR-S08_tracks_without_any_valid_artist_dropped"] == 1
    assert rec["TR-S08_repeated_artist_in_track_removed"] == 1

    t1 = credits[credits.track_id == _tid(1)].sort_values("artist_position")
    assert t1.artist_norm.tolist() == ["alpha", "beta"]                       # split, trimmed, repeated artist removed
    assert t1.artist_position.tolist() == [1, 2] and t1.is_primary_artist.tolist() == [True, False]
    assert (t1.popularity == 70.0).all() and (t1.is_high_popularity == 1).all()   # mean across genres, flag >= 70
    assert t1.tempo_bpm.isna().all() and t1.duration_ms.isna().all()               # placeholder zeros -> NULL
    assert _tid(3) not in set(credits.track_id)                                    # unattributable track leaves the facts
    assert credits[credits.track_id == _tid(4)].popularity.iloc[0] == 0            # popularity 0 kept as measured
    assert not credits.duplicated(["track_id", "artist_norm"]).any()
    assert set(out["track_genre"].track_id) <= set(credits.track_id)
    assert len(out["track_genre"]) == 3                                            # track 1 in pop + rock, track 4 in pop


# --------------------------------------------------------------------------------------
# Grammy rules TR-G01..G06
# --------------------------------------------------------------------------------------
def test_grammy_rules_on_a_small_frame():
    raw = pd.DataFrame([
        _gr_row(1, category="Soloist Or Soloists", artist=" Alpha  Band "),
        _gr_row(2, category="Soloist OR Soloists", artist="Beta"),
        _gr_row(3, category="Soloist Or Soloists", artist="Gamma"),
        _gr_row(4, category="Best New Artist", nominee="Delta", artist=None),                       # TR-G04
        _gr_row(5, category="Song Of The Year", nominee="Hit", artist=None,
                workers="Writer One & Writer Two, songwriters (Epsilon)"),                           # TR-G05
        _gr_row(6, category="Best Cast Show Album", artist=None, workers="Producer, producers (Various Artists)"),
        _gr_row(7, category="Best Rock Song", artist="(Various Artists)"),                           # TR-G06
        _gr_row(8, category="Best Rock Song", artist=None, workers=None),
    ])
    rows, categories, rec = transform.transform_grammy_rows(raw)
    by_id = rows.set_index("grammy_row_id")

    assert rec["TR-G02_category_labels_before"] == 6 and rec["TR-G02_category_keys_after"] == 5
    assert by_id.loc[[1, 2, 3], "category_key"].nunique() == 1                  # case variants collapse (RISK-11)
    assert categories.category_key.is_unique
    assert by_id.loc[1, "artist"] == "Alpha Band"                               # whitespace standardized
    assert by_id.loc[4, "artist"] == "Delta" and by_id.loc[4, "artist_source"] == "rule_new_artist"
    assert by_id.loc[5, "artist"] == "Epsilon" and by_id.loc[5, "artist_source"] == "rule_workers_performer"
    assert pd.isna(by_id.loc[6, "artist"]) and by_id.loc[6, "artist_source"] == "none"     # Various Artists from workers
    assert pd.isna(by_id.loc[7, "artist"]) and rec["TR-G06_placeholder_artist_nulled"] == 2
    assert pd.isna(by_id.loc[8, "artist"])                                       # no evidence -> stays missing
    assert (rows.win_count == 1).all()
    assert rows.ceremony_no.notna().all()


def test_workers_recovery_can_be_switched_off(monkeypatch):
    monkeypatch.setattr(transform, "ENABLE_WORKERS_RECOVERY", False)
    raw = pd.DataFrame([_gr_row(1, category="Song Of The Year", artist=None, workers="W, songwriter (Epsilon)")])
    rows, _, rec = transform.transform_grammy_rows(raw)
    assert pd.isna(rows.artist.iloc[0]) and rec["TR-G05_artist_recovered_workers_rule"] == 0


def test_grammy_missing_required_column_is_a_contract_error():
    with pytest.raises(transform.TransformContractError):
        transform.transform_grammy_rows(pd.DataFrame({"year": [1990]}))


# --------------------------------------------------------------------------------------
# Integration (TR-I01..I06) on a controlled mini world
# --------------------------------------------------------------------------------------
def _mini_world():
    spotify = pd.DataFrame([
        _sp_row(_tid(1), "Alpha", track_name="Hit Song", album_name="Debut"),
        _sp_row(_tid(2), "Alpha", track_name="Hit Song - Remastered", album_name="Debut"),
        _sp_row(_tid(3), "Alpha", track_name="Hit Song", album_name="Greatest Hits"),     # second version of the track
        _sp_row(_tid(4), "Alpha;Beta", track_name="Duet", album_name="Duet Album"),
        _sp_row(_tid(5), "Gamma Band", track_name="Other", album_name="Gamma Album"),
    ])
    grammy = pd.DataFrame([
        _gr_row(1, nominee="Hit Song", artist="Alpha"),                      # matches two track ids by name (fan-out 2)
        _gr_row(2, nominee="Debut", artist="Alpha"),                         # album match -> tracks 1 and 2
        _gr_row(3, nominee="Duet", artist="Alpha & Beta Singers"),           # composite -> credited ['alpha']
        _gr_row(4, nominee="Gamma Album", artist="Gamma Band"),              # album match
        _gr_row(5, nominee="Nothing Like It", artist="Alpha"),               # matchable, not matched
        _gr_row(6, nominee=None, artist="Alpha"),                            # not matchable (no nominee)
        _gr_row(7, nominee="Hit Song", artist=None),                         # not matchable (no artist)
        _gr_row(8, nominee="Hit Song", artist="Unknown Soloist"),            # Grammy-only artist
    ])
    return spotify, grammy


def test_integration_contract_on_a_mini_world():
    spotify, grammy = _mini_world()
    frames, rec = transform.transform_and_integrate(spotify, grammy)
    award = frames["prepared_grammy_award"].set_index("grammy_row_id")
    pairs = frames["prepared_award_track"]
    artists = frames["prepared_artist"].set_index("artist_norm")

    assert len(award) == len(grammy)                                          # no Grammy row lost (DQ-14 invariant)
    assert award.loc[1, "matched_track_count"] == 2                           # tracks 1 and 3 share name 'hit song'
    assert set(pairs[pairs.grammy_row_id == 1].track_id) == {_tid(1), _tid(3)}
    assert set(pairs[pairs.grammy_row_id == 1].match_type) == {"track_name"}
    assert set(pairs[pairs.grammy_row_id == 2].track_id) == {_tid(1), _tid(2)} and \
        set(pairs[pairs.grammy_row_id == 2].match_type) == {"album_name"}
    assert award.loc[4, "is_track_matched"] and award.loc[4, "matched_track_count"] == 1
    assert award.loc[5, "is_matchable"] and not award.loc[5, "is_track_matched"]
    assert not award.loc[6, "is_matchable"] and not award.loc[7, "is_matchable"]
    assert not award.loc[8, "is_track_matched"] and award.loc[8, "artist_match_strategy"] == "unmatched"
    assert not pairs.duplicated(["grammy_row_id", "track_id"]).any()          # grain of the bridge
    assert (award.is_track_matched == (award.matched_track_count > 0)).all()
    assert (~award.is_track_matched | award.is_matchable).all()

    # conformed artist dimension
    assert artists.loc["alpha", "in_spotify"] and artists.loc["alpha", "is_grammy_winner"]
    assert artists.loc["alpha", "grammy_win_count"] == 5                       # rows 1,2,3,5,6 credit Alpha
    assert artists.loc["alpha", "first_grammy_year"] == 1990
    assert artists.loc["beta", "grammy_win_count"] == 0 and artists.loc["beta", "in_spotify"]
    assert not artists.loc["unknown soloist", "in_spotify"] and artists.loc["unknown soloist", "is_grammy_winner"]
    assert (artists.is_grammy_winner == (artists.grammy_win_count > 0)).all()
    assert (artists.spotify_winner + artists.spotify_non_winner == artists.in_spotify.astype(int)).all()
    assert "(various artists)" not in artists.index


def test_prepared_contract_columns_are_present():
    spotify, grammy = _mini_world()
    frames, _ = transform.transform_and_integrate(spotify, grammy)
    expected = {
        "prepared_track_artist": validation.PREPARED_TRACK_ARTIST_COLUMNS,
        "prepared_artist": validation.PREPARED_ARTIST_COLUMNS,
        "prepared_category": validation.PREPARED_CATEGORY_COLUMNS,
        "prepared_grammy_award": validation.PREPARED_AWARD_COLUMNS,
        "prepared_award_track": validation.PREPARED_MATCH_COLUMNS,
    }
    for name, cols in expected.items():
        assert set(cols) <= set(frames[name].columns), f"{name} lacks {set(cols) - set(frames[name].columns)}"
    assert set(frames) == set(transform.PREPARED_DATASETS)


def test_spotify_missing_required_column_is_a_contract_error():
    with pytest.raises(transform.TransformContractError):
        transform.transform_spotify(pd.DataFrame({"track_id": ["x"]}))


# --------------------------------------------------------------------------------------
# End to end on the real files
# --------------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def real_raw():
    sp = pd.read_csv(spotify_csv_path())
    sp = sp.rename(columns={sp.columns[0]: "csv_index"})
    gr = pd.read_csv(grammy_csv_path())
    gr.insert(0, "grammy_row_id", range(1, len(gr) + 1))
    gr["published_at"] = pd.to_datetime(gr["published_at"], utc=True)
    gr["updated_at"] = pd.to_datetime(gr["updated_at"], utc=True)
    return sp, gr


@pytest.fixture(scope="module")
def real_result(real_raw):
    return transform.transform_and_integrate(*real_raw)


def test_real_data_reconciles(real_raw, real_result):
    sp, gr = real_raw
    frames, rec = real_result
    s = rec["spotify"]
    # every raw Spotify row is accounted for: dropped, deduplicated, or collapsed into a track
    listings = s["raw_rows"] - s["TR-S01_dropped_no_artist"] - s["TR-S02_exact_duplicates_removed"]
    assert listings - s["TR-S03_listings_collapsed"] == s["distinct_tracks"] + s["TR-S08_tracks_without_any_valid_artist_dropped"]
    assert s["TR-S01_dropped_no_artist"] == 1 and s["TR-S02_exact_duplicates_removed"] == 450      # P-S-02, P-S-03
    assert s["TR-S03_columns_varying_within_track"] == []                                          # only popularity varies
    assert s["TR-S03_tracks_with_different_popularity_by_genre"] == 720                            # RISK-04
    assert s["TR-S05_tempo_zero_to_null"] > 0
    assert len(frames["prepared_grammy_award"]) == len(gr) == 4810                                 # nothing lost
    assert rec["grammy"]["TR-G02_category_labels_before"] == 638                                   # RISK-11
    assert len(frames["prepared_category"]) < 638
    assert rec["integration"]["track_matched_rows"] > 0
    credits = frames["prepared_track_artist"]
    assert not credits.duplicated(["track_id", "artist_norm"]).any()
    assert credits.groupby("track_id").is_primary_artist.sum().eq(1).all()                         # one primary per track
    assert (credits.tempo_bpm.dropna() > 0).all() and (credits.duration_ms.dropna() > 0).all()


def test_real_output_passes_the_prepared_gate_without_critical_failures(real_raw, real_result):
    frames, _ = real_result
    gate_frames = {name: frames[name] for name in validation.GATES["prepared"]}
    report = validation.run_gate("prepared", gate_frames, batch_id="pytest_real", write_report=False,
                                 runtime={"raw_grammy_rows": len(real_raw[1])})
    assert report["failed_critical"] == [], report["failed_critical"]
    assert report["rules_evaluated"] == 7 and report["gate_passed"]


def _stage_raw(tmp_path, monkeypatch, real_raw, batch_id):
    monkeypatch.setattr(extract, "STAGING_ROOT", tmp_path / "staging")
    monkeypatch.setattr(transform, "STAGING_ROOT", tmp_path / "staging")
    monkeypatch.setattr(transform, "EVIDENCE_DIR", tmp_path / "evidence")
    sp, gr = real_raw
    return (extract._stage(sp, "spotify", batch_id, {"origin": "test"}),
            extract._stage(gr, "grammy", batch_id, {"origin": "test"}))


def test_run_transform_is_repeatable_and_stages_checkable_files(tmp_path, monkeypatch, real_raw):
    spotify_meta, grammy_meta = _stage_raw(tmp_path, monkeypatch, real_raw, "b1")
    first = transform.run_transform("b1", spotify_meta, grammy_meta)
    second = transform.run_transform("b1", spotify_meta, grammy_meta)             # rerun, same batch
    assert {k: v["sha256"] for k, v in first["datasets"].items()} == {k: v["sha256"] for k, v in second["datasets"].items()}
    assert {k: v["rows"] for k, v in first["datasets"].items()} == {k: v["rows"] for k, v in second["datasets"].items()}
    files = sorted(p.name for p in (tmp_path / "staging" / "b1").iterdir() if p.name.startswith("prepared"))
    assert not [f for f in files if f.endswith(".tmp")]                           # atomic writes leave no temp files
    loaded = transform.load_prepared(first)
    assert set(loaded) == set(transform.PREPARED_DATASETS)
    assert first["raw_grammy_rows"] == 4810 and first["datasets"]["prepared_grammy_award"]["rows"] == 4810


def test_prepared_file_tampering_is_detected(tmp_path, monkeypatch, real_raw):
    spotify_meta, grammy_meta = _stage_raw(tmp_path, monkeypatch, real_raw, "b2")
    meta = transform.run_transform("b2", spotify_meta, grammy_meta)
    with open(meta["datasets"]["prepared_category"]["path"], "ab") as fh:
        fh.write(b"x")
    with pytest.raises(transform.TransformContractError):
        transform.load_prepared(meta)


def test_batch_mismatch_between_branches_is_a_contract_error(tmp_path, monkeypatch, real_raw):
    spotify_meta, grammy_meta = _stage_raw(tmp_path, monkeypatch, real_raw, "b3")
    with pytest.raises(transform.TransformContractError):
        transform.run_transform("another_batch", spotify_meta, grammy_meta)


def test_transform_reads_only_validated_staged_files(tmp_path, monkeypatch, real_raw):
    spotify_meta, grammy_meta = _stage_raw(tmp_path, monkeypatch, real_raw, "b4")
    with open(grammy_meta["staged_path"], "ab") as fh:
        fh.write(b"x")                                                           # staged raw changed after its gate
    with pytest.raises(extract.ExtractionContractError):
        transform.run_transform("b4", spotify_meta, grammy_meta)
