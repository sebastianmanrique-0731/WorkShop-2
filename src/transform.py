"""Transformation and integration layer (Workshop §6.8): raw Spotify + raw Grammy -> prepared datasets.

One engineering phase that cleans, standardizes, harmonizes, derives and integrates. It produces exactly the
datasets of the contract in README §9 (the ones `validate_prepared` checks and `load_dw`
loads), using BUSINESS keys; surrogate keys are assigned at load time.

    prepared_track_artist   DW-F1 (+ DW-D2 attributes)   one distinct track x one credited individual artist
    prepared_artist         DW-D1                        one row per normalized artist name
    prepared_category       DW-D4                        one row per harmonized category
    prepared_grammy_award   DW-F2                        one row per Grammy award entry (no row lost)
    prepared_award_track    DW-B2                        one row per matched (award entry, track) pair
  plus two small load-time inputs that the contract derives "at load time" and that need source columns:
    prepared_year           DW-D5 (decade, ceremony number/title)
    prepared_track_genre    DW-D3 / DW-B1 (distinct track x genre listings)

Every decision below is documented, with rule, rationale, affected fields, before/after, exception handling and
analytical impact, in README §8. Each rule has an id (TR-xx) that the
reconciliation evidence reports, so a number in the evidence can be traced to a rule.

Design rules
  * Pure functions on DataFrames (`transform_and_integrate`); the file/staging side is `run_transform`.
  * Transformation never tries to "make a validation pass": every rule is defensible on its own (see the doc).
  * Deterministic: same input -> same output (stable ordering, no randomness), so a rerun reproduces the batch.
  * Contract violations that cannot be transformed safely raise TransformContractError (non-retryable).
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import unicodedata
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from src.config import PROJECT_ROOT

LOG = logging.getLogger("transform")

STAGING_ROOT = PROJECT_ROOT / "data" / "staging"
EVIDENCE_DIR = PROJECT_ROOT / "docs" / "evidence" / "transformation"

HIGH_POPULARITY_THRESHOLD = 70        # TR-S07, KPI-02
MIN_SPLIT_PART_LEN = 4                # TR-I03: parts of a split composite name must be longer than 3 characters
UNKNOWN_TEXT = "(unknown)"            # NOT NULL text in dim_track when the source name is missing (never matched)
PLACEHOLDER_ARTIST_KEYS = {"various artists", "(various artists)"}
ENABLE_WORKERS_RECOVERY = True        # TR-G05 (can be switched off; the reconciliation reports its contribution)

PREPARED_DATASETS = [
    "prepared_track_artist", "prepared_artist", "prepared_category", "prepared_grammy_award",
    "prepared_award_track", "prepared_year", "prepared_track_genre",
]


class TransformContractError(RuntimeError):
    """Deterministic transformation failure (missing input column, broken invariant). Retrying cannot fix it."""


# --------------------------------------------------------------------------------------
# Normalization (the integration key) and composite-name splitting
# --------------------------------------------------------------------------------------
_BRACKETS = re.compile(r"\(.*?\)|\[.*?\]")
_NON_WORD = re.compile(r"[^\w\s]")
_SPACES = re.compile(r"\s+")
_SPLIT = re.compile(r"\s+(?:&|and|featuring|feat\.?|with)\s+|\s*[,;]\s*", flags=re.I)
_TRAILING_GROUP = re.compile(r"\(([^()]+)\)\s*$")


def _clean_text(value) -> str | None:
    """Trim and collapse whitespace; empty -> None. Used for display values, never for matching."""
    if value is None or (isinstance(value, float) and np.isnan(value)) or value is pd.NA:
        return None
    text = _SPACES.sub(" ", str(value)).strip()
    return text or None


def norm_key(value) -> str | None:
    """Integration key. accent-strip -> casefold -> drop bracketed text -> punctuation to space -> single spaces.

    Same function for Spotify artists, Grammy artists, track/album names and nominees, so equality means the same
    thing everywhere. If the rule would leave nothing (e.g. an artist called '!!!'), the casefolded trimmed text is
    used, so a name never silently disappears. Non-Latin letters are kept (\\w is Unicode-aware)."""
    text = _clean_text(value)
    if text is None:
        return None
    folded = unicodedata.normalize("NFKD", text)
    folded = "".join(ch for ch in folded if not unicodedata.combining(ch)).casefold()
    key = _SPACES.sub(" ", _NON_WORD.sub(" ", _BRACKETS.sub(" ", folded))).strip()
    return key or folded.strip() or None


def split_artist_names(value: str) -> list[str]:
    return [p for p in _SPLIT.split(str(value)) if p and p.strip()]


def _map_unique(series: pd.Series, fn) -> pd.Series:
    """Apply a scalar function once per distinct value (the Spotify columns repeat heavily)."""
    uniques = series.dropna().unique()
    mapping = {u: fn(u) for u in uniques}
    return series.map(mapping)


# --------------------------------------------------------------------------------------
# Spotify branch
# --------------------------------------------------------------------------------------
SPOTIFY_REQUIRED = ["track_id", "artists", "album_name", "track_name", "popularity", "duration_ms", "explicit",
                    "danceability", "energy", "valence", "loudness", "tempo", "track_genre"]
GRAMMY_REQUIRED = ["grammy_row_id", "year", "title", "category", "nominee", "artist", "workers", "winner"]


def _require(df: pd.DataFrame, cols: list[str], name: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise TransformContractError(f"{name} is missing required columns {missing}")


def transform_spotify(raw: pd.DataFrame) -> tuple[dict[str, pd.DataFrame], dict]:
    """TR-S01..TR-S08. Returns ({'tracks','credits','track_genre'}, reconciliation)."""
    _require(raw, SPOTIFY_REQUIRED, "spotify_raw")
    rec: dict = {"raw_rows": int(len(raw))}
    sp = raw.copy()

    # TR-S01  rows without artists cannot be attributed to any artist dimension member (RISK-10)
    no_artist = sp["artists"].map(_clean_text).isna()
    rec["TR-S01_dropped_no_artist"] = int(no_artist.sum())
    sp = sp[~no_artist]

    # TR-S02  exact duplicates of the source grain (track_id, track_genre) are genuine duplicates (RISK-04)
    dup = sp.duplicated(["track_id", "track_genre"], keep="first")
    rec["TR-S02_exact_duplicates_removed"] = int(dup.sum())
    sp = sp[~dup]

    # Track x genre listings (input of dim_genre / bridge_artist_genre at load time)
    track_genre = sp[["track_id", "track_genre"]].drop_duplicates().reset_index(drop=True)

    # TR-S03  one row per distinct track: attributes are constant per track_id (verified), popularity is the mean
    #         across the genre listings (RISK-04, README §8)
    const_cols = ["artists", "track_name", "album_name", "explicit", "duration_ms", "danceability", "energy",
                  "valence", "loudness", "tempo"]
    varying = [c for c in const_cols if (sp.groupby("track_id", sort=False)[c].nunique(dropna=False) > 1).any()]
    rec["TR-S03_columns_varying_within_track"] = varying          # expected [] (otherwise 'first' is used and reported)
    agg = {c: "first" for c in const_cols}
    agg["popularity"] = "mean"
    tracks = sp.groupby("track_id", sort=False).agg(agg).reset_index()
    rec["TR-S03_listings_collapsed"] = int(len(sp) - len(tracks))
    rec["TR-S03_tracks_with_different_popularity_by_genre"] = int(
        (sp.groupby("track_id", sort=False)["popularity"].nunique() > 1).sum())

    # TR-S04  type conversion and standardization of text
    tracks["popularity"] = tracks["popularity"].astype(float).round(2)
    tracks["explicit"] = tracks["explicit"].astype(bool)
    tracks["track_name_clean"] = tracks["track_name"].map(_clean_text)
    tracks["album_name_clean"] = tracks["album_name"].map(_clean_text)
    rec["TR-S04_tracks_with_missing_name_or_album"] = int(
        (tracks["track_name_clean"].isna() | tracks["album_name_clean"].isna()).sum())
    tracks["track_name"] = tracks["track_name_clean"].fillna(UNKNOWN_TEXT)
    tracks["album_name"] = tracks["album_name_clean"].fillna(UNKNOWN_TEXT)

    # TR-S05  placeholder zeros are 'not detected', not measurements (RISK-08) -> NULL
    rec["TR-S05_tempo_zero_to_null"] = int((tracks["tempo"] == 0).sum())
    rec["TR-S05_duration_zero_to_null"] = int((tracks["duration_ms"] == 0).sum())
    tracks["tempo_bpm"] = tracks["tempo"].where(tracks["tempo"] > 0)
    tracks["duration_ms"] = tracks["duration_ms"].where(tracks["duration_ms"] > 0).astype(float)
    tracks = tracks.rename(columns={"loudness": "loudness_db"})
    # TR-S06  popularity = 0 is kept as a measured value (RISK-09; caveat in README §5)
    rec["TR-S06_popularity_zero_kept"] = int((tracks["popularity"] == 0).sum())
    # TR-S07  derived flag for KPI-02
    tracks["is_high_popularity"] = (tracks["popularity"] >= HIGH_POPULARITY_THRESHOLD).astype(int)

    # TR-S08  split the multi-valued artists column into one credit per individual artist (RISK-05)
    tracks["_names"] = tracks["artists"].map(lambda a: [n for n in (_clean_text(x) for x in str(a).split(";")) if n])
    credits = tracks[["track_id", "_names"]].explode("_names").rename(columns={"_names": "artist_raw"})
    credits = credits.dropna(subset=["artist_raw"])
    credits["artist_norm"] = _map_unique(credits["artist_raw"], norm_key)
    rec["TR-S08_credits_before_dedup"] = int(len(credits))
    placeholder = credits["artist_norm"].isin(PLACEHOLDER_ARTIST_KEYS)
    rec["TR-S08_placeholder_credits_removed"] = int(placeholder.sum())          # RISK-13 applied to Spotify as well
    credits = credits[~placeholder & credits["artist_norm"].notna()]
    same_artist_twice = credits.duplicated(["track_id", "artist_norm"], keep="first")
    rec["TR-S08_repeated_artist_in_track_removed"] = int(same_artist_twice.sum())
    credits = credits[~same_artist_twice].copy()
    credits["artist_position"] = credits.groupby("track_id", sort=False).cumcount() + 1
    credits["is_primary_artist"] = credits["artist_position"] == 1

    # tracks whose every credit disappeared cannot be attributed: they leave the facts
    keep_tracks = set(credits["track_id"])
    rec["TR-S08_tracks_without_any_valid_artist_dropped"] = int((~tracks["track_id"].isin(keep_tracks)).sum())
    tracks = tracks[tracks["track_id"].isin(keep_tracks)]
    track_genre = track_genre[track_genre["track_id"].isin(keep_tracks)].reset_index(drop=True)

    name_by_key = (credits.groupby(["artist_norm", "artist_raw"]).size().reset_index(name="n")
                   .sort_values(["artist_norm", "n", "artist_raw"], ascending=[True, False, True])
                   .drop_duplicates("artist_norm").set_index("artist_norm")["artist_raw"])

    prepared = credits.merge(
        tracks[["track_id", "track_name", "album_name", "explicit", "popularity", "is_high_popularity", "danceability",
                "energy", "valence", "loudness_db", "tempo_bpm", "duration_ms"]], on="track_id", how="left")
    prepared = prepared[[
        "track_id", "artist_norm", "artist_position", "is_primary_artist", "popularity", "is_high_popularity",
        "danceability", "energy", "valence", "loudness_db", "tempo_bpm", "duration_ms",
        "track_name", "album_name", "explicit"]].reset_index(drop=True)

    # keys for the track-level match (only from names that really exist in the source)
    match_src = tracks[["track_id", "track_name_clean", "album_name_clean"]].copy()
    match_src["track_key"] = _map_unique(match_src["track_name_clean"], norm_key)
    match_src["album_key"] = _map_unique(match_src["album_name_clean"], norm_key)

    rec["distinct_tracks"] = int(len(tracks))
    rec["credit_rows"] = int(len(prepared))
    rec["distinct_artists"] = int(prepared["artist_norm"].nunique())
    rec["track_genre_rows"] = int(len(track_genre))
    # reconciliation identity: listings -> tracks
    rec["identity_listings"] = (f"{rec['raw_rows']} raw rows - {rec['TR-S01_dropped_no_artist']} no artist - "
                                f"{rec['TR-S02_exact_duplicates_removed']} duplicates = "
                                f"{rec['raw_rows'] - rec['TR-S01_dropped_no_artist'] - rec['TR-S02_exact_duplicates_removed']} "
                                f"listings -> {rec['TR-S03_listings_collapsed']} collapsed -> "
                                f"{rec['distinct_tracks'] + rec['TR-S08_tracks_without_any_valid_artist_dropped']} tracks")
    return ({"credits": prepared, "track_genre": track_genre, "match_src": match_src,
             "artist_display": name_by_key.rename("artist_name").reset_index()}, rec)


# --------------------------------------------------------------------------------------
# Grammy branch
# --------------------------------------------------------------------------------------
def _canonical_category_names(cat_raw: pd.Series, cat_key: pd.Series) -> pd.DataFrame:
    """One display label per category_key: the most frequent delivered spelling (ties: alphabetical)."""
    t = pd.DataFrame({"category_key": cat_key, "category_name": cat_raw})
    t = (t.groupby(["category_key", "category_name"]).size().reset_index(name="n")
         .sort_values(["category_key", "n", "category_name"], ascending=[True, False, True])
         .drop_duplicates("category_key")[["category_key", "category_name"]])
    return t.reset_index(drop=True)


def transform_grammy_rows(raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """TR-G01..TR-G06 (no Spotify knowledge needed). Returns (rows, categories, reconciliation)."""
    _require(raw, GRAMMY_REQUIRED, "grammy_raw")
    rec: dict = {"raw_rows": int(len(raw)), "TR-G02_category_labels_before": int(raw["category"].nunique())}
    gr = raw.sort_values("grammy_row_id").reset_index(drop=True).copy()

    # TR-G01  standardize text (trim, collapse spaces); empty -> NULL
    for col in ("category", "nominee", "artist", "workers", "title"):
        gr[col] = gr[col].map(_clean_text)

    # TR-G02  harmonize category labels: case/space variants collapse into one category_key (RISK-11)
    gr["category_key"] = gr["category"].str.lower()
    categories = _canonical_category_names(gr["category"], gr["category_key"])
    rec["TR-G02_category_labels_after_trim"] = int(gr["category"].nunique())
    rec["TR-G02_category_keys_after"] = int(categories["category_key"].nunique())

    # TR-G03  ceremony number and title (dim_year); published_at / updated_at are web-publication dates and are NOT
    #         used as event dates (RISK-12)
    gr["ceremony_no"] = pd.to_numeric(gr["title"].str.extract(r"^(\d+)")[0], errors="coerce")
    gr["ceremony_title"] = gr["title"]

    # TR-G04  recover the missing artist with rules that have evidence (RISK-02, P-G-09)
    gr["artist_source"] = np.where(gr["artist"].notna(), "source", "none")
    new_artist = gr["artist"].isna() & gr["nominee"].notna() & gr["category_key"].str.startswith("best new artist")
    gr.loc[new_artist, "artist"] = gr.loc[new_artist, "nominee"]          # the nominee IS the artist in this family
    gr.loc[new_artist, "artist_source"] = "rule_new_artist"
    rec["TR-G04_artist_recovered_new_artist_rule"] = int(new_artist.sum())

    # TR-G05  performer in the trailing parentheses of `workers` ('songwriters (Lady Gaga)'); unstructured text
    #         convention, so the contribution is reported separately and can be switched off
    rec["TR-G05_enabled"] = bool(ENABLE_WORKERS_RECOVERY)
    workers_rule = pd.Series(False, index=gr.index)
    if ENABLE_WORKERS_RECOVERY:
        performer = gr["workers"].map(lambda w: (m.group(1).strip() if (m := _TRAILING_GROUP.search(w)) else None)
                                      if isinstance(w, str) else None)
        workers_rule = gr["artist"].isna() & performer.notna()
        gr.loc[workers_rule, "artist"] = performer[workers_rule]
        gr.loc[workers_rule, "artist_source"] = "rule_workers_performer"
    rec["TR-G05_artist_recovered_workers_rule"] = int(workers_rule.sum())

    # TR-G06  '(Various Artists)' is a placeholder, not an artist (RISK-13)
    gr["artist_whole_key"] = _map_unique(gr["artist"], norm_key)
    placeholder = gr["artist_whole_key"].isin(PLACEHOLDER_ARTIST_KEYS)
    rec["TR-G06_placeholder_artist_nulled"] = int(placeholder.sum())
    gr.loc[placeholder, ["artist", "artist_whole_key"]] = None
    gr.loc[placeholder, "artist_source"] = "none"

    gr["win_count"] = gr["winner"].map(lambda w: 1 if bool(w) else 0).astype(int)
    gr["year"] = gr["year"].astype(int)
    rec["rows_with_artist_after_rules"] = int(gr["artist"].notna().sum())
    rec["rows_without_artist_after_rules"] = int(gr["artist"].isna().sum())
    return gr, categories, rec


def resolve_artist(raw_artist: str | None, spotify_keys: set[str]) -> tuple[str | None, list[str], str]:
    """Integration contract for ONE Grammy artist string -> (primary_key, credited_keys, strategy).

        no_artist  no usable artist                                  -> (None, [], ...)
        exact      the whole normalized string is a Spotify artist   -> that key ('Mumford & Sons', 'Earth, Wind & Fire')
        split      whole string unknown, but a composite whose parts include Spotify artists
                   -> credited = the parts present in Spotify (each longer than MIN_SPLIT_PART_LEN-1 chars);
                      primary  = the first of them
        unmatched  nothing in Spotify                                -> whole key, kept as a Grammy-only artist
    """
    whole = norm_key(raw_artist)
    if whole is None or whole in PLACEHOLDER_ARTIST_KEYS:
        return None, [], "no_artist"
    if whole in spotify_keys:
        return whole, [whole], "exact"
    parts: list[str] = []
    for p in split_artist_names(raw_artist):
        k = norm_key(p)
        if k and k not in PLACEHOLDER_ARTIST_KEYS and len(k) >= MIN_SPLIT_PART_LEN and k not in parts:
            parts.append(k)
    matched = [k for k in parts if k in spotify_keys]
    if len(parts) > 1 and matched:
        return matched[0], matched, "split"
    return whole, [whole], "unmatched"


# --------------------------------------------------------------------------------------
# Integration + assembly
# --------------------------------------------------------------------------------------
def transform_and_integrate(spotify_raw: pd.DataFrame, grammy_raw: pd.DataFrame) -> tuple[dict[str, pd.DataFrame], dict]:
    """Full transformation. Returns (prepared frames, reconciliation evidence dict)."""
    sp, sp_rec = transform_spotify(spotify_raw)
    credits = sp["credits"]
    gr, categories, gr_rec = transform_grammy_rows(grammy_raw)

    spotify_keys = set(credits["artist_norm"])

    # ---- TR-I01..I03 artist resolution (one resolution per DISTINCT Grammy artist string) -------------------------
    distinct_artists = gr["artist"].dropna().unique()
    resolution = {a: resolve_artist(a, spotify_keys) for a in distinct_artists}
    gr["artist_norm"] = gr["artist"].map(lambda a: resolution[a][0] if isinstance(a, str) else None)
    gr["artist_match_strategy"] = gr["artist"].map(lambda a: resolution[a][2] if isinstance(a, str) else "no_artist")
    gr["_credited"] = gr["artist"].map(lambda a: resolution[a][1] if isinstance(a, str) else [])

    strat_rows = gr["artist_match_strategy"].value_counts().to_dict()
    strat_distinct = Counter(v[2] for v in resolution.values())

    # ---- TR-I04..I06 track-level match: (credited artist, nominee) against (artist, track name | album name) ------
    gr["nominee_key"] = _map_unique(gr["nominee"], norm_key)
    gr["is_matchable"] = gr["artist_norm"].notna() & gr["nominee_key"].notna()

    left = (gr.loc[gr["is_matchable"], ["grammy_row_id", "_credited", "nominee_key"]]
            .explode("_credited").rename(columns={"_credited": "artist_norm"}).dropna(subset=["artist_norm"]))
    src = sp["match_src"]
    credit_keys = credits[["track_id", "artist_norm"]]
    by_track = (credit_keys.merge(src[["track_id", "track_key"]].dropna(), on="track_id")
                .rename(columns={"track_key": "nominee_key"}).assign(match_type="track_name"))
    by_album = (credit_keys.merge(src[["track_id", "album_key"]].dropna(), on="track_id")
                .rename(columns={"album_key": "nominee_key"}).assign(match_type="album_name"))
    pairs = pd.concat([
        left.merge(by_track, on=["artist_norm", "nominee_key"], how="inner"),
        left.merge(by_album, on=["artist_norm", "nominee_key"], how="inner"),
    ], ignore_index=True)
    # a track matched through both name and album is reported once, with the stronger evidence (track_name)
    pairs["_prio"] = pairs["match_type"].map({"track_name": 0, "album_name": 1})
    pairs = (pairs.sort_values(["grammy_row_id", "track_id", "_prio"])
             .drop_duplicates(["grammy_row_id", "track_id"])[["grammy_row_id", "track_id", "match_type"]]
             .reset_index(drop=True))

    counts = pairs.groupby("grammy_row_id")["track_id"].nunique()
    gr["matched_track_count"] = gr["grammy_row_id"].map(counts).fillna(0).astype(int)
    gr["is_track_matched"] = gr["matched_track_count"] > 0

    award = gr[["grammy_row_id", "year", "category_key", "artist_norm", "nominee", "win_count", "is_matchable",
                "is_track_matched", "matched_track_count", "artist_source", "artist_match_strategy"]].copy()

    # ---- prepared_artist (conformed dimension: Spotify artists U Grammy-credited artists) ---------------------------
    credit_long = (gr.loc[gr["_credited"].map(len) > 0, ["grammy_row_id", "year", "artist", "_credited", "win_count"]]
                   .explode("_credited").rename(columns={"_credited": "artist_norm"}))
    credit_long = credit_long[credit_long["win_count"] > 0]
    wins = (credit_long.groupby("artist_norm")
            .agg(grammy_win_count=("grammy_row_id", "nunique"), first_grammy_year=("year", "min")).reset_index())
    sp_names = sp["artist_display"]
    grammy_names = (credit_long.assign(artist_name=credit_long["artist"])
                    .groupby(["artist_norm", "artist_name"]).size().reset_index(name="n")
                    .sort_values(["artist_norm", "n", "artist_name"], ascending=[True, False, True])
                    .drop_duplicates("artist_norm")[["artist_norm", "artist_name"]])
    artists = pd.DataFrame({"artist_norm": sorted(spotify_keys | set(credit_long["artist_norm"]))})
    artists = (artists.merge(sp_names.rename(columns={"artist_name": "name_sp"}), on="artist_norm", how="left")
               .merge(grammy_names.rename(columns={"artist_name": "name_gr"}), on="artist_norm", how="left")
               .merge(wins, on="artist_norm", how="left"))
    artists["artist_name"] = artists["name_sp"].fillna(artists["name_gr"]).fillna(artists["artist_norm"])
    artists["in_spotify"] = artists["artist_norm"].isin(spotify_keys)
    artists["grammy_win_count"] = artists["grammy_win_count"].fillna(0).astype(int)
    artists["is_grammy_winner"] = artists["grammy_win_count"] > 0
    artists["first_grammy_year"] = artists["first_grammy_year"].astype("Int64")
    artists["spotify_winner"] = (artists["in_spotify"] & artists["is_grammy_winner"]).astype(int)
    artists["spotify_non_winner"] = (artists["in_spotify"] & ~artists["is_grammy_winner"]).astype(int)
    artists = artists[["artist_norm", "artist_name", "in_spotify", "is_grammy_winner", "grammy_win_count",
                       "first_grammy_year", "spotify_winner", "spotify_non_winner"]]

    # ---- prepared_year -----------------------------------------------------------------------------------------------
    year = gr.groupby("year").agg(ceremony_no=("ceremony_no", "min"), ceremony_title=("ceremony_title", "first"),
                                  titles=("ceremony_title", "nunique")).reset_index()
    if (year["titles"] > 1).any():
        raise TransformContractError(f"Several ceremony titles for the same year: {year[year['titles'] > 1]['year'].tolist()}")
    year["decade"] = (year["year"] // 10) * 10
    year["ceremony_no"] = year["ceremony_no"].astype("Int64")
    year = year[["year", "decade", "ceremony_no", "ceremony_title"]]

    frames = {
        "prepared_track_artist": credits,
        "prepared_artist": artists,
        "prepared_category": categories,
        "prepared_grammy_award": award,
        "prepared_award_track": pairs,
        "prepared_year": year,
        "prepared_track_genre": sp["track_genre"],
    }

    rec = _reconciliation(sp_rec, gr_rec, gr, award, artists, pairs, strat_rows, strat_distinct, resolution, frames)
    _assert_invariants(frames, rec, len(grammy_raw))
    return frames, rec


def _reconciliation(sp_rec, gr_rec, gr, award, artists, pairs, strat_rows, strat_distinct, resolution, frames) -> dict:
    matchable = int(award["is_matchable"].sum())
    matched = int(award["is_track_matched"].sum())
    fan_out = award.loc[award["is_track_matched"], "matched_track_count"]
    in_sp_winners = artists[artists["in_spotify"] & artists["is_grammy_winner"]]
    return {
        "spotify": sp_rec,
        "grammy": gr_rec,
        "integration": {
            "contract": {"key": "norm_key(artist) (accent-strip, casefold, drop brackets, punctuation->space)",
                         "cardinality": "Grammy entry -> Spotify tracks: one-to-many (bridge); artist: many Grammy rows -> one artist",
                         "min_split_part_len": MIN_SPLIT_PART_LEN},
            "grammy_rows_by_artist_strategy": {k: int(v) for k, v in strat_rows.items()},
            "grammy_distinct_artist_strings_by_strategy": {k: int(v) for k, v in strat_distinct.items()},
            "grammy_rows_by_artist_source": {k: int(v) for k, v in award["artist_source"].value_counts().items()},
            "matchable_rows": matchable,
            "track_matched_rows": matched,
            "match_rate_matched_over_matchable": round(matched / matchable, 4) if matchable else None,
            "match_rate_matched_over_all_rows": round(matched / len(award), 4),
            "unmatched_matchable_rows": matchable - matched,
            "pairs": int(len(pairs)),
            "pairs_by_match_type": {k: int(v) for k, v in pairs["match_type"].value_counts().items()},
            "rows_matching_more_than_one_track": int((fan_out > 1).sum()),
            "max_tracks_per_award_entry": int(fan_out.max()) if len(fan_out) else 0,
            "grammy_only_artists": int((~artists["in_spotify"]).sum()),
            "spotify_artists_with_grammy_win": int(len(in_sp_winners)),
        },
        "prepared_rows": {name: int(len(df)) for name, df in frames.items()},
    }


def _assert_invariants(frames: dict[str, pd.DataFrame], rec: dict, raw_grammy_rows: int) -> None:
    """Structural invariants the transformation itself must guarantee (the prepared gate re-verifies them)."""
    a, c = frames["prepared_grammy_award"], frames["prepared_track_artist"]
    if len(a) != raw_grammy_rows:
        raise TransformContractError(f"Grammy rows changed during transformation: {raw_grammy_rows} -> {len(a)}")
    if a["grammy_row_id"].duplicated().any():
        raise TransformContractError("Duplicated grammy_row_id after transformation")
    if c.duplicated(["track_id", "artist_norm"]).any():
        raise TransformContractError("Duplicated (track_id, artist_norm) credit after transformation")
    if a["category_key"].isna().any():
        raise TransformContractError("Grammy rows without category_key after transformation")
    unknown = set(a["category_key"]) - set(frames["prepared_category"]["category_key"])
    if unknown:
        raise TransformContractError(f"category_key without dimension member: {sorted(unknown)[:5]}")
    unknown = set(a["year"]) - set(frames["prepared_year"]["year"])
    if unknown:
        raise TransformContractError(f"year without dimension member: {sorted(unknown)[:5]}")
    members = set(frames["prepared_artist"]["artist_norm"])
    if not set(c["artist_norm"]) <= members or not set(a["artist_norm"].dropna()) <= members:
        raise TransformContractError("Artist key without a prepared_artist member")
    if not set(frames["prepared_award_track"]["track_id"]) <= set(c["track_id"]):
        raise TransformContractError("Matched track_id missing from prepared_track_artist")


# --------------------------------------------------------------------------------------
# Staging + metadata (what the Airflow task exchanges)
# --------------------------------------------------------------------------------------
def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def run_transform(batch_id: str, spotify_meta: dict, grammy_meta: dict, fault: str | None = None) -> dict:
    """Read both validated raw extracts, transform and integrate, stage the prepared datasets and return compact metadata.

    The prepared files are written atomically (tmp + rename). Re-running with the same batch_id overwrites the same
    files, so a rerun cannot leave stale or duplicated prepared data."""
    from src.extract import load_staged                       # verifies path + checksum of each staged raw file

    spotify_raw = load_staged(spotify_meta)
    grammy_raw = load_staged(grammy_meta)
    if spotify_meta.get("batch_id") != batch_id or grammy_meta.get("batch_id") != batch_id:
        raise TransformContractError(
            f"Batch mismatch: run {batch_id!r}, spotify {spotify_meta.get('batch_id')!r}, grammy {grammy_meta.get('batch_id')!r}")

    frames, rec = transform_and_integrate(spotify_raw, grammy_raw)
    from src.extract import apply_fault                        # controlled fault for the prepared gate (Test B variant)
    frames["prepared_track_artist"], fault_meta = apply_fault(frames["prepared_track_artist"], "prepared", fault)

    out_dir = STAGING_ROOT / batch_id
    out_dir.mkdir(parents=True, exist_ok=True)
    datasets: dict[str, dict] = {}
    for name, df in frames.items():
        path = out_dir / f"{name}.parquet"
        tmp = path.with_suffix(".parquet.tmp")
        df.to_parquet(tmp, index=False)
        os.replace(tmp, path)
        datasets[name] = {"path": str(path), "rows": int(len(df)), "sha256": _sha256(path)}

    rec = {**rec, "batch_id": batch_id, "raw_grammy_rows": int(len(grammy_raw)), "raw_spotify_rows": int(len(spotify_raw)),
           "transformed_at": datetime.now(timezone.utc).isoformat()}
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    evidence_path = EVIDENCE_DIR / f"reconciliation__{batch_id}.json"
    tmp = evidence_path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(rec, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    os.replace(tmp, evidence_path)

    meta = {
        "batch_id": batch_id,
        "datasets": datasets,
        "raw_grammy_rows": int(len(grammy_raw)),
        "raw_spotify_rows": int(len(spotify_raw)),
        "reconciliation_path": str(evidence_path),
        "match_rate_matched_over_matchable": rec["integration"]["match_rate_matched_over_matchable"],
        "transformed_at": rec["transformed_at"],
        **fault_meta,
    }
    meta_path = out_dir / "prepared.meta.json"
    tmp = meta_path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps({**meta, "meta_path": str(meta_path)}, indent=2), encoding="utf-8")
    os.replace(tmp, meta_path)
    meta["meta_path"] = str(meta_path)

    i = rec["integration"]
    LOG.info("[transform_and_integrate] batch=%s spotify_raw=%d -> tracks=%d credits=%d | grammy_raw=%d -> award rows=%d "
             "| artists=%d (grammy-only=%d) | matchable=%d matched=%d (%.1f%%) pairs=%d | evidence=%s",
             batch_id, rec["raw_spotify_rows"], rec["spotify"]["distinct_tracks"], rec["spotify"]["credit_rows"],
             rec["raw_grammy_rows"], len(frames["prepared_grammy_award"]), len(frames["prepared_artist"]),
             i["grammy_only_artists"], i["matchable_rows"], i["track_matched_rows"],
             100 * (i["match_rate_matched_over_matchable"] or 0), i["pairs"], evidence_path)
    return meta


def load_prepared(meta: dict) -> dict[str, pd.DataFrame]:
    """Read the staged prepared datasets back (used by validate_prepared and load_dw); verifies every checksum."""
    frames = {}
    for name, info in meta["datasets"].items():
        path = Path(info["path"])
        if not path.is_file():
            raise TransformContractError(f"Prepared file {path} is missing; rerun transform_and_integrate.")
        if _sha256(path) != info["sha256"]:
            raise TransformContractError(f"Prepared file {path} changed since transformation (checksum mismatch).")
        frames[name] = pd.read_parquet(path)
    return frames


if __name__ == "__main__":     # local run without Airflow:  python -m src.transform
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    from src.local_frames import grammy_raw, spotify_raw
    out, report = transform_and_integrate(spotify_raw(), grammy_raw())
    print(json.dumps({k: report[k] for k in ("integration", "prepared_rows")}, indent=2, default=str))
