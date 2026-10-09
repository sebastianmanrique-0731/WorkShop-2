"""Great Expectations validation layer (Workshop §6.5 rules · §6.6 GX design).

Single source of truth
    RULES below declares every quality rule (DQ-xx): dataset/layer, attributes, quality dimension,
    rule text, metric/threshold, severity, related requirements/risks and the GX expectation(s)
    that automate it. The GX-to-Rule mapping (docs/evidence/validation/gx_expectation_map.csv) and the
    committed GX project in ./gx are GENERATED from this registry, so code and evidence cannot drift apart.

GX object model (GX 1.x)
    Data Source (pandas)  ->  Data Asset (one per dataset)  ->  Batch Definition (whole dataframe)
    Expectation Suite (one per dataset)  +  Batch Definition  =  Validation Definition
    Checkpoint (one per dataset) runs its Validation Definition with the batch DataFrame
    Gate = raw_spotify | raw_grammy | prepared  (the three Airflow validation tasks) = its datasets' Checkpoints

    At run time each gate builds an EPHEMERAL context from this registry. The two raw gates run in parallel and a
    shared file context would make them rewrite the same files in ./gx at the same time. `python -m src.validation
    setup` writes the same objects to ./gx so they can be inspected and versioned.

Severity policy (decided HERE, not by GX's overall `success` flag)
    GX marks a validation as failed when ANY expectation fails, regardless of how important it is.
    The pipeline needs a finer decision, so every expectation carries meta {rule_id, gx_id, severity}
    and `evaluate_gate()` applies the policy of the workshop table:
        critical      -> gate fails (DataQualityError, non-retryable) and downstream tasks do not run
        warning       -> recorded and exposed in the report; the gate continues
        informational -> recorded for trend/monitoring; never blocks
    Every gate execution writes a machine-readable JSON report with per-rule evidence.

Usage (local, outside Airflow)
    python -m src.validation setup                 # create/refresh the GX project (suites, definitions, checkpoints)
    python -m src.validation run raw_spotify       # run a gate against the preserved/extracted data
    python -m src.validation export-docs           # regenerate the Expectation -> Rule map from RULES
"""
from __future__ import annotations

import csv
import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.expectations.row_conditions import Column   # needs GX >= 1.9
import pandas as pd

from src.config import PROJECT_ROOT

LOG = logging.getLogger("validation")

GX_ROOT = PROJECT_ROOT  # the GX project lives in ./gx
EVIDENCE_DIR = PROJECT_ROOT / "docs" / "evidence" / "validation"

CRITICAL, WARNING, INFO = "critical", "warning", "informational"

# --------------------------------------------------------------------------------------
# Contracts shared with extraction / transformation
# --------------------------------------------------------------------------------------
SPOTIFY_COLUMNS = [
    "csv_index", "track_id", "artists", "album_name", "track_name", "popularity", "duration_ms",
    "explicit", "danceability", "energy", "key", "loudness", "mode", "speechiness", "acousticness",
    "instrumentalness", "liveness", "valence", "tempo", "time_signature", "track_genre",
]
GRAMMY_COLUMNS = [
    "grammy_row_id", "year", "title", "published_at", "updated_at", "category", "nominee", "artist",
    "workers", "img", "winner",
]
UNIT_FEATURES = ["danceability", "energy", "speechiness", "acousticness", "instrumentalness", "liveness", "valence"]

PREPARED_TRACK_ARTIST_COLUMNS = [
    "track_id", "artist_norm", "artist_position", "is_primary_artist", "popularity", "is_high_popularity",
    "danceability", "energy", "valence", "loudness_db", "tempo_bpm", "duration_ms",
]
PREPARED_ARTIST_COLUMNS = [
    "artist_norm", "artist_name", "in_spotify", "is_grammy_winner", "grammy_win_count", "first_grammy_year",
    "spotify_winner", "spotify_non_winner",   # helper flags: in_spotify AND (NOT) is_grammy_winner; their SUM = group size
]
PREPARED_AWARD_COLUMNS = [
    "grammy_row_id", "year", "category_key", "artist_norm", "nominee", "win_count",
    "is_matchable", "is_track_matched", "matched_track_count",
]
PREPARED_CATEGORY_COLUMNS = ["category_key", "category_name"]
PREPARED_MATCH_COLUMNS = ["grammy_row_id", "track_id", "match_type"]

# Dataset (= GX data asset = expectation suite stem) -> gate that validates it
DATASET_GATE = {
    "spotify_raw": "raw_spotify",
    "grammy_raw": "raw_grammy",
    "prepared_artist": "prepared",
    "prepared_track_artist": "prepared",
    "prepared_grammy_award": "prepared",
}
GATES = {
    "raw_spotify": ["spotify_raw"],
    "raw_grammy": ["grammy_raw"],
    "prepared": ["prepared_artist", "prepared_track_artist", "prepared_grammy_award"],
}


class DataQualityError(RuntimeError):
    """Deterministic data-quality failure (a Critical rule failed). Must NOT be retried by the orchestrator."""

    def __init__(self, message: str, report: dict | None = None):
        super().__init__(message)
        self.report = report


# --------------------------------------------------------------------------------------
# Rule registry
# --------------------------------------------------------------------------------------
@dataclass
class Rule:
    rule_id: str
    dataset: str                       # key of DATASET_GATE
    attributes: str
    dimension: str
    rule: str                          # what must be true
    threshold: str                     # metric / threshold, human readable
    severity: str
    requirements: str                  # AR-xx
    risks: str                         # RISK-xx / contract / invariant
    justification: str                 # why this threshold is an engineering decision
    expectations: Callable[[], list] = field(repr=False, default=lambda: [])

    @property
    def layer(self) -> str:
        return {"raw_spotify": "Raw · Spotify", "raw_grammy": "Raw · Grammy", "prepared": "Prepared"}[DATASET_GATE[self.dataset]]


def _year_max() -> int:
    return date.today().year


RULES: list[Rule] = [
    # ------------------------------------------------------------------ RAW · SPOTIFY (5)
    Rule("DQ-01", "spotify_raw", "all required columns; row count", "Validity (structure) / completeness (volume)",
         "The 21 required columns exist and the extract is neither empty nor truncated.",
         "missing columns = 0; 50,000 <= rows <= 500,000", CRITICAL, "AR-01, AR-02, AR-03", "Source contract; P-S-01 (114,000 rows)",
         "Nothing can be transformed without the columns, and a retry cannot create them. The lower bound is about half the observed volume: it catches an empty or truncated file without alarming on a legitimately smaller sample; the upper bound guards against a runaway concatenation.",
         lambda: [gxe.ExpectTableColumnsToMatchSet(column_set=SPOTIFY_COLUMNS, exact_match=False),
                  gxe.ExpectTableRowCountToBeBetween(min_value=50_000, max_value=500_000)]),
    Rule("DQ-02", "spotify_raw", "track_id", "Validity / completeness (key)",
         "track_id is present and is a 22-character base-62 Spotify id.",
         "100% non-null and 100% match ^[A-Za-z0-9]{22}$", CRITICAL, "AR-01, AR-02, AR-03", "RISK-04; source contract",
         "track_id is the business key of dim_track: a single malformed id corrupts the dimension key, so there is no tolerance.",
         lambda: [gxe.ExpectColumnValuesToNotBeNull(column="track_id"),
                  gxe.ExpectColumnValuesToMatchRegex(column="track_id", regex=r"^[A-Za-z0-9]{22}$")]),
    Rule("DQ-03", "spotify_raw", "popularity, danceability, energy, speechiness, acousticness, instrumentalness, liveness, valence, key, mode",
         "Validity (domain)",
         "popularity is in 0-100; bounded audio features are in [0, 1]; key is in 0-11; mode is in {0, 1}.",
         "100% within domain", CRITICAL, "AR-01, AR-03", "Source contract; P-S-06 (0 violations)",
         "These domains are defined by Spotify's documentation. A value outside them is impossible, not noisy: it means a shifted column or a corrupted file, and every popularity or audio KPI built on it would be wrong. Zero tolerance.",
         lambda: [gxe.ExpectColumnValuesToBeBetween(column="popularity", min_value=0, max_value=100)]
                 + [gxe.ExpectColumnValuesToBeBetween(column=c, min_value=0, max_value=1) for c in UNIT_FEATURES]
                 + [gxe.ExpectColumnValuesToBeBetween(column="key", min_value=0, max_value=11),
                    gxe.ExpectColumnValuesToBeInSet(column="mode", value_set=[0, 1])]),
    Rule("DQ-04", "spotify_raw", "artists, track_genre", "Completeness",
         "artists (the integration key with Grammy) and track_genre (the AR-02 axis) are present.",
         "artists non-null >= 99%; track_genre 100% non-null", CRITICAL, "AR-01, AR-02, AR-03", "RISK-10; P-S-02 (1 row without artists)",
         "Every requirement joins through the artist and AR-02 slices by genre. If more than 1% of rows have no artist the integration is no longer representative (observed: 1 row, 0.001%), and a null genre cannot be loaded.",
         lambda: [gxe.ExpectColumnValuesToNotBeNull(column="artists", mostly=0.99),
                  gxe.ExpectColumnValuesToNotBeNull(column="track_genre")]),
    Rule("DQ-05", "spotify_raw", "tempo, loudness, duration_ms", "Validity (placeholders / plausibility)",
         "tempo is a real measurement (> 0, since 0 is Spotify's 'not detected'); loudness is within [-60, 0] dB; duration is within (0, 2 h].",
         "share plausible >= 99% for each column", WARNING, "AR-03", "RISK-08; P-S-06 (157 tempo = 0; 90 loudness > 0 dB; 1 duration = 0)",
         "A share p of zero tempos biases the mean by about p x mean, so up to 1% (about 1.2 BPM) the transformation can null them safely; above that the placeholder rate changed and someone must look. Long mixes up to 87 min are real, hence the 2 h cap. Warning, not critical: the transformation handles these values deterministically.",
         lambda: [gxe.ExpectColumnValuesToBeBetween(column="tempo", min_value=0, strict_min=True, mostly=0.99),
                  gxe.ExpectColumnValuesToBeBetween(column="loudness", min_value=-60, max_value=0, mostly=0.99),
                  gxe.ExpectColumnValuesToBeBetween(column="duration_ms", min_value=0, max_value=7_200_000, strict_min=True, mostly=0.99)]),

    # ------------------------------------------------------------------ RAW · GRAMMY (5)
    Rule("DQ-06", "grammy_raw", "all required columns; row count", "Validity (structure) / completeness (volume)",
         "The 11 columns of the source table exist in the database extract and it is neither empty nor truncated.",
         "missing columns = 0; 1,000 <= rows <= 20,000", CRITICAL, "AR-01, AR-02, AR-03", "Source contract (sql/source_setup.sql); P-G-01 (4,810 rows)",
         "A missing column is a schema change, not a transient error. An empty extract means the source table was not prepared. The floor is about 20% of the observed volume (62 award years x 70+ entries).",
         lambda: [gxe.ExpectTableColumnsToMatchSet(column_set=GRAMMY_COLUMNS, exact_match=False),
                  gxe.ExpectTableRowCountToBeBetween(min_value=1_000, max_value=20_000)]),
    Rule("DQ-07", "grammy_raw", "grammy_row_id", "Uniqueness",
         "grammy_row_id is the primary key: present and unique.",
         "100% non-null and unique", CRITICAL, "AR-02, AR-03", "Source contract (PK); P-G-03",
         "It becomes the primary key of fact_grammy_award; a duplicate would break the load and double-count wins.",
         lambda: [gxe.ExpectColumnValuesToNotBeNull(column="grammy_row_id"), gxe.ExpectColumnValuesToBeUnique(column="grammy_row_id")]),
    Rule("DQ-08", "grammy_raw", "year", "Validity (domain)",
         "year is an award-year label between the first ceremony (labelled 1958) and the current year.",
         f"1958 <= year <= current year ({_year_max()} at design time), 100%", CRITICAL, "AR-02, AR-03", "RISK-12; P-G-06 (1958-2019, no gaps)",
         "The first Grammy ceremony is labelled 1958 in this source and an award cannot be dated in the future. The bounds come from domain knowledge, not from the batch. Year feeds dim_year and the decade axis of AR-02/AR-03.",
         lambda: [gxe.ExpectColumnValuesToNotBeNull(column="year"),
                  gxe.ExpectColumnValuesToBeBetween(column="year", min_value=1958, max_value=_year_max())]),
    Rule("DQ-09", "grammy_raw", "artist", "Completeness",
         "Enough award entries carry an artist to support artist-level integration.",
         "non-null >= 50% (baseline 61.75%)", WARNING, "AR-01, AR-02, AR-03", "RISK-02; P-G-02, P-G-08 (38.25% missing)",
         "Missingness is structural (every Song Of The Year has no artist). Below 50% fewer than half of the entries could ever be matched and the KPIs would describe a minority of the source. Warning, not critical: the pipeline still produces valid, coverage-labelled results (KPI-06).",
         lambda: [gxe.ExpectColumnValuesToNotBeNull(column="artist", mostly=0.50)]),
    Rule("DQ-10", "grammy_raw", "winner", "Consistency (scope monitoring)",
         "The source keeps its documented scope: winners only.",
         "winner = True for 100% of rows", INFO, "AR-01, AR-02, AR-03", "RISK-01; P-G-07 (4,810 of 4,810)",
         "If nominees who lost appear, the meaning of every 'win' KPI changes. That needs a human decision but does not make the batch unsafe, so it is informational and never blocks.",
         lambda: [gxe.ExpectColumnValuesToBeInSet(column="winner", value_set=[True])]),

    # ------------------------------------------------------------------ PREPARED (7)
    Rule("DQ-11", "prepared_track_artist", "track_id, artist_norm, artist_position, is_primary_artist", "Uniqueness / completeness (grain)",
         "Exactly one row per (track, artist), with keys present and exactly one primary artist per track.",
         "duplicate (track_id, artist_norm) = 0; keys 100% non-null; artist_position >= 1; is_primary_artist <=> position = 1", CRITICAL,
         "AR-01, AR-02, AR-03", "RISK-04, RISK-05; invariant of the grain of DW-F1",
         "This is the declared grain and primary key of fact_track_artist. A duplicate would double-count a track for an artist in every KPI; the primary flag is what lets KPI-05 count each track once.",
         lambda: [gxe.ExpectTableColumnsToMatchSet(column_set=PREPARED_TRACK_ARTIST_COLUMNS, exact_match=False),
                  gxe.ExpectCompoundColumnsToBeUnique(column_list=["track_id", "artist_norm"]),
                  gxe.ExpectColumnValuesToNotBeNull(column="track_id"), gxe.ExpectColumnValuesToNotBeNull(column="artist_norm"),
                  gxe.ExpectColumnValuesToBeBetween(column="artist_position", min_value=1),
                  gxe.ExpectColumnValuesToBeInSet(column="artist_position", value_set=[1], row_condition=Column("is_primary_artist") == True),
                  gxe.ExpectColumnValuesToBeBetween(column="artist_position", min_value=2, row_condition=Column("is_primary_artist") == False)]),
    Rule("DQ-12", "prepared_track_artist", "popularity, danceability, energy, valence, tempo_bpm, duration_ms", "Validity (domain, post-transformation)",
         "After transformation: popularity in [0, 100]; danceability/energy/valence in [0, 1]; tempo_bpm > 0 and duration_ms > 0 when present (placeholder zeros are NULL).",
         "100% within domain (NULL allowed only for tempo_bpm and duration_ms)", CRITICAL, "AR-01, AR-03", "RISK-08, RISK-09; invariant of TR-S05",
         "Placeholder zeros must have become NULL. If one survives, the transformation is wrong and the KPI-05 averages would be distorted, so loading must stop.",
         lambda: [gxe.ExpectColumnValuesToNotBeNull(column="popularity"),
                  gxe.ExpectColumnValuesToBeBetween(column="popularity", min_value=0, max_value=100),
                  gxe.ExpectColumnValuesToBeBetween(column="danceability", min_value=0, max_value=1),
                  gxe.ExpectColumnValuesToBeBetween(column="energy", min_value=0, max_value=1),
                  gxe.ExpectColumnValuesToBeBetween(column="valence", min_value=0, max_value=1),
                  gxe.ExpectColumnValuesToBeBetween(column="tempo_bpm", min_value=0, strict_min=True),
                  gxe.ExpectColumnValuesToBeBetween(column="duration_ms", min_value=0, strict_min=True)]),
    Rule("DQ-13", "prepared_artist", "artist_norm, is_grammy_winner, grammy_win_count", "Uniqueness / consistency (integration key)",
         "One row per normalized artist; the Grammy placeholder never becomes an artist; the winner flag agrees with the win count.",
         "artist_norm unique and not a placeholder (100%); is_grammy_winner <=> grammy_win_count >= 1", CRITICAL, "AR-01, AR-02",
         "RISK-05, RISK-13; DW constraints UNIQUE(artist_norm), ck_dim_artist_flag",
         "artist_norm is the integration key and the UNIQUE business key of dim_artist. A placeholder in the dimension would pollute every winner KPI, and a flag that disagrees with the count would make KPI-01 compare the wrong groups.",
         lambda: [gxe.ExpectTableColumnsToMatchSet(column_set=PREPARED_ARTIST_COLUMNS, exact_match=False),
                  gxe.ExpectColumnValuesToNotBeNull(column="artist_norm"), gxe.ExpectColumnValuesToBeUnique(column="artist_norm"),
                  gxe.ExpectColumnValuesToNotBeInSet(column="artist_norm", value_set=["", "(various artists)", "various artists"]),
                  gxe.ExpectColumnValuesToNotBeNull(column="is_grammy_winner"),
                  gxe.ExpectColumnValuesToBeBetween(column="grammy_win_count", min_value=1, row_condition=Column("is_grammy_winner") == True),
                  gxe.ExpectColumnValuesToBeInSet(column="grammy_win_count", value_set=[0], row_condition=Column("is_grammy_winner") == False)]),
    Rule("DQ-14", "prepared_grammy_award", "grammy_row_id, year, category_key, win_count; row count", "Completeness / uniqueness (reconciliation)",
         "No Grammy award entry is lost or duplicated by the transformation, and the foreign keys of the fact are valid.",
         "prepared rows == raw rows (runtime value); grammy_row_id unique; year in [1958, current year]; category_key not null; win_count = 1", CRITICAL,
         "AR-02, AR-03", "RISK-02, RISK-11, RISK-12; invariant of the grain of DW-F2",
         "Unmatched and artist-less rows must stay in the fact so the match rate has a denominator; a silent row loss would inflate it. An out-of-range year has no dim_year row and the load would fail or create orphans.",
         lambda: [gxe.ExpectTableColumnsToMatchSet(column_set=PREPARED_AWARD_COLUMNS, exact_match=False),
                  gxe.ExpectColumnValuesToBeUnique(column="grammy_row_id"),
                  gxe.ExpectColumnValuesToBeBetween(column="year", min_value=1958, max_value=_year_max()),
                  gxe.ExpectColumnValuesToNotBeNull(column="category_key"),
                  gxe.ExpectColumnValuesToBeInSet(column="win_count", value_set=[1])]),   # row-count expectation added at runtime
    Rule("DQ-15", "prepared_artist", "spotify_winner, spotify_non_winner (helper flags)", "Completeness (analytical readiness)",
         "AR-01 has two non-trivial comparison groups: Spotify artists with and without Grammy wins.",
         "winners in Spotify >= 30 AND non-winners in Spotify >= 30", CRITICAL, "AR-01", "RISK-07; analytical requirement AR-01",
         "30 observations per group is the usual lower bound to compare two means with reasonable stability. Below it KPI-01 would not be interpretable and the load would publish a misleading result.",
         lambda: [gxe.ExpectColumnSumToBeBetween(column="spotify_winner", min_value=30),
                  gxe.ExpectColumnSumToBeBetween(column="spotify_non_winner", min_value=30)]),
    Rule("DQ-16", "prepared_grammy_award", "is_track_matched among is_matchable", "Consistency (integration integrity)",
         "The track-level integration did not collapse: a minimum share of matchable Grammy rows matched a Spotify track.",
         "matched / matchable >= 1% (observed 12.8%)", CRITICAL, "AR-03", "RISK-06, RISK-07; P-X-03",
         "Below 1% (about 40 rows) the normalization or the key is broken (e.g. a changed name format) and KPI-05 would rest on a handful of rows. The drift between 1% and the baseline is made visible by KPI-06, not by blocking.",
         lambda: [gxe.ExpectColumnValuesToBeInSet(column="is_track_matched", value_set=[True], mostly=0.01, row_condition=Column("is_matchable") == True)]),
    Rule("DQ-17", "prepared_grammy_award", "matched_track_count", "Consistency (cardinality)",
         "Only a small share of award entries fan out to many Spotify tracks. A few long albums or box sets are expected; a widespread fan-out is not.",
         "matched_track_count <= 15 for >= 99% of rows (baseline: 4 of 4,810 = 0.08%)", WARNING, "AR-03",
         "RISK-06, RISK-14; P-X-05 (track-name matches: up to 9) and README §8 (album matches: 4 entries above 15, max 110)",
         "An album title legitimately matches every track of that album (The Complete Hank Williams = 110, The Wall = 21, Abbey Road = 18), so a per-row cap of 15 "
         "would warn on every healthy batch. What would signal a real problem is a systemic fan-out (generic titles, a normalization that over-matches), which "
         "shows up as many rows above 15, not as a few. 1% of the 4,810 entries is 48 rows: 12 times the baseline, and about 9% of the 544 matched entries. "
         "Warning, not critical: the rows stay in the bridge and are flagged, and KPI-06 and the evidence report expose them.",
         lambda: [gxe.ExpectColumnValuesToBeBetween(column="matched_track_count", min_value=0, max_value=15, mostly=0.99)]),
]

RULES_BY_ID = {r.rule_id: r for r in RULES}


# --------------------------------------------------------------------------------------
# Suite construction
# --------------------------------------------------------------------------------------
def expectations_for(dataset: str, runtime: dict | None = None) -> list:
    """All expectations of a dataset, annotated with rule_id / gx_id / severity metadata.

    `runtime` carries values known only at execution time. Currently:
        runtime["raw_grammy_rows"] -> adds the DQ-14 reconciliation expectation (prepared == raw)."""
    runtime = runtime or {}
    out = []
    for rule in (r for r in RULES if r.dataset == dataset):
        exps = list(rule.expectations())
        if rule.rule_id == "DQ-14":
            # The reconciliation expectation exists only at run time (it needs the raw row count). Its id slot is ALWAYS
            # reserved so the committed ./gx project, the Expectation -> Rule map and the run reports share the same ids.
            if "raw_grammy_rows" in runtime:
                exps.append(gxe.ExpectTableRowCountToEqual(value=int(runtime["raw_grammy_rows"])))
            else:
                exps.append(None)
        for e in exps:
            if e is not None:
                e.meta = {"rule_id": rule.rule_id, "severity": rule.severity}
            out.append(e)
    for i, e in enumerate(out, start=1):
        if e is not None:
            e.meta["gx_id"] = f"GX-{_gx_offset(dataset) + i:02d}"
    return [e for e in out if e is not None]


def _gx_offset(dataset: str) -> int:
    """Stable GX ids: each dataset owns a block of 100 ids (GX-101.., GX-201..) so ids never shift when a rule is added elsewhere."""
    order = list(DATASET_GATE)
    return (order.index(dataset) + 1) * 100


def suite_name(dataset: str) -> str:
    return f"{dataset}_suite"


def checkpoint_name(dataset: str) -> str:
    return f"{dataset}_checkpoint"


def get_context(mode: str = "file"):
    if mode == "ephemeral":
        return gx.get_context(mode="ephemeral")
    return gx.get_context(mode="file", project_root_dir=str(GX_ROOT))


def setup_project(context=None, runtime: dict | None = None, datasets: list[str] | None = None):
    """Create (or refresh) data sources, assets, suites, validation definitions and checkpoints.

    Idempotent: existing objects with the same name are replaced. `datasets` limits the work to one gate."""
    context = context or get_context()
    try:
        source = context.data_sources.get("pandas_runtime")
    except Exception:
        source = context.data_sources.add_pandas("pandas_runtime")

    for dataset in datasets or list(DATASET_GATE):
        try:
            asset = source.get_asset(dataset)
        except Exception:
            asset = source.add_dataframe_asset(dataset)
        try:
            batch_def = asset.get_batch_definition("whole_dataframe")
        except Exception:
            batch_def = asset.add_batch_definition_whole_dataframe("whole_dataframe")

        suite = gx.ExpectationSuite(name=suite_name(dataset), expectations=expectations_for(dataset, runtime))
        try:
            context.suites.delete(suite.name)
        except Exception:
            pass
        suite = context.suites.add(suite)

        vd_name = f"{dataset}_validation"
        try:
            context.validation_definitions.delete(vd_name)
        except Exception:
            pass
        vd = context.validation_definitions.add(gx.ValidationDefinition(name=vd_name, data=batch_def, suite=suite))

        # One Checkpoint per dataset: Checkpoint.run() passes ONE dataframe to all its validation definitions,
        # and the prepared gate validates three different dataframes.
        try:
            context.checkpoints.delete(checkpoint_name(dataset))
        except Exception:
            pass
        context.checkpoints.add(gx.Checkpoint(
            name=checkpoint_name(dataset),
            validation_definitions=[vd],
            result_format={"result_format": "SUMMARY"},
        ))
    return context


# --------------------------------------------------------------------------------------
# Execution + policy
# --------------------------------------------------------------------------------------
def _json_safe(obj):
    return json.loads(json.dumps(obj, default=str))


def run_gate(gate: str, frames: dict[str, pd.DataFrame], batch_id: str, runtime: dict | None = None,
             write_report: bool = True, evidence_dir: Path | None = None) -> dict:
    """Run one gate's Checkpoint and return the policy report (dict). Does NOT raise; see evaluate_gate()."""
    if gate not in GATES:
        raise ValueError(f"Unknown gate {gate!r}; expected one of {list(GATES)}")
    missing = [d for d in GATES[gate] if d not in frames]
    if missing:
        raise ValueError(f"Gate {gate} needs dataframes for {GATES[gate]}; missing {missing}")

    context = setup_project(get_context("ephemeral"), runtime=runtime, datasets=GATES[gate])

    started = datetime.now(timezone.utc)
    rules_report: dict[str, dict] = {}
    datasets_report: dict[str, dict] = {}

    for dataset in GATES[gate]:
        cp_result = context.checkpoints.get(checkpoint_name(dataset)).run(batch_parameters={"dataframe": frames[dataset]})
        result = next(iter(cp_result.run_results.values()))
        datasets_report[dataset] = {
            "checkpoint": checkpoint_name(dataset),
            "rows": int(len(frames[dataset])),
            "gx_success": bool(result.success),
            "expectations_evaluated": len(result.results),
        }
        for r in result.results:
            meta = r.expectation_config.meta or {}
            rid = meta.get("rule_id", "UNMAPPED")
            entry = rules_report.setdefault(rid, {
                "rule_id": rid, "dataset": dataset, "severity": meta.get("severity", "unmapped"),
                "passed": True, "expectations": [],
            })
            res = r.result or {}
            entry["expectations"].append({
                "gx_id": meta.get("gx_id"),
                "expectation_type": r.expectation_config.type,
                "kwargs": _json_safe({k: v for k, v in r.expectation_config.kwargs.items() if k != "batch_id"}),
                "success": bool(r.success),
                "observed": _json_safe({k: res[k] for k in
                                        ("element_count", "unexpected_count", "unexpected_percent", "missing_percent",
                                         "observed_value", "partial_unexpected_list") if k in res}),
            })
            entry["passed"] = entry["passed"] and bool(r.success)

    failed = [r for r in rules_report.values() if not r["passed"]]
    by_sev = lambda sev: sorted(r["rule_id"] for r in failed if r["severity"] == sev)  # noqa: E731
    report = {
        "gate": gate,
        "checkpoints": [checkpoint_name(d) for d in GATES[gate]],
        "batch_id": batch_id,
        "gx_version": gx.__version__,
        "started_at": started.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "datasets": datasets_report,
        "rules_evaluated": len(rules_report),
        "rules_passed": len(rules_report) - len(failed),
        "failed_critical": by_sev(CRITICAL),
        "failed_warning": by_sev(WARNING),
        "failed_informational": by_sev(INFO),
        "gate_passed": not by_sev(CRITICAL),
        "policy": "critical -> stop; warning -> record and continue; informational -> record",
        "rules": sorted(rules_report.values(), key=lambda r: r["rule_id"]),
    }
    if write_report:
        out_dir = evidence_dir or EVIDENCE_DIR
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"{gate}__{batch_id}.json"
        path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        report["report_path"] = str(path)
    return report


def evaluate_gate(report: dict) -> dict:
    """Apply the severity policy. Raises DataQualityError (non-retryable) when a Critical rule failed."""
    gate = report["gate"]
    for rid in report["failed_warning"]:
        LOG.warning("[%s] WARNING rule %s failed (batch %s) - continuing under documented policy", gate, rid, report["batch_id"])
    for rid in report["failed_informational"]:
        LOG.info("[%s] INFO rule %s outside baseline (batch %s)", gate, rid, report["batch_id"])
    if report["failed_critical"]:
        raise DataQualityError(
            f"Gate {gate} FAILED on critical rule(s) {report['failed_critical']} (batch {report['batch_id']}). "
            f"Evidence: {report.get('report_path', '<not written>')}", report)
    LOG.info("[%s] PASSED: %d/%d rules ok, warnings=%s info=%s (batch %s)", gate, report["rules_passed"],
             report["rules_evaluated"], report["failed_warning"], report["failed_informational"], report["batch_id"])
    return report


def validate_staged_raw(gate: str, meta: dict) -> dict:
    """Raw gate used by the DAG tasks (Workshop §6.7): validate a STAGED extract and apply the severity policy.

    `meta` is the compact dict returned by extract_spotify / extract_grammys. The staged file is re-opened and its
    checksum verified (ExtractionContractError if it changed), the gate's Checkpoint runs on it, the JSON report is
    written BEFORE the policy is applied (so a blocked batch still leaves evidence), and then:
        Critical failed -> DataQualityError (non-retryable, downstream tasks do not run)
        Warning / Informational -> logged and recorded; the gate passes.
    Returns a compact summary (small enough for XCom), never the data."""
    if gate not in ("raw_spotify", "raw_grammy"):
        raise ValueError(f"validate_staged_raw only handles the raw gates, got {gate!r}")
    from src.extract import load_staged                       # local import: extract does not depend on validation

    frames = {GATES[gate][0]: load_staged(meta)}
    report = run_gate(gate, frames, batch_id=meta["batch_id"])
    evaluate_gate(report)                                       # raises DataQualityError on a Critical failure
    return {**_gate_summary(report), "staged_path": meta["staged_path"], "staged_sha256": meta["staged_sha256"],
            "rows": meta["rows"]}


def validate_staged_prepared(meta: dict) -> dict:
    """Prepared gate used by the DAG task validate_prepared (Workshop §6.9).

    `meta` is the compact dict returned by transform_and_integrate. Every prepared file is re-opened with its checksum
    verified, the prepared Checkpoints run (DQ-11..DQ-17, with the raw Grammy row count as the DQ-14 runtime value),
    the report is written, and a Critical failure raises DataQualityError so load_dw never starts.
    Returns the prepared metadata plus the gate summary: load_dw needs both and receives nothing else."""
    from src.transform import load_prepared                    # local import: transform does not depend on validation

    frames = load_prepared(meta)
    report = run_gate("prepared", {d: frames[d] for d in GATES["prepared"]}, batch_id=meta["batch_id"],
                      runtime={"raw_grammy_rows": meta["raw_grammy_rows"]})
    evaluate_gate(report)
    return {**meta, "prepared_gate": _gate_summary(report)}


def _gate_summary(report: dict) -> dict:
    """The part of a gate report that is small enough for XCom."""
    return {k: report.get(k) for k in ("gate", "batch_id", "gate_passed", "rules_evaluated", "rules_passed", "failed_critical",
                                       "failed_warning", "failed_informational", "report_path")}


# --------------------------------------------------------------------------------------
# Documentation export (generated from RULES, so code and docs cannot drift apart)
# --------------------------------------------------------------------------------------
def _scope(e) -> str:
    kw = e.configuration.kwargs if hasattr(e, "configuration") else {}
    scope = kw.get("column") or kw.get("column_list") or kw.get("column_set") or "(table)"
    return scope if isinstance(scope, str) else "+".join(map(str, scope))[:60]


def write_expectation_map(path: Path | None = None) -> list[dict]:
    """GX Expectation -> Rule ID map (machine readable evidence of Workshop §6.6)."""
    path = path or EVIDENCE_DIR / "gx_expectation_map.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for dataset in DATASET_GATE:
        for e in expectations_for(dataset, runtime={"raw_grammy_rows": 0}):
            rows.append({"gx_id": e.meta["gx_id"], "rule_id": e.meta["rule_id"], "severity": e.meta["severity"],
                         "gate": DATASET_GATE[dataset], "expectation_suite": suite_name(dataset),
                         "validation_definition": f"{dataset}_validation", "checkpoint": checkpoint_name(dataset),
                         "expectation_type": e.expectation_type, "column_or_scope": _scope(e)})
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]), lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    return rows


def export_docs() -> None:
    """Regenerate the Expectation -> Rule map and the committed GX project from RULES."""
    write_expectation_map()
    setup_project()


def design_evidence() -> None:
    """Raw gates on the real files (success) and the two controlled Critical failures (Test B candidates)."""
    from src.local_frames import grammy_raw, spotify_raw
    for old in EVIDENCE_DIR.glob("*.json"):
        if old.name.startswith(("raw_spotify__design", "raw_grammy__design", "prepared__design")):
            old.unlink()
    for gate, frames in (("raw_spotify", {"spotify_raw": spotify_raw()}), ("raw_grammy", {"grammy_raw": grammy_raw()})):
        r = run_gate(gate, frames, "design_success")
        print(f"{gate}: {r['rules_passed']}/{r['rules_evaluated']} passed, warnings={r['failed_warning']}, info={r['failed_informational']}")
    g = grammy_raw(); g.loc[3, "year"] = 1850                                  # violates DQ-08
    print("raw_grammy controlled failure ->", run_gate("raw_grammy", {"grammy_raw": g}, "design_failure_DQ-08")["failed_critical"])
    s = spotify_raw(); s.loc[5, "popularity"] = 150                            # violates DQ-03
    print("raw_spotify controlled failure ->", run_gate("raw_spotify", {"spotify_raw": s}, "design_failure_DQ-03")["failed_critical"])

    # Prepared gate on the REAL output of transform_and_integrate (same rules as the DAG task validate_prepared)
    from src import transform
    frames, rec = transform.transform_and_integrate(spotify_raw(), grammy_raw())
    transform.EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    for old_file in transform.EVIDENCE_DIR.glob("reconciliation__design*.json"):
        old_file.unlink()
    rec = {**rec, "batch_id": "design_success", "raw_grammy_rows": int(len(grammy_raw()))}
    (transform.EVIDENCE_DIR / "reconciliation__design_success.json").write_text(
        json.dumps(rec, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    r = run_gate("prepared", {k: frames[k] for k in GATES["prepared"]}, "design_success", runtime={"raw_grammy_rows": rec["raw_grammy_rows"]})
    print(f"prepared: {r['rules_passed']}/{r['rules_evaluated']} passed, critical={r['failed_critical']}, warnings={r['failed_warning']}")


def _cli(argv: list[str]) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    cmd = argv[1] if len(argv) > 1 else "help"
    if cmd == "setup":
        setup_project()
        print("GX project ready in ./gx")
        return 0
    if cmd == "design-evidence":
        design_evidence()
        return 0
    if cmd == "export-docs":
        export_docs()
        print("Wrote docs/evidence/validation/gx_expectation_map.csv and refreshed ./gx")
        return 0
    if cmd == "run" and len(argv) > 2:
        from src.local_frames import frames_for_gate
        gate = argv[2]
        report = run_gate(gate, frames_for_gate(gate), batch_id=datetime.now(timezone.utc).strftime("local_%Y%m%dT%H%M%SZ"), write_report=False)
        print(json.dumps({k: report[k] for k in ("gate", "rules_evaluated", "rules_passed", "failed_critical",
                                                 "failed_warning", "failed_informational", "gate_passed")}, indent=2))
        evaluate_gate(report)
        return 0
    print(__doc__)
    return 1


if __name__ == "__main__":
    sys.exit(_cli(sys.argv))
