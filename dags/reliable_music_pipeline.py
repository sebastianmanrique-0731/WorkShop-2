"""Reliable batch pipeline: Spotify (CSV) + Grammy (PostgreSQL) -> validated -> Data Warehouse (music_dw).

    extract_spotify -> validate_spotify_raw  ─┐
                                              ├─> transform_and_integrate -> validate_prepared -> load_dw
    extract_grammys -> validate_grammys_raw  ─┘

Orchestration only: the logic lives in src/extract.py, src/validation.py, src/transform.py and src/load.py.
Tasks exchange a compact dict (paths, row counts, checksums, batch id, gate summary), never data frames.

Failure policy (table in README §11)
  * extract_grammys : 3 retries, 30 s exponential (max 5 min) -> database is a network dependency.
  * extract_spotify : 1 retry, 15 s -> a local file has few transient causes.
  * load_dw         : 2 retries, 60 s -> transient DB errors only; the load is ONE transaction, so a retry starts
                      from the previous committed state and cannot duplicate anything.
  * validate_*, transform_and_integrate : retries=0. Deterministic: the same input gives the same result.
  * Contract / data-quality errors raise AirflowFailException, which Airflow never retries, in every task.
  * Default trigger rule (all_success): a failed gate leaves every downstream task `upstream_failed`.

Controlled failures (Test B, §7.2): param fault_injection =
  spotify_popularity_out_of_range (DQ-03) | grammy_year_out_of_range (DQ-08) | prepared_popularity_out_of_range (DQ-12)
"""
from __future__ import annotations

from datetime import timedelta

from airflow.sdk import Param, dag, get_current_context, task
from airflow.exceptions import AirflowFailException   # in 3.1.8 this is the public path (not airflow.sdk.exceptions)

from src import extract as extract_lib
from src import load as load_lib
from src import transform as transform_lib
from src import validation as validation_lib

FAULT_CHOICES = ["none", *extract_lib.FAULTS]
CONTRACT_ERRORS = (extract_lib.ExtractionContractError, transform_lib.TransformContractError,
                   validation_lib.DataQualityError, load_lib.LoadContractError)

DEFAULT_ARGS = {
    "owner": "etl-workshop-2",
    "retries": 0,   # explicit: retries are opt-in per task class, never blanket
}


def _run_context() -> tuple[str, str | None, str]:
    ctx = get_current_context()
    return extract_lib.batch_id_from_run_id(ctx["run_id"]), ctx["params"].get("fault_injection"), ctx["run_id"]


def _fail_fast(fn, *args, **kwargs):
    """Run `fn`; a deterministic contract / data-quality error becomes AirflowFailException (no retry)."""
    try:
        return fn(*args, **kwargs)
    except CONTRACT_ERRORS as exc:
        raise AirflowFailException(f"{type(exc).__name__}: {exc}") from exc


@dag(
    dag_id="reliable_music_pipeline",
    description="Spotify CSV + Grammy DB -> raw validation -> transform -> prepared validation -> DW",
    schedule=None,                 # batch pipeline triggered on demand
    catchup=False,
    max_active_runs=1,             # one batch at a time
    default_args=DEFAULT_ARGS,
    params={
        "fault_injection": Param("none", type="string", enum=FAULT_CHOICES,
                                 description="Test B only: inject one Critical violation after a clean step."),
    },
    tags=["workshop-2", "etl", "great-expectations"],
)
def reliable_music_pipeline():

    # ---- Spotify branch (file source) -----------------------------------------------------
    @task(retries=1, retry_delay=timedelta(seconds=15))
    def extract_spotify() -> dict:
        batch_id, fault, _ = _run_context()
        return _fail_fast(extract_lib.extract_spotify, batch_id, fault)

    @task(retries=0)
    def validate_spotify_raw(meta: dict) -> dict:
        return _fail_fast(validation_lib.validate_staged_raw, "raw_spotify", meta)

    # ---- Grammy branch (relational source database) ---------------------------------------
    @task(retries=3, retry_delay=timedelta(seconds=30), retry_exponential_backoff=True,
          max_retry_delay=timedelta(minutes=5))
    def extract_grammys() -> dict:
        batch_id, fault, _ = _run_context()
        return _fail_fast(extract_lib.extract_grammys, batch_id, fault)

    @task(retries=0)
    def validate_grammys_raw(meta: dict) -> dict:
        return _fail_fast(validation_lib.validate_staged_raw, "raw_grammy", meta)

    # ---- Convergence ----------------------------------------------------------------------
    @task(retries=0)
    def transform_and_integrate(spotify_gate: dict, grammy_gate: dict) -> dict:
        batch_id, fault, _ = _run_context()
        return _fail_fast(transform_lib.run_transform, batch_id, spotify_gate, grammy_gate, fault)

    @task(retries=0)
    def validate_prepared(prepared: dict) -> dict:
        return _fail_fast(validation_lib.validate_staged_prepared, prepared)

    @task(retries=2, retry_delay=timedelta(seconds=60))
    def load_dw(accepted: dict) -> dict:
        _, _, run_id = _run_context()
        return _fail_fast(load_lib.load_dw, accepted, dag_run_id=run_id)

    spotify_gate = validate_spotify_raw(extract_spotify())
    grammy_gate = validate_grammys_raw(extract_grammys())
    # transform receives BOTH gate outputs: it cannot start unless both raw gates passed;
    # load_dw receives the output of validate_prepared: it cannot start unless the prepared gate passed.
    load_dw(validate_prepared(transform_and_integrate(spotify_gate, grammy_gate)))


reliable_music_pipeline()
