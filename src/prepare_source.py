"""Source preparation (Workshop §6.2): load the provided Grammy CSV into the relational source DB.

This creates the OPERATIONAL SOURCE that the pipeline later extracts from.
It is NOT the ETL Load into the Data Warehouse.

Properties
  * Data is loaded as delivered (no cleaning), so raw validation sees real source quality.
  * Atomic and rerunnable: table recreation + COPY run in ONE transaction. A failure leaves the
    previous table untouched; a rerun yields the same table (no duplicates).
  * Reconciles the database against the CSV (row count, per-column empty/NULL counts, year range,
    distinct categories, winner count) and writes the evidence to docs/evidence/.
  * Exits non-zero if anything does not reconcile.
  * Environment bootstrap: also creates the empty DW database (music_dw) if it is missing; the DW tables belong to
    load_dw.

Usage
  docker compose --profile setup run --rm prepare-source
  python -m src.prepare_source            # from the repo root, with the variables of .env exported
"""
from __future__ import annotations

import csv
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

from psycopg2 import sql

from src.config import PROJECT_ROOT, dw_db_params, grammy_csv_path, source_db_params

LOG = logging.getLogger("prepare_source")

EXPECTED_HEADER = [
    "year", "title", "published_at", "updated_at", "category",
    "nominee", "artist", "workers", "img", "winner",
]
SETUP_SQL = PROJECT_ROOT / "sql" / "source_setup.sql"
EVIDENCE_PATH = PROJECT_ROOT / "docs" / "evidence" / "source_prep_reconciliation.json"


def read_csv_facts(path: Path) -> dict:
    """Facts computed straight from the CSV with the csv module (handles quoted newlines)."""
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        if header != EXPECTED_HEADER:
            raise ValueError(f"Unexpected CSV header.\n  got:      {header}\n  expected: {EXPECTED_HEADER}")
        rows = 0
        empty = {c: 0 for c in header}
        years, categories, winners_true = [], set(), 0
        for rec in reader:
            if len(rec) != len(header):
                raise ValueError(f"Row {rows + 2} has {len(rec)} fields, expected {len(header)}")
            rows += 1
            for col, value in zip(header, rec):
                if value == "":
                    empty[col] += 1
            years.append(int(rec[0]))
            categories.add(rec[4])
            winners_true += rec[9].strip().lower() == "true"
    return {
        "rows": rows,
        "empty_by_column": empty,
        "year_min": min(years),
        "year_max": max(years),
        "distinct_categories": len(categories),
        "winner_true": winners_true,
    }


def load_into_database(path: Path) -> None:
    ddl = SETUP_SQL.read_text(encoding="utf-8")
    copy_sql = (
        f"COPY grammy_awards ({', '.join(EXPECTED_HEADER)}) "
        "FROM STDIN WITH (FORMAT csv, HEADER true, NULL '')"
    )
    conn = psycopg2.connect(**source_db_params())
    try:
        with conn:  # one transaction: commit on success, rollback on any error
            with conn.cursor() as cur:
                cur.execute(ddl)
                with path.open(newline="", encoding="utf-8") as fh:
                    cur.copy_expert(copy_sql, fh)
    finally:
        conn.close()


def read_database_facts() -> dict:
    conn = psycopg2.connect(**source_db_params())
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM grammy_awards")
            rows = cur.fetchone()[0]
            empty = {}
            for col in EXPECTED_HEADER:
                # NULL (empty CSV field) or an empty string both count as "empty" for reconciliation
                cur.execute(
                    f"SELECT COUNT(*) FROM grammy_awards WHERE {col} IS NULL "
                    f"OR CAST({col} AS TEXT) = ''"
                )
                empty[col] = cur.fetchone()[0]
            cur.execute("SELECT MIN(year), MAX(year), COUNT(DISTINCT category), COUNT(*) FILTER (WHERE winner) FROM grammy_awards")
            ymin, ymax, ncat, wins = cur.fetchone()
    finally:
        conn.close()
    return {
        "rows": rows,
        "empty_by_column": empty,
        "year_min": ymin,
        "year_max": ymax,
        "distinct_categories": ncat,
        "winner_true": wins,
    }


def reconcile(csv_facts: dict, db_facts: dict) -> tuple[list[dict], bool]:
    checks = []

    def add(name, expected, actual):
        checks.append({"check": name, "csv": expected, "database": actual, "match": expected == actual})

    for key in ("rows", "year_min", "year_max", "distinct_categories", "winner_true"):
        add(key, csv_facts[key], db_facts[key])
    for col in EXPECTED_HEADER:
        add(f"empty_values[{col}]", csv_facts["empty_by_column"][col], db_facts["empty_by_column"][col])
    return checks, all(c["match"] for c in checks)


def ensure_dw_database() -> bool:
    """Environment bootstrap: create the (empty) Data Warehouse database if it does not exist. Returns True if created.

    Done here, in the one-off setup job, instead of a docker-entrypoint-initdb.d shell script: that script needs the
    executable bit, which is lost when the repository travels as a zip, and then the DW database silently never exists.
    The DW tables are created by load_dw (sql/dw_schema.sql, idempotent)."""
    name = dw_db_params()["dbname"]
    conn = psycopg2.connect(**source_db_params())
    conn.autocommit = True                                         # CREATE DATABASE cannot run inside a transaction
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,))
            if cur.fetchone():
                return False
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
            return True
    finally:
        conn.close()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    csv_path = grammy_csv_path()
    LOG.info("Source preparation started. CSV=%s", csv_path)
    LOG.info("DW database %s: %s", dw_db_params()["dbname"], "created" if ensure_dw_database() else "already exists")

    csv_facts = read_csv_facts(csv_path)
    LOG.info("CSV facts: rows=%s years=%s-%s", csv_facts["rows"], csv_facts["year_min"], csv_facts["year_max"])

    load_into_database(csv_path)
    db_facts = read_database_facts()
    LOG.info("Database facts: rows=%s", db_facts["rows"])

    checks, ok = reconcile(csv_facts, db_facts)
    report = {
        "executed_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "csv_file": csv_path.name,
        "target_table": "grammy_awards",
        "reconciled": ok,
        "checks": checks,
    }
    EVIDENCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    EVIDENCE_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")

    for c in checks:
        LOG.log(logging.INFO if c["match"] else logging.ERROR,
                "%-28s csv=%-8s db=%-8s %s", c["check"], c["csv"], c["database"], "OK" if c["match"] else "MISMATCH")
    if not ok:
        LOG.error("Reconciliation FAILED. See %s", EVIDENCE_PATH)
        return 1
    LOG.info("Reconciliation OK (%d checks). Evidence: %s", len(checks), EVIDENCE_PATH)
    return 0


if __name__ == "__main__":
    sys.exit(main())
