-- ============================================================================
-- Grammy OPERATIONAL SOURCE table  (Workshop §6.2 · source preparation)
--
-- This is NOT the ETL Load stage. It only makes the provided Grammy CSV available
-- in a relational database so the pipeline can EXTRACT from the database.
--
-- Design decisions
--   * Values are stored as delivered: no cleaning, no trimming, no case changes.
--     Quality problems must stay visible for the raw validation gate (§6.7).
--   * Empty CSV fields become NULL (CSV format, NULL '').
--   * grammy_row_id = position of the row in the CSV (1-based). The file has no
--     natural primary key: (year, category, nominee, artist) is unique in this file
--     but contains NULLs, so it cannot be a PK.
--   * published_at / updated_at are TIMESTAMPTZ: all 4,810 values parse (ISO 8601 with offset).
-- ============================================================================

DROP TABLE IF EXISTS grammy_awards;

CREATE TABLE grammy_awards (
    grammy_row_id  INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    year           INTEGER      NOT NULL,
    title          TEXT         NOT NULL,
    published_at   TIMESTAMPTZ  NOT NULL,
    updated_at     TIMESTAMPTZ  NOT NULL,
    category       TEXT         NOT NULL,
    nominee        TEXT,
    artist         TEXT,
    workers        TEXT,
    img            TEXT,
    winner         BOOLEAN      NOT NULL
);

COMMENT ON TABLE grammy_awards IS
    'Operational source for the Grammy branch. Loaded as-is from the_grammy_awards.csv by src/prepare_source.py.';
COMMENT ON COLUMN grammy_awards.grammy_row_id IS 'Row position in the source CSV (1-based). Technical key only.';
COMMENT ON COLUMN grammy_awards.year IS 'Award year label as delivered (e.g. 2019 for the 62nd ceremony). Not the ceremony calendar year.';
