-- =========================================================================
-- 00_schema_drift_control.sql
-- Schema-drift control tables.
--
-- Source systems WILL add, rename, drop or retype columns without asking
-- first. Undetected, that either throws an opaque cast error at 2am or,
-- worse, silently succeeds and drops a new column on the floor.
--
-- Before every bronze load the incoming file's columns (name + coarse type
-- family) are compared to the last-known-good schema stored here:
--   1. IDENTICAL             -> load proceeds.
--   2. ADDITIVE (new column) -> load proceeds, logged INFO. Silver/gold use
--      explicit column lists, so a new column can never silently change a
--      business number; it is triaged and added deliberately.
--   3. BREAKING (column removed / unsafe retype) -> that entity's load is
--      HALTED before bronze, logged BLOCKING. Other entities still load.
-- The comparison itself lives in pipeline/schema_guard.py.
-- =========================================================================
CREATE SCHEMA IF NOT EXISTS ctl;

CREATE TABLE IF NOT EXISTS ctl.schema_snapshot (
    entity          VARCHAR,
    column_name     VARCHAR,
    column_type     VARCHAR,
    ordinal_position INTEGER,
    captured_at     TIMESTAMP,
    PRIMARY KEY (entity, column_name)
);

CREATE TABLE IF NOT EXISTS ctl.schema_drift_log (
    drift_id    INTEGER,
    run_id      INTEGER,
    entity      VARCHAR,
    drift_type  VARCHAR,   -- COLUMN_ADDED / COLUMN_REMOVED / TYPE_CHANGED
    column_name VARCHAR,
    detail      VARCHAR,
    severity    VARCHAR,   -- INFO (additive) / BLOCKING (breaking)
    detected_at TIMESTAMP
);
