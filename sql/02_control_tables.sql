-- =========================================================================
-- 02_control_tables.sql
-- Operational metadata - the difference between "a query that happens to be
-- right today" and a pipeline someone can operate at 3am.
--   ctl.watermark        per (source, entity): how far we have loaded
--   ctl.pipeline_run_log one row per step per run: rows, status, errors
--   ctl.dq_exceptions    every record flagged / quarantined / corrected.
--                        APPEND-ONLY BY RUN so quality can be trended.
-- =========================================================================
CREATE SCHEMA IF NOT EXISTS ctl;

CREATE TABLE IF NOT EXISTS ctl.watermark (
    source_system     VARCHAR,
    entity            VARCHAR,
    last_watermark_ts TIMESTAMP,   -- max AvailableToETL successfully processed
    last_run_id       INTEGER,
    updated_at        TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ctl.pipeline_run_log (
    run_id INTEGER, layer VARCHAR, table_name VARCHAR,
    status VARCHAR,                -- SUCCESS / FAILED
    rows_in BIGINT, rows_out BIGINT, rows_rejected BIGINT,
    started_at TIMESTAMP, ended_at TIMESTAMP, error_message VARCHAR
);

CREATE TABLE IF NOT EXISTS ctl.dq_exceptions (
    exception_id INTEGER, run_id INTEGER, layer VARCHAR, entity VARCHAR,
    business_key VARCHAR, rule_code VARCHAR, severity VARCHAR,
    description VARCHAR, treatment VARCHAR, detected_at TIMESTAMP
);
