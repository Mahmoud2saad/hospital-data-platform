-- =========================================================================
-- 11_monitoring.sql
-- Logging records WHAT happened; monitoring answers "is the pipeline healthy
-- right now, and is data quality getting better or worse?" in one query.
-- These views are what a dashboard (Grafana / Power BI ops page / Slack
-- digest) sits on.
-- =========================================================================
CREATE OR REPLACE VIEW ctl.vw_run_health AS
SELECT run_id,
       MIN(started_at) AS run_started_at, MAX(ended_at) AS run_ended_at,
       DATEDIFF('second', MIN(started_at), MAX(ended_at)) AS run_duration_seconds,
       COUNT(*) AS steps_total,
       SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS steps_succeeded,
       SUM(CASE WHEN status = 'FAILED'  THEN 1 ELSE 0 END) AS steps_failed,
       MAX(CASE WHEN status = 'FAILED' THEN error_message END) AS last_error
FROM ctl.pipeline_run_log GROUP BY run_id ORDER BY run_id DESC;

-- Exception counts by severity PER RUN: turns the register from a snapshot
-- into an operating SLA.
CREATE OR REPLACE VIEW ctl.vw_dq_trend AS
SELECT run_id, severity, COUNT(*) AS exception_count
FROM ctl.dq_exceptions GROUP BY run_id, severity ORDER BY run_id DESC, severity;

CREATE OR REPLACE VIEW ctl.vw_schema_drift_trend AS
SELECT run_id, entity, severity, COUNT(*) AS drift_events
FROM ctl.schema_drift_log GROUP BY run_id, entity, severity ORDER BY run_id DESC;

-- Single "is anything wrong right now" check. Empty result = healthy.
-- The DQ alert is RATE-based (HIGH exceptions per valid admission), so it
-- scales with volume instead of firing on a fixed count. 5% is illustrative.
CREATE OR REPLACE VIEW ctl.vw_current_alerts AS
SELECT 'PIPELINE_FAILED' AS alert_type, CAST(run_id AS VARCHAR) AS detail
FROM ctl.vw_run_health
WHERE run_id = (SELECT MAX(run_id) FROM ctl.vw_run_health) AND steps_failed > 0
UNION ALL
SELECT 'BLOCKING_SCHEMA_DRIFT', entity FROM ctl.schema_drift_log
WHERE run_id = (SELECT MAX(run_id) FROM ctl.pipeline_run_log) AND severity = 'BLOCKING'
UNION ALL
SELECT 'HIGH_SEVERITY_DQ_RATE', CAST(ROUND(100.0 * t.exception_count / NULLIF(v.n, 0), 2) AS VARCHAR) || '% of valid admissions'
FROM ctl.vw_dq_trend t, (SELECT SUM(is_valid_admission) AS n FROM gold.fact_admission) v
WHERE t.run_id = (SELECT MAX(run_id) FROM ctl.pipeline_run_log)
  AND t.severity = 'HIGH' AND t.exception_count > 0.05 * v.n;
