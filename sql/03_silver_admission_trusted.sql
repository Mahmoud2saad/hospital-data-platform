-- =========================================================================
-- 03_silver_admission_trusted.sql
-- Grain: ONE row per business AdmissionID. Get this wrong and every KPI
-- built on top (readmission, revenue, doctor productivity) is wrong.
--
-- Business rules implemented (numbering used throughout docs/):
--   BR-1  Valid admission = latest source version of the encounter, excluding
--         Cancelled and Test encounters and encounters deleted at source.
--   BR-2  Latest version wins: highest SourceVersion, tie-break latest
--         LastModifiedDateTime. Versions are never summed or averaged.
--
-- Steps:
--   1. Deduplicate source versions (BR-2).
--   2. Flag - never silently drop - orphan physician keys, impossible or
--      missing discharge timestamps, and source-side deletes.
--   3. Apply the valid-admission rule (BR-1).
-- =========================================================================
CREATE SCHEMA IF NOT EXISTS silver;

-- Physician SCD2 rows de-duplicated across batches: a re-sent row for the same
-- (DoctorNaturalKey, EffectiveFrom) supersedes the earlier one (e.g. an open
-- assignment later closed with an EffectiveTo).
CREATE OR REPLACE TABLE silver.doctor_scd_trusted AS
SELECT * EXCLUDE (rn, _batch_id, _loaded_at)
FROM (
    SELECT *, ROW_NUMBER() OVER (
               PARTITION BY DoctorNaturalKey, EffectiveFrom
               ORDER BY SourceLastUpdated DESC) AS rn
    FROM bronze.dim_doctors_scd
) WHERE rn = 1;

CREATE OR REPLACE TABLE silver.admission_trusted AS
WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (
               PARTITION BY AdmissionID
               ORDER BY SourceVersion DESC, LastModifiedDateTime DESC) AS rn
    FROM bronze.fact_admissions
),
latest AS (
    SELECT * EXCLUDE (rn, _batch_id, _loaded_at) FROM deduped WHERE rn = 1
),
flagged AS (
    SELECT
        l.*,
        CASE WHEN doc.DoctorNaturalKey IS NULL THEN 1 ELSE 0 END AS flag_orphan_doctor,
        CASE WHEN l.EncounterStatus = 'Completed' AND l.DischargeDateTime IS NULL
             THEN 1 ELSE 0 END AS flag_missing_discharge,
        CASE WHEN l.DischargeDateTime IS NOT NULL
                  AND l.DischargeDateTime < l.AdmissionDateTime THEN 1 ELSE 0 END AS flag_negative_los,
        CASE WHEN EXISTS (
                 SELECT 1 FROM bronze.source_change_log c
                 WHERE c.Entity = 'Admission' AND c.ChangeType = 'DELETE'
                   AND c.BusinessKey = l.AdmissionID
                   AND c.SourceLastModified >= l.LastModifiedDateTime)
             THEN 1 ELSE 0 END AS flag_source_deleted,
        CASE WHEN l.DischargeDateTime IS NOT NULL
                  AND l.DischargeDateTime >= l.AdmissionDateTime
             THEN DATEDIFF('hour', l.AdmissionDateTime, l.DischargeDateTime) END AS los_hours
    FROM latest l
    LEFT JOIN (SELECT DISTINCT DoctorNaturalKey FROM silver.doctor_scd_trusted) doc
           ON l.DoctorNaturalKey = doc.DoctorNaturalKey
)
SELECT
    *,
    CASE WHEN EncounterStatus IN ('Cancelled', 'Test') OR flag_source_deleted = 1
         THEN 0 ELSE 1 END AS is_valid_admission,
    CASE WHEN flag_missing_discharge = 1 OR flag_negative_los = 1 THEN 1 ELSE 0 END AS is_date_quarantined
FROM flagged;
