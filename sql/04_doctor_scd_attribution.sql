-- =========================================================================
-- 04_doctor_scd_attribution.sql
-- BR-3  Historical activity is attributed to the physician's organisational
--       assignment EFFECTIVE ON THE EVENT DATE - never the current record.
--
-- Source convention: EffectiveFrom / EffectiveTo are INCLUSIVE calendar dates
-- (a row ending 2026-03-31 covers all of 31 March). The join therefore tests
-- admission < EffectiveTo + 1 day. (Treating EffectiveTo as an exclusive
-- midnight timestamp would push every admission on the last day of an
-- assignment into a false "coverage gap".)
--
-- Edge cases and how they resolve:
--   (a) OVERLAP    two SCD rows cover the event date (back-dated correction).
--                  Resolve to the row with the latest SourceLastUpdated and
--                  raise a HIGH exception.
--   (b) GAP        no SCD row covers the event date. Fall back to the nearest
--                  PRIOR assignment (not the current one) and raise MEDIUM.
--   (c) UNRESOLVED no prior assignment at all -> DoctorSK = -1 (Unknown).
--   (d) ORPHAN     key absent from the SCD entirely -> DoctorSK = -1, HIGH.
-- =========================================================================
CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.dim_doctor AS
SELECT
    ROW_NUMBER() OVER (ORDER BY DoctorNaturalKey, EffectiveFrom) AS DoctorSK,
    DoctorNaturalKey, DoctorName, Specialty, Department, Branch,
    EffectiveFrom, EffectiveTo, IsCurrent, SourceLastUpdated
FROM silver.doctor_scd_trusted
UNION ALL
SELECT -1, 'UNKNOWN', 'Unknown / Unresolved Physician', 'Unknown', 'Unknown', 'Unknown',
       TIMESTAMP '1900-01-01', NULL, 1, TIMESTAMP '1900-01-01';

CREATE OR REPLACE TABLE silver.doctor_attribution AS
WITH admissions AS (
    SELECT AdmissionID, DoctorNaturalKey, AdmissionDateTime
    FROM silver.admission_trusted
    WHERE flag_orphan_doctor = 0
),
exact_match AS (
    SELECT a.AdmissionID, d.DoctorSK,
           ROW_NUMBER() OVER (PARTITION BY a.AdmissionID ORDER BY d.SourceLastUpdated DESC) AS rn,
           COUNT(*)     OVER (PARTITION BY a.AdmissionID) AS n_matches
    FROM admissions a
    JOIN gold.dim_doctor d
      ON a.DoctorNaturalKey = d.DoctorNaturalKey
     AND d.EffectiveFrom <= a.AdmissionDateTime
     AND (d.EffectiveTo IS NULL OR a.AdmissionDateTime < d.EffectiveTo + INTERVAL 1 DAY)
),
fallback_prior AS (
    SELECT a.AdmissionID, d.DoctorSK,
           ROW_NUMBER() OVER (PARTITION BY a.AdmissionID ORDER BY d.EffectiveFrom DESC) AS rn
    FROM admissions a
    JOIN gold.dim_doctor d
      ON a.DoctorNaturalKey = d.DoctorNaturalKey
     AND d.EffectiveFrom <= a.AdmissionDateTime
    WHERE a.AdmissionID NOT IN (SELECT AdmissionID FROM exact_match)
)
SELECT
    a.AdmissionID,
    COALESCE(em.DoctorSK, fp.DoctorSK, -1) AS DoctorSK,
    CASE WHEN em.AdmissionID IS NOT NULL AND em.n_matches > 1 THEN 1 ELSE 0 END AS flag_scd_overlap,
    CASE WHEN em.AdmissionID IS NULL AND fp.AdmissionID IS NOT NULL THEN 1 ELSE 0 END AS flag_scd_gap_fallback,
    CASE WHEN em.AdmissionID IS NULL AND fp.AdmissionID IS NULL THEN 1 ELSE 0 END AS flag_scd_unresolved
FROM admissions a
LEFT JOIN (SELECT * FROM exact_match WHERE rn = 1) em ON a.AdmissionID = em.AdmissionID
LEFT JOIN (SELECT * FROM fallback_prior WHERE rn = 1) fp ON a.AdmissionID = fp.AdmissionID;
