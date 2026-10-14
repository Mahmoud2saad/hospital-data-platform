-- =========================================================================
-- 07_gold_readmission_kpi.sql
-- 30-day UNPLANNED readmission KPI at the INDEX-ADMISSION grain: one row per
-- eligible index admission, auditable end to end.
--
-- Interpretation decisions (defended in docs/design_decisions.md):
--   BR-4  Eligible index = valid, non-quarantined admission whose disposition
--         is NOT Died and NOT Transfer (a transfer continues the same episode
--         of care).
--   BR-5  "Exclude planned admissions" applies to the CANDIDATE READMISSION
--         (CMS-style): an Elective return is not an unplanned bounce-back.
--         The first UNPLANNED admission inside the window is matched; an
--         elective admission earlier in the window does not mask a later
--         unplanned one.
--   BR-6  The search is not branch-scoped (Jeddah -> Riyadh counts).
--   BR-7  Observation window: as-of = MAX(LastModifiedDateTime). Index
--         discharges inside the trailing 30 days lack a full window and are
--         flagged and excluded from the denominator (not silently dropped).
-- =========================================================================
CREATE OR REPLACE TABLE gold.fact_readmission_index AS
WITH as_of AS (
    SELECT MAX(LastModifiedDateTime) AS as_of_ts FROM gold.fact_admission
),
eligible_index AS (
    SELECT f.* FROM gold.fact_admission f
    WHERE f.is_valid_admission = 1 AND f.is_date_quarantined = 0
      AND f.DischargeDisposition NOT IN ('Died', 'Transfer')
),
in_window AS (
    SELECT idx.AdmissionID AS index_admission_id,
           nxt.AdmissionID AS next_admission_id, nxt.AdmissionDateTime AS next_admission_dt,
           nxt.AdmissionType AS next_admission_type, nxt.Branch AS next_branch,
           DATEDIFF('day', idx.DischargeDateTime, nxt.AdmissionDateTime) AS days_to_next
    FROM eligible_index idx
    JOIN gold.fact_admission nxt
      ON idx.PatientID = nxt.PatientID
     AND nxt.AdmissionID <> idx.AdmissionID
     AND nxt.is_valid_admission = 1 AND nxt.is_date_quarantined = 0
     AND nxt.AdmissionDateTime >  idx.DischargeDateTime
     AND nxt.AdmissionDateTime <= idx.DischargeDateTime + INTERVAL 30 DAY
),
unplanned AS (
    SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY index_admission_id ORDER BY next_admission_dt) AS rn
                   FROM in_window WHERE next_admission_type <> 'Elective') WHERE rn = 1
),
planned AS (
    SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY index_admission_id ORDER BY next_admission_dt) AS rn
                   FROM in_window WHERE next_admission_type = 'Elective') WHERE rn = 1
)
SELECT
    idx.AdmissionID AS index_admission_id, idx.PatientID, idx.PatientSK,
    idx.BranchSK, idx.DepartmentSK,
    idx.Branch AS index_branch, idx.Department AS index_department, idx.DoctorSK AS index_doctor_sk,
    idx.AdmissionType AS index_admission_type, idx.DischargeDateTime AS index_discharge_dt,
    u.next_admission_id, u.next_admission_dt, u.next_admission_type, u.next_branch, u.days_to_next,
    p.next_admission_id AS planned_admission_id_in_window,
    (SELECT as_of_ts FROM as_of) AS as_of_ts,
    CASE WHEN idx.DischargeDateTime >  (SELECT as_of_ts FROM as_of) - INTERVAL 30 DAY THEN 1 ELSE 0 END AS flag_insufficient_observation,
    CASE WHEN idx.DischargeDateTime <= (SELECT as_of_ts FROM as_of) - INTERVAL 30 DAY THEN 1 ELSE 0 END AS is_denominator,
    CASE WHEN idx.DischargeDateTime <= (SELECT as_of_ts FROM as_of) - INTERVAL 30 DAY
              AND u.next_admission_id IS NOT NULL THEN 1 ELSE 0 END AS is_numerator,
    CASE WHEN p.next_admission_id IS NOT NULL AND u.next_admission_id IS NULL THEN 1 ELSE 0 END AS flag_excluded_planned_readmission
FROM eligible_index idx
LEFT JOIN unplanned u ON idx.AdmissionID = u.index_admission_id
LEFT JOIN planned   p ON idx.AdmissionID = p.index_admission_id;

-- One measure pair (SUM(is_denominator), SUM(is_numerator)) re-sliced at every
-- grain, so the KPI can never be defined two different ways in two reports.
CREATE OR REPLACE VIEW gold.vw_readmission_rate_group AS
SELECT 'Group' AS Grain, SUM(is_denominator) AS eligible_index_admissions,
       SUM(is_numerator) AS unplanned_readmissions,
       ROUND(100.0 * SUM(is_numerator) / NULLIF(SUM(is_denominator), 0), 2) AS readmission_rate_pct
FROM gold.fact_readmission_index;

CREATE OR REPLACE VIEW gold.vw_readmission_rate_by_branch AS
SELECT index_branch AS Branch, SUM(is_denominator) AS eligible_index_admissions,
       SUM(is_numerator) AS unplanned_readmissions,
       ROUND(100.0 * SUM(is_numerator) / NULLIF(SUM(is_denominator), 0), 2) AS readmission_rate_pct
FROM gold.fact_readmission_index GROUP BY 1 ORDER BY 1;

CREATE OR REPLACE VIEW gold.vw_readmission_rate_by_department AS
SELECT index_branch AS Branch, index_department AS Department,
       SUM(is_denominator) AS eligible_index_admissions, SUM(is_numerator) AS unplanned_readmissions,
       ROUND(100.0 * SUM(is_numerator) / NULLIF(SUM(is_denominator), 0), 2) AS readmission_rate_pct
FROM gold.fact_readmission_index GROUP BY 1, 2 ORDER BY 1, 2;

CREATE OR REPLACE VIEW gold.vw_readmission_rate_by_physician AS
SELECT d.DoctorNaturalKey, d.DoctorName, r.index_branch AS Branch, r.index_department AS Department,
       SUM(r.is_denominator) AS eligible_index_admissions, SUM(r.is_numerator) AS unplanned_readmissions,
       ROUND(100.0 * SUM(r.is_numerator) / NULLIF(SUM(r.is_denominator), 0), 2) AS readmission_rate_pct
FROM gold.fact_readmission_index r
JOIN gold.dim_doctor d ON r.index_doctor_sk = d.DoctorSK
GROUP BY 1, 2, 3, 4
HAVING SUM(r.is_denominator) > 0
ORDER BY 7 DESC, 1, 3, 4;
