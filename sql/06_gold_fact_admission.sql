-- =========================================================================
-- 06_gold_fact_admission.sql
-- Grain: one row per deduplicated business admission, with explicit
-- exception flags and dimension keys. Every downstream mart is built FROM
-- this table, never from bronze - one conformed definition of "admission".
--   Valid inpatient admissions = SUM(is_valid_admission)
-- =========================================================================
CREATE OR REPLACE TABLE gold.fact_admission AS
SELECT
    t.AdmissionID, t.PatientID, t.SourceSystem,
    COALESCE(p.PatientSK, -1)    AS PatientSK,
    COALESCE(b.BranchSK, -1)     AS BranchSK,
    COALESCE(dp.DepartmentSK, -1) AS DepartmentSK,
    COALESCE(da.DoctorSK, -1)    AS DoctorSK,
    CAST(strftime(t.AdmissionDateTime, '%Y%m%d') AS INTEGER) AS AdmitDateSK,
    CAST(strftime(t.DischargeDateTime, '%Y%m%d') AS INTEGER) AS DischargeDateSK,
    t.AdmissionDateTime, t.DischargeDateTime, t.AdmissionType, t.EncounterStatus,
    t.DischargeDisposition, t.Branch, t.Department, t.DoctorNaturalKey,
    t.los_hours, t.GrossCharge, t.SourceVersion, t.LastModifiedDateTime,
    t.is_valid_admission, t.is_date_quarantined,
    t.flag_orphan_doctor, t.flag_missing_discharge, t.flag_negative_los, t.flag_source_deleted,
    COALESCE(da.flag_scd_overlap, 0)      AS flag_scd_overlap,
    COALESCE(da.flag_scd_gap_fallback, 0) AS flag_scd_gap_fallback
FROM silver.admission_trusted t
LEFT JOIN silver.doctor_attribution da ON t.AdmissionID = da.AdmissionID
LEFT JOIN gold.dim_patient    p  ON t.PatientID  = p.PatientID
LEFT JOIN gold.dim_branch     b  ON t.Branch     = b.BranchName
LEFT JOIN gold.dim_department dp ON t.Department = dp.DepartmentName;
