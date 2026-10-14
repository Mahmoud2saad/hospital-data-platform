SELECT t.AdmissionID, t.PatientID, t.SourceSystem,
       COALESCE(p.PatientSK, -1) AS PatientSK, COALESCE(b.BranchSK, -1) AS BranchSK,
       COALESCE(dp.DepartmentSK, -1) AS DepartmentSK, COALESCE(da.DoctorSK, -1) AS DoctorSK,
       CAST(strftime(t.AdmissionDateTime, '%Y%m%d') AS INTEGER) AS AdmitDateSK,
       CAST(strftime(t.DischargeDateTime, '%Y%m%d') AS INTEGER) AS DischargeDateSK,
       t.AdmissionDateTime, t.DischargeDateTime, t.AdmissionType, t.EncounterStatus, t.DischargeDisposition,
       t.Branch, t.Department, t.DoctorNaturalKey, t.los_hours, t.GrossCharge, t.SourceVersion, t.LastModifiedDateTime,
       t.is_valid_admission, t.is_date_quarantined, t.flag_orphan_doctor, t.flag_missing_discharge,
       t.flag_negative_los, t.flag_source_deleted,
       COALESCE(da.flag_scd_overlap, 0) AS flag_scd_overlap, COALESCE(da.flag_scd_gap_fallback, 0) AS flag_scd_gap_fallback
FROM {{ ref('admission_trusted') }} t
LEFT JOIN {{ ref('doctor_attribution') }} da ON t.AdmissionID = da.AdmissionID
LEFT JOIN {{ ref('dim_patient') }} p ON t.PatientID = p.PatientID
LEFT JOIN {{ ref('dim_branch') }} b ON t.Branch = b.BranchName
LEFT JOIN {{ ref('dim_department') }} dp ON t.Department = dp.DepartmentName
