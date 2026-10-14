-- BR-4..BR-7: 30-day unplanned readmission at index-admission grain.
WITH as_of AS (SELECT MAX(LastModifiedDateTime) AS as_of_ts FROM {{ ref('fact_admission') }}),
eligible_index AS (
    SELECT f.* FROM {{ ref('fact_admission') }} f
    WHERE f.is_valid_admission = 1 AND f.is_date_quarantined = 0 AND f.DischargeDisposition NOT IN ('Died', 'Transfer')
),
in_window AS (
    SELECT idx.AdmissionID AS index_admission_id, nxt.AdmissionID AS next_admission_id,
           nxt.AdmissionDateTime AS next_admission_dt, nxt.AdmissionType AS next_admission_type, nxt.Branch AS next_branch,
           DATEDIFF('day', idx.DischargeDateTime, nxt.AdmissionDateTime) AS days_to_next
    FROM eligible_index idx
    JOIN {{ ref('fact_admission') }} nxt
      ON idx.PatientID = nxt.PatientID AND nxt.AdmissionID <> idx.AdmissionID
     AND nxt.is_valid_admission = 1 AND nxt.is_date_quarantined = 0
     AND nxt.AdmissionDateTime > idx.DischargeDateTime
     AND nxt.AdmissionDateTime <= idx.DischargeDateTime + INTERVAL 30 DAY
),
unplanned AS (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY index_admission_id ORDER BY next_admission_dt) AS rn
                             FROM in_window WHERE next_admission_type <> 'Elective') WHERE rn = 1),
planned AS (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY index_admission_id ORDER BY next_admission_dt) AS rn
                           FROM in_window WHERE next_admission_type = 'Elective') WHERE rn = 1)
SELECT idx.AdmissionID AS index_admission_id, idx.PatientID, idx.PatientSK, idx.BranchSK, idx.DepartmentSK,
       idx.Branch AS index_branch, idx.Department AS index_department, idx.DoctorSK AS index_doctor_sk,
       idx.AdmissionType AS index_admission_type, idx.DischargeDateTime AS index_discharge_dt,
       u.next_admission_id, u.next_admission_dt, u.next_admission_type, u.next_branch, u.days_to_next,
       p.next_admission_id AS planned_admission_id_in_window, (SELECT as_of_ts FROM as_of) AS as_of_ts,
       CASE WHEN idx.DischargeDateTime >  (SELECT as_of_ts FROM as_of) - INTERVAL 30 DAY THEN 1 ELSE 0 END AS flag_insufficient_observation,
       CASE WHEN idx.DischargeDateTime <= (SELECT as_of_ts FROM as_of) - INTERVAL 30 DAY THEN 1 ELSE 0 END AS is_denominator,
       CASE WHEN idx.DischargeDateTime <= (SELECT as_of_ts FROM as_of) - INTERVAL 30 DAY AND u.next_admission_id IS NOT NULL THEN 1 ELSE 0 END AS is_numerator,
       CASE WHEN p.next_admission_id IS NOT NULL AND u.next_admission_id IS NULL THEN 1 ELSE 0 END AS flag_excluded_planned_readmission
FROM eligible_index idx
LEFT JOIN unplanned u ON idx.AdmissionID = u.index_admission_id
LEFT JOIN planned   p ON idx.AdmissionID = p.index_admission_id
