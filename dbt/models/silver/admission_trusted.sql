-- One row per AdmissionID. BR-1 (valid admission) and BR-2 (latest version wins).
WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY AdmissionID ORDER BY SourceVersion DESC, LastModifiedDateTime DESC) AS rn
    FROM {{ source('bronze', 'fact_admissions') }}
),
latest AS (SELECT * EXCLUDE (rn, _batch_id, _loaded_at) FROM deduped WHERE rn = 1),
flagged AS (
    SELECT
        l.*,
        CASE WHEN doc.DoctorNaturalKey IS NULL THEN 1 ELSE 0 END AS flag_orphan_doctor,
        CASE WHEN l.EncounterStatus = 'Completed' AND l.DischargeDateTime IS NULL THEN 1 ELSE 0 END AS flag_missing_discharge,
        CASE WHEN l.DischargeDateTime IS NOT NULL AND l.DischargeDateTime < l.AdmissionDateTime THEN 1 ELSE 0 END AS flag_negative_los,
        CASE WHEN EXISTS (
                 SELECT 1 FROM {{ source('bronze', 'source_change_log') }} c
                 WHERE c.Entity = 'Admission' AND c.ChangeType = 'DELETE' AND c.BusinessKey = l.AdmissionID
                   AND c.SourceLastModified >= l.LastModifiedDateTime)
             THEN 1 ELSE 0 END AS flag_source_deleted,
        CASE WHEN l.DischargeDateTime IS NOT NULL AND l.DischargeDateTime >= l.AdmissionDateTime
             THEN DATEDIFF('hour', l.AdmissionDateTime, l.DischargeDateTime) END AS los_hours
    FROM latest l
    LEFT JOIN (SELECT DISTINCT DoctorNaturalKey FROM {{ ref('doctor_scd_trusted') }}) doc
           ON l.DoctorNaturalKey = doc.DoctorNaturalKey
)
SELECT *,
    CASE WHEN EncounterStatus IN ('Cancelled', 'Test') OR flag_source_deleted = 1 THEN 0 ELSE 1 END AS is_valid_admission,
    CASE WHEN flag_missing_discharge = 1 OR flag_negative_los = 1 THEN 1 ELSE 0 END AS is_date_quarantined
FROM flagged
