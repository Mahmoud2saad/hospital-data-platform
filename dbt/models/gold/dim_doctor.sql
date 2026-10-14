SELECT ROW_NUMBER() OVER (ORDER BY DoctorNaturalKey, EffectiveFrom) AS DoctorSK,
       DoctorNaturalKey, DoctorName, Specialty, Department, Branch, EffectiveFrom, EffectiveTo, IsCurrent, SourceLastUpdated
FROM {{ ref('doctor_scd_trusted') }}
UNION ALL
SELECT -1, 'UNKNOWN', 'Unknown / Unresolved Physician', 'Unknown', 'Unknown', 'Unknown',
       TIMESTAMP '1900-01-01', NULL, 1, TIMESTAMP '1900-01-01'
