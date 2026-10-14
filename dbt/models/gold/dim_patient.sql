SELECT ROW_NUMBER() OVER (ORDER BY PatientID) AS PatientSK, PatientID, MIN(first_seen) AS FirstSeenDate
FROM (SELECT PatientID, CAST(AdmissionDateTime AS DATE) AS first_seen FROM {{ ref('admission_trusted') }}
      UNION ALL SELECT PatientID, ServiceDate FROM {{ source('bronze', 'billing_transactions') }})
GROUP BY PatientID
UNION ALL SELECT -1, 'UNKNOWN', NULL
