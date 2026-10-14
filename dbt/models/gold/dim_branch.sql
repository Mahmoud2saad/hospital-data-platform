SELECT ROW_NUMBER() OVER (ORDER BY BranchName) AS BranchSK, BranchName,
       CASE BranchName WHEN 'Riyadh' THEN 'Central' WHEN 'Buraidah' THEN 'Central' WHEN 'Jeddah' THEN 'Western'
                       WHEN 'Dammam' THEN 'Eastern' WHEN 'Tabuk' THEN 'Northern' ELSE 'Unmapped' END AS Region
FROM (SELECT Branch AS BranchName FROM {{ ref('admission_trusted') }}
      UNION SELECT Branch FROM {{ source('bronze', 'billing_transactions') }}
      UNION SELECT Branch FROM {{ ref('doctor_scd_trusted') }})
UNION ALL SELECT -1, 'UNKNOWN', 'Unknown'
