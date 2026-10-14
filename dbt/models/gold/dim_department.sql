SELECT ROW_NUMBER() OVER (ORDER BY DepartmentName) AS DepartmentSK, DepartmentName,
       CASE WHEN DepartmentName IN ('Surgery', 'Orthopedics') THEN 'Surgical'
            WHEN DepartmentName = 'Emergency' THEN 'Emergency' ELSE 'Medical' END AS SpecialtyGroup
FROM (SELECT Department AS DepartmentName FROM {{ ref('admission_trusted') }}
      UNION SELECT Department FROM {{ ref('doctor_scd_trusted') }})
UNION ALL SELECT -1, 'UNKNOWN', 'Unknown'
