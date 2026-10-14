-- Active grants of the latest security-matrix snapshot, expanded to (user x branch x department).
WITH ent AS (
    SELECT DISTINCT UserEmail, AccessLevel, Branch, Department
    FROM {{ source('bronze', 'security_matrix_rls') }}
    WHERE IsActive = 1 AND _batch_id = (SELECT MAX(_batch_id) FROM {{ source('bronze', 'security_matrix_rls') }})
),
bd AS (SELECT DISTINCT Branch, Department, BranchSK, DepartmentSK FROM {{ ref('fact_admission') }})
SELECT DISTINCT UserEmail, Branch, Department, BranchSK, DepartmentSK FROM (
    SELECT e.UserEmail, bd.* FROM ent e CROSS JOIN bd WHERE e.AccessLevel = 'Enterprise'
    UNION ALL SELECT e.UserEmail, bd.* FROM ent e JOIN bd ON bd.Branch = e.Branch WHERE e.AccessLevel = 'Branch'
    UNION ALL SELECT e.UserEmail, bd.* FROM ent e JOIN bd ON bd.Branch = e.Branch AND bd.Department = e.Department WHERE e.AccessLevel = 'Department'
)
