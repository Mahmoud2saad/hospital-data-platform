-- The dbt model must equal the table built by the plain-SQL pipeline (rows returned = failures).
(SELECT * FROM {{ ref('dim_doctor') }} EXCEPT SELECT * FROM {{ source('legacy_gold', 'dim_doctor') }})
UNION ALL
(SELECT * FROM {{ source('legacy_gold', 'dim_doctor') }} EXCEPT SELECT * FROM {{ ref('dim_doctor') }})
