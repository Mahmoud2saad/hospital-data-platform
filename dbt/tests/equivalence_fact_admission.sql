-- The dbt model must equal the table built by the plain-SQL pipeline (rows returned = failures).
(SELECT * FROM {{ ref('fact_admission') }} EXCEPT SELECT * FROM {{ source('legacy_gold', 'fact_admission') }})
UNION ALL
(SELECT * FROM {{ source('legacy_gold', 'fact_admission') }} EXCEPT SELECT * FROM {{ ref('fact_admission') }})
