-- The dbt model must equal the table built by the plain-SQL pipeline (rows returned = failures).
(SELECT * FROM {{ ref('fact_readmission_index') }} EXCEPT SELECT * FROM {{ source('legacy_gold', 'fact_readmission_index') }})
UNION ALL
(SELECT * FROM {{ source('legacy_gold', 'fact_readmission_index') }} EXCEPT SELECT * FROM {{ ref('fact_readmission_index') }})
