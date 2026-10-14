-- The dbt model must equal the table built by the plain-SQL pipeline (rows returned = failures).
(SELECT * FROM {{ ref('fact_billing_transaction') }} EXCEPT SELECT * FROM {{ source('legacy_gold', 'fact_billing_transaction') }})
UNION ALL
(SELECT * FROM {{ source('legacy_gold', 'fact_billing_transaction') }} EXCEPT SELECT * FROM {{ ref('fact_billing_transaction') }})
