SELECT * EXCLUDE (rn, _batch_id, _loaded_at)
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY TransactionID ORDER BY LastModifiedDateTime DESC) AS rn
      FROM {{ source('bronze', 'billing_transactions') }}) WHERE rn = 1
