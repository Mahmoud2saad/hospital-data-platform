-- Physician SCD2 rows de-duplicated across batches (re-sent row for the same key + EffectiveFrom supersedes).
SELECT * EXCLUDE (rn, _batch_id, _loaded_at)
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY DoctorNaturalKey, EffectiveFrom ORDER BY SourceLastUpdated DESC) AS rn
    FROM {{ source('bronze', 'dim_doctors_scd') }}
) WHERE rn = 1
