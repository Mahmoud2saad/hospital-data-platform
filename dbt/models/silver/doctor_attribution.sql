-- BR-3: physician assignment effective on the event date. EffectiveTo is an INCLUSIVE date.
WITH admissions AS (
    SELECT AdmissionID, DoctorNaturalKey, AdmissionDateTime FROM {{ ref('admission_trusted') }} WHERE flag_orphan_doctor = 0
),
exact_match AS (
    SELECT a.AdmissionID, d.DoctorSK,
           ROW_NUMBER() OVER (PARTITION BY a.AdmissionID ORDER BY d.SourceLastUpdated DESC) AS rn,
           COUNT(*)     OVER (PARTITION BY a.AdmissionID) AS n_matches
    FROM admissions a
    JOIN {{ ref('dim_doctor') }} d
      ON a.DoctorNaturalKey = d.DoctorNaturalKey AND d.EffectiveFrom <= a.AdmissionDateTime
     AND (d.EffectiveTo IS NULL OR a.AdmissionDateTime < d.EffectiveTo + INTERVAL 1 DAY)
),
fallback_prior AS (
    SELECT a.AdmissionID, d.DoctorSK,
           ROW_NUMBER() OVER (PARTITION BY a.AdmissionID ORDER BY d.EffectiveFrom DESC) AS rn
    FROM admissions a
    JOIN {{ ref('dim_doctor') }} d ON a.DoctorNaturalKey = d.DoctorNaturalKey AND d.EffectiveFrom <= a.AdmissionDateTime
    WHERE a.AdmissionID NOT IN (SELECT AdmissionID FROM exact_match)
)
SELECT a.AdmissionID,
       COALESCE(em.DoctorSK, fp.DoctorSK, -1) AS DoctorSK,
       CASE WHEN em.AdmissionID IS NOT NULL AND em.n_matches > 1 THEN 1 ELSE 0 END AS flag_scd_overlap,
       CASE WHEN em.AdmissionID IS NULL AND fp.AdmissionID IS NOT NULL THEN 1 ELSE 0 END AS flag_scd_gap_fallback,
       CASE WHEN em.AdmissionID IS NULL AND fp.AdmissionID IS NULL THEN 1 ELSE 0 END AS flag_scd_unresolved
FROM admissions a
LEFT JOIN (SELECT * FROM exact_match WHERE rn = 1) em ON a.AdmissionID = em.AdmissionID
LEFT JOIN (SELECT * FROM fallback_prior WHERE rn = 1) fp ON a.AdmissionID = fp.AdmissionID
