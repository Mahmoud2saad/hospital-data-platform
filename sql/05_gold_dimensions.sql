-- =========================================================================
-- 05_gold_dimensions.sql
-- Conformed dimensions of the star schema (docs/erd.png). Every dimension has
-- a synthetic "Unknown" member (SK = -1) so facts never lose rows to a
-- failed lookup. Surrogate keys are assigned deterministically at build time
-- (the whole gold layer is rebuilt together); in production they become
-- IDENTITY / sequence keys maintained by MERGE (see 12_*.sql).
-- gold.dim_doctor (SCD2) is built in 04_doctor_scd_attribution.sql.
-- =========================================================================
CREATE OR REPLACE TABLE gold.dim_date AS
WITH bounds AS (
    SELECT date_trunc('year', LEAST(
               (SELECT MIN(CAST(AdmissionDateTime AS DATE)) FROM silver.admission_trusted),
               (SELECT MIN(ServiceDate) FROM bronze.billing_transactions))) AS d0,
           date_trunc('year', (SELECT MAX(CAST(AdmissionDateTime AS DATE)) FROM silver.admission_trusted))
               + INTERVAL 2 YEAR - INTERVAL 1 DAY AS d1
)
SELECT
    CAST(strftime(d, '%Y%m%d') AS INTEGER) AS DateSK,
    CAST(d AS DATE)                        AS CalendarDate,
    year(d)                                AS Year,
    quarter(d)                             AS Quarter,
    month(d)                               AS Month,
    strftime(d, '%b')                      AS MonthName,
    CAST(date_trunc('month', d) AS DATE)   AS MonthStart,
    dayofweek(d)                           AS DayOfWeek,           -- 0 = Sunday
    CASE WHEN dayofweek(d) IN (5, 6) THEN 1 ELSE 0 END AS IsWeekend  -- KSA weekend: Fri/Sat
FROM bounds, generate_series(d0, d1, INTERVAL 1 DAY) AS t(d);

CREATE OR REPLACE TABLE gold.dim_branch AS
SELECT ROW_NUMBER() OVER (ORDER BY BranchName) AS BranchSK, BranchName,
       CASE BranchName WHEN 'Riyadh' THEN 'Central' WHEN 'Buraidah' THEN 'Central' WHEN 'Jeddah' THEN 'Western'
                       WHEN 'Dammam' THEN 'Eastern' WHEN 'Tabuk' THEN 'Northern' ELSE 'Unmapped' END AS Region
FROM (SELECT Branch AS BranchName FROM silver.admission_trusted
      UNION SELECT Branch FROM bronze.billing_transactions
      UNION SELECT Branch FROM silver.doctor_scd_trusted)
UNION ALL SELECT -1, 'UNKNOWN', 'Unknown';

CREATE OR REPLACE TABLE gold.dim_department AS
SELECT ROW_NUMBER() OVER (ORDER BY DepartmentName) AS DepartmentSK, DepartmentName,
       CASE WHEN DepartmentName IN ('Surgery', 'Orthopedics') THEN 'Surgical'
            WHEN DepartmentName = 'Emergency' THEN 'Emergency'
            ELSE 'Medical' END AS SpecialtyGroup
FROM (SELECT Department AS DepartmentName FROM silver.admission_trusted
      UNION SELECT Department FROM silver.doctor_scd_trusted)
UNION ALL SELECT -1, 'UNKNOWN', 'Unknown';

CREATE OR REPLACE TABLE gold.dim_patient AS
SELECT ROW_NUMBER() OVER (ORDER BY PatientID) AS PatientSK, PatientID, MIN(first_seen) AS FirstSeenDate
FROM (SELECT PatientID, CAST(AdmissionDateTime AS DATE) AS first_seen FROM silver.admission_trusted
      UNION ALL SELECT PatientID, ServiceDate FROM bronze.billing_transactions)
GROUP BY PatientID
UNION ALL SELECT -1, 'UNKNOWN', NULL;
