-- =========================================================================
-- 12_production_scale_reference.sql
-- "How does this change at 500M+ rows?" The pipeline in this repo runs on
-- DuckDB (right engine for a local, reproducible, executable demo).
-- DuckDB has no partitioned tables, clustered columnstore indexes or MERGE
-- in the SQL Server / Snowflake sense, so THIS file is reference DDL for
-- production targets, not something executed against the local warehouse.
--
-- Two targets are shown side by side because hospital groups commonly run
-- SQL Server / SSIS close to the source HIS systems and Snowflake as the
-- enterprise warehouse.
-- =========================================================================

-- ---------------------------------------------------------------------
-- A) SQL Server target: partitioned table + incremental MERGE
-- ---------------------------------------------------------------------

/*
-- 1. Partition function/scheme on the admission month - this is what turns
--    "scan 500M rows" into "scan this month's partition" for any date-
--    bounded report, and lets old partitions be archived/compressed
--    independently without touching current data.
CREATE PARTITION FUNCTION pf_admission_month (DATE)
    AS RANGE RIGHT FOR VALUES ('2024-01-01','2024-02-01', ... );  -- monthly boundaries, generated

CREATE PARTITION SCHEME ps_admission_month
    AS PARTITION pf_admission_month ALL TO ([PRIMARY]);

CREATE TABLE gold.fact_admission (
    AdmissionSK        BIGINT IDENTITY(1,1) NOT NULL,
    AdmissionID         VARCHAR(20)          NOT NULL,
    AdmissionDateTime     DATETIME2            NOT NULL,
    DischargeDateTime      DATETIME2            NULL,
    PatientSK                BIGINT               NOT NULL,
    DoctorSK                   BIGINT               NOT NULL,
    BranchSK                     BIGINT               NOT NULL,
    DepartmentSK                   BIGINT               NOT NULL,
    -- ... remaining measure/flag columns ...
    CONSTRAINT PK_fact_admission PRIMARY KEY CLUSTERED (AdmissionDateTime, AdmissionSK)
) ON ps_admission_month (AdmissionDateTime);

-- 2. Nonclustered columnstore for analytic scan performance (star-schema
--    fact tables at this volume are columnstore, not rowstore, in SQL
--    Server - 10-100x scan compression/speedup for SUM/COUNT/GROUP BY).
CREATE NONCLUSTERED COLUMNSTORE INDEX ix_fact_admission_cs
    ON gold.fact_admission (DoctorSK, BranchSK, DepartmentSK, AdmissionDateTime, LOS_Hours, GrossCharge);

-- 3. Supporting B-tree indexes for point lookups / the readmission
--    self-join (PatientID + date range is the hot path for the readmission KPI at scale).
CREATE INDEX ix_fact_admission_patient_date
    ON gold.fact_admission (PatientSK, AdmissionDateTime) INCLUDE (DischargeDateTime, AdmissionID);

-- 4. Incremental load via MERGE, keyed on the business key, driven by the
--    watermark - never a full rebuild once a table crosses ~10-50M rows.
--    This is the production version of "CREATE OR REPLACE TABLE" in 05.
MERGE gold.fact_admission AS tgt
USING staging.admission_trusted_delta AS src   -- only rows with
                                                 -- AvailableToETL > @watermark
    ON tgt.AdmissionID = src.AdmissionID
WHEN MATCHED AND src.LastModifiedDateTime > tgt.LastModifiedDateTime THEN
    UPDATE SET tgt.DischargeDateTime = src.DischargeDateTime,
               tgt.DoctorSK = src.DoctorSK,
               tgt.LastModifiedDateTime = src.LastModifiedDateTime
               -- ... remaining columns ...
WHEN NOT MATCHED BY TARGET THEN
    INSERT (AdmissionID, AdmissionDateTime, DischargeDateTime, PatientSK, DoctorSK,
            BranchSK, DepartmentSK, LastModifiedDateTime)
    VALUES (src.AdmissionID, src.AdmissionDateTime, src.DischargeDateTime, src.PatientSK,
            src.DoctorSK, src.BranchSK, src.DepartmentSK, src.LastModifiedDateTime);
*/

-- ---------------------------------------------------------------------
-- B) Snowflake target: clustered table + Streams/Tasks incremental
-- ---------------------------------------------------------------------

/*
CREATE OR REPLACE TABLE gold.fact_admission (
    AdmissionID          VARCHAR,
    AdmissionDateTime      TIMESTAMP_NTZ,
    DischargeDateTime       TIMESTAMP_NTZ,
    PatientSK                 NUMBER,
    DoctorSK                    NUMBER,
    BranchSK                      NUMBER,
    DepartmentSK                    NUMBER
    -- ... remaining columns ...
)
CLUSTER BY (DATE_TRUNC('MONTH', AdmissionDateTime), BranchSK);
-- Snowflake has no explicit indexes; CLUSTER BY + automatic micro-partition
-- pruning is the equivalent lever - cluster on the columns date-range and
-- branch filters hit most often (matches the RLS branch predicate too).

-- A Stream captures row-level changes on the bronze landing table; a Task
-- runs the MERGE on a schedule (or triggered by the stream having data) -
-- this is the direct Snowflake equivalent of the SQL Server MERGE above,
-- and of the watermark-driven ctl.vw_next_batch pattern in sql/10.
CREATE OR REPLACE STREAM stream_bronze_admissions ON TABLE bronze.fact_admissions;

CREATE OR REPLACE TASK task_merge_fact_admission
    WAREHOUSE = ETL_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('stream_bronze_admissions')
AS
MERGE INTO gold.fact_admission AS tgt
USING (SELECT * FROM stream_bronze_admissions) AS src
    ON tgt.AdmissionID = src.AdmissionID
WHEN MATCHED AND src.LastModifiedDateTime > tgt.LastModifiedDateTime THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
*/

-- ---------------------------------------------------------------------
-- C) Semantic-model strategy at this volume (applies to either target)
-- ---------------------------------------------------------------------
-- Power BI / the reporting semantic layer should NOT query gold.fact_admission
-- directly for a KPI tile at 500M rows. Pre-aggregate the specific reusable
-- measures (readmission rate, revenue) into small materialized summary
-- tables refreshed by the same incremental job, exactly like
-- gold.vw_readmission_rate_by_* in this repo but materialized rather
-- than views once volume makes view-time aggregation too slow - the SQL
-- text of the measure does not change, only whether it's computed on
-- read or on write.
