-- =========================================================================
-- 01_bronze_ddl.sql
-- Bronze layer: landed source extracts, no business logic. Append-only:
-- a source correction (new SourceVersion / LastModifiedDateTime) lands as a
-- NEW row, giving full history and the ability to rebuild silver/gold from
-- bronze without re-pulling the source systems.
--
-- Engine for the local build: DuckDB. Production target: SQL Server
-- (landing) -> Snowflake (curated); see 12_production_scale_reference.sql.
-- =========================================================================
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.fact_admissions (
    SourceSystem VARCHAR, AdmissionID VARCHAR, PatientID VARCHAR,
    AdmissionDateTime TIMESTAMP, DischargeDateTime TIMESTAMP,
    AdmissionType VARCHAR, EncounterStatus VARCHAR, DischargeDisposition VARCHAR,
    Branch VARCHAR, Department VARCHAR, DoctorNaturalKey VARCHAR,
    LastModifiedDateTime TIMESTAMP, SourceVersion INTEGER, GrossCharge DECIMAL(14,2),
    _batch_id INTEGER, _loaded_at TIMESTAMP   -- loader metadata, not in source
);

CREATE TABLE IF NOT EXISTS bronze.dim_doctors_scd (
    DoctorNaturalKey VARCHAR, DoctorName VARCHAR, Specialty VARCHAR,
    Department VARCHAR, Branch VARCHAR,
    EffectiveFrom TIMESTAMP, EffectiveTo TIMESTAMP, IsCurrent INTEGER,
    SourceLastUpdated TIMESTAMP,
    _batch_id INTEGER, _loaded_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.billing_transactions (
    TransactionID VARCHAR, AdmissionID VARCHAR, PatientID VARCHAR, Branch VARCHAR,
    TransactionType VARCHAR, TransactionDate TIMESTAMP, ServiceDate DATE,
    AmountExVAT DECIMAL(14,2), InvoiceReference VARCHAR, RelatedTransactionID VARCHAR,
    LastModifiedDateTime TIMESTAMP,
    _batch_id INTEGER, _loaded_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.security_matrix_rls (
    UserEmail VARCHAR, AccessLevel VARCHAR, Branch VARCHAR, Department VARCHAR, IsActive INTEGER,
    _batch_id INTEGER, _loaded_at TIMESTAMP   -- full snapshot per batch; latest batch wins
);

CREATE TABLE IF NOT EXISTS bronze.source_change_log (
    ChangeSequence INTEGER, SourceSystem VARCHAR, Entity VARCHAR, BusinessKey VARCHAR,
    ChangeType VARCHAR, SourceLastModified TIMESTAMP, AvailableToETL TIMESTAMP, Comment VARCHAR,
    _batch_id INTEGER, _loaded_at TIMESTAMP
);
