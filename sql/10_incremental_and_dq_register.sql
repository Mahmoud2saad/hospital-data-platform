-- =========================================================================
-- 10_incremental_and_dq_register.sql
--   (A) the watermark view that gates incremental pulls, and
--   (B) the data-quality exception register, built FROM the flags raised in
--       silver/gold so it can never drift out of sync with what the pipeline
--       actually did. Rows are appended per run (history kept for trending).
--
-- Session variable `run_id` is set by pipeline/run_pipeline.py.
-- =========================================================================

-- (A) The watermark is AvailableToETL, NOT SourceLastModified: a record can be
-- modified at T but only become known to ETL later (propagation lag). Using
-- SourceLastModified would silently skip late-arriving corrections. The
-- watermark advances to MAX(AvailableToETL) actually processed, never now().
CREATE OR REPLACE VIEW ctl.vw_next_batch AS
SELECT c.*
FROM bronze.source_change_log c
LEFT JOIN ctl.watermark w ON w.source_system = c.SourceSystem AND w.entity = c.Entity
WHERE c.AvailableToETL > COALESCE(w.last_watermark_ts, TIMESTAMP '1900-01-01');

-- (B) DQ register - one row per (entity, business key, rule) raised this run.
DELETE FROM ctl.dq_exceptions WHERE run_id = getvariable('run_id');

INSERT INTO ctl.dq_exceptions
SELECT
    (SELECT COALESCE(MAX(exception_id), 0) FROM ctl.dq_exceptions) + ROW_NUMBER() OVER (ORDER BY entity, rule_code, business_key),
    getvariable('run_id'), layer, entity, business_key, rule_code, severity, description, treatment,
    current_timestamp
FROM (
    SELECT 'bronze->silver' AS layer, 'Admissions' AS entity, AdmissionID AS business_key,
           'DUP_SOURCE_VERSION' AS rule_code, 'MEDIUM' AS severity,
           'Multiple source versions landed for this AdmissionID.' AS description,
           'Retained latest SourceVersion (tie-break: latest LastModifiedDateTime); earlier versions kept in bronze for audit.' AS treatment
    FROM (SELECT AdmissionID FROM bronze.fact_admissions GROUP BY 1 HAVING COUNT(*) > 1)

    UNION ALL SELECT 'bronze->silver', 'Admissions', AdmissionID, 'NON_INPATIENT_ENCOUNTER', 'LOW',
           'EncounterStatus = ' || EncounterStatus || ' (test/training or cancelled encounter).',
           'Excluded from is_valid_admission; retained in silver for traceability.'
    FROM silver.admission_trusted WHERE EncounterStatus IN ('Cancelled', 'Test')

    UNION ALL SELECT 'bronze->silver', 'Admissions', AdmissionID, 'ORPHAN_DOCTOR_FK', 'HIGH',
           'DoctorNaturalKey ' || DoctorNaturalKey || ' does not exist in Physician_Assignments.',
           'Mapped to the synthetic Unknown physician (DoctorSK = -1); admission retained; routed for master-data investigation.'
    FROM silver.admission_trusted WHERE flag_orphan_doctor = 1

    UNION ALL SELECT 'bronze->silver', 'Admissions', AdmissionID, 'NEGATIVE_LOS', 'HIGH',
           'DischargeDateTime is earlier than AdmissionDateTime (impossible length of stay).',
           'Admission retained for volume counts; quarantined (is_date_quarantined = 1) from date-sequenced logic (readmission).'
    FROM silver.admission_trusted WHERE flag_negative_los = 1

    UNION ALL SELECT 'bronze->silver', 'Admissions', AdmissionID, 'MISSING_DISCHARGE', 'HIGH',
           'EncounterStatus = Completed but DischargeDateTime is NULL.',
           'Admission retained for volume counts; quarantined (is_date_quarantined = 1) from date-sequenced logic (readmission).'
    FROM silver.admission_trusted WHERE flag_missing_discharge = 1

    UNION ALL SELECT 'bronze->silver', 'Admissions', AdmissionID, 'SOURCE_DELETED', 'MEDIUM',
           'A DELETE event for this admission arrived in the change feed after its latest version.',
           'Excluded from is_valid_admission (soft delete); row retained in bronze/silver for audit.'
    FROM silver.admission_trusted WHERE flag_source_deleted = 1

    UNION ALL SELECT 'silver->gold', 'Physician_Assignments', AdmissionID, 'SCD_OVERLAP', 'HIGH',
           'Two physician-assignment rows both cover this admission''s date (overlapping EffectiveFrom/EffectiveTo).',
           'Resolved to the row with the latest SourceLastUpdated (most recent correction); flagged, not silently dropped (BR-3).'
    FROM silver.doctor_attribution WHERE flag_scd_overlap = 1

    UNION ALL SELECT 'silver->gold', 'Physician_Assignments', AdmissionID, 'SCD_COVERAGE_GAP', 'MEDIUM',
           'No physician-assignment row covers this admission''s date (coverage gap in source history).',
           'Resolved to the nearest PRIOR assignment (never the current record); flagged for data-steward review.'
    FROM silver.doctor_attribution WHERE flag_scd_gap_fallback = 1

    UNION ALL SELECT 'bronze->silver', 'Billing_Extract', TransactionID, 'DUP_SOURCE_VERSION', 'MEDIUM',
           'Multiple versions landed for this TransactionID (amount correction).',
           'Retained the row with the latest LastModifiedDateTime; earlier version kept in bronze for audit.'
    FROM (SELECT TransactionID FROM bronze.billing_transactions GROUP BY 1 HAVING COUNT(*) > 1)

    UNION ALL SELECT 'silver->gold', 'Billing_Extract', TransactionID, 'ORPHAN_BILLING_ADMISSION', 'MEDIUM',
           'Transaction references AdmissionID ' || AdmissionID || ', which is not present in the admissions extract.',
           'Retained in revenue totals (cash is real) but flagged flag_orphan_admission = 1; routed to billing / HIS reconciliation.'
    FROM gold.fact_billing_transaction WHERE flag_orphan_admission = 1

    UNION ALL SELECT 'gold', 'Admissions', AdmissionID, 'ILLOGICAL_ADMIT_AFTER_DEATH', 'HIGH',
           'Patient has an inpatient admission after a prior encounter whose DischargeDisposition = Died.',
           'Both admissions retained (no evidence which is wrong); flagged for identity / MRN reconciliation - likely record-linkage or MRN reuse, not a valid readmission.'
    FROM (SELECT DISTINCT nxt.AdmissionID
          FROM gold.fact_admission died
          JOIN gold.fact_admission nxt
            ON died.PatientID = nxt.PatientID AND nxt.AdmissionDateTime > died.DischargeDateTime
          WHERE died.DischargeDisposition = 'Died'
            AND died.is_valid_admission = 1 AND nxt.is_valid_admission = 1)

    UNION ALL SELECT 'gold', 'Fact_Readmission_Index', index_admission_id, 'INSUFFICIENT_OBSERVATION_WINDOW', 'LOW',
           'Index discharge is within 30 days of the dataset as-of date; a qualifying readmission may exist beyond the data horizon.',
           'Excluded from the readmission denominator; re-evaluated automatically once later data lands.'
    FROM gold.fact_readmission_index WHERE flag_insufficient_observation = 1
);
