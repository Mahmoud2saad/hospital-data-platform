-- =========================================================================
-- 08_gold_revenue_reconciliation.sql
-- Two legitimate revenue views plus a bridge explaining why they differ.
-- Deliberately ONE fact table with two date roles (service date / transaction
-- date) - not two forked facts, which would duplicate every transaction and
-- invite a "which one is right" argument.
--   BR-8  Credit notes and adjustments keep their sign and belong to the
--         month they were RAISED, not the month of service.
-- Silver dedup: same "multiple source versions" problem as admissions
-- (an invoice amount corrected after first landing) -> latest version wins.
-- =========================================================================
CREATE OR REPLACE TABLE silver.billing_trusted AS
SELECT * EXCLUDE (rn, _batch_id, _loaded_at)
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY TransactionID ORDER BY LastModifiedDateTime DESC) AS rn
      FROM bronze.billing_transactions) WHERE rn = 1;

CREATE OR REPLACE TABLE gold.fact_billing_transaction AS
SELECT
    b.TransactionID, b.AdmissionID, b.PatientID, b.Branch, b.TransactionType,
    COALESCE(p.PatientSK, -1) AS PatientSK, COALESCE(br.BranchSK, -1) AS BranchSK,
    CAST(strftime(b.ServiceDate, '%Y%m%d') AS INTEGER)     AS ServiceDateSK,
    CAST(strftime(b.TransactionDate, '%Y%m%d') AS INTEGER) AS TransactionDateSK,
    b.ServiceDate, b.TransactionDate, b.AmountExVAT, b.InvoiceReference, b.RelatedTransactionID,
    CASE WHEN date_trunc('month', b.ServiceDate) <> date_trunc('month', b.TransactionDate) THEN 1 ELSE 0 END AS flag_crosses_month_boundary,
    CASE WHEN b.RelatedTransactionID IS NOT NULL THEN 1 ELSE 0 END AS flag_linked_to_prior_transaction,
    CASE WHEN a.AdmissionID IS NULL THEN 1 ELSE 0 END AS flag_orphan_admission
FROM silver.billing_trusted b
LEFT JOIN silver.admission_trusted a ON b.AdmissionID = a.AdmissionID
LEFT JOIN gold.dim_patient p ON b.PatientID = p.PatientID
LEFT JOIN gold.dim_branch br ON b.Branch = br.BranchName;

CREATE OR REPLACE VIEW gold.vw_revenue_service_period AS
SELECT date_trunc('month', ServiceDate) AS revenue_month, SUM(AmountExVAT) AS service_period_revenue
FROM gold.fact_billing_transaction GROUP BY 1 ORDER BY 1;

CREATE OR REPLACE VIEW gold.vw_revenue_transaction_period AS
SELECT date_trunc('month', TransactionDate) AS revenue_month, SUM(AmountExVAT) AS transaction_period_revenue
FROM gold.fact_billing_transaction GROUP BY 1 ORDER BY 1;

-- Bridge: per service month, partitions revenue by TYPE and timing so the
-- components always sum exactly to service-period revenue:
--   invoices billed in the service month | invoices billed in a later month
--   (pure timing) | credit notes | adjustments.
CREATE OR REPLACE VIEW gold.vw_revenue_reconciliation_bridge AS
SELECT
    date_trunc('month', ServiceDate) AS service_month,
    SUM(CASE WHEN TransactionType = 'INV' AND flag_crosses_month_boundary = 0 THEN AmountExVAT ELSE 0 END) AS recognized_same_month,
    SUM(CASE WHEN TransactionType = 'INV' AND flag_crosses_month_boundary = 1 THEN AmountExVAT ELSE 0 END) AS invoiced_in_later_month,
    SUM(CASE WHEN TransactionType = 'CN'  THEN AmountExVAT ELSE 0 END) AS credit_notes_and_reversals,
    SUM(CASE WHEN TransactionType = 'ADJ' THEN AmountExVAT ELSE 0 END) AS adjustments,
    SUM(AmountExVAT) AS total_service_period_revenue
FROM gold.fact_billing_transaction GROUP BY 1 ORDER BY 1;
