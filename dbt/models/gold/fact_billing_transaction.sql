-- BR-8: one billing fact, two date roles; credit notes / adjustments keep their sign.
SELECT b.TransactionID, b.AdmissionID, b.PatientID, b.Branch, b.TransactionType,
       COALESCE(p.PatientSK, -1) AS PatientSK, COALESCE(br.BranchSK, -1) AS BranchSK,
       CAST(strftime(b.ServiceDate, '%Y%m%d') AS INTEGER) AS ServiceDateSK,
       CAST(strftime(b.TransactionDate, '%Y%m%d') AS INTEGER) AS TransactionDateSK,
       b.ServiceDate, b.TransactionDate, b.AmountExVAT, b.InvoiceReference, b.RelatedTransactionID,
       CASE WHEN date_trunc('month', b.ServiceDate) <> date_trunc('month', b.TransactionDate) THEN 1 ELSE 0 END AS flag_crosses_month_boundary,
       CASE WHEN b.RelatedTransactionID IS NOT NULL THEN 1 ELSE 0 END AS flag_linked_to_prior_transaction,
       CASE WHEN a.AdmissionID IS NULL THEN 1 ELSE 0 END AS flag_orphan_admission
FROM {{ ref('billing_trusted') }} b
LEFT JOIN {{ ref('admission_trusted') }} a ON b.AdmissionID = a.AdmissionID
LEFT JOIN {{ ref('dim_patient') }} p ON b.PatientID = p.PatientID
LEFT JOIN {{ ref('dim_branch') }} br ON b.Branch = br.BranchName
