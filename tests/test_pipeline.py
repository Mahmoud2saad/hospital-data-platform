"""
Warehouse-vs-oracle tests on a freshly generated dataset (seed 123, ~1.5k admissions).
Three families: (1) grain / integrity invariants, (2) business rules compared
ROW BY ROW against an independent pandas implementation, (3) detection of every
deliberately planted defect (recall) with no false alarms (precision).
"""
import pandas as pd
import pytest

from tests import oracle


# ------------------------------------------------------------------ grain & integrity
def test_fact_admission_grain_is_unique(con):
    assert con.execute("SELECT COUNT(*) - COUNT(DISTINCT AdmissionID) FROM gold.fact_admission").fetchone()[0] == 0


def test_raw_vs_trusted_counts_match_manifest(con, manifest):
    assert con.execute("SELECT COUNT(*) FROM bronze.fact_admissions").fetchone()[0] == manifest["counts"]["admission_source_rows"]
    assert con.execute("SELECT COUNT(*) FROM gold.fact_admission").fetchone()[0] == manifest["counts"]["distinct_admissions"]
    assert con.execute("SELECT COUNT(*) FROM silver.billing_trusted").fetchone()[0] == manifest["counts"]["billing_distinct_transactions"]


def test_no_admission_silently_dropped(con):
    n = con.execute("""SELECT COUNT(DISTINCT b.AdmissionID) FROM bronze.fact_admissions b
                       LEFT JOIN gold.fact_admission g USING (AdmissionID) WHERE g.AdmissionID IS NULL""").fetchone()[0]
    assert n == 0


def test_valid_admission_set_matches_oracle(con, raw):
    adm = oracle.admissions(raw)
    got = {r[0] for r in con.execute("SELECT AdmissionID FROM gold.fact_admission WHERE is_valid_admission = 1").fetchall()}
    assert got == set(adm[adm.valid].AdmissionID)


def test_latest_version_wins_including_tiebreak(con, raw):
    adm = oracle.admissions(raw)
    got = con.execute("SELECT AdmissionID, SourceVersion, LastModifiedDateTime, GrossCharge FROM gold.fact_admission").df()
    m = got.merge(adm[["AdmissionID", "SourceVersion", "LastModifiedDateTime", "GrossCharge"]], on="AdmissionID",
                  suffixes=("", "_o"))
    assert (m.SourceVersion == m.SourceVersion_o).all()
    assert (m.LastModifiedDateTime == m.LastModifiedDateTime_o).all()
    assert ((m.GrossCharge - m.GrossCharge_o).abs() < 0.005).all()


def test_dimension_keys_resolve(con):
    q = lambda s: con.execute(s).fetchone()[0]
    assert q("SELECT COUNT(*) FROM gold.fact_admission WHERE BranchSK = -1 OR DepartmentSK = -1 OR PatientSK = -1") == 0
    assert q("SELECT COUNT(*) FROM gold.fact_admission f LEFT JOIN gold.dim_date d ON d.DateSK = f.AdmitDateSK WHERE d.DateSK IS NULL") == 0
    assert q("SELECT COUNT(*) FROM gold.fact_billing_transaction f LEFT JOIN gold.dim_date d ON d.DateSK = f.ServiceDateSK WHERE d.DateSK IS NULL") == 0
    for t, k in (("dim_branch", "BranchSK"), ("dim_department", "DepartmentSK"), ("dim_patient", "PatientSK"),
                 ("dim_doctor", "DoctorSK"), ("dim_date", "DateSK")):
        assert q(f"SELECT COUNT(*) - COUNT(DISTINCT {k}) FROM gold.{t}") == 0, f"{t} surrogate key not unique"


def test_doctor_sk_never_null(con):
    assert con.execute("SELECT COUNT(*) FROM gold.fact_admission WHERE DoctorSK IS NULL").fetchone()[0] == 0


# ------------------------------------------------------------------ planted defects: recall & precision
RULES = ["NON_INPATIENT_ENCOUNTER", "ORPHAN_DOCTOR_FK", "NEGATIVE_LOS", "MISSING_DISCHARGE", "SCD_OVERLAP",
         "SCD_COVERAGE_GAP", "ILLOGICAL_ADMIT_AFTER_DEATH", "SOURCE_DELETED"]


@pytest.mark.parametrize("rule", RULES)
def test_admission_rule_detects_exactly_the_planted_cases(con, manifest, rule):
    got = {r[0] for r in con.execute(
        "SELECT business_key FROM ctl.dq_exceptions WHERE rule_code = ? AND entity <> 'Billing_Extract'", [rule]).fetchall()}
    assert got == set(manifest["planted_admission_anomalies"][rule]), rule


def test_duplicate_admission_versions_detected(con, manifest):
    got = {r[0] for r in con.execute(
        "SELECT business_key FROM ctl.dq_exceptions WHERE rule_code='DUP_SOURCE_VERSION' AND entity='Admissions'").fetchall()}
    assert got == set(manifest["planted_admission_anomalies"]["DUP_SOURCE_VERSION"])


def test_billing_rules_detect_exactly_the_planted_cases(con, manifest):
    for rule in ("DUP_SOURCE_VERSION", "ORPHAN_BILLING_ADMISSION"):
        got = {r[0] for r in con.execute(
            "SELECT business_key FROM ctl.dq_exceptions WHERE rule_code=? AND entity='Billing_Extract'", [rule]).fetchall()}
        assert got == set(manifest["planted_billing_anomalies"][rule]), rule


def test_assignment_boundary_days_are_not_false_gap_or_overlap(con, manifest):
    """Regression: EffectiveTo is an inclusive date, so the last day of an assignment is covered."""
    ids = manifest["must_not_be_flagged_scd"]
    assert ids
    n = con.execute("SELECT COUNT(*) FROM gold.fact_admission WHERE AdmissionID IN (SELECT UNNEST(?)) "
                    "AND (flag_scd_gap_fallback = 1 OR flag_scd_overlap = 1)", [ids]).fetchone()[0]
    assert n == 0


def test_doctor_attribution_matches_oracle_for_every_admission(con, raw):
    adm = oracle.admissions(raw)
    exp = oracle.attribution(raw, adm)
    got = con.execute("""SELECT f.AdmissionID, d.DoctorNaturalKey AS doc_key, d.EffectiveFrom AS doc_from,
                                f.flag_scd_overlap AS overlap, f.flag_scd_gap_fallback AS gap, f.flag_orphan_doctor AS orphan
                         FROM gold.fact_admission f JOIN gold.dim_doctor d ON d.DoctorSK = f.DoctorSK""").df()
    m = got.merge(exp, on="AdmissionID", suffixes=("", "_o"))
    assert len(m) == len(exp)
    for c in ("overlap", "gap", "orphan"):
        assert (m[c] == m[c + "_o"]).all(), c
    resolved = m[m.orphan == 0]
    assert (resolved.doc_key == resolved.doc_key_o).all()
    assert (pd.to_datetime(resolved.doc_from) == pd.to_datetime(resolved.doc_from_o)).all()


def test_orphan_doctor_maps_to_unknown_member(con, manifest):
    n = con.execute("SELECT COUNT(*) FROM gold.fact_admission WHERE AdmissionID IN (SELECT UNNEST(?)) AND DoctorSK = -1",
                    [manifest["planted_admission_anomalies"]["ORPHAN_DOCTOR_FK"]]).fetchone()[0]
    assert n == len(manifest["planted_admission_anomalies"]["ORPHAN_DOCTOR_FK"])


def test_quarantine_keeps_rows_but_excludes_from_sequencing(con, manifest):
    ids = manifest["planted_admission_anomalies"]["NEGATIVE_LOS"] + manifest["planted_admission_anomalies"]["MISSING_DISCHARGE"]
    r = con.execute("SELECT COUNT(*), SUM(is_valid_admission), SUM(is_date_quarantined) FROM gold.fact_admission "
                    "WHERE AdmissionID IN (SELECT UNNEST(?))", [ids]).fetchone()
    assert r[0] == len(ids) and r[1] >= len(ids) - 2 and r[2] == len(ids)
    n = con.execute("SELECT COUNT(*) FROM gold.fact_readmission_index WHERE index_admission_id IN (SELECT UNNEST(?))",
                    [ids]).fetchone()[0]
    assert n == 0


# ------------------------------------------------------------------ readmission KPI
def test_readmission_matches_oracle_row_by_row(con, raw):
    exp = oracle.readmission(oracle.admissions(raw)).set_index("index_admission_id")
    got = con.execute("SELECT index_admission_id, is_denominator, is_numerator, next_admission_id, "
                      "flag_excluded_planned_readmission AS excluded_planned FROM gold.fact_readmission_index").df().set_index("index_admission_id")
    assert set(got.index) == set(exp.index)
    got, exp = got.sort_index(), exp.sort_index()
    assert (got.is_denominator == exp.is_denominator).all()
    assert (got.is_numerator == exp.is_numerator).all()
    assert (got.excluded_planned == exp.excluded_planned).all()
    num = got.is_numerator == 1
    assert (got[num].next_admission_id == exp[num].next_admission_id).all()


def test_readmission_rate_is_plausible_and_nonzero(con):
    den, num, rate = con.execute("SELECT eligible_index_admissions, unplanned_readmissions, readmission_rate_pct "
                                 "FROM gold.vw_readmission_rate_group").fetchone()
    assert den > 500 and num > 10 and 2 < rate < 20


def test_transfer_and_death_are_never_index_admissions(con):
    n = con.execute("SELECT COUNT(*) FROM gold.fact_readmission_index i JOIN gold.fact_admission f "
                    "ON f.AdmissionID = i.index_admission_id WHERE f.DischargeDisposition IN ('Died','Transfer')").fetchone()[0]
    assert n == 0


def test_cross_branch_readmission_counts(con):
    assert con.execute("SELECT COUNT(*) FROM gold.fact_readmission_index WHERE is_numerator = 1 AND next_branch <> index_branch").fetchone()[0] > 0


def test_readmission_aggregation_grains_agree(con):
    g = con.execute("SELECT unplanned_readmissions, eligible_index_admissions FROM gold.vw_readmission_rate_group").fetchone()
    for v in ("branch", "department", "physician"):
        s = con.execute(f"SELECT SUM(unplanned_readmissions), SUM(eligible_index_admissions) FROM gold.vw_readmission_rate_by_{v}").fetchone()
        assert tuple(s) == tuple(g), f"{v} grain disagrees with group"


# ------------------------------------------------------------------ revenue
def test_revenue_views_match_oracle(con, raw):
    b = oracle.billing(raw)
    svc = {pd.Period(r[0], "M"): float(r[1]) for r in con.execute("SELECT revenue_month, service_period_revenue FROM gold.vw_revenue_service_period").fetchall()}
    trx = {pd.Period(r[0], "M"): float(r[1]) for r in con.execute("SELECT revenue_month, transaction_period_revenue FROM gold.vw_revenue_transaction_period").fetchall()}
    assert svc == pytest.approx(oracle.revenue_by_month(b, "ServiceDate"))
    assert trx == pytest.approx(oracle.revenue_by_month(b, "TransactionDate"))


def test_both_revenue_views_have_the_same_grand_total(con):
    a = con.execute("SELECT SUM(service_period_revenue) FROM gold.vw_revenue_service_period").fetchone()[0]
    b = con.execute("SELECT SUM(transaction_period_revenue) FROM gold.vw_revenue_transaction_period").fetchone()[0]
    assert a == b


def test_bridge_components_sum_exactly_to_service_revenue_every_month(con):
    bad = con.execute("""SELECT COUNT(*) FROM gold.vw_revenue_reconciliation_bridge
        WHERE ABS(recognized_same_month + invoiced_in_later_month + credit_notes_and_reversals + adjustments
                  - total_service_period_revenue) > 0.005""").fetchone()[0]
    assert bad == 0


def test_credit_notes_keep_their_sign_and_link_to_an_invoice(con):
    assert con.execute("SELECT COUNT(*) FROM gold.fact_billing_transaction WHERE TransactionType='CN' AND AmountExVAT >= 0").fetchone()[0] == 0
    assert con.execute("""SELECT COUNT(*) FROM gold.fact_billing_transaction c LEFT JOIN gold.fact_billing_transaction i
        ON i.TransactionID = c.RelatedTransactionID WHERE c.TransactionType='CN' AND i.TransactionID IS NULL""").fetchone()[0] == 0


def test_late_invoices_land_in_a_later_month_than_service(con, manifest):
    ids = manifest["late_invoice_transactions"]
    assert ids
    n = con.execute("SELECT COUNT(*) FROM gold.fact_billing_transaction WHERE TransactionID IN (SELECT UNNEST(?)) "
                    "AND flag_crosses_month_boundary = 0", [ids]).fetchone()[0]
    assert n == 0


# ------------------------------------------------------------------ security (RLS)
def test_rls_visible_rows_match_oracle_for_every_user(con, raw):
    adm = oracle.admissions(raw)
    exp = oracle.entitlements(raw, adm)
    for user, ids in exp.items():
        got = {r[0] for r in con.execute("SELECT AdmissionID FROM gold.secured_admission(?)", [user]).fetchall()}
        assert got == ids, user


def test_rls_deactivated_and_unknown_users_see_nothing(con):
    for u in ("former.employee@mhg.example", "former.contractor@mhg.example", "nobody@mhg.example"):
        assert con.execute("SELECT COUNT(*) FROM gold.secured_admission(?)", [u]).fetchone()[0] == 0


def test_rls_revoked_grant_does_not_leak_on_active_user(con):
    br = {r[0] for r in con.execute("SELECT DISTINCT Branch FROM gold.secured_admission('mixed.access@mhg.example')").fetchall()}
    assert br == {"Buraidah"}


def test_rls_branch_directors_partition_the_enterprise_view(con):
    ent = con.execute("SELECT COUNT(*) FROM gold.secured_admission('enterprise.admin@mhg.example')").fetchone()[0]
    tot = sum(con.execute("SELECT COUNT(*) FROM gold.secured_admission(?)", [f"{b.lower()}.director@mhg.example"]).fetchone()[0]
              for b in ("Riyadh", "Jeddah", "Dammam", "Tabuk", "Buraidah"))
    assert ent == con.execute("SELECT COUNT(*) FROM gold.fact_admission").fetchone()[0] == tot


def test_rls_redundant_grants_do_not_duplicate_rows(con):
    # jeddah.director holds a Branch grant AND a redundant Department grant inside it
    n = con.execute("SELECT COUNT(*) FROM gold.secured_admission('jeddah.director@mhg.example')").fetchone()[0]
    d = con.execute("SELECT COUNT(DISTINCT AdmissionID) FROM gold.secured_admission('jeddah.director@mhg.example')").fetchone()[0]
    assert n == d


# ------------------------------------------------------------------ monitoring & ops
def test_dq_exceptions_linked_to_run_id(con):
    assert con.execute("SELECT COUNT(*) FROM ctl.dq_exceptions WHERE run_id IS NULL").fetchone()[0] == 0


def test_run_health_and_alerts_on_healthy_run(con):
    assert con.execute("SELECT steps_failed FROM ctl.vw_run_health ORDER BY run_id DESC LIMIT 1").fetchone()[0] == 0
    assert con.execute("SELECT COUNT(*) FROM ctl.vw_current_alerts").fetchone()[0] == 0


def test_every_step_is_logged_with_row_counts(con):
    r = con.execute("SELECT COUNT(*), SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) FROM ctl.pipeline_run_log").fetchone()
    assert r[0] == r[1] and r[0] >= 14
    assert con.execute("SELECT rows_out FROM ctl.pipeline_run_log WHERE table_name = 'bronze.fact_admissions'").fetchone()[0] > 0


def test_pipeline_is_idempotent(dataset, tmp_path):
    from run_pipeline import Pipeline
    d, _ = dataset
    outs = []
    for i in range(2):
        p = Pipeline(d, tmp_path / f"w{i}.duckdb", None, quiet=True)
        p.run_batch("full")
        outs.append({t: p.con.execute(f"SELECT * FROM {t} ORDER BY ALL").df() for t in
                     ("gold.fact_admission", "gold.fact_readmission_index", "gold.fact_billing_transaction",
                      "gold.dim_doctor", "gold.entitlement_expanded")})
        p.con.close()
    for t in outs[0]:
        pd.testing.assert_frame_equal(outs[0][t], outs[1][t], check_dtype=False)


def test_rerun_on_same_warehouse_does_not_duplicate_dq_history(dataset, tmp_path):
    """Second full run appends a NEW run_id to ctl.dq_exceptions (history for trending), never duplicates a run."""
    from run_pipeline import Pipeline
    d, _ = dataset
    p = Pipeline(d, tmp_path / "w.duckdb", None, quiet=True)
    p.run_batch("full")
    n1 = p.con.execute("SELECT COUNT(*) FROM ctl.dq_exceptions").fetchone()[0]
    p.con.execute("DELETE FROM bronze.fact_admissions; DELETE FROM bronze.dim_doctors_scd; "
                  "DELETE FROM bronze.billing_transactions; DELETE FROM bronze.source_change_log; "
                  "DELETE FROM bronze.security_matrix_rls")
    p.run_batch("full")
    runs = p.con.execute("SELECT run_id, COUNT(*) FROM ctl.dq_exceptions GROUP BY 1 ORDER BY 1").fetchall()
    assert len(runs) == 2 and runs[0][1] == runs[1][1] == n1
    p.con.close()
