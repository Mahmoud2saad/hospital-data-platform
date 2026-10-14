"""CDC / watermark behaviour: batches converge to the full load, replays are no-ops, late arrivals are caught."""
import pandas as pd
import pytest

from run_pipeline import Pipeline

GOLD = {"gold.fact_admission": "AdmissionID", "gold.fact_readmission_index": "index_admission_id",
        "gold.fact_billing_transaction": "TransactionID", "gold.dim_doctor": "DoctorSK",
        "gold.entitlement_expanded": "ALL"}


@pytest.fixture(scope="module")
def pair(dataset, tmp_path_factory):
    d, _ = dataset
    t = tmp_path_factory.mktemp("inc")
    inc = Pipeline(d, t / "inc.duckdb", None, quiet=True)
    cuts = inc.batch_cutoffs(5)
    for c in cuts:
        assert inc.run_batch("incremental", c) == 0
    full = Pipeline(d, t / "full.duckdb", None, quiet=True)
    full.run_batch("full")
    return inc, full, cuts


def test_incremental_batches_converge_to_the_full_load(pair):
    inc, full, _ = pair
    for t, k in GOLD.items():
        pd.testing.assert_frame_equal(inc.con.execute(f"SELECT * FROM {t} ORDER BY {k}").df(),
                                      full.con.execute(f"SELECT * FROM {t} ORDER BY {k}").df(), check_dtype=False)


def test_bronze_row_counts_match_full_load(pair):
    inc, full, _ = pair
    for t in ("bronze.fact_admissions", "bronze.dim_doctors_scd", "bronze.billing_transactions", "bronze.source_change_log"):
        assert inc.count(t) == full.count(t), t


def test_replaying_the_last_batch_lands_nothing(pair):
    inc, _, cuts = pair
    before = {t: inc.count(t) for t in ("bronze.fact_admissions", "bronze.billing_transactions", "bronze.source_change_log")}
    assert inc.run_batch("incremental", cuts[-1]) == 0
    assert {t: inc.count(t) for t in before} == before


def test_each_batch_lands_only_rows_past_the_watermark(pair):
    inc, _, cuts = pair
    rows = inc.con.execute("SELECT run_id, rows_in FROM ctl.pipeline_run_log WHERE table_name='bronze.fact_admissions' ORDER BY run_id").fetchall()
    assert all(r[1] > 0 for r in rows[:5])
    assert rows[-1][1] == 0                                   # the replay


def test_watermark_equals_max_available_to_etl(pair):
    inc, full, _ = pair
    a = inc.con.execute("SELECT source_system, entity, last_watermark_ts FROM ctl.watermark ORDER BY 1,2").fetchall()
    b = full.con.execute("SELECT source_system, entity, last_watermark_ts FROM ctl.watermark ORDER BY 1,2").fetchall()
    assert a == b and a


def test_source_delete_takes_effect_only_once_its_event_has_arrived(dataset, tmp_path):
    d, manifest = dataset
    log = pd.read_csv(d / "Change_Feed.csv", parse_dates=["AvailableToETL"])
    dele = log[log.ChangeType == "DELETE"].sort_values("AvailableToETL").iloc[0]
    key, avail = dele.BusinessKey, dele.AvailableToETL
    p = Pipeline(d, tmp_path / "w.duckdb", None, quiet=True)
    p.run_batch("incremental", avail - pd.Timedelta(seconds=1))
    assert p.con.execute("SELECT is_valid_admission FROM gold.fact_admission WHERE AdmissionID=?", [key]).fetchone()[0] == 1
    p.run_batch("incremental", avail)
    assert p.con.execute("SELECT is_valid_admission FROM gold.fact_admission WHERE AdmissionID=?", [key]).fetchone()[0] == 0
    p.con.close()


def test_late_arriving_correction_replaces_the_earlier_version(dataset, tmp_path):
    """A corrected version that becomes available later must supersede the one already in gold."""
    d, _ = dataset
    adm = pd.read_csv(d / "Admissions.csv", parse_dates=["LastModifiedDateTime"])
    log = pd.read_csv(d / "Change_Feed.csv", parse_dates=["SourceLastModified", "AvailableToETL"])
    ups = log[(log.Entity == "Admission") & (log.ChangeType == "UPDATE")].sort_values("AvailableToETL")
    u = ups.iloc[len(ups) // 2]
    first = log[(log.Entity == "Admission") & (log.BusinessKey == u.BusinessKey) & (log.ChangeType == "INSERT")].iloc[0]
    p = Pipeline(d, tmp_path / "w.duckdb", None, quiet=True)
    p.run_batch("incremental", max(first.AvailableToETL, u.AvailableToETL) - pd.Timedelta(seconds=1)
                if first.AvailableToETL >= u.AvailableToETL else u.AvailableToETL - pd.Timedelta(seconds=1))
    v_before = p.con.execute("SELECT MAX(SourceVersion), COUNT(*) FROM bronze.fact_admissions WHERE AdmissionID=?", [u.BusinessKey]).fetchone()
    p.run_batch("incremental", u.AvailableToETL)
    final = p.con.execute("SELECT LastModifiedDateTime FROM gold.fact_admission WHERE AdmissionID=?", [u.BusinessKey]).fetchone()[0]
    assert pd.Timestamp(final) >= u.SourceLastModified
    assert p.con.execute("SELECT COUNT(*) FROM bronze.fact_admissions WHERE AdmissionID=?", [u.BusinessKey]).fetchone()[0] >= v_before[1]
    p.con.close()


def test_dq_history_accumulates_across_incremental_runs(pair):
    inc, _, _ = pair
    runs = inc.con.execute("SELECT COUNT(DISTINCT run_id) FROM ctl.dq_exceptions").fetchone()[0]
    assert runs >= 5
    assert inc.con.execute("SELECT COUNT(DISTINCT run_id) FROM ctl.vw_dq_trend").fetchone()[0] == runs
