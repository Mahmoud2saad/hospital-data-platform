"""Schema-drift control: additive passes, breaking blocks only the affected entity."""
import shutil

import duckdb
import pandas as pd
import pytest

from run_pipeline import Pipeline


@pytest.fixture()
def wh(dataset, tmp_path):
    d, _ = dataset
    local = tmp_path / "data"
    shutil.copytree(d, local)
    p = Pipeline(local, tmp_path / "w.duckdb", None, quiet=True)
    assert p.run_batch("full") == 0           # establishes the last-known-good schema snapshots
    return p, local


def drift(p, run_id):
    return p.con.execute("SELECT entity, drift_type, column_name, severity FROM ctl.schema_drift_log WHERE run_id = ?",
                         [run_id]).fetchall()


def rerun(p):
    for t in ("bronze.fact_admissions", "bronze.dim_doctors_scd", "bronze.billing_transactions",
              "bronze.security_matrix_rls", "bronze.source_change_log"):
        p.con.execute(f"DELETE FROM {t}")
    failed = p.run_batch("full")
    return failed


def test_additive_column_is_logged_info_and_load_proceeds(wh):
    p, d = wh
    f = pd.read_csv(d / "Billing_Extract.csv")
    f["CostCenter"] = "CC-1"
    f.to_csv(d / "Billing_Extract.csv", index=False)
    assert rerun(p) == 0
    assert drift(p, p.run_id) == [("billing_transactions", "COLUMN_ADDED", "CostCenter", "INFO")]
    assert p.count("bronze.billing_transactions") > 0
    assert "CostCenter" not in p._cols("bronze.billing_transactions")       # not silently propagated


def test_removed_column_blocks_only_that_entity(wh):
    p, d = wh
    f = pd.read_csv(d / "Billing_Extract.csv").drop(columns=["AmountExVAT"])
    f.to_csv(d / "Billing_Extract.csv", index=False)
    rerun(p)
    assert ("billing_transactions", "COLUMN_REMOVED", "AmountExVAT", "BLOCKING") in drift(p, p.run_id)
    assert p.count("bronze.billing_transactions") == 0                       # skipped
    assert p.count("bronze.fact_admissions") > 0                              # others unaffected
    assert p.con.execute("SELECT COUNT(*) FROM ctl.vw_current_alerts WHERE alert_type = 'BLOCKING_SCHEMA_DRIFT'").fetchone()[0] == 1


def test_unsafe_type_change_blocks(wh):
    p, d = wh
    f = pd.read_csv(d / "Admissions.csv")
    f["GrossCharge"] = "N/A"
    f.to_csv(d / "Admissions.csv", index=False)
    rerun(p)
    assert ("fact_admissions", "TYPE_CHANGED", "GrossCharge", "BLOCKING") in drift(p, p.run_id)
    assert p.count("bronze.fact_admissions") == 0


def test_blocked_load_does_not_overwrite_the_trusted_baseline(wh):
    p, d = wh
    good = pd.read_csv(d / "Admissions.csv")
    bad = good.copy()
    bad["GrossCharge"] = "N/A"
    bad.to_csv(d / "Admissions.csv", index=False)
    rerun(p)
    snap = dict(p.con.execute("SELECT column_name, column_type FROM ctl.schema_snapshot WHERE entity='fact_admissions'").fetchall())
    assert snap["GrossCharge"] == "DOUBLE"
    good.to_csv(d / "Admissions.csv", index=False)                       # source fixed -> loads cleanly again
    assert rerun(p) == 0 and p.count("bronze.fact_admissions") > 0


def test_safe_widening_integer_to_double_is_allowed(wh):
    p, d = wh
    f = pd.read_csv(d / "Admissions.csv")
    f["SourceVersion"] = f["SourceVersion"].astype(float) + 0.5
    f.to_csv(d / "Admissions.csv", index=False)
    rerun(p)
    assert not [e for e in drift(p, p.run_id) if e[3] == "BLOCKING"]
