"""Runs the dbt project (13 models, 32 tests incl. row-for-row equivalence with the SQL pipeline)."""
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from run_pipeline import Pipeline

DBT = Path(__file__).resolve().parent.parent / "dbt"
pytestmark = pytest.mark.skipif(shutil.which("dbt") is None, reason="dbt-duckdb not installed")


def test_dbt_build_passes_and_matches_sql_pipeline(dataset, tmp_path):
    d, _ = dataset
    db = tmp_path / "w.duckdb"
    Pipeline(d, db, None, quiet=True).run_batch("full")
    work = tmp_path / "dbt"
    shutil.copytree(DBT, work, ignore=shutil.ignore_patterns("target", "logs"))
    r = subprocess.run(["dbt", "build", "--profiles-dir", ".", "--no-partial-parse"], cwd=work,
                       env={**os.environ, "MHG_DB": str(db)}, capture_output=True, text=True)
    assert r.returncode == 0, r.stdout[-3000:]
    assert "ERROR=0" in r.stdout and "PASS=45" in r.stdout
