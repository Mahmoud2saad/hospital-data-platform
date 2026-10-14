"""
Mutation check: break a business rule on purpose and confirm the test-suite notices.

For each mutation the repo is copied to a temp dir, one SQL rule is altered, and the fast
subset of the suite is run. The mutation is "killed" (good) if pytest fails, "survived" (bad)
if it still passes.

    python scripts/mutation_check.py
"""
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MUTATIONS = [
    ("04_doctor_scd_attribution.sql", "EffectiveTo treated as exclusive (false coverage gaps)",
     "a.AdmissionDateTime < d.EffectiveTo + INTERVAL 1 DAY", "a.AdmissionDateTime < d.EffectiveTo"),
    ("07_gold_readmission_kpi.sql", "elective returns counted as unplanned readmissions",
     "WHERE next_admission_type <> 'Elective') WHERE rn = 1", "WHERE 1 = 1) WHERE rn = 1"),
    ("03_silver_admission_trusted.sql", "oldest source version wins instead of latest",
     "ORDER BY SourceVersion DESC, LastModifiedDateTime DESC", "ORDER BY SourceVersion ASC, LastModifiedDateTime ASC"),
    ("09_security_rls.sql", "revoked (IsActive = 0) grants still honoured",
     "WHERE IsActive = 1\n  AND _batch_id", "WHERE _batch_id"),
    ("08_gold_revenue_reconciliation.sql", "credit notes lose their sign in the bridge",
     "SUM(CASE WHEN TransactionType = 'CN'  THEN AmountExVAT ELSE 0 END)",
     "SUM(CASE WHEN TransactionType = 'CN'  THEN ABS(AmountExVAT) ELSE 0 END)"),
]


def main():
    survived = 0
    for sql, label, old, new in MUTATIONS:
        with tempfile.TemporaryDirectory() as t:
            work = Path(t) / "repo"
            shutil.copytree(ROOT, work, ignore=shutil.ignore_patterns("output", "__pycache__", ".pytest_cache", "dbt", "airflow"))
            f = work / "sql" / sql
            text = f.read_text()
            assert old in text, f"mutation target not found in {sql}"
            f.write_text(text.replace(old, new))
            r = subprocess.run([sys.executable, "-m", "pytest", "tests/test_edge_cases.py", "tests/test_pipeline.py", "-x", "-q"],
                               cwd=work, capture_output=True, text=True)
            killed = r.returncode != 0
            survived += not killed
            print(f"{'KILLED  ' if killed else 'SURVIVED'}  {sql:<38} {label}")
    print(f"\n{len(MUTATIONS) - survived}/{len(MUTATIONS)} mutations killed")
    sys.exit(1 if survived else 0)


if __name__ == "__main__":
    main()
