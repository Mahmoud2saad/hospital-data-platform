"""
demo_incremental.py
===================
Proves the watermark-gated incremental path end to end (not just describes it).

  1. Replays the change feed as N batches into a fresh warehouse, printing what
     each batch landed and where each watermark moved.
  2. Replays the LAST batch again with the watermark unchanged (a retry after a
     mid-batch failure): it must land zero new bronze rows.
  3. Builds a second warehouse with one full load and asserts the incremental
     gold layer is IDENTICAL to it - late-arriving corrections included.

    python pipeline/demo_incremental.py [--batches 4] [--data data]
"""
from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_pipeline import ROOT, Pipeline  # noqa: E402

GOLD_TABLES = ["gold.fact_admission", "gold.fact_readmission_index", "gold.fact_billing_transaction",
               "gold.dim_doctor", "gold.entitlement_expanded"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", type=Path, default=ROOT / "data")
    ap.add_argument("--batches", type=int, default=4)
    a = ap.parse_args()
    tmp = Path(tempfile.mkdtemp(prefix="mhg_demo_"))

    inc = Pipeline(a.data, tmp / "inc.duckdb", None, quiet=True)
    cuts = inc.batch_cutoffs(a.batches)
    print(f"change feed replayed as {len(cuts)} batches\n")
    print(f"{'batch':>5} {'cutoff (AvailableToETL <=)':<28} {'admission rows':>15} {'billing rows':>13} {'valid admissions':>17}")
    for i, cut in enumerate(cuts, 1):
        inc.run_batch("incremental", cut, export=False)
        print(f"{i:>5} {str(cut):<28} {inc.count('bronze.fact_admissions'):>15,} "
              f"{inc.count('bronze.billing_transactions'):>13,} "
              f"{inc.con.execute('SELECT SUM(is_valid_admission) FROM gold.fact_admission').fetchone()[0]:>17,}")

    before = inc.count("bronze.fact_admissions"), inc.count("bronze.billing_transactions")
    inc.run_batch("incremental", cuts[-1], export=False)           # replay with the watermark as-is
    after = inc.count("bronze.fact_admissions"), inc.count("bronze.billing_transactions")
    assert before == after, f"replay landed new rows: {before} -> {after}"
    print(f"\nPASS replay of the last batch landed 0 new rows (bronze stayed at {after[0]:,} / {after[1]:,}).")

    full = Pipeline(a.data, tmp / "full.duckdb", None, quiet=True)
    full.run_batch("full", export=False)
    for t in GOLD_TABLES:
        order = {"gold.fact_admission": "AdmissionID", "gold.fact_readmission_index": "index_admission_id",
                 "gold.fact_billing_transaction": "TransactionID", "gold.dim_doctor": "DoctorSK",
                 "gold.entitlement_expanded": "ALL"}[t]
        a_df = inc.con.execute(f"SELECT * FROM {t} ORDER BY {order}").df()
        b_df = full.con.execute(f"SELECT * FROM {t} ORDER BY {order}").df()
        assert a_df.equals(b_df), f"{t} differs between incremental and full load"
    print(f"PASS incremental ({len(cuts)} batches) == full load across {len(GOLD_TABLES)} gold tables.")
    print("\nwatermarks after the last batch:")
    for r in inc.con.execute("SELECT * FROM ctl.watermark ORDER BY 1,2").fetchall()[:8]:
        print("   ", r[0], r[1], r[2])


if __name__ == "__main__":
    main()
