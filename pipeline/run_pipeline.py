"""
run_pipeline.py
===============
Orchestrator for the hospital-group data platform. Python only orchestrates
(order, logging, failure handling, export); every business rule lives in
sql/*.sql so it is reviewable by analysts and portable to SQL Server /
Snowflake without touching orchestration code.

Two modes
---------
full         Rebuild the warehouse from scratch from the source extracts.
incremental  Simulate production CDC. The change feed (Change_Feed) says
             when each source row became AvailableToETL; each batch loads only
             rows past the per-(source, entity) watermark up to a cutoff, then
             advances the watermark to the max AvailableToETL actually
             processed. Silver/gold are rebuilt from the accumulated bronze, so
             N incremental batches converge to EXACTLY the same gold state as
             one full load (asserted in tests/test_incremental.py).

Usage
-----
    python pipeline/run_pipeline.py                          # full load
    python pipeline/run_pipeline.py --mode incremental --batches 4
    python pipeline/run_pipeline.py --data /tmp/ds --db /tmp/x.duckdb --no-export
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from schema_guard import check_and_snapshot  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent

# bronze table -> (csv, change-log entity, business-key column, version-timestamp column)
BRONZE = {
    "bronze.fact_admissions": ("Admissions.csv", "Admission", "AdmissionID", "LastModifiedDateTime"),
    "bronze.dim_doctors_scd": ("Physician_Assignments.csv", "Doctor", "DoctorNaturalKey", "SourceLastUpdated"),
    "bronze.billing_transactions": ("Billing_Extract.csv", "Transaction", "TransactionID", "LastModifiedDateTime"),
    "bronze.security_matrix_rls": ("Access_Matrix.csv", None, None, None),
    "bronze.source_change_log": ("Change_Feed.csv", None, None, None),
}

# step -> (rows_in table, rows_out table)
TRANSFORMS = [
    ("03_silver_admission_trusted.sql", "bronze.fact_admissions", "silver.admission_trusted"),
    ("04_doctor_scd_attribution.sql", "silver.doctor_scd_trusted", "silver.doctor_attribution"),
    ("05_gold_dimensions.sql", None, "gold.dim_patient"),
    ("06_gold_fact_admission.sql", "silver.admission_trusted", "gold.fact_admission"),
    ("07_gold_readmission_kpi.sql", "gold.fact_admission", "gold.fact_readmission_index"),
    ("08_gold_revenue_reconciliation.sql", "bronze.billing_transactions", "gold.fact_billing_transaction"),
    ("09_security_rls.sql", None, "gold.entitlement_expanded"),
    ("10_incremental_and_dq_register.sql", None, None),
    ("11_monitoring.sql", None, None),
]

EXPORTS = {
    "gold_fact_admission.csv": "SELECT * FROM gold.fact_admission ORDER BY AdmissionID",
    "gold_fact_readmission_index.csv": "SELECT * FROM gold.fact_readmission_index ORDER BY index_admission_id",
    "gold_fact_billing_transaction.csv": "SELECT * FROM gold.fact_billing_transaction ORDER BY TransactionID",
    "gold_readmission_rate_group.csv": "SELECT * FROM gold.vw_readmission_rate_group",
    "gold_readmission_rate_by_branch.csv": "SELECT * FROM gold.vw_readmission_rate_by_branch",
    "gold_readmission_rate_by_department.csv": "SELECT * FROM gold.vw_readmission_rate_by_department",
    "gold_readmission_rate_by_physician.csv": "SELECT * FROM gold.vw_readmission_rate_by_physician",
    "gold_revenue_service_period.csv": "SELECT * FROM gold.vw_revenue_service_period",
    "gold_revenue_transaction_period.csv": "SELECT * FROM gold.vw_revenue_transaction_period",
    "gold_revenue_reconciliation_bridge.csv": "SELECT * FROM gold.vw_revenue_reconciliation_bridge",
    "gold_dim_doctor.csv": "SELECT * FROM gold.dim_doctor ORDER BY DoctorSK",
    "gold_entitlement_expanded.csv": "SELECT * FROM gold.entitlement_expanded ORDER BY ALL",
    "ctl_dq_exceptions.csv": "SELECT * FROM ctl.dq_exceptions ORDER BY run_id, severity, entity, business_key",
    "ctl_schema_drift_log.csv": "SELECT * FROM ctl.schema_drift_log ORDER BY drift_id",
    "ctl_pipeline_run_log.csv": "SELECT * FROM ctl.pipeline_run_log ORDER BY started_at",
    "ctl_watermark.csv": "SELECT * FROM ctl.watermark ORDER BY 1,2",
    "ctl_run_health.csv": "SELECT * FROM ctl.vw_run_health",
    "ctl_dq_trend.csv": "SELECT * FROM ctl.vw_dq_trend",
    "ctl_current_alerts.csv": "SELECT * FROM ctl.vw_current_alerts",
}


class Pipeline:
    def __init__(self, data_dir: Path, db_path: Path, out_dir: Path | None, reset: bool = True, quiet: bool = False):
        self.data_dir, self.db_path, self.out_dir, self.quiet = Path(data_dir), Path(db_path), out_dir, quiet
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        if reset and self.db_path.exists():
            self.db_path.unlink()
        self.con = duckdb.connect(str(self.db_path))
        for f in ("01_bronze_ddl.sql", "02_control_tables.sql", "00_schema_drift_control.sql"):
            self.con.execute((ROOT / "sql" / f).read_text())
        self.run_id = 0

    # ------------------------------------------------------------------ helpers
    def say(self, msg):
        if not self.quiet:
            print(msg)

    def count(self, table):
        return self.con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] if table else 0

    def next_run_id(self):
        return self.con.execute("SELECT COALESCE(MAX(run_id), 0) + 1 FROM ctl.pipeline_run_log").fetchone()[0]

    def log_step(self, layer, name, fn, rows_in_tbl=None, rows_out_tbl=None):
        started = datetime.now()
        try:
            res = fn()
            r_in, r_out = (res if isinstance(res, tuple) else (self.count(rows_in_tbl), self.count(rows_out_tbl)))
            rej = max(r_in - r_out, 0) if rows_in_tbl and rows_out_tbl else 0
            self.con.execute("INSERT INTO ctl.pipeline_run_log VALUES (?,?,?,?,?,?,?,?,?,?)",
                             [self.run_id, layer, name, "SUCCESS", r_in, r_out, rej, started, datetime.now(), None])
            self.say(f"  [OK]   {layer:<12} {name:<40} in={r_in:<8} out={r_out}")
        except Exception as e:  # log, then fail loudly - never swallow
            self.con.execute("INSERT INTO ctl.pipeline_run_log VALUES (?,?,?,?,?,?,?,?,?,?)",
                             [self.run_id, layer, name, "FAILED", 0, 0, 0, started, datetime.now(), str(e)])
            self.say(f"  [FAIL] {layer} {name}: {e}")
            raise

    def _cols(self, table):
        return [r[0] for r in self.con.execute(f"DESCRIBE {table}").fetchall() if not r[0].startswith("_")]

    # ------------------------------------------------------------------ bronze
    def bronze_load(self, mode: str, cutoff: datetime | None):
        cutoff = cutoff or datetime(2999, 12, 31)           # incremental without a cutoff = everything available
        self.say("-- schema-drift check + bronze --")
        log_csv = str(self.data_dir / "Change_Feed.csv")
        for table, (csv_name, entity, key, ts_col) in BRONZE.items():
            short = table.split(".")[1]
            csv = str(self.data_dir / csv_name)
            if not check_and_snapshot(self.con, self.run_id, short, csv, quiet=self.quiet):
                self.con.execute("INSERT INTO ctl.pipeline_run_log VALUES (?,?,?,?,?,?,?,?,?,?)",
                                 [self.run_id, "bronze", table, "FAILED", 0, 0, 0, datetime.now(), datetime.now(),
                                  "Breaking schema drift - load skipped, see ctl.schema_drift_log"])
                self.say(f"  [SKIP] {table}: breaking schema drift - not loaded this run")
                continue
            cols = ", ".join(f"s.{c}" for c in self._cols(table))
            meta = f"{self.run_id} AS _batch_id, current_timestamp AS _loaded_at"
            if mode == "full" or table == "bronze.security_matrix_rls":
                if table == "bronze.security_matrix_rls" and mode == "incremental" and self.count(table):
                    continue                                   # reference snapshot already landed
                sql = f"INSERT INTO {table} SELECT {cols}, {meta} FROM read_csv_auto('{csv}') s"
            elif table == "bronze.source_change_log":
                sql = (f"INSERT INTO {table} SELECT {cols}, {meta} FROM read_csv_auto('{csv}') s "
                       f"LEFT JOIN ctl.watermark w ON w.source_system = s.SourceSystem AND w.entity = s.Entity "
                       f"WHERE s.AvailableToETL > COALESCE(w.last_watermark_ts, TIMESTAMP '1900-01-01') "
                       f"AND s.AvailableToETL <= TIMESTAMP '{cutoff}'")
            else:
                sql = (f"INSERT INTO {table} SELECT {cols}, {meta} FROM read_csv_auto('{csv}') s "
                       f"JOIN (SELECT SourceSystem, Entity, BusinessKey, SourceLastModified, ChangeType, "
                       f"MIN(AvailableToETL) AS AvailableToETL FROM read_csv_auto('{log_csv}') "
                       f"GROUP BY ALL) c ON c.Entity = '{entity}' AND c.BusinessKey = s.{key} "
                       f"AND c.SourceLastModified = s.{ts_col} AND c.ChangeType <> 'DELETE' "
                       f"LEFT JOIN ctl.watermark w ON w.source_system = c.SourceSystem AND w.entity = c.Entity "
                       f"WHERE c.AvailableToETL > COALESCE(w.last_watermark_ts, TIMESTAMP '1900-01-01') "
                       f"AND c.AvailableToETL <= TIMESTAMP '{cutoff}'")

            def _load(sql=sql, table=table):
                before = self.count(table)
                self.con.execute(sql)
                total = self.count(table)
                return (total - before, total)       # rows_in = landed this run, rows_out = table total
            self.log_step("bronze", table, _load)

    def untracked_rows(self):
        """Source rows with no change-log entry would never load incrementally -> make that loud."""
        log_csv = str(self.data_dir / "Change_Feed.csv")
        found = []
        for table, (csv_name, entity, key, ts_col) in BRONZE.items():
            if not entity:
                continue
            n = self.con.execute(
                f"SELECT COUNT(*) FROM read_csv_auto('{self.data_dir / csv_name}') s "
                f"WHERE NOT EXISTS (SELECT 1 FROM read_csv_auto('{log_csv}') c WHERE c.Entity = '{entity}' "
                f"AND c.BusinessKey = s.{key} AND c.SourceLastModified = s.{ts_col})").fetchone()[0]
            if n:
                found.append((entity, n))
        return found

    # ------------------------------------------------------------------ transforms
    def transform(self):
        self.say("-- silver / gold --")
        self.con.execute(f"SET VARIABLE run_id = {self.run_id}")
        for sql_file, tin, tout in TRANSFORMS:
            def _run(f=sql_file):
                self.con.execute((ROOT / "sql" / f).read_text())
                if f.startswith("10_"):                       # DQ step reports exceptions raised this run
                    return (0, self.con.execute("SELECT COUNT(*) FROM ctl.dq_exceptions WHERE run_id = ?",
                                                [self.run_id]).fetchone()[0])
            self.log_step("silver/gold", sql_file, _run, tin, tout)

    def advance_watermark(self):
        """Advance to MAX(AvailableToETL) actually processed - never now()."""
        self.con.execute("""
            DELETE FROM ctl.watermark w WHERE EXISTS (
                SELECT 1 FROM bronze.source_change_log c WHERE c.SourceSystem = w.source_system AND c.Entity = w.entity)""")
        self.con.execute("""
            INSERT INTO ctl.watermark
            SELECT SourceSystem, Entity, MAX(AvailableToETL), ?, current_timestamp
            FROM bronze.source_change_log GROUP BY 1, 2""", [self.run_id])

    # ------------------------------------------------------------------ one run
    def run_batch(self, mode="full", cutoff: datetime | None = None, export=True):
        self.run_id = self.next_run_id()
        t0 = time.time()
        self.say(f"=== run {self.run_id} ({mode}{', cutoff ' + str(cutoff) if cutoff else ''}) ===")
        self.bronze_load(mode, cutoff)
        self.transform()
        self.advance_watermark()
        if mode == "incremental":
            for entity, n in self.untracked_rows():
                self.say(f"  [WARN] {n} {entity} source row(s) have no change-log entry and will never load incrementally")
        if export and self.out_dir:
            self.export()
        failed = self.con.execute("SELECT COUNT(*) FROM ctl.pipeline_run_log WHERE run_id=? AND status='FAILED'",
                                  [self.run_id]).fetchone()[0]
        self.say(f"=== run {self.run_id} finished in {time.time() - t0:.1f}s, {failed} failed step(s) ===")
        return failed

    def export(self):
        self.out_dir.mkdir(parents=True, exist_ok=True)
        for fname, query in EXPORTS.items():
            self.con.execute(query).df().to_csv(self.out_dir / fname, index=False)
        self.say(f"  exported {len(EXPORTS)} files to {self.out_dir}")

    def batch_cutoffs(self, n: int):
        ts = [r[0] for r in duckdb.sql(
            f"SELECT AvailableToETL FROM read_csv_auto('{self.data_dir / 'Change_Feed.csv'}') "
            f"ORDER BY 1").fetchall()]
        cuts = [ts[max(int(len(ts) * (i + 1) / n) - 1, 0)] for i in range(n)]
        cuts[-1] = ts[-1]
        return sorted(set(cuts))


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", type=Path, default=ROOT / "data")
    ap.add_argument("--db", type=Path, default=ROOT / "output" / "warehouse.duckdb")
    ap.add_argument("--out", type=Path, default=ROOT / "output")
    ap.add_argument("--mode", choices=["full", "incremental"], default="full")
    ap.add_argument("--batches", type=int, default=4, help="incremental: number of batches to replay")
    ap.add_argument("--no-export", action="store_true")
    a = ap.parse_args(argv)

    p = Pipeline(a.data, a.db, a.out)
    failed = 0
    if a.mode == "full":
        failed = p.run_batch("full", export=not a.no_export)
    else:
        for i, cut in enumerate(p.batch_cutoffs(a.batches), 1):
            failed += p.run_batch("incremental", cut, export=(not a.no_export and i == a.batches))
    p.con.close()
    if failed:
        raise SystemExit(f"{failed} step(s) failed - see ctl.pipeline_run_log")


if __name__ == "__main__":
    main()
