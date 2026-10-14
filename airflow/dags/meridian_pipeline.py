"""
Airflow DAG: hourly incremental run of the hospital-group data platform.

    land_bronze  ->  transform  ->  data_quality_gate  ->  dbt_build  ->  publish

* land_bronze        schema-drift check + watermark-gated incremental load into bronze
                     (rows with AvailableToETL in (watermark, data_interval_end]).
* transform          silver / gold / DQ register / monitoring via the plain-SQL layer.
* data_quality_gate  fails the run if ctl.vw_current_alerts is non-empty (pipeline failure,
                     blocking schema drift, or a HIGH-severity exception rate spike).
* dbt_build          the dbt port of the same models plus 32 tests, including row-for-row
                     equivalence with the SQL layer (runs only when the gate passes).
* publish            exports the marts, the Excel DQ register and the dashboard.

Retries are safe because every layer is idempotent and the watermark only advances at the
end of a successful load (see docs/runbook.md).

NOTE: written against the Airflow 2.9+/3.x TaskFlow API. The task logic it calls
(pipeline/run_pipeline.py) is covered by the pytest suite; the DAG file itself is validated
structurally by tests/test_dag.py and has not been run on a live Airflow deployment.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

try:                                    # Airflow 3.x
    from airflow.sdk import dag, task
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:                     # Airflow 2.x
    from airflow.decorators import dag, task
    from airflow.operators.bash import BashOperator

ROOT = Path(os.environ.get("MHG_HOME", Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(ROOT / "pipeline"))

DEFAULT_ARGS = {"owner": "data-engineering", "retries": 2, "retry_delay": timedelta(minutes=5),
                "execution_timeout": timedelta(minutes=30)}


def _pipeline():
    from run_pipeline import Pipeline
    return Pipeline(Path(os.environ.get("MHG_DATA", ROOT / "data")),
                    Path(os.environ.get("MHG_DB", ROOT / "output" / "warehouse.duckdb")),
                    ROOT / "output", reset=False, quiet=False)


@dag(dag_id="meridian_incremental_pipeline", schedule="@hourly", start_date=datetime(2026, 1, 1), catchup=False,
     max_active_runs=1, default_args=DEFAULT_ARGS, tags=["healthcare", "elt", "synthetic-data"])
def meridian_incremental_pipeline():

    @task
    def land_bronze(data_interval_end=None) -> int:
        p = _pipeline()
        p.run_id = p.next_run_id()
        p.bronze_load("incremental", None)           # demo data is historical -> load everything past the watermark
        return p.run_id

    @task
    def transform(run_id: int) -> int:
        p = _pipeline()
        p.run_id = run_id
        p.transform()
        p.advance_watermark()                         # only after bronze + silver/gold succeeded
        return run_id

    @task
    def data_quality_gate(run_id: int) -> int:
        p = _pipeline()
        alerts = p.con.execute("SELECT alert_type, detail FROM ctl.vw_current_alerts").fetchall()
        if alerts:
            raise RuntimeError(f"data-quality / health gate failed: {alerts}")
        return run_id

    dbt_build = BashOperator(task_id="dbt_build", cwd=str(ROOT / "dbt"),
                             bash_command="dbt build --profiles-dir . --no-partial-parse")

    @task
    def publish(run_id: int) -> None:
        p = _pipeline()
        p.export()
        os.system(f'{sys.executable} "{ROOT}/pipeline/export_dq_register.py" && '
                  f'{sys.executable} "{ROOT}/pipeline/build_dashboard.py"')

    rid = land_bronze()
    gated = data_quality_gate(transform(rid))
    gated >> dbt_build >> publish(gated)


meridian_incremental_pipeline()
