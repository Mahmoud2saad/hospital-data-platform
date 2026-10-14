"""Structural checks on the Airflow DAG (Airflow itself is not required to run the suite)."""
import ast
from pathlib import Path

DAG = Path(__file__).resolve().parent.parent / "airflow" / "dags" / "meridian_pipeline.py"


def _tree():
    return ast.parse(DAG.read_text())


def test_dag_file_parses_and_declares_the_expected_tasks():
    tree = _tree()
    inner = {n.name for f in ast.walk(tree) if isinstance(f, ast.FunctionDef) and f.name == "meridian_incremental_pipeline"
             for n in ast.walk(f) if isinstance(n, ast.FunctionDef)}
    assert {"land_bronze", "transform", "data_quality_gate", "publish"} <= inner
    assert "BashOperator" in DAG.read_text() and "dbt build" in DAG.read_text()


def test_dag_is_idempotent_friendly():
    src = DAG.read_text()
    assert "catchup=False" in src and "max_active_runs=1" in src and '"retries"' in src


def test_dag_only_uses_pipeline_apis_that_exist():
    import sys
    sys.path.insert(0, str(DAG.parents[2] / "pipeline"))
    from run_pipeline import Pipeline
    for attr in ("bronze_load", "transform", "advance_watermark", "next_run_id", "export"):
        assert hasattr(Pipeline, attr), attr


def test_dag_task_flow_runs_end_to_end_without_airflow(dataset, tmp_path):
    """Execute the same calls the DAG tasks make, in order, against a fresh warehouse."""
    from run_pipeline import Pipeline
    d, _ = dataset
    p = Pipeline(d, tmp_path / "w.duckdb", None, reset=False, quiet=True)
    p.run_id = p.next_run_id()
    p.bronze_load("incremental", None)
    p.transform()
    p.advance_watermark()
    assert p.con.execute("SELECT COUNT(*) FROM ctl.vw_current_alerts").fetchone()[0] == 0
    assert p.count("gold.fact_admission") > 0
