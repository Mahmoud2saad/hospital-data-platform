import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipeline"))
sys.path.insert(0, str(ROOT))

from generate_data import generate          # noqa: E402
from run_pipeline import Pipeline           # noqa: E402
from tests import oracle                    # noqa: E402


@pytest.fixture(scope="session")
def dataset(tmp_path_factory):
    d = tmp_path_factory.mktemp("dataset")
    manifest = generate(d, n_admissions=1500, seed=123)
    return d, manifest


@pytest.fixture(scope="session")
def raw(dataset):
    return oracle.load_raw(dataset[0])


@pytest.fixture(scope="session")
def warehouse(dataset, tmp_path_factory):
    d, _ = dataset
    out = tmp_path_factory.mktemp("wh")
    p = Pipeline(d, out / "wh.duckdb", out / "csv", quiet=True)
    assert p.run_batch("full") == 0
    p.con.close()
    return out / "wh.duckdb"


@pytest.fixture(scope="session")
def con(warehouse):
    c = duckdb.connect(str(warehouse), read_only=True)
    yield c
    c.close()


@pytest.fixture(scope="session")
def manifest(dataset):
    return dataset[1]
