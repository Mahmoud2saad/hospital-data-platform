import hashlib

import pandas as pd

from generate_data import generate


def _digest(d):
    return {p.name: hashlib.md5(p.read_bytes()).hexdigest() for p in sorted(d.glob("*")) if p.is_file()}


def test_generator_is_deterministic(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    generate(a, 400, seed=5)
    generate(b, 400, seed=5)
    assert _digest(a) == _digest(b)


def test_different_seed_gives_different_data(tmp_path):
    generate(tmp_path / "a", 400, seed=5)
    generate(tmp_path / "b", 400, seed=6)
    assert _digest(tmp_path / "a") != _digest(tmp_path / "b")


def test_manifest_counts_match_files(dataset):
    d, m = dataset
    assert len(pd.read_csv(d / "Admissions.csv")) == m["counts"]["admission_source_rows"]
    assert len(pd.read_csv(d / "Billing_Extract.csv")) == m["counts"]["billing_source_rows"]
    assert len(pd.read_csv(d / "Change_Feed.csv")) == m["counts"]["change_log_rows"]


def test_every_planted_anomaly_type_is_present(manifest):
    for rule, ids in manifest["planted_admission_anomalies"].items():
        assert ids, f"generator planted no {rule} cases"


def test_no_real_world_identifiers(dataset):
    d, _ = dataset
    for f in d.glob("*.csv"):
        text = f.read_text().lower()
        assert "sgh" not in text and "saudi" not in text


def test_every_source_row_has_a_change_log_entry(raw):
    log = raw["log"]
    for df, ent, key, ts in ((raw["adm"], "Admission", "AdmissionID", "LastModifiedDateTime"),
                             (raw["scd"], "Doctor", "DoctorNaturalKey", "SourceLastUpdated"),
                             (raw["bill"], "Transaction", "TransactionID", "LastModifiedDateTime")):
        keys = set(zip(log[log.Entity == ent].BusinessKey, log[log.Entity == ent].SourceLastModified))
        missing = [k for k in zip(df[key], df[ts]) if k not in keys]
        assert not missing, f"{ent}: {len(missing)} rows have no change-log entry"


def test_source_versions_have_unique_timestamps(raw):
    assert not raw["adm"].duplicated(["AdmissionID", "LastModifiedDateTime"]).any()
