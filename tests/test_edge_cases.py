"""
Hand-built micro scenarios whose correct answer is obvious by inspection.
These pin down the subtle business-rule semantics independently of the
random generator.
"""
import pandas as pd
import pytest

from run_pipeline import Pipeline

ADM_COLS = ["SourceSystem", "AdmissionID", "PatientID", "AdmissionDateTime", "DischargeDateTime", "AdmissionType",
            "EncounterStatus", "DischargeDisposition", "Branch", "Department", "DoctorNaturalKey",
            "LastModifiedDateTime", "SourceVersion", "GrossCharge"]


def adm(aid, pid, admit, disch, typ="Emergency", disp="Home", doc="D001", branch="Riyadh", dept="Cardiology",
        status="Completed"):
    lm = (pd.Timestamp(disch) + pd.Timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S") if disch else admit
    return ["HIS_" + branch, aid, pid, admit, disch, typ, status, disp, branch, dept, doc, lm, 1, 1000.0]


@pytest.fixture()
def micro(tmp_path):
    rows = [
        # P1: emergency stay, then an ELECTIVE return (planned), then an EMERGENCY return -> still a readmission
        adm("A1", "P1", "2026-01-01 08:00:00", "2026-01-10 08:00:00"),
        adm("A2", "P1", "2026-01-15 08:00:00", "2026-01-16 08:00:00", typ="Elective"),
        adm("A3", "P1", "2026-01-25 08:00:00", "2026-01-26 08:00:00"),
        # P2: only a planned (elective) return -> excluded, flagged, NOT a readmission
        adm("A4", "P2", "2026-01-01 08:00:00", "2026-01-10 08:00:00"),
        adm("A5", "P2", "2026-01-20 08:00:00", "2026-01-21 08:00:00", typ="Elective"),
        # P3: transfer disposition -> never an index admission
        adm("A6", "P3", "2026-01-02 08:00:00", "2026-01-04 08:00:00", disp="Transfer"),
        adm("A7", "P3", "2026-01-05 08:00:00", "2026-01-06 08:00:00"),
        # P5: exactly 30 days after discharge counts, 30 days + 1 minute does not
        adm("A8", "P5", "2026-02-01 08:00:00", "2026-02-02 08:00:00"),
        adm("A9", "P5", "2026-03-04 08:00:00", "2026-03-05 08:00:00"),            # exactly +30d
        adm("A10", "P6", "2026-02-01 08:00:00", "2026-02-02 08:00:00"),
        adm("A11", "P6", "2026-03-04 08:01:00", "2026-03-05 08:00:00"),           # +30d +1min
        # P7: second admission is Cancelled -> must not count
        adm("A12", "P7", "2026-02-01 08:00:00", "2026-02-02 08:00:00"),
        adm("A13", "P7", "2026-02-10 08:00:00", "2026-02-11 08:00:00", status="Cancelled"),
        # SCD boundary: D002 moved on 2026-04-01 (old row inclusive To = 2026-03-31)
        adm("A14", "P8", "2026-03-31 20:00:00", "2026-04-01 06:00:00", doc="D002", dept="Surgery"),
        # observation window: as-of is the max LastModified below (A99)
        adm("A15", "P9", "2026-05-20 08:00:00", "2026-05-22 08:00:00"),
        adm("A99", "P99", "2026-06-01 08:00:00", "2026-06-05 08:00:00"),
    ]
    pd.DataFrame(rows, columns=ADM_COLS).to_csv(tmp_path / "Admissions.csv", index=False)
    pd.DataFrame([
        ["D001", "Doc One", "Cardiology", "Cardiology", "Riyadh", "2025-01-01", None, 1, "2025-01-01 08:00:00"],
        ["D002", "Doc Two", "General Surgery", "Surgery", "Riyadh", "2025-01-01", "2026-03-31", 0, "2026-04-01 08:00:00"],
        ["D002", "Doc Two", "General Surgery", "Surgery", "Jeddah", "2026-04-01", None, 1, "2026-04-01 09:00:00"],
    ], columns=["DoctorNaturalKey", "DoctorName", "Specialty", "Department", "Branch", "EffectiveFrom",
                "EffectiveTo", "IsCurrent", "SourceLastUpdated"]).to_csv(tmp_path / "Physician_Assignments.csv", index=False)
    pd.DataFrame([["T1", "A1", "P1", "Riyadh", "INV", "2026-01-11 10:00:00", "2026-01-01", 500.0, "INV-A1", None,
                   "2026-01-11 11:00:00"]],
                 columns=["TransactionID", "AdmissionID", "PatientID", "Branch", "TransactionType", "TransactionDate",
                          "ServiceDate", "AmountExVAT", "InvoiceReference", "RelatedTransactionID",
                          "LastModifiedDateTime"]).to_csv(tmp_path / "Billing_Extract.csv", index=False)
    pd.DataFrame([["a@x.example", "Enterprise", "ALL", "ALL", 1]],
                 columns=["UserEmail", "AccessLevel", "Branch", "Department", "IsActive"]).to_csv(
        tmp_path / "Access_Matrix.csv", index=False)
    pd.DataFrame([[1, "HIS_Riyadh", "Admission", "A1", "INSERT", "2026-01-10 10:00:00", "2026-01-10 10:05:00", "x"]],
                 columns=["ChangeSequence", "SourceSystem", "Entity", "BusinessKey", "ChangeType",
                          "SourceLastModified", "AvailableToETL", "Comment"]).to_csv(
        tmp_path / "Change_Feed.csv", index=False)
    p = Pipeline(tmp_path, tmp_path / "w.duckdb", None, quiet=True)
    assert p.run_batch("full") == 0
    yield p.con
    p.con.close()


def row(con, idx):
    return con.execute("SELECT * FROM gold.fact_readmission_index WHERE index_admission_id = ?", [idx]).df().iloc[0]


def test_elective_return_does_not_mask_a_later_unplanned_readmission(micro):
    r = row(micro, "A1")
    assert r.is_numerator == 1 and r.next_admission_id == "A3" and r.planned_admission_id_in_window == "A2"
    assert r.flag_excluded_planned_readmission == 0


def test_only_a_planned_return_is_excluded_and_flagged(micro):
    r = row(micro, "A4")
    assert r.is_denominator == 1 and r.is_numerator == 0 and r.flag_excluded_planned_readmission == 1


def test_transfer_is_not_an_index_admission(micro):
    assert micro.execute("SELECT COUNT(*) FROM gold.fact_readmission_index WHERE index_admission_id = 'A6'").fetchone()[0] == 0


def test_thirty_day_window_boundary_is_inclusive_to_the_minute(micro):
    assert row(micro, "A8").is_numerator == 1
    assert row(micro, "A10").is_numerator == 0


def test_cancelled_return_never_counts(micro):
    assert row(micro, "A12").is_numerator == 0


def test_last_day_of_assignment_is_covered_not_a_gap(micro):
    r = micro.execute("SELECT flag_scd_gap_fallback, flag_scd_overlap, d.Branch FROM gold.fact_admission f "
                      "JOIN gold.dim_doctor d USING (DoctorSK) WHERE AdmissionID = 'A14'").fetchone()
    assert r == (0, 0, "Riyadh")


def test_recent_discharge_has_insufficient_observation(micro):
    r = row(micro, "A99")
    assert r.flag_insufficient_observation == 1 and r.is_denominator == 0 and r.is_numerator == 0
    r = row(micro, "A15")
    assert r.is_denominator == 0 or r.flag_insufficient_observation == 1
