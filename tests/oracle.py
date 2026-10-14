"""
Independent pandas re-implementation of the business rules. It shares NO code
with the SQL layer: tests compare the warehouse against this oracle row by row,
so a bug has to be made twice, independently, to slip through.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd


def load_raw(d: Path) -> dict:
    ts = ["AdmissionDateTime", "DischargeDateTime", "LastModifiedDateTime"]
    adm = pd.read_csv(d / "Admissions.csv", parse_dates=ts)
    scd = pd.read_csv(d / "Physician_Assignments.csv", parse_dates=["EffectiveFrom", "EffectiveTo", "SourceLastUpdated"])
    bill = pd.read_csv(d / "Billing_Extract.csv", parse_dates=["TransactionDate", "ServiceDate", "LastModifiedDateTime"])
    sec = pd.read_csv(d / "Access_Matrix.csv")
    log = pd.read_csv(d / "Change_Feed.csv", parse_dates=["SourceLastModified", "AvailableToETL"])
    return dict(adm=adm, scd=scd, bill=bill, sec=sec, log=log)


# ---------------------------------------------------------------- admissions
def admissions(raw) -> pd.DataFrame:
    a = raw["adm"].sort_values(["AdmissionID", "SourceVersion", "LastModifiedDateTime"])
    a = a.drop_duplicates("AdmissionID", keep="last").copy()
    dele = raw["log"][raw["log"].ChangeType == "DELETE"][["BusinessKey", "SourceLastModified"]]
    m = a.merge(dele, left_on="AdmissionID", right_on="BusinessKey", how="left")
    deleted = (m["SourceLastModified"] >= m["LastModifiedDateTime"]).groupby(m["AdmissionID"]).any()
    a["deleted"] = a["AdmissionID"].map(deleted).fillna(False).astype(bool)
    a["valid"] = ~a.EncounterStatus.isin(["Cancelled", "Test"]) & ~a.deleted
    a["quarantined"] = ((a.EncounterStatus == "Completed") & a.DischargeDateTime.isna()) | \
                       (a.DischargeDateTime.notna() & (a.DischargeDateTime < a.AdmissionDateTime))
    return a.reset_index(drop=True)


def readmission(adm: pd.DataFrame) -> pd.DataFrame:
    as_of = adm.LastModifiedDateTime.max()
    ok = adm[adm.valid & ~adm.quarantined]
    by_pat = {p: g.sort_values("AdmissionDateTime") for p, g in ok.groupby("PatientID")}
    rows = []
    for r in ok[~ok.DischargeDisposition.isin(["Died", "Transfer"])].itertuples():
        g = by_pat[r.PatientID]
        w = g[(g.AdmissionID != r.AdmissionID) & (g.AdmissionDateTime > r.DischargeDateTime) &
              (g.AdmissionDateTime <= r.DischargeDateTime + pd.Timedelta(days=30))]
        unplanned = w[w.AdmissionType != "Elective"]
        planned = w[w.AdmissionType == "Elective"]
        full_window = r.DischargeDateTime <= as_of - pd.Timedelta(days=30)
        rows.append(dict(index_admission_id=r.AdmissionID, is_denominator=int(full_window),
                         is_numerator=int(full_window and len(unplanned) > 0),
                         next_admission_id=unplanned.iloc[0].AdmissionID if len(unplanned) else None,
                         excluded_planned=int(len(planned) > 0 and len(unplanned) == 0)))
    return pd.DataFrame(rows)


# ---------------------------------------------------------------- doctors
def scd_clean(raw) -> pd.DataFrame:
    s = raw["scd"].sort_values(["DoctorNaturalKey", "EffectiveFrom", "SourceLastUpdated"])
    return s.drop_duplicates(["DoctorNaturalKey", "EffectiveFrom"], keep="last")


def attribution(raw, adm) -> pd.DataFrame:
    """Expected (doctor row, overlap flag, gap flag) per admission. EffectiveTo is an INCLUSIVE date."""
    scd = scd_clean(raw)
    known = set(scd.DoctorNaturalKey)
    out = []
    for r in adm.itertuples():
        if r.DoctorNaturalKey not in known:
            out.append((r.AdmissionID, None, None, 0, 0, 1))
            continue
        d = r.AdmissionDateTime.normalize()
        s = scd[scd.DoctorNaturalKey == r.DoctorNaturalKey]
        hit = s[(s.EffectiveFrom <= d) & (s.EffectiveTo.isna() | (d <= s.EffectiveTo))]
        if len(hit) >= 1:
            best = hit.sort_values("SourceLastUpdated").iloc[-1]
            out.append((r.AdmissionID, best.DoctorNaturalKey, best.EffectiveFrom, int(len(hit) > 1), 0, 0))
        else:
            prior = s[s.EffectiveFrom <= d].sort_values("EffectiveFrom")
            if len(prior):
                out.append((r.AdmissionID, prior.iloc[-1].DoctorNaturalKey, prior.iloc[-1].EffectiveFrom, 0, 1, 0))
            else:
                out.append((r.AdmissionID, None, None, 0, 0, 0))
    return pd.DataFrame(out, columns=["AdmissionID", "doc_key", "doc_from", "overlap", "gap", "orphan"])


# ---------------------------------------------------------------- billing
def billing(raw) -> pd.DataFrame:
    b = raw["bill"].sort_values(["TransactionID", "LastModifiedDateTime"]).drop_duplicates("TransactionID", keep="last")
    return b.reset_index(drop=True)


def revenue_by_month(b: pd.DataFrame, col: str) -> dict:
    return b.groupby(b[col].dt.to_period("M"))["AmountExVAT"].sum().round(2).to_dict()


# ---------------------------------------------------------------- RLS
def entitlements(raw, adm) -> dict:
    """user -> set(AdmissionID) the user may see."""
    sec = raw["sec"]
    sec = sec[sec.IsActive == 1].drop_duplicates()
    combos = set(zip(adm.Branch, adm.Department))
    out = {u: set() for u in raw["sec"].UserEmail.unique()}
    for g in sec.itertuples():
        if g.AccessLevel == "Enterprise":
            allowed = combos
        elif g.AccessLevel == "Branch":
            allowed = {c for c in combos if c[0] == g.Branch}
        else:
            allowed = {(g.Branch, g.Department)} & combos
        ids = set(adm[[c in allowed for c in zip(adm.Branch, adm.Department)]].AdmissionID)
        out[g.UserEmail] |= ids
    return out
