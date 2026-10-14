"""
generate_data.py
================
Synthetic source-system extracts for a fictional multi-branch hospital group
("Meridian Health Group"). Nothing here is derived from real patient, billing
or staff data - every identifier, name and amount is random.

What it produces (same shape as the extracts the pipeline ingests):

    Admissions.csv       one row per admission *source version*
    Physician_Assignments.csv       physician assignment history (SCD type 2)
    Billing_Extract.csv  invoices / credit notes / adjustments
    Access_Matrix.csv   user -> branch / department entitlements
    Change_Feed.csv     CDC-style feed: when each change became available to ETL
    ground_truth.json         manifest of every anomaly that was deliberately planted

The generator is *adversarial on purpose*. Real hospital extracts are messy, so
a realistic set of defects is injected at configurable rates: duplicate source
versions, impossible/missing discharge dates, test and cancelled encounters,
orphan physician keys, overlapping / gapped SCD2 windows, admissions after a
recorded death, source-side deletes, late-arriving corrections, late invoices,
credit notes, orphan invoices, and so on. The manifest records exactly which
business keys were affected, so the test-suite can assert that the pipeline
catches every planted defect (recall) and nothing else (precision).

Usage:
    python pipeline/generate_data.py                       # demo profile  (~2.5k admissions)
    python pipeline/generate_data.py --admissions 250000   # scale benchmark
    python pipeline/generate_data.py --seed 7 --out /tmp/other_dataset

Deterministic: the same --seed and --admissions always give byte-identical files.
"""
from __future__ import annotations

import argparse
import json
import math
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent

# --------------------------------------------------------------------------- reference model
BRANCH_WEIGHT = {"Riyadh": 0.32, "Jeddah": 0.25, "Dammam": 0.15, "Tabuk": 0.14, "Buraidah": 0.14}
BRANCH_DEPTS = {
    "Riyadh": ["Cardiology", "Internal Medicine", "Neurology", "Surgery", "Emergency"],
    "Jeddah": ["Orthopedics", "Surgery", "Cardiology", "Internal Medicine", "Emergency"],
    "Dammam": ["Cardiology", "Surgery", "Internal Medicine"],
    "Tabuk": ["Internal Medicine", "Emergency", "Orthopedics"],
    "Buraidah": ["Emergency", "Internal Medicine", "Surgery"],
}
DEPT_SPECIALTY = {
    "Cardiology": "Cardiology", "Internal Medicine": "Internal Medicine", "Neurology": "Neurology",
    "Surgery": "General Surgery", "Orthopedics": "Orthopedics", "Emergency": "Emergency Medicine",
}
DEPT_DAILY_CHARGE = {  # SAR per inpatient day, before noise
    "Cardiology": 6500, "Neurology": 5500, "Surgery": 7000,
    "Orthopedics": 6000, "Emergency": 3500, "Internal Medicine": 3000,
}
FIRST = ["Ahmed", "Mona", "Yousef", "Sara", "Omar", "Laila", "Nour", "Hany", "Rania", "Karim", "Khalid",
         "Huda", "Faisal", "Reem", "Tariq", "Dina", "Salman", "Amal", "Bader", "Maha", "Walid", "Salma",
         "Majed", "Nada", "Ziad", "Lina", "Rakan", "Hessa", "Adel", "Jood", "Fahad", "Ghada", "Hamza",
         "Ibtisam", "Nasser", "Rawan", "Sami", "Taghreed", "Anas", "Wafa"]
LAST = ["Alharbi", "Alotaibi", "Alqahtani", "Alghamdi", "Alzahrani", "Alshehri", "Almutairi", "Aldosari",
        "Alsubaie", "Alshammari", "Alanazi", "Alyami", "Alsaedi", "Alomari", "Alruwaili", "Alharthi",
        "Alsulami", "Albishi", "Alqurashi", "Alhazmi", "Alasmari", "AlJuhani", "Almalki", "Alkhaldi",
        "Alsahli", "Alamri", "Alsayed", "Hassan", "Mostafa", "Fawzy", "Samir", "Nasser", "Wael", "Adel",
        "Khaled", "Younes", "Mansour", "Darwish", "Haddad", "Karam"]

TS = "%Y-%m-%d %H:%M:%S"
D = "%Y-%m-%d"


def _ts(x):
    return x.strftime(TS) if isinstance(x, (datetime, pd.Timestamp)) and not pd.isna(x) else ""


def _round_min(dt: datetime, q: int = 5) -> datetime:
    return dt.replace(second=0, microsecond=0, minute=(dt.minute // q) * q)


# --------------------------------------------------------------------------- doctors
def build_doctors(rng, n_adm, start: date, end: date):
    """Return (published SCD rows, truth segments by combo, anomaly metadata)."""
    combos = [(b, d) for b, ds in BRANCH_DEPTS.items() for d in ds]
    names, used = [], set()
    while len(names) < 400:
        nm = f"{rng.choice(FIRST)} {rng.choice(LAST)}"
        if nm not in used:
            used.add(nm)
            names.append(nm)
    name_iter = iter(names)

    scd, truth = [], {c: [] for c in combos}
    seq = 0
    base_start = date(2024, 1, 1)
    base_ts = datetime(2024, 1, 1, 8, 0)
    base_docs = []

    def new_key():
        nonlocal seq
        seq += 1
        return f"D{seq:03d}"

    for (b, d) in combos:
        n_base = 4 if b in ("Riyadh", "Jeddah") else 3
        for _ in range(n_base):
            base_docs.append(dict(key=new_key(), name=next(name_iter), branch=b, dept=d))

    # --- transfers: <=1 outgoing doctor per combo so every combo always keeps staff
    transfer_meta, boundary_slots = [], []
    by_combo = {}
    for bd in base_docs:
        by_combo.setdefault((bd["branch"], bd["dept"]), []).append(bd)
    transferring = set()
    for combo, docs in by_combo.items():
        if rng.random() < 0.55:
            transferring.add(docs[0]["key"])

    lo = start + timedelta(days=45)
    hi = end - timedelta(days=45)
    span = (hi - lo).days
    for bd in base_docs:
        k, nm, b, d = bd["key"], bd["name"], bd["branch"], bd["dept"]
        if k in transferring:
            td = lo + timedelta(days=int(rng.integers(0, span)))
            tb, tdpt = combos[int(rng.integers(0, len(combos)))]
            while (tb, tdpt) == (b, d):
                tb, tdpt = combos[int(rng.integers(0, len(combos)))]
            old_to = td - timedelta(days=1)
            scd.append(dict(DoctorNaturalKey=k, DoctorName=nm, Specialty=DEPT_SPECIALTY[d], Department=d, Branch=b,
                            EffectiveFrom=base_start, EffectiveTo=old_to, IsCurrent=0,
                            SourceLastUpdated=datetime.combine(old_to, datetime.min.time()) + timedelta(hours=10)))
            scd.append(dict(DoctorNaturalKey=k, DoctorName=nm, Specialty=DEPT_SPECIALTY[tdpt], Department=tdpt,
                            Branch=tb, EffectiveFrom=td, EffectiveTo=None, IsCurrent=1,
                            SourceLastUpdated=datetime.combine(td, datetime.min.time()) + timedelta(hours=9)))
            truth[(b, d)].append((k, base_start, old_to))
            truth[(tb, tdpt)].append((k, td, None))
            transfer_meta.append(dict(doctor=k, transfer_date=td, old=(b, d), new=(tb, tdpt)))
        else:
            scd.append(dict(DoctorNaturalKey=k, DoctorName=nm, Specialty=DEPT_SPECIALTY[d], Department=d, Branch=b,
                            EffectiveFrom=base_start, EffectiveTo=None, IsCurrent=1, SourceLastUpdated=base_ts))
            truth[(b, d)].append((k, base_start, None))

    # --- anomaly doctors (used ONLY by the planted admissions)
    n_anom = max(2, n_adm // 1500)
    overlap_docs, gap_docs = [], []
    for _ in range(n_anom):
        b, d = combos[int(rng.integers(0, len(combos)))]
        k = new_key()
        nm = next(name_iter)
        T = lo + timedelta(days=int(rng.integers(0, span)))           # old assignment ends (inclusive)
        newfrom = T - timedelta(days=6)                               # back-dated correction => 7-day overlap
        scd.append(dict(DoctorNaturalKey=k, DoctorName=nm, Specialty=DEPT_SPECIALTY[d], Department=d, Branch=b,
                        EffectiveFrom=base_start, EffectiveTo=T, IsCurrent=0,
                        SourceLastUpdated=datetime.combine(T, datetime.min.time()) + timedelta(days=1, hours=8)))
        scd.append(dict(DoctorNaturalKey=k, DoctorName=nm, Specialty=DEPT_SPECIALTY[d], Department=d, Branch=b,
                        EffectiveFrom=newfrom, EffectiveTo=None, IsCurrent=1,
                        SourceLastUpdated=datetime.combine(T, datetime.min.time()) + timedelta(days=1, hours=9)))
        overlap_docs.append(dict(doctor=k, branch=b, dept=d, win_from=newfrom, win_to=T))
    for _ in range(n_anom):
        b, d = combos[int(rng.integers(0, len(combos)))]
        k = new_key()
        nm = next(name_iter)
        G = lo + timedelta(days=int(rng.integers(0, span)))           # first uncovered day
        scd.append(dict(DoctorNaturalKey=k, DoctorName=nm, Specialty=DEPT_SPECIALTY[d], Department=d, Branch=b,
                        EffectiveFrom=base_start, EffectiveTo=G - timedelta(days=1), IsCurrent=0,
                        SourceLastUpdated=datetime.combine(G, datetime.min.time()) + timedelta(hours=7)))
        scd.append(dict(DoctorNaturalKey=k, DoctorName=nm, Specialty=DEPT_SPECIALTY[d], Department=d, Branch=b,
                        EffectiveFrom=G + timedelta(days=4), EffectiveTo=None, IsCurrent=1,
                        SourceLastUpdated=datetime.combine(G + timedelta(days=4), datetime.min.time()) + timedelta(hours=8)))
        gap_docs.append(dict(doctor=k, branch=b, dept=d, win_from=G, win_to=G + timedelta(days=3)))

    return scd, truth, dict(transfers=transfer_meta, overlap=overlap_docs, gap=gap_docs, last_key_seq=seq)


# --------------------------------------------------------------------------- admissions
def gen_natural_admissions(rng, n, start: date, end: date, truth):
    branches = list(BRANCH_WEIGHT)
    bw = np.array([BRANCH_WEIGHT[b] for b in branches])
    bw = bw / bw.sum()
    t0 = datetime.combine(start, datetime.min.time())
    t_last = datetime.combine(end - timedelta(days=7), datetime.min.time())
    span_s = int((t_last - t0).total_seconds())

    def pick_doctor(branch, dept, when: datetime):
        d0 = when.date()
        cands = [k for (k, f, t) in truth[(branch, dept)] if f <= d0 and (t is None or d0 <= t)]
        return cands[int(rng.integers(0, len(cands)))] if cands else truth[(branch, dept)][0][0]

    def pick_dept(branch, adm_type):
        ds = BRANCH_DEPTS[branch]
        w = np.ones(len(ds))
        for i, dpt in enumerate(ds):
            if adm_type == "Emergency" and dpt == "Emergency":
                w[i] = 3.0
            if adm_type == "Elective" and dpt in ("Surgery", "Orthopedics"):
                w[i] = 2.0
            if adm_type == "Elective" and dpt == "Emergency":
                w[i] = 0.3
        return ds[int(rng.choice(len(ds), p=w / w.sum()))]

    rows, pseq = [], 0
    while len(rows) < n:
        pseq += 1
        pid = f"P{pseq:06d}"
        home = branches[int(rng.choice(len(branches), p=bw))]
        t = t0 + timedelta(seconds=int(rng.integers(0, span_s)))
        k = 0
        while True:
            if t > t_last:
                break
            adm_type = (rng.choice(["Elective", "Emergency", "Urgent"], p=[0.30, 0.45, 0.25]) if k == 0 else
                        rng.choice(["Elective", "Emergency", "Urgent"], p=[0.22, 0.50, 0.28]))
            branch = home if rng.random() < 0.88 else branches[int(rng.choice(len(branches), p=bw))]
            dept = pick_dept(branch, adm_type)
            median_h = {"Elective": 72, "Urgent": 66, "Emergency": 54}[adm_type]
            if dept == "Emergency":
                median_h *= 0.55
            los_h = float(np.clip(rng.lognormal(math.log(median_h), 0.6), 6, 480))
            admit = _round_min(t, 15)
            discharge = _round_min(admit + timedelta(hours=los_h), 5)
            p_die = 0.04 if adm_type == "Emergency" else 0.012
            r = rng.random()
            disposition = "Died" if r < p_die else ("Transfer" if r < p_die + 0.07 else "Home")
            charge = DEPT_DAILY_CHARGE[dept] * (los_h / 24) ** 0.8 * rng.lognormal(0, 0.25)
            rows.append(dict(
                PatientID=pid, AdmissionDateTime=admit, DischargeDateTime=discharge, AdmissionType=adm_type,
                EncounterStatus="Completed", DischargeDisposition=disposition, Branch=branch, Department=dept,
                DoctorNaturalKey=pick_doctor(branch, dept, admit), GrossCharge=round(max(charge, 800.0), 2),
                LastModifiedDateTime=discharge + timedelta(hours=2), SourceVersion=1, tags=set()))
            if disposition == "Died":
                break
            if rng.random() < 0.20:
                gap_days = rng.uniform(0.3, 29.5) if rng.random() < 0.5 else rng.uniform(31, 150)
                t = discharge + timedelta(days=gap_days)
                k += 1
            else:
                break
    return rows[:n]


def build_admissions(rng, n, start, end, truth, meta):
    rows = gen_natural_admissions(rng, n, start, end, truth)
    df = pd.DataFrame(rows)
    pool = list(rng.permutation(len(df)))      # natural rows available for mutation, each used once

    def take(k):
        return [pool.pop() for _ in range(min(k, len(pool)))]

    cnt = lambda rate, floor=2: max(floor, int(round(n * rate)))

    # ---- mutations of natural rows
    for i in take(cnt(0.003)):
        df.at[i, "DischargeDateTime"] = df.at[i, "AdmissionDateTime"] - timedelta(hours=int(rng.integers(6, 48)))
        df.at[i, "tags"] = {"negative_los"}
    for i in take(cnt(0.003)):
        df.at[i, "DischargeDateTime"] = pd.NaT
        df.at[i, "tags"] = {"missing_discharge"}
    for j, i in enumerate(take(cnt(0.004))):
        df.at[i, "DoctorNaturalKey"] = f"D9{j % 90 + 10:02d}"
        df.at[i, "tags"] = {"orphan_doctor"}
    deleted = take(cnt(0.003))
    for i in deleted:
        df.at[i, "tags"] = {"source_deleted"}
    dup_ids = take(cnt(0.035, 5))
    for pos, i in enumerate(dup_ids):
        tag = "dup_tiebreak" if pos % 4 == 0 else ("dup_v3" if pos % 4 == 1 else "dup_v2")
        df.at[i, "tags"] = {"dup_version", tag}

    extra = []

    def ex(patient, admit, los_h, status, disp, branch, dept, adm_type, doctor, charge, tags, discharge=True):
        dis = _round_min(admit + timedelta(hours=los_h), 5)
        extra.append(dict(PatientID=patient, AdmissionDateTime=admit, DischargeDateTime=dis if discharge else pd.NaT,
                          AdmissionType=adm_type, EncounterStatus=status, DischargeDisposition=disp, Branch=branch,
                          Department=dept, DoctorNaturalKey=doctor, GrossCharge=charge,
                          LastModifiedDateTime=dis + timedelta(hours=2), SourceVersion=1, tags=set(tags)))

    branches = list(BRANCH_WEIGHT)
    any_doc = lambda b, d: truth[(b, d)][0][0]
    t_lo = datetime.combine(start + timedelta(days=5), datetime.min.time())
    t_hi = datetime.combine(end - timedelta(days=15), datetime.min.time())
    rand_t = lambda: _round_min(t_lo + timedelta(seconds=int(rng.integers(0, int((t_hi - t_lo).total_seconds())))), 15)
    rand_combo = lambda: (lambda b: (b, BRANCH_DEPTS[b][int(rng.integers(0, len(BRANCH_DEPTS[b])))]))(
        branches[int(rng.integers(0, len(branches)))])
    px = {"test": 0, "cancel": 0, "died": 0, "scd": 0}

    def pid(kind, prefix):
        px[kind] += 1
        return f"P{prefix}{px[kind]:05d}"

    for _ in range(cnt(0.004)):
        b, d = rand_combo()
        ex(pid("test", 99), rand_t(), 2, "Test", "Home", b, d, "Emergency", any_doc(b, d), 1.0, {"test_encounter"})
    for _ in range(cnt(0.010)):
        b, d = rand_combo()
        ex(pid("cancel", 96), rand_t(), 24, "Cancelled", "Home", b, d, "Emergency", any_doc(b, d),
           float(rng.integers(2000, 9000)), {"cancelled_encounter"})
    for _ in range(cnt(0.0015)):               # patient admitted again AFTER a recorded death
        b, d = rand_combo()
        a0 = rand_t()
        p = pid("died", 98)
        ex(p, a0, 72, "Completed", "Died", b, d, "Emergency", any_doc(b, d), 15000.0, {"died_index"})
        b2, d2 = rand_combo()
        ex(p, a0 + timedelta(days=int(rng.integers(5, 25))), 48, "Completed", "Home", b2, d2, "Emergency",
           any_doc(b2, d2), 9000.0, {"after_death"})
    for od in meta["overlap"]:
        for _ in range(2):
            a = datetime.combine(od["win_from"] + timedelta(days=int(rng.integers(0, 7))), datetime.min.time()) \
                + timedelta(hours=int(rng.integers(7, 21)))
            ex(pid("scd", 97), a, 48, "Completed", "Home", od["branch"], od["dept"], "Emergency", od["doctor"],
               float(rng.integers(4000, 20000)), {"scd_overlap"})
    for gd in meta["gap"]:
        for _ in range(2):
            a = datetime.combine(gd["win_from"] + timedelta(days=int(rng.integers(0, 4))), datetime.min.time()) \
                + timedelta(hours=int(rng.integers(7, 21)))
            ex(pid("scd", 97), a, 48, "Completed", "Home", gd["branch"], gd["dept"], "Emergency", gd["doctor"],
               float(rng.integers(4000, 20000)), {"scd_gap"})
    # boundary admissions: last day of an assignment / first day of the next - must NOT raise SCD flags
    for tm in meta["transfers"]:
        td, (ob, od_), (nb, nd) = tm["transfer_date"], tm["old"], tm["new"]
        a1 = datetime.combine(td - timedelta(days=1), datetime.min.time()) + timedelta(hours=20)
        ex(pid("scd", 97), a1, 30, "Completed", "Home", ob, od_, "Emergency", tm["doctor"],
           float(rng.integers(4000, 20000)), {"scd_boundary_last_day"})
        a2 = datetime.combine(td, datetime.min.time()) + timedelta(hours=8)
        ex(pid("scd", 97), a2, 30, "Completed", "Home", nb, nd, "Emergency", tm["doctor"],
           float(rng.integers(4000, 20000)), {"scd_boundary_first_day"})

    df = pd.concat([df, pd.DataFrame(extra)], ignore_index=True)
    df = df.sort_values(["AdmissionDateTime", "PatientID"], kind="mergesort").reset_index(drop=True)
    df["AdmissionID"] = [f"A{i + 1:07d}" for i in range(len(df))]
    df["SourceSystem"] = np.where(df["EncounterStatus"].eq("Test"), "HIS_Test", "HIS_" + df["Branch"])
    return df


def expand_versions(rng, df):
    """Turn tagged rows into multiple source versions (final version == the row as built)."""
    out = []
    for r in df.to_dict("records"):
        tags = r["tags"]
        if "dup_version" not in tags:
            out.append(r)
            continue
        n_versions = 3 if "dup_v3" in tags else 2
        tie = "dup_tiebreak" in tags
        for v in range(1, n_versions + 1):
            c = dict(r)
            if v < n_versions:
                if pd.notna(c["DischargeDateTime"]):
                    c["DischargeDateTime"] = max(c["AdmissionDateTime"] + timedelta(hours=2),
                                                 c["DischargeDateTime"] - timedelta(hours=int(rng.integers(2, 30))))
                c["GrossCharge"] = round(c["GrossCharge"] * float(rng.uniform(0.82, 0.95)), 2)
                # strictly increasing across versions: offsets are (n-v) days + 0..5h
                c["LastModifiedDateTime"] = r["LastModifiedDateTime"] - timedelta(
                    days=(n_versions - v), hours=int(rng.integers(0, 6)))
            c["SourceVersion"] = 1 if tie else v
            out.append(c)
    return pd.DataFrame(out)


# --------------------------------------------------------------------------- billing
def build_billing(rng, adm_final: pd.DataFrame, as_of: datetime):
    rows = []
    n_adm = len(adm_final)
    billable = adm_final[(adm_final.EncounterStatus == "Completed")]
    for r in billable.itertuples():
        if rng.random() > 0.97:
            continue
        base_dt = r.DischargeDateTime if pd.notna(r.DischargeDateTime) else r.AdmissionDateTime + timedelta(days=1)
        lag = rng.uniform(1, 21) if rng.random() < 0.9 else rng.uniform(21, 45)
        tdate = _round_min(base_dt + timedelta(days=lag), 60)
        if tdate > as_of:
            continue
        amt = round(r.GrossCharge * float(rng.uniform(0.72, 0.97)), 2)
        rows.append(dict(prov=len(rows), AdmissionID=r.AdmissionID, PatientID=r.PatientID, Branch=r.Branch,
                         TransactionType="INV", TransactionDate=tdate,
                         ServiceDate=r.AdmissionDateTime.date(), AmountExVAT=amt,
                         InvoiceReference=f"INV-{r.AdmissionID}", rel=None,
                         LastModifiedDateTime=tdate + timedelta(hours=1), tags=set()))
    invoices = list(rows)
    rng.shuffle(invoices)
    pick = iter(invoices)

    def nxt(k):
        return [next(pick) for _ in range(min(k, len(invoices)))] if k else []

    base_n = len(invoices)
    n_corr, n_cn, n_adj, n_late = (max(3, int(base_n * x)) for x in (0.015, 0.03, 0.02, 0.015))

    # corrected invoices: earlier wrong version of the same TransactionID
    extra = []
    for inv in nxt(n_corr):
        inv["tags"].add("billing_dup_version")
        v1 = dict(inv)
        v1["tags"] = set(inv["tags"])
        v1["AmountExVAT"] = round(inv["AmountExVAT"] * float(rng.uniform(0.9, 0.98)), 2)
        v1["LastModifiedDateTime"] = inv["LastModifiedDateTime"] - timedelta(days=1, hours=int(rng.integers(0, 3)))
        v1["prov_dup_of"] = inv["prov"]
        extra.append(v1)
    for inv in nxt(n_cn):                       # credit notes, usually raised in a later month
        d = _round_min(inv["TransactionDate"] + timedelta(days=float(rng.uniform(3, 70))), 60)
        if d > as_of:
            continue
        extra.append(dict(prov=-1, AdmissionID=inv["AdmissionID"], PatientID=inv["PatientID"], Branch=inv["Branch"],
                          TransactionType="CN", TransactionDate=d, ServiceDate=inv["ServiceDate"],
                          AmountExVAT=-round(abs(inv["AmountExVAT"]) * float(rng.uniform(0.05, 0.40)), 2),
                          InvoiceReference=f"CN-{inv['AdmissionID']}", rel=inv["prov"],
                          LastModifiedDateTime=d + timedelta(hours=1), tags={"credit_note"}))
    for inv in nxt(n_adj):
        d = _round_min(inv["TransactionDate"] + timedelta(days=float(rng.uniform(1, 45))), 60)
        if d > as_of:
            continue
        extra.append(dict(prov=-1, AdmissionID=inv["AdmissionID"], PatientID=inv["PatientID"], Branch=inv["Branch"],
                          TransactionType="ADJ", TransactionDate=d, ServiceDate=inv["ServiceDate"],
                          AmountExVAT=round(float(rng.choice([-1, 1])) * float(rng.uniform(50, 900)), 2),
                          InvoiceReference=f"ADJ-{inv['AdmissionID']}", rel=None,
                          LastModifiedDateTime=d + timedelta(hours=1), tags={"adjustment"}))
    for inv in nxt(n_late):                     # late supplementary invoice, crosses month boundary
        d = _round_min(inv["TransactionDate"] + timedelta(days=float(rng.uniform(35, 80))), 60)
        if d > as_of:
            continue
        extra.append(dict(prov=-1, AdmissionID=inv["AdmissionID"], PatientID=inv["PatientID"], Branch=inv["Branch"],
                          TransactionType="INV", TransactionDate=d, ServiceDate=inv["ServiceDate"],
                          AmountExVAT=round(inv["AmountExVAT"] * float(rng.uniform(0.05, 0.20)), 2),
                          InvoiceReference=f"INV-{inv['AdmissionID']}-LATE", rel=None,
                          LastModifiedDateTime=d + timedelta(hours=1), tags={"late_invoice"}))
    # orphan invoices - AdmissionID unknown to the admissions extract
    pats = adm_final["PatientID"].unique()
    for j in range(max(2, int(n_adm * 0.002))):
        b = list(BRANCH_WEIGHT)[int(rng.integers(0, 5))]
        d = _round_min(as_of - timedelta(days=float(rng.uniform(5, 300))), 60)
        extra.append(dict(prov=-1, AdmissionID=f"A9{j + 1:06d}", PatientID=str(rng.choice(pats)), Branch=b,
                          TransactionType="INV", TransactionDate=d, ServiceDate=(d - timedelta(days=9)).date(),
                          AmountExVAT=round(float(rng.uniform(1500, 15000)), 2),
                          InvoiceReference=f"INV-A9{j + 1:06d}", rel=None,
                          LastModifiedDateTime=d + timedelta(hours=1), tags={"orphan_billing"}))

    allrows = rows + extra
    # assign TransactionIDs: ordered by TransactionDate; versions of one invoice share the ID
    finals = [r for r in allrows if "prov_dup_of" not in r]
    finals.sort(key=lambda r: (r["TransactionDate"], r["InvoiceReference"]))
    id_of_prov, n = {}, 0
    for r in finals:
        n += 1
        r["TransactionID"] = f"T{n:07d}"
        if r["prov"] >= 0:
            id_of_prov[r["prov"]] = r["TransactionID"]
    for r in allrows:
        if "prov_dup_of" in r:
            r["TransactionID"] = id_of_prov[r["prov_dup_of"]]
    for r in allrows:
        r["RelatedTransactionID"] = id_of_prov.get(r["rel"]) if r["rel"] is not None else None
    df = pd.DataFrame(allrows).sort_values(["TransactionID", "LastModifiedDateTime"]).reset_index(drop=True)
    return df


# --------------------------------------------------------------------------- security matrix
def build_security(rng):
    rows = []
    add = lambda u, lvl, b, d, a=1: rows.append(dict(UserEmail=f"{u}@mhg.example", AccessLevel=lvl, Branch=b,
                                                      Department=d, IsActive=a))
    add("enterprise.admin", "Enterprise", "ALL", "ALL")
    add("quality.director", "Enterprise", "ALL", "ALL")
    for b in BRANCH_WEIGHT:
        add(f"{b.lower()}.director", "Branch", b, "ALL")
    add("jeddah.director", "Department", "Jeddah", "Cardiology")          # redundant grant -> must not duplicate rows
    add("cardio.head", "Department", "Riyadh", "Cardiology")
    add("cardio.head", "Department", "Dammam", "Cardiology")
    add("cardio.head", "Department", "Jeddah", "Cardiology")
    add("im.head", "Department", "Riyadh", "Internal Medicine")
    add("ops.multi", "Department", "Jeddah", "Orthopedics")
    add("ops.multi", "Department", "Jeddah", "Surgery")
    add("ops.multi", "Department", "Tabuk", "Internal Medicine")
    add("mixed.access", "Branch", "Buraidah", "ALL")
    add("mixed.access", "Branch", "Riyadh", "ALL", 0)                    # revoked grant on an otherwise active user
    add("former.employee", "Branch", "Riyadh", "ALL", 0)
    add("former.contractor", "Department", "Tabuk", "Emergency", 0)
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- change log
def build_change_log(rng, adm, scd, bill, deleted_ids):
    ev = []

    def lag():
        r = rng.random()
        if r < 0.94:
            return timedelta(minutes=float(rng.uniform(2, 15)))
        if r < 0.99:
            return timedelta(hours=float(rng.uniform(6, 48)))
        return timedelta(days=float(rng.uniform(2, 5)))              # late-arriving correction

    first_seen = set()
    for r in adm.sort_values(["AdmissionID", "LastModifiedDateTime"]).itertuples():
        ct = "UPDATE" if r.AdmissionID in first_seen else "INSERT"
        first_seen.add(r.AdmissionID)
        ev.append((r.SourceSystem, "Admission", r.AdmissionID, ct, r.LastModifiedDateTime,
                   r.LastModifiedDateTime + lag(), "Initial version" if ct == "INSERT" else "Corrected version"))
    for r in scd.itertuples():
        init = r.SourceLastUpdated == datetime(2024, 1, 1, 8, 0)
        ev.append((f"HIS_{r.Branch}", "Doctor", r.DoctorNaturalKey, "INSERT" if init else "UPDATE",
                   r.SourceLastUpdated, (r.SourceLastUpdated + timedelta(minutes=5)) if init else
                   r.SourceLastUpdated + lag(), "Master data load" if init else "Assignment change"))
    seen_t = set()
    for r in bill.sort_values(["TransactionID", "LastModifiedDateTime"]).itertuples():
        ct = "UPDATE" if r.TransactionID in seen_t else "INSERT"
        seen_t.add(r.TransactionID)
        ev.append(("Billing", "Transaction", r.TransactionID, ct, r.LastModifiedDateTime,
                   r.LastModifiedDateTime + lag(), r.InvoiceReference))
    lm = adm.groupby("AdmissionID")["LastModifiedDateTime"].max()
    src = adm.drop_duplicates("AdmissionID").set_index("AdmissionID")["SourceSystem"]
    for a in deleted_ids:
        s_mod = lm[a] + timedelta(hours=float(rng.uniform(12, 48)))
        ev.append((src[a], "Admission", a, "DELETE", s_mod, s_mod + lag(), "Source-side cancellation / deletion"))
    log = pd.DataFrame(ev, columns=["SourceSystem", "Entity", "BusinessKey", "ChangeType", "SourceLastModified",
                                    "AvailableToETL", "Comment"])
    log = log.sort_values(["AvailableToETL", "Entity", "BusinessKey"], kind="mergesort").reset_index(drop=True)
    log.insert(0, "ChangeSequence", range(1, len(log) + 1))
    return log


# --------------------------------------------------------------------------- main
def generate(out_dir: Path, n_admissions: int = 2500, seed: int = 42,
             start: date = date(2025, 1, 1), end: date = date(2026, 6, 30)) -> dict:
    rng = np.random.default_rng(seed)
    out_dir.mkdir(parents=True, exist_ok=True)

    scd_rows, truth, meta = build_doctors(rng, n_admissions, start, end)
    adm_final = build_admissions(rng, n_admissions, start, end, truth, meta)
    adm = expand_versions(rng, adm_final)
    scd = pd.DataFrame(scd_rows)
    as_of = adm["LastModifiedDateTime"].max()
    bill = build_billing(rng, adm_final, as_of)
    sec = build_security(rng)
    deleted_ids = adm_final.loc[adm_final.tags.apply(lambda t: "source_deleted" in t), "AdmissionID"].tolist()
    log = build_change_log(rng, adm, scd, bill, deleted_ids)

    # ---- write CSVs (formats identical to what the loader/schema guard expects)
    cols = ["SourceSystem", "AdmissionID", "PatientID", "AdmissionDateTime", "DischargeDateTime", "AdmissionType",
            "EncounterStatus", "DischargeDisposition", "Branch", "Department", "DoctorNaturalKey",
            "LastModifiedDateTime", "SourceVersion", "GrossCharge"]
    a = adm.sort_values(["AdmissionID", "SourceVersion", "LastModifiedDateTime"])[cols].copy()
    for c in ("AdmissionDateTime", "DischargeDateTime", "LastModifiedDateTime"):
        a[c] = a[c].map(_ts)
    a.to_csv(out_dir / "Admissions.csv", index=False)

    s = scd.sort_values(["DoctorNaturalKey", "EffectiveFrom"]).copy()
    s["EffectiveFrom"] = s["EffectiveFrom"].map(lambda x: x.strftime(D))
    s["EffectiveTo"] = s["EffectiveTo"].map(lambda x: x.strftime(D) if x is not None and not pd.isna(x) else "")
    s["SourceLastUpdated"] = s["SourceLastUpdated"].map(_ts)
    s.to_csv(out_dir / "Physician_Assignments.csv", index=False)

    bcols = ["TransactionID", "AdmissionID", "PatientID", "Branch", "TransactionType", "TransactionDate",
             "ServiceDate", "AmountExVAT", "InvoiceReference", "RelatedTransactionID", "LastModifiedDateTime"]
    b = bill[bcols].copy()
    b["TransactionDate"] = b["TransactionDate"].map(_ts)
    b["LastModifiedDateTime"] = b["LastModifiedDateTime"].map(_ts)
    b["ServiceDate"] = b["ServiceDate"].map(lambda x: x.strftime(D))
    b["RelatedTransactionID"] = b["RelatedTransactionID"].fillna("")
    b.to_csv(out_dir / "Billing_Extract.csv", index=False)

    sec.to_csv(out_dir / "Access_Matrix.csv", index=False)
    lg = log.copy()
    lg["SourceLastModified"] = lg["SourceLastModified"].map(_ts)
    lg["AvailableToETL"] = lg["AvailableToETL"].map(_ts)
    lg.to_csv(out_dir / "Change_Feed.csv", index=False)

    # ---- ground-truth manifest
    ids = lambda tag: sorted(adm_final.loc[adm_final.tags.apply(lambda t: tag in t), "AdmissionID"])
    manifest = {
        "seed": seed, "requested_admissions": n_admissions,
        "generated_at_note": "deterministic - no wall-clock timestamps",
        "as_of_ts": _ts(as_of),
        "counts": {"admission_source_rows": int(len(adm)), "distinct_admissions": int(len(adm_final)),
                   "doctor_scd_rows": int(len(scd)), "billing_source_rows": int(len(bill)),
                   "billing_distinct_transactions": int(bill.TransactionID.nunique()),
                   "change_log_rows": int(len(log)), "rls_matrix_rows": int(len(sec))},
        "planted_admission_anomalies": {
            "DUP_SOURCE_VERSION": ids("dup_version"),
            "NON_INPATIENT_ENCOUNTER": sorted(ids("test_encounter") + ids("cancelled_encounter")),
            "ORPHAN_DOCTOR_FK": ids("orphan_doctor"),
            "NEGATIVE_LOS": ids("negative_los"),
            "MISSING_DISCHARGE": ids("missing_discharge"),
            "SCD_OVERLAP": ids("scd_overlap"),
            "SCD_COVERAGE_GAP": ids("scd_gap"),
            "ILLOGICAL_ADMIT_AFTER_DEATH": ids("after_death"),
            "SOURCE_DELETED": ids("source_deleted"),
        },
        "must_not_be_flagged_scd": sorted(ids("scd_boundary_last_day") + ids("scd_boundary_first_day")),
        "planted_billing_anomalies": {
            "DUP_SOURCE_VERSION": sorted(bill.loc[bill.tags.apply(lambda t: "billing_dup_version" in t),
                                                  "TransactionID"].unique()),
            "ORPHAN_BILLING_ADMISSION": sorted(bill.loc[bill.tags.apply(lambda t: "orphan_billing" in t),
                                                        "TransactionID"]),
        },
        "late_invoice_transactions": sorted(bill.loc[bill.tags.apply(lambda t: "late_invoice" in t), "TransactionID"]),
        "doctor_anomalies": {"overlap": [{**o, "win_from": str(o["win_from"]), "win_to": str(o["win_to"])}
                                         for o in meta["overlap"]],
                             "gap": [{**g, "win_from": str(g["win_from"]), "win_to": str(g["win_to"])}
                                     for g in meta["gap"]]},
    }
    (out_dir / "ground_truth.json").write_text(json.dumps(manifest, indent=2, default=str))
    return manifest


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--admissions", type=int, default=2500, help="approx. number of natural admissions (default 2500)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", type=Path, default=ROOT / "data")
    ap.add_argument("--start", type=date.fromisoformat, default=date(2025, 1, 1))
    ap.add_argument("--end", type=date.fromisoformat, default=date(2026, 6, 30))
    args = ap.parse_args()
    m = generate(args.out, args.admissions, args.seed, args.start, args.end)
    print(f"wrote synthetic dataset to {args.out}")
    for k, v in m["counts"].items():
        print(f"  {k:<32} {v:>9,}")
    print("  planted anomalies:", {k: len(v) for k, v in m["planted_admission_anomalies"].items()})


if __name__ == "__main__":
    main()
