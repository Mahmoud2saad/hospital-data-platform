"""
schema_guard.py
===============
Schema-drift detector, called once per source file before it is inserted
into bronze. Compares the incoming column set / coarse type family against
ctl.schema_snapshot (last-known-good schema for that entity) and classifies
every difference:

    COLUMN_ADDED    INFO      load proceeds; the new column is NOT loaded into
                              bronze (explicit column lists everywhere) until
                              the DDL is changed deliberately.
    COLUMN_REMOVED  BLOCKING  load for THIS entity is skipped.
    TYPE_CHANGED    BLOCKING  unless it is a safe widening (see SAFE_TRANSITIONS).

One bad source file never takes down the whole batch - the other entities
still load. The snapshot is only refreshed when the load proceeds, so a
blocked file is compared against the same trusted baseline on the next run.

Deliberately simple (name + type family). In production this sits in front of
a schema registry / data-contract check rather than being hand-rolled per
entity; the point here is the CONTROL PATTERN.
"""
from __future__ import annotations

from datetime import datetime

import duckdb

# known type family -> incoming families that are a safe widening (or no change)
SAFE_TRANSITIONS = {
    "INTEGER": {"INTEGER", "BIGINT", "DOUBLE"},
    "BIGINT": {"BIGINT", "DOUBLE"},
    "DOUBLE": {"DOUBLE"},
    "DATE": {"DATE", "TIMESTAMP"},
    "TIMESTAMP": {"TIMESTAMP"},
    "VARCHAR": {"VARCHAR"},
}


def _type_family(t: str) -> str:
    t = t.upper()
    if t.startswith("DECIMAL") or t in ("DOUBLE", "FLOAT", "REAL"):
        return "DOUBLE"
    if t in ("INTEGER", "INT", "SMALLINT", "TINYINT"):
        return "INTEGER"
    if t == "BIGINT":
        return "BIGINT"
    if t.startswith("TIMESTAMP"):
        return "TIMESTAMP"
    if t == "DATE":
        return "DATE"
    return "VARCHAR"


def check_and_snapshot(con: duckdb.DuckDBPyConnection, run_id: int, entity: str, csv_path: str,
                       quiet: bool = False) -> bool:
    """True -> load may proceed; False -> skip this entity (breaking drift)."""
    incoming = con.execute("DESCRIBE SELECT * FROM read_csv_auto(?)", [str(csv_path)]).fetchdf()
    incoming_cols = {r["column_name"]: _type_family(r["column_type"]) for _, r in incoming.iterrows()}

    known = con.execute("SELECT column_name, column_type FROM ctl.schema_snapshot WHERE entity = ?",
                        [entity]).fetchdf()
    known_cols = {r["column_name"]: r["column_type"] for _, r in known.iterrows()}

    proceed, events = True, []
    if known_cols:                                   # first-ever load has nothing to compare against
        for col in known_cols:
            if col not in incoming_cols:
                events.append(("COLUMN_REMOVED", col,
                               f"Column '{col}' is in the last known schema but missing from this file.", "BLOCKING"))
                proceed = False
        for col, fam in incoming_cols.items():
            if col not in known_cols:
                events.append(("COLUMN_ADDED", col,
                               f"New column '{col}' ({fam}) - not loaded to bronze until the DDL is updated; "
                               f"silver/gold are unaffected.", "INFO"))
            elif fam not in SAFE_TRANSITIONS.get(known_cols[col], {known_cols[col]}):
                events.append(("TYPE_CHANGED", col,
                               f"Column '{col}' changed type family {known_cols[col]} -> {fam} "
                               f"(not a safe widening).", "BLOCKING"))
                proceed = False

    for drift_type, col, detail, severity in events:
        con.execute(
            "INSERT INTO ctl.schema_drift_log VALUES "
            "((SELECT COALESCE(MAX(drift_id), 0) + 1 FROM ctl.schema_drift_log), ?, ?, ?, ?, ?, ?, ?)",
            [run_id, entity, drift_type, col, detail, severity, datetime.now()])
        if not quiet:
            print(f"    [schema-drift:{severity}] {entity}.{col}: {detail}")

    if proceed:
        con.execute("DELETE FROM ctl.schema_snapshot WHERE entity = ?", [entity])
        for i, (col, fam) in enumerate(incoming_cols.items()):
            con.execute("INSERT INTO ctl.schema_snapshot VALUES (?,?,?,?,?)",
                        [entity, col, fam, i, datetime.now()])
    return proceed
