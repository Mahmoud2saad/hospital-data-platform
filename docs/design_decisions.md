# Design decisions & business rules

Fictional organisation: **Meridian Health Group** - five hospital branches (Riyadh, Jeddah, Dammam, Tabuk, Buraidah), each running its own Hospital Information System (HIS), plus a central billing system, a physician master with history, and a security matrix. All data is synthetic (`pipeline/generate_data.py`).

## 1. Scope
The platform turns messy source extracts into a trusted admission layer, a 30-day readmission KPI, a revenue reconciliation, and row-level security, with the operational controls (drift detection, incremental loads, monitoring, replay) needed to run it unattended. The engine is DuckDB so the whole thing is executable in seconds; all logic is in `sql/*.sql` and Python only orchestrates, so it ports to SQL Server / Snowflake (`sql/12_*.sql`).

## 2. Business rules
| ID | Rule | Where |
|---|---|---|
| BR-1 | A valid admission is the latest source version of an encounter, excluding Cancelled, Test, and source-deleted encounters | `03` |
| BR-2 | Latest version wins: highest `SourceVersion`, tie-break latest `LastModifiedDateTime`. Never sum or average versions | `03`, `08` |
| BR-3 | Historical activity is attributed to the physician assignment effective on the event date, never the current record | `04` |
| BR-4 | Eligible readmission index = valid, non-quarantined, disposition not Died / Transfer | `07` |
| BR-5 | Planned (Elective) admissions are excluded as the *candidate readmission*; the first *unplanned* admission within 30 days is matched | `07` |
| BR-6 | Readmission search is not branch-scoped (Jeddah -> Riyadh counts) | `07` |
| BR-7 | Index discharges inside the trailing 30 days of the data horizon lack a full window: flagged and excluded from the denominator | `07` |
| BR-8 | Credit notes / adjustments keep their sign and belong to the month they were raised | `08` |

## 3. Key design choices
- **Grain first.** `gold.fact_admission` is one row per business admission. Every mart derives from it, never from bronze, so there is one definition of "an admission".
- **Quarantine, never delete.** Rows with impossible dates stay in the fact for volume counts but are excluded from date-sequenced logic. Every exception is a flag plus a row in `ctl.dq_exceptions`.
- **Physician SCD2.** Overlapping windows resolve to the most recently corrected row (HIGH exception); gaps fall back to the nearest *prior* assignment (MEDIUM); unknown keys map to a synthetic `DoctorSK = -1` member. `EffectiveTo` is an **inclusive** date.
- **Readmission interpretation.** Elective exclusion applies to the readmission, not the index (CMS-style). It is isolated in one predicate so the alternative reading is a one-line change. One measure pair `(SUM(is_denominator), SUM(is_numerator))` is re-sliced at every grain, and a test enforces that group / branch / department / physician totals agree.
- **One billing fact, two date roles.** Service-period and transaction-period revenue are two aggregations of one table, not two forked facts (which would double-count through shared dimensions). The bridge partitions revenue by type and timing and sums exactly to service-period revenue every month.
- **RLS as an expanded bridge.** `gold.entitlement_expanded` (user x branch x department) is semi-joined to facts, instead of a per-row correlated predicate. Inactive grants are filtered at the base; redundant grants cannot duplicate rows.
- **Watermark on `AvailableToETL`, not `SourceLastModified`**, so late-arriving corrections are never skipped. The watermark advances to the max value actually processed, never `now()`.
- **Idempotent by construction.** Gold is rebuilt deterministically from append-only bronze.

## 4. How correctness is established
1. **Independent oracle.** `tests/oracle.py` re-implements the rules in pandas with no shared code; the warehouse is compared to it row by row (valid set, latest versions, SCD attribution, every readmission index row, revenue by month, RLS visibility per user).
2. **Planted-defect manifest.** The generator records every defect it injects; tests assert the DQ register catches exactly those keys per rule (recall) and nothing else (precision).
3. **Hand-built edge cases.** 30-day boundary to the minute, elective-then-emergency return, transfer index, cancelled return, assignment boundary day, observation window.
4. **Operational properties.** Idempotent re-runs, CDC batches == full load, replay lands zero rows, a DELETE only takes effect once its event has arrived, schema drift (additive / removed / retyped / baseline preserved).
5. **Mutation check.** `scripts/mutation_check.py` breaks five rules one at a time (inclusive `EffectiveTo`, unplanned-readmission filter, latest-version ordering, revoked RLS grants, credit-note sign) and requires the suite to fail each time; all 5 are killed.

## 5. Issues found during design review (and fixed)
- **Assignment end dates were treated as exclusive midnight timestamps**, pushing every admission on the last day of an assignment into a false "coverage gap". Now inclusive, with a regression test.
- **An elective return masked a later unplanned one** (only the first subsequent admission was examined). Now the first *unplanned* admission in the window is matched.
- **Positional bronze `INSERT`** would misalign if a source added a column mid-file; loads now use explicit column lists, and additive drift is logged INFO.
- **The DQ register was rebuilt each run**, so a "trend by run" had a single point. It is now append-only by run.
- **The HIGH-severity alert used a fixed count**; it is now rate-based (HIGH exceptions per valid admission).
- **Source deletes in the change feed were ignored**; they now soft-delete the admission (`SOURCE_DELETED`).
- **Orphan billing** (invoices for unknown admissions) was invisible; now flagged and still counted in revenue.
- **ERD vs code drift**: conformed `dim_date`, `dim_branch`, `dim_department`, `dim_patient` and surrogate keys now exist in `gold`.

## 6. Known limitations
- Type inference uses a coarse family; a column that is entirely blank in a file can look like a type change. A data-contract / schema registry replaces this in production.
- Surrogate keys are assigned at build time (the whole gold layer is rebuilt together). In production they are identity/sequence keys maintained by `MERGE`.
- `as-of` for the observation window is `MAX(LastModifiedDateTime)`; a real deployment would use the extract timestamp.
- Silver/gold are full rebuilds from bronze per batch. Fine to tens of millions of rows; beyond that see `sql/12_*.sql`.

## 7. Orchestration, dbt and what was *not* verified
- **dbt** (`dbt/`) re-implements silver and gold as models. Because the same logic now exists twice, four singular tests assert the two implementations return identical rows (`EXCEPT` both ways) - the SQL layer is the executable specification, dbt is the maintainable production form.
- **Airflow** (`airflow/dags/`) wires the steps into an hourly DAG with a health gate. It could not be run in the build environment (no Airflow install), so it is checked structurally and its task logic is exercised directly by the test-suite. Treat it as a reviewed design, not a deployed one.
- **Not done:** a run against Snowflake / SQL Server (`sql/12_*.sql` is reference DDL only), streaming ingestion, and a BI semantic model - the dashboard is a static HTML view over the gold layer.
