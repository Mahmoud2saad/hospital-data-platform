# Replay & Recovery Runbook

"It's 2am and a step failed - what do I actually do?"

## 1. How to tell something failed

`ctl.pipeline_run_log` holds one row per (run, layer, step) with status, row counts and `error_message`.

```sql
SELECT * FROM ctl.pipeline_run_log
WHERE run_id = (SELECT MAX(run_id) FROM ctl.pipeline_run_log) AND status = 'FAILED';

SELECT * FROM ctl.vw_current_alerts;      -- empty = healthy
```

In production this is a scheduled alert (Airflow task failure -> Slack / PagerDuty); the underlying diagnostic is the same table.

## 2. Failure points and what to do

| Failure point | Meaning | Action |
|---|---|---|
| Schema guard returns `BLOCKING` | A source column was removed or retyped unsafely | **Do not force the load.** Inspect `ctl.schema_drift_log`. Fix at source, or update the bronze DDL / silver column lists and re-baseline `ctl.schema_snapshot` once the change is confirmed intentional. Other entities already loaded normally. |
| Bronze INSERT fails (corrupt file, dropped feed) | Bad or partial extract | Re-run. Bronze is append-only and every downstream layer resolves "latest version wins", so a clean re-pull never double-counts. For an incremental run the watermark only advances at the end, so nothing is skipped. |
| A silver/gold SQL step fails | Transformation bug, or a NEW edge case no rule handles | Query bronze for the offending rows, decide the correct treatment, encode it as an explicit rule (never patch data in place), re-run. Gold is rebuilt from bronze, so the fix applies retroactively to all history. |
| Watermark moved but a dashboard looks stale | Downstream refresh/cache issue | Confirm `ctl.watermark.updated_at` moved; if so the pipeline is fine - escalate to the BI refresh schedule. |
| `ROW_NOT_IN_CHANGE_LOG` warning (incremental) | A source row has no change-feed entry and will never be picked up | Raise with the source team; run a full load to pick the row up in the meantime. |

## 3. Why "just re-run it" is safe (and tested)

Silver and gold are `CREATE OR REPLACE`, rebuilt deterministically from bronze, and every dedupe rule is "latest version wins". Nothing accumulates downstream, so:

- re-running after a **transient** failure reproduces the state a successful first attempt would have produced - never a duplicate, never a half-updated gold layer;
- re-running after fixing a **logic** bug applies the corrected rule to all history, with no separate backfill job.

This is asserted, not claimed:

- `tests/test_pipeline.py::test_pipeline_is_idempotent` - two full runs give identical gold tables.
- `tests/test_incremental.py::test_incremental_batches_converge_to_the_full_load` - N CDC batches give exactly the same gold state as one full load.
- `tests/test_incremental.py::test_replaying_the_last_batch_lands_nothing` - retrying a batch with the watermark unchanged lands zero rows.
- `python pipeline/demo_incremental.py` shows all three in one printed run.

## 4. What is deliberately not retried

Breaking schema drift halts only the affected entity. A partial source outage should degrade gracefully, not take the whole batch down.

## 5. At 500M+ rows

The *logic* and the idempotency guarantees do not change. Cost and blast radius do: a full `CREATE OR REPLACE` rebuild stops being viable, so gold becomes incremental `MERGE`s keyed on the watermark (only rows with `AvailableToETL > last_watermark`), with idempotency coming from the MERGE keys. Partitioning, columnstore and Snowflake equivalents are in `sql/12_production_scale_reference.sql`.
