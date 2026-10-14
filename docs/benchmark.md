# Scale benchmark

Measured once on a 2-vCPU / 7 GB cloud container (Python 3.13, DuckDB 1.5, pandas 3.0), no tuning.
Reproduce with `make bench` (numbers will vary by machine).

| Stage | Wall time |
|---|---|
| `generate_data.py --admissions 250000` (single-threaded Python) | ~52 s |
| `run_pipeline.py --no-export` (bronze + silver + gold + DQ register + monitoring) | ~6 s (pipeline reports 5.8 s) |

Dataset: 265,872 admission rows, 261,246 billing rows, 528,609 change-feed events, 741 SCD rows (~1.06M rows).
Result: 254,934 distinct admissions, 250,684 valid; group readmission rate 7.77% (218,325 eligible index admissions); 27,823 DQ exceptions raised.

Step log from the run:

```
  [OK]   bronze       bronze.fact_admissions                   in=265872   out=265872
  [OK]   bronze       bronze.dim_doctors_scd                   in=741      out=741
  [OK]   bronze       bronze.billing_transactions              in=261246   out=261246
  [OK]   bronze       bronze.security_matrix_rls               in=19       out=19
  [OK]   bronze       bronze.source_change_log                 in=528609   out=528609
  [OK]   silver/gold  03_silver_admission_trusted.sql          in=265872   out=254934
  [OK]   silver/gold  04_doctor_scd_attribution.sql            in=741      out=253934
  [OK]   silver/gold  05_gold_dimensions.sql                   in=0        out=210878
  [OK]   silver/gold  06_gold_fact_admission.sql               in=254934   out=254934
  [OK]   silver/gold  07_gold_readmission_kpi.sql              in=254934   out=225448
  [OK]   silver/gold  08_gold_revenue_reconciliation.sql       in=261246   out=257611
  [OK]   silver/gold  09_security_rls.sql                      in=0        out=67
  [OK]   silver/gold  10_incremental_and_dq_register.sql       in=0        out=27823
  [OK]   silver/gold  11_monitoring.sql                        in=0        out=0
=== run 1 finished in 5.8s, 0 failed step(s) ===
```

Full rebuild per batch is fine at this size. Beyond tens of millions of rows see `sql/12_production_scale_reference.sql` (partitioning, columnstore, MERGE).
