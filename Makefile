PY ?= python3

.PHONY: install data run incremental test dq figures dashboard dbt mutation bench all clean
install:      ; $(PY) -m pip install -r requirements.txt
data:         ; $(PY) pipeline/generate_data.py                       # regenerate the committed demo dataset
run:          ; $(PY) pipeline/run_pipeline.py                        # full load -> output/
incremental:  ; $(PY) pipeline/demo_incremental.py                    # CDC batches == full load, replay is a no-op
test:         ; $(PY) -m pytest tests -q
dq:           ; $(PY) pipeline/export_dq_register.py
figures:      ; $(PY) pipeline/make_figures.py
dashboard:    ; $(PY) pipeline/build_dashboard.py                      # docs/dashboard.html
dbt:          ; cd dbt && dbt build --profiles-dir .                  # needs `make run` first
mutation:     ; $(PY) scripts/mutation_check.py
bench:        # 250k admissions (~1M rows incl. billing & change feed)
	$(PY) pipeline/generate_data.py --admissions 250000 --out /tmp/mhg_bench/data
	time $(PY) pipeline/run_pipeline.py --data /tmp/mhg_bench/data --db /tmp/mhg_bench/wh.duckdb --no-export
all: run test dq figures dashboard incremental dbt
clean:        ; rm -rf output .pytest_cache
