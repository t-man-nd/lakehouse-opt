# Files added/changed for the real TLC lane

New (real-data lane):
  lakehouse_pipeline.py, optimization_benchmark.py
  scripts/__init__.py, scripts/download_sources.py
  src/config.py, src/spark_session.py, src/delta_utils.py, src/dq_rules.py
  src/tlc_bronze.py, src/tlc_profile.py, src/tlc_silver.py, src/reference_data.py
  src/time_travel.py, src/gold.py
  tests/__init__.py, tests/conftest.py, tests/tlc_like.py
  tests/test_dq_rules.py, tests/test_gold_logic.py, tests/test_support_tools.py
  tests/test_offline_smoke.py, tests/test_pipeline_e2e.py
  docs/DATA_SOURCES.md, docs/DQ_RULES.md, docs/PIPELINE_RUNBOOK.md

Changed:
  README.md   (quick-start section added near the top; nothing removed)
  .gitignore  (data/lakehouse*, spark-warehouse, metastore_db, .pytest_cache)

Unchanged (team work): src/bronze.py, src/silver.py, src/generator.py,
  src/schema_evolution.py, src/generate_batch_c2.py, tests/test_silver.py, docs/* (existing)

Regenerated on first run: docs/source_data_manifest.md (superset of the earlier 2025 manifest)
