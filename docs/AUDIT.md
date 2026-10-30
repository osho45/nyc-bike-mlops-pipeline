# Dataset migration and completion audit

Audit date: 2026-10-27. Repository: `osho45/nyc-bike-mlops-pipeline`. Original revision: `26bf620cf223b2eb795face326a7301e98443d62`. Only this working tree was modified. No commits, pushes, model replacement or deletion of pre-existing datasets occurred. The separate NYC Taxi AWS project was not modified.

> This is the initial audit snapshot. Docker and bounded Spark/Hadoop-Iceberg/PostgreSQL/API/dashboard execution have since passed; see [the current validation report](DOCKER_VALIDATION.md). The estimate and unverified flags below describe the earlier audit, not the latest container run.

## 1. Original dataset: NYC Yellow Taxi, not Citi Bike

The conclusion comes from executable contracts and source URLs, not the repository name. At the original revision:

| Evidence file | Actual behavior |
|---|---|
| `jobs/download_raw_data.py:5–23` | `download_nyc_taxi_data`, `yellow_tripdata_YYYY-MM.parquet`, TLC URL `https://d37ci6vzurychx.cloudfront.net/trip-data/`; an import-time loop downloads 2025 through May 2026 |
| `jobs/bronze_ingest.py:9–12,59` | `VendorID`, `tpep_pickup_datetime`, `PULocationID`, `DOLocationID`, `fare_amount`, taxi extras; `bronze.yellow_taxi_raw` |
| `jobs/silver_wap.py` | Reads `yellow_taxi_raw`, publishes `yellow_taxi_trips`, validates fares/passengers and taxi-zone rules |
| `jobs/gold_bi.py:41` | Reads taxi Silver and builds fleet/trip-distance activity |
| `jobs/gold_ml.py:43,60` | Taxi Silver; `sequence(2, 263)` manufactures taxi pickup-zone grid, not bike stations |
| `dags/1_bronze_ingestion.py`, `dags/2_silver_wap_purification.py`, `dags/3a_gold_bi_marts.py`, `dags/3b_gold_ml_observations.py` | Taxi file-drop and Iceberg asset URIs |
| `config/local.yml:14–16` | `incoming_taxi_data_uri`, `yellow_taxi_raw`, `yellow_taxi_trips` |
| `jobs/ml_features.py`, `jobs/ml_train_pipeline.py`, `jobs/ml_batch_score.py`, `jobs/ml_experiments.py` | Pickup-location history, taxi-zone champion, hourly ride-count classes; useful generic lag/model-selection machinery |
| `jobs/sync_to_postgres.py`, `jobs/sync_to_duckdb.py`, `fastapi/main.py` | Numeric `pickup_location_id` keys in cache/queries/endpoints |
| `streamlit/app.py:469` | Integer Pickup Location ID control and taxi-oriented display |
| Original `readme.md`, Compose and image configuration | Generic MLOps architecture and old service/data paths; no functional Citi Bike ingestion |
| All seven original notebooks | Taxi examples/downloads/experiments; retained unchanged in `notebooks/legacy_taxi/` |

There was **no active mixture**: the pipeline was taxi-based end to end. No local dataset, deployed champion or automated test suite was present at audit start, so no historical metrics could be verified. Existing PNGs are not Citi Bike evidence.

Exact original line excerpts and notebook-cell matches are retained in [original_dataset_evidence.json](audit/original_dataset_evidence.json). Paths/line numbers there refer to the original revision. The original sources remain recoverable with `git show 26bf620:<path>`; there is no need to modify another repository to inspect them.

## 2. Migration performed

- Replaced TLC ingestion with explicit official NYC Citi Bike downloads, streaming full ZIPs or bounded HTTP-range samples, CRC/checksum manifests and overwrite protection. No import-time network loop remains. All modern-schema CSV members are read; no assumption that a month has just one CSV.
- Added shared contracts/reference transformations in `jobs/citibike.py` and shared Spark stages in `jobs/lakehouse.py`; original job entry points and Airflow medallion structure remain.
- Replaced taxi columns/validation with ride ID, opaque station ID, bike/member types, station coordinates, NYC-local source timestamps, UTC storage and duration. Quarantine has explicit reasons. DST ambiguity is rejected explicitly; optional end station fields remain nullable.
- Preserved Bronze lineage, candidate audit and publication concepts. Bronze checksum reuse returns canonical ingestion run IDs. Silver/Gold publish via atomic partition replacement rather than separate delete/insert steps. Exact duplicates collapse; conflicting ride IDs fail. Actual Spark semantics still require runtime validation.
- Replaced taxi zone grid with observed start-station × UTC-hour grids. BI aggregates ride starts/duration/member/electric counts. Sample manifests/filenames are rejected before production Spark starts.
- Retained the 13-feature lag/rolling/cyclical framework, chronological splits, training-only class thresholds, three model families/six experiment plan and asymmetric decision policy. Added dense-history checks, NYC calendar features, text station IDs, training-cutoff guards and checkpoint preservation. Training remains single-station, not a citywide model registry.
- Migrated predictions, PostgreSQL/DuckDB keys, API queries, station controls and serving configuration. Added station discovery, bounded API queries and distinct liveness/readiness. An explicit DuckDB mode supports lightweight local checks; PostgreSQL remains the default production cache.
- Separated storage (`data/citibike/warehouse`, `models/citibike`), Compose project/ports/container names and Airflow image tag from the taxi project. Added non-overwriting local secret initialization and ignored generated/data/model files.
- Updated dependency pins after real execution exposed PyIceberg 0.11.1/PyArrow 16 incompatibility. Dashboard dependency pins now match the tested versions; `use_container_width=True` matches Streamlit 1.41.
- Replaced the macOS-only hard-coded backfill loop with an explicit bounded, dry-run-by-default UTC helper. Full-month Spark check tooling uses a new isolated warehouse. Destructive reset defaults to refusal.
- Preserved historical notebooks/images as clearly labeled legacy material. Rewrote setup/methodology/status documentation instead of retaining unsupported “production-grade” or live financial-performance claims.

The official source documents the modern fields and instructs users to sum all monthly CSV members: [Citi Bike System Data](https://citibikenyc.com/system-data). Historic archive naming/schema variation remains a limitation; modern January 2024 naming was verified directly against the official bucket. No restricted dataset or paid service was used.

## 3. Executed validation and failures found

Official input: `202401-citibike-tripdata.zip`, sampled from `202401-citibike-tripdata_1.csv` using public HTTP Range requests. The entire 369,035,302-byte archive was **not** downloaded. The saved sample has 20,000 rows, SHA-256 `4954ee134282588a584cede1b9da6990a7bda88dadff12e1f80ce9f25fe2b244`, and approximately 4 MiB transferred. It is explicitly marked sampled.

The final bounded validator output is in ignored `artifacts/citibike-real-audit-04/validation.json`; a portable summary is [validation_summary.json](audit/validation_summary.json).

| Executed check | Genuine result |
|---|---|
| Real modern CSV parsing and station-ID preservation | Passed |
| Cleaning reconciliation | 19,986 unique accepted; 14 rejected; zero exact duplicates |
| Rejection breakdown | 9 outside source month; 4 missing start station; 1 invalid duration |
| Station/hour aggregation | 1,651 stations, 17,794 nonempty BI station-hours |
| Sample-scoped Gold ML grid | 546,481 rows; unique station/hour keys; totals reconcile |
| Actual PyIceberg SQL/SQLite catalogue | Bronze 20,000, Silver 19,986, BI 17,794, ML 546,481 rows written/scanned successfully |
| Lightweight real-source model smoke | Logistic fit on 114 training hours; 24 validation/25 test hours reserved; finite next-hour probabilities; model explicitly not deployable |
| Actual DuckDB cache + FastAPI TestClient | Readiness, station discovery, prediction reads and query validation passed |
| Automated regression suite | 44 passed, 3 skipped, 0 failed in the recorded final run; dependency warnings remain |
| Streamlit AppTest | Empty state and four-card/charts rendering of actual smoke prediction; lookback change passed; HTTP fetch mocked |
| Compilation and environment | Python compileall passed; `pip check`: no broken requirements |

These are **functional** checks. Sample-derived counts are not full-month demand and cannot produce defensible generalization scores. No accuracy, F1, cost savings or production champion metric is claimed. Synthetic feature/model-family tests exercise functions only; their metrics are not reported as real-world results.

Failures observed and resolved during validation:

1. PyIceberg file URI paths became `/C:/...` on Windows: added a local audit FileIO adapter. This adapter does not alter Spark production IO.
2. Actual append failed because PyArrow 16 rejected `store_decimal_as_integer`: pinned and installed PyArrow 19.0.1; all four writes then passed.
3. Initial dashboard tests failed because Plotly was missing in the isolated audit environment: installed the pinned dependency, then both dashboard tests passed. Dashboard requirements were also aligned with the tested Streamlit version.
4. Original DAGs selected only event `[0]`: now refuse coalesced events rather than silently losing months. This is a safety fix, **not** a complete multi-event orchestration implementation.

Skipped: Docker Compose runtime/build validation, Airflow DAG imports/scheduling, and opt-in Spark/Java/Iceberg runtime validation. Docker and Java executables were unavailable, and Airflow/PySpark runtime integration was not installed/executed. PostgreSQL was not running. None of those are counted as passing. Streamlit AppTest is not browser automation. A full source-to-dashboard network test was not performed.

## 4. Completion assessment

**Estimated overall completion: approximately 60%.** This is a subjective engineering estimate of runnable, validated project readiness, not a measured coverage/accuracy statistic. Working reference data contracts/features and local serving earn credit; unexecuted distributed integration, missing complete-data evaluation and production hardening prevent a higher assessment. “Code exists” is not treated as “complete.”

| Component | Classification | Evidence / unfinished work |
|---|---|---|
| Official ingestion | Implemented but needs fixes | Real bounded source verified; full ZIP streaming/member handling implemented but full monthly ingest not run; historic schema/naming coverage limited |
| Bronze/Silver Spark ETL | Implemented but needs fixes | Migrated contracts, quarantine and publication; pandas reference passed; Spark jobs not executed |
| Gold BI/Gold ML Spark layers | Implemented but needs fixes | Reference totals/grids verified; actual Spark tables/full-month density not verified |
| Iceberg persistence | Partially implemented | Actual SQL-catalog append/read passed; production Hadoop-catalog writes, MERGE, overwrite, retries and restart not verified |
| Airflow orchestration/watcher | Partially implemented | Asset URIs/lineage migrated; manifest validation tested; live imports/events/scheduling unverified; multi-event workflow incomplete |
| Data quality | Implemented but needs fixes | Reference checks, DST, duplicates, quarantine tests pass; production Spark parity and rejection threshold on complete data pending |
| Single-station feature engineering | Complete and tested | Leakage-safe lags, chronological split, train-only thresholds, local calendar and online/offline parity tests pass |
| Model training tooling | Partially implemented | Genuine small fit and all three family unit smoke tests pass; complete six-experiment Citi Bike run/champion not executed |
| Model evaluation | Partially implemented | Classification/penalty/confusion-matrix machinery retained and tested functionally; complete-data held-out benchmark and baseline comparison missing |
| Batch inference / feedback | Partially implemented | Station/checkpoint/time contracts migrated; actual Spark scoring, feedback settlement and cache replication pending |
| FastAPI | Implemented but needs fixes | Actual DuckDB-cache API tests pass, including opaque IDs, timezone lookup and failures; PostgreSQL-pool integration/load behavior unverified |
| Streamlit dashboard | Implemented but needs fixes | AppTest render/filter passes with actual smoke data; live HTTP browser and PostgreSQL-backed session pending |
| Docker | Implemented but needs fixes | Paths/ports/dependencies/images isolated; YAML inspected; no image build, Compose runtime or service health validation |
| PostgreSQL/cache persistence | Partially implemented | Text key schema and atomic upserts migrated; no real PostgreSQL write/read/restart test; DuckDB local adapter passed |
| End-to-end integration | Partially implemented | Bounded official sample → reference marts → real SQL-catalog Iceberg → lightweight model → DuckDB → API passes; actual Airflow/Spark/PostgreSQL chain not run |
| Automated CI / operational monitoring | Missing | Local regression suite exists; no CI execution, alerting, drift monitoring or validated registry deployment |

## 5. Prioritized finishing checklist

### P0 — required for a credible full-project demonstration

- [ ] Provide Docker Linux containers/WSL2 or Java 17 + Spark/Iceberg runtime. Build all images, run `docker compose config --quiet`, check Airflow import errors and service health. Current pins/image constraints must be tested, not assumed compatible.
- [ ] Ingest **one complete official month** in an isolated Citi Bike warehouse. Run `tools/full_stack_check.py`; reconcile source/accepted/rejected/duplicate counts and BI/ML sums. Verify month boundaries, observed-station cardinality, DST parity and quality gate using the actual Spark engine.
- [ ] Exercise asset lineage, duplicate-source retry, downstream-failure recovery and corrected-month replay. Implement safe multi-event handling before rapid multi-month uploads. Avoid replaying an older corrected archive over newer Silver state; version/ordering protection is not implemented.
- [ ] Train bounded six-family experiments on complete chronological station history; evaluate the selected champion once on held-out data versus the naive baseline. Use a new models directory. Check sparse-class handling and publish only genuine metrics.
- [ ] Run actual post-cutoff batch scoring, Iceberg-to-PostgreSQL upsert and repeated-run idempotency. Verify feedback actuals and timezone handling across both stores. Current feedback updates the PostgreSQL cache; durable Iceberg feedback parity remains a gap.
- [ ] Test live API readiness/errors, station lookup and dashboard charts/filtering in a real browser, backed by PostgreSQL. Run restart/persistence checks. Record real screenshots only after this passes.

### P1 — reliability and reproducibility

- [ ] Implement multi-event processing and bounded watcher concurrency; current watcher uses in-memory retry/lock state and first-page asset lookup, not a durable event queue. `.processed` acknowledges event receipt, not successful ETL.
- [ ] Verify Hadoop metadata version-hint reads for cache synchronization under concurrent writes. Validate append/MERGE/partition replacement failure recovery and Spark-session cleanup on errors. Set PostgreSQL session timezone explicitly if running outside UTC containers.
- [ ] Prevent train/test leakage across repeated manual evaluations; freeze complete-data evaluation periods and persist source checksums, split boundaries, model seed and dependency versions. Add baseline test metrics to training experiment summaries.
- [ ] Test wider date/archive variations; add explicit legacy Citi Bike adapters only when real samples justify them. Establish station lifecycle/closure handling before claiming zero-count hours represent actual service availability. Sample extraction currently expects direct modern CSV members; the full reader also supports bounded nested archives.
- [ ] Add CI for the lightweight suite and a separate container integration job. Test the full six-experiment path and cache sync/feedback with disposable databases. Validate memory/disk requirements on the intended machine.
- [ ] Audit tracked teaching credentials and notebook outputs/history before public sharing. Rotate secrets, bind exposed services appropriately, remove hard-coded teaching authentication through a reviewed change, and test fresh-volume initialization without deleting existing volumes.

### P2 — extensions, not prerequisites for the current single-station scope

- [ ] Multi-station champions or a global station model; exact-count forecasting and rebalancing decisions require additional evaluation.
- [ ] Real model registry/tracking service if desired; legacy `mlflow_run_id` strings currently identify local JSON runs only.
- [ ] Data/model drift monitoring, retraining policy, alerting, backups and recovery procedures.
- [ ] Replace historical taxi screenshots with a verified Citi Bike full-stack demonstration; optional dashboard station discovery selector and richer BI visualizations.

## Readiness verdict

- **Ready now:** local bounded genuine-data/reference/Iceberg-client/API demonstration and regression suite, with the sample caveat visible.
- **Not yet verified ready:** complete Docker/Airflow/Spark/PostgreSQL demonstration, deployable Citi Bike champion or production use.
- **GitHub showcase:** suitable as an honestly labeled work in progress after reviewing tracked teaching credentials/history. Not ready to advertise as a finished production-grade Citi Bike MLOps platform. No commit or push was made.
