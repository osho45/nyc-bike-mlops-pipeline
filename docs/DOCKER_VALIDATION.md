# Local Docker infrastructure validation

Date: 2026-10-27 (America/Los_Angeles). **Latest status: bounded container end-to-end validation passed.** The earlier missing-Docker/WSL blocker is resolved. No full-month ingestion or expensive model training was performed.

> Follow-up validation now covers real Chromium, service restarts, live asset dependencies/retries and duplicate replay. See [RECOVERY_VALIDATION.md](RECOVERY_VALIDATION.md). Sections below describe the initial Docker validation.

> Later evidence: [correctness/crash restoration](CORRECTNESS_RECOVERY.md) and [successful complete-month Spark ETL](FULL_MONTH_VALIDATION.md). Optional sample/fixture serving containers described below were intentionally stopped for full-month memory headroom; their data is preserved.

## Verified runtime

Docker Engine 29.9.2, Compose v5.6.1, Linux containers. The engine exposes 3.673 GiB RAM. The three application images built successfully: Airflow/Spark/Iceberg, FastAPI and Streamlit. Dependency consistency checks passed in all three images and in the host virtual environment.

| Component | Actual verification |
|---|---|
| Airflow metadata PostgreSQL | Healthy; Airflow schema migration completed with exit 0 |
| Airflow API/authentication | Authenticated REST requests passed; API health healthy |
| Airflow scheduler and DAG processor | Healthy heartbeats; seven registered DAGs; zero import errors |
| Spark / Java / Iceberg | Real Spark 3.5.6 + Java 17 executed Bronze, Silver, both Gold jobs and prediction MERGE against the Hadoop Iceberg catalogue |
| Operational PostgreSQL | Healthy; schema bootstrapped on the fresh volume; real text station keys and repeated upserts verified |
| FastAPI | Live PostgreSQL-backed readiness, station discovery, list/single prediction and invalid-limit handling passed |
| Streamlit | Health HTTP 200; real API fetch, four metrics, two charts, sample warning and lookback filtering passed using AppTest |
| Container networking | Airflow reached API/UI; UI reached API; API reached PostgreSQL using Compose DNS |

Streamlit AppTest used real HTTP responses, not mocked fetches. Browser automation was **not** performed. The optional watcher and Jupyter services were deliberately not started; production ingestion and scoring DAGs remain paused. No triggerer is configured because these validation tasks do not use deferrable operators.

## Final genuine-data pipeline

DAG: `validation_citibike_sample`; final run: `bounded-sample-03`; state: **success**. Airflow duration: **52.03 seconds**; checker duration: **49.04 seconds**.

Input: existing official January 2024 sample, SHA-256 `4954ee134282588a584cede1b9da6990a7bda88dadff12e1f80ce9f25fe2b244`. No new dataset download occurred.

| Result | Actual value |
|---|---:|
| Source rows | 20,000 |
| Accepted rides | 19,986 |
| Quarantined | 14 |
| Dense sample-scoped Gold ML rows | 546,481 |
| Smoke model training rows | 114 |
| Served station ID | `6140.05` |
| Served prediction hour (UTC) | `2024-01-13 23:00:00` |
| PostgreSQL rows after two identical upserts | 1 |

The pipeline verified Bronze checksum-retry idempotency, Silver/quarantine reconciliation, BI/ML ride totals, accepted-ID/rejection-reason/fractional-duration parity with the reference cleaner, a tiny logistic fit, actual Iceberg batch scoring and actual Iceberg-to-PostgreSQL synchronization. The model was fitted on training rows only. No classification accuracy or generalization metric is claimed.

Sample isolation is enforced: catalogue `citibike_validation`; warehouse and smoke checkpoint under `artifacts/container-sample-03/`; database `citibike_validation_container_sample_03`. Sample mode refuses production catalogue/warehouse paths and limits Bronze input to 20,000 records. Production ingestion still rejects samples, and production scoring rejects validation-only checkpoints. Sample counts and artificial zeros are not population demand; the dashboard displays a warning.

The production cache contained zero rows before validation and still contains zero rows. Existing datasets, model paths, application settings and other repositories were preserved. Earlier failed/successful validation directories and databases were retained.

## Failures encountered and fixed

1. **Spark heap exhaustion in Silver quarantine code generation.** Run `bounded-sample-01` failed with `java.lang.OutOfMemoryError: Java heap space`. The recursive CASE expression embedded its previous value twice per rule, expanding rapidly. Replaced it with linear ordered `coalesce(when(rule_failed, reason), ...)`. Run 02 then passed using the same 512 MiB driver heap; no data-quality rule or priority was relaxed.
2. **Fractional durations truncated.** Cross-engine comparison showed up to 0.995 seconds lost because Spark cast timestamps to integer seconds. Changed duration computation/validation to microsecond differences. Run 03 passed explicit reference parity checks within one microsecond.
3. **Host pytest directory locked by Docker Desktop bind mounts.** A rerun produced 17 fixture setup errors while deleting the old fixed test directory. Tests now allocate a new repository-local path per run. The subsequent final suite passed.
4. **Fresh operational database lacked a schema initialization mount.** Mounted the existing non-destructive `CREATE TABLE IF NOT EXISTS` SQL into PostgreSQL initialization. Fresh startup and API readiness passed. This does not mutate or migrate an existing table schema automatically.
5. Added a root `.dockerignore` for datasets, checkpoints, secrets, caches and generated files. Constrained Airflow concurrency, paused new DAGs by default, and added the strictly isolated/manual validation path rather than weakening production sample guards.

A diagnostic initially tried importing `requests` inside the minimal API image; that optional probe failed because the API does not require Requests. The actual API probe used Python's standard-library HTTP client and passed; no unnecessary API dependency was added.

Non-fatal warnings remain: missing optional native Hadoop library (Java fallback used), Iceberg version-hint lookup warnings when creating a new candidate table, local-only Airflow task logs, dependency deprecations and AppTest's expected missing-ScriptRunContext warning. Some Spark stderr warnings are labeled error by Airflow's stream logger; the final task/run states and assertions establish success.

## Tests and evidence

- Final host suite: **48 passed, 2 skipped, 0 failures/errors**, 17 warnings. Host-only Airflow import and opt-in Spark tests remain skipped because those runtimes are provided in Docker; actual container execution above verifies the corresponding runtime paths.
- `pip check`: passed on host and all application images.
- `compileall` and `git diff --check`: passed.
- Live sample-backed API/Streamlit probe passed after final run 03.

Portable summary: [audit/docker_validation_summary.json](audit/docker_validation_summary.json).

Ignored detailed artifacts:

- `artifacts/docker-resume-build.log`
- `artifacts/docker-resume-status-03.json`
- `artifacts/container-sample-03/validation.json`
- `artifacts/docker-resume-serving-final.log`
- `artifacts/docker-resume-tests-final.xml`
- `logs/dag_id=validation_citibike_sample/run_id=bounded-sample-03/`

Earlier attempt artifacts remain available, including the run-01 failure. Existing local `.env` values were preserved; secrets are not included in the portable report.

## Running demonstration and reproduction

| Interface | URL |
|---|---|
| Sample dashboard (final isolated database) | http://127.0.0.1:8503/ |
| Sample API docs | http://127.0.0.1:8011/docs |
| Airflow | http://127.0.0.1:18080/ |
| Original/default dashboard | http://127.0.0.1:8502/ |
| Original/default API | http://127.0.0.1:8010/docs |

Required core services are left running. Previous sample-02 UI/API containers are retained stopped with `_02` suffixes, so Compose can report them as orphans; this is intentional preservation, not a service failure. Do not use `--remove-orphans` or volume deletion as a troubleshooting shortcut.

From the repository root:

```powershell
docker compose config --quiet
docker compose build
docker compose up -d --no-build airflow-webserver airflow-scheduler airflow-dag-processor fastapi streamlit
docker compose ps -a
docker compose logs -f airflow-scheduler
```

Only the validation DAG was unpaused. To repeat the bounded pipeline, use a **new run ID and output directory**:

```powershell
docker compose exec -T airflow-scheduler python -m tools.airflow_validation_control trigger --run-id bounded-sample-next --output artifacts/container-sample-next
docker compose exec -T airflow-scheduler python -m tools.airflow_validation_control status --run-id bounded-sample-next
python -m pytest -q
```

The existing sample manifest must be present at `data/citibike/audit-source-cached/202401-citibike-tripdata.sample.csv.manifest.json`. The checker validates its checksum; missing samples are not downloaded automatically. The helper authenticates using the existing local Airflow auth file without printing tokens/passwords. Do not print expanded Compose configuration into shared logs; use `--quiet`.

The sample serving containers were created separately using `docker compose run -d --no-deps`, with `POSTGRES_CACHE_DB=citibike_validation_container_sample_03` for FastAPI, and `API_BASE_URL=http://citibike_validation_api:8000`, `STATION_ID=6140.05` for Streamlit. Ports are bound to loopback at 8011 and 8503. They do not replace the original API/dashboard service configuration. `tools/probe_sample_serving.py` can be piped into `docker exec -i citibike_validation_ui python -` to repeat the live-HTTP/AppTest check.

## Remaining limits and next action

No blocker remains for this **bounded local infrastructure demonstration**. This does not validate complete-month ingestion, large batches, a deployable citywide/station model, real-world prediction quality, production asset-event lineage/coalescing, restart/disaster recovery, or real browser behavior. The watcher/Jupyter services and deferrable-task support were not exercised.

Next: review the sample dashboard and Airflow run, then test isolated restart/retry and asset-event handling before authorizing complete-month ingestion or the full model experiment matrix. Docker currently has only 3.673 GiB available, so keep conservative concurrency and verify resource use before scaling. The first pipeline failure was fixed in code; increasing RAM was not required for the successful sample run.

No commits or pushes were made. No original datasets, checkpoints, volumes or user data were deleted.
