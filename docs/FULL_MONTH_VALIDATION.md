# Controlled full-month Citi Bike ETL validation

> Follow-up: [FULL_MONTH_ORCHESTRATION.md](FULL_MONTH_ORCHESTRATION.md) now verifies isolated real watcher/Airflow delivery, automatic retry and duplicate/replay behavior on this archive. The scope/timings below describe the original standalone Spark run.

This run uses the complete official **January 2024 NYC Citi Bike archive**, not the earlier 20,000-row sample. It executes the existing Bronze, Silver, Gold BI and Gold ML Spark functions in one isolated process. It does **not** train a model or publish to the original warehouse, PostgreSQL cache, or application.

## Source and isolation

- Source: <https://s3.amazonaws.com/tripdata/202401-citibike-tripdata.zip>.
- Downloaded and CRC-verified on 2026-10-27. The downloader recorded a non-sample manifest, archive size **369,035,302 bytes**, SHA-256 `0a2e81eacd7bf3890712de8f2a1b56bda985c17d9e61b08acf5e7c7ec9f20eb0`.
- Both CSV members are consumed: `202401-citibike-tripdata_1.csv` (1,000,000 rows) and `202401-citibike-tripdata_2.csv` (888,085 rows).
- Input remains under ignored `data/citibike/full_month_validation/incoming/`; all run output remains under ignored `artifacts/full-month-202401-01/`.
- Catalog: `citibike_fullmonth`; warehouse: `/opt/airflow/artifacts/full-month-202401-01/warehouse`. Previous warehouses, source files, checkpoints and fixture data are retained.

Docker reported **3,943,489,536 bytes (3.673 GiB)** total memory. Optional validation UI/API services and the correctness-fixture PostgreSQL service were stopped, without removing their containers or volumes. Core Airflow, PostgreSQL, API and dashboard services remained running. All automatic data-processing DAGs were paused to avoid concurrent ingestion.

The worker uses `local[1]` (one Spark task at a time), a 1 GiB JVM heap, eight sequential shuffle partitions, a two-CPU container quota and a **2 GiB hard container memory limit** with no additional swap. The eight shuffle partitions do not mean eight concurrent tasks.

## Actual results

**PASS: complete-month Spark ETL, reconciliation, feature audit and idempotent replay.** Container `citibike_fullmonth_202401_01` exited **0**, without OOM or OOM-kill events, on its first attempt.

| Check | Measured result |
|---|---:|
| Bronze source records | 1,888,085 |
| Silver accepted unique rides | 1,885,944 |
| Quarantine | 2,141 |
| Quarantine: missing start station | 1,160 |
| Quarantine: invalid duration (nonpositive or over 24 hours) | 571 |
| Quarantine: outside source month | 410 |
| Bronze duplicate non-null ride IDs / Silver duplicate IDs | 0 / 0 |
| Observed start stations | 2,223 |
| Gold BI occupied station-hours | 620,302 |
| Gold ML station-hours | 1,653,912 = 2,223 × 744 |
| Ride-count sum in **each** Gold table | 1,885,944 |
| Duplicate station-hour keys in **each** Gold table | 0 |
| Complete feature rows after warmup | 1,280,448 |
| Intentional 168-hour warmup rows | 373,464 |
| Null/NaN/infinite values after warmup, all 13 features | 0 |
| Largest NumPy/Spark feature delta across three stations | 8.354e-13 |
| Replay changes to counts, schemas or snapshot IDs across six tables | 0 |

Station identifiers remain strings: the parity samples include `2733.03`, `SYS038`, and the busiest station `6140.05`; each contributes 576 complete feature rows. The required UTC span is January 1 at 05:00 through February 1 at 05:00, exclusive, corresponding to the complete New York local month. Gold ML is the existing dense observation contract; derived features were audited without fitting a model or writing a feature/model checkpoint. Missing start-station records cannot be attributed to a station and are not silently assigned one.

| Timed operation | Seconds |
|---|---:|
| Independent streamed source audit | 19.622 |
| Bronze | 70.560 |
| Silver | 284.487 |
| Gold BI | 8.206 |
| Gold ML | 9.427 |
| Layer and feature audit | 38.811 |
| Replay Bronze / Silver / Gold BI / Gold ML | 4.952 / 1.155 / 1.218 / 1.251 |
| Measured runner total, including Spark startup and additional snapshot queries | **456.659** |
| Docker container lifetime, including initial archive hashing/startup/teardown | **462.357** |

The worker ran from `2026-10-27T22:37:47.321Z` to `2026-10-27T22:45:29.678Z`. The four first-pass ETL stages sum to **372.680 seconds**; the rest is verification/startup rather than model training. These are single-run local timings, not a throughput SLA.

Peak sampled working set was **2,046,468,096 bytes (1,951.664 MiB, 1.906 GiB)**. Kernel `memory.peak` was **2,147,483,648 bytes (2 GiB)**, including file cache. There were **3,193 memory-limit/reclaim events**, **0 OOM events**, and **0 OOM kills**. Finishing at this cap does not imply abundant headroom; keep one worker and do not add training or parallel-month jobs to this configuration.

| Data volume (logical file bytes, including table metadata/checksum files) | Bytes |
|---|---:|
| Official ZIP | 369,035,302 |
| CSV members combined | 369,035,008 |
| Temporary normalized Parquet, 38 files | 107,749,807 |
| Bronze table directory | 63,897,791 |
| Published Silver table directory | 70,977,898 |
| Retained Silver audit candidate | 70,969,322 |
| Quarantine table directory | 126,365 |
| Gold BI table directory | 5,563,914 |
| Gold ML table directory | 719,664 |
| Ingestion ledger directory | 17,357 |
| Complete isolated warehouse, including retained candidate | **212,272,311** |

Temporary staging is cleaned normally by its temporary-directory lifecycle; original datasets and checkpoints were not deleted. Logical byte totals are not allocated host-disk blocks or peak temporary disk occupancy.

## Fixes, tests and preservation

- Preflight found an incorrectly encoded `environment` key in the prepared profile. The profile was cleaned, and `load_config` now reads `utf-8-sig`, accepting normal UTF-8 and Windows BOM-prefixed YAML. A regression test covers this case.
- Added the resumable, resource-measured audit/replay runner. Bronze now reports normalized staging bytes/file count and source rows alongside its existing lineage output; publication behavior is unchanged.
- Added a regression test for streamed reference totals and globally duplicated ride IDs across multiple archive members.
- Full-month processing required **no quality-rule relaxation or data-correctness workaround**. Iceberg emitted a missing version-hint warning while creating a new candidate table; the subsequent commit/publication and independent reads succeeded. Optional native Hadoop and network-interface warnings also did not prevent execution.
- Regression suite: **90 passed, 2 skipped**, zero failures. The skips are host Airflow import and opt-in Spark readiness checks; actual Airflow health/import checks and full Spark/Iceberg execution were performed in Docker. Host dependency consistency, compilation, Compose configuration and Git whitespace checks pass.
- Before/after preservation fingerprints match exactly for the original genuine sample, champion model/metadata, all six original sample Iceberg tables and both original PostgreSQL databases. Airflow metadata, scheduler and DAG processor remain healthy; DAG import errors: **0**. The triggerer remains intentionally absent, as before.

Portable actual output: [audit/full_month_validation.json](audit/full_month_validation.json), including table schemas, before/after replay snapshots, memory counters and preservation fingerprints. Detailed ignored local artifacts: `artifacts/full-month-202401-01/validation.json`, `memory-attempt-1.csv`, `container.log`, `container-state.json`, plus `artifacts/full-month-tests-final.xml` and the `artifacts/fullmonth-original-{before,after}.json` preservation snapshots.

## Reproduce or inspect

From the repository root in PowerShell, with the existing local Docker image and verified archive:

```powershell
$repoPath = (Get-Location).Path
docker run -d --name citibike_fullmonth_next --hostname localhost --network none --entrypoint python --cpus=2 --memory=2g --memory-swap=2g --mount "type=bind,source=$repoPath,target=/opt/airflow" --workdir /opt/airflow -e PYTHONPATH=/opt/airflow -e SPARK_LOCAL_IP=127.0.0.1 -e "JAVA_TOOL_OPTIONS=-Duser.home=/home/airflow -XX:ActiveProcessorCount=2" citibike-airflow-spark-iceberg:3.2.2-python3.11 -m tools.full_month_validate --source data/citibike/full_month_validation/incoming/202401-citibike-tripdata.zip --output artifacts/full-month-next --config config/full_month_validation.yml --year 2024 --month 1
docker logs -f citibike_fullmonth_next
docker stats citibike_fullmonth_next
Get-Content artifacts/full-month-next/validation.json
```

Use a **new container name and output directory** for an independent run. To resume an interrupted run, preserve its output directory and source, choose a new container name, and add `--resume` while keeping the original `--output` value. Completed stages are skipped and existing publication/replay guards handle partially completed stages. Never run two workers against the same output directory.

The isolated worker has no network access. Downloading is a separate explicit action; it does not happen during validation. To obtain the source if absent:

```powershell
.\.venv\Scripts\python.exe -m jobs.download_raw_data --year 2024 --month 1 --output data/citibike/full_month_validation/incoming --max-download-mb 512
```

## Validation contract

`tools.full_month_validate` independently streams the archive through the pandas quality contract and compares that reference with actual Iceberg tables. It checks all source members, accepted/rejected totals and reasons, unique ride IDs, timestamps/station types, positive durations, month boundaries, source revisions, duplicate station-hour keys and both Gold ride-count sums.

Gold BI contains occupied station-hours. Gold ML contains **all hours for every observed start station**, including zero-ride hours. Features are computed from these observations: four lags, four trailing statistics and five calendar features. The first 168 hours per station are intentionally unavailable for complete training features. All remaining feature rows must be finite and complete. An independent Spark feature calculation is compared with the existing NumPy implementation on the first, last and busiest stations.

Replay repeats all four stage entry points and compares **row counts, schemas and exact Iceberg snapshot IDs** for Bronze, Silver, quarantine, both Gold tables and the ingestion ledger. Count equality alone is insufficient for this check.

Memory is sampled every 0.5 seconds from the worker's cgroup. Sampled working set is `memory.current - inactive_file`; the exact kernel `memory.peak` includes charged filesystem cache. These are container measurements, not total laptop RAM or JVM heap alone. Stage and total elapsed times exclude the separate source download.

## Remaining scope

Full-month ETL is verified for this modern-schema January archive on the current local Docker host. This run directly invoked the production Spark stage functions in an isolated catalog; it did **not** deliver the complete archive through the filesystem watcher/Airflow asset chain, serve full-month predictions, train a model, test multiple complete months concurrently, or certify production scalability. Earlier bounded event, browser, retry and recovery evidence remains in [CORRECTNESS_RECOVERY.md](CORRECTNESS_RECOVERY.md).

Next: exercise full-month orchestration separately with scoring disabled, or review the retained Gold data before an explicitly authorized lightweight chronological training/evaluation run. Keep the current full-month data isolated and one Spark worker active at a time. Optional fixture services are intentionally left stopped to conserve RAM; the original API/dashboard remain at ports 8010/8502. No commits or pushes were made.
