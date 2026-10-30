# Official monthly downloads and sequential ingestion

> Follow-up: [CONTROLLED_INGESTION.md](CONTROLLED_INGESTION.md) records the now-running multi-year coordinator, year gates and resource safeguards. Statements below about not starting the full run describe the earlier pilot snapshot. Full execution now also requires the verified coordinator dry-run gate.

**Pilot verified, 2026-10-27 America/Los_Angeles. The multi-year run was not started.** Actual evidence: [audit/monthly_ingestion.json](audit/monthly_ingestion.json). Source availability was fetched from the public bucket linked by [Citi Bike System Data](https://citibikenyc.com/system-data), not inferred from the calendar or repository name.

| Calendar year, New York local time | Role | Official monthly ZIPs currently listed |
|---|---|---:|
| 2024 | Training | 12 |
| 2025 | Validation/model selection | 12 |
| 2026 | Held-out testing | 9: January–September |

The 33 currently listed archives total **23,865,257,406 bytes**. October–December 2026 were not listed and are not requested. The controller rediscovers availability on every invocation, follows paginated listings, excludes Jersey City/non-monthly objects, and requires all 2024/2025 months before a full run. Listing an object does not certify its contents: every month must still pass download/schema and ETL audits.

## Verified missing-month pilot

January's previously verified complete archive was reused into the cache, with **zero archive-download bytes** and no January ingestion. The missing-month pilot downloaded the official `202402-citibike-tripdata.zip`, **414,712,349 bytes**, SHA-256 `f0209d02b862ec7422ea251f55626c007c8289adb62230e870ed7da24ef77f05`.

A genuine **8,388,608-byte** HTTP range prefix was retained to exercise interrupted-transfer recovery. The controller resumed with `Range`/`If-Match`, transferred the remaining **406,323,741 bytes**, checked the complete ZIP CRCs and each modern CSV header, and computed its full SHA-256. No recognition/model metrics are involved. SHA-256 is a locally computed, pinned integrity digest; Citi Bike does not supply an independent SHA-256 here. The multipart S3 ETag is an origin-version validator, **not** a whole-file MD5.

All three CSV members were ingested: 1,000,000 + 1,000,000 + 121,501 records.

| Actual audit | Result |
|---|---:|
| Bronze source rows | 2,121,501 |
| Silver accepted unique rides | 2,118,773 |
| Quarantine | 2,728 |
| Missing start station / invalid duration / outside February | 1,864 / 631 / 233 |
| Duplicate non-null Bronze ride IDs / Silver IDs | 0 / 0 |
| Gold BI occupied station-hours | 626,288 |
| Gold ML dense station-hours | 1,552,776 = 2,231 stations × 696 hours |
| Ride-count sum in each Gold table | 2,118,773 |
| Duplicate station-hour keys | 0 |
| Complete monthly feature rows after warmup | 1,177,968 |
| Intentional monthly-only warmup rows | 374,808 |
| Null/NaN/infinite values across 13 post-warmup features | 0 |

The controller completed in **650.686 seconds**, including source transfer/verification, real watcher delivery, four asset-triggered Airflow DAGs and the audit. All four DAGs succeeded on attempt 1; all five worker jobs exited 0. Component times include each subprocess's fresh JVM startup/teardown:

| Worker job | Seconds |
|---|---:|
| Bronze | 94.991 |
| Silver | 339.800 |
| Gold BI | 23.107 |
| Gold ML | 23.230 |
| Independent source/layer/continuity audit | 80.152 |

The controller is capped at 256 MiB and peaked at **108,634,112 bytes (103.602 MiB)**. The Spark worker is capped at **2 GiB**, with a 1 GiB driver, `local[1]`, two CPU quota/Java processors and eight sequential shuffle partitions. Peak charged worker memory, including file cache, was **2,009,538,560 bytes (1.872 GiB)**; sampled working set peaked at **1,789,165,568 bytes (1.666 GiB)**. Sampling is every 0.5 seconds. Memory-limit, OOM and OOM-kill counters were **0**. These are per-container measurements, not whole-laptop RAM. Docker has 3.673 GiB allocated. The new warehouse occupies **237,740,393 logical file bytes**, including retained Silver candidate/metadata files.

A second `pilot --month 2024-02` returned `CHECKPOINT_REUSED`: **zero new asset events, zero new Spark jobs**, and identical six-table snapshot IDs/counts. Only one incoming and one of each successful outlet event exist. The source-cache manifests, archived receipt, queue results and monthly checkpoints are retained locally.

## Download, delivery and recovery contracts

- `jobs/monthly_archives.py` discovers official objects, streams bounded chunks and keeps `.zip.partial` plus `.partial.json` origin checkpoints. Safe HTTP resume requires an unchanged ETag/length and an exact `206 Content-Range`. Ignored ranges, changed origins, oversized responses or corrupt existing files fail closed without overwriting existing data.
- Transfer/connection failures and retryable HTTP statuses use bounded retries/backoff. Complete files pass CRC/header checks and receive a SHA-256 manifest. A crash between the final data rename and manifest publication can repair its manifest from the completed download checkpoint without another download. Windows-created manifest paths are normalized to the actual cache path when reused inside Docker.
- The new `--resume --reuse` options also expose this behavior through the existing `jobs.download_raw_data` CLI. Legacy sample/correction workflows remain available; automated multi-year ingestion pins initial source versions. Origin corrections require the explicit revision/predecessor workflow and checkpoint review, rather than silently overwriting a pinned source.
- The archive cache is outside the watch directory. Only a fully verified archive and atomic manifest are staged for the existing watcher. The watcher retains its 10-second reconciliation fallback, paused-consumer protection and unique archived receipts.
- Four `monthly_citibike_{bronze,silver,bi,ml}` DAGs use the same stage/lineage code as the existing pipeline, the one-slot `citibike_spark` pool and Airflow retries. Assets have their own `local://citibike/monthly-01/…` namespace. The capped file-transport worker permits ETL/audit modules only; no training/scoring module can be dispatched.
- The controller waits for the actual created DAGs to succeed and for both Gold outlets, then queues a fresh source/layer/feature audit. Only then does it atomically mark `checkpoints/YYYY-MM.json` verified. One global OS lock prevents competing controllers; each monthly stage/publication also retains existing locks and idempotency guards.
- An interrupted controller reuses the cache, delivery ID, queue response and committed publications. After a recorded failure, a subsequent invocation retains delivery history and gives the retry a new delivery ID; committed stages replay rather than append. The controller stops on an unresolved month and does not advance to later sources.
- Verified-month skips check that the same table UUIDs still exist. This assumes controller ownership of the isolated tables; arbitrary external in-place edits/manual corrections require an explicit audit/checkpoint review. Completed checkpoints are not proof against an unrelated writer bypassing the pipeline.
- A worker restart requires `--resume` when old state exists. Previous memory/summary/fatal/stop files and interrupted-job telemetry are archived under `worker-attempts/`; completed queue results and dataset/model checkpoints are preserved.

Runtime state lives under ignored `artifacts/monthly-ingestion-01/`. The catalog is **`citibike_monthly`**, warehouse `artifacts/monthly-ingestion-01/warehouse`, cache `data/citibike/monthly/archives`. Original production paths/configuration are not redirected.

## Cross-month context and leakage protection

Gold ML stays a monthly-partitioned **observation table**. Feature construction orders a station's entire history, without grouping/resetting at month or year boundaries. All lags use earlier hours; rolling statistics are shifted. Missing station-months are not fabricated as active zero-demand history: actual gaps restart the 168-hour warmup, and duplicate hours are rejected/validated. The Spark implementation requires unique, hour-aligned Gold keys and verifies the trailing 168-row span is exactly 168 hours.

The pilot read January's existing warehouse **without writing it** to validate February's first hour for station `6140.05`. All **696 February feature rows** were available with prior context, compared with 528 from February alone. Spark and NumPy features matched; lags 1/2/24/168 matched the actual earlier-hour ride counts. Mutating the current and future counts left the first-hour feature vector unchanged. In a normal multi-year run, context comes from earlier months already loaded into the monthly catalog; the old January warehouse is only an optional pilot reference.

The generated profile sets `ml.split_policy: calendar_year`. When explicitly trained later, `ml_train_pipeline` selects its station using **2024 only**, and `prepare_year_model_frames` fits class thresholds on **2024 only**, with 2025 reserved for selection and 2026 for testing. The existing finalization helper previously combined training and validation; strict year mode now disables that behavior, so **2025 is excluded even from final fitting**. A stub-model regression verifies the fitted rows without training a real model. Exported metadata separately records the actual fit cutoff and validation-based selection/information cutoff; scoring retains the conservative latter cutoff to avoid backtesting before model-selection information existed. Year membership is **New York local calendar time**, not the UTC year. Cyclical encodings use fixed periods. Tests confirm that changing validation/test counts cannot change training features or thresholds. Existing fractional-split behavior remains the default for older configurations.

Earlier observed validation/test hours may supply later lag features for **one-hour walk-forward evaluation**. This does not justify using future observations for a fixed-origin forecast of the whole held-out year. No model was trained or evaluated by this ingestion workflow, and no test-set performance claim is made.

## Inspect the completed local run

```powershell
docker exec citibike_airflow_scheduler python -m tools.monthly_ingestion status
Get-Content artifacts/monthly-ingestion-01/checkpoints/2024-02.json
Get-Content artifacts/monthly-ingestion-01/pilot-complete.json
docker logs citibike_monthly_pilot_01
docker logs citibike_monthly_worker_01
.\.venv\Scripts\python.exe -m pytest -q
```

All dedicated pilot containers are stopped; all monthly/production ingestion and scoring DAGs, including scheduled discovery, remain paused. Core services remain running. Inspection does not trigger another month.

## Fresh setup or resume

The existing Docker stack/image and one-slot Spark pool must be available. Run `prepare` **only if the monthly root does not exist**; it refuses to overwrite retained state:

```powershell
docker exec citibike_airflow_scheduler python -m tools.monthly_ingestion prepare
docker exec citibike_airflow_scheduler python -m tools.monthly_ingestion plan
```

For the current prepared workspace, skip `prepare`. The watcher dependency was copied from the already installed `watchdog==4.0.2` into the monthly root and import-tested; no extra dependency download was needed. On a fresh workspace, install that small package into the isolated target once (the Airflow image already includes requests/PyYAML):

```powershell
$repoPath = (Get-Location).Path
docker run --name citibike_monthly_deps --network nyc-bike-mlops_platform_network --entrypoint python --memory=192m --memory-swap=192m --mount "type=bind,source=$repoPath,target=/opt/airflow" --workdir /opt/airflow citibike-airflow-spark-iceberg:3.2.2-python3.11 -m pip install --no-cache-dir --no-deps --target artifacts/monthly-ingestion-01/watcher-deps watchdog==4.0.2
```

Start a capped worker and watcher with **new container names**. `--resume` archives existing worker telemetry/stop markers and preserves queue results. No Docker socket is exposed and no volumes are removed:

```powershell
$repoPath = (Get-Location).Path
docker run -d --name citibike_monthly_worker_next --hostname localhost --network none --entrypoint python --cpus=2 --memory=2g --memory-swap=2g --mount "type=bind,source=$repoPath,target=/opt/airflow" --workdir /opt/airflow -e PYTHONPATH=/opt/airflow -e SPARK_LOCAL_IP=127.0.0.1 -e "JAVA_TOOL_OPTIONS=-Duser.home=/home/airflow -XX:ActiveProcessorCount=2" citibike-airflow-spark-iceberg:3.2.2-python3.11 -m tools.full_month_queue --root /opt/airflow/artifacts/monthly-ingestion-01 --resume
docker run -d --name citibike_monthly_watcher_next --network nyc-bike-mlops_platform_network --entrypoint python --memory=192m --memory-swap=192m --mount "type=bind,source=$repoPath,target=/opt/airflow" --mount "type=bind,source=$repoPath/airflow_auth,target=/opt/airflow/auth,readonly" --workdir /opt/airflow -e PYTHONPATH=/opt/airflow/artifacts/monthly-ingestion-01/watcher-deps:/opt/airflow citibike-airflow-spark-iceberg:3.2.2-python3.11 -m tools.monthly_ingestion watcher
```

A **single** missing-month pilot selects the first not-yet-cached official archive; specify `--month YYYY-MM` to resume the same month. For this workspace the completed month is `2024-02`; omitting the month now would choose another missing archive, not simply inspect February.

```powershell
# Resume/check the completed pilot, without selecting another missing month:
docker run --name citibike_monthly_pilot_resume --network nyc-bike-mlops_platform_network --entrypoint python --memory=256m --memory-swap=256m --mount "type=bind,source=$repoPath,target=/opt/airflow" --mount "type=bind,source=$repoPath/airflow_auth,target=/opt/airflow/auth,readonly" --workdir /opt/airflow -e PYTHONPATH=/opt/airflow citibike-airflow-spark-iceberg:3.2.2-python3.11 -m tools.monthly_ingestion pilot --month 2024-02
```

**Not executed:** to ingest all currently available months sequentially after explicitly deciding to proceed, use the same capped controller command with a new name and replace its final arguments with `-m tools.monthly_ingestion run --all`. It requires a verified `pilot-complete.json`, starts at January 2024, reuses verified archives/checkpoints and advances only after each month's audit. Do not run multiple controllers/workers against the same state directory. Keep original production/scoring and unrelated fixture workloads paused.

Alternatively, explicitly enable **`monthly_citibike_downloads`** after starting the capped worker/watcher. It is implemented but **left paused and not executed**. Its daily 08:00 UTC schedule discovers newly published objects; its coordinator occupies `default_pool`, not the Spark pool, while downstream jobs use `citibike_spark`. It requires at least two LocalExecutor slots (the current stack has two) to avoid waiting on downstream work in a single-slot executor. The external capped controller is the validated path; the scheduled multi-year coordinator still needs its first actual run.

To stop after work finishes: pause the monthly DAGs with `python -m tools.monthly_ingestion pause` inside the scheduler, stop the dedicated watcher, and create the `stop-worker` control file in the monthly root for graceful worker shutdown. Keep datasets/checkpoints and Docker volumes intact.

## Tests, limits and readiness

**109 passed, 2 skipped, zero failures/errors** (17 dependency warnings). Skips require host Airflow/PySpark; actual Docker DAG imports and full Spark processing passed. Coverage includes resumable/cut transfers, changed-origin rejection, corrupted-file preservation, crash-after-rename recovery, paginated discovery, full-run gating, worker-state preservation, cross-year context, train-only thresholds, validation-excluding finalization and gap handling. Compilation, dependency consistency, Compose checks and documentation links pass.

Original source/model/PostgreSQL fingerprints, the previous orchestration tables and all 93 prior January warehouse files are unchanged. No pipeline model training, scoring, commits or pushes occurred.

**Ready for a controlled sequential multi-year ingestion; not yet validated across all 33 archive contents or the scheduled full-run coordinator.** The pilot and checkpoint replay have passed and the execution gate is prepared, but no multi-year run was launched. The listed archives alone require about 22.23 GiB; actual ETL/retained-candidate/spill/backup requirements are additional. Host C: free space was **114,518,429,696 bytes** during this validation. Larger months or schema changes may expose new limits; the controller stops at a failed month and retains recovery state instead of claiming success or advancing silently. Monitor disk and the existing 2 GiB worker cap throughout the eventual run.
