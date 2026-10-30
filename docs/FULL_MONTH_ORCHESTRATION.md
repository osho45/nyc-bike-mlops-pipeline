# Full-month watcher and Airflow asset-event validation

> Follow-up: [MONTHLY_INGESTION.md](MONTHLY_INGESTION.md) adds official resumable discovery/download automation and a successful missing-month February pilot. This report retains the earlier January evidence.

This validation uses the existing `sftp_mock_daemon.py` watcher, Airflow's real asset-event API/scheduler, `run_asset_stage`, and the existing Bronze/Silver/Gold jobs. The input is the complete official January 2024 archive already verified in [FULL_MONTH_VALIDATION.md](FULL_MONTH_VALIDATION.md).

**Verified on 2026-10-27:** all three deliveries completed through real asset events, with **12 successful DAG runs**, **13 successful Spark subprocesses**, one deliberately failed/retried Airflow task attempt, and no failed DAGs. A fresh Spark audit matched the original full-month baseline exactly. The dedicated validation services are now stopped and their DAGs paused; core services and all retained data remain available.

Portable actual evidence: [audit/full_month_orchestration.json](audit/full_month_orchestration.json). This includes Airflow's actual event-to-created-DAG relationships, task attempts, measured timings/memory, replay decisions and final audit output. It does not infer success from code or receipt files alone.

## Actual results

| Reconciliation | Asset-produced result |
|---|---:|
| Bronze source records | 1,888,085 |
| Silver unique accepted rides | 1,885,944 |
| Quarantine | 2,141 |
| Missing start station / invalid duration / outside January | 1,160 / 571 / 410 |
| Gold BI occupied station-hours | 620,302 |
| Gold ML dense station-hours | 1,653,912 |
| Stations × complete month hours | 2,223 × 744 |
| Ride sum in each Gold table | 1,885,944 |
| Duplicate ride IDs / duplicate station-hour keys | 0 / 0 |
| Complete feature rows after 168-hour warmup | 1,280,448 |
| Intentional warmup rows | 373,464 |
| Invalid post-warmup values across all 13 features | 0 |
| Baseline schema/metric equality | Pass |

The source is the same **369,035,302-byte** official January ZIP, SHA-256 `0a2e81eacd7bf3890712de8f2a1b56bda985c17d9e61b08acf5e7c7ec9f20eb0`. No new download or sample substitution occurred. All CSV members were ingested. Gold ML retains the existing observation-table contract; the audit derives and verifies features without model training.

| Case | Incoming asset event → final successful DAG | Manifest staging → incoming event |
|---|---:|---:|
| Initial full month, including planned retry | **563.286353 s** | 32.142471 s |
| Duplicate, including pre-fix watcher recovery | **65.403415 s** | 79.595059 s |
| Later live replay, after fix, without watcher restart | **63.763310 s** | **13.105346 s** |

The initial watcher delay includes its missing-auth-mount launch correction. The duplicate's **79.595 seconds** includes the missed-notification investigation and watcher restart. These delays must not be presented as normal throughput. The post-fix live replay includes the 10-second reconciliation interval plus source checksum verification. Timing starts at the actual incoming event for the middle column; no source download is included.

Actual Spark subprocess times (including each fresh JVM's startup/teardown):

| Stage | Initial chain | Duplicate | Replay |
|---|---:|---:|---:|
| Bronze | 88.178 s, then 18.089 s retry | 15.272 s | 16.036 s |
| Silver | 325.517 s | 11.706 s | 11.817 s |
| Gold BI | 23.006 s | 11.289 s | 11.625 s |
| Gold ML | 30.275 s | 11.650 s | 11.910 s |

The standalone baseline reused one Spark session; these asset tasks start independent subprocesses. The initial case also includes a 45-second configured retry delay and scheduler/API/task overhead. This is a correctness run, not a controlled comparative performance benchmark.

Docker still had **3,943,489,536 bytes (3.673 GiB)** allocated. Across all orchestration jobs, worker peak sampled working set was **1,744,142,336 bytes (1.624 GiB)**; exact kernel peak including charged file cache was **1,985,646,592 bytes (1.849 GiB)**. The 2 GiB cap was never exceeded; memory-limit events, OOM events and OOM kills were all **0**. Sampling interval: 0.5 seconds. The worker's 949.998-second lifetime also includes idle preparation, investigation and gaps between cases; it is not ETL processing time.

The independent final Spark audit took **58.330 seconds**, exited 0, and peaked at **1,171,894,272 charged bytes**, with no memory-limit/OOM events. These are worker-container measurements, not total host RAM. The dedicated watcher's final-process cgroup peak and 192 MiB limit are recorded separately in the portable evidence.

Airflow's event records confirm each incoming event created one Bronze DAG, each successful Bronze outlet created Silver, and each Silver outlet created both Gold DAGs. Gold outlets created **no** downstream training/scoring run. All 12 runs have `run_type=asset_triggered` and `state=success`. Each asset has exactly three successful event records. The first Bronze run used attempt 2; every later task succeeded on attempt 1.

At the planned failure checkpoint, Bronze already contained **1,888,085 rows** and the ledger **one row**, but Bronze outlet events and downstream runs were both **zero**. Retry reused the original canonical ingestion ID and left both table snapshots/metadata hashes unchanged. Duplicate and replay kept **all six tables' metadata hashes, snapshot IDs, schemas and counts identical**. Their logs report idempotent/replay decisions; no additional normalization, Silver candidate write, quarantine append or Gold overwrite occurred. Fresh Spark reads confirm no duplicate published keys.

## Failures, fixes and regression results

- **Missed bind-mount notifications, reproduced:** a new atomic manifest rename remained undiscovered by the live native observer for more than **55.4 seconds**. Merely handling move events did not solve Docker Desktop's missing cross-container notifications. The daemon now reconciles the watched directory every 10 seconds as well as processing native events and known pending retries. The later live replay was delivered in 13.105346 seconds with an unchanged watcher process start time.
- Added `on_moved` handling so native atomic-rename notifications are also accepted.
- Duplicate manifest receipts now get unique archive names instead of overwriting the original receipt. All three receipts remain available.
- Added a consumer-DAG environment override for the isolated watcher; production defaults remain unchanged and an empty override fails closed.
- The first watcher launch lacked its auth-file mount and emitted no event. A read-only mount fixed the launch. The failed container is retained, not hidden or deleted.
- The Bronze failure was deliberate and recovered automatically. No unexpected Spark job failure occurred.
- The test observer now requires the expected new run count, avoiding premature completion from earlier successful deliveries.
- Final lifecycle check found that the PID-1 watcher ignored Docker's SIGTERM and was force-stopped with exit 137, **not** OOM. A SIGTERM handler now uses the existing Ctrl+C observer-cleanup path. An idle restart/stop test after all deliveries exited **0**, emitted no extra events, and left data unchanged; the pre-fix stop evidence is retained.

Final regression suite: **98 passed, 2 skipped, 0 failures/errors** (17 dependency warnings). Skips are host Airflow imports and opt-in Spark readiness; real container DAG imports, asset scheduling and Spark/Iceberg execution were exercised here. Added tests cover isolated worker restrictions, saved-response reuse, atomic renames, missed-notification reconciliation, duplicate receipt preservation, consumer overrides, observer completion and SIGTERM cleanup. Compilation, dependency consistency, Compose configuration and whitespace checks pass.

Original source/model/cache fingerprints and six prior sample-table snapshots match before/after. **All 93 files** in the prior complete-month warehouse also retain identical SHA-256 hashes; the original complete archive checksum is unchanged. Airflow metadata, scheduler and DAG processor remain healthy, with **zero DAG import errors**. Production ingestion/scoring DAGs stayed paused and had no new runs during this validation. No datasets, checkpoints, tables or volumes were deleted; no commits or pushes occurred.

## Isolation and resource contract

The dedicated DAGs are `validation_fullmonth_bronze`, `validation_fullmonth_silver`, `validation_fullmonth_bi`, and `validation_fullmonth_ml`. They consume only `validation://citibike/fullmonth-events-01/…` assets. Both Gold branches depend on Silver, as in the existing architecture; the one-slot `citibike_spark` pool serializes their processing.

The catalog is `citibike_fullmonth_events`, with a new warehouse under `artifacts/fullmonth-orchestration-01/warehouse`. The watcher observes only that run's `incoming/` directory. The original archive/manifest and baseline warehouse are retained. Production ingestion/scoring DAGs remain paused. No training or scoring DAG consumes these validation assets.

Airflow schedules genuine tasks, but a validation-only file transport sends their existing subprocess commands to a separate Docker worker. This keeps Spark's **2 GiB hard limit** separate from the scheduler's memory. The worker permits only the four medallion ETL modules, the isolated config/source directory, `local[1]`, and a **1 GiB driver heap**. It runs commands without a shell and has no network access or Docker socket. This is test transport, not a replacement for the application's asset orchestration or a distributed production executor.

The watcher is a separate 192 MiB-capped container on the existing internal Docker network. Its consumer-DAG environment override targets only the validation DAGs; defaults for the production watcher remain unchanged. Authentication is read from the existing file through a read-only mount and is never written into the report.

## Cases and evidence

1. **Initial delivery:** an atomically renamed non-sample manifest enters the watched directory. The watcher validates size/checksum and consumer readiness, posts the incoming asset event, and archives its receipt. Airflow must schedule all four DAGs from asset events, not manual DAG triggers.
2. **Automatic retry:** the first Bronze attempt deliberately fails after its successful Iceberg commit but before publishing its outlet. While `up_for_retry`, no Bronze outlet/Silver/Gold run may exist. Attempt 2 must reuse the canonical Bronze ingestion ID and keep its table/ledger snapshots unchanged.
3. **Duplicate delivery and later replay:** deliver the same month/checksum again through the watcher. Compare all six tables' metadata hashes, snapshots, schemas and row counts with the completed initial chain. Job logs must show replay/idempotent decisions, not another normalization, append, candidate write or Gold overwrite.
4. **Final independent audit:** reread the event-produced tables through Spark and compare all reconciliation and feature metrics with the completed full-month baseline, including the 13 post-warmup features. No model fitting is involved.

Detailed ignored artifacts are under `artifacts/fullmonth-orchestration-01/`: queue requests/results and per-job memory CSVs, `worker-memory.csv`, observer timelines, archived manifests, Airflow event/task status snapshots, preservation snapshots and the final audit. `.processed` acknowledges only successful event delivery, not downstream ETL completion.

## Controls for the prepared local run

The dedicated root and asset namespace are fixed in `tools/full_month_events.py` and `dags/validation_fullmonth_events.py`, matching the earlier repository validation fixtures. Setup refuses an existing run directory. Do not rerun setup over retained evidence or point this DAG set at a production warehouse.

```powershell
# Inspect existing real DAG/task/event results; does not trigger processing.
docker exec citibike_airflow_scheduler python -m tools.full_month_events status --label manual-inspection
docker logs citibike_fullmonth_event_worker
docker logs citibike_fullmonth_watcher_02
Get-Content artifacts/fullmonth-orchestration-01/audit.json

# Regression tests
.\.venv\Scripts\python.exe -m pytest -q
```

To run this fixture from a clean checkout with the existing verified archive/baseline and stack, execute `python -m tools.full_month_events setup` **inside the scheduler container**, then activate its dedicated DAGs with the `activate` action. Start the capped worker and watcher with the commands recorded below before using `drop --label initial`. Do not run `activate`/`drop` if you only want to inspect results.

The watcher requires the small `watchdog` package; this run installed version 4.0.2 only into ignored `watcher-deps/`, leaving Airflow's installed environment unchanged.

```powershell
$repoPath = (Get-Location).Path
docker exec citibike_airflow_scheduler python -m tools.full_month_events setup
docker exec citibike_airflow_scheduler python -m tools.full_month_events activate
docker run --name citibike_fullmonth_watcher_setup --network nyc-bike-mlops_platform_network --entrypoint python --memory=192m --memory-swap=192m --mount "type=bind,source=$repoPath,target=/opt/airflow" --workdir /opt/airflow citibike-airflow-spark-iceberg:3.2.2-python3.11 -m pip install --no-cache-dir --no-deps --target artifacts/fullmonth-orchestration-01/watcher-deps watchdog==4.0.2
docker run -d --name citibike_fullmonth_event_worker --hostname localhost --network none --entrypoint python --cpus=2 --memory=2g --memory-swap=2g --mount "type=bind,source=$repoPath,target=/opt/airflow" --workdir /opt/airflow -e PYTHONPATH=/opt/airflow -e SPARK_LOCAL_IP=127.0.0.1 -e "JAVA_TOOL_OPTIONS=-Duser.home=/home/airflow -XX:ActiveProcessorCount=2" citibike-airflow-spark-iceberg:3.2.2-python3.11 -m tools.full_month_queue --root /opt/airflow/artifacts/fullmonth-orchestration-01
docker run -d --name citibike_fullmonth_watcher_02 --network nyc-bike-mlops_platform_network --entrypoint python --memory=192m --memory-swap=192m --mount "type=bind,source=$repoPath,target=/opt/airflow" --mount "type=bind,source=$repoPath/airflow_auth,target=/opt/airflow/auth,readonly" --workdir /opt/airflow -e PYTHONPATH=/opt/airflow/artifacts/fullmonth-orchestration-01/watcher-deps:/opt/airflow citibike-airflow-spark-iceberg:3.2.2-python3.11 -m tools.full_month_events watcher
docker exec citibike_airflow_scheduler python -m tools.full_month_events drop --label initial
docker exec -d citibike_airflow_scheduler python -m tools.full_month_events observe --label initial
```

For duplicate/replay cases, wait until the prior chain has completely succeeded, save a status snapshot, then use `drop --label duplicate` or `drop --label replay`. Run `observe --label duplicate --expected-runs 2` or `observe --label replay --expected-runs 3` to wait for new runs rather than accepting previous successes. Each action only stages a manifest; the watcher supplies the actual event. Allow consumers to remain active until all events finish. The validation-only planned failure flag is present only on the initial manifest.

After validation, pause these dedicated DAGs (`python -m tools.full_month_events pause` inside the scheduler), stop the dedicated watcher, and request graceful worker shutdown by creating `artifacts/fullmonth-orchestration-01/stop-worker`. Retain evidence and all data; do not remove volumes or reset runtime state.

## Verdict and remaining scope

**Full-month orchestration is verified for the tested isolated January configuration. No blocker remains for this scope.** The watcher fix resolves the actual delivery stall found on this Docker Desktop bind mount. A healthy `.processed` receipt still does not replace DAG completion checks, and delivery is not an exactly-once transaction across the filesystem and Airflow API; idempotent publication handles repeated delivery.

The original production DAG IDs/default subprocess deployment were deliberately not activated on the complete archive. The isolated DAGs share the same lineage/ETL logic but use a capped validation transport. This does not certify unrestricted production deployment, concurrent full-month batches, long outages/operator-pause races, or full-month model quality and prediction serving. Next: separately authorize and validate lightweight chronological model evaluation and full-month scoring/cache/API/dashboard behavior. No training or scoring was performed in this pipeline validation.
