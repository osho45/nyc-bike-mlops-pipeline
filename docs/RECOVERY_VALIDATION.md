# Browser, recovery and asset-event validation

Date: 2026-10-27, America/Los_Angeles. **Result: passed for the bounded local sample.** This extends [DOCKER_VALIDATION.md](DOCKER_VALIDATION.md); no full-month ingestion or expensive model training occurred.

> The remaining multiweek, distinct-month, version-ordering and crash/restore checks have since passed. See [CORRECTNESS_RECOVERY.md](CORRECTNESS_RECOVERY.md). This file records the earlier phase.

## Actual browser verification

Headless Chromium controlled through Playwright 1.55.0 opened the real running Streamlit application at `http://127.0.0.1:8503/`. It used the PostgreSQL-backed FastAPI service at `http://127.0.0.1:8011/`. There were no HTTP mocks.

Verified:

- The rendered predicted class matches the actual `/predictions/6140.05` API response.
- All History, 7 Days, 10 Days and 14 Days controls rerender both Plotly charts.
- Four metric cards, two charts and the validation-only warning are present.
- Unknown station input shows the empty-data warning; restoring `6140.05` recovers the prediction and charts.
- Desktop (1440 x 1080) and mobile (390 x 844) rendering; genuine screenshots saved.
- Zero browser page errors, console errors, failed network requests or HTTP error responses during the final check.
- Correct single-hour axis labels, visible hourly bars and visible cumulative markers.

Evidence is saved locally in ignored `artifacts/browser-verified-01/`: `browser.json`, `desktop.png`, `charts.png`, `mobile.png`. An earlier screenshot exposed sub-millisecond ticks and invisible one-point cumulative lines. The dashboard now gives single observations a one-hour axis, a 45-minute bar width and visible markers. Multiple-observation charts retain their existing behavior/layout. Prediction values and thresholds were not changed.

**Limit:** only one smoke prediction exists. Filter interactions and rerendering are verified; different multiweek membership/boundary results are not demonstrated by this one-row fixture. Sample counts and the old smoke model are not real-world demand accuracy or financial evidence.

## Restart, outage and persistence tests

Restarted the operational PostgreSQL service, Airflow metadata PostgreSQL, Airflow API, scheduler, DAG processor, original API/UI and isolated sample API/UI. No `down -v`, volume removal, dataset deletion or checkpoint replacement was used.

Before/after equality checks passed for:

- Genuine source CSV SHA-256 and existing model/metadata file hashes.
- All six original sample Iceberg tables: metadata SHA-256, snapshot IDs and actual row counts.
- PostgreSQL prediction row counts and content fingerprints in the default and sample-03 databases.
- The pre-existing successful `validation_citibike_sample` / `bounded-sample-03` Airflow run.

A fresh Spark JVM read the persisted tables after restart:

| Table | Rows |
|---|---:|
| Bronze rides | 20,000 |
| Silver rides | 19,986 |
| Quarantine | 14 |
| Gold BI station/hour | 17,794 |
| Gold ML station/hour | 546,481 |
| Predictions | 1 |

The initial database restart exposed an actual API defect: first readiness request returned **500**, followed by recovery. The pool contained a stale socket, and rollback during cleanup raised `InterfaceError`, masking the intended 503.

Fix: validate borrowed sockets before use, discard stale connections, retry acquisition once, return 503 on pool exhaustion/unavailability, and prevent cleanup from masking request errors. Pool creation is guarded against concurrent initialization.

After deploying the fix:

- First five readiness requests after a database restart: **200, 200, 200, 200, 200**.
- During an actual stopped-database interval: `/health` **200**, `/ready` **503**.
- After PostgreSQL startup: readiness **200** automatically and the original prediction remained present.
- Regression tests cover stale sockets, failed-query cleanup and pool exhaustion.

Evidence: ignored `artifacts/recovery-before.json`, `recovery-verified.json`, `recovery-api-restart.json`, `recovery-database-outage.json`, `recovery-spark-read.json`; portable fingerprints in [audit/recovery_persistence.json](audit/recovery_persistence.json).

These were graceful service restarts and outages, not abrupt host power loss, corrupt storage or recovery from deleted Docker volumes.

## Live asset-event dependencies, retries and duplicates

Added a strictly isolated validation asset chain:

```text
validation incoming -> validation_asset_bronze -> validation_asset_silver
                                                -> validation_asset_bi
                                                -> validation_asset_ml
```

Production DAGs and the validation DAGs now use the same `jobs/asset_lineage.py` normalization and job-invocation logic. Production asset URIs, DAG IDs, task IDs, medallion jobs and default storage/configuration remain intact. Validation uses only the existing 20,000-row CSV, `citibike_validation` catalogue and `artifacts/asset-validation-01/warehouse`. No new training pipeline or champion was produced.

### Findings and fixes

1. **Coalesced/duplicate events:** production DAGs previously refused all multi-event batches. Shared handling now deduplicates identical year/month/SHA-256 events, processes every distinct source, and propagates either the original single-lineage contract or an explicit `lineages` batch. Per-source run IDs avoid sharing one Bronze ingestion ID across a batch. Canonical committed run IDs are reused after retries.
2. **Conflicting data versions:** two different checksums for one month in the same batch fail safely rather than picking an arbitrary overwrite order. Missing/malformed lineage and conflicting canonical IDs also fail. Cross-run corrected-archive ordering remains an operator concern.
3. **Retries and resource control:** production Spark tasks have bounded retries. A one-slot `citibike_spark` Airflow pool serializes Spark jobs within the 3.673 GiB Docker budget; fresh-stack initialization creates it.
4. **Paused-consumer event loss:** the live Airflow 3.2.2 API recorded two events while their consumer DAG was paused, but created no scheduling-queue entry. The watcher now retains manifests for retry unless all required ingestion consumers are active and available; it does not archive/acknowledge them in that case. This gate has regression coverage. It cannot remove a race where an operator pauses a consumer after the preflight check; explicit replay/monitoring remains necessary.

### Live test sequence and observed outcomes

- Kept every production DAG paused throughout.
- Enabled the four isolated consumers. Stopped the idle scheduler, posted two identical genuine-source events, and confirmed **one durable queue entry** before starting the scheduler again. Pausing the DAG itself is not a valid way to queue these test events in this Airflow version.
- Bronze committed 20,000 rows, then deliberately raised a validation-only exception **before publishing its outlet event**.
- Captured `up_for_retry` on attempt 1 with **zero Bronze outlet events and no Silver/Gold runs**. This proves a failed producer did not advance dependencies.
- Bronze attempt 2 reused the canonical committed ingestion, succeeded without another append, and emitted its outlet event.
- Silver and both Gold consumers then completed automatically through asset dependencies; both fan-out jobs waited for Silver success and the one-slot Spark pool.
- Replayed two more identical incoming events with the scheduler temporarily stopped. A second complete asset-triggered run succeeded at every stage with no injected failure.
- Final history: **two successful runs per stage**; first Bronze run has two attempts, second has one. Outlet counts are two each. Six incoming events are recorded in total: two initially posted to the paused consumer, then the two queued pairs.

After duplicate replay:

| Invariant | Actual result |
|---|---:|
| Bronze rows / duplicate ride IDs | 20,000 / 0 |
| Bronze append snapshots | 1, unchanged |
| Ingestion ledger rows | 1 |
| Silver rows / duplicate ride IDs | 19,986 / 0 |
| Quarantine rows | 14 |
| Gold BI / Gold ML ride sums | 19,986 / 19,986 |
| Duplicate station/hour keys | 0 |
| Gold ML rows | 546,481 |

The complete before/after logical counts and Bronze snapshot ID are identical. Original sample-03 data and checkpoints are also unchanged. The intentionally failing attempt is retained in Airflow logs; it is expected evidence, not an unresolved failure.

**Coverage limit:** live tests replay the same January sample. Multiple distinct months are covered by unit tests of batching/lineage propagation, not a multi-month live ingestion. Full-month watcher delivery, corrected-archive ordering across runs and durable end-to-end source acknowledgment still need further work.

## Regression status and current services

**66 passed, 2 skipped, 0 failures/errors**, 17 third-party warnings. The skipped tests are host-only Airflow/Spark integration entry points; the actual Docker tests above exercised these runtimes. `compileall`, `pip check` and `git diff --check` passed. All **11 DAGs** are registered with **zero import errors**.

The four completed validation asset consumers are paused again. All five original production DAGs remain paused. Required services and the sample dashboard/API are left running. Older stateless test containers are retained stopped; Compose orphan warnings refer to this preservation, not failed services.

Portable combined evidence: [audit/browser_recovery_asset_summary.json](audit/browser_recovery_asset_summary.json). Detailed logs/results and real screenshots remain under ignored `artifacts/` and `logs/`.

## Reproducing the bounded checks

Browser tooling is optional and excluded from production dependencies:

```powershell
python -m pip install -r requirements-browser.txt
$env:PLAYWRIGHT_BROWSERS_PATH = "$PWD/artifacts/browser-cache"
python -m playwright install chromium
python -m tools.browser_validate --output artifacts/browser-new-check
python -m pytest -q
```

Use a new browser output directory. The browser helper requires the existing sample UI/API at 8503/8011 and the known sample station/prediction.

Inspect persistence and asset status:

```powershell
docker compose exec -T airflow-scheduler python -m tools.recovery_snapshot
docker compose exec -T airflow-scheduler python -m tools.asset_validation_control status
docker compose exec -T airflow-scheduler python -m tools.asset_validation_counts
```

For another duplicate replay, ensure the scheduler has **no active tasks**, then:

```powershell
docker compose stop airflow-scheduler
docker compose exec -T airflow-webserver python -m tools.asset_validation_control emit
docker compose start airflow-scheduler
docker compose exec -T airflow-scheduler python -m tools.asset_validation_control status
# After all four stages finish:
docker compose exec -T airflow-scheduler python -m tools.asset_validation_control pause
```

Setup has already been performed. `setup` deliberately refuses to overwrite an existing validation directory. `--fail-once` on `emit` deliberately injects a post-commit Bronze failure on attempt 1; use only for this isolated validation. These commands never unpause production DAGs or weaken production sample guards. Do not stop the scheduler while other work is active.

## Remaining blockers and next action

No blocker remains for the bounded local demonstration, browser checks, tested restart paths or same-source asset retries/duplicates. Review the dashboard at `http://127.0.0.1:8503/` and Airflow at `http://127.0.0.1:18080/`.

Next priorities are multiweek prediction/filter fixtures, stronger source-event acknowledgment/replay monitoring, corrected-version ordering, and crash/backup-restore testing. Complete-month ingestion, resource scaling and a real held-out model benchmark require separate authorization and execution. Existing sample/smoke results do not establish population demand accuracy or production readiness.

No datasets/checkpoints were deleted, no expensive training was run, and nothing was committed or pushed.
