# Data correctness, source revisions and crash/restore validation

Date: 2026-10-27, America/Los_Angeles. **All scoped bounded checks passed.** This supersedes the remaining-check list in [RECOVERY_VALIDATION.md](RECOVERY_VALIDATION.md).

> Subsequent controlled execution verified the complete January 2024 Spark/Iceberg ETL, feature audit and exact replay. See [FULL_MONTH_VALIDATION.md](FULL_MONTH_VALIDATION.md). Full-month readiness statements below describe the earlier bounded checkpoint; full-month watcher/Airflow delivery and model training remain outside that newer run too.

> A later [full-month orchestration validation](FULL_MONTH_ORCHESTRATION.md) also passed, including a real bind-mount watcher fix, automatic retry and duplicate/replay checks. Model training/scoring remain unvalidated on the full month.

## Scope and data provenance

- Reused the existing **20,000 genuine January 2024 Citi Bike rows**.
- Downloaded **2,000 genuine February 2024 rows** from the official `202402-citibike-tripdata.zip` with bounded HTTP Range requests: **1,048,890 transferred bytes**. No complete monthly archive was downloaded or ingested.
- Multiweek prediction records are **explicitly synthetic test fixtures**, not model outputs or accuracy results.
- Revision-2/3 sources are **controlled correction fixtures** derived from January by removing one/two accepted rides. They are not claimed to be official republished archives.
- All new data, revisions, database volumes, backups and crash workers are isolated from the existing sample and production data. Existing source/model/table fingerprints were verified unchanged.

## 1. Multiweek filters: actual before/after browser results

A 1,009-hour synthetic fixture spans 42 days and the New York November DST fall-back. PostgreSQL received timezone-aware NYC timestamps and stores distinct UTC hours. Playwright/Chromium exercised the running API and Streamlit services, without mocks.

| Selection | Preserved pre-fix runtime code | Fixed runtime |
|---|---:|---:|
| All History | 1,000 | **1,009** |
| 7 Days | 169 | **168** |
| 10 Days | 241 | **240** |
| 14 Days | 337 | **336** |

The old API silently limited history to its first 1,000 records. The old client included both ends of a seven-day interval, yielding 169 hourly observations. Both behaviors were reproduced in a real browser using the preserved pre-fix application files mounted into disposable baseline containers against only the synthetic database.

Fixes:

- The API defines trailing elapsed UTC windows as **`latest - N days < timestamp <= latest`**, anchored to the latest recorded prediction, not the current wall clock.
- Optional `lookback_days`, `before` and `through` parameters provide server-side filtering and stable keyset pagination. Responses retain existing fields and add `total_count`, `next_before`, the pinned latest timestamp and the exclusive cutoff.
- Streamlit fetches every required page, pins the anchor across pages and no longer applies a second inconsistent client cutoff. It fails visibly if the safe display budget is exceeded, rather than silently truncating All History.
- Unit tests insert a new prediction between page reads and verify the pinned earlier snapshot still returns the correct remaining records. Exact cutoff, invalid cursors, UTC/DST and deduplication cases pass.
- Synthetic fixtures have an explicit UI warning. Real-browser checks assert all displayed record counts and every chart trace's point count.

The existing genuine one-prediction dashboard also passed its complete desktop/mobile browser regression after deployment, with no console/page/network errors. Screenshots are genuine browser captures, stored locally in ignored `artifacts/correctness-01/browser-before/`, `browser-windows/` and `browser-original-sample/`.

## 2. Live distinct-month batching

The new isolated `validation_correctness_*` asset chain uses the same `jobs/asset_lineage.py` invoker and actual Spark jobs as production. The input batch deliberately arrived as **February, January, January again**. It was normalized, deduplicated and propagated through Bronze, Silver and both Gold asset consumers.

Observed initial publication:

| Source month | Input records | Accepted rides | Rejected |
|---|---:|---:|---:|
| January | 20,000 | 19,986 | 14 |
| February | 2,000 | 2,000 | 0 |

Both months reached both Gold layers, with ride sums matching Silver. January's duplicate did not cause another ingestion. The two source files have distinct canonical ingestion IDs; Silver now explicitly scopes reads by source year/month as well as run ID.

Four complete live asset runs per stage passed: initial two-month batch, revision 2, stale revision 1, and a coalesced current/old/current replay. All tasks succeeded. New correctness DAGs are paused again; all production DAGs remained paused.

Evidence: ignored `artifacts/correctness-01/batch-data.json`, `asset-runs-completed.json` and event/status JSON files. This is a live **bounded multi-month** check, not full monthly ingestion or a live filesystem-watcher transfer of complete archives.

## 3. Corrected-source ordering and idempotency

Source ordering previously depended on whichever event arrived last; an older replay could overwrite a corrected month. The new protocol uses explicit positive integer `source_revision` and predecessor `supersedes_sha256` values. Discovery/download time is not treated as source authority.

Rules:

- An initial month begins at revision 1.
- A changed publication must be exactly the next revision and name the currently published checksum as its predecessor.
- Replaying the same revision/checksum is a no-op.
- Older revisions are recorded as stale and cannot overwrite the active publication.
- Conflicting checksums for one revision, gaps or an invalid predecessor fail safely.
- Coalesced explicit correction chains are sorted and validated. Gold processes each affected month once using its latest committed Silver publication.

Revision/checksum fields are written **in the same atomic Silver partition commit as the data**. There is no separate publication-state ledger that can get ahead of the table after a crash. Gold stores the same lineage and skips already-published content. Per-month OS locks serialize participating jobs and are released by the kernel after process death. Bronze checksum/event verification and ledger repair support replay after a partial ingestion bookkeeping failure.

Actual outcomes:

- January revision 1: **19,986 accepted rides**.
- Controlled revision 2: **19,985**, with February still **2,000**.
- Replaying old revision 1, then a coalesced revision-2/revision-1/revision-2 batch, left **all inspected table metadata versions/snapshot IDs and counts exactly unchanged**.
- Controlled revision 3 after crash recovery: **19,984 January rides**, February still **2,000**.

Negative conflict/gap/predecessor cases are covered by regression tests. The live tests cover the ordered correction, stale replay and mixed old/current duplicate replay, not every possible concurrent external writer.

For future genuine corrections, the downloader accepts `--source-revision N --supersedes-sha256 HEX`. Revised files are saved as `YYYYMM-citibike-tripdata.rN.zip`, preserving the original archive. The watcher checks that the filename, manifest revision and predecessor contract agree. Existing legacy manifests default to revision 1. These are **operator-declared project revisions**; the official public archive does not supply this sequence. Missing predecessors are rejected for explicit operator replay, not silently guessed or buffered forever.

## 4. Controlled crashes and restoration

### PostgreSQL

A dedicated `citibike_correctness_postgres` container and named volume contain only fixtures. A committed recovery row and all 1,009 prediction-fixture rows were saved. A second insert was left uncommitted, then the database container was killed with **SIGKILL, exit 137**.

After startup, PostgreSQL logs show WAL redo/automatic recovery. The committed row survived, the uncommitted row was absent, and the prediction fingerprint exactly matched the pre-crash database. The existing API recovered automatically and returned the expected window counts.

A custom-format `pg_dump` was restored with `pg_restore --exit-on-error` into a **fresh `correctness_restored` database**, without cleaning or overwriting the original. Row counts and content fingerprints match exactly. A separate restored-data API returned HTTP 200 and **336** records for the 14-day window.

### Spark/Iceberg

Only disposable correctness workers were killed; the main Airflow/database services were not crashed in this phase.

1. A worker ingested revision 3 and paused **before** the atomic Silver publication. SIGKILL returned **137**. Silver and Gold retained their exact revision-2 snapshots; Bronze revision 3 remained available for resume.
2. A fresh worker acquired the automatically released lock, reused Bronze and published Silver revision 3, then paused **after** that commit. Another SIGKILL returned **137**. Silver contained complete revision 3; Gold still contained revision 2.
3. A normal fresh worker resumed. Silver reported replay and kept its committed snapshot unchanged; only the affected Gold month was brought to revision 3. The worker exited **0**. February stayed unchanged.

With writers quiesced, **286 warehouse files** were copied and checksum-verified. Restoration created a **separate copy**, preserving the original. Because Hadoop Iceberg manifests contain absolute paths, a disposable reader shadow-mounted the restored copy at the same logical warehouse path. A sentinel present only in the restored copy proves this was not accidentally reading the original.

Fresh Spark and PyIceberg reads from that restored mount matched the saved snapshots and data. Silver, Gold BI and Gold ML each reconcile to **21,984 current rides**. Backup corruption/repeated-restore refusal are regression-tested before any original path is modified. No dataset, volume or checkpoint was deleted.

Backups are under ignored `artifacts/correctness-01/`: `postgres.dump`, `warehouse-backup/`, `warehouse-restored/`, and `warehouse-backup-manifest.json`. Crash workers and earlier application containers are retained stopped for evidence.

## 5. Regression/integration results and failures resolved

**88 passed, 2 skipped, 0 failures/errors**, 17 dependency warnings. The skipped entry points require host Airflow/PySpark; actual Docker execution verified those runtime paths. Compilation, dependency consistency and Git whitespace checks pass. **15 DAGs** load with **zero import errors**.

Failures found/resolved or expected during the work:

- Reproduced silent history truncation and inclusive cutoff errors in the preserved UI/API; fixed and verified in both Chromium and API tests.
- Replaced arrival-order overwrites with explicit revision/predecessor checks, atomic lineage publication and replay guards.
- The two Spark SIGKILL exits and PostgreSQL crash are deliberate tests, not unresolved failures.
- An initial new test loader encountered the SQL file's UTF-8 BOM; the fixture loader now uses `utf-8-sig`, matching existing loaders.
- Docker's old untagged image digest was not launchable for a baseline. Preserved application files were extracted from the still-running pre-fix containers and mounted read-only into disposable baseline containers instead; the resulting before counts were measured, not inferred.
- A few early status snapshots were taken before Airflow finalized task states. Final history confirms four successful asset runs per stage; no success was assumed from those preliminary reads.

Portable combined evidence: [audit/correctness_recovery_summary.json](audit/correctness_recovery_summary.json). Detailed evidence and real screenshots remain locally under ignored `artifacts/correctness-01/` and `logs/`.

## Current interfaces and safe reproduction

- Genuine existing sample dashboard: `http://127.0.0.1:8503/` (data preserved).
- Clearly synthetic multiweek fixture dashboard: `http://127.0.0.1:8504/`.
- Fixture API: `http://127.0.0.1:8012/docs`; restored database API: `http://127.0.0.1:8013/docs`.
- Airflow: `http://127.0.0.1:18080/`.

```powershell
python -m pytest -q
$env:PLAYWRIGHT_BROWSERS_PATH = "$PWD/artifacts/browser-cache"
python -m tools.browser_windows --output artifacts/window-browser-next
python -m tools.browser_validate --output artifacts/genuine-browser-next
docker compose exec -T airflow-scheduler python -m tools.correctness_control status
docker compose exec -T airflow-scheduler python -m tools.correctness_engine inspect
```

Use new browser output paths. Setup/backup/restore tools intentionally refuse existing output targets. Do not rerun crash hooks against non-isolated configurations or stop services with active user work. Correctness fixture secrets are in an ignored local env file and were not printed or included in reports.

## Full-month readiness verdict

**Ready for a controlled first full-month ETL correctness validation in a fresh warehouse; not yet certified for full-month performance or production deployment.** No full-month execution has occurred, and no model benchmark is claimed.

A separate [full-month profile](../config/full_month_validation.yml) is prepared with a fresh catalogue/warehouse/model path, one Spark thread, a 1 GiB driver and unchanged quality gates. It rejects sample inputs and does not rewire the running production DAGs. The bounded tests used a 512 MiB driver; the proposed full-month resource profile is preparation, not a full-size memory/performance result.

Before the next authorized run:

1. Stop optional fixture services or increase Docker's current **3.673 GiB** memory budget. Keep a single Spark task active. Approximately 110 GiB free host disk was observed, but monitor actual archive/staging/warehouse growth.
2. Download one complete official archive into a new directory, validate its checksum/CRC and modern schema, then use a **new** isolated output path. Do not reuse sample CSVs as complete-month inputs.
3. Run only medallion ETL/reconciliation first. Example prepared command (not executed):

```powershell
docker compose exec -T --workdir /opt/airflow airflow-scheduler python -m tools.full_stack_check --config config/full_month_validation.yml --source data/citibike/full_month_validation/incoming/202401-citibike-tripdata.zip --year 2024 --month 1 --output artifacts/full-month-next
```

4. Evaluate full-month rejection reasons, missing-station behavior, station/hour coverage, fractional durations, source revision lineage, runtime and resource use before any expensive training.

Remaining limits: full-size throughput/memory, unquiesced/disaster-site backup restoration, host-wide power loss, full Airflow-metadata backup restoration, external writers bypassing this pipeline's locks, and model quality. Local backups were restored on the same host/logical path. Missing correction predecessors require operator action. Filesystem watcher delivery of a complete archive remains part of the next full-month check.

Original datasets/checkpoints and earlier test data were preserved. No expensive model training, commits or pushes occurred.
