# Controlled multi-year ingestion — live run

**Completed update (October 28, 2026): all 33 months and three year gates are verified. Initial bounded ML results are documented in [ML_READINESS.md](ML_READINESS.md). The remainder of this page is the historical ingestion/monitor-recovery handoff.**

**Historical status:** The Airflow coordinator is processing the official archive plan sequentially. No model training or scoring is enabled. Check the live status rather than interpreting this document as a completed multi-year benchmark:

```powershell
.\.venv\Scripts\python.exe -m tools.ingestion_status
Get-Content artifacts/monthly-ingestion-01/controlled-run.json
Get-Content artifacts/monthly-ingestion-01/monthly-metrics.jsonl -Tail 5
docker logs --tail 20 citibike_controlled_worker_02
```

Airflow: <http://127.0.0.1:18080/>. Coordinator DAG: `monthly_citibike_downloads`; actual run: `controlled_multiyear_resume_02`. Its task runs in `citibike_airflow_scheduler` (recorded task PID **7650**). The worker is `citibike_controlled_worker_02`; watcher: `citibike_controlled_watcher_01`. The Windows resource monitor PID is recorded in `artifacts/monthly-ingestion-01/resources/monitor.pid` (17800 at recovery handoff). Do not launch a second controller/worker for this directory.

Cron scheduling is paused to prevent another batch from competing with the current one. **The already-running coordinator task continues** and activates the four downstream asset DAGs as needed. Closing the browser or this conversation does not stop these actual local processes. Docker/Windows shutdown or sleep can interrupt them; checkpoints and atomic Iceberg publications support recovery.

## Verified progress at monitor recovery

January-June 2024 are verified; July resumed after the heartbeat publication fix. See [monitor recovery evidence](MONITOR_RECOVERY.md). The portable [progress snapshot](audit/controlled_ingestion_progress.json) is timestamped; it is not automatically kept current after this report. Use the commands above for subsequent progress.

| Month | Raw records | Accepted rides | Quarantine | Gold BI rows | Gold ML rows |
|---|---:|---:|---:|---:|---:|
| January 2024 | 1,888,085 | 1,885,944 | 2,141 | 620,302 | 1,653,912 |
| February 2024 | 2,121,501 | 2,118,773 | 2,728 | 626,288 | 1,552,776 |
| March 2024 | 2,663,295 | 2,659,446 | 3,849 | 716,904 | 1,661,348 |
| April 2024 | 3,217,063 | 3,213,330 | 3,733 | 759,539 | 1,599,840 |
| May 2024 | 4,133,961 | 4,130,969 | 2,992 | 864,455 | 1,668,792 |
| June 2024 | 4,783,576 | 4,779,156 | 4,420 | 911,806 | 1,636,560 |

January's already verified Iceberg partitions were copied into the isolated monthly catalog in **109.334 worker seconds**, without redownloading, normalizing or repeating Silver transformations. The donor snapshot IDs had to match the prior independent full-month audit. Adoption is restart-safe per table; existing complete copies are counted/checked instead of appended again. The original warehouse is read-only. February's existing verified checkpoint was skipped in **0.183 seconds**, with no Spark work or download.

March's **520,746,126-byte** archive downloaded and passed origin-version, CRC/header and SHA-256 checks. Its checksum is `d1a58caa37a66bbe6477680f3fba283f6f638475855591a4a2e8b73dc4cf8470`. Total month processing/audit time was **762.840 seconds**. Worker times: Bronze **96.662**, Silver **422.906**, Gold BI **25.636**, Gold ML **22.692**, audit **94.760** seconds. The year plan contains 33 listed archives; the remaining months are **not** claimed verified.

March checks found zero duplicate ride IDs, zero duplicate station-hour keys and zero invalid values across the 13 post-warmup features. Quarantine reasons: missing start station **2,793**, invalid duration **818**, outside source month **238**. Gold counts reconcile to accepted rides. March's New York month spans **743 UTC hours** because of daylight saving time; **2,236 × 743 = 1,661,348** dense observations were checked. Monthly-only complete features: **1,285,700** rows. For the continuity sample (`6140.05`), all **743 March hours** have features using February context, with Spark/NumPy parity and an unchanged first-hour feature vector after current/future target mutation.

April also passed all source/layer/duplicate/feature checks. Its **629,567,797-byte** archive contains 3,217,063 records; total elapsed month time was **951.987 seconds**. Quarantine reasons were missing start station **2,504**, invalid duration **870**, outside month **359**. Monthly-only complete feature rows: **1,226,544**; the continuity sample retained all **720 April hours** with March context, parity and no current/future target leakage. The observed charged worker peak reached **2,081,546,240 bytes (about 1.939 GiB)**, still below 2 GiB with zero memory-limit/OOM events. No complete-year gate has passed yet.

## Coordinator validation before launch

The scheduled DAG was manually triggered as `bounded_coordinator_01` for a bounded dry run. It deliberately failed after a sandbox checkpoint and **succeeded on actual Airflow attempt 2**, restoring the earlier attempt's checkpoint. The task occupied `default_pool`, not the Spark pool. The test exercised:

- Sorting a reversed plan into month/year order and skipping completed January/February checkpoints.
- Interruption before March's simulated commit; no later month executed before recovery.
- Restoring the sandbox checkpoint, then resuming March before April.
- Blocking 2025 until the simulated 2024 gate succeeded; a failed year gate prevented entry to 2025.
- A resource-stop check preventing any simulated execution.

These are **control-flow simulations**, not synthetic data-quality or accuracy results: real downloads, data-table writes and asset deliveries were all **zero** in the dry test. Real Airflow metadata/task execution did occur. It validates the scheduled DAG entry point and retry behavior; it does not claim a wall-clock cron tick was awaited.

Final regression suite: **113 passed, 2 skipped**, zero failures/errors (17 dependency warnings). Host-only Airflow/PySpark checks are skipped; real Airflow execution and full Spark month processing were verified in Docker. The new tests cover ordered advancement, failed-year blocking, duplicate plans, missing/stale/low disk guards, resource-stop behavior and conservative training-readiness reporting.

Detailed proof is retained at `artifacts/monthly-ingestion-01/coordinator-preflight.json` and `coordinator-checks/bounded_coordinator_01/`. The full run refuses to begin without this verified preflight plus the earlier missing-month pilot. The existing daily cron remains paused; actual ingestion is the explicitly authorized current run.

## Year gates and leakage boundaries

`tools/multi_year.py` uses one controller lock and an ordered plan. It cannot begin the next year until `tools/year_audit.py` writes that year's verified report. Required 2024 and 2025 sources must each contain all 12 months; 2026 uses the official months listed when this run was planned. Available months are discovered from the official bucket, not guessed from the calendar.

Each year audit checks month-checkpoint/source hashes, all layer totals, ingestion-ledger uniqueness, accepted ride IDs across months and years, station-hour uniqueness and Gold/Silver reconciliation. It computes past-only features across monthly boundaries for all stations, checks finite values, and compares eligible-row counts with an independent contiguous-segment/gap oracle. Only observations through the audited year enter the computation. Valid prior-year context is allowed; future-year observations are excluded. A verified unchanged year report can be reused on coordinator recovery when its months/source hashes still match.

Every new month also retains independent source/layer audits and a previous-month feature-boundary/parity/mutation check. Source gaps restart warmup; no absent station-month is fabricated as active zero-demand history. Detailed month reports are `checkpoints/YYYY-MM.audit.json` and `checkpoints/YYYY-MM.json`; year reports will be `years/YYYY.json`.

**2024 remains training, 2025 validation, and 2026 held-out testing.** No model is fitted, selected, scored or tuned by these audits. Real test-year data is only ingested and checked for data integrity; no test-set performance metric is produced. Model training is **not ready yet**: no complete year gate has passed at this initial report, and remaining years still need ingestion/audit. The status helper reports readiness only after the entire plan and all three year gates complete.

## Resource and recovery safeguards

- Spark remains `local[1]`, 1 GiB driver heap, one-slot Spark pool, two CPU quota/Java processors and a **2 GiB hard worker cap with no additional swap**. No simultaneous monthly ETL jobs are dispatched by the single controller.
- The Windows monitor reads actual host-drive free space every 10 seconds. Docker's bind-mount filesystem free-space report is deliberately not used. Before each month, required free space is **10 GiB plus five times the archive's size** for source/staging/warehouse/spill headroom. A missing or >120-second-old host heartbeat fails closed.
- Downloads recheck resources during streaming and preserve partial/origin checkpoints on interruption. Before advancing, the controller waits for both Gold branches and a fresh audit; unresolved failures stop progression.
- During a Spark job, the worker checks resource guards every five seconds. A resource stop terminates the isolated subprocess group (including the JVM), with forced termination only if graceful termination fails. Iceberg's atomic snapshots preserve committed data; uncompleted stages are resumed/replayed after correction.
- Java heap OOM, kernel-killed jobs or a missing worker create retained resource/fatal markers. Resource failures bypass pointless stage retries and pause monthly consumers. The host monitor also detects worker-container disappearance, so clients do not wait silently on a dead worker.
- Monthly metrics record elapsed time, source/download bytes, quarantine/accepted counts, job count and sampled working-set peaks. Queue results preserve each stage's logs, duration and cgroup memory/events. Kernel peaks/events in per-job results are cumulative worker counters; sampled per-job working-set peaks are separate measurements. Model checkpoints/data are not deleted.

At the March checkpoint, observed charged worker peak was **2,033,324,032 bytes (about 1.894 GiB)** with **zero memory-limit, OOM or OOM-kill events**. Free host space remained above 100 GiB. These are observed running values, not a guarantee that larger months or annual sorts will fit. The controller must stop on unresolved limits rather than relax source-quality rules or silently increase the cap.

If ingestion stops, inspect `controlled-run.json`, `resource-stop.json`/`worker-fatal.json`, the failed monthly checkpoint, queue logs and the Airflow task state. Preserve failed partials and diagnostics. Correct the actual cause before acknowledging/archiving stop markers; do not erase them just to force progress. Restart the worker with `--resume` and a new container name to preserve telemetry/results. Resume an existing `up_for_retry` coordinator by unpausing it when safe; if its DAG run is failed, trigger a single new controlled run with `conf={"controlled_run":true}` after checking that no other coordinator is active. Completed months and unchanged year gates will be reused.

The documented resource-monitor script is `tools/monitor_ingestion.ps1`; its PID/stdout/stderr and host readings are under `resources/`. For a deliberate stop, pause/stop the coordinator first, allow any chosen in-flight publication to finish or be safely canceled, stop the monitor before deliberately removing its watched worker, then stop only the dedicated watcher/worker. Do not remove Docker volumes or touch original warehouses/model files.

## Remaining work

The current persistent run must finish the remaining 2024 months and its annual gate, then 2025 and its gate, then the available 2026 months and final gate. Actual failures/resource pressure may require investigation; no success is predeclared. Only after those checks pass should model training be separately authorized. No commits or pushes are made by this work.
