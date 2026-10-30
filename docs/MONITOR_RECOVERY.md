# Host monitor recovery: July 2024 resume

## Confirmed cause

The coordinator stopped after six verified months, during July Silver processing. The host monitor **had not exited**: PID 19364 was alive, and its journal contained fresh readings immediately before and after the failure. Its old `Move-Item -Force` publication temporarily removed the heartbeat pathname across the Windows/Docker bind mount. The worker's single `exists()` check interpreted that gap as a missing monitor and terminated the job. July Silver exited with SIGTERM (`-15`), not an OOM; kernel `oom` and `oom_kill` counters were zero.

A bounded reproduction using the original publication method produced **33 missing-path reads and two invalid JSON reads**, alongside 1,674 successful reads over 600 publications. This was a publication/reader race, not exhausted host disk space.

## Changes and safeguards

- The PowerShell publisher now uses .NET atomic file replacement and writes an append-only journal before replacing the primary file. Transient publication errors are retained in `monitor-errors.jsonl` and retried on subsequent cycles.
- Readers retry transient reads and recover the latest valid journal record or in-process cached record. Records retain their original producer timestamp. **A heartbeat older than 120 seconds still stops ingestion**; malformed, future-dated and invalid-space readings are rejected.
- A repository-specific mutex prevents concurrent publishers. Producer ID, actual PID, sequence and lifecycle status identify the owner. Abandoned mutexes can be recovered.
- `start_ingestion_monitor.ps1` checks a matching fresh heartbeat before declaring startup successful. Existing processes are identified by their script command line before explicit restart; unrelated PIDs are never stopped. A live PID with a stale heartbeat is reported as unhealthy. Process logs are retained per attempt.
- The status command uses the same resilient heartbeat reader. Explicit resource-stop markers remain authoritative. New low-space readings cannot be masked by older cached high-space readings.
- The reserve remains **10 GiB plus five times the source archive bytes**, with the separate 10 GiB hard floor. Spark remains `local[1]`, 1 GiB heap, two CPU threads, **2 GiB hard cgroup memory with zero extra swap**, and one Spark pool slot. The worker samples memory every 0.5 seconds and checks guards during jobs every five seconds.

No stale timestamps are refreshed by readers, and no resource safeguard was disabled. A sustained monitor failure still deliberately stops the pipeline; Windows shutdown/sleep is not automatically overcome.

## Actual validation and resumed state

- Regression suite: **119 passed, 2 skipped**, 17 dependency warnings. Host Airflow/PySpark checks are skipped; actual Docker worker/Airflow execution was checked separately. Six new regressions exercise transient missing reads, journal fallback/partial writes, malformed JSON, stale producer expiry, low disk overriding cache and explicit stop flags.
- Fixed publisher stress test: **200 publications, 580 successful guard checks, zero false resource stops**. The journal recovered 91 checks; primary supplied 489. Advancing the test clock beyond the final producer timestamp by 121 seconds correctly rejected the expired heartbeat. A concurrent publisher was refused by the mutex in a separate fixture.
- Failed marker and old scripts/logs were preserved before acknowledging this specific false alarm. The old worker exited cleanly. Existing queue results and telemetry were archived by `--resume`.
- Airflow run **`controlled_multiyear_resume_02`**, coordinator task PID **7650**, explicitly logged `CHECKPOINT_REUSED` for January through June. Its daily cron remains paused to prevent competing runs; the authorized running task continues.
- New worker: **`citibike_controlled_worker_02`**. Host monitor: **PID 17800**, producer `1fc89517d0c14a3aacb81e86347f9018`. Watcher: `citibike_controlled_watcher_01`.
- July reused the checksum-verified **923,324,464-byte** archive, SHA-256 `fd5dccd7bd69c499f6d0f78e8b10e91316ff5bfabdf25b7fc0efc3007c27d414`, with zero download bytes this call. Resumed Bronze succeeded in **29.757 seconds** and returned `idempotent: true` with the original ingestion lineage; it did not append duplicate rides. July Silver is running at this report, not yet independently verified.
- Before/after-resume fingerprints are identical for **all six checkpoint files, six table UUIDs and 45 referenced data files** for January-June across Bronze, Silver, quarantine, both Gold layers and the ingestion ledger. No earlier month was reprocessed.
- A live Silver observation showed **101,462,740,992 bytes free**, heartbeat age **9.445 seconds**, sequence **15**, current worker memory **974,839,808 bytes**, charged peak **975,134,720 bytes**, a **2,147,483,648-byte cap**, and zero OOM/limit events. The memory log was less than one second old. These are timestamped observations, not final July peak measurements.

The coordinator continues sequentially with unchanged monthly audits and annual gates. July and later months are not declared complete here. All three year gates and the rest of the plan remain necessary before model-training readiness.

## Monitoring and recovery

From the project directory:

```powershell
.\.venv\Scripts\python.exe -m tools.ingestion_status
Get-Content artifacts/monthly-ingestion-01/resources/monitor-status.json
Get-Content artifacts/monthly-ingestion-01/worker-memory.csv -Tail 3
docker logs --tail 15 citibike_controlled_worker_02
```

Airflow UI: <http://127.0.0.1:18080/>. Live coordinator log:

```powershell
docker exec citibike_airflow_scheduler tail -n 15 /opt/airflow/logs/dag_id=monthly_citibike_downloads/run_id=controlled_multiyear_resume_02/task_id=discover_download_and_wait/attempt=1.log
```

Checked monitor startup is idempotent:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File tools/start_ingestion_monitor.ps1 -Repository $PWD.Path
# Explicit monitor recovery, only after inspecting the failure:
powershell.exe -NoProfile -ExecutionPolicy Bypass -File tools/start_ingestion_monitor.ps1 -Repository $PWD.Path -Restart
```

Restarting the monitor never clears resource-stop markers. Inspect and preserve them, verify actual resources and worker health, then acknowledge only a resolved cause before resuming a stopped coordinator. Do not launch another controller while the current one is running. Follow [controlled recovery instructions](CONTROLLED_INGESTION.md#resource-and-recovery-safeguards) if a worker also needs replacement.

Local evidence is retained under `artifacts/monthly-ingestion-01/monitor-recovery-01/`; original-race evidence is under `monitor-repro-01/`. These are generated/ignored artifacts. The portable [progress snapshot](audit/controlled_ingestion_progress.json) is timestamped; use the live status command for current readings. No data was deleted, no pipeline model training was run, and no commits or pushes were made.
