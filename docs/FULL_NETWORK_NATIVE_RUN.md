# Full-network execution on the current host

This separate workflow is explicitly authorized for the existing 1,898-station cohort. It leaves the previous 2 GiB/4 GiB profiles, disabled planning stages, frozen policies and production serving unchanged. It reads only the 24 approved 2024/2025 Gold observation files in the existing transfer manifest; it does not open the mixed-year warehouse catalog or 2026 data.

## Work and safeguards

The controller runs sequential, isolated containers using the pinned existing image. Raw extraction now has a 384 MiB hard cap; feature preparation, sealing and exports retain a 768 MiB hard cap; native construction and fitting have a 1,536 MiB hard cap. All use two CPUs, no additional swap, an 85% container-memory stop gate and a read-only project mount. Only `artifacts/full-network-native-01` is writable. Existing services and data are preserved.

Raw extraction admission requires 640 MiB free host RAM (384 MiB cap plus 256 MiB reserve); its runtime host reserve is 256 MiB. The smaller cap is based on the previous 384-station raw extraction sampled peak of 178,110,464 bytes, and requires verification at full cohort size. Other phases retain admission of the worker cap plus 1 GiB host RAM: 1.75 GiB for feature preparation/exports and 2.5 GiB for construction/fitting. All phases require guest available RAM of the cap plus 512 MiB and 16 GiB free disk. Other runtime reserves remain 1 GiB host RAM, 512 MiB guest RAM and 10 GiB disk. Host samples are recorded every two seconds while working, every 30 seconds while waiting; worker memory is sampled every 20 milliseconds. A missing/stale heartbeat, resource breach, OOM event, nonzero worker exit or failed integrity check stops execution. Resource failures are not automatically retried.

The last prelaunch measurement had 930,959,360 bytes of Windows available RAM, 2,406,912,000 bytes of guest available RAM, 731,303,936 bytes of guest swap in use and 33,202,475,008 bytes free disk. Host RAM failed admission. Waiting for real RAM is independent of availability of the preferred 4 GiB environment. Close unrelated applications to free RAM; no further authorization is required for the controller to proceed after checks pass.

The workflow shards raw observations by station, generates and checks one station's features at a time, and reuses completed station outputs. It checks all 168-hour carries, lag values, streamed/reference feature parity, row identities, labels, file hashes and feature order. A virtual sequence reads immutable station matrices through one reusable 4,096-row buffer. This avoids a second full feature matrix; it is **not** an external-memory LightGBM learner. Native binned training data remain resident and may still exceed the cap. If that happens, the run stops rather than substituting station-specific models or sampling away training rows.

One global Poisson model uses the existing deterministic settings: 100 rounds, 15 leaves, learning rate 0.05, 63 bins and two threads. Training-only bin construction and deferred validation preserve the prior optimized protocol. Expected rows are 16,353,168 training and 16,626,480 validation. Each station has 8,616 training hours after the first 168-hour warmup, and 8,760 validation hours. Validation retains observed historical lag semantics; it is hourly conditional forecasting, not an autonomous year-long rollout.

## Start, monitor and resume (PowerShell, repository root)

For a foreground start, if no controller is already running:

```powershell
.\.venv\Scripts\python.exe -S -u -m tools.full_network_supervisor --fail-fast
```

The launched background controller's PID is in `artifacts/full-network-native-01/controller-lease.json` and `launch.json`. Do not start a second controller.

```powershell
Get-Content artifacts/full-network-native-01/status.json
Get-Content artifacts/full-network-native-01/resources.json
Get-Content artifacts/full-network-native-01/supervisor.stdout.log -Tail 15 -Wait
docker ps --filter label=com.citibike.full-native=1
```

For live worker progress, use `docker logs --follow <container-name>` with the name in `status.json`. Per-job logs are saved after worker exit. Per-attempt `memory.csv`, container state and result records retain runtime, peak memory and OOM evidence. `resources.jsonl` retains host/guest/disk history.

After an ordinary interruption, first inspect the controller lease and any owned Docker containers. Do not resume while either is running:

```powershell
.\.venv\Scripts\python.exe -S -u -m tools.full_network_supervisor --resume --fail-fast
```

Completed phases and station matrices are reused. Incomplete attempts are retained and a new attempt is created without overwrite. Fit checkpoints are saved every ten rounds, but **mid-fit iteration resumption is not implemented**: an interrupted fit restarts its 100 rounds from the retained native binary in a new attempt. A `resource-stop.json` requires investigation; do not blindly remove it or repeatedly retry an OOM. A source/config hash mismatch also blocks resumption.

## Deliverables and current verification

Successful execution creates a model, ten-round checkpoints, training curves, all-round validation curves, complete training/validation prediction Parquets, MAE/RMSE/WAPE, canonical row-key hashes, source/config/image hashes and `final_report.json`/`final_report.md`. The operational decision policy is not recalibrated or applied by this forecast-training workflow. No full-network metrics exist until execution completes.

Local regression run: **199 passed, 2 skipped, 3 deselected**. The excluded three tests are the existing `small_model_families_and_genuine_metrics` learner cases; they were not run under the current host memory shortage. Five new tests cover cross-station reader boundaries, independent native samples, bounded reads, truncation, development-only month/station scheduling and actual-RAM admission. Full-cohort native construction/fitting remains an execution-time capacity check, not a pre-verified success.

Datasets, original checkpoints and 114 protected artifact hashes are preserved. No 2026 evaluation, deployment, commits or pushes are performed.

## Raw admission revision 01

The idle original controller PID 21088 was stopped with no workers active. Its source, contract and status were preserved under `admission-revision-01/`; `revision.json` records old/new contract hashes and the three changed source/config files. All 114 protected artifacts remained unchanged. The restart uses `--fail-fast`: insufficient admission produces `blocked_resources` and exits, including at later phases, instead of waiting indefinitely. Runtime/OOM failures still stop and require investigation.

Revision tests: **201 passed, 2 skipped, 3 deselected**. Seven focused tests include reduced raw admission boundaries and fail-fast exit without starting a worker or leaving a stale lease. Original prelaunch measurements above are historical; use `resources.json` and `status.json` for current readings. Restart logs are `supervisor-revision-01.stdout.log` and `supervisor-revision-01.stderr.log`.

### Verified heartbeat defect and recovery

The first reduced-cap trial started successfully and reached a sampled container peak of **148,635,648 bytes** (about 142 MiB), below the 326.4 MiB stop gate. It was interrupted by a Windows sharing violation replacing `resources.json`, not by OOM: controller traceback recorded WinError 5, and the worker subsequently exited 75 on stale heartbeat. Docker recorded `OOMKilled=false`. No completed raw-month marker had been published.

The controller now retries only `PermissionError` on atomic replacement for at most one second, well below the unchanged 30-second heartbeat deadline. Persistent errors still fail; memory and heartbeat protections are unchanged. The failed container was renamed and retained, the partial attempt was preserved, and the non-OOM failure was explicitly reviewed before resumption. `heartbeat-recovery-01/` contains the original error, container state, old source/contract, recovery record and tests. Updated tests: **203 passed, 2 skipped, 3 deselected**, including transient and persistent sharing-lock cases.

Latest restart logs are `supervisor-revision-02.stdout.log` and `supervisor-revision-02.stderr.log`. Refer to live status for the current PID and phase. A sampled partial-run peak does not prove every month or the native learner fits; those remain guarded execution-time checks.

### Completed full-cohort first-phase validation

After the repair, January 2024 raw extraction completed for all 1,898 stations: **1,412,112 rows**, **131.262 seconds**, kernel container peak **149,872,640 bytes (142.93 MiB)** under the 384 MiB cap. All memory `max`, `oom` and `oom_kill` counters were zero; source hash and row-count checks passed. The result is saved in `jobs/raw-2024-01/attempt-002/result.json`, with the reusable raw checkpoint in `raw/2024-01/done.json`. Controller PID 6332 automatically proceeded to February 2024 with one active worker.

The station-completion counter remains zero during raw extraction; it counts completed feature matrices, not raw station directories. This successful raw phase does not verify full-network construction/fitting capacity. Later admission limits are unchanged, and fail-fast operation will stop with an explicit resource blocker if they cannot be met.
