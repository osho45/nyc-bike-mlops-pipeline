# Controlled 384-station native LightGBM capacity validation

This stage reuses `artifacts/network-scaling-prep-02/stage_plans/384.json` unchanged. Station eligibility and selection use only 2024/2025 demand and seasonality. It is a resource/correctness experiment, not operational-policy certification. No 2026 records or outcomes are used.

## Scope and safeguards

Each phase runs in a separate container with two CPUs, a **2 GiB hard memory cap**, no extra swap and no network. The charged-memory stop threshold remains **1.7 GiB**. Kernel `memory.peak`, 20 ms samples, cgroup limit/OOM events and host disk heartbeat checks are retained. Construction, fitting and export each require the preceding completed phase to pass its integrity/resource admission.

The runner allows only `smoke`, `96`, `192` and `384`; 384 additionally requires the completed smoke and 192 artifacts. **Full-network execution remains disabled.** Fixed parameters: Poisson objective, 100 boosting rounds, 15 leaves, learning rate 0.05, 63 bins, seed 42, two threads. Validation uses training-derived bins; there is no search, recalibration or frozen-policy modification.

## Correctness and retention

- Per-station 2024/2025 histories must contain 17,544 unique, contiguous hourly observations. Station identity, targets and 1/2/24/168-hour lags are checked for every feature row against timestamp lookups and a one-station reference implementation.
- Feature matrices are float32 disk-backed files with immutable manifests, labels and row-key hashes. Full-population DataFrames are never loaded. At most one station history and bounded input/output batches are resident.
- Native construction verifies feature order, labels and training-only bin sampling. Validation explicitly references the training Dataset. Binary reload repeats row/label/order checks.
- Complete training and validation forecasts, keys, labels, matrices, native binaries, source/code hashes, training curves and checkpoints are retained locally. Batched exports are independently reconciled with native evaluation metrics.

## Artifacts and restart

New run: `artifacts/native-network-capacity-03/384/`. Prior capacity runs and protected models/policies remain unchanged. Each phase contains `result.json` and `memory.csv`; source snapshots and software versions are stored at run root. Iteration checkpoints are saved at rounds 20, 40, 60, 80 and 100.

Restart at the next uncompleted phase after checking the preceding result and container exit status. Existing phase directories are refused, never overwritten. Completed preparation and binary construction can be reused for subsequent phases. An interrupted fit preserves its iteration checkpoints, but automatic mid-fit continuation is not implemented; do not claim it resumes automatically. A failed or partial phase requires investigation and an explicitly separate output path rather than silent retries.

Use a unique container name for each phase and keep recorded code unchanged between phases:

```powershell
docker run -d --name citibike_native_384_prepare_01 --network none --cpus=2 --memory=2g --memory-swap=2g --entrypoint python --mount "type=bind,source=$($PWD.Path),target=/opt/airflow" --workdir /opt/airflow -e PYTHONPATH=/opt/airflow -e OMP_NUM_THREADS=2 -e OPENBLAS_NUM_THREADS=2 citibike-airflow-spark-iceberg:3.2.2-python3.11 -u -m tools.native_network_capacity --stage 384 --phase prepare --run /opt/airflow/artifacts/native-network-capacity-03/384 --smoke-run /opt/airflow/artifacts/native-network-capacity-01/smoke --previous-run /opt/airflow/artifacts/native-network-capacity-02/192
```

Run phases `prepare`, `construct`, `fit`, `export` sequentially, changing the container name and `--phase`. Never proceed after a failed phase. The command above records this execution; choose a **new** artifact path and names to reproduce it.

Read-only reconciliation and regressions:

```powershell
.\.venv\Scripts\python.exe -B -m tools.report_native_384
.\.venv\Scripts\python.exe -B -m pytest -q -k "not small_model_families_and_genuine_metrics"
```




## Full-network projection methodology

After successful reconciliation, estimate 1,898-station phase memory and runtime from the measured 96/192/384 stages. Use the larger of proportional scaling from 384 and extrapolation using the largest nonnegative pairwise slope, then add a **50% safety margin** because the target is almost five times larger. These are conservative estimates, not measured bounds or permission to override stop gates. Compare projected memory against both the **1.7 GiB stop gate** and **2 GiB hard cap**. Keep full-network training disabled regardless of the recommendation.

## Verified 384-station measurements

**3,308,544 training rows (2024), 3,363,840 validation rows (2025), 6,672,384 feature rows total.** Extraction scanned 38,820,660 development rows and selected 6,736,896 raw hourly observations. All 384 station histories passed target, identity, lag, feature order and cross-month continuity checks, including the 2024-to-2025 boundary.

| Phase | Work seconds | Kernel peak GiB | Prior projected GiB | Rows/second |
|---|---:|---:|---:|---:|
| Feature preparation | 900.954 | 0.198 | 0.242 | 7,406 |
| Native construction | 35.863 | 0.694 | 0.879 | 186,050 |
| Reload + fit/checkpoints | 60.173 | 0.408 | 0.511 | 54,984 |
| Batched export + retention | 88.308 | 0.320 | 0.399 | 75,558 |

Total phase work: **1085.298 seconds (18.09 minutes)**. First container start to final exit: **1162.97 seconds (19.38 minutes)**, including startup/admission gaps. Boosting alone: **33.491 seconds** for 100 rounds. Pure prediction: **12.680 seconds**, **526,213 rows/second**. Table rates include verification and I/O; fit rate counts training rows once per complete 100-round fit.

The maximum measured peak was **0.694 GiB**, **21.0% below the prior 0.879 GiB projection**. These are cgroup charged high-water measurements including file cache, not process RSS. Construction sampled anonymous/file/working-set peaks were **319.81/381.83/329.18 MiB**. Fitting-only charged/anonymous peaks were **417.50/396.87 MiB**. All four containers exited zero, with **zero max/OOM/OOM-kill events**.

The host disk journal contains **114 heartbeats** during execution; maximum observed gap **10.34 seconds**, minimum free space **32.11 GiB**. Thus recorded monitoring stayed within its 120-second freshness and 10 GiB reserve limits.

| Split | MAE (rides/hour) | RMSE | WAPE |
|---|---:|---:|---:|
| 2024 training | 1.245531 | 2.247936 | 0.544028 (54.40%) |
| 2025 validation | 1.257711 | 2.313769 | 0.538963 (53.90%) |

Saved prediction files independently reproduce MAE/RMSE/WAPE within relative tolerance 1e-12. Native evaluation MAE/RMSE agree with batched export within 1e-10. Full row-key/label hashes, all 14 ordered features, matrices, native binaries, source inputs and executed source snapshots passed verification. Validation sampled zero independent binning rows; native construction used 200,000 training samples and maximum 4,096-row batches.

Checkpoints at rounds 20/40/60/80/100 contain the expected tree counts; the final checkpoint exactly matches the saved model. Model SHA-256: `575ad72ccf6e7d8d21ad0278df62736e1c172c297a2bbfd55438773f83e05b05`. No new operational policy was calibrated or applied, so this capacity result supplies no operational-penalty or station/season guard certification. Forecast metrics across cohorts have different evaluation populations and do not demonstrate model-quality improvement.

Data retained locally: raw **68.77 MiB**, matrices **432.38 MiB**, native **98.87 MiB**, checkpoints **0.55 MiB**, forecasts **92.00 MiB**. All generated run artifacts remain ignored by Git.

## Measured scaling and full-network decision

| Phase | 96 seconds / GiB | 192 seconds / GiB | 384 seconds / GiB | 1,898 projected seconds / GiB |
|---|---:|---:|---:|---:|
| prepare | 219.45 / 0.183 | 490.91 / 0.186 | 900.95 / 0.198 | 7,774 / 1.467 |
| construct | 9.10 / 0.262 | 16.99 / 0.409 | 35.86 / 0.694 | 278 / 5.148 |
| fit | 15.13 / 0.194 | 28.63 / 0.266 | 60.17 / 0.408 | 464 / 3.024 |
| export | 23.03 / 0.172 | 43.08 / 0.221 | 88.31 / 0.320 | 668 / 2.374 |

**Do not authorize 1,898-station execution under the unchanged 2 GiB cap.** The conservative projected construction peak is **5.148 GiB**, exceeding both the 1.7 GiB stop gate and hard cap; fitting and export also project above the cap. The estimated phase-work total is **9,184 seconds (2.55 hours)** with the 50% margin, excluding startup/admission delays. Expected matrices contain **16,353,168 training** and **16,626,480 validation** rows. These are estimates, not measured full-network performance or confidence intervals.

Proportional extrapolation deliberately overcounts fixed preparation overhead; the largest observed slope protects against recent runtime variability. Cache ownership, allocator behavior, host load and filesystem throughput can change at larger scale. Only one run per cohort was measured. Full-network execution must remain disabled until memory use is reduced and demonstrated in a separately authorized intermediate stage, or a separately approved larger memory budget is validated on suitable hardware. Do not weaken stop gates.

## Tests, preservation and evidence

**167 passed, two skipped, three unrelated learner-fit tests deselected**, with 14 dependency warnings. The native adapter/stage-authorization/projection subset passed all eight tests. Real learner fitting occurred only in capped containers. The CLI rejects `--stage 1898`; full-network training is explicitly disabled in configuration.

All **37 protected artifact hashes** remained unchanged, including previous models, policies, fallback rules, checkpoints, predictions and selection manifests. No 2026 records/outcomes were accessed, no frozen policy or serving changes were made, and there were no deployments, commits or pushes.

See [portable machine-readable audit](audit/native_capacity_384.json). Local evidence is in `artifacts/native-network-capacity-03/`: `capacity_summary.json`, `container_execution.txt`, `monitor_continuity.json`, `data_volumes.json`, `tests_final.xml`, `full_network_disabled.txt`, and per-phase results/memory CSVs.
