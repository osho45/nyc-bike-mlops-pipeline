> Historical 192-station report. The later authorized 384-station execution is documented in [NATIVE_CAPACITY_384.md](NATIVE_CAPACITY_384.md); statements below about 384 being disabled describe the earlier stage.

# Controlled 192-station native LightGBM capacity validation

This stage reuses `artifacts/network-scaling-prep-02/stage_plans/192.json` unchanged. Station eligibility and selection use only 2024/2025 demand and seasonality. It is a resource/correctness experiment, not operational-policy certification. No 2026 records or outcomes are used.

## Scope and safeguards

Each phase runs in a separate container with two CPUs, a **2 GiB hard memory cap**, no extra swap and no network. The charged-memory stop threshold remains **1.7 GiB**. Kernel `memory.peak`, 20 ms samples, cgroup limit/OOM events and host disk heartbeat checks are retained. Construction, fitting and export each require the preceding completed phase to pass its integrity/resource admission.

The runner allows only `smoke`, `96` and `192`; 192 additionally requires the completed smoke and 96 artifacts. **384 and full-network execution remain disabled.** Fixed parameters: Poisson objective, 100 boosting rounds, 15 leaves, learning rate 0.05, 63 bins, seed 42, two threads. Validation uses training-derived bins; there is no search, recalibration or frozen-policy modification.

## Correctness and retention

- Per-station 2024/2025 histories must contain 17,544 unique, contiguous hourly observations. Station identity, targets and 1/2/24/168-hour lags are checked for every feature row against timestamp lookups and a one-station reference implementation.
- Feature matrices are float32 disk-backed files with immutable manifests, labels and row-key hashes. Full-population DataFrames are never loaded. At most one station history and bounded input/output batches are resident.
- Native construction verifies feature order, labels and training-only bin sampling. Validation explicitly references the training Dataset. Binary reload repeats row/label/order checks.
- Complete training and validation forecasts, keys, labels, matrices, native binaries, source/code hashes, training curves and checkpoints are retained locally. Batched exports are independently reconciled with native evaluation metrics.

## Artifacts and restart

New run: `artifacts/native-network-capacity-02/192/`. Prior capacity runs and protected models/policies remain unchanged. Each phase contains `result.json` and `memory.csv`; source snapshots and software versions are stored at run root. Iteration checkpoints are saved at rounds 20, 40, 60, 80 and 100.

Restart at the next uncompleted phase after checking the preceding result and container exit status. Existing phase directories are refused, never overwritten. Completed preparation and binary construction can be reused for subsequent phases. An interrupted fit preserves its iteration checkpoints, but automatic mid-fit continuation is not implemented; do not claim it resumes automatically. A failed or partial phase requires investigation and an explicitly separate output path rather than silent retries.

Use a unique container name for each phase and keep recorded code unchanged between phases:

```powershell
docker run -d --name citibike_native_192_prepare_01 --network none --cpus=2 --memory=2g --memory-swap=2g --entrypoint python --mount "type=bind,source=$($PWD.Path),target=/opt/airflow" --workdir /opt/airflow -e PYTHONPATH=/opt/airflow -e OMP_NUM_THREADS=2 -e OPENBLAS_NUM_THREADS=2 citibike-airflow-spark-iceberg:3.2.2-python3.11 -u -m tools.native_network_capacity --stage 192 --phase prepare --run /opt/airflow/artifacts/native-network-capacity-02/192 --smoke-run /opt/airflow/artifacts/native-network-capacity-01/smoke --previous-run /opt/airflow/artifacts/native-network-capacity-01/96
```

Run phases `prepare`, `construct`, `fit`, `export` sequentially, changing the container name and `--phase`. Never proceed after a failed phase. The command above records this execution; choose a **new** artifact path and names to reproduce it.

Read-only reconciliation and regressions:

```powershell
.\.venv\Scripts\python.exe -B -m tools.report_native_192
.\.venv\Scripts\python.exe -B -m pytest -q -k "not small_model_families_and_genuine_metrics"
```



## Verified measurements

**192 stations; 1,654,272 training rows (2024), 1,681,920 validation rows (2025), 3,336,192 feature rows total.** Source extraction scanned 38,820,660 development rows and selected 3,368,448 raw hourly observations. All 192 station histories passed continuity, identity, lag, target and feature checks.

| Phase | Work seconds | Kernel peak GiB | Prior projected GiB | Rows/second |
|---|---:|---:|---:|---:|
| Feature preparation | 490.913 | 0.186 | 0.239 | 6,796 |
| Native construction | 16.986 | 0.409 | 0.496 | 196,407 |
| Binary reload + fitting/checkpoints | 28.634 | 0.266 | 0.318 | 57,774 |
| Batched export + retention | 43.077 | 0.221 | 0.268 | 77,447 |

Phase work totals **579.610 seconds (9.66 minutes)**, excluding container startup and admission gaps. First container start to final exit: **11 minutes 24.75 seconds**, including those gaps. Boosting itself took **15.671 seconds** for 100 rounds. Pure batched prediction took **6.199 seconds**, **538,166 rows/second**. Table rates include each phase's verification and I/O; fit rate counts training rows once per complete 100-round fit, not per-tree processing.

The maximum measured container peak was **0.409 GiB (419.26 MiB)** during construction, **17.4% below the 0.496 GiB projection**. This is charged cgroup high-water memory, including file cache; it is not process RSS. Construction sampled working-set/anonymous peaks were approximately **228.36/220.44 MiB**; fitting-only charged/anonymous peaks were approximately **271.92/257.48 MiB**. There were **zero max/OOM/OOM-kill events in all four containers**, all exited zero, and host disk monitoring remained current.

| Split | MAE (rides/hour) | RMSE | WAPE |
|---|---:|---:|---:|
| 2024 training | 1.275294 | 2.307032 | 0.540501 (54.05%) |
| 2025 validation | 1.276772 | 2.363243 | 0.545169 (54.52%) |

MAE/RMSE agree with native LightGBM's final evaluation within relative tolerance 1e-10. Independent batched reads of both saved prediction files reproduce MAE/RMSE/WAPE within 1e-12 and verify complete row-key/label hashes. All 14 features retain identical order; native validation sampled zero independent binning rows and reused training-reference bins. Matrices, binaries, source inputs, retained predictions and protected pre-existing models/policies passed their hash checks.

Model SHA-256: `da977b71e6918c79193d6441d6328549327520468c150bc7841c2645410ebc68`. Checkpoints exist at rounds 20/40/60/80/100. Train Poisson objective decreased from 0.171485 to -1.776480 (negative values are valid for the constant-free objective). No operational decision policy was calibrated or scored, so operational penalty/guard certification is deliberately absent.

Data written: raw **34.42 MiB**, matrices **216.20 MiB**, native **49.44 MiB**, checkpoints **0.54 MiB**, forecasts **46.55 MiB**. Full forecasts and matrices remain local and ignored by Git.

## 384-station recommendation

A **separately authorized, controlled 384-station capacity stage is supported by these resource measurements**. Linear extrapolation of observed 96/192 peaks with an additional 25% margin estimates:

| Phase | Projected 384 peak GiB |
|---|---:|
| prepare | 0.242 |
| construct | 0.879 |
| fit | 0.511 |
| export | 0.399 |

The largest estimate is **0.879 GiB**, below the unchanged 1.7 GiB stop gate. It is a projection, not a memory guarantee: preserve one-job sequencing, phase isolation, disk checks and all correctness admissions. **384 and full-network stages remain disabled and were not run.** Operational quality, calibration and station/season guards require separate development-only validation before any deployment decision.

## Tests and evidence

**166 passed, two skipped, three unrelated learner-fit tests deselected**, 14 dependency warnings. The native adapter/authorization test subset separately passed all seven tests. Actual learner integration was exercised only inside capped containers. No failures occurred. The stage-authorization regression explicitly rejects 384/full-network stages.

See [portable machine-readable evidence](audit/native_capacity_192.json). Local execution details: `artifacts/native-network-capacity-02/container_execution.txt`, `capacity_summary.json`, `tests.xml`, `data_volumes.json`, and per-phase memory CSVs/results. Existing checkpoints, datasets, frozen policies and production serving were preserved. No 2026 inspection/scoring, deployment, commits or pushes occurred.
