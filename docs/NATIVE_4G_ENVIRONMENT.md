> For the portable native-Linux/Windows controller, independent probe and transfer instructions, see [TRAINING_HOST_TRANSFER.md](TRAINING_HOST_TRANSFER.md). The measurements below describe the original resource-blocked host.

# Isolated 4 GiB training environment: prepared, resource-blocked

**No 4 GiB worker or benchmark was launched. Full-network execution is not authorized.** The current Docker/WSL VM has less total RAM than the proposed worker cap, and Windows physical-memory headroom is insufficient. A larger cgroup limit would not create more RAM.

## Actual resource evidence

Recorded sample: **2026-10-29T22:26:14.447088+00:00**. Free/available memory fluctuates; these are recorded measurements, not reservations.

| Resource | Measured | This profile's startup requirement |
|---|---:|---:|
| Windows usable physical RAM | 7.692 GiB | At least 12 GiB |
| Windows available physical RAM | 0.535 GiB | At least 6 GiB |
| Docker/WSL reported total RAM | 3.673 GiB | At least 6 GiB |
| Docker/WSL available RAM | 2.258 GiB | At least 5 GiB |
| Repository-volume free disk | 31.03 GiB | At least 10 GiB |
| Active 4 GiB training workers | 0 | At most one |

The 12/6/5 GiB admission requirements are conservative policy choices for this isolated profile, not universal LightGBM requirements. Six GiB free Windows RAM allows a full 4 GiB worker budget plus a 2 GiB host reserve without relying on paging or guest overcommit. Guest availability must separately cover the worker plus a 1 GiB VM reserve. Pagefile/swap capacity is not counted as physical headroom.

An initial CIM query needed elevated read access; the successful read and subsequent native Windows API samples agree on usable RAM. No host/WSL settings were changed. There is no user `.wslconfig` file. Existing production services remain running.

## Prepared configuration and worker

- [`docker-compose.native-4g.yml`](../docker-compose.native-4g.yml) is a **separate Compose project**, opt-in through `native-4g`. It does not merge into or change the production stack.
- [`config/native_training_4g.yml`](../config/native_training_4g.yml) records the isolated resource contract. Its thresholds mirror fail-closed code; editing the policy file alone cannot weaken enforced admission.
- Worker RAM is exactly **4 GiB**, CPU quota two, and RAM-plus-swap limit also 4 GiB, meaning no additional container swap allowance. Runtime stop threshold is **3.4 GiB (85%)**. A 64 MiB `/tmp` tmpfs counts toward worker memory.
- The repository and all existing artifacts are mounted **read-only**. Only `artifacts/native-4g-validation-01/` is writable. The root filesystem is read-only, networking is disabled and image pulls are forbidden.
- The local image identity must match `sha256:809353853ccd99ce12d5a4f359780f025f4932ac46fce3f065c8f57477c85c18` before admission. The verified local tag is used with `pull_policy: never`; an image configuration ID is not incorrectly used as a registry manifest digest.
- [`tools/native_4g_worker.py`](../tools/native_4g_worker.py) is restricted to the saved **384-station** source, 2024 training and 2025 validation. It exposes no 1,898-station option. It uses the optimized block-buffered constructor, label-cache release, fixed 100-round compact/deferred fits, checkpoints, deferred curve recovery and complete exports.
- Intended runtime comparisons assert original binary hashes, model/checkpoint hashes, every forecast file, row counts/order/labels, training-reference bins and validation curves. **Those 4 GiB runtime comparisons have not yet been executed.** The worker code is prepared and unit checked, not claimed to be benchmark-verified on this host.

The existing **2 GiB defaults and production services remain unchanged**. No global monkeypatch or relaxed legacy admission is used.

## Admission, monitoring and stop gates

[`tools/native_4g_preflight.py`](../tools/native_4g_preflight.py) reads Windows physical memory, Docker total RAM, guest available RAM from a read-only Linux probe, the image identity and actual Windows disk space. A nonzero preflight result means **do not launch a worker**. It never launches training, changes WSL, stops services or substitutes swap for RAM.

The continuous host monitor writes an atomic resource heartbeat and append-only journal. During a worker run, every two seconds the worker rechecks:

- heartbeat age at most **30 seconds**, valid producer identity and expected image;
- at least **2 GiB Windows physical RAM available** and **1 GiB guest available RAM**;
- at least **10 GiB disk free**, at most one training worker and no resource-stop marker;
- actual cgroup `memory.max=4 GiB`, `memory.swap.max=0` and CPU quota no greater than two.

The ordinary memory sampler retains its 20 ms sampling and 85% gate. Any resource violation stops the run; hard-kill/failure results must not be retried automatically. The monitor's displayed `ready/blocked` status describes **startup admission**; a running worker uses the lower runtime reserves above, so a blocked startup status does not itself mean an active worker violated runtime reserves.

An exclusive worker lease prevents overlapping phase work. A separate monitor lease prevents multiple heartbeat producers. Source/config/native-input hashes gate reuse of completed phases. All results are namespaced under `runs/<run-name>/` and existing phase directories are refused.

### Restart evidence and limits

Two actual bounded monitor sessions completed cleanly, each producing sequences **0,1**, with distinct producer IDs and increasing timestamps. Normal restart is verified. Unit tests also verify stale heartbeat rejection, exclusive leases, stale-lease refusal, source-independent completed-phase preservation and failure before model allocation.

A crashed/killed monitor or worker can leave its lease behind. **Do not clear leases/resource-stop markers automatically.** Confirm the owning process/container has stopped, inspect the recorded failure, restore resource headroom, then deliberately recover the generated control files. Partial experiment files and checkpoints remain intact. Use a fresh run name after a failed phase instead of overwriting it.

Completed construction can feed subsequent fit phases; completed fits can feed validation/export phases, with code and configuration unchanged. Automatic mid-fit continuation is still unsupported. **Actual 4 GiB kernel enforcement and real 4 GiB worker stop/restart have not been tested**, because hardware admission failed before a worker could be safely launched.

## Tests and status

| Check | Result |
|---|---|
| Host/Docker/WSL resource collection | Verified; admission correctly blocked |
| Compose rendering and isolation settings | Passed |
| Dedicated admission/restart tests | 10 passed |
| Full regression suite | 186 passed, 2 skipped, 3 unrelated learner-fit tests deselected |
| Continuous monitor normal restart | Verified with two bounded sessions |
| Existing defaults/artifacts | 114 protected hashes unchanged |
| Existing services | Same nine service names remain running; no change commands issued |
| 4 GiB construction/fitting/exports | **Not run: resource blocked** |
| 4 GiB model/checkpoint/forecast/runtime/peak comparisons | **Pending, no metrics fabricated** |
| 1,898-station execution | Disabled; not run |

There were 14 dependency warnings. Tests simulate eligible resources for gate logic; they do not imply the current machine passed admission or had a real 4 GiB worker. No datasets, models, checkpoints or user data were deleted. No 2026 data/outcomes were accessed, and no frozen policy, serving configuration, deployment, commit or push was made.

## Projection reassessment

No new 4 GiB performance measurements exist. The last verified **2 GiB** experiments projected, with a conservative 50% margin:

| Full-network phase | Historical projection |
|---|---:|
| Optimized construction | 2.553 GiB |
| Compact fitting, normal in-loop validation | 2.714 GiB |
| Fixed-budget deferred fitting | 2.108 GiB |

These estimates are mathematically below a 4 GiB worker's **3.4 GiB** stop threshold; the compact-fit estimate leaves about **0.686 GiB** below that threshold. **This is arithmetic feasibility only, not newly verified capacity or host headroom.** The 4 GiB benchmark harness, mount behavior, allocator behavior and runtime enforcement still need actual validation on an eligible machine.

**The 1,898-station run cannot safely be authorized on the current host.** Prefer a local machine with **16 GiB or more physical RAM** and sufficient free headroom. Docker must report at least 6 GiB actual total RAM and 5 GiB available; a nominal 6 GiB WSL setting can report less usable RAM, so verify actual values. Do not merely raise WSL's limit on this approximately 8 GiB laptop while keeping all workloads active. Any Docker/WSL restart or global-memory change requires a separately planned maintenance action; none was performed here.

## Exact next steps on an eligible host

1. Provision adequate physical RAM and Docker/WSL capacity without silently disrupting production services. Rerun admission. Stop if it fails.
2. Start the continuous host monitor, verify fresh heartbeats, then run the status check and bounded 384-station phases below.
3. Inspect actual cgroup limits, memory events, resource logs and hashes after **every** phase. Stop on any failure. Compare against saved baselines before authorizing a larger cohort.
4. Request separate full-network authorization only after real 4 GiB validation and host-headroom checks pass. This profile remains limited to 384.

Run from the repository root. The existing controlled worker is used only as a read-only `/proc/meminfo` probe; no service state is changed:

```powershell
.\.venv\Scripts\python.exe -B -m tools.native_4g_preflight --probe-container citibike_controlled_worker_02
if ($LASTEXITCODE -ne 0) { throw "Insufficient resources: do not launch training" }
```

On a separate host without that container, use an existing Linux container as the probe. A tiny dedicated probe can be provisioned independently if needed; it is not a training worker. Do not reference an absent container or guess available VM memory.

In a second terminal, **after admission passes**, keep this process running through all worker phases:

```powershell
.\.venv\Scripts\python.exe -B -m tools.native_4g_preflight --probe-container citibike_controlled_worker_02 --monitor --interval 5
```

Only on an eligible host with fresh monitoring, run these sequentially, checking each exit/result before the next. They are documentation for the pending benchmark, **not commands executed during this task**:

```powershell
docker compose -f docker-compose.native-4g.yml --profile native-4g run --no-deps --name citibike_4g_status01 trainer --task status
docker compose -f docker-compose.native-4g.yml --profile native-4g run --no-deps --name citibike_4g_construct01 trainer --task construct --run-name run01
docker compose -f docker-compose.native-4g.yml --profile native-4g run --no-deps --name citibike_4g_compact_fit01 trainer --task fit --variant compact --run-name run01
docker compose -f docker-compose.native-4g.yml --profile native-4g run --no-deps --name citibike_4g_compact_export01 trainer --task export --variant compact --run-name run01
docker compose -f docker-compose.native-4g.yml --profile native-4g run --no-deps --name citibike_4g_deferred_fit01 trainer --task fit --variant deferred --run-name run01
docker compose -f docker-compose.native-4g.yml --profile native-4g run --no-deps --name citibike_4g_deferred_curves01 trainer --task curves --variant deferred --run-name run01
docker compose -f docker-compose.native-4g.yml --profile native-4g run --no-deps --name citibike_4g_deferred_export01 trainer --task export --variant deferred --run-name run01
```

For a clean reattempt use new container names and a new `--run-name` such as `run02`; preserve all `run01` files. Keep code/configuration fixed across phases. The source repository stays read-only inside the worker, and new output stays inside the isolated artifact root.

Safe checks that do not launch training:

```powershell
docker compose -f docker-compose.native-4g.yml --profile native-4g config --format json
.\.venv\Scripts\python.exe -B -m pytest tests/test_native_4g_resources.py -q
```

See [machine-readable validation evidence](audit/native_4g_environment.json). Local evidence is in `artifacts/native-4g-validation-01/`: preflight snapshots, monitor journal/restart identities, rendered Compose JSON, empty 4 GiB container list, unchanged-service list, protected hashes, regression XML and `validation_summary.json`.
