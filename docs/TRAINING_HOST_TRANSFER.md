# Transfer and run native training on a separate 16 GiB+ host

## Scope and readiness

This is a portable **training-only working-tree snapshot**, not a clone of the production stack. It carries the exact uncommitted source needed by the native worker, immutable 384-station benchmark inputs/results, and an optional 2024/2025 Gold observation snapshot. It excludes credentials, Git history, virtualenvs, live leases/heartbeats, raw archives, Bronze/Silver/BI tables, production databases and 2026 data/outcomes.

The 4 GiB worker remains opt-in and **384-only**. No remote machine was provisioned, no image payload was exported, and no training was launched in this preparation task. A real new-host admission/benchmark result does not yet exist. The current laptop remains unsuitable for a 4 GiB worker.

**Important full-network gap:** no verified 1,898-station training entry point currently exists. The existing workers intentionally reject that size. The final section supplies an exact non-training readiness command and identifies the required executor generalization. It does not invent an unsupported training command or enable a full-network stage.

## Host and storage requirements

| Resource | Preparation target / gate |
|---|---|
| Architecture | Native Linux/amd64 execution; ARM emulation is not validated |
| Physical RAM | Recommend 16 GiB or more; admission minimum total 12 GiB |
| Host free RAM | At least 6 GiB at startup; 2 GiB runtime reserve |
| Docker/WSL RAM | Actual reported total at least 6 GiB, startup available at least 5 GiB; 1 GiB available runtime reserve |
| Worker | 4 GiB hard cap, no extra swap, 3.4 GiB stop gate |
| CPU | Two worker CPUs/threads; recommend at least four logical host CPUs to leave capacity for the OS/monitor |
| Disk | Recommend **40 GiB free** on Docker/data storage; never breach the 10 GiB runtime reserve |
| Platform | Docker Linux containers and cgroup v2; Compose plugin; host Python 3.11+ |

Host monitoring now supports **native Windows Python** and **native Linux Python** with stdlib-only dependencies. Windows checks use physical-memory APIs; native Linux reads `/proc/meminfo`. Running the controller inside WSL is rejected because guest memory is not Windows physical headroom. On Windows run it from native PowerShell, even if file transfer uses WSL/rsync.

The isolated profile has a 64 MiB read-only, networkless resource probe. It does not depend on the production Airflow/Spark worker. It does not start the production Compose stack. Receive files as the intended non-root operator and create the new writable output directory as that user. If a root-owned transfer prevents creation, review/fix ownership of only that new output directory; do not recursively change immutable datasets or run the model as root. Linux UID/GID can be supplied with `NATIVE_UID`/`NATIVE_GID`; Windows can keep the defaults.

Docker Engine/CLI **29.9.2**, Compose **5.6.1**, Linux/amd64 and cgroup v2 were observed locally. Install Docker/Python separately if absent, following [Docker's Linux installation instructions](https://docs.docker.com/engine/install/ubuntu/) or [Docker Desktop's WSL instructions](https://docs.docker.com/desktop/features/wsl/). No installation, WSL restart or service reconfiguration was performed here. Verify actual memory after setup; a nominal WSL cap is not a measurement.

## Exact data tiers and why they are small

1. **Benchmark tier:** approximately **624 MiB**. Includes original 384 `X.f32`, `y.f32`, keys/manifests, native binaries, model, five checkpoints, training curves, original complete forecasts and audit/provenance records. These are real files from `artifacts/native-network-capacity-03/384/`, not symlinks into prior experiments.
2. **Development-source tier:** approximately **20 MiB**, including the **19.34 MiB** Gold Parquet payload. Twenty-four explicit files contain **38,820,660** 2024/2025 station-hour observations. Monthly checkpoints, the development population profile and frozen 384/all-eligible manifests accompany them.
3. **Source/config/docs tier:** less than 1 MiB. AST-based dependency discovery includes current uncommitted modules without importing/executing them. Only approved configuration/docs are included; the unrelated full-project README and credentials are not required.
4. **Exact runtime image:** **8.83 GiB unpacked**, separate from the data manifest. The installed environment is recorded in [native_runtime_lock.json](../config/native_runtime_lock.json), including Python 3.11.15, NumPy 2.4.6, pandas 2.2.3, LightGBM 4.6.0 and PyArrow 19.0.1, plus the complete installed-package inventory.

The manifest records exact bytes and SHA-256 per file. The image dominates transfer/storage. Do not copy the whole warehouse, original ZIP/CSV archives, Docker volumes or all prior experiments. No duplicate download is needed. A source archive rebuild can change package resolution and image identity; transferring the verified image is the reproducible route. A smaller image would need its own trust/resource/model validation and is not silently substituted.

The automatic disk gate checks the project/output volume. Check the Docker image-store volume separately before loading an image, especially when Docker Desktop stores its VHD on another drive. On native Linux inspect `docker info --format '{{.DockerRootDir}}'` and available space on that filesystem as well as the project filesystem.

The 40 GiB recommendation covers an imported image (~9 GiB), a possible retained image archive (~9 GiB), benchmark/full-network working data (~5 GiB), temporary/log space and the 10 GiB reserve. Full-network float32 features plus labels alone project to **1,978,778,880 bytes (1.84 GiB)**, excluding keys/native bins/forecasts. Store only one image export; do not keep both compressed and uncompressed archives unnecessarily. The image export was **not** created here to avoid consuming another ~9 GiB locally.

## Source-side manifest creation and verification

Run from this repository root using its existing environment. The optional development inventory reads **only** explicit `target_year=2024/2025` partitions, never the mixed-year Iceberg metadata/catalog. It requires one unambiguous Parquet file per month, matching population-profile row counts and verified source checksum/revision footer statistics. Duplicate versions fail closed instead of being combined.

```powershell
.\.venv\Scripts\python.exe -B -m tools.training_transfer plan --root . --include-development --output artifacts/training-transfer-01/manifest.json
.\.venv\Scripts\python.exe -B -m tools.training_transfer verify --root . --manifest artifacts/training-transfer-01/manifest.json
```

These choose a new immutable manifest filename; an existing manifest is refused. To intentionally capture later source changes use a new filename such as `manifest-v2.json`. SHA-256 of the manifest itself should be recorded and checked through a trusted channel; hashes detect corruption but do not authenticate an untrusted source.

A clone alone is insufficient: these verified changes are uncommitted and have not been pushed. The manifest snapshots the actual working tree. It preserves relative paths, while the container mount is always `/opt/airflow`; no `C:\Users\...` path must be recreated on the destination. Original experiment JSON contains historical provenance pointers, including a mixed-year Iceberg metadata path. Those pointers remain unchanged for hash fidelity; the 384 benchmark does not dereference them or need that catalog. Do not transfer/dereference the mixed-year warehouse metadata for this handoff.

## Resumable transfer without overwriting user files

For a destination available as a local/removable/mounted share, the stdlib-only copier can stage the approved file set:

```powershell
python -B -m tools.training_transfer copy --root . --manifest artifacts/training-transfer-01/manifest.json --destination X:\citibike-native
```

Replace `X:\citibike-native` with the actual destination. Identical existing files are hash-checked and skipped. Incomplete `.transfer-part` files resume only after their existing prefix is verified. Conflicting final files or partial prefixes are preserved and rejected. Completion uses no-clobber publication. On filesystems that cannot support atomic publication, use a native-filesystem receiver/rsync rather than disabling integrity checks. No source file is deleted or altered.

For SSH transfer from Linux/WSL, use the generated file list. This example is a plan, not a connection executed here:

```bash
rsync -az --partial --partial-dir=.transfer-partial --ignore-existing \
  --files-from=artifacts/training-transfer-01/manifest.files.txt \
  ./ USER@NEW_HOST:/srv/citibike-native/
```

The partial directory permits retries without recopying completed files; `--ignore-existing` preserves existing destination files. Always verify hashes afterward: a conflicting pre-existing file is detected, not silently replaced. Transfer `manifest.json` and `manifest.files.txt` separately into `artifacts/training-transfer-01/` on the destination. Do not transfer source-side runtime heartbeat files, leases or resource-stop markers.

Only recreate the same selected file layout. The copied Gold files are a curated **ML source snapshot**, not a restored Iceberg catalog. They are byte-preserved and validated against 2024/2025 provenance. They need no Airflow, PostgreSQL, Spark cluster, catalog or ingestion watcher to run the saved 384 benchmark.

## Runtime image transfer: preserve exact identity

The verified local image tag is `citibike-airflow-spark-iceberg:3.2.2-python3.11`. Expected configuration ID:

```text
sha256:809353853ccd99ce12d5a4f359780f025f4932ac46fce3f065c8f57477c85c18
```

First inspect the destination for this exact image ID. If it is already present, skip image export/transfer/load entirely. Otherwise check free disk before creating one export. On the source Windows host, when actually ready to transfer:

```powershell
docker image inspect citibike-airflow-spark-iceberg:3.2.2-python3.11 --format "{{.Id}} {{.Architecture}} {{.Size}}"
docker image save --output artifacts/training-transfer-01/runtime-image.tar citibike-airflow-spark-iceberg:3.2.2-python3.11
Get-FileHash -Algorithm SHA256 artifacts/training-transfer-01/runtime-image.tar | Select-Object -ExpandProperty Hash | Set-Content -Encoding ascii artifacts/training-transfer-01/runtime-image.sha256
```

Do not pipe binary image data through Windows PowerShell 5 text pipelines. `docker image save --output` writes the bytes directly. Transfer this single archive and its checksum using a resumable file tool. This image archive is separate from the small data manifest. On Linux, an alternative is a single streamed `docker image save ... | gzip > runtime-image.tar.gz`, with its own checksum; avoid first writing an additional raw tar. See [Docker's image-save reference](https://docs.docker.com/reference/cli/docker/image/save/).

On the target, place the archive/checksum at the same `artifacts/training-transfer-01/` paths, then:

```bash
cd /srv/citibike-native
python3 -c "from pathlib import Path; from tools.training_transfer import sha; p=Path('artifacts/training-transfer-01'); assert sha(p/'runtime-image.tar').lower()==(p/'runtime-image.sha256').read_text(encoding='ascii').strip().lower()"
docker image load --input artifacts/training-transfer-01/runtime-image.tar
docker image inspect citibike-airflow-spark-iceberg:3.2.2-python3.11 --format '{{.Id}} {{.Architecture}} {{.Size}}'
```

The ID must match exactly and architecture must be amd64. The profile forbids pulls, and admission rejects a different image. A tag alone is not trusted. A fresh build from `airflow-image/Dockerfile` is an auditable fallback recipe, **not** a byte-identical environment; do not bypass the identity gate to accept it without separate validation.

## Target setup and verification

On native Linux:

```bash
cd /srv/citibike-native
python3 -B -m tools.training_transfer verify --root . --manifest artifacts/training-transfer-01/manifest.json
mkdir -p artifacts/native-4g-validation-01
export NATIVE_UID="$(id -u)"
export NATIVE_GID="$(id -g)"
docker compose -f docker-compose.native-4g.yml --profile native-4g --profile native-4g-probe config --format json
```

On Windows use native PowerShell from the received project root, replace `python3` with native `python`, create the artifact directory with `New-Item -ItemType Directory -Force`, and leave UID/GID defaults unless required. Run the controller on the actual Docker daemon host, not through an SSH/TCP Docker context; remote endpoints are rejected to prevent mixing local RAM with a different server. No host ML packages are needed for transfer verification or admission. All model dependencies live in the imported image. Do not run the Windows host controller inside WSL.

Start only the isolated 64 MiB probe, never the production stack:

```bash
docker compose -f docker-compose.native-4g.yml --profile native-4g-probe run -d --no-deps --name citibike_4g_resource_probe resource-probe
python3 -B -m tools.native_4g_preflight --probe-container citibike_4g_resource_probe
```

Admission must exit zero. It checks host physical memory, guest availability, image identity, native amd64 execution, cgroup v2 and free disk. Stop on a nonzero result. Do not raise the current laptop's WSL allocation to simulate a larger physical host.

In a separate terminal at the same project root, keep the monitor running:

```bash
python3 -B -m tools.native_4g_preflight --probe-container citibike_4g_resource_probe --monitor --interval 5
```

It publishes fresh host heartbeats. Startup readiness is stricter than runtime reserves, so readiness may become blocked while an admitted worker is using RAM; the worker separately enforces runtime limits. Never start two training phases concurrently.

## Exact 384-station benchmark sequence

Use new container/run names. Execute **one line at a time**, inspect its exit code/result, and stop on any failure. No command below was executed on a new host during this preparation task.

```bash
docker compose -f docker-compose.native-4g.yml --profile native-4g run --no-deps --name cb4g_status01 trainer --task status
docker compose -f docker-compose.native-4g.yml --profile native-4g run --no-deps --name cb4g_construct01 trainer --task construct --run-name host01
docker compose -f docker-compose.native-4g.yml --profile native-4g run --no-deps --name cb4g_compact_fit01 trainer --task fit --variant compact --run-name host01
docker compose -f docker-compose.native-4g.yml --profile native-4g run --no-deps --name cb4g_compact_export01 trainer --task export --variant compact --run-name host01
docker compose -f docker-compose.native-4g.yml --profile native-4g run --no-deps --name cb4g_deferred_fit01 trainer --task fit --variant deferred --run-name host01
docker compose -f docker-compose.native-4g.yml --profile native-4g run --no-deps --name cb4g_deferred_curves01 trainer --task curves --variant deferred --run-name host01
docker compose -f docker-compose.native-4g.yml --profile native-4g run --no-deps --name cb4g_deferred_export01 trainer --task export --variant deferred --run-name host01
```

Useful monitoring/verification:

```bash
docker logs --tail 20 cb4g_compact_fit01
docker inspect --format '{{.HostConfig.Memory}} {{.HostConfig.MemorySwap}} {{.HostConfig.NanoCpus}} {{json .State}}' cb4g_compact_fit01
python3 -B -m tools.training_transfer verify --root . --manifest artifacts/training-transfer-01/manifest.json
```

Expected limits: memory and memory-plus-swap both **4294967296**, quota **2000000000 NanoCpus**. The worker independently checks cgroup 4 GiB/zero extra swap. Per-phase results must show zero `max`, `oom`, `oom_kill` events, kernel peak below **3.4 GiB**, and `correctness_passed=true`.

All outputs stay under `artifacts/native-4g-validation-01/runs/host01/`. Construction must reproduce the baseline binary hashes. Fits must reproduce model and checkpoint hashes. Exports must reproduce all training/validation Parquet files and scores. A CPU/platform/library-induced mismatch is a stop-and-investigate result, not permission to weaken comparisons. Record actual new-host timings and memory; old measurements are not fresh host results.

Keep code/configuration fixed between phases. Reuse completed phases only after hash/resource admission; existing phase directories are never overwritten. Use a fresh run name after a failed phase. Normal monitor restarts use a new producer identity. Killed workers/monitors can leave leases/resource-stop markers: inspect the owning process/container and recover deliberately, never auto-clear these files. Automatic mid-fit continuation remains unsupported; checkpoints are preserved.

## Data reconstruction and eventual full-network gate

Prefer reuse of the existing benchmark matrices and native binaries. If they are unavailable, **do not redownload trip archives automatically**. The optional curated Gold snapshot supplies the raw station-hour inputs, with verified source-month revisions. Rebuilding features must:

- use the frozen cohort manifest and sorted station-code mapping;
- join each station's 2024 and 2025 history in UTC, preserving all 17,544 hours;
- carry the preceding 168 hours across months and the year boundary;
- drop only the 168-hour training warmup, leaving 8,616 training and 8,760 validation feature rows per fully covered station;
- retain original vocabulary of feature names/order, float32 representation, keys/labels and training-only bins.

Do not reconstruct 2025 independently or train bins using validation. The optional full-network manifest contains **1,898 eligible stations**, distinct from the 384-station code mapping; never apply the old model/encoder to those new station codes.

The exact eventual **readiness** command is available now, but intentionally returns a blocked status and never invokes a model:

```bash
python3 -B -m tools.training_transfer full-network-plan --root . --manifest artifacts/training-transfer-01/manifest.json --benchmark-root artifacts/native-4g-validation-01/runs/host01
```

It verifies transferred files and requires genuine 4 GiB construction, compact fit/export, deferred fit/curve/export receipts. Even with those receipts it keeps training disabled pending explicit authorization and implementation of a generalized 1,898-station executor/portable feature loader. **There is no working full-network training command to present as verified today.** Existing `native_4g_worker` is hard-coded to 384 and exact baseline comparisons. Generalization must use the curated Parquet file list and frozen `all_eligible.json`, preserve year splits/keys/168-hour continuity, add model-quality/guard validation, and retain the same safety/lease/restart controls before any full run is authorized.

Expected full-network feature rows are **16,353,168 training + 16,626,480 validation**. The plan command is a documented blocker audit, not a hidden enable switch. Current config and manifests keep `full_network_training_enabled=false`/training disabled. No 2026 files or outcomes are needed or permitted.

## Planning estimates from saved measurements

| Work | Existing measurement / estimate |
|---|---|
| 384 buffered construction | 35.46 s, 0.344 GiB on the original machine |
| 384 compact fit | 34.95 s, 0.366 GiB |
| 384 deferred fit | 25.86 s, 0.284 GiB; add 56.71 s for all validation curves |
| 384 complete export | About 90 s per variant |
| Both 384 benchmark variants | Roughly 6-15 minutes including phases, startup, verification and IO; estimate, not new-host measurement |
| Full-network construction projection | 2.553 GiB including conservative 50% margin |
| Full-network normal fitting projection | 2.714 GiB including conservative 50% margin |
| Full-network deferred fitting projection | 2.108 GiB, restricted fixed-budget protocol |
| Full pipeline including feature preparation | Prior conservative estimate about 2.55 hours for 100 rounds on two CPUs; no hyperparameter search, not a measured full run |

These extrapolations do not prove new-host safety. The 4 GiB stop gate is 3.4 GiB, leaving only estimated headroom until measured benchmarks pass. Cold caches, disks, CPU ISA and allocator behavior differ. Deferred validation reduces fitting memory but adds work. Forecast quality and operational policies need independent development-only validation after capacity approval; no deployment is implied.

## What was validated here

The isolated resource probe was started with an actual 64 MiB/zero-extra-swap cap and no network, supplied two live heartbeat samples through the stdlib-only controller, and was stopped afterward. Admission correctly remained blocked on the small source host; no 4 GiB worker was started.

Source dependency closure was copied to a small isolated directory and imported inside the exact existing image with no fallback to the original checkout; no model was invoked. Compose rendering, native Linux/Windows monitor logic, WSL rejection, partial-copy resume, identical-file skipping, conflict/path-traversal protection and the disabled full-network planner were tested. Real 2024/2025 inventory footer/count/version checks passed. No image archive or full benchmark-data duplicate was created locally.

Local regression validation passed **194 tests**, with two skipped, three unrelated learner-fit tests deselected and 14 dependency warnings. Eight new tests cover transfer/restart safety and portability; the existing resource-gate suite also passed.

No remote transfer or remote admission/benchmark was executed. No paid infrastructure was provisioned, no 2026 data accessed, no frozen policy changed and no deployment/commit/push occurred. See the generated transfer manifest and `docs/audit/training_host_transfer.json` for final exact inventory/test evidence.

After every worker has stopped, end the host monitor normally and stop only the isolated probe with `docker stop citibike_4g_resource_probe`. Do not stop the production stack, delete image exports or clear leases automatically.
