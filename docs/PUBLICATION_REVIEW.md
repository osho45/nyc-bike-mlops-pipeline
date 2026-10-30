# Publication review

> Historical pre-remediation review of the original working tree. Credential rotation, MIT licensing and separate clean-history preparation subsequently proceeded; see [SECURITY_REMEDIATION.md](SECURITY_REMEDIATION.md) for the current scope. Earlier history warnings below describe why the original repository must remain private.

This review prepares the working tree for human approval. No commit, push, deployment, full-network restart, new test-period data read or 2026 scoring was performed.

## Verified evidence

| Claim | Saved evidence checked |
|---|---|
| 33 months, 125,217,844 source records; 125,092,464 accepted; 125,380 quarantined | All 33 month entries and complete/year-gate status in `audit/controlled_ingestion_progress.json` |
| 384-station 2024/2025 rows and MAE/RMSE/WAPE | `audit/native_capacity_384.json`, compared with original ignored `artifacts/native-network-capacity-03/384/export/result.json` |
| 48-station 17.839892% modeled penalty reduction | `audit/cohort48_2026.json`, compared with existing `aggregate_metrics.json`; 2.1325318656 versus 2.5955806618 |
| 189 passing guards, three supported failures, 48 insufficient-support outcomes | Saved classifications in the frozen 48-station report; no reevaluation |
| Native memory reductions | `audit/native_memory_optimization.json` and `audit/native_fit_memory.json`; exact-model/prediction parity checks retained |
| End-to-end implementation | Earlier genuine-sample Docker/asset/recovery/browser reports; these historical successes are distinguished from current service availability |

`audit/publication_results.json` records selected values and SHA-256 hashes of the portable source reports. `python -m tools.publication_assets` independently asserts the reported values against retained original summaries and generates all new result plots. It does not open raw data or invoke a model. No 2026 outcome exploration or additional test scoring was done.

## Actual screenshots and diagram

Files in `images/`:

- `citibike-architecture.svg`: code-generated, self-contained architecture diagram. The dashed promotion gate is intentional; experimental Poisson models are not deployed to the existing classifier serving path.
- `forecast-metrics-384-stations.png`: count MAE/RMSE/WAPE, with WAPE displayed as percent.
- `operational-penalty-48-stations.png`: modeled cost comparison; explicitly labeled reused holdout and illustrative units.
- `native-memory-optimization-384-stations.png`: separate native construction/fitting measurements, in GiB.
- `streamlit-genuine-2024-sample.png`: actual desktop Chromium capture of the retained January 2024 smoke prediction.
- `streamlit-genuine-2024-sample-mobile.png`: actual mobile-width Chromium capture.
- `fastapi-genuine-sample-response.png`: actual Swagger Execute response from the retained sample PostgreSQL database.

The screenshot run used existing `citibike_validation_container_sample_03` data: exactly one prediction for station `6140.05`, January 13, 2024 at 23:00 UTC. Four lookback filters, two chart renders, API/dashboard agreement and Swagger Execute were verified; no page or browser-console errors occurred. The dataset is genuine but bounded, not a representative demand benchmark. Temporary local API/Streamlit processes were stopped afterward; original services and persistence were left intact. No mockups, synthetic replacements or populated production-cache claims are used. Screenshots contain application content only, without browser address bars, desktop/user paths or credentials.

Airflow services were stopped during review, so **no new Airflow screenshot was captured**. The main API `/health` and `/ready` returned 200, but `/stations` was empty. The default Streamlit returned 200. Thus the populated screenshots come from the explicitly labeled retained sample, not the default empty store. Ignored browser logs/report: `artifacts/publication-review/browser.json`.

## Training pause and preservation

The full-network supervisor had already exited with `blocked_resources` before this task; its next phase was May 2024 raw extraction. No supervisor or full-network worker was active. January–April 2024 raw checkpoints are preserved. There are zero completed feature matrices and no full-network trained model. Nothing was restarted or canceled unnecessarily. The README describes this as **paused**, not failed model development.

Models, datasets, source checkpoints, frozen policies and experiment outputs remain local. The public Compose configuration intentionally changes credential handling; its exact prepublication bytes were archived at `artifacts/publication-review/docker-compose.before-publication.yaml`, verified against the existing protected hash. No frozen hash map was edited to conceal this source/config change. Historical training/transfer admission that compares the old Compose hash needs an explicit reviewed provenance update before resumption; do not bypass that check. All other protected-file hashes remain unchanged.

## Git and credential readiness

The README is normalized to `README.md`; the prior working README is retained in ignored publication artifacts. Generated Airflow authentication and historical taxi notebooks/root result images were removed **from the Git index only**. Existing local files and moved `notebooks/legacy_taxi/` copies remain available. No history was rewritten.

New ignore rules cover generated authentication, historical taxi assets, credential-key files, dumps and browser outputs; existing rules exclude data, models, artifacts, logs, environments and caches. `.env.example` contains empty placeholders. Compose now requires environment-provided database passwords and a notebook token. New installations generate random credentials; Airflow auth initialization preserves existing passwords. Existing local credentials were retained in ignored `.env`, not rotated, so initialized databases and services were not disrupted. The two PowerShell monitor defaults now derive the repository directory instead of embedding a personal path. Dashboard legend/hover text now says modeled penalty rather than implying measured revenue.

**Manual security review remains necessary before public publication:** generated auth was tracked in earlier history, and the original configuration contained public teaching defaults. Removing current tracked files does not remove older blobs. Rotate any reused credentials and decide whether a separately reviewed history cleanup is appropriate before making the repository public. No credential value is reproduced here. Do not run `docker compose down -v` to rotate passwords; existing volumes require intentional database-role/password migration. Do not expose the unauthenticated local API/dashboard publicly.

For an existing checkout with an older `.env`, add `POSTGRES_METADATA_PASSWORD` and `JUPYTER_TOKEN` using its existing local values; the initializer deliberately does not overwrite an existing `.env`. Fresh initialization generates both. No Docker service was recreated using the revised configuration during this review.

At this earlier review no software license file was present. An [MIT LICENSE](../LICENSE) has subsequently been added; it does not relicense Citi Bike data or third-party dependencies.

## Validation and review candidates

The final test, command, rendering, image-link and candidate-file checks are recorded below after execution. Historical test counts elsewhere describe their original runs rather than today's total.

Review these changes together: `README.md`, `.gitignore`, `.env.example`, `docker-compose.yaml`, the authentication initializer and local initializer, portable PowerShell defaults, presentation-only Streamlit labels, `tools/publication_assets.py`, `tools/capture_publication.py`, `tests/test_publication.py`, `docs/audit/publication_results.json` and the seven selected visuals. The repository also has substantial earlier uncommitted Citi Bike implementation files; publish their required source/config/tests with the documentation rather than staging the README alone. Review `git status`, `git diff` and `git diff --cached` before a later authorized commit.

Only README case normalization and privacy/provenance asset removals were staged here. Remaining implementation/documentation changes are left for final review. No blanket `git add .` was run.

Remaining work is operational, not a documentation claim: finish full-network preparation/training on admitted resources, validate a larger-memory host, obtain independent prospective evaluation, resolve policy guard limitations, review security/history and licensing, and separately approve any production promotion.

### Final executed checks

| Check | Actual result |
|---|---|
| Full pytest regression suite | **210 passed, 2 skipped**, zero failures/errors; 17 dependency warnings |
| Skip scope | Host Airflow scheduling unavailable; opt-in Spark/Java/Iceberg test not executed in this environment |
| Publication-specific tests | **4 passed**: README links, portable metric reconciliation, self-contained SVG, non-overwriting random auth initialization |
| Compose | `docker compose config --quiet` passed; no deployment/recreation |
| CLI entry points | Help commands passed for downloader, reference validator, backfill and existing classifier training; no ingestion or training command was executed for these checks |
| Syntax | Python compilation and both modified PowerShell script parses passed |
| Real browser | Sample desktop/mobile, four filters, API/dashboard agreement and real Swagger execution passed with no page/console errors |
| README/diagram rendering | All **six embedded images** loaded with alt text; standalone SVG and HTML-embedded SVG rendered in Chromium |
| Visual size | Seven selected files total **480,179 bytes**; no selected image is a large binary artifact |
| Git checks | `git diff --check` passed; reviewed candidates contain no data/model/runtime directories or files over 2 MiB; selected credential/personal-path patterns had no findings |
| Preservation | 113 protected-file hashes unchanged; exact original Compose archived under its original hash; full-network source contract and four raw checkpoints unchanged |

The machine-readable [publication checks](audit/publication_checks.json) contain the final candidate count, image-load checks and test totals. The ignored `artifacts/publication-review/staging-candidates.txt` lists review candidates; it is not a recommendation to bypass exclusions with forced staging. The credential-pattern scan is deliberately scoped and is not a substitute for reviewing old Git history or rotating previously exposed defaults.

Optional screenshot reproduction requires `requirements-browser.txt`, a locally installed Chromium under `artifacts/browser-cache`, the retained sample PostgreSQL database and an initialized `.env`: `python -m tools.capture_publication`. `python -m tools.check_publication` renders documentation and checks candidates using that browser and the recorded test XML. Neither tool starts full-network training. Unavailable retained sample data must be reported rather than replaced with mock predictions.
