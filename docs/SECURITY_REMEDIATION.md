# Credential remediation and clean-history publication

## Scope and verified local actions

Historical generated Airflow authentication, database passwords, signing secrets and a notebook token were identified in the original Git history. That history was preserved locally; it must not be included in the clean publication repository.

Before rotation, a private access-restricted, Git-ignored backup captured the repository's source working tree, complete Git directory, all-ref Git bundle, index/diffs, local environment/authentication state and database dumps. The Git bundle verified, copied Git objects passed `git fsck`, source-copy checksums matched, and **all six PostgreSQL databases restored successfully** in an isolated 256 MiB container with no exposed port. Every restored public-table row count matched. Datasets and model artifacts were retained in place rather than physically duplicated; this source/Git/database backup is not a second copy of all trip archives.

| Credential | Dependencies and treatment | Verification |
|---|---|---|
| Airflow login password | Ignored generated auth file, API clients/watcher | Replaced with a random value; current retained file differs from exposed history |
| Metadata PostgreSQL role | Airflow metadata and stopped Airflow components | SCRAM verifier updated; new login accepted, old password rejected; metadata counts preserved |
| Cache PostgreSQL role | All cache/sample databases and FastAPI connection pool | SCRAM verifier updated; new login accepted, old password rejected; all table counts preserved |
| Airflow API signing secret | Web/API and local components | New random value applied to local environment and stopped container configuration |
| Airflow JWT signing secret | Token verification and local components | New random value applied; future API start uses the new key |
| Airflow Fernet key | Connection passwords/extras and variable encryption | New valid key generated; **zero connection/variable records** existed, so no stored ciphertext required re-encryption |
| Notebook token | Optional local notebook server | New random environment value; no notebook server was started |

The existing FastAPI service was reconfigured with the new database credential using the **same image**, and database-backed readiness and dashboard health passed. Airflow containers were refreshed with `--no-start`; schedulers and full-network training were not started. Database containers and volumes were not recreated. Their old bootstrap environment values may remain visible in private Docker inspection, but those passwords no longer authenticate; future Compose recreation uses the new local environment. Stopped historical containers/private backups are not publication files.

Airflow login/signature behavior was verified by configuration, not a fresh live Airflow session: the service remains stopped. Existing browser sessions/tokens should be treated as invalid after the next start. Never replace a Fernet key blindly when encrypted records exist. For a populated installation, follow the [Airflow key-rotation procedure](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/fernet.html), with a verified backup and both keys during migration. PostgreSQL password changes follow [ALTER ROLE](https://www.postgresql.org/docs/17/sql-alterrole.html); plaintext secrets were not placed in commands or console output.

## Source and history boundaries

Current source uses ignored environment/auth files and empty `.env.example` placeholders. Legacy notebooks and old root images remain local and excluded. The separate publication repository is created from an explicit source/documentation allowlist, not cloned from the old Git database. Only the seven selected README assets are included. Public checks inspect both the source files and every reachable publication commit, rather than assuming `.gitignore` removes old history.

The public full-network authorization configuration defaults to disabled. Existing 2024/2025 source pipelines and tests remain available, but the private machine's explicit training authorization is not transferable to another clone. The original frozen model/policy artifacts and source contract are not modified to force training or pass resource gates.

## Remaining risks and reproducibility limits

- Previously copied credentials cannot be recalled from third-party clones or caches. Rotation invalidates the owned local values; reused credentials on any other host/account need separate rotation. No other host or paid service was accessed.
- The old repository, private backup, local environment and Docker metadata still contain sensitive historical material. Keep them private; do not upload a bundle, dump, private backup, old Git directory or original remote history.
- A clean source clone intentionally omits original datasets, model binaries and private experiment state. Reproducing full experiments requires the documented data and admitted hardware. Portable metrics and seven visuals are retained; they are not substitute model artifacts.
- Local API/dashboard services are not a production deployment or public authenticated endpoint. Access control, TLS, ingress restrictions, dependency review and independent prospective ML validation remain future work.
- The 2026 results remain a **reused holdout**; no model was rescored, trained or selected during credential remediation. Full-network 1,898-station training remains paused/future work.
- Pattern scans are defense in depth, not proof that every possible secret representation is detectable. Final source/history findings, test totals, inventory and commit identity are recorded in the accompanying publication report.

The MIT license applies to project source only. Public trip data and third-party libraries retain their own terms.
