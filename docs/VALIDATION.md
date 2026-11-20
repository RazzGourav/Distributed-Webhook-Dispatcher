# Validation record

Verified on 20 November 2026 using Python 3.12.14 and PostgreSQL 17.12.

| Check | Result |
|---|---|
| Automated unit/integration tests | 33 passed in 31.73 seconds |
| Application statement coverage | 89% |
| Ruff lint | Passed |
| Ruff formatting | Passed, 20 Python files |
| Frozen dependency resolution | Passed |
| Exported requirements matches lockfile | Passed |
| Render published JSON schema | Passed |
| Fresh database initialization | Passed |
| Local HTTP smoke test | Four scenarios passed |

The integration tests use actual PostgreSQL and local HTTP receivers. The
recovery test simulates the persisted state left by an interrupted worker; it
is not a chaos-engineering or network-partition test.

## Local HTTP smoke measurements

These came from a running Uvicorn process with its embedded worker and a separate
PostgreSQL process. They are illustrative local results, not a load benchmark.

| Scenario | Ingestion time | Final status | Attempts |
|---|---|---|---|
| Success | 0.007 s | success | 1 |
| Fail twice, then succeed | 0.003 s | success | 3 |
| Always HTTP 503 | 0.003 s | failed | 4 |
| Receiver waits 10 seconds | 0.004 s | success | 1 |

The slow receiver's outbound attempt took approximately 10,001 ms while its
ingestion call returned in approximately 4 ms.

## Verification boundaries

Docker Compose and Dockerfile are provided; this environment did not have a
Docker daemon, so the image build and Compose runtime were not executed here.
The equivalent Python application and database were run directly.

Render's public schema validation checks Blueprint structure. It is not account
capacity validation or proof of a successful deployment. Live Render checks and
the actual public URL must be added after deployment. Check the repository's
Actions tab for the independent CI result on the submitted commit.
