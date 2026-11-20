# Distributed Webhook Dispatcher

A FastAPI service that accepts an event, commits it to PostgreSQL, and returns
`202 Accepted`. Background workers deliver the payload with retries and shared
per-destination rate limits. Built for the Web3Task Backend Intern evaluation.

**Stack:** Python 3.12 · FastAPI · Procrastinate · PostgreSQL 17 · aiohttp.

## Run locally

Prerequisites: Git, Python 3.12 and Docker Desktop with Compose.

```bash
git clone https://github.com/RazzGourav/Distributed-Webhook-Dispatcher.git
cd Distributed-Webhook-Dispatcher
python scripts/init_env.py
docker compose up --build
```

Open **http://localhost:8000/docs**. Read `API_KEY` from your local `.env`, click
**Authorize**, and enter that value. Do not commit `.env`.

Use `http://api:8000/demo/receiver/success` as the destination when running the
complete Compose stack. `api` is the container's service name. The Compose setup
allows private addresses **only for this local demonstration**. The Render
configuration blocks them.

`docker compose down` stops the services while preserving database data.
`docker compose down -v` deletes the local database volume; use only when you
intend to reset it. The checked-in database credentials are local development
credentials; Render creates its own credentials and keeps the database private.

### Run Python directly

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then:

```bash
python scripts/init_env.py
docker compose up -d db
uv sync --frozen
uv run uvicorn dispatcher.main:create_app --factory --port 8000 --no-access-log
```

`init_env.py` refuses to overwrite an existing `.env`; skip it if you already
created one. With Python outside Docker, set `ALLOW_PRIVATE_NETWORKS=true` in
`.env` only for local testing, and use `http://127.0.0.1:8000/demo/receiver/success`.
For public destinations leave the setting false.

`requirements.txt` is also provided, with pinned transitive dependencies and
hashes. Docker installs it with `pip --require-hashes`. `uv.lock` is the source
of truth; CI verifies the exported requirements match it.

## Test the API

These curl examples use Bash/Git Bash. On Windows PowerShell use `curl.exe`, or
use Swagger `/docs`, which avoids shell quoting differences.

```bash
export BASE_URL=http://localhost:8000
# Set API_KEY to the value in your local .env; never use a repository/public key.
export API_KEY='YOUR_LOCAL_API_KEY'

curl -i "$BASE_URL/api/webhooks/dispatch" \
  -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' \
  -d '{"destinationUrl":"http://api:8000/demo/receiver/flaky","eventId":"evt_123456","payload":{"event":"user.signup","userId":"usr_999"}}'
```

Expected response:

```json
{"status":"queued","jobId":"a-generated-uuid"}
```

Then inspect delivery history:

```bash
curl "$BASE_URL/api/webhooks/jobs/REPLACE_WITH_JOB_ID" -H "X-API-Key: $API_KEY"
curl "$BASE_URL/health/ready"
```

The flaky receiver fails twice with 503, then succeeds. The returned history
contains response codes, attempt numbers, durations, timestamps, and retry delays.
Status responses intentionally omit the original payload and destination URL.

| Endpoint | Purpose | Authentication |
|---|---|---|
| `POST /api/webhooks/dispatch` | Validate and durably enqueue | `X-API-Key` |
| `GET /api/webhooks/jobs/{jobId}` | Status and attempt history | `X-API-Key` |
| `GET /health/live` | Process liveness | Public |
| `GET /health/ready` | Database + worker readiness | Public |
| `GET /docs` | Interactive API documentation | Public; operations require key |
| `POST /demo/receiver/{mode}` | Fixed-function evaluation receiver | Public, only if enabled |

`eventId` accepts 1–128 letters, digits, `_`, `.`, `:`, or `-`. `payload` must
be a JSON object. Extra top-level fields, unsafe literal destinations, and invalid
URLs return 422. Missing/invalid credentials return 401. Bodies above 64 KiB return
413. Queue admission failure returns 503, never a misleading 202.

The API key is required because an unprotected dispatcher is an open outbound
request proxy. Share the deployment's evaluation key privately with the reviewer.

## Demo scenarios

Set `DEMO_ENABLED=true` to enable these routes. On Render use the **actual public
service URL**, e.g. `https://YOUR-SERVICE.onrender.com/demo/receiver/flaky`.

| Mode | Receiver behavior | Expected dispatcher result |
|---|---|---|
| `success` | HTTP 200 | Success, 1 attempt |
| `flaky` | HTTP 503 twice, then 200 | Success, 3 attempts |
| `fail` | HTTP 503 always | Failed, 4 attempts |
| `rate-limit` | HTTP 429 always | Failed, 4 attempts |
| `bad-request` | HTTP 400 | Failed, 1 attempt |
| `slow` | Wait 10 seconds, then HTTP 200 | Fast ingestion; eventual success |

Run an automated smoke check (reads `API_KEY` from the environment or `.env`):

```bash
uv run python scripts/smoke.py --base-url http://localhost:8000 --receiver-base http://api:8000
# Live deployment: use its HTTPS URL for both arguments.
```

## Architecture

```mermaid
flowchart TD
    C[Client] --> A[FastAPI validation]
    A --> T[Database transaction]
    T --> D[(PostgreSQL jobs and history)]
    T --> R[202 with job ID]
    D --> W[Competing workers]
    W --> L[Domain lock and token bucket]
    L --> H[HTTP receiver]
    H --> O{Delivery outcome}
    O -->|2xx| S[Record success]
    O -->|Transient failure| B[Schedule retry]
    O -->|Permanent or exhausted| F[Record failure]
    B --> D
```

**Persistent queue, without dual writes.** Procrastinate manages the job queue,
worker coordination, future scheduling and execution locks. psycopg inserts the
delivery record and Procrastinate job in one PostgreSQL transaction. Workers see
neither until commit. The API does no destination DNS resolution or delivery I/O.

**Non-blocking execution.** Workers use `async` database and HTTP clients. No HTTP
request is made inside the ingestion handler, and no synchronous `sleep` is used.
Backoff and rate deferrals are scheduled in PostgreSQL, releasing worker slots.

**Distributed rate control.** A token bucket is stored per canonical hostname,
shared across processes. A row lock makes token consumption atomic. Defaults:
two tokens/second, capacity one. A Procrastinate `mutex` lock also allows only
one in-flight delivery per hostname. Different ports/paths/schemes on the same
hostname share a bucket. Subdomains are distinct destinations. Limits apply to
dispatch admission; network/DNS latency can alter observed arrival spacing.

**Retry policy.** One initial attempt plus three retries. Retry on HTTP 5xx,
429, timeout, and transport failures. Delays are **2, 4, 8 seconds after each
failed attempt**, not measured from the original enqueue time. A valid
`Retry-After` can increase the delay, capped at 300 seconds. Other 4xx, redirects,
unsafe destinations and TLS validation errors are terminal. Redirects are never
followed. Successful 2xx response headers acknowledge delivery; response bodies
are not buffered. Rate-limit deferrals do not consume the four-attempt budget.

**Delivery semantics.** This is bounded, at-least-once delivery, not exactly-once.
Each attempt reserves its number before the HTTP call. If a process dies after
sending but before committing the outcome, recovery records that attempt as
`unknown` and consumes its budget. The receiver may already have processed it.
Every delivery carries `X-Webhook-Id`, `X-Webhook-Job-Id`, and `X-Webhook-Attempt`.
Receivers should atomically deduplicate `X-Webhook-Id`. Repeating an ingestion
request creates a new job; `eventId` is metadata, not ingestion deduplication.

**Recovery.** Worker heartbeats are written every 5 seconds. A recovery loop
checks every 15 seconds for jobs belonging to absent workers or workers stale
for 60 seconds. Recovery itself is coordinated with a PostgreSQL advisory lock.
Graceful shutdown allows 15 seconds for current work. A persisted success is
not delivered again if only the queue acknowledgment was interrupted. Unexpected
programming errors are logged and terminal, rather than retried forever.

**State and logs.** PostgreSQL stores payloads, job state and per-attempt history.
Structured application logs contain identifiers and outcomes, not payloads,
API keys or complete destination URLs. Terminal history and queue records are
removed after seven days by default; pending work is preserved. Operational
queue execution status and business delivery status differ: an exhausted delivery
can be a completed queue task because failure was handled and recorded correctly.

**Outbound safety.** Only HTTP(S), with ports 80/443 by default. Reject URL
credentials, fragments, private/reserved IPs, local names and IP transition
addresses. DNS answers are checked by the actual HTTP connector; mixed public/
private results fail closed. No redirects, implicit proxy environment, or
connection reuse bypasses the resolver. DNS checks happen in the worker, so a
hostname that resolves privately may be accepted initially and later fail delivery.

## Tests and CI

Tests require a **disposable PostgreSQL database** and erase its queue/history.
Never point `TEST_DATABASE_URL` at the deployed or personal development database.

```bash
docker compose up -d db
docker compose exec db createdb -U dispatcher dispatcher_test
uv sync --frozen
export TEST_DATABASE_URL=postgresql://dispatcher:dispatcher@localhost:5432/dispatcher_test
uv run pytest --cov=dispatcher --cov-report=term-missing
uv run ruff check .
uv run ruff format --check .
```

PowerShell environment equivalent:

```powershell
$env:TEST_DATABASE_URL = "postgresql://dispatcher:dispatcher@localhost:5432/dispatcher_test"
uv run pytest -q
```

The suite uses real PostgreSQL and local HTTP receivers. It covers transaction
rollback, validation/authentication, a ten-second receiver, real 2/4/8 backoff,
429/5xx/4xx classification, timeouts, shared rate control with two workers,
DNS safety, durable queue pickup and interrupted-worker recovery. GitHub Actions
runs it against PostgreSQL 17 and checks formatting and dependency export drift.

## Deploy on Render

The repository includes a free-tier Blueprint. The Docker image runs as a
non-root user; the API and a bounded worker share one free web service. PostgreSQL
is a separate managed service in the same region. Database access is private.

1. Push the repository, including `render.yaml`, to `main`.
2. [Create the Render Blueprint](https://dashboard.render.com/blueprint/new?repo=https://github.com/RazzGourav/Distributed-Webhook-Dispatcher).
3. Review the two **Free** plans: web service and PostgreSQL. If a free database
   is already in use, stop and choose an appropriate existing/new database; do
   not delete an unrelated database or silently upgrade the plan.
4. Apply the reviewed plan. Render generates `API_KEY` and supplies `DATABASE_URL`.
5. Wait for deployment to become live. Open `/health/ready` and `/docs`.
6. Retrieve the API key from Render's Environment page; keep it private.
7. Run the smoke script using the live URL as both base URLs.
8. Include the public API URL, repository URL and evaluation key in the submission.

Automatic deploys are disabled in the Blueprint for controlled evaluation changes.
Schema initialization runs under a database lock at startup. Custom migrations
are numbered and checksum-verified; never edit an applied migration. Procrastinate
is locked to the tested version; follow its upstream schema migration procedure
before upgrading that dependency against an existing database.

### Free hosting limitations and production path

- Render Free sleeps after 15 idle minutes and may take about a minute to wake.
  Cold-start latency is distinct from warm ingestion latency. Sleeping workers
  cannot deliver; persisted jobs resume when the service wakes. There is no
  continuous-delivery SLA on the free service.
- Free Render PostgreSQL expires after **30 days**, has 1 GB storage and no
  backups. Record the expiry date and migrate/upgrade before it expires.
- The database is durable across app restarts, not protected against database
  deletion, account limits, or every infrastructure failure.
- For production: always-on API and workers, backed-up PostgreSQL, an egress
  firewall, per-client credentials/quotas, monitoring/alerts, webhook signatures,
  queue-capacity admission control and a reviewed retention policy. This version
  uses one shared API key and is not a multi-tenant platform. Authentication is
  not a full inbound denial-of-service defense. Terminal failed deliveries can
  be inspected; no automatic dead-letter replay UI is included.
- For a separate worker, set API `EMBEDDED_WORKER=false` and start
  `uv run python -m dispatcher.run_worker` with the same database, API key, and
  delivery policy settings. Scale worker processes deliberately: each process
  can use 12 pooled database connections plus a notification connection.
- Python dependencies are locked; base Docker image tags and CI action major
  tags remain floating. Pin their digests/SHAs for a release with stronger
  supply-chain reproducibility. Keep clocks synchronized on production hosts.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `API_KEY` | Required, at least 16 characters | Dispatch/status authentication |
| `DATABASE_URL` | Local development connection | PostgreSQL DSN |
| `EMBEDDED_WORKER` | `true` | Run worker with API |
| `DEMO_ENABLED` | `false` | Enable fixed demo receivers |
| `ALLOW_PRIVATE_NETWORKS` | `false` | Local test override; never enable publicly |
| `WORKER_CONCURRENCY` | `8` | Parallel jobs across different hosts |
| `REQUEST_TIMEOUT` | `12` seconds | Entire outbound HTTP request |
| `DOMAIN_RATE` | `2` per second | Shared token refill rate |
| `DOMAIN_BURST` | `1` | Token capacity |
| `MAX_BODY_BYTES` | `65536` | Incoming request cap |
| `RETENTION_DAYS` | `7` | Terminal delivery history retention |
| `LOG_LEVEL` | `INFO` | Application log level |

See [the teaching walkthrough](docs/WALKTHROUGH.md) and
[submission draft](docs/SUBMISSION.md).

## References

- [FastAPI](https://fastapi.tiangolo.com/)
- [Procrastinate](https://procrastinate.readthedocs.io/en/stable/)
- [Transactional enqueue](https://procrastinate.readthedocs.io/en/stable/howto/production/external_connection.html)
- [Psycopg connection pools](https://www.psycopg.org/psycopg3/docs/advanced/pool.html)
- [aiohttp client](https://docs.aiohttp.org/en/stable/client_advanced.html)
- [Render free-tier limitations](https://render.com/docs/free)
