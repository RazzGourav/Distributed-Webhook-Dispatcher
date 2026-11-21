# Distributed Webhook Dispatcher

A production-grade webhook delivery engine built with Python and PostgreSQL. It accepts event payloads via REST API, persists them durably, and delivers them asynchronously through background workers with exponential retries and per-domain rate limiting.

- **Live Deployment:** https://webhook-dispatcher-prib.onrender.com
- **Interactive API Docs (Swagger):** https://webhook-dispatcher-prib.onrender.com/docs
- **Health Check:** https://webhook-dispatcher-prib.onrender.com/health/ready

## Stack

Python 3.12 · FastAPI · Procrastinate · PostgreSQL 17 · psycopg · aiohttp

## System Architecture

```mermaid
flowchart TD
    C[Client] --> A[FastAPI — Validation & Auth]
    A --> T[Atomic DB Transaction]
    T --> D[(PostgreSQL — Jobs, History, Rate Buckets)]
    T --> R[HTTP 202 Accepted + Job ID]
    D --> W[Background Workers — Competing Consumers]
    W --> L[Domain Lock + Token Bucket]
    L --> H[HTTP POST to Destination]
    H --> O{Delivery Outcome}
    O -->|2xx| S[Record Success]
    O -->|5xx / 429 / Timeout| B[Schedule Retry with Backoff]
    O -->|4xx / Redirect / Unsafe| F[Record Terminal Failure]
    B --> D
```

### How It Works

**1. Non-blocking ingestion.** The API validates the request, generates a job UUID, and writes both the delivery record and the Procrastinate queue job in a single PostgreSQL transaction. If the transaction fails, nothing is committed — there are no orphan records. The client gets `202 Accepted` before any outbound HTTP call happens.

**2. Durable background processing.** Workers are coordinated by Procrastinate, which uses PostgreSQL for persistent scheduling, competing job acquisition, heartbeats, and execution locks. No work is lost on process restarts — pending jobs survive in the database and resume automatically.

**3. Exponential backoff retries.** Failed deliveries (5xx, 429, timeouts, network errors) are retried up to 3 times with delays of 2, 4, and 8 seconds between attempts. The delays are scheduled in PostgreSQL — workers release their slots during backoff instead of sleeping. A valid `Retry-After` header can increase the delay (capped at 300s). Non-retryable errors (4xx, TLS failures, redirects) are terminal immediately.

**4. Per-domain rate limiting.** A token-bucket is stored per canonical hostname in PostgreSQL. Row-level locking (`SELECT ... FOR UPDATE`) ensures atomic token consumption across concurrent workers. Default: 2 tokens/second, capacity 1. A Procrastinate mutex lock serializes in-flight deliveries per domain. Rate-limit deferrals don't consume the retry budget.

**5. SSRF protection.** Outbound requests are restricted to public HTTP/HTTPS on ports 80/443. Private IPs, localhost, reserved ranges, link-local, and transition addresses are all blocked. DNS resolution is validated at the connector level via a custom `PublicResolver` to prevent DNS rebinding attacks. Redirects are disabled entirely.

**6. Crash recovery.** Workers write heartbeats every 5 seconds. A recovery loop checks for jobs belonging to stale or absent workers. Interrupted attempts are marked `unknown` (since the destination may have already processed the payload), and the retry budget is consumed conservatively. This is bounded at-least-once delivery, not exactly-once.

### Database Schema

Three application tables alongside Procrastinate's queue tables:

| Table | Purpose |
|---|---|
| `deliveries` | Job state, payload, destination, attempt count, timestamps |
| `delivery_attempts` | Per-attempt record: status, response code, duration, retry delay |
| `domain_buckets` | Token bucket state per hostname for rate limiting |

Schema is auto-initialized at startup via numbered, checksum-verified migrations under an advisory lock.

## API Reference

| Endpoint | Method | Auth | Description |
|---|---|---|---|
| `/api/webhooks/dispatch` | POST | `X-API-Key` | Accept and queue a webhook delivery |
| `/api/webhooks/jobs/{jobId}` | GET | `X-API-Key` | Get delivery status and attempt history |
| `/health/live` | GET | Public | Process liveness check |
| `/health/ready` | GET | Public | Database + worker readiness check |
| `/docs` | GET | Public | Interactive Swagger documentation |
| `/demo/receiver/{mode}` | POST | Public | Built-in test receivers (when enabled) |

### Request Format

```json
{
  "destinationUrl": "https://target-server.com/endpoint",
  "eventId": "evt_123456",
  "payload": { "event": "user.signup", "userId": "usr_999" }
}
```

- `eventId`: 1–128 characters, alphanumeric plus `_`, `.`, `:`, `-`
- `payload`: Any valid JSON object (no NaN/Infinity)
- `destinationUrl`: Public HTTP(S) URL, ports 80/443 only
- Extra fields, URL credentials, fragments, or private destinations → 422
- Missing/invalid API key → 401
- Body over 64 KiB → 413

### Response

```json
{
  "status": "queued",
  "jobId": "a-generated-uuid"
}
```

### Status Response

```json
{
  "jobId": "...",
  "eventId": "evt_123456",
  "status": "success",
  "attemptCount": 1,
  "createdAt": "2026-11-21T...",
  "updatedAt": "2026-11-21T...",
  "nextAttemptAt": null,
  "attempts": [
    {
      "number": 1,
      "status": "success",
      "response_code": 200,
      "error": null,
      "started_at": "...",
      "finished_at": "...",
      "duration_ms": 45,
      "retry_delay_seconds": null
    }
  ]
}
```

## Running Locally

### Prerequisites

- **Git**
- **Docker Desktop** with Compose
- **Python 3.12** (for running directly without Docker)

### Quick Start with Docker Compose

```bash
git clone https://github.com/RazzGourav/Distributed-Webhook-Dispatcher.git
cd Distributed-Webhook-Dispatcher
python scripts/init_env.py          # Generates .env with a random API key
docker compose up --build
```

Open **http://localhost:8000/docs** in your browser. Click **Authorize** and enter the `API_KEY` value from your `.env` file.

When running the full Compose stack, use `http://api:8000/demo/receiver/success` as the destination URL (`api` is the container's service name).

### Running Python Directly

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then:

```bash
python scripts/init_env.py          # Skip if .env already exists
docker compose up -d db             # Start PostgreSQL only
uv sync --frozen
uv run uvicorn dispatcher.main:create_app --factory --port 8000 --no-access-log
```

When running Python outside Docker, set `ALLOW_PRIVATE_NETWORKS=true` in `.env` for local testing, and use `http://127.0.0.1:8000/demo/receiver/success` as the destination.

`requirements.txt` is provided with pinned transitive dependencies and hashes. Docker uses `pip --require-hashes`. `uv.lock` is the source of truth; CI verifies the two stay in sync.

### Stopping

```bash
docker compose down          # Stop services, preserve database
docker compose down -v       # Stop services AND delete database volume
```

## Testing the API

These examples use Bash/Git Bash. On PowerShell use `curl.exe`, or use Swagger at `/docs`.

### Dispatch a Webhook

```bash
export BASE_URL=http://localhost:8000
export API_KEY='your-api-key-from-env-file'

curl -i "$BASE_URL/api/webhooks/dispatch" \
  -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' \
  -d '{"destinationUrl":"http://api:8000/demo/receiver/flaky","eventId":"evt_123456","payload":{"event":"user.signup","userId":"usr_999"}}'
```

### Check Delivery Status

```bash
curl "$BASE_URL/api/webhooks/jobs/REPLACE_WITH_JOB_ID" -H "X-API-Key: $API_KEY"
```

### PowerShell Equivalents

```powershell
$apiKey = "your-api-key-from-env-file"

# Dispatch
curl.exe -X POST "http://localhost:8000/api/webhooks/dispatch" `
  -H "X-API-Key: $apiKey" `
  -H "Content-Type: application/json" `
  -d '{\"destinationUrl\":\"http://api:8000/demo/receiver/flaky\",\"eventId\":\"evt_test\",\"payload\":{\"event\":\"user.signup\"}}'

# Check status
curl.exe "http://localhost:8000/api/webhooks/jobs/YOUR_JOB_ID" -H "X-API-Key: $apiKey"

# Health
curl.exe http://localhost:8000/health/ready
```

## Demo Scenarios

Set `DEMO_ENABLED=true` (enabled by default in Docker Compose). These built-in receivers simulate different destination behaviors:

| Mode | Receiver Behavior | Expected Result |
|---|---|---|
| `success` | Returns 200 | Delivered, 1 attempt |
| `flaky` | Returns 503 twice, then 200 | Delivered, 3 attempts (retries at 2s, 4s) |
| `fail` | Always returns 503 | Failed, 4 attempts (delays: 2s, 4s, 8s) |
| `rate-limit` | Always returns 429 | Failed, 4 attempts |
| `bad-request` | Returns 400 | Failed, 1 attempt (non-retryable) |
| `slow` | Waits 10 seconds, then 200 | 202 returns instantly; delivery completes after ~10s |

### Automated Smoke Test

The smoke script exercises all scenarios end-to-end:

```bash
# Local (reads API_KEY from .env automatically)
uv run python scripts/smoke.py --base-url http://localhost:8000 --receiver-base http://api:8000

# Against a live deployment
uv run python scripts/smoke.py --base-url https://YOUR-SERVICE.onrender.com --receiver-base https://YOUR-SERVICE.onrender.com
```

## Tests and CI

Tests run against a real PostgreSQL database with real HTTP receivers — no mocks for the core delivery path.

```bash
docker compose up -d db
docker compose exec db createdb -U dispatcher dispatcher_test
uv sync --frozen
export TEST_DATABASE_URL=postgresql://dispatcher:dispatcher@localhost:5432/dispatcher_test
uv run pytest --cov=dispatcher --cov-report=term-missing
uv run ruff check .
uv run ruff format --check .
```

PowerShell:

```powershell
docker compose exec db createdb -U dispatcher dispatcher_test
$env:TEST_DATABASE_URL = "postgresql://dispatcher:dispatcher@localhost:5432/dispatcher_test"
uv run pytest -q
```

The test suite covers:
- Transaction atomicity (rollback on queue failure leaves no orphan rows)
- Input validation and authentication (401, 413, 422)
- 10-second slow receiver proving non-blocking ingestion
- Full 2/4/8 second backoff with real timing assertions
- HTTP status classification (400 terminal, 302 terminal, 429 retried, 500 retried)
- Timeout detection and retry
- Token bucket atomicity across 20 concurrent acquisitions
- Two competing workers sharing domain rate limits (peak concurrency = 1)
- Job survival across API process restart
- Interrupted worker recovery with `unknown` attempt status
- Idempotent schema migrations

GitHub Actions CI runs against PostgreSQL 17 and verifies linting, formatting, and dependency export consistency.

## Deploy on Render

The repository includes a free-tier Blueprint (`render.yaml`) that provisions a web service and PostgreSQL database.

1. Push the repository (including `render.yaml`) to `main` on GitHub.
2. Go to [Render Blueprint](https://dashboard.render.com/blueprint/new) and connect your repository.
3. Review the two Free plans: one web service and one PostgreSQL database.
4. Apply the plan. Render auto-generates `API_KEY` and provides `DATABASE_URL`.
5. Wait for deployment to go live. Check `/health/ready` and `/docs`.
6. Find your API key on Render's Environment page.
7. Run the smoke script against the live URL.

Schema initialization runs at startup. Auto-deploy is disabled in the Blueprint for controlled changes.

### Hosted API Usage

Once deployed, test it the same way:

```bash
export BASE_URL=https://webhook-dispatcher-prib.onrender.com
export API_KEY='your-render-api-key'

# Health check
curl "$BASE_URL/health/ready"

# Dispatch (use the live URL for the demo receiver)
curl -i "$BASE_URL/api/webhooks/dispatch" \
  -H "X-API-Key: $API_KEY" -H 'Content-Type: application/json' \
  -d "{\"destinationUrl\":\"$BASE_URL/demo/receiver/flaky\",\"eventId\":\"evt_live_test\",\"payload\":{\"event\":\"test\"}}"

# Check status
curl "$BASE_URL/api/webhooks/jobs/JOB_ID" -H "X-API-Key: $API_KEY"
```

### Free Tier Limitations

- Render Free sleeps after 15 idle minutes. Cold-start takes ~30–60 seconds. Pending jobs resume when the service wakes.
- Free PostgreSQL expires after 30 days with 1 GB storage and no backups.
- The service runs a single process with both the API and an embedded worker.

### Production Considerations

For a production deployment beyond the free tier:
- Separate always-on API and worker processes (`EMBEDDED_WORKER=false` for the API, run `python -m dispatcher.run_worker` separately)
- Backed-up PostgreSQL with connection pooling
- Per-client API credentials and quotas
- Monitoring, alerting, and structured log aggregation
- Webhook payload signing (HMAC)
- Queue capacity admission control
- Egress firewall as an additional SSRF defense layer
- Reviewed data retention policy

## Configuration

| Variable | Default | Description |
|---|---|---|
| `API_KEY` | Required (≥16 chars) | Authentication for dispatch and status endpoints |
| `DATABASE_URL` | Local dev connection | PostgreSQL connection string |
| `EMBEDDED_WORKER` | `true` | Run the background worker alongside the API |
| `DEMO_ENABLED` | `false` | Enable built-in test receivers |
| `ALLOW_PRIVATE_NETWORKS` | `false` | Allow private IPs as destinations (local testing only) |
| `WORKER_CONCURRENCY` | `8` | Parallel delivery slots across different domains |
| `REQUEST_TIMEOUT` | `12` seconds | Outbound HTTP request timeout |
| `DOMAIN_RATE` | `2` per second | Token bucket refill rate per domain |
| `DOMAIN_BURST` | `1` | Token bucket capacity |
| `MAX_BODY_BYTES` | `65536` | Maximum incoming request body size |
| `RETENTION_DAYS` | `7` | Days to keep terminal delivery records |
| `LOG_LEVEL` | `INFO` | Application log level |

## Project Structure

```
├── dispatcher/
│   ├── main.py              # FastAPI app, routes, authentication, body limiter
│   ├── config.py            # Pydantic settings from environment
│   ├── models.py            # Request/response schemas with validation
│   ├── database.py          # Connection pool, migrations, query helpers
│   ├── worker.py            # Delivery logic, retry policy, session, recovery
│   ├── limiter.py           # Shared token-bucket rate limiter
│   ├── security.py          # URL validation, SSRF protection, DNS resolver
│   ├── runtime.py           # Startup/shutdown lifecycle management
│   ├── run_worker.py        # Standalone worker entrypoint
│   └── migrations/
│       └── 001_initial.sql  # Schema: deliveries, attempts, rate buckets
├── tests/
│   ├── conftest.py          # Fixtures: settings, pool, client, HTTP receiver
│   ├── test_dispatcher.py   # Integration tests with real PostgreSQL
│   └── test_security.py     # URL validation and DNS rebinding tests
├── scripts/
│   ├── init_env.py          # Generate .env with random API key
│   └── smoke.py             # End-to-end smoke test for all scenarios
├── .github/workflows/ci.yml # GitHub Actions: test, lint, format, dep check
├── compose.yaml             # Docker Compose: PostgreSQL + app
├── Dockerfile               # Production image (non-root, hashed deps)
├── render.yaml              # Render Blueprint for free-tier deployment
├── pyproject.toml            # Project metadata and dependencies
├── requirements.txt          # Pinned dependencies with hashes
└── uv.lock                   # Lockfile (source of truth)
```

## References

- [FastAPI](https://fastapi.tiangolo.com/)
- [Procrastinate](https://procrastinate.readthedocs.io/en/stable/)
- [Transactional Enqueue](https://procrastinate.readthedocs.io/en/stable/howto/production/external_connection.html)
- [psycopg Connection Pools](https://www.psycopg.org/psycopg3/docs/advanced/pool.html)
- [aiohttp Client](https://docs.aiohttp.org/en/stable/client_advanced.html)
- [Render Free-Tier Docs](https://render.com/docs/free)
