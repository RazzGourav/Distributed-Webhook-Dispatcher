# Implementation plan

## Acceptance criteria

- `POST /api/webhooks/dispatch` validates destinationUrl, eventId and payload;
  commits a persistent job and returns HTTP 202 without making the delivery call.
- A worker POSTs the payload, retries 5xx/429/network failures/timeouts after
  2, 4, 8 seconds, and records all attempts. Maximum four outbound attempts.
- A PostgreSQL token bucket and a queue lock coordinate per-host traffic across
  worker processes. Rate deferrals do not consume delivery attempts.
- Public Render deployment, GitHub repository, curl examples and walkthrough.

## Architecture

Python 3.12, FastAPI/Pydantic, Procrastinate, PostgreSQL/psycopg, aiohttp.
Procrastinate provides persistent scheduling, competing workers, locks and
worker heartbeats. One database transaction inserts the business record and
queue job, avoiding a dual-write gap. A bounded worker runs alongside the API
on Render Free; the same worker can run separately for always-on deployments.

## Build sequence

1. Configuration, versioned schema, queue integration and API validation.
2. Delivery worker, retry policy, shared token bucket and safe HTTP transport.
3. Status/history endpoint, health checks and reproducible demo receiver.
4. Real PostgreSQL integration tests, linting, lockfile and CI.
5. Docker Compose, Render blueprint, README and interview walkthrough.
6. Publish/deploy once account access is available; test the actual live URL.

## Deliberate boundaries

No frontend, blockchain, Redis, or Kubernetes is required for this assignment.
An API key protects dispatch/status. Outbound destinations must be public HTTP(S),
with DNS checked at connection time and redirects disabled. API request size and
worker concurrency are bounded. Delivery is at-least-once, not exactly-once;
receivers should deduplicate the event ID. Four-attempt budget still applies to
interrupted attempts whose downstream outcome cannot be known.

Render Free sleeps after inactivity; pending work resumes when the service wakes.
Render Free PostgreSQL expires after 30 days and has no backups. This is a
production-oriented implementation with an evaluation deployment, not a claim
of production availability or unlimited capacity.
