# Understand and defend this implementation

Start with a running local service and `/docs`. This guide teaches the system in
the order an event travels through it. Read the implementation alongside each
section; do not memorize claims you cannot demonstrate.

## 1. Explain the problem in 30 seconds

“The caller wants to send an event to another server. That server can be slow,
offline or rate-limited. My API saves the delivery request and returns 202. A
worker delivers it independently, retries transient failures, and limits traffic
to each hostname. PostgreSQL preserves the queue and attempt history across app
restarts.”

202 means **accepted for processing**, not delivered successfully. The status
endpoint is where the caller checks the final outcome.

## 2. Trace the request through the files

| File | Responsibility | What to inspect |
|---|---|---|
| `dispatcher/config.py` | Typed environment settings | Bounds and secret handling |
| `dispatcher/models.py` | API payload validation | Required fields and event ID constraints |
| `dispatcher/main.py` | Routes, authentication, body cap | The dispatch transaction |
| `dispatcher/database.py` | Pool and migrations | Advisory lock, migration checksum |
| `dispatcher/worker.py` | HTTP attempts and retries | `deliver`, `DeliveryRetry`, recovery |
| `dispatcher/limiter.py` | Shared token bucket | Row lock and database time |
| `dispatcher/security.py` | Safe destination resolution | Connector-level DNS validation |
| `dispatcher/runtime.py` | Startup/shutdown | Shared resource lifetime |
| `dispatcher/run_worker.py` | Standalone worker entrypoint | Same code, separate process |
| `tests/` | Behavioral evidence | Real database and HTTP receivers |

## 3. Why this stack?

**FastAPI/Pydantic:** request parsing, validation, OpenAPI and Swagger come from
libraries. There is no benefit to hand-writing a JSON schema validator.

**Procrastinate:** persists and schedules jobs using PostgreSQL, coordinates
competing workers, and provides execution locks and retry decisions. We avoid
writing a queue protocol, acknowledgment mechanism and worker scheduler ourselves.

**psycopg:** async PostgreSQL operations and transactions. The small data model
does not need a large ORM layer. Queries use parameters, not interpolated input.

**aiohttp:** asynchronous HTTP plus an injectable resolver. Its connector uses
the DNS addresses we validate, which matters for SSRF protection.

**One PostgreSQL instance:** holds the queue, business state, attempt history,
rate-limit state and heartbeats. A second system such as Redis would add a second
deployment and a transaction boundary. This is a scope choice, not a claim that
Redis or Celery are unsuitable technologies.

## 4. The transaction that makes 202 meaningful

Inside `dispatch`:

1. Validate URL syntax and known unsafe literal addresses.
2. Generate a public job UUID.
3. Open a database transaction.
4. Insert a delivery row with the payload.
5. Ask Procrastinate to enqueue using **that same connection**.
6. Store the internal queue ID and commit.
7. Return 202 with the public UUID.

If step 5 fails, the entire transaction rolls back. The tests deliberately force
that failure and assert there is no orphan delivery row.

Why not `FastAPI.BackgroundTasks` alone? A task only held in process memory is
lost when that process exits. An async function is not automatically a durable
queue. Here the durable record exists before the response is returned.

## 5. Follow one retry sequence

POST to `/demo/receiver/fail`. Inspect the job repeatedly:

| Attempt | Receiver response | Decision |
|---|---|---|
| 1 | 503 | Record attempt; schedule another after 2 seconds |
| 2 | 503 | Schedule after 4 seconds |
| 3 | 503 | Schedule after 8 seconds |
| 4 | 503 | Terminal failure; no fifth request |

The formula is `delay = 2 ** attempt_number` for attempt numbers 1, 2 and 3.
The wait begins after the failed HTTP attempt completes. HTTP execution and queue
latency are additional, so a failure sequence takes at least 14 seconds.

`Deferred` carries the wait duration to `DeliveryRetry`, which returns the
library's `RetryDecision`. The worker does not sleep through the delay. It gives
its slot back while the queue stores the future schedule.

Rate deferrals use the same scheduling facility, but do not increment
`deliveries.attempt_count`. Otherwise, a busy destination could exhaust retries
without receiving a single request.

`Retry-After` supports delta seconds and HTTP dates, with a 300-second cap. The
backoff is a minimum. Other 4xx usually indicate a request the receiver rejects;
repeating it immediately would not help. Redirects are terminal for safety.

## 6. Understand the token bucket mathematically

For a hostname, store `tokens` and `updated_at`. At admission time:

```text
refilled = min(capacity, old_tokens + elapsed_seconds * refill_rate)
```

If at least one token exists, consume one. Otherwise:

```text
wait_seconds = (1 - refilled) / refill_rate
```

With capacity 1 and rate 2/second, the bucket permits one dispatch now and then
refills one token after half a second. Capacity 1 avoids a burst of simultaneous
admissions. The separate queue lock serializes all HTTP calls for the hostname,
even when one is slow.

`SELECT ... FOR UPDATE` prevents two workers from reading the same token and
both spending it. PostgreSQL's clock provides one time source for this calculation.
The transaction is short and commits before network I/O. Holding a DB transaction
open for a ten-second HTTP request would waste connections and hold locks.

The quota is per exact normalized hostname. `api.example.com` and `example.com`
have different buckets. Paths, ports and scheme do not split a hostname's quota.
Rate limits coordinate dispatch starts; DNS/network delays are not controllable.

## 7. What “distributed” means here

Multiple worker processes may point to the same PostgreSQL database. Procrastinate
coordinates job acquisition, and both the mutex and token bucket are shared.
An in-memory Python dictionary would only coordinate tasks inside one process.

The evaluation deployment uses one process to fit free hosting. That does not
change the distributed coordination design. The test suite starts two worker
instances against the same database and checks that five jobs are delivered once
in the normal case, serialized and rate-limited.

For a real multi-process demo, run the API with `EMBEDDED_WORKER=false`, then run
`uv run python -m dispatcher.run_worker` in two other terminals with the same
settings. Give the database enough connection capacity first.

## 8. The crash window and exactly-once delivery

Consider this order:

1. Receiver processes an event.
2. Worker crashes before recording the 200 response.
3. The database still says the attempt is `sending`.

The dispatcher cannot know what happened remotely. It marks the attempt
`unknown` during recovery, then retries if budget remains. A successful receiver
may therefore see the event twice. No local database transaction can atomically
commit a third party's independent business transaction.

The receiver should store a unique event ID and perform its business update in
one transaction. The header `X-Webhook-Id` supplies that identity. This service
does **not** enforce ingestion deduplication: resubmitting the POST creates
another delivery. Be explicit about that distinction in the interview.

At most four attempt reservations are made, even if a crash occurs just before
sending. This conservative budget can mean fewer than four actual network calls.
It prevents uncontrolled retry storms but cannot guarantee successful delivery.

## 9. Security reasoning you should be able to explain

An API that accepts arbitrary URLs can be tricked into contacting cloud metadata
or private services. This is server-side request forgery (SSRF).

Checking a hostname once is insufficient: its DNS can return a public address
first and a private address later. `PublicResolver` validates the addresses used
by the actual connector. If any answer is non-public, the request is blocked.
IP literals are validated separately because a connector may skip DNS for them.
Redirects are disabled because a public server could redirect to a private URL.

API authentication prevents unauthenticated dispatch requests. It does not make
the destination trusted. Request-size and time limits bound individual operations.
Response bodies are never buffered because only the response status is required.

The development-only private-address override enables a Docker self-test. Leave
it false in any internet-facing deployment. For production, an egress firewall
provides another enforcement layer outside application code.

## 10. Prove behavior to the reviewer

Suggested five-minute demo:

1. Open `/health/ready`, then authorize Swagger.
2. Submit `success`, copy the job ID and show one successful attempt.
3. Submit `flaky`; show two retrying attempts followed by success.
4. Submit `slow`; show the 202 response before delivery finishes.
5. Submit `fail`; show four attempts, with delays 2, 4 and 8.
6. Show the concurrency test and transaction-rollback test.
7. Explain the free-host sleep limitation and exactly-once caveat yourself.

The smoke script automates steps 2–5. Local smoke latency is not a production
benchmark. Measure the deployed endpoint after warm-up and report actual results.

## 11. Common interview questions

**What if PostgreSQL is down when the API is called?**
It cannot durably accept work, so dispatch returns 503. The caller can retry.
If the transaction committed but the response was lost, a client retry can create
a duplicate: ingestion idempotency would be the next reliability enhancement.

**What if the receiver is slow?**
Only a bounded worker slot is occupied, with a 12-second request timeout. The
caller already received 202. The per-domain mutex stops concurrent pressure on
that receiver; other hostnames can use other slots.

**What if the worker goes away during a retry delay?**
The scheduled job remains in PostgreSQL. Another worker, or the restarted worker,
can execute it after its schedule becomes due.

**Why no jitter?**
The evaluation's exact 2/4/8 schedule is easy to verify, and the shared bucket
limits outbound traffic. Production policy could add bounded jitter on top of
the minimum backoff for large fleets.

**How are failures observable?**
The status endpoint exposes attempt records; structured logs correlate them by
job ID; readiness checks the database and worker heartbeat. Production would add
metrics for queue age, queue size, retry/failure rates and alert thresholds.

**Is this production-ready?**
It implements production-oriented patterns and tests, but the free deployment
has no always-on guarantee, database backups, or multi-tenant quota system.
Describe those limits honestly and show the upgrade path in README.

## 12. Exercises to check your understanding

1. Explain why returning 202 before committing the transaction is unsafe.
2. Predict the effect of changing bucket capacity from 1 to 5.
3. Explain why a 429 deferral should differ from a local bucket deferral.
4. Describe what a receiver must do to safely ignore duplicate event IDs.
5. Trace what happens if the fourth attempt is interrupted.
6. Name the test that would catch a non-atomic queue insert.
7. Explain why adding worker processes does not multiply the per-domain quota.

Once you can answer these from the code, rehearse the demo without this guide.
