"""Queue orchestration stays in Procrastinate; delivery policy stays here."""

import asyncio
import logging
import math
import time
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

import aiohttp
import procrastinate
import psycopg
from psycopg_pool import PoolTimeout

from dispatcher.database import fetch_one
from dispatcher.limiter import acquire_token
from dispatcher.security import PublicResolver, UnsafeDestination, validate_destination

log = logging.getLogger(__name__)
MAX_ATTEMPTS = 4


class Deferred(Exception):
    def __init__(self, seconds: float):
        super().__init__("Delivery deferred")
        self.seconds = max(0.01, seconds)


class DeliveryRetry(procrastinate.BaseRetryStrategy):
    def get_retry_decision(self, *, exception, job):
        if isinstance(exception, Deferred):
            return procrastinate.RetryDecision(retry_in={"seconds": exception.seconds})
        if isinstance(exception, (psycopg.OperationalError, psycopg.InterfaceError, PoolTimeout)):
            # Infrastructure outages must not discard a previously accepted event.
            return procrastinate.RetryDecision(retry_in={"seconds": 5})
        return None


def retry_delay(attempt: int, header: str | None = None) -> float:
    delay = float(2**attempt)
    if header:
        try:
            seconds = float(header)
        except ValueError:
            try:
                seconds = (parsedate_to_datetime(header) - datetime.now(UTC)).total_seconds()
            except (ValueError, TypeError, OverflowError):
                seconds = 0
        # Bound malicious or accidentally enormous Retry-After values.
        if not math.isnan(seconds):
            delay = max(delay, min(300, max(0, seconds)))
    return delay


def make_session(settings):
    connector = aiohttp.TCPConnector(
        resolver=None if settings.allow_private_networks else PublicResolver(),
        use_dns_cache=False,
        limit=settings.worker_concurrency,
        force_close=True,
    )
    return aiohttp.ClientSession(
        connector=connector,
        timeout=aiohttp.ClientTimeout(total=settings.request_timeout),
        trust_env=False,
        auto_decompress=False,
    )


async def deliver(pool, session, settings, delivery_id: str):
    async with pool.connection() as conn:
        row = await fetch_one(conn, "SELECT * FROM deliveries WHERE id=%s", (delivery_id,))
    if not row or row["status"] in {"success", "failed"}:
        return

    # A previous worker may have died after sending, before recording the result.
    # Count that reservation; do not pretend the receiver did not see the request.
    if row["status"] == "sending":
        async with pool.connection() as conn, conn.transaction():
            await conn.execute(
                """UPDATE delivery_attempts SET status='unknown',
                error='worker_interrupted',finished_at=clock_timestamp()
                WHERE delivery_id=%s AND status='sending'""",
                (delivery_id,),
            )
            await conn.execute(
                """UPDATE deliveries SET status=%s,
                next_attempt_at=CASE WHEN %s::float IS NULL THEN NULL
                    ELSE clock_timestamp() + %s * interval '1 second' END,
                updated_at=clock_timestamp() WHERE id=%s""",
                (
                    "failed" if row["attempt_count"] >= MAX_ATTEMPTS else "retrying",
                    None
                    if row["attempt_count"] >= MAX_ATTEMPTS
                    else retry_delay(row["attempt_count"]),
                    retry_delay(row["attempt_count"]),
                    delivery_id,
                ),
            )
        if row["attempt_count"] >= MAX_ATTEMPTS:
            return
        raise Deferred(retry_delay(row["attempt_count"]))

    if row["next_attempt_at"]:
        remaining = (row["next_attempt_at"] - datetime.now(UTC)).total_seconds()
        if remaining > 0:
            raise Deferred(remaining)
    wait = await acquire_token(pool, row["domain"], settings.domain_rate, settings.domain_burst)
    if wait:
        raise Deferred(wait)

    number = row["attempt_count"] + 1
    async with pool.connection() as conn, conn.transaction():
        await conn.execute(
            """UPDATE deliveries SET status='sending',attempt_count=%s,
            next_attempt_at=NULL,updated_at=clock_timestamp() WHERE id=%s""",
            (number, delivery_id),
        )
        await conn.execute(
            """INSERT INTO delivery_attempts(delivery_id,number,status)
            VALUES (%s,%s,'sending')""",
            (delivery_id, number),
        )

    started = time.monotonic()
    response_code, error, retry_after = None, None, None
    retryable = False
    try:
        validate_destination(row["destination_url"], settings.allow_private_networks)
        async with session.post(
            row["destination_url"],
            json=row["payload"],
            allow_redirects=False,
            headers={
                "X-Webhook-Id": row["event_id"],
                "X-Webhook-Job-Id": delivery_id,
                "X-Webhook-Attempt": str(number),
                "User-Agent": "WebhookDispatcher/1.0",
            },
        ) as response:
            response_code = response.status
            retryable = response_code == 429 or 500 <= response_code <= 599
            retry_after = response.headers.get("Retry-After") if retryable else None
            # A webhook ACK is its HTTP status; do not buffer an untrusted body.
            if not 200 <= response_code <= 299:
                error = f"http_{response_code}"
    except UnsafeDestination:
        error = "unsafe_destination"
    except (aiohttp.ClientSSLError, aiohttp.InvalidURL):
        error = "invalid_tls_or_url"
    except (TimeoutError, aiohttp.ClientError, OSError) as exc:
        error = "timeout" if isinstance(exc, TimeoutError) else "network_error"
        retryable = True

    will_retry = retryable and number < MAX_ATTEMPTS
    status = "retrying" if will_retry else ("failed" if error else "success")
    delay = retry_delay(number, retry_after) if will_retry else None
    duration = round((time.monotonic() - started) * 1000)
    async with pool.connection() as conn, conn.transaction():
        await conn.execute(
            """UPDATE delivery_attempts SET status=%s,response_code=%s,
            error=%s,finished_at=clock_timestamp(),duration_ms=%s,retry_delay_seconds=%s
            WHERE delivery_id=%s AND number=%s""",
            (status, response_code, error, duration, delay, delivery_id, number),
        )
        await conn.execute(
            """UPDATE deliveries SET status=%s,
            next_attempt_at=CASE WHEN %s::float IS NULL THEN NULL
                ELSE clock_timestamp() + %s * interval '1 second' END,
            updated_at=clock_timestamp() WHERE id=%s""",
            (status, delay, delay, delivery_id),
        )
    log.info(
        "delivery_attempt",
        extra={
            "job_id": delivery_id,
            "attempt": number,
            "status": status,
            "response_code": response_code,
            "duration_ms": duration,
        },
    )
    if will_retry:
        raise Deferred(delay)


def make_queue(pool, session, settings):
    queue = procrastinate.App(connector=procrastinate.PsycopgConnector())

    @queue.task(name="deliver_webhook", retry=DeliveryRetry())
    async def delivery_task(delivery_id: str):
        try:
            await deliver(pool, session, settings, delivery_id)
        except (Deferred, psycopg.OperationalError, psycopg.InterfaceError, PoolTimeout):
            raise
        except Exception as exc:
            log.error(
                "delivery_internal_error",
                extra={"job_id": delivery_id, "error_type": type(exc).__name__},
            )
            async with pool.connection() as conn, conn.transaction():
                await conn.execute(
                    """UPDATE delivery_attempts SET status='unknown',
                    error='internal_error',finished_at=clock_timestamp()
                    WHERE delivery_id=%s AND status='sending'""",
                    (delivery_id,),
                )
                await conn.execute(
                    """UPDATE deliveries SET status='failed',next_attempt_at=NULL,
                    updated_at=clock_timestamp() WHERE id=%s AND status NOT IN ('success','failed')""",
                    (delivery_id,),
                )
            raise

    return queue, delivery_task


async def maintenance(pool, queue, settings):
    """Recover abandoned jobs and prune bounded history; coordinate instances."""
    while True:
        try:
            async with pool.connection() as conn, conn.transaction():
                locked = await fetch_one(conn, "SELECT pg_try_advisory_xact_lock(70261002) AS ok")
                if locked["ok"]:
                    # Library retry operation uses its own connection. Advisory
                    # lock prevents competing recovery loops requeuing twice.
                    jobs = await queue.job_manager.get_stalled_jobs(
                        task_name="deliver_webhook", seconds_since_heartbeat=60
                    )
                    for job in jobs:
                        await queue.job_manager.retry_job_by_id_async(job.id, datetime.now(UTC))
                        log.warning("recovered_job", extra={"queue_id": job.id})
                    await conn.execute(
                        """DELETE FROM deliveries WHERE status IN ('success','failed')
                        AND updated_at < now() - %s * interval '1 day'""",
                        (settings.retention_days,),
                    )
                    await conn.execute(
                        "DELETE FROM domain_buckets WHERE updated_at < now() - interval '1 day'"
                    )
            await queue.job_manager.delete_old_jobs(
                nb_hours=settings.retention_days * 24,
                include_failed=True,
                include_cancelled=True,
                include_aborted=True,
            )
        except Exception:
            # Do not emit credentials, payloads, or destination query strings.
            log.error("maintenance_failed")
        await asyncio.sleep(15)
