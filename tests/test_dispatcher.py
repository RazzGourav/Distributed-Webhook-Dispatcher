import asyncio
import time
from contextlib import suppress

import pytest
from aiohttp import web

from dispatcher.database import fetch_one, migrate
from dispatcher.limiter import acquire_token
from dispatcher.worker import make_queue, make_session


async def submit(client, url="https://example.com/hook", event="evt_123"):
    return await client.post(
        "/api/webhooks/dispatch",
        json={
            "destinationUrl": url,
            "eventId": event,
            "payload": {"event": "user.signup", "userId": "usr_999"},
        },
    )


async def wait_done(client, job_id, timeout=25):
    async with asyncio.timeout(timeout):
        while True:
            response = await client.get(f"/api/webhooks/jobs/{job_id}")
            data = response.json()
            if data["status"] in {"success", "failed"}:
                return data
            await asyncio.sleep(0.025)


async def run_worker(app):
    return asyncio.create_task(
        app.state.resources["queue"].run_worker_async(
            concurrency=8,
            install_signal_handlers=False,
            fetch_job_polling_interval=0.02,
            shutdown_graceful_timeout=1,
        )
    )


async def stop(task):
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


async def test_validation_auth_size_and_status(client):
    http, app = client
    assert (
        await http.post("/api/webhooks/dispatch", json={}, headers={"X-API-Key": "wrong"})
    ).status_code == 401
    assert (await http.post("/api/webhooks/dispatch", json={})).status_code == 422
    assert (await http.post("/api/webhooks/dispatch", content=b"x" * 70000)).status_code == 413
    assert (await http.get("/api/webhooks/jobs/not-a-uuid")).status_code == 422
    assert (
        await http.get("/api/webhooks/jobs/00000000-0000-0000-0000-000000000000")
    ).status_code == 404
    result = await submit(http)
    assert result.status_code == 202
    assert result.json()["status"] == "queued"
    status = await http.get(f"/api/webhooks/jobs/{result.json()['jobId']}")
    assert status.json()["attemptCount"] == 0
    assert (await http.get("/health/ready")).status_code == 503


async def test_transaction_rolls_back_if_enqueue_fails(client, monkeypatch):
    http, app = client
    task = app.state.resources["delivery_task"]

    def broken(**kwargs):
        raise RuntimeError("simulated queue failure")

    monkeypatch.setattr(task, "configure", broken)
    assert (await submit(http)).status_code == 503
    async with app.state.resources["pool"].connection() as conn:
        count = await fetch_one(conn, "SELECT count(*) AS n FROM deliveries")
    assert count["n"] == 0


async def test_api_returns_before_ten_second_receiver_finishes(client, receiver):
    http, app = client
    started = asyncio.Event()
    received = {}

    async def slow(request):
        received.update(payload=await request.json(), event=request.headers["X-Webhook-Id"])
        started.set()
        await asyncio.sleep(10)
        return web.Response(status=200)

    url = await receiver(slow)
    worker = await run_worker(app)
    try:
        before = time.monotonic()
        result = await submit(http, url)
        elapsed = time.monotonic() - before
        assert result.status_code == 202
        assert elapsed < 1, f"Ingestion blocked for {elapsed:.2f}s"
        await asyncio.wait_for(started.wait(), 2)
        assert (await http.get("/health/ready")).status_code == 200
        job = await wait_done(http, result.json()["jobId"])
        assert job["status"] == "success"
        assert job["attemptCount"] == 1
        assert received == {
            "payload": {"event": "user.signup", "userId": "usr_999"},
            "event": "evt_123",
        }
    finally:
        await stop(worker)


async def test_four_attempts_with_real_2_4_8_backoff(client, receiver):
    http, app = client
    arrivals = []

    async def fail(request):
        arrivals.append(time.monotonic())
        return web.Response(status=503)

    url = await receiver(fail)
    worker = await run_worker(app)
    try:
        result = await submit(http, url)
        job = await wait_done(http, result.json()["jobId"])
        assert job["status"] == "failed"
        assert job["attemptCount"] == len(arrivals) == 4
        assert [a["response_code"] for a in job["attempts"]] == [503] * 4
        assert [a["retry_delay_seconds"] for a in job["attempts"]] == [2, 4, 8, None]
        assert all(
            b - a >= delay for a, b, delay in zip(arrivals, arrivals[1:], [2, 4, 8], strict=False)
        )
    finally:
        await stop(worker)


@pytest.mark.parametrize("code,expected", [(400, 1), (302, 1), (429, 3), (500, 3)])
async def test_retry_classification_and_no_redirects(client, receiver, monkeypatch, code, expected):
    http, app = client
    calls = 0

    async def target(request):
        nonlocal calls
        calls += 1
        return web.Response(
            status=200 if calls == 3 else code, headers={"Location": "http://127.0.0.1:1/forbidden"}
        )

    monkeypatch.setattr("dispatcher.worker.retry_delay", lambda *args: 0.01)
    url = await receiver(target)
    worker = await run_worker(app)
    try:
        job = await wait_done(http, (await submit(http, url)).json()["jobId"])
        assert calls == job["attemptCount"] == expected
        assert job["status"] == ("success" if expected == 3 else "failed")
    finally:
        await stop(worker)


async def test_timeout_is_logged_and_retried(settings, pool, receiver, monkeypatch):
    from uuid import uuid4

    from psycopg.types.json import Jsonb

    from dispatcher.worker import Deferred, deliver

    settings.request_timeout = 0.03

    async def slow(request):
        await asyncio.sleep(0.1)
        return web.Response(status=200)

    url = await receiver(slow)
    identifier = str(uuid4())
    async with pool.connection() as conn:
        await conn.execute(
            """INSERT INTO deliveries(id,event_id,destination_url,domain,payload)
            VALUES (%s,'timeout',%s,'127.0.0.1',%s)""",
            (identifier, url, Jsonb({})),
        )
    async with make_session(settings) as session:
        with pytest.raises(Deferred):
            await deliver(pool, session, settings, identifier)
    async with pool.connection() as conn:
        row = await fetch_one(
            conn, "SELECT * FROM delivery_attempts WHERE delivery_id=%s", (identifier,)
        )
    assert row["error"] == "timeout"
    assert row["status"] == "retrying"
    assert row["response_code"] is None


async def test_token_bucket_atomic_across_connections(pool):
    decisions = await asyncio.gather(
        *(acquire_token(pool, "same.example", 2, 1) for _ in range(20))
    )
    assert sum(delay == 0 for delay in decisions) == 1
    assert await acquire_token(pool, "different.example", 2, 1) == 0


async def test_two_workers_share_domain_limits_and_do_not_double_deliver(
    client, settings, receiver
):
    http, app = client
    arrivals, ids = [], []
    active = peak = 0

    async def target(request):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        arrivals.append(time.monotonic())
        ids.append(request.headers["X-Webhook-Job-Id"])
        await asyncio.sleep(0.05)
        active -= 1
        return web.Response(status=200)

    url = await receiver(target)
    pool = app.state.resources["pool"]
    async with make_session(settings) as session:
        other, _ = make_queue(pool, session, settings)
        async with other.open_async(pool):
            workers = [
                await run_worker(app),
                asyncio.create_task(
                    other.run_worker_async(
                        concurrency=8,
                        install_signal_handlers=False,
                        fetch_job_polling_interval=0.02,
                    )
                ),
            ]
            try:
                responses = await asyncio.gather(*(submit(http, url, f"evt_{n}") for n in range(5)))
                jobs = await asyncio.gather(
                    *(wait_done(http, r.json()["jobId"]) for r in responses)
                )
                assert all(j["status"] == "success" and j["attemptCount"] == 1 for j in jobs)
                assert len(ids) == len(set(ids)) == 5
                assert peak == 1
                assert all(b - a >= 0.47 for a, b in zip(arrivals, arrivals[1:], strict=False))
            finally:
                for worker in workers:
                    await stop(worker)


async def test_jobs_survive_api_restart_and_worker_absence(client, settings, receiver):
    from httpx import ASGITransport, AsyncClient

    from dispatcher.main import create_app
    from tests.conftest import KEY

    http, _ = client

    async def ok(request):
        return web.Response(status=200)

    result = await submit(http, await receiver(ok))
    replacement = create_app(settings.model_copy(update={"embedded_worker": True}))
    async with replacement.router.lifespan_context(replacement):
        async with AsyncClient(
            transport=ASGITransport(app=replacement),
            base_url="http://test",
            headers={"X-API-Key": KEY},
        ) as second:
            assert (await wait_done(second, result.json()["jobId"]))["status"] == "success"


async def test_interrupted_attempt_recovered_with_unknown_history(client, settings, receiver):
    from dispatcher.worker import maintenance

    http, app = client

    async def ok(request):
        return web.Response(status=200)

    result = await submit(http, await receiver(ok))
    identifier = result.json()["jobId"]
    resources = app.state.resources
    async with resources["pool"].connection() as conn, conn.transaction():
        await conn.execute(
            "UPDATE deliveries SET status='sending',attempt_count=1 WHERE id=%s", (identifier,)
        )
        await conn.execute(
            "INSERT INTO delivery_attempts(delivery_id,number,status) VALUES (%s,1,'sending')",
            (identifier,),
        )
        await conn.execute(
            "UPDATE procrastinate_jobs SET status='doing',worker_id=NULL WHERE id=(SELECT queue_id FROM deliveries WHERE id=%s)",
            (identifier,),
        )
    recovery = asyncio.create_task(maintenance(resources["pool"], resources["queue"], settings))
    worker = await run_worker(app)
    try:
        job = await wait_done(http, identifier)
        assert job["status"] == "success"
        assert [a["status"] for a in job["attempts"]] == ["unknown", "success"]
    finally:
        await stop(recovery)
        await stop(worker)


async def test_migrations_are_repeatable(pool):
    await migrate(pool)
    await migrate(pool)


async def test_nonfinite_json_rejected(client):
    http, _ = client
    response = await http.post(
        "/api/webhooks/dispatch",
        content='{"destinationUrl":"https://example.com","eventId":"nan","payload":{"value":NaN}}',
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 422


async def test_fourth_interrupted_attempt_is_terminal(client, settings):
    from dispatcher.worker import deliver, make_session

    http, app = client
    identifier = (await submit(http)).json()["jobId"]
    pool = app.state.resources["pool"]
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE deliveries SET status='sending',attempt_count=4 WHERE id=%s", (identifier,)
        )
        await conn.execute(
            "INSERT INTO delivery_attempts(delivery_id,number,status) VALUES (%s,4,'sending')",
            (identifier,),
        )
    async with make_session(settings) as session:
        await deliver(pool, session, settings, identifier)
    job = (await http.get(f"/api/webhooks/jobs/{identifier}")).json()
    assert job["status"] == "failed"
    assert job["nextAttemptAt"] is None
    assert job["attemptCount"] == 4
    assert job["attempts"][0]["status"] == "unknown"


async def test_internal_error_is_terminal_and_visible(client, monkeypatch):
    http, app = client

    async def broken(*args):
        raise RuntimeError("simulated programming failure")

    monkeypatch.setattr("dispatcher.worker.deliver", broken)
    worker = await run_worker(app)
    try:
        job = await wait_done(http, (await submit(http)).json()["jobId"])
        assert job["status"] == "failed"
        assert job["attemptCount"] == 0
    finally:
        await stop(worker)
