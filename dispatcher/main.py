import asyncio
import hmac
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from starlette.types import ASGIApp, Receive, Scope, Send

from dispatcher.config import Settings
from dispatcher.database import fetch_one
from dispatcher.models import DispatchRequest, QueuedResponse
from dispatcher.runtime import runtime
from dispatcher.security import UnsafeDestination, validate_destination


class BodyLimit:
    def __init__(self, app: ASGIApp, limit: int):
        self.app, self.limit = app, limit

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        chunks, size = [], 0
        try:
            async with asyncio.timeout(10):
                while True:
                    message = await receive()
                    if message["type"] == "http.disconnect":
                        return
                    chunk = message.get("body", b"")
                    size += len(chunk)
                    if size > self.limit:
                        return await JSONResponse({"detail": "Request body too large"}, 413)(
                            scope, receive, send
                        )
                    chunks.append(chunk)
                    if not message.get("more_body", False):
                        break
        except TimeoutError:
            return await JSONResponse({"detail": "Request body timeout"}, 408)(scope, receive, send)
        consumed = False

        async def replay():
            nonlocal consumed
            if not consumed:
                consumed = True
                return {"type": "http.request", "body": b"".join(chunks), "more_body": False}
            return await receive()

        await self.app(scope, replay, send)


def create_app(settings: Settings | None = None):
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app):
        async with runtime(settings) as state:
            app.state.resources = state
            yield

    app = FastAPI(
        title="Distributed Webhook Dispatcher",
        version="1.0.0",
        description="Persistent delivery with exponential retries and shared per-domain limits.",
        lifespan=lifespan,
    )
    app.add_middleware(BodyLimit, limit=settings.max_body_bytes)

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request, exc):
        # Do not echo payload contents or non-standard floats in error responses.
        return JSONResponse(
            {
                "detail": [
                    {key: error[key] for key in ("loc", "msg", "type")} for error in exc.errors()
                ]
            },
            status_code=422,
        )

    key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

    async def authenticate(key: str | None = Depends(key_header)):
        if not key or not hmac.compare_digest(
            key.encode(), settings.api_key.get_secret_value().encode()
        ):
            raise HTTPException(401, "Invalid or missing API key")

    @app.get("/", include_in_schema=False)
    async def index():
        return {
            "service": "Distributed Webhook Dispatcher",
            "docs": "/docs",
            "health": "/health/ready",
        }

    @app.get("/health/live", tags=["Health"])
    async def live():
        return {"status": "alive"}

    @app.get("/health/ready", tags=["Health"])
    async def ready(request: Request):
        resources = request.app.state.resources
        try:
            async with resources["pool"].connection() as conn:
                heartbeat = await fetch_one(
                    conn,
                    """SELECT EXISTS(SELECT 1 FROM procrastinate_workers
                    WHERE last_heartbeat > now() - interval '60 seconds') AS alive""",
                )
            worker = resources["worker"]
            if not heartbeat["alive"] or (worker is not None and worker.done()):
                raise RuntimeError("Worker unavailable")
        except Exception:
            return JSONResponse({"status": "unavailable"}, 503)
        return {"status": "ready", "database": "ok", "worker": "ok"}

    @app.post(
        "/api/webhooks/dispatch",
        status_code=202,
        response_model=QueuedResponse,
        dependencies=[Depends(authenticate)],
        tags=["Webhooks"],
    )
    async def dispatch(body: DispatchRequest, request: Request):
        try:
            domain = validate_destination(str(body.destinationUrl), settings.allow_private_networks)
        except (UnsafeDestination, ValueError) as exc:
            raise HTTPException(422, str(exc)) from exc
        delivery_id = str(uuid4())
        resources = request.app.state.resources
        try:
            async with resources["pool"].connection() as conn, conn.transaction():
                await conn.execute(
                    """INSERT INTO deliveries
                    (id,event_id,destination_url,domain,payload) VALUES (%s,%s,%s,%s,%s)""",
                    (
                        delivery_id,
                        body.eventId,
                        str(body.destinationUrl),
                        domain,
                        Jsonb(body.payload),
                    ),
                )
                job_id = (
                    await resources["delivery_task"]
                    .configure(
                        connection=conn,
                        lock=f"domain:{domain}",
                        lock_mode="mutex",
                    )
                    .defer_async(delivery_id=delivery_id)
                )
                await conn.execute(
                    "UPDATE deliveries SET queue_id=%s WHERE id=%s", (job_id, delivery_id)
                )
        except Exception as exc:
            raise HTTPException(503, "Queue unavailable; retry the request") from exc
        return QueuedResponse(jobId=delivery_id)

    @app.get("/api/webhooks/jobs/{job_id}", dependencies=[Depends(authenticate)], tags=["Webhooks"])
    async def status(job_id: UUID, request: Request):
        async with request.app.state.resources["pool"].connection() as conn:
            row = await fetch_one(
                conn,
                """SELECT id,event_id,status,attempt_count,
                created_at,updated_at,next_attempt_at FROM deliveries WHERE id=%s""",
                (job_id,),
            )
            if not row:
                raise HTTPException(404, "Job not found or retention period elapsed")
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    """SELECT number,status,response_code,error,started_at,finished_at,
                    duration_ms,retry_delay_seconds FROM delivery_attempts
                    WHERE delivery_id=%s ORDER BY number""",
                    (job_id,),
                )
                attempts = await cursor.fetchall()
        return {
            "jobId": str(row["id"]),
            "eventId": row["event_id"],
            "status": row["status"],
            "attemptCount": row["attempt_count"],
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
            "nextAttemptAt": row["next_attempt_at"],
            "attempts": attempts,
        }

    if settings.demo_enabled:

        @app.post("/demo/receiver/{mode}", tags=["Demo"])
        async def demo(mode: str, request: Request):
            # This fixed-function receiver makes no outbound requests and stores no body.
            try:
                attempt = int(request.headers.get("X-Webhook-Attempt", "1"))
            except ValueError:
                raise HTTPException(400, "Invalid attempt header") from None
            if mode == "slow":
                await asyncio.sleep(10)
            codes = {
                "success": 200,
                "slow": 200,
                "fail": 503,
                "rate-limit": 429,
                "bad-request": 400,
                "flaky": 503 if attempt < 3 else 200,
            }
            if mode not in codes:
                raise HTTPException(404, "Unknown receiver mode")
            return JSONResponse({"received": True}, codes[mode])

    return app
