import asyncio
import logging
from contextlib import asynccontextmanager, suppress

from pythonjsonlogger.json import JsonFormatter

from dispatcher.database import make_pool, migrate
from dispatcher.worker import maintenance, make_queue, make_session


def configure_logging(level):
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    logging.basicConfig(level=level, handlers=[handler], force=True)
    # Third-party debug messages can include job arguments or DB connection info.
    logging.getLogger("procrastinate").setLevel(logging.WARNING)
    logging.getLogger("psycopg.pool").setLevel(logging.WARNING)


@asynccontextmanager
async def runtime(settings):
    configure_logging(settings.log_level)
    async with make_pool(settings) as pool:
        await pool.wait()
        await migrate(pool)
        async with make_session(settings) as session:
            queue, task = make_queue(pool, session, settings)
            async with queue.open_async(pool):
                worker = recovery = None
                if settings.embedded_worker:
                    worker = asyncio.create_task(
                        queue.run_worker_async(
                            concurrency=settings.worker_concurrency,
                            install_signal_handlers=False,
                            shutdown_graceful_timeout=15,
                            update_heartbeat_interval=5,
                            stalled_worker_timeout=60,
                            fetch_job_polling_interval=0.5,
                        ),
                        name="delivery-worker",
                    )
                    recovery = asyncio.create_task(maintenance(pool, queue, settings))
                try:
                    yield {"pool": pool, "queue": queue, "delivery_task": task, "worker": worker}
                finally:
                    for running in (recovery, worker):
                        if running:
                            running.cancel()
                    for running in (recovery, worker):
                        if running:
                            with suppress(asyncio.CancelledError):
                                await running
