import os

import pytest
from aiohttp import web
from httpx import ASGITransport, AsyncClient

from dispatcher.config import Settings
from dispatcher.database import make_pool, migrate
from dispatcher.main import create_app

KEY = "test-api-key-not-for-deployment"


@pytest.fixture
def settings():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.fail(
            "Set TEST_DATABASE_URL to a disposable PostgreSQL database; tests erase its data"
        )
    return Settings(
        database_url=url,
        api_key=KEY,
        allow_private_networks=True,
        embedded_worker=False,
        demo_enabled=True,
    )


@pytest.fixture
async def pool(settings):
    async with make_pool(settings) as pool:
        await migrate(pool)
        async with pool.connection() as conn:
            await conn.execute(
                "TRUNCATE deliveries, domain_buckets, procrastinate_jobs, procrastinate_workers CASCADE"
            )
        yield pool


@pytest.fixture
async def client(settings, pool):
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test", headers={"X-API-Key": KEY}
        ) as client:
            yield client, app


@pytest.fixture
async def receiver():
    runners = []

    async def start(handler):
        app = web.Application()
        app.router.add_post("/{path:.*}", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        runners.append(runner)
        return f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}/hook"

    yield start
    for runner in runners:
        await runner.cleanup()
