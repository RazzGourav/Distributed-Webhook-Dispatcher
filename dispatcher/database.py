import asyncio
import hashlib
from pathlib import Path

import procrastinate
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from dispatcher.config import Settings


def make_pool(settings: Settings) -> AsyncConnectionPool:
    async def configure(conn):
        # Keep autocommit out of pool.kwargs: the queue's LISTEN connection
        # explicitly supplies it when cloning connection parameters.
        await conn.set_autocommit(True)

    return AsyncConnectionPool(
        settings.database_url.get_secret_value(),
        open=False,
        min_size=1,
        max_size=12,
        timeout=5,
        configure=configure,
        kwargs={
            "connect_timeout": 5,
            "options": "-c statement_timeout=10000 -c lock_timeout=5000",
        },
    )


async def fetch_one(conn, sql, params=()):
    async with conn.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(sql, params)
        return await cursor.fetchone()


async def migrate(pool):
    async with pool.connection() as conn, conn.transaction():
        # Serialize startup migrations, including two instances starting together.
        await conn.execute("SELECT pg_advisory_xact_lock(70261001)")
        row = await fetch_one(conn, "SELECT to_regclass('procrastinate_jobs') AS name")
        if not row["name"]:
            await conn.execute(procrastinate.schema.SchemaManager.get_schema())
        await conn.execute("""CREATE TABLE IF NOT EXISTS schema_migrations (
            name text PRIMARY KEY, checksum text NOT NULL,
            applied_at timestamptz NOT NULL DEFAULT now())""")
        for path in sorted(Path(__file__).with_name("migrations").glob("*.sql")):
            content = path.read_text()
            checksum = hashlib.sha256(content.encode()).hexdigest()
            existing = await fetch_one(
                conn, "SELECT checksum FROM schema_migrations WHERE name=%s", (path.name,)
            )
            if existing:
                if existing["checksum"] != checksum:
                    raise RuntimeError(f"Applied migration changed: {path.name}")
                continue
            await conn.execute(content)
            await conn.execute(
                "INSERT INTO schema_migrations(name, checksum) VALUES (%s,%s)",
                (path.name, checksum),
            )


async def main():
    async with make_pool(Settings()) as pool:
        await migrate(pool)


if __name__ == "__main__":
    asyncio.run(main())
