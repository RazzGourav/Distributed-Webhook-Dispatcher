from dispatcher.database import fetch_one


async def acquire_token(pool, domain: str, rate: float, burst: int) -> float:
    """Return 0 on admission, otherwise seconds until a token is available.

    Row locking and the database clock make this shared across all workers.
    Never hold this transaction open during HTTP I/O.
    """
    async with pool.connection() as conn, conn.transaction():
        await conn.execute(
            "INSERT INTO domain_buckets(domain,tokens) VALUES (%s,%s) ON CONFLICT DO NOTHING",
            (domain, burst),
        )
        bucket = await fetch_one(
            conn, "SELECT * FROM domain_buckets WHERE domain=%s FOR UPDATE", (domain,)
        )
        now = (await fetch_one(conn, "SELECT clock_timestamp() AS now"))["now"]
        elapsed = max(0, (now - bucket["updated_at"]).total_seconds())
        tokens = min(float(burst), bucket["tokens"] + elapsed * rate)
        wait = max(0, (1 - tokens) / rate)
        await conn.execute(
            "UPDATE domain_buckets SET tokens=%s,updated_at=%s WHERE domain=%s",
            (tokens - 1 if wait == 0 else tokens, now, domain),
        )
        return wait
