import socket
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import pytest
from aiohttp.resolver import ThreadedResolver

from dispatcher.security import PublicResolver, UnsafeDestination, validate_destination
from dispatcher.worker import retry_delay


@pytest.mark.parametrize(
    "url",
    [
        "file:///etc/passwd",
        "http://localhost",
        "http://127.0.0.1",
        "http://10.0.0.1",
        "http://169.254.169.254/latest",
        "http://[::1]",
        "http://[::ffff:127.0.0.1]",
        "https://example.com:8080",
        "https://user:password@example.com",
        "https://example.com/#x",
        "http://224.0.0.1",
        "http://internal.local",
        "http://[2002:7f00:1::]",
    ],
)
def test_unsafe_urls(url):
    with pytest.raises(UnsafeDestination):
        validate_destination(url)


def test_domain_normalization():
    assert validate_destination("https://EXAMPLE.com./path?x=1") == "example.com"
    assert validate_destination("http://127.0.0.1:9000", True) == "127.0.0.1"


async def test_rebinding_and_mixed_dns_answers(monkeypatch):
    async def resolve(*args, **kwargs):
        return [{"host": "1.1.1.1"}, {"host": "127.0.0.1"}]

    monkeypatch.setattr(ThreadedResolver, "resolve", resolve)
    resolver = PublicResolver()
    with pytest.raises(UnsafeDestination):
        await resolver.resolve("example.com", 443, socket.AF_INET)
    await resolver.close()


def test_retry_schedule_and_retry_after():
    assert [retry_delay(n) for n in (1, 2, 3)] == [2, 4, 8]
    assert retry_delay(1, "10") == 10
    assert retry_delay(1, "999999") == 300
    assert retry_delay(1, "garbage") == 2
    assert retry_delay(1, "NaN") == 2
    assert retry_delay(1, "-10") == 2
    assert 8 <= retry_delay(1, format_datetime(datetime.now(UTC) + timedelta(seconds=10))) <= 10
