"""Validate addresses again inside the connector to prevent DNS rebinding."""

import ipaddress
import socket
from urllib.parse import urlsplit

from aiohttp.resolver import ThreadedResolver


class UnsafeDestination(ValueError):
    pass


def public_address(address: str) -> bool:
    ip = ipaddress.ip_address(address)
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped:
        ip = ip.ipv4_mapped
    if isinstance(ip, ipaddress.IPv6Address) and (ip.sixtofour or ip.teredo):
        return False
    return ip.is_global and not ip.is_multicast and not ip.is_reserved


def validate_destination(url: str, allow_private: bool = False) -> str:
    parsed = urlsplit(url)
    host = (parsed.hostname or "").lower().rstrip(".")
    if parsed.scheme not in {"http", "https"} or not host:
        raise UnsafeDestination("Use a complete HTTP or HTTPS URL")
    if parsed.username or parsed.password or parsed.fragment:
        raise UnsafeDestination("Credentials and fragments are not allowed in URLs")
    if not allow_private and parsed.port not in {None, 80, 443}:
        raise UnsafeDestination("Only ports 80 and 443 are allowed")
    if not allow_private:
        if host == "localhost" or host.endswith((".localhost", ".local", ".internal")):
            raise UnsafeDestination("Destination must be public")
        try:
            ipaddress.ip_address(host)
        except ValueError:
            pass  # DNS is checked by PublicResolver at connection time.
        else:
            if not public_address(host):
                raise UnsafeDestination("Destination must be public")
    return host.encode("idna").decode("ascii")


class PublicResolver(ThreadedResolver):
    async def resolve(self, host, port=0, family=socket.AF_INET):
        records = await super().resolve(host, port, family)
        if not records or any(not public_address(r["host"]) for r in records):
            raise UnsafeDestination("DNS resolved to a non-public address")
        # aiohttp connects to these checked addresses; it does not re-resolve them.
        return records
