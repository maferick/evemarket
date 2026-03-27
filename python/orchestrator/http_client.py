"""Shared HTTP client utilities for the orchestrator.

Provides an IPv4-only HTTPS handler to work around IPv6 connectivity
issues on servers where IPv6 DNS resolves but connections hang.
"""

from __future__ import annotations

import http.client
import socket
import ssl
import urllib.request


def _create_connection_ipv4(address: tuple[str, int], timeout: float | None = None,
                            source_address: tuple[str, int] | None = None) -> socket.socket:
    """Like socket.create_connection but forces AF_INET (IPv4 only)."""
    host, port = address
    # Resolve only IPv4 addresses
    infos = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
    if not infos:
        raise OSError(f"No IPv4 address found for {host}")

    last_error: OSError | None = None
    for family, socktype, proto, _canonname, sockaddr in infos:
        sock = None
        try:
            sock = socket.socket(family, socktype, proto)
            if timeout is not None:
                sock.settimeout(timeout)
            if source_address:
                sock.bind(source_address)
            sock.connect(sockaddr)
            return sock
        except OSError as exc:
            last_error = exc
            if sock is not None:
                sock.close()

    if last_error is not None:
        raise last_error
    raise OSError(f"Failed to connect to {host}:{port}")


class _IPv4HTTPSConnection(http.client.HTTPSConnection):
    """HTTPS connection that forces IPv4 to avoid IPv6 hangs."""

    def connect(self) -> None:
        self.sock = _create_connection_ipv4(
            (self.host, self.port or 443),
            timeout=self.timeout,
            source_address=self.source_address,
        )
        context = self._context if hasattr(self, "_context") else ssl.create_default_context()
        self.sock = context.wrap_socket(self.sock, server_hostname=self.host)


class _IPv4HTTPConnection(http.client.HTTPConnection):
    """HTTP connection that forces IPv4."""

    def connect(self) -> None:
        self.sock = _create_connection_ipv4(
            (self.host, self.port or 80),
            timeout=self.timeout,
            source_address=self.source_address,
        )


class _IPv4HTTPHandler(urllib.request.HTTPHandler):
    def http_open(self, req: urllib.request.Request) -> http.client.HTTPResponse:
        return self.do_open(_IPv4HTTPConnection, req)


class _IPv4HTTPSHandler(urllib.request.HTTPSHandler):
    def https_open(self, req: urllib.request.Request) -> http.client.HTTPResponse:
        return self.do_open(_IPv4HTTPSConnection, req)


# Global opener that forces IPv4
ipv4_opener = urllib.request.build_opener(_IPv4HTTPHandler, _IPv4HTTPSHandler)
