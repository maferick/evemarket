"""Shared HTTP client utilities for the orchestrator.

Provides an IPv4-only HTTPS handler to work around IPv6 connectivity
issues on servers where IPv6 DNS resolves but connections hang.
"""

from __future__ import annotations

import http.client
import socket
import ssl
import urllib.request


class _IPv4HTTPSConnection(http.client.HTTPSConnection):
    """HTTPS connection that forces IPv4 to avoid IPv6 hangs."""

    def connect(self) -> None:
        self.sock = socket.create_connection(
            (self.host, self.port or 443),
            timeout=self.timeout,
            source_address=self.source_address,
        )
        context = self._context if hasattr(self, "_context") else ssl.create_default_context()
        self.sock = context.wrap_socket(self.sock, server_hostname=self.host)


class _IPv4HTTPHandler(urllib.request.HTTPHandler):
    def http_open(self, req: urllib.request.Request) -> http.client.HTTPResponse:
        return self.do_open(http.client.HTTPConnection, req)


class _IPv4HTTPSHandler(urllib.request.HTTPSHandler):
    def https_open(self, req: urllib.request.Request) -> http.client.HTTPResponse:
        return self.do_open(_IPv4HTTPSConnection, req)


# Global opener that forces IPv4
ipv4_opener = urllib.request.build_opener(_IPv4HTTPHandler, _IPv4HTTPSHandler)
