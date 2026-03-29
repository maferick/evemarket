from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any
import urllib.error
import urllib.request

from .http_client import ipv4_opener
from .json_utils import json_dumps_safe


@dataclass(slots=True)
class Neo4jConfig:
    enabled: bool
    url: str
    username: str
    password: str
    database: str
    timeout_seconds: int
    log_file: str

    @classmethod
    def from_runtime(cls, raw: dict[str, Any]) -> "Neo4jConfig":
        return cls(
            enabled=bool(raw.get("enabled", False)),
            url=str(raw.get("url", "http://127.0.0.1:7474")).rstrip("/"),
            username=str(raw.get("username", "neo4j")),
            password=str(raw.get("password", "")),
            database=str(raw.get("database", "neo4j")),
            timeout_seconds=max(3, int(raw.get("timeout_seconds", 15))),
            log_file=str(raw.get("log_file", "")).strip(),
        )


class Neo4jError(RuntimeError):
    pass


class Neo4jClient:
    def __init__(self, config: Neo4jConfig):
        self._config = config

    @property
    def _endpoint(self) -> str:
        return f"{self._config.url}/db/{self._config.database}/tx/commit"

    def query(self, statement: str, parameters: dict[str, Any] | None = None, *, timeout_seconds: int | None = None) -> list[dict[str, Any]]:
        payload = {
            "statements": [
                {
                    "statement": statement,
                    "parameters": parameters or {},
                    "resultDataContents": ["row"],
                }
            ]
        }
        auth = base64.b64encode(f"{self._config.username}:{self._config.password}".encode("utf-8")).decode("ascii")
        request = urllib.request.Request(
            self._endpoint,
            data=json_dumps_safe(payload).encode("utf-8"),
            method="POST",
            headers={
                "Authorization": f"Basic {auth}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        try:
            effective_timeout = timeout_seconds if timeout_seconds is not None else self._config.timeout_seconds
            with ipv4_opener.open(request, timeout=effective_timeout) as response:
                body = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            details = error.read().decode("utf-8", errors="replace")
            raise Neo4jError(f"Neo4j query failed ({error.code}): {details}") from error
        except urllib.error.URLError as error:
            raise Neo4jError(f"Neo4j query failed: {error.reason}") from error

        errors = body.get("errors") or []
        if errors:
            raise Neo4jError(str(errors[0]))

        results = body.get("results") or []
        if not results:
            return []
        columns = results[0].get("columns") or []
        rows = []
        for item in results[0].get("data") or []:
            row = item.get("row") or []
            rows.append({columns[idx]: row[idx] for idx in range(min(len(columns), len(row)))})
        return rows
