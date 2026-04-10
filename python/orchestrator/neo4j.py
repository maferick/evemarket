from __future__ import annotations

import base64
import json
import logging
from dataclasses import dataclass, field
import time
from typing import Any
import urllib.error
import urllib.request

from .http_client import ipv4_opener
from .json_utils import json_dumps_safe

logger = logging.getLogger(__name__)


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


@dataclass(slots=True)
class QueryPlanMetrics:
    """Metrics extracted from a PROFILE'd or EXPLAIN'd query plan."""
    statement: str
    db_hits: int = 0
    rows: int = 0
    estimated_rows: float = 0.0
    elapsed_ms: float = 0.0
    operator_count: int = 0
    has_cartesian_product: bool = False
    has_eager: bool = False
    has_node_by_label_scan: bool = False
    has_all_nodes_scan: bool = False
    planner: str = ""
    runtime: str = ""
    operators: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def severity(self) -> str:
        """Return 'critical', 'warning', or 'ok' based on plan anti-patterns."""
        if self.has_cartesian_product or self.has_all_nodes_scan:
            return "critical"
        if self.has_eager or self.has_node_by_label_scan or self.db_hits > 100_000:
            return "warning"
        return "ok"

    def to_dict(self) -> dict[str, Any]:
        return {
            "statement": self.statement[:200],
            "db_hits": self.db_hits,
            "rows": self.rows,
            "estimated_rows": self.estimated_rows,
            "elapsed_ms": self.elapsed_ms,
            "operator_count": self.operator_count,
            "has_cartesian_product": self.has_cartesian_product,
            "has_eager": self.has_eager,
            "has_node_by_label_scan": self.has_node_by_label_scan,
            "has_all_nodes_scan": self.has_all_nodes_scan,
            "planner": self.planner,
            "runtime": self.runtime,
            "severity": self.severity(),
            "warnings": self.warnings,
            "operators": self.operators[:20],
        }


class Neo4jError(RuntimeError):
    pass


_TRANSIENT_RETRY_LIMIT = 3
_TRANSIENT_BASE_SLEEP = 0.5


_RETRYABLE_CODES = frozenset({
    "Neo.DatabaseError.General.UnknownError",
})


def _is_transient_error(errors: list) -> bool:
    """Return True when the first Neo4j error has a transient (retryable) code."""
    if not errors:
        return False
    code = str(errors[0].get("code", ""))
    return code.startswith("Neo.TransientError.") or code in _RETRYABLE_CODES


class Neo4jClient:
    def __init__(self, config: Neo4jConfig):
        self._config = config

    @property
    def _endpoint(self) -> str:
        return f"{self._config.url}/db/{self._config.database}/tx/commit"

    def _execute_payload(
        self,
        payload: dict[str, Any],
        *,
        timeout_seconds: int | None = None,
    ) -> dict[str, Any]:
        """Execute a raw HTTP payload against the Neo4j transactional endpoint."""
        auth = base64.b64encode(
            f"{self._config.username}:{self._config.password}".encode("utf-8")
        ).decode("ascii")
        effective_timeout = (
            timeout_seconds if timeout_seconds is not None else self._config.timeout_seconds
        )

        body: dict[str, Any] = {}
        for attempt in range(_TRANSIENT_RETRY_LIMIT + 1):
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
                with ipv4_opener.open(request, timeout=effective_timeout) as response:
                    body = json.loads(response.read().decode("utf-8"))
            except json.JSONDecodeError as error:
                if attempt < _TRANSIENT_RETRY_LIMIT:
                    time.sleep(_TRANSIENT_BASE_SLEEP * (2 ** attempt))
                    continue
                raise Neo4jError(
                    f"Neo4j response was not valid JSON (truncated/corrupt at char {error.pos})"
                ) from error
            except urllib.error.HTTPError as error:
                details = error.read().decode("utf-8", errors="replace")
                raise Neo4jError(f"Neo4j query failed ({error.code}): {details}") from error
            except urllib.error.URLError as error:
                # URLError wraps socket errors raised during connect()/handshake
                # (connection refused, DNS failure, timeouts, etc).  Retry
                # transient network problems before giving up — Neo4j restarts
                # and brief network hiccups should not fail an entire compute
                # job.
                if attempt < _TRANSIENT_RETRY_LIMIT:
                    time.sleep(_TRANSIENT_BASE_SLEEP * (2 ** attempt))
                    continue
                raise Neo4jError(f"Neo4j query failed: {error.reason}") from error
            except (ConnectionResetError, BrokenPipeError, TimeoutError) as error:
                # Raised directly (not wrapped in URLError) when the peer
                # drops the TCP connection mid-read — e.g. a Neo4j restart
                # between `open()` and `read()`, or an idle keepalive that
                # the OS reaped.  Retry with backoff on the same schedule
                # as other transient errors.
                if attempt < _TRANSIENT_RETRY_LIMIT:
                    time.sleep(_TRANSIENT_BASE_SLEEP * (2 ** attempt))
                    continue
                raise Neo4jError(f"Neo4j query failed: {error}") from error
            except OSError as error:
                # Catch-all for any other socket-level error that bubbles up
                # as a bare OSError (Errno 111/104/32/…).  Retry with backoff.
                if attempt < _TRANSIENT_RETRY_LIMIT:
                    time.sleep(_TRANSIENT_BASE_SLEEP * (2 ** attempt))
                    continue
                raise Neo4jError(f"Neo4j query failed: {error}") from error

            errors = body.get("errors") or []
            if errors:
                if attempt < _TRANSIENT_RETRY_LIMIT and _is_transient_error(errors):
                    time.sleep(_TRANSIENT_BASE_SLEEP * (2 ** attempt))
                    continue
                raise Neo4jError(str(errors[0]))

            break

        return body

    @staticmethod
    def _extract_rows(result: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract row dicts from a single Neo4j result set."""
        columns = result.get("columns") or []
        rows = []
        for item in result.get("data") or []:
            row = item.get("row") or []
            rows.append({columns[idx]: row[idx] for idx in range(min(len(columns), len(row)))})
        return rows

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
        t0 = time.perf_counter()
        body = self._execute_payload(payload, timeout_seconds=timeout_seconds)
        elapsed_ms = (time.perf_counter() - t0) * 1000

        if elapsed_ms > 5000:
            logger.warning(
                "Slow Neo4j query (%.0fms): %.120s", elapsed_ms, statement.strip()
            )

        results = body.get("results") or []
        if not results:
            return []
        return self._extract_rows(results[0])

    def query_batch(
        self,
        statements: list[tuple[str, dict[str, Any] | None]],
        *,
        timeout_seconds: int | None = None,
    ) -> list[list[dict[str, Any]]]:
        """Execute multiple statements in a single HTTP request.

        Each entry in *statements* is a (cypher, parameters) tuple.
        Returns one list of row-dicts per statement, in order.
        """
        if not statements:
            return []

        payload = {
            "statements": [
                {
                    "statement": stmt,
                    "parameters": params or {},
                    "resultDataContents": ["row"],
                }
                for stmt, params in statements
            ]
        }

        body = self._execute_payload(payload, timeout_seconds=timeout_seconds)
        results = body.get("results") or []
        return [self._extract_rows(r) for r in results]

    # ------------------------------------------------------------------
    #  Query plan analysis (PROFILE / EXPLAIN)
    # ------------------------------------------------------------------

    def profile(
        self,
        statement: str,
        parameters: dict[str, Any] | None = None,
        *,
        timeout_seconds: int | None = None,
    ) -> QueryPlanMetrics:
        """Run a PROFILE'd query and return plan diagnostics.

        PROFILE actually executes the query, so results are real (not estimated).
        Use for hot-query validation where you need actual db-hit counts.
        """
        return self._plan_query("PROFILE", statement, parameters, timeout_seconds=timeout_seconds)

    def explain(
        self,
        statement: str,
        parameters: dict[str, Any] | None = None,
        *,
        timeout_seconds: int | None = None,
    ) -> QueryPlanMetrics:
        """Run an EXPLAIN'd query and return plan diagnostics.

        EXPLAIN does not execute the query — only estimates are available.
        Use for cheap plan-shape checks without side effects.
        """
        return self._plan_query("EXPLAIN", statement, parameters, timeout_seconds=timeout_seconds)

    def _plan_query(
        self,
        plan_mode: str,
        statement: str,
        parameters: dict[str, Any] | None = None,
        *,
        timeout_seconds: int | None = None,
    ) -> QueryPlanMetrics:
        prefixed = f"{plan_mode} {statement.lstrip()}"
        payload = {
            "statements": [
                {
                    "statement": prefixed,
                    "parameters": parameters or {},
                    "resultDataContents": ["row"],
                    "includeStats": True,
                }
            ]
        }

        t0 = time.perf_counter()
        body = self._execute_payload(payload, timeout_seconds=timeout_seconds)
        elapsed_ms = (time.perf_counter() - t0) * 1000

        results = body.get("results") or []
        if not results:
            return QueryPlanMetrics(statement=statement, elapsed_ms=elapsed_ms)

        result = results[0]
        plan_key = "profile" if plan_mode == "PROFILE" else "plan"
        plan = result.get(plan_key) or {}

        return self._parse_plan(statement, plan, elapsed_ms)

    @staticmethod
    def _parse_plan(statement: str, plan: dict[str, Any], elapsed_ms: float) -> QueryPlanMetrics:
        """Walk a Neo4j query plan tree and extract aggregate metrics."""
        metrics = QueryPlanMetrics(statement=statement, elapsed_ms=elapsed_ms)

        if not plan:
            return metrics

        # Top-level planner/runtime from plan arguments
        args = plan.get("args") or {}
        metrics.planner = str(args.get("planner", args.get("planner-impl", "")))
        metrics.runtime = str(args.get("runtime", args.get("runtime-impl", "")))

        # Walk the operator tree (BFS)
        queue: list[dict[str, Any]] = [plan]
        while queue:
            node = queue.pop(0)
            metrics.operator_count += 1

            op_type = str(node.get("operatorType", "")).lower()
            node_args = node.get("args") or {}

            # Accumulate db hits and rows (PROFILE only)
            metrics.db_hits += int(node.get("dbHits", node_args.get("DbHits", 0)))
            metrics.rows += int(node.get("rows", node_args.get("Rows", 0)))

            # Estimated rows (both EXPLAIN and PROFILE)
            est = float(node.get("estimatedRows", node_args.get("EstimatedRows", 0)))
            if est > metrics.estimated_rows:
                metrics.estimated_rows = est

            # Detect anti-patterns
            if "cartesianproduct" in op_type.replace(" ", "").replace("-", ""):
                metrics.has_cartesian_product = True
                metrics.warnings.append(f"CartesianProduct operator: {op_type}")
            if "eager" == op_type.strip():
                metrics.has_eager = True
                metrics.warnings.append("Eager operator forces full materialization")
            if "nodelabelscan" in op_type.replace(" ", "").replace("-", ""):
                metrics.has_node_by_label_scan = True
            if "allnodesscan" in op_type.replace(" ", "").replace("-", ""):
                metrics.has_all_nodes_scan = True
                metrics.warnings.append("AllNodesScan: no label filter on MATCH")

            # Record operator summary
            metrics.operators.append({
                "operator": str(node.get("operatorType", "")),
                "db_hits": int(node.get("dbHits", node_args.get("DbHits", 0))),
                "rows": int(node.get("rows", node_args.get("Rows", 0))),
                "estimated_rows": float(node.get("estimatedRows", node_args.get("EstimatedRows", 0))),
            })

            # Traverse children
            for child in node.get("children") or []:
                queue.append(child)

        # Cardinality blowup detection
        if metrics.estimated_rows > 0 and metrics.rows > 0:
            ratio = metrics.rows / metrics.estimated_rows
            if ratio > 10:
                metrics.warnings.append(
                    f"Cardinality blowup: actual rows ({metrics.rows}) >> estimated ({metrics.estimated_rows:.0f}), ratio {ratio:.1f}x"
                )

        return metrics
