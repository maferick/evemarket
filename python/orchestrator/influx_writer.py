"""Inline InfluxDB dual-write helpers for rollup sync jobs.

When ``influxdb.write_on_rollup`` is enabled, rollup sync jobs can push
points directly to InfluxDB as part of the same processing pass that
populates the MariaDB rollup tables.  This eliminates the latency gap
between MariaDB rollup completion and the separate ``influx_export``
batch, making InfluxDB data available for reads almost immediately.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from .config import OrchestratorConfig
from .influx import InfluxConfig, InfluxWriter, InfluxWriteError, encode_point


@dataclass(slots=True)
class RollupInfluxBridge:
    """Lightweight bridge between rollup processors and InfluxDB writes."""

    writer: InfluxWriter | None
    enabled: bool
    batch_size: int
    _pending: list[str]
    _written: int

    @classmethod
    def from_config(cls, config: OrchestratorConfig) -> "RollupInfluxBridge":
        influx_raw = dict(config.raw.get("influxdb", config.raw.get("influx", {})))
        influx_enabled = bool(influx_raw.get("enabled", False))
        write_on_rollup = bool(influx_raw.get("write_on_rollup", False))

        if not influx_enabled or not write_on_rollup:
            return cls(writer=None, enabled=False, batch_size=0, _pending=[], _written=0)

        influx_config = InfluxConfig.from_runtime(influx_raw)
        errors = influx_config.validate()
        if errors:
            print(f"[influx_writer] InfluxDB config invalid, dual-write disabled: {'; '.join(errors)}", file=sys.stderr)
            return cls(writer=None, enabled=False, batch_size=0, _pending=[], _written=0)

        batch_size = max(100, int(influx_raw.get("rollup_write_batch_size", 500)))
        return cls(
            writer=InfluxWriter(influx_config),
            enabled=True,
            batch_size=batch_size,
            _pending=[],
            _written=0,
        )

    @property
    def written_count(self) -> int:
        return self._written

    def enqueue_market_price(
        self,
        bucket_start: date | datetime,
        source_type: str,
        source_id: int,
        type_id: int,
        window: str,
        fields: dict[str, Any],
    ) -> None:
        if not self.enabled:
            return
        tags = {
            "window": window,
            "source_type": source_type,
            "source_id": str(source_id),
            "type_id": str(type_id),
        }
        self._pending.append(encode_point("market_item_price", tags, fields, bucket_start))
        self._maybe_flush()

    def enqueue_market_stock(
        self,
        bucket_start: date | datetime,
        source_type: str,
        source_id: int,
        type_id: int,
        window: str,
        fields: dict[str, Any],
    ) -> None:
        if not self.enabled:
            return
        tags = {
            "window": window,
            "source_type": source_type,
            "source_id": str(source_id),
            "type_id": str(type_id),
        }
        self._pending.append(encode_point("market_item_stock", tags, fields, bucket_start))
        self._maybe_flush()

    def enqueue_killmail_hull_loss(
        self,
        bucket_start: date | datetime,
        hull_type_id: int,
        window: str,
        fields: dict[str, Any],
    ) -> None:
        if not self.enabled:
            return
        tags = {
            "window": window,
            "hull_type_id": str(hull_type_id),
        }
        self._pending.append(encode_point("killmail_hull_loss", tags, fields, bucket_start))
        self._maybe_flush()

    def enqueue_killmail_item_loss(
        self,
        bucket_start: date | datetime,
        type_id: int,
        hull_type_id: int,
        window: str,
        fields: dict[str, Any],
    ) -> None:
        if not self.enabled:
            return
        tags = {
            "window": window,
            "type_id": str(type_id),
            "hull_type_id": str(hull_type_id or 0),
        }
        self._pending.append(encode_point("killmail_item_loss", tags, fields, bucket_start))
        self._maybe_flush()

    def _maybe_flush(self) -> None:
        if len(self._pending) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self._pending or not self.writer:
            return
        try:
            self.writer.write_lines(self._pending)
            self._written += len(self._pending)
        except InfluxWriteError as exc:
            print(f"[influx_writer] Dual-write flush failed ({len(self._pending)} points): {exc}", file=sys.stderr)
        finally:
            self._pending = []
