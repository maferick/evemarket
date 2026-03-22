from __future__ import annotations

import hashlib
import json
import resource
import time
from dataclasses import dataclass, field
from typing import Any


def utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def payload_checksum(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def resident_memory_bytes() -> int:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    factor = 1024 if usage.ru_maxrss < 1024 * 1024 * 1024 else 1
    return int(usage.ru_maxrss * factor)


@dataclass(slots=True)
class BatchProgress:
    rows_processed: int = 0
    batches_completed: int = 0
    rows_written: int = 0
    last_type_id: int = 0


@dataclass(slots=True)
class WorkerStats:
    started_at_monotonic: float = field(default_factory=time.monotonic)
    started_at_iso: str = field(default_factory=utc_now_iso)
    progress: BatchProgress = field(default_factory=BatchProgress)

    def duration_ms(self) -> int:
        return int((time.monotonic() - self.started_at_monotonic) * 1000)
