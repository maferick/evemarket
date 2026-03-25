from __future__ import annotations

from typing import Any

from .jobs import run_killmail_r2z2_stream


class ZKillR2Z2Adapter:
    """Stable integration boundary for external zKill ingestion."""

    adapter_key = "zkill_r2z2_adapter"
    job_key = "killmail_r2z2_sync"

    def run_stream_once(self, context: Any) -> dict[str, Any]:
        return run_killmail_r2z2_stream(context)
