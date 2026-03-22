from __future__ import annotations

import time
import urllib.error
import urllib.request
from typing import Any

from ..bridge import PhpBridge
from ..worker_runtime import payload_checksum, resident_memory_bytes, utc_now_iso


def _http_json(url: str, user_agent: str, timeout_seconds: int = 25) -> tuple[int, dict[str, Any]]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": user_agent,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            status = int(getattr(response, "status", response.getcode()))
            payload = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as error:
        status = int(error.code)
        payload = error.read().decode("utf-8", errors="replace")
    except urllib.error.URLError as error:
        raise RuntimeError(f"R2Z2 request failed: {error.reason}") from error

    import json

    decoded = json.loads(payload) if payload.strip() else {}
    if not isinstance(decoded, dict):
        decoded = {}
    return status, decoded


def _sleep_with_budget(seconds: int, deadline: float) -> bool:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return False
    time.sleep(min(float(seconds), max(0.0, remaining)))
    return time.monotonic() < deadline


def _flush_batch(bridge: PhpBridge, pending_payloads: list[dict[str, Any]]) -> dict[str, Any]:
    if not pending_payloads:
        return {
            "rows_seen": 0,
            "rows_written": 0,
            "killmails_fetched": 0,
            "duplicates": 0,
            "filtered": 0,
            "invalid": 0,
            "last_processed_sequence": None,
        }

    response = bridge.call("process-killmail-batch", payload={"payloads": pending_payloads})
    return dict(response.get("result") or {})


class KillmailEntityResolver:
    def __init__(self, user_agent: str):
        self.user_agent = user_agent
        self._character_cache: dict[int, dict[str, Any] | None] = {}
        self._corporation_cache: dict[int, dict[str, Any] | None] = {}

    def _fetch_profile(self, url: str) -> dict[str, Any] | None:
        status, payload = _http_json(url, self.user_agent)
        if status != 200:
            return None

        return payload

    def _character_profile(self, character_id: int) -> dict[str, Any] | None:
        if character_id <= 0:
            return None

        if character_id not in self._character_cache:
            self._character_cache[character_id] = self._fetch_profile(
                f"https://esi.evetech.net/latest/characters/{character_id}/?datasource=tranquility"
            )

        return self._character_cache[character_id]

    def _corporation_profile(self, corporation_id: int) -> dict[str, Any] | None:
        if corporation_id <= 0:
            return None

        if corporation_id not in self._corporation_cache:
            self._corporation_cache[corporation_id] = self._fetch_profile(
                f"https://esi.evetech.net/latest/corporations/{corporation_id}/?datasource=tranquility"
            )

        return self._corporation_cache[corporation_id]

    def _enrich_actor(self, actor: dict[str, Any]) -> None:
        character_id = int(actor.get("character_id") or 0)
        corporation_id = int(actor.get("corporation_id") or 0)
        alliance_id = int(actor.get("alliance_id") or 0)

        if corporation_id <= 0 and character_id > 0:
            character_profile = self._character_profile(character_id) or {}
            corporation_id = int(character_profile.get("corporation_id") or 0)
            if corporation_id > 0:
                actor["corporation_id"] = corporation_id

        if alliance_id <= 0 and corporation_id > 0:
            corporation_profile = self._corporation_profile(corporation_id) or {}
            alliance_id = int(corporation_profile.get("alliance_id") or 0)
            if alliance_id > 0:
                actor["alliance_id"] = alliance_id

    def enrich_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        for payload_key in ("esi", "killmail"):
            killmail = payload.get(payload_key)
            if not isinstance(killmail, dict):
                continue

            victim = killmail.get("victim")
            if isinstance(victim, dict):
                self._enrich_actor(victim)

            attackers = killmail.get("attackers")
            if isinstance(attackers, list):
                for attacker in attackers:
                    if isinstance(attacker, dict):
                        self._enrich_actor(attacker)

        return payload


def run_killmail_r2z2_stream(context: Any) -> dict[str, Any]:
    bridge = PhpBridge(context.php_binary, context.app_root)
    bridge_response = bridge.call("killmail-context")
    job_context = dict(bridge_response.get("context") or {})

    if not bool(job_context.get("enabled")):
        return {
            "status": "skipped",
            "summary": "Killmail ingestion is disabled in settings.",
            "rows_seen": 0,
            "rows_written": 0,
            "cursor": str(job_context.get("cursor") or "0"),
            "checksum": payload_checksum({"cursor": job_context.get("cursor") or "0", "rows_written": 0}),
            "duration_ms": 0,
            "started_at": utc_now_iso(),
            "finished_at": utc_now_iso(),
            "warnings": ["Killmail ingestion disabled in settings."],
            "meta": {
                "execution_mode": "python",
                "outcome_reason": "Killmail ingestion is disabled.",
            },
        }

    sequence_url = str(job_context.get("sequence_url") or "").strip()
    base_url = str(job_context.get("base_url") or "").rstrip("/")
    user_agent = str(job_context.get("user_agent") or "SupplyCore killmail-ingestion/2.0")
    poll_sleep_seconds = max(10, int(job_context.get("poll_sleep_seconds") or 10))
    max_sequences = max(1, int(job_context.get("max_sequences_per_run") or 120))
    batch_size = max(1, min(50, context.batch_size // 20 or 25))
    entity_resolver = KillmailEntityResolver(user_agent)

    if sequence_url == "" or base_url == "":
        raise RuntimeError("Killmail Python worker requires both sequence and base R2Z2 URLs.")

    start_iso = utc_now_iso()
    started_at = time.monotonic()
    deadline = started_at + max(15, context.timeout_seconds - 5)
    cursor_raw = str(job_context.get("cursor") or "").strip()
    last_saved_sequence = int(cursor_raw) if cursor_raw.isdigit() else None
    next_sequence: int | None = last_saved_sequence + 1 if last_saved_sequence is not None else None
    latest_remote_sequence: int | None = None
    last_processed_sequence: int | None = last_saved_sequence

    warnings: list[str] = []
    pending_payloads: list[dict[str, Any]] = []
    total_rows_seen = 0
    total_rows_written = 0
    total_duplicates = 0
    total_filtered = 0
    total_invalid = 0
    total_sequence_files_fetched = 0
    total_sequence_404s = 0
    total_rate_limits = 0
    batches_flushed = 0
    first_sequence_attempted: int | None = None
    last_sequence_attempted: int | None = None

    while time.monotonic() < deadline and total_sequence_files_fetched < max_sequences:
        if next_sequence is None:
            probe_status, probe_payload = _http_json(sequence_url, user_agent)
            if probe_status == 200:
                latest_remote_sequence = int(probe_payload.get("sequence") or 0)
                if latest_remote_sequence <= 0:
                    raise RuntimeError("R2Z2 sequence probe returned an invalid sequence value.")
                next_sequence = latest_remote_sequence if last_saved_sequence is None else max(last_saved_sequence + 1, next_sequence or 0)
            elif probe_status in (403, 429):
                total_rate_limits += 1
                warnings.append(f"R2Z2 sequence probe returned status {probe_status}; sleeping {poll_sleep_seconds}s before retry.")
                if not _sleep_with_budget(poll_sleep_seconds, deadline):
                    break
                continue
            else:
                raise RuntimeError(f"Unable to read killmail sequence.json, status={probe_status}")

        sequence_id = int(next_sequence)
        if first_sequence_attempted is None:
            first_sequence_attempted = sequence_id
        last_sequence_attempted = sequence_id
        status, payload = _http_json(f"{base_url}/{sequence_id}.json", user_agent)

        if status == 200:
            payload = entity_resolver.enrich_payload(payload)
            pending_payloads.append(payload)
            total_sequence_files_fetched += 1
            next_sequence = sequence_id + 1
            if len(pending_payloads) >= batch_size:
                batch_result = _flush_batch(bridge, pending_payloads)
                pending_payloads = []
                batches_flushed += 1
                total_rows_seen += int(batch_result.get("rows_seen") or 0)
                total_rows_written += int(batch_result.get("rows_written") or 0)
                total_duplicates += int(batch_result.get("duplicates") or 0)
                total_filtered += int(batch_result.get("filtered") or 0)
                total_invalid += int(batch_result.get("invalid") or 0)
                batch_last_processed = batch_result.get("last_processed_sequence")
                if batch_last_processed is not None:
                    last_processed_sequence = int(batch_last_processed)
                context.emit(
                    "python_worker.batch_progress",
                    {
                        "schedule_id": context.schedule_id,
                        "job_key": context.job_key,
                        "batches_completed": batches_flushed,
                        "rows_processed": total_rows_seen,
                        "rows_written": total_rows_written,
                        "last_sequence_attempted": last_sequence_attempted,
                        "last_processed_sequence": last_processed_sequence,
                        "memory_usage_bytes": resident_memory_bytes(),
                        "duration_ms": int((time.monotonic() - started_at) * 1000),
                    },
                )
            continue

        if pending_payloads:
            batch_result = _flush_batch(bridge, pending_payloads)
            pending_payloads = []
            batches_flushed += 1
            total_rows_seen += int(batch_result.get("rows_seen") or 0)
            total_rows_written += int(batch_result.get("rows_written") or 0)
            total_duplicates += int(batch_result.get("duplicates") or 0)
            total_filtered += int(batch_result.get("filtered") or 0)
            total_invalid += int(batch_result.get("invalid") or 0)
            batch_last_processed = batch_result.get("last_processed_sequence")
            if batch_last_processed is not None:
                last_processed_sequence = int(batch_last_processed)

        if status == 404:
            total_sequence_404s += 1
            warnings.append(f"R2Z2 returned 404 for sequence {sequence_id}; sleeping {poll_sleep_seconds}s before retry.")
            if not _sleep_with_budget(poll_sleep_seconds, deadline):
                break
            continue

        if status in (403, 429):
            total_rate_limits += 1
            warnings.append(f"R2Z2 returned status {status} for sequence {sequence_id}; sleeping {poll_sleep_seconds}s before retry.")
            if not _sleep_with_budget(poll_sleep_seconds, deadline):
                break
            continue

        raise RuntimeError(f"R2Z2 sequence fetch failed for {sequence_id} with status={status}")

    if pending_payloads:
        batch_result = _flush_batch(bridge, pending_payloads)
        pending_payloads = []
        batches_flushed += 1
        total_rows_seen += int(batch_result.get("rows_seen") or 0)
        total_rows_written += int(batch_result.get("rows_written") or 0)
        total_duplicates += int(batch_result.get("duplicates") or 0)
        total_filtered += int(batch_result.get("filtered") or 0)
        total_invalid += int(batch_result.get("invalid") or 0)
        batch_last_processed = batch_result.get("last_processed_sequence")
        if batch_last_processed is not None:
            last_processed_sequence = int(batch_last_processed)

    cursor_end = str(last_processed_sequence if last_processed_sequence is not None else (last_saved_sequence or 0))
    checksum = payload_checksum({
        "rows_seen": total_rows_seen,
        "rows_written": total_rows_written,
        "cursor": cursor_end,
        "batches_flushed": batches_flushed,
    })

    if total_rows_written > 0:
        outcome_reason = "Python streamed killmail ingestion batches and kept polling until the worker budget expired."
    elif total_rows_seen > 0 and total_duplicates == total_rows_seen:
        outcome_reason = "All fetched killmails were already present in storage."
    elif total_rows_seen > 0 and total_filtered + total_invalid == total_rows_seen:
        outcome_reason = "Fetched killmails did not pass tracked-entity filters or were invalid."
    elif total_sequence_404s > 0:
        outcome_reason = "Python worker caught up to the live R2Z2 tip and stayed in the documented poll/sleep loop."
    else:
        outcome_reason = "Python worker completed without new killmail inserts."

    return {
        "status": "success",
        "summary": "Killmail R2Z2 ingestion ran in Python with continuous polling and bridge-backed batch persistence.",
        "rows_seen": total_rows_seen,
        "rows_written": total_rows_written,
        "cursor": cursor_end,
        "checksum": checksum,
        "duration_ms": int((time.monotonic() - started_at) * 1000),
        "started_at": start_iso,
        "finished_at": utc_now_iso(),
        "warnings": warnings[-10:],
        "meta": {
            "execution_mode": "python",
            "poll_sleep_seconds": poll_sleep_seconds,
            "continuous_polling": True,
            "sequence_files_fetched": total_sequence_files_fetched,
            "sequence_404s_encountered": total_sequence_404s,
            "rate_limit_responses": total_rate_limits,
            "duplicates": total_duplicates,
            "filtered": total_filtered,
            "invalid": total_invalid,
            "killmails_fetched": total_rows_seen,
            "killmails_inserted": total_rows_written,
            "first_sequence_attempted": first_sequence_attempted,
            "last_sequence_attempted": last_sequence_attempted,
            "last_saved_sequence_before_run": last_saved_sequence,
            "last_processed_sequence": last_processed_sequence,
            "latest_remote_sequence": latest_remote_sequence,
            "batches_flushed": batches_flushed,
            "memory_usage_bytes": resident_memory_bytes(),
            "outcome_reason": outcome_reason,
        },
    }
