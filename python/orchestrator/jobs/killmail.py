from __future__ import annotations

import time
import urllib.error
import urllib.request
from typing import Any

from ..bridge import PhpBridge
from ..http_client import ipv4_opener
from ..job_result import JobResult
from ..worker_runtime import payload_checksum, resident_memory_bytes, utc_now_iso


def _http_json(url: str, user_agent: str, timeout_seconds: int = 25) -> tuple[int, Any]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": user_agent,
        },
    )
    try:
        with ipv4_opener.open(request, timeout=timeout_seconds) as response:
            status = int(getattr(response, "status", response.getcode()))
            payload = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as error:
        status = int(error.code)
        payload = error.read().decode("utf-8", errors="replace")
    except urllib.error.URLError as error:
        raise RuntimeError(f"R2Z2 request failed: {error.reason}") from error

    import json

    if not payload.strip():
        return status, {}

    try:
        decoded = json.loads(payload)
    except json.JSONDecodeError as error:
        if status == 200:
            raise RuntimeError(
                f"R2Z2 response from {url} with status=200 contained invalid JSON: {error.msg}"
            ) from error
        decoded = {}
    return status, decoded


def _classify_sequence_payload(payload: Any) -> tuple[str, dict[str, Any], bool]:
    if not isinstance(payload, dict):
        return "malformed_non_object_payload", {}, False
    if payload == {}:
        return "valid_empty_payload", {}, True
    return "valid_payload", payload, False


def _accumulate_batch_result(
    *,
    batch_result: dict[str, Any],
    totals: dict[str, int],
) -> tuple[int, int]:
    totals["rows_seen"] += int(batch_result.get("rows_seen") or 0)
    totals["rows_matched"] += int(batch_result.get("rows_matched") or 0)
    totals["rows_skipped_existing"] += int(batch_result.get("rows_skipped_existing") or 0)
    totals["rows_filtered_out"] += int(batch_result.get("rows_filtered_out") or 0)
    totals["rows_write_attempted"] += int(batch_result.get("rows_write_attempted") or 0)
    totals["rows_written"] += int(batch_result.get("rows_written") or 0)
    totals["rows_failed"] += int(batch_result.get("rows_failed") or 0)
    totals["duplicates"] += int(batch_result.get("duplicates") or 0)
    totals["filtered"] += int(batch_result.get("filtered") or 0)
    totals["invalid"] += int(batch_result.get("invalid") or 0)

    checkpoint_state = dict((batch_result.get("meta") or {}).get("checkpoint_state") or {})
    if checkpoint_state.get("checkpoint_updated"):
        return 1, 0
    if str(checkpoint_state.get("reason") or "") != "no_processed_sequence":
        return 0, 1
    return 0, 0


def _sequence_outcome_from_batch_result(batch_result: dict[str, Any]) -> str:
    if int(batch_result.get("rows_written") or 0) > 0:
        return "inserted"
    if int(batch_result.get("rows_skipped_existing") or 0) > 0:
        return "already_existing"
    if int(batch_result.get("rows_filtered_out") or 0) > 0:
        return "filtered_out"
    if int(batch_result.get("invalid") or 0) > 0:
        return "invalid_payload"
    if int(batch_result.get("rows_failed") or 0) > 0:
        return "write_failed"
    return "non_actionable"


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


def _sync_run_start(bridge: PhpBridge, job_context: dict[str, Any]) -> int:
    dataset_key = str(job_context.get("dataset_key") or "").strip()
    if dataset_key == "":
        return 0

    response = bridge.call(
        "sync-run-start",
        payload={
            "dataset_key": dataset_key,
            "run_mode": "incremental",
        },
    )
    result = dict(response.get("result") or {})
    return int(result.get("run_id") or 0)


def _sync_run_finish(bridge: PhpBridge, job_context: dict[str, Any], run_id: int, result: dict[str, Any]) -> dict[str, Any]:
    dataset_key = str(job_context.get("dataset_key") or "").strip()
    if dataset_key == "":
        return {}

    response = bridge.call(
        "sync-run-finish",
        payload={
            "run_id": run_id,
            "dataset_key": dataset_key,
            "run_mode": "incremental",
            "job_key": str(job_context.get("job_key") or ""),
            "status": str(result.get("status") or "failed"),
            "rows_seen": int(result.get("rows_seen") or 0),
            "rows_written": int(result.get("rows_written") or 0),
            "cursor": str(result.get("cursor") or ""),
            "checksum": str(result.get("checksum") or ""),
            "error_message": str(result.get("error") or result.get("summary") or ""),
        },
    )
    return dict(response.get("result") or {})


def _sync_cursor_checkpoint(
    bridge: PhpBridge,
    job_context: dict[str, Any],
    cursor: str,
    *,
    rows_written: int,
    outcome_reason: str,
) -> dict[str, Any]:
    dataset_key = str(job_context.get("dataset_key") or "").strip()
    if dataset_key == "" or cursor.strip() == "":
        return {
            "checkpoint_updated": False,
            "cursor": cursor,
            "reason": "missing_dataset_or_cursor",
        }

    response = bridge.call(
        "sync-cursor-upsert",
        payload={
            "dataset_key": dataset_key,
            "run_mode": "incremental",
            "cursor": cursor,
            "rows_written": rows_written,
            "outcome_reason": outcome_reason,
        },
    )
    result = dict(response.get("result") or {})
    return {
        "checkpoint_updated": bool(result.get("checkpoint_updated")),
        "cursor": str(result.get("cursor") or cursor),
        "reason": str(result.get("reason") or ""),
    }


def _zero_write_reason(
    *,
    rows_seen: int,
    rows_matched: int,
    rows_filtered_out: int,
    rows_skipped_existing: int,
    rows_failed: int,
    rows_written: int,
    checkpoint_updated: bool,
    sequence_404s: int,
    latest_remote_sequence: int | None,
    cursor_before: str,
    cursor_after: str,
) -> str:
    if rows_written > 0:
        return ""
    if rows_seen == 0 and sequence_404s > 0:
        return "caught_up_to_live_tip"
    if rows_seen == 0:
        return "unknown"
    if rows_failed > 0:
        return "write_failed"
    if rows_matched == 0 and rows_filtered_out == rows_seen:
        return "no_tracked_entity_matches"
    if rows_skipped_existing == rows_seen:
        return "already_existed_locally"
    if rows_filtered_out == rows_seen:
        return "filter_excluded_all"
    if cursor_after == cursor_before and latest_remote_sequence is not None and latest_remote_sequence > 0:
        return "checkpoint_prevented_write"
    if cursor_after != "" and not checkpoint_updated:
        return "checkpoint_prevented_write"
    if rows_matched > 0 and rows_written == 0:
        return "downstream_persistence_blocked"
    return "unknown"


def _flush_pending_batch(
    *,
    bridge: PhpBridge,
    job_context: dict[str, Any],
    pending_payloads: list[dict[str, Any]],
    batch_index: int,
    last_processed_sequence: int | None,
    logger_context: Any,
    started_at: float,
    totals: dict[str, int],
) -> tuple[dict[str, Any], int | None]:
    batch_result = _flush_batch(bridge, pending_payloads)
    batch_meta = dict(batch_result.get("meta") or {})
    batch_last_processed = batch_result.get("last_processed_sequence")
    if batch_last_processed is not None:
        last_processed_sequence = int(batch_last_processed)

    checkpoint_state = {
        "checkpoint_updated": False,
        "cursor": "",
        "reason": "no_processed_sequence",
    }
    if last_processed_sequence is not None and last_processed_sequence > 0:
        checkpoint_state = _sync_cursor_checkpoint(
            bridge,
            job_context,
            str(last_processed_sequence),
            rows_written=int(batch_result.get("rows_written") or 0),
            outcome_reason=str(batch_result.get("reason_for_zero_write") or "batch_processed"),
        )

    batch_result["meta"] = {
        **batch_meta,
        "checkpoint_state": checkpoint_state,
    }
    logger_context.emit(
        "zkill.batch_completed",
        {
            "job_key": logger_context.job_key,
            "batch_index": batch_index,
            "rows_seen": batch_result.get("rows_seen"),
            "rows_matched": batch_result.get("rows_matched"),
            "rows_filtered_out": batch_result.get("rows_filtered_out"),
            "rows_skipped_existing": batch_result.get("rows_skipped_existing"),
            "rows_write_attempted": batch_result.get("rows_write_attempted"),
            "rows_written": batch_result.get("rows_written"),
            "rows_failed": batch_result.get("rows_failed"),
            "cursor_after": str(last_processed_sequence or ""),
            "first_sequence_seen": batch_meta.get("first_sequence_seen"),
            "last_sequence_seen": batch_meta.get("last_sequence_seen"),
            "reason_for_zero_write": batch_result.get("reason_for_zero_write"),
            "checkpoint_state": checkpoint_state,
            "duration_ms": int((time.monotonic() - started_at) * 1000),
            "memory_usage_bytes": resident_memory_bytes(),
            "running_rows_seen": totals["rows_seen"] + int(batch_result.get("rows_seen") or 0),
            "running_rows_written": totals["rows_written"] + int(batch_result.get("rows_written") or 0),
        },
    )
    return batch_result, last_processed_sequence


class KillmailEntityResolver:
    def __init__(self, user_agent: str):
        self.user_agent = user_agent
        self._character_cache: dict[int, dict[str, Any] | None] = {}
        self._corporation_cache: dict[int, dict[str, Any] | None] = {}

    def _fetch_profile(self, url: str) -> dict[str, Any] | None:
        status, payload = _http_json(url, self.user_agent)
        if status != 200 or not isinstance(payload, dict):
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
        return JobResult.skipped(
            job_key="killmail_r2z2_sync",
            reason=f"Killmail ingestion is disabled in settings. (bridge returned enabled={job_context.get('enabled')!r})",
            meta={
                "execution_mode": "python",
                "cursor": str(job_context.get("cursor") or "0"),
                "checksum": payload_checksum({"cursor": job_context.get("cursor") or "0", "rows_written": 0}),
                "bridge_context_keys": list(job_context.keys()),
                "bridge_enabled_raw": job_context.get("enabled"),
            },
        ).to_dict()

    tracked_alliance_count = max(0, int(job_context.get("tracked_alliance_count") or 0))
    tracked_corporation_count = max(0, int(job_context.get("tracked_corporation_count") or 0))
    tracked_entity_count = tracked_alliance_count + tracked_corporation_count
    if tracked_entity_count <= 0:
        return JobResult.skipped(
            job_key="killmail_r2z2_sync",
            reason="Killmail ingestion is enabled, but no tracked alliances or corporations are configured.",
            meta={
                "execution_mode": "python",
                "cursor": str(job_context.get("cursor") or "0"),
                "checksum": payload_checksum({"cursor": job_context.get("cursor") or "0", "rows_written": 0}),
                "tracked_alliance_count": tracked_alliance_count,
                "tracked_corporation_count": tracked_corporation_count,
                "tracked_entity_count": tracked_entity_count,
            },
        ).to_dict()

    sequence_url = str(job_context.get("sequence_url") or "").strip()
    base_url = str(job_context.get("base_url") or "").rstrip("/")
    user_agent = str(job_context.get("user_agent") or "SupplyCore killmail-ingestion/2.0")
    poll_sleep_seconds = max(6, int(job_context.get("poll_sleep_seconds") or 10))
    max_sequences = max(1, int(job_context.get("max_sequences_per_run") or 10000))
    batch_size = max(1, min(50, context.batch_size // 20 or 25))
    entity_resolver = KillmailEntityResolver(user_agent)

    if sequence_url == "" or base_url == "":
        raise RuntimeError("Killmail Python worker requires both sequence and base R2Z2 URLs.")

    start_iso = utc_now_iso()
    started_at = time.monotonic()
    deadline = started_at + max(15, context.timeout_seconds - 5)
    run_id = _sync_run_start(bridge, job_context)
    cursor_raw = str(job_context.get("cursor") or "").strip()
    last_saved_sequence = int(cursor_raw) if cursor_raw.isdigit() else None
    next_sequence: int | None = last_saved_sequence + 1 if last_saved_sequence is not None else None
    latest_remote_sequence: int | None = None
    last_processed_sequence: int | None = last_saved_sequence

    warnings: list[str] = []
    pending_payloads: list[dict[str, Any]] = []
    totals = {
        "rows_seen": 0,
        "rows_written": 0,
        "rows_matched": 0,
        "rows_skipped_existing": 0,
        "rows_filtered_out": 0,
        "rows_write_attempted": 0,
        "rows_failed": 0,
        "duplicates": 0,
        "filtered": 0,
        "invalid": 0,
    }
    total_sequence_files_fetched = 0
    total_sequence_404s = 0
    total_rate_limits = 0
    batches_flushed = 0
    first_sequence_seen: int | None = None
    last_sequence_seen: int | None = None
    checkpoint_updates = 0
    checkpoint_failures = 0
    first_failure_message = ""

    try:
        while time.monotonic() < deadline and total_sequence_files_fetched < max_sequences:
            if next_sequence is None:
                probe_status, probe_payload = _http_json(sequence_url, user_agent)
                if probe_status == 200:
                    if not isinstance(probe_payload, dict):
                        raise RuntimeError("R2Z2 sequence probe returned a non-object JSON payload.")
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
            if first_sequence_seen is None:
                first_sequence_seen = sequence_id
            last_sequence_seen = sequence_id
            status, payload = _http_json(f"{base_url}/{sequence_id}.json", user_agent)

            if status == 200:
                payload_classification, normalized_payload, valid_non_actionable = _classify_sequence_payload(payload)
                context.emit(
                    "zkill.sequence_fetch",
                    {
                        "job_key": context.job_key,
                        "sequence_id": sequence_id,
                        "http_status": status,
                        "payload_classification": payload_classification,
                        "fetch_outcome": "valid_http_200",
                        "valid_non_actionable": valid_non_actionable,
                    },
                )
                if payload_classification == "malformed_non_object_payload":
                    raise RuntimeError(
                        f"R2Z2 sequence {sequence_id} returned HTTP 200 with malformed non-object JSON payload."
                    )

                normalized_payload = entity_resolver.enrich_payload(normalized_payload)
                normalized_payload["requested_sequence_id"] = sequence_id
                normalized_payload["sequence_id"] = int(normalized_payload.get("sequence_id") or sequence_id)
                pending_payloads.append(normalized_payload)
                total_sequence_files_fetched += 1
                next_sequence = sequence_id + 1
                # R2Z2 rate limit is 20 req/s; sleep 100ms between fetches to stay at ~10 req/s
                time.sleep(0.1)
                if len(pending_payloads) >= batch_size:
                    batch_result, last_processed_sequence = _flush_pending_batch(
                        bridge=bridge,
                        job_context=job_context,
                        pending_payloads=pending_payloads,
                        batch_index=batches_flushed + 1,
                        last_processed_sequence=last_processed_sequence,
                        logger_context=context,
                        started_at=started_at,
                        totals={
                            "rows_seen": totals["rows_seen"],
                            "rows_written": totals["rows_written"],
                        },
                    )
                    pending_payloads = []
                    batches_flushed += 1
                    checkpoint_update_increment, checkpoint_failure_increment = _accumulate_batch_result(
                        batch_result=batch_result,
                        totals=totals,
                    )
                    checkpoint_updates += checkpoint_update_increment
                    checkpoint_failures += checkpoint_failure_increment
                    context.emit(
                        "zkill.sequence_processed",
                        {
                            "job_key": context.job_key,
                            "sequence_id": sequence_id,
                            "http_status": status,
                            "payload_classification": payload_classification,
                            "processing_outcome": _sequence_outcome_from_batch_result(batch_result),
                            "rows_matched": batch_result.get("rows_matched"),
                            "rows_filtered_out": batch_result.get("rows_filtered_out"),
                            "rows_skipped_existing": batch_result.get("rows_skipped_existing"),
                            "rows_written": batch_result.get("rows_written"),
                            "rows_failed": batch_result.get("rows_failed"),
                            "valid_non_actionable": valid_non_actionable
                            or int(batch_result.get("rows_written") or 0) == 0,
                            "checkpoint_state": dict((batch_result.get("meta") or {}).get("checkpoint_state") or {}),
                        },
                    )
                    continue

                if pending_payloads:
                    batch_result, last_processed_sequence = _flush_pending_batch(
                        bridge=bridge,
                        job_context=job_context,
                        pending_payloads=pending_payloads,
                        batch_index=batches_flushed + 1,
                        last_processed_sequence=last_processed_sequence,
                        logger_context=context,
                        started_at=started_at,
                        totals={
                            "rows_seen": totals["rows_seen"],
                            "rows_written": totals["rows_written"],
                        },
                    )
                    pending_payloads = []
                    batches_flushed += 1
                    checkpoint_update_increment, checkpoint_failure_increment = _accumulate_batch_result(
                        batch_result=batch_result,
                        totals=totals,
                    )
                    checkpoint_updates += checkpoint_update_increment
                    checkpoint_failures += checkpoint_failure_increment
                    context.emit(
                        "zkill.sequence_processed",
                        {
                            "job_key": context.job_key,
                            "sequence_id": sequence_id,
                            "http_status": status,
                            "payload_classification": payload_classification,
                            "processing_outcome": _sequence_outcome_from_batch_result(batch_result),
                            "rows_matched": batch_result.get("rows_matched"),
                            "rows_filtered_out": batch_result.get("rows_filtered_out"),
                            "rows_skipped_existing": batch_result.get("rows_skipped_existing"),
                            "rows_written": batch_result.get("rows_written"),
                            "rows_failed": batch_result.get("rows_failed"),
                            "valid_non_actionable": valid_non_actionable
                            or int(batch_result.get("rows_written") or 0) == 0,
                            "checkpoint_state": dict((batch_result.get("meta") or {}).get("checkpoint_state") or {}),
                        },
                    )
                continue

            if pending_payloads:
                batch_result, last_processed_sequence = _flush_pending_batch(
                    bridge=bridge,
                    job_context=job_context,
                    pending_payloads=pending_payloads,
                    batch_index=batches_flushed + 1,
                    last_processed_sequence=last_processed_sequence,
                    logger_context=context,
                    started_at=started_at,
                    totals={
                        "rows_seen": totals["rows_seen"],
                        "rows_written": totals["rows_written"],
                    },
                )
                pending_payloads = []
                batches_flushed += 1
                checkpoint_update_increment, checkpoint_failure_increment = _accumulate_batch_result(
                    batch_result=batch_result,
                    totals=totals,
                )
                checkpoint_updates += checkpoint_update_increment
                checkpoint_failures += checkpoint_failure_increment

            if status == 404:
                total_sequence_404s += 1
                context.emit(
                    "zkill.sequence_fetch",
                    {
                        "job_key": context.job_key,
                        "sequence_id": sequence_id,
                        "http_status": status,
                        "payload_classification": "not_found",
                        "fetch_outcome": "sleep_and_retry",
                        "valid_non_actionable": True,
                    },
                )
                warnings.append(f"R2Z2 returned 404 for sequence {sequence_id}; sleeping {poll_sleep_seconds}s before retry.")
                if not _sleep_with_budget(poll_sleep_seconds, deadline):
                    break
                continue

            if status in (403, 429):
                total_rate_limits += 1
                context.emit(
                    "zkill.sequence_fetch",
                    {
                        "job_key": context.job_key,
                        "sequence_id": sequence_id,
                        "http_status": status,
                        "payload_classification": "retryable_http_status",
                        "fetch_outcome": "sleep_and_retry",
                        "valid_non_actionable": False,
                    },
                )
                warnings.append(f"R2Z2 returned status {status} for sequence {sequence_id}; sleeping {poll_sleep_seconds}s before retry.")
                if not _sleep_with_budget(poll_sleep_seconds, deadline):
                    break
                continue

            context.emit(
                "zkill.sequence_fetch",
                {
                    "job_key": context.job_key,
                    "sequence_id": sequence_id,
                    "http_status": status,
                    "payload_classification": "unexpected_http_status",
                    "fetch_outcome": "fatal_fetch_error",
                    "valid_non_actionable": False,
                },
            )
            raise RuntimeError(f"R2Z2 sequence fetch failed for {sequence_id} with status={status}")

        if pending_payloads:
            batch_result, last_processed_sequence = _flush_pending_batch(
                bridge=bridge,
                job_context=job_context,
                pending_payloads=pending_payloads,
                batch_index=batches_flushed + 1,
                last_processed_sequence=last_processed_sequence,
                logger_context=context,
                started_at=started_at,
                totals={
                    "rows_seen": totals["rows_seen"],
                    "rows_written": totals["rows_written"],
                },
            )
            pending_payloads = []
            batches_flushed += 1
            checkpoint_update_increment, checkpoint_failure_increment = _accumulate_batch_result(
                batch_result=batch_result,
                totals=totals,
            )
            checkpoint_updates += checkpoint_update_increment
            checkpoint_failures += checkpoint_failure_increment

        cursor_end = str(last_processed_sequence if last_processed_sequence is not None else (last_saved_sequence or 0))
        checksum = payload_checksum({
            "rows_seen": totals["rows_seen"],
            "rows_written": totals["rows_written"],
            "cursor": cursor_end,
            "batches_flushed": batches_flushed,
        })

        checkpoint_updated = checkpoint_updates > 0 or cursor_end == str(last_saved_sequence or 0)
        reason_for_zero_write = _zero_write_reason(
            rows_seen=totals["rows_seen"],
            rows_matched=totals["rows_matched"],
            rows_filtered_out=totals["rows_filtered_out"],
            rows_skipped_existing=totals["rows_skipped_existing"],
            rows_failed=totals["rows_failed"],
            rows_written=totals["rows_written"],
            checkpoint_updated=checkpoint_updated,
            sequence_404s=total_sequence_404s,
            latest_remote_sequence=latest_remote_sequence,
            cursor_before=str(last_saved_sequence or 0),
            cursor_after=cursor_end,
        )
        if totals["rows_written"] > 0:
            outcome_reason = "Python streamed killmail ingestion batches and kept polling until the worker budget expired."
        else:
            outcome_reason = {
                "no_tracked_entity_matches": "Fetched killmails did not match any tracked alliance or corporation.",
                "already_existed_locally": "All fetched killmails were already present in local storage.",
                "write_failed": "Fetched killmails reached persistence but a write failed before completion.",
                "transaction_rolled_back": "The persistence transaction rolled back before any qualifying rows were committed.",
                "filter_excluded_all": "Fetched killmails were excluded by tracked-entity filters.",
                "checkpoint_prevented_write": "Cursor checkpointing did not advance after processing the batch.",
                "downstream_persistence_blocked": "Qualifying killmails were matched but could not be persisted locally.",
                "caught_up_to_live_tip": "Python worker caught up to the live R2Z2 tip and stayed in the documented poll/sleep loop.",
                "unknown": "Python worker completed without new killmail inserts and the zero-write cause requires follow-up.",
            }.get(reason_for_zero_write, "Python worker completed without new killmail inserts.")

        result = JobResult.success(
            job_key="killmail_r2z2_sync",
            summary="Killmail R2Z2 ingestion ran in Python with continuous polling and bridge-backed batch persistence.",
            rows_processed=totals["rows_seen"],
            rows_written=totals["rows_written"],
            rows_seen=totals["rows_seen"],
            rows_failed=totals["rows_failed"],
            duration_ms=int((time.monotonic() - started_at) * 1000),
            started_at=start_iso,
            finished_at=utc_now_iso(),
            warnings=warnings[-10:],
            batches_completed=batches_flushed,
            meta={
                "execution_mode": "python",
                "cursor": cursor_end,
                "checksum": checksum,
                "poll_sleep_seconds": poll_sleep_seconds,
                "continuous_polling": True,
                "sequence_files_fetched": total_sequence_files_fetched,
                "sequence_404s_encountered": total_sequence_404s,
                "rate_limit_responses": total_rate_limits,
                "duplicates": totals["duplicates"],
                "filtered": totals["filtered"],
                "invalid": totals["invalid"],
                "rows_matched": totals["rows_matched"],
                "rows_skipped_existing": totals["rows_skipped_existing"],
                "rows_filtered_out": totals["rows_filtered_out"],
                "rows_write_attempted": totals["rows_write_attempted"],
                "killmails_fetched": totals["rows_seen"],
                "killmails_inserted": totals["rows_written"],
                "first_sequence_seen": first_sequence_seen,
                "last_sequence_seen": last_sequence_seen,
                "cursor_before": str(last_saved_sequence or 0),
                "cursor_after": cursor_end,
                "last_saved_sequence_before_run": last_saved_sequence,
                "last_processed_sequence": last_processed_sequence,
                "latest_remote_sequence": latest_remote_sequence,
                "memory_usage_bytes": resident_memory_bytes(),
                "checkpoint_updates": checkpoint_updates,
                "checkpoint_failures": checkpoint_failures,
                "checkpoint_state": "updated" if checkpoint_updated else "unchanged",
                "reason_for_zero_write": reason_for_zero_write,
                "no_write_reason": reason_for_zero_write,
                "outcome_reason": outcome_reason,
            },
        ).to_dict()
        result["cursor"] = cursor_end
        result["checksum"] = checksum
        result["run_id"] = run_id
        _sync_run_finish(bridge, job_context, run_id, result)
        return result
    except Exception as error:
        if first_failure_message == "":
            first_failure_message = str(error)
        _sync_run_finish(
            bridge,
            job_context,
            run_id,
            JobResult.failed(
                job_key="sync_killmail_feed",
                error=str(error),
                meta={
                    "rows_seen": totals["rows_seen"],
                    "rows_written": totals["rows_written"],
                    "rows_failed": max(1, totals["rows_failed"]),
                    "checkpoint_before": str(last_saved_sequence or 0),
                    "checkpoint_after": str(last_processed_sequence if last_processed_sequence is not None else (last_saved_sequence or 0)),
                    "rows_matched": totals["rows_matched"],
                    "rows_filtered_out": totals["rows_filtered_out"],
                    "rows_skipped_existing": totals["rows_skipped_existing"],
                    "rows_write_attempted": totals["rows_write_attempted"],
                    "first_sequence_seen": first_sequence_seen,
                    "last_sequence_seen": last_sequence_seen,
                    "reason_for_zero_write": "transaction_rolled_back",
                    "checkpoint_state": "unchanged",
                    "outcome_reason": first_failure_message,
                },
            ).to_dict(),
        )
        raise
