"""Backfill killmails by downloading EveRef's daily tarballs.

https://data.everef.net/killmails/{YYYY}/killmails-{YYYY-MM-DD}.tar.bz2

Each tarball contains one ``killmails/{killmail_id}.json`` file per killmail.
Every JSON is a verbatim copy of ESI's ``/killmails/{id}/{hash}/`` response
plus two EveRef-added keys (``killmail_hash`` and ``http_last_modified``), so
no follow-up ESI fetch is ever required — this path is limited only by
network bandwidth and the ``process-killmail-batch`` bridge throughput.

For each day in the requested range, the job:

  1. Skips days already recorded in setting
     ``killmail_everef_backfill_last_date``.
  2. Downloads ``killmails-YYYY-MM-DD.tar.bz2`` to ``stage_dir`` (default
     ``/tmp/supplycore_everef``) if it isn't already staged.
  3. Streams the tarball entry by entry, converting each JSON into the
     standard ``process-killmail-batch`` payload shape.
  4. Dedupes against the DB via ``killmail-ids-existing`` in chunks of 500.
  5. Submits the survivors to ``process-killmail-batch`` with
     ``skip_entity_filter=True`` (store everything, not only tracked
     entities).
  6. Deletes the staged tarball and advances
     ``killmail_everef_backfill_last_date`` to the completed day.

The job is idempotent: restarting after a crash resumes from the last
fully-completed day, and ``killmail-ids-existing`` deduping protects
against partial processing of the in-progress day.
"""

from __future__ import annotations

import json
import logging
import os
import tarfile
import time
import urllib.error
import urllib.request
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from ..bridge import PhpBridge
from ..http_client import ipv4_opener
from ..worker_runtime import utc_now_iso

logger = logging.getLogger("supplycore.everef_backfill")

EVEREF_BASE_URL = "https://data.everef.net/killmails"
DEFAULT_STAGE_DIR = "/tmp/supplycore_everef"
DEFAULT_BATCH_SIZE = 100
DEFAULT_DEDUP_CHUNK = 500
DOWNLOAD_TIMEOUT_SECONDS = 300
DOWNLOAD_MAX_ATTEMPTS = 4
DOWNLOAD_BACKOFF_SECONDS = 5.0
PROGRESS_SETTING_KEY = "killmail_everef_backfill_progress"
LAST_DATE_SETTING_KEY = "killmail_everef_backfill_last_date"


def _everef_url_for(day: date) -> str:
    return f"{EVEREF_BASE_URL}/{day.year:04d}/killmails-{day.isoformat()}.tar.bz2"


def _stage_path(stage_dir: Path, day: date) -> Path:
    return stage_dir / f"killmails-{day.isoformat()}.tar.bz2"


def _download_with_retries(url: str, dest: Path, user_agent: str) -> tuple[int, int]:
    """Download *url* to *dest* with retries.  Returns (status, bytes_written).

    Status 200 on success, non-200 on terminal failure, 0 on network error.
    """
    headers = {"User-Agent": user_agent, "Accept": "application/x-bzip2"}
    request = urllib.request.Request(url, headers=headers)
    tmp_path = dest.with_suffix(dest.suffix + ".part")

    for attempt in range(1, DOWNLOAD_MAX_ATTEMPTS + 1):
        try:
            with ipv4_opener.open(request, timeout=DOWNLOAD_TIMEOUT_SECONDS) as response:
                status = int(getattr(response, "status", response.getcode()))
                if status != 200:
                    logger.warning("EveRef download %s returned status=%d (attempt %d)", url, status, attempt)
                    if status == 404:
                        return 404, 0
                    time.sleep(DOWNLOAD_BACKOFF_SECONDS * attempt)
                    continue
                bytes_written = 0
                with tmp_path.open("wb") as fh:
                    while True:
                        chunk = response.read(1024 * 256)
                        if not chunk:
                            break
                        fh.write(chunk)
                        bytes_written += len(chunk)
                tmp_path.replace(dest)
                return 200, bytes_written
        except urllib.error.HTTPError as exc:
            status = int(exc.code)
            logger.warning("EveRef download %s HTTPError status=%d (attempt %d)", url, status, attempt)
            if status == 404:
                return 404, 0
        except (urllib.error.URLError, OSError, TimeoutError) as exc:
            logger.warning("EveRef download %s failed: %s (attempt %d)", url, exc, attempt)
        if attempt < DOWNLOAD_MAX_ATTEMPTS:
            time.sleep(DOWNLOAD_BACKOFF_SECONDS * attempt)

    try:
        if tmp_path.exists():
            tmp_path.unlink()
    except OSError:
        pass
    return 0, 0


def _entry_to_payload(raw: dict[str, Any]) -> dict[str, Any] | None:
    """Convert one EveRef killmail JSON into a ``process-killmail-batch`` payload."""
    try:
        km_id = int(raw.get("killmail_id") or 0)
    except (TypeError, ValueError):
        return None
    km_hash = str(raw.get("killmail_hash") or "").strip()
    if km_id <= 0 or not km_hash:
        return None

    killmail_time_str = str(raw.get("killmail_time") or "")
    try:
        uploaded_at = int(
            datetime.fromisoformat(killmail_time_str.replace("Z", "+00:00")).timestamp()
        )
    except (ValueError, AttributeError):
        uploaded_at = int(time.time())

    # Strip EveRef-added keys; emit a clean ESI-native body.
    esi_body: dict[str, Any] = {
        "killmail_id": km_id,
        "killmail_time": killmail_time_str,
        "solar_system_id": raw.get("solar_system_id"),
        "victim": raw.get("victim") or {},
        "attackers": raw.get("attackers") or [],
    }
    if "war_id" in raw:
        esi_body["war_id"] = raw["war_id"]
    if "moon_id" in raw:
        esi_body["moon_id"] = raw["moon_id"]

    return {
        "killmail_id": km_id,
        "hash": km_hash,
        "esi": esi_body,
        "zkb": {},
        "sequence_id": km_id,
        "requested_sequence_id": km_id,
        "uploaded_at": uploaded_at,
    }


def _dedup_against_db(bridge: PhpBridge, killmail_ids: list[int]) -> set[int]:
    """Return the subset of ``killmail_ids`` already present in the DB."""
    existing: set[int] = set()
    for start in range(0, len(killmail_ids), DEFAULT_DEDUP_CHUNK):
        chunk = killmail_ids[start:start + DEFAULT_DEDUP_CHUNK]
        try:
            resp = bridge.call("killmail-ids-existing", payload={"killmail_ids": chunk})
            existing.update(int(x) for x in (resp.get("existing") or []))
        except Exception as exc:
            logger.warning("killmail-ids-existing failed for chunk of %d: %s", len(chunk), exc)
    return existing


def _submit_batch(bridge: PhpBridge, payloads: list[dict[str, Any]]) -> tuple[int, int, int]:
    """Submit one batch to ``process-killmail-batch``.

    Returns ``(written, filtered, duplicates)``.
    """
    if not payloads:
        return 0, 0, 0
    try:
        result = bridge.call(
            "process-killmail-batch",
            payload={"payloads": payloads, "skip_entity_filter": True},
        )
    except Exception as exc:
        logger.warning("process-killmail-batch failed (batch=%d): %s", len(payloads), exc)
        return 0, 0, 0
    batch_result = result.get("result") or {}
    return (
        int(batch_result.get("rows_written") or 0),
        int(batch_result.get("filtered") or 0),
        int(batch_result.get("duplicates") or 0),
    )


def _process_day(
    bridge: PhpBridge,
    stage_dir: Path,
    day: date,
    user_agent: str,
    batch_size: int,
) -> dict[str, int]:
    """Download, extract, and import one day's killmails.

    Returns a per-day counters dict.  Raises ``RuntimeError`` on terminal
    download failure (caller decides whether to stop or skip).
    """
    url = _everef_url_for(day)
    dest = _stage_path(stage_dir, day)

    counters = {
        "downloaded_bytes": 0,
        "seen": 0,
        "already_existing": 0,
        "submitted": 0,
        "written": 0,
        "filtered": 0,
        "duplicates": 0,
    }

    if dest.exists():
        logger.info("[%s] reusing staged tarball %s (%d bytes)", day, dest, dest.stat().st_size)
    else:
        logger.info("[%s] downloading %s", day, url)
        status, bytes_written = _download_with_retries(url, dest, user_agent)
        counters["downloaded_bytes"] = bytes_written
        if status == 404:
            logger.warning("[%s] EveRef 404 — no killmails published for this day, skipping", day)
            return counters
        if status != 200:
            raise RuntimeError(f"EveRef download failed for {day}: status={status}")

    # Stream tarball entries into fixed-size batches.
    pending_payloads: list[dict[str, Any]] = []
    pending_ids: list[int] = []

    def flush() -> None:
        if not pending_payloads:
            return
        existing = _dedup_against_db(bridge, pending_ids)
        counters["already_existing"] += len(existing)
        fresh = [p for p in pending_payloads if int(p["killmail_id"]) not in existing]
        counters["submitted"] += len(fresh)
        if fresh:
            written, filtered, duplicates = _submit_batch(bridge, fresh)
            counters["written"] += written
            counters["filtered"] += filtered
            counters["duplicates"] += duplicates
        pending_payloads.clear()
        pending_ids.clear()

    with tarfile.open(dest, mode="r:bz2") as tf:
        for member in tf:
            if not member.isfile() or not member.name.endswith(".json"):
                continue
            fh = tf.extractfile(member)
            if fh is None:
                continue
            try:
                raw_bytes = fh.read()
            finally:
                fh.close()
            try:
                raw = json.loads(raw_bytes)
            except json.JSONDecodeError:
                continue
            if not isinstance(raw, dict):
                continue
            counters["seen"] += 1
            payload = _entry_to_payload(raw)
            if payload is None:
                continue
            pending_payloads.append(payload)
            pending_ids.append(int(payload["killmail_id"]))
            if len(pending_payloads) >= batch_size:
                flush()
        flush()

    # Successful day — unstage the tarball.
    try:
        dest.unlink()
    except OSError as exc:
        logger.warning("[%s] failed to delete staged tarball %s: %s", day, dest, exc)

    logger.info(
        "[%s] done: seen=%d existing=%d submitted=%d written=%d filtered=%d duplicates=%d",
        day,
        counters["seen"],
        counters["already_existing"],
        counters["submitted"],
        counters["written"],
        counters["filtered"],
        counters["duplicates"],
    )
    return counters


def _save_progress(bridge: PhpBridge, day: date, counters: dict[str, int]) -> None:
    try:
        bridge.call("update-setting", payload={
            "key": LAST_DATE_SETTING_KEY,
            "value": day.isoformat(),
        })
    except Exception as exc:
        logger.warning("Failed to update %s=%s: %s", LAST_DATE_SETTING_KEY, day.isoformat(), exc)
    try:
        bridge.call("update-setting", payload={
            "key": PROGRESS_SETTING_KEY,
            "value": json.dumps({
                "last_completed_date": day.isoformat(),
                "updated_at": utc_now_iso(),
                **counters,
            }),
        })
    except Exception:
        pass


def run_killmail_everef_backfill(
    context: Any,
    *,
    start_date: str,
    end_date: str | None = None,
    stage_dir: str = DEFAULT_STAGE_DIR,
    batch_size: int = DEFAULT_BATCH_SIZE,
    user_agent: str = "SupplyCore killmail-everef-backfill/1.0",
) -> dict[str, Any]:
    """Backfill killmails day-by-day from EveRef daily tarballs.

    ``start_date`` / ``end_date`` are ``YYYY-MM-DD`` strings (end_date is
    inclusive, defaults to yesterday UTC).
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    bridge = PhpBridge(context.php_binary, context.app_root)

    try:
        start = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=UTC).date()
    except ValueError as exc:
        return {"status": "failed", "error": f"Invalid --start-date: {exc}"}

    yesterday = (datetime.now(UTC) - timedelta(days=1)).date()
    if end_date:
        try:
            end = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=UTC).date()
        except ValueError as exc:
            return {"status": "failed", "error": f"Invalid --end-date: {exc}"}
    else:
        end = yesterday

    if end > yesterday:
        # EveRef files for "today" are still being written in place.
        logger.info("Capping --end-date %s at yesterday %s", end, yesterday)
        end = yesterday

    if start > end:
        return {"status": "failed", "error": f"start_date ({start}) is after end_date ({end})."}

    # Resume: skip days already recorded in last_date.
    resume_after: date | None = None
    try:
        resp = bridge.call("get-setting", payload={"key": LAST_DATE_SETTING_KEY})
        raw = str((resp or {}).get("value") or "").strip()
        if raw:
            resume_after = datetime.strptime(raw, "%Y-%m-%d").replace(tzinfo=UTC).date()
    except Exception:
        resume_after = None

    effective_start = start
    if resume_after is not None and resume_after >= start:
        effective_start = resume_after + timedelta(days=1)
        if effective_start > end:
            logger.info("Already caught up: last_completed=%s >= end=%s", resume_after, end)
            return {
                "status": "success",
                "message": f"Already caught up (last_completed={resume_after.isoformat()}).",
                "resume_after": resume_after.isoformat(),
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
            }
        logger.info("Resuming from %s (last_completed=%s)", effective_start, resume_after)

    stage_path = Path(stage_dir).resolve()
    stage_path.mkdir(parents=True, exist_ok=True)
    logger.info("Stage dir: %s", stage_path)

    started_at = utc_now_iso()
    totals = {
        "days_processed": 0,
        "days_skipped_404": 0,
        "days_failed": 0,
        "downloaded_bytes": 0,
        "seen": 0,
        "already_existing": 0,
        "submitted": 0,
        "written": 0,
        "filtered": 0,
        "duplicates": 0,
    }

    current = effective_start
    while current <= end:
        try:
            counters = _process_day(bridge, stage_path, current, user_agent, batch_size)
        except Exception as exc:
            logger.error("[%s] terminal failure: %s", current, exc)
            totals["days_failed"] += 1
            # Stop the run so the caller can investigate.  Progress up to the
            # previous day is already persisted via _save_progress, so a
            # restart will resume here.
            return {
                "status": "failed",
                "error": f"Day {current.isoformat()} failed: {exc}",
                "started_at": started_at,
                "finished_at": utc_now_iso(),
                "last_completed_date": (current - timedelta(days=1)).isoformat() if current > effective_start else (resume_after.isoformat() if resume_after else None),
                **totals,
            }

        if counters["seen"] == 0 and counters["downloaded_bytes"] == 0:
            totals["days_skipped_404"] += 1
        else:
            totals["days_processed"] += 1
        for key in ("downloaded_bytes", "seen", "already_existing", "submitted",
                    "written", "filtered", "duplicates"):
            totals[key] += counters[key]

        _save_progress(bridge, current, counters)
        current += timedelta(days=1)

    return {
        "status": "success",
        "started_at": started_at,
        "finished_at": utc_now_iso(),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "effective_start": effective_start.isoformat(),
        "last_completed_date": end.isoformat(),
        **totals,
    }
