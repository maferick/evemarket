from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from .sync_runtime import run_sync_phase_job


def _http_json(url: str, user_agent: str, timeout_seconds: int = 25) -> tuple[int, Any]:
    request = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": user_agent})
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            status = int(getattr(response, "status", response.getcode()))
            payload = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as error:
        return int(error.code), {}
    except urllib.error.URLError as error:
        raise RuntimeError(f"R2Z2 request failed: {error.reason}") from error
    if not payload.strip():
        return status, {}
    try:
        return status, json.loads(payload)
    except json.JSONDecodeError as error:
        if status == 200:
            raise RuntimeError(f"R2Z2 returned invalid JSON: {error.msg}") from error
        return status, {}


def _transform_payload(payload: dict[str, Any], db: SupplyCoreDb) -> dict[str, Any]:
    """Transform R2Z2 payload into killmail_events row format."""
    killmail = payload.get("esi") or payload.get("killmail") or {}
    victim = killmail.get("victim") or {}
    zkb = payload.get("zkb") or {}
    sequence_id = int(payload.get("sequence_id") or payload.get("requested_sequence_id") or 0)
    system_id = int(killmail["solar_system_id"]) if killmail.get("solar_system_id") is not None else None

    # Resolve region_id from solar_system_id
    region_id = None
    if system_id and system_id > 0:
        row = db.fetch_one("SELECT region_id FROM ref_systems WHERE system_id = %s LIMIT 1", (system_id,))
        if row:
            region_id = int(row["region_id"])

    # Parse killmail_time
    killmail_time = None
    if killmail.get("killmail_time"):
        try:
            dt = datetime.fromisoformat(str(killmail["killmail_time"]).replace("Z", "+00:00"))
            killmail_time = dt.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass

    uploaded_at = None
    if payload.get("uploaded_at"):
        try:
            uploaded_at = datetime.fromtimestamp(int(payload["uploaded_at"]), tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass

    return {
        "sequence_id": sequence_id,
        "killmail_id": int(payload.get("killmail_id") or 0),
        "killmail_hash": str(payload.get("hash") or ""),
        "uploaded_at": uploaded_at,
        "sequence_updated": int(payload["sequence_updated"]) if payload.get("sequence_updated") is not None else None,
        "killmail_time": killmail_time,
        "solar_system_id": system_id,
        "region_id": region_id,
        "victim_character_id": int(victim["character_id"]) if victim.get("character_id") is not None else None,
        "victim_corporation_id": int(victim["corporation_id"]) if victim.get("corporation_id") is not None else None,
        "victim_alliance_id": int(victim["alliance_id"]) if victim.get("alliance_id") is not None else None,
        "victim_ship_type_id": int(victim["ship_type_id"]) if victim.get("ship_type_id") is not None else None,
        "victim_damage_taken": int(victim["damage_taken"]) if victim.get("damage_taken") is not None else None,
        "zkb_total_value": float(zkb["totalValue"]) if zkb.get("totalValue") is not None else None,
        "zkb_fitted_value": float(zkb["fittedValue"]) if zkb.get("fittedValue") is not None else None,
        "zkb_dropped_value": float(zkb["droppedValue"]) if zkb.get("droppedValue") is not None else None,
        "zkb_destroyed_value": float(zkb["destroyedValue"]) if zkb.get("destroyedValue") is not None else None,
        "zkb_points": int(zkb["points"]) if zkb.get("points") is not None else None,
        "zkb_npc": bool(zkb.get("npc")),
        "zkb_solo": bool(zkb.get("solo")),
        "zkb_awox": bool(zkb.get("awox")),
        "zkb_json": json.dumps(zkb, separators=(",", ":"), ensure_ascii=False),
        "raw_killmail_json": json.dumps(killmail, separators=(",", ":"), ensure_ascii=False),
        "attackers": killmail.get("attackers") or [],
        "items": victim.get("items") or [],
    }


def _matches_tracked_entities(event: dict[str, Any], tracked_alliance_ids: set[int], tracked_corp_ids: set[int]) -> bool:
    if not tracked_alliance_ids and not tracked_corp_ids:
        return True
    victim_alliance = event.get("victim_alliance_id")
    if victim_alliance and int(victim_alliance) in tracked_alliance_ids:
        return True
    victim_corp = event.get("victim_corporation_id")
    if victim_corp and int(victim_corp) in tracked_corp_ids:
        return True
    return False


def _event_exists(db: SupplyCoreDb, sequence_id: int, killmail_id: int, killmail_hash: str) -> bool:
    row = db.fetch_one(
        "SELECT sequence_id FROM killmail_events WHERE sequence_id = %s OR (killmail_id = %s AND killmail_hash = %s) LIMIT 1",
        (sequence_id, killmail_id, killmail_hash),
    )
    return row is not None


def _persist_event(db: SupplyCoreDb, event: dict[str, Any]) -> bool:
    """Insert or update a killmail event and its payload."""
    with db.cursor() as (conn, cur):
        # Upsert killmail_events
        cur.execute(
            """INSERT INTO killmail_events
                  (sequence_id, killmail_id, killmail_hash, uploaded_at, sequence_updated,
                   killmail_time, solar_system_id, region_id,
                   victim_character_id, victim_corporation_id, victim_alliance_id,
                   victim_ship_type_id, victim_damage_taken, battle_id,
                   zkb_total_value, zkb_fitted_value, zkb_dropped_value, zkb_destroyed_value,
                   zkb_points, zkb_npc, zkb_solo, zkb_awox, zkb_json, raw_killmail_json)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON DUPLICATE KEY UPDATE
                  killmail_id=VALUES(killmail_id), killmail_hash=VALUES(killmail_hash),
                  uploaded_at=VALUES(uploaded_at), sequence_updated=VALUES(sequence_updated),
                  killmail_time=VALUES(killmail_time), solar_system_id=VALUES(solar_system_id),
                  region_id=VALUES(region_id),
                  victim_character_id=VALUES(victim_character_id),
                  victim_corporation_id=VALUES(victim_corporation_id),
                  victim_alliance_id=VALUES(victim_alliance_id),
                  victim_ship_type_id=VALUES(victim_ship_type_id),
                  victim_damage_taken=VALUES(victim_damage_taken),
                  battle_id=VALUES(battle_id),
                  zkb_total_value=VALUES(zkb_total_value), zkb_fitted_value=VALUES(zkb_fitted_value),
                  zkb_dropped_value=VALUES(zkb_dropped_value), zkb_destroyed_value=VALUES(zkb_destroyed_value),
                  zkb_points=VALUES(zkb_points), zkb_npc=VALUES(zkb_npc),
                  zkb_solo=VALUES(zkb_solo), zkb_awox=VALUES(zkb_awox),
                  zkb_json=VALUES(zkb_json), raw_killmail_json=VALUES(raw_killmail_json),
                  updated_at=CURRENT_TIMESTAMP""",
            (
                event["sequence_id"], event["killmail_id"], event["killmail_hash"],
                event.get("uploaded_at"), event.get("sequence_updated"),
                event.get("killmail_time"), event.get("solar_system_id"), event.get("region_id"),
                event.get("victim_character_id"), event.get("victim_corporation_id"),
                event.get("victim_alliance_id"), event.get("victim_ship_type_id"),
                event.get("victim_damage_taken"), None,
                event.get("zkb_total_value"), event.get("zkb_fitted_value"),
                event.get("zkb_dropped_value"), event.get("zkb_destroyed_value"),
                event.get("zkb_points"),
                int(event.get("zkb_npc") or 0), int(event.get("zkb_solo") or 0), int(event.get("zkb_awox") or 0),
                event.get("zkb_json", "{}"), event.get("raw_killmail_json", "{}"),
            ),
        )

        # Upsert killmail_event_payloads
        cur.execute(
            """INSERT INTO killmail_event_payloads (sequence_id, killmail_id, killmail_hash, zkb_json, raw_killmail_json)
               VALUES (%s, %s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE
                  killmail_id=VALUES(killmail_id), killmail_hash=VALUES(killmail_hash),
                  zkb_json=VALUES(zkb_json), raw_killmail_json=VALUES(raw_killmail_json),
                  updated_at=CURRENT_TIMESTAMP""",
            (event["sequence_id"], event["killmail_id"], event["killmail_hash"],
             event.get("zkb_json", "{}"), event.get("raw_killmail_json", "{}")),
        )

        # Replace attackers
        seq = event["sequence_id"]
        cur.execute("DELETE FROM killmail_attackers WHERE sequence_id = %s", (seq,))
        attackers = event.get("attackers") or []
        for i, attacker in enumerate(attackers):
            if not isinstance(attacker, dict):
                continue
            cur.execute(
                """INSERT INTO killmail_attackers
                      (sequence_id, attacker_index, character_id, corporation_id, alliance_id,
                       ship_type_id, weapon_type_id, damage_done, final_blow, security_status)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (seq, i,
                 int(attacker["character_id"]) if attacker.get("character_id") is not None else None,
                 int(attacker["corporation_id"]) if attacker.get("corporation_id") is not None else None,
                 int(attacker["alliance_id"]) if attacker.get("alliance_id") is not None else None,
                 int(attacker["ship_type_id"]) if attacker.get("ship_type_id") is not None else None,
                 int(attacker["weapon_type_id"]) if attacker.get("weapon_type_id") is not None else None,
                 int(attacker["damage_done"]) if attacker.get("damage_done") is not None else None,
                 int(bool(attacker.get("final_blow", False))),
                 float(attacker["security_status"]) if attacker.get("security_status") is not None else None),
            )

        # Replace items
        cur.execute("DELETE FROM killmail_items WHERE sequence_id = %s", (seq,))
        items = event.get("items") or []
        item_index = 0
        for item in _flatten_items(items):
            if not isinstance(item, dict):
                continue
            qty_dropped = int(item.get("quantity_dropped") or 0)
            qty_destroyed = int(item.get("quantity_destroyed") or 0)
            role = "dropped" if qty_dropped > 0 else ("destroyed" if qty_destroyed > 0 else "fitted")
            cur.execute(
                """INSERT INTO killmail_items
                      (sequence_id, item_index, item_type_id, item_flag, quantity_dropped, quantity_destroyed, singleton, item_role)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                (seq, item_index,
                 int(item.get("item_type_id") or 0),
                 int(item["flag"]) if item.get("flag") is not None else None,
                 qty_dropped, qty_destroyed,
                 int(item["singleton"]) if item.get("singleton") is not None else None,
                 role),
            )
            item_index += 1

        conn.commit()
    return True


def _flatten_items(items: list[Any], depth: int = 0) -> list[dict[str, Any]]:
    """Recursively flatten nested item containers (e.g., cargo containers)."""
    result: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        result.append(item)
        nested = item.get("items")
        if isinstance(nested, list) and depth < 5:
            result.extend(_flatten_items(nested, depth + 1))
    return result


class _EntityResolver:
    """Cache-backed ESI entity resolver for enriching killmail actors."""
    def __init__(self, user_agent: str):
        self.user_agent = user_agent
        self._char_cache: dict[int, dict[str, Any] | None] = {}
        self._corp_cache: dict[int, dict[str, Any] | None] = {}

    def _fetch(self, url: str) -> dict[str, Any] | None:
        status, data = _http_json(url, self.user_agent)
        return data if status == 200 and isinstance(data, dict) else None

    def enrich_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        for key in ("esi", "killmail"):
            km = payload.get(key)
            if not isinstance(km, dict):
                continue
            victim = km.get("victim")
            if isinstance(victim, dict):
                self._enrich_actor(victim)
            for attacker in (km.get("attackers") or []):
                if isinstance(attacker, dict):
                    self._enrich_actor(attacker)
        return payload

    def _enrich_actor(self, actor: dict[str, Any]) -> None:
        char_id = int(actor.get("character_id") or 0)
        corp_id = int(actor.get("corporation_id") or 0)
        alliance_id = int(actor.get("alliance_id") or 0)
        if corp_id <= 0 and char_id > 0:
            if char_id not in self._char_cache:
                self._char_cache[char_id] = self._fetch(f"https://esi.evetech.net/latest/characters/{char_id}/?datasource=tranquility")
            profile = self._char_cache[char_id] or {}
            corp_id = int(profile.get("corporation_id") or 0)
            if corp_id > 0:
                actor["corporation_id"] = corp_id
        if alliance_id <= 0 and corp_id > 0:
            if corp_id not in self._corp_cache:
                self._corp_cache[corp_id] = self._fetch(f"https://esi.evetech.net/latest/corporations/{corp_id}/?datasource=tranquility")
            profile = self._corp_cache[corp_id] or {}
            alliance_id = int(profile.get("alliance_id") or 0)
            if alliance_id > 0:
                actor["alliance_id"] = alliance_id


def _processor(db: SupplyCoreDb) -> dict[str, object]:
    enabled = (db.fetch_app_setting("killmail_ingestion_enabled", "0") or "0").strip()
    if enabled != "1":
        return {
            "status": "skipped", "rows_processed": 0, "rows_written": 0,
            "warnings": ["Killmail ingestion is disabled in settings."],
            "summary": "Killmail R2Z2 sync skipped: ingestion disabled.",
        }

    sequence_url = (db.fetch_app_setting("killmail_r2z2_sequence_url", "https://r2z2.zkillboard.com/ephemeral/sequence.json") or "").strip()
    base_url = (db.fetch_app_setting("killmail_r2z2_base_url", "https://r2z2.zkillboard.com/ephemeral") or "").rstrip("/")
    user_agent = (db.fetch_app_setting("killmail_user_agent", "SupplyCore killmail-ingestion/2.0") or "SupplyCore killmail-ingestion/2.0").strip()
    max_sequences = max(1, min(200, int(db.fetch_app_setting("killmail_ingestion_max_sequences_per_run", "120") or 120)))

    if not sequence_url or not base_url:
        return {
            "status": "failed", "rows_processed": 0, "rows_written": 0,
            "warnings": ["Missing R2Z2 URL configuration."],
            "summary": "Killmail R2Z2 sync failed: missing URL config.",
        }

    # Load tracked entities for filtering
    tracked_alliances = {int(r["alliance_id"]) for r in db.fetch_all("SELECT alliance_id FROM killmail_tracked_alliances WHERE is_active = 1")}
    tracked_corps = {int(r["corporation_id"]) for r in db.fetch_all("SELECT corporation_id FROM killmail_tracked_corporations WHERE is_active = 1")}

    # Get current cursor from sync_state
    cursor_row = db.fetch_one("SELECT last_cursor FROM sync_state WHERE dataset_key = 'killmail.r2z2.stream' LIMIT 1")
    cursor_raw = str(cursor_row.get("last_cursor") or "").strip() if cursor_row else ""
    last_saved_sequence = int(cursor_raw) if cursor_raw.isdigit() else None
    next_sequence: int | None = last_saved_sequence + 1 if last_saved_sequence is not None else None

    entity_resolver = _EntityResolver(user_agent)
    started = time.monotonic()
    deadline = started + 150  # 2.5 min budget within the 3 min timeout

    rows_seen = 0
    rows_written = 0
    rows_filtered = 0
    rows_duplicate = 0
    rows_failed = 0
    sequences_fetched = 0
    sequence_404s = 0
    last_processed_sequence: int | None = last_saved_sequence
    warnings: list[str] = []

    # Probe for latest sequence if no cursor
    if next_sequence is None:
        status, probe = _http_json(sequence_url, user_agent)
        if status == 200 and isinstance(probe, dict):
            remote_seq = int(probe.get("sequence") or 0)
            if remote_seq > 0:
                next_sequence = remote_seq
        elif status in (403, 429):
            return {
                "rows_processed": 0, "rows_written": 0,
                "warnings": [f"R2Z2 sequence probe returned {status}, backing off."],
                "summary": f"Killmail R2Z2: rate limited by sequence probe ({status}).",
            }
        else:
            return {
                "status": "failed", "rows_processed": 0, "rows_written": 0,
                "warnings": [f"R2Z2 sequence probe failed with status {status}."],
                "summary": f"Killmail R2Z2: sequence probe failed ({status}).",
            }

    while time.monotonic() < deadline and sequences_fetched < max_sequences and next_sequence is not None:
        seq_id = int(next_sequence)
        status, payload = _http_json(f"{base_url}/{seq_id}.json", user_agent)

        if status == 404:
            sequence_404s += 1
            # We've caught up to the live tip
            break

        if status in (403, 429):
            warnings.append(f"R2Z2 rate limited at sequence {seq_id}.")
            break

        if status != 200:
            warnings.append(f"R2Z2 returned {status} for sequence {seq_id}.")
            break

        if not isinstance(payload, dict) or not payload:
            next_sequence = seq_id + 1
            sequences_fetched += 1
            last_processed_sequence = seq_id
            continue

        # Enrich with ESI data (alliance/corp resolution)
        payload = entity_resolver.enrich_payload(payload)
        payload["sequence_id"] = int(payload.get("sequence_id") or seq_id)
        payload["requested_sequence_id"] = seq_id

        event = _transform_payload(payload, db)
        rows_seen += 1
        sequences_fetched += 1

        if event["sequence_id"] <= 0:
            next_sequence = seq_id + 1
            last_processed_sequence = seq_id
            continue

        # Dedup check
        if _event_exists(db, event["sequence_id"], event["killmail_id"], event["killmail_hash"]):
            rows_duplicate += 1
            next_sequence = seq_id + 1
            last_processed_sequence = seq_id
            continue

        # Entity filter
        if not _matches_tracked_entities(event, tracked_alliances, tracked_corps):
            rows_filtered += 1
            next_sequence = seq_id + 1
            last_processed_sequence = seq_id
            continue

        # Persist
        try:
            _persist_event(db, event)
            rows_written += 1
        except Exception as exc:
            rows_failed += 1
            warnings.append(f"Failed to persist sequence {seq_id}: {exc}")

        next_sequence = seq_id + 1
        last_processed_sequence = seq_id

    # Update cursor
    cursor_end = str(last_processed_sequence) if last_processed_sequence is not None else cursor_raw
    if cursor_end and cursor_end != cursor_raw:
        db.upsert_sync_state(
            dataset_key="killmail.r2z2.stream",
            status="success",
            row_count=rows_written,
            cursor=cursor_end,
        )

    at_tip = sequence_404s > 0 and rows_seen == 0
    summary_parts = [f"{rows_written} inserted"]
    if rows_duplicate:
        summary_parts.append(f"{rows_duplicate} duplicate")
    if rows_filtered:
        summary_parts.append(f"{rows_filtered} filtered")
    if at_tip:
        summary_parts.append("caught up to live tip")

    return {
        "rows_processed": rows_seen,
        "rows_written": rows_written,
        "rows_seen": rows_seen,
        "rows_failed": rows_failed,
        "warnings": warnings[-10:],
        "summary": f"Killmail R2Z2: {sequences_fetched} sequences fetched, {', '.join(summary_parts)}.",
        "checkpoint_before": cursor_raw,
        "checkpoint_after": cursor_end,
        "meta": {
            "sequences_fetched": sequences_fetched,
            "sequence_404s": sequence_404s,
            "rows_duplicate": rows_duplicate,
            "rows_filtered": rows_filtered,
            "cursor_before": cursor_raw,
            "cursor_after": cursor_end,
            "at_live_tip": at_tip,
        },
    }


def run_killmail_r2z2_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="killmail_r2z2_sync", phase="A", objective="killmail r2z2 ingestion", processor=_processor)
