"""Theater Intelligence — Phase 2: Battle clustering into theaters.

Groups existing battles from ``battle_rollups`` into multi-system theaters
based on:
  1. Time proximity (battles within a configurable window)
  2. System proximity (same constellation or region)
  3. Participant overlap (shared characters across battles)

Populates: ``theaters``, ``theater_battles``, ``theater_systems``
"""

from __future__ import annotations

import hashlib
import sys
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from orchestrator.config import resolve_app_root  # noqa: F401
    from orchestrator.db import SupplyCoreDb
    from orchestrator.job_result import JobResult
    from orchestrator.json_utils import json_dumps_safe
    from orchestrator.job_utils import finish_job_run, start_job_run
else:
    from ..config import resolve_app_root  # noqa: F401
    from ..db import SupplyCoreDb
    from ..job_result import JobResult
    from ..json_utils import json_dumps_safe
    from ..job_utils import finish_job_run, start_job_run

# ── Configuration ────────────────────────────────────────────────────────────

# Max time gap (seconds) between two battles to be considered part of the same
# theater.  Two battles in the same constellation within this window will be
# grouped.
THEATER_TIME_WINDOW_SECONDS = 45 * 60  # 45 minutes

# Minimum participant overlap ratio to merge battles from different
# constellations (but same region) into the same theater.
MIN_PARTICIPANT_OVERLAP = 0.10  # 10%

# Minimum number of participants across all battles for a theater to be stored.
MIN_THEATER_PARTICIPANTS = 10

# Minimum participants for a battle to be a clustering seed.  Battles below
# this threshold are not used to *form* theaters but can be absorbed into an
# existing theater via sub-threshold absorption.
MIN_CLUSTERING_SEED_PARTICIPANTS = MIN_THEATER_PARTICIPANTS

# Time margin (seconds) for sub-threshold absorption: how far outside the
# theater's time window to look for small battles in the same systems.
ABSORPTION_MARGIN_SECONDS = 5 * 60  # 5 minutes

BATCH_SIZE = 500

# Gate-distance merge defaults (overridden by app_settings)
DEFAULT_MAX_GATE_DISTANCE = 5
DEFAULT_GATE_MERGE_MIN_OVERLAP = 0.05
DEFAULT_HIGHSEC_THRESHOLD = 0.45

# Lookback window: only cluster battles from the last N hours.  Older theaters
# are auto-locked so they are never recalculated unless manually unlocked.
DEFAULT_LOOKBACK_HOURS = 48  # ~2 days


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _theater_log(runtime: dict[str, Any] | None, event: str, payload: dict[str, Any]) -> None:
    log_path = str(((runtime or {}).get("log_file") or "")).strip()
    if log_path == "":
        return
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {"event": event, "timestamp": datetime.now(UTC).isoformat(), **payload}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json_dumps_safe(record) + "\n")


# ── Union-Find for merging battles ──────────────────────────────────────────

class _UnionFind:
    """Simple union-find (disjoint set) structure for merging battle groups."""

    def __init__(self) -> None:
        self._parent: dict[str, str] = {}
        self._rank: dict[str, int] = {}

    def find(self, x: str) -> str:
        if x not in self._parent:
            self._parent[x] = x
            self._rank[x] = 0
        root = x
        while self._parent[root] != root:
            root = self._parent[root]
        # Path compression
        while self._parent[x] != root:
            self._parent[x], x = root, self._parent[x]
        return root

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self._rank[ra] < self._rank[rb]:
            ra, rb = rb, ra
        self._parent[rb] = ra
        if self._rank[ra] == self._rank[rb]:
            self._rank[ra] += 1

    def groups(self) -> dict[str, list[str]]:
        result: dict[str, list[str]] = defaultdict(list)
        for key in self._parent:
            result[self.find(key)].append(key)
        return dict(result)


# ── Core logic ──────────────────────────────────────────────────────────────

def _load_clustering_settings(db: SupplyCoreDb) -> dict[str, Any]:
    """Load clustering and engagement expansion parameters from app_settings."""
    rows = db.fetch_all(
        "SELECT setting_key, setting_value FROM app_settings "
        "WHERE setting_key LIKE 'universe_graph_%%' OR setting_key LIKE 'theater_%%'"
    )
    raw = {r["setting_key"]: r["setting_value"] for r in rows}
    return {
        # Gate-distance merge settings
        "max_gate_distance": int(raw.get("universe_graph_max_gate_distance", DEFAULT_MAX_GATE_DISTANCE)),
        "gate_merge_min_overlap": float(raw.get("universe_graph_gate_merge_min_overlap", DEFAULT_GATE_MERGE_MIN_OVERLAP)),
        "ignore_highsec_adjacency": raw.get("universe_graph_ignore_highsec_adjacency", "0") == "1",
        "highsec_threshold": float(raw.get("universe_graph_highsec_threshold", DEFAULT_HIGHSEC_THRESHOLD)),
        # Engagement expansion settings
        "theater_time_window_seconds": int(raw.get("theater_time_window_seconds", THEATER_TIME_WINDOW_SECONDS)),
        "theater_min_participants": int(raw.get("theater_min_participants", MIN_THEATER_PARTICIPANTS)),
        "theater_absorption_margin_seconds": int(raw.get("theater_absorption_margin_seconds", ABSORPTION_MARGIN_SECONDS)),
        "theater_min_participant_overlap": float(raw.get("theater_min_participant_overlap", MIN_PARTICIPANT_OVERLAP)),
        # Lookback window — only battles within this window are re-clustered
        "theater_clustering_lookback_hours": int(raw.get("theater_clustering_lookback_hours", DEFAULT_LOOKBACK_HOURS)),
    }


def _load_battles(db: SupplyCoreDb, cutoff: str | None = None) -> list[dict[str, Any]]:
    """Load eligible battles within the lookback window.

    Only battles whose started_at >= *cutoff* are loaded.  Battles already
    assigned to a locked theater are always excluded regardless of age.
    """
    where_clauses = ["br.participant_count >= %s"]
    params: list[Any] = [MIN_THEATER_PARTICIPANTS]

    if cutoff is not None:
        where_clauses.append("br.started_at >= %s")
        params.append(cutoff)

    where_clauses.append(
        """br.battle_id NOT IN (
              SELECT tb.battle_id
              FROM theater_battles tb
              INNER JOIN theaters t ON t.theater_id = tb.theater_id
              WHERE t.locked_at IS NOT NULL
          )"""
    )

    where_sql = " AND ".join(where_clauses)
    return db.fetch_all(
        f"""
        SELECT
            br.battle_id,
            br.system_id,
            br.started_at,
            br.ended_at,
            br.duration_seconds,
            br.participant_count,
            br.battle_size_class,
            rs.constellation_id,
            rs.region_id,
            rs.system_name,
            rs.security
        FROM battle_rollups br
        INNER JOIN ref_systems rs ON rs.system_id = br.system_id
        WHERE {where_sql}
        ORDER BY br.started_at ASC
        """,
        tuple(params),
    )


def _load_battle_participants(db: SupplyCoreDb, battle_ids: set[str]) -> dict[str, set[int]]:
    """Load character sets per battle for overlap detection."""
    if not battle_ids:
        return {}

    result: dict[str, set[int]] = defaultdict(set)
    id_list = list(battle_ids)

    for offset in range(0, len(id_list), BATCH_SIZE):
        chunk = id_list[offset:offset + BATCH_SIZE]
        placeholders = ",".join(["%s"] * len(chunk))
        rows = db.fetch_all(
            f"""
            SELECT battle_id, character_id
            FROM battle_participants
            WHERE battle_id IN ({placeholders})
            """,
            tuple(chunk),
        )
        for row in rows:
            bid = str(row.get("battle_id") or "")
            cid = int(row.get("character_id") or 0)
            if bid and cid > 0:
                result[bid].add(cid)

    return dict(result)


def _participant_overlap(chars_a: set[int], chars_b: set[int]) -> float:
    """Jaccard-like overlap: intersection / min(|A|, |B|)."""
    if not chars_a or not chars_b:
        return 0.0
    intersection = len(chars_a & chars_b)
    smaller = min(len(chars_a), len(chars_b))
    return intersection / smaller if smaller > 0 else 0.0


def _cluster_battles(
    battles: list[dict[str, Any]],
    participants: dict[str, set[int]],
    gate_svc: Any | None = None,
    clustering_settings: dict[str, Any] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Group battles into theaters using union-find.

    Merge criteria:
    1. Same constellation + time overlap → always merge
    2. Same region + time overlap + participant overlap ≥ threshold → merge
    3. Gate distance ≤ max_gate_distance + time overlap + participant overlap (when Neo4j available)
    """
    uf = _UnionFind()

    # Index battles by constellation and region for pairwise comparison
    by_constellation: dict[int, list[dict[str, Any]]] = defaultdict(list)
    by_region: dict[int, list[dict[str, Any]]] = defaultdict(list)
    battle_map: dict[str, dict[str, Any]] = {}

    for b in battles:
        bid = str(b["battle_id"])
        uf.find(bid)  # ensure registered
        battle_map[bid] = b
        constellation_id = int(b.get("constellation_id") or 0)
        region_id = int(b.get("region_id") or 0)
        if constellation_id > 0:
            by_constellation[constellation_id].append(b)
        if region_id > 0:
            by_region[region_id].append(b)

    def _times_overlap(a: dict[str, Any], b: dict[str, Any]) -> bool:
        """Check if two battles are within THEATER_TIME_WINDOW_SECONDS of each other."""
        a_start = a["started_at"]
        a_end = a["ended_at"]
        b_start = b["started_at"]
        b_end = b["ended_at"]
        # Convert to timestamps if strings
        if isinstance(a_start, str):
            a_start = datetime.strptime(a_start, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
        if isinstance(a_end, str):
            a_end = datetime.strptime(a_end, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
        if isinstance(b_start, str):
            b_start = datetime.strptime(b_start, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
        if isinstance(b_end, str):
            b_end = datetime.strptime(b_end, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)

        gap = max(
            (b_start - a_end).total_seconds(),
            (a_start - b_end).total_seconds(),
        )
        return gap <= THEATER_TIME_WINDOW_SECONDS

    # Pass 1: merge battles in the same constellation within time window
    for constellation_id, group in by_constellation.items():
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                if _times_overlap(group[i], group[j]):
                    uf.union(str(group[i]["battle_id"]), str(group[j]["battle_id"]))

    # Pass 2: merge battles in the same region (different constellations) if
    # time overlap AND participant overlap meets threshold
    for region_id, group in by_region.items():
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                bid_i = str(group[i]["battle_id"])
                bid_j = str(group[j]["battle_id"])
                # Skip if already in same group
                if uf.find(bid_i) == uf.find(bid_j):
                    continue
                if not _times_overlap(group[i], group[j]):
                    continue
                chars_i = participants.get(bid_i, set())
                chars_j = participants.get(bid_j, set())
                if _participant_overlap(chars_i, chars_j) >= MIN_PARTICIPANT_OVERLAP:
                    uf.union(bid_i, bid_j)

    # Pass 3: Gate-distance-based merging (cross-constellation, cross-region).
    # Only runs when Neo4j is available and the gate distance service is provided.
    if gate_svc is not None:
        settings = clustering_settings or {}
        max_gate_dist = settings.get("max_gate_distance", DEFAULT_MAX_GATE_DISTANCE)
        gate_min_overlap = settings.get("gate_merge_min_overlap", DEFAULT_GATE_MERGE_MIN_OVERLAP)
        ignore_highsec = settings.get("ignore_highsec_adjacency", False)
        highsec_threshold = settings.get("highsec_threshold", DEFAULT_HIGHSEC_THRESHOLD)

        system_security: dict[int, float] = {
            int(b["system_id"]): float(b.get("security") or 0)
            for b in battles
        }

        for i in range(len(battles)):
            for j in range(i + 1, len(battles)):
                bid_i = str(battles[i]["battle_id"])
                bid_j = str(battles[j]["battle_id"])
                if uf.find(bid_i) == uf.find(bid_j):
                    continue
                if not _times_overlap(battles[i], battles[j]):
                    continue
                sys_i = int(battles[i]["system_id"])
                sys_j = int(battles[j]["system_id"])
                # Skip highsec adjacency if configured
                if ignore_highsec:
                    if (system_security.get(sys_i, 0) >= highsec_threshold
                            and system_security.get(sys_j, 0) >= highsec_threshold):
                        continue
                dist = gate_svc.distance(sys_i, sys_j)
                if dist is not None and dist <= max_gate_dist:
                    chars_i = participants.get(bid_i, set())
                    chars_j = participants.get(bid_j, set())
                    if _participant_overlap(chars_i, chars_j) >= gate_min_overlap:
                        uf.union(bid_i, bid_j)

    # Collect groups
    raw_groups = uf.groups()
    theater_groups: dict[str, list[dict[str, Any]]] = {}
    for root, battle_ids in raw_groups.items():
        theater_battles = [battle_map[bid] for bid in battle_ids if bid in battle_map]
        if theater_battles:
            theater_groups[root] = theater_battles

    return theater_groups


# ── Sub-threshold battle absorption ──────────────────────────────────────────

def _load_sub_threshold_battles(
    db: SupplyCoreDb,
    system_ids: list[int],
    time_start: str,
    time_end: str,
    already_assigned: set[str],
    margin_seconds: int = ABSORPTION_MARGIN_SECONDS,
) -> list[dict[str, Any]]:
    """Load battles below the clustering-seed threshold in the given systems/window.

    These are small skirmishes (< MIN_CLUSTERING_SEED_PARTICIPANTS) that were
    excluded from the main clustering pass but likely belong to the same
    engagement.  Only returns battles not already assigned to a theater.
    """
    if not system_ids or not time_start or not time_end:
        return []

    try:
        dt_start = datetime.strptime(time_start, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
        dt_end = datetime.strptime(time_end, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
    except (ValueError, TypeError):
        return []

    exp_start = (dt_start - timedelta(seconds=margin_seconds)).strftime("%Y-%m-%d %H:%M:%S")
    exp_end = (dt_end + timedelta(seconds=margin_seconds)).strftime("%Y-%m-%d %H:%M:%S")

    sys_ph = ",".join(["%s"] * len(system_ids))
    rows = db.fetch_all(
        f"""
        SELECT
            br.battle_id,
            br.system_id,
            br.started_at,
            br.ended_at,
            br.duration_seconds,
            br.participant_count,
            br.battle_size_class,
            rs.constellation_id,
            rs.region_id,
            rs.system_name,
            rs.security
        FROM battle_rollups br
        INNER JOIN ref_systems rs ON rs.system_id = br.system_id
        WHERE br.system_id IN ({sys_ph})
          AND br.participant_count < %s
          AND br.participant_count > 0
          AND br.started_at <= %s
          AND br.ended_at >= %s
        ORDER BY br.started_at ASC
        """,
        (*system_ids, MIN_CLUSTERING_SEED_PARTICIPANTS, exp_end, exp_start),
    )

    return [r for r in rows if str(r["battle_id"]) not in already_assigned]


def _absorb_sub_threshold_battles(
    db: SupplyCoreDb,
    theater_groups: dict[str, list[dict[str, Any]]],
    participants: dict[str, set[int]],
    computed_at: str,
) -> dict[str, int]:
    """Absorb sub-threshold battles into existing theaters.

    For each theater, find small battles in the theater's systems within its
    time window (+ margin) and add them to the theater.  This captures early
    skirmishes, stragglers, and third-party kills that weren't large enough
    to seed their own theater.

    Returns diagnostics: {theater_id: absorbed_count}.
    """
    # Collect all battle_ids already assigned to any theater
    assigned: set[str] = set()
    for group_battles in theater_groups.values():
        for b in group_battles:
            assigned.add(str(b["battle_id"]))

    diagnostics: dict[str, int] = {}
    for root, group_battles in list(theater_groups.items()):
        # Determine theater's systems and time window
        system_ids: list[int] = []
        starts: list[datetime] = []
        ends: list[datetime] = []
        for b in group_battles:
            sys_id = int(b.get("system_id") or 0)
            if sys_id > 0 and sys_id not in system_ids:
                system_ids.append(sys_id)
            s = b["started_at"]
            e = b["ended_at"]
            if isinstance(s, str):
                s = datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
            if isinstance(e, str):
                e = datetime.strptime(e, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
            starts.append(s)
            ends.append(e)

        if not starts or not ends or not system_ids:
            continue

        time_start = min(starts).strftime("%Y-%m-%d %H:%M:%S")
        time_end = max(ends).strftime("%Y-%m-%d %H:%M:%S")

        sub_battles = _load_sub_threshold_battles(
            db, system_ids, time_start, time_end, assigned,
        )
        if sub_battles:
            for sb in sub_battles:
                bid = str(sb["battle_id"])
                assigned.add(bid)
                group_battles.append(sb)
            diagnostics[root] = len(sub_battles)

    return diagnostics


# ── Opponent-aware theater splitting ───────────────────────────────────────

def _load_battle_opponent_alliances(
    db: SupplyCoreDb,
    battle_ids: set[str],
    friendly_alliance_ids: set[int],
    friendly_corporation_ids: set[int],
) -> dict[str, dict[int, int]]:
    """Load non-friendly alliance pilot counts per battle from battle_participants.

    Returns {battle_id: {alliance_id: pilot_count}}.
    Characters from friendly alliances/corporations are excluded so that
    only hostile or neutral entities remain — these define the opponent
    composition of each battle.
    """
    if not battle_ids:
        return {}

    result: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    id_list = list(battle_ids)

    for offset in range(0, len(id_list), BATCH_SIZE):
        chunk = id_list[offset:offset + BATCH_SIZE]
        placeholders = ",".join(["%s"] * len(chunk))
        rows = db.fetch_all(
            f"""
            SELECT battle_id, alliance_id, corporation_id
            FROM battle_participants
            WHERE battle_id IN ({placeholders})
              AND alliance_id IS NOT NULL
              AND alliance_id > 0
            """,
            tuple(chunk),
        )
        for row in rows:
            bid = str(row.get("battle_id") or "")
            aid = int(row.get("alliance_id") or 0)
            cid = int(row.get("corporation_id") or 0)
            if not bid or aid <= 0:
                continue
            # Skip friendly alliances and corporations
            if aid in friendly_alliance_ids:
                continue
            if cid > 0 and cid in friendly_corporation_ids:
                continue
            result[bid][aid] += 1

    return {bid: dict(counts) for bid, counts in result.items()}


def _load_friendly_ids(db: SupplyCoreDb) -> tuple[set[int], set[int]]:
    """Load friendly alliance and corporation IDs from corp_contacts + manual tracking tables."""
    friendly_alliance_ids: set[int] = set()
    friendly_corporation_ids: set[int] = set()

    # ESI corp contacts (positive standing = friendly)
    try:
        rows = db.fetch_all(
            """SELECT contact_id, contact_type
               FROM corp_contacts
               WHERE contact_type IN ('alliance', 'corporation')
                 AND standing > 0"""
        )
        for row in rows:
            cid = int(row.get("contact_id") or 0)
            if cid <= 0:
                continue
            if row.get("contact_type") == "alliance":
                friendly_alliance_ids.add(cid)
            elif row.get("contact_type") == "corporation":
                friendly_corporation_ids.add(cid)
    except Exception:
        pass

    # Manual tracked alliances/corporations
    try:
        for row in db.fetch_all("SELECT alliance_id FROM killmail_tracked_alliances WHERE active = 1"):
            aid = int(row.get("alliance_id") or 0)
            if aid > 0:
                friendly_alliance_ids.add(aid)
    except Exception:
        pass

    try:
        for row in db.fetch_all("SELECT corporation_id FROM killmail_tracked_corporations WHERE active = 1"):
            cid = int(row.get("corporation_id") or 0)
            if cid > 0:
                friendly_corporation_ids.add(cid)
    except Exception:
        pass

    return friendly_alliance_ids, friendly_corporation_ids


# Minimum absolute pilot count and share for a non-friendly alliance to
# count as a battle's "primary opponent" for the boundary-change signal.
OPPONENT_SPLIT_MIN_PILOTS = 5
OPPONENT_SPLIT_PRIMARY_MIN_SHARE = 0.30


# ── Boundary-based theater splitting ───────────────────────────────────────
#
# Instead of union-find over shared opponents (which fails when the same
# alliances appear across multiple distinct engagements), we walk the
# chronologically sorted constituent battles of a theater and score each
# boundary between adjacent battles.  Boundaries with a high enough score
# become split points.  The scoring model mirrors how a human reader
# decides "is this still the same fight, or did the engagement pivot?".
#
# Signals (weights are starting values — tune with real data):
#
#   Geographic pivot          0/+2/+3/+5  by gate distance tier:
#                                   adjacent (<=1) = 0
#                                   nearby (2)     = +2
#                                   moderate (3)   = +3 (also unknown)
#                                   far (>=4)      = +5
#   Scale jump to/from mega    +3  one side is "mega" (>=200 pilots) and the
#                                  other is not — megas are strong anchors
#   Command roster change      +3  set of is_command pilots changed
#                                  substantially between the two battles
#   Friendly overlap drop      +2  Jaccard of friendly character sets < 0.4
#   Primary opponent change    +2  dominant non-friendly alliance flipped
#   Time gap                   +1  gap between battles > 5 minutes
#
# A boundary with score >= BOUNDARY_SPLIT_THRESHOLD (default 6) becomes a
# split point.  Hard floor: battles in the same system with <30 min gap
# never split (keeps in-system grinds together).  Safety valve: same-system
# battles with >=30 min gap CAN split (catches downtime/reform cases).
#
# Threshold 6 is intentionally conservative: it requires either two strong
# signals (3+3) or a strong signal plus supporting evidence, rather than
# firing on any two weak signals.  Lower to 5 only if real data shows
# persistent under-splitting.

BOUNDARY_SPLIT_THRESHOLD = 6

BOUNDARY_WEIGHT_SCALE_JUMP = 3
BOUNDARY_WEIGHT_COMMAND_CHANGE = 3
BOUNDARY_WEIGHT_FRIENDLY_OVERLAP_DROP = 2
BOUNDARY_WEIGHT_PRIMARY_OPPONENT_CHANGE = 2
BOUNDARY_WEIGHT_TIME_GAP = 1

# Geographic pivot weight tiers.  Gate distance between the two battles'
# systems picks the weight — further apart is a stronger "different
# engagement" signal.  A 4+ gate hop alone nearly crosses the threshold,
# so any additional supporting signal will split.
BOUNDARY_WEIGHT_GEO_ADJACENT = 0
BOUNDARY_WEIGHT_GEO_NEARBY = 2
BOUNDARY_WEIGHT_GEO_MODERATE = 3    # also the fallback when distance is unknown
BOUNDARY_WEIGHT_GEO_FAR = 5

# A "mega" battle for anchor purposes — scale jumps to/from this category
# fire the scale-jump signal.
BOUNDARY_MEGA_PILOT_THRESHOLD = 200

# Friendly overlap Jaccard below this fires the overlap-drop signal.
BOUNDARY_FRIENDLY_OVERLAP_THRESHOLD = 0.40

# Command roster Jaccard below this fires the command-change signal.
BOUNDARY_COMMAND_OVERLAP_THRESHOLD = 0.50

# Minimum number of command-tagged pilots required in BOTH battles before
# the command-change signal can fire.  With only 1 command pilot on either
# side the Jaccard becomes a single-pilot artifact (0 or 1), which is too
# noisy to be trustworthy — skip the signal in that case.
BOUNDARY_COMMAND_MIN_PILOTS = 2

# Time gap in seconds that fires the time-gap signal.
BOUNDARY_TIME_GAP_SECONDS = 5 * 60

# Same-system safety valve: same system with gap >= this can still split.
BOUNDARY_SAME_SYSTEM_GAP_SECONDS = 30 * 60


def _geo_pivot_weight(prev_sys: int, curr_sys: int, gate_svc: Any | None) -> tuple[int, str]:
    """Return (weight, reason_label) for the geographic pivot signal.

    Uses gate distance tiers so that 'nearby non-adjacent' and
    'far apart' systems get different weights.  Falls back to the
    moderate tier when gate distance data is unavailable
    (GateDistanceService.distance() already swallows Neo4j errors
    and returns None on failure).
    """
    if prev_sys <= 0 or curr_sys <= 0 or prev_sys == curr_sys:
        return 0, ""
    dist = gate_svc.distance(prev_sys, curr_sys) if gate_svc is not None else None
    if dist is None:
        return BOUNDARY_WEIGHT_GEO_MODERATE, "geo_pivot(?)"
    if dist <= 1:
        return BOUNDARY_WEIGHT_GEO_ADJACENT, ""
    if dist == 2:
        return BOUNDARY_WEIGHT_GEO_NEARBY, f"geo_pivot({dist})"
    if dist == 3:
        return BOUNDARY_WEIGHT_GEO_MODERATE, f"geo_pivot({dist})"
    return BOUNDARY_WEIGHT_GEO_FAR, f"geo_pivot({dist})"


def _load_battle_friendly_characters(
    db: SupplyCoreDb,
    battle_ids: set[str],
    friendly_alliance_ids: set[int],
    friendly_corporation_ids: set[int],
) -> tuple[dict[str, set[int]], dict[str, set[int]]]:
    """Load friendly character sets and command subset per battle.

    Returns (friendly_chars, command_chars) dicts keyed by battle_id.
    A character is "friendly" if its alliance_id is in friendly_alliance_ids
    or its corporation_id is in friendly_corporation_ids.  A character is
    "command" if its is_command flag is set on that battle.
    """
    friendly_chars: dict[str, set[int]] = defaultdict(set)
    command_chars: dict[str, set[int]] = defaultdict(set)

    if not battle_ids:
        return {}, {}

    id_list = list(battle_ids)
    for offset in range(0, len(id_list), BATCH_SIZE):
        chunk = id_list[offset:offset + BATCH_SIZE]
        placeholders = ",".join(["%s"] * len(chunk))
        rows = db.fetch_all(
            f"""
            SELECT battle_id, character_id, alliance_id, corporation_id, is_command
            FROM battle_participants
            WHERE battle_id IN ({placeholders})
              AND character_id IS NOT NULL
              AND character_id > 0
            """,
            tuple(chunk),
        )
        for row in rows:
            bid = str(row.get("battle_id") or "")
            cid = int(row.get("character_id") or 0)
            if not bid or cid <= 0:
                continue
            aid = int(row.get("alliance_id") or 0)
            corp = int(row.get("corporation_id") or 0)
            is_friendly = (aid in friendly_alliance_ids) or (corp in friendly_corporation_ids)
            if not is_friendly:
                continue
            friendly_chars[bid].add(cid)
            if int(row.get("is_command") or 0) == 1:
                command_chars[bid].add(cid)

    return dict(friendly_chars), dict(command_chars)


def _battle_primary_opponent(opp: dict[int, int]) -> int:
    """Return the alliance_id of the dominant non-friendly opponent, or 0."""
    if not opp:
        return 0
    top_aid, top_count = max(opp.items(), key=lambda kv: kv[1])
    total = sum(opp.values())
    if top_count < OPPONENT_SPLIT_MIN_PILOTS:
        return 0
    if total > 0 and (top_count / total) < OPPONENT_SPLIT_PRIMARY_MIN_SHARE:
        return 0
    return int(top_aid)


def _jaccard(a: set[int], b: set[int]) -> float:
    if not a and not b:
        return 1.0
    union = len(a | b)
    if union == 0:
        return 1.0
    return len(a & b) / union


def _boundary_score(
    b_prev: dict[str, Any],
    b_curr: dict[str, Any],
    battle_opponents: dict[str, dict[int, int]],
    friendly_chars: dict[str, set[int]],
    command_chars: dict[str, set[int]],
    gate_svc: Any | None,
) -> tuple[int, list[str]]:
    """Compute a boundary score between two chronologically adjacent battles.

    Returns (score, reasons) where reasons is a list of which signals fired
    — useful for logging/diagnostics.
    """
    score = 0
    reasons: list[str] = []

    prev_bid = str(b_prev["battle_id"])
    curr_bid = str(b_curr["battle_id"])
    prev_sys = int(b_prev.get("system_id") or 0)
    curr_sys = int(b_curr.get("system_id") or 0)

    # Parse times (robust against str/datetime input)
    def _dt(v: Any) -> datetime | None:
        if isinstance(v, datetime):
            return v if v.tzinfo else v.replace(tzinfo=UTC)
        if isinstance(v, str) and v:
            try:
                return datetime.strptime(v, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
            except ValueError:
                return None
        return None

    prev_end = _dt(b_prev.get("ended_at")) or _dt(b_prev.get("started_at"))
    curr_start = _dt(b_curr.get("started_at")) or _dt(b_curr.get("ended_at"))
    gap_seconds = 0.0
    if prev_end and curr_start:
        gap_seconds = max(0.0, (curr_start - prev_end).total_seconds())

    # Hard floor: same system + short gap → never split
    same_system = prev_sys > 0 and prev_sys == curr_sys
    if same_system and gap_seconds < BOUNDARY_SAME_SYSTEM_GAP_SECONDS:
        return 0, ["same_system_floor"]

    # Geographic pivot — distance-tiered so adjacent hops don't fire,
    # nearby (2 gates) adds a small weight, and far (4+ gates) adds a
    # large weight that nearly reaches the threshold alone.
    geo_weight, geo_reason = _geo_pivot_weight(prev_sys, curr_sys, gate_svc)
    if geo_weight > 0:
        score += geo_weight
        reasons.append(geo_reason)

    # Scale jump to/from mega
    prev_pilots = int(b_prev.get("participant_count") or 0)
    curr_pilots = int(b_curr.get("participant_count") or 0)
    prev_is_mega = prev_pilots >= BOUNDARY_MEGA_PILOT_THRESHOLD
    curr_is_mega = curr_pilots >= BOUNDARY_MEGA_PILOT_THRESHOLD
    if prev_is_mega != curr_is_mega:
        score += BOUNDARY_WEIGHT_SCALE_JUMP
        reasons.append("scale_jump_mega")

    # Command roster change — only trust the signal when both battles have
    # enough command-tagged pilots that the Jaccard isn't a single-pilot
    # statistical artifact.
    prev_cmd = command_chars.get(prev_bid, set())
    curr_cmd = command_chars.get(curr_bid, set())
    if len(prev_cmd) >= BOUNDARY_COMMAND_MIN_PILOTS and len(curr_cmd) >= BOUNDARY_COMMAND_MIN_PILOTS:
        cmd_jaccard = _jaccard(prev_cmd, curr_cmd)
        if cmd_jaccard < BOUNDARY_COMMAND_OVERLAP_THRESHOLD:
            score += BOUNDARY_WEIGHT_COMMAND_CHANGE
            reasons.append(f"command_change({cmd_jaccard:.2f})")

    # Friendly overlap drop (Jaccard of friendly character sets)
    prev_friendly = friendly_chars.get(prev_bid, set())
    curr_friendly = friendly_chars.get(curr_bid, set())
    if prev_friendly or curr_friendly:
        friendly_jaccard = _jaccard(prev_friendly, curr_friendly)
        if friendly_jaccard < BOUNDARY_FRIENDLY_OVERLAP_THRESHOLD:
            score += BOUNDARY_WEIGHT_FRIENDLY_OVERLAP_DROP
            reasons.append(f"friendly_overlap_drop({friendly_jaccard:.2f})")

    # Primary opponent change
    prev_primary = _battle_primary_opponent(battle_opponents.get(prev_bid, {}))
    curr_primary = _battle_primary_opponent(battle_opponents.get(curr_bid, {}))
    if prev_primary and curr_primary and prev_primary != curr_primary:
        score += BOUNDARY_WEIGHT_PRIMARY_OPPONENT_CHANGE
        reasons.append("primary_opponent_change")

    # Time gap
    if gap_seconds >= BOUNDARY_TIME_GAP_SECONDS:
        score += BOUNDARY_WEIGHT_TIME_GAP
        reasons.append(f"time_gap({int(gap_seconds)}s)")

    return score, reasons


def _split_by_boundary_scoring(
    theater_groups: dict[str, list[dict[str, Any]]],
    battle_opponents: dict[str, dict[int, int]],
    friendly_chars: dict[str, set[int]],
    command_chars: dict[str, set[int]],
    gate_svc: Any | None,
    runtime: dict[str, Any] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Walk each theater's chronologically sorted battles and split at
    boundaries whose multi-signal score exceeds the threshold.

    The result is phase-like: a theater's battles are partitioned into
    contiguous time-ordered segments, each becoming its own theater.
    """
    new_groups: dict[str, list[dict[str, Any]]] = {}

    for root, group_battles in theater_groups.items():
        if len(group_battles) <= 1:
            new_groups[root] = group_battles
            continue

        # Sort chronologically by start time, with battle_id as a tiebreaker
        def _start(b: dict[str, Any]) -> tuple[datetime, str]:
            s = b.get("started_at")
            if isinstance(s, str):
                try:
                    s = datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
                except ValueError:
                    s = datetime.min.replace(tzinfo=UTC)
            elif isinstance(s, datetime):
                s = s if s.tzinfo else s.replace(tzinfo=UTC)
            else:
                s = datetime.min.replace(tzinfo=UTC)
            return (s, str(b.get("battle_id") or ""))

        sorted_battles = sorted(group_battles, key=_start)

        # Score each boundary between consecutive battles.  Log every
        # boundary (not just splits) so we can tune thresholds and weights
        # against real data.
        segment_starts: list[int] = [0]  # indices where a new segment begins
        all_boundaries: list[dict[str, Any]] = []
        for i in range(1, len(sorted_battles)):
            score, reasons = _boundary_score(
                sorted_battles[i - 1],
                sorted_battles[i],
                battle_opponents,
                friendly_chars,
                command_chars,
                gate_svc,
            )
            did_split = score >= BOUNDARY_SPLIT_THRESHOLD
            if did_split:
                segment_starts.append(i)
            all_boundaries.append({
                "boundary_index": i,
                "prev_battle": str(sorted_battles[i - 1]["battle_id"]),
                "prev_system": int(sorted_battles[i - 1].get("system_id") or 0),
                "curr_battle": str(sorted_battles[i]["battle_id"]),
                "curr_system": int(sorted_battles[i].get("system_id") or 0),
                "score": score,
                "reasons": reasons,
                "split": did_split,
            })

        # Always log the full boundary analysis for theaters with >1 battle,
        # even when no split occurred — this is our tuning feedback loop.
        if all_boundaries:
            _theater_log(runtime, "theater_clustering.boundary_analysis", {
                "theater_root": root,
                "battle_count": len(sorted_battles),
                "segments": len(segment_starts),
                "split_threshold": BOUNDARY_SPLIT_THRESHOLD,
                "boundaries": all_boundaries,
            })

        if len(segment_starts) == 1:
            # No splits
            new_groups[root] = sorted_battles
            continue

        # Build segments
        segment_starts.append(len(sorted_battles))
        for seg_idx in range(len(segment_starts) - 1):
            start_i = segment_starts[seg_idx]
            end_i = segment_starts[seg_idx + 1]
            segment_battles = sorted_battles[start_i:end_i]
            if not segment_battles:
                continue
            # Use the first battle's ID as the new group key (deterministic)
            new_key = f"{root}:seg{seg_idx}:{segment_battles[0]['battle_id']}"
            new_groups[new_key] = segment_battles

    return new_groups


def _compute_theater_id(battle_ids: list[str]) -> str:
    """Deterministic theater ID from sorted constituent battle IDs."""
    canonical = "|".join(sorted(battle_ids))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _build_theater_row(
    theater_id: str,
    battles: list[dict[str, Any]],
    participants: dict[str, set[int]],
    computed_at: str,
) -> dict[str, Any]:
    """Build a theater summary row from its constituent battles."""
    all_systems: dict[int, dict[str, Any]] = {}
    all_participants: set[int] = set()
    start_times: list[datetime] = []
    end_times: list[datetime] = []
    total_kills = 0

    for b in battles:
        bid = str(b["battle_id"])
        system_id = int(b.get("system_id") or 0)
        system_name = str(b.get("system_name") or "")
        participant_count = int(b.get("participant_count") or 0)

        s_start = b["started_at"]
        s_end = b["ended_at"]
        if isinstance(s_start, str):
            s_start = datetime.strptime(s_start, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
        if isinstance(s_end, str):
            s_end = datetime.strptime(s_end, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
        start_times.append(s_start)
        end_times.append(s_end)

        total_kills += participant_count  # approximate: will be refined in analysis phase

        if system_id > 0:
            sys_entry = all_systems.setdefault(system_id, {
                "system_id": system_id,
                "system_name": system_name,
                "kill_count": 0,
                "participant_count": 0,
            })
            sys_entry["kill_count"] += participant_count
            sys_entry["participant_count"] += participant_count

        chars = participants.get(bid, set())
        all_participants.update(chars)

    theater_start = min(start_times)
    theater_end = max(end_times)
    duration = max(1, int((theater_end - theater_start).total_seconds()))

    # Primary system = system with most participants
    primary_system_id = None
    primary_region_id = None
    if all_systems:
        primary = max(all_systems.values(), key=lambda s: s["participant_count"])
        primary_system_id = primary["system_id"]

    # Get region from first battle
    for b in battles:
        region_id = int(b.get("region_id") or 0)
        if region_id > 0:
            primary_region_id = region_id
            break

    return {
        "theater_id": theater_id,
        "label": None,
        "primary_system_id": primary_system_id,
        "region_id": primary_region_id,
        "start_time": theater_start.strftime("%Y-%m-%d %H:%M:%S"),
        "end_time": theater_end.strftime("%Y-%m-%d %H:%M:%S"),
        "duration_seconds": duration,
        "battle_count": len(battles),
        "system_count": len(all_systems),
        "total_kills": 0,  # placeholder — refined in theater_analysis
        "total_isk": 0,    # placeholder — refined in theater_analysis
        "participant_count": len(all_participants),
        "anomaly_score": 0,
        "computed_at": computed_at,
        "systems": all_systems,
    }


# ── DB writes ───────────────────────────────────────────────────────────────

def _flush_theaters(
    db: SupplyCoreDb,
    theaters: list[dict[str, Any]],
    battle_assignments: list[tuple[str, str, int, float, str | None]],
    system_rows: list[tuple[str, int, str | None, int, float, int, float, str | None]],
    cutoff: str | None = None,
) -> int:
    """Write theater data to MariaDB. Returns total rows written.

    When *cutoff* is given, only unlocked theaters whose start_time >= cutoff
    are eligible for replacement.  Theaters outside the window (older) and
    locked theaters are never touched.
    """
    rows_written = 0

    # Collect new theater IDs so we can prune stale rows
    new_theater_ids = {t["theater_id"] for t in theaters}

    with db.transaction() as (_, cursor):
        # Identify unlocked theater IDs that fall within the processing
        # window — these are the ones we are allowed to replace.
        if cutoff is not None:
            cursor.execute(
                "SELECT theater_id FROM theaters WHERE locked_at IS NULL AND start_time >= %s",
                (cutoff,),
            )
            stale_window_ids = {str(r["theater_id"]) for r in cursor.fetchall()}
        else:
            cursor.execute("SELECT theater_id FROM theaters WHERE locked_at IS NULL")
            stale_window_ids = {str(r["theater_id"]) for r in cursor.fetchall()}

        # IDs to delete = old unlocked theaters in the window that are NOT in the new output
        delete_ids = stale_window_ids - new_theater_ids
        if delete_ids:
            id_placeholders = ",".join(["%s"] * len(delete_ids))
            cursor.execute(f"DELETE FROM theater_systems WHERE theater_id IN ({id_placeholders})", tuple(delete_ids))
            cursor.execute(f"DELETE FROM theater_battles WHERE theater_id IN ({id_placeholders})", tuple(delete_ids))
            cursor.execute(f"DELETE FROM theaters WHERE theater_id IN ({id_placeholders})", tuple(delete_ids))
            for tbl in (
                "theater_alliance_summary",
                "theater_participants",
                "theater_timeline",
                "theater_turning_points",
                "theater_structure_kills",
                "theater_side_composition",
            ):
                try:
                    cursor.execute(f"DELETE FROM {tbl} WHERE theater_id IN ({id_placeholders})", tuple(delete_ids))
                except Exception:
                    pass

        # Delete existing battle/system rows for theaters we're re-inserting
        if new_theater_ids:
            new_id_placeholders = ",".join(["%s"] * len(new_theater_ids))
            cursor.execute(f"DELETE FROM theater_systems WHERE theater_id IN ({new_id_placeholders})", tuple(new_theater_ids))
            cursor.execute(f"DELETE FROM theater_battles WHERE theater_id IN ({new_id_placeholders})", tuple(new_theater_ids))

        # Upsert theaters — preserve analysis fields (total_kills, total_isk, anomaly_score)
        # Collapse the per-theater cursor.execute() loop into a single
        # executemany() to cut N round-trips down to one.
        if theaters:
            theater_rows = [
                (
                    t["theater_id"], t["label"], t["primary_system_id"], t["region_id"],
                    t["start_time"], t["end_time"], t["duration_seconds"],
                    t["battle_count"], t["system_count"], t["total_kills"], t["total_isk"],
                    t["participant_count"], t["anomaly_score"],
                    t.get("max_gate_span"), t.get("avg_gate_distance"), t.get("clustering_method", "constellation"),
                    t["computed_at"],
                )
                for t in theaters
            ]
            cursor.executemany(
                """
                INSERT INTO theaters (
                    theater_id, label, primary_system_id, region_id,
                    start_time, end_time, duration_seconds,
                    battle_count, system_count, total_kills, total_isk,
                    participant_count, anomaly_score,
                    max_gate_span, avg_gate_distance, clustering_method,
                    computed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    label = VALUES(label),
                    primary_system_id = VALUES(primary_system_id),
                    region_id = VALUES(region_id),
                    start_time = VALUES(start_time),
                    end_time = VALUES(end_time),
                    duration_seconds = VALUES(duration_seconds),
                    battle_count = VALUES(battle_count),
                    system_count = VALUES(system_count),
                    participant_count = VALUES(participant_count),
                    max_gate_span = VALUES(max_gate_span),
                    avg_gate_distance = VALUES(avg_gate_distance),
                    clustering_method = VALUES(clustering_method),
                    computed_at = VALUES(computed_at)
                """,
                theater_rows,
            )
            rows_written += max(0, int(cursor.rowcount or 0))

        # Insert theater_battles
        for chunk_start in range(0, len(battle_assignments), BATCH_SIZE):
            chunk = battle_assignments[chunk_start:chunk_start + BATCH_SIZE]
            cursor.executemany(
                """
                INSERT INTO theater_battles (theater_id, battle_id, system_id, weight, phase)
                VALUES (%s, %s, %s, %s, %s)
                """,
                chunk,
            )
            rows_written += max(0, int(cursor.rowcount or 0))

        # Insert theater_systems
        for chunk_start in range(0, len(system_rows), BATCH_SIZE):
            chunk = system_rows[chunk_start:chunk_start + BATCH_SIZE]
            cursor.executemany(
                """
                INSERT INTO theater_systems (
                    theater_id, system_id, system_name, kill_count,
                    isk_destroyed, participant_count, weight, phase
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                chunk,
            )
            rows_written += max(0, int(cursor.rowcount or 0))

    return rows_written


# ── Auto-lock aged-out theaters ────────────────────────────────────────────

def _auto_lock_old_theaters(db: SupplyCoreDb, cutoff: str, now_sql: str) -> int:
    """Lock unlocked theaters whose end_time is before *cutoff*.

    These theaters have fully aged out of the lookback window and should not
    be recalculated on future runs.  Users can manually unlock them via the
    ``locked_at`` column if needed.

    Returns the number of theaters locked.
    """
    return db.execute(
        """
        UPDATE theaters
        SET locked_at = %s
        WHERE locked_at IS NULL
          AND end_time < %s
        """,
        (now_sql, cutoff),
    )


# ── Entry point ─────────────────────────────────────────────────────────────

def run_theater_clustering(
    db: SupplyCoreDb,
    runtime: dict[str, Any] | None = None,
    neo4j_raw: dict[str, Any] | None = None,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Cluster battles into theaters and persist results."""
    job = start_job_run(db, "theater_clustering")
    started_monotonic = datetime.now(UTC)
    rows_processed = 0
    rows_written = 0
    computed_at = _now_sql()
    _theater_log(runtime, "theater_clustering.job.started", {"dry_run": dry_run, "computed_at": computed_at})

    try:
        # 0. Compute lookback window cutoff
        clustering_settings = _load_clustering_settings(db)
        lookback_hours = clustering_settings["theater_clustering_lookback_hours"]
        if lookback_hours > 0:
            cutoff_dt = datetime.now(UTC) - timedelta(hours=lookback_hours)
            cutoff_sql: str | None = cutoff_dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            cutoff_sql = None  # 0 = no window, process everything (legacy)
        _theater_log(runtime, "theater_clustering.lookback_window", {
            "lookback_hours": lookback_hours,
            "cutoff": cutoff_sql,
        })

        # 1. Load eligible battles within the lookback window
        battles = _load_battles(db, cutoff=cutoff_sql)
        rows_processed = len(battles)
        _theater_log(runtime, "theater_clustering.battles_loaded", {"count": len(battles), "cutoff": cutoff_sql})

        if not battles:
            finish_job_run(db, job, status="success", rows_processed=0, rows_written=0, meta={"theaters": 0})
            duration_ms = int((datetime.now(UTC) - started_monotonic).total_seconds() * 1000)
            result = JobResult.success(
                job_key="theater_clustering", summary="No eligible battles found.", rows_processed=0, rows_written=0, duration_ms=duration_ms,
            ).to_dict()
            _theater_log(runtime, "theater_clustering.job.success", result)
            return result

        # 2. Load participant sets for overlap detection
        battle_ids = {str(b["battle_id"]) for b in battles}
        participants = _load_battle_participants(db, battle_ids)
        _theater_log(runtime, "theater_clustering.participants_loaded", {"battles_with_participants": len(participants)})

        # 3. Optional gate distance service (clustering_settings loaded in step 0)
        gate_svc = None
        try:
            from ..neo4j import Neo4jClient, Neo4jConfig
            neo4j_config = Neo4jConfig.from_runtime(neo4j_raw or {})
            if neo4j_config.enabled:
                from ..services.gate_distance import GateDistanceService
                gate_svc = GateDistanceService(
                    Neo4jClient(neo4j_config),
                    max_distance=clustering_settings["max_gate_distance"],
                )
        except Exception:
            gate_svc = None  # Graceful degradation

        # 4. Cluster battles into theater groups
        theater_groups = _cluster_battles(battles, participants, gate_svc, clustering_settings)
        _theater_log(runtime, "theater_clustering.clustered", {"theater_count": len(theater_groups)})

        # 4b. Absorb sub-threshold battles into existing theaters.
        # Small skirmishes (< MIN_CLUSTERING_SEED_PARTICIPANTS) in the same
        # systems/time window are captured here so they are not lost from the
        # engagement's event set.
        absorption_diag = _absorb_sub_threshold_battles(db, theater_groups, participants, computed_at)
        if absorption_diag:
            total_absorbed = sum(absorption_diag.values())
            _theater_log(runtime, "theater_clustering.sub_threshold_absorbed", {
                "theaters_with_absorptions": len(absorption_diag),
                "total_battles_absorbed": total_absorbed,
            })

        # 4c. Boundary-based splitting: walk each theater's chronologically
        # sorted battles and split at phase boundaries (geographic pivot,
        # scale jump, command roster change, friendly overlap drop,
        # primary opponent change, time gap).  This handles the common
        # case of concurrent or sequential engagements against overlapping
        # opponents that get clustered together by the broad pass above.
        friendly_aids, friendly_cids = _load_friendly_ids(db)
        all_theater_battle_ids: set[str] = set()
        for group_battles in theater_groups.values():
            for b in group_battles:
                all_theater_battle_ids.add(str(b["battle_id"]))
        battle_opponents = _load_battle_opponent_alliances(db, all_theater_battle_ids, friendly_aids, friendly_cids)
        friendly_chars, command_chars = _load_battle_friendly_characters(
            db, all_theater_battle_ids, friendly_aids, friendly_cids,
        )

        pre_split_count = len(theater_groups)
        theater_groups = _split_by_boundary_scoring(
            theater_groups,
            battle_opponents,
            friendly_chars,
            command_chars,
            gate_svc,
            runtime,
        )
        split_count = len(theater_groups) - pre_split_count
        if split_count > 0:
            _theater_log(runtime, "theater_clustering.boundary_split_summary", {
                "theaters_before": pre_split_count,
                "theaters_after": len(theater_groups),
                "new_theaters_from_splits": split_count,
            })

        # 5. Build theater rows
        theater_rows: list[dict[str, Any]] = []
        battle_assignments: list[tuple[str, str, int, float, str | None]] = []
        system_rows: list[tuple[str, int, str | None, int, float, int, float, str | None]] = []

        for _root, group_battles in theater_groups.items():
            battle_ids_in_group = [str(b["battle_id"]) for b in group_battles]
            theater_id = _compute_theater_id(battle_ids_in_group)

            theater = _build_theater_row(theater_id, group_battles, participants, computed_at)

            # Enrich with gate-distance metrics when available
            if gate_svc is not None and len(theater["systems"]) > 1:
                sys_ids = list(theater["systems"].keys())
                distances: list[int] = []
                for si in range(len(sys_ids)):
                    for sj in range(si + 1, len(sys_ids)):
                        d = gate_svc.distance(int(sys_ids[si]), int(sys_ids[sj]))
                        if d is not None:
                            distances.append(d)
                theater["max_gate_span"] = max(distances) if distances else None
                theater["avg_gate_distance"] = round(sum(distances) / len(distances), 2) if distances else None
                theater["clustering_method"] = "gate_distance"
            else:
                theater["max_gate_span"] = None
                theater["avg_gate_distance"] = None
                theater["clustering_method"] = "constellation"

            # Skip tiny theaters
            if theater["participant_count"] < MIN_THEATER_PARTICIPANTS:
                continue

            theater_rows.append(theater)

            # Build battle assignment rows
            for b in group_battles:
                bid = str(b["battle_id"])
                sys_id = int(b.get("system_id") or 0)
                battle_assignments.append((theater_id, bid, sys_id, 1.0, None))

            # Build system rows
            for sys_id, sys_info in theater["systems"].items():
                total_sys_participants = sum(
                    int(bb.get("participant_count") or 0)
                    for bb in group_battles
                )
                weight = (sys_info["participant_count"] / total_sys_participants) if total_sys_participants > 0 else 1.0
                system_rows.append((
                    theater_id,
                    int(sys_id),
                    sys_info.get("system_name"),
                    sys_info["kill_count"],
                    0,  # isk_destroyed — refined in analysis phase
                    sys_info["participant_count"],
                    round(weight, 4),
                    None,  # phase
                ))

        _theater_log(runtime, "theater_clustering.theaters_built", {
            "theater_count": len(theater_rows),
            "battle_assignments": len(battle_assignments),
            "system_rows": len(system_rows),
        })

        # 6. Flush to DB (scoped to the lookback window)
        if not dry_run:
            rows_written = _flush_theaters(db, theater_rows, battle_assignments, system_rows, cutoff=cutoff_sql)

            # 7. Auto-lock theaters that have fully aged out of the window
            if cutoff_sql is not None:
                auto_locked = _auto_lock_old_theaters(db, cutoff_sql, computed_at)
                if auto_locked:
                    _theater_log(runtime, "theater_clustering.auto_locked", {"theaters_locked": auto_locked})

        finish_job_run(
            db, job,
            status="success",
            rows_processed=rows_processed,
            rows_written=rows_written,
            meta={"computed_at": computed_at, "theater_count": len(theater_rows)},
        )

        duration_ms = int((datetime.now(UTC) - started_monotonic).total_seconds() * 1000)
        result = JobResult.success(
            job_key="theater_clustering",
            summary=f"Clustered {rows_processed} battles into {len(theater_rows)} theaters (lookback {lookback_hours}h).",
            rows_processed=rows_processed,
            rows_written=0 if dry_run else rows_written,
            duration_ms=duration_ms,
            meta={
                "computed_at": computed_at,
                "theater_count": len(theater_rows),
                "battle_assignments": len(battle_assignments),
                "system_rows": len(system_rows),
                "lookback_hours": lookback_hours,
                "cutoff": cutoff_sql,
                "dry_run": dry_run,
            },
        ).to_dict()
        _theater_log(runtime, "theater_clustering.job.success", result)
        return result

    except Exception as exc:
        finish_job_run(db, job, status="failed", rows_processed=rows_processed, rows_written=rows_written, error_text=str(exc))
        _theater_log(runtime, "theater_clustering.job.failed", {"status": "failed", "error": str(exc)})
        raise
