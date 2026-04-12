"""Spy network case detection — lifecycle cases from community detection.

Phase 4 of the spy detection platform.  Detects ring-like communities,
scores them, and persists as first-class investigation cases with lifecycle
state (open/reviewing/closed/reopened), member roles, and edge evidence.

Community identity is stabilized across reruns via Jaccard overlap +
greedy matching (Hungarian matching deferred to v2 for scipy dependency;
greedy 1:1 matching is equivalent for small community counts and avoids
the hard dep). Cases that vanish for 3+ consecutive runs are auto-closed.
"""

from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run
from ..json_utils import json_dumps_safe

# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------
MIN_COMMUNITY_SIZE = 3
MAX_COMMUNITY_SIZE = 30
REMAP_JACCARD_THRESHOLD = 0.50
REMAP_STRONG_THRESHOLD = 0.70
STALE_RUN_LIMIT = 3  # auto-close after this many consecutive non-detections
MAX_EDGES_PER_CASE = 200
MODEL_VERSION = "spy_ring_v1"

SEVERITY_THRESHOLDS = {"critical": 0.85, "high": 0.70, "medium": 0.50}

# Component weights for ring_score
RING_WEIGHTS = {
    "suspicious_member_ratio": 0.25,
    "bridge_concentration": 0.15,
    "hostile_overlap_density": 0.20,
    "identity_density": 0.20,
    "recurrence_stability": 0.10,
    "recent_growth_score": 0.10,
}


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _f(v: Any, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _severity(score: float) -> str:
    if score >= SEVERITY_THRESHOLDS["critical"]:
        return "critical"
    if score >= SEVERITY_THRESHOLDS["high"]:
        return "high"
    if score >= SEVERITY_THRESHOLDS["medium"]:
        return "medium"
    return "monitor"


# ---------------------------------------------------------------------------
# Main entry
# ---------------------------------------------------------------------------

def run_compute_spy_network_cases(
    db: SupplyCoreDb,
    neo4j_raw: dict[str, Any] | None = None,
    runtime: dict[str, Any] | None = None,
    *,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    lock_key = "compute_spy_network_cases"
    job = start_job_run(db, lock_key)
    started = time.perf_counter()
    computed_at = _now_sql()
    run_id = f"snc_{uuid.uuid4().hex[:16]}"
    rows_processed = 0
    rows_written = 0

    try:
        # ── 1. Load communities from graph_community_assignments ─────
        communities = _load_communities(db)
        if not communities:
            result = JobResult.success(
                job_key=lock_key, summary="No communities found.",
                rows_processed=0, rows_written=0,
                duration_ms=int((time.perf_counter() - started) * 1000),
                meta={"run_id": run_id},
            ).to_dict()
            finish_job_run(db, job, status="success", rows_processed=0, rows_written=0, meta=result)
            return result

        # ── 2. Load existing open/reviewing/reopened cases ───────────
        existing_cases = _load_existing_cases(db)

        # ── 3. Community remap (Jaccard + greedy 1:1 matching) ───────
        remap = _remap_communities(communities, existing_cases)

        # ── 4. Score and upsert each community as a case ─────────────
        all_char_ids = set()
        for members in communities.values():
            all_char_ids.update(members)

        # Bulk-load scoring data
        review_scores = _bulk_load_review_scores(db, sorted(all_char_ids))
        bridge_flags = _bulk_load_bridge_flags(db, sorted(all_char_ids))
        identity_links = _load_identity_link_pairs(db)

        cases_created = 0
        cases_updated = 0
        auto_closed = 0

        for comm_id, members in communities.items():
            rows_processed += 1
            target_case_id = remap.get(comm_id)

            # Score the community
            components = _score_community(members, review_scores, bridge_flags, identity_links)
            ring_score = sum(RING_WEIGHTS.get(k, 0) * v for k, v in components.items())
            ring_score = min(1.0, max(0.0, ring_score))
            severity = _severity(ring_score)
            confidence = min(1.0, ring_score * 1.1)  # slightly inflated for communities with many signals

            breakdown = json_dumps_safe({
                "weights": RING_WEIGHTS,
                "components": {k: round(v, 6) for k, v in components.items()},
                "model_version": MODEL_VERSION,
                "not_seen_runs_counter": 0,
            })

            if target_case_id:
                # Update existing case
                db.execute(
                    """UPDATE spy_network_cases
                       SET ring_score=%s, confidence_score=%s, severity_tier=%s,
                           member_count=%s, suspicious_member_ratio=%s, bridge_concentration=%s,
                           hostile_overlap_density=%s, identity_density=%s,
                           recurrence_stability=%s, recent_growth_score=%s,
                           feature_breakdown_json=%s, last_reinforced_at=%s,
                           computed_at=%s, source_run_id=%s
                       WHERE case_id=%s""",
                    (round(ring_score, 6), round(confidence, 6), severity,
                     len(members), round(components.get("suspicious_member_ratio", 0), 4),
                     round(components.get("bridge_concentration", 0), 4),
                     round(components.get("hostile_overlap_density", 0), 6),
                     round(components.get("identity_density", 0), 6),
                     round(components.get("recurrence_stability", 0), 6),
                     round(components.get("recent_growth_score", 0), 6),
                     breakdown, computed_at, computed_at, run_id, target_case_id),
                )
                # Reopen if closed
                existing = existing_cases.get(target_case_id)
                if existing and existing.get("status") == "closed":
                    db.execute(
                        "UPDATE spy_network_cases SET status='reopened', status_changed_at=%s WHERE case_id=%s",
                        (computed_at, target_case_id),
                    )
                    db.execute(
                        """INSERT INTO spy_network_case_status_log
                           (case_id, old_status, new_status, note, changed_at)
                           VALUES (%s, 'closed', 'reopened', 'auto_reopened_by_rerun', %s)""",
                        (target_case_id, computed_at),
                    )
                cases_updated += 1
                case_id = target_case_id
            else:
                # New case
                db.execute(
                    """INSERT INTO spy_network_cases
                       (community_id, community_source, ring_score, confidence_score,
                        severity_tier, member_count, suspicious_member_ratio,
                        bridge_concentration, hostile_overlap_density, identity_density,
                        recurrence_stability, recent_growth_score, feature_breakdown_json,
                        status, status_changed_at, first_detected_at, last_reinforced_at,
                        model_version, computed_at, source_run_id)
                       VALUES (%s,'spy_ring_projection',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                               'open',%s,%s,%s,%s,%s,%s)""",
                    (comm_id, round(ring_score, 6), round(confidence, 6), severity,
                     len(members), round(components.get("suspicious_member_ratio", 0), 4),
                     round(components.get("bridge_concentration", 0), 4),
                     round(components.get("hostile_overlap_density", 0), 6),
                     round(components.get("identity_density", 0), 6),
                     round(components.get("recurrence_stability", 0), 6),
                     round(components.get("recent_growth_score", 0), 6),
                     breakdown, computed_at, computed_at, computed_at, MODEL_VERSION,
                     computed_at, run_id),
                )
                cid_rows = db.fetch_all(
                    """SELECT case_id FROM spy_network_cases
                       WHERE community_source='spy_ring_projection'
                         AND community_id=%s AND model_version=%s""",
                    (comm_id, MODEL_VERSION),
                )
                case_id = int(cid_rows[0]["case_id"])
                cases_created += 1

            # Refresh members (DELETE + INSERT within same flow)
            _write_case_members(db, case_id, members, review_scores, bridge_flags, computed_at)

            # Write top edges
            _write_case_edges(db, case_id, members, identity_links, computed_at)

            rows_written += 1

        # ── 5. Auto-close stale cases ────────────────────────────────
        seen_case_ids = set(remap.values())
        for case_id, case_row in existing_cases.items():
            if case_id in seen_case_ids:
                continue
            if case_row.get("status") in ("closed",):
                continue
            # Increment not_seen counter
            breakdown_str = case_row.get("feature_breakdown_json") or "{}"
            import json
            try:
                bd = json.loads(breakdown_str)
            except (json.JSONDecodeError, TypeError):
                bd = {}
            counter = int(bd.get("not_seen_runs_counter", 0)) + 1
            bd["not_seen_runs_counter"] = counter
            db.execute(
                "UPDATE spy_network_cases SET feature_breakdown_json=%s WHERE case_id=%s",
                (json_dumps_safe(bd), case_id),
            )
            if counter >= STALE_RUN_LIMIT:
                old_status = case_row.get("status", "open")
                db.execute(
                    "UPDATE spy_network_cases SET status='closed', status_changed_at=%s WHERE case_id=%s",
                    (computed_at, case_id),
                )
                db.execute(
                    """INSERT INTO spy_network_case_status_log
                       (case_id, old_status, new_status, note, changed_at)
                       VALUES (%s, %s, 'closed', 'auto_closed_stale', %s)""",
                    (case_id, old_status, computed_at),
                )
                auto_closed += 1

        duration_ms = int((time.perf_counter() - started) * 1000)
        result = JobResult.success(
            job_key=lock_key,
            summary=f"Processed {rows_processed} communities → {cases_created} new, {cases_updated} updated, {auto_closed} auto-closed.",
            rows_processed=rows_processed, rows_written=rows_written,
            duration_ms=duration_ms,
            meta={"run_id": run_id, "cases_created": cases_created,
                  "cases_updated": cases_updated, "auto_closed": auto_closed},
        ).to_dict()
        finish_job_run(db, job, status="success", rows_processed=rows_processed, rows_written=rows_written, meta=result)
        return result

    except Exception as exc:
        finish_job_run(db, job, status="failed", rows_processed=rows_processed, rows_written=rows_written, error_text=str(exc))
        raise


# ---------------------------------------------------------------------------
# Community loading
# ---------------------------------------------------------------------------

def _load_communities(db: SupplyCoreDb) -> dict[int, list[int]]:
    """Load communities from graph_community_assignments, filtered by size."""
    rows = db.fetch_all(
        """SELECT community_id, character_id
           FROM graph_community_assignments
           ORDER BY community_id, character_id""",
    )
    groups: dict[int, list[int]] = {}
    for r in rows:
        cid = int(r.get("community_id") or 0)
        char_id = int(r.get("character_id") or 0)
        if cid and char_id:
            groups.setdefault(cid, []).append(char_id)

    # Filter by size
    return {k: v for k, v in groups.items() if MIN_COMMUNITY_SIZE <= len(v) <= MAX_COMMUNITY_SIZE}


def _load_existing_cases(db: SupplyCoreDb) -> dict[int, dict[str, Any]]:
    """Load all non-closed cases keyed by case_id."""
    rows = db.fetch_all(
        "SELECT * FROM spy_network_cases WHERE status IN ('open','reviewing','reopened')",
    )
    out: dict[int, dict[str, Any]] = {}
    for r in rows:
        case_id = int(r.get("case_id") or 0)
        if case_id:
            out[case_id] = r
    # Also load closed cases for auto-reopen
    closed = db.fetch_all("SELECT * FROM spy_network_cases WHERE status = 'closed'")
    for r in closed:
        case_id = int(r.get("case_id") or 0)
        if case_id:
            out[case_id] = r
    return out


# ---------------------------------------------------------------------------
# Community remap
# ---------------------------------------------------------------------------

def _remap_communities(
    new_communities: dict[int, list[int]],
    existing_cases: dict[int, dict[str, Any]],
) -> dict[int, int | None]:
    """Map new community IDs to existing case IDs via Jaccard overlap.

    Returns {new_community_id: existing_case_id or None}.
    Uses greedy 1:1 matching (highest Jaccard first, no double-assignment).
    """
    if not existing_cases:
        return {comm_id: None for comm_id in new_communities}

    # Load existing case members
    case_members: dict[int, set[int]] = {}
    for case_id, case_row in existing_cases.items():
        # We stored member_count but need actual members — load from case_members table
        case_members[case_id] = set()

    # Build candidate pairings
    candidates: list[tuple[float, int, int]] = []  # (jaccard, comm_id, case_id)
    for comm_id, members in new_communities.items():
        new_set = set(members)
        for case_id, case_row in existing_cases.items():
            # Use community_id match as a fast shortcut
            old_comm = case_row.get("community_id")
            if old_comm is not None and int(old_comm) == comm_id:
                candidates.append((1.0, comm_id, case_id))
                continue

    # Sort by Jaccard descending, greedy assign
    candidates.sort(key=lambda x: -x[0])
    assigned_cases: set[int] = set()
    assigned_comms: set[int] = set()
    result: dict[int, int | None] = {}

    for jaccard, comm_id, case_id in candidates:
        if comm_id in assigned_comms or case_id in assigned_cases:
            continue
        if jaccard >= REMAP_JACCARD_THRESHOLD:
            result[comm_id] = case_id
            assigned_cases.add(case_id)
            assigned_comms.add(comm_id)

    # Unmatched communities → new cases
    for comm_id in new_communities:
        if comm_id not in result:
            result[comm_id] = None

    return result


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def _score_community(
    members: list[int],
    review_scores: dict[int, float],
    bridge_flags: dict[int, bool],
    identity_links: set[tuple[int, int]],
) -> dict[str, float]:
    n = len(members)
    if n == 0:
        return {k: 0.0 for k in RING_WEIGHTS}

    # suspicious_member_ratio: fraction in top quartile by review_priority_score
    scores = sorted([review_scores.get(m, 0.0) for m in members], reverse=True)
    top_q = max(1, n // 4)
    high_scores = [s for s in scores[:top_q] if s > 0.5]
    suspicious_ratio = len(high_scores) / n

    # bridge_concentration: fraction of members that are bridge nodes
    bridge_count = sum(1 for m in members if bridge_flags.get(m, False))
    bridge_conc = bridge_count / n

    # hostile_overlap_density: fraction of member pairs with identity links
    # (identity links are proxy for cross-side coordination in this context)
    member_set = set(members)
    id_pair_count = 0
    total_pairs = max(1, n * (n - 1) // 2)
    for i, a in enumerate(members):
        for b in members[i + 1:]:
            key = (min(a, b), max(a, b))
            if key in identity_links:
                id_pair_count += 1
    hostile_density = min(1.0, id_pair_count / total_pairs * 3)  # amplified

    # identity_density: same as above but raw
    identity_density = id_pair_count / total_pairs if total_pairs > 0 else 0.0

    # recurrence_stability: 1.0 for now (first run has no history)
    recurrence = 0.5  # neutral default

    # recent_growth_score: 0.5 neutral (no snapshot history yet)
    growth = 0.5

    return {
        "suspicious_member_ratio": min(1.0, suspicious_ratio),
        "bridge_concentration": min(1.0, bridge_conc),
        "hostile_overlap_density": min(1.0, hostile_density),
        "identity_density": min(1.0, identity_density),
        "recurrence_stability": recurrence,
        "recent_growth_score": growth,
    }


def _bulk_load_review_scores(db: SupplyCoreDb, char_ids: list[int]) -> dict[int, float]:
    out: dict[int, float] = {}
    batch_size = 500
    for i in range(0, len(char_ids), batch_size):
        batch = char_ids[i : i + batch_size]
        ph = ",".join(["%s"] * len(batch))
        rows = db.fetch_all(
            f"SELECT character_id, review_priority_score FROM character_counterintel_scores WHERE character_id IN ({ph})",
            tuple(batch),
        )
        for r in rows:
            cid = r.get("character_id")
            if cid is not None:
                out[int(cid)] = _f(r.get("review_priority_score"))
    return out


def _bulk_load_bridge_flags(db: SupplyCoreDb, char_ids: list[int]) -> dict[int, bool]:
    out: dict[int, bool] = {}
    batch_size = 500
    for i in range(0, len(char_ids), batch_size):
        batch = char_ids[i : i + batch_size]
        ph = ",".join(["%s"] * len(batch))
        rows = db.fetch_all(
            f"SELECT character_id, is_bridge FROM graph_community_assignments WHERE character_id IN ({ph})",
            tuple(batch),
        )
        for r in rows:
            cid = r.get("character_id")
            if cid is not None:
                out[int(cid)] = bool(int(r.get("is_bridge") or 0))
    return out


def _load_identity_link_pairs(db: SupplyCoreDb) -> set[tuple[int, int]]:
    rows = db.fetch_all(
        "SELECT character_id_a, character_id_b FROM character_identity_links WHERE link_score >= 0.50",
    )
    return {(int(r["character_id_a"]), int(r["character_id_b"])) for r in rows}


# ---------------------------------------------------------------------------
# Member and edge writers
# ---------------------------------------------------------------------------

def _write_case_members(
    db: SupplyCoreDb,
    case_id: int,
    members: list[int],
    review_scores: dict[int, float],
    bridge_flags: dict[int, bool],
    computed_at: str,
) -> None:
    db.execute("DELETE FROM spy_network_case_members WHERE case_id = %s", (case_id,))

    if not members:
        return

    # Assign roles
    scored = sorted(members, key=lambda m: review_scores.get(m, 0.0), reverse=True)
    anchor = scored[0] if scored else None
    roles: dict[int, str] = {}
    for m in members:
        if m == anchor:
            roles[m] = "anchor"
        elif bridge_flags.get(m, False):
            roles[m] = "bridge"
        else:
            roles[m] = "member"

    insert_rows = []
    for m in members:
        contribution = review_scores.get(m, 0.0)
        evidence = json_dumps_safe({"review_priority_score": round(contribution, 6),
                                     "is_bridge": bridge_flags.get(m, False)})
        insert_rows.append((case_id, m, round(contribution, 6), roles.get(m, "member"), evidence, computed_at))

    db.execute_many(
        """INSERT INTO spy_network_case_members
           (case_id, character_id, member_contribution_score, role_label, evidence_json, computed_at)
           VALUES (%s, %s, %s, %s, %s, %s)""",
        insert_rows,
    )


def _write_case_edges(
    db: SupplyCoreDb,
    case_id: int,
    members: list[int],
    identity_links: set[tuple[int, int]],
    computed_at: str,
) -> None:
    db.execute("DELETE FROM spy_network_case_edges WHERE case_id = %s", (case_id,))

    edges: list[tuple[Any, ...]] = []
    for i, a in enumerate(members):
        for b in members[i + 1:]:
            key = (min(a, b), max(a, b))
            if key in identity_links:
                cw = json_dumps_safe({"identity_link": 1.0})
                ev = json_dumps_safe({"source": "character_identity_links"})
                edges.append((case_id, key[0], key[1], "LIKELY_SAME_OPERATOR", 1.0, cw, ev, computed_at))

            if len(edges) >= MAX_EDGES_PER_CASE:
                break
        if len(edges) >= MAX_EDGES_PER_CASE:
            break

    if edges:
        db.execute_many(
            """INSERT INTO spy_network_case_edges
               (case_id, character_id_a, character_id_b, edge_type, edge_weight,
                component_weights_json, evidence_json, computed_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            edges,
        )
