"""Neo4j Intelligence Pipeline — suspicion signals, historical overlap, export.

This job runs the full intelligence computation pipeline inside Neo4j:

1. Pre-run inspection of existing graph state
2. Gap-fill: load new killmail/character/alliance data from MariaDB into Neo4j
3. Compute suspicion signals (presence, encounter vs engagement, peer norm, etc.)
4. Compute historical alliance overlap
5. Cross-system correlation
6. Export results to MariaDB output tables

All Cypher writes use MERGE to be idempotent.  ComputeCheckpoint nodes track
progress for resumable runs.
"""
from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

import json

from ..db import SupplyCoreDb
from ..eve_constants import HIGH_LOSS_ROLES, LOW_KILL_ROLES
from ..job_result import JobResult
from ..json_utils import json_dumps_safe
from ..neo4j import Neo4jClient, Neo4jConfig, Neo4jError

BATCH_SIZE = 500
RELATIONSHIP_WINDOW_DAYS = 30
OVERLAP_LOOKBACK_DAYS = 730

# Default suspicion signal weights — can be overridden by analyst recalibration.
DEFAULT_WEIGHTS = {
    "selective_non_engagement": 0.35,
    "peer_norm_kills": -0.20,
    "peer_norm_damage": -0.20,
    "high_presence_low_output": 0.15,
    "token_participation": 0.10,
    "loss_without_attack": 0.10,
}


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _load_recalibrated_weights(db: SupplyCoreDb) -> dict[str, float]:
    """Load the latest recalibrated weights from analyst_recalibration_log, or defaults."""
    weights = dict(DEFAULT_WEIGHTS)
    try:
        row = db.fetch_one(
            "SELECT weight_adjustments FROM analyst_recalibration_log ORDER BY computed_at DESC LIMIT 1"
        )
        if row and row.get("weight_adjustments"):
            saved = json.loads(str(row["weight_adjustments"]))
            after = saved.get("after")
            if isinstance(after, dict):
                weights.update(after)
    except Exception:
        pass
    return weights


def _check_quality_gate(db: SupplyCoreDb) -> tuple[bool, float]:
    """Check whether the latest data quality gate passed. Returns (passed, score)."""
    try:
        row = db.fetch_one(
            "SELECT quality_score, gate_passed FROM graph_data_quality_metrics ORDER BY computed_at DESC LIMIT 1"
        )
        if row:
            return bool(int(row.get("gate_passed") or 0)), float(row.get("quality_score") or 0)
    except Exception:
        pass
    # No quality check has run yet — allow pipeline to proceed.
    return True, 1.0


# ── Step 1: Pre-run inspection ────────────────────────────────────────────

def _inspect_graph(client: Neo4jClient) -> dict[str, Any]:
    """Query Neo4j to understand what already exists."""
    counts: dict[str, Any] = {}

    for label, key in [("Character", "characters"), ("Battle", "battles"),
                        ("Alliance", "alliances"), ("Killmail", "killmails")]:
        rows = client.query(f"MATCH (n:{label}) RETURN count(n) AS cnt")
        counts[key] = int(rows[0]["cnt"]) if rows else 0

    rel_rows = client.query("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType")
    counts["relationship_types"] = [r["relationshipType"] for r in rel_rows]

    scored = client.query("MATCH (c:Character) WHERE c.suspicion_score IS NOT NULL RETURN count(c) AS cnt")
    counts["already_scored"] = int(scored[0]["cnt"]) if scored else 0

    last_run = client.query("MATCH (c:Character) WHERE c.computed_at IS NOT NULL RETURN max(c.computed_at) AS last_run")
    counts["last_run"] = last_run[0].get("last_run") if last_run else None

    return counts


# ── Step 2: Ensure schema (constraints + indexes) ─────────────────────────

def _ensure_schema(client: Neo4jClient) -> None:
    """Create constraints and indexes if they don't already exist."""
    for stmt in [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (k:Killmail) REQUIRE k.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (sc:ShipClass) REQUIRE sc.id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (cp:ComputeCheckpoint) REQUIRE cp.run_id IS UNIQUE",
        "CREATE INDEX IF NOT EXISTS FOR (k:Killmail) ON (k.battle_id)",
        "CREATE INDEX IF NOT EXISTS FOR (c:Character) ON (c.tracked)",
    ]:
        try:
            client.query(stmt)
        except Neo4jError:
            pass  # Constraint/index may already exist in a different form.


# ── Step 3: Gap-fill from MariaDB into Neo4j ──────────────────────────────

def _gap_fill_killmails(client: Neo4jClient, db: SupplyCoreDb) -> int:
    """Load killmail events and their attacker/victim relationships into Neo4j."""
    # Load killmail nodes from events not yet in graph.
    killmails = db.fetch_all(
        """SELECT ke.killmail_id, ke.battle_id, ke.victim_damage_taken,
                  ke.mail_type, ke.victim_character_id, ke.victim_alliance_id,
                  ke.victim_ship_type_id, ke.solar_system_id
           FROM killmail_events ke
           WHERE ke.killmail_id > 0
           ORDER BY ke.killmail_id ASC
           LIMIT 5000"""
    )
    if not killmails:
        return 0

    written = 0
    for offset in range(0, len(killmails), BATCH_SIZE):
        batch = killmails[offset:offset + BATCH_SIZE]
        params = [{
            "id": int(r["killmail_id"]),
            "battle_id": str(r.get("battle_id") or ""),
            "damage": int(r.get("victim_damage_taken") or 0),
            "mail_type": str(r.get("mail_type") or "loss"),
            "low_damage": int(r.get("victim_damage_taken") or 0) < 1000,
        } for r in batch]

        client.query(
            """UNWIND $batch AS km
               CALL {
                   WITH km
                   MERGE (k:Killmail {id: km.id})
                   SET k.battle_id = km.battle_id,
                       k.damage = km.damage,
                       k.mail_type = km.mail_type,
                       k.low_damage = km.low_damage
               } IN TRANSACTIONS OF 500 ROWS""",
            {"batch": params},
        )
        written += len(batch)

    # Load attacker relationships.
    attackers = db.fetch_all(
        """SELECT ka.sequence_id, ke.killmail_id, ka.character_id,
                  ka.damage_done, ka.final_blow, ka.ship_type_id
           FROM killmail_attackers ka
           INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
           WHERE ka.character_id IS NOT NULL AND ka.character_id > 0
           ORDER BY ke.killmail_id ASC
           LIMIT 10000"""
    )
    for offset in range(0, len(attackers), BATCH_SIZE):
        batch = attackers[offset:offset + BATCH_SIZE]
        params = [{
            "character_id": int(r["character_id"]),
            "killmail_id": int(r["killmail_id"]),
            "damage": int(r.get("damage_done") or 0),
            "final_blow": bool(r.get("final_blow")),
        } for r in batch]

        client.query(
            """UNWIND $batch AS atk
               CALL {
                   WITH atk
                   MATCH (c:Character {character_id: atk.character_id})
                   MATCH (k:Killmail {id: atk.killmail_id})
                   MERGE (c)-[r:ATTACKED_ON]->(k)
                   SET r.damage = atk.damage,
                       r.final_blow = atk.final_blow
               } IN TRANSACTIONS OF 500 ROWS""",
            {"batch": params},
        )

    # Load victim relationships.
    victims = db.fetch_all(
        """SELECT ke.killmail_id, ke.victim_character_id, ke.victim_ship_type_id
           FROM killmail_events ke
           WHERE ke.victim_character_id IS NOT NULL
             AND ke.victim_character_id > 0
           ORDER BY ke.killmail_id ASC
           LIMIT 5000"""
    )
    for offset in range(0, len(victims), BATCH_SIZE):
        batch = victims[offset:offset + BATCH_SIZE]
        params = [{
            "character_id": int(r["victim_character_id"]),
            "killmail_id": int(r["killmail_id"]),
            "ship_type_id": int(r.get("victim_ship_type_id") or 0),
        } for r in batch]

        client.query(
            """UNWIND $batch AS v
               CALL {
                   WITH v
                   MATCH (c:Character {character_id: v.character_id})
                   MATCH (k:Killmail {id: v.killmail_id})
                   MERGE (c)-[r:VICTIM_OF]->(k)
                   SET r.ship_type_id = v.ship_type_id
               } IN TRANSACTIONS OF 500 ROWS""",
            {"batch": params},
        )

    return written


def _gap_fill_alliance_history(client: Neo4jClient, db: SupplyCoreDb) -> int:
    """Load ESI-derived alliance history into Neo4j as WAS_MEMBER_OF relationships."""
    history = db.fetch_all(
        """SELECT character_id, alliance_id, started_at, ended_at
           FROM character_alliance_history
           ORDER BY character_id, started_at"""
    )
    if not history:
        return 0

    written = 0
    for offset in range(0, len(history), BATCH_SIZE):
        batch = history[offset:offset + BATCH_SIZE]
        params = [{
            "character_id": int(r["character_id"]),
            "alliance_id": int(r["alliance_id"]),
            "started_at": str(r["started_at"]),
            "ended_at": str(r["ended_at"]) if r.get("ended_at") else None,
        } for r in batch]

        client.query(
            """UNWIND $batch AS h
               CALL {
                   WITH h
                   MERGE (c:Character {character_id: h.character_id})
                   MERGE (a:Alliance {alliance_id: h.alliance_id})
                   MERGE (c)-[r:WAS_MEMBER_OF]->(a)
                   SET r.started_at = h.started_at,
                       r.ended_at = h.ended_at
               } IN TRANSACTIONS OF 500 ROWS""",
            {"batch": params},
        )
        written += len(batch)

    return written


def _mark_tracked_characters(client: Neo4jClient, db: SupplyCoreDb) -> None:
    """Set tracked=true on characters belonging to tracked alliances/corps."""
    tracked_alliances = db.fetch_all(
        "SELECT alliance_id FROM killmail_tracked_alliances WHERE is_active = 1"
    )
    tracked_corps = db.fetch_all(
        "SELECT corporation_id FROM killmail_tracked_corporations WHERE is_active = 1"
    )
    alliance_ids = [int(r["alliance_id"]) for r in tracked_alliances]
    corp_ids = [int(r["corporation_id"]) for r in tracked_corps]

    if alliance_ids:
        client.query(
            """MATCH (c:Character)-[:MEMBER_OF_ALLIANCE]->(a:Alliance)
               WHERE a.alliance_id IN $alliance_ids
               SET c.tracked = true""",
            {"alliance_ids": alliance_ids},
        )
    if corp_ids:
        client.query(
            """MATCH (c:Character)-[:MEMBER_OF_CORPORATION]->(corp:Corporation)
               WHERE corp.corporation_id IN $corp_ids
               SET c.tracked = true""",
            {"corp_ids": corp_ids},
        )


# ── Step 4: Suspicion signal computation ───────────────────────────────────

def _compute_battle_presence(client: Neo4jClient) -> None:
    """Step 5.1: Battle presence stats per tracked character."""
    client.query(
        """MATCH (c:Character)
           WHERE c.tracked = true
           OPTIONAL MATCH (c)-[:PARTICIPATED_IN]->(b:Battle)
           WITH c, count(DISTINCT b) AS battles_present
           OPTIONAL MATCH (c)-[:ATTACKED_ON]->(k:Killmail)
           WITH c, battles_present, count(k) AS kills_total, sum(COALESCE(k.damage, 0)) AS damage_total
           OPTIONAL MATCH (c)-[:VICTIM_OF]->(lk:Killmail)
           WITH c, battles_present, kills_total, damage_total, count(lk) AS losses_total
           SET c.battles_present = battles_present,
               c.kills_total = kills_total,
               c.damage_total = damage_total,
               c.losses_total = losses_total"""
    )


def _compute_fleet_function(client: Neo4jClient) -> None:
    """Compute primary fleet function and ship size per character from most-used ship type."""
    client.query(
        """MATCH (c:Character)-[:USED_SHIP]->(s:ShipType)
           WHERE c.tracked = true AND s.fleet_function IS NOT NULL
           WITH c, s.fleet_function AS ff, s.ship_size AS sz, count(*) AS usage
           ORDER BY usage DESC
           WITH c, collect(ff)[0] AS primary_ff, collect(sz)[0] AS primary_sz
           SET c.primary_fleet_function = primary_ff,
               c.primary_ship_size = COALESCE(primary_sz, 'medium')"""
    )


def _compute_encounter_vs_engagement(client: Neo4jClient) -> None:
    """Step 5.2: Encounter vs engagement per opposing alliance."""
    client.query(
        """MATCH (c:Character)-[:ON_SIDE]->(my_side:BattleSide)<-[:HAS_SIDE]-(b:Battle)
                  -[:HAS_SIDE]->(opp_side:BattleSide)
           WHERE c.tracked = true AND my_side.side_key <> opp_side.side_key
           MATCH (opp_side)-[:REPRESENTED_BY_ALLIANCE]->(a:Alliance)
           WHERE NOT (c)-[:MEMBER_OF_ALLIANCE]->(a)
           WITH c, a, count(DISTINCT b) AS encountered
           OPTIONAL MATCH (c)-[:ATTACKED_ON]->(k:Killmail)<-[:VICTIM_OF]-(victim:Character)
                          -[:MEMBER_OF_ALLIANCE]->(a)
           WITH c, a, encountered, count(DISTINCT k) AS kills
           WITH c, a, encountered, kills,
                CASE WHEN encountered > 0
                     THEN toFloat(kills) / encountered ELSE 0.0 END AS eng_rate
           MERGE (c)-[rel:ENGAGED_ALLIANCE]->(a)
           SET rel.encounters = encountered,
               rel.kills = kills,
               rel.engagement_rate = eng_rate"""
    )


def _compute_peer_normalisation(client: Neo4jClient) -> None:
    """Step 5.3: Peer normalisation by fleet function and ship size.

    Compares each character against peers who fly the same fleet function
    AND same ship size class (small/medium/large/capital). A battleship
    pilot is only compared to other battleship pilots, not cruiser pilots.
    """
    client.query(
        """MATCH (c:Character)-[:USED_SHIP]->(sc:ShipType)
           WHERE c.tracked = true AND c.battles_present > 0
           WITH c,
                COALESCE(c.primary_fleet_function, sc.fleet_function, 'mainline_dps') AS my_ff,
                COALESCE(c.primary_ship_size, sc.ship_size, 'medium') AS my_sz
           MATCH (peer:Character)-[:USED_SHIP]->(ps:ShipType)
           WHERE peer <> c AND peer.battles_present > 0
             AND COALESCE(peer.primary_fleet_function, ps.fleet_function, 'mainline_dps') = my_ff
             AND COALESCE(peer.primary_ship_size, ps.ship_size, 'medium') = my_sz
           WITH c,
                avg(toFloat(peer.kills_total) / peer.battles_present) AS peer_avg_kpb,
                avg(toFloat(peer.damage_total) / peer.battles_present) AS peer_avg_dpb
           SET c.peer_avg_kills_per_battle = peer_avg_kpb,
               c.peer_avg_damage_per_battle = peer_avg_dpb,
               c.peer_norm_kills_delta =
                   (toFloat(c.kills_total) / c.battles_present) - peer_avg_kpb,
               c.peer_norm_damage_delta =
                   (toFloat(c.damage_total) / c.battles_present) - peer_avg_dpb"""
    )


def _compute_selective_non_engagement(client: Neo4jClient) -> None:
    """Step 5.4: Selective non-engagement score."""
    client.query(
        """MATCH (c:Character)-[e:ENGAGED_ALLIANCE]->(a:Alliance)
           WHERE c.tracked = true
           WITH c, avg(e.engagement_rate) AS mean_rate,
                   stdev(e.engagement_rate) AS sd_rate
           WHERE mean_rate > 0 AND sd_rate > 0
           OPTIONAL MATCH (c)-[e2:ENGAGED_ALLIANCE]->(a2:Alliance)
           WHERE e2.engagement_rate < (mean_rate - 1.5 * sd_rate)
             AND e2.encounters >= 3
           WITH c, count(a2) AS low_eng_count, mean_rate, sd_rate
           SET c.selective_non_engagement_score =
               CASE WHEN low_eng_count > 0
                    THEN toFloat(low_eng_count) * (mean_rate / sd_rate)
                    ELSE 0.0 END"""
    )


def _compute_high_presence_low_output(client: Neo4jClient) -> None:
    """Step 5.5: High presence / low output score."""
    client.query(
        """MATCH (c:Character)
           WHERE c.tracked = true AND c.battles_present > 0
           WITH c,
                toFloat(c.kills_total) / c.battles_present AS kpb,
                toFloat(c.damage_total) / c.battles_present AS dpb
           SET c.high_presence_low_output_score =
               CASE WHEN COALESCE(c.peer_avg_kills_per_battle, 0) > 0
                         AND kpb < c.peer_avg_kills_per_battle * 0.3
                         AND dpb < COALESCE(c.peer_avg_damage_per_battle, 0) * 0.3
                    THEN 1.0
                    ELSE CASE WHEN COALESCE(c.peer_avg_kills_per_battle, 0) > 0
                              THEN kpb / c.peer_avg_kills_per_battle
                              ELSE 0.0 END
               END"""
    )


def _compute_token_participation(client: Neo4jClient) -> None:
    """Step 5.6: Token participation score."""
    client.query(
        """MATCH (c:Character)
           WHERE c.tracked = true
           OPTIONAL MATCH (c)-[:ATTACKED_ON]->(k:Killmail)
           WITH c, count(k) AS total_km,
                count(CASE WHEN k.low_damage = true THEN 1 END) AS low_dm_km
           SET c.token_participation_score =
               CASE WHEN total_km > 0
                    THEN toFloat(low_dm_km) / total_km
                    ELSE 0.0 END"""
    )


def _compute_loss_pattern(client: Neo4jClient) -> None:
    """Step 5.7: Loss pattern signal — loss without attack in same battle."""
    client.query(
        """MATCH (c:Character)-[:VICTIM_OF]->(lk:Killmail)
           WHERE c.tracked = true AND lk.battle_id IS NOT NULL
           WITH c, lk.battle_id AS battle_id, count(lk) AS losses_in_battle
           OPTIONAL MATCH (c)-[:ATTACKED_ON]->(ak:Killmail)
           WHERE ak.battle_id = battle_id
           WITH c, count(DISTINCT battle_id) AS loss_battles,
                sum(CASE WHEN ak IS NOT NULL THEN 1 ELSE 0 END) AS attack_kms_in_loss_battles
           SET c.loss_without_attack_ratio =
               CASE WHEN loss_battles > 0
                    THEN 1.0 - (toFloat(attack_kms_in_loss_battles) / loss_battles)
                    ELSE 0.0 END"""
    )


def _compute_co_presence_clusters(client: Neo4jClient) -> None:
    """Step 5.8: Co-presence cluster detection."""
    client.query(
        """MATCH (c1:Character)-[:PARTICIPATED_IN]->(b:Battle)<-[:PARTICIPATED_IN]-(c2:Character)
           WHERE c1.tracked = true AND c2.tracked = true
             AND c1.character_id < c2.character_id
             AND COALESCE(c1.high_presence_low_output_score, 0) > 0.6
             AND COALESCE(c2.high_presence_low_output_score, 0) > 0.6
           WITH c1, c2, count(DISTINCT b) AS co_battles
           WHERE co_battles >= 5
           MERGE (c1)-[cl:CO_PRESENT_CLUSTER]->(c2)
           SET cl.co_battles = co_battles"""
    )
    # Mark bridge characters.
    client.query(
        """MATCH (c:Character)-[:CO_PRESENT_CLUSTER]-()
           WITH c, count(*) AS cluster_connections
           WHERE cluster_connections >= 2
           SET c.bridge_character = true"""
    )


def _compute_composition_adjusted_performance(client: Neo4jClient, db: SupplyCoreDb) -> None:
    """Compute composition-adjusted performance ratio per character.

    Loads side composition from MariaDB (theater_side_composition +
    theater_participants) and computes the ratio of the character's
    side expected_performance vs the opposing side's.

    comp_ratio > 1 → character's side was stronger (outperformance expected)
    comp_ratio < 1 → character's side was weaker (underperformance expected)
    comp_ratio = 1 → no composition data or balanced sides
    """
    # Load per-character side assignment and expected performance from MariaDB.
    # For characters in multiple theaters, average across all.
    rows = db.fetch_all(
        """SELECT
               tp.character_id,
               tp.side AS my_side,
               my_comp.side_expected_performance_score AS my_expected,
               opp_comp.side_expected_performance_score AS opp_expected
           FROM theater_participants tp
           INNER JOIN theater_side_composition my_comp
               ON my_comp.theater_id = tp.theater_id AND my_comp.side = tp.side
           INNER JOIN theater_side_composition opp_comp
               ON opp_comp.theater_id = tp.theater_id AND opp_comp.side <> tp.side
           WHERE my_comp.side_expected_performance_score > 0
             AND opp_comp.side_expected_performance_score > 0"""
    )
    if not rows:
        return

    # Aggregate: average composition advantage ratio per character
    char_ratios: dict[int, list[float]] = {}
    for r in rows:
        cid = int(r.get("character_id") or 0)
        my_exp = float(r.get("my_expected") or 1.0)
        opp_exp = float(r.get("opp_expected") or 1.0)
        if cid > 0 and opp_exp > 0:
            char_ratios.setdefault(cid, []).append(my_exp / opp_exp)

    # Set on Neo4j Character nodes in batches
    batch_data = [
        {"character_id": cid, "comp_ratio": sum(ratios) / len(ratios)}
        for cid, ratios in char_ratios.items()
        if ratios
    ]

    for offset in range(0, len(batch_data), BATCH_SIZE):
        batch = batch_data[offset:offset + BATCH_SIZE]
        client.query(
            """UNWIND $batch AS row
               CALL {
                   WITH row
                   MATCH (c:Character {character_id: row.character_id})
                   SET c.composition_advantage_ratio = row.comp_ratio
               } IN TRANSACTIONS OF 500 ROWS""",
            {"batch": batch},
        )


def _compute_score_assembly(client: Neo4jClient, weights: dict[str, float] | None = None) -> None:
    """Step 5.9: Assemble final suspicion score and flags.

    Weights are read from analyst recalibration log when available,
    falling back to DEFAULT_WEIGHTS.

    Role-aware adjustments:
    - HIGH_LOSS_ROLES (tackle, bubble, bomber, scout): suppress loss_without_attack
      and token_participation signals since dying is expected.
    - LOW_KILL_ROLES (logi, command, ewar, scout): suppress peer_kill_delta and
      high_presence_low_output since low kill counts are normal.
    """
    w = weights or DEFAULT_WEIGHTS
    client.query(
        """MATCH (c:Character)
           WHERE c.tracked = true
           WITH c,
               COALESCE(c.primary_fleet_function, 'mainline_dps') AS ff,
               COALESCE(c.selective_non_engagement_score, 0) AS sne,
               COALESCE(c.peer_norm_kills_delta, 0) AS raw_pnk,
               COALESCE(c.peer_norm_damage_delta, 0) AS raw_pnd,
               COALESCE(c.high_presence_low_output_score, 0) AS hplo,
               COALESCE(c.token_participation_score, 0) AS tp,
               COALESCE(c.loss_without_attack_ratio, 0) AS lwa,
               COALESCE(c.composition_advantage_ratio, 1.0) AS comp_ratio

           // ── Composition-adjusted peer deltas ──
           // comp_ratio > 1 → stronger side → discount positive outperformance
           // comp_ratio < 1 → weaker side → discount underperformance
           WITH c, ff, sne, hplo, tp, lwa, comp_ratio,
               CASE WHEN comp_ratio < 0.1 THEN 1.0 ELSE comp_ratio END AS safe_ratio,
               raw_pnk, raw_pnd

           WITH c, ff, sne, hplo, tp, lwa, safe_ratio AS comp_ratio,
               CASE WHEN raw_pnk >= 0 THEN raw_pnk / safe_ratio
                    ELSE raw_pnk * safe_ratio END AS pnk,
               CASE WHEN raw_pnd >= 0 THEN raw_pnd / safe_ratio
                    ELSE raw_pnd * safe_ratio END AS pnd,
               // Dampen high_presence_low_output when on weaker side
               CASE WHEN safe_ratio < 0.7 THEN hplo * safe_ratio
                    ELSE hplo END AS adj_hplo

           // ── Role-aware weight adjustments ──
           WITH c, ff, sne, pnk, pnd, adj_hplo, tp, lwa, comp_ratio,
               CASE WHEN ff IN $high_loss_roles THEN 0.0 ELSE $w_lwa END AS eff_w_lwa,
               CASE WHEN ff IN $high_loss_roles THEN $w_tp * 0.3 ELSE $w_tp END AS eff_w_tp,
               CASE WHEN ff IN $low_kill_roles THEN $w_pnk * 0.3 ELSE $w_pnk END AS eff_w_pnk,
               CASE WHEN ff IN $low_kill_roles THEN $w_hplo * 0.3 ELSE $w_hplo END AS eff_w_hplo

           // ── Final score assembly ──
           WITH c, ff, comp_ratio, sne, pnk, pnd, adj_hplo, tp, lwa,
               eff_w_lwa, eff_w_tp, eff_w_pnk, eff_w_hplo,
               (sne * $w_sne) +
               (pnk * eff_w_pnk) +
               (pnd * $w_pnd) +
               (adj_hplo * eff_w_hplo) +
               (tp * eff_w_tp) +
               (lwa * eff_w_lwa)
               AS raw_score,
               [
                   CASE WHEN sne > 0.7
                        THEN 'selective_non_engagement' END,
                   CASE WHEN adj_hplo > 0.6 AND NOT ff IN $low_kill_roles
                        THEN 'high_presence_low_output' END,
                   CASE WHEN tp > 0.5 AND NOT ff IN $high_loss_roles
                        THEN 'token_participation' END,
                   CASE WHEN pnk < -0.4 AND NOT ff IN $low_kill_roles
                        THEN 'peer_kill_delta' END,
                   CASE WHEN pnd < -0.4
                        THEN 'peer_damage_delta' END,
                   CASE WHEN c.bridge_character = true
                        THEN 'bridge_cluster_member' END,
                   CASE WHEN lwa > 0.7 AND NOT ff IN $high_loss_roles
                        THEN 'loss_without_attack' END
               ] AS all_flags
           WITH c, raw_score, comp_ratio,
                [f IN all_flags WHERE f IS NOT NULL] AS flags
           SET c.suspicion_score = raw_score,
               c.suspicion_flags = flags,
               c.composition_advantage_ratio = comp_ratio,
               c.computed_at = toString(datetime())""",
        {
            "w_sne": w.get("selective_non_engagement", 0.35),
            "w_pnk": w.get("peer_norm_kills", -0.20),
            "w_pnd": w.get("peer_norm_damage", -0.20),
            "w_hplo": w.get("high_presence_low_output", 0.15),
            "w_tp": w.get("token_participation", 0.10),
            "w_lwa": w.get("loss_without_attack", 0.10),
            "high_loss_roles": list(HIGH_LOSS_ROLES),
            "low_kill_roles": list(LOW_KILL_ROLES),
        },
    )


# ── Step 5: Historical alliance overlap ────────────────────────────────────

def _compute_shared_alliance_history(client: Neo4jClient) -> None:
    """Step 6.2: Find tracked members and attackers who shared an alliance."""
    client.query(
        """MATCH (tracked:Character)-[h1:WAS_MEMBER_OF]->(a:Alliance)<-[h2:WAS_MEMBER_OF]-(attacker:Character)
           WHERE tracked.tracked = true
             AND attacker <> tracked
             AND date(h1.started_at) <= COALESCE(date(h2.ended_at), date())
             AND date(h2.started_at) <= COALESCE(date(h1.ended_at), date())
             AND COALESCE(date(h1.ended_at), date()) >= date() - duration({days: $lookback_days})
           WITH tracked, attacker, a,
                CASE WHEN date(h1.started_at) > date(h2.started_at)
                     THEN h1.started_at ELSE h2.started_at END AS overlap_start,
                CASE WHEN COALESCE(date(h1.ended_at), date()) < COALESCE(date(h2.ended_at), date())
                     THEN h1.ended_at ELSE h2.ended_at END AS overlap_end
           MERGE (tracked)-[rel:SHARED_ALLIANCE_WITH]->(attacker)
           SET rel.alliance_id = a.alliance_id,
               rel.overlap_start = overlap_start,
               rel.overlap_end = overlap_end,
               rel.overlap_days = duration.between(
                   date(overlap_start),
                   COALESCE(date(overlap_end), date())
               ).days""",
        {"lookback_days": OVERLAP_LOOKBACK_DAYS},
    )


def _compute_former_allies_attacking(client: Neo4jClient) -> None:
    """Step 6.3: Count former allies now attacking tracked members."""
    client.query(
        """MATCH (tracked:Character)-[:VICTIM_OF]->(lk:Killmail)<-[:ATTACKED_ON]-(attacker:Character)
           WHERE tracked.tracked = true
             AND (tracked)-[:SHARED_ALLIANCE_WITH]->(attacker)
           WITH tracked,
                count(DISTINCT attacker) AS former_allies_attacking,
                count(lk) AS losses_to_former_allies
           SET tracked.former_allies_attacking = former_allies_attacking,
               tracked.losses_to_former_allies = losses_to_former_allies"""
    )


def _compute_repeat_targeting(client: Neo4jClient) -> None:
    """Step 6.4: Detect repeated targeting by former allies."""
    client.query(
        """MATCH (tracked:Character)-[:VICTIM_OF]->(lk:Killmail)<-[:ATTACKED_ON]-(attacker:Character)
           WHERE tracked.tracked = true
             AND (tracked)-[:SHARED_ALLIANCE_WITH]->(attacker)
           WITH tracked, attacker, count(lk) AS times_killed_by
           WHERE times_killed_by >= 3
           WITH tracked,
                count(attacker) AS repeat_attackers,
                sum(times_killed_by) AS total_repeat_kills
           SET tracked.repeat_former_ally_attackers = repeat_attackers,
               tracked.total_repeat_kills_by_former = total_repeat_kills"""
    )


def _compute_overlap_score(client: Neo4jClient) -> None:
    """Step 6.5: Historical overlap score."""
    client.query(
        """MATCH (c:Character)
           WHERE c.tracked = true
           WITH c,
                (COALESCE(c.former_allies_attacking, 0) * 1.0) * 0.40 +
                (COALESCE(c.losses_to_former_allies, 0) * 1.0) * 0.25 +
                (COALESCE(c.repeat_former_ally_attackers, 0) * 1.0) * 0.35
               AS raw_overlap_score
           SET c.historical_overlap_score = raw_overlap_score,
               c.overlap_computed_at = toString(datetime())"""
    )


# ── Step 6: Cross-system correlation ──────────────────────────────────────

def _compute_cross_system_correlation(client: Neo4jClient) -> None:
    """Step 7: Flag characters hit by both suspicion signals and overlap."""
    client.query(
        """MATCH (c:Character)
           WHERE c.tracked = true
             AND size(COALESCE(c.suspicion_flags, [])) >= 2
             AND COALESCE(c.historical_overlap_score, 0) > 0
           SET c.correlated_flag = true,
               c.combined_risk_score =
                   (COALESCE(c.suspicion_score, 0) * 0.6) +
                   (c.historical_overlap_score * 0.4)"""
    )


# ── Step 7: Export to MariaDB ──────────────────────────────────────────────

def _export_suspicion_signals(client: Neo4jClient, db: SupplyCoreDb, computed_at: str) -> int:
    """Export characters with >= 2 suspicion flags to MariaDB."""
    rows = client.query(
        """MATCH (c:Character)
           WHERE c.tracked = true
             AND size(COALESCE(c.suspicion_flags, [])) >= 2
           OPTIONAL MATCH (c)-[:MEMBER_OF_ALLIANCE]->(a:Alliance)
           OPTIONAL MATCH (c)-[e:ENGAGED_ALLIANCE]->(ea:Alliance)
           WITH c, COALESCE(a.alliance_id, 0) AS alliance_id,
                collect({
                    alliance_id: ea.alliance_id,
                    encounters: e.encounters,
                    engagement_rate: e.engagement_rate
                }) AS engagement_rates
           RETURN
               toInteger(c.character_id) AS character_id,
               toInteger(alliance_id) AS alliance_id,
               toInteger(COALESCE(c.battles_present, 0)) AS battles_present,
               toInteger(COALESCE(c.kills_total, 0)) AS kills_total,
               toInteger(COALESCE(c.losses_total, 0)) AS losses_total,
               toInteger(COALESCE(c.damage_total, 0)) AS damage_total,
               COALESCE(c.primary_fleet_function, 'mainline_dps') AS primary_fleet_function,
               toFloat(COALESCE(c.selective_non_engagement_score, 0)) AS selective_non_engagement_score,
               toFloat(COALESCE(c.high_presence_low_output_score, 0)) AS high_presence_low_output_score,
               toFloat(COALESCE(c.token_participation_score, 0)) AS token_participation_score,
               toFloat(COALESCE(c.loss_without_attack_ratio, 0)) AS loss_without_attack_ratio,
               toFloat(COALESCE(c.peer_norm_kills_delta, 0)) AS peer_norm_kills_delta,
               toFloat(COALESCE(c.peer_norm_damage_delta, 0)) AS peer_norm_damage_delta,
               toFloat(COALESCE(c.composition_advantage_ratio, 1.0)) AS composition_advantage_ratio,
               toFloat(COALESCE(c.suspicion_score, 0)) AS suspicion_score,
               c.suspicion_flags AS suspicion_flags,
               engagement_rates"""
    )

    if not rows:
        return 0

    with db.transaction() as (_, cursor):
        cursor.execute("DELETE FROM character_suspicion_signals")
        for r in rows:
            cid = int(r.get("character_id") or 0)
            if cid <= 0:
                continue
            cursor.execute(
                """INSERT INTO character_suspicion_signals
                   (character_id, alliance_id, battles_present, kills_total, losses_total,
                    damage_total, primary_fleet_function,
                    selective_non_engagement_score, high_presence_low_output_score,
                    token_participation_score, loss_without_attack_ratio,
                    peer_normalized_kills_delta, peer_normalized_damage_delta,
                    composition_adjusted_delta, side_expected_performance,
                    suspicion_score, suspicion_flags, engagement_rate_by_alliance, computed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    cid,
                    int(r.get("alliance_id") or 0),
                    int(r.get("battles_present") or 0),
                    int(r.get("kills_total") or 0),
                    int(r.get("losses_total") or 0),
                    int(r.get("damage_total") or 0),
                    str(r.get("primary_fleet_function") or "mainline_dps"),
                    float(r.get("selective_non_engagement_score") or 0),
                    float(r.get("high_presence_low_output_score") or 0),
                    float(r.get("token_participation_score") or 0),
                    float(r.get("loss_without_attack_ratio") or 0),
                    max(-9999.999999, min(9999.999999, float(r.get("peer_norm_kills_delta") or 0))),
                    max(-9999.999999, min(9999.999999, float(r.get("peer_norm_damage_delta") or 0))),
                    max(-9999.999999, min(9999.999999, float(r.get("composition_advantage_ratio") or 1.0) - 1.0)),  # delta from neutral
                    max(-9999.999999, min(9999.999999, float(r.get("composition_advantage_ratio") or 1.0))),
                    max(-9999.999999, min(9999.999999, float(r.get("suspicion_score") or 0))),
                    json_dumps_safe(r.get("suspicion_flags") or []),
                    json_dumps_safe(r.get("engagement_rates") or []),
                    computed_at,
                ),
            )

    return len(rows)


def _export_alliance_overlap(client: Neo4jClient, db: SupplyCoreDb, computed_at: str) -> int:
    """Export characters with non-zero overlap score to MariaDB."""
    rows = client.query(
        """MATCH (c:Character)
           WHERE c.tracked = true
             AND COALESCE(c.historical_overlap_score, 0) > 0
           OPTIONAL MATCH (c)-[:MEMBER_OF_ALLIANCE]->(a:Alliance)
           RETURN
               toInteger(c.character_id) AS character_id,
               toInteger(COALESCE(a.alliance_id, 0)) AS alliance_id,
               toInteger(COALESCE(c.former_allies_attacking, 0)) AS former_allies_attacking,
               toInteger(COALESCE(c.losses_to_former_allies, 0)) AS losses_to_former_allies,
               toInteger(COALESCE(c.repeat_former_ally_attackers, 0)) AS repeat_former_ally_attackers,
               toInteger(COALESCE(c.total_repeat_kills_by_former, 0)) AS total_repeat_kills_by_former,
               toFloat(c.historical_overlap_score) AS historical_overlap_score,
               CASE WHEN c.correlated_flag = true THEN 1 ELSE 0 END AS correlated_flag,
               toFloat(COALESCE(c.combined_risk_score, 0)) AS combined_risk_score"""
    )

    if not rows:
        return 0

    with db.transaction() as (_, cursor):
        cursor.execute("DELETE FROM character_alliance_overlap")
        for r in rows:
            cid = int(r.get("character_id") or 0)
            if cid <= 0:
                continue
            cursor.execute(
                """INSERT INTO character_alliance_overlap
                   (character_id, alliance_id, former_allies_attacking, losses_to_former_allies,
                    repeat_former_ally_attackers, total_repeat_kills_by_former,
                    historical_overlap_score, correlated_flag, combined_risk_score, computed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    cid,
                    int(r.get("alliance_id") or 0),
                    int(r.get("former_allies_attacking") or 0),
                    int(r.get("losses_to_former_allies") or 0),
                    int(r.get("repeat_former_ally_attackers") or 0),
                    int(r.get("total_repeat_kills_by_former") or 0),
                    float(r.get("historical_overlap_score") or 0),
                    int(r.get("correlated_flag") or 0),
                    float(r.get("combined_risk_score") or 0),
                    computed_at,
                ),
            )

    return len(rows)


# ── Main entry point ──────────────────────────────────────────────────────

def run_intelligence_pipeline(
    db: SupplyCoreDb,
    neo4j_raw: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run the full intelligence pipeline: graph gap-fill → signals → overlap → export."""
    started = time.perf_counter()
    config = Neo4jConfig.from_runtime(neo4j_raw or {})
    if not config.enabled:
        return JobResult.success(
            job_key="intelligence_pipeline",
            summary="Skipped — Neo4j disabled.",
            rows_processed=0,
            rows_written=0,
            duration_ms=0,
        ).to_dict()

    # Quality gate check — skip compute if data quality is below threshold.
    gate_passed, quality_score = _check_quality_gate(db)
    if not gate_passed:
        return JobResult.failed(
            job_key="intelligence_pipeline",
            error=f"Aborted — data quality gate failed (score {quality_score:.4f}). Run graph_data_quality_check for details.",
            duration_ms=int((time.perf_counter() - started) * 1000),
            meta={"quality_score": quality_score, "gate_passed": False},
        ).to_dict()

    # Load analyst-recalibrated weights (falls back to defaults if none exist).
    recalibrated_weights = _load_recalibrated_weights(db)

    client = Neo4jClient(config)
    computed_at = _now_sql()
    run_id = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    stage_timings: dict[str, int] = {}

    def _stage_completed(name: str) -> bool:
        """Check if a stage was already completed in a previous run (resume logic)."""
        try:
            rows = client.query(
                """MATCH (cp:ComputeCheckpoint)
                   WHERE cp[$stage_key] IS NOT NULL
                   RETURN cp.run_id AS rid, cp[$stage_key] AS completed_at
                   ORDER BY cp[$stage_key] DESC LIMIT 1""",
                {"stage_key": f"stage_{name}"},
            )
            if rows and rows[0].get("completed_at") is not None:
                # Only skip if the last checkpoint was within this run hour (fresh).
                return True
        except Neo4jError:
            pass
        return False

    def _timed(name: str, fn, *args):
        if _stage_completed(name) and name not in ("gap_fill_killmails", "gap_fill_alliance_history", "mark_tracked"):
            stage_timings[name] = 0
            skipped_stages.append(name)
            return
        t = time.perf_counter()
        fn(*args)
        stage_timings[name] = int((time.perf_counter() - t) * 1000)
        # Record checkpoint with inspection summary.
        try:
            client.query(
                """MERGE (cp:ComputeCheckpoint {run_id: $run_id})
                   SET cp[$stage_key] = timestamp(),
                       cp.stage_inspection = $stage_inspection,
                       cp.characters_existing = $characters_existing,
                       cp.battles_existing = $battles_existing""",
                {
                    "run_id": run_id,
                    "stage_key": f"stage_{name}",
                    "stage_inspection": json_dumps_safe(inspection),
                    "characters_existing": inspection.get("characters", 0),
                    "battles_existing": inspection.get("battles", 0),
                },
            )
        except Neo4jError:
            pass

    skipped_stages: list[str] = []

    # 1. Inspection.
    inspection = _inspect_graph(client)

    # 2. Schema.
    _ensure_schema(client)

    # 3. Gap-fill (always runs — incremental).
    _timed("gap_fill_killmails", _gap_fill_killmails, client, db)
    _timed("gap_fill_alliance_history", _gap_fill_alliance_history, client, db)
    _timed("mark_tracked", _mark_tracked_characters, client, db)

    # Re-inspect after gap-fill to decide if compute stages can be skipped.
    post_fill_inspection = _inspect_graph(client)
    new_data = (
        post_fill_inspection.get("characters", 0) != inspection.get("characters", 0)
        or post_fill_inspection.get("killmails", 0) != inspection.get("killmails", 0)
        or inspection.get("already_scored", 0) == 0
    )
    # If no new data was added and scores already exist, skip compute stages.
    if not new_data and inspection.get("already_scored", 0) > 0:
        skipped_stages.extend([
            "battle_presence", "fleet_function", "encounter_vs_engagement", "peer_normalisation",
            "selective_non_engagement", "high_presence_low_output", "token_participation",
            "loss_pattern", "co_presence_clusters",
            "composition_adjusted_performance", "score_assembly",
            "shared_alliance_history", "former_allies_attacking", "repeat_targeting",
            "overlap_score", "cross_system_correlation",
        ])
    else:
        # 4. Suspicion signal computation (ordered).
        _timed("battle_presence", _compute_battle_presence, client)
        _timed("fleet_function", _compute_fleet_function, client)
        _timed("encounter_vs_engagement", _compute_encounter_vs_engagement, client)
        _timed("peer_normalisation", _compute_peer_normalisation, client)
        _timed("selective_non_engagement", _compute_selective_non_engagement, client)
        _timed("high_presence_low_output", _compute_high_presence_low_output, client)
        _timed("token_participation", _compute_token_participation, client)
        _timed("loss_pattern", _compute_loss_pattern, client)
        _timed("co_presence_clusters", _compute_co_presence_clusters, client)

        # 4b. Composition normalization — compute per-character advantage ratios
        # from MariaDB side composition data (theater_side_composition).
        _timed("composition_adjusted_performance", _compute_composition_adjusted_performance, client, db)

        _timed("score_assembly", _compute_score_assembly, client, recalibrated_weights)

        # 5. Historical alliance overlap.
        _timed("shared_alliance_history", _compute_shared_alliance_history, client)
        _timed("former_allies_attacking", _compute_former_allies_attacking, client)
        _timed("repeat_targeting", _compute_repeat_targeting, client)
        _timed("overlap_score", _compute_overlap_score, client)

        # 6. Cross-system correlation.
        _timed("cross_system_correlation", _compute_cross_system_correlation, client)

    # 7. Export to MariaDB.
    t = time.perf_counter()
    suspicion_exported = _export_suspicion_signals(client, db, computed_at)
    overlap_exported = _export_alliance_overlap(client, db, computed_at)
    stage_timings["export"] = int((time.perf_counter() - t) * 1000)

    total_written = suspicion_exported + overlap_exported

    return JobResult.success(
        job_key="intelligence_pipeline",
        summary=f"Intelligence pipeline complete: {suspicion_exported} suspicion signals, {overlap_exported} overlap records exported.",
        rows_processed=inspection.get("characters", 0),
        rows_written=total_written,
        duration_ms=int((time.perf_counter() - started) * 1000),
        meta={
            "run_id": run_id,
            "computed_at": computed_at,
            "inspection": inspection,
            "stage_timings_ms": stage_timings,
            "skipped_stages": skipped_stages,
            "suspicion_exported": suspicion_exported,
            "overlap_exported": overlap_exported,
            "quality_score": quality_score,
            "recalibrated_weights": recalibrated_weights,
        },
    ).to_dict()
