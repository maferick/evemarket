// Neo4j Bloom — Entry Point Schema
// ─────────────────────────────────────────────────────────────────────────
// Bloom perspectives work best when analysts start from a small set of
// meaningful anchor nodes rather than scanning the whole graph.  This file
// declares the indexes that back the "smart entry point" labels written
// by the compute_bloom_entry_points Python job:
//
//     :HotBattle          — recent high-intensity engagements
//     :HighRiskPilot      — pilots above the suspicion threshold
//     :StrategicSystem    — systems with sustained recent battle density
//     :HotAlliance        — alliances with recent engagement volume
//
// These are additive labels applied on top of the canonical Character /
// Battle / System / Alliance nodes.  They are maintained incrementally —
// the compute job tags new qualifiers and untags nodes that no longer
// meet the criteria on every run.
//
// Canonical schema (constraints + core indexes) lives in
// setup/neo4j_indexes.cypher.  Run this file *after* the canonical schema
// has been applied.
//
// Idempotent: safe to re-run.

// ── Entry-point indexes ────────────────────────────────────────────────
// Each entry-point label has a cheap lookup index and a score index so
// Bloom search phrases can sort without a full scan.

CREATE INDEX hot_battle_tagged_at IF NOT EXISTS
    FOR (n:HotBattle) ON (n.bloom_tagged_at);
CREATE INDEX hot_battle_score IF NOT EXISTS
    FOR (n:HotBattle) ON (n.bloom_hot_score);

CREATE INDEX high_risk_pilot_tagged_at IF NOT EXISTS
    FOR (n:HighRiskPilot) ON (n.bloom_tagged_at);
CREATE INDEX high_risk_pilot_score IF NOT EXISTS
    FOR (n:HighRiskPilot) ON (n.suspicion_score_recent);

CREATE INDEX strategic_system_tagged_at IF NOT EXISTS
    FOR (n:StrategicSystem) ON (n.bloom_tagged_at);
CREATE INDEX strategic_system_battle_count IF NOT EXISTS
    FOR (n:StrategicSystem) ON (n.bloom_recent_battle_count);

CREATE INDEX hot_alliance_tagged_at IF NOT EXISTS
    FOR (n:HotAlliance) ON (n.bloom_tagged_at);
CREATE INDEX hot_alliance_engagement_count IF NOT EXISTS
    FOR (n:HotAlliance) ON (n.bloom_recent_engagement_count);

// ── Validation queries (for manual debugging) ──────────────────────────
//
// Count current entry points:
//
//     MATCH (n:HotBattle)       RETURN 'HotBattle'       AS tag, count(n) AS c
//     UNION ALL
//     MATCH (n:HighRiskPilot)   RETURN 'HighRiskPilot'   AS tag, count(n) AS c
//     UNION ALL
//     MATCH (n:StrategicSystem) RETURN 'StrategicSystem' AS tag, count(n) AS c
//     UNION ALL
//     MATCH (n:HotAlliance)     RETURN 'HotAlliance'     AS tag, count(n) AS c;
//
// Inspect a tagged pilot:
//
//     MATCH (p:HighRiskPilot)
//     RETURN p.character_id, p.name, p.suspicion_score, p.suspicion_score_recent, p.bloom_tagged_at
//     ORDER BY COALESCE(p.suspicion_score_recent, p.suspicion_score, 0) DESC
//     LIMIT 25;
