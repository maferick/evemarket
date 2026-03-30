'use strict';

const neo4j = require('neo4j-driver');

/**
 * Neo4j Cypher queries for the SupplyCore EveWho Intelligence Graph.
 */

// ── Ingestion ────────────────────────────────────────────────────────────────

/**
 * Upsert a character and their full corp history into Neo4j.
 *
 * @param {object} session - Neo4j session
 * @param {object} info - EveWho character info object
 * @param {Array} history - EveWho corp history array
 */
async function enrichCharacter(session, info, history) {
    // Upsert character node
    await session.run(
        `MERGE (c:Character {character_id: $id})
         SET c.name = $name,
             c.sec_status = $sec,
             c.enriched_at = datetime(),
             c.enriched = true`,
        {
            id: neo4j.int(info.character_id),
            name: info.name || '',
            sec: parseFloat(info.sec_status) || 0.0,
        }
    );

    // Upsert current corporation
    if (info.corporation_id) {
        const corpIsNpc = info.corporation_id < 2000000;
        await session.run(
            `MERGE (corp:Corporation {corporation_id: $corpId})
             ON CREATE SET corp.is_npc = $isNpc
             WITH corp
             MATCH (c:Character {character_id: $charId})
             MERGE (c)-[:CURRENT_CORP]->(corp)`,
            {
                corpId: neo4j.int(info.corporation_id),
                isNpc: corpIsNpc,
                charId: neo4j.int(info.character_id),
            }
        );
    }

    // Upsert current alliance
    if (info.alliance_id && info.alliance_id > 0) {
        await session.run(
            `MERGE (a:Alliance {alliance_id: $allianceId})
             WITH a
             MATCH (corp:Corporation {corporation_id: $corpId})
             MERGE (corp)-[:PART_OF {as_of: datetime()}]->(a)`,
            {
                allianceId: neo4j.int(info.alliance_id),
                corpId: neo4j.int(info.corporation_id),
            }
        );
    }

    // Upsert all corp history entries
    for (const h of history) {
        const isNpc = h.corporation_id < 2000000;
        const startDate = (h.start_date || '').replace(' ', 'T');
        const endDate = h.end_date ? h.end_date.replace(' ', 'T') : null;

        let durationDays = null;
        if (endDate) {
            durationDays = Math.round(
                (new Date(endDate) - new Date(startDate)) / 86400000
            );
        }

        await session.run(
            `MERGE (corp:Corporation {corporation_id: $corpId})
             ON CREATE SET corp.is_npc = $isNpc
             WITH corp
             MATCH (c:Character {character_id: $charId})
             MERGE (c)-[r:MEMBER_OF {corporation_id: $corpId, from: datetime($from)}]->(corp)
             SET r.to = CASE WHEN $to IS NULL THEN null ELSE datetime($to) END,
                 r.duration_days = $duration,
                 r.is_short_stay = ($duration IS NOT NULL AND $duration < 30)`,
            {
                corpId: neo4j.int(h.corporation_id),
                isNpc: isNpc,
                charId: neo4j.int(info.character_id),
                from: startDate,
                to: endDate,
                duration: durationDays !== null ? neo4j.int(durationDays) : null,
            }
        );
    }
}

// ── Query functions ──────────────────────────────────────────────────────────

/**
 * Q1 — Cross-side shared corp history for a battle.
 * Returns hostile pilots who previously served in the same corp as a friendly pilot.
 */
async function crossSideSharedHistory(session, battleId) {
    const result = await session.run(
        `MATCH (hostile:Character)-[:PARTICIPATED_IN {side: 'hostile'}]->(b:Battle {battle_id: $battleId})
         MATCH (friendly:Character)-[:PARTICIPATED_IN {side: 'friendly'}]->(b)
         MATCH (hostile)-[:MEMBER_OF]->(corp:Corporation)<-[:MEMBER_OF]-(friendly)
         WHERE NOT corp.is_npc
         RETURN
           hostile.character_id AS hostile_id,
           hostile.name AS hostile_pilot,
           friendly.character_id AS friendly_id,
           friendly.name AS friendly_pilot,
           corp.corporation_id AS shared_corp_id,
           corp.name AS shared_corp_name
         ORDER BY hostile.name
         LIMIT 200`,
        { battleId }
    );
    return result.records.map((r) => ({
        hostile_id: r.get('hostile_id')?.toNumber?.() ?? r.get('hostile_id'),
        hostile_pilot: r.get('hostile_pilot'),
        friendly_id: r.get('friendly_id')?.toNumber?.() ?? r.get('friendly_id'),
        friendly_pilot: r.get('friendly_pilot'),
        shared_corp_id: r.get('shared_corp_id')?.toNumber?.() ?? r.get('shared_corp_id'),
        shared_corp_name: r.get('shared_corp_name'),
    }));
}

/**
 * Q2 — Recent defectors: hostile pilots who left a friendly-aligned corp within N days.
 */
async function recentDefectors(session, battleId, withinDays = 90) {
    const result = await session.run(
        `MATCH (c:Character)-[:PARTICIPATED_IN {side: 'hostile'}]->(b:Battle {battle_id: $battleId})
         MATCH (c)-[m:MEMBER_OF]->(corp:Corporation)
         WHERE m.to IS NOT NULL
           AND NOT corp.is_npc
           AND duration.inDays(m.to, datetime()).days < $withinDays
         MATCH (friendly:Character)-[:PARTICIPATED_IN {side: 'friendly'}]->(b)
         MATCH (friendly)-[:MEMBER_OF]->(corp)
         RETURN DISTINCT
           c.character_id AS character_id,
           c.name AS pilot_name,
           corp.name AS corp_name,
           corp.corporation_id AS corp_id,
           m.to AS left_on,
           duration.inDays(m.to, datetime()).days AS days_ago
         ORDER BY days_ago ASC
         LIMIT 50`,
        { battleId, withinDays: neo4j.int(withinDays) }
    );
    return result.records.map((r) => ({
        character_id: r.get('character_id')?.toNumber?.() ?? r.get('character_id'),
        pilot_name: r.get('pilot_name'),
        corp_name: r.get('corp_name'),
        corp_id: r.get('corp_id')?.toNumber?.() ?? r.get('corp_id'),
        left_on: r.get('left_on')?.toString() ?? null,
        days_ago: r.get('days_ago')?.toNumber?.() ?? r.get('days_ago'),
    }));
}

/**
 * Q3 — Corp-hopping score for a single pilot (last 180 days, excluding NPC corps).
 */
async function corpHoppingScore(session, characterId) {
    const result = await session.run(
        `MATCH (c:Character {character_id: $charId})-[m:MEMBER_OF]->(corp:Corporation)
         WHERE NOT corp.is_npc
           AND m.from > datetime() - duration('P180D')
         WITH c, count(m) AS hops, collect({corp: corp.name, short: m.is_short_stay}) AS moves
         RETURN c.name AS name, hops, moves`,
        { charId: neo4j.int(characterId) }
    );
    if (result.records.length === 0) {
        return null;
    }
    const r = result.records[0];
    return {
        name: r.get('name'),
        hops: r.get('hops')?.toNumber?.() ?? r.get('hops'),
        moves: r.get('moves'),
    };
}

/**
 * Q4 — Shared battle history between two pilots across all battles.
 */
async function sharedBattleHistory(session, idA, idB) {
    const result = await session.run(
        `MATCH (a:Character {character_id: $idA})-[p1:PARTICIPATED_IN]->(b:Battle)
         MATCH (b)<-[p2:PARTICIPATED_IN]-(bb:Character {character_id: $idB})
         RETURN b.battle_id AS battle_id, b.system AS system, b.start_date AS start_date,
                p1.side AS side_a, p2.side AS side_b,
                p1.side = p2.side AS same_side
         ORDER BY b.start_date DESC
         LIMIT 50`,
        { idA: neo4j.int(idA), idB: neo4j.int(idB) }
    );
    return result.records.map((r) => ({
        battle_id: r.get('battle_id'),
        system: r.get('system'),
        start_date: r.get('start_date')?.toString() ?? null,
        side_a: r.get('side_a'),
        side_b: r.get('side_b'),
        same_side: r.get('same_side'),
    }));
}

/**
 * Q5 — Alliance infiltration risk score for a battle.
 */
async function allianceInfiltrationRisk(session, battleId, friendlyAllianceId) {
    const result = await session.run(
        `MATCH (hostile:Character)-[:PARTICIPATED_IN {side: 'hostile'}]->(b:Battle {battle_id: $battleId})
         MATCH (friendly_corp:Corporation)<-[:PART_OF]-(:Alliance {alliance_id: $friendlyAllianceId})
         MATCH (hostile)-[m:MEMBER_OF]->(friendly_corp)
         WHERE NOT friendly_corp.is_npc
         RETURN
           count(DISTINCT hostile) AS pilots_with_friendly_history,
           count(DISTINCT CASE WHEN duration.inDays(m.to, datetime()).days < 90
             THEN hostile END) AS recent_defectors,
           count(DISTINCT CASE WHEN m.is_short_stay
             THEN hostile END) AS short_stay_visits`,
        {
            battleId,
            friendlyAllianceId: neo4j.int(friendlyAllianceId),
        }
    );
    if (result.records.length === 0) {
        return { pilots_with_friendly_history: 0, recent_defectors: 0, short_stay_visits: 0 };
    }
    const r = result.records[0];
    return {
        pilots_with_friendly_history: r.get('pilots_with_friendly_history')?.toNumber?.() ?? 0,
        recent_defectors: r.get('recent_defectors')?.toNumber?.() ?? 0,
        short_stay_visits: r.get('short_stay_visits')?.toNumber?.() ?? 0,
    };
}

/**
 * Cross-side corp overlap for a specific character.
 * Returns corps where this character was a member alongside current enemies.
 */
async function characterCrossSideOverlap(session, characterId, battleId) {
    const result = await session.run(
        `MATCH (c:Character {character_id: $charId})-[:PARTICIPATED_IN]->(b:Battle {battle_id: $battleId})
         MATCH (c)-[:MEMBER_OF]->(corp:Corporation)<-[:MEMBER_OF]-(other:Character)
         MATCH (other)-[p:PARTICIPATED_IN]->(b)
         WHERE NOT corp.is_npc AND p.side <> 'friendly'
         RETURN
           corp.name AS corp_name,
           corp.corporation_id AS corp_id,
           count(DISTINCT other) AS enemy_count
         ORDER BY enemy_count DESC
         LIMIT 20`,
        { charId: neo4j.int(characterId), battleId }
    );
    return result.records.map((r) => ({
        corp_name: r.get('corp_name'),
        corp_id: r.get('corp_id')?.toNumber?.() ?? r.get('corp_id'),
        enemy_count: r.get('enemy_count')?.toNumber?.() ?? r.get('enemy_count'),
    }));
}

/**
 * Hostile corp adjacency: how many hostile pilots are in the same corp as this character.
 */
async function hostileCorpAdjacency(session, characterId) {
    const result = await session.run(
        `MATCH (c:Character {character_id: $charId})-[:MEMBER_OF]->(corp:Corporation)<-[:MEMBER_OF]-(other:Character)
         WHERE NOT corp.is_npc
         MATCH (other)-[:PARTICIPATED_IN {side: 'hostile'}]->(:Battle)
         RETURN count(DISTINCT other) AS hostile_neighbors`,
        { charId: neo4j.int(characterId) }
    );
    if (result.records.length === 0) {
        return 0;
    }
    return result.records[0].get('hostile_neighbors')?.toNumber?.() ?? 0;
}

module.exports = {
    enrichCharacter,
    crossSideSharedHistory,
    recentDefectors,
    corpHoppingScore,
    sharedBattleHistory,
    allianceInfiltrationRisk,
    characterCrossSideOverlap,
    hostileCorpAdjacency,
};
