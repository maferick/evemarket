'use strict';

/**
 * Queue Manager — helpers for the enrichment_queue MySQL table.
 */

/**
 * Claim a batch of pending characters for processing.
 * Returns an array of { character_id } rows.
 */
async function claimBatch(db, limit = 10) {
    const [rows] = await db.execute(
        `SELECT character_id FROM enrichment_queue
         WHERE status = 'pending' AND attempts < 3
         ORDER BY priority DESC, queued_at ASC
         LIMIT ?`,
        [limit]
    );

    if (rows.length === 0) {
        return [];
    }

    const ids = rows.map((r) => r.character_id);
    const placeholders = ids.map(() => '?').join(',');
    await db.execute(
        `UPDATE enrichment_queue
         SET status = 'processing', attempts = attempts + 1
         WHERE character_id IN (${placeholders})`,
        ids
    );

    return rows;
}

/**
 * Mark a character as successfully enriched.
 */
async function markDone(db, characterId) {
    await db.execute(
        `UPDATE enrichment_queue SET status = 'done', done_at = NOW() WHERE character_id = ?`,
        [characterId]
    );
}

/**
 * Mark a character as failed (resets to pending for retry if attempts < 3).
 */
async function markFailed(db, characterId, errorMessage) {
    await db.execute(
        `UPDATE enrichment_queue
         SET status = IF(attempts >= 3, 'failed', 'pending'),
             last_error = ?
         WHERE character_id = ?`,
        [String(errorMessage).substring(0, 500), characterId]
    );
}

/**
 * Enqueue characters from a battle for enrichment.
 * Hostile characters get priority boost based on their suspicion score.
 *
 * @param {object} db - MySQL connection
 * @param {string} battleId - Battle/theater ID
 * @param {Array<{character_id: number, side: string, suspicion_score: number}>} participants
 */
async function enqueueFromBattle(db, battleId, participants) {
    if (!participants || participants.length === 0) {
        return;
    }

    const values = [];
    const params = [];
    for (const p of participants) {
        const score = parseFloat(p.suspicion_score) || 0;
        const priority = p.side === 'hostile' || p.side === 'opponent'
            ? score * 2
            : score;
        values.push('(?, ?, ?, NOW())');
        params.push(p.character_id, 'pending', priority.toFixed(4));
    }

    await db.execute(
        `INSERT INTO enrichment_queue (character_id, status, priority, queued_at)
         VALUES ${values.join(', ')}
         ON DUPLICATE KEY UPDATE
           status = IF(status = 'done', status, VALUES(status)),
           priority = GREATEST(priority, VALUES(priority))`,
        params
    );
}

/**
 * Get enrichment progress stats for a theater/battle.
 */
async function enrichmentProgress(db, characterIds) {
    if (!characterIds || characterIds.length === 0) {
        return { total: 0, done: 0, pending: 0, failed: 0 };
    }

    const placeholders = characterIds.map(() => '?').join(',');
    const [rows] = await db.execute(
        `SELECT status, COUNT(*) AS cnt
         FROM enrichment_queue
         WHERE character_id IN (${placeholders})
         GROUP BY status`,
        characterIds
    );

    const result = { total: characterIds.length, done: 0, pending: 0, processing: 0, failed: 0 };
    for (const row of rows) {
        result[row.status] = parseInt(row.cnt, 10);
    }
    return result;
}

module.exports = {
    claimBatch,
    markDone,
    markFailed,
    enqueueFromBattle,
    enrichmentProgress,
};
