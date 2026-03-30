'use strict';

/**
 * EveWho Enrichment Worker
 *
 * Polls the enrichment_queue table in MySQL, fetches character corp history
 * from EveWho, and writes the graph data into Neo4j.
 *
 * Rate limit: 10 requests per 30 seconds (EveWho API).
 * Run as: node enrichment/evewho_enricher.js
 * Or via PM2: pm2 start enrichment/evewho_enricher.js --name evewho-enricher
 */

const neo4j = require('neo4j-driver');
const mysql = require('mysql2/promise');
const { enrichCharacter } = require('./neo4j_queries');
const { claimBatch, markDone, markFailed } = require('./queue_manager');

const RATE_LIMIT = 10;
const WINDOW_MS = 30_000;
const ENRICHMENT_TTL_MS = 24 * 60 * 60 * 1000; // 24 hours
const USER_AGENT = 'SupplyCore Intelligence Platform / contact@supplycore.app';

async function fetchEveWhoCharacter(characterId) {
    const url = `https://evewho.com/api/character/${characterId}`;
    const res = await fetch(url, {
        headers: { 'User-Agent': USER_AGENT },
    });
    if (!res.ok) {
        throw new Error(`EveWho returned ${res.status} for character ${characterId}`);
    }
    return res.json();
}

async function processCharacter(characterId, driver) {
    // Check if already enriched recently
    const session = driver.session();
    try {
        const existing = await session.run(
            'MATCH (c:Character {character_id: $id}) RETURN c.enriched_at AS enriched_at',
            { id: neo4j.int(characterId) }
        );
        if (existing.records.length > 0) {
            const enrichedAt = existing.records[0].get('enriched_at');
            if (enrichedAt && (Date.now() - new Date(enrichedAt.toString()).getTime()) < ENRICHMENT_TTL_MS) {
                return; // already fresh
            }
        }

        const data = await fetchEveWhoCharacter(characterId);
        if (!data.info?.[0]) {
            return;
        }

        await enrichCharacter(session, data.info[0], data.history || []);
    } finally {
        await session.close();
    }
}

async function processBatch(db, driver) {
    const rows = await claimBatch(db, RATE_LIMIT);
    if (rows.length === 0) {
        return 0;
    }

    let processed = 0;
    await Promise.all(rows.map(async (row) => {
        const charId = row.character_id;
        try {
            await processCharacter(charId, driver);
            await markDone(db, charId);
            processed++;
        } catch (err) {
            console.error(`[enricher] Failed character ${charId}: ${err.message}`);
            await markFailed(db, charId, err.message);
        }
    }));

    return processed;
}

async function run() {
    const dbConfig = {
        host: process.env.DB_HOST || '127.0.0.1',
        port: parseInt(process.env.DB_PORT || '3306', 10),
        user: process.env.DB_USERNAME || 'supplycore',
        password: process.env.DB_PASSWORD || '',
        database: process.env.DB_DATABASE || 'supplycore',
    };

    const neo4jUrl = process.env.NEO4J_URL || 'bolt://localhost:7687';
    const neo4jUser = process.env.NEO4J_USERNAME || 'neo4j';
    const neo4jPass = process.env.NEO4J_PASSWORD || '';

    const db = await mysql.createConnection(dbConfig);
    const driver = neo4j.driver(neo4jUrl, neo4j.auth.basic(neo4jUser, neo4jPass));

    // Verify connectivity
    await driver.verifyConnectivity();
    console.log('[enricher] Connected to Neo4j and MySQL. Starting enrichment loop.');

    process.on('SIGINT', async () => {
        console.log('[enricher] Shutting down...');
        await db.end();
        await driver.close();
        process.exit(0);
    });

    process.on('SIGTERM', async () => {
        console.log('[enricher] Shutting down...');
        await db.end();
        await driver.close();
        process.exit(0);
    });

    while (true) {
        const start = Date.now();
        try {
            const count = await processBatch(db, driver);
            if (count > 0) {
                console.log(`[enricher] Enriched ${count} characters`);
            }
        } catch (err) {
            console.error(`[enricher] Batch error: ${err.message}`);
        }
        const elapsed = Date.now() - start;
        const sleepMs = Math.max(0, WINDOW_MS - elapsed);
        if (sleepMs > 0) {
            await new Promise((r) => setTimeout(r, sleepMs));
        }
    }
}

run().catch((err) => {
    console.error('[enricher] Fatal:', err);
    process.exit(1);
});
