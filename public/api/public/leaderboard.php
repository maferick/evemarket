<?php

declare(strict_types=1);

require_once __DIR__ . '/../../../src/bootstrap.php';
require_once __DIR__ . '/../../../src/public_api.php';

// ── Authenticate ──────────────────────────────────────────────────────
$apiKey = public_api_authenticate();

// ── Input ────────────────────────────────────────────────────────────
$period = strtolower(trim((string) ($_GET['period'] ?? '7d')));      // 24h | 7d | 30d | 90d
$metric = strtolower(trim((string) ($_GET['metric'] ?? 'kills')));    // kills | isk_destroyed | losses | isk_lost
$limit  = min(50, max(5, (int) ($_GET['limit'] ?? 25)));

$periodMap = [
    '24h' => 1,
    '7d'  => 7,
    '30d' => 30,
    '90d' => 90,
];
if (!isset($periodMap[$period])) {
    $period = '7d';
}
$days = $periodMap[$period];
$since = (new DateTimeImmutable('now', new DateTimeZone('UTC')))
    ->modify("-{$days} days")
    ->format('Y-m-d H:i:s');

$allowedMetrics = ['kills', 'isk_destroyed', 'losses', 'isk_lost'];
if (!in_array($metric, $allowedMetrics, true)) {
    $metric = 'kills';
}

// ── Query ────────────────────────────────────────────────────────────
if ($metric === 'kills' || $metric === 'isk_destroyed') {
    $orderBy = $metric === 'kills'
        ? 'kill_count DESC, isk_destroyed DESC'
        : 'isk_destroyed DESC, kill_count DESC';

    // Group by character only so corp-switchers show as a single row.
    // MAX() on the descriptive columns is safe because they're used for display only.
    $rows = db_select(
        "SELECT ka.character_id,
                MAX(emc.entity_name)  AS character_name,
                MAX(ka.corporation_id) AS corporation_id,
                MAX(emc_c.entity_name) AS corporation_name,
                MAX(ka.alliance_id)    AS alliance_id,
                MAX(emc_a.entity_name) AS alliance_name,
                COUNT(DISTINCT ka.sequence_id) AS kill_count,
                COALESCE(SUM(ke.zkb_total_value), 0) AS isk_destroyed
         FROM killmail_attackers ka
         INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
         LEFT JOIN entity_metadata_cache emc
             ON emc.entity_type = 'character' AND emc.entity_id = ka.character_id
         LEFT JOIN entity_metadata_cache emc_c
             ON emc_c.entity_type = 'corporation' AND emc_c.entity_id = ka.corporation_id
         LEFT JOIN entity_metadata_cache emc_a
             ON emc_a.entity_type = 'alliance' AND emc_a.entity_id = ka.alliance_id
         WHERE ka.character_id IS NOT NULL
           AND ka.character_id > 0
           AND ke.effective_killmail_at >= ?
         GROUP BY ka.character_id
         ORDER BY {$orderBy}
         LIMIT " . (int) $limit,
        [$since]
    );
} else {
    $orderBy = $metric === 'losses'
        ? 'loss_count DESC, isk_lost DESC'
        : 'isk_lost DESC, loss_count DESC';

    $rows = db_select(
        "SELECT ke.victim_character_id AS character_id,
                MAX(emc.entity_name)   AS character_name,
                MAX(ke.victim_corporation_id) AS corporation_id,
                MAX(emc_c.entity_name) AS corporation_name,
                MAX(ke.victim_alliance_id)    AS alliance_id,
                MAX(emc_a.entity_name) AS alliance_name,
                COUNT(*) AS loss_count,
                COALESCE(SUM(ke.zkb_total_value), 0) AS isk_lost
         FROM killmail_events ke
         LEFT JOIN entity_metadata_cache emc
             ON emc.entity_type = 'character' AND emc.entity_id = ke.victim_character_id
         LEFT JOIN entity_metadata_cache emc_c
             ON emc_c.entity_type = 'corporation' AND emc_c.entity_id = ke.victim_corporation_id
         LEFT JOIN entity_metadata_cache emc_a
             ON emc_a.entity_type = 'alliance' AND emc_a.entity_id = ke.victim_alliance_id
         WHERE ke.victim_character_id IS NOT NULL
           AND ke.victim_character_id > 0
           AND ke.effective_killmail_at >= ?
         GROUP BY ke.victim_character_id
         ORDER BY {$orderBy}
         LIMIT " . (int) $limit,
        [$since]
    );
}

$ranked = [];
$rank = 0;
foreach ($rows as $r) {
    $rank++;
    $ranked[] = [
        'rank'             => $rank,
        'character_id'     => (int)    ($r['character_id'] ?? 0),
        'character_name'   => (string) ($r['character_name'] ?? ''),
        'corporation_id'   => (int)    ($r['corporation_id'] ?? 0),
        'corporation_name' => (string) ($r['corporation_name'] ?? ''),
        'alliance_id'      => (int)    ($r['alliance_id'] ?? 0),
        'alliance_name'    => (string) ($r['alliance_name'] ?? ''),
        'kill_count'       => (int)    ($r['kill_count'] ?? 0),
        'loss_count'       => (int)    ($r['loss_count'] ?? 0),
        'isk_destroyed'    => (float)  ($r['isk_destroyed'] ?? 0),
        'isk_lost'         => (float)  ($r['isk_lost'] ?? 0),
    ];
}

public_api_respond([
    'period'     => $period,
    'metric'     => $metric,
    'since'      => $since,
    'entries'    => $ranked,
], 120);
