<?php

declare(strict_types=1);

require_once __DIR__ . '/../../../src/bootstrap.php';
require_once __DIR__ . '/../../../src/public_api.php';

// ── Authenticate ──────────────────────────────────────────────────────
$apiKey = public_api_authenticate();

// ── Input ────────────────────────────────────────────────────────────
$characterId = max(0, (int) ($_GET['character_id'] ?? 0));
if ($characterId <= 0) {
    public_api_respond_error(400, 'Missing or invalid character_id.');
}

$killLimit = min(50, max(1, (int) ($_GET['limit'] ?? 25)));

// ── Character metadata ───────────────────────────────────────────────
$meta = db_select_one(
    "SELECT entity_id, entity_name,
            JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.corporation_id')) AS corporation_id,
            JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.alliance_id'))    AS alliance_id
     FROM entity_metadata_cache
     WHERE entity_type = 'character' AND entity_id = ?
     LIMIT 1",
    [$characterId]
);

$characterName = (string) ($meta['entity_name'] ?? '');
$corporationId = (int)    ($meta['corporation_id'] ?? 0);
$allianceId    = (int)    ($meta['alliance_id'] ?? 0);

// If we have no metadata, try to infer corp/alliance from the latest killmail
if ($characterName === '' || $corporationId === 0) {
    $latest = db_select_one(
        "SELECT victim_character_id, victim_corporation_id, victim_alliance_id
         FROM killmail_events
         WHERE victim_character_id = ?
         ORDER BY effective_killmail_at DESC
         LIMIT 1",
        [$characterId]
    );
    if ($latest) {
        if ($corporationId === 0) $corporationId = (int) ($latest['victim_corporation_id'] ?? 0);
        if ($allianceId === 0)    $allianceId    = (int) ($latest['victim_alliance_id'] ?? 0);
    }
    if ($characterName === '') {
        $latestAtk = db_select_one(
            "SELECT ka.character_id, ka.corporation_id, ka.alliance_id
             FROM killmail_attackers ka
             WHERE ka.character_id = ?
             ORDER BY ka.id DESC
             LIMIT 1",
            [$characterId]
        );
        if ($latestAtk) {
            if ($corporationId === 0) $corporationId = (int) ($latestAtk['corporation_id'] ?? 0);
            if ($allianceId === 0)    $allianceId    = (int) ($latestAtk['alliance_id'] ?? 0);
        }
    }
}

// Resolve corp/alliance names
$corpName = '';
$allianceName = '';
if ($corporationId > 0) {
    $row = db_select_one(
        "SELECT entity_name FROM entity_metadata_cache WHERE entity_type = 'corporation' AND entity_id = ? LIMIT 1",
        [$corporationId]
    );
    $corpName = (string) ($row['entity_name'] ?? '');
}
if ($allianceId > 0) {
    $row = db_select_one(
        "SELECT entity_name FROM entity_metadata_cache WHERE entity_type = 'alliance' AND entity_id = ? LIMIT 1",
        [$allianceId]
    );
    $allianceName = (string) ($row['entity_name'] ?? '');
}

// ── Stats: losses ────────────────────────────────────────────────────
$lossStats = db_select_one(
    "SELECT COUNT(*) AS loss_count,
            COALESCE(SUM(zkb_total_value), 0) AS isk_lost,
            MIN(effective_killmail_at) AS first_loss_at,
            MAX(effective_killmail_at) AS last_loss_at
     FROM killmail_events
     WHERE victim_character_id = ?",
    [$characterId]
) ?? [];

// ── Stats: kills (via attackers table) ───────────────────────────────
$killStats = db_select_one(
    "SELECT COUNT(DISTINCT ka.sequence_id) AS kill_count,
            COALESCE(SUM(ke.zkb_total_value), 0) AS isk_destroyed,
            SUM(CASE WHEN ka.final_blow = 1 THEN 1 ELSE 0 END) AS final_blows,
            SUM(CASE WHEN ke.zkb_solo = 1 THEN 1 ELSE 0 END) AS solo_kills
     FROM killmail_attackers ka
     INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
     WHERE ka.character_id = ?",
    [$characterId]
) ?? [];

// ── Recent losses ────────────────────────────────────────────────────
$recentLosses = db_select(
    "SELECT ke.sequence_id, ke.killmail_id, ke.killmail_time,
            ke.solar_system_id, rs.system_name, rs.security,
            ke.victim_ship_type_id, rt.type_name AS ship_name,
            ke.zkb_total_value, ke.zkb_points, ke.zkb_solo
     FROM killmail_events ke
     LEFT JOIN ref_systems rs    ON rs.system_id = ke.solar_system_id
     LEFT JOIN ref_item_types rt ON rt.type_id   = ke.victim_ship_type_id
     WHERE ke.victim_character_id = ?
     ORDER BY ke.effective_killmail_at DESC
     LIMIT " . (int) $killLimit,
    [$characterId]
);

// ── Recent kills ─────────────────────────────────────────────────────
$recentKills = db_select(
    "SELECT ke.sequence_id, ke.killmail_id, ke.killmail_time,
            ke.solar_system_id, rs.system_name, rs.security,
            ke.victim_character_id, emc_v.entity_name AS victim_name,
            ke.victim_ship_type_id, rt.type_name AS ship_name,
            ke.zkb_total_value, ke.zkb_points, ke.zkb_solo,
            ka.final_blow, ka.ship_type_id AS attacker_ship_type_id,
            rt2.type_name AS attacker_ship_name
     FROM killmail_attackers ka
     INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
     LEFT JOIN ref_systems rs      ON rs.system_id = ke.solar_system_id
     LEFT JOIN ref_item_types rt   ON rt.type_id   = ke.victim_ship_type_id
     LEFT JOIN ref_item_types rt2  ON rt2.type_id  = ka.ship_type_id
     LEFT JOIN entity_metadata_cache emc_v
         ON emc_v.entity_type = 'character' AND emc_v.entity_id = ke.victim_character_id
     WHERE ka.character_id = ?
     ORDER BY ke.effective_killmail_at DESC
     LIMIT " . (int) $killLimit,
    [$characterId]
);

// ── Top ships flown (as attacker) ────────────────────────────────────
$topShipsFlown = db_select(
    "SELECT ka.ship_type_id, rt.type_name, COUNT(*) AS uses
     FROM killmail_attackers ka
     LEFT JOIN ref_item_types rt ON rt.type_id = ka.ship_type_id
     WHERE ka.character_id = ? AND ka.ship_type_id IS NOT NULL
     GROUP BY ka.ship_type_id, rt.type_name
     ORDER BY uses DESC
     LIMIT 10",
    [$characterId]
);

// ── Top ships killed (victim ship type breakdown) ────────────────────
$topShipsKilled = db_select(
    "SELECT ke.victim_ship_type_id AS type_id, rt.type_name, COUNT(*) AS kills
     FROM killmail_attackers ka
     INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
     LEFT JOIN ref_item_types rt   ON rt.type_id = ke.victim_ship_type_id
     WHERE ka.character_id = ?
     GROUP BY ke.victim_ship_type_id, rt.type_name
     ORDER BY kills DESC
     LIMIT 10",
    [$characterId]
);

// ── Top systems active in (attacker + victim union) ──────────────────
$topSystems = db_select(
    "SELECT system_id, system_name, SUM(activity) AS activity
     FROM (
         SELECT ke.solar_system_id AS system_id, rs.system_name, COUNT(*) AS activity
         FROM killmail_attackers ka
         INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
         LEFT JOIN ref_systems rs ON rs.system_id = ke.solar_system_id
         WHERE ka.character_id = ?
         GROUP BY ke.solar_system_id, rs.system_name
         UNION ALL
         SELECT ke.solar_system_id AS system_id, rs.system_name, COUNT(*) AS activity
         FROM killmail_events ke
         LEFT JOIN ref_systems rs ON rs.system_id = ke.solar_system_id
         WHERE ke.victim_character_id = ?
         GROUP BY ke.solar_system_id, rs.system_name
     ) t
     GROUP BY system_id, system_name
     ORDER BY activity DESC
     LIMIT 10",
    [$characterId, $characterId]
);

// ── Shape output ─────────────────────────────────────────────────────
$shapeKillmail = static function (array $r): array {
    return [
        'sequence_id'   => (int)    ($r['sequence_id'] ?? 0),
        'killmail_id'   => (int)    ($r['killmail_id'] ?? 0),
        'killmail_time' => (string) ($r['killmail_time'] ?? ''),
        'system_id'     => (int)    ($r['solar_system_id'] ?? 0),
        'system_name'   => (string) ($r['system_name'] ?? ''),
        'security'      => $r['security'] !== null ? (float) $r['security'] : null,
        'ship_type_id'  => (int)    ($r['victim_ship_type_id'] ?? 0),
        'ship_name'     => (string) ($r['ship_name'] ?? ''),
        'value'         => (float)  ($r['zkb_total_value'] ?? 0),
        'points'        => (int)    ($r['zkb_points'] ?? 0),
        'solo'          => (bool)   ($r['zkb_solo'] ?? false),
    ];
};

$shapeKillRow = static function (array $r): array {
    return [
        'sequence_id'        => (int)    ($r['sequence_id'] ?? 0),
        'killmail_id'        => (int)    ($r['killmail_id'] ?? 0),
        'killmail_time'      => (string) ($r['killmail_time'] ?? ''),
        'system_id'          => (int)    ($r['solar_system_id'] ?? 0),
        'system_name'        => (string) ($r['system_name'] ?? ''),
        'security'           => $r['security'] !== null ? (float) $r['security'] : null,
        'victim_character_id'=> (int)    ($r['victim_character_id'] ?? 0),
        'victim_name'        => (string) ($r['victim_name'] ?? ''),
        'victim_ship_type_id'=> (int)    ($r['victim_ship_type_id'] ?? 0),
        'victim_ship_name'   => (string) ($r['ship_name'] ?? ''),
        'value'              => (float)  ($r['zkb_total_value'] ?? 0),
        'points'             => (int)    ($r['zkb_points'] ?? 0),
        'solo'               => (bool)   ($r['zkb_solo'] ?? false),
        'final_blow'         => (bool)   ($r['final_blow'] ?? false),
        'attacker_ship_type_id' => (int) ($r['attacker_ship_type_id'] ?? 0),
        'attacker_ship_name' => (string) ($r['attacker_ship_name'] ?? ''),
    ];
};

$killCount     = (int)   ($killStats['kill_count']    ?? 0);
$lossCount     = (int)   ($lossStats['loss_count']    ?? 0);
$iskDestroyed  = (float) ($killStats['isk_destroyed'] ?? 0);
$iskLost       = (float) ($lossStats['isk_lost']      ?? 0);
$finalBlows    = (int)   ($killStats['final_blows']   ?? 0);
$soloKills     = (int)   ($killStats['solo_kills']    ?? 0);

$efficiency = null;
$total = $iskDestroyed + $iskLost;
if ($total > 0) {
    $efficiency = round(($iskDestroyed / $total) * 100, 2);
}

$dangerRatio = null;
if ($killCount + $lossCount > 0) {
    $dangerRatio = round(($killCount / max(1, $killCount + $lossCount)) * 100, 1);
}

public_api_respond([
    'character' => [
        'character_id'     => $characterId,
        'character_name'   => $characterName,
        'corporation_id'   => $corporationId,
        'corporation_name' => $corpName,
        'alliance_id'      => $allianceId,
        'alliance_name'    => $allianceName,
        'portrait_url'     => "https://images.evetech.net/characters/{$characterId}/portrait?size=256",
    ],
    'stats' => [
        'kill_count'    => $killCount,
        'loss_count'    => $lossCount,
        'isk_destroyed' => $iskDestroyed,
        'isk_lost'      => $iskLost,
        'final_blows'   => $finalBlows,
        'solo_kills'    => $soloKills,
        'efficiency'    => $efficiency,
        'danger_ratio'  => $dangerRatio,
        'first_loss_at' => (string) ($lossStats['first_loss_at'] ?? ''),
        'last_loss_at'  => (string) ($lossStats['last_loss_at']  ?? ''),
    ],
    'recent_kills'    => array_map($shapeKillRow, $recentKills),
    'recent_losses'   => array_map($shapeKillmail, $recentLosses),
    'top_ships_flown' => array_map(static fn ($r) => [
        'type_id'   => (int)    ($r['ship_type_id'] ?? 0),
        'type_name' => (string) ($r['type_name'] ?? ''),
        'count'     => (int)    ($r['uses'] ?? 0),
    ], $topShipsFlown),
    'top_ships_killed' => array_map(static fn ($r) => [
        'type_id'   => (int)    ($r['type_id'] ?? 0),
        'type_name' => (string) ($r['type_name'] ?? ''),
        'count'     => (int)    ($r['kills'] ?? 0),
    ], $topShipsKilled),
    'top_systems' => array_map(static fn ($r) => [
        'system_id'   => (int)    ($r['system_id'] ?? 0),
        'system_name' => (string) ($r['system_name'] ?? ''),
        'activity'    => (int)    ($r['activity'] ?? 0),
    ], $topSystems),
], 60);
