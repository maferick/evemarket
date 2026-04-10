<?php

declare(strict_types=1);

require_once __DIR__ . '/../../../src/bootstrap.php';
require_once __DIR__ . '/../../../src/public_api.php';

// ── Authenticate ──────────────────────────────────────────────────────
$apiKey = public_api_authenticate();

// ── Filters ──────────────────────────────────────────────────────────
$page     = max(1, (int) ($_GET['page'] ?? 1));
$perPage  = min(100, max(1, (int) ($_GET['per_page'] ?? 50)));
$offset   = ($page - 1) * $perPage;

$mode     = strtolower(trim((string) ($_GET['mode'] ?? 'all')));        // all | solo | expensive | capital | awox | npc
$minValue = max(0, (float) ($_GET['min_value'] ?? 0));

$allowedModes = ['all', 'solo', 'expensive', 'capital', 'awox', 'npc'];
if (!in_array($mode, $allowedModes, true)) {
    $mode = 'all';
}

// ── Build query ──────────────────────────────────────────────────────
$where  = [];
$params = [];

if ($mode === 'solo') {
    $where[] = 'ke.zkb_solo = 1';
} elseif ($mode === 'awox') {
    $where[] = 'ke.zkb_awox = 1';
} elseif ($mode === 'npc') {
    $where[] = 'ke.zkb_npc = 1';
} elseif ($mode === 'expensive') {
    $where[] = 'COALESCE(ke.zkb_total_value, 0) >= ?';
    $params[] = 1_000_000_000.0; // 1b+
} elseif ($mode === 'capital') {
    // rough match via group IDs — fallback to points >= 1000
    $where[] = 'COALESCE(ke.zkb_points, 0) >= 1000';
}

if ($minValue > 0) {
    $where[] = 'COALESCE(ke.zkb_total_value, 0) >= ?';
    $params[] = $minValue;
}

$whereSql = $where === [] ? '' : ' WHERE ' . implode(' AND ', $where);

$sql = "SELECT ke.sequence_id,
               ke.killmail_id,
               ke.killmail_time,
               ke.solar_system_id,
               ke.region_id,
               ke.victim_character_id,
               ke.victim_corporation_id,
               ke.victim_alliance_id,
               ke.victim_ship_type_id,
               ke.zkb_total_value,
               ke.zkb_points,
               ke.zkb_solo,
               ke.zkb_awox,
               ke.zkb_npc,
               rs.system_name,
               rs.security AS security_status,
               rr.region_name,
               rt.type_name AS ship_name,
               emc_v.entity_name AS victim_character_name,
               emc_c.entity_name AS victim_corporation_name,
               emc_a.entity_name AS victim_alliance_name
        FROM killmail_events ke
        LEFT JOIN ref_systems rs     ON rs.system_id = ke.solar_system_id
        LEFT JOIN ref_regions rr     ON rr.region_id = ke.region_id
        LEFT JOIN ref_item_types rt  ON rt.type_id   = ke.victim_ship_type_id
        LEFT JOIN entity_metadata_cache emc_v
            ON emc_v.entity_type = 'character' AND emc_v.entity_id = ke.victim_character_id
        LEFT JOIN entity_metadata_cache emc_c
            ON emc_c.entity_type = 'corporation' AND emc_c.entity_id = ke.victim_corporation_id
        LEFT JOIN entity_metadata_cache emc_a
            ON emc_a.entity_type = 'alliance' AND emc_a.entity_id = ke.victim_alliance_id
        {$whereSql}
        ORDER BY ke.effective_killmail_at DESC
        LIMIT " . (int) $perPage . " OFFSET " . (int) $offset;

try {
    $rows = db_select($sql, $params);
} catch (Throwable $e) {
    public_api_respond_error(500, 'Query failed: ' . $e->getMessage());
}

$killmails = [];
foreach ($rows as $r) {
    $killmails[] = [
        'sequence_id'      => (int) ($r['sequence_id'] ?? 0),
        'killmail_id'      => (int) ($r['killmail_id'] ?? 0),
        'killmail_time'    => (string) ($r['killmail_time'] ?? ''),
        'system_id'        => (int) ($r['solar_system_id'] ?? 0),
        'system_name'      => (string) ($r['system_name'] ?? ''),
        'security_status'  => $r['security_status'] !== null ? (float) $r['security_status'] : null,
        'region_id'        => (int) ($r['region_id'] ?? 0),
        'region_name'      => (string) ($r['region_name'] ?? ''),
        'victim' => [
            'character_id'     => (int) ($r['victim_character_id'] ?? 0),
            'character_name'   => (string) ($r['victim_character_name'] ?? ''),
            'corporation_id'   => (int) ($r['victim_corporation_id'] ?? 0),
            'corporation_name' => (string) ($r['victim_corporation_name'] ?? ''),
            'alliance_id'      => (int) ($r['victim_alliance_id'] ?? 0),
            'alliance_name'    => (string) ($r['victim_alliance_name'] ?? ''),
            'ship_type_id'     => (int) ($r['victim_ship_type_id'] ?? 0),
            'ship_name'        => (string) ($r['ship_name'] ?? ''),
        ],
        'value'  => (float) ($r['zkb_total_value'] ?? 0),
        'points' => (int)   ($r['zkb_points'] ?? 0),
        'solo'   => (bool)  ($r['zkb_solo']  ?? false),
        'awox'   => (bool)  ($r['zkb_awox']  ?? false),
        'npc'    => (bool)  ($r['zkb_npc']   ?? false),
    ];
}

public_api_respond([
    'killmails' => $killmails,
    'page'      => $page,
    'per_page'  => $perPage,
    'mode'      => $mode,
], 30);
