<?php

declare(strict_types=1);

require_once __DIR__ . '/../../../src/bootstrap.php';
require_once __DIR__ . '/../../../src/public_api.php';

// ── Authenticate ──
$apiKey = public_api_authenticate();

// ── Validate input ──
$sequenceId = max(0, (int) ($_GET['sequence_id'] ?? 0));
if ($sequenceId <= 0) {
    public_api_respond_error(400, 'Missing or invalid sequence_id.');
}

// ── Reuse the existing killmail summary logic via cache-aside ──
$data = supplycore_cache_aside('killmail_summary', [$sequenceId], supplycore_cache_ttl('killmail_detail'), static function () use ($sequenceId): array {
    $event = db_killmail_detail($sequenceId);
    if ($event === null) {
        return ['error' => 'Killmail not found.'];
    }

    $killmail = killmail_decode_json_array(isset($event['raw_killmail_json']) ? (string) $event['raw_killmail_json'] : null);
    $victim = is_array($killmail['victim'] ?? null) ? $killmail['victim'] : [];
    $zkb = killmail_decode_json_array(isset($event['zkb_json']) ? (string) $event['zkb_json'] : null);

    $attackers = db_killmail_attackers_by_sequence($sequenceId);
    $items = db_killmail_items_by_sequence($sequenceId);

    $resolutionRequests = killmail_entity_resolution_requests($event, $attackers, $items);
    $resolvedEntities = killmail_entity_resolve_batch($resolutionRequests, false);

    $victimCharacter = killmail_resolved_entity($resolvedEntities, 'character', isset($event['victim_character_id']) ? (int) $event['victim_character_id'] : null);
    $victimCorporation = killmail_resolved_entity(
        $resolvedEntities, 'corporation',
        isset($event['victim_corporation_id']) ? (int) $event['victim_corporation_id'] : null,
        isset($event['victim_corporation_label']) ? (string) $event['victim_corporation_label'] : null
    );
    $victimAlliance = killmail_resolved_entity(
        $resolvedEntities, 'alliance',
        isset($event['victim_alliance_id']) ? (int) $event['victim_alliance_id'] : null,
        isset($event['victim_alliance_label']) ? (string) $event['victim_alliance_label'] : null
    );
    $victimShip = killmail_resolved_entity($resolvedEntities, 'type', isset($event['victim_ship_type_id']) ? (int) $event['victim_ship_type_id'] : null, isset($event['ship_type_name']) ? (string) $event['ship_type_name'] : null);
    $system = killmail_resolved_entity($resolvedEntities, 'system', isset($event['solar_system_id']) ? (int) $event['solar_system_id'] : null, isset($event['system_name']) ? (string) $event['system_name'] : null);
    $region = killmail_resolved_entity($resolvedEntities, 'region', isset($event['region_id']) ? (int) $event['region_id'] : null, isset($event['region_name']) ? (string) $event['region_name'] : null);

    $estimatedValue = killmail_value_amount($zkb);

    $groupedItems = killmail_loss_item_groups($items, $resolvedEntities);

    // Batch-fetch market prices
    $allTypeIds = [];
    $shipTypeId = $victimShip['id'] !== null ? (int) $victimShip['id'] : 0;
    if ($shipTypeId > 0) $allTypeIds[$shipTypeId] = true;
    foreach ($groupedItems as $group) {
        foreach ((array) ($group['rows'] ?? []) as $itemRow) {
            $tid = (int) ($itemRow['item_type_id'] ?? 0);
            if ($tid > 0) $allTypeIds[$tid] = true;
        }
    }
    $priceMap = [];
    if ($allTypeIds !== []) {
        $typeIdList = array_keys($allTypeIds);
        $placeholders = implode(',', array_fill(0, count($typeIdList), '?'));
        $priceRows = db_select(
            "SELECT p.type_id, p.avg_price
             FROM market_item_price_1d p
             INNER JOIN (
                 SELECT type_id, MAX(bucket_start) AS max_bucket
                 FROM market_item_price_1d
                 WHERE type_id IN ({$placeholders})
                   AND avg_price IS NOT NULL
                   AND avg_price > 0
                 GROUP BY type_id
             ) latest ON latest.type_id = p.type_id AND latest.max_bucket = p.bucket_start",
            $typeIdList
        );
        foreach ($priceRows as $pr) {
            $tid = (int) ($pr['type_id'] ?? 0);
            $price = (float) ($pr['avg_price'] ?? 0);
            if ($tid > 0 && $price > 0) {
                $priceMap[$tid] = $price;
            }
        }
    }

    $itemSummary = [];
    foreach ($groupedItems as $role => $group) {
        $rows = [];
        $groupValue = 0.0;
        foreach ((array) ($group['rows'] ?? []) as $itemRow) {
            $tid = (int) ($itemRow['item_type_id'] ?? 0);
            $qty = (int) ($itemRow['quantity'] ?? 1);
            $unitPrice = $priceMap[$tid] ?? null;
            $totalPrice = $unitPrice !== null ? $unitPrice * $qty : null;
            if ($totalPrice !== null) $groupValue += $totalPrice;
            $rows[] = [
                'name'        => (string) ($itemRow['item_name'] ?? 'Unknown'),
                'type_id'     => $tid,
                'quantity'    => $qty,
                'state'       => (string) ($itemRow['state_label'] ?? ''),
                'flag'        => $itemRow['item_flag'] ?? null,
                'unit_price'  => $unitPrice,
                'total_price' => $totalPrice,
            ];
        }
        $itemSummary[$role] = [
            'label'       => (string) ($group['label'] ?? $role),
            'total'       => (int) ($group['total_quantity'] ?? 0),
            'total_value' => $groupValue > 0 ? $groupValue : null,
            'rows'        => $rows,
        ];
    }

    $formattedAttackers = [];
    $finalBlow = null;
    foreach (array_slice($attackers, 0, 10) as $attacker) {
        if (!is_array($attacker)) continue;
        $atkChar = killmail_resolved_entity($resolvedEntities, 'character', isset($attacker['character_id']) ? (int) $attacker['character_id'] : null);
        $atkShip = killmail_resolved_entity($resolvedEntities, 'type', isset($attacker['ship_type_id']) ? (int) $attacker['ship_type_id'] : null, isset($attacker['ship_type_name']) ? (string) $attacker['ship_type_name'] : null);
        $atkCorp = killmail_resolved_entity($resolvedEntities, 'corporation', isset($attacker['corporation_id']) ? (int) $attacker['corporation_id'] : null);
        $entry = [
            'character_name'     => $atkChar['name'],
            'character_id'       => $atkChar['id'],
            'ship_display'       => $atkShip['name'],
            'ship_type_id'       => $atkShip['id'],
            'corporation_display' => $atkCorp['name'],
            'damage_done'        => (int) ($attacker['damage_done'] ?? 0),
            'final_blow'         => (int) ($attacker['final_blow'] ?? 0) === 1,
        ];
        $formattedAttackers[] = $entry;
        if ($entry['final_blow'] && $finalBlow === null) {
            $finalBlow = $entry;
        }
    }

    return [
        'error'          => null,
        'killmail_id'    => (int) ($event['killmail_id'] ?? 0),
        'sequence_id'    => $sequenceId,
        'killmail_time'  => killmail_format_datetime(isset($event['killmail_time']) ? (string) $event['killmail_time'] : null),
        'victim'         => [
            'character_name'     => $victimCharacter['name'],
            'character_id'       => $victimCharacter['id'],
            'corporation_display' => $victimCorporation['name'],
            'alliance_display'   => $victimAlliance['name'],
            'damage_taken'       => number_format((int) ($victim['damage_taken'] ?? 0)),
        ],
        'ship'           => [
            'name'       => $victimShip['name'],
            'type_id'    => $victimShip['id'],
            'render_url' => $victimShip['id'] !== null ? killmail_entity_image_url('type', (int) $victimShip['id'], 'render', 256) : null,
            'class'      => killmail_ship_class_label(isset($victimShip['metadata']['group_id']) ? (int) $victimShip['metadata']['group_id'] : null),
            'hull_price' => $shipTypeId > 0 && isset($priceMap[$shipTypeId]) ? $priceMap[$shipTypeId] : null,
        ],
        'location'       => [
            'system' => $system['name'],
            'region' => $region['name'],
        ],
        'value'          => $estimatedValue !== null ? supplycore_format_isk($estimatedValue) : null,
        'value_raw'      => $estimatedValue,
        'attacker_count' => count($attackers),
        'top_attackers'  => array_slice($formattedAttackers, 0, 5),
        'final_blow'     => $finalBlow,
        'items'          => $itemSummary,
        'zkb_url'        => 'https://zkillboard.com/kill/' . (int) ($event['killmail_id'] ?? 0) . '/',
    ];
}, [
    'dependencies' => ['killmail_detail'],
    'lock_ttl' => 10,
]);

if (isset($data['error']) && $data['error'] !== null) {
    public_api_respond_error(404, (string) $data['error']);
}

public_api_respond($data, 120);
