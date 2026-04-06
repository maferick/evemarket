<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';
require_once __DIR__ . '/../../src/map.php';

header('Content-Type: application/json; charset=utf-8');
header('Cache-Control: private, max-age=30');

$type = (string) ($_GET['type'] ?? '');
$scene = null;

switch ($type) {
    case 'system':
        $systemId = max(0, (int) ($_GET['system_id'] ?? 0));
        $hops = max(1, min(3, (int) ($_GET['hops'] ?? 2)));
        if ($systemId > 0) {
            $scene = map_build_system_scene($systemId, $hops);
        }
        break;

    case 'corridor':
        $corridorId = max(0, (int) ($_GET['corridor_id'] ?? 0));
        $systemIdsRaw = (string) ($_GET['system_ids'] ?? '');
        $surroundingHops = max(0, min(3, (int) ($_GET['hops'] ?? 1)));
        $systemIds = array_values(array_filter(
            array_map('intval', explode(',', $systemIdsRaw)),
            static fn(int $id): bool => $id > 0
        ));
        if ($corridorId > 0 && $systemIds !== []) {
            $scene = map_build_corridor_scene($corridorId, $systemIds, $surroundingHops);
        }
        break;

    case 'theater':
        $theaterId = trim((string) ($_GET['theater_id'] ?? ''));
        $systemIdsRaw = (string) ($_GET['system_ids'] ?? '');
        $hops = max(1, min(2, (int) ($_GET['hops'] ?? 1)));
        $systemIds = array_values(array_filter(
            array_map('intval', explode(',', $systemIdsRaw)),
            static fn(int $id): bool => $id > 0
        ));
        if ($theaterId !== '' && $systemIds !== []) {
            $scene = map_build_theater_scene($theaterId, $systemIds, $hops);
        }
        break;
}

if ($scene === null) {
    http_response_code(400);
    echo json_encode([
        'error' => 'Invalid request. Required: type=(system|corridor|theater) with appropriate scope params.',
    ]);
    exit;
}

echo json_encode(map_scene_to_json($scene), JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
