<?php

declare(strict_types=1);

require_once __DIR__ . '/lib/api-client.php';

$theaterId = trim((string) ($_GET['theater_id'] ?? ''));
if ($theaterId === '') {
    http_response_code(400);
    echo 'Missing theater_id parameter.';
    exit;
}

// Fetch theater data from SupplyCore (server-to-server, authenticated)
$data = proxy_api_get('/api/public/theater.php', ['theater_id' => $theaterId]);

if (isset($data['error']) && $data['error'] !== null) {
    http_response_code(404);
    $errorMsg = proxy_e((string) $data['error']);
    echo "<!doctype html><html><head><meta charset='UTF-8'><title>Error</title><link rel='stylesheet' href='assets/css/proxy.css'></head><body><div class='proxy-shell'><main class='proxy-main'><section class='proxy-card'><p class='proxy-error'>{$errorMsg}</p></section></main></div></body></html>";
    exit;
}

// Extract view data
$theater            = (array) ($data['theater'] ?? []);
$battles            = (array) ($data['battles'] ?? []);
$systems            = (array) ($data['systems'] ?? []);
$timeline           = (array) ($data['timeline'] ?? []);
$allianceSummary    = (array) ($data['alliance_summary'] ?? []);
$turningPoints      = (array) ($data['turning_points'] ?? []);
$participants       = (array) ($data['participants'] ?? []);
$structureKills     = (array) ($data['structure_kills'] ?? []);
$resolvedEntities   = (array) ($data['resolved_entities'] ?? []);
$shipTypeNames      = (array) ($data['ship_type_names'] ?? []);
$sideLabels         = (array) ($data['side_labels'] ?? ['friendly' => 'Friendlies', 'opponent' => 'Opposition', 'third_party' => 'Third Party']);
$sideAlliancesByPilots = (array) ($data['side_alliances_by_pilots'] ?? []);
$sidePanels         = (array) ($data['side_panels'] ?? []);
$dataQualityNotes   = (array) ($data['data_quality_notes'] ?? []);
$killmails          = (array) ($data['killmails'] ?? []);
$mapSvg             = $data['map_svg'] ?? null;
$durationLabel      = (string) ($data['duration_label'] ?? '0m');
$totalIskDestroyed  = (float) ($data['total_isk_destroyed'] ?? 0);
$theaterStartActual = (string) ($data['theater_start_actual'] ?? '');
$theaterEndActual   = (string) ($data['theater_end_actual'] ?? '');
$displayKillTotal   = (int) ($data['display_kill_total'] ?? 0);
$reportedKillTotal  = (int) ($data['reported_kill_total'] ?? 0);
$observedKillTotal  = (int) ($data['observed_kill_total'] ?? 0);

$trackedAllianceIds     = (array) ($data['tracked_alliance_ids'] ?? []);
$opponentAllianceIds    = (array) ($data['opponent_alliance_ids'] ?? []);
$trackedCorporationIds  = (array) ($data['tracked_corporation_ids'] ?? []);
$opponentCorporationIds = (array) ($data['opponent_corporation_ids'] ?? []);

// Build classify closure (same logic as SupplyCore)
$classifyAlliance = static function (int $allianceId, int $corporationId = 0) use ($trackedAllianceIds, $opponentAllianceIds, $trackedCorporationIds, $opponentCorporationIds): string {
    if ($allianceId > 0 && in_array($allianceId, $trackedAllianceIds, true)) return 'friendly';
    if ($corporationId > 0 && in_array($corporationId, $trackedCorporationIds, true)) return 'friendly';
    if ($allianceId > 0 && in_array($allianceId, $opponentAllianceIds, true)) return 'opponent';
    if ($corporationId > 0 && in_array($corporationId, $opponentCorporationIds, true)) return 'opponent';
    return 'third_party';
};

$sideColorClass = [
    'friendly'    => 'text-blue-300',
    'opponent'    => 'text-red-300',
    'third_party' => 'text-slate-400',
];
$sideBgClass = [
    'friendly'    => 'bg-blue-900/60',
    'opponent'    => 'bg-red-900/60',
    'third_party' => 'bg-slate-700',
];

$config = proxy_config();
$siteName = proxy_e((string) ($config['site_name'] ?? 'Battle Reports'));
$systemName = proxy_e((string) ($theater['primary_system_name'] ?? 'Unknown'));

?>
<!doctype html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title><?= $systemName ?> — <?= $siteName ?></title>
    <link rel="stylesheet" href="assets/css/proxy.css">
</head>
<body>
<div class="proxy-shell">
    <main class="proxy-main">

        <?php include __DIR__ . '/partials/_header.php'; ?>
        <?php include __DIR__ . '/partials/_location_map.php'; ?>
        <?php include __DIR__ . '/partials/_battle_report_classic_standalone.php'; ?>

    </main>
</div>
</body>
</html>
