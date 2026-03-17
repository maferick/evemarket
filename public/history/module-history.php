<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Module History';
$data = module_history_data();
$summary = $data['summary'] ?? [];
$tableColumns = [
    'module' => 'Module',
    'latest_price' => 'Latest Price',
    'seven_day_change' => '7 Day Change',
    'thirty_day_change' => '30 Day Change',
];
$tableRows = $data['rows'] ?? [];
$emptyMessage = 'No module history records yet.';

include __DIR__ . '/../../src/views/partials/header.php';
include __DIR__ . '/../../src/views/partials/module-page.php';
include __DIR__ . '/../../src/views/partials/footer.php';
