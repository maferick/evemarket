<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Missing Items';
$data = missing_items_data();
$summary = $data['summary'] ?? [];
$tableColumns = [
    'module' => 'Module',
    'jita_price' => 'Jita Price',
    'daily_volume' => 'Daily Volume',
    'priority' => 'Priority',
];
$tableRows = $data['rows'] ?? [];
$emptyMessage = 'No missing items detected.';

include __DIR__ . '/../../src/views/partials/header.php';
include __DIR__ . '/../../src/views/partials/module-page.php';
include __DIR__ . '/../../src/views/partials/footer.php';
