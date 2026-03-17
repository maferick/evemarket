<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Jita Comparison';
$data = jita_comparison_data();
$summary = $data['summary'] ?? [];
$tableColumns = [
    'module' => 'Module',
    'alliance_price' => 'Alliance Price',
    'jita_price' => 'Jita Price',
    'delta' => 'Delta',
];
$tableRows = $data['rows'] ?? [];
$emptyMessage = 'No comparison rows available.';

include __DIR__ . '/../../src/views/partials/header.php';
include __DIR__ . '/../../src/views/partials/module-page.php';
include __DIR__ . '/../../src/views/partials/footer.php';
