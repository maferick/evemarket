<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Alliance Structure Trends';
$data = alliance_trends_data($_GET);
$summary = $data['summary'] ?? [];
$filters = $data['filters'] ?? [];
$filterAction = '/history/alliance-trends';
$filterFields = [
    [
        'key' => 'days',
        'label' => 'Window',
        'type' => 'select',
        'value' => (string) ($filters['days'] ?? 30),
        'options' => [
            '14' => '14 days',
            '30' => '30 days',
            '60' => '60 days',
            '90' => '90 days',
            '180' => '180 days',
        ],
    ],
];
$tableColumns = [
    'date' => 'Date',
    'avg_price' => 'Avg Price',
    'volume' => 'Volume',
    'order_count' => 'Orders',
    'stock_sell_volume' => 'Sell Volume',
    'deviation_median' => 'Median Deviation',
    'trend' => 'Trend',
];
$tableRows = $data['rows'] ?? [];
$emptyMessage = (string) ($data['empty_message'] ?? 'No alliance trend snapshots yet.');

include __DIR__ . '/../../src/views/partials/header.php';
include __DIR__ . '/../../src/views/partials/module-page.php';
include __DIR__ . '/../../src/views/partials/footer.php';
