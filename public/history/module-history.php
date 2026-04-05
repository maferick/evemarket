<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Module History';
$data = module_history_data($_GET);
$summary = $data['summary'] ?? [];
$filters = $data['filters'] ?? [];
$registry = $data['module_registry'] ?? [];
$moduleOptions = [];
foreach ($registry as $key => $module) {
    $moduleOptions[$key] = (string) ($module['label'] ?? $key);
}
$filterAction = '/history/module-history';
$filterFields = [
    [
        'key' => 'module',
        'label' => 'Module',
        'type' => 'select',
        'value' => (string) ($filters['module'] ?? 'deviation_trend'),
        'options' => $moduleOptions,
    ],
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
    'module' => 'Analytics Module',
    'metric' => 'Metric Value',
    'context' => 'Context',
];
$tableRows = $data['rows'] ?? [];
$emptyMessage = (string) ($data['empty_message'] ?? 'No module history records yet.');

include __DIR__ . '/../../src/views/partials/header.php';
include __DIR__ . '/../../src/views/partials/module-page.php';
include __DIR__ . '/../../src/views/partials/footer.php';
