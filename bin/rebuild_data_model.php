<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/bootstrap.php';

function rebuild_cli_bool(string $value): bool
{
    return in_array(strtolower(trim($value)), ['1', 'true', 'yes', 'on'], true);
}

$options = getopt('', [
    'mode::',
    'window-days::',
    'full-reset::',
    'enable-partitioned-history::',
]);

$mode = trim((string) ($options['mode'] ?? 'rebuild-all-derived'));
$windowDays = isset($options['window-days']) ? max(1, (int) $options['window-days']) : null;
$fullReset = isset($options['full-reset']) ? rebuild_cli_bool((string) $options['full-reset']) : false;
$enablePartitionedHistory = isset($options['enable-partitioned-history'])
    ? rebuild_cli_bool((string) $options['enable-partitioned-history'])
    : true;

$allowedModes = [
    'rebuild-current-only',
    'rebuild-rollups-only',
    'rebuild-all-derived',
    'full-reset',
];

if (!in_array($mode, $allowedModes, true)) {
    fwrite(STDERR, "Unsupported --mode value. Use one of: " . implode(', ', $allowedModes) . PHP_EOL);
    exit(1);
}

if ($mode === 'full-reset') {
    $fullReset = true;
    $mode = 'rebuild-all-derived';
}

$result = [
    'mode' => $mode,
    'window' => supplycore_rebuild_window_bounds($windowDays),
    'full_reset' => $fullReset,
    'enable_partitioned_history' => $enablePartitionedHistory,
    'steps' => [],
];

try {
    if ($enablePartitionedHistory) {
        $result['steps']['partitioned_history'] = supplycore_rebuild_partitioned_market_history($result['window']);
    }

    if (in_array($mode, ['rebuild-current-only', 'rebuild-all-derived'], true)) {
        $result['steps']['current'] = supplycore_rebuild_current_layers('cli-rebuild', $fullReset);
    }

    if (in_array($mode, ['rebuild-rollups-only', 'rebuild-all-derived'], true)) {
        $result['steps']['rollups'] = supplycore_rebuild_rollup_layers('cli-rebuild', $windowDays, $fullReset);
    }

    echo json_encode($result, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . PHP_EOL;
    exit(0);
} catch (Throwable $exception) {
    $result['error'] = $exception->getMessage();
    fwrite(STDERR, json_encode($result, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . PHP_EOL);
    exit(1);
}
