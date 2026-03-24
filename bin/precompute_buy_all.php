<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/bootstrap.php';

$startedAt = microtime(true);
$summary = buy_all_precompute_refresh_defaults();
$durationMs = (microtime(true) - $startedAt) * 1000.0;

echo json_encode([
    'ok' => true,
    'refreshed' => $summary,
    'duration_ms' => round($durationMs, 2),
], JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT) . PHP_EOL;
