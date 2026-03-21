#!/usr/bin/env php
<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/bootstrap.php';

try {
    $state = scheduler_daemon_health_summary();
    fwrite(STDOUT, json_encode($state, JSON_UNESCAPED_SLASHES) . PHP_EOL);

    exit(!empty($state['is_healthy']) ? 0 : (($state['derived_status'] ?? 'stopped') === 'degraded' ? 1 : 2));
} catch (Throwable $exception) {
    fwrite(STDOUT, json_encode([
        'daemon_key' => 'master',
        'status' => 'failed',
        'derived_status' => 'degraded',
        'is_healthy' => false,
        'error' => scheduler_normalize_error_message($exception->getMessage()),
    ], JSON_UNESCAPED_SLASHES) . PHP_EOL);

    exit(2);
}
