#!/usr/bin/env php
<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/bootstrap.php';

try {
    $result = scheduler_watchdog_run('scheduler_daemon_output');
    scheduler_daemon_output('scheduler_watchdog.summary', $result);

    exit(($result['status'] ?? 'degraded') === 'degraded' ? 1 : 0);
} catch (Throwable $exception) {
    scheduler_daemon_output('scheduler_watchdog.error', [
        'error' => scheduler_normalize_error_message($exception->getMessage()),
    ], STDERR);

    exit(1);
}
