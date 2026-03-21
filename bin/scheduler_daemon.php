#!/usr/bin/env php
<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/bootstrap.php';

try {
    exit(scheduler_daemon_run('scheduler_daemon_output'));
} catch (Throwable $exception) {
    scheduler_daemon_output('scheduler_daemon.bootstrap_error', [
        'error' => scheduler_normalize_error_message($exception->getMessage()),
    ], STDERR);

    exit(1);
}
