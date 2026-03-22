#!/usr/bin/env php
<?php

declare(strict_types=1);

try {
    require_once __DIR__ . '/../src/bootstrap.php';

    fwrite(STDOUT, json_encode(orchestrator_runtime_config_export(), JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . PHP_EOL);
    exit(0);
} catch (Throwable $exception) {
    fwrite(STDERR, json_encode([
        'error' => scheduler_normalize_error_message($exception->getMessage()),
    ], JSON_UNESCAPED_SLASHES) . PHP_EOL);

    exit(1);
}
