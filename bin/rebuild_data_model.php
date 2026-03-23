<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/bootstrap.php';

$pythonBinary = scheduler_python_binary();
$runner = __DIR__ . '/rebuild_data_model.py';

if (!is_file($runner)) {
    fwrite(STDERR, json_encode([
        'error' => 'Missing Python rebuild runner at ' . $runner,
    ], JSON_UNESCAPED_SLASHES) . PHP_EOL);
    exit(1);
}

$args = array_slice($_SERVER['argv'] ?? [], 1);
$command = array_merge([
    escapeshellarg($pythonBinary),
    escapeshellarg($runner),
], array_map(static fn (string $arg): string => escapeshellarg($arg), $args));

passthru(implode(' ', $command), $exitCode);
exit((int) $exitCode);
