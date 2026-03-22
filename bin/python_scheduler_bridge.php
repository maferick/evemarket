#!/usr/bin/env php
<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/bootstrap.php';

function python_scheduler_bridge_read_stdin_json(): array
{
    $raw = stream_get_contents(STDIN);
    if (!is_string($raw) || trim($raw) === '') {
        return [];
    }

    $decoded = json_decode($raw, true);
    if (!is_array($decoded)) {
        throw new InvalidArgumentException('STDIN must contain a JSON object.');
    }

    return $decoded;
}

function python_scheduler_bridge_output(array $payload, int $exitCode = 0): never
{
    fwrite(STDOUT, json_encode($payload, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE) . PHP_EOL);
    exit($exitCode);
}

try {
    $options = getopt('', ['action:', 'job-key::', 'schedule-id::', 'snapshot-key::', 'reason::']);
    $action = trim((string) ($options['action'] ?? ''));

    if ($action === 'market-comparison-context') {
        python_scheduler_bridge_output(['ok' => true, 'context' => python_bridge_market_comparison_context()]);
    }

    if ($action === 'store-snapshot') {
        $snapshotKey = trim((string) ($options['snapshot-key'] ?? ''));
        if ($snapshotKey === '') {
            throw new InvalidArgumentException('Argument --snapshot-key is required for store-snapshot.');
        }

        $input = python_scheduler_bridge_read_stdin_json();
        $payload = is_array($input['payload'] ?? null) ? $input['payload'] : [];
        $meta = is_array($input['meta'] ?? null) ? $input['meta'] : [];
        $stored = supplycore_materialized_snapshot_store($snapshotKey, $payload, $meta);
        python_scheduler_bridge_output(['ok' => true, 'snapshot' => $stored]);
    }

    if ($action === 'run-job-handler') {
        $jobKey = trim((string) ($options['job-key'] ?? ''));
        if ($jobKey === '') {
            throw new InvalidArgumentException('Argument --job-key is required for run-job-handler.');
        }

        $reason = trim((string) ($options['reason'] ?? 'python-fallback'));
        $result = python_bridge_run_job_handler($jobKey, $reason);
        python_scheduler_bridge_output(['ok' => true, 'result' => $result]);
    }

    if ($action === 'finalize-job') {
        $scheduleId = (int) ($options['schedule-id'] ?? 0);
        if ($scheduleId <= 0) {
            throw new InvalidArgumentException('Argument --schedule-id must be a positive integer for finalize-job.');
        }

        $job = db_sync_schedule_fetch_by_id($scheduleId);
        if (!is_array($job)) {
            throw new RuntimeException('No scheduler job found for schedule ID ' . $scheduleId . '.');
        }

        $input = python_scheduler_bridge_read_stdin_json();
        $result = scheduler_finalize_python_job_result($job, $input);
        python_scheduler_bridge_output(['ok' => true, 'result' => $result]);
    }

    throw new InvalidArgumentException('Unknown or missing --action value.');
} catch (Throwable $exception) {
    python_scheduler_bridge_output([
        'ok' => false,
        'error' => scheduler_normalize_error_message($exception->getMessage()),
    ], 1);
}
