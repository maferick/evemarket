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

    if ($action === 'killmail-context') {
        python_scheduler_bridge_output(['ok' => true, 'context' => python_bridge_killmail_context()]);
    }

    if ($action === 'market-hub-local-history-context') {
        python_scheduler_bridge_output(['ok' => true, 'context' => python_bridge_market_hub_local_history_context()]);
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

    if ($action === 'process-killmail-batch') {
        $input = python_scheduler_bridge_read_stdin_json();
        $payloads = array_values(array_filter(
            array_map(
                static fn ($row): array => is_array($row) ? $row : [],
                (array) ($input['payloads'] ?? [])
            ),
            static fn (array $row): bool => $row !== []
        ));
        $result = python_bridge_process_killmail_batch($payloads);
        python_scheduler_bridge_output(['ok' => true, 'result' => $result]);
    }

    if ($action === 'worker-runtime-config') {
        $runtime = orchestrator_runtime_config_export();
        python_scheduler_bridge_output([
            'ok' => true,
            'workers' => (array) ($runtime['workers'] ?? []),
            'definitions' => worker_job_registry_definitions(),
        ]);
    }

    if ($action === 'queue-recurring-jobs') {
        $input = python_scheduler_bridge_read_stdin_json();
        $jobKeys = array_values(array_filter(array_map('strval', (array) ($input['job_keys'] ?? [])), static fn (string $jobKey): bool => trim($jobKey) !== ''));
        $result = db_worker_job_queue_due_recurring_jobs($jobKeys);
        python_scheduler_bridge_output(['ok' => true, 'result' => $result]);
    }

    if ($action === 'claim-worker-job') {
        $input = python_scheduler_bridge_read_stdin_json();
        $workerId = trim((string) ($input['worker_id'] ?? ''));
        if ($workerId === '') {
            throw new InvalidArgumentException('worker_id is required for claim-worker-job.');
        }

        $job = db_worker_job_claim_next(
            $workerId,
            (array) ($input['queues'] ?? []),
            (array) ($input['workload_classes'] ?? []),
            isset($input['lease_seconds']) ? (int) $input['lease_seconds'] : null
        );

        python_scheduler_bridge_output(['ok' => true, 'job' => $job]);
    }

    if ($action === 'heartbeat-worker-job') {
        $input = python_scheduler_bridge_read_stdin_json();
        $jobId = (int) ($input['job_id'] ?? 0);
        $workerId = trim((string) ($input['worker_id'] ?? ''));
        $result = db_worker_job_heartbeat($jobId, $workerId, isset($input['lease_seconds']) ? (int) $input['lease_seconds'] : null);
        python_scheduler_bridge_output(['ok' => true, 'result' => $result]);
    }

    if ($action === 'complete-worker-job') {
        $input = python_scheduler_bridge_read_stdin_json();
        $jobId = (int) ($input['job_id'] ?? 0);
        $workerId = trim((string) ($input['worker_id'] ?? ''));
        $result = is_array($input['result'] ?? null) ? $input['result'] : [];
        $stored = db_worker_job_complete($jobId, $workerId, $result);
        python_scheduler_bridge_output(['ok' => true, 'job' => $stored]);
    }

    if ($action === 'retry-worker-job') {
        $input = python_scheduler_bridge_read_stdin_json();
        $jobId = (int) ($input['job_id'] ?? 0);
        $workerId = trim((string) ($input['worker_id'] ?? ''));
        $error = trim((string) ($input['error'] ?? 'Worker job failed.'));
        $retryDelaySeconds = isset($input['retry_delay_seconds']) ? (int) $input['retry_delay_seconds'] : null;
        $result = is_array($input['result'] ?? null) ? $input['result'] : [];
        $stored = db_worker_job_retry($jobId, $workerId, $error, $retryDelaySeconds, $result);
        python_scheduler_bridge_output(['ok' => true, 'job' => $stored]);
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

    if ($action === 'sync-run-start') {
        $input = python_scheduler_bridge_read_stdin_json();
        $datasetKey = trim((string) ($input['dataset_key'] ?? ''));
        $runMode = trim((string) ($input['run_mode'] ?? 'incremental'));
        $result = python_bridge_sync_run_start($datasetKey, $runMode);
        python_scheduler_bridge_output(['ok' => true, 'result' => $result]);
    }

    if ($action === 'sync-cursor-upsert') {
        $input = python_scheduler_bridge_read_stdin_json();
        $result = python_bridge_sync_cursor_upsert($input);
        python_scheduler_bridge_output(['ok' => true, 'result' => $result]);
    }

    if ($action === 'sync-run-finish') {
        $input = python_scheduler_bridge_read_stdin_json();
        $result = python_bridge_sync_run_finish($input);
        python_scheduler_bridge_output(['ok' => true, 'result' => $result]);
    }

    throw new InvalidArgumentException('Unknown or missing --action value.');
} catch (Throwable $exception) {
    python_scheduler_bridge_output([
        'ok' => false,
        'error' => scheduler_normalize_error_message($exception->getMessage()),
    ], 1);
}
