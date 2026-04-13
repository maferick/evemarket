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

    if ($action === 'killmail-backfill-context') {
        python_scheduler_bridge_output(['ok' => true, 'context' => python_bridge_killmail_backfill_context()]);
    }

    if ($action === 'killmail-full-history-backfill-context') {
        python_scheduler_bridge_output(['ok' => true, 'context' => python_bridge_killmail_full_history_backfill_context()]);
    }

    if ($action === 'update-setting') {
        $input = python_scheduler_bridge_read_stdin_json();
        $key = trim((string) ($input['key'] ?? ''));
        $value = (string) ($input['value'] ?? '');
        if ($key !== '') {
            save_settings([$key => $value]);
        }
        python_scheduler_bridge_output(['ok' => true]);
    }

    if ($action === 'market-hub-local-history-context') {
        python_scheduler_bridge_output(['ok' => true, 'context' => python_bridge_market_hub_local_history_context()]);
    }

    if ($action === 'market-history-tables-context') {
        python_scheduler_bridge_output(['ok' => true, 'context' => python_bridge_market_history_tables_context()]);
    }

    if ($action === 'item-scope-context') {
        python_scheduler_bridge_output(['ok' => true, 'context' => python_bridge_item_scope_context()]);
    }

    if ($action === 'rebuild-partitioned-history') {
        $input = python_scheduler_bridge_read_stdin_json();
        $window = is_array($input['window'] ?? null) ? $input['window'] : [];
        python_scheduler_bridge_output(['ok' => true, 'result' => supplycore_rebuild_partitioned_market_history($window)]);
    }

    if ($action === 'refresh-derived-summaries') {
        $input = python_scheduler_bridge_read_stdin_json();
        $reason = trim((string) ($input['reason'] ?? 'python-rebuild'));
        $currentState = supplycore_refresh_current_state_cache($reason . ':current-state');
        // Doctrine intelligence refresh is now handled by the Python
        // compute_auto_doctrines job — nothing to do from the PHP side.
        python_scheduler_bridge_output([
            'ok' => true,
            'result' => [
                'current_state_rows_written' => (int) ($currentState['rows_written'] ?? 0),
                'doctrine_snapshot_groups' => 0,
            ],
        ]);
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

    if ($action === 'killmail-ids-existing') {
        $input = python_scheduler_bridge_read_stdin_json();
        $killmailIds = array_values(array_map('intval', (array) ($input['killmail_ids'] ?? [])));
        python_scheduler_bridge_output(['ok' => true, 'existing' => db_killmail_ids_existing($killmailIds)]);
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
        $skipEntityFilter = (bool) ($input['skip_entity_filter'] ?? false);
        $result = python_bridge_process_killmail_batch($payloads, $skipEntityFilter);
        python_scheduler_bridge_output(['ok' => true, 'result' => $result]);
    }

    if ($action === 'character-killmail-sync-context') {
        $appName = trim((string) get_setting('app_name', 'SupplyCore'));
        if ($appName === '') {
            $appName = 'SupplyCore';
        }
        $userAgent = $appName . ' character-killmail-sync/1.0 (+https://github.com/cvweiss/supplycore)';
        python_scheduler_bridge_output([
            'ok' => true,
            'context' => ['user_agent' => $userAgent],
        ]);
    }

    if ($action === 'character-killmail-queue-pending') {
        $input = python_scheduler_bridge_read_stdin_json();
        $limit = max(1, min(500, (int) ($input['limit'] ?? 50)));

        // Reap any rows stuck in 'processing' for more than 15 minutes. These
        // are characters a previous run claimed but never finished (time budget
        // expired mid-batch, job crashed, etc.). Without this, orphaned
        // 'processing' rows accumulate indefinitely and effectively drain the
        // queue without doing any real work.
        db_execute(
            "UPDATE character_killmail_queue
             SET status = 'pending'
             WHERE status = 'processing'
               AND (processed_at IS NULL
                    OR processed_at < DATE_SUB(UTC_TIMESTAMP(), INTERVAL 15 MINUTE))"
        );

        $rows = db_select(
            "SELECT character_id, priority, priority_reason, status, mode,
                    last_page_fetched, last_killmail_id_seen, last_killmail_at_seen,
                    killmails_found, backfill_complete, queued_at, last_success_at,
                    last_incremental_at, last_full_backfill_at
             FROM character_killmail_queue
             WHERE status IN ('pending', 'error')
             ORDER BY priority DESC, queued_at ASC
             LIMIT ?",
            [$limit]
        );
        // Mark these rows as 'processing' and stamp processed_at so the reap
        // logic above can recover them if the worker never comes back.
        if ($rows) {
            $ids = array_map(static fn (array $row): int => (int) $row['character_id'], $rows);
            $placeholders = implode(',', array_fill(0, count($ids), '?'));
            db_execute(
                "UPDATE character_killmail_queue
                 SET status = 'processing', processed_at = UTC_TIMESTAMP()
                 WHERE character_id IN ({$placeholders})",
                $ids
            );
        }
        python_scheduler_bridge_output(['ok' => true, 'rows' => $rows]);
    }

    if ($action === 'character-killmail-queue-update') {
        $input = python_scheduler_bridge_read_stdin_json();
        $characterId = (int) ($input['character_id'] ?? 0);
        if ($characterId <= 0) {
            throw new InvalidArgumentException('character_id is required for character-killmail-queue-update.');
        }

        $sets = [];
        $params = [];

        $status = (string) ($input['status'] ?? '');
        if ($status !== '' && in_array($status, ['pending', 'processing', 'done', 'error'], true)) {
            $sets[] = 'status = ?';
            $params[] = $status;
            if ($status === 'done') {
                $sets[] = 'last_success_at = UTC_TIMESTAMP()';
                $sets[] = 'processed_at = UTC_TIMESTAMP()';
            } elseif ($status === 'error') {
                $sets[] = 'processed_at = UTC_TIMESTAMP()';
            }
        }
        if (array_key_exists('last_page_fetched', $input)) {
            $sets[] = 'last_page_fetched = ?';
            $params[] = max(0, (int) $input['last_page_fetched']);
        }
        if (array_key_exists('last_killmail_id_seen', $input) && $input['last_killmail_id_seen'] !== null) {
            $sets[] = 'last_killmail_id_seen = ?';
            $params[] = (int) $input['last_killmail_id_seen'];
        }
        if (array_key_exists('last_killmail_at_seen', $input) && $input['last_killmail_at_seen'] !== null) {
            $seenRaw = (string) $input['last_killmail_at_seen'];
            $seenTs = $seenRaw !== '' ? strtotime($seenRaw) : false;
            if ($seenTs !== false) {
                $sets[] = 'last_killmail_at_seen = ?';
                $params[] = gmdate('Y-m-d H:i:s', $seenTs);
            }
        }
        if (array_key_exists('killmails_found_delta', $input)) {
            $delta = max(0, (int) $input['killmails_found_delta']);
            if ($delta > 0) {
                $sets[] = 'killmails_found = killmails_found + ?';
                $params[] = $delta;
            }
        }
        if (array_key_exists('backfill_complete', $input)) {
            $sets[] = 'backfill_complete = ?';
            $params[] = ((bool) $input['backfill_complete']) ? 1 : 0;
            if ((bool) $input['backfill_complete']) {
                $sets[] = 'last_full_backfill_at = UTC_TIMESTAMP()';
            }
        }
        if (array_key_exists('mode', $input) && in_array($input['mode'], ['incremental', 'backfill'], true)) {
            $sets[] = 'mode = ?';
            $params[] = (string) $input['mode'];
            if ($input['mode'] === 'incremental') {
                $sets[] = 'last_incremental_at = UTC_TIMESTAMP()';
            }
        }
        if (array_key_exists('last_error', $input) && $input['last_error'] !== null) {
            $sets[] = 'last_error = ?';
            $params[] = substr((string) $input['last_error'], 0, 500);
        } elseif ($status === 'done') {
            $sets[] = 'last_error = NULL';
        }

        if ($sets === []) {
            python_scheduler_bridge_output(['ok' => true, 'updated' => 0]);
        } else {
            $params[] = $characterId;
            $sql = 'UPDATE character_killmail_queue SET ' . implode(', ', $sets) . ' WHERE character_id = ?';
            $updated = db_execute($sql, $params);
            python_scheduler_bridge_output(['ok' => true, 'updated' => (int) $updated]);
        }
    }

    if ($action === 'repair-killmail-zkb') {
        $input = python_scheduler_bridge_read_stdin_json();
        $updates = (array) ($input['updates'] ?? []);
        $result = python_bridge_repair_killmail_zkb($updates);
        python_scheduler_bridge_output(['ok' => true, 'result' => $result]);
    }

    if ($action === 'killmails-missing-zkb') {
        $input = python_scheduler_bridge_read_stdin_json();
        $limit = max(1, min(1000, (int) ($input['limit'] ?? 500)));
        $offset = max(0, (int) ($input['offset'] ?? 0));
        $rows = db_select(
            'SELECT killmail_id, killmail_hash FROM killmail_events WHERE zkb_total_value IS NULL ORDER BY killmail_id ASC LIMIT ? OFFSET ?',
            [$limit, $offset]
        );
        $totalRow = db_select_one('SELECT COUNT(*) AS total FROM killmail_events WHERE zkb_total_value IS NULL');
        $total = (int) ($totalRow['total'] ?? 0);
        python_scheduler_bridge_output(['ok' => true, 'rows' => $rows, 'total' => $total]);
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

        $queues = (array) ($input['queues'] ?? []);
        $workloadClasses = (array) ($input['workload_classes'] ?? []);
        $executionModes = (array) ($input['execution_modes'] ?? []);

        $job = db_worker_job_claim_next(
            $workerId,
            $queues,
            $workloadClasses,
            $executionModes,
            isset($input['lease_seconds']) ? (int) $input['lease_seconds'] : null
        );

        $diagnostics = null;
        if (!is_array($job) || $job === []) {
            $diagnostics = db_worker_job_claim_diagnostics($queues, $workloadClasses, $executionModes);
        }

        python_scheduler_bridge_output(['ok' => true, 'job' => $job, 'diagnostics' => $diagnostics]);
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

    if ($action === 'finalize-job-by-key') {
        $jobKey = trim((string) ($options['job-key'] ?? ''));
        if ($jobKey === '') {
            throw new InvalidArgumentException('Argument --job-key is required for finalize-job-by-key.');
        }

        $rows = db_sync_schedule_fetch_by_job_keys([$jobKey]);
        $job = is_array($rows) && count($rows) > 0 ? $rows[0] : null;
        if (!is_array($job)) {
            throw new RuntimeException('No scheduler job found for job key ' . $jobKey . '.');
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

    if ($action === 'resolve-pending-entities') {
        $input = python_scheduler_bridge_read_stdin_json();
        $batchSize = max(1, min(2000, (int) ($input['batch_size'] ?? 500)));
        $retryAfterMinutes = max(1, (int) ($input['retry_after_minutes'] ?? 30));
        $result = killmail_entity_resolve_pending($batchSize, $retryAfterMinutes);
        python_scheduler_bridge_output(['ok' => true, 'result' => $result]);
    }

    throw new InvalidArgumentException('Unknown or missing --action value.');
} catch (Throwable $exception) {
    python_scheduler_bridge_output([
        'ok' => false,
        'error' => scheduler_normalize_error_message($exception->getMessage()),
    ], 1);
}
