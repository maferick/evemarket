#!/usr/bin/env php
<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/bootstrap.php';

const CRON_TICK_WATCHDOG_LOCK = 'supplycore:scheduler_watchdog';

function cron_tick_output(string $event, array $payload = [], $stream = STDOUT): void
{
    scheduler_daemon_output($event, $payload, $stream);
}

function cron_tick_main(): int
{
    $startedAt = microtime(true);
    $lockAcquired = false;

    try {
        $lockAcquired = db_runner_lock_acquire(CRON_TICK_WATCHDOG_LOCK, 0);
        if (!$lockAcquired) {
            cron_tick_output('cron_tick.watchdog_skipped', [
                'reason' => 'lock_held',
                'duration_ms' => (int) round((microtime(true) - $startedAt) * 1000),
            ]);

            return 0;
        }

        cron_tick_output('cron_tick.watchdog_started', [
            'scheduler_status' => 'checking',
        ]);

        $result = scheduler_watchdog_run('cron_tick_output');

        cron_tick_output('cron_tick.watchdog_summary', [
            'scheduler_status' => (string) ($result['status'] ?? 'healthy'),
            'action' => (string) ($result['action'] ?? 'none'),
            'duration_ms' => (int) round((microtime(true) - $startedAt) * 1000),
        ] + (array) ($result['health'] ?? []));

        return ($result['status'] ?? 'healthy') === 'degraded' ? 1 : 0;
    } catch (Throwable $exception) {
        cron_tick_output('cron_tick.watchdog_error', [
            'scheduler_status' => 'failed',
            'duration_ms' => (int) round((microtime(true) - $startedAt) * 1000),
            'error' => scheduler_normalize_error_message($exception->getMessage()),
        ], STDERR);

        return 1;
    } finally {
        if ($lockAcquired) {
            db_runner_lock_release(CRON_TICK_WATCHDOG_LOCK);
        }
    }
}

exit(cron_tick_main());
