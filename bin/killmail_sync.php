#!/usr/bin/env php
<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/bootstrap.php';

function killmail_sync_log(string $event, array $payload = []): void
{
    fwrite(STDOUT, json_encode(['event' => $event, 'ts' => gmdate(DATE_ATOM)] + $payload, JSON_UNESCAPED_SLASHES) . PHP_EOL);
}

$loop = in_array('--loop', $argv, true);

if (!killmail_ingestion_enabled()) {
    killmail_sync_log('killmail.sync.disabled', ['message' => 'Enable killmail ingestion in settings first.']);
    exit(0);
}

do {
    try {
        $result = sync_killmail_r2z2_stream('incremental');
        $meta = is_array($result['meta'] ?? null) ? $result['meta'] : [];
        killmail_sync_log('killmail.sync.success', [
            'rows_seen' => (int) ($result['rows_seen'] ?? 0),
            'rows_written' => (int) ($result['rows_written'] ?? 0),
            'cursor' => (string) ($result['cursor'] ?? ''),
            'outcome_reason' => (string) ($meta['outcome_reason'] ?? ''),
            'warnings' => $result['warnings'] ?? [],
        ] + $meta);
    } catch (Throwable $exception) {
        killmail_sync_log('killmail.sync.error', ['error' => $exception->getMessage()]);
    }

    if ($loop) {
        sleep(killmail_poll_sleep_seconds());
    }
} while ($loop);
