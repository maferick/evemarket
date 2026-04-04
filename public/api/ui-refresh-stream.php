<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

// Release the session file lock immediately — this endpoint never writes
// session data, and holding the lock for the full 60 s stream lifetime
// blocks every other request from the same browser session on session_start().
session_write_close();

$pageId = trim((string) ($_GET['page_id'] ?? ''));
$config = supplycore_live_refresh_page_config($pageId);
if ($config === null) {
    http_response_code(404);
    header('Content-Type: text/plain; charset=UTF-8');
    echo "Unknown live-refresh page.";
    exit;
}

$lastEventId = max(0, (int) ($_SERVER['HTTP_LAST_EVENT_ID'] ?? $_GET['cursor'] ?? 0));

// Bounded request lifetime — the client will reconnect via Last-Event-ID.
$maxLifetimeSeconds = 60;
$heartbeatIntervalSeconds = 15;
$pollIntervalSeconds = 2;

@set_time_limit($maxLifetimeSeconds + 5);
ignore_user_abort(false);

header('Content-Type: text/event-stream; charset=UTF-8');
header('Cache-Control: no-store, no-cache, must-revalidate, max-age=0');
header('Connection: keep-alive');
header('X-Accel-Buffering: no'); // Disable nginx proxy buffering for SSE

// Disable output buffering so events flush immediately.
while (ob_get_level() > 0) {
    ob_end_flush();
}

// ── Init event ──────────────────────────────────────────────────────────
$initial = supplycore_live_refresh_state_payload($config) + ['transport' => 'sse'];
echo "event: init\n";
echo 'data: ' . json_encode($initial, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE) . "\n\n";
flush();

// ── Event loop ──────────────────────────────────────────────────────────
$startedAt = time();
$lastHeartbeat = time();

while (true) {
    if (connection_aborted()) {
        break;
    }

    if ((time() - $startedAt) >= $maxLifetimeSeconds) {
        // Tell client to reconnect (clean end of stream).
        echo "event: reconnect\ndata: {}\n\n";
        flush();
        break;
    }

    // Fetch new events after the current cursor.
    $events = db_ui_refresh_events_after($lastEventId, 25);
    $emittedAny = false;

    foreach ($events as $row) {
        $event = supplycore_ui_refresh_normalize_event_row($row);
        $eventId = (int) ($event['id'] ?? 0);
        $lastEventId = max($lastEventId, $eventId);

        if (!supplycore_live_refresh_matches_page($config, $event)) {
            continue;
        }

        echo "event: ui-refresh\n";
        echo 'id: ' . $lastEventId . "\n";
        echo 'data: ' . json_encode($event, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE) . "\n\n";
        $emittedAny = true;
    }

    if ($emittedAny) {
        flush();
    }

    // Heartbeat to keep connection alive and detect stale clients.
    if ((time() - $lastHeartbeat) >= $heartbeatIntervalSeconds) {
        echo ": heartbeat " . gmdate('H:i:s') . "\n\n";
        flush();
        $lastHeartbeat = time();
    }

    // Short sleep between polls to avoid busy-waiting.
    usleep($pollIntervalSeconds * 1_000_000);
}
