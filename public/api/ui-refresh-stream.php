<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$pageId = trim((string) ($_GET['page_id'] ?? ''));
$config = supplycore_live_refresh_page_config($pageId);
if ($config === null) {
    http_response_code(404);
    header('Content-Type: text/plain; charset=UTF-8');
    echo "Unknown live-refresh page.";
    exit;
}

$lastEventId = max(0, (int) ($_GET['cursor'] ?? 0));

@set_time_limit(5);
ignore_user_abort(false);

header('Content-Type: text/event-stream; charset=UTF-8');
header('Cache-Control: no-store, no-cache, must-revalidate, max-age=0');
header('Connection: keep-alive');

$initial = supplycore_live_refresh_state_payload($config) + ['transport' => 'sse'];
echo "event: init\n";
echo 'data: ' . json_encode($initial, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE) . "\n\n";
@ob_flush();
@flush();

$events = db_ui_refresh_events_after($lastEventId, 25);
foreach ($events as $row) {
    $event = supplycore_ui_refresh_normalize_event_row($row);
    if (!supplycore_live_refresh_matches_page($config, $event)) {
        $lastEventId = max($lastEventId, (int) ($event['id'] ?? 0));
        continue;
    }

    $lastEventId = max($lastEventId, (int) ($event['id'] ?? 0));
    echo "event: ui-refresh\n";
    echo 'id: ' . $lastEventId . "\n";
    echo 'data: ' . json_encode($event, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE) . "\n\n";
}

@ob_flush();
@flush();

