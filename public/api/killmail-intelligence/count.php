<?php

declare(strict_types=1);

// AJAX endpoint for the /killmail-intelligence loss-list total count.
//
// Why this exists: db_killmail_overview_page() used to run a COUNT(*) across
// 1.5M+ killmail_events rows on every page render. Even with the composite
// idx_killmail_mailtype_effective_seq index that count adds measurable
// latency to the initial paint. The main page now defers the count here so
// the first paint lands as soon as the 25 rows come back; JS then fetches
// this endpoint to fill in the "Total: N losses" label.
//
// Response shape:
//   HTTP 200 { ok:true, total:N, filters:{...}, cached_at:ISO8601 }
//
// The response is cached alongside the main overview cache (same
// `killmail_overview` dependency tag) so killmail ingestion busts both
// entries at the same time.

require_once __DIR__ . '/../../../src/bootstrap.php';

header('Content-Type: application/json; charset=utf-8');
header('Cache-Control: private, max-age=30');

try {
    $allianceId = max(0, (int) ($_GET['alliance_id'] ?? 0));
    $corporationId = max(0, (int) ($_GET['corporation_id'] ?? 0));
    $trackedOnly = ((string) ($_GET['tracked_only'] ?? '0')) === '1';
    $mailTypeRaw = (string) ($_GET['mail_type'] ?? 'loss');
    $mailType = in_array($mailTypeRaw, ['kill', 'loss', ''], true) ? $mailTypeRaw : 'loss';
    $startDate = '2024-01-01 00:00:00';

    $filters = [
        'alliance_id' => $allianceId,
        'corporation_id' => $corporationId,
        'tracked_only' => $trackedOnly,
        'mail_type' => $mailType,
        'start_date' => $startDate,
    ];

    // Cache the count for the full overview TTL. The cache is invalidated
    // together with the main killmail_overview cache on ingestion, so the
    // number stays in sync with the displayed list.
    $cacheKey = [
        'total_count',
        'type', $mailType,
        'alliance', $allianceId,
        'corp', $corporationId,
        'tracked', (int) $trackedOnly,
    ];
    $total = supplycore_cache_aside(
        'killmail_overview',
        $cacheKey,
        supplycore_cache_ttl('killmail_summary'),
        static fn (): int => db_killmail_overview_total_count($filters),
        [
            'dependencies' => ['killmail_overview'],
            'lock_ttl' => 20,
        ]
    );

    echo json_encode([
        'ok' => true,
        'total' => (int) $total,
        'filters' => [
            'alliance_id' => $allianceId,
            'corporation_id' => $corporationId,
            'tracked_only' => $trackedOnly,
            'mail_type' => $mailType,
        ],
        'cached_at' => gmdate('c'),
    ], JSON_UNESCAPED_SLASHES);
} catch (Throwable $exception) {
    http_response_code(500);
    echo json_encode([
        'ok' => false,
        'error' => $exception->getMessage(),
    ], JSON_UNESCAPED_SLASHES);
}
