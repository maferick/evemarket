<?php

declare(strict_types=1);

// AJAX endpoint for the /killmail-intelligence ESI coverage panel.
//
// Why this exists: db_killmail_esi_coverage_snapshot() is a ~200 second query
// on the current dataset (see src/functions.php comments for details). It
// used to run inline on every /killmail-intelligence page render, making the
// page time out. This endpoint decouples that work from the page render:
// the page always loads fast and the coverage panel fetches its data here.
//
// Response shapes:
//
//   HTTP 200 { ok:true, coverage:{...}, refreshed:true|false }
//     - Cache hit (warm or stale), or we successfully recomputed
//
//   HTTP 202 { ok:true, coverage:{...}, refreshed:false, refreshing:true,
//              retry_after_seconds:15 }
//     - Another request is already recomputing the snapshot. We return
//       whatever cached value we have (possibly the empty stub) so the
//       panel still renders something, and the JS is expected to poll
//       after retry_after_seconds.
//
//   HTTP 500 { ok:false, error:"..." }
//     - Unhandled exception during compute or cache I/O
//
// Query parameters:
//
//   force=1   Skip the "cache warm → return immediately" fast path and
//             attempt a refresh regardless. Still respects single-flight:
//             if another request holds the lock, returns 202.
//
// Nginx note: because the cold-path compute can take minutes, the nginx
// location serving this endpoint should have a higher proxy_read_timeout
// than the default 60s. Suggested: proxy_read_timeout 300s. See the PR
// description for the snippet.

require_once __DIR__ . '/../../../src/bootstrap.php';

header('Content-Type: application/json; charset=utf-8');
header('Cache-Control: private, no-store');

try {
    $startDate = '2024-01-01';
    $force = isset($_GET['force']) && (string) $_GET['force'] === '1';

    // 1. Fast path: cache hit and not a forced refresh → return immediately.
    $cached = killmail_esi_coverage_cache_read($startDate);
    if (!$force && ($cached['cache_status'] ?? 'empty') === 'warm') {
        echo json_encode([
            'ok' => true,
            'coverage' => $cached,
            'refreshed' => false,
        ], JSON_UNESCAPED_SLASHES);
        exit;
    }

    // 2. Stale, empty, or forced. Try to acquire the single-flight lock.
    //    If another request is holding it, return the cached value (even
    //    if stale/empty) with a refreshing hint so the JS polls.
    $fresh = killmail_esi_coverage_refresh_with_singleflight($startDate);
    if ($fresh === null) {
        http_response_code(202);
        $cached['cache_status'] = 'refreshing';
        echo json_encode([
            'ok' => true,
            'coverage' => $cached,
            'refreshed' => false,
            'refreshing' => true,
            'retry_after_seconds' => 15,
        ], JSON_UNESCAPED_SLASHES);
        exit;
    }

    // 3. We held the lock and produced a fresh snapshot.
    echo json_encode([
        'ok' => true,
        'coverage' => $fresh,
        'refreshed' => true,
    ], JSON_UNESCAPED_SLASHES);
} catch (Throwable $exception) {
    http_response_code(500);
    echo json_encode([
        'ok' => false,
        'error' => $exception->getMessage(),
    ], JSON_UNESCAPED_SLASHES);
}
