<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

header('Content-Type: application/json; charset=UTF-8');
header('Cache-Control: no-store, no-cache, must-revalidate, max-age=0');

// ── GET: Return current job status for the log viewer ────────────────────────
// This replaces polling from the frontend — the log viewer SSE stream uses this
// as its data source, and jobs push updates by writing to the database which
// triggers version bumps via the existing ui_refresh_events mechanism.

if ($_SERVER['REQUEST_METHOD'] === 'GET') {
    $pageData = log_viewer_page_data();
    $externalHealth = log_viewer_external_health();

    echo json_encode([
        'kpi' => $pageData['kpi'],
        'jobs' => array_map(fn (array $j) => [
            'job_key' => $j['job_key'],
            'label' => $j['label'],
            'health' => $j['health'],
            'health_label' => $j['health_label'],
            'overdue' => $j['overdue'],
            'pressure_state' => $j['pressure_state'],
            'last_run_relative' => $j['last_run_relative'],
            'last_success_relative' => $j['last_success_relative'],
            'last_failure_message' => $j['last_failure_message'],
            'interval_seconds' => $j['interval_seconds'],
            'enabled' => $j['enabled'],
        ], $pageData['jobs']),
        'stuck_runs' => $pageData['stuck_runs'],
        'failed_runs' => $pageData['failed_runs'],
        'external' => $externalHealth,
        'generated_at' => gmdate(DATE_ATOM),
    ], JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);
    exit;
}

// ── POST: Accept job status push from Python workers ─────────────────────────
// Python jobs can POST status updates directly instead of relying on polling.
// This writes to the same tables that _finalize_job() uses, so the SSE stream
// picks up changes automatically.

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $body = json_decode((string) file_get_contents('php://input'), true);
    if (!is_array($body) || empty($body['job_key']) || empty($body['status'])) {
        http_response_code(400);
        echo json_encode(['error' => 'Missing required fields: job_key, status']);
        exit;
    }

    $jobKey = trim((string) $body['job_key']);
    $status = trim((string) $body['status']);
    $eventType = $status === 'failed' ? 'failure' : ($status === 'running' ? 'started' : 'finished');
    $errorText = $status === 'failed' ? trim((string) ($body['error'] ?? '')) : null;

    // Validate status
    $validStatuses = ['running', 'success', 'failed', 'skipped', 'delayed'];
    if (!in_array($status, $validStatuses, true)) {
        http_response_code(400);
        echo json_encode(['error' => 'Invalid status. Must be one of: ' . implode(', ', $validStatuses)]);
        exit;
    }

    // Update the scheduler_job_current_status table (same as Python _finalize_job)
    try {
        db_execute(
            "INSERT INTO scheduler_job_current_status (job_key, latest_status, latest_event_type, last_failure_message, updated_at)
             VALUES (?, ?, ?, ?, NOW())
             ON DUPLICATE KEY UPDATE
                latest_status = VALUES(latest_status),
                latest_event_type = VALUES(latest_event_type),
                last_failure_message = IF(VALUES(latest_status) = 'failed', VALUES(last_failure_message), last_failure_message),
                last_started_at = IF(VALUES(latest_event_type) = 'started', NOW(), last_started_at),
                last_finished_at = IF(VALUES(latest_event_type) IN ('finished', 'failure'), NOW(), last_finished_at),
                last_success_at = IF(VALUES(latest_status) = 'success', NOW(), last_success_at),
                last_failure_at = IF(VALUES(latest_status) = 'failed', NOW(), last_failure_at),
                updated_at = NOW()",
            [$jobKey, $status, $eventType, $errorText]
        );
    } catch (Throwable $e) {
        http_response_code(500);
        echo json_encode(['error' => 'Database update failed']);
        exit;
    }

    // Bump the log_viewer version so SSE picks it up
    try {
        db_execute(
            "INSERT INTO ui_refresh_events (job_key, job_status, domains_json, ui_sections_json, created_at)
             VALUES (?, ?, ?, ?, NOW())",
            [$jobKey, $status, '["scheduler"]', '["log-viewer-kpi","log-viewer-jobs","log-viewer-runs"]']
        );
        db_execute(
            "INSERT INTO ui_refresh_section_versions (version_key, version, job_key, job_status, event_id, updated_at)
             VALUES ('log_viewer_version', 1, ?, ?, LAST_INSERT_ID(), NOW())
             ON DUPLICATE KEY UPDATE
                version = version + 1,
                job_key = VALUES(job_key),
                job_status = VALUES(job_status),
                event_id = VALUES(event_id),
                updated_at = NOW()",
            [$jobKey, $status]
        );
    } catch (Throwable $e) {
        // Non-fatal: the status was already written to scheduler_job_current_status
    }

    echo json_encode(['ok' => true, 'job_key' => $jobKey, 'status' => $status]);
    exit;
}

http_response_code(405);
echo json_encode(['error' => 'Method not allowed']);
