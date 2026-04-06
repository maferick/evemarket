<?php

declare(strict_types=1);
require_once __DIR__ . '/../../src/bootstrap.php';

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    http_response_code(405);
    header('Allow: POST');
    exit('Method Not Allowed');
}

if (!validate_csrf($_POST['_token'] ?? null)) {
    http_response_code(419);
    exit('Invalid CSRF token');
}

$date = trim((string) ($_POST['date'] ?? ''));
if ($date === '' || preg_match('/^\d{4}-\d{2}-\d{2}$/', $date) !== 1) {
    $date = gmdate('Y-m-d');
}

$error = null;
$jobId = null;
$generated = 0;

if (!supplycore_ai_ollama_enabled()) {
    $status = 'disabled';
    $error = 'AI briefings are disabled in settings.';
} elseif (supplycore_ai_needs_queue('opposition_global')) {
    // RunPod: enqueue to ai_jobs for the systemd worker to drain.
    // Cold starts can take minutes — the web request must not wait.
    $status = 'queued';
    try {
        $jobId = supplycore_ai_jobs_enqueue(
            'opposition_global',
            $date,
            ['date' => $date]
        );
    } catch (Throwable $e) {
        error_log('[opposition-intel-generate] ' . $e->getMessage());
        $status = 'error';
        $error = $e->getMessage();
    }
} else {
    // Local Ollama / Claude / Groq: run inline on the web request.
    // These providers respond in seconds, no queue needed.
    @set_time_limit(0);
    @ini_set('max_execution_time', '0');
    ignore_user_abort(false);

    try {
        $generated = opposition_ai_generate_daily_briefings($date);
        $status = 'ok';
    } catch (Throwable $e) {
        error_log('[opposition-intel-generate] ' . $e->getMessage());
        $status = 'error';
        $error = $e->getMessage();
    }
}

$flash = [
    'status' => $status,
    'job_id' => $jobId,
    'generated' => $generated,
    'error' => $error,
    'date' => $date,
];
if (session_status() === PHP_SESSION_NONE) {
    @session_start();
}
$_SESSION['opposition_intel_flash'] = $flash;

header('Location: /opposition-intelligence/?date=' . urlencode($date));
exit;
