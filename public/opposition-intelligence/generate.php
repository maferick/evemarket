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

// Opposition briefings now run through the ai_jobs queue drained by
// bin/ai_briefing_worker.php (systemd: supplycore-ai-worker.service). The
// provider is picked per-feature via settings (ai_provider_opposition_global)
// — no more hard-coded runtime override here.
$error = null;
$jobId = null;
$status = 'queued';

if (!supplycore_ai_ollama_enabled()) {
    $status = 'disabled';
    $error = 'AI briefings are disabled in settings.';
} else {
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
}

$flash = [
    'status' => $status,
    'job_id' => $jobId,
    'error' => $error,
    'date' => $date,
];
if (session_status() === PHP_SESSION_NONE) {
    @session_start();
}
$_SESSION['opposition_intel_flash'] = $flash;

header('Location: /opposition-intelligence/?date=' . urlencode($date));
exit;
