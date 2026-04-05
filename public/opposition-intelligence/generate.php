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

// Generation can take several minutes on a local GPU/CPU — remove PHP timeout
// and make sure the HTTP stack won't cut us off either.
@set_time_limit(0);
@ini_set('max_execution_time', '0');
ignore_user_abort(false);

// Force the AI pipeline to use local Ollama with the gemma4:e2b model at the
// default local URL, regardless of whatever provider is configured in settings.
$GLOBALS['supplycore_ai_ollama_runtime_override'] = [
    'enabled' => true,
    'provider' => 'local',
    'url' => 'http://localhost:11434/api',
    'model' => 'gemma4:e2b',
    // Allow up to ~15 minutes for a single model call (gemma on local hardware
    // can be slow, especially on first load).
    'timeout' => 900,
    'capability_override' => 'auto',
    'runpod_url' => '',
    'runpod_api_key' => '',
    'runpod_api_key_masked' => '',
    'claude_api_key' => '',
    'claude_api_key_masked' => '',
    'claude_model' => '',
    'groq_api_key' => '',
    'groq_api_key_masked' => '',
    'groq_model' => '',
    'inferred_tier' => 'small',
    'capability_tier' => 'small',
    'strategy' => supplycore_ai_capability_strategy('small'),
];

$error = null;
$generated = 0;
try {
    $generated = opposition_ai_generate_daily_briefings($date);
} catch (Throwable $e) {
    error_log('[opposition-intel-generate] ' . $e->getMessage());
    $error = $e->getMessage();
}

$status = $error !== null ? 'error' : 'ok';
$flash = [
    'status' => $status,
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
