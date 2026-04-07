<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    http_response_code(405);
    header('Content-Type: application/json; charset=utf-8');
    echo json_encode(['error' => 'Method not allowed']);
    exit;
}

$body = json_decode((string) file_get_contents('php://input'), true);
if (!is_array($body)) {
    $body = $_POST;
}

$action = (string) ($body['action'] ?? '');

$result = match ($action) {
    'toggle_compound' => (function () use ($body): array {
        $type = (string) ($body['compound_type'] ?? '');
        $enabled = (bool) ($body['enabled'] ?? false);
        return ['success' => db_compound_toggle_enabled($type, $enabled), 'compound_type' => $type, 'enabled' => $enabled];
    })(),

    'set_override' => (function () use ($body): array {
        $key = (string) ($body['key'] ?? '');
        $value = (string) ($body['value'] ?? '');
        return ['success' => db_calibration_override_set($key, $value), 'key' => $key];
    })(),

    'freeze_calibration' => (function () use ($body): array {
        $frozen = (bool) ($body['frozen'] ?? false);
        return ['success' => db_calibration_override_set('cip_calibration_frozen', $frozen ? '1' : '0'), 'frozen' => $frozen];
    })(),

    'get_health' => (function (): array {
        return ['success' => true, 'health' => db_cip_operational_health()];
    })(),

    'get_calibration' => (function (): array {
        return ['success' => true, 'calibration' => db_intelligence_calibration_latest(), 'overrides' => db_calibration_overrides_get()];
    })(),

    default => ['success' => false, 'error' => 'Unknown action: ' . $action],
};

$accept = (string) ($_SERVER['HTTP_ACCEPT'] ?? '');
if (str_contains($accept, 'text/html')) {
    header('Location: /intelligence-events/admin.php');
    exit;
}

header('Content-Type: application/json; charset=utf-8');
echo json_encode($result);
