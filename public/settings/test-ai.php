<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

header('Content-Type: application/json');

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    http_response_code(405);
    echo json_encode(['success' => false, 'error' => 'POST only']);
    exit;
}

if (!supplycore_ai_ollama_enabled()) {
    echo json_encode(['success' => false, 'error' => 'AI is not enabled or provider is not configured. Check your settings.', 'provider' => 'none']);
    exit;
}

$result = supplycore_ai_test_connection();
echo json_encode($result, JSON_UNESCAPED_SLASHES);
