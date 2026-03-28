<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

header('Content-Type: application/json; charset=utf-8');

$familyId = (int) ($_GET['family_id'] ?? 0);

if ($familyId <= 0) {
    http_response_code(422);
    echo json_encode(['error' => 'Invalid family_id.', 'modules' => []], JSON_THROW_ON_ERROR);
    exit;
}

try {
    $modules = db_hostile_fit_family_modules($familyId);
    echo json_encode(['modules' => $modules], JSON_THROW_ON_ERROR);
} catch (Throwable $e) {
    http_response_code(500);
    echo json_encode(['error' => 'Failed to load modules.', 'modules' => []], JSON_THROW_ON_ERROR);
}
