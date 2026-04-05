<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    http_response_code(405);
    header('Content-Type: application/json; charset=utf-8');
    echo json_encode(['error' => 'Method not allowed']);
    exit;
}

$allianceId = (int) ($_POST['alliance_id'] ?? $_GET['alliance_id'] ?? 0);
if ($allianceId <= 0) {
    // Try JSON body
    $body = json_decode(file_get_contents('php://input'), true);
    $allianceId = (int) ($body['alliance_id'] ?? 0);
}

if ($allianceId <= 0) {
    http_response_code(400);
    header('Content-Type: application/json; charset=utf-8');
    echo json_encode(['error' => 'alliance_id is required']);
    exit;
}

$nowTracked = db_opposition_intel_toggle_track($allianceId);

// If the request came from a form (not AJAX), redirect back
$accept = (string) ($_SERVER['HTTP_ACCEPT'] ?? '');
if (str_contains($accept, 'text/html') || isset($_POST['alliance_id'])) {
    header('Location: /alliance-dossiers/view.php?alliance_id=' . $allianceId);
    exit;
}

header('Content-Type: application/json; charset=utf-8');
echo json_encode([
    'alliance_id' => $allianceId,
    'tracked' => $nowTracked,
    'message' => $nowTracked ? 'Alliance is now tracked for daily AI intel.' : 'Alliance tracking removed.',
]);
