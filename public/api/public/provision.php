<?php

declare(strict_types=1);

/**
 * Proxy Provisioning Endpoint
 *
 * Accepts a single-use provisioning token and returns the full proxy
 * configuration (endpoint, API key, secret, branding). The token is
 * consumed on success and cannot be reused.
 *
 * Usage:
 *   GET /api/public/provision.php?token=<64-char-hex>
 *
 * Responses:
 *   200  { "config": { ... } }
 *   400  { "error": "Missing token parameter." }
 *   401  { "error": "Invalid or expired provisioning token." }
 */

require_once __DIR__ . '/../../../src/bootstrap.php';
require_once __DIR__ . '/../../../src/public_api.php';

// No HMAC auth — the token itself is the credential
header('Content-Type: application/json; charset=utf-8');
header('Cache-Control: no-store');

$token = trim((string) ($_GET['token'] ?? ''));

if ($token === '') {
    public_api_respond_error(400, 'Missing token parameter.');
}

$config = public_api_provision_token_consume($token);

if ($config === null) {
    public_api_respond_error(401, 'Invalid or expired provisioning token.');
}

// Return config without caching
http_response_code(200);
echo json_encode(['config' => $config], JSON_UNESCAPED_SLASHES);
exit;
