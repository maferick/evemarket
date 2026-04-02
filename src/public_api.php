<?php

declare(strict_types=1);

/**
 * Public API authentication — HMAC-based request signing.
 *
 * Security model:
 *   - Each integration gets a key_id + shared secret (stored in app_settings)
 *   - Requests include: X-Api-Key, X-Signature, X-Timestamp headers
 *   - Signature = HMAC-SHA256(timestamp.method.path.sorted_query, secret)
 *   - Timestamp must be within ±300 seconds to prevent replay attacks
 *   - Optional IP allowlisting per key
 */

/**
 * Validate the incoming request against HMAC authentication.
 * Halts with 401/403 JSON on failure.
 */
function public_api_authenticate(): array
{
    $keyId     = trim((string) ($_SERVER['HTTP_X_API_KEY'] ?? ''));
    $signature = trim((string) ($_SERVER['HTTP_X_SIGNATURE'] ?? ''));
    $timestamp = trim((string) ($_SERVER['HTTP_X_TIMESTAMP'] ?? ''));

    if ($keyId === '' || $signature === '' || $timestamp === '') {
        public_api_respond_error(401, 'Missing authentication headers (X-Api-Key, X-Signature, X-Timestamp).');
    }

    // Timestamp replay window: 300 seconds (5 minutes)
    $ts = (int) $timestamp;
    $now = time();
    if (abs($now - $ts) > 300) {
        public_api_respond_error(401, 'Request timestamp expired or too far in the future.');
    }

    // Load registered keys from app_settings
    $keys = public_api_keys_load();
    if (!isset($keys[$keyId])) {
        public_api_respond_error(401, 'Unknown API key.');
    }

    $keyEntry = $keys[$keyId];
    $secret = (string) ($keyEntry['secret'] ?? '');
    if ($secret === '') {
        public_api_respond_error(500, 'API key misconfigured.');
    }

    // IP allowlist check (empty = allow all)
    $allowedIps = (array) ($keyEntry['allowed_ips'] ?? []);
    if ($allowedIps !== []) {
        $clientIp = (string) ($_SERVER['REMOTE_ADDR'] ?? '');
        if (!in_array($clientIp, $allowedIps, true)) {
            public_api_respond_error(403, 'IP address not allowed.');
        }
    }

    // Rebuild the signed message: timestamp.METHOD.path.sorted_query
    $method = strtoupper((string) ($_SERVER['REQUEST_METHOD'] ?? 'GET'));
    $path   = strtok((string) ($_SERVER['REQUEST_URI'] ?? '/'), '?');

    // Sort query parameters deterministically (exclude signature headers)
    $queryParams = $_GET;
    ksort($queryParams);
    $sortedQuery = http_build_query($queryParams);

    $message = implode('.', [$timestamp, $method, $path, $sortedQuery]);
    $expectedSignature = hash_hmac('sha256', $message, $secret);

    if (!hash_equals($expectedSignature, $signature)) {
        public_api_respond_error(401, 'Invalid signature.');
    }

    return [
        'key_id' => $keyId,
        'label'  => (string) ($keyEntry['label'] ?? ''),
    ];
}

/**
 * Load public API keys from app_settings table.
 * Format: { "key_id": { "secret": "...", "label": "...", "allowed_ips": [] } }
 */
function public_api_keys_load(): array
{
    $raw = db_select_one(
        "SELECT setting_value FROM app_settings WHERE setting_key = 'public_api_keys' LIMIT 1"
    );
    if ($raw === null) {
        return [];
    }
    $decoded = json_decode((string) ($raw['setting_value'] ?? ''), true);
    return is_array($decoded) ? $decoded : [];
}

/**
 * Generate a new API key pair and persist it.
 * Returns ['key_id' => '...', 'secret' => '...'].
 */
function public_api_key_generate(string $label, array $allowedIps = []): array
{
    $keyId  = 'pk_' . bin2hex(random_bytes(8));
    $secret = bin2hex(random_bytes(32));

    $keys = public_api_keys_load();
    $keys[$keyId] = [
        'secret'      => $secret,
        'label'       => $label,
        'created_at'  => date('Y-m-d H:i:s'),
        'allowed_ips' => $allowedIps,
    ];

    $json = json_encode($keys, JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);

    // Upsert into app_settings
    $existing = db_select_one("SELECT 1 FROM app_settings WHERE setting_key = 'public_api_keys' LIMIT 1");
    if ($existing !== null) {
        db_execute("UPDATE app_settings SET setting_value = ? WHERE setting_key = 'public_api_keys'", [$json]);
    } else {
        db_execute("INSERT INTO app_settings (setting_key, setting_value) VALUES ('public_api_keys', ?)", [$json]);
    }

    return ['key_id' => $keyId, 'secret' => $secret];
}

/**
 * Revoke an API key by key_id.
 */
function public_api_key_revoke(string $keyId): bool
{
    $keys = public_api_keys_load();
    if (!isset($keys[$keyId])) {
        return false;
    }
    unset($keys[$keyId]);
    $json = json_encode($keys, JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
    db_execute("UPDATE app_settings SET setting_value = ? WHERE setting_key = 'public_api_keys'", [$json]);
    return true;
}

/**
 * Send a JSON error response and halt.
 */
function public_api_respond_error(int $httpCode, string $message): never
{
    http_response_code($httpCode);
    header('Content-Type: application/json; charset=utf-8');
    echo json_encode(['error' => $message]);
    exit;
}

/**
 * Send a JSON success response and halt.
 */
function public_api_respond(array $data, int $cacheSeconds = 60): never
{
    http_response_code(200);
    header('Content-Type: application/json; charset=utf-8');
    header('Cache-Control: private, max-age=' . $cacheSeconds);
    echo json_encode($data, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);
    exit;
}
