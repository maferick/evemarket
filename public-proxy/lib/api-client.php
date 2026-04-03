<?php

declare(strict_types=1);

// Block known AI crawlers before any work. Mirror of src/bootstrap.php.
if (preg_match('/ClaudeBot|GPTBot|CCBot|anthropic-ai|ChatGPT-User|Bytespider|PetalBot|Google-Extended/i', $_SERVER['HTTP_USER_AGENT'] ?? '')) {
    http_response_code(403);
    header('Content-Type: text/plain');
    exit('Forbidden');
}

/**
 * SupplyCore Public API Client — HMAC-authenticated HTTP requests.
 *
 * All communication is server-to-server. The API key and secret
 * never leave this server and are never exposed to the browser.
 */

function proxy_config(): array
{
    static $config = null;
    if ($config === null) {
        // Prefer encrypted vault over plaintext config
        require_once __DIR__ . '/config-vault.php';
        $config = vault_load();

        if ($config === null) {
            // Fall back to plaintext config.php
            $path = __DIR__ . '/../config.php';
            if (!file_exists($path)) {
                http_response_code(500);
                echo 'Configuration missing. Run "php bin/configure.php" to create an encrypted vault, or copy config.example.php to config.php.';
                exit;
            }
            $config = require $path;
        }
    }
    return $config;
}

/**
 * Make an authenticated GET request to a SupplyCore public API endpoint.
 *
 * @param string $path   API path (e.g. "/api/public/theater.php")
 * @param array  $params Query parameters
 * @return array Decoded JSON response
 */
function proxy_api_get(string $path, array $params = []): array
{
    $config = proxy_config();

    $baseUrl  = rtrim((string) ($config['supplycore_url'] ?? ''), '/');
    $keyId    = (string) ($config['api_key_id'] ?? '');
    $secret   = (string) ($config['api_secret'] ?? '');
    $timeout  = (int) ($config['timeout'] ?? 15);

    if ($baseUrl === '' || $keyId === '' || $secret === '') {
        return ['error' => 'Proxy misconfigured — check config.php'];
    }

    // Build signed request
    $timestamp = (string) time();
    $method    = 'GET';

    // Sort query params deterministically
    ksort($params);
    $sortedQuery = http_build_query($params);

    // Sign: timestamp.METHOD.path.sorted_query
    $message   = implode('.', [$timestamp, $method, $path, $sortedQuery]);
    $signature = hash_hmac('sha256', $message, $secret);

    // Build full URL
    $url = $baseUrl . $path;
    if ($sortedQuery !== '') {
        $url .= '?' . $sortedQuery;
    }

    // Execute request via cURL
    $ch = curl_init();
    curl_setopt_array($ch, [
        CURLOPT_URL            => $url,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT        => $timeout,
        CURLOPT_CONNECTTIMEOUT => 5,
        CURLOPT_HTTPHEADER     => [
            'X-Api-Key: ' . $keyId,
            'X-Signature: ' . $signature,
            'X-Timestamp: ' . $timestamp,
            'Accept: application/json',
        ],
        CURLOPT_FOLLOWLOCATION => false,
        CURLOPT_SSL_VERIFYPEER => true,
    ]);

    $response = curl_exec($ch);
    $httpCode = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $curlError = curl_error($ch);
    curl_close($ch);

    if ($response === false || $curlError !== '') {
        return ['error' => 'Connection failed: ' . $curlError];
    }

    $decoded = json_decode((string) $response, true);
    if (!is_array($decoded)) {
        return ['error' => 'Invalid response from SupplyCore (HTTP ' . $httpCode . ')'];
    }

    if ($httpCode !== 200) {
        return ['error' => (string) ($decoded['error'] ?? 'API error (HTTP ' . $httpCode . ')')];
    }

    return $decoded;
}

/**
 * Format ISK values (mirrors SupplyCore's supplycore_format_isk).
 */
function proxy_format_isk(float $value): string
{
    if ($value <= 0) return '0';
    if ($value >= 1_000_000_000_000) return number_format($value / 1_000_000_000_000, 2) . 't';
    if ($value >= 1_000_000_000) return number_format($value / 1_000_000_000, 2) . 'b';
    if ($value >= 1_000_000) return number_format($value / 1_000_000, 1) . 'm';
    if ($value >= 1_000) return number_format($value / 1_000, 1) . 'k';
    return number_format($value, 0);
}

/**
 * Safe HTML output helper.
 */
function proxy_e(string $value): string
{
    return htmlspecialchars($value, ENT_QUOTES, 'UTF-8');
}
