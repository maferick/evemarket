<?php

declare(strict_types=1);

/**
 * SupplyCore Public Proxy — Standalone ESI SSO client.
 *
 * This is a minimal OAuth 2.0 client for the EVE Online Single Sign-On
 * endpoint at login.eveonline.com. It is *separate* from the ESI flow
 * used inside SupplyCore core — the proxy has its own client_id /
 * client_secret because it only needs to identify the visiting pilot,
 * not operate corp scopes.
 *
 * Configure with (proxy config or PROXY_ESI_* environment variables):
 *   esi_client_id
 *   esi_client_secret
 *   esi_callback_url   e.g. https://br.example.com/callback
 *
 * Scopes used: "publicData" only. Nothing sensitive is requested.
 */

require_once __DIR__ . '/api-client.php';

const PROXY_ESI_AUTH_URL  = 'https://login.eveonline.com/v2/oauth/authorize/';
const PROXY_ESI_TOKEN_URL = 'https://login.eveonline.com/v2/oauth/token';
const PROXY_ESI_VERIFY    = 'https://login.eveonline.com/oauth/verify';
const PROXY_ESI_SCOPES    = 'publicData';

function proxy_esi_config(): array
{
    $config = proxy_config();
    $clientId    = trim((string) ($config['esi_client_id']     ?? ''));
    $secret      = trim((string) ($config['esi_client_secret'] ?? ''));
    $callbackUrl = trim((string) ($config['esi_callback_url']  ?? ''));

    if ($clientId === '' || $secret === '' || $callbackUrl === '') {
        throw new RuntimeException('ESI SSO not configured. Set esi_client_id, esi_client_secret, and esi_callback_url in the proxy config.');
    }

    return [
        'client_id'     => $clientId,
        'client_secret' => $secret,
        'callback_url'  => $callbackUrl,
    ];
}

/**
 * Build the SSO authorize URL. Caller should pass the state
 * previously returned from proxy_oauth_state_create().
 */
function proxy_esi_authorize_url(string $state): string
{
    $cfg = proxy_esi_config();

    $query = http_build_query([
        'response_type' => 'code',
        'redirect_uri'  => $cfg['callback_url'],
        'client_id'     => $cfg['client_id'],
        'scope'         => PROXY_ESI_SCOPES,
        'state'         => $state,
    ]);

    return PROXY_ESI_AUTH_URL . '?' . $query;
}

/**
 * Exchange an authorization code for a token payload and verify it.
 * Returns ['token' => [...], 'verify' => [...]].
 */
function proxy_esi_exchange_code(string $code): array
{
    $cfg = proxy_esi_config();

    // Token request
    $ch = curl_init(PROXY_ESI_TOKEN_URL);
    curl_setopt_array($ch, [
        CURLOPT_POST           => true,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT        => 15,
        CURLOPT_CONNECTTIMEOUT => 5,
        CURLOPT_HTTPHEADER     => [
            'Authorization: Basic ' . base64_encode($cfg['client_id'] . ':' . $cfg['client_secret']),
            'Content-Type: application/x-www-form-urlencoded',
            'Accept: application/json',
            'Host: login.eveonline.com',
        ],
        CURLOPT_POSTFIELDS     => http_build_query([
            'grant_type' => 'authorization_code',
            'code'       => $code,
        ]),
        CURLOPT_SSL_VERIFYPEER => true,
    ]);
    $tokenBody = curl_exec($ch);
    $tokenHttp = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $tokenErr  = curl_error($ch);
    curl_close($ch);

    if ($tokenBody === false || $tokenErr !== '') {
        throw new RuntimeException('ESI token request failed: ' . $tokenErr);
    }

    $token = json_decode((string) $tokenBody, true);
    if (!is_array($token) || $tokenHttp >= 400) {
        throw new RuntimeException('ESI token exchange failed (HTTP ' . $tokenHttp . ').');
    }

    $accessToken = (string) ($token['access_token'] ?? '');
    if ($accessToken === '') {
        throw new RuntimeException('ESI token exchange returned no access_token.');
    }

    // Verify call
    $ch = curl_init(PROXY_ESI_VERIFY);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT        => 15,
        CURLOPT_CONNECTTIMEOUT => 5,
        CURLOPT_HTTPHEADER     => [
            'Authorization: Bearer ' . $accessToken,
            'Accept: application/json',
            'Host: login.eveonline.com',
        ],
        CURLOPT_SSL_VERIFYPEER => true,
    ]);
    $verifyBody = curl_exec($ch);
    $verifyHttp = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $verifyErr  = curl_error($ch);
    curl_close($ch);

    if ($verifyBody === false || $verifyErr !== '' || $verifyHttp >= 400) {
        throw new RuntimeException('ESI verify call failed (HTTP ' . $verifyHttp . ').');
    }

    $verify = json_decode((string) $verifyBody, true);
    if (!is_array($verify) || !isset($verify['CharacterID'])) {
        throw new RuntimeException('ESI verify response did not include CharacterID.');
    }

    return [
        'token'  => $token,
        'verify' => $verify,
    ];
}

/**
 * Look up corp/alliance for a character via the public ESI endpoint.
 * This is *unauthenticated* — ESI accepts anonymous GETs for public data.
 * Used to enrich the profile at login time.
 */
function proxy_esi_fetch_public_character(int $characterId): array
{
    if ($characterId <= 0) {
        return [];
    }

    $url = 'https://esi.evetech.net/latest/characters/' . $characterId . '/?datasource=tranquility';
    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT        => 10,
        CURLOPT_CONNECTTIMEOUT => 5,
        CURLOPT_HTTPHEADER     => ['Accept: application/json', 'User-Agent: SupplyCore-Proxy/1.0'],
        CURLOPT_SSL_VERIFYPEER => true,
    ]);
    $body = curl_exec($ch);
    $httpCode = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);

    if ($body === false || $httpCode >= 400) {
        return [];
    }

    $decoded = json_decode((string) $body, true);
    return is_array($decoded) ? $decoded : [];
}
