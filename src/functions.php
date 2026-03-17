<?php

declare(strict_types=1);

require_once __DIR__ . '/db.php';

date_default_timezone_set(config('app.timezone', 'UTC'));

function app_name(): string
{
    return (string) config('app.name', 'EveMarket');
}

function current_path(): string
{
    $uri = parse_url($_SERVER['REQUEST_URI'] ?? '/', PHP_URL_PATH);

    return $uri ?: '/';
}

function nav_items(): array
{
    return [
        [
            'label' => 'Dashboard',
            'path' => '/',
            'icon' => '📈',
            'children' => [],
        ],
        [
            'label' => 'Settings',
            'path' => '/settings',
            'icon' => '⚙️',
            'children' => [
                ['label' => 'General', 'path' => '/settings?section=general'],
                ['label' => 'Trading Stations', 'path' => '/settings?section=trading-stations'],
                ['label' => 'ESI Login', 'path' => '/settings?section=esi-login'],
                ['label' => 'Data Sync', 'path' => '/settings?section=data-sync'],
            ],
        ],
    ];
}

function setting_sections(): array
{
    return [
        'general' => ['title' => 'General Settings', 'description' => 'Core application behavior and preferences.'],
        'trading-stations' => ['title' => 'Trading Stations', 'description' => 'Select market and alliance station priorities.'],
        'esi-login' => ['title' => 'ESI Login', 'description' => 'Configure EVE SSO credentials and callback behavior.'],
        'data-sync' => ['title' => 'Data Sync', 'description' => 'Control database import and incremental update policies.'],
    ];
}

function active_section(): string
{
    $section = $_GET['section'] ?? 'general';

    return array_key_exists($section, setting_sections()) ? $section : 'general';
}

function get_settings(array $keys = []): array
{
    try {
        if ($keys === []) {
            $rows = db_select('SELECT setting_key, setting_value FROM app_settings');
        } else {
            $placeholders = implode(',', array_fill(0, count($keys), '?'));
            $rows = db_select("SELECT setting_key, setting_value FROM app_settings WHERE setting_key IN ($placeholders)", $keys);
        }
    } catch (Throwable) {
        return [];
    }

    $settings = [];
    foreach ($rows as $row) {
        $settings[$row['setting_key']] = $row['setting_value'];
    }

    return $settings;
}

function get_setting(string $key, mixed $default = null): mixed
{
    try {
        $row = db_select_one('SELECT setting_value FROM app_settings WHERE setting_key = ?', [$key]);
    } catch (Throwable) {
        return $default;
    }

    return $row['setting_value'] ?? $default;
}

function save_settings(array $settings): bool
{
    try {
        db_transaction(function () use ($settings): void {
            foreach ($settings as $key => $value) {
                db_execute(
                    'INSERT INTO app_settings (setting_key, setting_value) VALUES (?, ?) ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value), updated_at = CURRENT_TIMESTAMP',
                    [$key, (string) $value]
                );
            }
        });
    } catch (Throwable) {
        return false;
    }

    return true;
}

function station_options(): array
{
    try {
        return db_select('SELECT id, station_name, station_type FROM trading_stations ORDER BY station_name ASC');
    } catch (Throwable) {
        return [
            ['id' => 1, 'station_name' => 'Jita IV - Moon 4 - Caldari Navy Assembly Plant', 'station_type' => 'market'],
            ['id' => 2, 'station_name' => 'Amarr VIII (Oris) - Emperor Family Academy', 'station_type' => 'market'],
            ['id' => 3, 'station_name' => '1DQ1-A Keepstar', 'station_type' => 'alliance'],
            ['id' => 4, 'station_name' => 'T5ZI-S Fortizar', 'station_type' => 'alliance'],
        ];
    }
}

function grouped_station_options(): array
{
    $grouped = ['market' => [], 'alliance' => []];

    foreach (station_options() as $station) {
        $grouped[$station['station_type']][] = $station;
    }

    return $grouped;
}

function csrf_token(): string
{
    if (!isset($_SESSION['csrf_token'])) {
        $_SESSION['csrf_token'] = bin2hex(random_bytes(32));
    }

    return $_SESSION['csrf_token'];
}

function validate_csrf(?string $token): bool
{
    return is_string($token) && hash_equals($_SESSION['csrf_token'] ?? '', $token);
}

function flash(string $key, ?string $message = null): ?string
{
    if ($message !== null) {
        $_SESSION['flash'][$key] = $message;

        return null;
    }

    $value = $_SESSION['flash'][$key] ?? null;
    unset($_SESSION['flash'][$key]);

    return $value;
}

function base_url(string $path = ''): string
{
    return rtrim((string) config('app.base_url', ''), '/') . $path;
}


function esi_default_scopes(): array
{
    return [
        'publicData',
        'esi-location.read_location.v1',
        'esi-universe.read_structures.v1',
        'esi-markets.structure_markets.v1',
    ];
}

function esi_scopes_string(): string
{
    $stored = trim((string) get_setting('esi_scopes', ''));

    return $stored !== '' ? $stored : implode(' ', esi_default_scopes());
}

function esi_scope_list(): array
{
    $scopes = preg_split('/\s+/', trim(esi_scopes_string())) ?: [];

    return array_values(array_filter(array_unique($scopes), static fn ($scope) => $scope !== ''));
}

function esi_sso_authorize_url(): string
{
    $state = bin2hex(random_bytes(24));
    $_SESSION['esi_oauth_state'] = $state;

    $query = http_build_query([
        'response_type' => 'code',
        'redirect_uri' => (string) get_setting('esi_callback_url', base_url('/callback')),
        'client_id' => (string) get_setting('esi_client_id', ''),
        'scope' => implode(' ', esi_scope_list()),
        'state' => $state,
    ]);

    return 'https://login.eveonline.com/v2/oauth/authorize/?' . $query;
}

function http_post_form(string $url, array $headers, array $formData): array
{
    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_POST => true,
        CURLOPT_POSTFIELDS => http_build_query($formData),
        CURLOPT_HTTPHEADER => $headers,
        CURLOPT_TIMEOUT => 25,
    ]);

    $response = curl_exec($ch);
    $status = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $error = curl_error($ch);
    curl_close($ch);

    if ($response === false) {
        throw new RuntimeException('HTTP request failed: ' . $error);
    }

    $decoded = json_decode($response, true);
    if (!is_array($decoded)) {
        throw new RuntimeException('Invalid JSON response from ' . $url);
    }

    return ['status' => $status, 'json' => $decoded];
}

function http_get_json(string $url, array $headers = []): array
{
    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_HTTPHEADER => $headers,
        CURLOPT_TIMEOUT => 25,
    ]);

    $response = curl_exec($ch);
    $status = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $error = curl_error($ch);
    curl_close($ch);

    if ($response === false) {
        throw new RuntimeException('HTTP request failed: ' . $error);
    }

    $decoded = json_decode($response, true);
    if (!is_array($decoded)) {
        throw new RuntimeException('Invalid JSON response from ' . $url);
    }

    return ['status' => $status, 'json' => $decoded];
}

function esi_exchange_oauth_code(string $code): array
{
    $clientId = trim((string) get_setting('esi_client_id', ''));
    $clientSecret = trim((string) get_setting('esi_client_secret', ''));
    $redirectUri = trim((string) get_setting('esi_callback_url', base_url('/callback')));

    if ($clientId === '' || $clientSecret === '') {
        throw new RuntimeException('Missing ESI client credentials. Save them in Settings > ESI Login first.');
    }

    $tokenResponse = http_post_form(
        'https://login.eveonline.com/v2/oauth/token',
        [
            'Authorization: Basic ' . base64_encode($clientId . ':' . $clientSecret),
            'Content-Type: application/x-www-form-urlencoded',
            'Accept: application/json',
            'Host: login.eveonline.com',
        ],
        [
            'grant_type' => 'authorization_code',
            'code' => $code,
            'redirect_uri' => $redirectUri,
        ]
    );

    if (($tokenResponse['status'] ?? 500) >= 400) {
        throw new RuntimeException('ESI token exchange failed.');
    }

    $token = $tokenResponse['json'];
    $verifyResponse = http_get_json(
        'https://login.eveonline.com/oauth/verify',
        [
            'Authorization: Bearer ' . ($token['access_token'] ?? ''),
            'Accept: application/json',
            'Host: login.eveonline.com',
        ]
    );

    if (($verifyResponse['status'] ?? 500) >= 400) {
        throw new RuntimeException('ESI token verification failed.');
    }

    return [
        'token' => $token,
        'verify' => $verifyResponse['json'],
    ];
}

function esi_store_oauth_and_cache(array $tokenPayload, array $verifyPayload): void
{
    $expiresIn = (int) ($tokenPayload['expires_in'] ?? 0);
    $expiresAt = (new DateTimeImmutable('now', new DateTimeZone('UTC')))->modify('+' . max(0, $expiresIn) . ' seconds')->format('Y-m-d H:i:s');
    $scopes = isset($tokenPayload['scope']) ? (string) $tokenPayload['scope'] : esi_scopes_string();

    db_transaction(function () use ($tokenPayload, $verifyPayload, $expiresAt, $scopes): void {
        db_upsert_esi_oauth_token([
            'character_id' => (int) ($verifyPayload['CharacterID'] ?? 0),
            'character_name' => (string) ($verifyPayload['CharacterName'] ?? 'Unknown'),
            'owner_hash' => (string) ($verifyPayload['CharacterOwnerHash'] ?? ''),
            'access_token' => (string) ($tokenPayload['access_token'] ?? ''),
            'refresh_token' => (string) ($tokenPayload['refresh_token'] ?? ''),
            'token_type' => (string) ($tokenPayload['token_type'] ?? 'Bearer'),
            'scopes' => $scopes,
            'expires_at' => $expiresAt,
        ]);

        db_esi_cache_put('cache.esi.oauth.verify', 'latest', json_encode($verifyPayload, JSON_THROW_ON_ERROR));
        db_esi_cache_put('cache.esi.oauth.token', 'latest', json_encode([
            'token_type' => $tokenPayload['token_type'] ?? 'Bearer',
            'expires_in' => $tokenPayload['expires_in'] ?? null,
            'scope' => $scopes,
        ], JSON_THROW_ON_ERROR), null, $expiresAt);
    });
}
