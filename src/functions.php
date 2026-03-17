<?php

declare(strict_types=1);

require_once __DIR__ . '/db.php';

date_default_timezone_set(app_timezone());

function app_name(): string
{
    $configured = trim((string) get_setting('app_name', config('app.name', 'EveMarket')));

    return sanitize_app_name($configured);
}

function app_timezone(): string
{
    $timezone = trim((string) get_setting('app_timezone', config('app.timezone', 'UTC')));

    return sanitize_timezone($timezone);
}

function default_currency(): string
{
    $currency = strtoupper(trim((string) get_setting('default_currency', 'ISK')));

    return sanitize_currency($currency);
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
        return db_trading_station_options();
    } catch (Throwable) {
        return [
            ['id' => 1, 'station_name' => 'Jita IV - Moon 4 - Caldari Navy Assembly Plant', 'station_type' => 'market'],
            ['id' => 2, 'station_name' => 'Amarr VIII (Oris) - Emperor Family Academy', 'station_type' => 'market'],
            ['id' => 3, 'station_name' => '1DQ1-A Keepstar', 'station_type' => 'alliance'],
            ['id' => 4, 'station_name' => 'T5ZI-S Fortizar', 'station_type' => 'alliance'],
        ];
    }
}

function sanitize_station_selection(?string $value, string $stationType): string
{
    return $stationType === 'alliance'
        ? sanitize_alliance_station_selection($value)
        : sanitize_market_station_selection($value);
}


function sanitize_app_name(string $name): string
{
    $name = trim($name);

    if ($name === '') {
        return 'EveMarket';
    }

    return mb_substr($name, 0, 120);
}

function sanitize_timezone(string $timezone): string
{
    $timezone = trim($timezone);

    if ($timezone === '') {
        return 'UTC';
    }

    return in_array($timezone, timezone_identifiers_list(), true) ? $timezone : 'UTC';
}

function sanitize_currency(string $currency): string
{
    $currency = strtoupper(trim($currency));

    if ($currency === '' || !preg_match('/^[A-Z]{2,8}$/', $currency)) {
        return 'ISK';
    }

    return $currency;
}

function sanitize_market_station_selection(?string $value): string
{
    $stationId = (int) trim((string) $value);

    if ($stationId <= 0) {
        return '';
    }

    try {
        $station = db_trading_station_by_id($stationId, 'market');
    } catch (Throwable) {
        return '';
    }

    return $station === null ? '' : (string) $station['id'];
}

function sanitize_alliance_station_selection(?string $value): string
{
    $structureIdValue = trim((string) $value);
    if ($structureIdValue === '' || !preg_match('/^[1-9][0-9]{9,19}$/', $structureIdValue)) {
        return '';
    }

    $structureId = (int) $structureIdValue;
    if ($structureId <= 0) {
        return '';
    }

    $context = esi_lookup_context(['esi-universe.read_structures.v1']);
    if (!($context['ok'] ?? false)) {
        return '';
    }

    try {
        $metadata = esi_alliance_structure_metadata($structureId, $context['token']);
    } catch (Throwable) {
        return '';
    }

    if ($metadata === null) {
        return '';
    }

    db_alliance_structure_metadata_upsert(
        $structureId,
        $metadata['name'] ?? null,
        gmdate('Y-m-d H:i:s')
    );

    return (string) $structureId;
}

function selected_station_name(string $settingKey): ?string
{
    $stationType = $settingKey === 'alliance_station_id' ? 'alliance' : 'market';
    $stationId = (int) get_setting($settingKey, '');

    if ($stationId <= 0) {
        return null;
    }

    if ($stationType === 'alliance') {
        try {
            $metadata = db_alliance_structure_metadata_get($stationId);
        } catch (Throwable) {
            $metadata = null;
        }

        $name = trim((string) ($metadata['structure_name'] ?? ''));

        return $name !== '' ? $name : 'Structure #' . $stationId;
    }

    try {
        $station = db_trading_station_by_id($stationId, $stationType);
    } catch (Throwable) {
        return null;
    }

    return $station['station_name'] ?? null;
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

function incremental_strategy_options(): array
{
    return [
        'watermark_upsert' => 'Watermark + Upsert',
        'full_refresh' => 'Full Refresh',
    ];
}

function incremental_delete_policy_options(): array
{
    return [
        'none' => 'No delete handling',
        'soft_delete' => 'Soft delete when removed at source',
        'reconcile' => 'Periodic reconciliation scan',
    ];
}

function sanitize_incremental_strategy(?string $value): string
{
    $strategy = trim((string) $value);

    return array_key_exists($strategy, incremental_strategy_options()) ? $strategy : 'watermark_upsert';
}

function sanitize_incremental_delete_policy(?string $value): string
{
    $policy = trim((string) $value);

    return array_key_exists($policy, incremental_delete_policy_options()) ? $policy : 'reconcile';
}

function sanitize_incremental_chunk_size(mixed $value): string
{
    $chunk = max(100, min(10000, (int) $value));

    return (string) $chunk;
}

function sanitize_sync_interval_minutes(mixed $value, int $defaultMinutes): string
{
    $interval = (int) $value;
    if ($interval <= 0) {
        $interval = $defaultMinutes;
    }

    $interval = max(1, min(1440, $interval));

    return (string) $interval;
}

function sanitize_pipeline_enabled(mixed $value): string
{
    return $value === '1' || $value === 1 || $value === true || $value === 'on' ? '1' : '0';
}

function sanitize_backfill_start_date(mixed $value): string
{
    $raw = trim((string) $value);
    if ($raw === '') {
        return '';
    }

    $date = DateTimeImmutable::createFromFormat('Y-m-d', $raw, new DateTimeZone('UTC'));

    if (!$date instanceof DateTimeImmutable || $date->format('Y-m-d') !== $raw) {
        return '';
    }

    return $raw;
}

function sanitize_retention_days(mixed $value, int $defaultDays = 30): string
{
    $days = (int) $value;
    if ($days <= 0) {
        $days = $defaultDays;
    }

    $days = max(1, min(3650, $days));

    return (string) $days;
}

function data_sync_settings_from_request(array $request): array
{
    return [
        'incremental_updates_enabled' => isset($request['incremental_updates_enabled']) ? '1' : '0',
        'incremental_strategy' => sanitize_incremental_strategy($request['incremental_strategy'] ?? null),
        'incremental_delete_policy' => sanitize_incremental_delete_policy($request['incremental_delete_policy'] ?? null),
        'incremental_chunk_size' => sanitize_incremental_chunk_size($request['incremental_chunk_size'] ?? null),
        'alliance_current_sync_interval_minutes' => sanitize_sync_interval_minutes($request['alliance_current_sync_interval_minutes'] ?? null, 5),
        'alliance_history_sync_interval_minutes' => sanitize_sync_interval_minutes($request['alliance_history_sync_interval_minutes'] ?? null, 60),
        'hub_history_sync_interval_minutes' => sanitize_sync_interval_minutes($request['hub_history_sync_interval_minutes'] ?? null, 15),
        'alliance_current_pipeline_enabled' => sanitize_pipeline_enabled($request['alliance_current_pipeline_enabled'] ?? null),
        'alliance_history_pipeline_enabled' => sanitize_pipeline_enabled($request['alliance_history_pipeline_enabled'] ?? null),
        'hub_history_pipeline_enabled' => sanitize_pipeline_enabled($request['hub_history_pipeline_enabled'] ?? null),
        'alliance_current_backfill_start_date' => sanitize_backfill_start_date($request['alliance_current_backfill_start_date'] ?? null),
        'alliance_history_backfill_start_date' => sanitize_backfill_start_date($request['alliance_history_backfill_start_date'] ?? null),
        'hub_history_backfill_start_date' => sanitize_backfill_start_date($request['hub_history_backfill_start_date'] ?? null),
        'raw_order_snapshot_retention_days' => sanitize_retention_days($request['raw_order_snapshot_retention_days'] ?? null, 30),
    ];
}

function sync_watermark(string $datasetKey): ?string
{
    $state = db_sync_state_get($datasetKey);

    if ($state === null) {
        return null;
    }

    return $state['last_cursor'] ?: $state['last_success_at'];
}

function mark_sync_success(string $datasetKey, string $syncMode, ?string $cursor, int $rowsWritten, ?string $checksum = null): bool
{
    return db_sync_state_upsert(
        $datasetKey,
        $syncMode,
        'success',
        gmdate('Y-m-d H:i:s'),
        $cursor,
        $rowsWritten,
        $checksum,
        null
    );
}

function mark_sync_failure(string $datasetKey, string $syncMode, string $errorMessage): bool
{
    $state = db_sync_state_get($datasetKey);

    return db_sync_state_upsert(
        $datasetKey,
        $syncMode,
        'failed',
        $state['last_success_at'] ?? null,
        $state['last_cursor'] ?? null,
        (int) ($state['last_row_count'] ?? 0),
        $state['last_checksum'] ?? null,
        mb_substr($errorMessage, 0, 500)
    );
}



function sync_status_from_prefix(string $datasetPrefix, int $recentRunsLimit = 5): array
{
    try {
        $states = db_sync_state_by_dataset_prefix($datasetPrefix);
        $runs = db_sync_runs_recent_by_dataset_prefix($datasetPrefix, $recentRunsLimit);
    } catch (Throwable) {
        return [
            'states' => [],
            'runs' => [],
            'last_success_at' => null,
            'last_error_message' => null,
            'recent_rows_written' => 0,
        ];
    }

    $lastSuccessAt = null;
    foreach ($states as $state) {
        $candidate = $state['last_success_at'] ?? null;
        if (!is_string($candidate) || $candidate === '') {
            continue;
        }

        if ($lastSuccessAt === null || strtotime($candidate) > strtotime($lastSuccessAt)) {
            $lastSuccessAt = $candidate;
        }
    }

    $lastErrorMessage = null;
    foreach ($runs as $run) {
        if (($run['run_status'] ?? '') !== 'failed') {
            continue;
        }

        $message = trim((string) ($run['error_message'] ?? ''));
        if ($message !== '') {
            $lastErrorMessage = $message;
            break;
        }
    }

    if ($lastErrorMessage === null) {
        foreach ($states as $state) {
            $message = trim((string) ($state['last_error_message'] ?? ''));
            if ($message !== '') {
                $lastErrorMessage = $message;
                break;
            }
        }
    }

    $recentRowsWritten = 0;
    foreach ($runs as $run) {
        $recentRowsWritten += max(0, (int) ($run['written_rows'] ?? 0));
    }

    return [
        'states' => $states,
        'runs' => $runs,
        'last_success_at' => $lastSuccessAt,
        'last_error_message' => $lastErrorMessage,
        'recent_rows_written' => $recentRowsWritten,
    ];
}

function esi_default_scopes(): array
{
    return [
        'publicData',
        'esi-location.read_location.v1',
        'esi-search.search_structures.v1',
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

function esi_token_scope_list(array $token): array
{
    $scopes = preg_split('/\s+/', trim((string) ($token['scopes'] ?? ''))) ?: [];

    return array_values(array_filter(array_unique($scopes), static fn ($scope) => $scope !== ''));
}

function esi_required_market_structure_scopes(): array
{
    return [
        'esi-universe.read_structures.v1',
        'esi-markets.structure_markets.v1',
    ];
}

function esi_missing_scopes(array $token, array $requiredScopes): array
{
    return array_values(array_diff($requiredScopes, esi_token_scope_list($token)));
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
    $responseHeaders = [];
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_HTTPHEADER => $headers,
        CURLOPT_TIMEOUT => 25,
        CURLOPT_HEADERFUNCTION => static function ($curl, string $headerLine) use (&$responseHeaders): int {
            $trimmed = trim($headerLine);
            if ($trimmed === '' || !str_contains($trimmed, ':')) {
                return strlen($headerLine);
            }

            [$name, $value] = explode(':', $trimmed, 2);
            $normalizedName = mb_strtolower(trim($name));
            $normalizedValue = trim($value);
            if (!array_key_exists($normalizedName, $responseHeaders)) {
                $responseHeaders[$normalizedName] = $normalizedValue;

                return strlen($headerLine);
            }

            $existing = $responseHeaders[$normalizedName];
            if (is_array($existing)) {
                $existing[] = $normalizedValue;
                $responseHeaders[$normalizedName] = $existing;

                return strlen($headerLine);
            }

            $responseHeaders[$normalizedName] = [$existing, $normalizedValue];

            return strlen($headerLine);
        },
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

    return ['status' => $status, 'json' => $decoded, 'headers' => $responseHeaders];
}

function sync_http_retryable_status_codes(): array
{
    return [408, 420, 429, 500, 502, 503, 504];
}

function http_get_json_with_backoff(string $url, array $headers = [], int $maxAttempts = 4, int $baseDelayMs = 250): array
{
    $attempts = max(1, $maxAttempts);
    $delayMs = max(50, $baseDelayMs);
    $lastException = null;
    $lastResponse = null;

    for ($attempt = 1; $attempt <= $attempts; $attempt++) {
        try {
            $response = http_get_json($url, $headers);
            $status = (int) ($response['status'] ?? 500);
            $lastResponse = $response;

            if (!in_array($status, sync_http_retryable_status_codes(), true) || $attempt === $attempts) {
                return $response;
            }
        } catch (Throwable $exception) {
            $lastException = $exception;
            if ($attempt === $attempts) {
                break;
            }
        }

        usleep((int) (($delayMs * (2 ** ($attempt - 1))) * 1000));
    }

    if ($lastResponse !== null) {
        return $lastResponse;
    }

    throw new RuntimeException('HTTP request failed after retries.', 0, $lastException);
}

function sync_result_shape(): array
{
    return [
        'rows_seen' => 0,
        'rows_written' => 0,
        'cursor' => null,
        'checksum' => null,
        'warnings' => [],
    ];
}

function sync_mode_normalize(string $runMode): string
{
    return in_array($runMode, ['incremental', 'full'], true) ? $runMode : 'incremental';
}

function sync_dataset_key_alliance_structure_orders_current(int $structureId): string
{
    return 'alliance.structure.' . $structureId . '.orders.current';
}

function sync_dataset_key_alliance_structure_orders_history(int $structureId): string
{
    return 'alliance.structure.' . $structureId . '.orders.history';
}

function sync_dataset_key_market_hub_history_daily(string $hubKey): string
{
    return 'market.hub.' . $hubKey . '.history.daily';
}

function sync_dataset_key_maintenance_history_prune(): string
{
    return 'maintenance.history.prune';
}

function sync_run_finalize_success(int $runId, string $datasetKey, string $runMode, int $rowsSeen, int $rowsWritten, ?string $cursor, ?string $checksum): void
{
    mark_sync_success($datasetKey, $runMode, $cursor, $rowsWritten, $checksum);
    db_sync_run_finish($runId, 'success', $rowsSeen, $rowsWritten, $cursor, null);
}

function sync_run_finalize_failure(int $runId, string $datasetKey, string $runMode, string $errorMessage): void
{
    $message = mb_substr($errorMessage, 0, 500);
    mark_sync_failure($datasetKey, $runMode, $message);
    db_sync_run_finish($runId, 'failed', 0, 0, null, $message);
}

function sync_checksum(array $rows): string
{
    return hash('sha256', json_encode($rows, JSON_THROW_ON_ERROR));
}

function sync_source_id_from_hub_ref(string|int $hubRef): int
{
    if (is_int($hubRef) && $hubRef > 0) {
        return $hubRef;
    }

    $hubString = trim((string) $hubRef);
    if ($hubString !== '' && preg_match('/^[1-9][0-9]*$/', $hubString) === 1) {
        return (int) $hubString;
    }

    return (int) sprintf('%u', crc32(mb_strtolower($hubString)));
}

function canonicalize_esi_market_order(array $order, int $sourceId, string $observedAt): ?array
{
    $orderId = (int) ($order['order_id'] ?? 0);
    $typeId = (int) ($order['type_id'] ?? 0);
    if ($orderId <= 0 || $typeId <= 0) {
        return null;
    }

    $issuedAt = strtotime((string) ($order['issued'] ?? ''));
    if ($issuedAt === false) {
        $issuedAt = time();
    }

    $duration = max(1, (int) ($order['duration'] ?? 1));
    $expiresAt = strtotime((string) ($order['expires'] ?? ''));
    if ($expiresAt === false) {
        $expiresAt = strtotime('+' . $duration . ' days', $issuedAt);
    }

    return [
        'source_type' => 'alliance_structure',
        'source_id' => $sourceId,
        'type_id' => $typeId,
        'order_id' => $orderId,
        'is_buy_order' => !empty($order['is_buy_order']) ? 1 : 0,
        'price' => (float) ($order['price'] ?? 0),
        'volume_remain' => max(0, (int) ($order['volume_remain'] ?? 0)),
        'volume_total' => max(0, (int) ($order['volume_total'] ?? 0)),
        'min_volume' => max(1, (int) ($order['min_volume'] ?? 1)),
        'range' => (string) ($order['range'] ?? 'region'),
        'duration' => $duration,
        'issued' => gmdate('Y-m-d H:i:s', $issuedAt),
        'expires' => gmdate('Y-m-d H:i:s', $expiresAt),
        'observed_at' => $observedAt,
    ];
}

function sync_alliance_structure_orders(int $structureId, string $runMode = 'incremental'): array
{
    $result = sync_result_shape();
    $syncMode = sync_mode_normalize($runMode);
    $datasetKeyCurrent = sync_dataset_key_alliance_structure_orders_current($structureId);
    $datasetKeyHistory = sync_dataset_key_alliance_structure_orders_history($structureId);

    if ($structureId <= 0) {
        $result['warnings'][] = 'Invalid alliance structure id.';

        return $result;
    }

    $context = esi_lookup_context(esi_required_market_structure_scopes());
    if (($context['ok'] ?? false) !== true) {
        $result['warnings'][] = (string) ($context['error'] ?? 'Missing ESI context for structure market sync.');

        return $result;
    }

    $accessToken = (string) ($context['token']['access_token'] ?? '');
    $observedAt = gmdate('Y-m-d H:i:s');
    $page = 1;
    $maxPages = 1;
    $mappedOrders = [];

    while ($page <= $maxPages) {
        $response = http_get_json_with_backoff(
            'https://esi.evetech.net/latest/markets/structures/' . $structureId . '/?page=' . $page,
            [
                'Authorization: Bearer ' . $accessToken,
                'Accept: application/json',
            ]
        );

        $status = (int) ($response['status'] ?? 500);
        if ($status >= 400) {
            $result['warnings'][] = 'ESI structure market sync failed on page ' . $page . ' with status ' . $status . '.';
            break;
        }

        $payload = $response['json'] ?? [];
        if (!is_array($payload)) {
            $result['warnings'][] = 'ESI structure market page ' . $page . ' returned a non-array payload.';
            break;
        }

        $result['rows_seen'] += count($payload);
        foreach ($payload as $order) {
            if (!is_array($order)) {
                continue;
            }

            $mapped = canonicalize_esi_market_order($order, $structureId, $observedAt);
            if ($mapped !== null) {
                $mappedOrders[] = $mapped;
            }
        }

        $pagesHeader = $response['headers']['x-pages'] ?? '1';
        if (is_array($pagesHeader)) {
            $pagesHeader = end($pagesHeader);
        }

        $maxPages = max(1, (int) $pagesHeader);
        $page++;
    }

    $snapshotCursor = 'observed_at:' . $observedAt . ';page:' . max(1, $page - 1);
    $result['cursor'] = $snapshotCursor;

    $checksum = sync_checksum($mappedOrders);

    $runIdCurrent = db_sync_run_start($datasetKeyCurrent, $syncMode, sync_watermark($datasetKeyCurrent));
    $runIdHistory = db_sync_run_start($datasetKeyHistory, $syncMode, sync_watermark($datasetKeyHistory));
    $currentFinished = false;
    $historyFinished = false;

    try {
        if ($mappedOrders === []) {
            sync_run_finalize_success($runIdCurrent, $datasetKeyCurrent, $syncMode, $result['rows_seen'], 0, $snapshotCursor, $checksum);
            $currentFinished = true;
            sync_run_finalize_success($runIdHistory, $datasetKeyHistory, $syncMode, $result['rows_seen'], 0, $snapshotCursor, $checksum);
            $historyFinished = true;
            $result['checksum'] = $checksum;

            return $result;
        }

        $writtenCurrent = db_market_orders_current_bulk_upsert($mappedOrders);
        sync_run_finalize_success($runIdCurrent, $datasetKeyCurrent, $syncMode, $result['rows_seen'], $writtenCurrent, $snapshotCursor, $checksum);
        $currentFinished = true;

        $writtenHistory = db_market_orders_history_bulk_insert($mappedOrders);
        sync_run_finalize_success($runIdHistory, $datasetKeyHistory, $syncMode, $result['rows_seen'], $writtenHistory, $snapshotCursor, $checksum);
        $historyFinished = true;

        $result['rows_written'] = $writtenCurrent + $writtenHistory;
        $result['checksum'] = $checksum;

        return $result;
    } catch (Throwable $exception) {
        if (!$currentFinished) {
            sync_run_finalize_failure($runIdCurrent, $datasetKeyCurrent, $syncMode, $exception->getMessage());
        }

        if (!$historyFinished) {
            sync_run_finalize_failure($runIdHistory, $datasetKeyHistory, $syncMode, $exception->getMessage());
        }

        $result['warnings'][] = 'Alliance structure order sync failed: ' . $exception->getMessage();
        $result['checksum'] = $checksum;

        return $result;
    }
}

function eve_tycoon_history_payload_rows(array $payload): array
{
    foreach (['items', 'data', 'results', 'history'] as $key) {
        if (isset($payload[$key]) && is_array($payload[$key])) {
            return $payload[$key];
        }
    }

    if (array_is_list($payload)) {
        return $payload;
    }

    if (isset($payload['type_id'], $payload['date'])) {
        return [$payload];
    }

    return [];
}

function eve_tycoon_history_to_canonical(array $row, int $sourceId, string $observedAt, ?int $typeId = null): ?array
{
    $resolvedTypeId = $typeId ?? (int) ($row['type_id'] ?? $row['typeId'] ?? 0);
    $tradeDateRaw = (string) ($row['date'] ?? $row['trade_date'] ?? $row['day'] ?? '');
    $tradeDateTs = strtotime($tradeDateRaw);
    if ($resolvedTypeId <= 0 || $tradeDateTs === false) {
        return null;
    }

    $open = (float) ($row['open'] ?? $row['open_price'] ?? 0);
    $high = (float) ($row['high'] ?? $row['high_price'] ?? $open);
    $low = (float) ($row['low'] ?? $row['low_price'] ?? $open);
    $close = (float) ($row['close'] ?? $row['close_price'] ?? $open);
    $average = $row['average'] ?? $row['average_price'] ?? null;

    return [
        'source_type' => 'market_hub',
        'source_id' => $sourceId,
        'type_id' => $resolvedTypeId,
        'trade_date' => gmdate('Y-m-d', $tradeDateTs),
        'open_price' => $open,
        'high_price' => $high,
        'low_price' => $low,
        'close_price' => $close,
        'average_price' => $average !== null ? (float) $average : null,
        'volume' => max(0, (int) ($row['volume'] ?? 0)),
        'order_count' => isset($row['order_count']) ? max(0, (int) $row['order_count']) : (isset($row['orders']) ? max(0, (int) $row['orders']) : null),
        'source_label' => 'eve_tycoon',
        'observed_at' => $observedAt,
    ];
}

function sync_market_hub_history(string|int $hubRef, string $runMode = 'incremental'): array
{
    $result = sync_result_shape();
    $syncMode = sync_mode_normalize($runMode);
    $sourceId = sync_source_id_from_hub_ref($hubRef);
    $hubKey = trim((string) $hubRef);
    $datasetKey = sync_dataset_key_market_hub_history_daily($hubKey);
    if ($hubKey === '') {
        $result['warnings'][] = 'Market hub reference is required.';

        return $result;
    }

    $runId = db_sync_run_start($datasetKey, $syncMode, sync_watermark($datasetKey));

    try {
        $urlTemplate = trim((string) get_setting('eve_tycoon_history_url_template', 'https://evetycoon.com/api/v1/market/history/{hub}'));
        $endpoint = str_replace('{hub}', rawurlencode($hubKey), $urlTemplate);

        $response = http_get_json_with_backoff($endpoint, ['Accept: application/json']);
        $status = (int) ($response['status'] ?? 500);
        if ($status >= 400) {
            throw new RuntimeException('EVE Tycoon history sync failed with status ' . $status . '.');
        }

        $providerRows = eve_tycoon_history_payload_rows($response['json'] ?? []);
        $result['rows_seen'] = count($providerRows);
        $observedAt = gmdate('Y-m-d H:i:s');
        $canonicalRows = [];

        foreach ($providerRows as $providerRow) {
            if (!is_array($providerRow)) {
                continue;
            }

            if (isset($providerRow['history']) && is_array($providerRow['history'])) {
                $parentTypeId = (int) ($providerRow['type_id'] ?? $providerRow['typeId'] ?? 0);
                foreach ($providerRow['history'] as $historyRow) {
                    if (!is_array($historyRow)) {
                        continue;
                    }

                    $mapped = eve_tycoon_history_to_canonical($historyRow, $sourceId, $observedAt, $parentTypeId > 0 ? $parentTypeId : null);
                    if ($mapped !== null) {
                        $canonicalRows[] = $mapped;
                    }
                }

                continue;
            }

            $mapped = eve_tycoon_history_to_canonical($providerRow, $sourceId, $observedAt);
            if ($mapped !== null) {
                $canonicalRows[] = $mapped;
            }
        }

        if ($canonicalRows === []) {
            $result['warnings'][] = 'No canonical market history rows were mapped from provider payload.';
            $result['checksum'] = sync_checksum([]);
            $result['cursor'] = null;
            sync_run_finalize_success($runId, $datasetKey, $syncMode, $result['rows_seen'], 0, $result['cursor'], $result['checksum']);

            return $result;
        }

        $result['rows_written'] = db_market_history_daily_bulk_upsert($canonicalRows);
        $latestTradeDate = max(array_column($canonicalRows, 'trade_date'));
        $result['cursor'] = 'trade_date:' . $latestTradeDate;
        $result['checksum'] = sync_checksum($canonicalRows);
        sync_run_finalize_success($runId, $datasetKey, $syncMode, $result['rows_seen'], $result['rows_written'], $result['cursor'], $result['checksum']);

        return $result;
    } catch (Throwable $exception) {
        sync_run_finalize_failure($runId, $datasetKey, $syncMode, $exception->getMessage());
        $result['warnings'][] = $exception->getMessage();

        return $result;
    }
}

function sync_market_orders_history_prune(int $retentionDays, string $runMode = 'incremental'): array
{
    $result = sync_result_shape();
    $datasetKey = sync_dataset_key_maintenance_history_prune();
    $syncMode = sync_mode_normalize($runMode);
    $runId = db_sync_run_start($datasetKey, $syncMode, sync_watermark($datasetKey));

    try {
        $safeRetentionDays = max(1, min(3650, $retentionDays));
        $cutoffObservedAt = gmdate('Y-m-d H:i:s', strtotime('-' . $safeRetentionDays . ' days'));
        $deletedRows = db_market_orders_history_prune_before($cutoffObservedAt);

        $result['rows_seen'] = $deletedRows;
        $result['rows_written'] = $deletedRows;
        $result['cursor'] = 'cutoff:' . $cutoffObservedAt;
        $result['checksum'] = sync_checksum([
            'deleted_rows' => $deletedRows,
            'retention_days' => $safeRetentionDays,
            'cutoff_observed_at' => $cutoffObservedAt,
        ]);

        sync_run_finalize_success(
            $runId,
            $datasetKey,
            $syncMode,
            $result['rows_seen'],
            $result['rows_written'],
            $result['cursor'],
            $result['checksum']
        );

        return $result;
    } catch (Throwable $exception) {
        sync_run_finalize_failure($runId, $datasetKey, $syncMode, $exception->getMessage());
        $result['warnings'][] = 'History prune job failed: ' . $exception->getMessage();

        return $result;
    }
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

function esi_refresh_oauth_token(array $storedToken): array
{
    $clientId = trim((string) get_setting('esi_client_id', ''));
    $clientSecret = trim((string) get_setting('esi_client_secret', ''));
    $refreshToken = trim((string) ($storedToken['refresh_token'] ?? ''));

    if ($clientId === '' || $clientSecret === '' || $refreshToken === '') {
        throw new RuntimeException('ESI session is missing OAuth credentials. Please reconnect your character from Settings > ESI Login.');
    }

    $response = http_post_form(
        'https://login.eveonline.com/v2/oauth/token',
        [
            'Authorization: Basic ' . base64_encode($clientId . ':' . $clientSecret),
            'Content-Type: application/x-www-form-urlencoded',
            'Accept: application/json',
            'Host: login.eveonline.com',
        ],
        [
            'grant_type' => 'refresh_token',
            'refresh_token' => $refreshToken,
        ]
    );

    if (($response['status'] ?? 500) >= 400) {
        throw new RuntimeException('Unable to refresh ESI session. Please reconnect your character from Settings > ESI Login.');
    }

    $tokenPayload = $response['json'];
    $expiresIn = (int) ($tokenPayload['expires_in'] ?? 0);
    $expiresAt = (new DateTimeImmutable('now', new DateTimeZone('UTC')))->modify('+' . max(0, $expiresIn) . ' seconds')->format('Y-m-d H:i:s');
    $nextAccessToken = trim((string) ($tokenPayload['access_token'] ?? ''));
    $nextRefreshToken = trim((string) ($tokenPayload['refresh_token'] ?? $refreshToken));
    $tokenType = trim((string) ($tokenPayload['token_type'] ?? ($storedToken['token_type'] ?? 'Bearer')));
    $scopes = trim((string) ($tokenPayload['scope'] ?? ($storedToken['scopes'] ?? esi_scopes_string())));

    if ($nextAccessToken === '' || (int) ($storedToken['id'] ?? 0) <= 0) {
        throw new RuntimeException('ESI session refresh returned an invalid token payload. Please reconnect your character.');
    }

    db_update_esi_oauth_token_refresh(
        (int) $storedToken['id'],
        $nextAccessToken,
        $nextRefreshToken,
        $tokenType !== '' ? $tokenType : 'Bearer',
        $scopes,
        $expiresAt
    );

    db_esi_cache_put('cache.esi.oauth.token', 'latest', json_encode([
        'token_type' => $tokenType !== '' ? $tokenType : 'Bearer',
        'expires_in' => $tokenPayload['expires_in'] ?? null,
        'scope' => $scopes,
    ], JSON_THROW_ON_ERROR), null, $expiresAt);

    $storedToken['access_token'] = $nextAccessToken;
    $storedToken['refresh_token'] = $nextRefreshToken;
    $storedToken['token_type'] = $tokenType !== '' ? $tokenType : 'Bearer';
    $storedToken['scopes'] = $scopes;
    $storedToken['expires_at'] = $expiresAt;

    return $storedToken;
}

function esi_valid_access_token(int $refreshWindowSeconds = 120): string
{
    $storedToken = db_latest_esi_oauth_token();
    if ($storedToken === null) {
        throw new RuntimeException('No connected ESI character token found. Connect a character from Settings > ESI Login.');
    }

    $accessToken = trim((string) ($storedToken['access_token'] ?? ''));
    if ($accessToken === '') {
        throw new RuntimeException('Connected ESI token is invalid. Please reconnect your character.');
    }

    $expiresAt = strtotime((string) ($storedToken['expires_at'] ?? '')) ?: 0;
    if ($expiresAt <= (time() + max(0, $refreshWindowSeconds))) {
        $storedToken = esi_refresh_oauth_token($storedToken);
        $accessToken = trim((string) ($storedToken['access_token'] ?? ''));
        if ($accessToken === '') {
            throw new RuntimeException('Unable to refresh ESI session. Please reconnect your character from Settings > ESI Login.');
        }
    }

    return $accessToken;
}

function esi_lookup_context(array $requiredScopes = []): array
{
    $enabled = get_setting('esi_enabled', '0') === '1';
    if (!$enabled) {
        return ['ok' => false, 'status' => 403, 'error' => 'ESI login is disabled in settings.'];
    }

    $token = db_latest_esi_oauth_token();
    if ($token === null) {
        return ['ok' => false, 'status' => 401, 'error' => 'No connected ESI character token found.'];
    }

    try {
        $token['access_token'] = esi_valid_access_token();
    } catch (Throwable $exception) {
        return ['ok' => false, 'status' => 401, 'error' => $exception->getMessage()];
    }

    $missing = esi_missing_scopes($token, $requiredScopes);

    if ($missing !== []) {
        return ['ok' => false, 'status' => 403, 'error' => 'Missing required ESI scopes: ' . implode(', ', $missing) . '. Reconnect your character after updating scopes.'];
    }

    return ['ok' => true, 'token' => $token, 'status' => 200, 'error' => null];
}

function esi_structure_result_shape(array $structure): array
{
    $result = [
        'id' => (int) ($structure['id'] ?? 0),
        'name' => (string) ($structure['name'] ?? ''),
    ];

    if (!empty($structure['system'])) {
        $result['system'] = (string) $structure['system'];
    }

    if (!empty($structure['type'])) {
        $result['type'] = (string) $structure['type'];
    }

    return $result;
}


function esi_alliance_structure_metadata(int $structureId, array $tokenContext): ?array
{
    if ($structureId <= 0) {
        return null;
    }

    try {
        $accessToken = esi_valid_access_token();
    } catch (Throwable) {
        return null;
    }

    $cached = db_alliance_structure_metadata_get($structureId);
    if ($cached !== null && trim((string) ($cached['structure_name'] ?? '')) !== '') {
        return [
            'id' => $structureId,
            'name' => trim((string) $cached['structure_name']),
            'last_verified_at' => $cached['last_verified_at'] ?? null,
        ];
    }

    $response = http_get_json(
        'https://esi.evetech.net/latest/universe/structures/' . $structureId . '/',
        [
            'Authorization: Bearer ' . $accessToken,
            'Accept: application/json',
        ]
    );

    if (($response['status'] ?? 500) >= 400) {
        return null;
    }

    $name = trim((string) ($response['json']['name'] ?? ''));
    db_alliance_structure_metadata_upsert(
        $structureId,
        $name !== '' ? $name : null,
        gmdate('Y-m-d H:i:s')
    );

    return [
        'id' => $structureId,
        'name' => $name,
        'last_verified_at' => gmdate('Y-m-d H:i:s'),
    ];
}

function esi_structure_search(string $query, array $tokenContext): array
{
    $term = trim($query);
    if ($term === '') {
        return [];
    }

    $characterId = (int) ($tokenContext['character_id'] ?? 0);
    if ($characterId <= 0) {
        return [];
    }

    $accessToken = esi_valid_access_token();

    $cached = db_esi_structure_search_cache_get($characterId, $term);
    if ($cached !== null && isset($cached['payload_json'])) {
        try {
            $payload = json_decode((string) $cached['payload_json'], true, 512, JSON_THROW_ON_ERROR);
            if (is_array($payload)) {
                return $payload;
            }
        } catch (Throwable) {
            // Continue to ESI fetch.
        }
    }

    $searchResponse = http_get_json(
        'https://esi.evetech.net/latest/characters/' . $characterId . '/search/?categories=structure&strict=false&search=' . rawurlencode($term),
        [
            'Authorization: Bearer ' . $accessToken,
            'Accept: application/json',
        ]
    );

    if (($searchResponse['status'] ?? 500) >= 400) {
        throw new RuntimeException('Failed to search structures from ESI.');
    }

    $structureIds = $searchResponse['json']['structure'] ?? [];
    if (!is_array($structureIds)) {
        return [];
    }

    $results = [];
    foreach (array_slice($structureIds, 0, 20) as $structureId) {
        $id = (int) $structureId;
        if ($id <= 0) {
            continue;
        }

        try {
            $structureResponse = http_get_json(
                'https://esi.evetech.net/latest/universe/structures/' . $id . '/',
                [
                    'Authorization: Bearer ' . $accessToken,
                    'Accept: application/json',
                ]
            );
        } catch (Throwable) {
            continue;
        }

        if (($structureResponse['status'] ?? 500) >= 400) {
            continue;
        }

        $name = trim((string) ($structureResponse['json']['name'] ?? ''));
        if ($name === '') {
            continue;
        }

        if (mb_stripos($name, $term) === false) {
            continue;
        }

        db_alliance_structure_metadata_upsert($id, $name, gmdate('Y-m-d H:i:s'));

        $results[] = esi_structure_result_shape([
            'id' => $id,
            'name' => $name,
            'system' => isset($structureResponse['json']['solar_system_id']) ? (string) $structureResponse['json']['solar_system_id'] : null,
            'type' => isset($structureResponse['json']['type_id']) ? (string) $structureResponse['json']['type_id'] : null,
        ]);
    }

    db_esi_structure_search_cache_put(
        $characterId,
        $term,
        json_encode($results, JSON_THROW_ON_ERROR),
        gmdate('Y-m-d H:i:s', time() + 300)
    );

    return $results;
}

function runner_lock_acquire(string $lockName, int $timeoutSeconds = 0): bool
{
    return db_runner_lock_acquire($lockName, $timeoutSeconds);
}

function runner_lock_release(string $lockName): bool
{
    return db_runner_lock_release($lockName);
}
