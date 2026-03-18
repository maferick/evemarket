<?php

declare(strict_types=1);

require_once __DIR__ . '/db.php';

date_default_timezone_set(app_timezone());

function app_name(): string
{
    $configured = trim((string) get_setting('app_name', config('app.name', 'SupplyCore')));

    return sanitize_app_name($configured);
}

function brand_family_name(): string
{
    $configured = trim((string) get_setting('brand_family_name', 'SupplyCore'));

    return sanitize_app_name($configured);
}

function brand_console_label(): string
{
    $configured = trim((string) get_setting('brand_console_label', brand_family_name() . ' Console'));

    return sanitize_brand_label($configured, brand_family_name() . ' Console');
}

function brand_tagline(): string
{
    $configured = trim((string) get_setting('brand_tagline', 'Alliance logistics intelligence platform'));

    return sanitize_brand_label($configured, 'Alliance logistics intelligence platform');
}

function brand_logo_path(): string
{
    $configured = trim((string) get_setting('brand_logo_path', '/assets/branding/supplycore-logo.svg'));

    return sanitize_brand_asset_path($configured, '/assets/branding/supplycore-logo.svg');
}

function brand_favicon_path(): string
{
    $configured = trim((string) get_setting('brand_favicon_path', '/assets/branding/supplycore-favicon.svg'));

    return sanitize_brand_asset_path($configured, '/assets/branding/supplycore-favicon.svg');
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
            'icon' => '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" class="h-4 w-4" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" d="M4 19V5"/><path stroke-linecap="round" stroke-linejoin="round" d="M8 15l3-3 3 2 6-6"/></svg>',
            'children' => [],
        ],
        [
            'label' => 'Market Status',
            'path' => '/market-status',
            'icon' => '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" class="h-4 w-4" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" d="M3 6h18"/><path stroke-linecap="round" stroke-linejoin="round" d="M7 12h10"/><path stroke-linecap="round" stroke-linejoin="round" d="M10 18h4"/></svg>',
            'children' => [
                ['label' => 'Current Alliance Structure', 'path' => '/market-status/current-alliance'],
                ['label' => 'Reference Hub Comparison', 'path' => '/market-status/reference-comparison'],
                ['label' => 'Missing Items', 'path' => '/market-status/missing-items'],
                ['label' => 'Price Deviations', 'path' => '/market-status/price-deviations'],
            ],
        ],
        [
            'label' => 'History',
            'path' => '/history',
            'icon' => '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" class="h-4 w-4" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" d="M12 8v5l3 2"/><path stroke-linecap="round" stroke-linejoin="round" d="M21 12a9 9 0 1 1-3.3-6.95"/></svg>',
            'children' => [
                ['label' => 'Alliance Structure Trends', 'path' => '/history/alliance-trends'],
                ['label' => 'Module History', 'path' => '/history/module-history'],
            ],
        ],
        [
            'label' => 'Killmail Intelligence',
            'path' => '/killmail-intelligence',
            'icon' => '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" class="h-4 w-4" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" d="M13 3 4 14h6l-1 7 9-11h-6l1-7Z"/></svg>',
            'children' => [
                ['label' => 'Recent Killmails', 'path' => '/killmail-intelligence'],
            ],
        ],
        [
            'label' => 'Doctrine Fits',
            'path' => '/doctrine',
            'icon' => '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" class="h-4 w-4" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" d="M5 5h14v14H5z"/><path stroke-linecap="round" stroke-linejoin="round" d="M9 9h6M9 13h6M9 17h4"/></svg>',
            'children' => [
                ['label' => 'Doctrine Groups', 'path' => '/doctrine'],
                ['label' => 'Import Fit', 'path' => '/doctrine/import'],
            ],
        ],
        [
            'label' => 'Settings',
            'path' => '/settings',
            'icon' => '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" class="h-4 w-4" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" d="M12 15.5A3.5 3.5 0 1 0 12 8.5a3.5 3.5 0 0 0 0 7Z"/><path stroke-linecap="round" stroke-linejoin="round" d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06A1.65 1.65 0 0 0 15 19.4a1.65 1.65 0 0 0-1 .6 1.65 1.65 0 0 0-.33 1V21a2 2 0 1 1-4 0v-.09a1.65 1.65 0 0 0-.33-1 1.65 1.65 0 0 0-1-.6 1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.6 15a1.65 1.65 0 0 0-.6-1 1.65 1.65 0 0 0-1-.33H3a2 2 0 1 1 0-4h.09a1.65 1.65 0 0 0 1-.33 1.65 1.65 0 0 0 .6-1 1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.6a1.65 1.65 0 0 0 1-.6 1.65 1.65 0 0 0 .33-1V3a2 2 0 1 1 4 0v.09a1.65 1.65 0 0 0 .33 1 1.65 1.65 0 0 0 1 .6 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9c.3.3.5.68.6 1 .23.1.66.1 1 .1H21a2 2 0 1 1 0 4h-.09a1.65 1.65 0 0 0-1 .33c-.44.25-.79.6-1.01 1.07Z"/></svg>',
            'children' => [],
        ],
    ];
}

function setting_sections(): array
{
    return [
        'general' => ['title' => 'General Settings', 'description' => 'Core application behavior and preferences.'],
        'trading-stations' => ['title' => 'Trading Stations', 'description' => 'Configure your reference market hub and operational trading destination.'],
        'esi-login' => ['title' => 'ESI Login', 'description' => 'Configure EVE SSO credentials and callback behavior.'],
        'data-sync' => ['title' => 'Data Sync', 'description' => 'Control database import and incremental update policies.'],
        'killmail-intelligence' => ['title' => 'Killmail Intelligence', 'description' => 'Manage zKillboard stream ingestion, tracked entities, and demand prediction foundation.'],
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
            ['id' => 1, 'station_name' => 'Primary Market Hub', 'station_type' => 'market'],
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
        return 'SupplyCore';
    }

    return mb_substr($name, 0, 120);
}

function sanitize_brand_label(string $value, string $default): string
{
    $value = trim($value);

    if ($value === '') {
        return $default;
    }

    return mb_substr($value, 0, 160);
}

function sanitize_brand_asset_path(string $path, string $default): string
{
    $path = trim($path);

    if ($path === '') {
        return $default;
    }

    if ($path[0] !== '/' || preg_match('/[[:cntrl:]]/', $path) === 1) {
        return $default;
    }

    return mb_substr($path, 0, 255);
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
    $stationIdValue = trim((string) $value);
    if ($stationIdValue === '') {
        return '';
    }

    $stationId = (int) $stationIdValue;
    if ($stationId <= 0) {
        return '';
    }

    try {
        $station = db_ref_npc_station_by_id($stationId);
    } catch (Throwable) {
        $station = null;
    }

    if ($station !== null) {
        return (string) $stationId;
    }

    try {
        $metadata = esi_npc_station_metadata($stationId);
    } catch (Throwable) {
        $metadata = null;
    }

    if ($metadata !== null) {
        return (string) $stationId;
    }

    if (!preg_match('/^[1-9][0-9]{9,19}$/', $stationIdValue)) {
        return '';
    }

    try {
        $structureMetadata = db_alliance_structure_metadata_get($stationId);
    } catch (Throwable) {
        $structureMetadata = null;
    }

    if ($structureMetadata !== null) {
        return (string) $stationId;
    }

    $context = esi_lookup_context(['esi-universe.read_structures.v1']);
    if (!($context['ok'] ?? false)) {
        return '';
    }

    try {
        $resolved = esi_alliance_structure_metadata($stationId, $context['token']);
    } catch (Throwable) {
        return '';
    }

    if ($resolved === null) {
        return '';
    }

    db_alliance_structure_metadata_upsert(
        $stationId,
        $resolved['name'] ?? null,
        gmdate('Y-m-d H:i:s')
    );

    return (string) $stationId;
}

function sanitize_alliance_station_selection(?string $value): string
{
    $stationIdValue = trim((string) $value);
    if ($stationIdValue === '') {
        return '';
    }

    $stationId = (int) $stationIdValue;
    if ($stationId > 0) {
        try {
            $npcStation = db_ref_npc_station_by_id($stationId);
        } catch (Throwable) {
            $npcStation = null;
        }

        if ($npcStation !== null) {
            return (string) $stationId;
        }
    }

    if (!preg_match('/^[1-9][0-9]{9,19}$/', $stationIdValue)) {
        return '';
    }

    $structureId = (int) $stationIdValue;
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

function configured_structure_destination_id_for_esi_sync(): int
{
    $configured = trim((string) get_setting('alliance_station_id', '0'));
    if ($configured === '' || !preg_match('/^[1-9][0-9]{9,19}$/', $configured)) {
        return 0;
    }

    return (int) $configured;
}

function configured_alliance_structure_id(): int
{
    return configured_structure_destination_id_for_esi_sync();
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
            $npcStation = db_ref_npc_station_by_id($stationId);
        } catch (Throwable) {
            $npcStation = null;
        }

        if ($npcStation !== null) {
            $npcStationName = trim((string) ($npcStation['station_name'] ?? ''));
            if ($npcStationName !== '' && !is_placeholder_station_name($npcStationName, $stationId)) {
                return $npcStationName;
            }

            try {
                $metadata = esi_npc_station_metadata($stationId);
            } catch (Throwable) {
                $metadata = null;
            }

            if ($metadata !== null && trim((string) ($metadata['name'] ?? '')) !== '') {
                return trim((string) $metadata['name']);
            }

            return $npcStationName !== '' ? $npcStationName : ('Station #' . $stationId);
        }

        try {
            $metadata = db_alliance_structure_metadata_get($stationId);
        } catch (Throwable) {
            $metadata = null;
        }

        $name = trim((string) ($metadata['structure_name'] ?? ''));

        return $name !== '' ? $name : 'Structure #' . $stationId;
    }

    try {
        $station = db_ref_npc_station_by_id($stationId);
    } catch (Throwable) {
        $station = null;
    }

    if ($station !== null) {
        $stationName = trim((string) ($station['station_name'] ?? ''));
        if ($stationName !== '' && !is_placeholder_station_name($stationName, $stationId)) {
            return $stationName;
        }
    }

    try {
        $metadata = esi_npc_station_metadata($stationId);
    } catch (Throwable) {
        $metadata = null;
    }

    if ($metadata !== null && trim((string) ($metadata['name'] ?? '')) !== '') {
        return trim((string) $metadata['name']);
    }

    if (preg_match('/^[1-9][0-9]{9,19}$/', (string) $stationId) === 1) {
        try {
            $structureMetadata = db_alliance_structure_metadata_get($stationId);
        } catch (Throwable) {
            $structureMetadata = null;
        }

        $structureName = trim((string) ($structureMetadata['structure_name'] ?? ''));

        return $structureName !== '' ? $structureName : ('Structure #' . $stationId);
    }

    return null;
}

function is_placeholder_station_name(string $stationName, int $stationId): bool
{
    $normalized = trim($stationName);
    if ($normalized === '') {
        return true;
    }

    $stationIdString = (string) $stationId;

    return preg_match('/^Station\s*#?\s*' . preg_quote($stationIdString, '/') . '$/i', $normalized) === 1;
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

function sanitize_sync_schedule_enabled(mixed $value): int
{
    return $value === '1' || $value === 1 || $value === true || $value === 'on' ? 1 : 0;
}

function sanitize_sync_interval_value(mixed $value, int $default): int
{
    $interval = (int) $value;
    if ($interval <= 0) {
        $interval = $default;
    }

    return max(1, min(86400, $interval));
}

function sanitize_sync_interval_unit(mixed $value): string
{
    $unit = mb_strtolower(trim((string) $value));

    return in_array($unit, ['seconds', 'minutes'], true) ? $unit : 'minutes';
}

function sanitize_sync_schedule_interval_seconds(mixed $value, mixed $unit, int $defaultSeconds): int
{
    $safeUnit = sanitize_sync_interval_unit($unit);
    $defaultValue = $safeUnit === 'minutes' ? max(1, intdiv($defaultSeconds, 60)) : $defaultSeconds;
    $intervalValue = sanitize_sync_interval_value($value, $defaultValue);

    if ($safeUnit === 'minutes') {
        $intervalValue *= 60;
    }

    return max(1, min(86400, $intervalValue));
}

function sync_schedule_form_values(int $intervalSeconds): array
{
    $safeSeconds = max(1, $intervalSeconds);

    if ($safeSeconds % 60 === 0) {
        return ['value' => (string) intdiv($safeSeconds, 60), 'unit' => 'minutes'];
    }

    return ['value' => (string) $safeSeconds, 'unit' => 'seconds'];
}

function sanitize_pipeline_enabled(mixed $value): string
{
    return $value === '1' || $value === 1 || $value === true || $value === 'on' ? '1' : '0';
}

function sanitize_enabled_flag(mixed $value): string
{
    return sanitize_pipeline_enabled($value);
}

function data_sync_pipeline_setting_defaults(): array
{
    return [
        'incremental_updates_enabled' => '1',
        'incremental_strategy' => 'watermark_upsert',
        'incremental_delete_policy' => 'reconcile',
        'incremental_chunk_size' => '1000',
        'alliance_current_pipeline_enabled' => '1',
        'alliance_history_pipeline_enabled' => '1',
        'hub_history_pipeline_enabled' => '1',
        'market_hub_local_history_pipeline_enabled' => '1',
        'raw_order_snapshot_retention_days' => '30',
        'static_data_source_url' => 'https://developers.eveonline.com/static-data/eve-online-static-data-latest-jsonl.zip',
    ];
}

function data_sync_pipeline_setting_value(array $settings, string $key): string
{
    $defaults = data_sync_pipeline_setting_defaults();
    $value = $settings[$key] ?? ($defaults[$key] ?? '');

    return match ($key) {
        'incremental_updates_enabled',
        'alliance_current_pipeline_enabled',
        'alliance_history_pipeline_enabled',
        'hub_history_pipeline_enabled',
        'market_hub_local_history_pipeline_enabled' => sanitize_pipeline_enabled($value),
        'incremental_strategy' => sanitize_incremental_strategy((string) $value),
        'incremental_delete_policy' => sanitize_incremental_delete_policy((string) $value),
        'incremental_chunk_size' => sanitize_incremental_chunk_size($value),
        'raw_order_snapshot_retention_days' => sanitize_retention_days($value),
        'static_data_source_url' => sanitize_static_data_source_url($value),
        default => (string) $value,
    };
}

function data_sync_pipeline_settings_view(array $settings): array
{
    $resolved = [];

    foreach (array_keys(data_sync_pipeline_setting_defaults()) as $key) {
        $resolved[$key] = data_sync_pipeline_setting_value($settings, $key);
    }

    return $resolved;
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


function sync_automation_enabled_since_date(): string
{
    $stored = sanitize_backfill_start_date(get_setting('sync_automation_enabled_since', ''));

    if ($stored !== '') {
        return $stored;
    }

    return gmdate('Y-m-d');
}

function data_sync_backfill_start_date(string $settingKey, bool $pipelineEnabled, string $fallbackDate): string
{
    $existing = sanitize_backfill_start_date(get_setting($settingKey, ''));
    if ($existing !== '') {
        return $existing;
    }

    return $pipelineEnabled ? $fallbackDate : '';
}

function run_data_sync_now(?string $jobKey = null): array
{
    $lockName = 'cron_tick_runner';
    $lockAcquired = false;
    $definitions = data_sync_schedule_job_definitions();
    $normalizedJobKey = $jobKey === null ? '' : trim($jobKey);

    if ($normalizedJobKey !== '' && !isset($definitions[$normalizedJobKey])) {
        return [
            'ok' => false,
            'message' => 'Run now failed: unknown sync job selected.',
        ];
    }

    try {
        $lockAcquired = runner_lock_acquire($lockName, 0);
        if (!$lockAcquired) {
            return [
                'ok' => false,
                'message' => 'Data sync is already running from cron. Please wait a minute and try again.',
            ];
        }

        $forcedJobs = $normalizedJobKey === ''
            ? db_sync_schedule_force_due_all_enabled()
            : db_sync_schedule_force_due_by_job_keys([$normalizedJobKey]);
        $summary = cron_tick_run();

        $jobLabel = $normalizedJobKey === ''
            ? 'all enabled jobs'
            : ((string) ($definitions[$normalizedJobKey]['label'] ?? $normalizedJobKey) . ' job');

        return [
            'ok' => true,
            'message' => 'Run now completed for ' . $jobLabel . '. Forced ' . $forcedJobs . ' schedule(s); processed ' . (int) ($summary['jobs_processed'] ?? 0) . ' job(s), with ' . (int) ($summary['jobs_failed'] ?? 0) . ' failure(s).',
            'summary' => $summary,
        ];
    } catch (Throwable $exception) {
        return [
            'ok' => false,
            'message' => 'Run now failed: ' . scheduler_normalize_error_message($exception->getMessage()),
        ];
    } finally {
        if ($lockAcquired) {
            runner_lock_release($lockName);
        }
    }
}

function data_sync_schedule_job_definitions(): array
{
    return [
        'alliance_current_sync' => [
            'enabled_key' => 'alliance_current_sync_enabled',
            'interval_value_key' => 'alliance_current_sync_interval_value',
            'interval_unit_key' => 'alliance_current_sync_interval_unit',
            'default_interval_seconds' => 300,
            'label' => 'Alliance Current',
        ],
        'alliance_historical_sync' => [
            'enabled_key' => 'alliance_historical_sync_enabled',
            'interval_value_key' => 'alliance_historical_sync_interval_value',
            'interval_unit_key' => 'alliance_historical_sync_interval_unit',
            'default_interval_seconds' => 1800,
            'label' => 'Alliance History',
        ],
        'market_hub_current_sync' => [
            'enabled_key' => 'market_hub_current_sync_enabled',
            'interval_value_key' => 'market_hub_current_sync_interval_value',
            'interval_unit_key' => 'market_hub_current_sync_interval_unit',
            'default_interval_seconds' => 300,
            'label' => 'Hub Current',
        ],
        'market_hub_historical_sync' => [
            'enabled_key' => 'market_hub_historical_sync_enabled',
            'interval_value_key' => 'market_hub_historical_sync_interval_value',
            'interval_unit_key' => 'market_hub_historical_sync_interval_unit',
            'default_interval_seconds' => 1800,
            'label' => 'Hub History',
        ],
        'market_hub_local_history_sync' => [
            'enabled_key' => 'market_hub_local_history_sync_enabled',
            'interval_value_key' => 'market_hub_local_history_sync_interval_value',
            'interval_unit_key' => 'market_hub_local_history_sync_interval_unit',
            'default_interval_seconds' => 300,
            'label' => 'Local History',
        ],
        'killmail_r2z2_sync' => [
            'enabled_key' => 'killmail_r2z2_sync_enabled',
            'interval_value_key' => 'killmail_r2z2_sync_interval_value',
            'interval_unit_key' => 'killmail_r2z2_sync_interval_unit',
            'default_interval_seconds' => 60,
            'label' => 'Killmail R2Z2 Stream',
        ],
    ];
}

function sync_schedule_settings_view_model(): array
{
    $definitions = data_sync_schedule_job_definitions();

    try {
        $rows = db_sync_schedule_fetch_by_job_keys(array_keys($definitions));
    } catch (Throwable) {
        $rows = [];
    }

    $byJobKey = [];

    foreach ($rows as $row) {
        $jobKey = (string) ($row['job_key'] ?? '');
        if ($jobKey === '') {
            continue;
        }

        $byJobKey[$jobKey] = $row;
    }

    $viewModel = [];

    foreach ($definitions as $jobKey => $definition) {
        $defaultInterval = (int) $definition['default_interval_seconds'];
        $row = $byJobKey[$jobKey] ?? null;

        $enabled = $row === null
            ? 1
            : sanitize_sync_schedule_enabled($row['enabled'] ?? 1);

        $intervalSeconds = $row === null
            ? $defaultInterval
            : sanitize_sync_interval_value($row['interval_seconds'] ?? $defaultInterval, $defaultInterval);

        $formValues = sync_schedule_form_values($intervalSeconds);

        $viewModel[$jobKey] = [
            'job_key' => $jobKey,
            'label' => (string) ($definition['label'] ?? $jobKey),
            'enabled' => $enabled,
            'interval_seconds' => $intervalSeconds,
            'interval_value' => $formValues['value'],
            'interval_unit' => $formValues['unit'],
            'enabled_key' => (string) $definition['enabled_key'],
            'interval_value_key' => (string) $definition['interval_value_key'],
            'interval_unit_key' => (string) $definition['interval_unit_key'],
        ];
    }

    return $viewModel;
}

function save_data_sync_schedule_settings(array $request): bool
{
    try {
        db_transaction(function () use ($request): void {
            foreach (data_sync_schedule_job_definitions() as $jobKey => $definition) {
                $enabled = sanitize_sync_schedule_enabled($request[$definition['enabled_key']] ?? null);
                $intervalSeconds = sanitize_sync_schedule_interval_seconds(
                    $request[$definition['interval_value_key']] ?? null,
                    $request[$definition['interval_unit_key']] ?? null,
                    (int) $definition['default_interval_seconds']
                );

                db_sync_schedule_upsert($jobKey, $enabled, $intervalSeconds);
            }
        });
    } catch (Throwable) {
        return false;
    }

    return true;
}

function data_sync_settings_from_request(array $request): array
{
    $allianceCurrentEnabled = sanitize_pipeline_enabled($request['alliance_current_pipeline_enabled'] ?? null);
    $allianceHistoryEnabled = sanitize_pipeline_enabled($request['alliance_history_pipeline_enabled'] ?? null);
    $hubHistoryEnabled = sanitize_pipeline_enabled($request['hub_history_pipeline_enabled'] ?? null);
    $localHistoryEnabled = sanitize_pipeline_enabled($request['market_hub_local_history_pipeline_enabled'] ?? null);

    $baselineDate = sync_automation_enabled_since_date();

    return [
        'incremental_updates_enabled' => sanitize_pipeline_enabled($request['incremental_updates_enabled'] ?? null),
        'incremental_strategy' => sanitize_incremental_strategy($request['incremental_strategy'] ?? null),
        'incremental_delete_policy' => sanitize_incremental_delete_policy($request['incremental_delete_policy'] ?? null),
        'incremental_chunk_size' => sanitize_incremental_chunk_size($request['incremental_chunk_size'] ?? null),
        'alliance_current_pipeline_enabled' => $allianceCurrentEnabled,
        'alliance_history_pipeline_enabled' => $allianceHistoryEnabled,
        'hub_history_pipeline_enabled' => $hubHistoryEnabled,
        'market_hub_local_history_pipeline_enabled' => $localHistoryEnabled,
        'sync_automation_enabled_since' => $baselineDate,
        'alliance_current_backfill_start_date' => data_sync_backfill_start_date('alliance_current_backfill_start_date', $allianceCurrentEnabled === '1', $baselineDate),
        'alliance_history_backfill_start_date' => data_sync_backfill_start_date('alliance_history_backfill_start_date', $allianceHistoryEnabled === '1', $baselineDate),
        'hub_history_backfill_start_date' => data_sync_backfill_start_date('hub_history_backfill_start_date', $hubHistoryEnabled === '1', $baselineDate),
        'raw_order_snapshot_retention_days' => sanitize_retention_days($request['raw_order_snapshot_retention_days'] ?? null),
        'static_data_source_url' => sanitize_static_data_source_url($request['static_data_source_url'] ?? null),
    ];
}

function market_compare_setting_float(string $key, float $default, float $min, float $max): float
{
    $raw = get_setting($key, (string) $default);
    $value = (float) $raw;

    return max($min, min($max, $value));
}

function market_compare_setting_int(string $key, int $default, int $min, int $max): int
{
    $raw = get_setting($key, (string) $default);
    $value = (int) $raw;

    return max($min, min($max, $value));
}

function market_compare_thresholds(): array
{
    return [
        'deviation_percent' => market_compare_setting_float('market_compare_deviation_percent', 5.0, 0.1, 100.0),
        'min_alliance_sell_volume' => market_compare_setting_int('market_compare_min_alliance_sell_volume', 50, 0, 5000000),
        'min_alliance_sell_orders' => market_compare_setting_int('market_compare_min_alliance_sell_orders', 3, 0, 100000),
    ];
}


function market_comparison_context(): array
{
    return [
        'alliance_destination' => selected_station_name('alliance_station_id') ?? 'Operational destination not configured',
        'reference_hub' => market_hub_reference_name(),
    ];
}

function market_compare_aggregates(array $typeIds = []): array
{
    $allianceStructureId = configured_structure_destination_id_for_esi_sync();
    $marketHubRef = market_hub_setting_reference();
    $referenceSourceId = sync_source_id_from_hub_ref($marketHubRef);

    if ($allianceStructureId <= 0 || $referenceSourceId <= 0) {
        return [];
    }

    try {
        return db_market_orders_current_alliance_vs_reference_aggregates($allianceStructureId, $referenceSourceId, $typeIds);
    } catch (Throwable) {
        return [];
    }
}

function market_compare_evaluate_row(array $row, array $thresholds): array
{
    $alliancePrice = isset($row['alliance_best_sell_price']) ? (float) $row['alliance_best_sell_price'] : null;
    $referencePrice = isset($row['reference_best_sell_price']) ? (float) $row['reference_best_sell_price'] : null;
    $allianceSellVolume = (int) ($row['alliance_total_sell_volume'] ?? 0);
    $allianceSellOrders = (int) ($row['alliance_sell_order_count'] ?? 0);

    $inBothMarkets = $alliancePrice !== null && $referencePrice !== null;
    $missingInAlliance = $alliancePrice === null && $referencePrice !== null;

    $deviationPercent = 0.0;
    if ($inBothMarkets && $referencePrice > 0) {
        $deviationPercent = (($alliancePrice - $referencePrice) / $referencePrice) * 100.0;
    }

    $overpriced = $inBothMarkets && $deviationPercent >= $thresholds['deviation_percent'];
    $underpriced = $inBothMarkets && $deviationPercent <= -$thresholds['deviation_percent'];
    $weakAllianceStock = $allianceSellVolume < $thresholds['min_alliance_sell_volume'] || $allianceSellOrders < $thresholds['min_alliance_sell_orders'];

    $stockScore = market_score_stock_urgency($allianceSellVolume, $allianceSellOrders, $thresholds, $missingInAlliance);
    $priceDeltaScore = min(100, (int) round(abs($deviationPercent) * 2.2));
    $volumeScore = market_score_reference_activity((int) ($row['reference_total_sell_volume'] ?? 0));
    $freshnessScore = market_score_data_freshness((string) ($row['alliance_last_observed_at'] ?? ''), (string) ($row['reference_last_observed_at'] ?? ''));
    $stalenessScore = 100 - $freshnessScore;

    $opportunityPressure = $underpriced || $missingInAlliance ? $priceDeltaScore : (int) round($priceDeltaScore * 0.35);
    $riskPressure = $overpriced ? $priceDeltaScore : (int) round($priceDeltaScore * 0.25);

    $opportunityScore = (int) round(($stockScore * 0.35) + ($opportunityPressure * 0.35) + ($volumeScore * 0.2) + ($freshnessScore * 0.1));
    $riskScore = (int) round(($stockScore * 0.4) + ($riskPressure * 0.3) + ($stalenessScore * 0.2) + ($volumeScore * 0.1));

    if ($missingInAlliance) {
        $opportunityScore += 12;
        $riskScore += 15;
    }

    if ($weakAllianceStock && !$missingInAlliance) {
        $opportunityScore += 8;
        $riskScore += 10;
    }

    $opportunityScore = min(100, max(0, $opportunityScore));
    $riskScore = min(100, max(0, $riskScore));

    return [
        'type_id' => (int) ($row['type_id'] ?? 0),
        'type_name' => (string) ($row['type_name'] ?? ''),
        'alliance_best_sell_price' => $alliancePrice,
        'alliance_best_buy_price' => isset($row['alliance_best_buy_price']) ? (float) $row['alliance_best_buy_price'] : null,
        'alliance_total_sell_volume' => $allianceSellVolume,
        'alliance_total_buy_volume' => (int) ($row['alliance_total_buy_volume'] ?? 0),
        'alliance_sell_order_count' => $allianceSellOrders,
        'alliance_buy_order_count' => (int) ($row['alliance_buy_order_count'] ?? 0),
        'alliance_last_observed_at' => $row['alliance_last_observed_at'] ?? null,
        'reference_best_sell_price' => $referencePrice,
        'reference_best_buy_price' => isset($row['reference_best_buy_price']) ? (float) $row['reference_best_buy_price'] : null,
        'reference_total_sell_volume' => (int) ($row['reference_total_sell_volume'] ?? 0),
        'reference_total_buy_volume' => (int) ($row['reference_total_buy_volume'] ?? 0),
        'reference_sell_order_count' => (int) ($row['reference_sell_order_count'] ?? 0),
        'reference_buy_order_count' => (int) ($row['reference_buy_order_count'] ?? 0),
        'reference_last_observed_at' => $row['reference_last_observed_at'] ?? null,
        'in_both_markets' => $inBothMarkets,
        'missing_in_alliance' => $missingInAlliance,
        'overpriced_in_alliance' => $overpriced,
        'underpriced_in_alliance' => $underpriced,
        'weak_alliance_stock' => $weakAllianceStock,
        'deviation_percent' => $deviationPercent,
        'stock_score' => $stockScore,
        'price_delta_score' => $priceDeltaScore,
        'volume_score' => $volumeScore,
        'freshness_score' => $freshnessScore,
        'opportunity_score' => $opportunityScore,
        'risk_score' => $riskScore,
        'severity' => market_signal_severity(max($opportunityScore, $riskScore)),
        'opportunity_tier' => market_signal_severity($opportunityScore),
    ];
}

function market_score_stock_urgency(int $sellVolume, int $sellOrders, array $thresholds, bool $missingInAlliance): int
{
    if ($missingInAlliance) {
        return 100;
    }

    $volumeFloor = max(1, (int) ($thresholds['min_alliance_sell_volume'] ?? 50));
    $orderFloor = max(1, (int) ($thresholds['min_alliance_sell_orders'] ?? 3));

    $volumeCoverage = min(1.0, $sellVolume / ($volumeFloor * 2));
    $orderCoverage = min(1.0, $sellOrders / ($orderFloor * 2));

    return min(100, max(0, (int) round((1 - (($volumeCoverage * 0.7) + ($orderCoverage * 0.3))) * 100)));
}

function market_score_reference_activity(int $referenceVolume): int
{
    if ($referenceVolume <= 0) {
        return 0;
    }

    return min(100, (int) round(log10($referenceVolume + 1) * 25));
}

function market_score_data_freshness(string $allianceObservedAt, string $referenceObservedAt): int
{
    $allianceUnix = strtotime($allianceObservedAt);
    $referenceUnix = strtotime($referenceObservedAt);
    $latestUnix = max($allianceUnix ?: 0, $referenceUnix ?: 0);

    if ($latestUnix <= 0) {
        return 20;
    }

    $ageHours = max(0.0, (time() - $latestUnix) / 3600);
    $score = 100 - (int) round($ageHours * 8);

    return min(100, max(10, $score));
}

function market_signal_severity(int $score): string
{
    if ($score >= 75) {
        return 'High';
    }
    if ($score >= 45) {
        return 'Medium';
    }

    return 'Low';
}

function market_comparison_outcomes(array $typeIds = []): array
{
    $thresholds = market_compare_thresholds();
    $evaluatedRows = [];

    foreach (market_compare_aggregates($typeIds) as $row) {
        $evaluatedRows[] = market_compare_evaluate_row($row, $thresholds);
    }

    return [
        'thresholds' => $thresholds,
        'rows' => $evaluatedRows,
        'in_both_markets' => array_values(array_filter($evaluatedRows, static fn (array $row): bool => (bool) $row['in_both_markets'])),
        'missing_in_alliance' => array_values(array_filter($evaluatedRows, static fn (array $row): bool => (bool) $row['missing_in_alliance'])),
        'overpriced_in_alliance' => array_values(array_filter($evaluatedRows, static fn (array $row): bool => (bool) $row['overpriced_in_alliance'])),
        'underpriced_in_alliance' => array_values(array_filter($evaluatedRows, static fn (array $row): bool => (bool) $row['underpriced_in_alliance'])),
        'weak_or_missing_alliance_stock' => array_values(array_filter($evaluatedRows, static fn (array $row): bool => (bool) $row['weak_alliance_stock'] || (bool) $row['missing_in_alliance'])),
    ];
}

function market_comparison_top_rows(callable $predicate, string $sortKey, int $limit = 25): array
{
    $rows = array_values(array_filter(market_comparison_outcomes()['rows'], $predicate));
    usort($rows, static function (array $a, array $b) use ($sortKey): int {
        return ($b[$sortKey] ?? 0) <=> ($a[$sortKey] ?? 0);
    });

    return array_slice($rows, 0, max(1, $limit));
}

function market_format_isk(?float $price): string
{
    if ($price === null || $price <= 0) {
        return '—';
    }

    return number_format($price, 2, '.', ',') . ' ISK';
}

function market_format_percentage(float $value, int $precision = 1): string
{
    return number_format($value, $precision, '.', ',') . '%';
}

function dashboard_sync_dataset_active_error(array $states): ?string
{
    foreach ($states as $state) {
        if (!is_array($state) || (string) ($state['status'] ?? '') !== 'failed') {
            continue;
        }

        $message = trim((string) ($state['last_error_message'] ?? ''));
        if ($message !== '') {
            return $message;
        }
    }

    return null;
}

function dashboard_sync_health_panel(array $dataset): array
{
    $states = $dataset['states'] ?? [];
    $runs = $dataset['runs'] ?? [];
    $activeStateCount = count($states);
    $latestRun = is_array($runs[0] ?? null) ? $runs[0] : null;
    $activeError = dashboard_sync_dataset_active_error($states);
    $latestRunFailed = is_array($latestRun) && (string) ($latestRun['run_status'] ?? '') === 'failed';

    $status = 'Not synced';
    if ($activeStateCount > 0) {
        $status = ($activeError !== null || $latestRunFailed) ? 'Warning' : 'Healthy';
    }

    $lastSuccessAt = (string) ($dataset['last_success_at'] ?? '');
    $lastError = $activeError;
    if ($lastError === null && $latestRunFailed) {
        $candidate = trim((string) ($latestRun['error_message'] ?? ''));
        if ($candidate !== '') {
            $lastError = $candidate;
        }
    }

    return [
        'status' => $status,
        'last_success_at' => $lastSuccessAt !== '' ? $lastSuccessAt : 'No successful sync yet',
        'recent_rows_written' => (int) ($dataset['recent_rows_written'] ?? 0),
        'last_error' => $lastError !== null ? $lastError : 'None',
        'state_count' => $activeStateCount,
    ];
}

function dashboard_trend_snippets(array $rows): array
{
    $grouped = [];
    foreach ($rows as $row) {
        $typeId = (int) ($row['type_id'] ?? 0);
        if ($typeId <= 0) {
            continue;
        }

        if (!isset($grouped[$typeId])) {
            $grouped[$typeId] = [
                'type_id' => $typeId,
                'type_name' => (string) ($row['type_name'] ?? ''),
                'points' => [],
            ];
        }

        $grouped[$typeId]['points'][] = [
            'trade_date' => (string) ($row['trade_date'] ?? ''),
            'close_price' => (float) ($row['close_price'] ?? 0),
            'volume' => (int) ($row['volume'] ?? 0),
        ];
    }

    $snippets = [];
    foreach ($grouped as $series) {
        $points = array_values(array_filter($series['points'], static fn (array $point): bool => $point['trade_date'] !== '' && $point['close_price'] > 0));
        if (count($points) < 2) {
            continue;
        }

        usort($points, static fn (array $a, array $b): int => strcmp($b['trade_date'], $a['trade_date']));
        $latest = $points[0];
        $previous = $points[1];
        if ($previous['close_price'] <= 0) {
            continue;
        }

        $changePercent = (($latest['close_price'] - $previous['close_price']) / $previous['close_price']) * 100.0;
        $direction = $changePercent > 0.5 ? 'Up' : ($changePercent < -0.5 ? 'Down' : 'Flat');

        $snippets[] = [
            'module' => $series['type_name'] !== '' ? $series['type_name'] : ('Type #' . $series['type_id']),
            'movement' => sprintf('%+.1f%%', $changePercent),
            'direction' => $direction,
            'latest_close' => market_format_isk($latest['close_price']),
            'latest_volume' => (string) $latest['volume'],
        ];
    }

    usort($snippets, static fn (array $a, array $b): int => abs((float) $b['movement']) <=> abs((float) $a['movement']));

    return array_slice($snippets, 0, 6);
}

function market_hub_dashboard_trend_legacy_history_fallback_enabled(): bool
{
    return sanitize_enabled_flag(get_setting('dashboard_trend_legacy_history_fallback_enabled', '0')) === '1';
}

function dashboard_trend_history_dataset(string $marketHubRef, int $referenceSourceId): array
{
    $historyRows = [];
    $message = '';

    if ($marketHubRef === '' || $referenceSourceId <= 0) {
        return [
            'rows' => [],
            'message' => 'Trend snippets need a reference hub. Configure Settings → General first, then run the local-history sync job for that hub.',
        ];
    }

    try {
        $historyRows = db_market_hub_local_history_daily_latest_points_by_type(market_hub_local_history_source(), $referenceSourceId, [], 2, 80);
    } catch (Throwable) {
        $historyRows = [];
    }

    if ($historyRows !== []) {
        return [
            'rows' => $historyRows,
            'message' => '',
        ];
    }

    $message = 'Trend snippets use local hub history. Run the local-history sync job for ' . market_hub_reference_name() . ' and capture at least two daily snapshots; EVE Tycoon history sync does not populate this panel.';

    if (market_hub_dashboard_trend_legacy_history_fallback_enabled()) {
        try {
            $historyRows = db_market_history_daily_recent_window('market_hub', $referenceSourceId, 8, 80);
        } catch (Throwable) {
            $historyRows = [];
        }

        if ($historyRows !== []) {
            $message = 'Trend snippets are temporarily using legacy hub history fallback. Run the local-history sync job to migrate this panel to market_hub_local_history_daily.';
        }
    }

    return [
        'rows' => $historyRows,
        'message' => $message,
    ];
}

function dashboard_intelligence_data(): array
{
    $comparisonContext = market_comparison_context();
    $outcomes = market_comparison_outcomes();
    $rows = $outcomes['rows'];

    $rowCount = count($rows);
    $inBothCount = count($outcomes['in_both_markets']);
    $coveragePercent = $rowCount > 0 ? ($inBothCount / $rowCount) * 100.0 : 0.0;

    $opportunities = $rows;
    usort($opportunities, static fn (array $a, array $b): int => (($b['opportunity_score'] ?? 0) <=> ($a['opportunity_score'] ?? 0)) ?: (($b['volume_score'] ?? 0) <=> ($a['volume_score'] ?? 0)));
    $risks = $rows;
    usort($risks, static fn (array $a, array $b): int => (($b['risk_score'] ?? 0) <=> ($a['risk_score'] ?? 0)) ?: (($b['stock_score'] ?? 0) <=> ($a['stock_score'] ?? 0)));

    $allianceSync = sync_status_from_prefix('alliance.structure.');
    $hubCurrentSync = sync_status_from_prefix('market.hub.');
    $historySync = sync_status_from_prefix('market.history.daily.');

    $marketHubRef = market_hub_setting_reference();
    $referenceSourceId = sync_source_id_from_hub_ref($marketHubRef);
    $trendHistory = dashboard_trend_history_dataset($marketHubRef, $referenceSourceId);
    $historyRows = $trendHistory['rows'] ?? [];

    return [
        'kpis' => [
            ['label' => 'Top Opportunities', 'value' => (string) count(array_filter($rows, static fn (array $row): bool => (int) ($row['opportunity_score'] ?? 0) >= 60)), 'context' => 'High-confidence import/reprice candidates'],
            ['label' => 'Top Risks', 'value' => (string) count(array_filter($rows, static fn (array $row): bool => (int) ($row['risk_score'] ?? 0) >= 60)), 'context' => 'High-severity pricing or stock risk'],
            ['label' => 'Missing Seed Targets', 'value' => (string) count($outcomes['missing_in_alliance']), 'context' => 'Items in ' . $comparisonContext['reference_hub'] . ' not listed in alliance'],
            ['label' => 'Overlap Coverage', 'value' => market_format_percentage($coveragePercent), 'context' => $rowCount > 0 ? ($inBothCount . ' of ' . $rowCount . ' items in both markets') : 'No market overlap data yet'],
        ],
        'priority_queues' => [
            'opportunities' => array_map(static fn (array $row): array => [
                'module' => $row['type_name'] !== '' ? $row['type_name'] : ('Type #' . $row['type_id']),
                'signal' => (bool) ($row['missing_in_alliance'] ?? false) ? 'Import seed candidate' : sprintf('%+.1f%% vs hub', (float) ($row['deviation_percent'] ?? 0.0)),
                'score' => (int) ($row['opportunity_score'] ?? 0),
            ], array_slice(array_values(array_filter($opportunities, static fn (array $row): bool => (int) ($row['opportunity_score'] ?? 0) > 0)), 0, 5)),
            'risks' => array_map(static fn (array $row): array => [
                'module' => $row['type_name'] !== '' ? $row['type_name'] : ('Type #' . $row['type_id']),
                'signal' => (bool) ($row['overpriced_in_alliance'] ?? false) ? sprintf('%+.1f%% overpriced', (float) ($row['deviation_percent'] ?? 0.0)) : 'Stock or freshness risk',
                'score' => (int) ($row['risk_score'] ?? 0),
            ], array_slice(array_values(array_filter($risks, static fn (array $row): bool => (int) ($row['risk_score'] ?? 0) > 0)), 0, 5)),
            'missing_items' => array_map(static fn (array $row): array => [
                'module' => $row['type_name'] !== '' ? $row['type_name'] : ('Type #' . $row['type_id']),
                'signal' => 'Volume ' . (string) ($row['reference_total_sell_volume'] ?? 0) . ' · score ' . (string) ($row['opportunity_score'] ?? 0),
                'score' => (int) ($row['opportunity_score'] ?? 0),
            ], array_slice(array_values(array_filter($opportunities, static fn (array $row): bool => (bool) ($row['missing_in_alliance'] ?? false))), 0, 5)),
        ],
        'health_panels' => [
            'alliance_freshness' => dashboard_sync_health_panel($allianceSync),
            'sync_health' => dashboard_sync_health_panel($hubCurrentSync),
            'data_completeness' => [
                'status' => $rowCount > 0 ? 'Tracked' : 'Awaiting sync',
                'rows_compared' => $rowCount,
                'history_points' => count($historyRows),
                'history_sync' => dashboard_sync_health_panel($historySync),
            ],
        ],
        'trend_snippets' => dashboard_trend_snippets($historyRows),
        'trend_snippets_message' => (string) ($trendHistory['message'] ?? ''),
    ];
}

function current_alliance_market_status_data(): array
{
    $outcomes = market_comparison_outcomes();
    $thresholds = $outcomes['thresholds'] ?? [];
    $lowStockThreshold = max(1, (int) ($thresholds['min_alliance_sell_volume'] ?? 25));
    $staleAfterSeconds = 6 * 3600;

    $search = trim((string) ($_GET['q'] ?? ''));
    $searchNeedle = mb_strtolower($search);
    $sort = (string) ($_GET['sort'] ?? 'urgency');
    $allowedSort = ['urgency', 'stock', 'price_delta', 'score'];
    if (!in_array($sort, $allowedSort, true)) {
        $sort = 'urgency';
    }

    $allowedPageSizes = [25, 50, 100];
    $pageSize = (int) ($_GET['page_size'] ?? 25);
    if (!in_array($pageSize, $allowedPageSizes, true)) {
        $pageSize = 25;
    }

    $page = max(1, (int) ($_GET['page'] ?? 1));

    $rows = array_values(array_filter($outcomes['rows'], static function (array $row) use ($searchNeedle): bool {
        if (($row['alliance_best_sell_price'] ?? null) === null) {
            return false;
        }

        if ($searchNeedle === '') {
            return true;
        }

        return str_contains(mb_strtolower((string) ($row['type_name'] ?? '')), $searchNeedle);
    }));

    usort($rows, static function (array $a, array $b) use ($sort): int {
        return match ($sort) {
            'stock' => (($a['alliance_total_sell_volume'] ?? 0) <=> ($b['alliance_total_sell_volume'] ?? 0)) ?: (($b['risk_score'] ?? 0) <=> ($a['risk_score'] ?? 0)),
            'price_delta' => (abs((float) ($b['deviation_percent'] ?? 0.0)) <=> abs((float) ($a['deviation_percent'] ?? 0.0))) ?: (($b['opportunity_score'] ?? 0) <=> ($a['opportunity_score'] ?? 0)),
            'score', 'urgency' => (($b['risk_score'] ?? 0) <=> ($a['risk_score'] ?? 0)) ?: (($b['stock_score'] ?? 0) <=> ($a['stock_score'] ?? 0)),
            default => strcmp((string) ($a['type_name'] ?? ''), (string) ($b['type_name'] ?? '')),
        };
    });

    $totalItems = count($rows);
    $totalPages = max(1, (int) ceil($totalItems / $pageSize));
    $page = min($page, $totalPages);
    $offset = ($page - 1) * $pageSize;
    $pagedRows = array_slice($rows, $offset, $pageSize);

    $criticalItems = array_slice(array_values(array_filter($rows, static fn (array $row): bool => (int) ($row['risk_score'] ?? 0) >= 70 || (int) ($row['stock_score'] ?? 0) >= 80)), 0, 5);
    $allListings = array_values(array_filter($outcomes['rows'], static fn (array $row): bool => ($row['alliance_best_sell_price'] ?? null) !== null));
    $listingsWithStock = count(array_filter($allListings, static fn (array $row): bool => (int) ($row['alliance_total_sell_volume'] ?? 0) > 0));
    $lowStockCount = count(array_filter($allListings, static fn (array $row): bool => (int) ($row['alliance_total_sell_volume'] ?? 0) > 0 && (int) ($row['alliance_total_sell_volume'] ?? 0) < $lowStockThreshold));

    return [
        'summary' => [
            ['label' => 'Tracked Modules', 'value' => (string) count($outcomes['rows']), 'context' => 'Items monitored in current sync'],
            ['label' => 'Listings with Stock', 'value' => (string) $listingsWithStock, 'context' => 'Items with active alliance destination volume'],
            ['label' => 'Low Stock Count', 'value' => (string) $lowStockCount, 'context' => 'Listings below operational threshold (' . $lowStockThreshold . ')'],
            ['label' => 'Critical Restocks', 'value' => (string) count($criticalItems), 'context' => 'High urgency restock signals'],
        ],
        'highlights' => [
            'title' => 'Critical Items',
            'rows' => array_map(static fn (array $row): array => [
                'module' => $row['type_name'] !== '' ? $row['type_name'] : ('Type #' . $row['type_id']),
                'signal' => 'Stock ' . (string) ($row['alliance_total_sell_volume'] ?? 0) . ' · urgency ' . (string) ($row['risk_score'] ?? 0),
                'score' => (int) ($row['risk_score'] ?? 0),
            ], $criticalItems),
        ],
        'rows' => array_map(static function (array $row) use ($lowStockThreshold, $staleAfterSeconds): array {
            $stock = (int) ($row['alliance_total_sell_volume'] ?? 0);
            $observedAtRaw = (string) ($row['alliance_last_observed_at'] ?? '');
            $observedUnix = strtotime($observedAtRaw);
            $isStale = $observedUnix === false || (time() - $observedUnix) > $staleAfterSeconds;
            $riskScore = (int) ($row['risk_score'] ?? 0);

            return [
                'module' => $row['type_name'] !== '' ? $row['type_name'] : ('Type #' . $row['type_id']),
                'price' => market_format_isk($row['alliance_best_sell_price']),
                'stock' => (string) $stock,
                'restock_priority' => (string) $riskScore,
                'price_delta' => sprintf('%+.1f%%', (float) ($row['deviation_percent'] ?? 0.0)),
                'score' => (string) ((int) ($row['opportunity_score'] ?? 0)),
                'updated_at' => $observedAtRaw !== '' ? $observedAtRaw : '—',
                'stock_state' => $stock < $lowStockThreshold ? 'low' : 'healthy',
                'freshness_state' => $isStale ? 'stale' : 'fresh',
                'severity' => market_signal_severity($riskScore),
                'row_tone' => $riskScore >= 75 ? 'risk_high' : ($riskScore >= 45 ? 'risk_medium' : 'risk_low'),
            ];
        }, $pagedRows),
        'pagination' => [
            'search' => $search,
            'sort' => $sort,
            'sort_options' => [
                'urgency' => 'Urgency',
                'score' => 'Score',
                'price_delta' => 'Price delta',
                'stock' => 'Stock',
            ],
            'page_size' => $pageSize,
            'page_size_options' => $allowedPageSizes,
            'page' => $page,
            'total_pages' => $totalPages,
            'total_items' => $totalItems,
            'showing_from' => $totalItems > 0 ? $offset + 1 : 0,
            'showing_to' => min($offset + $pageSize, $totalItems),
        ],
    ];
}

function killmail_format_datetime(?string $value): string
{
    $trimmed = trim((string) $value);
    if ($trimmed === '') {
        return '—';
    }

    try {
        $date = new DateTimeImmutable($trimmed, new DateTimeZone('UTC'));
        $timezone = new DateTimeZone(app_timezone());

        return $date->setTimezone($timezone)->format('Y-m-d H:i:s T');
    } catch (Throwable) {
        return $trimmed;
    }
}

function killmail_relative_datetime(?string $value): string
{
    $trimmed = trim((string) $value);
    if ($trimmed === '') {
        return 'Never';
    }

    try {
        $date = new DateTimeImmutable($trimmed, new DateTimeZone('UTC'));
        $age = time() - $date->getTimestamp();

        return $age <= 0 ? 'just now' : (human_duration_ago($age) . ' ago');
    } catch (Throwable) {
        return 'Unknown';
    }
}

function killmail_last_sync_outcome_label(?array $latestRun): string
{
    if (!is_array($latestRun) || $latestRun === []) {
        return 'No sync runs yet';
    }

    $status = (string) ($latestRun['run_status'] ?? 'unknown');
    if ($status !== 'success') {
        return 'Failed';
    }

    $writtenRows = (int) ($latestRun['written_rows'] ?? 0);
    if ($writtenRows > 0) {
        return 'Inserted ' . number_format($writtenRows) . ' new killmail' . ($writtenRows === 1 ? '' : 's');
    }

    return 'No-op';
}

function killmail_match_sources(array $row): array
{
    $sources = [];

    if ((int) ($row['matches_victim_alliance'] ?? 0) === 1) {
        $sources[] = 'tracked victim alliance';
    }

    if ((int) ($row['matches_victim_corporation'] ?? 0) === 1) {
        $sources[] = 'tracked victim corporation';
    }

    return $sources;
}

function killmail_entity_display_name(?string $label, string $fallbackPrefix, ?int $id): string
{
    $label = trim((string) $label);
    if ($label !== '') {
        return $label;
    }

    if ($id !== null && $id > 0) {
        return $fallbackPrefix . ' ' . number_format($id);
    }

    return $fallbackPrefix . ' unavailable';
}

function killmail_entity_label_is_id_fallback(?string $label, string $fallbackPrefix, ?int $id): bool
{
    $entityId = (int) $id;
    $normalized = trim((string) $label);
    if ($normalized === '' || $entityId <= 0) {
        return false;
    }

    $candidates = [
        $fallbackPrefix . ' #' . $entityId,
        $fallbackPrefix . ' ' . number_format($entityId),
        $fallbackPrefix . ' #' . number_format($entityId),
    ];

    return in_array($normalized, $candidates, true);
}

function killmail_entity_preferred_name(array $resolvedEntities, string $type, ?int $id, ?string $label, string $fallbackPrefix): string
{
    $entityId = (int) $id;
    $normalizedLabel = trim((string) $label);
    $resolvedName = '';

    if ($entityId > 0) {
        $resolvedName = trim((string) ($resolvedEntities[$type][$entityId]['name'] ?? ''));
        if ($resolvedName !== '' && !str_starts_with($resolvedName, 'Unknown ')) {
            return $resolvedName;
        }
    }

    if ($normalizedLabel !== '' && !killmail_entity_label_is_id_fallback($normalizedLabel, $fallbackPrefix, $entityId)) {
        return $normalizedLabel;
    }

    return killmail_entity_display_name($normalizedLabel, $fallbackPrefix, $entityId > 0 ? $entityId : null);
}

function killmail_secondary_id(?int $id, string $prefix = 'ID'): string
{
    if ($id === null || $id <= 0) {
        return $prefix . ' unavailable';
    }

    return $prefix . ' ' . number_format($id);
}

function killmail_decode_json_array(?string $json): array
{
    $json = trim((string) $json);
    if ($json === '') {
        return [];
    }

    $decoded = json_decode($json, true);

    return is_array($decoded) ? $decoded : [];
}

function killmail_item_role_label(string $role): string
{
    return match ($role) {
        'dropped' => 'Dropped items',
        'destroyed' => 'Destroyed items',
        'fitted' => 'Fitted items',
        default => 'Other stored items',
    };
}

function killmail_entity_cacheable_types(): array
{
    return ['alliance', 'corporation', 'character', 'type', 'system', 'region'];
}

function killmail_entity_type_label_human(string $entityType): string
{
    return match ($entityType) {
        'alliance' => 'Alliance',
        'corporation' => 'Corporation',
        'character' => 'Character',
        'type' => 'Type',
        'system' => 'System',
        'region' => 'Region',
        default => 'Entity',
    };
}

function killmail_entity_image_url(string $entityType, ?int $entityId, string $variation = 'default', int $size = 128): ?string
{
    $id = (int) $entityId;
    if ($id <= 0) {
        return null;
    }

    $category = null;
    $resolvedVariation = $variation;
    $resolvedSize = max(32, min(1024, $size));

    switch ($entityType) {
        case 'alliance':
            $category = 'alliances';
            $resolvedVariation = $variation === 'default' ? 'logo' : $variation;
            break;
        case 'corporation':
            $category = 'corporations';
            $resolvedVariation = $variation === 'default' ? 'logo' : $variation;
            break;
        case 'character':
            $category = 'characters';
            $resolvedVariation = $variation === 'default' ? 'portrait' : $variation;
            break;
        case 'type':
            $category = 'types';
            $resolvedVariation = $variation === 'default' ? 'icon' : $variation;
            break;
    }

    if ($category === null) {
        return null;
    }

    return sprintf(
        'https://images.evetech.net/%s/%d/%s?size=%d',
        rawurlencode($category),
        $id,
        rawurlencode($resolvedVariation),
        $resolvedSize
    );
}

function killmail_entity_cache_ttl(string $entityType): ?string
{
    if (in_array($entityType, ['type', 'system', 'region'], true)) {
        return null;
    }

    return gmdate('Y-m-d H:i:s', strtotime('+30 days'));
}

function killmail_entity_resolution_requests(array $event, array $attackers = [], array $items = []): array
{
    $requests = array_fill_keys(killmail_entity_cacheable_types(), []);

    $add = static function (string $type, ?int $id) use (&$requests): void {
        $entityId = (int) $id;
        if (!isset($requests[$type]) || $entityId <= 0) {
            return;
        }

        $requests[$type][$entityId] = $entityId;
    };

    $add('character', isset($event['victim_character_id']) ? (int) $event['victim_character_id'] : null);
    $add('corporation', isset($event['victim_corporation_id']) ? (int) $event['victim_corporation_id'] : null);
    $add('alliance', isset($event['victim_alliance_id']) ? (int) $event['victim_alliance_id'] : null);
    $add('type', isset($event['victim_ship_type_id']) ? (int) $event['victim_ship_type_id'] : null);
    $add('system', isset($event['solar_system_id']) ? (int) $event['solar_system_id'] : null);
    $add('region', isset($event['region_id']) ? (int) $event['region_id'] : null);

    foreach ($attackers as $attacker) {
        if (!is_array($attacker)) {
            continue;
        }

        $add('character', isset($attacker['character_id']) ? (int) $attacker['character_id'] : null);
        $add('corporation', isset($attacker['corporation_id']) ? (int) $attacker['corporation_id'] : null);
        $add('alliance', isset($attacker['alliance_id']) ? (int) $attacker['alliance_id'] : null);
        $add('type', isset($attacker['ship_type_id']) ? (int) $attacker['ship_type_id'] : null);
        $add('type', isset($attacker['weapon_type_id']) ? (int) $attacker['weapon_type_id'] : null);
    }

    foreach ($items as $item) {
        if (!is_array($item)) {
            continue;
        }

        $add('type', isset($item['item_type_id']) ? (int) $item['item_type_id'] : null);
    }

    foreach ($requests as $type => $ids) {
        $requests[$type] = array_values($ids);
    }

    return $requests;
}

function killmail_entity_cache_prefill_from_references(array $requests): void
{
    $upserts = [];

    foreach (db_ref_item_types_by_ids($requests['type'] ?? []) as $row) {
        $typeId = (int) ($row['type_id'] ?? 0);
        if ($typeId <= 0) {
            continue;
        }

        $upserts[] = [
            'entity_type' => 'type',
            'entity_id' => $typeId,
            'entity_name' => trim((string) ($row['type_name'] ?? '')),
            'image_url' => killmail_entity_image_url('type', $typeId, 'icon', 128),
            'metadata_json' => json_encode([
                'group_id' => isset($row['group_id']) ? (int) $row['group_id'] : null,
                'market_group_id' => isset($row['market_group_id']) ? (int) $row['market_group_id'] : null,
                'published' => isset($row['published']) ? (int) $row['published'] : null,
            ], JSON_THROW_ON_ERROR),
            'source_system' => 'local_ref',
            'resolution_status' => 'resolved',
            'expires_at' => null,
            'resolved_at' => gmdate('Y-m-d H:i:s'),
        ];
    }

    foreach (db_ref_systems_by_ids($requests['system'] ?? []) as $row) {
        $systemId = (int) ($row['system_id'] ?? 0);
        if ($systemId <= 0) {
            continue;
        }

        $upserts[] = [
            'entity_type' => 'system',
            'entity_id' => $systemId,
            'entity_name' => trim((string) ($row['system_name'] ?? '')),
            'image_url' => null,
            'metadata_json' => json_encode([
                'region_id' => isset($row['region_id']) ? (int) $row['region_id'] : null,
                'security' => isset($row['security']) ? (float) $row['security'] : null,
            ], JSON_THROW_ON_ERROR),
            'source_system' => 'local_ref',
            'resolution_status' => 'resolved',
            'expires_at' => null,
            'resolved_at' => gmdate('Y-m-d H:i:s'),
        ];
    }

    foreach (db_ref_regions_by_ids($requests['region'] ?? []) as $row) {
        $regionId = (int) ($row['region_id'] ?? 0);
        if ($regionId <= 0) {
            continue;
        }

        $upserts[] = [
            'entity_type' => 'region',
            'entity_id' => $regionId,
            'entity_name' => trim((string) ($row['region_name'] ?? '')),
            'image_url' => null,
            'metadata_json' => null,
            'source_system' => 'local_ref',
            'resolution_status' => 'resolved',
            'expires_at' => null,
            'resolved_at' => gmdate('Y-m-d H:i:s'),
        ];
    }

    if ($upserts !== []) {
        db_entity_metadata_cache_upsert($upserts);
    }
}

function killmail_entity_cache_rows_by_type(array $requests): array
{
    $rowsByType = [];

    foreach ($requests as $type => $ids) {
        $rowsByType[$type] = [];
        foreach (db_entity_metadata_cache_get_many($type, (array) $ids) as $row) {
            $rowsByType[$type][(int) ($row['entity_id'] ?? 0)] = $row;
        }
    }

    return $rowsByType;
}

function killmail_entity_cache_is_current(array $row): bool
{
    if (($row['resolution_status'] ?? '') !== 'resolved') {
        return false;
    }

    $expiresAt = trim((string) ($row['expires_at'] ?? ''));
    if ($expiresAt === '') {
        return true;
    }

    return strtotime($expiresAt) !== false && strtotime($expiresAt) > time();
}

function killmail_entity_public_endpoint(string $entityType, int $entityId): ?string
{
    if ($entityId <= 0) {
        return null;
    }

    return match ($entityType) {
        'alliance' => 'https://esi.evetech.net/latest/alliances/' . $entityId . '/?datasource=tranquility',
        'corporation' => 'https://esi.evetech.net/latest/corporations/' . $entityId . '/?datasource=tranquility',
        'character' => 'https://esi.evetech.net/latest/characters/' . $entityId . '/?datasource=tranquility',
        default => null,
    };
}

function killmail_entity_network_resolve(array $missingByType): void
{
    $dynamicIds = [];
    foreach (['alliance', 'corporation', 'character'] as $type) {
        foreach ((array) ($missingByType[$type] ?? []) as $id) {
            $entityId = (int) $id;
            if ($entityId > 0) {
                $dynamicIds[$entityId] = $entityId;
            }
        }
    }

    $upserts = [];
    $resolvedByType = ['alliance' => [], 'corporation' => [], 'character' => []];

    if ($dynamicIds !== []) {
        try {
            $nameRows = esi_universe_names_lookup(array_values($dynamicIds));
        } catch (Throwable) {
            $nameRows = [];
        }

        foreach ($nameRows as $row) {
            if (!is_array($row)) {
                continue;
            }

            $id = (int) ($row['id'] ?? 0);
            $type = strtolower(trim((string) ($row['category'] ?? '')));
            $name = trim((string) ($row['name'] ?? ''));
            if ($id <= 0 || $name === '' || !isset($resolvedByType[$type])) {
                continue;
            }

            $resolvedByType[$type][$id] = true;
            $upserts[] = [
                'entity_type' => $type,
                'entity_id' => $id,
                'entity_name' => $name,
                'image_url' => killmail_entity_image_url($type, $id),
                'metadata_json' => json_encode(['via' => 'universe_names'], JSON_THROW_ON_ERROR),
                'source_system' => 'esi',
                'resolution_status' => 'resolved',
                'expires_at' => killmail_entity_cache_ttl($type),
                'resolved_at' => gmdate('Y-m-d H:i:s'),
                'last_error_message' => null,
            ];
        }
    }

    foreach (['alliance', 'corporation', 'character'] as $type) {
        foreach ((array) ($missingByType[$type] ?? []) as $id) {
            $entityId = (int) $id;
            if ($entityId <= 0 || isset($resolvedByType[$type][$entityId])) {
                continue;
            }

            $endpoint = killmail_entity_public_endpoint($type, $entityId);
            if ($endpoint === null) {
                continue;
            }

            try {
                $response = http_get_json($endpoint, [
                    'Accept: application/json',
                    'User-Agent: ' . esi_user_agent(),
                ]);
            } catch (Throwable $exception) {
                $response = ['status' => 599, 'json' => [], 'body' => '', 'error_message' => $exception->getMessage()];
            }

            if (($response['status'] ?? 500) >= 400) {
                db_entity_metadata_cache_upsert([[
                    'entity_type' => $type,
                    'entity_id' => $entityId,
                    'entity_name' => null,
                    'image_url' => killmail_entity_image_url($type, $entityId),
                    'metadata_json' => null,
                    'source_system' => 'esi',
                    'resolution_status' => 'failed',
                    'expires_at' => gmdate('Y-m-d H:i:s', strtotime('+6 hours')),
                    'resolved_at' => null,
                    'last_error_message' => 'ESI profile lookup failed with status ' . (int) ($response['status'] ?? 0),
                ]]);
                continue;
            }

            $name = trim((string) ($response['json']['name'] ?? ''));
            if ($name === '') {
                continue;
            }

            $resolvedByType[$type][$entityId] = true;
            $upserts[] = [
                'entity_type' => $type,
                'entity_id' => $entityId,
                'entity_name' => $name,
                'image_url' => killmail_entity_image_url($type, $entityId),
                'metadata_json' => json_encode((array) ($response['json'] ?? []), JSON_THROW_ON_ERROR),
                'source_system' => 'esi',
                'resolution_status' => 'resolved',
                'expires_at' => killmail_entity_cache_ttl($type),
                'resolved_at' => gmdate('Y-m-d H:i:s'),
                'last_error_message' => null,
            ];
        }
    }

    if ($upserts !== []) {
        db_entity_metadata_cache_upsert($upserts);
    }
}

function killmail_entity_placeholder(string $entityType, int $entityId): string
{
    return 'Unknown ' . strtolower(killmail_entity_type_label_human($entityType)) . ' (ID cached for resolution)';
}

function killmail_entity_resolve_batch(array $requests, bool $allowNetworkFallback = false): array
{
    $normalized = [];
    foreach (killmail_entity_cacheable_types() as $type) {
        $ids = array_values(array_unique(array_filter(
            array_map(static fn (mixed $id): int => (int) $id, (array) ($requests[$type] ?? [])),
            static fn (int $id): bool => $id > 0
        )));
        $normalized[$type] = $ids;
    }

    killmail_entity_cache_prefill_from_references($normalized);
    $cacheRowsByType = killmail_entity_cache_rows_by_type($normalized);

    $missingByType = [];
    foreach ($normalized as $type => $ids) {
        $missingByType[$type] = [];
        foreach ($ids as $id) {
            $row = $cacheRowsByType[$type][$id] ?? null;
            if (!is_array($row)) {
                $missingByType[$type][] = $id;
                continue;
            }

            if ($allowNetworkFallback && !killmail_entity_cache_is_current($row)) {
                $missingByType[$type][] = $id;
            }
        }
    }

    if ($allowNetworkFallback) {
        killmail_entity_network_resolve($missingByType);
        $cacheRowsByType = killmail_entity_cache_rows_by_type($normalized);
    }

    $resolved = [];
    foreach ($normalized as $type => $ids) {
        $resolved[$type] = [];
        $stillMissing = [];

        foreach ($ids as $id) {
            $row = $cacheRowsByType[$type][$id] ?? null;
            $name = trim((string) ($row['entity_name'] ?? ''));
            $resolved[$type][$id] = [
                'entity_type' => $type,
                'entity_id' => $id,
                'name' => $name !== '' ? $name : killmail_entity_placeholder($type, $id),
                'image_url' => ($imageUrl = trim((string) ($row['image_url'] ?? ''))) !== '' ? $imageUrl : killmail_entity_image_url($type, $id),
                'status' => $name !== '' ? (string) ($row['resolution_status'] ?? 'resolved') : 'pending',
                'source_system' => (string) ($row['source_system'] ?? ($name !== '' ? 'cache' : 'queue')),
                'metadata' => killmail_decode_json_array(isset($row['metadata_json']) ? (string) $row['metadata_json'] : null),
            ];

            if ($name === '') {
                $stillMissing[] = $id;
            }
        }

        if ($stillMissing !== []) {
            db_entity_metadata_cache_mark_pending($type, $stillMissing);
        }
    }

    return $resolved;
}

function killmail_prime_entity_metadata(array $requests): void
{
    try {
        killmail_entity_resolve_batch($requests, true);
    } catch (Throwable) {
        foreach ($requests as $type => $ids) {
            db_entity_metadata_cache_mark_pending((string) $type, (array) $ids);
        }
    }
}

function killmail_resolved_entity(array $resolved, string $type, ?int $id, ?string $fallbackName = null): array
{
    $entityId = (int) $id;
    $fallback = trim((string) $fallbackName);
    $row = $entityId > 0 ? ($resolved[$type][$entityId] ?? null) : null;
    $name = trim((string) ($row['name'] ?? ''));

    if ($name === '' && $fallback !== '') {
        $name = $fallback;
    }

    if ($name === '') {
        $name = $entityId > 0
            ? killmail_entity_placeholder($type, $entityId)
            : killmail_entity_type_label_human($type) . ' unavailable';
    }

    return [
        'id' => $entityId > 0 ? $entityId : null,
        'name' => $name,
        'status' => (string) ($row['status'] ?? ($entityId > 0 ? 'pending' : 'unavailable')),
        'image_url' => $entityId > 0 ? (($row['image_url'] ?? null) ?: killmail_entity_image_url($type, $entityId)) : null,
        'metadata' => is_array($row['metadata'] ?? null) ? $row['metadata'] : [],
    ];
}

function killmail_loss_item_groups(array $items, array $resolvedEntities = []): array
{
    $groups = [
        'dropped' => ['label' => killmail_item_role_label('dropped'), 'description' => 'Recovered from the wreck and immediately useful for supply planning.', 'rows' => [], 'total_quantity' => 0],
        'destroyed' => ['label' => killmail_item_role_label('destroyed'), 'description' => 'Removed from circulation and relevant for replacement demand.', 'rows' => [], 'total_quantity' => 0],
        'fitted' => ['label' => killmail_item_role_label('fitted'), 'description' => 'Seen on the fit, but not explicitly marked as dropped or destroyed in the payload.', 'rows' => [], 'total_quantity' => 0],
    ];

    foreach ($items as $item) {
        if (!is_array($item)) {
            continue;
        }

        $role = (string) ($item['item_role'] ?? 'other');
        if (!isset($groups[$role])) {
            continue;
        }

        $quantity = 0;
        if ($role === 'dropped') {
            $quantity = max(1, (int) ($item['quantity_dropped'] ?? 0));
        } elseif ($role === 'destroyed') {
            $quantity = max(1, (int) ($item['quantity_destroyed'] ?? 0));
        } else {
            $quantity = max(1, (int) ($item['quantity_destroyed'] ?? 0), (int) ($item['quantity_dropped'] ?? 0));
        }

        $typeId = isset($item['item_type_id']) ? (int) $item['item_type_id'] : null;
        $resolvedItem = killmail_resolved_entity($resolvedEntities, 'type', $typeId, isset($item['item_type_name']) ? (string) $item['item_type_name'] : null);

        $groups[$role]['rows'][] = [
            'item_name' => $resolvedItem['name'],
            'item_type_id' => $typeId,
            'item_icon_url' => $typeId !== null ? killmail_entity_image_url('type', $typeId, 'icon', 64) : null,
            'quantity' => $quantity,
            'quantity_label' => 'Qty ' . number_format($quantity),
            'state_label' => match ($role) {
                'dropped' => number_format($quantity) . ' dropped',
                'destroyed' => number_format($quantity) . ' destroyed',
                default => number_format($quantity) . ' fitted',
            },
            'item_flag' => isset($item['item_flag']) ? (int) $item['item_flag'] : null,
            'singleton' => isset($item['singleton']) ? (int) $item['singleton'] : null,
            'stored_at_display' => killmail_format_datetime(isset($item['created_at']) ? (string) $item['created_at'] : null),
        ];
        $groups[$role]['total_quantity'] += $quantity;
    }

    return $groups;
}

function killmail_value_amount(array $zkb): ?float
{
    if (!isset($zkb['totalValue']) || !is_numeric($zkb['totalValue'])) {
        return null;
    }

    return (float) $zkb['totalValue'];
}

function killmail_signal_strength_meta(int $itemCount, int $attackerCount, bool $trackedVictimLoss): array
{
    $score = $itemCount;
    if ($trackedVictimLoss) {
        $score += 3;
    }

    if ($attackerCount >= 10) {
        $score += 2;
    } elseif ($attackerCount >= 5) {
        $score += 1;
    }

    if ($score >= 10) {
        return [
            'label' => 'Strong signal',
            'context' => 'Rich fit data with strong logistics relevance.',
            'tone' => 'border-rose-500/40 bg-rose-500/10 text-rose-200',
        ];
    }

    if ($score >= 5) {
        return [
            'label' => 'Actionable signal',
            'context' => 'Useful loss data for supply and doctrine review.',
            'tone' => 'border-amber-500/40 bg-amber-500/10 text-amber-100',
        ];
    }

    return [
        'label' => 'Light signal',
        'context' => 'Lower detail, but still worth monitoring.',
        'tone' => 'border-sky-400/20 bg-sky-500/10 text-sky-100',
    ];
}

function killmail_supply_impact_meta(?float $estimatedValue, int $droppedQuantity, int $destroyedQuantity, int $fittedQuantity): array
{
    $score = 0;
    if ($estimatedValue !== null) {
        if ($estimatedValue >= 1000000000) {
            $score += 3;
        } elseif ($estimatedValue >= 250000000) {
            $score += 2;
        } elseif ($estimatedValue >= 50000000) {
            $score += 1;
        }
    }

    $quantityTotal = $droppedQuantity + $destroyedQuantity + $fittedQuantity;
    if ($quantityTotal >= 20) {
        $score += 3;
    } elseif ($quantityTotal >= 10) {
        $score += 2;
    } elseif ($quantityTotal >= 5) {
        $score += 1;
    }

    if ($destroyedQuantity >= 10) {
        $score += 2;
    } elseif ($destroyedQuantity >= 5) {
        $score += 1;
    }

    if ($score >= 6) {
        return [
            'label' => 'High supply impact',
            'context' => 'Expect noticeable replacement pressure.',
            'tone' => 'border-rose-500/40 bg-rose-500/10 text-rose-200',
        ];
    }

    if ($score >= 3) {
        return [
            'label' => 'Moderate supply impact',
            'context' => 'Worth watching for follow-on replenishment.',
            'tone' => 'border-amber-500/40 bg-amber-500/10 text-amber-100',
        ];
    }

    return [
        'label' => 'Low supply impact',
        'context' => 'Limited downstream replenishment pressure.',
        'tone' => 'border-emerald-500/40 bg-emerald-500/10 text-emerald-200',
    ];
}

function killmail_ship_class_label(?int $groupId): string
{
    $groupMap = [
        25 => 'Frigate',
        26 => 'Cruiser',
        27 => 'Battleship',
        28 => 'Industrial',
        29 => 'Capsule',
        30 => 'Titan',
        31 => 'Shuttle',
        324 => 'Assault Frigate',
        358 => 'Heavy Assault Cruiser',
        380 => 'Destroyer',
        419 => 'Battlecruiser',
        420 => 'Destroyer',
        463 => 'Mining Barge',
        485 => 'Dreadnought',
        513 => 'Freighter',
        540 => 'Command Ship',
        541 => 'Interdictor',
        543 => 'Exhumer',
        547 => 'Carrier',
        659 => 'Supercarrier',
        830 => 'Covert Ops',
        831 => 'Interceptor',
        832 => 'Logistics',
        833 => 'Force Recon',
        834 => 'Stealth Bomber',
        893 => 'Electronic Attack Frigate',
        894 => 'Heavy Interdictor',
        898 => 'Black Ops',
        900 => 'Marauder',
        902 => 'Jump Freighter',
        906 => 'Combat Recon',
        941 => 'Industrial Command Ship',
        963 => 'Strategic Cruiser',
        1022 => 'Prototype Exploration Ship',
        1201 => 'Attack Battlecruiser',
        1202 => 'Blockade Runner',
        1203 => 'Transport Ship',
        1283 => 'Expedition Frigate',
        1305 => 'Tactical Destroyer',
        1527 => 'Logistics Frigate',
        1534 => 'Command Destroyer',
        1972 => 'Flag Cruiser',
        1973 => 'Force Auxiliary',
    ];

    return $groupMap[$groupId ?? 0] ?? 'Ship class unavailable';
}

function killmail_item_empty_message(string $groupKey): string
{
    return match ($groupKey) {
        'dropped' => 'No modules dropped — no supply signal from this loss.',
        'destroyed' => 'No modules were explicitly destroyed in the stored record.',
        'fitted' => 'No fitted modules were extracted from this loss.',
        default => 'No item intelligence was extracted for this section.',
    };
}

function killmail_detail_data(): array
{
    $sequenceId = max(0, (int) ($_GET['sequence_id'] ?? 0));
    if ($sequenceId <= 0) {
        return [
            'error' => 'Select a stored killmail to inspect.',
            'detail' => null,
        ];
    }

    try {
        $event = db_killmail_detail($sequenceId);
        if ($event === null) {
            return [
                'error' => 'The requested killmail is not stored locally.',
                'detail' => null,
            ];
        }

        $attackers = db_killmail_attackers_by_sequence($sequenceId);
        $items = db_killmail_items_by_sequence($sequenceId);
    } catch (Throwable $exception) {
        return [
            'error' => $exception->getMessage(),
            'detail' => null,
        ];
    }

    $killmail = killmail_decode_json_array(isset($event['raw_killmail_json']) ? (string) $event['raw_killmail_json'] : null);
    $victim = is_array($killmail['victim'] ?? null) ? $killmail['victim'] : [];
    $zkb = killmail_decode_json_array(isset($event['zkb_json']) ? (string) $event['zkb_json'] : null);
    $matchSources = killmail_match_sources($event);
    $resolutionRequests = killmail_entity_resolution_requests($event, $attackers, $items);
    $resolvedEntities = killmail_entity_resolve_batch($resolutionRequests, false);
    $groupedItems = killmail_loss_item_groups($items, $resolvedEntities);
    $victimCharacter = killmail_resolved_entity($resolvedEntities, 'character', isset($event['victim_character_id']) ? (int) $event['victim_character_id'] : null);
    $victimCorporation = killmail_resolved_entity(
        $resolvedEntities,
        'corporation',
        isset($event['victim_corporation_id']) ? (int) $event['victim_corporation_id'] : null,
        isset($event['victim_corporation_label']) ? (string) $event['victim_corporation_label'] : null
    );
    $victimAlliance = killmail_resolved_entity(
        $resolvedEntities,
        'alliance',
        isset($event['victim_alliance_id']) ? (int) $event['victim_alliance_id'] : null,
        isset($event['victim_alliance_label']) ? (string) $event['victim_alliance_label'] : null
    );
    $victimShip = killmail_resolved_entity($resolvedEntities, 'type', isset($event['victim_ship_type_id']) ? (int) $event['victim_ship_type_id'] : null, isset($event['ship_type_name']) ? (string) $event['ship_type_name'] : null);
    $system = killmail_resolved_entity($resolvedEntities, 'system', isset($event['solar_system_id']) ? (int) $event['solar_system_id'] : null, isset($event['system_name']) ? (string) $event['system_name'] : null);
    $region = killmail_resolved_entity($resolvedEntities, 'region', isset($event['region_id']) ? (int) $event['region_id'] : null, isset($event['region_name']) ? (string) $event['region_name'] : null);

    $formattedAttackers = [];
    foreach ($attackers as $attacker) {
        if (!is_array($attacker)) {
            continue;
        }

        $attackerCharacter = killmail_resolved_entity($resolvedEntities, 'character', isset($attacker['character_id']) ? (int) $attacker['character_id'] : null);
        $attackerCorporation = killmail_resolved_entity($resolvedEntities, 'corporation', isset($attacker['corporation_id']) ? (int) $attacker['corporation_id'] : null);
        $attackerAlliance = killmail_resolved_entity($resolvedEntities, 'alliance', isset($attacker['alliance_id']) ? (int) $attacker['alliance_id'] : null);
        $attackerShip = killmail_resolved_entity($resolvedEntities, 'type', isset($attacker['ship_type_id']) ? (int) $attacker['ship_type_id'] : null, isset($attacker['ship_type_name']) ? (string) $attacker['ship_type_name'] : null);
        $attackerWeapon = killmail_resolved_entity($resolvedEntities, 'type', isset($attacker['weapon_type_id']) ? (int) $attacker['weapon_type_id'] : null, isset($attacker['weapon_type_name']) ? (string) $attacker['weapon_type_name'] : null);

        $formattedAttackers[] = [
            'attacker_index' => (int) ($attacker['attacker_index'] ?? 0),
            'character_name' => $attackerCharacter['name'],
            'character_image_url' => $attackerCharacter['id'] !== null ? killmail_entity_image_url('character', (int) $attackerCharacter['id'], 'portrait', 128) : null,
            'corporation_display' => $attackerCorporation['name'],
            'corporation_logo_url' => $attackerCorporation['id'] !== null ? killmail_entity_image_url('corporation', (int) $attackerCorporation['id'], 'logo', 64) : null,
            'alliance_display' => $attackerAlliance['name'],
            'alliance_logo_url' => $attackerAlliance['id'] !== null ? killmail_entity_image_url('alliance', (int) $attackerAlliance['id'], 'logo', 64) : null,
            'ship_display' => $attackerShip['name'],
            'ship_icon_url' => $attackerShip['id'] !== null ? killmail_entity_image_url('type', (int) $attackerShip['id'], 'icon', 64) : null,
            'weapon_display' => $attackerWeapon['name'],
            'final_blow' => (int) ($attacker['final_blow'] ?? 0) === 1,
            'security_status' => isset($attacker['security_status']) && $attacker['security_status'] !== null ? number_format((float) $attacker['security_status'], 2) : '—',
        ];
    }

    $topAttackers = array_slice($formattedAttackers, 0, 5);
    $finalBlow = null;
    foreach ($formattedAttackers as $attacker) {
        if ($attacker['final_blow']) {
            $finalBlow = $attacker;
            break;
        }
    }

    $estimatedValue = killmail_value_amount($zkb);
    $itemTotals = [
        'dropped' => (int) ($groupedItems['dropped']['total_quantity'] ?? 0),
        'destroyed' => (int) ($groupedItems['destroyed']['total_quantity'] ?? 0),
        'fitted' => (int) ($groupedItems['fitted']['total_quantity'] ?? 0),
    ];
    $storedItemCount = count($items);
    $signalStrength = killmail_signal_strength_meta($storedItemCount, count($formattedAttackers), (int) ($event['matched_tracked'] ?? 0) === 1);
    $supplyImpact = killmail_supply_impact_meta($estimatedValue, $itemTotals['dropped'], $itemTotals['destroyed'], $itemTotals['fitted']);

    return [
        'error' => null,
        'detail' => [
            'sequence_id' => (int) ($event['sequence_id'] ?? 0),
            'killmail_id' => (int) ($event['killmail_id'] ?? 0),
            'killmail_hash' => (string) ($event['killmail_hash'] ?? ''),
            'killmail_time_display' => killmail_format_datetime(isset($event['killmail_time']) ? (string) $event['killmail_time'] : null),
            'uploaded_at_display' => killmail_format_datetime(isset($event['uploaded_at']) ? (string) $event['uploaded_at'] : null),
            'created_at_display' => killmail_format_datetime(isset($event['created_at']) ? (string) $event['created_at'] : null),
            'updated_at_display' => killmail_format_datetime(isset($event['updated_at']) ? (string) $event['updated_at'] : null),
            'victim' => [
                'character_name' => $victimCharacter['name'],
                'character_portrait_url' => $victimCharacter['id'] !== null ? killmail_entity_image_url('character', (int) $victimCharacter['id'], 'portrait', 256) : null,
                'corporation_display' => $victimCorporation['name'],
                'corporation_logo_url' => $victimCorporation['id'] !== null ? killmail_entity_image_url('corporation', (int) $victimCorporation['id'], 'logo', 128) : null,
                'alliance_display' => $victimAlliance['name'],
                'alliance_logo_url' => $victimAlliance['id'] !== null ? killmail_entity_image_url('alliance', (int) $victimAlliance['id'], 'logo', 128) : null,
                'damage_taken' => number_format((int) ($victim['damage_taken'] ?? 0)),
                'tracked_badges' => $matchSources,
            ],
            'ship' => [
                'name' => $victimShip['name'],
                'render_url' => $victimShip['id'] !== null ? killmail_entity_image_url('type', (int) $victimShip['id'], 'render', 512) : null,
                'icon_url' => $victimShip['id'] !== null ? killmail_entity_image_url('type', (int) $victimShip['id'], 'icon', 128) : null,
                'class' => killmail_ship_class_label(isset($victimShip['metadata']['group_id']) ? (int) $victimShip['metadata']['group_id'] : null),
            ],
            'location' => [
                'system_display' => $system['name'],
                'region_display' => $region['name'],
                'security_status' => isset($system['metadata']['security']) ? number_format((float) $system['metadata']['security'], 1) : null,
            ],
            'attackers' => [
                'count' => count($formattedAttackers),
                'top_rows' => $topAttackers,
                'rows' => $formattedAttackers,
                'final_blow' => $finalBlow,
            ],
            'items' => $groupedItems,
            'item_totals' => $itemTotals,
            'stored_item_count' => $storedItemCount,
            'tracked_victim_loss' => (int) ($event['matched_tracked'] ?? 0) === 1,
            'match_context' => $matchSources === [] ? 'No tracked victim entity currently matches this stored loss.' : ('Matched on ' . implode(' and ', $matchSources) . '.'),
            'signal_strength' => $signalStrength,
            'supply_impact' => $supplyImpact,
            'loss_summary' => [
                'estimated_value_display' => $estimatedValue !== null ? number_format($estimatedValue, 0) . ' ISK' : 'Value unavailable',
                'item_count_display' => number_format($storedItemCount) . ' extracted items',
                'impact_summary' => $supplyImpact['context'],
            ],
            'zkb' => [
                'total_value_display' => $estimatedValue !== null ? number_format($estimatedValue, 0) . ' ISK' : 'Unavailable',
                'points_display' => isset($zkb['points']) ? number_format((int) $zkb['points']) : 'Unavailable',
                'npc' => !empty($zkb['npc']),
                'solo' => !empty($zkb['solo']),
                'awox' => !empty($zkb['awox']),
                'href' => isset($zkb['href']) ? (string) $zkb['href'] : '',
            ],
        ],
    ];
}

function killmail_overview_data(): array
{
    $recentHours = 24;
    $allowedPageSizes = [25, 50, 100];
    $pageSize = (int) ($_GET['page_size'] ?? 25);
    if (!in_array($pageSize, $allowedPageSizes, true)) {
        $pageSize = 25;
    }

    $filters = [
        'search' => trim((string) ($_GET['q'] ?? '')),
        'alliance_id' => max(0, (int) ($_GET['alliance_id'] ?? 0)),
        'corporation_id' => max(0, (int) ($_GET['corporation_id'] ?? 0)),
        'tracked_only' => sanitize_enabled_flag($_GET['tracked_only'] ?? '0') === '1',
        'page' => max(1, (int) ($_GET['page'] ?? 1)),
        'page_size' => $pageSize,
    ];

    try {
        $summaryRow = db_killmail_overview_summary($recentHours);
        $status = db_killmail_ingestion_status();
        $options = db_killmail_overview_filter_options();
        $listing = db_killmail_overview_page($filters);
    } catch (Throwable $exception) {
        return [
            'error' => $exception->getMessage(),
            'summary' => [],
            'status' => [
                'ingestion_enabled' => killmail_ingestion_enabled(),
                'last_sync_outcome' => 'Unavailable',
            ],
            'rows' => [],
            'filters' => $filters + [
                'page_size_options' => $allowedPageSizes,
                'alliance_options' => ['0' => 'All alliances'],
                'corporation_options' => ['0' => 'All corporations'],
            ],
            'pagination' => [
                'page' => 1,
                'page_size' => $pageSize,
                'page_size_options' => $allowedPageSizes,
                'total_pages' => 1,
                'total_items' => 0,
                'showing_from' => 0,
                'showing_to' => 0,
            ],
            'empty_message' => 'Killmail overview is unavailable because the database query failed.',
        ];
    }

    $totalCount = (int) ($summaryRow['total_count'] ?? 0);
    $recentCount = (int) ($summaryRow['recent_count'] ?? 0);
    $trackedMatchCount = (int) ($summaryRow['tracked_match_count'] ?? 0);
    $maxSequenceId = (int) ($summaryRow['max_sequence_id'] ?? ($status['max_sequence_id'] ?? 0));
    $state = is_array($status['state'] ?? null) ? $status['state'] : [];
    $latestRun = is_array($status['latest_run'] ?? null) ? $status['latest_run'] : null;
    $lastSuccessAt = isset($state['last_success_at']) ? (string) $state['last_success_at'] : null;
    $lastIngestedAt = isset($summaryRow['last_ingested_at']) ? (string) $summaryRow['last_ingested_at'] : null;
    $latestUploadedAt = isset($summaryRow['latest_uploaded_at']) ? (string) $summaryRow['latest_uploaded_at'] : null;
    $cursor = isset($state['last_cursor']) ? trim((string) $state['last_cursor']) : '';

    $overviewResolutionRequests = [
        'alliance' => [],
        'corporation' => [],
        'character' => [],
        'type' => [],
        'system' => [],
        'region' => [],
    ];

    foreach ((array) ($options['alliances'] ?? []) as $row) {
        $id = (int) ($row['entity_id'] ?? 0);
        if ($id > 0) {
            $overviewResolutionRequests['alliance'][$id] = $id;
        }
    }

    foreach ((array) ($options['corporations'] ?? []) as $row) {
        $id = (int) ($row['entity_id'] ?? 0);
        if ($id > 0) {
            $overviewResolutionRequests['corporation'][$id] = $id;
        }
    }

    foreach ((array) ($listing['rows'] ?? []) as $row) {
        $allianceId = (int) ($row['victim_alliance_id'] ?? 0);
        $corporationId = (int) ($row['victim_corporation_id'] ?? 0);
        $shipTypeId = (int) ($row['victim_ship_type_id'] ?? 0);
        $systemId = (int) ($row['solar_system_id'] ?? 0);
        $regionId = (int) ($row['region_id'] ?? 0);

        if ($allianceId > 0) {
            $overviewResolutionRequests['alliance'][$allianceId] = $allianceId;
        }
        if ($corporationId > 0) {
            $overviewResolutionRequests['corporation'][$corporationId] = $corporationId;
        }
        if ($shipTypeId > 0) {
            $overviewResolutionRequests['type'][$shipTypeId] = $shipTypeId;
        }
        if ($systemId > 0) {
            $overviewResolutionRequests['system'][$systemId] = $systemId;
        }
        if ($regionId > 0) {
            $overviewResolutionRequests['region'][$regionId] = $regionId;
        }
    }

    foreach ($overviewResolutionRequests as $type => $ids) {
        $overviewResolutionRequests[$type] = array_values($ids);
    }

    $resolvedOverviewEntities = killmail_entity_resolve_batch($overviewResolutionRequests, true);

    $allianceOptions = ['0' => 'All alliances'];
    foreach ((array) ($options['alliances'] ?? []) as $row) {
        $id = (int) ($row['entity_id'] ?? 0);
        if ($id <= 0) {
            continue;
        }

        $allianceOptions[(string) $id] = killmail_entity_preferred_name(
            $resolvedOverviewEntities,
            'alliance',
            $id,
            isset($row['entity_label']) ? (string) $row['entity_label'] : '',
            'Alliance'
        );
    }

    $corporationOptions = ['0' => 'All corporations'];
    foreach ((array) ($options['corporations'] ?? []) as $row) {
        $id = (int) ($row['entity_id'] ?? 0);
        if ($id <= 0) {
            continue;
        }

        $corporationOptions[(string) $id] = killmail_entity_preferred_name(
            $resolvedOverviewEntities,
            'corporation',
            $id,
            isset($row['entity_label']) ? (string) $row['entity_label'] : '',
            'Corporation'
        );
    }

    $rows = array_map(static function (array $row) use ($resolvedOverviewEntities): array {
        $matchSources = killmail_match_sources($row);
        $zkb = killmail_decode_json_array(isset($row['zkb_json']) ? (string) $row['zkb_json'] : null);
        $estimatedValue = killmail_value_amount($zkb);
        $shipTypeId = isset($row['victim_ship_type_id']) ? (int) $row['victim_ship_type_id'] : null;
        $signalStrength = killmail_signal_strength_meta(0, 0, (int) ($row['matched_tracked'] ?? 0) === 1);
        $supplyImpact = killmail_supply_impact_meta($estimatedValue, 0, 0, 0);

        return [
            'sequence_id' => (int) ($row['sequence_id'] ?? 0),
            'killmail_id' => (int) ($row['killmail_id'] ?? 0),
            'killmail_time_display' => killmail_format_datetime(isset($row['killmail_time']) ? (string) $row['killmail_time'] : null),
            'uploaded_at_display' => killmail_format_datetime(isset($row['uploaded_at']) ? (string) $row['uploaded_at'] : null),
            'created_at_display' => killmail_format_datetime(isset($row['created_at']) ? (string) $row['created_at'] : null),
            'victim_alliance' => killmail_entity_preferred_name($resolvedOverviewEntities, 'alliance', isset($row['victim_alliance_id']) ? (int) $row['victim_alliance_id'] : null, isset($row['victim_alliance_label']) ? (string) $row['victim_alliance_label'] : '', 'Alliance'),
            'victim_corporation' => killmail_entity_preferred_name($resolvedOverviewEntities, 'corporation', isset($row['victim_corporation_id']) ? (int) $row['victim_corporation_id'] : null, isset($row['victim_corporation_label']) ? (string) $row['victim_corporation_label'] : '', 'Corporation'),
            'ship_type' => killmail_entity_preferred_name($resolvedOverviewEntities, 'type', isset($row['victim_ship_type_id']) ? (int) $row['victim_ship_type_id'] : null, isset($row['ship_type_name']) ? (string) $row['ship_type_name'] : '', 'Ship'),
            'system' => killmail_entity_preferred_name($resolvedOverviewEntities, 'system', isset($row['solar_system_id']) ? (int) $row['solar_system_id'] : null, isset($row['system_name']) ? (string) $row['system_name'] : '', 'System'),
            'region' => killmail_entity_preferred_name($resolvedOverviewEntities, 'region', isset($row['region_id']) ? (int) $row['region_id'] : null, isset($row['region_name']) ? (string) $row['region_name'] : '', 'Region'),
            'matched_tracked' => (int) ($row['matched_tracked'] ?? 0) === 1,
            'match_context' => $matchSources === [] ? 'No tracked victim entity currently matches this stored loss.' : ('Matched on ' . implode(', ', $matchSources) . '.'),
            'ship_icon_url' => $shipTypeId !== null ? killmail_entity_image_url('type', $shipTypeId, 'icon', 64) : null,
            'estimated_value_display' => $estimatedValue !== null ? number_format($estimatedValue, 0) . ' ISK' : 'Value unavailable',
            'signal_strength' => $signalStrength,
            'supply_impact' => $supplyImpact,
            'inspect_url' => '/killmail-intelligence/view.php?sequence_id=' . urlencode((string) ((int) ($row['sequence_id'] ?? 0))),
        ];
    }, (array) ($listing['rows'] ?? []));

    $emptyMessage = $totalCount === 0
        ? 'No killmails have been stored yet. Enable killmail ingestion, run the sync worker, and this view will populate as local killmails arrive.'
        : 'No killmails matched the current filters. Try clearing search or filter controls.';

    return [
        'error' => null,
        'summary' => [
            ['label' => 'Total Ingested', 'value' => number_format($totalCount), 'context' => 'Killmails stored locally'],
            ['label' => 'Recent Ingestion', 'value' => number_format($recentCount), 'context' => 'Stored in the last ' . $recentHours . ' hours'],
            ['label' => 'Tracked Victim Losses', 'value' => number_format($trackedMatchCount), 'context' => 'Stored losses where the victim matches a tracked alliance or corporation'],
            ['label' => 'Last Processed Sequence', 'value' => $maxSequenceId > 0 ? number_format($maxSequenceId) : '—', 'context' => $cursor !== '' ? ('Cursor ' . $cursor) : 'Cursor not recorded yet'],
            ['label' => 'Sync Freshness', 'value' => killmail_relative_datetime($lastSuccessAt), 'context' => $lastSuccessAt !== null ? ('Last success ' . killmail_format_datetime($lastSuccessAt)) : 'No successful sync recorded'],
        ],
        'status' => [
            'ingestion_enabled' => killmail_ingestion_enabled(),
            'current_cursor' => $cursor !== '' ? $cursor : 'Unavailable',
            'last_sync_outcome' => killmail_last_sync_outcome_label($latestRun),
            'last_success_at' => killmail_format_datetime($lastSuccessAt),
            'last_sync_relative' => killmail_relative_datetime($lastSuccessAt),
            'last_uploaded_at' => killmail_format_datetime($latestUploadedAt),
            'last_ingested_at' => killmail_format_datetime($lastIngestedAt),
            'tracked_alliance_count' => (int) ($status['tracked_alliance_count'] ?? 0),
            'tracked_corporation_count' => (int) ($status['tracked_corporation_count'] ?? 0),
            'sync_status' => (string) ($state['status'] ?? 'idle'),
            'last_run_status' => (string) ($latestRun['run_status'] ?? 'not_run'),
            'last_run_source_rows' => (int) ($latestRun['source_rows'] ?? 0),
            'last_run_written_rows' => (int) ($latestRun['written_rows'] ?? 0),
            'last_run_finished_at' => killmail_format_datetime(isset($latestRun['finished_at']) ? (string) $latestRun['finished_at'] : null),
            'last_error' => trim((string) ($state['last_error_message'] ?? '')),
        ],
        'rows' => $rows,
        'filters' => $filters + [
            'page_size_options' => $allowedPageSizes,
            'alliance_options' => $allianceOptions,
            'corporation_options' => $corporationOptions,
        ],
        'pagination' => [
            'page' => (int) ($listing['page'] ?? 1),
            'page_size' => (int) ($listing['page_size'] ?? $pageSize),
            'page_size_options' => $allowedPageSizes,
            'total_pages' => (int) ($listing['total_pages'] ?? 1),
            'total_items' => (int) ($listing['total_items'] ?? 0),
            'showing_from' => (int) ($listing['showing_from'] ?? 0),
            'showing_to' => (int) ($listing['showing_to'] ?? 0),
        ],
        'empty_message' => $emptyMessage,
    ];
}

function human_duration_ago(int $seconds): string
{
    if ($seconds < 60) {
        return $seconds . 's';
    }

    if ($seconds < 3600) {
        return (int) floor($seconds / 60) . 'm';
    }

    if ($seconds < 86400) {
        return (int) floor($seconds / 3600) . 'h';
    }

    return (int) floor($seconds / 86400) . 'd';
}

function reference_hub_comparison_data(): array
{
    $comparisonContext = market_comparison_context();
    $outcomes = market_comparison_outcomes();
    $underpriced = $outcomes['underpriced_in_alliance'];
    $overpriced = $outcomes['overpriced_in_alliance'];

    usort($underpriced, static fn (array $a, array $b): int => (($b['opportunity_score'] ?? 0) <=> ($a['opportunity_score'] ?? 0)) ?: (($b['volume_score'] ?? 0) <=> ($a['volume_score'] ?? 0)));
    usort($overpriced, static fn (array $a, array $b): int => (($b['risk_score'] ?? 0) <=> ($a['risk_score'] ?? 0)) ?: (($b['price_delta_score'] ?? 0) <=> ($a['price_delta_score'] ?? 0)));

    $top = array_slice(array_values(array_filter(array_merge($underpriced, $overpriced), static fn (array $row): bool => (int) max((int) ($row['risk_score'] ?? 0), (int) ($row['opportunity_score'] ?? 0)) >= 60)), 0, 5);

    $rows = [];
    foreach ([
        ['label' => 'Underpriced vs Hub', 'items' => $underpriced, 'kind' => 'opportunity'],
        ['label' => 'Overpriced vs Hub', 'items' => $overpriced, 'kind' => 'risk'],
    ] as $group) {
        $rows[] = ['is_group_header' => true, 'module' => $group['label']];
        foreach (array_slice($group['items'], 0, 15) as $row) {
            $rows[] = [
                'module' => $row['type_name'] !== '' ? $row['type_name'] : ('Type #' . $row['type_id']),
                'alliance_price' => market_format_isk($row['alliance_best_sell_price']),
                'reference_price' => market_format_isk($row['reference_best_sell_price']),
                'delta' => sprintf('%+.1f%%', (float) ($row['deviation_percent'] ?? 0.0)),
                'score' => (string) ((int) ($group['kind'] === 'risk' ? ($row['risk_score'] ?? 0) : ($row['opportunity_score'] ?? 0))),
                'severity' => (string) ($group['kind'] === 'risk' ? ($row['severity'] ?? 'Low') : ($row['opportunity_tier'] ?? 'Low')),
                'row_tone' => $group['kind'] === 'risk' ? 'risk_high' : 'opp_high',
            ];
        }
    }

    return [
        'summary' => [
            ['label' => 'Compared Modules', 'value' => (string) count($outcomes['in_both_markets']), 'context' => 'Alliance vs ' . $comparisonContext['reference_hub'] . ' pairs'],
            ['label' => 'Best Underpriced', 'value' => (string) count($underpriced), 'context' => 'Alliance cheaper than ' . $comparisonContext['reference_hub']],
            ['label' => 'Overpriced Risks', 'value' => (string) count($overpriced), 'context' => 'Alliance pricier than ' . $comparisonContext['reference_hub']],
        ],
        'highlights' => [
            'title' => 'Top Opportunities',
            'rows' => array_map(static fn (array $row): array => [
                'module' => $row['type_name'] !== '' ? $row['type_name'] : ('Type #' . $row['type_id']),
                'signal' => sprintf('%+.1f%% · volume %d', (float) ($row['deviation_percent'] ?? 0.0), (int) ($row['reference_total_sell_volume'] ?? 0)),
                'score' => (int) max((int) ($row['opportunity_score'] ?? 0), (int) ($row['risk_score'] ?? 0)),
            ], $top),
        ],
        'rows' => $rows,
    ];
}

function missing_items_data(): array
{
    $comparisonContext = market_comparison_context();
    $outcomes = market_comparison_outcomes();
    $missingRows = $outcomes['missing_in_alliance'];

    usort($missingRows, static fn (array $a, array $b): int => (($b['opportunity_score'] ?? 0) <=> ($a['opportunity_score'] ?? 0)) ?: (($b['volume_score'] ?? 0) <=> ($a['volume_score'] ?? 0)));
    $topRows = array_slice($missingRows, 0, 5);

    return [
        'summary' => [
            ['label' => 'Missing Modules', 'value' => (string) count($missingRows), 'context' => 'No active alliance listing'],
            ['label' => 'High-Turnover Gaps', 'value' => (string) count(array_filter($missingRows, static fn (array $row): bool => (int) ($row['volume_score'] ?? 0) >= 55)), 'context' => 'Strong ' . $comparisonContext['reference_hub'] . ' movement'],
            ['label' => 'Top Seed Targets', 'value' => (string) count(array_filter($missingRows, static fn (array $row): bool => (int) ($row['opportunity_score'] ?? 0) >= 60)), 'context' => 'High opportunity score'],
        ],
        'highlights' => [
            'title' => 'Top Missing Items to Seed Market',
            'rows' => array_map(static fn (array $row): array => [
                'module' => $row['type_name'] !== '' ? $row['type_name'] : ('Type #' . $row['type_id']),
                'signal' => 'Volume ' . (string) ($row['reference_total_sell_volume'] ?? 0) . ' · impact ' . sprintf('%+.1f%%', (float) ($row['deviation_percent'] ?? 0.0)),
                'score' => (int) ($row['opportunity_score'] ?? 0),
            ], $topRows),
        ],
        'rows' => array_map(static function (array $row): array {
            return [
                'module' => $row['type_name'] !== '' ? $row['type_name'] : ('Type #' . $row['type_id']),
                'reference_price' => market_format_isk($row['reference_best_sell_price']),
                'daily_volume' => (string) ($row['reference_total_sell_volume'] ?? 0),
                'price_delta' => sprintf('%+.1f%%', (float) ($row['deviation_percent'] ?? 0.0)),
                'score' => (string) ((int) ($row['opportunity_score'] ?? 0)),
                'priority' => (string) ($row['opportunity_tier'] ?? 'Low'),
                'row_tone' => (int) ($row['opportunity_score'] ?? 0) >= 75 ? 'opp_high' : ((int) ($row['opportunity_score'] ?? 0) >= 45 ? 'opp_medium' : 'opp_low'),
            ];
        }, array_slice($missingRows, 0, 30)),
    ];
}

function price_deviations_data(): array
{
    $comparisonContext = market_comparison_context();
    $outcomes = market_comparison_outcomes();
    $alerts = array_values(array_filter($outcomes['rows'], static fn (array $row): bool => (bool) ($row['overpriced_in_alliance'] ?? false) || (bool) ($row['underpriced_in_alliance'] ?? false)));

    $actionable = array_values(array_filter($alerts, static fn (array $row): bool => (int) ($row['price_delta_score'] ?? 0) >= 45 && (int) ($row['volume_score'] ?? 0) >= 35));
    $noise = array_values(array_filter($alerts, static fn (array $row): bool => !in_array($row, $actionable, true)));

    usort($actionable, static fn (array $a, array $b): int => (($b['risk_score'] ?? 0) <=> ($a['risk_score'] ?? 0)) ?: (($b['price_delta_score'] ?? 0) <=> ($a['price_delta_score'] ?? 0)));
    usort($noise, static fn (array $a, array $b): int => (($b['price_delta_score'] ?? 0) <=> ($a['price_delta_score'] ?? 0)));

    $rows = [];
    foreach ([
        ['label' => 'Actionable', 'items' => $actionable],
        ['label' => 'Noise', 'items' => $noise],
    ] as $group) {
        $rows[] = ['is_group_header' => true, 'module' => $group['label']];
        foreach (array_slice($group['items'], 0, 15) as $row) {
            $rows[] = [
                'module' => $row['type_name'] !== '' ? $row['type_name'] : ('Type #' . $row['type_id']),
                'alliance_price' => market_format_isk($row['alliance_best_sell_price']),
                'reference_price' => market_format_isk($row['reference_best_sell_price']),
                'deviation' => sprintf('%+.1f%%', (float) ($row['deviation_percent'] ?? 0.0)),
                'severity' => (string) ($row['severity'] ?? 'Low'),
                'score' => (string) ((int) ($row['risk_score'] ?? 0)),
                'row_tone' => $group['label'] === 'Actionable' ? 'risk_high' : 'risk_low',
            ];
        }
    }

    return [
        'summary' => [
            ['label' => 'Deviation Alerts', 'value' => (string) count($alerts), 'context' => 'Outside configured threshold'],
            ['label' => 'Actionable', 'value' => (string) count($actionable), 'context' => 'High deviation + meaningful volume'],
            ['label' => 'Noise', 'value' => (string) count($noise), 'context' => 'Low relevance, monitor only'],
        ],
        'rows' => $rows,
    ];
}

function history_sanitize_days(mixed $value, int $default = 30): int
{
    $days = (int) $value;
    if ($days <= 0) {
        $days = $default;
    }

    return max(7, min(180, $days));
}

function history_filters(array $request, string $defaultModule = 'deviation_trend'): array
{
    $module = trim((string) ($request['module'] ?? $defaultModule));
    $registry = history_module_registry();
    if (!isset($registry[$module])) {
        $module = $defaultModule;
    }

    return [
        'days' => history_sanitize_days($request['days'] ?? 30),
        'module' => $module,
    ];
}

function history_date_range(int $days): array
{
    $safeDays = max(1, $days);
    $end = new DateTimeImmutable('now', new DateTimeZone('UTC'));
    $start = $end->sub(new DateInterval('P' . ($safeDays - 1) . 'D'));

    return ['start' => $start->format('Y-m-d'), 'end' => $end->format('Y-m-d')];
}

function history_source_context(): array
{
    $allianceStructureId = configured_structure_destination_id_for_esi_sync();
    $hubRef = market_hub_setting_reference();
    $hubSourceId = sync_source_id_from_hub_ref($hubRef);

    return [
        'alliance_structure_id' => $allianceStructureId,
        'alliance_structure_name' => selected_station_name('alliance_station_id') ?? 'Operational destination not configured',
        'hub_source_id' => $hubSourceId,
        'hub_name' => selected_station_name('market_station_id') ?? 'Market hub not configured',
    ];
}

function history_trend_label(float $delta): string
{
    if ($delta > 0.25) {
        return 'Up';
    }
    if ($delta < -0.25) {
        return 'Down';
    }

    return 'Stable';
}

function history_module_registry(): array
{
    return [
        'deviation_trend' => [
            'label' => 'Deviation Trend',
            'description' => 'Count of items outside configured destination-vs-reference-hub deviation thresholds over time.',
        ],
        'missing_items_trend' => [
            'label' => 'Missing Items Trend',
            'description' => 'Daily count of reference-hub-listed items with no destination history snapshot.',
        ],
        'stock_health_trend' => [
            'label' => 'Stock Health Trend',
            'description' => 'Daily weak-stock incidence derived from order depth and order count history.',
        ],
    ];
}

function alliance_trends_data(array $request = []): array
{
    $filters = history_filters($request, 'deviation_trend');
    $window = history_date_range($filters['days']);
    $source = history_source_context();

    if ($source['alliance_structure_id'] <= 0) {
        return [
            'filters' => $filters,
            'summary' => [
                ['label' => 'Status', 'value' => 'Unavailable', 'context' => 'Select an alliance structure in Settings to load trends.'],
            ],
            'rows' => [],
        ];
    }

    try {
        $daily = db_market_history_daily_aggregate_by_date_type_source('alliance_structure', $source['alliance_structure_id'], $window['start'], $window['end']);
        $stock = db_market_orders_history_stock_health_series('alliance_structure', $source['alliance_structure_id'], $window['start'], $window['end']);
        $deviation = $source['hub_source_id'] > 0
            ? db_market_history_daily_deviation_series($source['alliance_structure_id'], $source['hub_source_id'], $window['start'], $window['end'])
            : [];
    } catch (Throwable) {
        $daily = [];
        $stock = [];
        $deviation = [];
    }

    $byDate = [];
    foreach ($daily as $row) {
        $date = (string) ($row['trade_date'] ?? '');
        if ($date === '') {
            continue;
        }

        if (!isset($byDate[$date])) {
            $byDate[$date] = ['volume' => 0, 'orders' => 0, 'price_total' => 0.0, 'price_points' => 0];
        }

        $byDate[$date]['volume'] += (int) ($row['total_volume'] ?? 0);
        $byDate[$date]['orders'] += (int) ($row['total_order_count'] ?? 0);

        $price = isset($row['avg_close_price']) ? (float) $row['avg_close_price'] : 0.0;
        if ($price > 0) {
            $byDate[$date]['price_total'] += $price;
            $byDate[$date]['price_points']++;
        }
    }

    $deviationByDate = [];
    foreach ($deviation as $row) {
        $date = (string) ($row['trade_date'] ?? '');
        $value = isset($row['deviation_percent']) ? (float) $row['deviation_percent'] : null;
        if ($date === '' || $value === null) {
            continue;
        }
        $deviationByDate[$date][] = $value;
    }

    $stockByDate = [];
    foreach ($stock as $row) {
        $date = (string) ($row['observed_date'] ?? '');
        if ($date === '') {
            continue;
        }
        $stockByDate[$date] = ($stockByDate[$date] ?? 0) + (int) ($row['sell_volume'] ?? 0);
    }

    ksort($byDate);
    $rows = [];
    $previousAvg = null;
    foreach ($byDate as $date => $metrics) {
        $avg = $metrics['price_points'] > 0 ? ($metrics['price_total'] / $metrics['price_points']) : null;
        $medianDeviation = '—';
        if (isset($deviationByDate[$date]) && $deviationByDate[$date] !== []) {
            sort($deviationByDate[$date]);
            $medianDeviation = market_format_percentage((float) $deviationByDate[$date][intdiv(count($deviationByDate[$date]), 2)]);
        }

        $delta = $previousAvg !== null && $avg !== null && $previousAvg > 0 ? (($avg - $previousAvg) / $previousAvg) * 100.0 : 0.0;
        $rows[] = [
            'date' => $date,
            'avg_price' => market_format_isk($avg),
            'volume' => number_format((int) $metrics['volume']),
            'order_count' => number_format((int) $metrics['orders']),
            'stock_sell_volume' => number_format((int) ($stockByDate[$date] ?? 0)),
            'deviation_median' => $medianDeviation,
            'trend' => history_trend_label($delta),
        ];

        if ($avg !== null) {
            $previousAvg = $avg;
        }
    }

    $latestRow = $rows !== [] ? $rows[count($rows) - 1] : null;

    return [
        'filters' => $filters,
        'summary' => [
            ['label' => 'Window', 'value' => (string) $filters['days'] . ' days', 'context' => $window['start'] . ' → ' . $window['end']],
            ['label' => 'Alliance Source', 'value' => $source['alliance_structure_name'], 'context' => 'Structure history aggregates'],
            ['label' => 'Latest Snapshot', 'value' => $latestRow['date'] ?? 'No data', 'context' => $latestRow !== null ? ('Volume ' . ($latestRow['volume'] ?? '0')) : 'No historical rows yet'],
        ],
        'rows' => array_reverse($rows),
    ];
}

function module_history_data(array $request = []): array
{
    $filters = history_filters($request, 'deviation_trend');
    $registry = history_module_registry();
    $selected = $registry[$filters['module']];
    $window = history_date_range($filters['days']);
    $source = history_source_context();

    $rows = [];

    try {
        if ($filters['module'] === 'deviation_trend' && $source['alliance_structure_id'] > 0 && $source['hub_source_id'] > 0) {
            $series = db_market_history_daily_deviation_series($source['alliance_structure_id'], $source['hub_source_id'], $window['start'], $window['end']);
            $threshold = market_compare_thresholds()['deviation_percent'];
            $bucket = [];
            foreach ($series as $row) {
                $date = (string) ($row['trade_date'] ?? '');
                if ($date === '') {
                    continue;
                }
                $bucket[$date]['count'] = ($bucket[$date]['count'] ?? 0) + 1;
                if (abs((float) ($row['deviation_percent'] ?? 0.0)) >= $threshold) {
                    $bucket[$date]['alerts'] = ($bucket[$date]['alerts'] ?? 0) + 1;
                }
            }

            foreach ($bucket as $date => $values) {
                $rows[] = [
                    'date' => $date,
                    'module' => $selected['label'],
                    'metric' => number_format((int) ($values['alerts'] ?? 0)),
                    'context' => number_format((int) ($values['count'] ?? 0)) . ' compared items',
                ];
            }
        }

        if ($filters['module'] === 'stock_health_trend' && $source['alliance_structure_id'] > 0) {
            $series = db_market_orders_history_stock_health_series('alliance_structure', $source['alliance_structure_id'], $window['start'], $window['end']);
            $thresholds = market_compare_thresholds();
            $bucket = [];
            foreach ($series as $row) {
                $date = (string) ($row['observed_date'] ?? '');
                if ($date === '') {
                    continue;
                }
                $bucket[$date]['count'] = ($bucket[$date]['count'] ?? 0) + 1;
                $sellVolume = (int) ($row['sell_volume'] ?? 0);
                $sellOrders = (int) ($row['sell_order_count'] ?? 0);
                if ($sellVolume < $thresholds['min_alliance_sell_volume'] || $sellOrders < $thresholds['min_alliance_sell_orders']) {
                    $bucket[$date]['weak'] = ($bucket[$date]['weak'] ?? 0) + 1;
                }
            }

            foreach ($bucket as $date => $values) {
                $rows[] = [
                    'date' => $date,
                    'module' => $selected['label'],
                    'metric' => number_format((int) ($values['weak'] ?? 0)),
                    'context' => number_format((int) ($values['count'] ?? 0)) . ' tracked types',
                ];
            }
        }

        if ($filters['module'] === 'missing_items_trend' && $source['alliance_structure_id'] > 0 && $source['hub_source_id'] > 0) {
            $alliance = db_market_history_daily_aggregate_by_date_type_source('alliance_structure', $source['alliance_structure_id'], $window['start'], $window['end']);
            $hub = db_market_history_daily_aggregate_by_date_type_source('market_hub', $source['hub_source_id'], $window['start'], $window['end']);

            $allianceByDateType = [];
            foreach ($alliance as $row) {
                $allianceByDateType[(string) ($row['trade_date'] ?? '') . ':' . (int) ($row['type_id'] ?? 0)] = true;
            }

            $bucket = [];
            foreach ($hub as $row) {
                $date = (string) ($row['trade_date'] ?? '');
                $typeId = (int) ($row['type_id'] ?? 0);
                if ($date === '' || $typeId <= 0) {
                    continue;
                }
                $bucket[$date]['hub'] = ($bucket[$date]['hub'] ?? 0) + 1;
                if (!isset($allianceByDateType[$date . ':' . $typeId])) {
                    $bucket[$date]['missing'] = ($bucket[$date]['missing'] ?? 0) + 1;
                }
            }

            foreach ($bucket as $date => $values) {
                $rows[] = [
                    'date' => $date,
                    'module' => $selected['label'],
                    'metric' => number_format((int) ($values['missing'] ?? 0)),
                    'context' => number_format((int) ($values['hub'] ?? 0)) . ' hub-listed types',
                ];
            }
        }
    } catch (Throwable) {
        $rows = [];
    }

    usort($rows, static fn (array $a, array $b): int => strcmp((string) $b['date'], (string) $a['date']));

    return [
        'filters' => $filters,
        'module_registry' => $registry,
        'summary' => [
            ['label' => 'Selected Module', 'value' => $selected['label'], 'context' => $selected['description']],
            ['label' => 'Window', 'value' => (string) $filters['days'] . ' days', 'context' => $window['start'] . ' → ' . $window['end']],
            ['label' => 'Snapshots', 'value' => number_format(count($rows)), 'context' => 'Daily historical points for selected analytics module'],
        ],
        'rows' => $rows,
    ];
}

function sanitize_static_data_source_url(mixed $value): string
{
    $default = static_data_default_source_url();
    $candidate = trim((string) $value);

    if ($candidate === '') {
        return $default;
    }

    if (filter_var($candidate, FILTER_VALIDATE_URL) === false) {
        return $default;
    }

    $scheme = (string) parse_url($candidate, PHP_URL_SCHEME);
    if (!in_array(mb_strtolower($scheme), ['http', 'https'], true)) {
        return $default;
    }

    $normalized = mb_strtolower($candidate);
    if (!str_ends_with($normalized, '.zip')) {
        return $default;
    }

    return $candidate;
}

function static_data_default_source_url(): string
{
    return 'https://developers.eveonline.com/static-data/eve-online-static-data-latest-jsonl.zip';
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

function scheduler_job_dataset_key(string $jobKey): string
{
    return 'scheduler.job.' . $jobKey;
}

function scheduler_normalize_error_message(string $message): string
{
    $normalized = trim(preg_replace('/\s+/', ' ', $message) ?? '');
    if ($normalized === '') {
        $normalized = 'Job failed due to an unknown scheduler error.';
    }

    return mb_substr($normalized, 0, 500);
}

function scheduler_normalize_messages(array $messages): array
{
    $normalized = [];

    foreach ($messages as $message) {
        $text = scheduler_normalize_error_message((string) $message);
        if ($text === '') {
            continue;
        }

        $normalized[$text] = true;
    }

    return array_keys($normalized);
}

function scheduler_job_definitions(): array
{
    return [
        'alliance_current_sync' => [
            'timeout_seconds' => 180,
            'lock_ttl_seconds' => 300,
            'handler' => static function (): array {
                $structureId = configured_structure_destination_id_for_esi_sync();
                if ($structureId <= 0) {
                    throw new RuntimeException('Alliance current sync skipped: choose an alliance structure destination (NPC stations are not eligible for structure sync).');
                }

                return sync_alliance_structure_orders($structureId, 'incremental');
            },
        ],
        'alliance_historical_sync' => [
            'timeout_seconds' => 480,
            'lock_ttl_seconds' => 600,
            'handler' => static function (): array {
                $structureId = configured_structure_destination_id_for_esi_sync();
                if ($structureId <= 0) {
                    throw new RuntimeException('Alliance history sync skipped: choose an alliance structure destination (NPC stations are not eligible for structure sync).');
                }

                return sync_alliance_structure_orders($structureId, 'full');
            },
        ],
        'market_hub_historical_sync' => [
            'timeout_seconds' => 300,
            'lock_ttl_seconds' => 420,
            'handler' => static function (): array {
                $hubRef = market_hub_setting_reference();
                if ($hubRef === '') {
                    throw new RuntimeException('Hub history sync skipped: configure a market hub/station first.');
                }

                return sync_market_hub_history($hubRef, 'full');
            },
        ],
        'market_hub_local_history_sync' => [
            'timeout_seconds' => 180,
            'lock_ttl_seconds' => 300,
            'handler' => static function (): array {
                $hubRef = market_hub_setting_reference();
                if ($hubRef === '') {
                    throw new RuntimeException('Hub local history sync skipped: configure a market hub/station first.');
                }

                return sync_market_hub_local_history($hubRef, 'incremental');
            },
        ],
        'market_hub_current_sync' => [
            'timeout_seconds' => 240,
            'lock_ttl_seconds' => 360,
            'handler' => static function (): array {
                $hubRef = market_hub_setting_reference();
                if ($hubRef === '') {
                    throw new RuntimeException('Hub current sync skipped: configure a market hub/station first.');
                }

                return sync_market_hub_current_orders($hubRef, 'incremental');
            },
        ],
        'killmail_r2z2_sync' => [
            'timeout_seconds' => 90,
            'lock_ttl_seconds' => 180,
            'handler' => static function (): array {
                if (!killmail_ingestion_enabled()) {
                    return sync_result_shape() + ['warnings' => ['Killmail ingestion disabled in settings.']];
                }

                return sync_killmail_r2z2_stream('incremental');
            },
        ],
    ];
}


function scheduler_job_type(string $jobKey): string
{
    return match ($jobKey) {
        'alliance_current_sync', 'market_hub_current_sync' => 'sync.current',
        'alliance_historical_sync', 'market_hub_historical_sync', 'market_hub_local_history_sync' => 'sync.history',
        'killmail_r2z2_sync' => 'sync.killmail',
        default => 'sync.generic',
    };
}

function scheduler_job_status_from_sync_result(array $syncResult): string
{
    $rowsSeen = max(0, (int) ($syncResult['rows_seen'] ?? 0));
    $rowsWritten = max(0, (int) ($syncResult['rows_written'] ?? 0));
    $warnings = scheduler_normalize_messages((array) ($syncResult['warnings'] ?? []));

    if ($rowsSeen === 0 && $rowsWritten === 0 && $warnings !== []) {
        return 'skipped';
    }

    return 'success';
}

function scheduler_job_summary_message(array $result): string
{
    $status = (string) ($result['status'] ?? 'unknown');
    $jobKey = (string) ($result['job_key'] ?? 'unknown_job');

    if ($status === 'failed') {
        return (string) ($result['error'] ?? ('Job ' . $jobKey . ' failed.'));
    }

    if ($status === 'skipped') {
        $warnings = (array) ($result['warnings'] ?? []);

        return (string) ($warnings[0] ?? ('Job ' . $jobKey . ' skipped.'));
    }

    $meta = is_array($result['meta'] ?? null) ? $result['meta'] : [];
    $outcomeReason = trim((string) ($meta['outcome_reason'] ?? ''));
    if ($jobKey === 'killmail_r2z2_sync' && $outcomeReason !== '') {
        return 'Killmail sync outcome: ' . $outcomeReason . '.';
    }

    $rowsSeen = max(0, (int) ($result['rows_seen'] ?? 0));
    $rowsWritten = max(0, (int) ($result['rows_written'] ?? 0));

    return 'Processed ' . $rowsSeen . ' records, wrote ' . $rowsWritten . ' records.';
}
function scheduler_due_jobs(): array
{
    $definitions = scheduler_job_definitions();
    $due = db_sync_schedule_fetch_due_jobs(20);
    $claimed = [];

    foreach ($due as $job) {
        $scheduleId = (int) ($job['id'] ?? 0);
        if ($scheduleId <= 0) {
            continue;
        }

        $jobKey = (string) ($job['job_key'] ?? '');
        $lockTtl = (int) ($definitions[$jobKey]['lock_ttl_seconds'] ?? 300);
        $claimedJob = db_sync_schedule_claim_job($scheduleId, $lockTtl);
        if ($claimedJob !== null) {
            $claimed[] = $claimedJob;
        }
    }

    return $claimed;
}

function scheduler_run_job(array $job): array
{
    $jobKey = (string) ($job['job_key'] ?? '');
    $scheduleId = (int) ($job['id'] ?? 0);
    $definitions = scheduler_job_definitions();
    $definition = $definitions[$jobKey] ?? null;
    $datasetKey = scheduler_job_dataset_key($jobKey !== '' ? $jobKey : 'unknown');
    $jobType = scheduler_job_type($jobKey);
    $scheduledFor = (string) ($job['next_run_at'] ?? '');
    $startedAtUnix = microtime(true);
    $startedAtIso = gmdate(DATE_ATOM, (int) $startedAtUnix);
    $runId = db_sync_run_start($datasetKey, 'incremental', null);

    if ($definition === null || !isset($definition['handler']) || !is_callable($definition['handler'])) {
        $message = scheduler_normalize_error_message('Unknown scheduler job key: ' . ($jobKey !== '' ? $jobKey : '[empty]') . '.');
        mark_sync_failure($datasetKey, 'incremental', $message);
        db_sync_run_finish($runId, 'failed', 0, 0, null, $message);
        if ($scheduleId > 0) {
            db_sync_schedule_mark_failure($scheduleId, $message);
        }

        return [
            'job_id' => $scheduleId,
            'job_key' => $jobKey,
            'job_type' => $jobType,
            'scheduled_for' => $scheduledFor,
            'started_at' => $startedAtIso,
            'finished_at' => gmdate(DATE_ATOM),
            'duration_ms' => (int) round((microtime(true) - $startedAtUnix) * 1000),
            'status' => 'failed',
            'error' => $message,
            'rows_seen' => 0,
            'rows_written' => 0,
            'warnings' => [],
            'meta' => [],
            'summary' => $message,
        ];
    }

    $timeoutSeconds = max(30, min(3600, (int) ($definition['timeout_seconds'] ?? 300)));
    $startedAt = microtime(true);

    try {
        if (function_exists('set_time_limit')) {
            @set_time_limit($timeoutSeconds + 5);
        }

        $handler = $definition['handler'];
        $syncResult = $handler();
        $durationMs = (int) round((microtime(true) - $startedAt) * 1000);
        if ($durationMs > ($timeoutSeconds * 1000)) {
            throw new RuntimeException('Job exceeded timeout of ' . $timeoutSeconds . ' seconds.');
        }

        $rowsSeen = max(0, (int) ($syncResult['rows_seen'] ?? 0));
        $rowsWritten = max(0, (int) ($syncResult['rows_written'] ?? 0));
        $warnings = scheduler_normalize_messages((array) ($syncResult['warnings'] ?? []));
        $status = scheduler_job_status_from_sync_result($syncResult);
        $cursor = isset($syncResult['cursor']) && $syncResult['cursor'] !== null
            ? mb_substr(trim((string) $syncResult['cursor']), 0, 190)
            : 'finished_at:' . gmdate('Y-m-d H:i:s') . ';duration_ms:' . $durationMs;
        $checksum = isset($syncResult['checksum']) && $syncResult['checksum'] !== null
            ? mb_substr(trim((string) $syncResult['checksum']), 0, 190)
            : sync_checksum([
            'job_key' => $jobKey,
            'rows_seen' => $rowsSeen,
            'rows_written' => $rowsWritten,
            'warnings' => $warnings,
        ]);

        mark_sync_success($datasetKey, 'incremental', $cursor, $rowsWritten, $checksum);
        db_sync_run_finish($runId, 'success', $rowsSeen, $rowsWritten, $cursor, null);
        if ($scheduleId > 0) {
            db_sync_schedule_mark_success($scheduleId);
        }

        $result = [
            'job_id' => $scheduleId,
            'job_key' => $jobKey,
            'job_type' => $jobType,
            'scheduled_for' => $scheduledFor,
            'started_at' => $startedAtIso,
            'finished_at' => gmdate(DATE_ATOM),
            'status' => $status,
            'error' => null,
            'rows_seen' => $rowsSeen,
            'rows_written' => $rowsWritten,
            'warnings' => $warnings,
            'duration_ms' => $durationMs,
            'meta' => is_array($syncResult['meta'] ?? null) ? $syncResult['meta'] : [],
        ];
        $result['summary'] = scheduler_job_summary_message($result);

        return $result;
    } catch (Throwable $exception) {
        $message = scheduler_normalize_error_message($exception->getMessage());
        mark_sync_failure($datasetKey, 'incremental', $message);
        db_sync_run_finish($runId, 'failed', 0, 0, null, $message);
        if ($scheduleId > 0) {
            db_sync_schedule_mark_failure($scheduleId, $message);
        }

        return [
            'job_id' => $scheduleId,
            'job_key' => $jobKey,
            'job_type' => $jobType,
            'scheduled_for' => $scheduledFor,
            'started_at' => $startedAtIso,
            'finished_at' => gmdate(DATE_ATOM),
            'duration_ms' => (int) round((microtime(true) - $startedAtUnix) * 1000),
            'status' => 'failed',
            'error' => $message,
            'rows_seen' => 0,
            'rows_written' => 0,
            'warnings' => [],
            'meta' => [],
            'summary' => $message,
        ];
    }
}

function cron_tick_run(?callable $logger = null): array
{
    $jobs = scheduler_due_jobs();
    $results = [];
    $successCount = 0;
    $failureCount = 0;

    foreach ($jobs as $job) {
        $jobId = (int) ($job['id'] ?? 0);
        $jobKey = (string) ($job['job_key'] ?? 'unknown_job');
        $jobType = scheduler_job_type($jobKey);
        $scheduledFor = (string) ($job['next_run_at'] ?? '');

        if ($logger !== null) {
            $logger('job.started', [
                'job_id' => $jobId,
                'job' => $jobKey,
                'job_type' => $jobType,
                'scheduled_for' => $scheduledFor,
            ]);
        }

        $result = scheduler_run_job($job);
        $results[] = $result;

        if ($logger !== null) {
            $jobEvent = ($result['status'] ?? 'failed') === 'failed' ? 'job.failed' : 'job.completed';
            $logger($jobEvent, [
                'job_id' => (int) ($result['job_id'] ?? $jobId),
                'job' => (string) ($result['job_key'] ?? $jobKey),
                'job_type' => (string) ($result['job_type'] ?? $jobType),
                'scheduled_for' => (string) ($result['scheduled_for'] ?? $scheduledFor),
                'actual_start_time' => (string) ($result['started_at'] ?? ''),
                'finish_time' => (string) ($result['finished_at'] ?? ''),
                'duration_ms' => (int) ($result['duration_ms'] ?? 0),
                'status' => (string) ($result['status'] ?? 'failed'),
                'summary' => (string) ($result['summary'] ?? ''),
                'error' => ($result['status'] ?? '') === 'failed' ? (string) ($result['error'] ?? '') : null,
            ]);

            $meta = is_array($result['meta'] ?? null) ? $result['meta'] : [];
            if ($meta !== []) {
                $basePayload = [
                    'job_id' => (int) ($result['job_id'] ?? $jobId),
                    'job' => (string) ($result['job_key'] ?? $jobKey),
                    'status' => (string) ($result['status'] ?? 'failed'),
                ];
                $logger('job.' . str_replace('.', '_', $jobType) . '.outcome', $basePayload + $meta);

                if ($jobKey === 'killmail_r2z2_sync') {
                    $logger('job.killmail_r2z2_sync.outcome', $basePayload + $meta);
                }
            }
        }

        $jobStatus = (string) ($result['status'] ?? 'failed');
        if ($jobStatus === 'success') {
            $successCount++;
            continue;
        }

        if ($jobStatus === 'failed') {
            $failureCount++;
        }
    }

    return [
        'ran_at' => gmdate('Y-m-d H:i:s'),
        'jobs_due' => count($jobs),
        'jobs_processed' => count($results),
        'jobs_succeeded' => $successCount,
        'jobs_failed' => $failureCount,
        'results' => $results,
    ];
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
        'meta' => [],
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

function sync_dataset_key_market_hub_local_history_daily(string $hubKey): string
{
    return 'market.hub.' . $hubKey . '.history.local.daily';
}

function sync_dataset_key_market_hub_current_orders(string $hubKey): string
{
    return 'market.hub.' . $hubKey . '.orders.current';
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

function market_hub_setting_reference(): string
{
    $configuredHub = trim((string) get_setting('market_station_id', ''));

    if ($configuredHub === '') {
        return '';
    }

    $configuredStationId = (int) $configuredHub;
    if ($configuredStationId <= 0) {
        return '';
    }

    return $configuredHub;
}

function market_hub_reference_name(): string
{
    return selected_station_name('market_station_id') ?? 'Reference market hub not configured';
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

function market_hub_local_history_source(): string
{
    return 'market_hub_current_sync';
}

function market_hub_current_sync_latest_dataset(string|int|null $hubRef = null): array
{
    $resolvedHubRef = trim((string) ($hubRef ?? market_hub_setting_reference()));
    if ($resolvedHubRef === '') {
        return [
            'hub_ref' => '',
            'source' => market_hub_local_history_source(),
            'source_id' => 0,
            'observed_at' => null,
            'trade_date' => '',
            'rows' => [],
        ];
    }

    $sourceId = sync_source_id_from_hub_ref($resolvedHubRef);
    $rows = db_market_orders_current_latest_snapshot_rows('market_hub', $sourceId);
    $observedAt = $rows !== [] ? (string) ($rows[0]['observed_at'] ?? '') : '';

    return [
        'hub_ref' => $resolvedHubRef,
        'source' => market_hub_local_history_source(),
        'source_id' => $sourceId,
        'observed_at' => $observedAt !== '' ? $observedAt : null,
        'trade_date' => $observedAt !== '' ? market_hub_local_history_trade_date($observedAt) : '',
        'rows' => $rows,
    ];
}

function market_hub_current_snapshot_metric_seed(int $sourceId, int $typeId, string $observedAt): array
{
    return [
        'source_id' => $sourceId,
        'type_id' => $typeId,
        'observed_at' => $observedAt,
        'best_sell_price' => null,
        'best_buy_price' => null,
        'total_volume' => 0,
        'buy_order_count' => 0,
        'sell_order_count' => 0,
    ];
}

function market_hub_current_snapshot_finalize_metric(array $metric): ?array
{
    $typeId = (int) ($metric['type_id'] ?? 0);
    $sourceId = (int) ($metric['source_id'] ?? 0);
    $observedAt = trim((string) ($metric['observed_at'] ?? ''));
    if ($typeId <= 0 || $sourceId <= 0 || $observedAt === '') {
        return null;
    }

    $sellPrice = isset($metric['best_sell_price']) && $metric['best_sell_price'] !== null ? round((float) $metric['best_sell_price'], 2) : null;
    $buyPrice = isset($metric['best_buy_price']) && $metric['best_buy_price'] !== null ? round((float) $metric['best_buy_price'], 2) : null;
    $closePrice = $sellPrice ?? $buyPrice;
    if ($closePrice === null) {
        return null;
    }

    $spreadValue = null;
    $spreadPercent = null;
    if ($sellPrice !== null && $buyPrice !== null) {
        $spreadValue = round($sellPrice - $buyPrice, 2);
        if ($buyPrice > 0) {
            $spreadPercent = round(($spreadValue / $buyPrice) * 100, 4);
        }
    }

    return [
        'source' => market_hub_local_history_source(),
        'source_id' => $sourceId,
        'type_id' => $typeId,
        'trade_date' => market_hub_local_history_trade_date($observedAt),
        'close_price' => $closePrice,
        'buy_price' => $buyPrice,
        'sell_price' => $sellPrice,
        'spread_value' => $spreadValue,
        'spread_percent' => $spreadPercent,
        'volume' => max(0, (int) ($metric['total_volume'] ?? 0)),
        'buy_order_count' => max(0, (int) ($metric['buy_order_count'] ?? 0)),
        'sell_order_count' => max(0, (int) ($metric['sell_order_count'] ?? 0)),
        'captured_at' => $observedAt,
    ];
}

function market_hub_current_snapshot_metrics_by_type(array $rows, ?int $sourceId = null): array
{
    $aggregates = [];

    foreach ($rows as $row) {
        if (!is_array($row)) {
            continue;
        }

        $typeId = (int) ($row['type_id'] ?? 0);
        if ($typeId <= 0) {
            continue;
        }

        $rowSourceId = max(0, (int) ($row['source_id'] ?? $sourceId ?? 0));
        $observedAt = trim((string) ($row['observed_at'] ?? ''));
        if ($rowSourceId <= 0 || $observedAt === '') {
            continue;
        }

        if (!isset($aggregates[$typeId])) {
            $aggregates[$typeId] = market_hub_current_snapshot_metric_seed($rowSourceId, $typeId, $observedAt);
        }

        $price = (float) ($row['price'] ?? 0);
        $volumeRemain = max(0, (int) ($row['volume_remain'] ?? 0));
        $isBuyOrder = (int) ($row['is_buy_order'] ?? 0) === 1;

        $aggregates[$typeId]['total_volume'] += $volumeRemain;

        if ($volumeRemain <= 0) {
            continue;
        }

        if ($isBuyOrder) {
            $aggregates[$typeId]['buy_order_count']++;
            $currentBestBuy = $aggregates[$typeId]['best_buy_price'];
            if ($currentBestBuy === null || $price > (float) $currentBestBuy) {
                $aggregates[$typeId]['best_buy_price'] = $price;
            }

            continue;
        }

        $aggregates[$typeId]['sell_order_count']++;
        $currentBestSell = $aggregates[$typeId]['best_sell_price'];
        if ($currentBestSell === null || $price < (float) $currentBestSell) {
            $aggregates[$typeId]['best_sell_price'] = $price;
        }
    }

    $canonicalRows = [];
    foreach ($aggregates as $typeId => $aggregate) {
        $canonical = market_hub_current_snapshot_finalize_metric($aggregate);
        if ($canonical === null) {
            continue;
        }

        $canonicalRows[$typeId] = $canonical;
    }

    ksort($canonicalRows);

    return array_values($canonicalRows);
}

function market_hub_local_history_trade_date(string $observedAt): string
{
    $raw = trim($observedAt);
    if ($raw === '') {
        return '';
    }

    try {
        $utc = new DateTimeZone('UTC');
        $appTz = new DateTimeZone(app_timezone());
        $capturedAt = new DateTimeImmutable($raw, $utc);

        return $capturedAt->setTimezone($appTz)->format('Y-m-d');
    } catch (Throwable) {
        return '';
    }
}


function market_hub_local_history_window_days_default(): int
{
    $retentionDays = (int) get_setting('raw_order_snapshot_retention_days', '30');

    return max(1, min(365, $retentionDays));
}

function market_hub_local_history_window_days_normalize(?int $windowDays = null): int
{
    if ($windowDays === null) {
        return market_hub_local_history_window_days_default();
    }

    return max(1, min(365, $windowDays));
}

function market_hub_local_history_window_start_observed_at(int $windowDays): string
{
    $safeWindowDays = market_hub_local_history_window_days_normalize($windowDays);

    try {
        $appTz = new DateTimeZone(app_timezone());
        $windowStart = (new DateTimeImmutable('now', $appTz))
            ->setTime(0, 0, 0)
            ->modify('-' . ($safeWindowDays - 1) . ' days')
            ->setTimezone(new DateTimeZone('UTC'));

        return $windowStart->format('Y-m-d H:i:s');
    } catch (Throwable) {
        return gmdate('Y-m-d 00:00:00', strtotime('-' . ($safeWindowDays - 1) . ' days'));
    }
}

function market_hub_local_history_daily_bucket_seed(array $snapshotRow): array
{
    $closePrice = (float) ($snapshotRow['close_price'] ?? 0);

    return [
        'source' => market_hub_local_history_source(),
        'source_id' => max(0, (int) ($snapshotRow['source_id'] ?? 0)),
        'type_id' => max(0, (int) ($snapshotRow['type_id'] ?? 0)),
        'trade_date' => (string) ($snapshotRow['trade_date'] ?? ''),
        'open_price' => $closePrice,
        'high_price' => $closePrice,
        'low_price' => $closePrice,
        'close_price' => $closePrice,
        'buy_price' => $snapshotRow['buy_price'] ?? null,
        'sell_price' => $snapshotRow['sell_price'] ?? null,
        'spread_value' => $snapshotRow['spread_value'] ?? null,
        'spread_percent' => $snapshotRow['spread_percent'] ?? null,
        'volume' => max(0, (int) ($snapshotRow['volume'] ?? 0)),
        'buy_order_count' => max(0, (int) ($snapshotRow['buy_order_count'] ?? 0)),
        'sell_order_count' => max(0, (int) ($snapshotRow['sell_order_count'] ?? 0)),
        'captured_at' => (string) ($snapshotRow['captured_at'] ?? ''),
    ];
}

function market_hub_local_history_daily_bucket_observe(array $bucket, array $snapshotRow): array
{
    $closePrice = (float) ($snapshotRow['close_price'] ?? 0);
    $capturedAt = trim((string) ($snapshotRow['captured_at'] ?? ''));
    $currentCapturedAt = trim((string) ($bucket['captured_at'] ?? ''));

    $bucket['high_price'] = max((float) ($bucket['high_price'] ?? $closePrice), $closePrice);
    $bucket['low_price'] = min((float) ($bucket['low_price'] ?? $closePrice), $closePrice);

    if ((float) ($bucket['open_price'] ?? 0) <= 0) {
        $bucket['open_price'] = $closePrice;
    }

    if ($currentCapturedAt === '' || ($capturedAt !== '' && strcmp($capturedAt, $currentCapturedAt) >= 0)) {
        $bucket['close_price'] = $closePrice;
        $bucket['buy_price'] = $snapshotRow['buy_price'] ?? null;
        $bucket['sell_price'] = $snapshotRow['sell_price'] ?? null;
        $bucket['spread_value'] = $snapshotRow['spread_value'] ?? null;
        $bucket['spread_percent'] = $snapshotRow['spread_percent'] ?? null;
        $bucket['volume'] = max(0, (int) ($snapshotRow['volume'] ?? 0));
        $bucket['buy_order_count'] = max(0, (int) ($snapshotRow['buy_order_count'] ?? 0));
        $bucket['sell_order_count'] = max(0, (int) ($snapshotRow['sell_order_count'] ?? 0));
        $bucket['captured_at'] = $capturedAt;
    }

    return $bucket;
}

function market_hub_local_history_daily_rebuild_from_snapshot_metrics(array $snapshotMetrics): array
{
    $dailyBuckets = [];
    $snapshotCount = 0;
    $tradeDates = [];

    foreach ($snapshotMetrics as $metric) {
        if (!is_array($metric)) {
            continue;
        }

        $snapshotRow = market_hub_current_snapshot_finalize_metric($metric);
        if ($snapshotRow === null) {
            continue;
        }

        $tradeDate = trim((string) ($snapshotRow['trade_date'] ?? ''));
        $typeId = (int) ($snapshotRow['type_id'] ?? 0);
        $sourceId = (int) ($snapshotRow['source_id'] ?? 0);
        if ($tradeDate === '' || $typeId <= 0 || $sourceId <= 0) {
            continue;
        }

        $snapshotCount++;
        $tradeDates[$tradeDate] = true;
        $bucketKey = $tradeDate . ':' . $typeId;

        if (!isset($dailyBuckets[$bucketKey])) {
            $dailyBuckets[$bucketKey] = market_hub_local_history_daily_bucket_seed($snapshotRow);
            continue;
        }

        $dailyBuckets[$bucketKey] = market_hub_local_history_daily_bucket_observe($dailyBuckets[$bucketKey], $snapshotRow);
    }

    uasort($dailyBuckets, static function (array $left, array $right): int {
        $dateCompare = strcmp((string) ($left['trade_date'] ?? ''), (string) ($right['trade_date'] ?? ''));
        if ($dateCompare !== 0) {
            return $dateCompare;
        }

        return ((int) ($left['type_id'] ?? 0)) <=> ((int) ($right['type_id'] ?? 0));
    });

    return [
        'rows' => array_values($dailyBuckets),
        'snapshot_metric_rows' => $snapshotCount,
        'trade_dates' => array_keys($tradeDates),
    ];
}

function market_hub_local_history_daily_merge_row(array $snapshotRow, ?array $existingRow = null): array
{
    $effectiveSnapshot = $snapshotRow;
    $closePrice = (float) ($snapshotRow['close_price'] ?? 0);
    $capturedAt = trim((string) ($snapshotRow['captured_at'] ?? ''));
    $existingCapturedAt = trim((string) (($existingRow['observed_at'] ?? $existingRow['captured_at'] ?? '')));

    if ($existingRow !== null && $existingCapturedAt !== '' && $capturedAt !== '' && strcmp($existingCapturedAt, $capturedAt) > 0) {
        $effectiveSnapshot = [
            'source_id' => $snapshotRow['source_id'] ?? 0,
            'type_id' => $snapshotRow['type_id'] ?? 0,
            'trade_date' => $snapshotRow['trade_date'] ?? '',
            'close_price' => $existingRow['close_price'] ?? $closePrice,
            'buy_price' => $existingRow['buy_price'] ?? null,
            'sell_price' => $existingRow['sell_price'] ?? null,
            'spread_value' => $existingRow['spread_value'] ?? null,
            'spread_percent' => $existingRow['spread_percent'] ?? null,
            'volume' => $existingRow['volume'] ?? 0,
            'buy_order_count' => $existingRow['buy_order_count'] ?? 0,
            'sell_order_count' => $existingRow['sell_order_count'] ?? 0,
            'captured_at' => $existingCapturedAt,
        ];
        $closePrice = (float) ($effectiveSnapshot['close_price'] ?? $closePrice);
        $capturedAt = $existingCapturedAt;
    }

    $openPrice = $existingRow !== null ? (float) ($existingRow['open_price'] ?? $closePrice) : $closePrice;
    $highBaseline = $existingRow !== null ? (float) ($existingRow['high_price'] ?? $closePrice) : $closePrice;
    $lowBaseline = $existingRow !== null ? (float) ($existingRow['low_price'] ?? $closePrice) : $closePrice;

    return [
        'source' => market_hub_local_history_source(),
        'source_id' => max(0, (int) ($snapshotRow['source_id'] ?? 0)),
        'type_id' => max(0, (int) ($snapshotRow['type_id'] ?? 0)),
        'trade_date' => (string) ($snapshotRow['trade_date'] ?? ''),
        'open_price' => $openPrice,
        'high_price' => max($highBaseline, $closePrice),
        'low_price' => min($lowBaseline, $closePrice),
        'close_price' => $closePrice,
        'buy_price' => $effectiveSnapshot['buy_price'] ?? ($existingRow['buy_price'] ?? null),
        'sell_price' => $effectiveSnapshot['sell_price'] ?? ($existingRow['sell_price'] ?? null),
        'spread_value' => $effectiveSnapshot['spread_value'] ?? ($existingRow['spread_value'] ?? null),
        'spread_percent' => $effectiveSnapshot['spread_percent'] ?? ($existingRow['spread_percent'] ?? null),
        'volume' => max(0, (int) ($effectiveSnapshot['volume'] ?? 0)),
        'buy_order_count' => max(0, (int) ($effectiveSnapshot['buy_order_count'] ?? 0)),
        'sell_order_count' => max(0, (int) ($effectiveSnapshot['sell_order_count'] ?? 0)),
        'captured_at' => $capturedAt,
    ];
}

function market_hub_current_sync_daily_canonical_rows(string|int|null $hubRef = null): array
{
    $dataset = market_hub_current_sync_latest_dataset($hubRef);
    $sourceId = (int) ($dataset['source_id'] ?? 0);
    $tradeDate = (string) ($dataset['trade_date'] ?? '');
    $snapshotRows = market_hub_current_snapshot_metrics_by_type($dataset['rows'] ?? [], $sourceId);

    if ($sourceId <= 0 || $tradeDate === '' || $snapshotRows === []) {
        return [];
    }

    $existingRows = [];
    foreach (db_market_hub_local_history_daily_by_trade_date(market_hub_local_history_source(), $sourceId, $tradeDate) as $row) {
        $typeId = (int) ($row['type_id'] ?? 0);
        if ($typeId > 0) {
            $existingRows[$typeId] = $row;
        }
    }

    $canonicalRows = [];
    foreach ($snapshotRows as $snapshotRow) {
        $typeId = (int) ($snapshotRow['type_id'] ?? 0);
        if ($typeId <= 0) {
            continue;
        }

        $canonicalRows[] = market_hub_local_history_daily_merge_row($snapshotRow, $existingRows[$typeId] ?? null);
    }

    return $canonicalRows;
}

function canonicalize_esi_market_order(array $order, int $sourceId, string $observedAt, string $sourceType = 'alliance_structure'): ?array
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

    $normalizedSourceType = $sourceType === 'market_hub' ? 'market_hub' : 'alliance_structure';

    return [
        'source_type' => $normalizedSourceType,
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

function esi_market_request_headers(array $extraHeaders = []): array
{
    $headers = [
        'Accept: application/json',
        'X-Compatibility-Date: 2025-12-16',
        'X-Tenant: tranquility',
    ];

    foreach ($extraHeaders as $header) {
        $normalized = trim((string) $header);
        if ($normalized !== '') {
            $headers[] = $normalized;
        }
    }

    return $headers;
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
    $pagesProcessed = 0;

    while ($page <= $maxPages) {
        $response = http_get_json_with_backoff(
            'https://esi.evetech.net/latest/markets/structures/' . $structureId . '/?page=' . $page,
            esi_market_request_headers([
                'Authorization: Bearer ' . $accessToken,
            ])
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
        $pagesProcessed++;
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
            $result['meta'] = [
                'operational_market_id' => $structureId,
                'operational_market' => selected_station_name('alliance_station_id') ?? ('Structure ' . $structureId),
                'records_fetched' => (int) $result['rows_seen'],
                'records_inserted' => 0,
                'records_updated' => 0,
                'records_skipped' => (int) $result['rows_seen'],
                'records_deleted' => 0,
                'api_pages_processed' => $pagesProcessed,
                'no_changes' => true,
            ];

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
        $result['meta'] = [
            'operational_market_id' => $structureId,
                'operational_market' => selected_station_name('alliance_station_id') ?? ('Structure ' . $structureId),
            'records_fetched' => (int) $result['rows_seen'],
            'records_inserted' => 0,
            'records_updated' => (int) $writtenCurrent,
            'records_skipped' => max(0, (int) $result['rows_seen'] - (int) $writtenCurrent),
            'records_deleted' => 0,
            'history_rows_generated' => (int) $writtenHistory,
            'api_pages_processed' => $pagesProcessed,
            'no_changes' => ((int) $writtenCurrent + (int) $writtenHistory) === 0,
        ];

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

function eve_tycoon_current_orders_payload_rows(array $payload): array
{
    foreach (['items', 'data', 'results', 'orders'] as $key) {
        if (isset($payload[$key]) && is_array($payload[$key])) {
            return $payload[$key];
        }
    }

    return array_is_list($payload) ? $payload : [];
}

function eve_tycoon_current_order_to_canonical(array $row, int $sourceId, string $observedAt): ?array
{
    $orderId = (int) ($row['order_id'] ?? $row['orderId'] ?? 0);
    $typeId = (int) ($row['type_id'] ?? $row['typeId'] ?? 0);
    if ($orderId <= 0 || $typeId <= 0) {
        return null;
    }

    $issuedAt = strtotime((string) ($row['issued'] ?? $row['issued_at'] ?? ''));
    if ($issuedAt === false) {
        $issuedAt = time();
    }

    $duration = max(1, (int) ($row['duration'] ?? 90));
    $expiresAt = strtotime((string) ($row['expires'] ?? $row['expires_at'] ?? ''));
    if ($expiresAt === false) {
        $expiresAt = strtotime('+' . $duration . ' days', $issuedAt);
    }

    return [
        'source_type' => 'market_hub',
        'source_id' => $sourceId,
        'type_id' => $typeId,
        'order_id' => $orderId,
        'is_buy_order' => !empty($row['is_buy_order'] ?? $row['isBuyOrder'] ?? false) ? 1 : 0,
        'price' => (float) ($row['price'] ?? 0),
        'volume_remain' => max(0, (int) ($row['volume_remain'] ?? $row['volumeRemain'] ?? $row['volume'] ?? 0)),
        'volume_total' => max(0, (int) ($row['volume_total'] ?? $row['volumeTotal'] ?? $row['volume'] ?? 0)),
        'min_volume' => max(1, (int) ($row['min_volume'] ?? $row['minVolume'] ?? 1)),
        'range' => (string) ($row['range'] ?? 'region'),
        'duration' => $duration,
        'issued' => gmdate('Y-m-d H:i:s', $issuedAt),
        'expires' => gmdate('Y-m-d H:i:s', $expiresAt),
        'observed_at' => $observedAt,
    ];
}

function market_hub_reference_context(string|int $hubRef): array
{
    $hubKey = trim((string) $hubRef);
    $hubId = (int) $hubKey;
    $fallbackName = market_hub_reference_name();

    if ($hubId <= 0) {
        return [
            'hub_id' => $hubKey,
            'hub_name' => $fallbackName,
            'hub_type' => 'unknown',
            'api_source' => 'unknown',
            'region_id' => null,
            'system_id' => null,
            'structure_id' => null,
        ];
    }

    try {
        $npcStation = db_ref_npc_station_by_id($hubId);
    } catch (Throwable) {
        $npcStation = null;
    }

    try {
        $structureMetadata = db_alliance_structure_metadata_get($hubId);
    } catch (Throwable) {
        $structureMetadata = null;
    }

    $looksLikeStructureId = preg_match('/^[1-9][0-9]{9,19}$/', (string) $hubId) === 1;
    $hasStructureMetadata = $structureMetadata !== null;
    $preferStructure = $hasStructureMetadata || $looksLikeStructureId;

    if ($npcStation !== null && !$preferStructure) {
        $systemId = (int) ($npcStation['system_id'] ?? 0);
        $resolvedSystemId = $systemId > 0 ? $systemId : null;

        try {
            $regionId = $resolvedSystemId !== null ? db_ref_system_region_id($resolvedSystemId) : null;
        } catch (Throwable) {
            $regionId = null;
        }

        $npcName = trim((string) ($npcStation['station_name'] ?? ''));
        if ($npcName === '' || is_placeholder_station_name($npcName, $hubId)) {
            try {
                $metadata = esi_npc_station_metadata($hubId);
            } catch (Throwable) {
                $metadata = null;
            }

            if ($metadata !== null && trim((string) ($metadata['name'] ?? '')) !== '') {
                $npcName = trim((string) ($metadata['name'] ?? ''));
            }
        }

        return [
            'hub_id' => (string) $hubId,
            'hub_name' => $npcName !== '' ? $npcName : $fallbackName,
            'hub_type' => 'npc_station',
            'api_source' => 'esi.region_orders',
            'region_id' => $regionId,
            'system_id' => $resolvedSystemId,
            'structure_id' => null,
        ];
    }

    if ($preferStructure) {
        $structureName = trim((string) ($structureMetadata['structure_name'] ?? ''));

        return [
            'hub_id' => (string) $hubId,
            'hub_name' => $structureName !== '' ? $structureName : (selected_station_name('market_station_id') ?? ('Structure #' . $hubId)),
            'hub_type' => 'structure',
            'api_source' => 'esi.structure_orders',
            'region_id' => null,
            'system_id' => null,
            'structure_id' => $hubId,
        ];
    }

    return [
        'hub_id' => (string) $hubId,
        'hub_name' => selected_station_name('market_station_id') ?? $fallbackName,
        'hub_type' => 'unknown',
        'api_source' => 'unknown',
        'region_id' => null,
        'system_id' => null,
        'structure_id' => null,
    ];
}

function sync_market_hub_current_orders(string|int $hubRef, string $runMode = 'incremental'): array
{
    $result = sync_result_shape();
    $syncMode = sync_mode_normalize($runMode);
    $hubKey = trim((string) $hubRef);
    if ($hubKey === '') {
        $result['warnings'][] = 'Market hub reference is required.';

        return $result;
    }

    $hubContext = market_hub_reference_context($hubKey);
    $sourceId = sync_source_id_from_hub_ref($hubKey);
    $datasetKey = sync_dataset_key_market_hub_current_orders($hubKey);
    $runId = db_sync_run_start($datasetKey, $syncMode, sync_watermark($datasetKey));

    try {
        $observedAt = gmdate('Y-m-d H:i:s');
        $page = 1;
        $maxPages = 1;
        $canonicalRows = [];
        $pagesProcessed = 0;

        $selectedHubId = (string) ($hubContext['hub_id'] ?? $hubKey);
        $selectedHubName = (string) ($hubContext['hub_name'] ?? market_hub_reference_name());
        $selectedHubType = (string) ($hubContext['hub_type'] ?? 'unknown');
        $effectiveApiSource = (string) ($hubContext['api_source'] ?? 'unknown');
        $resolvedRegionId = null;
        $resolvedStructureId = null;

        if ($selectedHubType === 'structure') {
            $structureId = (int) ($hubContext['structure_id'] ?? 0);
            if ($structureId <= 0) {
                throw new RuntimeException('Hub current sync requires a valid structure ID when the reference hub type is structure.');
            }
            $resolvedStructureId = $structureId;

            $context = esi_lookup_context(esi_required_market_structure_scopes());
            if (($context['ok'] ?? false) !== true) {
                throw new RuntimeException((string) ($context['error'] ?? 'Missing ESI context for structure market sync.'));
            }

            $accessToken = (string) ($context['token']['access_token'] ?? '');

            while ($page <= $maxPages) {
                $endpoint = 'https://esi.evetech.net/latest/markets/structures/' . $structureId . '/?page=' . $page;
                $response = http_get_json_with_backoff(
                    $endpoint,
                    esi_market_request_headers([
                        'Authorization: Bearer ' . $accessToken,
                    ])
                );
                $status = (int) ($response['status'] ?? 500);

                if ($status >= 400) {
                    throw new RuntimeException('ESI structure market sync failed on page ' . $page . ' with status ' . $status . '.');
                }

                $payload = $response['json'] ?? [];
                if (!is_array($payload)) {
                    throw new RuntimeException('ESI structure market page ' . $page . ' returned a non-array payload.');
                }

                $pagesProcessed++;
                foreach ($payload as $order) {
                    if (!is_array($order)) {
                        continue;
                    }

                    $result['rows_seen']++;
                    $mapped = canonicalize_esi_market_order($order, $sourceId, $observedAt, 'market_hub');
                    if ($mapped !== null) {
                        $canonicalRows[] = $mapped;
                    }
                }

                $pagesHeader = $response['headers']['x-pages'] ?? '1';
                if (is_array($pagesHeader)) {
                    $pagesHeader = end($pagesHeader);
                }

                $maxPages = max(1, (int) $pagesHeader);
                $page++;
            }

            $result['cursor'] = 'observed_at:' . $observedAt . ';source:structure;id:' . $structureId . ';page:' . max(1, $page - 1);
        } elseif ($selectedHubType === 'npc_station') {
            $regionId = (int) ($hubContext['region_id'] ?? 0);
            $systemId = (int) ($hubContext['system_id'] ?? 0);
            $stationId = (int) $hubKey;
            if ($stationId <= 0 || $systemId <= 0 || $regionId <= 0) {
                throw new RuntimeException('Missing station→system→region mapping (selected_hub_id=' . $stationId . ', system_id=' . $systemId . ', resolved_region_id=' . $regionId . ').');
            }
            $resolvedRegionId = $regionId;

            while ($page <= $maxPages) {
                $endpoint = 'https://esi.evetech.net/latest/markets/' . $regionId . '/orders/?order_type=all&page=' . $page;
                $response = http_get_json_with_backoff($endpoint, esi_market_request_headers());
                $status = (int) ($response['status'] ?? 500);

                if ($status >= 400) {
                    throw new RuntimeException('ESI region market sync failed on page ' . $page . ' with status ' . $status . '.');
                }

                $payload = $response['json'] ?? [];
                if (!is_array($payload)) {
                    throw new RuntimeException('ESI region market page ' . $page . ' returned a non-array payload.');
                }

                $pagesProcessed++;
                foreach ($payload as $order) {
                    if (!is_array($order)) {
                        continue;
                    }

                    if ((int) ($order['location_id'] ?? 0) !== $stationId) {
                        continue;
                    }

                    $result['rows_seen']++;
                    $mapped = canonicalize_esi_market_order($order, $sourceId, $observedAt, 'market_hub');
                    if ($mapped !== null) {
                        $canonicalRows[] = $mapped;
                    }
                }

                $pagesHeader = $response['headers']['x-pages'] ?? '1';
                if (is_array($pagesHeader)) {
                    $pagesHeader = end($pagesHeader);
                }

                $maxPages = max(1, (int) $pagesHeader);
                $page++;
            }

            $result['cursor'] = 'observed_at:' . $observedAt . ';source:region;id:' . $regionId . ';page:' . max(1, $page - 1);
        } else {
            throw new RuntimeException('Hub current sync requires selected_hub_type to be structure or npc_station. Received: ' . $selectedHubType . '.');
        }

        $result['checksum'] = sync_checksum($canonicalRows);

        if ($canonicalRows === []) {
            $result['warnings'][] = 'No canonical market hub current rows were mapped from ESI payload.';
            $result['meta'] = [
                'selected_hub_id' => $selectedHubId,
                'selected_hub_name' => $selectedHubName,
                'selected_hub_type' => $selectedHubType,
                'system_id' => isset($hubContext['system_id']) && $hubContext['system_id'] !== null ? (int) $hubContext['system_id'] : null,
                'effective_api_source' => $effectiveApiSource,
                'resolved_region_id' => $resolvedRegionId,
                'resolved_structure_id' => $resolvedStructureId,
                'records_fetched' => (int) $result['rows_seen'],
                'records_inserted' => 0,
                'records_updated' => 0,
                'records_skipped' => (int) $result['rows_seen'],
                'records_deleted' => 0,
                'api_pages_processed' => $pagesProcessed,
                'no_changes' => true,
            ];
            sync_run_finalize_success($runId, $datasetKey, $syncMode, $result['rows_seen'], 0, $result['cursor'], $result['checksum']);

            return $result;
        }

        $result['rows_written'] = db_market_orders_current_bulk_upsert($canonicalRows);
        $result['meta'] = [
            'selected_hub_id' => $selectedHubId,
            'selected_hub_name' => $selectedHubName,
            'selected_hub_type' => $selectedHubType,
            'system_id' => isset($hubContext['system_id']) && $hubContext['system_id'] !== null ? (int) $hubContext['system_id'] : null,
            'effective_api_source' => $effectiveApiSource,
            'resolved_region_id' => $resolvedRegionId,
            'resolved_structure_id' => $resolvedStructureId,
            'records_fetched' => (int) $result['rows_seen'],
            'records_inserted' => 0,
            'records_updated' => (int) $result['rows_written'],
            'records_skipped' => max(0, (int) $result['rows_seen'] - (int) $result['rows_written']),
            'records_deleted' => 0,
            'api_pages_processed' => $pagesProcessed,
            'no_changes' => (int) $result['rows_written'] === 0,
        ];
        sync_run_finalize_success($runId, $datasetKey, $syncMode, $result['rows_seen'], $result['rows_written'], $result['cursor'], $result['checksum']);

        return $result;
    } catch (Throwable $exception) {
        sync_run_finalize_failure($runId, $datasetKey, $syncMode, $exception->getMessage());
        $result['warnings'][] = 'Hub current sync failed: ' . $exception->getMessage();
        $result['meta'] = [
            'selected_hub_id' => (string) ($hubContext['hub_id'] ?? $hubKey),
            'selected_hub_name' => (string) ($hubContext['hub_name'] ?? market_hub_reference_name()),
            'selected_hub_type' => (string) ($hubContext['hub_type'] ?? 'unknown'),
            'system_id' => isset($hubContext['system_id']) && $hubContext['system_id'] !== null ? (int) $hubContext['system_id'] : null,
            'effective_api_source' => (string) ($hubContext['api_source'] ?? 'unknown'),
            'resolved_region_id' => isset($hubContext['region_id']) && $hubContext['region_id'] !== null ? (int) $hubContext['region_id'] : null,
            'resolved_structure_id' => isset($hubContext['structure_id']) && $hubContext['structure_id'] !== null ? (int) $hubContext['structure_id'] : null,
        ];

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

function market_hub_history_fallback_type_ids(int $sourceId, int $limit = 200): array
{
    $safeLimit = max(1, min($limit, 2000));
    $orderTypeIds = db_market_orders_current_distinct_type_ids('market_hub', $sourceId, $safeLimit);
    $historyTypeIds = db_market_history_daily_distinct_type_ids('market_hub', $sourceId, $safeLimit);

    $fallbackTypeIds = array_merge($orderTypeIds, $historyTypeIds);

    $allianceSourceId = configured_structure_destination_id_for_esi_sync();
    if ($allianceSourceId > 0) {
        $fallbackTypeIds = array_merge(
            $fallbackTypeIds,
            db_market_orders_current_distinct_type_ids('alliance_structure', $allianceSourceId, $safeLimit),
            db_market_history_daily_distinct_type_ids('alliance_structure', $allianceSourceId, $safeLimit)
        );
    }

    $typeIds = array_values(array_unique(array_filter($fallbackTypeIds, static fn (int $typeId): bool => $typeId > 0)));
    sort($typeIds);

    return array_slice($typeIds, 0, $safeLimit);
}

function market_hub_history_api_reference(string $hubRef): string
{
    $normalized = trim($hubRef);
    if ($normalized === '') {
        return '';
    }

    $stationId = (int) $normalized;
    if ($stationId <= 0) {
        return $normalized;
    }

    try {
        $regionId = db_ref_npc_station_region_id($stationId);
    } catch (Throwable) {
        $regionId = null;
    }

    return $regionId !== null ? (string) $regionId : $normalized;
}

function sync_market_hub_history(string|int $hubRef, string $runMode = 'incremental'): array
{
    $result = sync_result_shape();
    $syncMode = sync_mode_normalize($runMode);
    $sourceId = sync_source_id_from_hub_ref($hubRef);
    $hubKey = trim((string) $hubRef);
    $historyApiHubRef = market_hub_history_api_reference($hubKey);
    $datasetKey = sync_dataset_key_market_hub_history_daily($hubKey);
    if ($hubKey === '') {
        $result['warnings'][] = 'Market hub reference is required.';

        return $result;
    }

    $runId = db_sync_run_start($datasetKey, $syncMode, sync_watermark($datasetKey));

    try {
        $urlTemplate = trim((string) get_setting('eve_tycoon_history_url_template', 'https://evetycoon.com/api/v1/market/history/{hub}'));
        $endpoint = str_replace('{hub}', rawurlencode($historyApiHubRef), $urlTemplate);

        $response = http_get_json_with_backoff($endpoint, ['Accept: application/json']);
        $status = (int) ($response['status'] ?? 500);
        $providerRows = [];
        $apiPagesProcessed = 1;

        if ($status < 400) {
            $providerRows = eve_tycoon_history_payload_rows($response['json'] ?? []);
            $result['rows_seen'] = count($providerRows);
        } else {
            $fallbackTypeIds = market_hub_history_fallback_type_ids($sourceId);
            if ($fallbackTypeIds === []) {
                $result['warnings'][] = 'Primary history endpoint returned ' . $status . ' and fallback could not run because no local type IDs were found for this hub yet.';
            }

            if ($fallbackTypeIds !== []) {
                $result['warnings'][] = 'Primary history endpoint returned ' . $status . '; falling back to per-type region history calls.';
            }
            $fallbackErrors = [];
            $rowsSeen = 0;

            foreach ($fallbackTypeIds as $typeId) {
                $apiPagesProcessed++;
                $fallbackEndpoint = 'https://evetycoon.com/api/v1/market/history/' . rawurlencode($historyApiHubRef) . '/' . rawurlencode((string) $typeId);
                $fallbackResponse = http_get_json_with_backoff($fallbackEndpoint, ['Accept: application/json']);
                $fallbackStatus = (int) ($fallbackResponse['status'] ?? 500);

                if ($fallbackStatus >= 400) {
                    $fallbackErrors[] = $typeId . ':' . $fallbackStatus;
                    continue;
                }

                $typeRows = eve_tycoon_history_payload_rows($fallbackResponse['json'] ?? []);
                foreach ($typeRows as $typeRow) {
                    if (!is_array($typeRow)) {
                        continue;
                    }

                    $typeRow['type_id'] = (int) ($typeRow['type_id'] ?? $typeRow['typeId'] ?? $typeId);
                    $providerRows[] = $typeRow;
                    $rowsSeen++;
                }
            }

            $result['rows_seen'] = $rowsSeen;
            if ($providerRows === [] && $fallbackTypeIds !== []) {
                $errorSummary = $fallbackErrors === [] ? 'unknown' : implode(', ', array_slice($fallbackErrors, 0, 8));
                throw new RuntimeException('EVE Tycoon history sync failed: fallback per-type history calls returned no rows. Errors: ' . $errorSummary . '.');
            }
        }

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
            $result['meta'] = [
                'reference_hub' => $hubKey,
                'reference_hub_history_api' => $historyApiHubRef,
                'reference_market_hub' => market_hub_reference_name(),
                'records_fetched' => (int) $result['rows_seen'],
                'records_inserted' => 0,
                'records_updated' => 0,
                'records_skipped' => (int) $result['rows_seen'],
                'records_deleted' => 0,
                'api_pages_processed' => $apiPagesProcessed,
                'history_rows_generated' => 0,
                'no_changes' => true,
            ];
            sync_run_finalize_success($runId, $datasetKey, $syncMode, $result['rows_seen'], 0, $result['cursor'], $result['checksum']);

            return $result;
        }

        $result['rows_written'] = db_market_history_daily_bulk_upsert($canonicalRows);
        $latestTradeDate = max(array_column($canonicalRows, 'trade_date'));
        $result['cursor'] = 'trade_date:' . $latestTradeDate;
        $result['checksum'] = sync_checksum($canonicalRows);
        $result['meta'] = [
            'reference_hub' => $hubKey,
            'reference_hub_history_api' => $historyApiHubRef,
                'reference_market_hub' => market_hub_reference_name(),
            'records_fetched' => (int) $result['rows_seen'],
            'records_inserted' => 0,
            'records_updated' => (int) $result['rows_written'],
            'records_skipped' => max(0, (int) $result['rows_seen'] - (int) $result['rows_written']),
            'records_deleted' => 0,
            'api_pages_processed' => $apiPagesProcessed,
            'history_rows_generated' => (int) $result['rows_written'],
            'no_changes' => (int) $result['rows_written'] === 0,
        ];
        sync_run_finalize_success($runId, $datasetKey, $syncMode, $result['rows_seen'], $result['rows_written'], $result['cursor'], $result['checksum']);

        return $result;
    } catch (Throwable $exception) {
        sync_run_finalize_failure($runId, $datasetKey, $syncMode, $exception->getMessage());
        $result['warnings'][] = $exception->getMessage();

        return $result;
    }
}

function sync_market_hub_local_history(string|int $hubRef, string $runMode = 'incremental', ?int $windowDays = null): array
{
    $result = sync_result_shape();
    $syncMode = sync_mode_normalize($runMode);
    $hubKey = trim((string) $hubRef);
    $datasetKey = sync_dataset_key_market_hub_local_history_daily($hubKey);

    if ($hubKey === '') {
        $result['warnings'][] = 'Local hub history sync skipped: configure a market hub/station first.';

        return $result;
    }

    $hubContext = market_hub_reference_context($hubKey);
    $sourceId = sync_source_id_from_hub_ref($hubKey);
    $safeWindowDays = market_hub_local_history_window_days_normalize($windowDays);
    $windowStartObservedAt = market_hub_local_history_window_start_observed_at($safeWindowDays);
    $runId = db_sync_run_start($datasetKey, $syncMode, sync_watermark($datasetKey));

    try {
        if ($sourceId <= 0) {
            $warning = 'Local hub history sync skipped: could not resolve a valid local source id for hub ' . ($hubContext['hub_name'] ?? $hubKey) . '.';
            $result['warnings'][] = $warning;
            $result['cursor'] = 'hub:' . $hubKey . ';state:missing_source_id';
            $result['checksum'] = sync_checksum([
                'hub' => $hubKey,
                'status' => 'missing_source_id',
            ]);
            $result['meta'] = [
                'reference_hub' => $hubKey,
                'reference_market_hub' => (string) ($hubContext['hub_name'] ?? market_hub_reference_name()),
                'source_id' => 0,
                'window_days' => $safeWindowDays,
                'window_start_observed_at' => $windowStartObservedAt,
                'records_fetched' => 0,
                'records_inserted' => 0,
                'records_updated' => 0,
                'records_skipped' => 0,
                'records_deleted' => 0,
                'history_rows_generated' => 0,
                'snapshot_days_seen' => 0,
                'no_changes' => true,
                'outcome_reason' => 'missing_source_id',
            ];
            sync_run_finalize_success($runId, $datasetKey, $syncMode, 0, 0, $result['cursor'], $result['checksum']);

            return $result;
        }

        $snapshotMetrics = db_market_orders_snapshot_metrics_window('market_hub', $sourceId, $windowStartObservedAt);
        $result['rows_seen'] = count($snapshotMetrics);

        if ($snapshotMetrics === []) {
            $warning = 'Local hub history sync found no local raw hub snapshots in the last ' . $safeWindowDays . ' day(s) for ' . ($hubContext['hub_name'] ?? $hubKey) . '. Run the hub current sync first or widen --window-days.';
            $result['warnings'][] = $warning;
            $result['cursor'] = 'source_id:' . $sourceId . ';state:awaiting_local_snapshots';
            $result['checksum'] = sync_checksum([
                'source_id' => $sourceId,
                'window_days' => $safeWindowDays,
                'status' => 'awaiting_local_snapshots',
            ]);
            $result['meta'] = [
                'reference_hub' => $hubKey,
                'reference_market_hub' => (string) ($hubContext['hub_name'] ?? market_hub_reference_name()),
                'source_id' => $sourceId,
                'window_days' => $safeWindowDays,
                'window_start_observed_at' => $windowStartObservedAt,
                'records_fetched' => 0,
                'records_inserted' => 0,
                'records_updated' => 0,
                'records_skipped' => 0,
                'records_deleted' => 0,
                'history_rows_generated' => 0,
                'snapshot_days_seen' => 0,
                'no_changes' => true,
                'outcome_reason' => 'awaiting_local_snapshots',
            ];
            sync_run_finalize_success($runId, $datasetKey, $syncMode, 0, 0, $result['cursor'], $result['checksum']);

            return $result;
        }

        $rebuilt = market_hub_local_history_daily_rebuild_from_snapshot_metrics($snapshotMetrics);
        $canonicalRows = is_array($rebuilt['rows'] ?? null) ? $rebuilt['rows'] : [];
        $historyRowCount = count($canonicalRows);
        $tradeDates = is_array($rebuilt['trade_dates'] ?? null) ? $rebuilt['trade_dates'] : [];
        $latestCapturedAt = $canonicalRows === [] ? '' : (string) ($canonicalRows[array_key_last($canonicalRows)]['captured_at'] ?? '');

        if ($canonicalRows === []) {
            $warning = 'Local hub history sync could not derive any daily rows from the local raw hub snapshots for ' . ($hubContext['hub_name'] ?? $hubKey) . '.';
            $result['warnings'][] = $warning;
            $result['cursor'] = 'source_id:' . $sourceId . ';window_days:' . $safeWindowDays . ';state:no_canonical_rows';
            $result['checksum'] = sync_checksum([
                'source_id' => $sourceId,
                'window_days' => $safeWindowDays,
                'status' => 'no_canonical_rows',
            ]);
            $result['meta'] = [
                'reference_hub' => $hubKey,
                'reference_market_hub' => (string) ($hubContext['hub_name'] ?? market_hub_reference_name()),
                'source_id' => $sourceId,
                'window_days' => $safeWindowDays,
                'window_start_observed_at' => $windowStartObservedAt,
                'records_fetched' => (int) $result['rows_seen'],
                'records_inserted' => 0,
                'records_updated' => 0,
                'records_skipped' => (int) $result['rows_seen'],
                'records_deleted' => 0,
                'history_rows_generated' => 0,
                'snapshot_days_seen' => count($tradeDates),
                'no_changes' => true,
                'outcome_reason' => 'no_canonical_rows',
            ];
            sync_run_finalize_success($runId, $datasetKey, $syncMode, $result['rows_seen'], 0, $result['cursor'], $result['checksum']);

            return $result;
        }

        $result['rows_written'] = db_market_hub_local_history_daily_bulk_upsert($canonicalRows);
        $result['cursor'] = 'source_id:' . $sourceId . ';window_days:' . $safeWindowDays . ';captured_at:' . $latestCapturedAt;
        $result['checksum'] = sync_checksum($canonicalRows);
        $result['meta'] = [
            'reference_hub' => $hubKey,
            'reference_market_hub' => (string) ($hubContext['hub_name'] ?? market_hub_reference_name()),
            'source_id' => $sourceId,
            'window_days' => $safeWindowDays,
            'window_start_observed_at' => $windowStartObservedAt,
            'captured_at' => $latestCapturedAt !== '' ? $latestCapturedAt : null,
            'records_fetched' => (int) $result['rows_seen'],
            'records_inserted' => 0,
            'records_updated' => (int) $result['rows_written'],
            'records_skipped' => max(0, $historyRowCount - (int) $result['rows_written']),
            'records_deleted' => 0,
            'history_rows_generated' => $historyRowCount,
            'snapshot_days_seen' => count($tradeDates),
            'no_changes' => (int) $result['rows_written'] === 0,
        ];

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
        $result['warnings'][] = 'Local hub history sync failed: ' . $exception->getMessage();

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
        $result['meta'] = [
            'records_fetched' => $deletedRows,
            'records_inserted' => 0,
            'records_updated' => 0,
            'records_skipped' => 0,
            'records_deleted' => $deletedRows,
            'history_rows_generated' => 0,
            'retention_days' => $safeRetentionDays,
            'no_changes' => $deletedRows === 0,
        ];

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

function esi_user_agent(): string
{
    $configured = trim((string) get_setting('app_name', 'SupplyCore'));

    return $configured !== ''
        ? $configured . ' supplycore/1.0 (+https://github.com/cvweiss/supplycore)'
        : 'SupplyCore supplycore/1.0 (+https://github.com/cvweiss/supplycore)';
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


function esi_entity_result_shape(array $entity): array
{
    return [
        'id' => (int) ($entity['id'] ?? 0),
        'name' => (string) ($entity['name'] ?? ''),
        'type' => (string) ($entity['type'] ?? ''),
    ];
}

function esi_universe_names_lookup(array $ids): array
{
    $queryIds = array_values(array_unique(array_map(static fn (mixed $id): int => (int) $id, $ids)));
    $queryIds = array_values(array_filter($queryIds, static fn (int $id): bool => $id > 0));
    if ($queryIds === []) {
        return [];
    }

    $ch = curl_init('https://esi.evetech.net/latest/universe/names/?datasource=tranquility');
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT => 25,
        CURLOPT_POST => true,
        CURLOPT_HTTPHEADER => ['Accept: application/json', 'Content-Type: application/json', 'User-Agent: ' . esi_user_agent()],
        CURLOPT_POSTFIELDS => json_encode($queryIds, JSON_THROW_ON_ERROR),
    ]);

    $body = curl_exec($ch);
    $status = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $error = curl_error($ch);
    curl_close($ch);

    if ($body === false) {
        throw new RuntimeException('Failed resolving entity IDs from ESI: ' . $error);
    }

    if ($status !== 200) {
        throw new RuntimeException('Failed resolving entity IDs from ESI. HTTP status=' . $status);
    }

    $decoded = json_decode($body, true);

    return is_array($decoded) ? $decoded : [];
}

function esi_alliance_and_corporation_search(string $query, array $tokenContext): array
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
    $searchResponse = http_get_json(
        'https://esi.evetech.net/latest/characters/' . $characterId . '/search/?categories=alliance,corporation&strict=false&search=' . rawurlencode($term),
        [
            'Authorization: Bearer ' . $accessToken,
            'Accept: application/json',
            'User-Agent: ' . esi_user_agent(),
        ]
    );

    if (($searchResponse['status'] ?? 500) >= 400) {
        throw new RuntimeException('Failed to search alliances and corporations from ESI.');
    }

    $ids = [];
    $entityTypesById = [];
    foreach (['alliance' => 'Alliance', 'corporation' => 'Corporation'] as $category => $label) {
        foreach ((array) ($searchResponse['json'][$category] ?? []) as $rawId) {
            $id = (int) $rawId;
            if ($id <= 0) {
                continue;
            }

            $ids[] = $id;
            $entityTypesById[$id] = $label;
        }
    }

    $results = [];
    foreach (esi_universe_names_lookup($ids) as $row) {
        if (!is_array($row)) {
            continue;
        }

        $id = (int) ($row['id'] ?? 0);
        $name = trim((string) ($row['name'] ?? ''));
        if ($id <= 0 || $name === '' || !isset($entityTypesById[$id])) {
            continue;
        }

        if (mb_stripos($name, $term) === false) {
            continue;
        }

        $results[$id] = esi_entity_result_shape([
            'id' => $id,
            'name' => $name,
            'type' => $entityTypesById[$id],
        ]);
    }

    usort($results, static function (array $a, array $b): int {
        $typeCompare = strcasecmp((string) ($a['type'] ?? ''), (string) ($b['type'] ?? ''));
        if ($typeCompare !== 0) {
            return $typeCompare;
        }

        return strcasecmp((string) ($a['name'] ?? ''), (string) ($b['name'] ?? ''));
    });

    return array_slice(array_values($results), 0, 20);
}



function killmail_entity_type_label(?string $type): ?string
{
    $normalized = strtolower(trim((string) $type));

    return match ($normalized) {
        'alliance' => 'Alliance',
        'corporation' => 'Corporation',
        default => null,
    };
}

function killmail_public_entity_lookup_by_id(int $id, ?string $type = null): array
{
    if ($id <= 0) {
        return [];
    }

    $types = [];
    $normalizedType = killmail_entity_type_label($type);
    if ($normalizedType !== null) {
        $types[] = $normalizedType;
    } else {
        $types = ['Alliance', 'Corporation'];
    }

    $endpoints = [
        'Alliance' => 'https://esi.evetech.net/latest/alliances/' . $id . '/?datasource=tranquility',
        'Corporation' => 'https://esi.evetech.net/latest/corporations/' . $id . '/?datasource=tranquility',
    ];

    $results = [];
    foreach ($types as $label) {
        $response = http_get_json($endpoints[$label], [
            'Accept: application/json',
            'User-Agent: ' . esi_user_agent(),
        ]);

        if (($response['status'] ?? 500) >= 400) {
            continue;
        }

        $name = trim((string) ($response['json']['name'] ?? ''));
        if ($name === '') {
            continue;
        }

        $results[] = esi_entity_result_shape([
            'id' => $id,
            'name' => $name,
            'type' => $label,
        ]);
    }

    return $results;
}

function killmail_entity_search(string $query, ?string $type = null): array
{
    $term = trim($query);
    if ($term === '') {
        return [];
    }

    $allowedType = killmail_entity_type_label($type);
    $results = [];

    if (preg_match('/^[1-9][0-9]{0,19}$/', $term) === 1) {
        foreach (killmail_public_entity_lookup_by_id((int) $term, $allowedType) as $row) {
            $results[(string) ($row['type'] ?? '') . ':' . (string) ($row['id'] ?? 0)] = $row;
        }
    }

    $context = esi_lookup_context();
    if (($context['ok'] ?? false) === true) {
        try {
            foreach (esi_alliance_and_corporation_search($term, (array) ($context['token'] ?? [])) as $row) {
                $rowType = killmail_entity_type_label((string) ($row['type'] ?? ''));
                if ($rowType === null) {
                    continue;
                }

                if ($allowedType !== null && $rowType !== $allowedType) {
                    continue;
                }

                $results[$rowType . ':' . (string) ($row['id'] ?? 0)] = [
                    'id' => (int) ($row['id'] ?? 0),
                    'name' => (string) ($row['name'] ?? ''),
                    'type' => $rowType,
                ];
            }
        } catch (Throwable) {
            // Fallback to exact-ID results only.
        }
    }

    $rows = array_values(array_filter($results, static function (array $row): bool {
        return (int) ($row['id'] ?? 0) > 0 && trim((string) ($row['name'] ?? '')) !== '';
    }));

    usort($rows, static function (array $a, array $b): int {
        $typeCompare = strcasecmp((string) ($a['type'] ?? ''), (string) ($b['type'] ?? ''));
        if ($typeCompare !== 0) {
            return $typeCompare;
        }

        return strcasecmp((string) ($a['name'] ?? ''), (string) ($b['name'] ?? ''));
    });

    return array_slice($rows, 0, 20);
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
            'type' => 'Structure',
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

function esi_npc_station_search(string $query, array $tokenContext): array
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

    $searchResponse = http_get_json(
        'https://esi.evetech.net/latest/characters/' . $characterId . '/search/?categories=station&strict=false&search=' . rawurlencode($term),
        [
            'Authorization: Bearer ' . $accessToken,
            'Accept: application/json',
        ]
    );

    if (($searchResponse['status'] ?? 500) >= 400) {
        throw new RuntimeException('Failed to search NPC stations from ESI.');
    }

    $stationIds = $searchResponse['json']['station'] ?? [];
    if (!is_array($stationIds)) {
        return [];
    }

    $results = [];
    foreach (array_slice($stationIds, 0, 20) as $stationId) {
        $id = (int) $stationId;
        if ($id <= 0) {
            continue;
        }

        try {
            $stationResponse = http_get_json(
                'https://esi.evetech.net/latest/universe/stations/' . $id . '/',
                ['Accept: application/json']
            );
        } catch (Throwable) {
            continue;
        }

        if (($stationResponse['status'] ?? 500) >= 400) {
            continue;
        }

        $name = trim((string) ($stationResponse['json']['name'] ?? ''));
        if ($name === '' || mb_stripos($name, $term) === false) {
            continue;
        }

        $results[] = esi_structure_result_shape([
            'id' => $id,
            'name' => $name,
            'system' => isset($stationResponse['json']['system_id']) ? (string) $stationResponse['json']['system_id'] : null,
            'type' => 'NPC Station',
        ]);
    }

    return $results;
}

function esi_npc_station_metadata(int $stationId): ?array
{
    if ($stationId <= 0) {
        return null;
    }

    $response = http_get_json(
        'https://esi.evetech.net/latest/universe/stations/' . $stationId . '/',
        ['Accept: application/json']
    );

    if (($response['status'] ?? 500) >= 400) {
        return null;
    }

    $name = trim((string) ($response['json']['name'] ?? ''));
    if ($name === '') {
        return null;
    }

    return [
        'id' => $stationId,
        'name' => $name,
        'system' => isset($response['json']['system_id']) ? (string) $response['json']['system_id'] : null,
        'type' => 'NPC Station',
    ];
}



function runner_lock_acquire(string $lockName, int $timeoutSeconds = 0): bool
{
    return db_runner_lock_acquire($lockName, $timeoutSeconds);
}

function runner_lock_release(string $lockName): bool
{
    return db_runner_lock_release($lockName);
}

function static_data_source_key(): string
{
    return 'official.jsonl';
}

function static_data_source_url(): string
{
    $default = static_data_default_source_url();
    $configured = trim((string) get_setting('static_data_source_url', $default));

    return $configured !== '' ? $configured : $default;
}

function static_data_import_mode(string $requestedMode = 'auto'): string
{
    $normalized = trim(mb_strtolower($requestedMode));
    $incrementalEnabled = get_setting('incremental_updates_enabled', '1') === '1';

    if ($normalized === 'full') {
        return 'full';
    }

    if ($normalized === 'incremental') {
        return $incrementalEnabled ? 'incremental' : 'full';
    }

    return $incrementalEnabled ? 'incremental' : 'full';
}

function static_data_fetch_remote_build_info(string $sourceUrl): array
{
    $headers = @get_headers($sourceUrl, true);
    if ($headers === false) {
        throw new RuntimeException('Unable to fetch static-data headers from source URL.');
    }

    $lastModified = '';
    $etag = '';

    if (is_array($headers['Last-Modified'] ?? null)) {
        $lastModified = (string) end($headers['Last-Modified']);
    } elseif (isset($headers['Last-Modified'])) {
        $lastModified = (string) $headers['Last-Modified'];
    }

    if (is_array($headers['ETag'] ?? null)) {
        $etag = (string) end($headers['ETag']);
    } elseif (isset($headers['ETag'])) {
        $etag = (string) $headers['ETag'];
    }

    $buildId = sha1($sourceUrl . '|' . trim($etag, '"') . '|' . $lastModified);

    return [
        'build_id' => $buildId,
        'etag' => trim((string) $etag, '"'),
        'last_modified' => $lastModified,
    ];
}

function static_data_storage_paths(string $buildId): array
{
    $baseDir = dirname(__DIR__) . '/storage/static-data';

    return [
        'base_dir' => $baseDir,
        'archive_path' => $baseDir . '/' . $buildId . '.jsonl.zip',
    ];
}

function static_data_ensure_local_jsonl_archive(string $sourceUrl, string $buildId): string
{
    $paths = static_data_storage_paths($buildId);
    if (!is_dir($paths['base_dir']) && !mkdir($paths['base_dir'], 0775, true) && !is_dir($paths['base_dir'])) {
        throw new RuntimeException('Unable to create static-data storage directory.');
    }

    $normalizedSource = mb_strtolower($sourceUrl);
    if (!str_ends_with($normalizedSource, '.zip')) {
        throw new RuntimeException('Unsupported static-data format. This importer supports JSONL ZIP archives (.zip).');
    }

    if (!is_file($paths['archive_path'])) {
        $context = stream_context_create(['http' => ['timeout' => 120]]);
        $payload = @file_get_contents($sourceUrl, false, $context);
        if ($payload === false) {
            throw new RuntimeException('Failed to download static-data package.');
        }

        if (file_put_contents($paths['archive_path'], $payload) === false) {
            throw new RuntimeException('Failed to write static-data archive to local storage.');
        }
    }

    return $paths['archive_path'];
}

function static_data_record_value(array $row, array $keys, mixed $default = null): mixed
{
    foreach ($keys as $key) {
        if (array_key_exists($key, $row)) {
            return $row[$key];
        }
    }

    return $default;
}

function static_data_localized_text(mixed $value): ?string
{
    if (is_string($value)) {
        $text = trim($value);

        return $text === '' ? null : $text;
    }

    if (!is_array($value)) {
        return null;
    }

    foreach (['en', 'en-us', 'EN'] as $key) {
        if (isset($value[$key]) && is_string($value[$key]) && trim($value[$key]) !== '') {
            return trim($value[$key]);
        }
    }

    foreach ($value as $candidate) {
        if (is_string($candidate) && trim($candidate) !== '') {
            return trim($candidate);
        }
    }

    return null;
}

function static_data_parse_jsonl_entry(array $entry): ?array
{
    if (!isset($entry['name']) || !isset($entry['index'])) {
        return null;
    }

    $fileName = basename((string) $entry['name']);
    if (!str_ends_with(mb_strtolower($fileName), '.jsonl')) {
        return null;
    }

    return [
        'file_name' => $fileName,
        'index' => (int) $entry['index'],
    ];
}

function static_data_extract_reference_rows_from_jsonl_archive(string $archivePath): array
{
    if (!class_exists(ZipArchive::class)) {
        throw new RuntimeException('PHP ZipArchive extension is required to import JSONL static data archives.');
    }

    $zip = new ZipArchive();
    if ($zip->open($archivePath) !== true) {
        throw new RuntimeException('Unable to open static-data ZIP archive.');
    }

    $entries = [];
    for ($index = 0; $index < $zip->numFiles; $index++) {
        $stat = $zip->statIndex($index);
        if (!is_array($stat)) {
            continue;
        }

        $parsed = static_data_parse_jsonl_entry(['name' => $stat['name'] ?? null, 'index' => $index]);
        if ($parsed === null) {
            continue;
        }

        $entries[mb_strtolower($parsed['file_name'])] = $parsed['index'];
    }

    $targets = [
        'mapregions.jsonl' => 'regions',
        'mapconstellations.jsonl' => 'constellations',
        'mapsolarsystems.jsonl' => 'systems',
        'npcstations.jsonl' => 'stations',
        'stations.jsonl' => 'stations',
        'marketgroups.jsonl' => 'market_groups',
        'types.jsonl' => 'types',
    ];

    $records = [
        'regions' => [],
        'constellations' => [],
        'systems' => [],
        'stations' => [],
        'market_groups' => [],
        'types' => [],
    ];

    foreach ($targets as $fileName => $targetKey) {
        if (!isset($entries[$fileName])) {
            continue;
        }

        $stream = $zip->getStream($zip->getNameIndex($entries[$fileName]));
        if (!is_resource($stream)) {
            continue;
        }

        while (($line = fgets($stream)) !== false) {
            $line = trim($line);
            if ($line === '') {
                continue;
            }

            try {
                $row = json_decode($line, true, 512, JSON_THROW_ON_ERROR);
            } catch (Throwable) {
                continue;
            }

            if (!is_array($row)) {
                continue;
            }

            if ($targetKey === 'regions') {
                $regionId = (int) static_data_record_value($row, ['regionID', 'region_id', '_key']);
                $regionName = static_data_localized_text(static_data_record_value($row, ['regionName', 'name']));
                if ($regionId > 0 && $regionName !== null) {
                    $records['regions'][] = ['region_id' => $regionId, 'region_name' => $regionName];
                }
                continue;
            }

            if ($targetKey === 'constellations') {
                $constellationId = (int) static_data_record_value($row, ['constellationID', 'constellation_id', '_key']);
                $regionId = (int) static_data_record_value($row, ['regionID', 'region_id', '_key']);
                $name = static_data_localized_text(static_data_record_value($row, ['constellationName', 'name']));
                if ($constellationId > 0 && $regionId > 0 && $name !== null) {
                    $records['constellations'][] = [
                        'constellation_id' => $constellationId,
                        'region_id' => $regionId,
                        'constellation_name' => $name,
                    ];
                }
                continue;
            }

            if ($targetKey === 'systems') {
                $systemId = (int) static_data_record_value($row, ['solarSystemID', 'system_id', '_key']);
                $constellationId = (int) static_data_record_value($row, ['constellationID', 'constellation_id', '_key']);
                $regionId = (int) static_data_record_value($row, ['regionID', 'region_id', '_key']);
                $name = static_data_localized_text(static_data_record_value($row, ['solarSystemName', 'name']));
                if ($systemId > 0 && $constellationId > 0 && $regionId > 0 && $name !== null) {
                    $records['systems'][] = [
                        'system_id' => $systemId,
                        'constellation_id' => $constellationId,
                        'region_id' => $regionId,
                        'system_name' => $name,
                        'security' => (float) static_data_record_value($row, ['securityStatus', 'security', 'security_status'], 0),
                    ];
                }
                continue;
            }

            if ($targetKey === 'stations') {
                $stationId = (int) static_data_record_value($row, ['stationID', 'station_id', '_key']);
                $name = static_data_localized_text(static_data_record_value($row, ['stationName', 'name']));
                if ($stationId > 0) {
                    if ($name === null) {
                        $name = 'Station ' . $stationId;
                    }
                    $records['stations'][] = [
                        'station_id' => $stationId,
                        'station_name' => $name,
                        'system_id' => (int) static_data_record_value($row, ['solarSystemID', 'system_id', '_key']),
                        'constellation_id' => (int) static_data_record_value($row, ['constellationID', 'constellation_id', '_key']),
                        'region_id' => (int) static_data_record_value($row, ['regionID', 'region_id', '_key']),
                        'station_type_id' => ($typeId = (int) static_data_record_value($row, ['stationTypeID', 'typeID', 'type_id'], 0)) > 0 ? $typeId : null,
                    ];
                }
                continue;
            }

            if ($targetKey === 'market_groups') {
                $groupId = (int) static_data_record_value($row, ['marketGroupID', 'market_group_id', '_key']);
                $name = static_data_localized_text(static_data_record_value($row, ['marketGroupName', 'name']));
                if ($groupId > 0 && $name !== null) {
                    $parentId = (int) static_data_record_value($row, ['parentGroupID', 'parent_group_id'], 0);
                    $records['market_groups'][] = [
                        'market_group_id' => $groupId,
                        'parent_group_id' => $parentId > 0 ? $parentId : null,
                        'market_group_name' => $name,
                        'description' => static_data_localized_text(static_data_record_value($row, ['description'])),
                    ];
                }
                continue;
            }

            if ($targetKey === 'types') {
                $typeId = (int) static_data_record_value($row, ['typeID', 'type_id', '_key']);
                $groupId = (int) static_data_record_value($row, ['groupID', 'group_id']);
                $name = static_data_localized_text(static_data_record_value($row, ['typeName', 'name']));
                if ($typeId > 0 && $groupId > 0 && $name !== null) {
                    $marketGroupId = (int) static_data_record_value($row, ['marketGroupID', 'market_group_id', '_key'], 0);
                    $publishedRaw = static_data_record_value($row, ['published'], 0);
                    $records['types'][] = [
                        'type_id' => $typeId,
                        'group_id' => $groupId,
                        'market_group_id' => $marketGroupId > 0 ? $marketGroupId : null,
                        'type_name' => $name,
                        'description' => static_data_localized_text(static_data_record_value($row, ['description'])),
                        'published' => ($publishedRaw === true || (int) $publishedRaw === 1) ? 1 : 0,
                        'volume' => ($volume = static_data_record_value($row, ['volume'], null)) !== null ? (float) $volume : null,
                    ];
                }
            }
        }

        fclose($stream);
    }

    $zip->close();

    if ($records['regions'] === [] || $records['constellations'] === [] || $records['systems'] === [] || $records['market_groups'] === [] || $records['types'] === []) {
        throw new RuntimeException('Static-data archive is missing one or more required JSONL datasets for import.');
    }

    return $records;
}

function static_data_import_reference_data(string $requestedMode = 'auto', bool $force = false): array
{
    $sourceKey = static_data_source_key();
    $sourceUrl = static_data_source_url();
    $state = db_static_data_import_state_get($sourceKey);
    $mode = static_data_import_mode($requestedMode);
    $checkedAt = gmdate('Y-m-d H:i:s');

    $remote = static_data_fetch_remote_build_info($sourceUrl);
    $remoteBuildId = (string) $remote['build_id'];
    $importedBuildId = (string) ($state['imported_build_id'] ?? '');

    if (!$force && $remoteBuildId === $importedBuildId) {
        db_static_data_import_state_upsert(
            $sourceKey,
            $sourceUrl,
            $remoteBuildId,
            $importedBuildId === '' ? null : $importedBuildId,
            $state['imported_mode'] ?? null,
            'success',
            $checkedAt,
            $state['last_import_started_at'] ?? null,
            $state['last_import_finished_at'] ?? null,
            null,
            json_encode(['remote' => $remote, 'note' => 'Already up to date.'], JSON_THROW_ON_ERROR)
        );

        return ['ok' => true, 'changed' => false, 'build_id' => $remoteBuildId, 'rows_written' => 0, 'mode' => $mode];
    }

    $startedAt = gmdate('Y-m-d H:i:s');
    db_static_data_import_state_upsert(
        $sourceKey,
        $sourceUrl,
        $remoteBuildId,
        $importedBuildId === '' ? null : $importedBuildId,
        $mode,
        'running',
        $checkedAt,
        $startedAt,
        null,
        null,
        json_encode(['remote' => $remote], JSON_THROW_ON_ERROR)
    );

    try {
        $archivePath = static_data_ensure_local_jsonl_archive($sourceUrl, $remoteBuildId);
        $dataset = static_data_extract_reference_rows_from_jsonl_archive($archivePath);

        $regions = $dataset['regions'];
        $constellations = $dataset['constellations'];
        $systems = $dataset['systems'];
        $stations = $dataset['stations'];
        $marketGroups = $dataset['market_groups'];
        $types = $dataset['types'];

        $rowsWritten = db_transaction(static function () use ($mode, $regions, $constellations, $systems, $stations, $marketGroups, $types): int {
            if ($mode === 'full') {
                db_reference_data_truncate_all();
            }

            $written = 0;
            $written += db_ref_regions_bulk_upsert($regions);
            $written += db_ref_constellations_bulk_upsert($constellations);
            $written += db_ref_systems_bulk_upsert($systems);
            $written += db_ref_npc_stations_bulk_upsert($stations);
            $written += db_ref_market_groups_bulk_upsert($marketGroups);
            $written += db_ref_item_types_bulk_upsert($types);

            return $written;
        });

        $finishedAt = gmdate('Y-m-d H:i:s');
        db_static_data_import_state_upsert(
            $sourceKey,
            $sourceUrl,
            $remoteBuildId,
            $remoteBuildId,
            $mode,
            'success',
            $checkedAt,
            $startedAt,
            $finishedAt,
            null,
            json_encode([
                'remote' => $remote,
                'dataset_counts' => [
                    'regions' => count($regions),
                    'constellations' => count($constellations),
                    'systems' => count($systems),
                    'npc_stations' => count($stations),
                    'market_groups' => count($marketGroups),
                    'item_types' => count($types),
                ],
            ], JSON_THROW_ON_ERROR)
        );

        return ['ok' => true, 'changed' => true, 'build_id' => $remoteBuildId, 'rows_written' => $rowsWritten, 'mode' => $mode];
    } catch (Throwable $exception) {
        db_static_data_import_state_upsert(
            $sourceKey,
            $sourceUrl,
            $remoteBuildId,
            $importedBuildId === '' ? null : $importedBuildId,
            $mode,
            'failed',
            $checkedAt,
            $startedAt,
            gmdate('Y-m-d H:i:s'),
            $exception->getMessage(),
            json_encode(['remote' => $remote], JSON_THROW_ON_ERROR)
        );

        throw $exception;
    }
}

function killmail_ingestion_enabled(): bool
{
    return sanitize_enabled_flag(get_setting('killmail_ingestion_enabled', '0')) === '1';
}

function killmail_sync_dataset_key(): string
{
    return 'killmail.r2z2.stream';
}

function sync_dataset_key_killmail_r2z2_stream(): string
{
    return killmail_sync_dataset_key();
}

function killmail_r2z2_sequence_url(): string
{
    $url = trim((string) get_setting('killmail_r2z2_sequence_url', 'https://r2z2.zkillboard.com/ephemeral/sequence.json'));

    return $url !== '' ? $url : 'https://r2z2.zkillboard.com/ephemeral/sequence.json';
}

function killmail_r2z2_base_url(): string
{
    $url = rtrim(trim((string) get_setting('killmail_r2z2_base_url', 'https://r2z2.zkillboard.com/ephemeral')), '/');

    return $url !== '' ? $url : 'https://r2z2.zkillboard.com/ephemeral';
}

function killmail_poll_sleep_seconds(): int
{
    return max(6, min(300, (int) get_setting('killmail_ingestion_poll_sleep_seconds', '6')));
}

function killmail_max_sequences_per_run(): int
{
    return max(1, min(5000, (int) get_setting('killmail_ingestion_max_sequences_per_run', '120')));
}

function killmail_parse_entity_lines(string $text): array
{
    $rows = [];
    foreach (preg_split('/\R+/', $text) as $line) {
        $line = trim((string) $line);
        if ($line === '') {
            continue;
        }

        if (preg_match('/^([0-9]{1,20})(?:\s*[|,:-]\s*(.+))?$/', $line, $m) !== 1) {
            continue;
        }

        $rows[] = [
            'id' => (int) $m[1],
            'label' => isset($m[2]) ? trim($m[2]) : null,
        ];
    }

    return $rows;
}

function killmail_parse_entity_name_lines(string $text): array
{
    $names = [];

    foreach (preg_split('/\R+/', $text) as $line) {
        $name = trim((string) $line);
        if ($name === '') {
            continue;
        }

        $names[(string) $name] = (string) $name;
    }

    return array_values($names);
}

function killmail_universe_ids_lookup(array $names): array
{
    $queryNames = array_values(array_filter(array_map(static fn (mixed $name): string => trim((string) $name), $names), static fn (string $name): bool => $name !== ''));
    if ($queryNames === []) {
        return [];
    }

    $ch = curl_init('https://esi.evetech.net/latest/universe/ids/?datasource=tranquility&language=en');
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT => 25,
        CURLOPT_POST => true,
        CURLOPT_HTTPHEADER => ['Accept: application/json', 'Content-Type: application/json', 'User-Agent: ' . esi_user_agent()],
        CURLOPT_POSTFIELDS => json_encode($queryNames, JSON_UNESCAPED_SLASHES),
    ]);

    $body = curl_exec($ch);
    $status = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $error = curl_error($ch);
    curl_close($ch);

    if ($body === false) {
        throw new RuntimeException('Failed resolving entity names from ESI: ' . $error);
    }

    if ($status !== 200) {
        throw new RuntimeException('Failed resolving entity names from ESI. HTTP status=' . $status);
    }

    $decoded = json_decode($body, true);

    return is_array($decoded) ? $decoded : [];
}

function killmail_resolve_tracked_entities(string $allianceText, string $corporationText): array
{
    $allianceLines = killmail_parse_entity_name_lines($allianceText);
    $corporationLines = killmail_parse_entity_name_lines($corporationText);
    $parsedAllianceRows = killmail_parse_entity_lines($allianceText);
    $parsedCorporationRows = killmail_parse_entity_lines($corporationText);

    $allianceById = [];
    $corporationById = [];
    $lookupNames = [];

    foreach ($parsedAllianceRows as $row) {
        $id = (int) ($row['id'] ?? 0);
        if ($id <= 0) {
            continue;
        }

        $allianceById[$id] = [
            'id' => $id,
            'label' => isset($row['label']) && trim((string) $row['label']) !== '' ? trim((string) $row['label']) : null,
        ];
    }

    foreach ($parsedCorporationRows as $row) {
        $id = (int) ($row['id'] ?? 0);
        if ($id <= 0) {
            continue;
        }

        $corporationById[$id] = [
            'id' => $id,
            'label' => isset($row['label']) && trim((string) $row['label']) !== '' ? trim((string) $row['label']) : null,
        ];
    }

    foreach ($allianceById as $id => $row) {
        if (($row['label'] ?? null) !== null) {
            continue;
        }

        foreach (killmail_public_entity_lookup_by_id($id, 'alliance') as $resolved) {
            $name = trim((string) ($resolved['name'] ?? ''));
            if ($name === '') {
                continue;
            }

            $allianceById[$id]['label'] = $name;
            break;
        }
    }

    foreach ($corporationById as $id => $row) {
        if (($row['label'] ?? null) !== null) {
            continue;
        }

        foreach (killmail_public_entity_lookup_by_id($id, 'corporation') as $resolved) {
            $name = trim((string) ($resolved['name'] ?? ''));
            if ($name === '') {
                continue;
            }

            $corporationById[$id]['label'] = $name;
            break;
        }
    }

    foreach ($allianceLines as $line) {
        if (preg_match('/^[1-9][0-9]{1,20}(?:\s*[|,:-]\s*.+)?$/', $line) === 1) {
            continue;
        }

        $lookupNames[$line] = true;
    }

    foreach ($corporationLines as $line) {
        if (preg_match('/^[1-9][0-9]{1,20}(?:\s*[|,:-]\s*.+)?$/', $line) === 1) {
            continue;
        }

        $lookupNames[$line] = true;
    }

    $unresolved = [];

    if ($lookupNames !== []) {
        try {
            $resolved = killmail_universe_ids_lookup(array_keys($lookupNames));

            foreach ((array) ($resolved['alliances'] ?? []) as $row) {
                $name = trim((string) ($row['name'] ?? ''));
                $id = (int) ($row['id'] ?? 0);
                if ($id <= 0 || $name === '' || !in_array($name, $allianceLines, true)) {
                    continue;
                }

                $allianceById[$id] = ['id' => $id, 'label' => $name];
            }

            foreach ((array) ($resolved['corporations'] ?? []) as $row) {
                $name = trim((string) ($row['name'] ?? ''));
                $id = (int) ($row['id'] ?? 0);
                if ($id <= 0 || $name === '' || !in_array($name, $corporationLines, true)) {
                    continue;
                }

                $corporationById[$id] = ['id' => $id, 'label' => $name];
            }
        } catch (Throwable $exception) {
            $unresolved[] = 'Unable to resolve names from ESI right now; use the lookup field or numeric IDs. ' . $exception->getMessage();
        }
    }

    foreach ($allianceLines as $line) {
        if (preg_match('/^[1-9][0-9]{1,20}(?:\s*[|,:-]\s*.+)?$/', $line) === 1) {
            continue;
        }

        $found = false;
        foreach ($allianceById as $row) {
            if (($row['label'] ?? '') === $line) {
                $found = true;
                break;
            }
        }

        if (!$found) {
            $unresolved[] = 'Alliance not found: ' . $line;
        }
    }

    foreach ($corporationLines as $line) {
        if (preg_match('/^[1-9][0-9]{1,20}(?:\s*[|,:-]\s*.+)?$/', $line) === 1) {
            continue;
        }

        $found = false;
        foreach ($corporationById as $row) {
            if (($row['label'] ?? '') === $line) {
                $found = true;
                break;
            }
        }

        if (!$found) {
            $unresolved[] = 'Corporation not found: ' . $line;
        }
    }

    ksort($allianceById);
    ksort($corporationById);

    return [
        'alliances' => array_values($allianceById),
        'corporations' => array_values($corporationById),
        'unresolved' => $unresolved,
    ];
}

function killmail_r2z2_fetch_json(string $url): array
{
    $userAgent = trim((string) get_setting('app_name', 'SupplyCore'));
    if ($userAgent === '') {
        $userAgent = 'SupplyCore';
    }

    $headers = [
        'Accept: application/json',
        'User-Agent: ' . $userAgent . ' killmail-ingestion/1.0 (+https://github.com/cvweiss/supplycore)',
    ];

    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT => 25,
        CURLOPT_HTTPHEADER => $headers,
    ]);
    $body = curl_exec($ch);
    $status = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $error = curl_error($ch);
    curl_close($ch);

    if ($body === false) {
        throw new RuntimeException('R2Z2 request failed: ' . $error);
    }

    $json = json_decode($body, true);
    if (!is_array($json)) {
        $json = [];
    }

    return ['status' => $status, 'json' => $json, 'body' => $body];
}

function killmail_extract_items(array $items, int &$index, string $role): array
{
    $rows = [];
    foreach ($items as $item) {
        if (!is_array($item)) {
            continue;
        }

        $rows[] = [
            'item_index' => $index++,
            'item_type_id' => (int) ($item['item_type_id'] ?? 0),
            'item_flag' => isset($item['flag']) ? (int) $item['flag'] : null,
            'quantity_dropped' => isset($item['quantity_dropped']) ? (int) $item['quantity_dropped'] : null,
            'quantity_destroyed' => isset($item['quantity_destroyed']) ? (int) $item['quantity_destroyed'] : null,
            'singleton' => isset($item['singleton']) ? (int) $item['singleton'] : null,
            'item_role' => $role,
        ];

        if (isset($item['items']) && is_array($item['items'])) {
            foreach (killmail_extract_items($item['items'], $index, $role) as $nested) {
                $rows[] = $nested;
            }
        }
    }

    return $rows;
}

function killmail_region_id_from_system(?int $systemId): ?int
{
    if ($systemId === null || $systemId <= 0) {
        return null;
    }

    try {
        $row = db_select_one('SELECT region_id FROM ref_systems WHERE system_id = ? LIMIT 1', [$systemId]);
    } catch (Throwable) {
        return null;
    }

    return isset($row['region_id']) ? (int) $row['region_id'] : null;
}


function killmail_event_matches_tracked_entities(array $event, array $attackers, array $trackedAllianceIds, array $trackedCorporationIds): bool
{
    if ($trackedAllianceIds === [] && $trackedCorporationIds === []) {
        return true;
    }

    $victimAllianceId = (int) ($event['victim_alliance_id'] ?? 0);
    if ($victimAllianceId > 0 && isset($trackedAllianceIds[$victimAllianceId])) {
        return true;
    }

    $victimCorporationId = (int) ($event['victim_corporation_id'] ?? 0);

    return $victimCorporationId > 0 && isset($trackedCorporationIds[$victimCorporationId]);
}

function killmail_transform_r2z2_payload(array $payload): array
{
    $killmail = is_array($payload['esi'] ?? null)
        ? $payload['esi']
        : (is_array($payload['killmail'] ?? null) ? $payload['killmail'] : []);
    $victim = is_array($killmail['victim'] ?? null) ? $killmail['victim'] : [];
    $zkb = is_array($payload['zkb'] ?? null) ? $payload['zkb'] : [];
    $sequenceId = (int) ($payload['sequence_id'] ?? 0);
    $systemId = isset($killmail['solar_system_id']) ? (int) $killmail['solar_system_id'] : null;

    $event = [
        'sequence_id' => $sequenceId,
        'killmail_id' => (int) ($payload['killmail_id'] ?? 0),
        'killmail_hash' => (string) ($payload['hash'] ?? ''),
        'uploaded_at' => isset($payload['uploaded_at']) ? gmdate('Y-m-d H:i:s', (int) $payload['uploaded_at']) : null,
        'sequence_updated' => isset($payload['sequence_updated']) ? (int) $payload['sequence_updated'] : null,
        'killmail_time' => isset($killmail['killmail_time']) ? gmdate('Y-m-d H:i:s', strtotime((string) $killmail['killmail_time'])) : null,
        'solar_system_id' => $systemId,
        'region_id' => killmail_region_id_from_system($systemId),
        'victim_character_id' => isset($victim['character_id']) ? (int) $victim['character_id'] : null,
        'victim_corporation_id' => isset($victim['corporation_id']) ? (int) $victim['corporation_id'] : null,
        'victim_alliance_id' => isset($victim['alliance_id']) ? (int) $victim['alliance_id'] : null,
        'victim_ship_type_id' => isset($victim['ship_type_id']) ? (int) $victim['ship_type_id'] : null,
        'zkb_json' => json_encode($zkb, JSON_UNESCAPED_SLASHES),
        'raw_killmail_json' => json_encode($killmail, JSON_UNESCAPED_SLASHES),
    ];

    $attackers = [];
    foreach ((array) ($killmail['attackers'] ?? []) as $i => $attacker) {
        if (!is_array($attacker)) {
            continue;
        }

        $attackers[] = [
            'attacker_index' => $i,
            'character_id' => isset($attacker['character_id']) ? (int) $attacker['character_id'] : null,
            'corporation_id' => isset($attacker['corporation_id']) ? (int) $attacker['corporation_id'] : null,
            'alliance_id' => isset($attacker['alliance_id']) ? (int) $attacker['alliance_id'] : null,
            'ship_type_id' => isset($attacker['ship_type_id']) ? (int) $attacker['ship_type_id'] : null,
            'weapon_type_id' => isset($attacker['weapon_type_id']) ? (int) $attacker['weapon_type_id'] : null,
            'final_blow' => (bool) ($attacker['final_blow'] ?? false),
            'security_status' => isset($attacker['security_status']) ? (float) $attacker['security_status'] : null,
        ];
    }

    $itemIndex = 0;
    $items = killmail_extract_items((array) ($victim['items'] ?? []), $itemIndex, 'other');
    foreach ($items as &$item) {
        $hasDropped = (int) ($item['quantity_dropped'] ?? 0) > 0;
        $hasDestroyed = (int) ($item['quantity_destroyed'] ?? 0) > 0;
        $item['item_role'] = $hasDropped ? 'dropped' : ($hasDestroyed ? 'destroyed' : 'fitted');
    }

    return ['event' => $event, 'attackers' => $attackers, 'items' => $items];
}

function sync_killmail_r2z2_stream(string $runMode = 'incremental'): array
{
    $runMode = sync_mode_normalize($runMode);
    $datasetKey = killmail_sync_dataset_key();
    $runId = db_sync_run_start($datasetKey, $runMode, db_sync_cursor_get($datasetKey));

    try {
        $trackedAllianceRows = db_killmail_tracked_alliances_active();
        $trackedCorporationRows = db_killmail_tracked_corporations_active();
        $trackedAllianceIds = [];
        foreach ($trackedAllianceRows as $row) {
            $id = (int) ($row['alliance_id'] ?? 0);
            if ($id > 0) {
                $trackedAllianceIds[$id] = true;
            }
        }

        $trackedCorporationIds = [];
        foreach ($trackedCorporationRows as $row) {
            $id = (int) ($row['corporation_id'] ?? 0);
            if ($id > 0) {
                $trackedCorporationIds[$id] = true;
            }
        }

        $sequenceProbe = killmail_r2z2_fetch_json(killmail_r2z2_sequence_url());
        $sequenceProbeStatus = (int) ($sequenceProbe['status'] ?? 0);
        $cursor = db_sync_cursor_get($datasetKey);
        $lastSavedSequence = $cursor !== null && preg_match('/^[0-9]+$/', $cursor) === 1 ? (int) $cursor : null;
        $pollBackoffSeconds = killmail_poll_sleep_seconds();

        if ($sequenceProbeStatus === 403 || $sequenceProbeStatus === 429) {
            $cursorEnd = $cursor !== null ? $cursor : '0';
            $checksum = sync_checksum([0, 0, $cursorEnd]);
            $warnings = ['R2Z2 sequence probe returned status ' . $sequenceProbeStatus . '; worker hit rate limiting and is backing off for ' . $pollBackoffSeconds . 's.'];
            sleep($pollBackoffSeconds);
            $meta = [
                'last_saved_sequence_before_run' => $lastSavedSequence,
                'latest_remote_sequence' => null,
                'first_sequence_attempted' => null,
                'last_sequence_attempted' => null,
                'sequence_files_fetched' => 0,
                'sequence_404s_encountered' => 0,
                'killmails_fetched' => 0,
                'killmails_skipped_duplicates' => 0,
                'killmails_filtered_tracked_rules' => 0,
                'killmails_inserted' => 0,
                'outcome_reason' => 'sequence probe rate limited or forbidden',
                'last_processed_sequence' => null,
            ];
            sync_run_finalize_success($runId, $datasetKey, $runMode, 0, 0, $cursorEnd, $checksum, $warnings);

            return sync_result_shape() + [
                'rows_seen' => 0,
                'rows_written' => 0,
                'cursor' => $cursorEnd,
                'checksum' => $checksum,
                'warnings' => $warnings,
                'meta' => $meta,
            ];
        }

        if ($sequenceProbeStatus !== 200) {
            throw new RuntimeException('Unable to read killmail sequence.json, status=' . $sequenceProbeStatus);
        }

        $latestSequence = (int) (($sequenceProbe['json']['sequence'] ?? 0));
        $nextSequence = $lastSavedSequence !== null ? ($lastSavedSequence + 1) : $latestSequence;
        $maxSteps = killmail_max_sequences_per_run();

        $rowsSeen = 0;
        $rowsWritten = 0;
        $sequenceFilesFetched = 0;
        $sequence404s = 0;
        $duplicateCount = 0;
        $filteredCount = 0;
        $killmailsFetched = 0;
        $lastProcessed = null;
        $warnings = [];
        $firstSequenceAttempted = null;
        $lastSequenceAttempted = null;

        for ($step = 0; $step < $maxSteps; $step++) {
            if ($firstSequenceAttempted === null) {
                $firstSequenceAttempted = $nextSequence;
            }
            $lastSequenceAttempted = $nextSequence;

            $url = killmail_r2z2_base_url() . '/' . $nextSequence . '.json';
            $response = killmail_r2z2_fetch_json($url);
            $status = (int) ($response['status'] ?? 0);

            if ($status === 404) {
                $sequence404s++;
                $warnings[] = 'R2Z2 returned 404 for sequence ' . $nextSequence . '; worker reached end-of-feed and is backing off for ' . $pollBackoffSeconds . 's.';
                sleep($pollBackoffSeconds);
                break;
            }

            if ($status === 429 || $status === 403) {
                $warnings[] = 'R2Z2 returned status ' . $status . ' for sequence ' . $nextSequence . '; worker hit rate limiting and is backing off for ' . $pollBackoffSeconds . 's.';
                sleep($pollBackoffSeconds);
                break;
            }

            if ($status !== 200) {
                throw new RuntimeException('R2Z2 sequence fetch failed for ' . $nextSequence . ' with status=' . $status);
            }

            $sequenceFilesFetched++;
            $killmailsFetched++;
            $rowsSeen++;

            $transformed = killmail_transform_r2z2_payload((array) ($response['json'] ?? []));
            $event = (array) ($transformed['event'] ?? []);
            $sequenceId = (int) ($event['sequence_id'] ?? 0);
            if ($sequenceId <= 0) {
                throw new RuntimeException('R2Z2 payload missing sequence_id for stream row ' . $nextSequence);
            }

            $killmailId = (int) ($event['killmail_id'] ?? 0);
            $killmailHash = (string) ($event['killmail_hash'] ?? '');
            if ($killmailId > 0 && $killmailHash !== '' && db_killmail_event_exists($sequenceId, $killmailId, $killmailHash)) {
                $duplicateCount++;
                $nextSequence = $sequenceId + 1;
                $lastProcessed = $sequenceId;
                usleep(100000);
                continue;
            }

            if (!killmail_event_matches_tracked_entities($event, (array) ($transformed['attackers'] ?? []), $trackedAllianceIds, $trackedCorporationIds)) {
                $filteredCount++;
                $nextSequence = $sequenceId + 1;
                $lastProcessed = $sequenceId;
                usleep(100000);
                continue;
            }

            killmail_prime_entity_metadata(killmail_entity_resolution_requests(
                $event,
                (array) ($transformed['attackers'] ?? []),
                (array) ($transformed['items'] ?? [])
            ));

            db_transaction(static function () use ($transformed, $sequenceId, &$rowsWritten): void {
                db_killmail_event_upsert($transformed['event']);
                db_killmail_attackers_replace($sequenceId, $transformed['attackers']);
                db_killmail_items_replace($sequenceId, $transformed['items']);
                $rowsWritten++;
            });

            $lastProcessed = $sequenceId;
            $nextSequence = $sequenceId + 1;
            usleep(100000);
        }

        $cursorEnd = $lastProcessed !== null ? (string) $lastProcessed : ($cursor ?? '0');
        $checksum = sync_checksum([$rowsSeen, $rowsWritten, $cursorEnd]);
        sync_run_finalize_success($runId, $datasetKey, $runMode, $rowsSeen, $rowsWritten, $cursorEnd, $checksum, $warnings);

        $outcomeReason = 'completed with inserts';
        if ($rowsWritten === 0) {
            if ($firstSequenceAttempted === null || ($lastSavedSequence !== null && $latestSequence <= $lastSavedSequence)) {
                $outcomeReason = 'no new sequences available';
            } elseif ($sequenceFilesFetched === 0 && $sequence404s > 0) {
                $outcomeReason = 'sequence state already current';
            } elseif ($killmailsFetched > 0 && $filteredCount === $killmailsFetched) {
                $outcomeReason = 'all fetched killmails filtered out';
            } elseif ($killmailsFetched > 0 && $duplicateCount === $killmailsFetched) {
                $outcomeReason = 'all fetched killmails were duplicates';
            } else {
                $outcomeReason = 'no records inserted';
            }
        }

        return sync_result_shape() + [
            'rows_seen' => $rowsSeen,
            'rows_written' => $rowsWritten,
            'cursor' => $cursorEnd,
            'checksum' => $checksum,
            'warnings' => $warnings,
            'meta' => [
                'last_saved_sequence_before_run' => $lastSavedSequence,
                'latest_remote_sequence' => $latestSequence,
                'first_sequence_attempted' => $firstSequenceAttempted,
                'last_sequence_attempted' => $lastSequenceAttempted,
                'sequence_files_fetched' => $sequenceFilesFetched,
                'sequence_404s_encountered' => $sequence404s,
                'killmails_fetched' => $killmailsFetched,
                'killmails_skipped_duplicates' => $duplicateCount,
                'killmails_filtered_tracked_rules' => $filteredCount,
                'killmails_inserted' => $rowsWritten,
                'outcome_reason' => $outcomeReason,
                'last_processed_sequence' => $lastProcessed,
            ],
        ];
    } catch (Throwable $exception) {
        sync_run_finalize_failure($runId, $datasetKey, $runMode, $exception->getMessage());
        throw $exception;
    }
}

function doctrine_default_group_name(): string
{
    return trim((string) get_setting('doctrine.default_group', 'SupplyCore Doctrine')) ?: 'SupplyCore Doctrine';
}

function doctrine_group_options(): array
{
    try {
        $groups = db_doctrine_groups_all();
    } catch (Throwable) {
        $groups = [];
    }

    if ($groups === []) {
        return [
            [
                'id' => 0,
                'group_name' => doctrine_default_group_name(),
                'description' => 'Create your first doctrine group to start importing alliance fits.',
                'fit_count' => 0,
                'item_count' => 0,
                'last_fit_updated_at' => null,
            ],
        ];
    }

    return $groups;
}

function doctrine_sanitize_group_name(string $name): string
{
    $clean = preg_replace('/\s+/', ' ', trim($name));
    if (!is_string($clean) || $clean === '') {
        return doctrine_default_group_name();
    }

    return mb_substr($clean, 0, 190);
}

function doctrine_sanitize_description(?string $value): ?string
{
    $clean = trim((string) $value);

    return $clean === '' ? null : mb_substr($clean, 0, 1000);
}

function doctrine_normalize_item_name(string $name): string
{
    $clean = preg_replace('/\s+/', ' ', trim($name));

    return mb_strtolower((string) $clean);
}

function doctrine_detect_format(string $text): string
{
    foreach (preg_split('/\R/', $text) as $line) {
        $trimmed = trim((string) $line);
        if ($trimmed === '') {
            continue;
        }

        return preg_match('/^\[[^\],]+\s*,\s*[^\]]+\]$/', $trimmed) === 1 ? 'eft' : 'buyall';
    }

    return 'buyall';
}

function doctrine_parse_quantity_and_name(string $line): array
{
    $trimmed = preg_replace('/\s+/', ' ', trim($line));
    if (!is_string($trimmed) || $trimmed === '') {
        return ['item_name' => '', 'quantity' => 0];
    }

    if (preg_match('/^(.*?)\s+x\s*(\d+)$/i', $trimmed, $matches) === 1) {
        return [
            'item_name' => trim($matches[1]),
            'quantity' => max(1, (int) $matches[2]),
        ];
    }

    if (preg_match('/^(\d+)\s*x\s+(.*?)$/i', $trimmed, $matches) === 1) {
        return [
            'item_name' => trim($matches[2]),
            'quantity' => max(1, (int) $matches[1]),
        ];
    }

    if (preg_match('/^(\d+)\s+(.*?)$/', $trimmed, $matches) === 1) {
        return [
            'item_name' => trim($matches[2]),
            'quantity' => max(1, (int) $matches[1]),
        ];
    }

    return ['item_name' => $trimmed, 'quantity' => 1];
}

function doctrine_eft_category_label(int $blockIndex, string $itemName): string
{
    $normalized = doctrine_normalize_item_name($itemName);

    if (str_contains($normalized, 'nanite repair paste') || str_contains($normalized, 'mobile depot')) {
        return 'Cargo';
    }

    $map = [
        0 => 'Low Slots',
        1 => 'Medium Slots',
        2 => 'High Slots',
        3 => 'Rig Slots',
        4 => 'Drone Bay',
        5 => 'Cargo',
        6 => 'Implants',
        7 => 'Boosters',
        8 => 'Subsystems',
        9 => 'Service Slots',
    ];

    return $map[$blockIndex] ?? 'Additional Items';
}

function doctrine_parse_eft(string $text): array
{
    $lines = preg_split('/\R/', $text) ?: [];
    $header = null;
    $items = [];
    $blockIndex = 0;
    $sawItemInBlock = false;

    foreach ($lines as $rawLine) {
        $line = trim((string) $rawLine);

        if ($line === '') {
            if ($sawItemInBlock) {
                $blockIndex++;
                $sawItemInBlock = false;
            }
            continue;
        }

        if ($header === null) {
            if (preg_match('/^\[\s*(.+?)\s*,\s*(.+?)\s*\]$/', $line, $matches) !== 1) {
                throw new RuntimeException('EFT import must begin with a [Ship, Fit Name] header.');
            }

            $header = [
                'ship_name' => trim($matches[1]),
                'fit_name' => trim($matches[2]),
            ];
            continue;
        }

        if (preg_match('/^\[empty.+slot\]$/i', $line) === 1) {
            $sawItemInBlock = true;
            continue;
        }

        $parsed = doctrine_parse_quantity_and_name($line);
        $itemName = trim((string) ($parsed['item_name'] ?? ''));
        if ($itemName === '') {
            continue;
        }

        $items[] = [
            'line_number' => count($items) + 1,
            'slot_category' => doctrine_eft_category_label($blockIndex, $itemName),
            'item_name' => $itemName,
            'quantity' => max(1, (int) ($parsed['quantity'] ?? 1)),
        ];
        $sawItemInBlock = true;
    }

    if ($header === null) {
        throw new RuntimeException('No EFT header was found in the import payload.');
    }

    return [
        'format' => 'eft',
        'fit_name' => $header['fit_name'],
        'ship_name' => $header['ship_name'],
        'items' => $items,
    ];
}

function doctrine_parse_buyall(string $text): array
{
    $items = [];

    foreach (preg_split('/\R/', $text) ?: [] as $rawLine) {
        $line = trim((string) $rawLine);
        if ($line === '') {
            continue;
        }

        $parsed = doctrine_parse_quantity_and_name($line);
        $itemName = trim((string) ($parsed['item_name'] ?? ''));
        if ($itemName === '') {
            continue;
        }

        $items[] = [
            'line_number' => count($items) + 1,
            'slot_category' => count($items) === 0 ? 'Hull' : 'Items',
            'item_name' => $itemName,
            'quantity' => max(1, (int) ($parsed['quantity'] ?? 1)),
        ];
    }

    $shipName = 'Unspecified Hull';
    $fitName = 'Imported BuyAll List';
    if ($items !== []) {
        $shipName = (string) ($items[0]['item_name'] ?? $shipName);
        $fitName = $shipName . ' BuyAll';
    }

    return [
        'format' => 'buyall',
        'fit_name' => $fitName,
        'ship_name' => $shipName,
        'items' => $items,
    ];
}

function doctrine_parse_import_text(string $text): array
{
    $trimmed = trim($text);
    if ($trimmed === '') {
        throw new RuntimeException('Paste an EFT fit or BuyAll list before importing.');
    }

    return doctrine_detect_format($trimmed) === 'eft'
        ? doctrine_parse_eft($trimmed)
        : doctrine_parse_buyall($trimmed);
}

function doctrine_extract_inventory_type_rows(array $payload): array
{
    foreach (['inventory_types', 'inventoryTypes', 'types'] as $key) {
        $rows = $payload[$key] ?? null;
        if (is_array($rows)) {
            return $rows;
        }
    }

    return [];
}

function doctrine_resolve_names_from_esi_ids(array $names): array
{
    if ($names === []) {
        return [];
    }

    try {
        $payload = killmail_universe_ids_lookup($names);
    } catch (Throwable) {
        return [];
    }

    $resolved = [];
    foreach (doctrine_extract_inventory_type_rows($payload) as $row) {
        if (!is_array($row)) {
            continue;
        }

        $canonicalName = trim((string) ($row['name'] ?? ''));
        $typeId = (int) ($row['id'] ?? 0);
        if ($canonicalName === '' || $typeId <= 0) {
            continue;
        }

        $resolved[doctrine_normalize_item_name($canonicalName)] = [
            'item_name' => $canonicalName,
            'type_id' => $typeId,
            'resolution_source' => 'esi',
        ];
    }

    return $resolved;
}

function doctrine_search_inventory_type_esi(string $query): ?array
{
    $term = trim($query);
    if ($term === '') {
        return null;
    }

    $context = esi_lookup_context();
    if (($context['ok'] ?? false) !== true) {
        return null;
    }

    $characterId = (int) (($context['token']['character_id'] ?? 0) ?: ($context['token']['characterId'] ?? 0));
    if ($characterId <= 0) {
        $token = db_latest_esi_oauth_token();
        $characterId = (int) ($token['character_id'] ?? 0);
    }
    if ($characterId <= 0) {
        return null;
    }

    try {
        $accessToken = esi_valid_access_token();
        $response = http_get_json(
            'https://esi.evetech.net/latest/characters/' . $characterId . '/search/?categories=inventory_type&strict=true&search=' . rawurlencode($term),
            [
                'Authorization: Bearer ' . $accessToken,
                'Accept: application/json',
                'User-Agent: ' . esi_user_agent(),
            ]
        );
    } catch (Throwable) {
        return null;
    }

    if ((int) ($response['status'] ?? 500) >= 400) {
        return null;
    }

    $ids = array_values(array_filter(array_map('intval', (array) ($response['json']['inventory_type'] ?? [])), static fn (int $id): bool => $id > 0));
    if ($ids === []) {
        return null;
    }

    try {
        $names = esi_universe_names_lookup($ids);
    } catch (Throwable) {
        return null;
    }

    foreach ($names as $row) {
        if (!is_array($row)) {
            continue;
        }

        $name = trim((string) ($row['name'] ?? ''));
        $id = (int) ($row['id'] ?? 0);
        if ($id <= 0 || $name === '') {
            continue;
        }

        if (doctrine_normalize_item_name($name) !== doctrine_normalize_item_name($term)) {
            continue;
        }

        return [
            'item_name' => $name,
            'type_id' => $id,
            'resolution_source' => 'esi',
        ];
    }

    return null;
}

function doctrine_resolve_item_names(array $names): array
{
    $unique = [];
    foreach ($names as $name) {
        $trimmed = trim((string) $name);
        if ($trimmed === '') {
            continue;
        }
        $unique[doctrine_normalize_item_name($trimmed)] = $trimmed;
    }

    if ($unique === []) {
        return [];
    }

    $resolved = [];

    try {
        foreach (db_item_name_cache_get_many(array_keys($unique)) as $row) {
            $normalized = doctrine_normalize_item_name((string) ($row['normalized_name'] ?? ''));
            if ($normalized === '') {
                continue;
            }

            $resolved[$normalized] = [
                'item_name' => (string) ($row['item_name'] ?? ($unique[$normalized] ?? '')),
                'type_id' => isset($row['type_id']) && $row['type_id'] !== null ? (int) $row['type_id'] : null,
                'resolution_source' => (string) ($row['resolution_source'] ?? 'cache'),
            ];
        }
    } catch (Throwable) {
        // Cache unavailable; continue to reference tables.
    }

    $lookupNames = [];
    foreach ($unique as $normalized => $original) {
        if (!isset($resolved[$normalized]) || (int) ($resolved[$normalized]['type_id'] ?? 0) <= 0) {
            $lookupNames[$normalized] = $original;
        }
    }

    if ($lookupNames !== []) {
        try {
            foreach (db_ref_item_types_by_names(array_values($lookupNames)) as $row) {
                $normalized = doctrine_normalize_item_name((string) ($row['type_name'] ?? ''));
                $typeId = (int) ($row['type_id'] ?? 0);
                if ($normalized === '' || $typeId <= 0) {
                    continue;
                }

                $resolved[$normalized] = [
                    'item_name' => (string) ($row['type_name'] ?? ''),
                    'type_id' => $typeId,
                    'resolution_source' => 'ref',
                ];
                try {
                    db_item_name_cache_upsert($normalized, (string) ($row['type_name'] ?? ''), $typeId, 'ref');
                } catch (Throwable) {
                    // Ignore cache write failures.
                }
                unset($lookupNames[$normalized]);
            }
        } catch (Throwable) {
            // Continue to ESI lookup.
        }
    }

    if ($lookupNames !== []) {
        foreach (doctrine_resolve_names_from_esi_ids(array_values($lookupNames)) as $normalized => $row) {
            $resolved[$normalized] = $row;
            try {
                db_item_name_cache_upsert($normalized, (string) ($row['item_name'] ?? $lookupNames[$normalized] ?? ''), (int) ($row['type_id'] ?? 0), 'esi');
            } catch (Throwable) {
                // Ignore cache write failures.
            }
            unset($lookupNames[$normalized]);
        }
    }

    if ($lookupNames !== []) {
        foreach ($lookupNames as $normalized => $original) {
            $found = doctrine_search_inventory_type_esi($original);
            if ($found !== null) {
                $resolved[$normalized] = $found;
                try {
                    db_item_name_cache_upsert($normalized, (string) $found['item_name'], (int) ($found['type_id'] ?? 0), 'esi');
                } catch (Throwable) {
                    // Ignore cache write failures.
                }
                continue;
            }

            $resolved[$normalized] = [
                'item_name' => $original,
                'type_id' => null,
                'resolution_source' => 'missing',
            ];
            try {
                db_item_name_cache_upsert($normalized, $original, null, 'missing');
            } catch (Throwable) {
                // Ignore cache write failures.
            }
        }
    }

    return $resolved;
}

function doctrine_resolve_parsed_fit(array $parsed, string $rawText): array
{
    $names = [$parsed['ship_name'] ?? ''];
    foreach ((array) ($parsed['items'] ?? []) as $item) {
        $names[] = (string) ($item['item_name'] ?? '');
    }

    $resolvedLookup = doctrine_resolve_item_names($names);
    $shipKey = doctrine_normalize_item_name((string) ($parsed['ship_name'] ?? ''));
    $shipResolved = $resolvedLookup[$shipKey] ?? [
        'item_name' => (string) ($parsed['ship_name'] ?? ''),
        'type_id' => null,
        'resolution_source' => 'missing',
    ];

    $resolvedItems = [];
    $unresolvedItems = [];
    foreach ((array) ($parsed['items'] ?? []) as $item) {
        $itemKey = doctrine_normalize_item_name((string) ($item['item_name'] ?? ''));
        $resolved = $resolvedLookup[$itemKey] ?? [
            'item_name' => (string) ($item['item_name'] ?? ''),
            'type_id' => null,
            'resolution_source' => 'missing',
        ];

        $resolvedItem = [
            'line_number' => (int) ($item['line_number'] ?? count($resolvedItems) + 1),
            'slot_category' => (string) ($item['slot_category'] ?? 'Items'),
            'item_name' => (string) ($resolved['item_name'] ?? ($item['item_name'] ?? '')),
            'type_id' => isset($resolved['type_id']) && $resolved['type_id'] !== null ? (int) $resolved['type_id'] : null,
            'quantity' => max(1, (int) ($item['quantity'] ?? 1)),
            'resolution_source' => (string) ($resolved['resolution_source'] ?? 'missing'),
        ];

        if (($resolvedItem['type_id'] ?? null) === null) {
            $unresolvedItems[] = $resolvedItem['item_name'];
        }

        $resolvedItems[] = $resolvedItem;
    }

    $fitName = trim((string) ($parsed['fit_name'] ?? ''));
    if ($fitName === '') {
        $fitName = trim((string) ($shipResolved['item_name'] ?? $parsed['ship_name'] ?? 'Imported Doctrine Fit'));
    }

    return [
        'fit' => [
            'fit_name' => mb_substr($fitName, 0, 190),
            'ship_name' => trim((string) ($shipResolved['item_name'] ?? $parsed['ship_name'] ?? 'Unknown Hull')),
            'ship_type_id' => isset($shipResolved['type_id']) && $shipResolved['type_id'] !== null ? (int) $shipResolved['type_id'] : null,
            'source_format' => (string) ($parsed['format'] ?? doctrine_detect_format($rawText)),
            'import_body' => $rawText,
            'item_count' => count($resolvedItems),
            'unresolved_count' => count($unresolvedItems) + ((int) ($shipResolved['type_id'] ?? 0) > 0 ? 0 : 1),
        ],
        'items' => $resolvedItems,
        'ship' => $shipResolved,
        'unresolved' => array_values(array_unique(array_filter(array_merge(
            (int) ($shipResolved['type_id'] ?? 0) > 0 ? [] : [trim((string) ($shipResolved['item_name'] ?? $parsed['ship_name'] ?? 'Unknown Hull'))],
            $unresolvedItems
        )))),
    ];
}

function doctrine_resolve_group_id_from_request(array $post): int
{
    $existingGroupId = max(0, (int) ($post['group_id'] ?? 0));
    $newGroupName = doctrine_sanitize_group_name((string) ($post['new_group_name'] ?? ''));
    $newGroupDescription = doctrine_sanitize_description($post['new_group_description'] ?? null);

    if ($existingGroupId > 0 && trim((string) ($post['new_group_name'] ?? '')) === '') {
        return $existingGroupId;
    }

    try {
        return db_doctrine_group_create($newGroupName, $newGroupDescription);
    } catch (Throwable) {
        return $existingGroupId;
    }
}

function doctrine_import_fit_from_request(array $post): array
{
    $rawText = trim((string) ($post['fit_payload'] ?? ''));
    if ($rawText === '') {
        return ['ok' => false, 'message' => 'Paste a doctrine fit payload before importing.'];
    }

    $groupId = doctrine_resolve_group_id_from_request($post);
    if ($groupId <= 0) {
        return ['ok' => false, 'message' => 'Select an existing doctrine group or enter a new group name.'];
    }

    try {
        $parsed = doctrine_parse_import_text($rawText);
        $resolved = doctrine_resolve_parsed_fit($parsed, $rawText);
    } catch (Throwable $exception) {
        return ['ok' => false, 'message' => $exception->getMessage()];
    }

    if (($resolved['unresolved'] ?? []) !== []) {
        return [
            'ok' => false,
            'message' => 'Unable to resolve all items to real EVE types. Resolve these names first: ' . implode(', ', array_slice((array) $resolved['unresolved'], 0, 12)),
            'parsed' => $parsed,
            'resolved' => $resolved,
        ];
    }

    $fit = (array) ($resolved['fit'] ?? []);
    $fit['doctrine_group_id'] = $groupId;

    try {
        $fitId = db_doctrine_fit_create($fit, (array) ($resolved['items'] ?? []));
    } catch (Throwable $exception) {
        return ['ok' => false, 'message' => 'Doctrine fit import failed: ' . $exception->getMessage()];
    }

    return [
        'ok' => true,
        'message' => 'Doctrine fit imported successfully.',
        'fit_id' => $fitId,
        'group_id' => $groupId,
        'parsed' => $parsed,
        'resolved' => $resolved,
    ];
}

function doctrine_ship_image_url(?int $typeId, int $size = 128): ?string
{
    $safeTypeId = max(0, (int) $typeId);
    if ($safeTypeId <= 0) {
        return null;
    }

    $safeSize = max(32, min(512, $size));

    return 'https://images.evetech.net/types/' . $safeTypeId . '/icon?size=' . $safeSize;
}

function doctrine_format_quantity(int $value): string
{
    return number_format(max(0, $value));
}

function doctrine_fit_item_market_rows(array $items): array
{
    $typeIds = [];
    foreach ($items as $item) {
        $typeId = (int) ($item['type_id'] ?? 0);
        if ($typeId > 0) {
            $typeIds[] = $typeId;
        }
    }

    $comparisonRows = market_comparison_outcomes($typeIds)['rows'] ?? [];
    $comparisonByTypeId = [];
    foreach ($comparisonRows as $row) {
        $typeId = (int) ($row['type_id'] ?? 0);
        if ($typeId > 0) {
            $comparisonByTypeId[$typeId] = $row;
        }
    }

    $rows = [];
    foreach ($items as $item) {
        $typeId = (int) ($item['type_id'] ?? 0);
        $market = $comparisonByTypeId[$typeId] ?? [];
        $requiredQty = max(1, (int) ($item['quantity'] ?? 1));
        $localQty = max(0, (int) ($market['alliance_total_sell_volume'] ?? 0));
        $missingQty = max(0, $requiredQty - $localQty);
        $coverageRatio = $requiredQty > 0 ? ($localQty / $requiredQty) : 0.0;
        $status = $missingQty <= 0 ? 'ok' : ($coverageRatio >= 0.5 ? 'low' : 'missing');

        $rows[] = $item + [
            'local_available_qty' => $localQty,
            'missing_qty' => $missingQty,
            'local_price' => isset($market['alliance_best_sell_price']) ? (float) $market['alliance_best_sell_price'] : null,
            'hub_price' => isset($market['reference_best_sell_price']) ? (float) $market['reference_best_sell_price'] : null,
            'market_status' => $status,
            'market_label' => match ($status) {
                'ok' => 'Stock OK',
                'low' => 'Low',
                default => 'Missing',
            },
            'observed_at' => $market['alliance_last_observed_at'] ?? null,
        ];
    }

    return $rows;
}

function doctrine_category_sort_order(string $category): int
{
    $map = [
        'Hull' => 5,
        'Low Slots' => 10,
        'Medium Slots' => 20,
        'High Slots' => 30,
        'Rig Slots' => 40,
        'Subsystems' => 50,
        'Service Slots' => 60,
        'Drone Bay' => 70,
        'Cargo' => 80,
        'Implants' => 90,
        'Boosters' => 100,
        'Items' => 110,
        'Additional Items' => 120,
    ];

    return $map[$category] ?? 999;
}

function doctrine_group_market_rows_by_category(array $items): array
{
    $categories = [];
    foreach (doctrine_fit_item_market_rows($items) as $row) {
        $category = trim((string) ($row['slot_category'] ?? 'Items'));
        if ($category === '') {
            $category = 'Items';
        }

        $categories[$category][] = $row;
    }

    uksort($categories, static function (string $a, string $b): int {
        return doctrine_category_sort_order($a) <=> doctrine_category_sort_order($b) ?: strcasecmp($a, $b);
    });

    return $categories;
}

function doctrine_groups_overview_data(): array
{
    $groups = doctrine_group_options();
    $fitCount = 0;
    $itemCount = 0;
    foreach ($groups as $group) {
        $fitCount += (int) ($group['fit_count'] ?? 0);
        $itemCount += (int) ($group['item_count'] ?? 0);
    }

    return [
        'summary' => [
            ['label' => 'Doctrine Groups', 'value' => (string) count($groups), 'context' => 'Organized doctrine collections ready for market mapping'],
            ['label' => 'Imported Fits', 'value' => (string) $fitCount, 'context' => 'Alliance fit payloads normalized and stored'],
            ['label' => 'Tracked Items', 'value' => (string) $itemCount, 'context' => 'Doctrine lines prepared for gap detection'],
            ['label' => 'Baseline Goal', 'value' => 'Ready', 'context' => 'Restock, hauling, and supply-gap workflows'],
        ],
        'groups' => $groups,
    ];
}

function doctrine_group_detail_data(int $groupId): array
{
    try {
        $group = db_doctrine_group_by_id($groupId);
        $fits = db_doctrine_fits_by_group($groupId);
    } catch (Throwable) {
        $group = null;
        $fits = [];
    }

    if ($group === null) {
        return ['group' => null, 'fits' => []];
    }

    foreach ($fits as &$fit) {
        $requiredQty = max(0, (int) ($fit['required_quantity'] ?? 0));
        $fit['required_quantity_label'] = doctrine_format_quantity($requiredQty);
        $fit['ship_image_url'] = doctrine_ship_image_url(isset($fit['ship_type_id']) ? (int) $fit['ship_type_id'] : null, 64);
    }
    unset($fit);

    return ['group' => $group, 'fits' => $fits];
}

function doctrine_fit_detail_view_model(int $fitId): array
{
    try {
        $fit = db_doctrine_fit_by_id($fitId);
        $items = db_doctrine_fit_items_by_fit($fitId);
    } catch (Throwable) {
        $fit = null;
        $items = [];
    }

    if ($fit === null) {
        return ['fit' => null, 'categories' => [], 'summary' => []];
    }

    $categories = doctrine_group_market_rows_by_category($items);
    $totalRequired = 0;
    $totalLocal = 0;
    $missingLines = 0;
    $okLines = 0;

    foreach ($categories as &$rows) {
        foreach ($rows as &$row) {
            $totalRequired += max(1, (int) ($row['quantity'] ?? 1));
            $totalLocal += max(0, (int) ($row['local_available_qty'] ?? 0));
            if ((int) ($row['missing_qty'] ?? 0) > 0) {
                $missingLines++;
            } else {
                $okLines++;
            }

            $row['required_qty_label'] = doctrine_format_quantity((int) ($row['quantity'] ?? 1));
            $row['local_available_qty_label'] = doctrine_format_quantity((int) ($row['local_available_qty'] ?? 0));
            $row['missing_qty_label'] = doctrine_format_quantity((int) ($row['missing_qty'] ?? 0));
            $row['local_price_label'] = market_format_isk(isset($row['local_price']) ? (float) $row['local_price'] : null);
            $row['hub_price_label'] = market_format_isk(isset($row['hub_price']) ? (float) $row['hub_price'] : null);
        }
        unset($row);
    }
    unset($rows);

    $fit['ship_image_url'] = doctrine_ship_image_url(isset($fit['ship_type_id']) ? (int) $fit['ship_type_id'] : null, 256);

    return [
        'fit' => $fit,
        'categories' => $categories,
        'summary' => [
            ['label' => 'Required Units', 'value' => doctrine_format_quantity($totalRequired), 'context' => 'Doctrine demand to satisfy one complete fit'],
            ['label' => 'Local Units', 'value' => doctrine_format_quantity($totalLocal), 'context' => 'Alliance market quantity available right now'],
            ['label' => 'Missing Lines', 'value' => doctrine_format_quantity($missingLines), 'context' => 'Immediate supply gaps to restock or haul'],
            ['label' => 'Stock OK Lines', 'value' => doctrine_format_quantity($okLines), 'context' => 'Lines already covered in alliance stock'],
        ],
    ];
}
