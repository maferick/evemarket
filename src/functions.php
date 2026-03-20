<?php

declare(strict_types=1);

require_once __DIR__ . '/db.php';
require_once __DIR__ . '/cache.php';

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
            'label' => 'Activity Priority',
            'path' => '/activity-priority',
            'icon' => '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" class="h-4 w-4" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" d="M4 7h16"/><path stroke-linecap="round" stroke-linejoin="round" d="M6 12h12"/><path stroke-linecap="round" stroke-linejoin="round" d="M9 17h6"/><path stroke-linecap="round" stroke-linejoin="round" d="m15 5 4 4-4 4"/></svg>',
            'children' => [
                ['label' => 'Doctrine Activity Board', 'path' => '/activity-priority'],
            ],
        ],
        [
            'label' => 'Doctrine Fits',
            'path' => '/doctrine',
            'icon' => '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" class="h-4 w-4" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" d="M5 5h14v14H5z"/><path stroke-linecap="round" stroke-linejoin="round" d="M9 9h6M9 13h6M9 17h4"/></svg>',
            'children' => [
                ['label' => 'Doctrine Groups', 'path' => '/doctrine'],
                ['label' => 'Fit Overview', 'path' => '/doctrine/fits'],
                ['label' => 'Bulk Import', 'path' => '/doctrine/import'],
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
        'item-scope' => ['title' => 'Item Scope', 'description' => 'Control which item classes are operationally relevant across market, doctrine, and loss-demand analytics.'],
        'ai-briefings' => ['title' => 'AI Briefings', 'description' => 'Configure either a local Ollama endpoint or a Runpod serverless endpoint for doctrine briefing summaries.'],
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

function item_scope_setting_keys(): array
{
    return [
        'item_scope_mode',
        'item_scope_operational_category_keys',
        'item_scope_tier_meta_group_ids',
        'item_scope_noise_filter_keys',
        'item_scope_include_category_ids',
        'item_scope_exclude_category_ids',
        'item_scope_include_group_ids',
        'item_scope_exclude_group_ids',
        'item_scope_include_market_group_ids',
        'item_scope_exclude_market_group_ids',
        'item_scope_include_meta_group_ids',
        'item_scope_exclude_meta_group_ids',
        'item_scope_include_type_ids',
        'item_scope_exclude_type_ids',
    ];
}

function ai_briefing_setting_defaults(): array
{
    return [
        'ollama_enabled' => '0',
        'ollama_provider' => 'local',
        'ollama_url' => 'http://localhost:11434/api',
        'ollama_model' => 'qwen2.5:1.5b-instruct',
        'ollama_timeout' => '20',
        'ollama_capability_tier' => 'auto',
        'ollama_runpod_url' => '',
        'ollama_runpod_api_key' => '',
    ];
}

function ai_briefing_setting_keys(): array
{
    return array_keys(ai_briefing_setting_defaults());
}

function ollama_provider_options(): array
{
    return [
        'local' => 'Local Ollama',
        'runpod' => 'Runpod Serverless',
    ];
}

function sanitize_ollama_provider(mixed $value): string
{
    $provider = mb_strtolower(trim((string) $value));

    return array_key_exists($provider, ollama_provider_options())
        ? $provider
        : ai_briefing_setting_defaults()['ollama_provider'];
}

function sanitize_ollama_url(?string $value): string
{
    $url = rtrim(trim((string) $value), '/');

    if ($url === '') {
        return ai_briefing_setting_defaults()['ollama_url'];
    }

    if (!preg_match('/^https?:\/\/.+/i', $url)) {
        return ai_briefing_setting_defaults()['ollama_url'];
    }

    return mb_substr($url, 0, 255);
}

function sanitize_ollama_model(?string $value): string
{
    $model = trim((string) $value);

    if ($model === '') {
        return ai_briefing_setting_defaults()['ollama_model'];
    }

    return mb_substr($model, 0, 120);
}

function sanitize_ollama_timeout(mixed $value): string
{
    $timeout = max(1, min(300, (int) $value));

    return (string) $timeout;
}

function sanitize_ollama_capability_tier(mixed $value): string
{
    $tier = mb_strtolower(trim((string) $value));

    return in_array($tier, ['auto', 'small', 'medium', 'large'], true)
        ? $tier
        : ai_briefing_setting_defaults()['ollama_capability_tier'];
}

function sanitize_ollama_runpod_url(?string $value): string
{
    $url = rtrim(trim((string) $value), '/');
    if ($url === '') {
        return '';
    }

    if (!preg_match('/^https?:\/\/.+/i', $url)) {
        return '';
    }

    return mb_substr($url, 0, 255);
}

function sanitize_ollama_runpod_api_key(?string $value): string
{
    return mb_substr(trim((string) $value), 0, 255);
}

function ai_briefing_settings_from_request(array $request): array
{
    return [
        'ollama_enabled' => sanitize_enabled_flag($request['ollama_enabled'] ?? null),
        'ollama_provider' => sanitize_ollama_provider($request['ollama_provider'] ?? null),
        'ollama_url' => sanitize_ollama_url($request['ollama_url'] ?? null),
        'ollama_model' => sanitize_ollama_model($request['ollama_model'] ?? null),
        'ollama_timeout' => sanitize_ollama_timeout($request['ollama_timeout'] ?? null),
        'ollama_capability_tier' => sanitize_ollama_capability_tier($request['ollama_capability_tier'] ?? null),
        'ollama_runpod_url' => sanitize_ollama_runpod_url($request['ollama_runpod_url'] ?? null),
        'ollama_runpod_api_key' => sanitize_ollama_runpod_api_key($request['ollama_runpod_api_key'] ?? null),
    ];
}

function item_scope_default_meta_groups(): array
{
    return [
        ['meta_group_id' => 1, 'meta_group_name' => 'Tech I', 'type_count' => 0],
        ['meta_group_id' => 2, 'meta_group_name' => 'Tech II', 'type_count' => 0],
        ['meta_group_id' => 4, 'meta_group_name' => 'Faction', 'type_count' => 0],
        ['meta_group_id' => 6, 'meta_group_name' => 'Deadspace', 'type_count' => 0],
        ['meta_group_id' => 5, 'meta_group_name' => 'Officer', 'type_count' => 0],
    ];
}

function item_scope_default_operational_category_keys(): array
{
    return ['ships', 'modules', 'rigs', 'ammo_charges', 'drones_fighters', 'fuel_structures', 'boosters'];
}

function item_scope_default_noise_filter_keys(): array
{
    return [
        'exclude_commodities_consumer_goods',
        'exclude_civilian_items',
        'exclude_blueprints',
        'exclude_skins',
        'exclude_non_market_mission_items',
    ];
}

function item_scope_decode_int_list(mixed $value): array
{
    if (is_array($value)) {
        $decoded = $value;
    } else {
        $raw = trim((string) $value);
        if ($raw === '') {
            return [];
        }

        try {
            $decoded = json_decode($raw, true, 512, JSON_THROW_ON_ERROR);
        } catch (Throwable) {
            $decoded = preg_split('/[\s,]+/', $raw) ?: [];
        }
    }

    return array_values(array_unique(array_filter(array_map(static fn (mixed $id): int => (int) $id, $decoded), static fn (int $id): bool => $id > 0)));
}

function item_scope_encode_int_list(array $ids): string
{
    return json_encode(item_scope_decode_int_list($ids), JSON_THROW_ON_ERROR);
}

function item_scope_decode_string_list(mixed $value): array
{
    if (is_array($value)) {
        $decoded = $value;
    } else {
        $raw = trim((string) $value);
        if ($raw === '') {
            return [];
        }

        try {
            $decoded = json_decode($raw, true, 512, JSON_THROW_ON_ERROR);
        } catch (Throwable) {
            $decoded = preg_split('/[\r\n,]+/', $raw) ?: [];
        }
    }

    $normalized = array_map(static fn (mixed $entry): string => trim(mb_strtolower((string) $entry)), $decoded);

    return array_values(array_unique(array_filter($normalized, static fn (string $entry): bool => $entry !== '')));
}

function item_scope_encode_string_list(array $values): string
{
    return json_encode(item_scope_decode_string_list($values), JSON_THROW_ON_ERROR);
}

function item_scope_default_config(): array
{
    return [
        'mode' => 'allow_list',
        'operational_category_keys' => item_scope_default_operational_category_keys(),
        'tier_meta_group_ids' => [1, 2],
        'noise_filter_keys' => item_scope_default_noise_filter_keys(),
        'include_category_ids' => [],
        'exclude_category_ids' => [],
        'include_group_ids' => [],
        'exclude_group_ids' => [],
        'include_market_group_ids' => [],
        'exclude_market_group_ids' => [],
        'include_meta_group_ids' => [],
        'exclude_meta_group_ids' => [],
        'include_type_ids' => [],
        'exclude_type_ids' => [],
    ];
}

function item_scope_normalize_mode(string $mode): string
{
    return $mode === 'allow_all' ? 'allow_all' : 'allow_list';
}

function item_scope_settings_to_config(array $settings): array
{
    $defaults = item_scope_default_config();

    return [
        'mode' => item_scope_normalize_mode((string) ($settings['item_scope_mode'] ?? $defaults['mode'])),
        'operational_category_keys' => item_scope_decode_string_list($settings['item_scope_operational_category_keys'] ?? $defaults['operational_category_keys']),
        'tier_meta_group_ids' => item_scope_decode_int_list($settings['item_scope_tier_meta_group_ids'] ?? $defaults['tier_meta_group_ids']),
        'noise_filter_keys' => item_scope_decode_string_list($settings['item_scope_noise_filter_keys'] ?? $defaults['noise_filter_keys']),
        'include_category_ids' => item_scope_decode_int_list($settings['item_scope_include_category_ids'] ?? $defaults['include_category_ids']),
        'exclude_category_ids' => item_scope_decode_int_list($settings['item_scope_exclude_category_ids'] ?? $defaults['exclude_category_ids']),
        'include_group_ids' => item_scope_decode_int_list($settings['item_scope_include_group_ids'] ?? $defaults['include_group_ids']),
        'exclude_group_ids' => item_scope_decode_int_list($settings['item_scope_exclude_group_ids'] ?? $defaults['exclude_group_ids']),
        'include_market_group_ids' => item_scope_decode_int_list($settings['item_scope_include_market_group_ids'] ?? $defaults['include_market_group_ids']),
        'exclude_market_group_ids' => item_scope_decode_int_list($settings['item_scope_exclude_market_group_ids'] ?? $defaults['exclude_market_group_ids']),
        'include_meta_group_ids' => item_scope_decode_int_list($settings['item_scope_include_meta_group_ids'] ?? $defaults['include_meta_group_ids']),
        'exclude_meta_group_ids' => item_scope_decode_int_list($settings['item_scope_exclude_meta_group_ids'] ?? $defaults['exclude_meta_group_ids']),
        'include_type_ids' => item_scope_decode_int_list($settings['item_scope_include_type_ids'] ?? $defaults['include_type_ids']),
        'exclude_type_ids' => item_scope_decode_int_list($settings['item_scope_exclude_type_ids'] ?? $defaults['exclude_type_ids']),
    ];
}

function item_scope_config(): array
{
    static $config = null;

    if ($config !== null) {
        return $config;
    }

    $config = item_scope_settings_to_config(get_settings(item_scope_setting_keys()));

    return $config;
}

function item_scope_operational_definitions(): array
{
    return [
        'ships' => [
            'label' => 'Ships',
            'description' => 'Combat hulls and logistics ships tracked as the alliance operating fleet.',
            'default' => true,
            'clauses' => [
                ['category_names' => ['ship']],
            ],
        ],
        'modules' => [
            'label' => 'Modules',
            'description' => 'Standard fit components, including subsystems, but excluding rig-only lines.',
            'default' => true,
            'clauses' => [
                [
                    'category_names' => ['module', 'subsystem'],
                    'exclude_group_keywords' => ['rig'],
                    'exclude_market_group_keywords' => ['rig'],
                ],
            ],
        ],
        'rigs' => [
            'label' => 'Rigs',
            'description' => 'Rig market segments separated from the wider module universe.',
            'default' => true,
            'clauses' => [
                ['category_names' => ['module'], 'group_keywords' => ['rig']],
                ['category_names' => ['module'], 'market_group_keywords' => ['rig']],
            ],
        ],
        'ammo_charges' => [
            'label' => 'Ammo & Charges',
            'description' => 'Ammunition, scripts, crystals, bombs, and other consumable charge-based items.',
            'default' => true,
            'clauses' => [
                ['category_names' => ['charge']],
            ],
        ],
        'drones_fighters' => [
            'label' => 'Drones & Fighters',
            'description' => 'Drone bays, carrier support, and fighter wings used in doctrine fits.',
            'default' => true,
            'clauses' => [
                ['category_names' => ['drone', 'fighter']],
            ],
        ],
        'fuel_structures' => [
            'label' => 'Fuel & Structures',
            'description' => 'Structure hulls, deployables, and operational fuel inputs for alliance infrastructure.',
            'default' => true,
            'clauses' => [
                ['category_names' => ['structure', 'deployable', 'starbase']],
                ['category_names' => ['charge'], 'group_keywords' => ['fuel', 'isotope', 'strontium', 'liquid ozone', 'heavy water']],
                ['category_names' => ['charge'], 'market_group_keywords' => ['fuel']],
            ],
        ],
        'boosters' => [
            'label' => 'Boosters',
            'description' => 'Combat and support boosters surfaced separately from general implants.',
            'default' => true,
            'clauses' => [
                ['category_names' => ['implant'], 'group_keywords' => ['booster']],
                ['category_names' => ['implant'], 'market_group_keywords' => ['booster']],
            ],
        ],
        'industry' => [
            'label' => 'Industry',
            'description' => 'Optional manufacturing and input chains for blueprints, materials, and components.',
            'default' => false,
            'clauses' => [
                ['category_names' => ['blueprint']],
                ['category_names' => ['commodity', 'planetary commodities'], 'market_group_keywords' => ['planetary', 'component', 'reaction', 'salvage', 'materials']],
            ],
        ],
    ];
}

function item_scope_noise_filter_definitions(): array
{
    return [
        'exclude_commodities_consumer_goods' => [
            'label' => 'Exclude commodities / consumer goods',
            'description' => 'Suppress commodity-style goods and planetary trade cargo by category and market taxonomy.',
            'default' => true,
            'clauses' => [
                ['category_names' => ['commodity', 'planetary commodities']],
                ['market_group_keywords' => ['commodity', 'consumer goods', 'trade goods', 'planetary']],
            ],
        ],
        'exclude_civilian_items' => [
            'label' => 'Exclude civilian items',
            'description' => 'Remove tutorial and civilian equipment from logistics views.',
            'default' => true,
            'clauses' => [
                ['group_keywords' => ['civilian']],
                ['market_group_keywords' => ['civilian']],
                ['type_name_keywords' => ['civilian']],
            ],
        ],
        'exclude_blueprints' => [
            'label' => 'Exclude blueprints',
            'description' => 'Filter blueprint categories unless Industry mode is explicitly enabled.',
            'default' => true,
            'clauses' => [
                ['category_names' => ['blueprint']],
                ['market_group_keywords' => ['blueprint']],
            ],
        ],
        'exclude_skins' => [
            'label' => 'Exclude skins',
            'description' => 'Remove cosmetic SKIN inventory and cosmetic market branches.',
            'default' => true,
            'clauses' => [
                ['category_names' => ['skin', 'skins']],
                ['market_group_keywords' => ['skin']],
                ['group_keywords' => ['skin']],
            ],
        ],
        'exclude_non_market_mission_items' => [
            'label' => 'Exclude mission / non-market / irrelevant items',
            'description' => 'Suppress unpublished, unmarketed, and mission-only inventory that should not drive alliance logistics.',
            'default' => true,
            'clauses' => [
                ['requires_missing_market_group' => true],
                ['market_group_keywords' => ['mission']],
            ],
        ],
    ];
}

function item_scope_catalog(): array
{
    static $catalog = null;

    if ($catalog !== null) {
        return $catalog;
    }

    try {
        $catalog = db_ref_item_scope_catalog();
    } catch (Throwable) {
        $catalog = [
            'categories' => [],
            'groups' => [],
            'market_groups' => [],
            'meta_groups' => [],
        ];
    }

    $metaGroups = [];
    foreach (item_scope_default_meta_groups() as $row) {
        $metaGroups[(int) $row['meta_group_id']] = $row;
    }
    foreach ((array) ($catalog['meta_groups'] ?? []) as $row) {
        $metaGroupId = (int) ($row['meta_group_id'] ?? 0);
        if ($metaGroupId <= 0) {
            continue;
        }
        $metaGroups[$metaGroupId] = [
            'meta_group_id' => $metaGroupId,
            'meta_group_name' => (string) ($row['meta_group_name'] ?? ($metaGroups[$metaGroupId]['meta_group_name'] ?? ('Meta Group #' . $metaGroupId))),
            'type_count' => (int) ($row['type_count'] ?? 0),
        ];
    }

    $metaGroups = array_intersect_key($metaGroups, array_flip([1, 2, 4, 5, 6]));
    ksort($metaGroups);
    $catalog['meta_groups'] = array_values($metaGroups);

    return $catalog;
}

function item_scope_market_group_parent_index(?array $catalog = null): array
{
    $catalog ??= item_scope_catalog();
    $index = [];

    foreach ((array) ($catalog['market_groups'] ?? []) as $row) {
        $marketGroupId = (int) ($row['market_group_id'] ?? 0);
        if ($marketGroupId <= 0) {
            continue;
        }

        $index[$marketGroupId] = (int) ($row['parent_group_id'] ?? 0);
    }

    return $index;
}

function item_scope_market_group_name_index(?array $catalog = null): array
{
    $catalog ??= item_scope_catalog();
    $index = [];

    foreach ((array) ($catalog['market_groups'] ?? []) as $row) {
        $marketGroupId = (int) ($row['market_group_id'] ?? 0);
        if ($marketGroupId <= 0) {
            continue;
        }

        $index[$marketGroupId] = (string) ($row['market_group_name'] ?? '');
    }

    return $index;
}

function item_scope_expand_market_group_ids(?int $marketGroupId, array $parentIndex): array
{
    $resolved = [];
    $current = $marketGroupId !== null ? (int) $marketGroupId : 0;

    while ($current > 0 && !isset($resolved[$current])) {
        $resolved[$current] = true;
        $current = (int) ($parentIndex[$current] ?? 0);
    }

    return array_keys($resolved);
}

function item_scope_metadata_by_type_ids(array $typeIds): array
{
    $typeIds = array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $typeId): bool => $typeId > 0)));
    if ($typeIds === []) {
        return [];
    }

    static $cache = [];
    $missing = [];
    foreach ($typeIds as $typeId) {
        if (!array_key_exists($typeId, $cache)) {
            $missing[] = $typeId;
        }
    }

    if ($missing !== []) {
        $parentIndex = item_scope_market_group_parent_index();
        $marketGroupNameIndex = item_scope_market_group_name_index();
        try {
            $rows = db_ref_item_scope_metadata_by_ids($missing);
        } catch (Throwable) {
            $rows = [];
        }

        foreach ($missing as $typeId) {
            $cache[$typeId] = null;
        }

        foreach ($rows as $row) {
            $typeId = (int) ($row['type_id'] ?? 0);
            if ($typeId <= 0) {
                continue;
            }

            $marketGroupPathIds = item_scope_expand_market_group_ids(isset($row['market_group_id']) ? (int) $row['market_group_id'] : null, $parentIndex);
            $marketGroupPathNames = array_values(array_filter(array_map(
                static fn (int $marketGroupId): string => trim((string) ($marketGroupNameIndex[$marketGroupId] ?? '')),
                $marketGroupPathIds
            ), static fn (string $name): bool => $name !== ''));

            $cache[$typeId] = [
                'type_id' => $typeId,
                'type_name' => (string) ($row['type_name'] ?? ''),
                'category_id' => isset($row['category_id']) ? (int) $row['category_id'] : null,
                'category_name' => (string) ($row['category_name'] ?? ''),
                'group_id' => isset($row['group_id']) ? (int) $row['group_id'] : null,
                'group_name' => (string) ($row['group_name'] ?? ''),
                'market_group_id' => isset($row['market_group_id']) ? (int) $row['market_group_id'] : null,
                'market_group_name' => (string) ($row['market_group_name'] ?? ''),
                'meta_group_id' => isset($row['meta_group_id']) ? (int) $row['meta_group_id'] : null,
                'meta_group_name' => (string) ($row['meta_group_name'] ?? ''),
                'published' => (int) ($row['published'] ?? 0) === 1,
                'market_group_path_ids' => $marketGroupPathIds,
                'market_group_path_names' => $marketGroupPathNames,
            ];
        }
    }

    $resolved = [];
    foreach ($typeIds as $typeId) {
        if (isset($cache[$typeId]) && is_array($cache[$typeId])) {
            $resolved[$typeId] = $cache[$typeId];
        }
    }

    return $resolved;
}

function item_scope_normalize_token(string $value): string
{
    return trim(mb_strtolower($value));
}

function item_scope_text_contains_keywords(array $texts, array $keywords): bool
{
    $keywords = array_values(array_filter(array_map(static fn (string $keyword): string => item_scope_normalize_token($keyword), $keywords), static fn (string $keyword): bool => $keyword !== ''));
    if ($keywords === []) {
        return true;
    }

    foreach ($texts as $text) {
        $normalized = item_scope_normalize_token((string) $text);
        if ($normalized === '') {
            continue;
        }

        foreach ($keywords as $keyword) {
            if (str_contains($normalized, $keyword)) {
                return true;
            }
        }
    }

    return false;
}

function item_scope_metadata_matches_clause(array $metadata, array $clause): bool
{
    $categoryNames = array_map(static fn (string $name): string => item_scope_normalize_token($name), (array) ($clause['category_names'] ?? []));
    $categoryName = item_scope_normalize_token((string) ($metadata['category_name'] ?? ''));
    if ($categoryNames !== [] && !in_array($categoryName, $categoryNames, true)) {
        return false;
    }

    if (($clause['requires_missing_market_group'] ?? false) === true && ($metadata['market_group_id'] ?? null) !== null) {
        return false;
    }

    $groupTexts = [(string) ($metadata['group_name'] ?? '')];
    $marketTexts = array_merge([(string) ($metadata['market_group_name'] ?? '')], (array) ($metadata['market_group_path_names'] ?? []));
    $typeTexts = [(string) ($metadata['type_name'] ?? '')];

    if (!item_scope_text_contains_keywords($groupTexts, (array) ($clause['group_keywords'] ?? []))) {
        return false;
    }
    if (!item_scope_text_contains_keywords($marketTexts, (array) ($clause['market_group_keywords'] ?? []))) {
        return false;
    }
    if (!item_scope_text_contains_keywords($typeTexts, (array) ($clause['type_name_keywords'] ?? []))) {
        return false;
    }

    if (($clause['exclude_group_keywords'] ?? []) !== [] && item_scope_text_contains_keywords($groupTexts, (array) $clause['exclude_group_keywords'])) {
        return false;
    }
    if (($clause['exclude_market_group_keywords'] ?? []) !== [] && item_scope_text_contains_keywords($marketTexts, (array) $clause['exclude_market_group_keywords'])) {
        return false;
    }
    if (($clause['exclude_type_name_keywords'] ?? []) !== [] && item_scope_text_contains_keywords($typeTexts, (array) $clause['exclude_type_name_keywords'])) {
        return false;
    }

    return true;
}

function item_scope_metadata_matches_definition(array $metadata, array $definition): bool
{
    foreach ((array) ($definition['clauses'] ?? []) as $clause) {
        if (item_scope_metadata_matches_clause($metadata, (array) $clause)) {
            return true;
        }
    }

    return false;
}

function item_scope_matches_operational_category(array $metadata, string $key): bool
{
    $definitions = item_scope_operational_definitions();
    $definition = $definitions[$key] ?? null;

    return is_array($definition) ? item_scope_metadata_matches_definition($metadata, $definition) : false;
}

function item_scope_matches_noise_filter(array $metadata, string $key): bool
{
    $definitions = item_scope_noise_filter_definitions();
    $definition = $definitions[$key] ?? null;

    return is_array($definition) ? item_scope_metadata_matches_definition($metadata, $definition) : false;
}

function item_scope_has_advanced_includes(array $config): bool
{
    return $config['include_category_ids'] !== []
        || $config['include_group_ids'] !== []
        || $config['include_market_group_ids'] !== []
        || $config['include_meta_group_ids'] !== [];
}

function item_scope_market_group_matches(array $metadata, array $selectedIds): bool
{
    if ($selectedIds === []) {
        return false;
    }

    $pathIds = item_scope_decode_int_list($metadata['market_group_path_ids'] ?? []);
    foreach ($pathIds as $marketGroupId) {
        if (in_array($marketGroupId, $selectedIds, true)) {
            return true;
        }
    }

    return false;
}

function item_scope_metadata_matches(array $metadata, array $config, string $prefix): bool
{
    return ($metadata['category_id'] !== null && in_array((int) $metadata['category_id'], $config[$prefix . '_category_ids'], true))
        || ($metadata['group_id'] !== null && in_array((int) $metadata['group_id'], $config[$prefix . '_group_ids'], true))
        || item_scope_market_group_matches($metadata, $config[$prefix . '_market_group_ids'])
        || ($metadata['meta_group_id'] !== null && in_array((int) $metadata['meta_group_id'], $config[$prefix . '_meta_group_ids'], true));
}

function item_scope_has_meta_group_catalog_data(?array $catalog = null): bool
{
    $catalog ??= item_scope_catalog();

    foreach ((array) ($catalog['meta_groups'] ?? []) as $row) {
        if ((int) ($row['type_count'] ?? 0) > 0) {
            return true;
        }
    }

    return false;
}

function item_scope_type_metadata_in_scope(array $metadata, ?array $config = null): bool
{
    $config ??= item_scope_config();
    $typeId = (int) ($metadata['type_id'] ?? 0);
    if ($typeId <= 0 || !((bool) ($metadata['published'] ?? false))) {
        return false;
    }

    if (in_array($typeId, $config['include_type_ids'], true)) {
        return true;
    }

    if (in_array($typeId, $config['exclude_type_ids'], true)) {
        return false;
    }

    foreach ($config['noise_filter_keys'] as $noiseKey) {
        if (item_scope_matches_noise_filter($metadata, $noiseKey)) {
            return false;
        }
    }

    $tierFilterActive = $config['tier_meta_group_ids'] !== [] && item_scope_has_meta_group_catalog_data();
    if ($tierFilterActive) {
        $metaGroupId = isset($metadata['meta_group_id']) ? (int) $metadata['meta_group_id'] : null;
        if ($metaGroupId === null || !in_array($metaGroupId, $config['tier_meta_group_ids'], true)) {
            return false;
        }
    }

    $operationalMatch = false;
    foreach ($config['operational_category_keys'] as $categoryKey) {
        if (item_scope_matches_operational_category($metadata, (string) $categoryKey)) {
            $operationalMatch = true;
            break;
        }
    }

    $advancedIncludeMatch = item_scope_metadata_matches($metadata, $config, 'include');
    if ($config['mode'] === 'allow_list' && !$operationalMatch && !$advancedIncludeMatch) {
        return false;
    }

    if (item_scope_metadata_matches($metadata, $config, 'exclude')) {
        return false;
    }

    return true;
}

function item_scope_is_type_id_in_scope(int $typeId, ?array $config = null, ?array $metadataByType = null): bool
{
    if ($typeId <= 0) {
        return false;
    }

    $metadataByType ??= item_scope_metadata_by_type_ids([$typeId]);
    $metadata = $metadataByType[$typeId] ?? null;
    if (!is_array($metadata)) {
        return false;
    }

    return item_scope_type_metadata_in_scope($metadata, $config);
}

function item_scope_type_metadata(int $typeId, ?array $metadataByType = null): array
{
    if ($typeId <= 0) {
        return [];
    }

    $metadataByType ??= item_scope_metadata_by_type_ids([$typeId]);

    return is_array($metadataByType[$typeId] ?? null) ? (array) $metadataByType[$typeId] : [];
}

function item_scope_type_is_consumable(int $typeId, ?array $metadataByType = null): bool
{
    if ($typeId <= 0) {
        return false;
    }

    $metadata = item_scope_type_metadata($typeId, $metadataByType);
    if ($metadata === []) {
        return false;
    }

    $categoryName = item_scope_normalize_token((string) ($metadata['category_name'] ?? ''));
    if ($categoryName === 'charge') {
        return true;
    }

    return item_scope_matches_operational_category($metadata, 'boosters');
}

function item_scope_type_is_durable_loss_relevant(int $typeId, ?array $config = null, ?array $metadataByType = null): bool
{
    if ($typeId <= 0) {
        return false;
    }

    if (!item_scope_is_type_id_in_scope($typeId, $config, $metadataByType)) {
        return false;
    }

    return !item_scope_type_is_consumable($typeId, $metadataByType);
}

function item_scope_filter_rows(array $rows, callable $typeIdResolver, ?array $config = null): array
{
    $config ??= item_scope_config();
    $typeIds = [];
    foreach ($rows as $row) {
        $typeId = (int) $typeIdResolver($row);
        if ($typeId > 0) {
            $typeIds[] = $typeId;
        }
    }

    $metadataByType = item_scope_metadata_by_type_ids($typeIds);

    return array_values(array_filter($rows, static function (array $row) use ($typeIdResolver, $config, $metadataByType): bool {
        $typeId = (int) $typeIdResolver($row);

        return $typeId > 0 && item_scope_is_type_id_in_scope($typeId, $config, $metadataByType);
    }));
}

function item_scope_parse_override_input(string $input): array
{
    $tokens = array_values(array_filter(array_map(static fn (string $value): string => trim($value), preg_split('/[\r\n,]+/', $input) ?: []), static fn (string $value): bool => $value !== ''));
    if ($tokens === []) {
        return ['type_ids' => [], 'unresolved' => []];
    }

    $typeIds = [];
    $names = [];
    foreach ($tokens as $token) {
        if (preg_match('/^[1-9][0-9]*$/', $token) === 1) {
            $typeIds[] = (int) $token;
        } else {
            $names[] = $token;
        }
    }

    $resolvedRows = [];
    try {
        if ($typeIds !== []) {
            $resolvedRows = array_merge($resolvedRows, db_ref_item_scope_metadata_by_ids($typeIds));
        }
        if ($names !== []) {
            $resolvedRows = array_merge($resolvedRows, db_ref_item_types_by_names($names));
        }
    } catch (Throwable) {
    }

    $resolvedTypeIds = [];
    $resolvedNames = [];
    foreach ($resolvedRows as $row) {
        $typeId = (int) ($row['type_id'] ?? 0);
        if ($typeId <= 0) {
            continue;
        }
        $resolvedTypeIds[] = $typeId;
        $resolvedNames[mb_strtolower((string) ($row['type_name'] ?? ''))] = true;
    }

    $unresolved = [];
    foreach ($tokens as $token) {
        if (preg_match('/^[1-9][0-9]*$/', $token) === 1) {
            if (!in_array((int) $token, $resolvedTypeIds, true)) {
                $unresolved[] = $token;
            }
            continue;
        }

        if (!isset($resolvedNames[mb_strtolower($token)])) {
            $unresolved[] = $token;
        }
    }

    return [
        'type_ids' => array_values(array_unique(array_filter(array_map('intval', $resolvedTypeIds), static fn (int $typeId): bool => $typeId > 0))),
        'unresolved' => $unresolved,
    ];
}

function item_scope_settings_payload_from_request(array $request): array
{
    $includeOverrides = item_scope_parse_override_input((string) ($request['item_scope_include_overrides'] ?? ''));
    $excludeOverrides = item_scope_parse_override_input((string) ($request['item_scope_exclude_overrides'] ?? ''));

    return [
        'settings' => [
            'item_scope_mode' => item_scope_normalize_mode((string) ($request['item_scope_mode'] ?? 'allow_list')),
            'item_scope_operational_category_keys' => item_scope_encode_string_list((array) ($request['item_scope_operational_category_keys'] ?? [])),
            'item_scope_tier_meta_group_ids' => item_scope_encode_int_list((array) ($request['item_scope_tier_meta_group_ids'] ?? [])),
            'item_scope_noise_filter_keys' => item_scope_encode_string_list((array) ($request['item_scope_noise_filter_keys'] ?? [])),
            'item_scope_include_category_ids' => item_scope_encode_int_list((array) ($request['item_scope_include_category_ids'] ?? [])),
            'item_scope_exclude_category_ids' => item_scope_encode_int_list((array) ($request['item_scope_exclude_category_ids'] ?? [])),
            'item_scope_include_group_ids' => item_scope_encode_int_list((array) ($request['item_scope_include_group_ids'] ?? [])),
            'item_scope_exclude_group_ids' => item_scope_encode_int_list((array) ($request['item_scope_exclude_group_ids'] ?? [])),
            'item_scope_include_market_group_ids' => item_scope_encode_int_list((array) ($request['item_scope_include_market_group_ids'] ?? [])),
            'item_scope_exclude_market_group_ids' => item_scope_encode_int_list((array) ($request['item_scope_exclude_market_group_ids'] ?? [])),
            'item_scope_include_meta_group_ids' => item_scope_encode_int_list((array) ($request['item_scope_include_meta_group_ids'] ?? [])),
            'item_scope_exclude_meta_group_ids' => item_scope_encode_int_list((array) ($request['item_scope_exclude_meta_group_ids'] ?? [])),
            'item_scope_include_type_ids' => item_scope_encode_int_list($includeOverrides['type_ids']),
            'item_scope_exclude_type_ids' => item_scope_encode_int_list($excludeOverrides['type_ids']),
        ],
        'messages' => array_values(array_filter([
            $includeOverrides['unresolved'] !== [] ? ('Include overrides not resolved locally: ' . implode(', ', $includeOverrides['unresolved']) . '.') : null,
            $excludeOverrides['unresolved'] !== [] ? ('Exclude overrides not resolved locally: ' . implode(', ', $excludeOverrides['unresolved']) . '.') : null,
        ])),
    ];
}

function item_scope_summary_lines(array $config, ?array $catalog = null): array
{
    $catalog ??= item_scope_catalog();
    $operationalDefinitions = item_scope_operational_definitions();
    $noiseDefinitions = item_scope_noise_filter_definitions();
    $tierLabels = array_column((array) ($catalog['meta_groups'] ?? []), 'meta_group_name', 'meta_group_id');

    $operationalLabels = [];
    foreach ($config['operational_category_keys'] as $key) {
        if (isset($operationalDefinitions[$key])) {
            $operationalLabels[] = (string) $operationalDefinitions[$key]['label'];
        }
    }

    $noiseLabels = [];
    foreach ($config['noise_filter_keys'] as $key) {
        if (isset($noiseDefinitions[$key])) {
            $noiseLabels[] = (string) $noiseDefinitions[$key]['label'];
        }
    }

    $tierNames = array_map(static fn (int $id): string => (string) ($tierLabels[$id] ?? ('Meta Group #' . $id)), $config['tier_meta_group_ids']);
    $lines = [
        $config['mode'] === 'allow_list'
            ? 'Operational allow-list mode is active. Items must match a selected operational category or advanced include before they flow into alliance analytics.'
            : 'Allow-all mode is active. Published items stay visible unless blocked by the shared noise filters, tier toggles, or advanced exclusions.',
        $operationalLabels !== []
            ? 'Operational categories: ' . implode(', ', $operationalLabels) . '.'
            : 'Operational categories: none selected.',
        $tierNames !== []
            ? 'Meta tiers enabled: ' . implode(', ', $tierNames) . '.'
            : 'Meta tiers enabled: all tiers.',
        $noiseLabels !== []
            ? 'Noise filters enabled: ' . implode(', ', $noiseLabels) . '.'
            : 'Noise filters enabled: none.',
    ];

    if ($config['include_group_ids'] !== [] || $config['include_market_group_ids'] !== []) {
        $lines[] = 'Advanced includes are active for specific groups or market groups.';
    }
    if ($config['exclude_group_ids'] !== [] || $config['exclude_market_group_ids'] !== []) {
        $lines[] = 'Advanced excludes are active for specific groups or market groups.';
    }
    if ($config['include_type_ids'] !== []) {
        $lines[] = 'Explicit item includes: ' . number_format(count($config['include_type_ids'])) . '.';
    }
    if ($config['exclude_type_ids'] !== []) {
        $lines[] = 'Explicit item excludes: ' . number_format(count($config['exclude_type_ids'])) . '.';
    }

    return $lines;
}

function item_scope_view_model(): array
{
    $config = item_scope_config();
    $catalog = item_scope_catalog();
    $operationalDefinitions = item_scope_operational_definitions();
    $noiseDefinitions = item_scope_noise_filter_definitions();

    $includeOverrides = item_scope_metadata_by_type_ids($config['include_type_ids']);
    $excludeOverrides = item_scope_metadata_by_type_ids($config['exclude_type_ids']);
    $publishedCount = 0;
    $inScopeCount = 0;
    $metadataByType = [];

    try {
        $publishedTypeIds = db_ref_item_type_ids_published();
        $publishedCount = count($publishedTypeIds);
        $metadataByType = item_scope_metadata_by_type_ids($publishedTypeIds);
        foreach ($publishedTypeIds as $typeId) {
            if (item_scope_is_type_id_in_scope($typeId, $config, $metadataByType)) {
                $inScopeCount++;
            }
        }
    } catch (Throwable) {
        $publishedCount = 0;
        $inScopeCount = 0;
        $metadataByType = [];
    }

    $operationalRows = [];
    foreach ($operationalDefinitions as $key => $definition) {
        $matchedCount = 0;
        foreach ($metadataByType as $metadata) {
            if (item_scope_matches_operational_category($metadata, $key)) {
                $matchedCount++;
            }
        }

        $operationalRows[] = [
            'key' => $key,
            'label' => (string) ($definition['label'] ?? $key),
            'description' => (string) ($definition['description'] ?? ''),
            'selected' => in_array($key, $config['operational_category_keys'], true),
            'default' => (bool) ($definition['default'] ?? false),
            'type_count' => $matchedCount,
        ];
    }

    $tierRows = [];
    $tierCountById = array_column((array) ($catalog['meta_groups'] ?? []), 'type_count', 'meta_group_id');
    foreach ((array) ($catalog['meta_groups'] ?? []) as $row) {
        $metaGroupId = (int) ($row['meta_group_id'] ?? 0);
        if ($metaGroupId <= 0) {
            continue;
        }

        $tierRows[] = [
            'meta_group_id' => $metaGroupId,
            'meta_group_name' => (string) ($row['meta_group_name'] ?? ('Meta Group #' . $metaGroupId)),
            'selected' => in_array($metaGroupId, $config['tier_meta_group_ids'], true),
            'type_count' => (int) ($tierCountById[$metaGroupId] ?? 0),
        ];
    }

    $noiseRows = [];
    foreach ($noiseDefinitions as $key => $definition) {
        $matchedCount = 0;
        foreach ($metadataByType as $metadata) {
            if (item_scope_matches_noise_filter($metadata, $key)) {
                $matchedCount++;
            }
        }

        $noiseRows[] = [
            'key' => $key,
            'label' => (string) ($definition['label'] ?? $key),
            'description' => (string) ($definition['description'] ?? ''),
            'selected' => in_array($key, $config['noise_filter_keys'], true),
            'default' => (bool) ($definition['default'] ?? false),
            'type_count' => $matchedCount,
        ];
    }

    return [
        'config' => $config,
        'catalog' => $catalog,
        'summary_lines' => item_scope_summary_lines($config, $catalog),
        'stats' => [
            'published_count' => $publishedCount,
            'in_scope_count' => $inScopeCount,
            'excluded_count' => max(0, $publishedCount - $inScopeCount),
        ],
        'operational_rows' => $operationalRows,
        'tier_rows' => $tierRows,
        'noise_rows' => $noiseRows,
        'override_rows' => [
            'include' => array_values($includeOverrides),
            'exclude' => array_values($excludeOverrides),
        ],
    ];
}

function supplycore_cache_ttl(string $category): int
{
    return match ($category) {
        'dashboard' => 120,
        'market_summary' => 180,
        'market_listing' => 90,
        'killmail_summary' => 120,
        'killmail_detail' => 600,
        'doctrine_summary' => 180,
        'doctrine_detail' => 300,
        'metadata_dynamic' => 86400,
        'metadata_static' => 604800,
        'structure_metadata' => 3600,
        default => 300,
    };
}

function supplycore_analytics_cache_ttl(): int
{
    return function_exists('db_time_series_cache_ttl_seconds')
        ? db_time_series_cache_ttl_seconds()
        : max(60, min(3600, (int) get_setting('analytics_bucket_cache_ttl_seconds', '300')));
}

function supplycore_activity_doctrine_cache_key(): string
{
    return 'activity:doctrine';
}

function supplycore_activity_items_cache_key(): string
{
    return 'activity:items';
}

function supplycore_dashboard_top_cache_key(): string
{
    return 'dashboard:top';
}

function supplycore_cached_json_read(string $key): ?array
{
    if (!supplycore_redis_enabled()) {
        return null;
    }

    return supplycore_redis_get_json($key);
}

function supplycore_cached_json_write(string $key, array $payload, ?int $ttlSeconds = null): array
{
    if (supplycore_redis_enabled()) {
        supplycore_redis_set_json($key, $payload, $ttlSeconds ?? supplycore_analytics_cache_ttl());
    }

    return $payload;
}

function supplycore_cache_version(string $namespace): int
{
    if (!supplycore_redis_enabled()) {
        return 1;
    }

    $safeNamespace = preg_replace('/[^a-z0-9:_-]+/i', '-', strtolower(trim($namespace))) ?: 'default';
    $value = supplycore_redis_get('cache-version:' . $safeNamespace);
    $version = (int) $value;

    return $version > 0 ? $version : 1;
}

function supplycore_cache_key(string $namespace, array $parts = [], array $dependencies = []): string
{
    $safeNamespace = preg_replace('/[^a-z0-9:_-]+/i', '-', strtolower(trim($namespace))) ?: 'default';
    $resolvedDependencies = array_values(array_unique(array_merge([$safeNamespace], array_map(
        static fn (mixed $dependency): string => preg_replace('/[^a-z0-9:_-]+/i', '-', strtolower(trim((string) $dependency))) ?: 'default',
        $dependencies
    ))));

    $versions = [];
    foreach ($resolvedDependencies as $dependency) {
        $versions[$dependency] = supplycore_cache_version($dependency);
    }

    return sprintf(
        'cache:%s:%s:%s',
        $safeNamespace,
        http_build_query($versions, '', ','),
        sha1(json_encode($parts, JSON_THROW_ON_ERROR))
    );
}

function supplycore_cache_read(string $namespace, array $parts = [], array $dependencies = []): mixed
{
    if (!supplycore_redis_enabled()) {
        return null;
    }

    $payload = supplycore_redis_get(supplycore_cache_key($namespace, $parts, $dependencies));
    if ($payload === null || $payload === '') {
        return null;
    }

    try {
        $decoded = json_decode($payload, true, 512, JSON_THROW_ON_ERROR);
    } catch (Throwable) {
        return null;
    }

    return $decoded['value'] ?? null;
}

function supplycore_cache_write(string $namespace, array $parts, mixed $value, int $ttlSeconds, array $dependencies = []): mixed
{
    if (!supplycore_redis_enabled()) {
        return $value;
    }

    try {
        $encoded = json_encode([
            'stored_at' => gmdate(DATE_ATOM),
            'value' => $value,
        ], JSON_THROW_ON_ERROR);
    } catch (Throwable) {
        return $value;
    }

    supplycore_redis_set(
        supplycore_cache_key($namespace, $parts, $dependencies),
        $encoded,
        max(1, $ttlSeconds)
    );

    return $value;
}

function supplycore_cache_bust(array|string $namespaces): void
{
    $list = is_array($namespaces) ? $namespaces : [$namespaces];

    foreach ($list as $namespace) {
        $safeNamespace = preg_replace('/[^a-z0-9:_-]+/i', '-', strtolower(trim((string) $namespace))) ?: '';
        if ($safeNamespace === '') {
            continue;
        }

        supplycore_redis_incr('cache-version:' . $safeNamespace);
    }
}

function supplycore_cache_lock_name(string $namespace, array $parts = []): string
{
    $safeNamespace = preg_replace('/[^a-z0-9:_-]+/i', '-', strtolower(trim($namespace))) ?: 'default';

    return 'cache-build:' . $safeNamespace . ':' . sha1(json_encode($parts, JSON_THROW_ON_ERROR));
}

function supplycore_cache_aside(string $namespace, array $parts, int $ttlSeconds, callable $resolver, array $options = []): mixed
{
    $dependencies = array_values(array_unique(array_filter(array_map(
        static fn (mixed $value): string => trim((string) $value),
        (array) ($options['dependencies'] ?? [])
    ), static fn (string $value): bool => $value !== '')));

    $cached = supplycore_cache_read($namespace, $parts, $dependencies);
    if ($cached !== null) {
        return $cached;
    }

    $lockSeconds = max(1, (int) ($options['lock_ttl'] ?? 15));
    $lockName = supplycore_cache_lock_name($namespace, $parts);
    $lockToken = supplycore_redis_lock_acquire($lockName, $lockSeconds);

    if ($lockToken === null && supplycore_redis_locking_enabled()) {
        $deadline = microtime(true) + 2.5;
        do {
            usleep(100000);
            $cached = supplycore_cache_read($namespace, $parts, $dependencies);
            if ($cached !== null) {
                return $cached;
            }
        } while (microtime(true) < $deadline);
    }

    try {
        $value = $resolver();
    } finally {
        if ($lockToken !== null) {
            supplycore_redis_lock_release($lockName, $lockToken);
        }
    }

    return supplycore_cache_write($namespace, $parts, $value, $ttlSeconds, $dependencies);
}

function supplycore_cache_invalidate_for_dataset(string $datasetKey): void
{
    $normalized = trim($datasetKey);
    if ($normalized === '') {
        return;
    }

    if (str_starts_with($normalized, 'alliance.structure.') || str_starts_with($normalized, 'market.hub.')) {
        supplycore_cache_bust(['dashboard', 'market_compare', 'doctrine']);

        return;
    }

    if (str_starts_with($normalized, 'killmail.r2z2')) {
        supplycore_cache_bust(['dashboard', 'killmail_overview', 'killmail_detail']);

        return;
    }

    if (str_starts_with($normalized, 'static_data.')) {
        supplycore_cache_bust(['dashboard', 'market_compare', 'doctrine', 'killmail_overview', 'killmail_detail', 'metadata_entities', 'metadata_structures']);
    }
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

    $updated = db_alliance_structure_metadata_upsert(
        $stationId,
        $resolved['name'] ?? null,
        gmdate('Y-m-d H:i:s')
    );
    if ($updated) {
        supplycore_cache_bust('metadata_structures');
    }

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

    $updated = db_alliance_structure_metadata_upsert(
        $structureId,
        $metadata['name'] ?? null,
        gmdate('Y-m-d H:i:s')
    );
    if ($updated) {
        supplycore_cache_bust('metadata_structures');
    }

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

    return supplycore_cache_aside('metadata_structures', [$settingKey, $stationType, $stationId], supplycore_cache_ttl('structure_metadata'), static function () use ($stationId, $stationType): ?string {
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
    }, [
        'dependencies' => ['metadata_structures'],
        'lock_ttl' => 10,
    ]);
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
        'redis_cache_enabled' => config('redis.enabled', false) ? '1' : '0',
        'redis_locking_enabled' => config('redis.lock_enabled', true) ? '1' : '0',
        'redis_host' => (string) config('redis.host', '127.0.0.1'),
        'redis_port' => (string) config('redis.port', 6379),
        'redis_database' => (string) config('redis.database', 0),
        'redis_password' => (string) config('redis.password', ''),
        'redis_prefix' => (string) config('redis.prefix', 'supplycore'),
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
        'market_hub_local_history_pipeline_enabled',
        'redis_cache_enabled',
        'redis_locking_enabled' => sanitize_pipeline_enabled($value),
        'incremental_strategy' => sanitize_incremental_strategy((string) $value),
        'incremental_delete_policy' => sanitize_incremental_delete_policy((string) $value),
        'incremental_chunk_size' => sanitize_incremental_chunk_size($value),
        'raw_order_snapshot_retention_days' => sanitize_retention_days($value),
        'static_data_source_url' => sanitize_static_data_source_url($value),
        'redis_host' => sanitize_redis_host((string) $value),
        'redis_port' => sanitize_redis_port($value),
        'redis_database' => sanitize_redis_database($value),
        'redis_prefix' => sanitize_redis_prefix((string) $value),
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

function scheduler_reset_process_targets(): array
{
    $paths = [
        scheduler_job_runner_script_path(),
        dirname(__DIR__) . '/bin/cron_tick.php',
    ];

    return array_values(array_unique(array_filter(array_map(static function (string $path): string {
        $resolved = realpath($path);

        return $resolved !== false ? $resolved : $path;
    }, $paths), static fn (string $path): bool => $path !== '')));
}

function scheduler_find_running_processes(): array
{
    if (!function_exists('exec')) {
        return [];
    }

    $targets = scheduler_reset_process_targets();
    if ($targets === []) {
        return [];
    }

    $output = [];
    $exitCode = 1;
    @exec('ps -eo pid=,command=', $output, $exitCode);
    if ($exitCode !== 0) {
        return [];
    }

    $currentPid = function_exists('getmypid') ? (int) getmypid() : 0;
    $processes = [];

    foreach ($output as $line) {
        $trimmed = trim((string) $line);
        if ($trimmed === '') {
            continue;
        }

        if (!preg_match('/^(\d+)\s+(.*)$/', $trimmed, $matches)) {
            continue;
        }

        $pid = (int) ($matches[1] ?? 0);
        $command = trim((string) ($matches[2] ?? ''));
        if ($pid <= 0 || $command === '' || $pid === $currentPid) {
            continue;
        }

        foreach ($targets as $target) {
            if (!str_contains($command, $target)) {
                continue;
            }

            $processes[$pid] = [
                'pid' => $pid,
                'command' => $command,
            ];
            break;
        }
    }

    ksort($processes);

    return array_values($processes);
}

function scheduler_signal_processes(array $pids, int $signal): void
{
    if ($pids === []) {
        return;
    }

    $safeSignal = max(1, $signal);
    $safePids = array_values(array_filter(array_map(static fn (mixed $pid): int => (int) $pid, $pids), static fn (int $pid): bool => $pid > 0));
    if ($safePids === []) {
        return;
    }

    if (function_exists('posix_kill')) {
        foreach ($safePids as $pid) {
            @posix_kill($pid, $safeSignal);
        }

        return;
    }

    if (!function_exists('exec')) {
        return;
    }

    @exec(sprintf(
        'kill -%d %s >/dev/null 2>&1',
        $safeSignal,
        implode(' ', array_map('intval', $safePids))
    ));
}

function scheduler_reset_runtime_state(): array
{
    $message = 'Scheduler locks were reset manually from Settings.';
    $processes = scheduler_find_running_processes();
    $pids = array_values(array_map(static fn (array $process): int => (int) ($process['pid'] ?? 0), $processes));

    scheduler_signal_processes($pids, 15);
    usleep(250000);

    $remainingProcesses = scheduler_find_running_processes();
    $remainingPids = array_values(array_map(static fn (array $process): int => (int) ($process['pid'] ?? 0), $remainingProcesses));
    if ($remainingPids !== []) {
        scheduler_signal_processes($remainingPids, 9);
        usleep(250000);
    }

    $appLockReleased = db_runner_lock_force_release('supplycore:cron_tick');
    $runnerLockReleased = db_runner_lock_force_release('cron_tick_runner');

    if (supplycore_redis_locking_enabled()) {
        supplycore_redis_del(['lock:runner:cron_tick_runner']);
    }

    $resetSchedules = db_sync_schedule_reset_locks($message);
    $finalProcesses = scheduler_find_running_processes();

    $releasedLocks = [];
    if ($appLockReleased) {
        $releasedLocks[] = 'supplycore:cron_tick';
    }
    if ($runnerLockReleased) {
        $releasedLocks[] = 'cron_tick_runner';
    }

    return [
        'ok' => $finalProcesses === [],
        'killed_processes' => $processes,
        'remaining_processes' => $finalProcesses,
        'released_locks' => $releasedLocks,
        'reset_schedule_count' => $resetSchedules,
    ];
}

function scheduler_reset_runtime_state_message(array $result): string
{
    $killedCount = count((array) ($result['killed_processes'] ?? []));
    $remainingCount = count((array) ($result['remaining_processes'] ?? []));
    $lockCount = count((array) ($result['released_locks'] ?? []));
    $scheduleCount = (int) ($result['reset_schedule_count'] ?? 0);

    $message = 'Scheduler reset completed. Cleared ' . $scheduleCount . ' locked schedule(s), released ' . $lockCount . ' runner lock(s), and targeted ' . $killedCount . ' PHP scheduler process(es).';
    if ($remainingCount > 0) {
        $message .= ' Warning: ' . $remainingCount . ' scheduler process(es) still appear to be running and may need manual review.';
    }

    return $message;
}

function data_sync_schedule_job_definitions(): array
{
    return [
        'alliance_current_sync' => [
            'enabled_key' => 'alliance_current_sync_enabled',
            'interval_value_key' => 'alliance_current_sync_interval_value',
            'interval_unit_key' => 'alliance_current_sync_interval_unit',
            'default_interval_seconds' => 60,
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
            'default_interval_seconds' => 60,
            'label' => 'Hub Current',
        ],
        'current_state_refresh_sync' => [
            'enabled_key' => 'current_state_refresh_sync_enabled',
            'interval_value_key' => 'current_state_refresh_sync_interval_value',
            'interval_unit_key' => 'current_state_refresh_sync_interval_unit',
            'default_interval_seconds' => 300,
            'label' => 'Current-State Refresh',
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
            'label' => 'Hub Snapshot History',
        ],
        'doctrine_intelligence_sync' => [
            'enabled_key' => 'doctrine_intelligence_sync_enabled',
            'interval_value_key' => 'doctrine_intelligence_sync_interval_value',
            'interval_unit_key' => 'doctrine_intelligence_sync_interval_unit',
            'default_interval_seconds' => 300,
            'label' => 'Doctrine Intelligence Batch',
        ],
        'market_comparison_summary_sync' => [
            'enabled_key' => 'market_comparison_summary_sync_enabled',
            'interval_value_key' => 'market_comparison_summary_sync_interval_value',
            'interval_unit_key' => 'market_comparison_summary_sync_interval_unit',
            'default_interval_seconds' => 300,
            'label' => 'Market Comparison Batch',
        ],
        'loss_demand_summary_sync' => [
            'enabled_key' => 'loss_demand_summary_sync_enabled',
            'interval_value_key' => 'loss_demand_summary_sync_interval_value',
            'interval_unit_key' => 'loss_demand_summary_sync_interval_unit',
            'default_interval_seconds' => 300,
            'label' => 'Loss-Demand Batch',
        ],
        'dashboard_summary_sync' => [
            'enabled_key' => 'dashboard_summary_sync_enabled',
            'interval_value_key' => 'dashboard_summary_sync_interval_value',
            'interval_unit_key' => 'dashboard_summary_sync_interval_unit',
            'default_interval_seconds' => 300,
            'label' => 'Dashboard Summary Batch',
        ],
        'activity_priority_summary_sync' => [
            'enabled_key' => 'activity_priority_summary_sync_enabled',
            'interval_value_key' => 'activity_priority_summary_sync_interval_value',
            'interval_unit_key' => 'activity_priority_summary_sync_interval_unit',
            'default_interval_seconds' => 300,
            'label' => 'Activity Priority Batch',
        ],
        'analytics_bucket_1h_sync' => [
            'enabled_key' => 'analytics_bucket_1h_sync_enabled',
            'interval_value_key' => 'analytics_bucket_1h_sync_interval_value',
            'interval_unit_key' => 'analytics_bucket_1h_sync_interval_unit',
            'default_interval_seconds' => 300,
            'label' => 'Analytics Buckets (1h)',
        ],
        'analytics_bucket_1d_sync' => [
            'enabled_key' => 'analytics_bucket_1d_sync_enabled',
            'interval_value_key' => 'analytics_bucket_1d_sync_interval_value',
            'interval_unit_key' => 'analytics_bucket_1d_sync_interval_unit',
            'default_interval_seconds' => 900,
            'label' => 'Analytics Buckets (1d)',
        ],
        'rebuild_ai_briefings' => [
            'enabled_key' => 'rebuild_ai_briefings_enabled',
            'interval_value_key' => 'rebuild_ai_briefings_interval_value',
            'interval_unit_key' => 'rebuild_ai_briefings_interval_unit',
            'default_interval_seconds' => 300,
            'label' => 'Doctrine AI Briefings',
        ],
        'forecasting_ai_sync' => [
            'enabled_key' => 'forecasting_ai_sync_enabled',
            'interval_value_key' => 'forecasting_ai_sync_interval_value',
            'interval_unit_key' => 'forecasting_ai_sync_interval_unit',
            'default_interval_seconds' => 3600,
            'label' => 'Forecasting / AI Batch',
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

function sanitize_redis_host(?string $value): string
{
    $host = trim((string) $value);

    return $host !== '' ? mb_substr($host, 0, 190) : (string) config('redis.host', '127.0.0.1');
}

function sanitize_redis_prefix(?string $value): string
{
    $prefix = preg_replace('/[^a-z0-9:_-]+/i', '-', trim((string) $value)) ?? '';
    $prefix = trim($prefix, '-:');

    return $prefix !== '' ? mb_substr($prefix, 0, 80) : (string) config('redis.prefix', 'supplycore');
}

function sanitize_redis_port(mixed $value): string
{
    return (string) max(1, min(65535, (int) $value));
}

function sanitize_redis_database(mixed $value): string
{
    return (string) max(0, min(15, (int) $value));
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
        'redis_cache_enabled' => isset($request['redis_cache_enabled']) ? '1' : '0',
        'redis_locking_enabled' => isset($request['redis_locking_enabled']) ? '1' : '0',
        'redis_host' => sanitize_redis_host($request['redis_host'] ?? null),
        'redis_port' => sanitize_redis_port($request['redis_port'] ?? config('redis.port', 6379)),
        'redis_database' => sanitize_redis_database($request['redis_database'] ?? config('redis.database', 0)),
        'redis_password' => trim((string) ($request['redis_password'] ?? '')),
        'redis_prefix' => sanitize_redis_prefix($request['redis_prefix'] ?? null),
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

function market_comparison_snapshot_key(): string
{
    return 'market_comparison_summaries';
}

function loss_demand_snapshot_key(): string
{
    return 'loss_demand_summaries';
}

function dashboard_snapshot_key(): string
{
    return 'dashboard_summaries';
}

function market_comparison_outcomes_build(array $typeIds = []): array
{
    $thresholds = market_compare_thresholds();
    $evaluatedRows = [];

    foreach (market_compare_aggregates($typeIds) as $row) {
        $evaluatedRows[] = market_compare_evaluate_row($row, $thresholds);
    }

    $evaluatedRows = item_scope_filter_rows(
        $evaluatedRows,
        static fn (array $row): int => (int) ($row['type_id'] ?? 0)
    );

    return [
        'thresholds' => $thresholds,
        'rows' => array_values($evaluatedRows),
        'in_both_markets' => array_values(array_filter($evaluatedRows, static fn (array $row): bool => (bool) $row['in_both_markets'])),
        'missing_in_alliance' => array_values(array_filter($evaluatedRows, static fn (array $row): bool => (bool) $row['missing_in_alliance'])),
        'overpriced_in_alliance' => array_values(array_filter($evaluatedRows, static fn (array $row): bool => (bool) $row['overpriced_in_alliance'])),
        'underpriced_in_alliance' => array_values(array_filter($evaluatedRows, static fn (array $row): bool => (bool) $row['underpriced_in_alliance'])),
        'weak_or_missing_alliance_stock' => array_values(array_filter($evaluatedRows, static fn (array $row): bool => (bool) ($row['weak_alliance_stock'] ?? false) || (bool) ($row['missing_in_alliance'] ?? false))),
    ];
}

function market_comparison_snapshot_payload(): array
{
    return supplycore_materialized_snapshot_read_or_bootstrap(
        market_comparison_snapshot_key(),
        static fn (): array => market_comparison_outcomes_build(),
        'market-comparison-bootstrap'
    );
}

function market_comparison_refresh_summary(string $reason = 'manual'): array
{
    supplycore_materialized_snapshot_mark_updating(market_comparison_snapshot_key(), $reason);
    $snapshot = market_comparison_outcomes_build();

    return supplycore_materialized_snapshot_store(market_comparison_snapshot_key(), $snapshot, [
        'reason' => $reason,
        'row_count' => count((array) ($snapshot['rows'] ?? [])),
    ]);
}

function market_comparison_refresh_summary_job_result(string $reason = 'manual'): array
{
    $snapshot = market_comparison_refresh_summary($reason);
    $rowCount = count((array) ($snapshot['rows'] ?? []));
    $freshness = is_array($snapshot['_freshness'] ?? null) ? $snapshot['_freshness'] : [];

    return sync_result_shape() + [
        'rows_seen' => $rowCount,
        'rows_written' => $rowCount,
        'cursor' => 'market_comparison:' . gmdate('Y-m-d H:i:s'),
        'checksum' => sync_checksum([
            'rows' => $rowCount,
            'computed_at' => (string) ($freshness['computed_at'] ?? gmdate(DATE_ATOM)),
            'reason' => $reason,
        ]),
        'meta' => [
            'outcome_reason' => 'Market comparison summaries were recomputed and written to materialized storage plus Redis.',
            'snapshot_generated_at' => (string) ($freshness['computed_at'] ?? ''),
        ],
    ];
}

function loss_demand_summary_build(): array
{
    $marketSnapshot = market_comparison_snapshot_payload();
    $marketRows = array_values((array) ($marketSnapshot['rows'] ?? []));
    $typeLookup = [];

    foreach ($marketRows as $row) {
        $typeId = (int) ($row['type_id'] ?? 0);
        if ($typeId > 0) {
            $typeLookup[$typeId] = $row;
        }
    }

    if ($typeLookup === []) {
        return [
            'rows' => [],
            'summary' => [
                'tracked_type_count' => 0,
                'active_loss_demand_count' => 0,
                'losses_24h' => 0,
                'losses_7d' => 0,
            ],
            'top_rows' => [],
        ];
    }

    try {
        $lossRows = db_killmail_tracked_recent_item_losses(array_keys($typeLookup), 24 * 7);
    } catch (Throwable) {
        $lossRows = [];
    }

    $lossRows = item_scope_filter_rows(
        $lossRows,
        static fn (array $row): int => (int) ($row['type_id'] ?? 0)
    );

    $rows = [];
    foreach ($lossRows as $row) {
        $typeId = (int) ($row['type_id'] ?? 0);
        if ($typeId <= 0) {
            continue;
        }

        $market = $typeLookup[$typeId] ?? [];
        $quantity7d = max(0, (int) ($row['quantity_7d'] ?? 0));
        $losses7d = max(0, (int) ($row['losses_7d'] ?? 0));
        $referencePrice = isset($market['reference_best_sell_price']) ? (float) $market['reference_best_sell_price'] : null;
        $estimatedRestock = $referencePrice !== null ? round($referencePrice * $quantity7d, 2) : 0.0;
        $priorityScore = (int) min(100, round(($quantity7d * 1.2) + ($losses7d * 6.0) + max(0, (int) ($market['volume_score'] ?? 0) * 0.35)));

        $rows[] = [
            'type_id' => $typeId,
            'type_name' => (string) (($market['type_name'] ?? $row['type_name']) ?? ''),
            'quantity_24h' => max(0, (int) ($row['quantity_24h'] ?? 0)),
            'quantity_7d' => $quantity7d,
            'losses_24h' => max(0, (int) ($row['losses_24h'] ?? 0)),
            'losses_7d' => $losses7d,
            'latest_loss_at' => $row['latest_loss_at'] ?? null,
            'reference_best_sell_price' => $referencePrice,
            'estimated_restock_isk' => $estimatedRestock,
            'volume_score' => (int) ($market['volume_score'] ?? 0),
            'priority_score' => $priorityScore,
        ];
    }

    usort($rows, static fn (array $a, array $b): int => ((int) ($b['priority_score'] ?? 0) <=> (int) ($a['priority_score'] ?? 0)) ?: ((int) ($b['quantity_7d'] ?? 0) <=> (int) ($a['quantity_7d'] ?? 0)));

    return [
        'rows' => $rows,
        'summary' => [
            'tracked_type_count' => count($typeLookup),
            'active_loss_demand_count' => count($rows),
            'losses_24h' => array_sum(array_map(static fn (array $row): int => (int) ($row['quantity_24h'] ?? 0), $rows)),
            'losses_7d' => array_sum(array_map(static fn (array $row): int => (int) ($row['quantity_7d'] ?? 0), $rows)),
        ],
        'top_rows' => array_slice($rows, 0, 12),
    ];
}

function loss_demand_snapshot_payload(): array
{
    return supplycore_materialized_snapshot_read_or_bootstrap(
        loss_demand_snapshot_key(),
        static fn (): array => loss_demand_summary_build(),
        'loss-demand-bootstrap'
    );
}

function loss_demand_refresh_summary(string $reason = 'manual'): array
{
    supplycore_materialized_snapshot_mark_updating(loss_demand_snapshot_key(), $reason);
    $snapshot = loss_demand_summary_build();

    return supplycore_materialized_snapshot_store(loss_demand_snapshot_key(), $snapshot, [
        'reason' => $reason,
        'row_count' => count((array) ($snapshot['rows'] ?? [])),
    ]);
}

function loss_demand_refresh_summary_job_result(string $reason = 'manual'): array
{
    $snapshot = loss_demand_refresh_summary($reason);
    $rowCount = count((array) ($snapshot['rows'] ?? []));
    $freshness = is_array($snapshot['_freshness'] ?? null) ? $snapshot['_freshness'] : [];

    return sync_result_shape() + [
        'rows_seen' => $rowCount,
        'rows_written' => $rowCount,
        'cursor' => 'loss_demand:' . gmdate('Y-m-d H:i:s'),
        'checksum' => sync_checksum([
            'rows' => $rowCount,
            'computed_at' => (string) ($freshness['computed_at'] ?? gmdate(DATE_ATOM)),
            'reason' => $reason,
        ]),
        'meta' => [
            'outcome_reason' => 'Loss-demand summaries were recomputed from tracked killmail demand and written to materialized storage plus Redis.',
            'snapshot_generated_at' => (string) ($freshness['computed_at'] ?? ''),
        ],
    ];
}

function dashboard_intelligence_data_build(): array
{
    $marketHubRef = market_hub_setting_reference();
    $allianceStructureId = configured_alliance_structure_id();
    $comparisonContext = market_comparison_context();
    $outcomes = market_comparison_snapshot_payload();
    $rows = array_values((array) ($outcomes['rows'] ?? []));
    $rowCount = count($rows);
    $inBothCount = count((array) ($outcomes['in_both_markets'] ?? []));
    $coveragePercent = $rowCount > 0 ? ($inBothCount / $rowCount) * 100.0 : 0.0;

    $opportunities = $rows;
    usort($opportunities, static fn (array $a, array $b): int => (($b['opportunity_score'] ?? 0) <=> ($a['opportunity_score'] ?? 0)) ?: (($b['volume_score'] ?? 0) <=> ($a['volume_score'] ?? 0)));
    $risks = $rows;
    usort($risks, static fn (array $a, array $b): int => (($b['risk_score'] ?? 0) <=> ($a['risk_score'] ?? 0)) ?: (($b['stock_score'] ?? 0) <=> ($a['stock_score'] ?? 0)));

    $allianceSync = $allianceStructureId !== null
        ? sync_status_for_dataset_keys([sync_dataset_key_alliance_structure_orders_current($allianceStructureId)])
        : ['states' => [], 'runs' => [], 'last_success_at' => null, 'last_error_message' => null, 'recent_rows_written' => 0];
    $hubCurrentSync = $marketHubRef !== ''
        ? sync_status_for_dataset_keys([sync_dataset_key_market_hub_current_orders($marketHubRef)])
        : ['states' => [], 'runs' => [], 'last_success_at' => null, 'last_error_message' => null, 'recent_rows_written' => 0];
    $historySync = $marketHubRef !== ''
        ? sync_status_for_dataset_keys([sync_dataset_key_market_hub_local_history_daily($marketHubRef)])
        : ['states' => [], 'runs' => [], 'last_success_at' => null, 'last_error_message' => null, 'recent_rows_written' => 0];

    $referenceSourceId = sync_source_id_from_hub_ref($marketHubRef);
    $trendHistory = dashboard_trend_history_dataset($marketHubRef, $referenceSourceId);
    $historyRows = $trendHistory['rows'] ?? [];
    $doctrineSnapshot = doctrine_snapshot_cache_payload();
    if ($doctrineSnapshot === null) {
        $doctrineSnapshot = doctrine_refresh_intelligence('dashboard-bootstrap');
    }
    $doctrine = doctrine_groups_overview_data();
    $lossDemand = loss_demand_snapshot_payload();
    $lossDemandTop = array_slice((array) ($lossDemand['top_rows'] ?? []), 0, 5);
    $aiBriefings = doctrine_ai_dashboard_briefings(6);

    $atRiskDoctrines = array_slice(array_values((array) ($doctrine['not_ready_fits'] ?? [])), 0, 5);
    if ($atRiskDoctrines === []) {
        $fits = array_values((array) ($doctrineSnapshot['fits'] ?? []));
        $atRiskDoctrines = array_slice(array_values(array_filter($fits, static fn (array $fit): bool => (int) (($fit['supply']['gap_to_target_fit_count'] ?? 0)) > 0)), 0, 5);
    }

    return [
        'kpis' => [
            ['label' => 'Top Opportunities', 'value' => (string) count(array_filter($rows, static fn (array $row): bool => (int) ($row['opportunity_score'] ?? 0) >= 60)), 'context' => 'High-confidence import/reprice candidates'],
            ['label' => 'Top Risks', 'value' => (string) count(array_filter($rows, static fn (array $row): bool => (int) ($row['risk_score'] ?? 0) >= 60)), 'context' => 'High-severity pricing or stock risk'],
            ['label' => 'Missing Seed Targets', 'value' => (string) count((array) ($outcomes['missing_in_alliance'] ?? [])), 'context' => 'Items in ' . $comparisonContext['reference_hub'] . ' not listed in alliance'],
            ['label' => 'Overlap Coverage', 'value' => market_format_percentage($coveragePercent), 'context' => $rowCount > 0 ? ($inBothCount . ' of ' . $rowCount . ' items in both markets') : 'No market overlap data yet'],
        ],
        'priority_queues' => [
            'opportunities' => array_map(static fn (array $row): array => dashboard_priority_queue_item(
                $row,
                (bool) ($row['missing_in_alliance'] ?? false) ? 'Import seed candidate' : sprintf('%+.1f%% vs hub', (float) ($row['deviation_percent'] ?? 0.0)),
                (int) ($row['opportunity_score'] ?? 0)
            ), array_slice(array_values(array_filter($opportunities, static fn (array $row): bool => (int) ($row['opportunity_score'] ?? 0) > 0)), 0, 5)),
            'risks' => array_map(static fn (array $row): array => dashboard_priority_queue_item(
                $row,
                (bool) ($row['overpriced_in_alliance'] ?? false) ? sprintf('%+.1f%% overpriced', (float) ($row['deviation_percent'] ?? 0.0)) : 'Stock or freshness risk',
                (int) ($row['risk_score'] ?? 0)
            ), array_slice(array_values(array_filter($risks, static fn (array $row): bool => (int) ($row['risk_score'] ?? 0) > 0)), 0, 5)),
            'missing_items' => array_map(static fn (array $row): array => dashboard_priority_queue_item(
                $row,
                'Volume ' . (string) ($row['reference_total_sell_volume'] ?? 0) . ' · score ' . (string) ($row['opportunity_score'] ?? 0),
                (int) ($row['opportunity_score'] ?? 0)
            ), array_slice(array_values(array_filter($opportunities, static fn (array $row): bool => (bool) ($row['missing_in_alliance'] ?? false))), 0, 5)),
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
        'doctrine' => $doctrine,
        'top_at_risk_doctrines' => $atRiskDoctrines,
        'top_shortages' => array_slice((array) ($doctrine['top_missing_items'] ?? []), 0, 5),
        'top_loss_demand_items' => $lossDemandTop,
        'top_comparison_signals' => array_slice($risks, 0, 5),
        'doctrine_readiness_rollups' => array_slice((array) ($doctrine['groups'] ?? []), 0, 8),
        'ai_briefings' => $aiBriefings,
    ];
}

function dashboard_snapshot_payload(): array
{
    $cached = supplycore_cached_json_read(supplycore_dashboard_top_cache_key());
    if (is_array($cached) && $cached !== []) {
        return $cached;
    }

    return supplycore_materialized_snapshot_read_or_bootstrap(
        dashboard_snapshot_key(),
        static fn (): array => dashboard_intelligence_data_build(),
        'dashboard-bootstrap'
    );
}

function dashboard_refresh_summary(string $reason = 'manual'): array
{
    supplycore_materialized_snapshot_mark_updating(dashboard_snapshot_key(), $reason);
    $snapshot = dashboard_intelligence_data_build();
    $stored = supplycore_materialized_snapshot_store(dashboard_snapshot_key(), $snapshot, [
        'reason' => $reason,
        'queue_count' => count((array) ($snapshot['priority_queues']['opportunities'] ?? []))
            + count((array) ($snapshot['priority_queues']['risks'] ?? []))
            + count((array) ($snapshot['priority_queues']['missing_items'] ?? [])),
    ]);

    supplycore_cached_json_write(supplycore_dashboard_top_cache_key(), $stored);

    return $stored;
}

function dashboard_refresh_summary_job_result(string $reason = 'manual'): array
{
    $snapshot = dashboard_refresh_summary($reason);
    $rowCount = count((array) ($snapshot['top_comparison_signals'] ?? []));
    $freshness = is_array($snapshot['_freshness'] ?? null) ? $snapshot['_freshness'] : [];

    return sync_result_shape() + [
        'rows_seen' => $rowCount,
        'rows_written' => $rowCount,
        'cursor' => 'dashboard_summary:' . gmdate('Y-m-d H:i:s'),
        'checksum' => sync_checksum([
            'rows' => $rowCount,
            'computed_at' => (string) ($freshness['computed_at'] ?? gmdate(DATE_ATOM)),
            'reason' => $reason,
        ]),
        'meta' => [
            'outcome_reason' => 'Dashboard summaries were assembled from doctrine, market, and loss-demand materialized layers and written to Redis.',
            'snapshot_generated_at' => (string) ($freshness['computed_at'] ?? ''),
        ],
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

    return supplycore_cache_aside(
        'market_compare',
        ['aggregates', $allianceStructureId, $marketHubRef, $referenceSourceId, array_values($typeIds)],
        supplycore_cache_ttl('market_summary'),
        static function () use ($allianceStructureId, $referenceSourceId, $typeIds): array {
            try {
                return db_market_orders_current_alliance_vs_reference_aggregates($allianceStructureId, $referenceSourceId, $typeIds);
            } catch (Throwable) {
                return [];
            }
        },
        [
            'dependencies' => ['market_compare'],
            'lock_ttl' => 20,
        ]
    );
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
    $snapshot = market_comparison_snapshot_payload();
    $allRows = array_values((array) ($snapshot['rows'] ?? []));

    if ($typeIds !== []) {
        $allowedTypeIds = array_fill_keys(
            array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $typeId): bool => $typeId > 0))),
            true
        );
        $allRows = array_values(array_filter($allRows, static fn (array $row): bool => isset($allowedTypeIds[(int) ($row['type_id'] ?? 0)])));
    }

    return [
        'thresholds' => is_array($snapshot['thresholds'] ?? null) ? $snapshot['thresholds'] : market_compare_thresholds(),
        'rows' => $allRows,
        'in_both_markets' => array_values(array_filter($allRows, static fn (array $row): bool => (bool) ($row['in_both_markets'] ?? false))),
        'missing_in_alliance' => array_values(array_filter($allRows, static fn (array $row): bool => (bool) ($row['missing_in_alliance'] ?? false))),
        'overpriced_in_alliance' => array_values(array_filter($allRows, static fn (array $row): bool => (bool) ($row['overpriced_in_alliance'] ?? false))),
        'underpriced_in_alliance' => array_values(array_filter($allRows, static fn (array $row): bool => (bool) ($row['underpriced_in_alliance'] ?? false))),
        'weak_or_missing_alliance_stock' => array_values(array_filter($allRows, static fn (array $row): bool => (bool) ($row['weak_alliance_stock'] ?? false) || (bool) ($row['missing_in_alliance'] ?? false))),
        '_freshness' => is_array($snapshot['_freshness'] ?? null) ? $snapshot['_freshness'] : supplycore_snapshot_freshness(market_comparison_snapshot_key()),
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

function sync_status_for_dataset_keys(array $datasetKeys): array
{
    $normalizedKeys = array_values(array_unique(array_filter(array_map(
        static fn (mixed $datasetKey): string => trim((string) $datasetKey),
        $datasetKeys
    ), static fn (string $datasetKey): bool => $datasetKey !== '')));

    if ($normalizedKeys === []) {
        return [
            'states' => [],
            'runs' => [],
            'last_success_at' => null,
            'last_error_message' => null,
            'recent_rows_written' => 0,
        ];
    }

    try {
        $states = [];
        $runs = [];
        $lastSuccessAt = null;
        $lastErrorMessage = null;
        $recentRowsWritten = 0;

        foreach ($normalizedKeys as $datasetKey) {
            $state = db_sync_state_get($datasetKey);
            if ($state !== null) {
                $states[] = $state;

                $candidate = $state['last_success_at'] ?? null;
                if (is_string($candidate) && $candidate !== '' && ($lastSuccessAt === null || strtotime($candidate) > strtotime($lastSuccessAt))) {
                    $lastSuccessAt = $candidate;
                }

                $recentRowsWritten += max(0, (int) ($state['last_row_count'] ?? 0));
            }

            $latestRun = db_sync_run_latest_by_dataset($datasetKey);
            if ($latestRun !== null) {
                $runs[] = $latestRun;

                if ($lastErrorMessage === null && (string) ($latestRun['run_status'] ?? '') === 'failed') {
                    $message = trim((string) ($latestRun['error_message'] ?? ''));
                    if ($message !== '') {
                        $lastErrorMessage = $message;
                    }
                }
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

        usort($states, static fn (array $a, array $b): int => strcmp((string) ($b['updated_at'] ?? ''), (string) ($a['updated_at'] ?? '')));
        usort($runs, static fn (array $a, array $b): int => (int) ($b['id'] ?? 0) <=> (int) ($a['id'] ?? 0));

        return [
            'states' => $states,
            'runs' => $runs,
            'last_success_at' => $lastSuccessAt,
            'last_error_message' => $lastErrorMessage,
            'recent_rows_written' => $recentRowsWritten,
        ];
    } catch (Throwable) {
        return [
            'states' => [],
            'runs' => [],
            'last_success_at' => null,
            'last_error_message' => null,
            'recent_rows_written' => 0,
        ];
    }
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
    $rows = item_scope_filter_rows(
        $rows,
        static fn (array $row): int => (int) ($row['type_id'] ?? 0)
    );

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

function dashboard_trend_history_dataset(string $marketHubRef, int $referenceSourceId): array
{
    if ($marketHubRef === '' || $referenceSourceId <= 0) {
        return [
            'rows' => [],
            'message' => 'Trend snippets need a reference hub. Configure Settings → General first, then run the hub history snapshot job for that hub.',
        ];
    }

    try {
        $historyRows = db_market_history_daily_recent_window('market_hub', $referenceSourceId, 8, 80);
    } catch (Throwable) {
        $historyRows = [];
    }

    if ($historyRows !== []) {
        return [
            'rows' => $historyRows,
            'message' => '',
        ];
    }

    return [
        'rows' => [],
        'message' => 'Trend snippets use SupplyCore snapshot history. Run the hub current sync for ' . market_hub_reference_name() . ' and capture at least two daily history builds from local snapshots.',
    ];
}

function dashboard_intelligence_data(): array
{
    return dashboard_snapshot_payload();
}

function dashboard_priority_queue_item(array $row, string $signal, int $score): array
{
    $typeId = max(0, (int) ($row['type_id'] ?? 0));

    return [
        'module' => $row['type_name'] !== '' ? $row['type_name'] : ('Type #' . $typeId),
        'signal' => $signal,
        'score' => max(0, $score),
        'type_id' => $typeId > 0 ? $typeId : null,
        'image_url' => $typeId > 0 ? killmail_entity_image_url('type', $typeId, 'icon', 64) : null,
    ];
}

function current_alliance_market_status_data(): array
{
    $search = trim((string) ($_GET['q'] ?? ''));
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
    $cacheParts = ['current-alliance', $sort, $pageSize, $page, $search];
    $useCache = $search === '' && $page === 1 && $pageSize === 25 && $sort === 'urgency';
    $resolver = static function () use ($search, $sort, $pageSize, $page): array {
        $outcomes = market_comparison_outcomes();
        $thresholds = $outcomes['thresholds'] ?? [];
        $lowStockThreshold = max(1, (int) ($thresholds['min_alliance_sell_volume'] ?? 25));
        $staleAfterSeconds = 6 * 3600;
        $searchNeedle = mb_strtolower($search);

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
        $resolvedPage = min($page, $totalPages);
        $offset = ($resolvedPage - 1) * $pageSize;
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
        'freshness' => $outcomes['_freshness'] ?? supplycore_snapshot_freshness(market_comparison_snapshot_key()),
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
            'page' => $resolvedPage,
            'total_pages' => $totalPages,
            'total_items' => $totalItems,
            'showing_from' => $totalItems > 0 ? $offset + 1 : 0,
            'showing_to' => min($offset + $pageSize, $totalItems),
        ],
        ];
    };

    if ($useCache) {
        return supplycore_cache_aside('market_compare', $cacheParts, supplycore_cache_ttl('market_listing'), $resolver, [
            'dependencies' => ['market_compare'],
            'lock_ttl' => 20,
        ]);
    }

    return $resolver();
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

function killmail_entity_redis_cache_key(string $type, int $entityId): string
{
    return sprintf(
        'metadata:entity:v%d:%s:%d',
        supplycore_cache_version('metadata_entities'),
        preg_replace('/[^a-z0-9:_-]+/i', '-', strtolower($type)) ?: 'entity',
        max(0, $entityId)
    );
}

function killmail_entity_cache_rows_by_type(array $requests): array
{
    $rowsByType = [];
    $databaseMisses = [];

    foreach ($requests as $type => $ids) {
        $rowsByType[$type] = [];
        $databaseMisses[$type] = [];
        $cacheKeys = [];
        $idMap = [];

        foreach ((array) $ids as $id) {
            $entityId = (int) $id;
            if ($entityId <= 0) {
                continue;
            }

            $cacheKey = killmail_entity_redis_cache_key($type, $entityId);
            $cacheKeys[] = $cacheKey;
            $idMap[] = $entityId;
        }

        $cachedPayloads = supplycore_redis_enabled() ? supplycore_redis_mget($cacheKeys) : [];
        foreach ($idMap as $index => $entityId) {
            $payload = $cachedPayloads[$index] ?? null;
            if (!is_string($payload) || $payload === '') {
                $databaseMisses[$type][] = $entityId;

                continue;
            }

            try {
                $row = json_decode($payload, true, 512, JSON_THROW_ON_ERROR);
            } catch (Throwable) {
                $databaseMisses[$type][] = $entityId;

                continue;
            }

            if (is_array($row)) {
                $rowsByType[$type][$entityId] = $row;
            }
        }
    }

    foreach ($databaseMisses as $type => $ids) {
        foreach (db_entity_metadata_cache_get_many($type, (array) $ids) as $row) {
            $entityId = (int) ($row['entity_id'] ?? 0);
            if ($entityId <= 0) {
                continue;
            }

            $rowsByType[$type][$entityId] = $row;

            $ttl = supplycore_cache_ttl(in_array($type, ['type', 'system', 'region'], true) ? 'metadata_static' : 'metadata_dynamic');
            $expiresAt = trim((string) ($row['expires_at'] ?? ''));
            if ($expiresAt !== '') {
                $expiresUnix = strtotime($expiresAt);
                if ($expiresUnix !== false) {
                    $ttl = max(60, $expiresUnix - time());
                }
            }

            supplycore_redis_set(
                killmail_entity_redis_cache_key($type, $entityId),
                json_encode($row, JSON_THROW_ON_ERROR),
                $ttl
            );
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

        $typeId = isset($item['item_type_id']) ? (int) $item['item_type_id'] : 0;
        if ($typeId <= 0 || !item_scope_is_type_id_in_scope($typeId)) {
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


function killmail_doctrine_impact_confidence(int $matchedItemCount, int $matchedFitCount, bool $matchedHull): array
{
    $score = $matchedItemCount + min(2, max(0, $matchedFitCount - 1)) + ($matchedHull ? 1 : 0);
    $level = 'low';
    $label = 'Low';
    $tone = 'border-sky-500/40 bg-sky-500/10 text-sky-100';

    if ($score >= 5) {
        $level = 'high';
        $label = 'High';
        $tone = 'border-emerald-500/40 bg-emerald-500/10 text-emerald-100';
    } elseif ($score >= 3) {
        $level = 'medium';
        $label = 'Medium';
        $tone = 'border-amber-500/40 bg-amber-500/10 text-amber-100';
    }

    return [
        'level' => $level,
        'label' => $label,
        'tone' => $tone,
        'score' => $score,
    ];
}

function killmail_doctrine_impact_severity(int $matchedGroupCount, int $matchedMeaningfulItemCount, bool $matchedHull): array
{
    $score = ($matchedGroupCount * 2) + $matchedMeaningfulItemCount + ($matchedHull ? 2 : 0);
    $level = 'weak';
    $label = 'Weak';
    $tone = 'border-sky-500/40 bg-sky-500/10 text-sky-100';

    if ($score >= 9) {
        $level = 'strong';
        $label = 'Strong';
        $tone = 'border-red-500/40 bg-red-500/10 text-red-100';
    } elseif ($score >= 6) {
        $level = 'likely';
        $label = 'Likely';
        $tone = 'border-amber-500/40 bg-amber-500/10 text-amber-100';
    } elseif ($score >= 3) {
        $level = 'possible';
        $label = 'Possible';
        $tone = 'border-cyan-500/40 bg-cyan-500/10 text-cyan-100';
    }

    return [
        'level' => $level,
        'label' => $label,
        'tone' => $tone,
        'score' => $score,
    ];
}

function killmail_doctrine_impact_debug_enabled(): bool
{
    return sanitize_enabled_flag($_GET['debug_doctrine_impact'] ?? '0') === '1';
}

function killmail_doctrine_impact_log_debug(array $debug): void
{
    if (!killmail_doctrine_impact_debug_enabled()) {
        return;
    }

    try {
        error_log('[killmail.doctrine_impact] ' . json_encode($debug, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES));
    } catch (Throwable) {
    }
}

function doctrine_impact_fit_catalog(): array
{
    static $catalog = null;
    if (is_array($catalog)) {
        return $catalog;
    }

    try {
        $fits = db_doctrine_fits_all();
    } catch (Throwable) {
        $catalog = ['fits' => [], 'doctrine_type_ids' => []];

        return $catalog;
    }

    $fitIds = array_values(array_filter(array_map(static fn (array $fit): int => (int) ($fit['id'] ?? 0), $fits), static fn (int $fitId): bool => $fitId > 0));
    $itemsByFitId = [];
    if ($fitIds !== []) {
        try {
            foreach (db_doctrine_fit_items_by_fit_ids($fitIds) as $item) {
                $fitId = (int) ($item['doctrine_fit_id'] ?? 0);
                if ($fitId > 0) {
                    $itemsByFitId[$fitId][] = $item;
                }
            }
        } catch (Throwable) {
            $itemsByFitId = [];
        }
    }

    $normalizedItemsByFitId = [];
    $allTypeIds = [];
    foreach ($fits as $fit) {
        $fitId = (int) ($fit['id'] ?? 0);
        if ($fitId <= 0) {
            continue;
        }

        $normalizedItems = doctrine_normalize_persisted_fit_items($fit, $itemsByFitId[$fitId] ?? []);
        $normalizedItemsByFitId[$fitId] = $normalizedItems;
        foreach ($normalizedItems as $item) {
            $typeId = (int) ($item['type_id'] ?? 0);
            if ($typeId > 0) {
                $allTypeIds[] = $typeId;
            }
        }
    }

    $metadataByType = item_scope_metadata_by_type_ids(array_values(array_unique($allTypeIds)));
    $fitCatalog = [];
    $allDoctrineTypeIds = [];

    foreach ($fits as $fit) {
        $fitId = (int) ($fit['id'] ?? 0);
        if ($fitId <= 0) {
            continue;
        }

        $groupIds = doctrine_parse_group_csv($fit['group_ids_csv'] ?? null);
        $groupNames = doctrine_parse_group_names_csv($fit['group_names_csv'] ?? null);
        $durableTypeIds = [];
        $consumableTypeIds = [];
        $durableItems = [];
        $consumableItems = [];

        foreach ($normalizedItemsByFitId[$fitId] ?? [] as $item) {
            $typeId = (int) ($item['type_id'] ?? 0);
            if ($typeId <= 0) {
                continue;
            }

            $itemName = trim((string) (($item['type_name'] ?? null) ?: ($item['item_name'] ?? '')));
            $payload = [
                'type_id' => $typeId,
                'item_name' => $itemName !== '' ? $itemName : ('Type #' . $typeId),
                'quantity' => max(1, (int) ($item['quantity'] ?? 1)),
                'slot_category' => (string) ($item['slot_category'] ?? ''),
            ];

            if (item_scope_type_is_consumable($typeId, $metadataByType)) {
                $consumableTypeIds[$typeId] = true;
                $consumableItems[$typeId] = $payload;
                continue;
            }

            $durableTypeIds[$typeId] = true;
            $durableItems[$typeId] = $payload;
            $allDoctrineTypeIds[$typeId] = true;
        }

        $fitCatalog[] = [
            'id' => $fitId,
            'fit_name' => (string) ($fit['fit_name'] ?? 'Doctrine fit'),
            'ship_name' => (string) ($fit['ship_name'] ?? ''),
            'ship_type_id' => isset($fit['ship_type_id']) ? (int) $fit['ship_type_id'] : null,
            'ship_image_url' => doctrine_ship_image_url(isset($fit['ship_type_id']) ? (int) ($fit['ship_type_id'] ?? 0) : null, 64),
            'group_ids' => $groupIds,
            'group_names' => $groupNames,
            'durable_type_ids' => array_values(array_map('intval', array_keys($durableTypeIds))),
            'consumable_type_ids' => array_values(array_map('intval', array_keys($consumableTypeIds))),
            'durable_items_by_type_id' => $durableItems,
            'consumable_items_by_type_id' => $consumableItems,
        ];
    }

    $catalog = [
        'fits' => $fitCatalog,
        'doctrine_type_ids' => array_values(array_map('intval', array_keys($allDoctrineTypeIds))),
    ];

    return $catalog;
}

function killmail_doctrine_impact(array $event, array $items, array $resolvedEntities = []): array
{
    $catalog = doctrine_impact_fit_catalog();
    $victimShipTypeId = isset($event['victim_ship_type_id']) ? (int) ($event['victim_ship_type_id'] ?? 0) : 0;
    $candidateTypeIds = [];
    $candidateRows = [];

    if ($victimShipTypeId > 0) {
        $candidateTypeIds[] = $victimShipTypeId;
        $resolvedHull = killmail_resolved_entity($resolvedEntities, 'type', $victimShipTypeId, isset($event['ship_type_name']) ? (string) $event['ship_type_name'] : null);
        $candidateRows[] = [
            'type_id' => $victimShipTypeId,
            'quantity' => 1,
            'role' => 'hull',
            'item_name' => (string) ($resolvedHull['name'] ?? ('Type #' . $victimShipTypeId)),
            'state_label' => 'Victim hull',
        ];
    }

    foreach ($items as $item) {
        if (!is_array($item)) {
            continue;
        }

        $role = (string) ($item['item_role'] ?? 'other');
        if (!in_array($role, ['dropped', 'destroyed', 'fitted'], true)) {
            continue;
        }

        $typeId = (int) ($item['item_type_id'] ?? 0);
        if ($typeId <= 0) {
            continue;
        }

        $quantity = match ($role) {
            'dropped' => max(1, (int) ($item['quantity_dropped'] ?? 0)),
            'destroyed' => max(1, (int) ($item['quantity_destroyed'] ?? 0)),
            default => max(1, (int) ($item['quantity_destroyed'] ?? 0), (int) ($item['quantity_dropped'] ?? 0)),
        };
        $resolvedItem = killmail_resolved_entity($resolvedEntities, 'type', $typeId, isset($item['item_type_name']) ? (string) $item['item_type_name'] : null);

        $candidateTypeIds[] = $typeId;
        $candidateRows[] = [
            'type_id' => $typeId,
            'quantity' => $quantity,
            'role' => $role,
            'item_name' => (string) ($resolvedItem['name'] ?? ('Type #' . $typeId)),
            'state_label' => match ($role) {
                'dropped' => number_format($quantity) . ' dropped',
                'destroyed' => number_format($quantity) . ' destroyed',
                default => number_format($quantity) . ' fitted',
            },
        ];
    }

    $candidateTypeIds = array_values(array_unique(array_filter(
        array_map('intval', $candidateTypeIds),
        static fn (int $typeId): bool => $typeId > 0
    )));
    $metadataByType = item_scope_metadata_by_type_ids($candidateTypeIds);
    $primaryVictimTypeIds = [];
    $secondaryVictimTypeIds = [];
    $victimRowsByTypeId = [];

    foreach ($candidateRows as $row) {
        $typeId = (int) ($row['type_id'] ?? 0);
        if ($typeId <= 0) {
            continue;
        }

        $victimRowsByTypeId[$typeId] ??= $row;
        if (($row['role'] ?? '') === 'hull' || item_scope_type_is_durable_loss_relevant($typeId, null, $metadataByType)) {
            $primaryVictimTypeIds[$typeId] = true;
            continue;
        }

        if (item_scope_type_is_consumable($typeId, $metadataByType)) {
            $secondaryVictimTypeIds[$typeId] = true;
        }
    }

    $matchedFitsById = [];
    $matchedGroups = [];
    $matchedPrimaryTypeIds = [];
    $matchedSecondaryTypeIds = [];
    $matchedItemsByTypeId = [];

    foreach ((array) ($catalog['fits'] ?? []) as $fit) {
        $fitId = (int) ($fit['id'] ?? 0);
        if ($fitId <= 0) {
            continue;
        }

        $fitPrimaryIntersection = array_values(array_intersect(
            array_keys($primaryVictimTypeIds),
            (array) ($fit['durable_type_ids'] ?? [])
        ));
        $fitSecondaryIntersection = array_values(array_intersect(
            array_keys($secondaryVictimTypeIds),
            (array) ($fit['consumable_type_ids'] ?? [])
        ));
        if ($fitPrimaryIntersection === [] && $fitSecondaryIntersection === []) {
            continue;
        }

        $matchedPrimaryItems = [];
        foreach ($fitPrimaryIntersection as $typeId) {
            $typeId = (int) $typeId;
            if ($typeId <= 0) {
                continue;
            }

            $matchedPrimaryTypeIds[$typeId] = true;
            $fitItem = (array) (($fit['durable_items_by_type_id'][$typeId] ?? []));
            $victimItem = (array) ($victimRowsByTypeId[$typeId] ?? []);
            $itemName = trim((string) (($fitItem['item_name'] ?? null) ?: ($victimItem['item_name'] ?? ('Type #' . $typeId))));
            $slotCategory = (string) ($fitItem['slot_category'] ?? '');
            $matchedPrimaryItems[$typeId] = [
                'type_id' => $typeId,
                'item_name' => $itemName !== '' ? $itemName : ('Type #' . $typeId),
                'slot_category' => $slotCategory,
                'victim_state_label' => (string) ($victimItem['state_label'] ?? ''),
                'is_hull' => $typeId === $victimShipTypeId,
            ];
        }

        $matchedSecondaryItems = [];
        foreach ($fitSecondaryIntersection as $typeId) {
            $typeId = (int) $typeId;
            if ($typeId <= 0) {
                continue;
            }

            $matchedSecondaryTypeIds[$typeId] = true;
            $fitItem = (array) (($fit['consumable_items_by_type_id'][$typeId] ?? []));
            $victimItem = (array) ($victimRowsByTypeId[$typeId] ?? []);
            $matchedSecondaryItems[$typeId] = [
                'type_id' => $typeId,
                'item_name' => (string) (($fitItem['item_name'] ?? null) ?: ($victimItem['item_name'] ?? ('Type #' . $typeId))),
                'victim_state_label' => (string) ($victimItem['state_label'] ?? ''),
            ];
        }

        ksort($matchedPrimaryItems);
        ksort($matchedSecondaryItems);

        $matchedFitsById[$fitId] = [
            'id' => $fitId,
            'fit_name' => (string) ($fit['fit_name'] ?? 'Doctrine fit'),
            'ship_name' => (string) (($fit['ship_name'] ?? '') !== '' ? $fit['ship_name'] : ($fit['fit_name'] ?? 'Doctrine fit')),
            'ship_image_url' => $fit['ship_image_url'] ?? null,
            'group_names' => array_values(array_filter(array_map(static fn ($value): string => trim((string) $value), (array) ($fit['group_names'] ?? [])), static fn (string $value): bool => $value !== '')),
            'matched_primary_items' => array_values($matchedPrimaryItems),
            'matched_secondary_items' => array_values($matchedSecondaryItems),
            'matched_primary_line_count' => count($matchedPrimaryItems),
            'matched_secondary_line_count' => count($matchedSecondaryItems),
        ];

        $fitGroupNames = $matchedFitsById[$fitId]['group_names'];
        if ($fitGroupNames === []) {
            $fitGroupNames = ['Ungrouped'];
            $matchedFitsById[$fitId]['group_names'] = $fitGroupNames;
        }

        foreach ($matchedPrimaryItems as $typeId => $matchedItem) {
            $typeId = (int) $typeId;
            $matchedItemsByTypeId[$typeId] ??= [
                'type_id' => $typeId,
                'item_name' => (string) ($matchedItem['item_name'] ?? ('Type #' . $typeId)),
                'slot_category' => (string) ($matchedItem['slot_category'] ?? ''),
                'victim_state_label' => (string) ($matchedItem['victim_state_label'] ?? ''),
                'is_hull' => (bool) ($matchedItem['is_hull'] ?? false),
                'fit_ids' => [],
                'fit_names' => [],
                'group_names' => [],
            ];
            $matchedItemsByTypeId[$typeId]['fit_ids'][$fitId] = true;
            $matchedItemsByTypeId[$typeId]['fit_names'][(string) ($matchedFitsById[$fitId]['fit_name'] ?? 'Doctrine fit')] = true;
            foreach ($fitGroupNames as $groupName) {
                $matchedItemsByTypeId[$typeId]['group_names'][$groupName] = true;
            }
        }

        foreach ($fitGroupNames as $groupName) {
            $normalizedGroup = trim((string) $groupName);
            if ($normalizedGroup === '') {
                continue;
            }

            $matchedGroups[$normalizedGroup] ??= [
                'group_name' => $normalizedGroup,
                'fit_ids' => [],
                'fit_names' => [],
                'matched_item_type_ids' => [],
                'matched_item_names' => [],
                'matched_secondary_type_ids' => [],
                'matched_secondary_item_names' => [],
                'matched_hull' => false,
            ];
            $matchedGroups[$normalizedGroup]['fit_ids'][$fitId] = true;
            $matchedGroups[$normalizedGroup]['fit_names'][(string) ($matchedFitsById[$fitId]['fit_name'] ?? 'Doctrine fit')] = true;
            foreach ($matchedPrimaryItems as $typeId => $matchedItem) {
                $matchedGroups[$normalizedGroup]['matched_item_type_ids'][(int) $typeId] = true;
                $matchedGroups[$normalizedGroup]['matched_item_names'][(string) ($matchedItem['item_name'] ?? '')] = true;
                if ((bool) ($matchedItem['is_hull'] ?? false)) {
                    $matchedGroups[$normalizedGroup]['matched_hull'] = true;
                }
            }
            foreach ($matchedSecondaryItems as $typeId => $matchedItem) {
                $matchedGroups[$normalizedGroup]['matched_secondary_type_ids'][(int) $typeId] = true;
                $matchedGroups[$normalizedGroup]['matched_secondary_item_names'][(string) ($matchedItem['item_name'] ?? '')] = true;
            }
        }
    }

    $matchedFits = array_values($matchedFitsById);
    usort($matchedFits, static function (array $a, array $b): int {
        return ((int) ($b['matched_primary_line_count'] ?? 0) <=> (int) ($a['matched_primary_line_count'] ?? 0))
            ?: ((int) ($b['matched_secondary_line_count'] ?? 0) <=> (int) ($a['matched_secondary_line_count'] ?? 0))
            ?: strcasecmp((string) ($a['fit_name'] ?? ''), (string) ($b['fit_name'] ?? ''));
    });

    $matchedPrimaryTypeIds = array_values(array_map('intval', array_keys($matchedPrimaryTypeIds)));
    $matchedSecondaryTypeIds = array_values(array_map('intval', array_keys($matchedSecondaryTypeIds)));
    sort($matchedPrimaryTypeIds);
    sort($matchedSecondaryTypeIds);

    $matchedItemRows = array_values(array_map(static function (array $item): array {
        $item['fit_count'] = count((array) ($item['fit_ids'] ?? []));
        $item['group_count'] = count((array) ($item['group_names'] ?? []));
        $slotCategory = strtolower(trim((string) ($item['slot_category'] ?? '')));
        $genericPenalty = in_array($slotCategory, ['low', 'mid', 'high', 'subsystem', 'service'], true) ? 1 : 0;
        $item['is_generic_overlap'] = !$item['is_hull'] && $genericPenalty > 0 && ($item['group_count'] >= 2 || $item['fit_count'] >= 3);
        $item['signal_score'] = ($item['is_hull'] ? 100 : 0)
            + ($item['group_count'] * 6)
            + min(3, $item['fit_count'])
            + ($slotCategory === 'rig' ? 2 : 0)
            + ($slotCategory === 'drone' ? 1 : 0)
            - $genericPenalty;
        $item['fit_names'] = array_values(array_keys((array) ($item['fit_names'] ?? [])));
        sort($item['fit_names']);
        $item['group_names'] = array_values(array_keys((array) ($item['group_names'] ?? [])));
        sort($item['group_names']);
        unset($item['fit_ids']);

        return $item;
    }, $matchedItemsByTypeId));

    usort($matchedItemRows, static function (array $a, array $b): int {
        return ((int) ($b['signal_score'] ?? 0) <=> (int) ($a['signal_score'] ?? 0))
            ?: ((int) ($b['group_count'] ?? 0) <=> (int) ($a['group_count'] ?? 0))
            ?: ((int) ($b['fit_count'] ?? 0) <=> (int) ($a['fit_count'] ?? 0))
            ?: strcasecmp((string) ($a['item_name'] ?? ''), (string) ($b['item_name'] ?? ''));
    });

    $meaningfulItemRows = array_values(array_filter($matchedItemRows, static fn (array $item): bool => !((bool) ($item['is_generic_overlap'] ?? false))));
    if ($meaningfulItemRows === []) {
        $meaningfulItemRows = $matchedItemRows;
    }

    $topMatchedItemRows = array_slice($meaningfulItemRows, 0, 5);

    $matchedGroupRows = array_values(array_map(static function (array $group): array {
        $fitCount = count((array) ($group['fit_ids'] ?? []));
        $itemCount = count((array) ($group['matched_item_type_ids'] ?? []));
        $confidence = killmail_doctrine_impact_confidence($itemCount, $fitCount, (bool) ($group['matched_hull'] ?? false));
        $group['fit_names'] = array_values(array_keys((array) ($group['fit_names'] ?? [])));
        sort($group['fit_names']);
        $group['matched_item_names'] = array_values(array_filter(array_keys((array) ($group['matched_item_names'] ?? [])), static fn (string $value): bool => trim($value) !== ''));
        natcasesort($group['matched_item_names']);
        $group['matched_item_names'] = array_values($group['matched_item_names']);
        $group['matched_secondary_item_names'] = array_values(array_filter(array_keys((array) ($group['matched_secondary_item_names'] ?? [])), static fn (string $value): bool => trim($value) !== ''));
        natcasesort($group['matched_secondary_item_names']);
        $group['matched_secondary_item_names'] = array_values($group['matched_secondary_item_names']);
        $group['fit_count'] = $fitCount;
        $group['matched_item_count'] = $itemCount;
        $group['matched_secondary_count'] = count((array) ($group['matched_secondary_type_ids'] ?? []));
        $group['confidence'] = $confidence;
        $group['preview_item_names'] = array_slice($group['matched_item_names'], 0, 3);
        unset($group['fit_ids'], $group['matched_item_type_ids'], $group['matched_secondary_type_ids'], $group['matched_hull']);

        return $group;
    }, $matchedGroups));
    usort($matchedGroupRows, static function (array $a, array $b): int {
        return ((int) (($b['confidence']['score'] ?? 0)) <=> (int) (($a['confidence']['score'] ?? 0)))
            ?: ((int) ($b['matched_item_count'] ?? 0) <=> (int) ($a['matched_item_count'] ?? 0))
            ?: strcasecmp((string) ($a['group_name'] ?? ''), (string) ($b['group_name'] ?? ''));
    });

    $matched = $matchedPrimaryTypeIds !== [];
    $meaningfulItemCount = count($meaningfulItemRows);
    $severity = killmail_doctrine_impact_severity(count($matchedGroupRows), $meaningfulItemCount, in_array($victimShipTypeId, $matchedPrimaryTypeIds, true));
    $topItemNames = array_values(array_map(static fn (array $item): string => (string) ($item['item_name'] ?? ''), $topMatchedItemRows));
    $context = $matched
        ? ('Doctrine signal is ' . strtolower((string) ($severity['label'] ?? 'weak'))
            . ': ' . number_format(count($matchedGroupRows)) . ' doctrine group' . (count($matchedGroupRows) === 1 ? '' : 's')
            . ' affected by ' . number_format($meaningfulItemCount) . ' meaningful durable item' . ($meaningfulItemCount === 1 ? '' : 's') . '.')
        : 'No durable doctrine fit item type_ids intersected the victim-side hull and stored loss items.';
    if (!$matched && $matchedSecondaryTypeIds !== []) {
        $context .= ' Secondary consumable-only overlaps were detected, but they do not drive the primary doctrine-impact signal.';
    }

    $debug = [
        'sequence_id' => (int) ($event['sequence_id'] ?? 0),
        'victim_item_type_ids_considered' => $candidateTypeIds,
        'victim_durable_type_ids_considered' => array_values(array_map('intval', array_keys($primaryVictimTypeIds))),
        'victim_consumable_type_ids_considered' => array_values(array_map('intval', array_keys($secondaryVictimTypeIds))),
        'doctrine_item_type_ids_considered' => (array) ($catalog['doctrine_type_ids'] ?? []),
        'primary_intersection_type_ids' => $matchedPrimaryTypeIds,
        'secondary_intersection_type_ids' => $matchedSecondaryTypeIds,
        'intersection_count' => count($matchedPrimaryTypeIds),
        'secondary_intersection_count' => count($matchedSecondaryTypeIds),
        'matched_fit_ids' => array_values(array_map(static fn (array $fit): int => (int) ($fit['id'] ?? 0), $matchedFits)),
        'matched_group_names' => array_values(array_map(static fn (array $group): string => (string) ($group['group_name'] ?? ''), $matchedGroupRows)),
        'meaningful_item_type_ids' => array_values(array_map(static fn (array $item): int => (int) ($item['type_id'] ?? 0), $meaningfulItemRows)),
        'top_item_type_ids' => array_values(array_map(static fn (array $item): int => (int) ($item['type_id'] ?? 0), $topMatchedItemRows)),
        'no_match_reason' => $matched
            ? null
            : match (true) {
                $candidateRows === [] => 'No victim-side hull or item rows were available for doctrine comparison.',
                $primaryVictimTypeIds === [] && $secondaryVictimTypeIds === [] => 'Victim-side rows did not include any comparable type_ids.',
                ($catalog['doctrine_type_ids'] ?? []) === [] => 'No normalized doctrine fit item type_ids are available locally.',
                $matchedSecondaryTypeIds !== [] => 'Only consumable doctrine items overlapped; primary durable doctrine impact remains empty.',
                default => 'Victim-side durable type_ids did not intersect any normalized doctrine fit durable type_ids.',
            },
    ];
    killmail_doctrine_impact_log_debug($debug);

    return [
        'matched' => $matched,
        'label' => $matched ? 'Doctrine impact detected' : 'No doctrine impact',
        'tone' => $matched
            ? ($severity['tone'] ?? 'border-emerald-500/40 bg-emerald-500/10 text-emerald-100')
            : 'border-slate-500/40 bg-slate-500/10 text-slate-300',
        'context' => $context,
        'severity' => $severity,
        'matched_groups' => $matchedGroupRows,
        'matched_fits' => $matchedFits,
        'matched_items' => $matchedItemRows,
        'matched_item_names' => $topItemNames,
        'top_matched_items' => $topMatchedItemRows,
        'meaningful_item_count' => $meaningfulItemCount,
        'matched_item_count' => count($matchedPrimaryTypeIds),
        'matched_fit_count' => count($matchedFits),
        'matched_group_count' => count($matchedGroupRows),
        'matched_line_count' => count($matchedPrimaryTypeIds),
        'supporting_consumable_count' => count($matchedSecondaryTypeIds),
        'debug' => $debug,
    ];
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

    return supplycore_cache_aside('killmail_detail', [$sequenceId], supplycore_cache_ttl('killmail_detail'), static function () use ($sequenceId): array {
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
        $doctrineImpact = killmail_doctrine_impact($event, $items, $resolvedEntities);
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
        $storedItemCount = array_sum(array_map(static fn (array $group): int => count((array) ($group['rows'] ?? [])), $groupedItems));
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
            'doctrine_impact' => $doctrineImpact,
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
    }, [
        'dependencies' => ['killmail_detail'],
        'lock_ttl' => 20,
    ]);
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
    $useCache = $filters['search'] === '' && $filters['alliance_id'] === 0 && $filters['corporation_id'] === 0 && $filters['tracked_only'] === false && $filters['page'] === 1 && $filters['page_size'] === 25;

    $resolver = static function () use ($recentHours, $filters, $allowedPageSizes, $pageSize): array {
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
    };

    if ($useCache) {
        return supplycore_cache_aside('killmail_overview', ['default-page', $pageSize], supplycore_cache_ttl('killmail_summary'), $resolver, [
            'dependencies' => ['killmail_overview'],
            'lock_ttl' => 20,
        ]);
    }

    return $resolver();
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
    return supplycore_cache_aside('market_compare', ['reference-comparison'], supplycore_cache_ttl('market_summary'), static function (): array {
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
        'freshness' => $outcomes['_freshness'] ?? supplycore_snapshot_freshness(market_comparison_snapshot_key()),
        ];
    }, [
        'dependencies' => ['market_compare'],
        'lock_ttl' => 20,
    ]);
}

function missing_items_data(): array
{
    return supplycore_cache_aside('market_compare', ['missing-items'], supplycore_cache_ttl('market_summary'), static function (): array {
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
        'freshness' => $outcomes['_freshness'] ?? supplycore_snapshot_freshness(market_comparison_snapshot_key()),
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
    }, [
        'dependencies' => ['market_compare'],
        'lock_ttl' => 20,
    ]);
}

function price_deviations_data(): array
{
    return supplycore_cache_aside('market_compare', ['price-deviations'], supplycore_cache_ttl('market_summary'), static function (): array {
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
        'freshness' => $outcomes['_freshness'] ?? supplycore_snapshot_freshness(market_comparison_snapshot_key()),
        ];
    }, [
        'dependencies' => ['market_compare'],
        'lock_ttl' => 20,
    ]);
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

function static_data_manifest_url_for_source(string $sourceUrl): ?string
{
    $host = mb_strtolower((string) parse_url($sourceUrl, PHP_URL_HOST));
    $path = (string) parse_url($sourceUrl, PHP_URL_PATH);

    if ($host !== 'developers.eveonline.com') {
        return null;
    }

    if (!str_starts_with($path, '/static-data/')) {
        return null;
    }

    return 'https://developers.eveonline.com/static-data/tranquility/latest.jsonl';
}

function static_data_fetch_text_payload(string $url, int $timeoutSeconds = 30): string
{
    if (!function_exists('curl_init')) {
        throw new RuntimeException('PHP cURL extension is required to fetch static-data payloads.');
    }

    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_FOLLOWLOCATION => true,
        CURLOPT_CONNECTTIMEOUT => 10,
        CURLOPT_TIMEOUT => $timeoutSeconds,
        CURLOPT_HTTPHEADER => [
            'Accept: application/json, text/plain, */*',
            'User-Agent: SupplyCore/1.0',
        ],
    ]);

    $payload = curl_exec($ch);
    $error = curl_error($ch);
    $status = (int) curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
    curl_close($ch);

    if (!is_string($payload) || $payload === '' || $status >= 400) {
        throw new RuntimeException('Unable to fetch static-data payload from source URL.' . ($error !== '' ? ' ' . $error : ''));
    }

    return $payload;
}

function static_data_fetch_binary_payload(string $url, int $timeoutSeconds = 120): string
{
    if (!function_exists('curl_init')) {
        throw new RuntimeException('PHP cURL extension is required to download static-data archives.');
    }

    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_FOLLOWLOCATION => true,
        CURLOPT_CONNECTTIMEOUT => 15,
        CURLOPT_TIMEOUT => $timeoutSeconds,
        CURLOPT_HTTPHEADER => [
            'Accept: application/zip, application/octet-stream, */*',
            'User-Agent: SupplyCore/1.0',
        ],
    ]);

    $payload = curl_exec($ch);
    $error = curl_error($ch);
    $status = (int) curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
    curl_close($ch);

    if (!is_string($payload) || $payload === '' || $status >= 400) {
        throw new RuntimeException('Failed to download static-data package.' . ($error !== '' ? ' ' . $error : ''));
    }

    return $payload;
}

function static_data_build_info_from_manifest(string $manifestUrl): ?array
{
    $payload = static_data_fetch_text_payload($manifestUrl);

    foreach (preg_split("/(?:\r\n|\n|\r)/", $payload) as $line) {
        $line = trim((string) $line);
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

        $buildNumber = trim((string) ($row['buildNumber'] ?? $row['build_number'] ?? ''));
        $releaseDate = trim((string) ($row['releaseDate'] ?? $row['release_date'] ?? ''));
        if ($buildNumber === '' && $releaseDate === '') {
            continue;
        }

        return [
            'build_id' => $buildNumber !== '' ? $buildNumber : sha1($manifestUrl . '|' . $releaseDate),
            'etag' => null,
            'last_modified' => $releaseDate !== '' ? $releaseDate : null,
            'manifest_url' => $manifestUrl,
        ];
    }

    return null;
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
    $ok = db_sync_state_upsert(
        $datasetKey,
        $syncMode,
        'success',
        gmdate('Y-m-d H:i:s'),
        $cursor,
        $rowsWritten,
        $checksum,
        null
    );

    if ($ok) {
        supplycore_cache_invalidate_for_dataset($datasetKey);
    }

    return $ok;
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

function scheduler_timeout_env_key(string $jobKey): string
{
    $normalizedKey = strtoupper((string) preg_replace('/[^A-Za-z0-9]+/', '_', $jobKey));

    return 'SCHEDULER_TIMEOUT_' . trim($normalizedKey, '_');
}

function scheduler_job_timeout_seconds(string $jobKey, int $defaultSeconds = 300): int
{
    $fallback = max(30, min(3600, $defaultSeconds));
    $configuredDefault = max(30, min(3600, (int) config('scheduler.default_timeout_seconds', $fallback)));
    $envValue = getenv(scheduler_timeout_env_key($jobKey));

    if ($envValue === false || trim((string) $envValue) === '') {
        return $configuredDefault;
    }

    return max(30, min(3600, (int) $envValue));
}

function scheduler_job_lock_ttl_seconds(int $defaultSeconds, int $timeoutSeconds): int
{
    $baseLockTtl = max(30, min(3600, $defaultSeconds));
    $timeoutBackedTtl = min(3600, max($baseLockTtl, $timeoutSeconds + 60));

    return $timeoutBackedTtl;
}

function scheduler_job_runner_script_path(): string
{
    return dirname(__DIR__) . '/bin/scheduler_job_runner.php';
}

function scheduler_cron_log_path(): string
{
    return dirname(__DIR__) . '/storage/logs/cron.log';
}

function scheduler_job_runs_in_background(string $jobKey): bool
{
    $definitions = scheduler_job_definitions();
    $definition = $definitions[$jobKey] ?? null;

    return ($definition['execution'] ?? 'inline') === 'background';
}

function scheduler_dispatch_background_job(array $job): array
{
    $scheduleId = (int) ($job['id'] ?? 0);
    $jobKey = trim((string) ($job['job_key'] ?? ''));
    $jobType = scheduler_job_type($jobKey);
    $scheduledFor = (string) ($job['next_run_at'] ?? '');
    $startedAt = gmdate(DATE_ATOM);

    if ($scheduleId <= 0 || $jobKey === '') {
        throw new RuntimeException('Background scheduler dispatch requires a valid claimed job.');
    }

    $phpBinary = PHP_BINARY !== '' ? PHP_BINARY : 'php';
    $scriptPath = scheduler_job_runner_script_path();
    $logPath = scheduler_cron_log_path();
    $command = sprintf(
        '%s %s --schedule-id=%d >> %s 2>&1 &',
        escapeshellarg($phpBinary),
        escapeshellarg($scriptPath),
        $scheduleId,
        escapeshellarg($logPath)
    );

    exec($command, $output, $exitCode);
    if ($exitCode !== 0) {
        throw new RuntimeException('Failed to dispatch background job "' . $jobKey . '".');
    }

    return [
        'job_id' => $scheduleId,
        'job_key' => $jobKey,
        'job_type' => $jobType,
        'scheduled_for' => $scheduledFor,
        'started_at' => $startedAt,
        'finished_at' => null,
        'duration_ms' => 0,
        'status' => 'dispatched',
        'error' => null,
        'rows_seen' => 0,
        'rows_written' => 0,
        'warnings' => [],
        'meta' => [
            'execution' => 'background',
            'outcome_reason' => 'Job was dispatched to a background PHP worker so other due jobs can continue immediately.',
        ],
        'summary' => 'Dispatched to a background worker.',
    ];
}

function scheduler_background_dispatch_failure_result(array $job, Throwable $exception): array
{
    $jobKey = trim((string) ($job['job_key'] ?? ''));
    $scheduleId = (int) ($job['id'] ?? 0);
    $jobType = scheduler_job_type($jobKey);
    $scheduledFor = (string) ($job['next_run_at'] ?? '');
    $datasetKey = scheduler_job_dataset_key($jobKey !== '' ? $jobKey : 'unknown');
    $message = scheduler_normalize_error_message($exception->getMessage());

    mark_sync_failure($datasetKey, 'incremental', $message);
    if ($scheduleId > 0) {
        db_sync_schedule_mark_failure($scheduleId, $message);
    }

    return [
        'job_id' => $scheduleId,
        'job_key' => $jobKey,
        'job_type' => $jobType,
        'scheduled_for' => $scheduledFor,
        'started_at' => gmdate(DATE_ATOM),
        'finished_at' => gmdate(DATE_ATOM),
        'duration_ms' => 0,
        'status' => 'failed',
        'error' => $message,
        'rows_seen' => 0,
        'rows_written' => 0,
        'warnings' => [],
        'meta' => [
            'execution' => 'background',
        ],
        'summary' => $message,
    ];
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
            'timeout_seconds' => 3600,
            'lock_ttl_seconds' => 3900,
            'execution' => 'background',
            'handler' => static function (): array {
                $structureId = configured_structure_destination_id_for_esi_sync();
                if ($structureId <= 0) {
                    throw new RuntimeException('Alliance history sync skipped: choose an alliance structure destination (NPC stations are not eligible for structure sync).');
                }

                return sync_alliance_market_history($structureId, 'full');
            },
        ],
        'market_hub_historical_sync' => [
            'timeout_seconds' => 3600,
            'lock_ttl_seconds' => 3900,
            'execution' => 'background',
            'handler' => static function (): array {
                $hubRef = market_hub_setting_reference();
                if ($hubRef === '') {
                    throw new RuntimeException('Hub history sync skipped: configure a market hub/station first.');
                }

                return sync_market_hub_history($hubRef, 'full');
            },
        ],
        'market_hub_local_history_sync' => [
            'timeout_seconds' => 3600,
            'lock_ttl_seconds' => 3900,
            'execution' => 'background',
            'handler' => static function (): array {
                $hubRef = market_hub_setting_reference();
                if ($hubRef === '') {
                    throw new RuntimeException('Hub snapshot history sync skipped: configure a market hub/station first.');
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
        'current_state_refresh_sync' => [
            'timeout_seconds' => 90,
            'lock_ttl_seconds' => 180,
            'handler' => static function (): array {
                return supplycore_refresh_current_state_cache('scheduler');
            },
        ],
        'doctrine_intelligence_sync' => [
            'timeout_seconds' => 180,
            'lock_ttl_seconds' => 300,
            'handler' => static function (): array {
                return doctrine_refresh_intelligence_job_result('scheduler');
            },
        ],
        'market_comparison_summary_sync' => [
            'timeout_seconds' => 180,
            'lock_ttl_seconds' => 300,
            'handler' => static function (): array {
                return market_comparison_refresh_summary_job_result('scheduler');
            },
        ],
        'loss_demand_summary_sync' => [
            'timeout_seconds' => 180,
            'lock_ttl_seconds' => 300,
            'handler' => static function (): array {
                return loss_demand_refresh_summary_job_result('scheduler');
            },
        ],
        'dashboard_summary_sync' => [
            'timeout_seconds' => 180,
            'lock_ttl_seconds' => 300,
            'handler' => static function (): array {
                return dashboard_refresh_summary_job_result('scheduler');
            },
        ],
        'activity_priority_summary_sync' => [
            'timeout_seconds' => 180,
            'lock_ttl_seconds' => 300,
            'handler' => static function (): array {
                return activity_priority_refresh_summary_job_result('scheduler');
            },
        ],
        'analytics_bucket_1h_sync' => [
            'timeout_seconds' => 180,
            'lock_ttl_seconds' => 300,
            'handler' => static function (): array {
                return analytics_bucket_refresh_job_result('1h', 'scheduler');
            },
        ],
        'analytics_bucket_1d_sync' => [
            'timeout_seconds' => 240,
            'lock_ttl_seconds' => 360,
            'handler' => static function (): array {
                return analytics_bucket_refresh_job_result('1d', 'scheduler');
            },
        ],
        'rebuild_ai_briefings' => [
            'timeout_seconds' => 180,
            'lock_ttl_seconds' => 300,
            'execution' => 'background',
            'handler' => static function (): array {
                $runningHistoryJobs = scheduler_ai_briefing_running_history_jobs();
                if ($runningHistoryJobs !== []) {
                    $historyJobLabels = array_map(
                        static fn (string $jobKey): string => str_replace('_', ' ', $jobKey),
                        $runningHistoryJobs
                    );

                    return sync_result_shape() + [
                        'warnings' => ['AI briefing rebuild skipped while history sync is running: ' . implode(', ', $historyJobLabels) . '.'],
                        'meta' => [
                            'outcome_reason' => 'Doctrine AI briefings were deferred because a history sync job is still running.',
                            'blocking_jobs' => $runningHistoryJobs,
                        ],
                    ];
                }

                return rebuild_ai_briefings_job_result('scheduler');
            },
        ],
        'forecasting_ai_sync' => [
            'timeout_seconds' => 180,
            'lock_ttl_seconds' => 300,
            'execution' => 'background',
            'handler' => static function (): array {
                return supplycore_refresh_forecasting_snapshot_job_result('scheduler');
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
        'alliance_current_sync', 'market_hub_current_sync', 'current_state_refresh_sync' => 'sync.current',
        'alliance_historical_sync', 'market_hub_historical_sync', 'market_hub_local_history_sync' => 'sync.history',
        'doctrine_intelligence_sync' => 'sync.doctrine',
        'market_comparison_summary_sync' => 'sync.market_summary',
        'loss_demand_summary_sync' => 'sync.loss_demand',
        'dashboard_summary_sync' => 'sync.dashboard',
        'activity_priority_summary_sync' => 'sync.activity_priority',
        'analytics_bucket_1h_sync', 'analytics_bucket_1d_sync' => 'sync.analytics',
        'rebuild_ai_briefings' => 'sync.doctrine_ai',
        'forecasting_ai_sync' => 'sync.forecasting',
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

function scheduler_ensure_default_jobs_registered(): void
{
    foreach (data_sync_schedule_job_definitions() as $jobKey => $definition) {
        db_sync_schedule_ensure_job(
            $jobKey,
            1,
            (int) ($definition['default_interval_seconds'] ?? 300)
        );
    }
}

function scheduler_ai_briefing_blocking_job_keys(): array
{
    return [
        'alliance_historical_sync',
        'market_hub_historical_sync',
        'market_hub_local_history_sync',
    ];
}

function scheduler_ai_briefing_running_history_jobs(): array
{
    try {
        return db_sync_schedule_running_job_keys(scheduler_ai_briefing_blocking_job_keys());
    } catch (Throwable) {
        return [];
    }
}

function scheduler_due_jobs(): array
{
    scheduler_ensure_default_jobs_registered();

    $definitions = scheduler_job_definitions();
    $due = db_sync_schedule_fetch_due_jobs(20);
    $claimed = [];

    foreach ($due as $job) {
        $scheduleId = (int) ($job['id'] ?? 0);
        if ($scheduleId <= 0) {
            continue;
        }

        $jobKey = (string) ($job['job_key'] ?? '');
        $defaultTimeout = (int) ($definitions[$jobKey]['timeout_seconds'] ?? 300);
        $timeoutSeconds = scheduler_job_timeout_seconds($jobKey, $defaultTimeout);
        $defaultLockTtl = (int) ($definitions[$jobKey]['lock_ttl_seconds'] ?? 300);
        $lockTtl = scheduler_job_lock_ttl_seconds($defaultLockTtl, $timeoutSeconds);
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

    $timeoutSeconds = scheduler_job_timeout_seconds($jobKey, (int) ($definition['timeout_seconds'] ?? 300));
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
    $dispatchedCount = 0;

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

        try {
            $result = scheduler_job_runs_in_background($jobKey)
                ? scheduler_dispatch_background_job($job)
                : scheduler_run_job($job);
        } catch (Throwable $exception) {
            $result = scheduler_background_dispatch_failure_result($job, $exception);
        }
        $results[] = $result;

        if ($logger !== null) {
            $resultStatus = (string) ($result['status'] ?? 'failed');
            $jobEvent = match ($resultStatus) {
                'failed' => 'job.failed',
                'dispatched' => 'job.dispatched',
                default => 'job.completed',
            };
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

        if ($jobStatus === 'dispatched') {
            $dispatchedCount++;
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
        'jobs_dispatched' => $dispatchedCount,
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

function http_post_json(string $url, array $headers, array $payload, int $timeoutSeconds = 25): array
{
    $jsonPayload = json_encode($payload, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);
    if (!is_string($jsonPayload)) {
        throw new RuntimeException('Unable to encode JSON payload for ' . $url);
    }

    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_POST => true,
        CURLOPT_POSTFIELDS => $jsonPayload,
        CURLOPT_HTTPHEADER => array_merge(['Content-Type: application/json'], $headers),
        CURLOPT_TIMEOUT => max(1, $timeoutSeconds),
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

function http_get_json(string $url, array $headers = [], int $timeoutSeconds = 25): array
{
    $ch = curl_init($url);
    $responseHeaders = [];
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_HTTPHEADER => $headers,
        CURLOPT_TIMEOUT => max(1, $timeoutSeconds),
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

function http_get_json_multi(array $requests, int $timeoutSeconds = 25): array
{
    if ($requests === []) {
        return [];
    }

    $multiHandle = curl_multi_init();
    if ($multiHandle === false) {
        throw new RuntimeException('Unable to initialize concurrent HTTP client.');
    }

    $handles = [];
    $responseHeadersByKey = [];

    try {
        foreach ($requests as $key => $request) {
            $requestKey = (string) $key;
            $url = trim((string) ($request['url'] ?? ''));
            if ($url === '') {
                continue;
            }

            $responseHeadersByKey[$requestKey] = [];
            $handle = curl_init($url);
            curl_setopt_array($handle, [
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_HTTPHEADER => is_array($request['headers'] ?? null) ? $request['headers'] : [],
                CURLOPT_TIMEOUT => max(1, $timeoutSeconds),
                CURLOPT_HEADERFUNCTION => static function ($curl, string $headerLine) use (&$responseHeadersByKey, $requestKey): int {
                    $trimmed = trim($headerLine);
                    if ($trimmed === '' || !str_contains($trimmed, ':')) {
                        return strlen($headerLine);
                    }

                    [$name, $value] = explode(':', $trimmed, 2);
                    $normalizedName = mb_strtolower(trim($name));
                    $normalizedValue = trim($value);
                    if (!array_key_exists($normalizedName, $responseHeadersByKey[$requestKey])) {
                        $responseHeadersByKey[$requestKey][$normalizedName] = $normalizedValue;

                        return strlen($headerLine);
                    }

                    $existing = $responseHeadersByKey[$requestKey][$normalizedName];
                    if (is_array($existing)) {
                        $existing[] = $normalizedValue;
                        $responseHeadersByKey[$requestKey][$normalizedName] = $existing;

                        return strlen($headerLine);
                    }

                    $responseHeadersByKey[$requestKey][$normalizedName] = [$existing, $normalizedValue];

                    return strlen($headerLine);
                },
            ]);

            curl_multi_add_handle($multiHandle, $handle);
            $handles[$requestKey] = [
                'handle' => $handle,
                'url' => $url,
            ];
        }

        do {
            $status = curl_multi_exec($multiHandle, $active);
            if ($status > CURLM_OK) {
                break;
            }

            if ($active) {
                $selected = curl_multi_select($multiHandle, 1.0);
                if ($selected === -1) {
                    usleep(10000);
                }
            }
        } while ($active);

        $responses = [];

        foreach ($handles as $key => $entry) {
            $handle = $entry['handle'];
            $body = curl_multi_getcontent($handle);
            $status = (int) curl_getinfo($handle, CURLINFO_HTTP_CODE);
            $error = curl_error($handle);
            $decoded = null;
            $errorMessage = null;

            if ($body === false || $error !== '') {
                $errorMessage = 'HTTP request failed: ' . ($error !== '' ? $error : ('Unable to fetch ' . $entry['url']));
            } else {
                $decoded = json_decode($body, true);
                if (!is_array($decoded)) {
                    $errorMessage = 'Invalid JSON response from ' . $entry['url'];
                }
            }

            $responses[$key] = [
                'status' => $status,
                'json' => $decoded,
                'headers' => $responseHeadersByKey[$key] ?? [],
                'error' => $errorMessage,
            ];

            curl_multi_remove_handle($multiHandle, $handle);
            curl_close($handle);
        }

        return $responses;
    } finally {
        curl_multi_close($multiHandle);
    }
}

function http_get_json_multi_with_backoff(array $requests, int $maxAttempts = 4, int $baseDelayMs = 250, int $timeoutSeconds = 25): array
{
    if ($requests === []) {
        return [];
    }

    $attempts = max(1, $maxAttempts);
    $delayMs = max(50, $baseDelayMs);
    $pending = $requests;
    $resolved = [];

    for ($attempt = 1; $attempt <= $attempts; $attempt++) {
        $responses = http_get_json_multi($pending, $timeoutSeconds);
        $retry = [];

        foreach ($pending as $key => $request) {
            $response = $responses[(string) $key] ?? null;
            if (!is_array($response)) {
                $retry[$key] = $request;
                continue;
            }

            $status = (int) ($response['status'] ?? 500);
            $hasError = trim((string) ($response['error'] ?? '')) !== '';
            $shouldRetry = $hasError || in_array($status, sync_http_retryable_status_codes(), true);

            if ($shouldRetry && $attempt < $attempts) {
                $retry[$key] = $request;
                continue;
            }

            $resolved[(string) $key] = $response;
        }

        if ($retry === []) {
            return $resolved;
        }

        $pending = $retry;
        usleep((int) (($delayMs * (2 ** ($attempt - 1))) * 1000));
    }

    return $resolved;
}

function market_order_page_canonical_rows(
    array $payload,
    int $sourceId,
    string $observedAt,
    string $sourceType,
    ?int $locationIdFilter = null
): array {
    $rowsSeen = 0;
    $canonicalRows = [];

    foreach ($payload as $order) {
        if (!is_array($order)) {
            continue;
        }

        if ($locationIdFilter !== null && (int) ($order['location_id'] ?? 0) !== $locationIdFilter) {
            continue;
        }

        $rowsSeen++;
        $mapped = canonicalize_esi_market_order($order, $sourceId, $observedAt, $sourceType);
        if ($mapped !== null) {
            $canonicalRows[] = $mapped;
        }
    }

    return [
        'rows_seen' => $rowsSeen,
        'rows' => $canonicalRows,
    ];
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

function sync_dataset_key_alliance_structure_history_daily(int $structureId): string
{
    return 'alliance.structure.' . $structureId . '.history.daily';
}

function sync_dataset_key_market_hub_orders_history(string $hubKey): string
{
    return 'market.hub.' . $hubKey . '.orders.history';
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
    return market_snapshot_trade_date($observedAt);
}

function market_snapshot_trade_date(string $observedAt): string
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

function market_history_daily_point_from_snapshot_metric(array $metric, string $sourceType): ?array
{
    $normalizedSourceType = $sourceType === 'market_hub' ? 'market_hub' : 'alliance_structure';
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

    return [
        'source_type' => $normalizedSourceType,
        'source_id' => $sourceId,
        'type_id' => $typeId,
        'trade_date' => market_snapshot_trade_date($observedAt),
        'open_price' => $closePrice,
        'high_price' => $closePrice,
        'low_price' => $closePrice,
        'close_price' => $closePrice,
        'average_price' => $closePrice,
        'volume' => max(0, (int) ($metric['total_volume'] ?? 0)),
        'order_count' => max(0, (int) ($metric['buy_order_count'] ?? 0)) + max(0, (int) ($metric['sell_order_count'] ?? 0)),
        'source_label' => 'supplycore_snapshot',
        'observed_at' => $observedAt,
        '_average_sum' => $closePrice,
        '_average_count' => 1,
    ];
}

function market_history_daily_rebuild_from_snapshot_metrics(array $snapshotMetrics, string $sourceType): array
{
    $dailyBuckets = [];
    $snapshotCount = 0;
    $tradeDates = [];

    foreach ($snapshotMetrics as $metric) {
        if (!is_array($metric)) {
            continue;
        }

        $snapshotRow = market_history_daily_point_from_snapshot_metric($metric, $sourceType);
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
            $dailyBuckets[$bucketKey] = $snapshotRow;
            continue;
        }

        $bucket = $dailyBuckets[$bucketKey];
        $closePrice = (float) ($snapshotRow['close_price'] ?? 0);
        $bucket['high_price'] = max((float) ($bucket['high_price'] ?? $closePrice), $closePrice);
        $bucket['low_price'] = min((float) ($bucket['low_price'] ?? $closePrice), $closePrice);
        $bucket['_average_sum'] = (float) ($bucket['_average_sum'] ?? 0.0) + $closePrice;
        $bucket['_average_count'] = (int) ($bucket['_average_count'] ?? 0) + 1;

        $bucketObservedAt = trim((string) ($bucket['observed_at'] ?? ''));
        $rowObservedAt = trim((string) ($snapshotRow['observed_at'] ?? ''));
        if ($bucketObservedAt === '' || ($rowObservedAt !== '' && strcmp($rowObservedAt, $bucketObservedAt) >= 0)) {
            $bucket['close_price'] = $closePrice;
            $bucket['volume'] = max(0, (int) ($snapshotRow['volume'] ?? 0));
            $bucket['order_count'] = max(0, (int) ($snapshotRow['order_count'] ?? 0));
            $bucket['observed_at'] = $rowObservedAt;
            $bucket['source_label'] = (string) ($snapshotRow['source_label'] ?? 'supplycore_snapshot');
        }

        $dailyBuckets[$bucketKey] = $bucket;
    }

    uasort($dailyBuckets, static function (array $left, array $right): int {
        $dateCompare = strcmp((string) ($left['trade_date'] ?? ''), (string) ($right['trade_date'] ?? ''));
        if ($dateCompare !== 0) {
            return $dateCompare;
        }

        return ((int) ($left['type_id'] ?? 0)) <=> ((int) ($right['type_id'] ?? 0));
    });

    $rows = [];
    foreach ($dailyBuckets as $bucket) {
        $averageCount = max(1, (int) ($bucket['_average_count'] ?? 1));
        $bucket['average_price'] = round(((float) ($bucket['_average_sum'] ?? (float) ($bucket['close_price'] ?? 0.0))) / $averageCount, 2);
        unset($bucket['_average_sum'], $bucket['_average_count']);
        $rows[] = $bucket;
    }

    return [
        'rows' => $rows,
        'snapshot_metric_rows' => $snapshotCount,
        'trade_dates' => array_keys($tradeDates),
    ];
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
    $datasetKeyCurrent = sync_dataset_key_market_hub_current_orders($hubKey);
    $datasetKeyHistory = sync_dataset_key_market_hub_orders_history($hubKey);
    $runIdCurrent = db_sync_run_start($datasetKeyCurrent, $syncMode, sync_watermark($datasetKeyCurrent));
    $runIdHistory = db_sync_run_start($datasetKeyHistory, $syncMode, sync_watermark($datasetKeyHistory));
    $currentFinished = false;
    $historyFinished = false;

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
            $requestHeaders = esi_market_request_headers([
                'Authorization: Bearer ' . $accessToken,
            ]);
            $parallelPagesPerBatch = 6;

            $endpoint = 'https://esi.evetech.net/latest/markets/structures/' . $structureId . '/?page=' . $page;
            $response = http_get_json_with_backoff($endpoint, $requestHeaders);
            $status = (int) ($response['status'] ?? 500);

            if ($status >= 400) {
                throw new RuntimeException('ESI structure market sync failed on page ' . $page . ' with status ' . $status . '.');
            }

            $payload = $response['json'] ?? [];
            if (!is_array($payload)) {
                throw new RuntimeException('ESI structure market page ' . $page . ' returned a non-array payload.');
            }

            $pagesProcessed++;
            $processedPage = market_order_page_canonical_rows($payload, $sourceId, $observedAt, 'market_hub');
            $result['rows_seen'] += (int) ($processedPage['rows_seen'] ?? 0);
            $canonicalRows = array_merge($canonicalRows, $processedPage['rows'] ?? []);

            $pagesHeader = $response['headers']['x-pages'] ?? '1';
            if (is_array($pagesHeader)) {
                $pagesHeader = end($pagesHeader);
            }

            $maxPages = max(1, (int) $pagesHeader);
            $page++;

            while ($page <= $maxPages) {
                $batchEndPage = min($maxPages, $page + $parallelPagesPerBatch - 1);
                $requests = [];

                for ($batchPage = $page; $batchPage <= $batchEndPage; $batchPage++) {
                    $requests[$batchPage] = [
                        'url' => 'https://esi.evetech.net/latest/markets/structures/' . $structureId . '/?page=' . $batchPage,
                        'headers' => $requestHeaders,
                    ];
                }

                $responses = http_get_json_multi_with_backoff($requests);
                ksort($responses, SORT_NUMERIC);

                foreach ($responses as $batchPage => $batchResponse) {
                    $batchStatus = (int) ($batchResponse['status'] ?? 500);
                    $batchError = trim((string) ($batchResponse['error'] ?? ''));
                    if ($batchError !== '') {
                        throw new RuntimeException('ESI structure market sync failed on page ' . $batchPage . ': ' . $batchError);
                    }

                    if ($batchStatus >= 400) {
                        throw new RuntimeException('ESI structure market sync failed on page ' . $batchPage . ' with status ' . $batchStatus . '.');
                    }

                    $batchPayload = $batchResponse['json'] ?? [];
                    if (!is_array($batchPayload)) {
                        throw new RuntimeException('ESI structure market page ' . $batchPage . ' returned a non-array payload.');
                    }

                    $pagesProcessed++;
                    $processedPage = market_order_page_canonical_rows($batchPayload, $sourceId, $observedAt, 'market_hub');
                    $result['rows_seen'] += (int) ($processedPage['rows_seen'] ?? 0);
                    $canonicalRows = array_merge($canonicalRows, $processedPage['rows'] ?? []);
                }

                $page = $batchEndPage + 1;
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
            $requestHeaders = esi_market_request_headers();
            $parallelPagesPerBatch = 6;

            $endpoint = 'https://esi.evetech.net/latest/markets/' . $regionId . '/orders/?order_type=all&page=' . $page;
            $response = http_get_json_with_backoff($endpoint, $requestHeaders);
            $status = (int) ($response['status'] ?? 500);

            if ($status >= 400) {
                throw new RuntimeException('ESI region market sync failed on page ' . $page . ' with status ' . $status . '.');
            }

            $payload = $response['json'] ?? [];
            if (!is_array($payload)) {
                throw new RuntimeException('ESI region market page ' . $page . ' returned a non-array payload.');
            }

            $pagesProcessed++;
            $processedPage = market_order_page_canonical_rows($payload, $sourceId, $observedAt, 'market_hub', $stationId);
            $result['rows_seen'] += (int) ($processedPage['rows_seen'] ?? 0);
            $canonicalRows = array_merge($canonicalRows, $processedPage['rows'] ?? []);

            $pagesHeader = $response['headers']['x-pages'] ?? '1';
            if (is_array($pagesHeader)) {
                $pagesHeader = end($pagesHeader);
            }

            $maxPages = max(1, (int) $pagesHeader);
            $page++;

            while ($page <= $maxPages) {
                $batchEndPage = min($maxPages, $page + $parallelPagesPerBatch - 1);
                $requests = [];

                for ($batchPage = $page; $batchPage <= $batchEndPage; $batchPage++) {
                    $requests[$batchPage] = [
                        'url' => 'https://esi.evetech.net/latest/markets/' . $regionId . '/orders/?order_type=all&page=' . $batchPage,
                        'headers' => $requestHeaders,
                    ];
                }

                $responses = http_get_json_multi_with_backoff($requests);
                ksort($responses, SORT_NUMERIC);

                foreach ($responses as $batchPage => $batchResponse) {
                    $batchStatus = (int) ($batchResponse['status'] ?? 500);
                    $batchError = trim((string) ($batchResponse['error'] ?? ''));
                    if ($batchError !== '') {
                        throw new RuntimeException('ESI region market sync failed on page ' . $batchPage . ': ' . $batchError);
                    }

                    if ($batchStatus >= 400) {
                        throw new RuntimeException('ESI region market sync failed on page ' . $batchPage . ' with status ' . $batchStatus . '.');
                    }

                    $batchPayload = $batchResponse['json'] ?? [];
                    if (!is_array($batchPayload)) {
                        throw new RuntimeException('ESI region market page ' . $batchPage . ' returned a non-array payload.');
                    }

                    $pagesProcessed++;
                    $processedPage = market_order_page_canonical_rows($batchPayload, $sourceId, $observedAt, 'market_hub', $stationId);
                    $result['rows_seen'] += (int) ($processedPage['rows_seen'] ?? 0);
                    $canonicalRows = array_merge($canonicalRows, $processedPage['rows'] ?? []);
                }

                $page = $batchEndPage + 1;
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
            sync_run_finalize_success($runIdCurrent, $datasetKeyCurrent, $syncMode, $result['rows_seen'], 0, $result['cursor'], $result['checksum']);
            $currentFinished = true;
            sync_run_finalize_success($runIdHistory, $datasetKeyHistory, $syncMode, $result['rows_seen'], 0, $result['cursor'], $result['checksum']);
            $historyFinished = true;

            return $result;
        }

        $writtenCurrent = db_market_orders_current_bulk_upsert($canonicalRows);
        sync_run_finalize_success($runIdCurrent, $datasetKeyCurrent, $syncMode, $result['rows_seen'], $writtenCurrent, $result['cursor'], $result['checksum']);
        $currentFinished = true;

        $writtenHistory = db_market_orders_history_bulk_insert($canonicalRows);
        sync_run_finalize_success($runIdHistory, $datasetKeyHistory, $syncMode, $result['rows_seen'], $writtenHistory, $result['cursor'], $result['checksum']);
        $historyFinished = true;

        $result['rows_written'] = $writtenCurrent + $writtenHistory;
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

function sync_market_history_from_snapshots(
    string $sourceType,
    int $sourceId,
    string $datasetKey,
    string $runMode,
    ?int $windowDays,
    array $context,
    string $syncLabel
): array {
    $result = sync_result_shape();
    $syncMode = sync_mode_normalize($runMode);
    $normalizedSourceType = $sourceType === 'market_hub' ? 'market_hub' : 'alliance_structure';
    $safeWindowDays = market_hub_local_history_window_days_normalize($windowDays);
    $windowStartObservedAt = market_hub_local_history_window_start_observed_at($safeWindowDays);
    $runId = db_sync_run_start($datasetKey, $syncMode, sync_watermark($datasetKey));

    try {
        if ($sourceId <= 0) {
            $sourceName = (string) ($context['source_name'] ?? $context['reference_market_hub'] ?? $context['operational_market'] ?? ('Source #' . $sourceId));
            $warning = $syncLabel . ' skipped: could not resolve a valid local source id for ' . $sourceName . '.';
            $result['warnings'][] = $warning;
            $result['cursor'] = 'source_type:' . $normalizedSourceType . ';state:missing_source_id';
            $result['checksum'] = sync_checksum([
                'source_type' => $normalizedSourceType,
                'source_id' => $sourceId,
                'status' => 'missing_source_id',
            ]);
            $result['meta'] = $context + [
                'source_type' => $normalizedSourceType,
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
                'outcome_reason' => 'missing_source_id',
            ];
            sync_run_finalize_success($runId, $datasetKey, $syncMode, 0, 0, $result['cursor'], $result['checksum']);

            return $result;
        }

        $snapshotMetricResult = db_market_orders_snapshot_metrics_window_ensure_summary($normalizedSourceType, $sourceId, $windowStartObservedAt);
        $snapshotMetrics = is_array($snapshotMetricResult['rows'] ?? null) ? $snapshotMetricResult['rows'] : [];
        $summaryRowsWritten = max(0, (int) ($snapshotMetricResult['summary_rows_written'] ?? 0));
        $result['rows_seen'] = count($snapshotMetrics);

        if ($snapshotMetrics === []) {
            $sourceName = (string) ($context['source_name'] ?? $context['reference_market_hub'] ?? $context['operational_market'] ?? ('Source #' . $sourceId));
            $warning = $syncLabel . ' found no local raw order snapshots in the last ' . $safeWindowDays . ' day(s) for ' . $sourceName . '. Run the matching current sync first or widen --window-days.';
            $result['warnings'][] = $warning;
            $result['cursor'] = 'source_type:' . $normalizedSourceType . ';source_id:' . $sourceId . ';state:awaiting_local_snapshots';
            $result['checksum'] = sync_checksum([
                'source_type' => $normalizedSourceType,
                'source_id' => $sourceId,
                'window_days' => $safeWindowDays,
                'status' => 'awaiting_local_snapshots',
            ]);
            $result['meta'] = $context + [
                'source_type' => $normalizedSourceType,
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

        $rebuilt = market_history_daily_rebuild_from_snapshot_metrics($snapshotMetrics, $normalizedSourceType);
        $canonicalRows = is_array($rebuilt['rows'] ?? null) ? $rebuilt['rows'] : [];
        $historyRowCount = count($canonicalRows);
        $tradeDates = is_array($rebuilt['trade_dates'] ?? null) ? $rebuilt['trade_dates'] : [];
        $latestObservedAt = $canonicalRows === [] ? '' : (string) ($canonicalRows[array_key_last($canonicalRows)]['observed_at'] ?? '');

        if ($canonicalRows === []) {
            $sourceName = (string) ($context['source_name'] ?? $context['reference_market_hub'] ?? $context['operational_market'] ?? ('Source #' . $sourceId));
            $warning = $syncLabel . ' could not derive any daily rows from the local raw order snapshots for ' . $sourceName . '.';
            $result['warnings'][] = $warning;
            $result['cursor'] = 'source_type:' . $normalizedSourceType . ';source_id:' . $sourceId . ';window_days:' . $safeWindowDays . ';state:no_canonical_rows';
            $result['checksum'] = sync_checksum([
                'source_type' => $normalizedSourceType,
                'source_id' => $sourceId,
                'window_days' => $safeWindowDays,
                'status' => 'no_canonical_rows',
            ]);
            $result['meta'] = $context + [
                'source_type' => $normalizedSourceType,
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

        $result['rows_written'] = db_market_history_daily_bulk_upsert($canonicalRows);
        $result['cursor'] = 'source_type:' . $normalizedSourceType . ';source_id:' . $sourceId . ';window_days:' . $safeWindowDays . ';observed_at:' . $latestObservedAt;
        $result['checksum'] = sync_checksum($canonicalRows);
        $result['meta'] = $context + [
            'source_type' => $normalizedSourceType,
            'source_id' => $sourceId,
            'window_days' => $safeWindowDays,
            'window_start_observed_at' => $windowStartObservedAt,
            'observed_at' => $latestObservedAt !== '' ? $latestObservedAt : null,
            'records_fetched' => (int) $result['rows_seen'],
            'records_inserted' => 0,
            'records_updated' => (int) $result['rows_written'],
            'records_skipped' => max(0, $historyRowCount - (int) $result['rows_written']),
            'records_deleted' => 0,
            'history_rows_generated' => $historyRowCount,
            'snapshot_summary_rows_written' => $summaryRowsWritten,
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
        $result['warnings'][] = $syncLabel . ' failed: ' . $exception->getMessage();

        return $result;
    }
}

function sync_alliance_market_history(int $structureId, string $runMode = 'incremental', ?int $windowDays = null): array
{
    if ($structureId <= 0) {
        return [
            'rows_seen' => 0,
            'rows_written' => 0,
            'warnings' => ['Alliance history sync skipped: choose an alliance structure destination first.'],
            'meta' => [],
            'cursor' => null,
            'checksum' => null,
        ];
    }

    return sync_market_history_from_snapshots(
        'alliance_structure',
        $structureId,
        sync_dataset_key_alliance_structure_history_daily($structureId),
        $runMode,
        $windowDays,
        [
            'operational_market_id' => $structureId,
            'operational_market' => selected_station_name('alliance_station_id') ?? ('Structure ' . $structureId),
            'source_name' => selected_station_name('alliance_station_id') ?? ('Structure ' . $structureId),
        ],
        'Alliance history sync'
    );
}

function sync_market_hub_history(string|int $hubRef, string $runMode = 'incremental', ?int $windowDays = null): array
{
    $hubKey = trim((string) $hubRef);
    if ($hubKey === '') {
        return [
            'rows_seen' => 0,
            'rows_written' => 0,
            'warnings' => ['Hub history sync skipped: configure a market hub/station first.'],
            'meta' => [],
            'cursor' => null,
            'checksum' => null,
        ];
    }

    $hubContext = market_hub_reference_context($hubKey);
    $sourceId = sync_source_id_from_hub_ref($hubKey);

    return sync_market_history_from_snapshots(
        'market_hub',
        $sourceId,
        sync_dataset_key_market_hub_history_daily($hubKey),
        $runMode,
        $windowDays,
        [
            'reference_hub' => $hubKey,
            'reference_market_hub' => (string) ($hubContext['hub_name'] ?? market_hub_reference_name()),
            'selected_hub_type' => (string) ($hubContext['hub_type'] ?? 'unknown'),
            'effective_api_source' => (string) ($hubContext['api_source'] ?? 'unknown'),
            'resolved_region_id' => isset($hubContext['region_id']) && $hubContext['region_id'] !== null ? (int) $hubContext['region_id'] : null,
            'resolved_structure_id' => isset($hubContext['structure_id']) && $hubContext['structure_id'] !== null ? (int) $hubContext['structure_id'] : null,
            'source_name' => (string) ($hubContext['hub_name'] ?? market_hub_reference_name()),
        ],
        'Hub history sync'
    );
}

function sync_market_hub_local_history(string|int $hubRef, string $runMode = 'incremental', ?int $windowDays = null): array
{
    $hubKey = trim((string) $hubRef);
    if ($hubKey === '') {
        return [
            'rows_seen' => 0,
            'rows_written' => 0,
            'warnings' => ['Hub snapshot history sync skipped: configure a market hub/station first.'],
            'meta' => [],
            'cursor' => null,
            'checksum' => null,
        ];
    }

    $hubContext = market_hub_reference_context($hubKey);
    $sourceId = sync_source_id_from_hub_ref($hubKey);

    return sync_market_history_from_snapshots(
        'market_hub',
        $sourceId,
        sync_dataset_key_market_hub_local_history_daily($hubKey),
        $runMode,
        $windowDays,
        [
            'reference_hub' => $hubKey,
            'reference_market_hub' => (string) ($hubContext['hub_name'] ?? market_hub_reference_name()),
            'selected_hub_type' => (string) ($hubContext['hub_type'] ?? 'unknown'),
            'effective_api_source' => (string) ($hubContext['api_source'] ?? 'unknown'),
            'resolved_region_id' => isset($hubContext['region_id']) && $hubContext['region_id'] !== null ? (int) $hubContext['region_id'] : null,
            'resolved_structure_id' => isset($hubContext['structure_id']) && $hubContext['structure_id'] !== null ? (int) $hubContext['structure_id'] : null,
            'source_name' => (string) ($hubContext['hub_name'] ?? market_hub_reference_name()),
        ],
        'Hub snapshot history sync'
    );
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
    $updated = db_alliance_structure_metadata_upsert(
        $structureId,
        $name !== '' ? $name : null,
        gmdate('Y-m-d H:i:s')
    );
    if ($updated) {
        supplycore_cache_bust('metadata_structures');
    }

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

        if (db_alliance_structure_metadata_upsert($id, $name, gmdate('Y-m-d H:i:s'))) {
            supplycore_cache_bust('metadata_structures');
        }

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
    if (supplycore_redis_locking_enabled()) {
        $ttl = max(30, $timeoutSeconds > 0 ? $timeoutSeconds : 300);
        $token = supplycore_redis_lock_acquire('runner:' . $lockName, $ttl);
        if ($token !== null) {
            $GLOBALS['__supplycore_runner_lock_tokens'][$lockName] = $token;

            return true;
        }
    }

    return db_runner_lock_acquire($lockName, $timeoutSeconds);
}

function runner_lock_release(string $lockName): bool
{
    $token = $GLOBALS['__supplycore_runner_lock_tokens'][$lockName] ?? null;
    if (is_string($token) && $token !== '') {
        unset($GLOBALS['__supplycore_runner_lock_tokens'][$lockName]);

        return supplycore_redis_lock_release('runner:' . $lockName, $token);
    }

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
    $manifestUrl = static_data_manifest_url_for_source($sourceUrl);
    if ($manifestUrl !== null) {
        try {
            $manifestInfo = static_data_build_info_from_manifest($manifestUrl);
            if ($manifestInfo !== null) {
                return $manifestInfo;
            }
        } catch (Throwable) {
            // Fall back to archive headers for non-standard mirrors or manifest fetch failures.
        }
    }

    if (!function_exists('get_headers')) {
        throw new RuntimeException('Unable to fetch static-data metadata from source URL.');
    }

    $context = stream_context_create([
        'http' => [
            'method' => 'HEAD',
            'timeout' => 30,
            'ignore_errors' => true,
            'header' => "User-Agent: SupplyCore/1.0\r\nAccept: */*\r\n",
        ],
    ]);
    $headers = @get_headers($sourceUrl, true, $context);
    if ($headers === false && function_exists('curl_init')) {
        $ch = curl_init($sourceUrl);
        curl_setopt_array($ch, [
            CURLOPT_NOBODY => true,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_HEADER => true,
            CURLOPT_FOLLOWLOCATION => true,
            CURLOPT_CONNECTTIMEOUT => 10,
            CURLOPT_TIMEOUT => 30,
            CURLOPT_HTTPHEADER => ['Accept: */*', 'User-Agent: SupplyCore/1.0'],
        ]);
        $rawHeaders = curl_exec($ch);
        $status = (int) curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
        curl_close($ch);

        if (is_string($rawHeaders) && $rawHeaders !== '' && $status > 0 && $status < 400) {
            $headers = [];
            foreach (preg_split("/(?:\r\n|\n|\r)/", trim($rawHeaders)) as $line) {
                if (!str_contains($line, ':')) {
                    continue;
                }

                [$name, $value] = array_map('trim', explode(':', $line, 2));
                if ($name === '') {
                    continue;
                }

                if (isset($headers[$name])) {
                    $headers[$name] = array_merge((array) $headers[$name], [$value]);
                } else {
                    $headers[$name] = $value;
                }
            }
        }
    }

    if ($headers === false) {
        throw new RuntimeException('Unable to fetch static-data metadata from source URL.');
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
        $payload = static_data_fetch_binary_payload($sourceUrl);
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
        'categories.jsonl' => 'categories',
        'invcategories.jsonl' => 'categories',
        'groups.jsonl' => 'groups',
        'invgroups.jsonl' => 'groups',
        'marketgroups.jsonl' => 'market_groups',
        'metatypes.jsonl' => 'meta_types',
        'invmetatypes.jsonl' => 'meta_types',
        'metagroups.jsonl' => 'meta_groups',
        'invmetagroups.jsonl' => 'meta_groups',
        'types.jsonl' => 'types',
    ];

    $records = [
        'regions' => [],
        'constellations' => [],
        'systems' => [],
        'stations' => [],
        'categories' => [],
        'groups' => [],
        'market_groups' => [],
        'meta_types' => [],
        'meta_groups' => [],
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

            if ($targetKey === 'categories') {
                $categoryId = (int) static_data_record_value($row, ['categoryID', 'category_id', '_key']);
                $name = static_data_localized_text(static_data_record_value($row, ['categoryName', 'name']));
                if ($categoryId > 0 && $name !== null) {
                    $publishedRaw = static_data_record_value($row, ['published'], 1);
                    $records['categories'][] = [
                        'category_id' => $categoryId,
                        'category_name' => $name,
                        'published' => ($publishedRaw === true || (int) $publishedRaw === 1) ? 1 : 0,
                    ];
                }
                continue;
            }

            if ($targetKey === 'groups') {
                $groupId = (int) static_data_record_value($row, ['groupID', 'group_id', '_key']);
                $categoryId = (int) static_data_record_value($row, ['categoryID', 'category_id']);
                $name = static_data_localized_text(static_data_record_value($row, ['groupName', 'name']));
                if ($groupId > 0 && $categoryId > 0 && $name !== null) {
                    $publishedRaw = static_data_record_value($row, ['published'], 1);
                    $records['groups'][] = [
                        'group_id' => $groupId,
                        'category_id' => $categoryId,
                        'group_name' => $name,
                        'published' => ($publishedRaw === true || (int) $publishedRaw === 1) ? 1 : 0,
                    ];
                }
                continue;
            }

            if ($targetKey === 'meta_groups') {
                $metaGroupId = (int) static_data_record_value($row, ['metaGroupID', 'meta_group_id', '_key']);
                $name = static_data_localized_text(static_data_record_value($row, ['metaGroupName', 'name']));
                if ($metaGroupId > 0 && $name !== null) {
                    $records['meta_groups'][] = [
                        'meta_group_id' => $metaGroupId,
                        'meta_group_name' => $name,
                    ];
                }
                continue;
            }

            if ($targetKey === 'meta_types') {
                $typeId = (int) static_data_record_value($row, ['typeID', 'type_id']);
                $metaGroupId = (int) static_data_record_value($row, ['metaGroupID', 'meta_group_id']);
                if ($typeId > 0 && $metaGroupId > 0) {
                    $records['meta_types'][$typeId] = $metaGroupId;
                }
                continue;
            }

            if ($targetKey === 'types') {
                $typeId = (int) static_data_record_value($row, ['typeID', 'type_id', '_key']);
                $groupId = (int) static_data_record_value($row, ['groupID', 'group_id']);
                $name = static_data_localized_text(static_data_record_value($row, ['typeName', 'name']));
                if ($typeId > 0 && $groupId > 0 && $name !== null) {
                    $categoryId = (int) static_data_record_value($row, ['categoryID', 'category_id'], 0);
                    $marketGroupId = (int) static_data_record_value($row, ['marketGroupID', 'market_group_id', '_key'], 0);
                    $metaGroupId = (int) static_data_record_value($row, ['metaGroupID', 'meta_group_id'], (int) ($records['meta_types'][$typeId] ?? 0));
                    $publishedRaw = static_data_record_value($row, ['published'], 0);
                    $records['types'][] = [
                        'type_id' => $typeId,
                        'category_id' => $categoryId,
                        'group_id' => $groupId,
                        'market_group_id' => $marketGroupId > 0 ? $marketGroupId : null,
                        'meta_group_id' => $metaGroupId > 0 ? $metaGroupId : null,
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

    $groupCategoryById = [];
    foreach ($records['groups'] as $groupRow) {
        $groupId = (int) ($groupRow['group_id'] ?? 0);
        $categoryId = (int) ($groupRow['category_id'] ?? 0);
        if ($groupId > 0 && $categoryId > 0) {
            $groupCategoryById[$groupId] = $categoryId;
        }
    }

    foreach ($records['types'] as &$typeRow) {
        $categoryId = (int) ($typeRow['category_id'] ?? 0);
        if ($categoryId <= 0) {
            $typeRow['category_id'] = (int) ($groupCategoryById[(int) ($typeRow['group_id'] ?? 0)] ?? 0);
        }
    }
    unset($typeRow);

    $records['types'] = array_values(array_filter(
        $records['types'],
        static fn (array $row): bool => (int) ($row['category_id'] ?? 0) > 0
    ));

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
        $categories = $dataset['categories'];
        $groups = $dataset['groups'];
        $marketGroups = $dataset['market_groups'];
        $metaGroups = $dataset['meta_groups'];
        $types = $dataset['types'];

        if ($mode === 'full') {
            db_reference_data_truncate_all();
        }

        $rowsWritten = db_transaction(static function () use ($regions, $constellations, $systems, $stations, $categories, $groups, $marketGroups, $metaGroups, $types): int {
            $written = 0;
            $written += db_ref_regions_bulk_upsert($regions);
            $written += db_ref_constellations_bulk_upsert($constellations);
            $written += db_ref_systems_bulk_upsert($systems);
            $written += db_ref_npc_stations_bulk_upsert($stations);
            $written += db_ref_item_categories_bulk_upsert($categories);
            $written += db_ref_item_groups_bulk_upsert($groups);
            $written += db_ref_market_groups_bulk_upsert($marketGroups);
            $written += db_ref_meta_groups_bulk_upsert($metaGroups);
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
                    'item_categories' => count($categories),
                    'item_groups' => count($groups),
                    'market_groups' => count($marketGroups),
                    'meta_groups' => count($metaGroups),
                    'item_types' => count($types),
                ],
            ], JSON_THROW_ON_ERROR)
        );

        supplycore_cache_invalidate_for_dataset('static_data.reference');

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

function doctrine_parse_group_csv(?string $csv): array
{
    if ($csv === null || trim($csv) === '') {
        return [];
    }

    return array_values(array_unique(array_filter(array_map('intval', explode(',', $csv)), static fn (int $id): bool => $id > 0)));
}

function doctrine_parse_group_names_csv(?string $csv): array
{
    if ($csv === null || trim($csv) === '') {
        return [];
    }

    return array_values(array_filter(array_map('trim', explode('||', $csv)), static fn (string $name): bool => $name !== ''));
}

function doctrine_group_options(): array
{
    try {
        return db_doctrine_groups_all();
    } catch (Throwable) {
        return [];
    }
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

function doctrine_normalize_label(string $value): string
{
    $clean = preg_replace('/\s+/', ' ', trim($value));

    return mb_strtolower((string) $clean);
}

function doctrine_html_xpath(string $html): ?DOMXPath
{
    if (trim($html) === '' || !class_exists(DOMDocument::class)) {
        return null;
    }

    $internalErrors = libxml_use_internal_errors(true);
    $document = new DOMDocument();
    $loaded = $document->loadHTML('<?xml encoding="utf-8" ?>' . $html, LIBXML_NOERROR | LIBXML_NOWARNING | LIBXML_NONET);
    libxml_clear_errors();
    libxml_use_internal_errors($internalErrors);

    if ($loaded !== true) {
        return null;
    }

    return new DOMXPath($document);
}

function doctrine_html_text(?DOMNode $node): string
{
    if (!$node instanceof DOMNode) {
        return '';
    }

    return trim(preg_replace('/\s+/', ' ', (string) $node->textContent) ?? '');
}

function doctrine_html_collect_texts(DOMXPath $xpath, array $queries, int $maxLength = 1000, ?DOMNode $contextNode = null): array
{
    $texts = [];

    foreach ($queries as $query) {
        $nodes = @$xpath->query($query, $contextNode);
        if (!$nodes instanceof DOMNodeList) {
            continue;
        }

        foreach ($nodes as $node) {
            $text = doctrine_html_text($node);
            if ($text === '') {
                continue;
            }

            $key = doctrine_normalize_label($text);
            if ($key === '') {
                continue;
            }

            $texts[$key] = mb_substr($text, 0, $maxLength);
        }
    }

    return array_values($texts);
}

function doctrine_html_collect_block_texts(DOMXPath $xpath, array $queries, int $maxLength = 20000, ?DOMNode $contextNode = null): array
{
    $texts = [];

    foreach ($queries as $query) {
        $nodes = @$xpath->query($query, $contextNode);
        if (!$nodes instanceof DOMNodeList) {
            continue;
        }

        foreach ($nodes as $node) {
            if (!$node instanceof DOMNode) {
                continue;
            }

            $text = trim(str_replace(["\r\n", "\r"], "\n", (string) $node->textContent));
            if ($text === '') {
                continue;
            }

            $key = doctrine_normalize_label(preg_replace('/\s+/', ' ', $text) ?? $text);
            if ($key === '') {
                continue;
            }

            $texts[$key] = mb_substr($text, 0, $maxLength);
        }
    }

    return array_values($texts);
}

function doctrine_html_first_text(DOMXPath $xpath, array $queries, ?DOMNode $contextNode = null): ?string
{
    foreach (doctrine_html_collect_texts($xpath, $queries, 1000, $contextNode) as $text) {
        if ($text !== '') {
            return $text;
        }
    }

    return null;
}

function doctrine_html_attr_candidates(DOMXPath $xpath, array $attributeNames): array
{
    $values = [];

    foreach ($attributeNames as $attributeName) {
        $nodes = @$xpath->query('//*[@' . $attributeName . ']');
        if (!$nodes instanceof DOMNodeList) {
            continue;
        }

        foreach ($nodes as $node) {
            if (!$node instanceof DOMElement) {
                continue;
            }

            $value = trim($node->getAttribute($attributeName));
            if ($value === '') {
                continue;
            }

            $values[] = $value;
        }
    }

    return $values;
}

function doctrine_extract_buyall_payload_from_html(string $html, ?DOMXPath $xpath = null): ?string
{
    $candidates = [];
    $xpath ??= doctrine_html_xpath($html);

    if ($xpath instanceof DOMXPath) {
        foreach (['data-clipboard-text', 'data-clipboard', 'data-copy', 'data-fit-buyall', 'data-buyall', 'data-raw-buyall'] as $attr) {
            foreach (doctrine_html_attr_candidates($xpath, [$attr]) as $value) {
                $candidates[] = html_entity_decode($value, ENT_QUOTES | ENT_HTML5);
            }
        }

        foreach (doctrine_html_collect_block_texts($xpath, [
            '//textarea[contains(@name, "buy") or contains(@id, "buy") or contains(@class, "buy")]',
            '//pre[contains(@class, "buy") or contains(@id, "buy")]',
            '//code[contains(@class, "buy") or contains(@id, "buy")]',
            '//textarea',
            '//pre',
            '//code',
        ], 20000) as $value) {
            $candidates[] = html_entity_decode($value, ENT_QUOTES | ENT_HTML5);
        }
    }

    if (preg_match_all('/(?:clipboard|buyall|copy)[^>="\']{0,80}(?:text|payload|value)?[^"\']*["\']([^"\']*(?:\r?\n)[^"\']*)["\']/i', $html, $matches) === 1 || !empty($matches[1])) {
        foreach ((array) ($matches[1] ?? []) as $value) {
            $candidates[] = html_entity_decode((string) $value, ENT_QUOTES | ENT_HTML5);
        }
    }

    foreach ($candidates as $candidate) {
        $candidate = trim(str_replace(["\r\n", "\r"], "\n", (string) $candidate));
        if ($candidate === '' || substr_count($candidate, "\n") < 1) {
            continue;
        }

        $lines = array_values(array_filter(array_map('trim', explode("\n", $candidate)), static fn (string $line): bool => $line !== ''));
        if ($lines === []) {
            continue;
        }

        if (preg_match('/^\[[^\],]+\s*,\s*[^\]]+\]$/', $lines[0]) === 1) {
            continue;
        }

        return implode("\n", $lines);
    }

    return null;
}

function doctrine_extract_eft_payload_from_html(string $html, ?DOMXPath $xpath = null): ?string
{
    $xpath ??= doctrine_html_xpath($html);
    $candidates = [];

    if ($xpath instanceof DOMXPath) {
        foreach (['data-eft', 'data-clipboard-eft', 'data-raw-eft'] as $attr) {
            foreach (doctrine_html_attr_candidates($xpath, [$attr]) as $value) {
                $candidates[] = html_entity_decode($value, ENT_QUOTES | ENT_HTML5);
            }
        }

        foreach (doctrine_html_collect_block_texts($xpath, [
            '//textarea[contains(@name, "eft") or contains(@id, "eft") or contains(@class, "eft")]',
            '//pre[contains(@class, "eft") or contains(@id, "eft")]',
            '//code[contains(@class, "eft") or contains(@id, "eft")]',
            '//textarea',
            '//pre',
            '//code',
        ], 20000) as $value) {
            $candidates[] = html_entity_decode($value, ENT_QUOTES | ENT_HTML5);
        }
    }

    if (preg_match_all('/\[[^\],]+\s*,\s*[^\]]+\](?:\R.*)+/m', html_entity_decode($html, ENT_QUOTES | ENT_HTML5), $matches) === 1 || !empty($matches[0])) {
        foreach ((array) ($matches[0] ?? []) as $value) {
            $candidates[] = trim((string) $value);
        }
    }

    foreach ($candidates as $candidate) {
        $candidate = trim(str_replace(["\r\n", "\r"], "\n", (string) $candidate));
        if ($candidate !== '' && preg_match('/^\[[^\],]+\s*,\s*[^\]]+\]/', $candidate) === 1) {
            return $candidate;
        }
    }

    return null;
}

function doctrine_extract_html_fit_title(DOMXPath $xpath): string
{
    $candidates = doctrine_html_collect_texts($xpath, [
        '//*[@data-fit-title]',
        '//*[contains(@class, "fit-title")]',
        '//main//h1',
        '//main//h2',
        '//main//h3',
        '//main//h4',
        '//h1',
        '//h2',
        '//h3',
        '//h4[contains(@class, "modal-title")]',
        '//title',
    ], 255);

    $ignored = [
        'fit', 'fitting', 'fittings', 'fit information', 'fittings and doctrines',
        'view all fits', 'view all categories', 'copy fit (eft)', 'copy buy all',
        'save to eve', 'winter coalition - english auth', 'fit - winter coalition - english auth',
    ];

    foreach ($candidates as $candidate) {
        $normalized = doctrine_normalize_label($candidate);
        if ($normalized === '' || in_array($normalized, $ignored, true)) {
            continue;
        }

        if (mb_strlen($candidate) < 6 || mb_strlen($candidate) > 190) {
            continue;
        }

        return trim($candidate);
    }

    return '';
}

function doctrine_extract_html_group_labels(DOMXPath $xpath): array
{
    $labels = doctrine_html_collect_texts($xpath, [
        '//dt[contains(translate(normalize-space(.), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "doctrine")]/following-sibling::dd[1]//a',
        '//dt[contains(translate(normalize-space(.), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "doctrine")]/following-sibling::dd[1]//*[self::a or self::span or self::li]',
        '//*[self::h1 or self::h2 or self::h3 or self::h4][contains(translate(normalize-space(.), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "doctrine")]/following-sibling::*[1]//a',
        '//*[contains(@class, "breadcrumb")]//a',
        '//*[contains(@class, "group")]//a',
        '//*[contains(@class, "group")][self::a or self::span or self::div]',
        '//*[contains(@class, "group")]//*[self::a or self::span]',
        '//*[contains(@class, "tag")][self::a or self::span or self::div]',
        '//*[contains(@class, "tag")]//*[self::a or self::span]',
        '//*[contains(@class, "chip")][self::a or self::span or self::div]',
        '//*[contains(@class, "chip")]//*[self::a or self::span]',
        '//*[contains(@class, "badge")][self::a or self::span or self::div]',
        '//*[contains(@class, "badge")]//*[self::a or self::span]',
        '//a[contains(@href, "/doctrine/") or contains(@href, "/group/")]',
    ], 190);

    $ignored = [
        'buy all', 'copy buy all', 'copy eft', 'copy fit', 'back', 'doctrine fits',
        'fit', 'fitting', 'fittings', 'eft', 'copy', 'clipboard', 'doctrines',
        'fittings and doctrines', 'view all fits', 'view all categories', 'pk umbrella fits',
    ];

    return array_values(array_filter($labels, static function (string $label) use ($ignored): bool {
        $normalized = doctrine_normalize_label($label);
        if ($normalized === '' || in_array($normalized, $ignored, true)) {
            return false;
        }

        return mb_strlen($label) >= 3 && mb_strlen($label) <= 190;
    }));
}

function doctrine_extract_html_notes(DOMXPath $xpath): ?string
{
    $notes = doctrine_html_collect_texts($xpath, [
        '//*[contains(@class, "note") or contains(@class, "help") or contains(@class, "comment")]',
        '//*[contains(@data-role, "note") or contains(@data-role, "comment")]',
        '//section[contains(@class, "note")]//p',
        '//article[contains(@class, "note")]//p',
    ], 5000);

    if ($notes === []) {
        return null;
    }

    return trim(implode("\n\n", array_slice($notes, 0, 6)));
}

function doctrine_extract_html_visible_items(DOMXPath $xpath): array
{
    $items = [];
    $groupQueries = [
        '//*[contains(@class, "slot") and (self::section or self::div)]',
        '//*[contains(@class, "fit-items") and (self::section or self::div)]/*',
        '//*[contains(@class, "module-group")]',
    ];

    foreach ($groupQueries as $query) {
        $groups = @$xpath->query($query);
        if (!$groups instanceof DOMNodeList) {
            continue;
        }

        foreach ($groups as $groupNode) {
            $category = doctrine_html_first_text($xpath, [
                './/h2', './/h3', './/h4', './/*[@data-slot-group]', './/*[contains(@class, "title")]',
            ], $groupNode);
            $category = $category !== null ? mb_substr($category, 0, 80) : 'Items';

            $itemNodes = @$xpath->query('.//li|.//tr|.//*[contains(@class, "item")]', $groupNode);
            if (!$itemNodes instanceof DOMNodeList) {
                continue;
            }

            foreach ($itemNodes as $itemNode) {
                $text = doctrine_html_text($itemNode);
                if ($text === '' || mb_strlen($text) > 255) {
                    continue;
                }

                $parsed = doctrine_parse_quantity_and_name($text);
                $itemName = trim((string) ($parsed['item_name'] ?? ''));
                if ($itemName === '') {
                    continue;
                }

                $items[] = [
                    'line_number' => count($items) + 1,
                    'slot_category' => $category,
                    'source_role' => doctrine_source_role_from_category($category),
                    'item_name' => $itemName,
                    'quantity' => max(1, (int) ($parsed['quantity'] ?? 1)),
                ];
            }
        }

        if ($items !== []) {
            break;
        }
    }

    return $items;
}

function doctrine_source_role_from_category(string $category): string
{
    $normalized = doctrine_normalize_label($category);

    return match (true) {
        str_contains($normalized, 'drone') => 'drone',
        str_contains($normalized, 'cargo') || str_contains($normalized, 'ammo') => 'cargo',
        str_contains($normalized, 'implant') => 'implant',
        str_contains($normalized, 'booster') => 'booster',
        str_contains($normalized, 'subsystem') => 'subsystem',
        str_contains($normalized, 'service') => 'service',
        str_contains($normalized, 'hull') => 'hull',
        default => 'fit',
    };
}

function doctrine_parse_html_fit_page(string $html, string $sourceReference = ''): array
{
    $xpath = doctrine_html_xpath($html);
    if (!$xpath instanceof DOMXPath) {
        throw new RuntimeException('Uploaded HTML could not be parsed.');
    }

    $fitTitle = doctrine_extract_html_fit_title($xpath);

    $buyAllPayload = doctrine_extract_buyall_payload_from_html($html, $xpath);
    $eftPayload = doctrine_extract_eft_payload_from_html($html, $xpath);
    $groupLabels = doctrine_extract_html_group_labels($xpath);
    $notes = doctrine_extract_html_notes($xpath);
    $visibleItems = doctrine_extract_html_visible_items($xpath);

    $buyAllParsed = null;
    $eftParsed = null;
    $warnings = [];

    if ($buyAllPayload !== null) {
        try {
            $buyAllParsed = doctrine_parse_buyall($buyAllPayload);
        } catch (Throwable $exception) {
            $warnings[] = 'Buy All payload could not be parsed: ' . $exception->getMessage();
        }
    }

    if ($eftPayload !== null) {
        try {
            $eftParsed = doctrine_parse_eft($eftPayload);
        } catch (Throwable $exception) {
            $warnings[] = 'Embedded EFT payload could not be parsed: ' . $exception->getMessage();
        }
    }

    $candidateHulls = array_values(array_unique(array_filter([
        trim((string) ($buyAllParsed['ship_name'] ?? '')),
        trim((string) ($eftParsed['ship_name'] ?? '')),
        doctrine_html_first_text($xpath, [
            '//*[contains(@class, "ship")]//*[self::h1 or self::h2 or self::h3 or self::span]',
            '//*[@data-ship-name]',
            '//*[contains(@class, "hull")]//*[self::span or self::h2 or self::h3]',
        ]) ?? '',
    ])));

    $shipName = $candidateHulls[0] ?? '';
    if (count(array_map('doctrine_normalize_label', $candidateHulls)) > 1) {
        $normalizedHulls = array_values(array_unique(array_map('doctrine_normalize_label', $candidateHulls)));
        if (count($normalizedHulls) > 1) {
            $warnings[] = 'HTML and fallback sources disagree on hull name.';
        }
    }

    $fitName = trim((string) $fitTitle);
    if ($fitName === '') {
        $fitName = trim((string) ($eftParsed['fit_name'] ?? ''));
    }
    if ($fitName === '' && $buyAllParsed !== null) {
        $fitName = trim((string) ($buyAllParsed['fit_name'] ?? ''));
    }

    if ($fitName === '') {
        $fitName = pathinfo($sourceReference !== '' ? $sourceReference : 'Imported HTML Fit', PATHINFO_FILENAME);
    }

    $sourceFormat = $buyAllParsed !== null ? 'buyall' : ($eftParsed !== null ? 'eft' : 'buyall');
    $items = $buyAllParsed['items'] ?? ($eftParsed['items'] ?? $visibleItems);

    if ($buyAllParsed === null && $eftParsed === null && $visibleItems === []) {
        $warnings[] = 'No Buy All payload, embedded EFT, or visible item list was detected.';
    }

    return [
        'format' => $sourceFormat,
        'source_type' => 'html',
        'source_reference' => $sourceReference,
        'fit_name' => trim($fitName),
        'ship_name' => trim($shipName),
        'items' => is_array($items) ? $items : [],
        'notes' => $notes,
        'raw_html' => $html,
        'raw_buyall' => $buyAllPayload,
        'raw_eft' => $eftPayload,
        'group_labels' => $groupLabels,
        'visible_items' => $visibleItems,
        'warnings' => array_values(array_unique(array_filter($warnings))),
        'metadata' => [
            'fit_title' => $fitTitle,
            'candidate_hulls' => $candidateHulls,
            'group_labels' => $groupLabels,
            'visible_items' => $visibleItems,
        ],
    ];
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
            'source_role' => doctrine_source_role_from_category(doctrine_eft_category_label($blockIndex, $itemName)),
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
    $rawLines = array_values(array_filter(array_map(static fn (mixed $line): string => trim((string) $line), preg_split('/\R/', $text) ?: []), static fn (string $line): bool => $line !== ''));
    if ($rawLines === []) {
        throw new RuntimeException('BuyAll imports must include at least the ship hull on the first line.');
    }

    $shipName = $rawLines[0];
    $items = [];

    foreach ($rawLines as $index => $line) {
        $parsed = doctrine_parse_quantity_and_name($line);
        $itemName = trim((string) ($parsed['item_name'] ?? ''));
        if ($itemName === '') {
            continue;
        }

        $items[] = [
            'line_number' => count($items) + 1,
            'slot_category' => $index === 0 ? 'Hull' : 'Items',
            'source_role' => $index === 0 ? 'hull' : 'fit',
            'item_name' => $itemName,
            'quantity' => max(1, (int) ($parsed['quantity'] ?? 1)),
        ];
    }

    return [
        'format' => 'buyall',
        'fit_name' => $shipName !== '' ? ($shipName . ' BuyAll') : 'Imported BuyAll List',
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
        // Continue.
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
                }
                unset($lookupNames[$normalized]);
            }
        } catch (Throwable) {
        }
    }

    if ($lookupNames !== []) {
        foreach (doctrine_resolve_names_from_esi_ids(array_values($lookupNames)) as $normalized => $row) {
            $resolved[$normalized] = $row;
            try {
                db_item_name_cache_upsert($normalized, (string) ($row['item_name'] ?? $lookupNames[$normalized] ?? ''), (int) ($row['type_id'] ?? 0), 'esi');
            } catch (Throwable) {
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
            }
        }
    }

    return $resolved;
}

function doctrine_resolve_parsed_fit(array $parsed, string $rawText): array
{
    $hullStockTrackedOverride = null;
    if (array_key_exists('hull_is_stock_tracked', $parsed)) {
        $hullStockTrackedOverride = (bool) $parsed['hull_is_stock_tracked'];
    }

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

    $resolvedItems = doctrine_ensure_hull_item(
        $resolvedItems,
        $shipResolved,
        (string) ($parsed['ship_name'] ?? ''),
        $hullStockTrackedOverride
    );

    $hullItem = doctrine_find_hull_item($resolvedItems);
    $hullIsStockTracked = (bool) ($hullItem['is_stock_tracked'] ?? true);

    $fitName = trim((string) ($parsed['fit_name'] ?? ''));
    if ($fitName === '') {
        $fitName = trim((string) ($shipResolved['item_name'] ?? $parsed['ship_name'] ?? 'Imported Doctrine Fit'));
    }

    $shipMissing = (int) ($shipResolved['type_id'] ?? 0) <= 0;
    $warnings = array_values(array_unique(array_filter(array_map(
        static fn (mixed $warning): string => trim((string) $warning),
        (array) ($parsed['warnings'] ?? [])
    ))));
    $unresolved = array_values(array_unique(array_filter(array_merge(
        $shipMissing ? [trim((string) ($shipResolved['item_name'] ?? $parsed['ship_name'] ?? 'Unknown Hull'))] : [],
        $unresolvedItems
    ))));
    $status = doctrine_fit_status_from_warnings($warnings, $unresolved, (string) ($parsed['conflict_state'] ?? 'none'));
    $metadata = (array) ($parsed['metadata'] ?? []);
    $metadata['group_labels'] = array_values(array_unique(array_filter(array_map(
        static fn (mixed $value): string => trim((string) $value),
        (array) ($parsed['group_labels'] ?? [])
    ))));
    $metadata['source_reference'] = (string) ($parsed['source_reference'] ?? '');
    $metadata['detected_source_type'] = (string) ($parsed['source_type'] ?? 'manual');
    $fingerprintHash = doctrine_fit_item_fingerprint($resolvedItems);

    return [
        'fit' => [
            'fit_name' => mb_substr($fitName, 0, 190),
            'ship_name' => trim((string) ($shipResolved['item_name'] ?? $parsed['ship_name'] ?? 'Unknown Hull')),
            'ship_type_id' => isset($shipResolved['type_id']) && $shipResolved['type_id'] !== null ? (int) $shipResolved['type_id'] : null,
            'source_type' => (string) ($parsed['source_type'] ?? 'manual'),
            'source_format' => (string) ($parsed['format'] ?? doctrine_detect_format($rawText)),
            'source_reference' => ($parsed['source_reference'] ?? null) !== null ? mb_substr(trim((string) $parsed['source_reference']), 0, 255) : null,
            'notes' => ($parsed['notes'] ?? null) !== null ? trim((string) $parsed['notes']) : null,
            'import_body' => $rawText,
            'raw_html' => ($parsed['raw_html'] ?? null) !== null ? (string) $parsed['raw_html'] : null,
            'raw_buyall' => ($parsed['raw_buyall'] ?? null) !== null ? (string) $parsed['raw_buyall'] : null,
            'raw_eft' => ($parsed['raw_eft'] ?? null) !== null ? (string) $parsed['raw_eft'] : null,
            'metadata_json' => json_encode($metadata, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE),
            'parse_warnings_json' => json_encode($warnings, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE),
            'parse_status' => $status['parse_status'],
            'review_status' => $status['review_status'],
            'conflict_state' => (string) ($parsed['conflict_state'] ?? 'none'),
            'fingerprint_hash' => $fingerprintHash,
            'warning_count' => $status['warning_count'],
            'item_count' => count($resolvedItems),
            'unresolved_count' => count($unresolved),
        ],
        'items' => $resolvedItems,
        'ship' => $shipResolved,
        'unresolved' => $unresolved,
        'warnings' => $warnings,
        'metadata' => $metadata,
        'ship_missing' => $shipMissing,
        'hull_is_stock_tracked' => $hullIsStockTracked,
        'hull_tracking_default_reason' => doctrine_hull_stock_tracking_reason($shipResolved, $hullIsStockTracked),
    ];
}

function doctrine_default_hull_stock_tracking(array $shipResolved, string $fallbackShipName = ''): bool
{
    $defaultRule = trim((string) get_setting('doctrine.hull_stocking_default', 'exclude_supercapital_hulls'));
    if ($defaultRule === '' || $defaultRule === 'track_all_hulls') {
        return true;
    }

    $shipTypeId = isset($shipResolved['type_id']) && $shipResolved['type_id'] !== null ? (int) $shipResolved['type_id'] : 0;
    $shipName = trim((string) ($shipResolved['item_name'] ?? $fallbackShipName));
    $labels = [doctrine_normalize_item_name($shipName)];

    if ($shipTypeId > 0) {
        try {
            $metadataRows = db_ref_item_scope_metadata_by_ids([$shipTypeId]);
        } catch (Throwable) {
            $metadataRows = [];
        }

        $metadata = is_array($metadataRows[0] ?? null) ? $metadataRows[0] : [];
        foreach (['group_name', 'category_name', 'market_group_name', 'type_name'] as $field) {
            $labels[] = doctrine_normalize_item_name((string) ($metadata[$field] ?? ''));
        }
    }

    $joined = implode(' ', array_filter($labels));
    if ($joined === '') {
        return true;
    }

    return !(str_contains($joined, 'titan') || str_contains($joined, 'supercarrier'));
}

function doctrine_hull_stock_tracking_reason(array $shipResolved, bool $isStockTracked): string
{
    $shipName = trim((string) ($shipResolved['item_name'] ?? 'this hull'));

    if ($isStockTracked) {
        return $shipName !== ''
            ? $shipName . ' still contributes to alliance stocking recommendations.'
            : 'This hull still contributes to alliance stocking recommendations.';
    }

    return $shipName !== ''
        ? $shipName . ' is treated as an externally managed or specialty hull and is excluded from stocking urgency.'
        : 'This hull is treated as externally managed and excluded from stocking urgency.';
}

function doctrine_find_hull_item(array $items): ?array
{
    foreach ($items as $item) {
        if (strcasecmp((string) ($item['slot_category'] ?? ''), 'Hull') === 0) {
            return $item;
        }
    }

    return null;
}

function doctrine_ensure_hull_item(array $items, array $shipResolved, string $fallbackShipName = '', ?bool $hullIsStockTracked = null): array
{
    $shipName = trim((string) ($shipResolved['item_name'] ?? $fallbackShipName));
    if ($shipName === '') {
        return $items;
    }

    $shipKey = doctrine_normalize_item_name($shipName);
    $shipTypeId = isset($shipResolved['type_id']) && $shipResolved['type_id'] !== null ? (int) $shipResolved['type_id'] : null;
    $hullItem = null;
    $remainingItems = [];

    foreach ($items as $item) {
        $itemKey = doctrine_normalize_item_name((string) ($item['item_name'] ?? ''));
        $itemTypeId = isset($item['type_id']) && $item['type_id'] !== null ? (int) $item['type_id'] : null;
        $isHullCategory = strcasecmp((string) ($item['slot_category'] ?? ''), 'Hull') === 0;

        if ($isHullCategory || ($shipTypeId !== null && $itemTypeId === $shipTypeId) || ($shipKey !== '' && $itemKey === $shipKey)) {
            if ($hullItem === null) {
                $hullItem = $item;
            }
            continue;
        }

        $item['is_stock_tracked'] = !array_key_exists('is_stock_tracked', $item) || (bool) $item['is_stock_tracked'];
        $remainingItems[] = $item;
    }

    $resolvedTracking = $hullIsStockTracked ?? ($hullItem !== null && array_key_exists('is_stock_tracked', $hullItem)
        ? (bool) $hullItem['is_stock_tracked']
        : doctrine_default_hull_stock_tracking($shipResolved, $fallbackShipName));

    $hullItem = ($hullItem ?? []) + [
        'resolution_source' => (string) ($shipResolved['resolution_source'] ?? 'missing'),
    ];
    $hullItem['slot_category'] = 'Hull';
    $hullItem['source_role'] = 'hull';
    $hullItem['item_name'] = $shipName;
    $hullItem['type_id'] = $shipTypeId;
    $hullItem['quantity'] = 1;
    $hullItem['is_stock_tracked'] = $resolvedTracking;
    $hullItem['resolution_source'] = (string) ($hullItem['resolution_source'] ?? ($shipResolved['resolution_source'] ?? 'missing'));

    $items = array_merge([$hullItem], $remainingItems);

    foreach ($items as $index => &$item) {
        $item['line_number'] = $index + 1;
        $item['is_stock_tracked'] = array_key_exists('is_stock_tracked', $item) ? (bool) $item['is_stock_tracked'] : true;
        $item['source_role'] = (string) ($item['source_role'] ?? doctrine_source_role_from_category((string) ($item['slot_category'] ?? 'Items')));
    }
    unset($item);

    return $items;
}

function doctrine_fit_item_fingerprint(array $items): string
{
    $parts = [];

    foreach ($items as $item) {
        $name = doctrine_normalize_item_name((string) ($item['item_name'] ?? ''));
        if ($name === '') {
            continue;
        }

        $parts[] = implode(':', [
            (string) ($item['source_role'] ?? doctrine_source_role_from_category((string) ($item['slot_category'] ?? 'Items'))),
            doctrine_normalize_label((string) ($item['slot_category'] ?? 'Items')),
            (string) ((int) ($item['type_id'] ?? 0)),
            $name,
            (string) max(1, (int) ($item['quantity'] ?? 1)),
        ]);
    }

    sort($parts, SORT_STRING);

    return hash('sha256', implode('|', $parts));
}

function doctrine_fit_status_from_warnings(array $warnings, array $unresolved, string $conflictState = 'none'): array
{
    $needsReview = $warnings !== [] || $unresolved !== [] || $conflictState !== 'none';

    return [
        'parse_status' => $needsReview ? 'review' : 'ready',
        'review_status' => $needsReview ? 'needs_review' : 'clean',
        'warning_count' => count($warnings),
    ];
}

function doctrine_selected_group_ids(array $post): array
{
    $groupIds = [];
    foreach ((array) ($post['group_ids'] ?? []) as $groupId) {
        $id = (int) $groupId;
        if ($id > 0) {
            $groupIds[] = $id;
        }
    }

    $single = (int) ($post['group_id'] ?? 0);
    if ($single > 0) {
        $groupIds[] = $single;
    }

    return array_values(array_unique(array_filter($groupIds, static fn (int $id): bool => $id > 0)));
}

function doctrine_request_hull_stock_tracked(array $post): ?bool
{
    if (!array_key_exists('hull_stock_tracked', $post)) {
        return null;
    }

    return in_array((string) $post['hull_stock_tracked'], ['1', 'true', 'on', 'yes'], true);
}

function doctrine_render_editable_item_lines(array $items): string
{
    $lines = [];
    foreach ($items as $item) {
        $category = trim((string) ($item['slot_category'] ?? 'Items'));
        $quantity = max(1, (int) ($item['quantity'] ?? 1));
        $itemName = trim((string) ($item['item_name'] ?? ''));
        if ($itemName === '') {
            continue;
        }

        $prefix = $category !== '' ? ($category . ' :: ') : '';
        $suffix = $quantity !== 1 ? (' x' . $quantity) : '';
        $lines[] = $prefix . $itemName . $suffix;
    }

    return implode("\n", $lines);
}

function doctrine_parse_editable_item_lines(string $text, string $shipName = ''): array
{
    $items = [];
    foreach (preg_split('/\R/', $text) ?: [] as $rawLine) {
        $line = trim((string) $rawLine);
        if ($line === '') {
            continue;
        }

        $category = 'Items';
        $lineBody = $line;
        if (preg_match('/^(.*?)\s*::\s*(.+)$/', $line, $matches) === 1) {
            $category = trim($matches[1]) !== '' ? trim($matches[1]) : 'Items';
            $lineBody = trim($matches[2]);
        }

        $parsed = doctrine_parse_quantity_and_name($lineBody);
        $itemName = trim((string) ($parsed['item_name'] ?? ''));
        if ($itemName === '') {
            continue;
        }

        if (count($items) === 0 && doctrine_normalize_item_name($itemName) === doctrine_normalize_item_name($shipName)) {
            $category = 'Hull';
        }

        $items[] = [
            'line_number' => count($items) + 1,
            'slot_category' => mb_substr($category, 0, 80),
            'source_role' => doctrine_source_role_from_category($category),
            'item_name' => $itemName,
            'quantity' => max(1, (int) ($parsed['quantity'] ?? 1)),
        ];
    }

    return $items;
}

function doctrine_create_group_if_requested(array $post): int
{
    $newGroupNameRaw = trim((string) ($post['new_group_name'] ?? ''));
    if ($newGroupNameRaw === '') {
        return 0;
    }

    return db_doctrine_group_create(
        doctrine_sanitize_group_name($newGroupNameRaw),
        doctrine_sanitize_description($post['new_group_description'] ?? null)
    );
}

function doctrine_fit_group_suggestions(array $fit, int $excludeFitId = 0): array
{
    try {
        return db_doctrine_group_suggestions_for_fit(
            (string) ($fit['fit_name'] ?? ''),
            isset($fit['ship_type_id']) && $fit['ship_type_id'] !== null ? (int) $fit['ship_type_id'] : null,
            $excludeFitId
        );
    } catch (Throwable) {
        return [];
    }
}

function doctrine_prepare_fit_draft(array $resolved, array $groupIds = [], int $fitId = 0): array
{
    $fit = (array) ($resolved['fit'] ?? []);
    $items = (array) ($resolved['items'] ?? []);
    $suggestions = doctrine_fit_group_suggestions($fit, $fitId);
    $detectedLabels = array_values(array_unique(array_filter(array_map(
        'trim',
        (array) (($resolved['metadata'] ?? [])['group_labels'] ?? [])
    ))));
    $detectedGroupIds = [];
    foreach ($detectedLabels as $label) {
        foreach (doctrine_group_options() as $group) {
            if (doctrine_normalize_label((string) ($group['group_name'] ?? '')) === doctrine_normalize_label($label)) {
                $detectedGroupIds[] = (int) ($group['id'] ?? 0);
            }
        }
    }
    $suggestedGroupIds = array_values(array_map(static fn (array $row): int => (int) ($row['id'] ?? 0), $suggestions));
    $selectedGroupIds = array_values(array_unique(array_merge($groupIds, $detectedGroupIds, $suggestedGroupIds)));

    return [
        'fit_id' => $fitId,
        'fit' => $fit,
        'items' => $items,
        'item_lines_text' => doctrine_render_editable_item_lines($items),
        'group_ids' => $selectedGroupIds,
        'suggested_groups' => $suggestions,
        'unresolved' => array_values(array_unique((array) ($resolved['unresolved'] ?? []))),
        'warnings' => array_values(array_unique((array) ($resolved['warnings'] ?? []))),
        'detected_group_labels' => $detectedLabels,
        'metadata' => (array) ($resolved['metadata'] ?? []),
        'ship_missing' => (bool) ($resolved['ship_missing'] ?? false),
        'hull_is_stock_tracked' => (bool) ($resolved['hull_is_stock_tracked'] ?? true),
        'hull_tracking_default_reason' => (string) ($resolved['hull_tracking_default_reason'] ?? ''),
        'ready_to_save' => ((array) ($resolved['unresolved'] ?? [])) === [] && ((array) ($resolved['warnings'] ?? [])) === [] && trim((string) ($fit['ship_name'] ?? '')) !== '' && trim((string) ($fit['fit_name'] ?? '')) !== '',
    ];
}

function doctrine_build_draft_from_import_request(array $post): array
{
    $rawText = trim((string) ($post['fit_payload'] ?? ''));
    if ($rawText === '') {
        throw new RuntimeException('Paste a doctrine fit payload before importing.');
    }

    $parsed = doctrine_parse_import_text($rawText);
    $parsed['hull_is_stock_tracked'] = doctrine_request_hull_stock_tracked($post);
    $resolved = doctrine_resolve_parsed_fit($parsed, $rawText);

    return doctrine_prepare_fit_draft($resolved, doctrine_selected_group_ids($post));
}

function doctrine_build_draft_from_editor_request(array $post, int $fitId = 0): array
{
    $fitName = trim((string) ($post['fit_name'] ?? ''));
    $shipName = trim((string) ($post['ship_name'] ?? ''));
    $sourceFormat = in_array((string) ($post['source_format'] ?? ''), ['eft', 'buyall'], true) ? (string) $post['source_format'] : 'buyall';
    $rawText = trim((string) ($post['import_body'] ?? ''));
    $itemLinesText = trim((string) ($post['item_lines_text'] ?? ''));
    $items = doctrine_parse_editable_item_lines($itemLinesText, $shipName);

    if ($shipName === '') {
        throw new RuntimeException('Ship name is required before saving a doctrine fit.');
    }
    if ($fitName === '') {
        throw new RuntimeException('Fit name is required before saving a doctrine fit.');
    }
    if ($items === []) {
        throw new RuntimeException('Add at least one parsed item line before saving a doctrine fit.');
    }

    $parsed = [
        'format' => $sourceFormat,
        'fit_name' => $fitName,
        'ship_name' => $shipName,
        'items' => $items,
    ];
    $parsed['hull_is_stock_tracked'] = doctrine_request_hull_stock_tracked($post);
    $resolved = doctrine_resolve_parsed_fit($parsed, $rawText !== '' ? $rawText : $itemLinesText);
    $resolved['fit']['fit_name'] = mb_substr($fitName, 0, 190);
    $resolved['fit']['source_format'] = $sourceFormat;
    $resolved['fit']['import_body'] = $rawText !== '' ? $rawText : $itemLinesText;

    return doctrine_prepare_fit_draft($resolved, doctrine_selected_group_ids($post), $fitId);
}

function doctrine_attach_conflicts_to_draft(array $draft, int $excludeFitId = 0): array
{
    $fit = (array) ($draft['fit'] ?? []);

    try {
        $conflicts = db_doctrine_fit_conflicts(
            (string) ($fit['fit_name'] ?? ''),
            isset($fit['ship_type_id']) && $fit['ship_type_id'] !== null ? (int) $fit['ship_type_id'] : null,
            (string) ($fit['ship_name'] ?? ''),
            isset($fit['fingerprint_hash']) ? (string) $fit['fingerprint_hash'] : null,
            $excludeFitId
        );
    } catch (Throwable) {
        $conflicts = [];
    }

    $conflictState = 'none';
    foreach ($conflicts as $row) {
        $sameName = doctrine_normalize_label((string) ($row['fit_name'] ?? '')) === doctrine_normalize_label((string) ($fit['fit_name'] ?? ''))
            && doctrine_normalize_label((string) ($row['ship_name'] ?? '')) === doctrine_normalize_label((string) ($fit['ship_name'] ?? ''));
        $sameFingerprint = trim((string) ($row['fingerprint_hash'] ?? '')) !== ''
            && trim((string) ($row['fingerprint_hash'] ?? '')) === trim((string) ($fit['fingerprint_hash'] ?? ''));

        if ($sameName && $sameFingerprint) {
            $conflictState = 'duplicate_items';
            break;
        }

        if ($sameName) {
            $conflictState = 'duplicate_name';
        } elseif ($sameFingerprint) {
            $conflictState = 'version_conflict';
        }
    }

    if ($conflictState !== 'none') {
        $draft['warnings'][] = 'Potential duplicate or version conflict detected.';
        $draft['ready_to_save'] = false;
    }

    $draft['warnings'] = array_values(array_unique(array_filter(array_map('trim', (array) ($draft['warnings'] ?? [])))));
    $draft['conflicts'] = array_map(static function (array $row): array {
        $row['group_names'] = doctrine_parse_group_names_csv($row['group_names_csv'] ?? null);

        return $row;
    }, $conflicts);
    $draft['fit']['conflict_state'] = $conflictState;
    $draft['fit']['parse_status'] = (($draft['warnings'] ?? []) !== [] || ($draft['unresolved'] ?? []) !== [] || $conflictState !== 'none') ? 'review' : (string) (($draft['fit']['parse_status'] ?? 'ready'));
    $draft['fit']['review_status'] = (($draft['fit']['parse_status'] ?? 'ready') === 'review') ? 'needs_review' : 'clean';
    $draft['fit']['warning_count'] = count((array) ($draft['warnings'] ?? []));
    $draft['fit']['parse_warnings_json'] = json_encode((array) ($draft['warnings'] ?? []), JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);

    return $draft;
}

function doctrine_bulk_preview_session_key(): string
{
    return 'doctrine_bulk_import_preview';
}

function doctrine_bulk_import_preview_store(array $preview): void
{
    $_SESSION[doctrine_bulk_preview_session_key()] = $preview;
}

function doctrine_bulk_import_preview_fetch(): ?array
{
    $preview = $_SESSION[doctrine_bulk_preview_session_key()] ?? null;

    return is_array($preview) ? $preview : null;
}

function doctrine_bulk_import_preview_clear(): void
{
    unset($_SESSION[doctrine_bulk_preview_session_key()]);
}

function doctrine_uploaded_files(string $field): array
{
    $files = $_FILES[$field] ?? null;
    if (!is_array($files) || !isset($files['name']) || !is_array($files['name'])) {
        return [];
    }

    $rows = [];
    foreach ($files['name'] as $index => $name) {
        $error = (int) ($files['error'][$index] ?? UPLOAD_ERR_NO_FILE);
        if ($error !== UPLOAD_ERR_OK) {
            continue;
        }

        $tmpName = (string) ($files['tmp_name'][$index] ?? '');
        if ($tmpName === '' || !is_uploaded_file($tmpName)) {
            continue;
        }

        $rows[] = [
            'name' => (string) $name,
            'tmp_name' => $tmpName,
            'size' => (int) ($files['size'][$index] ?? 0),
            'type' => (string) ($files['type'][$index] ?? ''),
        ];
    }

    return $rows;
}

function doctrine_match_uploaded_eft_fallback(array $htmlParsed, array $eftDraftsByKey): ?array
{
    $keys = [
        doctrine_normalize_label((string) ($htmlParsed['source_reference'] ?? '')),
        doctrine_normalize_label(pathinfo((string) ($htmlParsed['source_reference'] ?? ''), PATHINFO_FILENAME)),
        doctrine_normalize_label((string) ($htmlParsed['fit_name'] ?? '')),
    ];

    foreach ($keys as $key) {
        if ($key !== '' && isset($eftDraftsByKey[$key]) && is_array($eftDraftsByKey[$key])) {
            return $eftDraftsByKey[$key];
        }
    }

    return null;
}

function doctrine_bulk_import_build_preview(): array
{
    $htmlFiles = doctrine_uploaded_files('html_files');
    $eftFiles = doctrine_uploaded_files('eft_files');

    if ($htmlFiles === [] && $eftFiles === []) {
        throw new RuntimeException('Upload at least one Winter Coalition HTML fit page or EFT fallback file.');
    }

    $eftDraftsByKey = [];
    foreach ($eftFiles as $file) {
        $body = (string) file_get_contents($file['tmp_name']);
        if (trim($body) === '') {
            continue;
        }

        $parsed = doctrine_parse_eft($body);
        $draft = doctrine_attach_conflicts_to_draft(
            doctrine_prepare_fit_draft(
                doctrine_resolve_parsed_fit([
                    'format' => 'eft',
                    'source_type' => 'eft',
                    'source_reference' => (string) ($file['name'] ?? ''),
                    'fit_name' => (string) ($parsed['fit_name'] ?? ''),
                    'ship_name' => (string) ($parsed['ship_name'] ?? ''),
                    'items' => (array) ($parsed['items'] ?? []),
                    'raw_eft' => $body,
                    'warnings' => [],
                    'metadata' => ['group_labels' => []],
                ], $body)
            )
        );

        $eftDraftsByKey[doctrine_normalize_label((string) ($file['name'] ?? ''))] = $draft;
        $eftDraftsByKey[doctrine_normalize_label(pathinfo((string) ($file['name'] ?? ''), PATHINFO_FILENAME))] = $draft;
        $eftDraftsByKey[doctrine_normalize_label((string) ($draft['fit']['fit_name'] ?? ''))] = $draft;
    }

    $rows = [];
    foreach ($htmlFiles as $index => $file) {
        $html = (string) file_get_contents($file['tmp_name']);
        if (trim($html) === '') {
            continue;
        }

        $parsed = doctrine_parse_html_fit_page($html, (string) ($file['name'] ?? ('fit-' . $index . '.html')));
        $eftFallback = doctrine_match_uploaded_eft_fallback($parsed, $eftDraftsByKey);

        if ($eftFallback !== null) {
            $fallbackFit = (array) ($eftFallback['fit'] ?? []);
            $htmlHull = doctrine_normalize_label((string) ($parsed['ship_name'] ?? ''));
            $eftHull = doctrine_normalize_label((string) ($fallbackFit['ship_name'] ?? ''));
            if ($htmlHull !== '' && $eftHull !== '' && $htmlHull !== $eftHull) {
                $parsed['warnings'][] = 'HTML and uploaded EFT fallback disagree on hull identity.';
                $parsed['conflict_state'] = 'source_mismatch';
            }

            if (trim((string) ($parsed['raw_eft'] ?? '')) === '') {
                $parsed['raw_eft'] = (string) ($fallbackFit['raw_eft'] ?? ($fallbackFit['import_body'] ?? ''));
            }

            if (($parsed['items'] ?? []) === [] && isset($eftFallback['items'])) {
                $parsed['items'] = (array) $eftFallback['items'];
                $parsed['format'] = 'eft';
            }
        }

        $resolved = doctrine_resolve_parsed_fit($parsed, (string) ($parsed['raw_buyall'] ?? $parsed['raw_eft'] ?? $html));
        $draft = doctrine_attach_conflicts_to_draft(doctrine_prepare_fit_draft($resolved));
        $draft['source_filename'] = (string) ($file['name'] ?? '');
        $draft['source_type_label'] = strtoupper((string) (($draft['fit']['source_type'] ?? 'html')));
        $rows[] = $draft;
    }

    if ($rows === [] && $eftFiles !== []) {
        foreach ($eftFiles as $file) {
            $body = (string) file_get_contents($file['tmp_name']);
            if (trim($body) === '') {
                continue;
            }

            $parsed = doctrine_parse_eft($body);
            $draft = doctrine_attach_conflicts_to_draft(
                doctrine_prepare_fit_draft(
                    doctrine_resolve_parsed_fit([
                        'format' => 'eft',
                        'source_type' => 'eft',
                        'source_reference' => (string) ($file['name'] ?? ''),
                        'fit_name' => (string) ($parsed['fit_name'] ?? ''),
                        'ship_name' => (string) ($parsed['ship_name'] ?? ''),
                        'items' => (array) ($parsed['items'] ?? []),
                        'raw_eft' => $body,
                        'warnings' => [],
                        'metadata' => ['group_labels' => []],
                    ], $body)
                )
            );
            $draft['source_filename'] = (string) ($file['name'] ?? '');
            $rows[] = $draft;
        }
    }

    return [
        'created_at' => gmdate('Y-m-d H:i:s'),
        'rows' => $rows,
        'counts' => [
            'total' => count($rows),
            'ready' => count(array_filter($rows, static fn (array $row): bool => (($row['fit']['parse_status'] ?? 'ready') === 'ready'))),
            'review' => count(array_filter($rows, static fn (array $row): bool => (($row['fit']['parse_status'] ?? 'ready') === 'review'))),
        ],
    ];
}

function doctrine_bulk_import_selected_group_ids(array $row, array $post): array
{
    $index = (string) ($row['source_filename'] ?? md5((string) (($row['fit']['fit_name'] ?? '') . '|' . ($row['fit']['ship_name'] ?? ''))));
    $posted = (array) ($post['row_group_ids'][$index] ?? []);
    $groupIds = array_values(array_unique(array_filter(array_map('intval', $posted), static fn (int $id): bool => $id > 0)));

    if ($groupIds !== []) {
        return $groupIds;
    }

    return array_values(array_unique(array_filter(array_map('intval', (array) ($row['group_ids'] ?? [])), static fn (int $id): bool => $id > 0)));
}

function doctrine_ensure_groups_from_labels(array $labels, array $selectedGroupIds = []): array
{
    $groupIds = array_values(array_unique(array_filter(array_map('intval', $selectedGroupIds), static fn (int $id): bool => $id > 0)));
    $existingByName = [];
    foreach (doctrine_group_options() as $group) {
        $existingByName[doctrine_normalize_label((string) ($group['group_name'] ?? ''))] = (int) ($group['id'] ?? 0);
    }

    foreach ($labels as $label) {
        $clean = doctrine_sanitize_group_name((string) $label);
        $key = doctrine_normalize_label($clean);
        if ($key === '') {
            continue;
        }

        if (isset($existingByName[$key]) && $existingByName[$key] > 0) {
            $groupIds[] = $existingByName[$key];
            continue;
        }

        $newId = db_doctrine_group_create($clean, null);
        $existingByName[$key] = $newId;
        $groupIds[] = $newId;
    }

    return array_values(array_unique(array_filter($groupIds, static fn (int $id): bool => $id > 0)));
}

function doctrine_bulk_import_save_from_request(array $post): array
{
    $preview = doctrine_bulk_import_preview_fetch();
    if (!is_array($preview) || !isset($preview['rows']) || !is_array($preview['rows'])) {
        return ['ok' => false, 'message' => 'The import preview expired. Upload the files again before saving.'];
    }

    $results = ['created' => 0, 'updated' => 0, 'skipped' => 0, 'review' => 0];

    foreach ($preview['rows'] as $row) {
        $sourceKey = (string) ($row['source_filename'] ?? md5((string) (($row['fit']['fit_name'] ?? '') . '|' . ($row['fit']['ship_name'] ?? ''))));
        $action = (string) (($post['row_action'][$sourceKey] ?? (($row['fit']['parse_status'] ?? 'ready') === 'ready' ? 'create' : 'review')));
        if ($action === 'skip') {
            $results['skipped']++;
            continue;
        }

        $groupIds = doctrine_ensure_groups_from_labels(
            (array) ($row['detected_group_labels'] ?? []),
            doctrine_bulk_import_selected_group_ids($row, $post)
        );

        $fit = (array) ($row['fit'] ?? []);
        $items = (array) ($row['items'] ?? []);
        $fit['conflict_state'] = (string) ($fit['conflict_state'] ?? 'none');

        if ($action === 'review') {
            $fit['parse_status'] = 'review';
            $fit['review_status'] = 'needs_review';
            $fit['warning_count'] = max((int) ($fit['warning_count'] ?? 0), count((array) ($row['warnings'] ?? [])));
            $fit['parse_warnings_json'] = json_encode((array) ($row['warnings'] ?? []), JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);
        }

        if ($action === 'update') {
            $targetId = (int) ($post['row_target_fit_id'][$sourceKey] ?? 0);
            if ($targetId <= 0) {
                $results['review']++;
                continue;
            }

            db_doctrine_fit_update($targetId, $fit, $items, $groupIds);
            $results['updated']++;
            continue;
        }

        db_doctrine_fit_create($fit, $items, $groupIds);
        if ($action === 'review') {
            $results['review']++;
        } else {
            $results['created']++;
        }
    }

    doctrine_bulk_import_preview_clear();
    doctrine_schedule_intelligence_refresh('bulk-fit-import');

    return ['ok' => true, 'message' => 'Bulk doctrine import processed.', 'results' => $results];
}

function doctrine_import_fit_from_request(array $post): array
{
    try {
        $draft = doctrine_attach_conflicts_to_draft(doctrine_build_draft_from_editor_request($post));
    } catch (Throwable $exception) {
        return ['ok' => false, 'message' => $exception->getMessage()];
    }

    $groupIds = doctrine_selected_group_ids($post);
    try {
        $newGroupId = doctrine_create_group_if_requested($post);
    } catch (Throwable $exception) {
        return ['ok' => false, 'message' => 'Unable to create doctrine group: ' . $exception->getMessage(), 'draft' => $draft];
    }
    if ($newGroupId > 0) {
        $groupIds[] = $newGroupId;
    }
    $groupIds = array_values(array_unique(array_filter($groupIds, static fn (int $id): bool => $id > 0)));

    if ($groupIds === []) {
        return ['ok' => false, 'message' => 'Assign the fit to at least one doctrine group before saving.', 'draft' => $draft];
    }

    try {
        $fitId = db_doctrine_fit_create((array) ($draft['fit'] ?? []), (array) ($draft['items'] ?? []), $groupIds);
    } catch (Throwable $exception) {
        return ['ok' => false, 'message' => 'Doctrine fit import failed: ' . $exception->getMessage(), 'draft' => $draft];
    }

    doctrine_schedule_intelligence_refresh('fit-import');

    return [
        'ok' => true,
        'message' => (($draft['fit']['parse_status'] ?? 'ready') === 'review')
            ? 'Doctrine fit saved and flagged for review.'
            : 'Doctrine fit imported successfully.',
        'fit_id' => $fitId,
        'draft' => $draft,
    ];
}

function doctrine_update_fit_from_request(int $fitId, array $post): array
{
    try {
        $draft = doctrine_attach_conflicts_to_draft(doctrine_build_draft_from_editor_request($post, $fitId), $fitId);
    } catch (Throwable $exception) {
        return ['ok' => false, 'message' => $exception->getMessage()];
    }

    $groupIds = doctrine_selected_group_ids($post);
    if ($groupIds === []) {
        return ['ok' => false, 'message' => 'Assign the fit to at least one doctrine group before saving.', 'draft' => $draft];
    }

    try {
        db_doctrine_fit_update($fitId, (array) ($draft['fit'] ?? []), (array) ($draft['items'] ?? []), $groupIds);
    } catch (Throwable $exception) {
        return ['ok' => false, 'message' => 'Doctrine fit update failed: ' . $exception->getMessage(), 'draft' => $draft];
    }

    doctrine_schedule_intelligence_refresh('fit-update');

    return [
        'ok' => true,
        'message' => (($draft['fit']['parse_status'] ?? 'ready') === 'review')
            ? 'Doctrine fit updated and remains flagged for review.'
            : 'Doctrine fit updated successfully.',
        'draft' => $draft,
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

function doctrine_market_lookup_by_type_ids(array $typeIds): array
{
    $typeIds = array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $id): bool => $id > 0)));
    if ($typeIds === []) {
        return [];
    }

    $comparisonRows = market_comparison_outcomes($typeIds)['rows'] ?? [];
    $lookup = [];
    foreach ($comparisonRows as $row) {
        $typeId = (int) ($row['type_id'] ?? 0);
        if ($typeId > 0) {
            $lookup[$typeId] = $row;
        }
    }

    return $lookup;
}

function doctrine_fit_item_market_rows(array $items, array $comparisonByTypeId = []): array
{
    $items = item_scope_filter_rows(
        $items,
        static fn (array $item): int => (int) ($item['type_id'] ?? 0)
    );

    if ($comparisonByTypeId === []) {
        $comparisonByTypeId = doctrine_market_lookup_by_type_ids(array_map(static fn (array $item): int => (int) ($item['type_id'] ?? 0), $items));
    }

    $rows = [];
    foreach ($items as $item) {
        $typeId = (int) ($item['type_id'] ?? 0);
        $market = $comparisonByTypeId[$typeId] ?? [];
        $requiredQty = max(1, (int) ($item['quantity'] ?? 1));
        $localQty = max(0, (int) ($market['alliance_total_sell_volume'] ?? 0));
        $missingQty = max(0, $requiredQty - $localQty);
        $coverageRatio = $requiredQty > 0 ? min(1.0, $localQty / $requiredQty) : 0.0;
        $status = $missingQty <= 0 ? 'ok' : ($coverageRatio >= 0.5 ? 'low' : 'missing');
        $hubPrice = isset($market['reference_best_sell_price']) ? (float) $market['reference_best_sell_price'] : null;

        $rows[] = $item + [
            'is_stock_tracked' => !array_key_exists('is_stock_tracked', $item) || (bool) $item['is_stock_tracked'],
            'local_available_qty' => $localQty,
            'missing_qty' => $missingQty,
            'local_price' => isset($market['alliance_best_sell_price']) ? (float) $market['alliance_best_sell_price'] : null,
            'hub_price' => $hubPrice,
            'restock_gap_isk' => (!array_key_exists('is_stock_tracked', $item) || (bool) $item['is_stock_tracked']) && $hubPrice !== null ? ($hubPrice * $missingQty) : null,
            'market_status' => (!array_key_exists('is_stock_tracked', $item) || (bool) $item['is_stock_tracked']) ? $status : 'external',
            'market_label' => (!array_key_exists('is_stock_tracked', $item) || (bool) $item['is_stock_tracked'])
                ? match ($status) {
                    'ok' => 'Stock OK',
                    'low' => 'Low',
                    default => 'Missing',
                }
                : 'Externally managed',
            'stocking_state_label' => (!array_key_exists('is_stock_tracked', $item) || (bool) $item['is_stock_tracked'])
                ? 'Tracked for stocking'
                : 'Externally managed',
            'stocking_note' => (!array_key_exists('is_stock_tracked', $item) || (bool) $item['is_stock_tracked'])
                ? ''
                : 'Specialty hull (excluded from stocking)',
            'observed_at' => $market['alliance_last_observed_at'] ?? null,
        ];
    }

    return $rows;
}

function doctrine_filter_stock_tracked_rows(array $rows): array
{
    return array_values(array_filter($rows, static fn (array $row): bool => !array_key_exists('is_stock_tracked', $row) || (bool) $row['is_stock_tracked']));
}

function doctrine_filter_stock_tracked_items(array $items): array
{
    return array_values(array_filter($items, static fn (array $item): bool => !array_key_exists('is_stock_tracked', $item) || (bool) $item['is_stock_tracked']));
}

function doctrine_fit_items_equal(array $before, array $after): bool
{
    $normalize = static function (array $items): array {
        return array_map(static function (array $item): array {
            return [
                'line_number' => (int) ($item['line_number'] ?? 0),
                'slot_category' => (string) ($item['slot_category'] ?? ''),
                'source_role' => (string) ($item['source_role'] ?? ''),
                'item_name' => (string) ($item['item_name'] ?? ''),
                'type_id' => isset($item['type_id']) && $item['type_id'] !== null ? (int) $item['type_id'] : null,
                'quantity' => (int) ($item['quantity'] ?? 0),
                'is_stock_tracked' => !array_key_exists('is_stock_tracked', $item) || (bool) $item['is_stock_tracked'],
                'resolution_source' => (string) ($item['resolution_source'] ?? ''),
            ];
        }, array_values($items));
    };

    return $normalize($before) === $normalize($after);
}

function doctrine_normalize_persisted_fit_items(array $fit, array $items, bool $persistChanges = true): array
{
    $shipResolved = [
        'item_name' => (string) ($fit['ship_name'] ?? ''),
        'type_id' => isset($fit['ship_type_id']) && $fit['ship_type_id'] !== null ? (int) $fit['ship_type_id'] : null,
        'resolution_source' => ((int) ($fit['ship_type_id'] ?? 0) > 0) ? 'ref' : 'missing',
    ];

    $normalized = doctrine_ensure_hull_item($items, $shipResolved, (string) ($fit['ship_name'] ?? ''));
    if ($persistChanges && !doctrine_fit_items_equal($items, $normalized)) {
        $fitId = (int) ($fit['id'] ?? 0);
        if ($fitId > 0) {
            try {
                db_doctrine_fit_replace_items($fitId, $normalized);
                db_doctrine_fit_sync_item_totals($fitId, count($normalized), (int) ($fit['unresolved_count'] ?? 0));
            } catch (Throwable) {
            }
        }
    }

    return $normalized;
}

function doctrine_supply_summary(array $rows): array
{
    $totalRequired = 0;
    $totalLocal = 0;
    $missingLines = 0;
    $okLines = 0;
    $totalMissingQty = 0;
    $restockGap = 0.0;
    $trackedRestockLines = 0;

    foreach ($rows as $row) {
        $required = max(1, (int) ($row['quantity'] ?? 1));
        $local = max(0, (int) ($row['local_available_qty'] ?? 0));
        $missing = max(0, (int) ($row['missing_qty'] ?? 0));
        $totalRequired += $required;
        $totalLocal += $local;
        $totalMissingQty += $missing;
        if ($missing > 0) {
            $missingLines++;
        } else {
            $okLines++;
        }
        if (isset($row['restock_gap_isk']) && $row['restock_gap_isk'] !== null) {
            $restockGap += (float) $row['restock_gap_isk'];
            $trackedRestockLines++;
        }
    }

    $coveragePercent = $totalRequired > 0 ? min(100.0, ($totalLocal / $totalRequired) * 100.0) : 0.0;
    $marketReady = $missingLines === 0;
    $status = $marketReady ? 'ready' : ($coveragePercent >= 70.0 ? 'warning' : 'critical');

    return [
        'market_ready' => $marketReady,
        'status' => $status,
        'status_label' => match ($status) {
            'ready' => 'Market ready',
            'warning' => 'Partial gap',
            default => 'Critical gap',
        },
        'total_required' => $totalRequired,
        'total_local' => $totalLocal,
        'missing_lines' => $missingLines,
        'ok_lines' => $okLines,
        'total_missing_qty' => $totalMissingQty,
        'restock_gap_isk' => $restockGap,
        'coverage_percent' => $coveragePercent,
        'tracked_restock_lines' => $trackedRestockLines,
    ];
}

function doctrine_complete_fit_availability(array $rows): array
{
    $completeFits = null;
    $bottleneck = null;

    foreach ($rows as $row) {
        $requiredQty = max(1, (int) ($row['quantity'] ?? 1));
        $localQty = max(0, (int) ($row['local_available_qty'] ?? 0));
        $fitCount = intdiv($localQty, $requiredQty);

        if (
            $completeFits === null
            || $fitCount < $completeFits
            || (
                $fitCount === $completeFits
                && $bottleneck !== null
                && $localQty < (int) ($bottleneck['local_qty'] ?? PHP_INT_MAX)
            )
        ) {
            $completeFits = $fitCount;
            $bottleneck = [
                'item_name' => (string) ($row['item_name'] ?? 'Unknown item'),
                'type_id' => isset($row['type_id']) ? (int) $row['type_id'] : null,
                'local_qty' => $localQty,
                'required_qty' => $requiredQty,
                'complete_fit_limit' => $fitCount,
                'is_stock_tracked' => !array_key_exists('is_stock_tracked', $row) || (bool) $row['is_stock_tracked'],
            ];
        }
    }

    if ($completeFits === null || $bottleneck === null) {
        return [
            'complete_fits_available' => 0,
            'bottleneck_item_name' => null,
            'bottleneck_type_id' => null,
            'bottleneck_quantity' => 0,
            'bottleneck_required_quantity' => 0,
            'minimum_stock_constraint' => 0,
            'constraint_label' => 'No doctrine items mapped yet.',
        ];
    }

    $bottleneckItemName = trim((string) ($bottleneck['item_name'] ?? ''));
    $bottleneckQty = max(0, (int) ($bottleneck['local_qty'] ?? 0));
    $bottleneckRequired = max(1, (int) ($bottleneck['required_qty'] ?? 1));
    $resolvedFits = max(0, (int) $completeFits);
    $isStockTracked = (bool) ($bottleneck['is_stock_tracked'] ?? true);
    $constraintPrefix = $isStockTracked ? '' : 'External bottleneck · ';

    return [
        'complete_fits_available' => $resolvedFits,
        'bottleneck_item_name' => $bottleneckItemName !== '' ? $bottleneckItemName : 'Unknown item',
        'bottleneck_type_id' => isset($bottleneck['type_id']) ? (int) $bottleneck['type_id'] : null,
        'bottleneck_quantity' => $bottleneckQty,
        'bottleneck_required_quantity' => $bottleneckRequired,
        'bottleneck_is_stock_tracked' => $isStockTracked,
        'external_bottleneck' => !$isStockTracked,
        'bottleneck_label' => $isStockTracked ? 'Bottleneck' : 'External bottleneck',
        'bottleneck_management_label' => $isStockTracked ? 'Tracked for stocking' : 'Externally managed',
        'minimum_stock_constraint' => $resolvedFits,
        'constraint_label' => $constraintPrefix . doctrine_format_quantity($resolvedFits) . ' complete fits from ' . ($bottleneckItemName !== '' ? $bottleneckItemName : 'Unknown item'),
    ];
}

function doctrine_local_history_index(array $historyRows): array
{
    $indexed = [];

    foreach ($historyRows as $row) {
        $typeId = max(0, (int) ($row['type_id'] ?? 0));
        $tradeDate = trim((string) (($row['trade_date'] ?? $row['observed_date']) ?? ''));
        if ($typeId <= 0 || $tradeDate === '') {
            continue;
        }

        $indexed[$typeId][$tradeDate] = $row;
    }

    return $indexed;
}

function doctrine_complete_fit_history_series(array $items, array $historyByTypeId, int $days = 7): array
{
    $safeDays = max(3, min(30, $days));
    $dateMap = [];

    foreach ($items as $item) {
        $typeId = max(0, (int) ($item['type_id'] ?? 0));
        if ($typeId <= 0 || !isset($historyByTypeId[$typeId])) {
            continue;
        }

        foreach ((array) $historyByTypeId[$typeId] as $tradeDate => $row) {
            if (trim((string) $tradeDate) !== '') {
                $dateMap[(string) $tradeDate] = true;
            }
        }
    }

    if ($dateMap === []) {
        return [];
    }

    $dates = array_keys($dateMap);
    sort($dates);
    $dates = array_slice($dates, max(0, count($dates) - $safeDays));

    $series = [];
    foreach ($dates as $tradeDate) {
        $completeFits = null;

        foreach ($items as $item) {
            $requiredQty = max(1, (int) ($item['quantity'] ?? 1));
            $typeId = max(0, (int) ($item['type_id'] ?? 0));
            $historyRow = ($historyByTypeId[$typeId][$tradeDate] ?? null);
            $localQty = is_array($historyRow) ? max(0, (int) ($historyRow['volume'] ?? 0)) : 0;
            $fitCount = intdiv($localQty, $requiredQty);
            $completeFits = $completeFits === null ? $fitCount : min($completeFits, $fitCount);
        }

        $series[] = [
            'trade_date' => $tradeDate,
            'complete_fits_available' => max(0, (int) ($completeFits ?? 0)),
        ];
    }

    return $series;
}

function doctrine_average_ints(array $values): float
{
    if ($values === []) {
        return 0.0;
    }

    return array_sum($values) / count($values);
}

function doctrine_readiness_trend(array $series, int $currentCompleteFits): array
{
    if ($series === []) {
        return [
            'direction' => 'unknown',
            'label' => 'Trend unavailable',
            'context' => 'Need more local history snapshots before readiness trend can be estimated.',
            'delta' => 0.0,
            'recent_average' => null,
            'prior_average' => null,
        ];
    }

    $values = array_values(array_map(static fn (array $row): int => max(0, (int) ($row['complete_fits_available'] ?? 0)), $series));
    $recent = array_slice($values, -3);
    $priorWindow = array_slice($values, -6, 3);
    $prior = $priorWindow === [] ? array_slice($values, 0, max(0, count($values) - count($recent))) : $priorWindow;

    if ($prior === []) {
        $baseline = (float) ($values[0] ?? 0);
        $latest = (float) ($values[count($values) - 1] ?? 0);
        $delta = $latest - $baseline;

        return [
            'direction' => $delta < 0 ? 'down' : ($delta > 0 ? 'up' : 'flat'),
            'label' => $delta < 0 ? 'Trending down' : ($delta > 0 ? 'Trending up' : 'Stable'),
            'context' => 'Based on the last ' . count($values) . ' local stock snapshots.',
            'delta' => $delta,
            'recent_average' => $latest,
            'prior_average' => $baseline,
        ];
    }

    $recentAverage = doctrine_average_ints($recent);
    $priorAverage = doctrine_average_ints($prior);
    $delta = $recentAverage - $priorAverage;
    $direction = 'flat';
    $label = 'Stable';

    if ($delta <= -0.75 || $currentCompleteFits < (int) floor($priorAverage)) {
        $direction = 'down';
        $label = 'Trending down';
    } elseif ($delta >= 0.75 || $currentCompleteFits > (int) ceil($priorAverage)) {
        $direction = 'up';
        $label = 'Trending up';
    }

    return [
        'direction' => $direction,
        'label' => $label,
        'context' => 'Recent 3-snapshot average ' . number_format($recentAverage, 1) . ' fits vs prior average ' . number_format($priorAverage, 1) . '.',
        'delta' => $delta,
        'recent_average' => $recentAverage,
        'prior_average' => $priorAverage,
    ];
}

function doctrine_item_loss_index(array $rows): array
{
    $indexed = [];

    foreach ($rows as $row) {
        $typeId = max(0, (int) ($row['type_id'] ?? 0));
        if ($typeId <= 0) {
            continue;
        }

        $indexed[$typeId] = [
            'quantity_24h' => max(0, (int) ($row['quantity_24h'] ?? 0)),
            'quantity_7d' => max(0, (int) ($row['quantity_7d'] ?? 0)),
            'quantity_window' => max(0, (int) ($row['quantity_window'] ?? 0)),
            'losses_24h' => max(0, (int) ($row['losses_24h'] ?? 0)),
            'losses_7d' => max(0, (int) ($row['losses_7d'] ?? 0)),
            'losses_window' => max(0, (int) ($row['losses_window'] ?? 0)),
            'latest_loss_at' => $row['latest_loss_at'] ?? null,
        ];
    }

    return $indexed;
}

function doctrine_hull_loss_index(array $rows): array
{
    $indexed = [];

    foreach ($rows as $row) {
        $typeId = max(0, (int) ($row['type_id'] ?? 0));
        if ($typeId <= 0) {
            continue;
        }

        $indexed[$typeId] = [
            'losses_24h' => max(0, (int) ($row['losses_24h'] ?? 0)),
            'losses_7d' => max(0, (int) ($row['losses_7d'] ?? 0)),
            'losses_window' => max(0, (int) ($row['losses_window'] ?? 0)),
            'latest_loss_at' => $row['latest_loss_at'] ?? null,
        ];
    }

    return $indexed;
}

function doctrine_loss_pressure_signals(array $items, ?int $shipTypeId, array $itemLossByType, array $hullLossByType): array
{
    $topItem = null;
    $equivalentFits24h = 0;
    $equivalentFits7d = 0;

    foreach ($items as $item) {
        $typeId = max(0, (int) ($item['type_id'] ?? 0));
        $requiredQty = max(1, (int) ($item['quantity'] ?? 1));
        $itemLoss = $itemLossByType[$typeId] ?? [];
        $itemEquivalent24h = (int) ceil(max(0, (int) ($itemLoss['quantity_24h'] ?? 0)) / $requiredQty);
        $itemEquivalent7d = (int) ceil(max(0, (int) ($itemLoss['quantity_7d'] ?? 0)) / $requiredQty);

        $equivalentFits24h = max($equivalentFits24h, $itemEquivalent24h);
        $equivalentFits7d = max($equivalentFits7d, $itemEquivalent7d);

        if (
            $topItem === null
            || $itemEquivalent7d > (int) ($topItem['equivalent_fits_7d'] ?? -1)
            || (
                $itemEquivalent7d === (int) ($topItem['equivalent_fits_7d'] ?? -1)
                && (int) ($itemLoss['quantity_7d'] ?? 0) > (int) ($topItem['quantity_7d'] ?? -1)
            )
        ) {
            $topItem = [
                'item_name' => (string) ($item['item_name'] ?? 'Unknown item'),
                'type_id' => $typeId > 0 ? $typeId : null,
                'quantity_24h' => max(0, (int) ($itemLoss['quantity_24h'] ?? 0)),
                'quantity_7d' => max(0, (int) ($itemLoss['quantity_7d'] ?? 0)),
                'losses_24h' => max(0, (int) ($itemLoss['losses_24h'] ?? 0)),
                'losses_7d' => max(0, (int) ($itemLoss['losses_7d'] ?? 0)),
                'equivalent_fits_24h' => $itemEquivalent24h,
                'equivalent_fits_7d' => $itemEquivalent7d,
            ];
        }
    }

    $hullLoss = $shipTypeId !== null && $shipTypeId > 0 ? ($hullLossByType[$shipTypeId] ?? []) : [];

    return [
        'hull_losses_24h' => max(0, (int) ($hullLoss['losses_24h'] ?? 0)),
        'hull_losses_7d' => max(0, (int) ($hullLoss['losses_7d'] ?? 0)),
        'item_equivalent_fit_losses_24h' => $equivalentFits24h,
        'item_equivalent_fit_losses_7d' => $equivalentFits7d,
        'top_pressure_item' => $topItem,
    ];
}

function doctrine_bottleneck_restock_signal(array $availability, array $historyByTypeId): array
{
    if (($availability['external_bottleneck'] ?? false) === true) {
        return [
            'direction' => 'external',
            'label' => 'External bottleneck',
            'context' => 'The current fit ceiling is constrained by an externally managed hull, so no restock urgency is generated for it.',
        ];
    }

    $typeId = max(0, (int) ($availability['bottleneck_type_id'] ?? 0));
    if ($typeId <= 0 || !isset($historyByTypeId[$typeId])) {
        return [
            'direction' => 'unknown',
            'label' => 'Restock trend unavailable',
            'context' => 'No recent local stock history exists for the current bottleneck item.',
        ];
    }

    $rows = array_values((array) $historyByTypeId[$typeId]);
    usort($rows, static fn (array $a, array $b): int => strcmp((string) ($a['trade_date'] ?? ''), (string) ($b['trade_date'] ?? '')));
    $values = array_map(static fn (array $row): int => max(0, (int) ($row['volume'] ?? 0)), $rows);
    $recent = array_slice($values, -3);
    $priorWindow = array_slice($values, -6, 3);
    $prior = $priorWindow === [] ? array_slice($values, 0, max(0, count($values) - count($recent))) : $priorWindow;

    if ($recent === [] || $prior === []) {
        return [
            'direction' => 'unknown',
            'label' => 'Restock trend limited',
            'context' => 'Need at least two local history windows for bottleneck trend analysis.',
        ];
    }

    $recentAverage = doctrine_average_ints($recent);
    $priorAverage = doctrine_average_ints($prior);
    $delta = $recentAverage - $priorAverage;
    $direction = 'flat';
    $label = 'Replacement stable';

    if ($delta <= -1.0) {
        $direction = 'down';
        $label = 'Replacement lagging';
    } elseif ($delta >= 1.0) {
        $direction = 'up';
        $label = 'Replacement improving';
    }

    return [
        'direction' => $direction,
        'label' => $label,
        'context' => ($availability['bottleneck_item_name'] ?? 'Bottleneck item') . ' averaged ' . number_format($recentAverage, 1) . ' local units recently vs ' . number_format($priorAverage, 1) . ' previously.',
    ];
}

function doctrine_item_depletion_index(array $historyByTypeId): array
{
    $indexed = [];

    foreach ($historyByTypeId as $typeId => $rowsByDate) {
        $rows = array_values((array) $rowsByDate);
        usort($rows, static fn (array $a, array $b): int => strcmp((string) ($a['trade_date'] ?? ''), (string) ($b['trade_date'] ?? '')));
        if ($rows === []) {
            continue;
        }

        $volumes = array_values(array_map(static fn (array $row): int => max(0, (int) ($row['volume'] ?? 0)), $rows));
        $latestVolume = $volumes[count($volumes) - 1] ?? 0;
        $previousVolume = $volumes[count($volumes) - 2] ?? $latestVolume;
        $recent = array_slice($volumes, -3);
        $priorWindow = array_slice($volumes, -6, 3);
        $prior = $priorWindow === [] ? array_slice($volumes, 0, max(0, count($volumes) - count($recent))) : $priorWindow;
        $recentAverage = doctrine_average_ints($recent);
        $priorAverage = doctrine_average_ints($prior === [] ? [$previousVolume] : $prior);
        $depletion24h = (int) round($previousVolume - $latestVolume);
        $depletion7d = (int) round($priorAverage - $recentAverage);
        $classification = 'stable';

        if ($depletion24h > 0 || $depletion7d > 0.75) {
            $classification = 'draining';
        } elseif ($depletion24h < 0 || $depletion7d < -0.75) {
            $classification = 'recovering';
        }

        $indexed[(int) $typeId] = [
            'latest_volume' => $latestVolume,
            'previous_volume' => $previousVolume,
            'recent_average' => $recentAverage,
            'prior_average' => $priorAverage,
            'depletion_24h' => $depletion24h,
            'depletion_7d' => $depletion7d,
            'classification' => $classification,
        ];
    }

    return $indexed;
}

function doctrine_fit_depletion_signal(array $items, array $depletionByType): array
{
    if ($items === []) {
        return [
            'depletion_24h' => 0,
            'depletion_7d' => 0,
            'fit_equivalent_24h' => 0.0,
            'fit_equivalent_7d' => 0.0,
            'classification' => 'stable',
            'context' => 'No local depletion signal is available yet.',
        ];
    }

    $fitEquivalent24h = 0.0;
    $fitEquivalent7d = 0.0;
    $totalDepletion24h = 0;
    $totalDepletion7d = 0;
    $drainingCount = 0;
    $recoveringCount = 0;

    foreach ($items as $item) {
        $typeId = max(0, (int) ($item['type_id'] ?? 0));
        $requiredQty = max(1, (int) ($item['quantity'] ?? 1));
        $signal = $depletionByType[$typeId] ?? [];
        $depletion24h = (int) ($signal['depletion_24h'] ?? 0);
        $depletion7d = (int) ($signal['depletion_7d'] ?? 0);
        $classification = (string) ($signal['classification'] ?? 'stable');

        $totalDepletion24h += $depletion24h;
        $totalDepletion7d += $depletion7d;
        $fitEquivalent24h = max($fitEquivalent24h, $depletion24h / $requiredQty);
        $fitEquivalent7d = max($fitEquivalent7d, $depletion7d / $requiredQty);

        if ($classification === 'draining') {
            $drainingCount++;
        } elseif ($classification === 'recovering') {
            $recoveringCount++;
        }
    }

    $classification = 'stable';
    if ($drainingCount > max(0, $recoveringCount)) {
        $classification = 'draining';
    } elseif ($recoveringCount > max(0, $drainingCount)) {
        $classification = 'recovering';
    }

    return [
        'depletion_24h' => $totalDepletion24h,
        'depletion_7d' => $totalDepletion7d,
        'fit_equivalent_24h' => round($fitEquivalent24h, 2),
        'fit_equivalent_7d' => round($fitEquivalent7d, 2),
        'classification' => $classification,
        'context' => match ($classification) {
            'draining' => 'Recent local stock is draining faster than the prior window.',
            'recovering' => 'Recent local stock is recovering faster than the prior window.',
            default => 'Local stock movement is broadly stable.',
        },
    ];
}

function doctrine_recommended_target_fit_count(array $availability, array $trend, array $lossSignals, array $restockSignal): array
{
    $depletionSignal = is_array($restockSignal['depletion_signal'] ?? null) ? $restockSignal['depletion_signal'] : [];
    $baselineTarget = 3;
    $minimumTarget = 2;
    $maximumTarget = 12;
    $lossFloor = max(
        0,
        (int) ceil(max(
            (int) ($lossSignals['hull_losses_7d'] ?? 0),
            (int) ($lossSignals['item_equivalent_fit_losses_7d'] ?? 0)
        ))
    );
    $recentPressure = max(
        0,
        (int) ceil(max(
            (int) ($lossSignals['hull_losses_24h'] ?? 0),
            (int) ($lossSignals['item_equivalent_fit_losses_24h'] ?? 0)
        ))
    );
    $lossAdjustment = min(4, (int) ceil(($lossFloor * 0.5) + ($recentPressure * 0.8)));
    $depletionAdjustment = 0;
    if (($depletionSignal['classification'] ?? 'stable') === 'draining') {
        $depletionAdjustment = min(3, max(1, (int) ceil((float) ($depletionSignal['fit_equivalent_7d'] ?? 0.0))));
    }
    $recoveryAdjustment = 0;
    if (($depletionSignal['classification'] ?? 'stable') === 'recovering' && ($trend['direction'] ?? 'unknown') !== 'down') {
        $recoveryAdjustment = min(2, max(1, (int) round(abs((float) ($depletionSignal['fit_equivalent_7d'] ?? 0.0)))));
    }
    $trendAdjustment = ($trend['direction'] ?? 'unknown') === 'down' ? 1 : 0;
    $recommended = $baselineTarget + $lossAdjustment + $depletionAdjustment + $trendAdjustment - $recoveryAdjustment;
    $recommended = max($minimumTarget, min($maximumTarget, max($recommended, $lossFloor, $recentPressure + 1)));
    $completeFits = max(0, (int) ($availability['complete_fits_available'] ?? 0));
    $gap = max(0, $recommended - $completeFits);

    return [
        'recommended_target_fit_count' => $recommended,
        'gap_to_target_fit_count' => $gap,
        'baseline_target_fit_count' => $baselineTarget,
        'loss_adjustment' => $lossAdjustment,
        'depletion_adjustment' => $depletionAdjustment,
        'recovery_adjustment' => $recoveryAdjustment,
        'trend_adjustment' => $trendAdjustment,
        'minimum_target_fit_count' => $minimumTarget,
        'maximum_target_fit_count' => $maximumTarget,
    ];
}

function doctrine_readiness_state(array $availability, array $targetPlan): array
{
    $completeFits = max(0, (int) ($availability['complete_fits_available'] ?? 0));
    $targetFits = max(0, (int) ($targetPlan['recommended_target_fit_count'] ?? 0));
    $gap = max(0, (int) ($targetPlan['gap_to_target_fit_count'] ?? 0));

    $state = 'market_ready';
    $label = 'Market ready';
    $context = 'Current complete-fit stock covers the present rule-based target, so ships can be fielded now.';
    $explanation = 'Complete fits currently meet or exceed the target fit count.';

    if ($completeFits <= 0 || $gap >= max(3, (int) ceil(max(1, $targetFits) * 0.5))) {
        $state = 'critical_gap';
        $label = 'Critical gap';
        $context = $completeFits <= 0
            ? 'Current local stock cannot field a complete fit right now.'
            : 'Current complete-fit availability sits materially below the target buffer.';
        $explanation = 'Complete fits available: ' . doctrine_format_quantity($completeFits)
            . ' against target ' . doctrine_format_quantity($targetFits)
            . ' with a gap of ' . doctrine_format_quantity($gap) . '.';
    } elseif ($gap > 0) {
        $state = 'partial_gap';
        $label = 'Partial gap';
        $context = 'Ships can still be fielded now, but current complete-fit stock remains below target.';
        $explanation = 'Complete fits available: ' . doctrine_format_quantity($completeFits)
            . ' against target ' . doctrine_format_quantity($targetFits)
            . ' with a gap of ' . doctrine_format_quantity($gap) . '.';
    }

    return [
        'state' => $state,
        'label' => $label,
        'context' => $context,
        'explanation' => $explanation,
        'code' => $state,
        'severity' => match ($state) {
            'critical_gap' => 3,
            'partial_gap' => 2,
            default => 1,
        },
    ];
}

function doctrine_resupply_pressure(array $base, array $availability, array $trend, array $lossSignals, array $restockSignal, array $targetPlan): array
{
    $completeFits = max(0, (int) ($availability['complete_fits_available'] ?? 0));
    $rawGap = max(0, (int) ($targetPlan['gap_to_target_fit_count'] ?? 0));
    $recentPressure = max(
        0,
        (int) ($lossSignals['hull_losses_24h'] ?? 0),
        (int) ($lossSignals['item_equivalent_fit_losses_24h'] ?? 0)
    );
    $weeklyPressure = max(
        0,
        (int) ($lossSignals['hull_losses_7d'] ?? 0),
        (int) ($lossSignals['item_equivalent_fit_losses_7d'] ?? 0)
    );
    $depletionSignal = is_array($restockSignal['depletion_signal'] ?? null) ? $restockSignal['depletion_signal'] : [];
    $targetGapPressure = (($availability['external_bottleneck'] ?? false) === true && (int) ($base['missing_lines'] ?? 0) === 0)
        ? 0
        : $rawGap;
    $bottleneckPressure = (($availability['external_bottleneck'] ?? false) === true)
        ? 0
        : (((int) ($availability['bottleneck_required_quantity'] ?? 0) > 0 && (int) ($availability['bottleneck_quantity'] ?? 0) <= (int) ($availability['bottleneck_required_quantity'] ?? 0)) ? 1 : 0);

    $score = 0;
    $drivers = [];

    if ($recentPressure > 0) {
        $score += $recentPressure >= max(1, $completeFits) ? 4 : 2;
        $drivers[] = doctrine_format_quantity($recentPressure) . ' fit-equivalent losses landed in the last 24h.';
    }

    if ($weeklyPressure > 0) {
        $score += $weeklyPressure > max(1, $completeFits) ? 3 : 1;
        $drivers[] = doctrine_format_quantity($weeklyPressure) . ' fit-equivalent losses landed over 7d.';
    }

    if ($targetGapPressure > 0) {
        $score += $targetGapPressure >= 3 ? 4 : 2;
        $drivers[] = 'Current stock sits ' . doctrine_format_quantity($targetGapPressure) . ' fits below the target buffer.';
    }

    if (($depletionSignal['classification'] ?? 'stable') === 'draining') {
        $score += max(2, min(4, (int) ceil((float) ($depletionSignal['fit_equivalent_7d'] ?? 0.0))));
        $drivers[] = 'Local stock is draining by roughly ' . doctrine_format_quantity((int) ceil((float) ($depletionSignal['fit_equivalent_7d'] ?? 0.0))) . ' fit-equivalents over the recent window.';
    }

    if (($trend['direction'] ?? 'unknown') === 'down') {
        $score += 1;
        $drivers[] = 'Complete-fit readiness trend is moving down.';
    }

    if ($bottleneckPressure > 0) {
        $score += 1;
        $drivers[] = 'The current bottleneck item is already at or below one-fit coverage.';
    }

    $state = 'stable';
    $label = 'Stable';
    $context = 'Current stock movement and recent losses do not yet threaten the doctrine buffer.';
    $explanation = 'Recent losses, depletion, and sell-through remain within the current local buffer.';

    if ($score >= 10 || ($recentPressure > 0 && $completeFits <= $recentPressure) || $targetGapPressure >= 3) {
        $state = 'urgent_resupply';
        $label = 'Urgent resupply';
        $context = 'Recent losses or local stock drain can exhaust the remaining doctrine buffer very quickly.';
        $explanation = 'Urgent pressure: ' . implode(' ', array_slice($drivers, 0, 3));
    } elseif ($score >= 7 || $targetGapPressure > 0 || ($depletionSignal['classification'] ?? 'stable') === 'draining') {
        $state = 'resupply_soon';
        $label = 'Resupply soon';
        $context = 'Current stock is still usable, but the present burn rate suggests the buffer will become insufficient soon.';
        $explanation = 'Resupply planning should start now because ' . implode(' ', array_slice($drivers, 0, 3));
    } elseif ($score >= 4 || $weeklyPressure > 0 || ($trend['direction'] ?? 'unknown') === 'down') {
        $state = 'elevated';
        $label = 'Elevated';
        $context = 'Readiness is intact, but recent killmails or local drain are tightening the stock cushion.';
        $explanation = 'Pressure is elevated because ' . implode(' ', array_slice($drivers, 0, 2));
    }

    return [
        'state' => $state,
        'label' => $label,
        'context' => $context,
        'explanation' => $explanation,
        'code' => $state,
        'severity' => match ($state) {
            'urgent_resupply' => 4,
            'resupply_soon' => 3,
            'elevated' => 2,
            default => 1,
        },
        'drivers' => $drivers,
        'likely_enough_based_on_recent_losses' => in_array($state, ['stable', 'elevated'], true),
    ];
}

function doctrine_combined_supply_status(array $readiness, array $pressure): array
{
    $readinessLabel = (string) ($readiness['label'] ?? 'Unknown readiness');
    $pressureLabel = (string) ($pressure['label'] ?? 'Unknown pressure');

    return [
        'code' => (string) ($readiness['code'] ?? 'unknown') . '__' . (string) ($pressure['code'] ?? 'unknown'),
        'label' => $readinessLabel . ' · ' . $pressureLabel,
        'context' => trim((string) ($readiness['context'] ?? '') . ' ' . (string) ($pressure['context'] ?? '')),
        'explanation' => trim((string) ($readiness['explanation'] ?? '') . ' ' . (string) ($pressure['explanation'] ?? '')),
    ];
}

function doctrine_operational_supply(array $rows, array $items, array $fit, array $historyByTypeId, array $itemLossByType, array $hullLossByType): array
{
    $hullItem = doctrine_find_hull_item($items);
    $hullIsStockTracked = !is_array($hullItem) || !array_key_exists('is_stock_tracked', $hullItem) || (bool) $hullItem['is_stock_tracked'];
    $trackedRows = doctrine_filter_stock_tracked_rows($rows);
    $trackedItems = doctrine_filter_stock_tracked_items($items);
    $base = doctrine_supply_summary($trackedRows);
    $availability = doctrine_complete_fit_availability($rows);
    $historySeries = doctrine_complete_fit_history_series($items, $historyByTypeId, 7);
    $trend = doctrine_readiness_trend($historySeries, (int) ($availability['complete_fits_available'] ?? 0));
    $lossSignals = doctrine_loss_pressure_signals(
        $trackedItems,
        $hullIsStockTracked
            ? (isset($fit['ship_type_id']) ? (int) $fit['ship_type_id'] : null)
            : null,
        $itemLossByType,
        $hullLossByType
    );
    $displayLossSignals = doctrine_loss_pressure_signals(
        $items,
        isset($fit['ship_type_id']) ? (int) $fit['ship_type_id'] : null,
        $itemLossByType,
        $hullLossByType
    );
    $depletionByType = doctrine_item_depletion_index($historyByTypeId);
    $depletionSignal = doctrine_fit_depletion_signal($trackedItems, $depletionByType);
    $restockSignal = doctrine_bottleneck_restock_signal($availability, $historyByTypeId);
    $restockSignal['depletion_signal'] = $depletionSignal;
    $targetPlan = doctrine_recommended_target_fit_count($availability, $trend, $lossSignals, $restockSignal);
    $readiness = doctrine_readiness_state($availability, $targetPlan);
    $pressure = doctrine_resupply_pressure($base, $availability, $trend, $lossSignals, $restockSignal, $targetPlan);
    $combined = doctrine_combined_supply_status($readiness, $pressure);

    $coveragePercent = (float) ($base['coverage_percent'] ?? 0.0);
    $scoreLossPressure = min(40.0, max(
        (int) ($lossSignals['hull_losses_24h'] ?? 0) * 8.0,
        (int) ($lossSignals['item_equivalent_fit_losses_7d'] ?? 0) * 4.5,
        (int) ($lossSignals['hull_losses_7d'] ?? 0) * 4.0
    ));
    $scoreStockGap = min(30.0, max(0.0, ((float) ($targetPlan['gap_to_target_fit_count'] ?? 0) * 8.0) + max(0.0, (100.0 - $coveragePercent) * 0.08)));
    $scoreDepletion = min(20.0, max(0.0, ((float) ($depletionSignal['fit_equivalent_7d'] ?? 0.0) * 6.0) + (($depletionSignal['classification'] ?? 'stable') === 'draining' ? 4.0 : 0.0)));
    $scoreBottleneck = ($availability['external_bottleneck'] ?? false)
        ? 0.0
        : min(10.0, max(0.0, ((int) ($availability['bottleneck_required_quantity'] ?? 0) > 0 && (int) ($availability['bottleneck_quantity'] ?? 0) <= (int) ($availability['bottleneck_required_quantity'] ?? 0)) ? 7.0 : 3.0));
    $totalScore = round($scoreLossPressure + $scoreStockGap + $scoreDepletion + $scoreBottleneck, 2);

    return $base + $availability + $targetPlan + [
        'status' => (string) ($readiness['state'] ?? 'market_ready'),
        'status_label' => (string) ($readiness['label'] ?? 'Market ready'),
        'readiness_state' => (string) ($readiness['state'] ?? 'market_ready'),
        'readiness_label' => (string) ($readiness['label'] ?? 'Market ready'),
        'readiness_context' => (string) ($readiness['context'] ?? ''),
        'readiness_explanation' => (string) ($readiness['explanation'] ?? ''),
        'resupply_pressure_state' => (string) ($pressure['state'] ?? 'stable'),
        'resupply_pressure_label' => (string) ($pressure['label'] ?? 'Stable'),
        'resupply_pressure_context' => (string) ($pressure['context'] ?? ''),
        'resupply_pressure_explanation' => (string) ($pressure['explanation'] ?? ''),
        'combined_status_code' => (string) ($combined['code'] ?? ''),
        'combined_status_label' => (string) ($combined['label'] ?? ''),
        'combined_status_context' => (string) ($combined['context'] ?? ''),
        'recommendation_code' => (string) ($combined['code'] ?? ''),
        'recommendation_text' => (string) ($combined['label'] ?? ''),
        'recommendation_explanation' => (string) ($combined['explanation'] ?? ''),
        'planning_context' => (string) ($combined['context'] ?? ''),
        'readiness_trend' => $trend['label'],
        'readiness_trend_direction' => $trend['direction'],
        'readiness_trend_context' => $trend['context'],
        'complete_fit_history' => $historySeries,
        'recent_hull_losses_24h' => $displayLossSignals['hull_losses_24h'],
        'recent_hull_losses_7d' => $displayLossSignals['hull_losses_7d'],
        'recent_item_fit_losses_24h' => $lossSignals['item_equivalent_fit_losses_24h'],
        'recent_item_fit_losses_7d' => $lossSignals['item_equivalent_fit_losses_7d'],
        'top_pressure_item' => $lossSignals['top_pressure_item'],
        'depletion_24h' => $depletionSignal['depletion_24h'],
        'depletion_7d' => $depletionSignal['depletion_7d'],
        'depletion_fit_equivalent_24h' => $depletionSignal['fit_equivalent_24h'],
        'depletion_fit_equivalent_7d' => $depletionSignal['fit_equivalent_7d'],
        'depletion_state' => $depletionSignal['classification'],
        'depletion_context' => $depletionSignal['context'],
        'restock_trend' => $restockSignal['label'],
        'restock_trend_direction' => $restockSignal['direction'],
        'restock_trend_context' => $restockSignal['context'],
        'externally_managed' => (bool) (($availability['external_bottleneck'] ?? false) === true && (int) ($base['missing_lines'] ?? 0) === 0),
        'tracked_item_count' => count($trackedItems),
        'hull_tracking_note' => doctrine_hull_stock_tracking_reason([
            'item_name' => (string) ($fit['ship_name'] ?? ''),
            'type_id' => isset($fit['ship_type_id']) ? (int) $fit['ship_type_id'] : null,
        ], $hullIsStockTracked),
        'driver_scores' => [
            'loss_pressure' => round($scoreLossPressure, 2),
            'stock_gap' => round($scoreStockGap, 2),
            'depletion' => round($scoreDepletion, 2),
            'bottleneck' => round($scoreBottleneck, 2),
            'total' => $totalScore,
        ],
        'likely_enough_based_on_recent_losses' => (bool) ($pressure['likely_enough_based_on_recent_losses'] ?? false),
    ];
}

function doctrine_readiness_label_from_state(string $state): string
{
    return match ($state) {
        'critical_gap', 'critical' => 'Critical gap',
        'partial_gap', 'warning' => 'Partial gap',
        default => 'Market ready',
    };
}

function doctrine_pressure_label_from_state(string $state): string
{
    return match ($state) {
        'urgent_resupply' => 'Urgent resupply',
        'resupply_soon' => 'Resupply soon',
        'elevated' => 'Elevated',
        default => 'Stable',
    };
}

function doctrine_resupply_pressure_tone(string $state): string
{
    return match ($state) {
        'stable' => 'border-emerald-400/20 bg-emerald-500/10 text-emerald-100',
        'elevated' => 'border-sky-400/20 bg-sky-500/10 text-sky-100',
        'resupply_soon' => 'border-amber-400/20 bg-amber-500/10 text-amber-100',
        default => 'border-rose-400/20 bg-rose-500/10 text-rose-200',
    };
}

function doctrine_combined_status_badges(array $supply): array
{
    return [
        [
            'label' => (string) ($supply['readiness_label'] ?? $supply['status_label'] ?? 'Market ready'),
            'tone' => doctrine_supply_status_tone((string) ($supply['readiness_state'] ?? $supply['status'] ?? 'market_ready')),
            'title' => 'Readiness',
        ],
        [
            'label' => (string) ($supply['resupply_pressure_label'] ?? 'Stable'),
            'tone' => doctrine_resupply_pressure_tone((string) ($supply['resupply_pressure_state'] ?? 'stable')),
            'title' => 'Resupply pressure',
        ],
    ];
}

function doctrine_supply_status_tone(string $status): string
{
    return match ($status) {
        'market_ready', 'ready' => 'border-emerald-400/20 bg-emerald-500/10 text-emerald-100',
        'partial_gap', 'warning' => 'border-amber-400/20 bg-amber-500/10 text-amber-100',
        default => 'border-rose-400/20 bg-rose-500/10 text-rose-200',
    };
}

function doctrine_group_market_rows_by_category(array $items, array $comparisonByTypeId = []): array
{
    $categories = [];
    foreach (doctrine_fit_item_market_rows($items, $comparisonByTypeId) as $row) {
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

function doctrine_snapshot_delta(?array $previous, array $current): array
{
    if (!is_array($previous) || $previous === []) {
        return [
            'target_delta' => 0,
            'complete_delta' => 0,
            'fit_gap_delta' => 0,
            'readiness_changed' => false,
            'pressure_changed' => false,
            'recommendation_changed' => false,
            'bottleneck_changed' => false,
            'summary' => 'First recorded doctrine snapshot for this fit.',
        ];
    }

    $targetDelta = (int) ($current['target_fits'] ?? 0) - (int) ($previous['target_fits'] ?? 0);
    $completeDelta = (int) ($current['complete_fits_available'] ?? 0) - (int) ($previous['complete_fits_available'] ?? 0);
    $fitGapDelta = (int) ($current['fit_gap'] ?? 0) - (int) ($previous['fit_gap'] ?? 0);
    $readinessChanged = (string) ($current['readiness_state'] ?? '') !== (string) ($previous['readiness_state'] ?? '');
    $currentPressureCode = (string) ($current['resupply_pressure_code'] ?? $current['resupply_pressure_state'] ?? 'stable');
    $previousPressureCode = (string) ($previous['resupply_pressure_code'] ?? $previous['resupply_pressure_state'] ?? 'stable');
    $pressureChanged = $currentPressureCode !== $previousPressureCode;
    $recommendationChanged = (string) ($current['recommendation_code'] ?? '') !== (string) ($previous['recommendation_code'] ?? '');
    $bottleneckChanged = (int) ($current['bottleneck_type_id'] ?? 0) !== (int) ($previous['bottleneck_type_id'] ?? 0);
    $parts = [];

    if ($targetDelta !== 0) {
        $parts[] = 'Target ' . ($targetDelta > 0 ? 'rose' : 'fell') . ' by ' . doctrine_format_quantity(abs($targetDelta)) . '.';
    }
    if ($completeDelta !== 0) {
        $parts[] = 'Complete fits ' . ($completeDelta > 0 ? 'improved' : 'fell') . ' by ' . doctrine_format_quantity(abs($completeDelta)) . '.';
    }
    if ($readinessChanged) {
        $parts[] = 'Readiness changed from ' . doctrine_readiness_label_from_state((string) ($previous['readiness_state'] ?? 'market_ready')) . ' to ' . doctrine_readiness_label_from_state((string) ($current['readiness_state'] ?? 'market_ready')) . '.';
    }
    if ($pressureChanged) {
        $parts[] = 'Resupply pressure changed from ' . (string) ($previous['resupply_pressure_text'] ?? doctrine_pressure_label_from_state((string) ($previous['resupply_pressure_state'] ?? 'stable'))) . ' to ' . (string) ($current['resupply_pressure_text'] ?? doctrine_pressure_label_from_state((string) ($current['resupply_pressure_state'] ?? 'stable'))) . '.';
    }
    if ($recommendationChanged) {
        $parts[] = 'Combined outlook moved from ' . (string) ($previous['recommendation_text'] ?? 'previous state') . ' to ' . (string) ($current['recommendation_text'] ?? 'current state') . '.';
    }
    if ($bottleneckChanged) {
        $parts[] = 'The bottleneck item changed.';
    }

    return [
        'target_delta' => $targetDelta,
        'complete_delta' => $completeDelta,
        'fit_gap_delta' => $fitGapDelta,
        'readiness_changed' => $readinessChanged,
        'pressure_changed' => $pressureChanged,
        'recommendation_changed' => $recommendationChanged,
        'bottleneck_changed' => $bottleneckChanged,
        'summary' => $parts !== [] ? implode(' ', $parts) : 'No material doctrine change since the previous snapshot.',
    ];
}

function doctrine_snapshot_history_view_model(int $fitId, array $latestSupply): array
{
    $history = [];

    try {
        $history = db_doctrine_fit_snapshot_history($fitId, 12);
    } catch (Throwable) {
        $history = [];
    }

    if ($history === []) {
        return [
            'latest' => null,
            'previous' => null,
            'timeline' => [],
            'trend_points' => [],
            'change' => [
                'target_delta' => 0,
                'complete_delta' => 0,
                'fit_gap_delta' => 0,
                'readiness_changed' => false,
                'pressure_changed' => false,
                'recommendation_changed' => false,
                'bottleneck_changed' => false,
                'summary' => 'No doctrine snapshot history has been recorded yet.',
            ],
        ];
    }

    $latest = $history[0];
    $previous = $history[1] ?? null;
    $change = doctrine_snapshot_delta($previous, $latest);
    $trendPoints = [];
    $timeline = [];

    foreach (array_reverse($history) as $row) {
        $trendPoints[] = [
            'snapshot_time' => (string) ($row['snapshot_time'] ?? ''),
            'complete_fits_available' => (int) ($row['complete_fits_available'] ?? 0),
            'target_fits' => (int) ($row['target_fits'] ?? 0),
            'fit_gap' => (int) ($row['fit_gap'] ?? 0),
            'loss_24h' => (int) ($row['loss_24h'] ?? 0),
            'total_score' => (float) ($row['total_score'] ?? 0.0),
        ];
    }

    for ($i = 0; $i < count($history); $i++) {
        $current = $history[$i];
        $prior = $history[$i + 1] ?? null;
        $delta = doctrine_snapshot_delta($prior, $current);
        if ($delta['readiness_changed'] || $delta['pressure_changed'] || $delta['recommendation_changed'] || $delta['bottleneck_changed'] || $delta['target_delta'] !== 0 || $delta['fit_gap_delta'] !== 0 || $i === 0) {
            $timeline[] = [
                'snapshot_time' => (string) ($current['snapshot_time'] ?? ''),
                'recommendation_text' => (string) ($current['recommendation_text'] ?? ''),
                'readiness_state' => (string) ($current['readiness_state'] ?? 'market_ready'),
                'readiness_text' => doctrine_readiness_state(
                    ['complete_fits_available' => (int) ($current['complete_fits_available'] ?? 0)],
                    [
                        'recommended_target_fit_count' => (int) ($current['target_fits'] ?? 0),
                        'gap_to_target_fit_count' => (int) ($current['fit_gap'] ?? 0),
                    ]
                )['label'],
                'resupply_pressure_state' => (string) ($current['resupply_pressure_state'] ?? 'stable'),
                'resupply_pressure_text' => (string) ($current['resupply_pressure_text'] ?? doctrine_pressure_label_from_state((string) ($current['resupply_pressure_state'] ?? 'stable'))),
                'target_fits' => (int) ($current['target_fits'] ?? 0),
                'fit_gap' => (int) ($current['fit_gap'] ?? 0),
                'summary' => $delta['summary'],
            ];
        }
    }

    return [
        'latest' => $latest,
        'previous' => $previous,
        'timeline' => array_slice($timeline, 0, 8),
        'trend_points' => $trendPoints,
        'change' => $change,
    ];
}

function doctrine_global_item_layer(array $fitsById, array $allDoctrineItems, array $depletionByType = []): array
{
    $outcomes = market_comparison_outcomes();
    $rows = $outcomes['rows'] ?? [];
    $doctrineItemMeta = [];

    foreach ($allDoctrineItems as $item) {
        $typeId = max(0, (int) ($item['type_id'] ?? 0));
        if ($typeId <= 0) {
            continue;
        }

        $isStockTracked = !array_key_exists('is_stock_tracked', $item) || (bool) $item['is_stock_tracked'];
        if (!isset($doctrineItemMeta[$typeId])) {
            $doctrineItemMeta[$typeId] = [
                'fit_count' => 0,
                'is_bottleneck' => false,
                'is_external_bottleneck' => false,
            ];
        }
        if ($isStockTracked) {
            $doctrineItemMeta[$typeId]['fit_count']++;
        }
    }

    foreach ($fitsById as $fit) {
        $supply = (array) ($fit['supply'] ?? []);
        $bottleneckTypeId = max(0, (int) ($supply['bottleneck_type_id'] ?? 0));
        if ($bottleneckTypeId > 0) {
            $doctrineItemMeta[$bottleneckTypeId]['is_bottleneck'] = true;
            $doctrineItemMeta[$bottleneckTypeId]['is_external_bottleneck'] = !((bool) ($supply['bottleneck_is_stock_tracked'] ?? true));
        }
    }

    $rankedRows = [];
    foreach ($rows as $row) {
        $typeId = max(0, (int) ($row['type_id'] ?? 0));
        $doctrineMeta = $doctrineItemMeta[$typeId] ?? ['fit_count' => 0, 'is_bottleneck' => false];
        $isDoctrine = (int) ($doctrineMeta['fit_count'] ?? 0) > 0;
        $depletion = $depletionByType[$typeId] ?? [];
        $priority = max((int) ($row['opportunity_score'] ?? 0), (int) ($row['risk_score'] ?? 0));
        if ($isDoctrine) {
            $priority = (int) round($priority * 1.6);
        }
        if (($doctrineMeta['is_bottleneck'] ?? false) === true && ($doctrineMeta['is_external_bottleneck'] ?? false) !== true) {
            $priority += 18;
        }
        if (($depletion['classification'] ?? 'stable') === 'draining') {
            $priority += 10;
        }

        $rankedRows[] = $row + [
            'is_doctrine_item' => $isDoctrine,
            'doctrine_fit_count' => (int) ($doctrineMeta['fit_count'] ?? 0),
            'is_bottleneck_item' => (bool) ($doctrineMeta['is_bottleneck'] ?? false),
            'is_external_bottleneck' => (bool) ($doctrineMeta['is_external_bottleneck'] ?? false),
            'depletion_state' => (string) ($depletion['classification'] ?? 'stable'),
            'depletion_24h' => (int) ($depletion['depletion_24h'] ?? 0),
            'depletion_7d' => (int) ($depletion['depletion_7d'] ?? 0),
            'priority_score' => min(100, max(0, $priority)),
        ];
    }

    usort($rankedRows, static function (array $a, array $b): int {
        $doctrineWeight = ((bool) ($b['is_doctrine_item'] ?? false) <=> (bool) ($a['is_doctrine_item'] ?? false));
        if ($doctrineWeight !== 0) {
            return $doctrineWeight;
        }

        $bottleneckWeight = ((bool) ($b['is_bottleneck_item'] ?? false) <=> (bool) ($a['is_bottleneck_item'] ?? false));
        if ($bottleneckWeight !== 0) {
            return $bottleneckWeight;
        }

        return ((int) ($b['priority_score'] ?? 0) <=> (int) ($a['priority_score'] ?? 0))
            ?: strcasecmp((string) ($a['type_name'] ?? ''), (string) ($b['type_name'] ?? ''));
    });

    return [
        'rows' => $rankedRows,
        'top_restock_items' => array_slice(array_values(array_filter($rankedRows, static fn (array $row): bool => (bool) ($row['missing_in_alliance'] ?? false) || (bool) ($row['weak_alliance_stock'] ?? false))), 0, 12),
    ];
}

function doctrine_operational_snapshot_build(bool $persistSnapshots = false, string $reason = 'manual'): array
{
    try {
        $groups = db_doctrine_groups_all();
        $fits = db_doctrine_fits_all();
        $ungroupedFits = db_doctrine_ungrouped_fits();
    } catch (Throwable) {
        return ['groups' => [], 'fits' => [], 'ungrouped_fits' => [], 'top_missing_items' => [], 'global_layer' => ['rows' => [], 'top_restock_items' => []]];
    }

    $fitIds = array_values(array_map(static fn (array $fit): int => (int) ($fit['id'] ?? 0), $fits));
    $fitsByLookup = [];
    foreach ($fits as $fit) {
        $fitsByLookup[(int) ($fit['id'] ?? 0)] = $fit;
    }
    $itemsByFitId = [];
    $allTypeIds = [];
    try {
        foreach (db_doctrine_fit_items_by_fit_ids($fitIds) as $item) {
            $fitId = (int) ($item['doctrine_fit_id'] ?? 0);
            $itemsByFitId[$fitId][] = $item;
        }
    } catch (Throwable) {
    }

    foreach ($fitIds as $fitId) {
        $normalizedItems = doctrine_normalize_persisted_fit_items($fitsByLookup[$fitId] ?? ['id' => $fitId], $itemsByFitId[$fitId] ?? []);
        $itemsByFitId[$fitId] = item_scope_filter_rows(
            $normalizedItems,
            static fn (array $item): int => (int) ($item['type_id'] ?? 0)
        );
        foreach ($itemsByFitId[$fitId] as $item) {
            $typeId = (int) ($item['type_id'] ?? 0);
            if ($typeId > 0 && item_scope_is_type_id_in_scope($typeId)) {
                $allTypeIds[] = $typeId;
            }
        }
    }

    $comparisonByTypeId = doctrine_market_lookup_by_type_ids($allTypeIds);
    $localHistoryByTypeId = [];
    $itemLossByType = [];
    $hullLossByType = [];
    $allianceStructureId = configured_structure_destination_id_for_esi_sync();
    $hubRef = market_hub_setting_reference();
    $hubSourceId = sync_source_id_from_hub_ref($hubRef);
    if ($allianceStructureId > 0) {
        $startDate = gmdate('Y-m-d', strtotime('-14 days'));
        $endDate = gmdate('Y-m-d');
        try {
            $localHistoryByTypeId = doctrine_local_history_index(
                db_market_orders_history_stock_health_series(
                    'alliance_structure',
                    $allianceStructureId,
                    $startDate,
                    $endDate,
                    $allTypeIds
                )
            );
        } catch (Throwable) {
            $localHistoryByTypeId = [];
        }
    } elseif ($hubSourceId > 0) {
        try {
            $localHistoryByTypeId = doctrine_local_history_index(
                db_market_hub_local_history_daily_window_by_type_ids(
                    market_hub_local_history_source(),
                    $hubSourceId,
                    $allTypeIds,
                    14
                )
            );
        } catch (Throwable) {
            $localHistoryByTypeId = [];
        }
    }

    $durableLossTypeIds = array_values(array_filter(array_unique($allTypeIds), static fn (int $typeId): bool => item_scope_type_is_durable_loss_relevant($typeId)));
    try {
        $itemLossByType = doctrine_item_loss_index(db_killmail_tracked_recent_item_losses($durableLossTypeIds, 24 * 7));
    } catch (Throwable) {
        $itemLossByType = [];
    }

    $hullTypeIds = array_values(array_unique(array_filter(array_map(
        static fn (array $fit): int => isset($fit['ship_type_id']) ? (int) $fit['ship_type_id'] : 0,
        $fits
    ), static fn (int $typeId): bool => $typeId > 0 && item_scope_is_type_id_in_scope($typeId))));
    try {
        $hullLossByType = doctrine_hull_loss_index(db_killmail_tracked_recent_hull_losses($hullTypeIds, 24 * 7));
    } catch (Throwable) {
        $hullLossByType = [];
    }

    $depletionByType = doctrine_item_depletion_index($localHistoryByTypeId);
    $topMissing = [];
    $fitsById = [];
    $latestSnapshotsByFitId = [];
    if ($persistSnapshots) {
        try {
            foreach (db_doctrine_fit_latest_snapshots($fitIds) as $snapshot) {
                $latestSnapshotsByFitId[(int) ($snapshot['fit_id'] ?? 0)] = $snapshot;
            }
        } catch (Throwable) {
            $latestSnapshotsByFitId = [];
        }
    }
    $snapshotTime = gmdate('Y-m-d H:i:s');

    foreach ($fits as $fit) {
        $fitId = (int) ($fit['id'] ?? 0);
        $fitItems = $itemsByFitId[$fitId] ?? [];
        $marketRows = doctrine_fit_item_market_rows($fitItems, $comparisonByTypeId);
        $summary = doctrine_operational_supply($marketRows, $fitItems, $fit, $localHistoryByTypeId, $itemLossByType, $hullLossByType);
        $groupIds = doctrine_parse_group_csv($fit['group_ids_csv'] ?? null);
        $groupNames = doctrine_parse_group_names_csv($fit['group_names_csv'] ?? null);
        $fit['group_ids'] = $groupIds;
        $fit['group_names'] = $groupNames;
        $fit['ship_image_url'] = doctrine_ship_image_url(isset($fit['ship_type_id']) ? (int) $fit['ship_type_id'] : null, 64);
        $fit['supply'] = $summary;
        $fitsById[$fitId] = $fit;

        if ($persistSnapshots && $fitId > 0) {
            $snapshotRow = [
                'fit_id' => $fitId,
                'snapshot_time' => $snapshotTime,
                'complete_fits_available' => (int) ($summary['complete_fits_available'] ?? 0),
                'target_fits' => (int) ($summary['recommended_target_fit_count'] ?? 0),
                'fit_gap' => (int) ($summary['gap_to_target_fit_count'] ?? 0),
                'bottleneck_type_id' => isset($summary['bottleneck_type_id']) ? (int) $summary['bottleneck_type_id'] : null,
                'bottleneck_quantity' => (int) ($summary['bottleneck_quantity'] ?? 0),
                'readiness_state' => (string) ($summary['readiness_state'] ?? $summary['status'] ?? 'unknown'),
                'resupply_pressure_state' => (string) ($summary['resupply_pressure_state'] ?? 'stable'),
                'resupply_pressure_code' => (string) ($summary['resupply_pressure_state'] ?? 'stable'),
                'resupply_pressure_text' => (string) ($summary['resupply_pressure_label'] ?? 'Stable'),
                'recommendation_code' => (string) ($summary['recommendation_code'] ?? 'observe'),
                'recommendation_text' => (string) ($summary['combined_status_label'] ?? $summary['recommendation_text'] ?? ''),
                'loss_24h' => max((int) ($summary['recent_hull_losses_24h'] ?? 0), (int) ($summary['recent_item_fit_losses_24h'] ?? 0)),
                'loss_7d' => max((int) ($summary['recent_hull_losses_7d'] ?? 0), (int) ($summary['recent_item_fit_losses_7d'] ?? 0)),
                'local_coverage_pct' => (float) ($summary['coverage_percent'] ?? 0.0),
                'depletion_24h' => (int) ($summary['depletion_24h'] ?? 0),
                'depletion_7d' => (int) ($summary['depletion_7d'] ?? 0),
                'total_score' => (float) (($summary['driver_scores']['total'] ?? 0.0)),
                'score_loss_pressure' => (float) (($summary['driver_scores']['loss_pressure'] ?? 0.0)),
                'score_stock_gap' => (float) (($summary['driver_scores']['stock_gap'] ?? 0.0)),
                'score_depletion' => (float) (($summary['driver_scores']['depletion'] ?? 0.0)),
                'score_bottleneck' => (float) (($summary['driver_scores']['bottleneck'] ?? 0.0)),
            ];
            try {
                db_doctrine_fit_snapshot_insert($snapshotRow);
                $fit['snapshot_change'] = doctrine_snapshot_delta($latestSnapshotsByFitId[$fitId] ?? null, $snapshotRow);
                $fit['latest_snapshot'] = $snapshotRow;
                $fit['snapshot_reason'] = $reason;
                $fitsById[$fitId] = $fit;
            } catch (Throwable) {
            }
        }

        foreach ($marketRows as $row) {
            if (($row['is_stock_tracked'] ?? true) !== true) {
                continue;
            }
            $missingQty = (int) ($row['missing_qty'] ?? 0);
            if ($missingQty <= 0) {
                continue;
            }
            $key = (string) ((int) ($row['type_id'] ?? 0) > 0 ? 'type:' . (int) ($row['type_id'] ?? 0) : 'name:' . doctrine_normalize_item_name((string) ($row['item_name'] ?? '')));
            if (!isset($topMissing[$key])) {
                $topMissing[$key] = [
                    'item_name' => (string) ($row['item_name'] ?? ''),
                    'type_id' => isset($row['type_id']) ? (int) $row['type_id'] : null,
                    'missing_qty' => 0,
                    'restock_gap_isk' => 0.0,
                    'fit_count' => 0,
                    'priority_score' => 0,
                ];
            }
            $topMissing[$key]['missing_qty'] += $missingQty;
            $topMissing[$key]['restock_gap_isk'] += (float) ($row['restock_gap_isk'] ?? 0.0);
            $topMissing[$key]['fit_count']++;
            $topMissing[$key]['priority_score'] += (int) (($summary['driver_scores']['total'] ?? 0) + (($summary['bottleneck_type_id'] ?? null) === ($row['type_id'] ?? null) ? 12 : 0));
        }
    }

    foreach ($groups as &$group) {
        $groupId = (int) ($group['id'] ?? 0);
        $groupFits = array_values(array_filter($fitsById, static fn (array $fit): bool => in_array($groupId, (array) ($fit['group_ids'] ?? []), true)));
        $group['fits'] = $groupFits;
        $group['ready_fit_count'] = count(array_filter($groupFits, static fn (array $fit): bool => (($fit['supply']['readiness_state'] ?? 'market_ready') === 'market_ready')));
        $group['gap_fit_count'] = count(array_filter($groupFits, static fn (array $fit): bool => (($fit['supply']['readiness_state'] ?? 'market_ready') !== 'market_ready')));
        $group['missing_lines'] = array_sum(array_map(static fn (array $fit): int => (int) (($fit['supply']['missing_lines'] ?? 0)), $groupFits));
        $group['total_missing_qty'] = array_sum(array_map(static fn (array $fit): int => (int) (($fit['supply']['total_missing_qty'] ?? 0)), $groupFits));
        $group['restock_gap_isk'] = array_sum(array_map(static fn (array $fit): float => (float) (($fit['supply']['restock_gap_isk'] ?? 0.0)), $groupFits));
        $group['coverage_percent'] = count($groupFits) > 0 ? array_sum(array_map(static fn (array $fit): float => (float) (($fit['supply']['coverage_percent'] ?? 0.0)), $groupFits)) / count($groupFits) : 0.0;
        $group['complete_fits_available'] = array_sum(array_map(static fn (array $fit): int => (int) (($fit['supply']['complete_fits_available'] ?? 0)), $groupFits));
        $group['target_fit_count'] = array_sum(array_map(static fn (array $fit): int => (int) (($fit['supply']['recommended_target_fit_count'] ?? 0)), $groupFits));
        $group['fit_gap_count'] = array_sum(array_map(static fn (array $fit): int => (int) (($fit['supply']['gap_to_target_fit_count'] ?? 0)), $groupFits));
        $group['loss_pressure_fit_count'] = count(array_filter($groupFits, static fn (array $fit): bool => !in_array((string) (($fit['supply']['resupply_pressure_state'] ?? 'stable')), ['stable', 'elevated'], true)));
        $group['pressure_fit_count'] = count(array_filter($groupFits, static fn (array $fit): bool => (($fit['supply']['resupply_pressure_state'] ?? 'stable') !== 'stable')));
        $group['trending_down_fit_count'] = count(array_filter($groupFits, static fn (array $fit): bool => (($fit['supply']['readiness_trend_direction'] ?? 'unknown') === 'down')));

        $readinessSeverity = 0;
        $pressureSeverity = 0;
        $group['status'] = 'market_ready';
        $group['status_label'] = 'Market ready';
        $group['pressure_state'] = 'stable';
        $group['pressure_label'] = 'Stable';

        foreach ($groupFits as $fit) {
            $supply = (array) ($fit['supply'] ?? []);
            $fitReadiness = doctrine_readiness_state(
                ['complete_fits_available' => (int) ($supply['complete_fits_available'] ?? 0)],
                [
                    'recommended_target_fit_count' => (int) ($supply['recommended_target_fit_count'] ?? 0),
                    'gap_to_target_fit_count' => (int) ($supply['gap_to_target_fit_count'] ?? 0),
                ]
            );
            if ((int) ($fitReadiness['severity'] ?? 0) > $readinessSeverity) {
                $readinessSeverity = (int) ($fitReadiness['severity'] ?? 0);
                $group['status'] = (string) ($fitReadiness['state'] ?? 'market_ready');
                $group['status_label'] = (string) ($fitReadiness['label'] ?? 'Market ready');
            }

            $fitPressureState = (string) ($supply['resupply_pressure_state'] ?? 'stable');
            $fitPressureSeverity = match ($fitPressureState) {
                'urgent_resupply' => 4,
                'resupply_soon' => 3,
                'elevated' => 2,
                default => 1,
            };
            if ($fitPressureSeverity > $pressureSeverity) {
                $pressureSeverity = $fitPressureSeverity;
                $group['pressure_state'] = $fitPressureState;
                $group['pressure_label'] = (string) ($supply['resupply_pressure_label'] ?? 'Stable');
            }
        }

        $group['combined_status_label'] = (string) ($group['status_label'] ?? 'Market ready') . ' · ' . (string) ($group['pressure_label'] ?? 'Stable');
        $group['readiness_trend'] = $group['trending_down_fit_count'] > 0
            ? 'Trending down'
            : (count(array_filter($groupFits, static fn (array $fit): bool => (($fit['supply']['readiness_trend_direction'] ?? 'unknown') === 'up'))) > 0 ? 'Trending up' : 'Stable');
    }
    unset($group);

    usort($groups, static fn (array $a, array $b): int => ((int) ($b['fit_gap_count'] ?? 0) <=> (int) ($a['fit_gap_count'] ?? 0)) ?: strcasecmp((string) ($a['group_name'] ?? ''), (string) ($b['group_name'] ?? '')));
    usort($topMissing, static fn (array $a, array $b): int => ((int) ($b['priority_score'] ?? 0) <=> (int) ($a['priority_score'] ?? 0)) ?: ((int) ($b['missing_qty'] ?? 0) <=> (int) ($a['missing_qty'] ?? 0)));
    $globalLayer = doctrine_global_item_layer($fitsById, array_merge(...array_values($itemsByFitId ?: [[]])), $depletionByType);

    return [
        'groups' => $groups,
        'fits' => array_values($fitsById),
        'ungrouped_fits' => $ungroupedFits,
        'top_missing_items' => array_slice(array_values($topMissing), 0, 10),
        'global_layer' => $globalLayer,
    ];
}

function doctrine_operational_snapshot(): array
{
    return supplycore_cache_aside('doctrine', ['operational-snapshot'], supplycore_cache_ttl('doctrine_summary'), static function (): array {
        $snapshot = doctrine_snapshot_cache_payload();
        if ($snapshot !== null) {
            return $snapshot;
        }

        return doctrine_operational_snapshot_build(false, 'cached-read-fallback');
    }, [
        'dependencies' => ['doctrine', 'market_compare', 'killmail_overview'],
        'lock_ttl' => 20,
    ]);
}

function supplycore_summary_refresh_interval_seconds(): int
{
    $configured = (int) get_setting('summary_refresh_interval_seconds', '300');

    return max(60, min(3600, $configured > 0 ? $configured : 300));
}

function supplycore_summary_stale_after_seconds(): int
{
    return max(600, supplycore_summary_refresh_interval_seconds() * 3);
}

function supplycore_materialized_snapshot_keys(): array
{
    return [
        'doctrine_fit_intelligence',
        'doctrine_group_intelligence',
        'market_comparison_summaries',
        'loss_demand_summaries',
        'dashboard_summaries',
        'activity_priority_summaries',
    ];
}

function supplycore_materialized_snapshot_redis_key(string $snapshotKey): string
{
    return 'intelligence_snapshot:' . trim($snapshotKey);
}

function supplycore_materialized_snapshot_normalize_meta(string $snapshotKey, array $meta = []): array
{
    $intervalSeconds = max(60, (int) ($meta['refresh_interval_seconds'] ?? supplycore_summary_refresh_interval_seconds()));
    $staleAfterSeconds = max($intervalSeconds, (int) ($meta['stale_after_seconds'] ?? supplycore_summary_stale_after_seconds()));
    $computedAt = trim((string) ($meta['computed_at'] ?? ($meta['generated_at'] ?? '')));
    $status = trim((string) ($meta['status'] ?? 'ready'));
    if ($status === '') {
        $status = 'ready';
    }

    $computedUnix = $computedAt !== '' ? strtotime($computedAt) : false;
    $ageSeconds = $computedUnix !== false ? max(0, time() - $computedUnix) : null;
    $freshnessState = 'stale';
    if ($status === 'updating') {
        $freshnessState = 'updating';
    } elseif ($ageSeconds !== null && $ageSeconds <= (int) ceil($intervalSeconds * 1.5)) {
        $freshnessState = 'fresh';
    }

    return $meta + [
        'snapshot_key' => $snapshotKey,
        'status' => $status,
        'computed_at' => $computedAt !== '' ? $computedAt : null,
        'refresh_interval_seconds' => $intervalSeconds,
        'stale_after_seconds' => $staleAfterSeconds,
        'age_seconds' => $ageSeconds,
        'freshness_state' => $freshnessState,
        'freshness_label' => match ($freshnessState) {
            'fresh' => 'Fresh',
            'updating' => 'Updating',
            default => 'Stale',
        },
    ];
}

function supplycore_materialized_snapshot_fetch(string $snapshotKey): ?array
{
    $redisPayload = supplycore_redis_get_json(supplycore_materialized_snapshot_redis_key($snapshotKey));
    if (is_array($redisPayload) && isset($redisPayload['payload']) && is_array($redisPayload['payload'])) {
        $meta = supplycore_materialized_snapshot_normalize_meta($snapshotKey, is_array($redisPayload['meta'] ?? null) ? $redisPayload['meta'] : []);

        return [
            'payload' => $redisPayload['payload'],
            'meta' => $meta,
            'source' => 'redis',
        ];
    }

    try {
        $row = db_intelligence_snapshot_get($snapshotKey);
    } catch (Throwable) {
        $row = null;
    }

    if (!is_array($row)) {
        return null;
    }

    $payload = json_decode((string) ($row['payload_json'] ?? ''), true);
    if (!is_array($payload)) {
        $payload = [];
    }

    $storedMeta = json_decode((string) ($row['metadata_json'] ?? ''), true);
    $meta = supplycore_materialized_snapshot_normalize_meta($snapshotKey, is_array($storedMeta) ? $storedMeta : []);
    if (($row['computed_at'] ?? null) !== null) {
        $meta['computed_at'] = (string) $row['computed_at'];
    }
    if (($row['refresh_started_at'] ?? null) !== null) {
        $meta['refresh_started_at'] = (string) $row['refresh_started_at'];
    }
    if (($row['expires_at'] ?? null) !== null) {
        $meta['expires_at'] = (string) $row['expires_at'];
    }
    $meta['status'] = (string) ($row['snapshot_status'] ?? ($meta['status'] ?? 'ready'));
    $meta = supplycore_materialized_snapshot_normalize_meta($snapshotKey, $meta);

    $ttlSeconds = max(60, $meta['stale_after_seconds']);
    supplycore_redis_set_json(supplycore_materialized_snapshot_redis_key($snapshotKey), [
        'payload' => $payload,
        'meta' => $meta,
    ], $ttlSeconds);

    return [
        'payload' => $payload,
        'meta' => $meta,
        'source' => 'db',
    ];
}

function supplycore_materialized_snapshot_attach_meta(array $payload, array $meta): array
{
    $payload['_freshness'] = supplycore_materialized_snapshot_normalize_meta((string) ($meta['snapshot_key'] ?? 'snapshot'), $meta);

    return $payload;
}

function supplycore_materialized_snapshot_store(string $snapshotKey, array $payload, array $meta = []): array
{
    $computedAt = gmdate(DATE_ATOM);
    $normalizedMeta = supplycore_materialized_snapshot_normalize_meta($snapshotKey, $meta + [
        'snapshot_key' => $snapshotKey,
        'status' => 'ready',
        'computed_at' => $computedAt,
        'generated_at' => $computedAt,
    ]);
    $payloadJson = json_encode($payload, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);
    $metaJson = json_encode($normalizedMeta, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);
    $expiresAt = gmdate('Y-m-d H:i:s', time() + max(60, (int) $normalizedMeta['stale_after_seconds']));

    db_intelligence_snapshot_upsert(
        $snapshotKey,
        'ready',
        is_string($payloadJson) ? $payloadJson : null,
        is_string($metaJson) ? $metaJson : null,
        gmdate('Y-m-d H:i:s'),
        $normalizedMeta['refresh_started_at'] ?? null,
        $expiresAt
    );

    supplycore_redis_set_json(supplycore_materialized_snapshot_redis_key($snapshotKey), [
        'payload' => $payload,
        'meta' => $normalizedMeta + ['expires_at' => $expiresAt],
    ], max(60, (int) $normalizedMeta['stale_after_seconds']));

    return supplycore_materialized_snapshot_attach_meta($payload, $normalizedMeta + ['expires_at' => $expiresAt]);
}

function supplycore_materialized_snapshot_mark_updating(string $snapshotKey, string $reason): void
{
    $existing = supplycore_materialized_snapshot_fetch($snapshotKey);
    $payload = is_array($existing['payload'] ?? null) ? $existing['payload'] : [];
    $meta = supplycore_materialized_snapshot_normalize_meta($snapshotKey, is_array($existing['meta'] ?? null) ? $existing['meta'] : []);
    $meta['status'] = 'updating';
    $meta['reason'] = $reason;
    $meta['refresh_started_at'] = gmdate(DATE_ATOM);
    $metaJson = json_encode($meta, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);
    $expiresAt = gmdate('Y-m-d H:i:s', time() + max(60, (int) $meta['stale_after_seconds']));

    db_intelligence_snapshot_mark_updating($snapshotKey, is_string($metaJson) ? $metaJson : null, $expiresAt);

    if ($payload !== []) {
        supplycore_redis_set_json(supplycore_materialized_snapshot_redis_key($snapshotKey), [
            'payload' => $payload,
            'meta' => $meta + ['expires_at' => $expiresAt],
        ], max(60, (int) $meta['stale_after_seconds']));
    }
}

function supplycore_materialized_snapshot_read_or_bootstrap(string $snapshotKey, callable $builder, string $reason): array
{
    $snapshot = supplycore_materialized_snapshot_fetch($snapshotKey);
    if ($snapshot !== null && is_array($snapshot['payload'] ?? null) && $snapshot['payload'] !== []) {
        return supplycore_materialized_snapshot_attach_meta($snapshot['payload'], is_array($snapshot['meta'] ?? null) ? $snapshot['meta'] : []);
    }

    $payload = $builder();
    if (!is_array($payload)) {
        $payload = [];
    }

    return supplycore_materialized_snapshot_store($snapshotKey, $payload, [
        'reason' => $reason,
        'status' => 'ready',
    ]);
}

function supplycore_snapshot_freshness(string $snapshotKey): array
{
    $snapshot = supplycore_materialized_snapshot_fetch($snapshotKey);

    return is_array($snapshot['meta'] ?? null)
        ? supplycore_materialized_snapshot_normalize_meta($snapshotKey, $snapshot['meta'])
        : supplycore_materialized_snapshot_normalize_meta($snapshotKey, []);
}

function supplycore_format_datetime(?string $value): string
{
    if ($value === null) {
        return 'Unavailable';
    }

    $timestamp = strtotime($value);
    if ($timestamp === false) {
        return 'Unavailable';
    }

    return gmdate('Y-m-d H:i:s \U\T\C', $timestamp);
}

function supplycore_relative_datetime(?string $value): string
{
    if ($value === null) {
        return 'Never';
    }

    $timestamp = strtotime($value);
    if ($timestamp === false) {
        return 'Unknown';
    }

    $seconds = max(0, time() - $timestamp);

    return human_duration_ago($seconds) . ' ago';
}

function supplycore_page_freshness_view_model(array $freshness): array
{
    $computedAt = isset($freshness['computed_at']) ? (string) $freshness['computed_at'] : null;
    $state = (string) ($freshness['freshness_state'] ?? 'stale');

    return [
        'state' => $state,
        'label' => (string) ($freshness['freshness_label'] ?? ucfirst($state)),
        'computed_at' => supplycore_format_datetime($computedAt),
        'computed_relative' => supplycore_relative_datetime($computedAt),
        'reason' => (string) ($freshness['reason'] ?? ''),
        'tone' => match ($state) {
            'fresh' => 'border-emerald-400/20 bg-emerald-500/10 text-emerald-100',
            'updating' => 'border-sky-400/20 bg-sky-500/10 text-sky-100',
            default => 'border-amber-400/20 bg-amber-500/10 text-amber-100',
        },
        'message' => match ($state) {
            'fresh' => 'Background summaries are current.',
            'updating' => 'A background refresh is currently rebuilding this view.',
            default => 'Displayed results are older than the target refresh cadence.',
        },
    ];
}

function doctrine_fit_snapshot_key(): string
{
    return 'doctrine_fit_intelligence';
}

function doctrine_group_snapshot_key(): string
{
    return 'doctrine_group_intelligence';
}

function doctrine_snapshot_cache_payload(): ?array
{
    $fitSnapshot = supplycore_materialized_snapshot_fetch(doctrine_fit_snapshot_key());
    $groupSnapshot = supplycore_materialized_snapshot_fetch(doctrine_group_snapshot_key());

    if (!is_array($fitSnapshot) || !is_array($groupSnapshot)) {
        return null;
    }

    $fitPayload = is_array($fitSnapshot['payload'] ?? null) ? $fitSnapshot['payload'] : [];
    $groupPayload = is_array($groupSnapshot['payload'] ?? null) ? $groupSnapshot['payload'] : [];
    $fitMeta = is_array($fitSnapshot['meta'] ?? null) ? $fitSnapshot['meta'] : [];
    $groupMeta = is_array($groupSnapshot['meta'] ?? null) ? $groupSnapshot['meta'] : [];
    $freshness = $fitMeta;

    if (($groupMeta['computed_at'] ?? null) !== null && strtotime((string) $groupMeta['computed_at']) > strtotime((string) ($fitMeta['computed_at'] ?? '1970-01-01T00:00:00+00:00'))) {
        $freshness = $groupMeta;
    }

    return supplycore_materialized_snapshot_attach_meta([
        'groups' => array_values((array) ($groupPayload['groups'] ?? [])),
        'fits' => array_values((array) ($fitPayload['fits'] ?? [])),
        'ungrouped_fits' => array_values((array) ($fitPayload['ungrouped_fits'] ?? [])),
        'top_missing_items' => array_values((array) ($fitPayload['top_missing_items'] ?? [])),
        'global_layer' => is_array($fitPayload['global_layer'] ?? null)
            ? $fitPayload['global_layer']
            : ['rows' => [], 'top_restock_items' => []],
    ], $freshness);
}

function doctrine_snapshot_metadata(): array
{
    return supplycore_snapshot_freshness(doctrine_fit_snapshot_key());
}

function doctrine_store_snapshot(array $snapshot, string $reason): bool
{
    $meta = [
        'reason' => $reason,
        'fit_count' => count((array) ($snapshot['fits'] ?? [])),
        'group_count' => count((array) ($snapshot['groups'] ?? [])),
        'top_missing_count' => count((array) ($snapshot['top_missing_items'] ?? [])),
    ];

    supplycore_materialized_snapshot_store(doctrine_fit_snapshot_key(), [
        'fits' => array_values((array) ($snapshot['fits'] ?? [])),
        'ungrouped_fits' => array_values((array) ($snapshot['ungrouped_fits'] ?? [])),
        'top_missing_items' => array_values((array) ($snapshot['top_missing_items'] ?? [])),
        'global_layer' => is_array($snapshot['global_layer'] ?? null)
            ? $snapshot['global_layer']
            : ['rows' => [], 'top_restock_items' => []],
    ], $meta);
    supplycore_materialized_snapshot_store(doctrine_group_snapshot_key(), [
        'groups' => array_values((array) ($snapshot['groups'] ?? [])),
    ], $meta);

    return true;
}

function doctrine_refresh_intelligence_job_result(string $reason = 'manual'): array
{
    $snapshot = doctrine_refresh_intelligence($reason);
    $fitCount = count((array) ($snapshot['fits'] ?? []));
    $meta = is_array($snapshot['_freshness'] ?? null)
        ? $snapshot['_freshness']
        : doctrine_snapshot_metadata();

    return sync_result_shape() + [
        'rows_seen' => $fitCount,
        'rows_written' => $fitCount,
        'cursor' => 'doctrine_snapshot:' . gmdate('Y-m-d H:i:s'),
        'checksum' => sync_checksum([
            'fit_count' => $fitCount,
            'generated_at' => (string) ($meta['computed_at'] ?? gmdate(DATE_ATOM)),
            'reason' => $reason,
        ]),
        'meta' => [
            'outcome_reason' => 'Doctrine fit and doctrine group intelligence were refreshed into materialized summaries and Redis delivery cache.',
            'snapshot_generated_at' => (string) ($meta['computed_at'] ?? ''),
            'snapshot_reason' => (string) ($meta['reason'] ?? $reason),
        ],
    ];
}

function doctrine_refresh_intelligence(string $reason = 'manual'): array
{
    supplycore_materialized_snapshot_mark_updating(doctrine_fit_snapshot_key(), $reason);
    supplycore_materialized_snapshot_mark_updating(doctrine_group_snapshot_key(), $reason);

    $snapshot = doctrine_operational_snapshot_build(true, $reason);
    doctrine_store_snapshot($snapshot, $reason);
    supplycore_cache_bust(['doctrine', 'dashboard']);

    return supplycore_materialized_snapshot_attach_meta($snapshot, doctrine_snapshot_metadata());
}

function doctrine_schedule_intelligence_refresh(string $reason = 'manual'): void
{
    foreach ([doctrine_fit_snapshot_key(), doctrine_group_snapshot_key(), market_comparison_snapshot_key(), loss_demand_snapshot_key(), dashboard_snapshot_key(), activity_priority_snapshot_key()] as $snapshotKey) {
        supplycore_materialized_snapshot_mark_updating($snapshotKey, $reason);
    }

    try {
        db_sync_schedule_force_due_by_job_keys(doctrine_refresh_trigger_job_keys());
    } catch (Throwable) {
    }
}

function doctrine_refresh_trigger_job_keys(): array
{
    return [
        'doctrine_intelligence_sync',
        'market_comparison_summary_sync',
        'loss_demand_summary_sync',
        'dashboard_summary_sync',
        'activity_priority_summary_sync',
        'analytics_bucket_1h_sync',
        'analytics_bucket_1d_sync',
        'rebuild_ai_briefings',
    ];
}

function analytics_bucket_refresh_job_result(string $resolution = '1h', string $reason = 'manual'): array
{
    $result = db_time_series_refresh_all($resolution);
    $meta = is_array($result['meta'] ?? null) ? $result['meta'] : [];
    $safeResolution = $resolution === '1d' ? '1d' : '1h';
    $killmailMeta = is_array($meta['killmail'] ?? null) ? $meta['killmail'] : [];
    $marketMeta = is_array($meta['market'] ?? null) ? $meta['market'] : [];

    return sync_result_shape() + [
        'rows_seen' => (int) ($result['rows_seen'] ?? 0),
        'rows_written' => (int) ($result['rows_written'] ?? 0),
        'cursor' => 'analytics_bucket:' . $safeResolution . ':' . gmdate('Y-m-d H:i:s'),
        'checksum' => sync_checksum([
            'resolution' => $safeResolution,
            'rows_seen' => (int) ($result['rows_seen'] ?? 0),
            'rows_written' => (int) ($result['rows_written'] ?? 0),
            'reason' => $reason,
        ]),
        'meta' => [
            'outcome_reason' => 'MariaDB analytics bucket tables were incrementally upserted for ' . $safeResolution . ' windows.',
            'resolution' => $safeResolution,
            'killmail_rows_processed' => (int) ($killmailMeta['source_rows'] ?? $killmailMeta['rows_seen'] ?? 0),
            'market_rows_processed' => (int) ($marketMeta['source_rows'] ?? $marketMeta['rows_seen'] ?? 0),
            'killmail_duration_ms' => (int) ($killmailMeta['duration_ms'] ?? 0),
            'market_duration_ms' => (int) ($marketMeta['duration_ms'] ?? 0),
            'last_killmail_processed_timestamp' => (string) ($killmailMeta['last_processed_timestamp'] ?? ''),
            'last_market_processed_timestamp' => (string) ($marketMeta['last_processed_timestamp'] ?? ''),
            'has_more_work' => !empty($meta['has_more_work']),
        ] + $meta,
    ];
}

function supplycore_refresh_current_state_cache(string $reason = 'manual'): array
{
    $market = market_comparison_refresh_summary($reason . ':market');
    $lossDemand = loss_demand_refresh_summary($reason . ':loss-demand');
    $dashboard = dashboard_refresh_summary($reason . ':dashboard');
    supplycore_cache_bust(['dashboard', 'market_compare']);

    $rows = count((array) ($market['rows'] ?? []));
    $queues = count((array) ($dashboard['priority_queues']['opportunities'] ?? []))
        + count((array) ($dashboard['priority_queues']['risks'] ?? []))
        + count((array) ($dashboard['priority_queues']['missing_items'] ?? []));

    return sync_result_shape() + [
        'rows_seen' => $rows,
        'rows_written' => $queues + count((array) ($lossDemand['rows'] ?? [])),
        'cursor' => 'current_state:' . gmdate('Y-m-d H:i:s'),
        'checksum' => sync_checksum([
            'reason' => $reason,
            'rows' => $rows,
            'queues' => $queues,
        ]),
        'meta' => [
            'outcome_reason' => 'Market comparison, loss-demand, and dashboard summaries were refreshed into materialized current-state layers.',
        ],
    ];
}

function supplycore_forecasting_snapshot_setting_key(): string
{
    return 'supplycore_forecasting_snapshot_v1';
}

function supplycore_forecasting_snapshot_metadata_setting_key(): string
{
    return 'supplycore_forecasting_snapshot_meta_v1';
}

function supplycore_forecasting_snapshot(): array
{
    $raw = trim((string) get_setting(supplycore_forecasting_snapshot_setting_key(), ''));
    if ($raw === '') {
        return [];
    }

    $decoded = json_decode($raw, true);

    return is_array($decoded) ? $decoded : [];
}

function supplycore_forecasting_snapshot_store(array $snapshot, string $reason): bool
{
    $meta = [
        'generated_at' => gmdate(DATE_ATOM),
        'reason' => $reason,
        'forecast_count' => count((array) ($snapshot['forecast_rows'] ?? [])),
        'anomaly_count' => count((array) ($snapshot['anomalies'] ?? [])),
    ];

    return save_settings([
        supplycore_forecasting_snapshot_setting_key() => json_encode($snapshot, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE),
        supplycore_forecasting_snapshot_metadata_setting_key() => json_encode($meta, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE),
    ]);
}

function supplycore_forecasting_snapshot_build(string $reason = 'manual'): array
{
    $snapshot = doctrine_snapshot_cache_payload();
    if ($snapshot === null) {
        $snapshot = doctrine_refresh_intelligence($reason . ':bootstrap');
    }

    $fits = array_values((array) ($snapshot['fits'] ?? []));
    $forecastRows = [];
    $anomalies = [];
    $briefings = [];
    $explanations = [];

    foreach ($fits as $fit) {
        $fitId = (int) ($fit['id'] ?? 0);
        $fitName = trim((string) ($fit['fit_name'] ?? ('Fit #' . $fitId)));
        $supply = is_array($fit['supply'] ?? null) ? $fit['supply'] : [];
        $gap = max(0, (int) ($supply['gap_to_target_fit_count'] ?? 0));
        $target = max(0, (int) ($supply['recommended_target_fit_count'] ?? 0));
        $losses7d = max((int) ($supply['recent_hull_losses_7d'] ?? 0), (int) ($supply['recent_item_fit_losses_7d'] ?? 0));
        $depletionState = (string) ($supply['depletion_state'] ?? 'stable');
        $totalScore = (float) (($supply['driver_scores']['total'] ?? 0.0));
        $restockGap = (float) ($supply['restock_gap_isk'] ?? 0.0);
        $pressureIndex = round(($gap * 12) + ($losses7d * 4) + ($depletionState === 'draining' ? 15 : 0) + min(35, $totalScore * 0.4), 2);

        $forecastRows[] = [
            'fit_id' => $fitId,
            'fit_name' => $fitName,
            'target_fits' => $target,
            'fit_gap' => $gap,
            'losses_7d' => $losses7d,
            'depletion_state' => $depletionState,
            'pressure_index' => $pressureIndex,
            'restock_gap_isk' => $restockGap,
            'recommended_action' => (string) ($supply['recommendation_text'] ?? 'Observe'),
        ];

        if ($gap >= 3 || $totalScore >= 70.0 || $depletionState === 'draining') {
            $anomalies[] = [
                'fit_id' => $fitId,
                'fit_name' => $fitName,
                'signal' => $depletionState === 'draining'
                    ? 'Depletion is draining faster than the doctrine buffer.'
                    : ($gap >= 3 ? 'Doctrine target gap has widened materially.' : 'Recommendation pressure score spiked above the normal threshold.'),
            ];
        }

        if ($gap > 0 || $restockGap > 0.0) {
            $briefings[] = [
                'fit_id' => $fitId,
                'fit_name' => $fitName,
                'briefing' => $fitName . ' needs ' . doctrine_format_quantity($gap) . ' more fit'
                    . ($gap === 1 ? '' : 's')
                    . ' to reach target, with ' . market_format_isk($restockGap) . ' estimated hub spend.',
            ];
        }

        $explanations[] = [
            'fit_id' => $fitId,
            'fit_name' => $fitName,
            'recommendation_code' => (string) ($supply['recommendation_code'] ?? 'observe'),
            'recommendation_text' => (string) ($supply['recommendation_text'] ?? 'Observe'),
            'recommendation_explanation' => (string) ($supply['recommendation_explanation'] ?? 'No explanation available.'),
        ];
    }

    usort($forecastRows, static fn (array $a, array $b): int => ((float) ($b['pressure_index'] ?? 0.0) <=> (float) ($a['pressure_index'] ?? 0.0)) ?: strcasecmp((string) ($a['fit_name'] ?? ''), (string) ($b['fit_name'] ?? '')));
    usort($anomalies, static fn (array $a, array $b): int => strcasecmp((string) ($a['fit_name'] ?? ''), (string) ($b['fit_name'] ?? '')));
    usort($briefings, static fn (array $a, array $b): int => strcasecmp((string) ($a['fit_name'] ?? ''), (string) ($b['fit_name'] ?? '')));

    return [
        'generated_at' => gmdate(DATE_ATOM),
        'reason' => $reason,
        'forecast_rows' => array_slice($forecastRows, 0, 12),
        'anomalies' => array_slice($anomalies, 0, 12),
        'briefings' => array_slice($briefings, 0, 8),
        'recommendation_explanations' => array_slice($explanations, 0, 20),
    ];
}

function supplycore_refresh_forecasting_snapshot_job_result(string $reason = 'manual'): array
{
    $snapshot = supplycore_forecasting_snapshot_build($reason);
    supplycore_forecasting_snapshot_store($snapshot, $reason);

    return sync_result_shape() + [
        'rows_seen' => count((array) ($snapshot['forecast_rows'] ?? [])),
        'rows_written' => count((array) ($snapshot['briefings'] ?? [])) + count((array) ($snapshot['anomalies'] ?? [])),
        'cursor' => 'forecasting_snapshot:' . gmdate('Y-m-d H:i:s'),
        'checksum' => sync_checksum([
            'generated_at' => (string) ($snapshot['generated_at'] ?? gmdate(DATE_ATOM)),
            'reason' => $reason,
            'forecast_rows' => count((array) ($snapshot['forecast_rows'] ?? [])),
        ]),
        'meta' => [
            'outcome_reason' => 'Slow forecasting batch refreshed target-adjustment, anomaly, briefing, and recommendation-explanation views from the latest medium snapshot.',
        ],
    ];
}

function doctrine_groups_overview_data(): array
{
    $snapshot = doctrine_operational_snapshot();
    $groups = $snapshot['groups'] ?? [];
    $fits = $snapshot['fits'] ?? [];
    $briefingMap = doctrine_ai_briefings_fetch_map(array_merge(
        array_map(static fn (array $group): array => ['entity_type' => 'group', 'entity_id' => (int) ($group['id'] ?? 0)], $groups),
        array_map(static fn (array $fit): array => ['entity_type' => 'fit', 'entity_id' => (int) ($fit['id'] ?? 0)], $fits)
    ));

    foreach ($groups as &$group) {
        $group['ai_briefing'] = $briefingMap['group:' . (int) ($group['id'] ?? 0)] ?? null;
    }
    unset($group);

    foreach ($fits as &$fit) {
        $fit['ai_briefing'] = $briefingMap['fit:' . (int) ($fit['id'] ?? 0)] ?? null;
    }
    unset($fit);

    $notReadyFits = array_values(array_filter($fits, static fn (array $fit): bool => (($fit['supply']['readiness_state'] ?? 'market_ready') !== 'market_ready')));
    $pressureFits = array_values(array_filter($fits, static fn (array $fit): bool => (($fit['supply']['resupply_pressure_state'] ?? 'stable') !== 'stable')));
    usort($notReadyFits, static fn (array $a, array $b): int => ((float) (($b['supply']['driver_scores']['total'] ?? 0.0)) <=> (float) (($a['supply']['driver_scores']['total'] ?? 0.0))) ?: ((int) (($b['supply']['gap_to_target_fit_count'] ?? 0)) <=> (int) (($a['supply']['gap_to_target_fit_count'] ?? 0))));
    $totalMissingQty = array_sum(array_map(static fn (array $fit): int => (int) (($fit['supply']['total_missing_qty'] ?? 0)), $notReadyFits));
    $restockGap = array_sum(array_map(static fn (array $fit): float => (float) (($fit['supply']['restock_gap_isk'] ?? 0.0)), $notReadyFits));
    $completeFitsAvailable = array_sum(array_map(static fn (array $fit): int => (int) (($fit['supply']['complete_fits_available'] ?? 0)), $fits));
    $targetFitsDesired = array_sum(array_map(static fn (array $fit): int => (int) (($fit['supply']['recommended_target_fit_count'] ?? 0)), $fits));
    $fitGap = max(0, $targetFitsDesired - $completeFitsAvailable);
    $watchFits = count($notReadyFits);
    $pressureFitCount = count($pressureFits);
    $globalLayer = $snapshot['global_layer'] ?? ['rows' => [], 'top_restock_items' => []];
    $topBottlenecks = array_values(array_filter($globalLayer['rows'] ?? [], static fn (array $row): bool => (bool) ($row['is_bottleneck_item'] ?? false)));

    return [
        'summary' => [
            ['label' => 'Doctrine Groups', 'value' => (string) count($groups), 'context' => 'Operational doctrine collections under active readiness tracking'],
            ['label' => 'Complete Fits', 'value' => doctrine_format_quantity($completeFitsAvailable), 'context' => doctrine_format_quantity($watchFits) . ' fits have readiness gaps · ' . doctrine_format_quantity($pressureFitCount) . ' fits show pressure'],
            ['label' => 'Target Fits', 'value' => doctrine_format_quantity($targetFitsDesired), 'context' => 'Readiness uses fit coverage; resupply pressure uses losses, depletion, drain, and bottlenecks'],
            ['label' => 'Fit Gap', 'value' => doctrine_format_quantity($fitGap), 'context' => market_format_isk($restockGap) . ' estimated hub spend still missing'],
        ],
        'groups' => $groups,
        'fits' => $fits,
        'not_ready_fits' => $notReadyFits,
        'pressure_fits' => $pressureFits,
        'top_missing_items' => $snapshot['top_missing_items'] ?? [],
        'top_bottlenecks' => array_slice($topBottlenecks, 0, 8),
        'highest_priority_restock_items' => array_slice((array) ($globalLayer['top_restock_items'] ?? []), 0, 8),
        'global_layer' => $globalLayer,
        'ungrouped_fits' => $snapshot['ungrouped_fits'] ?? [],
        'freshness' => $snapshot['_freshness'] ?? doctrine_snapshot_metadata(),
    ];
}

function doctrine_group_detail_data(int $groupId): array
{
    $snapshot = doctrine_operational_snapshot();
    $groups = $snapshot['groups'] ?? [];
    foreach ($groups as $group) {
        if ((int) ($group['id'] ?? 0) === $groupId) {
            $fits = array_values((array) ($group['fits'] ?? []));
            $briefingMap = doctrine_ai_briefings_fetch_map(array_merge(
                [['entity_type' => 'group', 'entity_id' => $groupId]],
                array_map(static fn (array $fit): array => ['entity_type' => 'fit', 'entity_id' => (int) ($fit['id'] ?? 0)], $fits)
            ));
            $group['ai_briefing'] = $briefingMap['group:' . $groupId] ?? null;
            foreach ($fits as &$fit) {
                $fit['ai_briefing'] = $briefingMap['fit:' . (int) ($fit['id'] ?? 0)] ?? null;
            }
            unset($fit);

            return ['group' => $group, 'fits' => $fits, 'freshness' => $snapshot['_freshness'] ?? doctrine_snapshot_metadata()];
        }
    }

    return ['group' => null, 'fits' => [], 'freshness' => $snapshot['_freshness'] ?? doctrine_snapshot_metadata()];
}

function doctrine_parse_json_array(mixed $value): array
{
    if (is_array($value)) {
        return $value;
    }

    if (!is_string($value) || trim($value) === '') {
        return [];
    }

    $decoded = json_decode($value, true);

    return is_array($decoded) ? $decoded : [];
}

function doctrine_fit_readiness_status(array $fit): string
{
    if ((int) ($fit['unresolved_count'] ?? 0) > 0) {
        return 'Unresolved';
    }

    if ((string) ($fit['parse_status'] ?? 'ready') === 'review') {
        return 'Needs review';
    }

    if ((string) ($fit['conflict_state'] ?? 'none') !== 'none') {
        return 'Conflict';
    }

    return 'Ready';
}

function doctrine_fit_overview_data(array $query = []): array
{
    $filters = [
        'search' => (string) ($query['q'] ?? ''),
        'group_id' => (int) ($query['group_id'] ?? 0),
        'hull' => (string) ($query['hull'] ?? ''),
        'source_type' => (string) ($query['source_type'] ?? ''),
        'parse_status' => (string) ($query['parse_status'] ?? ''),
        'review_status' => (string) ($query['review_status'] ?? ''),
        'conflict_state' => (string) ($query['conflict_state'] ?? ''),
        'unresolved_only' => in_array((string) ($query['unresolved_only'] ?? ''), ['1', 'true', 'yes', 'on'], true),
    ];
    $sort = (string) ($query['sort'] ?? 'updated_desc');

    try {
        $fits = db_doctrine_fit_overview($filters, $sort);
    } catch (Throwable) {
        $fits = [];
    }

    foreach ($fits as &$fit) {
        $fit['group_names'] = doctrine_parse_group_names_csv($fit['group_names_csv'] ?? null);
        $fit['readiness_status'] = doctrine_fit_readiness_status($fit);
        $fit['parse_status_label'] = (($fit['parse_status'] ?? 'ready') === 'review') ? 'Review' : 'Ready';
    }
    unset($fit);

    return [
        'filters' => $filters,
        'sort' => $sort,
        'fits' => $fits,
        'groups' => doctrine_group_options(),
        'summary' => [
            'total' => count($fits),
            'review' => count(array_filter($fits, static fn (array $fit): bool => (($fit['parse_status'] ?? 'ready') === 'review'))),
            'unresolved' => count(array_filter($fits, static fn (array $fit): bool => ((int) ($fit['unresolved_count'] ?? 0) > 0))),
            'conflicts' => count(array_filter($fits, static fn (array $fit): bool => (($fit['conflict_state'] ?? 'none') !== 'none'))),
        ],
    ];
}

function doctrine_fit_detail_view_model(int $fitId): array
{
    return supplycore_cache_aside('doctrine', ['fit-detail', $fitId], supplycore_cache_ttl('doctrine_detail'), static function () use ($fitId): array {
        $snapshot = doctrine_operational_snapshot();
        $fitFromSnapshot = null;

        foreach (array_values((array) ($snapshot['fits'] ?? [])) as $candidate) {
            if ((int) ($candidate['id'] ?? 0) === $fitId) {
                $fitFromSnapshot = $candidate;
                break;
            }
        }

        try {
            $fit = db_doctrine_fit_by_id($fitId);
            $items = db_doctrine_fit_items_by_fit($fitId);
        } catch (Throwable) {
            $fit = null;
            $items = [];
        }

        if ($fit === null) {
            return ['fit' => null, 'categories' => [], 'summary' => [], 'items' => [], 'freshness' => $snapshot['_freshness'] ?? []];
        }

        if (is_array($fitFromSnapshot)) {
            $fit = array_replace($fit, $fitFromSnapshot);
        }

        $fit['group_ids'] = doctrine_parse_group_csv($fit['group_ids_csv'] ?? null);
        $fit['group_names'] = doctrine_parse_group_names_csv($fit['group_names_csv'] ?? null);
        $fit['metadata'] = doctrine_parse_json_array($fit['metadata_json'] ?? null);
        $fit['parse_warnings'] = doctrine_parse_json_array($fit['parse_warnings_json'] ?? null);
        $fit['readiness_status'] = doctrine_fit_readiness_status($fit);
        $items = doctrine_normalize_persisted_fit_items($fit, $items);
        $items = item_scope_filter_rows(
            $items,
            static fn (array $item): int => (int) ($item['type_id'] ?? 0)
        );
        $comparison = doctrine_market_lookup_by_type_ids(array_map(static fn (array $item): int => (int) ($item['type_id'] ?? 0), $items));
        $categories = doctrine_group_market_rows_by_category($items, $comparison);

        foreach ($categories as &$rows) {
            foreach ($rows as &$row) {
                $row['required_qty_label'] = doctrine_format_quantity((int) ($row['quantity'] ?? 1));
                $row['local_available_qty_label'] = doctrine_format_quantity((int) ($row['local_available_qty'] ?? 0));
                $row['missing_qty_label'] = (($row['is_stock_tracked'] ?? true) === true)
                    ? doctrine_format_quantity((int) ($row['missing_qty'] ?? 0))
                    : 'Externally managed';
                $row['local_price_label'] = market_format_isk(isset($row['local_price']) ? (float) $row['local_price'] : null);
                $row['hub_price_label'] = market_format_isk(isset($row['hub_price']) ? (float) $row['hub_price'] : null);
                $row['restock_gap_label'] = (($row['is_stock_tracked'] ?? true) === true)
                    ? market_format_isk(isset($row['restock_gap_isk']) ? (float) $row['restock_gap_isk'] : null)
                    : 'Externally managed';
            }
            unset($row);
        }
        unset($rows);

        $supply = is_array($fit['supply'] ?? null) ? $fit['supply'] : [];
        $history = doctrine_snapshot_history_view_model($fitId, $supply);
        $fit['ship_image_url'] = doctrine_ship_image_url(isset($fit['ship_type_id']) ? (int) $fit['ship_type_id'] : null, 256);
        $fit['supply'] = $supply;
        $fit['snapshot_history'] = $history;
        $fit['ai_briefing'] = doctrine_ai_briefing_get('fit', $fitId);

        return [
            'fit' => $fit,
            'categories' => $categories,
            'items' => $items,
            'summary' => [
                ['label' => 'Readiness', 'value' => (string) ($supply['readiness_label'] ?? $supply['status_label'] ?? 'Market ready'), 'context' => (string) ($supply['readiness_context'] ?? 'Operational readiness unavailable.')],
                ['label' => 'Resupply Pressure', 'value' => (string) ($supply['resupply_pressure_label'] ?? 'Stable'), 'context' => (string) ($supply['resupply_pressure_context'] ?? 'Resupply pressure unavailable.')],
                ['label' => 'Complete Fits', 'value' => doctrine_format_quantity((int) ($supply['complete_fits_available'] ?? 0)), 'context' => (string) ($supply['constraint_label'] ?? 'Current fleet-ready fit count unavailable.')],
                ['label' => 'Fit Gap', 'value' => doctrine_format_quantity((int) ($supply['gap_to_target_fit_count'] ?? 0)), 'context' => market_format_isk((float) ($supply['restock_gap_isk'] ?? 0.0)) . ' estimated hub spend to close the doctrine gap'],
            ],
            'snapshot_history' => $history,
            'freshness' => $snapshot['_freshness'] ?? [],
        ];
    }, [
        'dependencies' => ['doctrine', 'market_compare', 'killmail_overview'],
        'lock_ttl' => 20,
    ]);
}

function supplycore_ai_ollama_config(): array
{
    static $config = null;

    if (is_array($config)) {
        return $config;
    }

    $defaults = ai_briefing_setting_defaults();
    $settings = get_settings(ai_briefing_setting_keys());
    $provider = sanitize_ollama_provider($settings['ollama_provider'] ?? $defaults['ollama_provider']);

    $config = [
        'enabled' => (($settings['ollama_enabled'] ?? $defaults['ollama_enabled']) === '1'),
        'provider' => $provider,
        'url' => sanitize_ollama_url($settings['ollama_url'] ?? $defaults['ollama_url']),
        'model' => sanitize_ollama_model($settings['ollama_model'] ?? $defaults['ollama_model']),
        'timeout' => max(1, (int) sanitize_ollama_timeout($settings['ollama_timeout'] ?? $defaults['ollama_timeout'])),
        'capability_override' => sanitize_ollama_capability_tier($settings['ollama_capability_tier'] ?? $defaults['ollama_capability_tier']),
        'runpod_url' => sanitize_ollama_runpod_url($settings['ollama_runpod_url'] ?? $defaults['ollama_runpod_url']),
        'runpod_api_key' => sanitize_ollama_runpod_api_key($settings['ollama_runpod_api_key'] ?? $defaults['ollama_runpod_api_key']),
    ];

    $inferredTier = supplycore_ai_infer_model_capability_tier((string) ($config['model'] ?? ''));
    $effectiveTier = ($config['capability_override'] ?? 'auto') !== 'auto'
        ? (string) $config['capability_override']
        : $inferredTier;
    $strategy = supplycore_ai_capability_strategy($effectiveTier);

    return [
        'enabled' => (bool) ($config['enabled'] ?? false),
        'provider' => (string) ($config['provider'] ?? $defaults['ollama_provider']),
        'url' => (string) ($config['url'] ?? $defaults['ollama_url']),
        'model' => (string) ($config['model'] ?? $defaults['ollama_model']),
        'timeout' => max(1, (int) ($config['timeout'] ?? (int) $defaults['ollama_timeout'])),
        'capability_override' => (string) ($config['capability_override'] ?? 'auto'),
        'runpod_url' => (string) ($config['runpod_url'] ?? ''),
        'runpod_api_key' => (string) ($config['runpod_api_key'] ?? ''),
        'runpod_api_key_masked' => supplycore_mask_secret((string) ($config['runpod_api_key'] ?? '')),
        'inferred_tier' => $inferredTier,
        'capability_tier' => $effectiveTier,
        'strategy' => $strategy,
    ];
}

function supplycore_mask_secret(string $value): string
{
    $trimmed = trim($value);
    if ($trimmed === '') {
        return '';
    }

    $length = mb_strlen($trimmed);
    if ($length <= 8) {
        return str_repeat('•', $length);
    }

    return mb_substr($trimmed, 0, 4) . str_repeat('•', max(4, $length - 8)) . mb_substr($trimmed, -4);
}

function supplycore_ai_infer_model_capacity_billions(string $model): ?float
{
    $normalized = mb_strtolower(trim($model));
    if ($normalized === '') {
        return null;
    }

    if (preg_match('/(?:^|[:\-_ ])(\d+(?:\.\d+)?)\s*b(?:[^a-z0-9]|$)/i', $normalized, $matches) === 1) {
        return max(0.0, (float) $matches[1]);
    }

    if (preg_match('/(\d+(?:\.\d+)?)\s*b\b/i', $normalized, $matches) === 1) {
        return max(0.0, (float) $matches[1]);
    }

    return null;
}

function supplycore_ai_infer_model_capability_tier(string $model): string
{
    $billions = supplycore_ai_infer_model_capacity_billions($model);
    if ($billions !== null) {
        if ($billions < 3.0) {
            return 'small';
        }

        if ($billions <= 8.0) {
            return 'medium';
        }

        return 'large';
    }

    $normalized = mb_strtolower(trim($model));
    if ($normalized === '') {
        return 'small';
    }

    if (str_contains($normalized, '1.5b') || str_contains($normalized, '2b')) {
        return 'small';
    }

    if (str_contains($normalized, '3b') || str_contains($normalized, '7b') || str_contains($normalized, '8b')) {
        return 'medium';
    }

    return 'medium';
}

function supplycore_ai_capability_strategy(string $tier): array
{
    $normalizedTier = in_array($tier, ['small', 'medium', 'large'], true) ? $tier : 'small';

    $base = [
        'tier' => $normalizedTier,
        'background_only' => true,
        'strict_json' => true,
        'authoritative_source' => 'deterministic',
        'candidate_limit' => 5,
        'dashboard_limit' => 3,
        'history_limit' => 0,
        'group_history_fit_limit' => 0,
        'prompt_style' => 'compact',
        'prompt_max_chars' => 1600,
        'summary_max_chars' => 240,
        'action_max_chars' => 220,
        'explanation_max_chars' => 0,
        'operator_briefing_max_chars' => 0,
        'trend_max_chars' => 0,
        'task_labels' => [
            'fit' => 'doctrine fit briefing',
            'group' => 'doctrine group briefing',
        ],
        'enabled_tasks' => [
            'fit_briefing',
            'group_briefing',
            'dashboard_critical_note',
        ],
        'include_previous_recommendation' => false,
        'include_snapshot_delta' => false,
        'include_cross_signal_reasoning' => false,
        'include_historical_context' => false,
        'include_operator_briefing' => false,
        'include_trend_interpretation' => false,
        'forecast_language' => 'avoid',
    ];

    return match ($normalizedTier) {
        'medium' => $base + [
            'candidate_limit' => 12,
            'dashboard_limit' => 6,
            'history_limit' => 2,
            'group_history_fit_limit' => 2,
            'prompt_style' => 'balanced',
            'prompt_max_chars' => 3200,
            'summary_max_chars' => 420,
            'action_max_chars' => 320,
            'explanation_max_chars' => 260,
            'enabled_tasks' => [
                'fit_briefing',
                'group_briefing',
                'dashboard_critical_note',
                'recommendation_change_explanation',
                'doctrine_pressure_summary',
                'prioritized_restock_note',
            ],
            'include_previous_recommendation' => true,
            'include_snapshot_delta' => true,
            'include_cross_signal_reasoning' => true,
            'forecast_language' => 'minimal',
        ],
        'large' => $base + [
            'candidate_limit' => 20,
            'dashboard_limit' => 8,
            'history_limit' => 4,
            'group_history_fit_limit' => 3,
            'prompt_style' => 'rich',
            'prompt_max_chars' => 5200,
            'summary_max_chars' => 600,
            'action_max_chars' => 420,
            'explanation_max_chars' => 360,
            'operator_briefing_max_chars' => 360,
            'trend_max_chars' => 320,
            'enabled_tasks' => [
                'fit_briefing',
                'group_briefing',
                'dashboard_critical_note',
                'recommendation_change_explanation',
                'doctrine_pressure_summary',
                'prioritized_restock_note',
                'top_doctrine_risk_briefing',
                'market_doctrine_synthesis',
                'operator_guidance',
                'limited_forecast_commentary',
            ],
            'include_previous_recommendation' => true,
            'include_snapshot_delta' => true,
            'include_cross_signal_reasoning' => true,
            'include_historical_context' => true,
            'include_operator_briefing' => true,
            'include_trend_interpretation' => true,
            'forecast_language' => 'bounded',
        ],
        default => $base,
    };
}

function supplycore_ai_strategy(): array
{
    $config = supplycore_ai_ollama_config();

    return is_array($config['strategy'] ?? null)
        ? $config['strategy']
        : supplycore_ai_capability_strategy((string) ($config['capability_tier'] ?? 'small'));
}

function supplycore_ai_ollama_enabled(): bool
{
    $config = supplycore_ai_ollama_config();

    if ($config['enabled'] !== true || $config['model'] === '') {
        return false;
    }

    if (($config['provider'] ?? 'local') === 'runpod') {
        return ($config['runpod_url'] ?? '') !== '' && ($config['runpod_api_key'] ?? '') !== '';
    }

    return ($config['url'] ?? '') !== '';
}

function supplycore_ai_ollama_available(): bool
{
    static $availability = [];

    $config = supplycore_ai_ollama_config();
    if (!supplycore_ai_ollama_enabled()) {
        return false;
    }

    $cacheKey = implode('|', [
        (string) ($config['provider'] ?? 'local'),
        (string) ($config['url'] ?? ''),
        (string) ($config['runpod_url'] ?? ''),
        (string) ($config['model'] ?? ''),
        (string) ($config['timeout'] ?? 20),
    ]);
    if (array_key_exists($cacheKey, $availability)) {
        return $availability[$cacheKey];
    }

    if (($config['provider'] ?? 'local') === 'runpod') {
        $availability[$cacheKey] = ($config['runpod_url'] ?? '') !== '' && ($config['runpod_api_key'] ?? '') !== '';

        return $availability[$cacheKey];
    }

    $endpoint = rtrim((string) ($config['url'] ?? ''), '/') . '/tags';

    try {
        $response = http_get_json($endpoint, [], max(1, (int) ($config['timeout'] ?? 20)));
        $status = (int) ($response['status'] ?? 0);
        $json = is_array($response['json'] ?? null) ? $response['json'] : [];
        $availability[$cacheKey] = $status >= 200 && $status < 300 && is_array($json['models'] ?? null);
    } catch (Throwable) {
        $availability[$cacheKey] = false;
    }

    return $availability[$cacheKey];
}

function supplycore_ai_status_summary(): array
{
    $config = supplycore_ai_ollama_config();
    $provider = (string) ($config['provider'] ?? 'local');
    $available = supplycore_ai_ollama_available();

    if ($provider === 'runpod') {
        return [
            'ok' => $available,
            'label' => $available ? 'Runpod configured' : 'Runpod incomplete',
            'description' => $available
                ? 'Runpod serverless endpoint and API key are saved for AI briefing requests.'
                : 'Save both the Runpod endpoint and API key to enable serverless AI briefings.',
        ];
    }

    return [
        'ok' => $available,
        'label' => $available ? 'Ollama reachable' : 'Ollama not reachable',
        'description' => 'Status is checked against the configured local Ollama API endpoint.',
    ];
}

function supplycore_ai_log(string $event, array $payload = []): void
{
    try {
        error_log('[supplycore.ai] ' . json_encode([
            'event' => $event,
            'ts' => gmdate(DATE_ATOM),
        ] + $payload, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES));
    } catch (Throwable) {
        error_log('[supplycore.ai] ' . $event);
    }
}

function supplycore_ai_priority_rank(string $priority): int
{
    return match ($priority) {
        'critical' => 4,
        'high' => 3,
        'medium' => 2,
        default => 1,
    };
}

function supplycore_ai_priority_tone(string $priority): string
{
    return match ($priority) {
        'critical' => 'border-rose-400/20 bg-rose-500/10 text-rose-200',
        'high' => 'border-amber-400/20 bg-amber-500/10 text-amber-100',
        'medium' => 'border-sky-400/20 bg-sky-500/10 text-sky-100',
        default => 'border-emerald-400/20 bg-emerald-500/10 text-emerald-100',
    };
}

function doctrine_ai_cache_key(string $entityType, int $entityId): string
{
    return 'doctrine_ai_briefing:' . $entityType . ':' . max(0, $entityId);
}

function doctrine_ai_briefing_normalize_row(?array $row): ?array
{
    if (!is_array($row)) {
        return null;
    }

    $responseJson = json_decode((string) ($row['response_json'] ?? ''), true);
    $sourcePayload = json_decode((string) ($row['source_payload_json'] ?? ''), true);
    $priority = in_array((string) ($row['priority_level'] ?? 'medium'), ['low', 'medium', 'high', 'critical'], true)
        ? (string) $row['priority_level']
        : 'medium';

    return [
        'entity_type' => (string) ($row['entity_type'] ?? ''),
        'entity_id' => (int) ($row['entity_id'] ?? 0),
        'fit_id' => isset($row['fit_id']) ? (int) $row['fit_id'] : null,
        'group_id' => isset($row['group_id']) ? (int) $row['group_id'] : null,
        'generation_status' => (string) ($row['generation_status'] ?? 'ready'),
        'computed_at' => ($row['computed_at'] ?? null) !== null ? (string) $row['computed_at'] : null,
        'computed_relative' => supplycore_relative_datetime(($row['computed_at'] ?? null) !== null ? (string) $row['computed_at'] : null),
        'model_name' => ($row['model_name'] ?? null) !== null ? (string) $row['model_name'] : null,
        'headline' => trim((string) ($row['headline'] ?? '')),
        'summary' => trim((string) ($row['summary'] ?? '')),
        'action_text' => trim((string) ($row['action_text'] ?? '')),
        'explanation_text' => trim((string) ($responseJson['explanation'] ?? '')),
        'operator_briefing' => trim((string) ($responseJson['operator_briefing'] ?? '')),
        'trend_interpretation' => trim((string) ($responseJson['trend_interpretation'] ?? '')),
        'priority_level' => $priority,
        'priority_rank' => supplycore_ai_priority_rank($priority),
        'priority_tone' => supplycore_ai_priority_tone($priority),
        'capability_tier' => (string) (($sourcePayload['capability']['tier'] ?? $sourcePayload['capability_tier'] ?? 'small')),
        'source_payload' => is_array($sourcePayload) ? $sourcePayload : [],
        'response_json' => is_array($responseJson) ? $responseJson : [],
        'error_message' => ($row['error_message'] ?? null) !== null ? (string) $row['error_message'] : null,
        'updated_at' => ($row['updated_at'] ?? null) !== null ? (string) $row['updated_at'] : null,
    ];
}

function doctrine_ai_briefing_get(string $entityType, int $entityId): ?array
{
    $normalizedType = in_array($entityType, ['fit', 'group'], true) ? $entityType : '';
    $safeEntityId = max(0, $entityId);
    if ($normalizedType === '' || $safeEntityId <= 0) {
        return null;
    }

    $cached = supplycore_redis_get_json(doctrine_ai_cache_key($normalizedType, $safeEntityId));
    if (is_array($cached)) {
        return doctrine_ai_briefing_normalize_row($cached);
    }

    try {
        $row = db_doctrine_ai_briefing_get($normalizedType, $safeEntityId);
    } catch (Throwable) {
        $row = null;
    }

    if (!is_array($row)) {
        return null;
    }

    supplycore_redis_set_json(doctrine_ai_cache_key($normalizedType, $safeEntityId), $row, 300);

    return doctrine_ai_briefing_normalize_row($row);
}

function doctrine_ai_briefings_fetch_map(array $targets): array
{
    $normalizedTargets = [];
    $cacheKeys = [];

    foreach ($targets as $target) {
        if (!is_array($target)) {
            continue;
        }

        $entityType = in_array((string) ($target['entity_type'] ?? ''), ['fit', 'group'], true)
            ? (string) $target['entity_type']
            : '';
        $entityId = max(0, (int) ($target['entity_id'] ?? 0));
        if ($entityType === '' || $entityId <= 0) {
            continue;
        }

        $key = $entityType . ':' . $entityId;
        $normalizedTargets[$key] = ['entity_type' => $entityType, 'entity_id' => $entityId];
        $cacheKeys[$key] = doctrine_ai_cache_key($entityType, $entityId);
    }

    if ($normalizedTargets === []) {
        return [];
    }

    $map = [];
    $cacheKeyEntries = array_values($cacheKeys);
    $cachedRows = supplycore_redis_enabled() ? supplycore_redis_mget($cacheKeyEntries) : [];
    $targetKeys = array_keys($cacheKeys);
    foreach ($cacheKeyEntries as $index => $cacheKey) {
        $key = $targetKeys[$index] ?? null;
        if (!is_string($key)) {
            continue;
        }
        $cachedPayload = $cachedRows[$index] ?? null;
        if (!is_string($cachedPayload) || $cachedPayload === '') {
            continue;
        }

        $decoded = json_decode($cachedPayload, true);
        if (!is_array($decoded)) {
            continue;
        }

        $map[$key] = doctrine_ai_briefing_normalize_row($decoded);
    }

    $missingTargets = [];
    foreach ($normalizedTargets as $key => $target) {
        if (!array_key_exists($key, $map)) {
            $missingTargets[] = $target;
        }
    }

    if ($missingTargets !== []) {
        try {
            foreach (db_doctrine_ai_briefings_get_many($missingTargets) as $row) {
                $key = (string) ($row['entity_type'] ?? '') . ':' . (int) ($row['entity_id'] ?? 0);
                $map[$key] = doctrine_ai_briefing_normalize_row($row);
                supplycore_redis_set_json(doctrine_ai_cache_key((string) ($row['entity_type'] ?? ''), (int) ($row['entity_id'] ?? 0)), $row, 300);
            }
        } catch (Throwable) {
        }
    }

    return array_filter($map, static fn (mixed $value): bool => is_array($value));
}

function doctrine_ai_recent_change_summary_for_group(array $group): string
{
    $parts = [];
    $fitGapCount = max(0, (int) ($group['fit_gap_count'] ?? 0));
    $pressureFitCount = max(0, (int) ($group['pressure_fit_count'] ?? 0));
    $trendingDownFits = max(0, (int) ($group['trending_down_fit_count'] ?? 0));

    if ($fitGapCount > 0) {
        $parts[] = doctrine_format_quantity($fitGapCount) . ' fits are currently below target coverage.';
    }
    if ($pressureFitCount > 0) {
        $parts[] = doctrine_format_quantity($pressureFitCount) . ' fits show non-stable resupply pressure.';
    }
    if ($trendingDownFits > 0) {
        $parts[] = doctrine_format_quantity($trendingDownFits) . ' fits are trending down.';
    }

    return $parts !== [] ? implode(' ', $parts) : 'No material group-level change is currently flagged by deterministic signals.';
}

function doctrine_ai_previous_recommendation_fact(?array $briefing): ?array
{
    if (!is_array($briefing)) {
        return null;
    }

    $action = trim((string) ($briefing['action_text'] ?? ''));
    $priority = trim((string) ($briefing['priority_level'] ?? ''));
    if ($action === '' && $priority === '') {
        return null;
    }

    return [
        'priority' => $priority !== '' ? $priority : null,
        'action' => $action !== '' ? $action : null,
        'computed_at' => ($briefing['computed_at'] ?? null) !== null ? (string) $briefing['computed_at'] : null,
    ];
}

function doctrine_ai_history_context_for_fit(int $fitId, int $limit = 0): array
{
    $safeFitId = max(0, $fitId);
    $safeLimit = max(0, min(6, $limit));
    if ($safeFitId <= 0 || $safeLimit <= 0) {
        return [];
    }

    try {
        $rows = db_doctrine_fit_snapshot_history($safeFitId, $safeLimit + 1);
    } catch (Throwable) {
        return [];
    }

    $history = [];
    foreach (array_slice($rows, 0, $safeLimit) as $row) {
        $history[] = [
            'snapshot_time' => (string) ($row['snapshot_time'] ?? ''),
            'ready_fits' => (int) ($row['complete_fits_available'] ?? 0),
            'target_fits' => (int) ($row['target_fit_count'] ?? 0),
            'fit_gap' => (int) ($row['fit_gap_count'] ?? 0),
            'readiness_state' => (string) ($row['readiness_state'] ?? ''),
            'pressure_state' => (string) ($row['resupply_pressure_state'] ?? ''),
            'loss_24h' => (int) ($row['recent_hull_losses_24h'] ?? 0),
            'loss_7d' => (int) ($row['recent_hull_losses_7d'] ?? 0),
            'restock_gap_isk' => (float) ($row['restock_gap_isk'] ?? 0.0),
            'bottleneck_item' => ($row['bottleneck_item_name'] ?? null) !== null ? (string) $row['bottleneck_item_name'] : null,
        ];
    }

    return $history;
}

function doctrine_ai_history_context_for_group(array $group, int $historyLimit = 0, int $fitLimit = 0): array
{
    $safeHistoryLimit = max(0, min(4, $historyLimit));
    $safeFitLimit = max(0, min(4, $fitLimit));
    if ($safeHistoryLimit <= 0 || $safeFitLimit <= 0) {
        return [];
    }

    $fits = array_values((array) ($group['fits'] ?? []));
    usort($fits, static function (array $a, array $b): int {
        $aSupply = is_array($a['supply'] ?? null) ? $a['supply'] : [];
        $bSupply = is_array($b['supply'] ?? null) ? $b['supply'] : [];

        return ((float) ($bSupply['driver_scores']['total'] ?? 0.0) <=> (float) ($aSupply['driver_scores']['total'] ?? 0.0))
            ?: ((int) ($a['id'] ?? 0) <=> (int) ($b['id'] ?? 0));
    });

    $history = [];
    foreach (array_slice($fits, 0, $safeFitLimit) as $fit) {
        $fitId = (int) ($fit['id'] ?? 0);
        if ($fitId <= 0) {
            continue;
        }

        $history[] = [
            'fit_name' => (string) ($fit['fit_name'] ?? ('Fit #' . $fitId)),
            'history' => doctrine_ai_history_context_for_fit($fitId, $safeHistoryLimit),
        ];
    }

    return array_values(array_filter($history, static fn (array $row): bool => $row['history'] !== []));
}

function doctrine_ai_source_payload_for_fit(array $fit, ?array $previousBriefing = null, ?array $strategy = null): array
{
    $resolvedStrategy = is_array($strategy) ? $strategy : supplycore_ai_strategy();
    $supply = is_array($fit['supply'] ?? null) ? $fit['supply'] : [];
    $recentChange = is_array($fit['snapshot_change'] ?? null)
        ? (string) (($fit['snapshot_change']['summary'] ?? '') ?: 'No prior doctrine snapshot change is available.')
        : 'No prior doctrine snapshot change is available.';

    $payload = [
        'entity_type' => 'fit',
        'entity_id' => (int) ($fit['id'] ?? 0),
        'name' => trim((string) ($fit['fit_name'] ?? ('Fit #' . (int) ($fit['id'] ?? 0)))),
        'group_name' => implode(', ', (array) ($fit['group_names'] ?? [])),
        'readiness_state' => (string) ($supply['readiness_label'] ?? $supply['status_label'] ?? 'Market ready'),
        'resupply_pressure' => (string) ($supply['resupply_pressure_label'] ?? 'Stable'),
        'complete_fits' => (int) ($supply['complete_fits_available'] ?? 0),
        'target_fits' => (int) ($supply['recommended_target_fit_count'] ?? 0),
        'fit_gap' => (int) ($supply['gap_to_target_fit_count'] ?? 0),
        'bottleneck_item' => ($supply['bottleneck_item_name'] ?? null) !== null ? (string) $supply['bottleneck_item_name'] : null,
        'bottleneck_quantity' => (int) ($supply['bottleneck_quantity'] ?? 0),
        'loss_24h' => max((int) ($supply['recent_hull_losses_24h'] ?? 0), (int) ($supply['recent_item_fit_losses_24h'] ?? 0)),
        'loss_7d' => max((int) ($supply['recent_hull_losses_7d'] ?? 0), (int) ($supply['recent_item_fit_losses_7d'] ?? 0)),
        'depletion_signal' => (string) ($supply['depletion_state'] ?? 'stable'),
        'current_recommendation' => (string) ($supply['recommendation_text'] ?? ''),
        'capability' => [
            'tier' => (string) ($resolvedStrategy['tier'] ?? 'small'),
            'prompt_style' => (string) ($resolvedStrategy['prompt_style'] ?? 'compact'),
            'enabled_tasks' => array_values((array) ($resolvedStrategy['enabled_tasks'] ?? [])),
        ],
        'signals' => [
            'restock_gap_isk' => (float) ($supply['restock_gap_isk'] ?? 0.0),
            'market_price_label' => (string) ($supply['market_price_status_label'] ?? ''),
            'readiness_code' => (string) ($supply['readiness_code'] ?? ''),
            'pressure_code' => (string) ($supply['resupply_pressure_code'] ?? ''),
        ],
    ];

    if (($resolvedStrategy['include_snapshot_delta'] ?? false) === true) {
        $payload['recent_change_summary'] = $recentChange;
    }

    if (($resolvedStrategy['include_previous_recommendation'] ?? false) === true) {
        $payload['previous_recommendation'] = doctrine_ai_previous_recommendation_fact($previousBriefing);
    }

    if (($resolvedStrategy['include_historical_context'] ?? false) === true) {
        $payload['history'] = doctrine_ai_history_context_for_fit((int) ($fit['id'] ?? 0), (int) ($resolvedStrategy['history_limit'] ?? 0));
    }

    return $payload;
}

function doctrine_ai_source_payload_for_group(array $group, ?array $previousBriefing = null, ?array $strategy = null): array
{
    $resolvedStrategy = is_array($strategy) ? $strategy : supplycore_ai_strategy();
    $fits = array_values((array) ($group['fits'] ?? []));
    $loss24h = 0;
    $loss7d = 0;
    $topBottleneckItem = null;
    $topBottleneckQty = 0;
    $topScore = -1.0;
    $depletionSignal = 'stable';
    $restockGapIsk = 0.0;
    $criticalFitCount = 0;

    foreach ($fits as $fit) {
        $supply = is_array($fit['supply'] ?? null) ? $fit['supply'] : [];
        $loss24h = max($loss24h, max((int) ($supply['recent_hull_losses_24h'] ?? 0), (int) ($supply['recent_item_fit_losses_24h'] ?? 0)));
        $loss7d = max($loss7d, max((int) ($supply['recent_hull_losses_7d'] ?? 0), (int) ($supply['recent_item_fit_losses_7d'] ?? 0)));
        $restockGapIsk += (float) ($supply['restock_gap_isk'] ?? 0.0);
        if ((string) ($supply['readiness_state'] ?? '') === 'critical_gap') {
            $criticalFitCount++;
        }
        $score = (float) (($supply['driver_scores']['total'] ?? 0.0));
        if ($score > $topScore) {
            $topScore = $score;
            $topBottleneckItem = ($supply['bottleneck_item_name'] ?? null) !== null ? (string) $supply['bottleneck_item_name'] : null;
            $topBottleneckQty = (int) ($supply['bottleneck_quantity'] ?? 0);
            $depletionSignal = (string) ($supply['depletion_state'] ?? 'stable');
        }
    }

    $payload = [
        'entity_type' => 'group',
        'entity_id' => (int) ($group['id'] ?? 0),
        'name' => trim((string) ($group['group_name'] ?? ('Group #' . (int) ($group['id'] ?? 0)))),
        'readiness_state' => (string) ($group['status_label'] ?? 'Market ready'),
        'resupply_pressure' => (string) ($group['pressure_label'] ?? 'Stable'),
        'complete_fits' => (int) ($group['complete_fits_available'] ?? 0),
        'target_fits' => (int) ($group['target_fit_count'] ?? 0),
        'fit_gap' => (int) ($group['fit_gap_count'] ?? 0),
        'bottleneck_item' => $topBottleneckItem,
        'bottleneck_quantity' => $topBottleneckQty,
        'loss_24h' => $loss24h,
        'loss_7d' => $loss7d,
        'depletion_signal' => $depletionSignal,
        'current_recommendation' => (string) ($group['combined_status_label'] ?? ''),
        'capability' => [
            'tier' => (string) ($resolvedStrategy['tier'] ?? 'small'),
            'prompt_style' => (string) ($resolvedStrategy['prompt_style'] ?? 'compact'),
            'enabled_tasks' => array_values((array) ($resolvedStrategy['enabled_tasks'] ?? [])),
        ],
        'signals' => [
            'fit_count' => (int) ($group['fit_count'] ?? count($fits)),
            'critical_fit_count' => $criticalFitCount,
            'pressure_fit_count' => (int) ($group['pressure_fit_count'] ?? 0),
            'trending_down_fit_count' => (int) ($group['trending_down_fit_count'] ?? 0),
            'restock_gap_isk' => $restockGapIsk,
        ],
    ];

    if (($resolvedStrategy['include_snapshot_delta'] ?? false) === true) {
        $payload['recent_change_summary'] = doctrine_ai_recent_change_summary_for_group($group);
    }

    if (($resolvedStrategy['include_previous_recommendation'] ?? false) === true) {
        $payload['previous_recommendation'] = doctrine_ai_previous_recommendation_fact($previousBriefing);
    }

    if (($resolvedStrategy['include_historical_context'] ?? false) === true) {
        $payload['history'] = doctrine_ai_history_context_for_group(
            $group,
            (int) ($resolvedStrategy['history_limit'] ?? 0),
            (int) ($resolvedStrategy['group_history_fit_limit'] ?? 0)
        );
    }

    return $payload;
}

function doctrine_ai_system_prompt(array $strategy, array $sourcePayload): string
{
    $taskLabel = (string) (($strategy['task_labels'][(string) ($sourcePayload['entity_type'] ?? '')] ?? 'doctrine briefing'));
    $instructions = [
        'You are an alliance logistics analyst.',
        'Use only the provided deterministic facts.',
        'Deterministic doctrine, market, loss, and readiness calculations remain authoritative.',
        'Do not invent data, market states, or history.',
        'Return JSON only.',
        'Generate a ' . $taskLabel . ' that matches the configured model capability tier: ' . (string) ($strategy['tier'] ?? 'small') . '.',
    ];

    if (($strategy['include_cross_signal_reasoning'] ?? false) === true) {
        $instructions[] = 'You may compare readiness, losses, depletion, and bottlenecks when the facts explicitly support it.';
    } else {
        $instructions[] = 'Avoid broad cross-doctrine reasoning; stay tightly scoped to the provided entity facts.';
    }

    $forecastLanguage = (string) ($strategy['forecast_language'] ?? 'avoid');
    if ($forecastLanguage === 'avoid') {
        $instructions[] = 'Avoid forecasting language beyond simple summary and prioritization.';
    } elseif ($forecastLanguage === 'minimal') {
        $instructions[] = 'Keep forward-looking language limited to near-term operator prioritization grounded in provided deltas.';
    } else {
        $instructions[] = 'Any forward-looking note must be limited, conditional, and grounded only in provided snapshot/history facts.';
    }

    return implode(' ', $instructions);
}

function doctrine_ai_output_schema(array $strategy): array
{
    $properties = [
        'headline' => ['type' => 'string'],
        'summary' => ['type' => 'string'],
        'action' => ['type' => 'string'],
        'priority' => ['type' => 'string', 'enum' => ['low', 'medium', 'high', 'critical']],
    ];

    if (($strategy['explanation_max_chars'] ?? 0) > 0) {
        $properties['explanation'] = ['type' => 'string'];
    }

    if (($strategy['include_operator_briefing'] ?? false) === true) {
        $properties['operator_briefing'] = ['type' => 'string'];
    }

    if (($strategy['include_trend_interpretation'] ?? false) === true) {
        $properties['trend_interpretation'] = ['type' => 'string'];
    }

    return [
        'type' => 'object',
        'required' => ['headline', 'summary', 'action', 'priority'],
        'properties' => $properties,
        'additionalProperties' => false,
    ];
}

function doctrine_ai_prompt_source_payload(array $sourcePayload, array $strategy): array
{
    $payload = $sourcePayload;

    if (($strategy['tier'] ?? 'small') === 'small') {
        unset($payload['history'], $payload['previous_recommendation']);
    }

    return $payload;
}

function doctrine_ai_user_prompt(array $sourcePayload, array $strategy): string
{
    $facts = doctrine_ai_prompt_source_payload($sourcePayload, $strategy);
    $prompt = [
        'Task: Summarize this doctrine logistics state using the configured capability tier.',
        'Rules:',
        '- Keep the response operational and bounded.',
        '- Use only facts from the payload.',
        '- Deterministic values remain the source of truth.',
    ];

    if (($strategy['tier'] ?? 'small') === 'small') {
        $prompt[] = '- Keep prompts and outputs short. Focus on the top critical item only.';
    } elseif (($strategy['tier'] ?? 'small') === 'medium') {
        $prompt[] = '- Explain key deltas and prioritize the clearest next action.';
    } else {
        $prompt[] = '- Include a richer explanation of why priorities changed, plus bounded operator guidance.';
    }

    $prompt[] = 'Facts:';
    $prompt[] = json_encode($facts, (($strategy['prompt_style'] ?? 'compact') === 'compact' ? 0 : JSON_PRETTY_PRINT) | JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);

    $rendered = implode("\n", $prompt);
    $maxChars = max(800, (int) ($strategy['prompt_max_chars'] ?? 1600));

    return mb_substr($rendered, 0, $maxChars);
}

function doctrine_ai_validate_response(array $response, array $strategy): array
{
    $headline = trim((string) ($response['headline'] ?? ''));
    $summary = trim((string) ($response['summary'] ?? ''));
    $action = trim((string) ($response['action'] ?? ''));
    $priority = trim((string) ($response['priority'] ?? ''));

    if ($headline === '' || $summary === '' || $action === '') {
        throw new RuntimeException('AI response is missing one or more required fields.');
    }

    if (!in_array($priority, ['low', 'medium', 'high', 'critical'], true)) {
        throw new RuntimeException('AI response priority is invalid.');
    }

    return [
        'headline' => mb_substr($headline, 0, 255),
        'summary' => mb_substr($summary, 0, max(120, (int) ($strategy['summary_max_chars'] ?? 240))),
        'action' => mb_substr($action, 0, max(120, (int) ($strategy['action_max_chars'] ?? 220))),
        'explanation' => mb_substr(trim((string) ($response['explanation'] ?? '')), 0, max(0, (int) ($strategy['explanation_max_chars'] ?? 0))),
        'operator_briefing' => mb_substr(trim((string) ($response['operator_briefing'] ?? '')), 0, max(0, (int) ($strategy['operator_briefing_max_chars'] ?? 0))),
        'trend_interpretation' => mb_substr(trim((string) ($response['trend_interpretation'] ?? '')), 0, max(0, (int) ($strategy['trend_max_chars'] ?? 0))),
        'priority' => $priority,
    ];
}

function doctrine_ai_fallback_response(array $sourcePayload, string $reason, ?array $strategy = null): array
{
    $resolvedStrategy = is_array($strategy) ? $strategy : supplycore_ai_strategy();
    $gap = max(0, (int) ($sourcePayload['fit_gap'] ?? 0));
    $pressure = (string) ($sourcePayload['resupply_pressure'] ?? 'Stable');
    $readiness = (string) ($sourcePayload['readiness_state'] ?? 'Market ready');
    $name = trim((string) ($sourcePayload['name'] ?? 'Doctrine item'));
    $bottleneckItem = trim((string) ($sourcePayload['bottleneck_item'] ?? ''));

    $priority = 'low';
    if ($gap >= 3 || str_contains(mb_strtolower($pressure), 'urgent') || str_contains(mb_strtolower($readiness), 'critical')) {
        $priority = 'critical';
    } elseif ($gap > 0 || str_contains(mb_strtolower($pressure), 'soon')) {
        $priority = 'high';
    } elseif (str_contains(mb_strtolower($pressure), 'elevated')) {
        $priority = 'medium';
    }

    $response = [
        'headline' => $name . ': ' . ($gap > 0 ? doctrine_format_quantity($gap) . ' fit gap' : $pressure),
        'summary' => trim($readiness . ' with ' . $pressure . '. ' . ($bottleneckItem !== '' ? ('Primary bottleneck remains ' . $bottleneckItem . '.') : 'Use deterministic doctrine metrics for precise quantities.')),
        'action' => $gap > 0
            ? 'Prioritize the current bottleneck and close the remaining fit gap before the next doctrine review.'
            : 'Monitor this doctrine state in the next background refresh and verify deterministic stock numbers before acting.',
        'priority' => $priority,
        '_fallback_reason' => $reason,
    ];

    if (($resolvedStrategy['explanation_max_chars'] ?? 0) > 0) {
        $response['explanation'] = ($sourcePayload['recent_change_summary'] ?? null) !== null
            ? (string) $sourcePayload['recent_change_summary']
            : 'No higher-order explanation is available because the deterministic fallback path was used.';
    }

    if (($resolvedStrategy['include_operator_briefing'] ?? false) === true) {
        $response['operator_briefing'] = 'Keep AI guidance secondary to the current doctrine snapshot, market totals, and loss indicators shown in SupplyCore.';
    }

    if (($resolvedStrategy['include_trend_interpretation'] ?? false) === true) {
        $response['trend_interpretation'] = ($sourcePayload['history'] ?? []) !== []
            ? 'Recent stored snapshot history is available; review the deterministic trend rows before making forecast-oriented decisions.'
            : 'Trend interpretation is limited until more local snapshot history is available.';
    }

    return $response;
}

function doctrine_ai_generate_briefing_result(array $sourcePayload): array
{
    $config = supplycore_ai_ollama_config();
    $strategy = supplycore_ai_strategy();

    try {
        $briefing = supplycore_ai_ollama_generate_json($sourcePayload);

        return [
            'status' => 'ready',
            'briefing' => $briefing,
            'error_message' => null,
            'model_name' => (string) ($config['model'] ?? ''),
        ];
    } catch (Throwable $exception) {
        $fallback = doctrine_ai_fallback_response($sourcePayload, $exception->getMessage(), $strategy);

        return [
            'status' => 'fallback',
            'briefing' => $fallback,
            'error_message' => $exception->getMessage(),
            'model_name' => 'deterministic-fallback',
        ];
    }
}

function supplycore_ai_ollama_generate_json(array $sourcePayload): array
{
    $config = supplycore_ai_ollama_config();
    $strategy = supplycore_ai_strategy();
    if ($config['enabled'] !== true) {
        throw new RuntimeException('Ollama integration is disabled.');
    }

    $systemPrompt = doctrine_ai_system_prompt($strategy, $sourcePayload);
    $userPrompt = doctrine_ai_user_prompt($sourcePayload, $strategy);
    $schema = doctrine_ai_output_schema($strategy);

    if (($config['provider'] ?? 'local') === 'runpod') {
        $decoded = supplycore_ai_runpod_generate_json($config, $systemPrompt, $userPrompt, $schema);

        return doctrine_ai_validate_response($decoded, $strategy);
    }

    $endpoint = $config['url'] . '/generate';
    $requestPayload = [
        'model' => $config['model'],
        'stream' => false,
        'system' => $systemPrompt,
        'prompt' => $userPrompt,
        'format' => $schema,
        'options' => [
            'temperature' => 0.1,
        ],
    ];

    $response = http_post_json($endpoint, [], $requestPayload, $config['timeout']);
    $status = (int) ($response['status'] ?? 0);
    $json = is_array($response['json'] ?? null) ? $response['json'] : [];
    if ($status < 200 || $status >= 300) {
        throw new RuntimeException('Ollama returned HTTP ' . $status . '.');
    }

    $rawModelResponse = trim((string) ($json['response'] ?? ''));
    if ($rawModelResponse === '') {
        throw new RuntimeException('Ollama returned an empty response payload.');
    }

    $decoded = json_decode($rawModelResponse, true);
    if (!is_array($decoded)) {
        throw new RuntimeException('Ollama returned malformed JSON content.');
    }

    return doctrine_ai_validate_response($decoded, $strategy);
}

function supplycore_ai_runpod_generate_json(array $config, string $systemPrompt, string $userPrompt, array $schema): array
{
    $headers = ['Authorization: Bearer ' . (string) ($config['runpod_api_key'] ?? '')];
    $requestPayload = [
        'input' => [
            'prompt' => supplycore_ai_runpod_prompt($config, $systemPrompt, $userPrompt, $schema),
        ],
    ];
    $response = http_post_json(
        supplycore_ai_runpod_submit_url((string) ($config['runpod_url'] ?? '')),
        $headers,
        $requestPayload,
        supplycore_ai_runpod_request_timeout($config)
    );
    $polledResponse = supplycore_ai_runpod_poll_until_complete($config, $headers, $response);

    return supplycore_ai_decode_runpod_response($polledResponse);
}

function supplycore_ai_runpod_prompt(array $config, string $systemPrompt, string $userPrompt, array $schema): string
{
    $schemaJson = json_encode($schema, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);
    if (!is_string($schemaJson)) {
        $schemaJson = '{}';
    }

    return trim(implode("\n\n", [
        'You are using the model "' . (string) ($config['model'] ?? '') . '" through a Runpod serverless endpoint.',
        'SYSTEM INSTRUCTIONS',
        $systemPrompt,
        'USER REQUEST',
        $userPrompt,
        'OUTPUT REQUIREMENTS',
        'Return JSON only. Do not include markdown fences or any commentary outside the JSON object.',
        'The JSON object must match this schema exactly:',
        $schemaJson,
    ]));
}

function supplycore_ai_runpod_submit_url(string $url): string
{
    $normalized = rtrim(trim($url), '/');
    if ($normalized === '') {
        return '';
    }

    if (preg_match('#/runsync$#i', $normalized) === 1) {
        return (string) preg_replace('#/runsync$#i', '/run', $normalized);
    }

    return $normalized;
}

function supplycore_ai_runpod_status_url(string $url, string $jobId): string
{
    $normalized = rtrim(trim($url), '/');
    if ($normalized === '') {
        throw new RuntimeException('Runpod endpoint is not configured.');
    }

    if (preg_match('#/(?:run|runsync)$#i', $normalized) === 1) {
        return (string) preg_replace('#/(?:run|runsync)$#i', '/status/' . rawurlencode($jobId), $normalized);
    }

    return $normalized . '/status/' . rawurlencode($jobId);
}

function supplycore_ai_runpod_request_timeout(array $config): int
{
    $configuredTimeout = max(1, (int) ($config['timeout'] ?? 20));

    return min(10, $configuredTimeout);
}

function supplycore_ai_runpod_poll_until_complete(array $config, array $headers, array $response): array
{
    $status = (int) ($response['status'] ?? 0);
    $json = is_array($response['json'] ?? null) ? $response['json'] : [];
    if ($status < 200 || $status >= 300) {
        throw new RuntimeException('Runpod returned HTTP ' . $status . '.');
    }

    $jobStatus = supplycore_ai_runpod_job_status($json);
    if (in_array($jobStatus, ['COMPLETED', 'SUCCESS'], true)) {
        return $response;
    }

    if ($jobStatus === '' && supplycore_ai_runpod_response_has_output($json)) {
        return $response;
    }

    if (in_array($jobStatus, ['FAILED', 'CANCELLED', 'TIMED_OUT'], true)) {
        throw new RuntimeException('Runpod job failed with status: ' . $jobStatus . '.');
    }

    $jobId = trim((string) ($json['id'] ?? $json['job_id'] ?? ''));
    if ($jobId === '') {
        throw new RuntimeException('Runpod did not return a job ID for async polling.');
    }

    $deadline = microtime(true) + max(1, (int) ($config['timeout'] ?? 20));
    $pollDelayMicroseconds = 750000;
    $statusUrl = supplycore_ai_runpod_status_url((string) ($config['runpod_url'] ?? ''), $jobId);
    $lastKnownStatus = $jobStatus !== '' ? $jobStatus : 'SUBMITTED';

    while (microtime(true) < $deadline) {
        usleep($pollDelayMicroseconds);

        $statusResponse = http_get_json($statusUrl, $headers, supplycore_ai_runpod_request_timeout($config));
        $statusCode = (int) ($statusResponse['status'] ?? 0);
        $statusJson = is_array($statusResponse['json'] ?? null) ? $statusResponse['json'] : [];
        if ($statusCode < 200 || $statusCode >= 300) {
            throw new RuntimeException('Runpod status returned HTTP ' . $statusCode . '.');
        }

        $lastKnownStatus = supplycore_ai_runpod_job_status($statusJson);
        if (in_array($lastKnownStatus, ['COMPLETED', 'SUCCESS'], true)) {
            return $statusResponse;
        }

        if ($lastKnownStatus === '' && supplycore_ai_runpod_response_has_output($statusJson)) {
            return $statusResponse;
        }

        if (in_array($lastKnownStatus, ['FAILED', 'CANCELLED', 'TIMED_OUT'], true)) {
            throw new RuntimeException('Runpod job failed with status: ' . $lastKnownStatus . '.');
        }
    }

    throw new RuntimeException('Runpod job did not finish before timeout (last status: ' . $lastKnownStatus . ').');
}

function supplycore_ai_runpod_job_status(array $payload): string
{
    return strtoupper(trim((string) ($payload['status'] ?? '')));
}

function supplycore_ai_runpod_response_has_output(array $payload): bool
{
    return array_key_exists('output', $payload) || array_key_exists('response', $payload);
}

function supplycore_ai_decode_runpod_response(array $response): array
{
    $status = (int) ($response['status'] ?? 0);
    $json = is_array($response['json'] ?? null) ? $response['json'] : [];
    if ($status < 200 || $status >= 300) {
        throw new RuntimeException('Runpod returned HTTP ' . $status . '.');
    }

    $jobStatus = supplycore_ai_runpod_job_status($json);
    if ($jobStatus !== '' && !in_array($jobStatus, ['COMPLETED', 'SUCCESS'], true)) {
        throw new RuntimeException('Runpod job did not complete synchronously (status: ' . $jobStatus . ').');
    }

    $candidates = [
        $json['output']['response'] ?? null,
        $json['output']['text'] ?? null,
        $json['output'][0]['response'] ?? null,
        $json['output'][0]['text'] ?? null,
        $json['response'] ?? null,
        $json['output'] ?? null,
    ];

    foreach ($candidates as $candidate) {
        if (is_array($candidate)) {
            if ($candidate !== [] && array_keys($candidate) !== range(0, count($candidate) - 1)) {
                return $candidate;
            }

            $candidate = json_encode($candidate, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);
        }

        $decoded = supplycore_ai_decode_json_string((string) $candidate);
        if (is_array($decoded)) {
            return $decoded;
        }
    }

    throw new RuntimeException('Runpod returned an unsupported response payload.');
}

function supplycore_ai_decode_json_string(string $value): ?array
{
    $trimmed = trim($value);
    if ($trimmed === '') {
        return null;
    }

    $decoded = json_decode($trimmed, true);
    if (is_array($decoded)) {
        return $decoded;
    }

    if (preg_match('/\{.*\}/s', $trimmed, $matches) === 1) {
        $decoded = json_decode((string) $matches[0], true);
        if (is_array($decoded)) {
            return $decoded;
        }
    }

    return null;
}

function doctrine_ai_store_briefing(array $sourcePayload, array $briefing, string $status, ?string $errorMessage = null): bool
{
    $entityType = (string) ($sourcePayload['entity_type'] ?? '');
    $entityId = max(0, (int) ($sourcePayload['entity_id'] ?? 0));
    if (!in_array($entityType, ['fit', 'group'], true) || $entityId <= 0) {
        return false;
    }

    $modelName = $status === 'ready' ? (string) supplycore_ai_ollama_config()['model'] : 'deterministic-fallback';
    $sourcePayloadJson = json_encode($sourcePayload, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);
    $responseJson = json_encode($briefing, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);
    $row = [
        'entity_type' => $entityType,
        'entity_id' => $entityId,
        'generation_status' => $status,
        'computed_at' => gmdate('Y-m-d H:i:s'),
        'model_name' => $modelName,
        'headline' => (string) ($briefing['headline'] ?? ''),
        'summary' => (string) ($briefing['summary'] ?? ''),
        'action_text' => (string) ($briefing['action'] ?? ''),
        'priority_level' => (string) ($briefing['priority'] ?? 'medium'),
        'source_payload_json' => is_string($sourcePayloadJson) ? $sourcePayloadJson : null,
        'response_json' => is_string($responseJson) ? $responseJson : null,
        'error_message' => $errorMessage,
    ];

    $saved = db_doctrine_ai_briefing_upsert($row);
    if ($saved) {
        $dbRow = db_doctrine_ai_briefing_get($entityType, $entityId);
        if (is_array($dbRow)) {
            supplycore_redis_set_json(doctrine_ai_cache_key($entityType, $entityId), $dbRow, 300);
        }
    }

    return $saved;
}

function doctrine_ai_entity_label(array $candidate): string
{
    $entityType = (string) ($candidate['entity_type'] ?? '');
    $entity = is_array($candidate['entity'] ?? null) ? $candidate['entity'] : [];
    $entityId = (int) ($candidate['entity_id'] ?? 0);

    if ($entityType === 'fit') {
        return trim((string) ($entity['fit_name'] ?? ('Fit #' . $entityId)));
    }

    if ($entityType === 'group') {
        return trim((string) ($entity['group_name'] ?? ('Group #' . $entityId)));
    }

    return $entityType !== '' ? ($entityType . ':' . $entityId) : ('Entity #' . $entityId);
}

function doctrine_ai_candidate_score(array $entity, string $entityType): float
{
    if ($entityType === 'fit') {
        $supply = is_array($entity['supply'] ?? null) ? $entity['supply'] : [];
        $priority = (float) (($supply['driver_scores']['total'] ?? 0.0));
        $gap = (int) ($supply['gap_to_target_fit_count'] ?? 0);
        $pressureWeight = match ((string) ($supply['resupply_pressure_state'] ?? 'stable')) {
            'urgent_resupply' => 40,
            'resupply_soon' => 24,
            'elevated' => 10,
            default => 0,
        };
        $readinessWeight = match ((string) ($supply['readiness_state'] ?? 'market_ready')) {
            'critical_gap' => 35,
            'partial_gap' => 18,
            default => 0,
        };

        return $priority + ($gap * 12) + $pressureWeight + $readinessWeight;
    }

    $gap = (int) ($entity['fit_gap_count'] ?? 0);
    $pressureWeight = match ((string) ($entity['pressure_state'] ?? 'stable')) {
        'urgent_resupply' => 40,
        'resupply_soon' => 24,
        'elevated' => 10,
        default => 0,
    };
    $readinessWeight = match ((string) ($entity['status'] ?? 'market_ready')) {
        'critical_gap' => 35,
        'partial_gap' => 18,
        default => 0,
    };

    return ($gap * 14) + $pressureWeight + $readinessWeight + ((int) ($entity['pressure_fit_count'] ?? 0) * 3);
}

function doctrine_ai_select_candidates(array $snapshot, int $limit = 8): array
{
    $candidates = [];

    foreach ((array) ($snapshot['fits'] ?? []) as $fit) {
        $fitId = (int) ($fit['id'] ?? 0);
        if ($fitId <= 0) {
            continue;
        }

        $score = doctrine_ai_candidate_score($fit, 'fit');
        if ($score <= 0) {
            continue;
        }

        $candidates[] = [
            'entity_type' => 'fit',
            'entity_id' => $fitId,
            'score' => $score,
            'entity' => $fit,
        ];
    }

    foreach ((array) ($snapshot['groups'] ?? []) as $group) {
        $groupId = (int) ($group['id'] ?? 0);
        if ($groupId <= 0) {
            continue;
        }

        $score = doctrine_ai_candidate_score($group, 'group');
        if ($score <= 0) {
            continue;
        }

        $candidates[] = [
            'entity_type' => 'group',
            'entity_id' => $groupId,
            'score' => $score,
            'entity' => $group,
        ];
    }

    usort($candidates, static function (array $a, array $b): int {
        return ((float) ($b['score'] ?? 0.0) <=> (float) ($a['score'] ?? 0.0))
            ?: strcmp((string) ($a['entity_type'] ?? ''), (string) ($b['entity_type'] ?? ''))
            ?: ((int) ($a['entity_id'] ?? 0) <=> (int) ($b['entity_id'] ?? 0));
    });

    return array_slice($candidates, 0, max(1, min(20, $limit)));
}

function doctrine_ai_find_candidate(array $snapshot, ?string $entityType = null, ?int $entityId = null, int $limit = 8): ?array
{
    $safeEntityType = in_array((string) $entityType, ['fit', 'group'], true) ? (string) $entityType : null;
    $safeEntityId = $entityId !== null ? max(0, $entityId) : null;
    $candidates = doctrine_ai_select_candidates($snapshot, max(1, $limit));

    if ($safeEntityType === null || $safeEntityId === null || $safeEntityId <= 0) {
        return $candidates[0] ?? null;
    }

    foreach ($candidates as $candidate) {
        if ((string) ($candidate['entity_type'] ?? '') === $safeEntityType && (int) ($candidate['entity_id'] ?? 0) === $safeEntityId) {
            return $candidate;
        }
    }

    $entities = $safeEntityType === 'fit'
        ? (array) ($snapshot['fits'] ?? [])
        : (array) ($snapshot['groups'] ?? []);

    foreach ($entities as $entity) {
        if ((int) ($entity['id'] ?? 0) !== $safeEntityId) {
            continue;
        }

        return [
            'entity_type' => $safeEntityType,
            'entity_id' => $safeEntityId,
            'score' => doctrine_ai_candidate_score($entity, $safeEntityType),
            'entity' => $entity,
        ];
    }

    return null;
}

function doctrine_ai_debug_preview(?string $entityType = null, ?int $entityId = null, int $limit = 5): array
{
    $snapshot = doctrine_snapshot_cache_payload();
    if ($snapshot === null) {
        $snapshot = doctrine_refresh_intelligence('ai-debug');
    }

    $strategy = supplycore_ai_strategy();
    $candidateLimit = max(1, min((int) ($strategy['candidate_limit'] ?? 8), $limit));
    $candidates = doctrine_ai_select_candidates($snapshot, $candidateLimit);
    $selectedCandidate = doctrine_ai_find_candidate($snapshot, $entityType, $entityId, max($candidateLimit, 8));
    if (!is_array($selectedCandidate)) {
        throw new RuntimeException('No doctrine AI candidate is available for preview.');
    }

    $resolvedEntityType = (string) ($selectedCandidate['entity_type'] ?? '');
    $resolvedEntityId = (int) ($selectedCandidate['entity_id'] ?? 0);
    $entity = is_array($selectedCandidate['entity'] ?? null) ? $selectedCandidate['entity'] : [];
    $previousBriefing = doctrine_ai_briefing_get($resolvedEntityType, $resolvedEntityId);
    $sourcePayload = $resolvedEntityType === 'fit'
        ? doctrine_ai_source_payload_for_fit($entity, $previousBriefing, $strategy)
        : doctrine_ai_source_payload_for_group($entity, $previousBriefing, $strategy);
    $generation = doctrine_ai_generate_briefing_result($sourcePayload);

    return [
        'config' => supplycore_ai_ollama_config() + [
            'available' => supplycore_ai_ollama_available(),
        ],
        'selected_candidate' => [
            'entity_type' => $resolvedEntityType,
            'entity_id' => $resolvedEntityId,
            'label' => doctrine_ai_entity_label($selectedCandidate),
            'score' => (float) ($selectedCandidate['score'] ?? 0.0),
        ],
        'candidate_list' => array_map(static function (array $candidate): array {
            return [
                'entity_type' => (string) ($candidate['entity_type'] ?? ''),
                'entity_id' => (int) ($candidate['entity_id'] ?? 0),
                'label' => doctrine_ai_entity_label($candidate),
                'score' => (float) ($candidate['score'] ?? 0.0),
            ];
        }, $candidates),
        'previous_briefing' => $previousBriefing,
        'source_payload' => $sourcePayload,
        'generation' => $generation,
    ];
}

function rebuild_ai_briefings_job_result(string $reason = 'manual'): array
{
    $snapshot = doctrine_snapshot_cache_payload();
    if ($snapshot === null) {
        $snapshot = doctrine_refresh_intelligence($reason . ':bootstrap');
    }

    $config = supplycore_ai_ollama_config();
    $strategy = supplycore_ai_strategy();
    $candidates = doctrine_ai_select_candidates($snapshot, (int) ($strategy['candidate_limit'] ?? 8));
    $processed = 0;
    $written = 0;
    $fallbacks = 0;
    $warnings = [];

    if ($config['enabled'] !== true) {
        $warnings[] = 'Ollama integration is disabled; storing deterministic fallback briefings only.';
    }

    foreach ($candidates as $candidate) {
        $entityType = (string) ($candidate['entity_type'] ?? '');
        $entityId = (int) ($candidate['entity_id'] ?? 0);
        $entity = is_array($candidate['entity'] ?? null) ? $candidate['entity'] : [];
        if (!in_array($entityType, ['fit', 'group'], true) || $entityId <= 0 || $entity === []) {
            continue;
        }

        $processed++;
        $previousBriefing = doctrine_ai_briefing_get($entityType, $entityId);
        $sourcePayload = $entityType === 'fit'
            ? doctrine_ai_source_payload_for_fit($entity, $previousBriefing, $strategy)
            : doctrine_ai_source_payload_for_group($entity, $previousBriefing, $strategy);

        supplycore_ai_log('briefing.attempt', [
            'entity_type' => $entityType,
            'entity_id' => $entityId,
            'reason' => $reason,
            'model' => $config['model'],
            'capability_tier' => (string) ($config['capability_tier'] ?? 'small'),
        ]);

        $generation = doctrine_ai_generate_briefing_result($sourcePayload);
        $briefing = is_array($generation['briefing'] ?? null) ? $generation['briefing'] : [];
        $status = (string) ($generation['status'] ?? 'fallback');
        $errorMessage = ($generation['error_message'] ?? null) !== null ? (string) $generation['error_message'] : null;

        doctrine_ai_store_briefing($sourcePayload, $briefing, $status, $errorMessage);
        if ($status === 'ready') {
            $written++;
            supplycore_ai_log('briefing.success', [
                'entity_type' => $entityType,
                'entity_id' => $entityId,
                'priority' => (string) ($briefing['priority'] ?? 'medium'),
            ]);
        } else {
            $fallbacks++;
            supplycore_ai_log('briefing.failure', [
                'entity_type' => $entityType,
                'entity_id' => $entityId,
                'error' => $errorMessage,
            ]);
        }
    }

    dashboard_refresh_summary($reason . ':ai-briefings');
    supplycore_cache_bust(['dashboard', 'doctrine']);

    return sync_result_shape() + [
        'rows_seen' => $processed,
        'rows_written' => $written + $fallbacks,
        'warnings' => $warnings,
        'cursor' => 'doctrine_ai_briefings:' . gmdate('Y-m-d H:i:s'),
        'checksum' => sync_checksum([
            'processed' => $processed,
            'written' => $written,
            'fallbacks' => $fallbacks,
            'reason' => $reason,
            'model' => $config['model'],
            'tier' => $config['capability_tier'] ?? 'small',
        ]),
        'meta' => [
            'outcome_reason' => 'Doctrine AI briefings were rebuilt in the background using a centralized capability tier strategy over deterministic doctrine facts.',
            'processed_candidates' => $processed,
            'fallback_count' => $fallbacks,
            'model_name' => $config['model'],
            'capability_tier' => $config['capability_tier'] ?? 'small',
            'candidate_limit' => (int) ($strategy['candidate_limit'] ?? 0),
        ],
    ];
}

function doctrine_ai_dashboard_briefings(int $limit = 6): array
{
    $strategy = supplycore_ai_strategy();
    $resolvedLimit = max(1, min((int) ($strategy['dashboard_limit'] ?? $limit), $limit));

    try {
        $rows = db_doctrine_ai_briefings_top($resolvedLimit);
    } catch (Throwable) {
        $rows = [];
    }

    $briefings = [];
    foreach ($rows as $row) {
        $normalized = doctrine_ai_briefing_normalize_row($row);
        if ($normalized !== null) {
            $briefings[] = $normalized;
        }
    }

    if ($briefings === []) {
        return [];
    }

    $snapshot = doctrine_snapshot_cache_payload();
    $fitsById = [];
    foreach ((array) ($snapshot['fits'] ?? []) as $fit) {
        $fitsById[(int) ($fit['id'] ?? 0)] = $fit;
    }
    $groupsById = [];
    foreach ((array) ($snapshot['groups'] ?? []) as $group) {
        $groupsById[(int) ($group['id'] ?? 0)] = $group;
    }

    foreach ($briefings as &$briefing) {
        if (($briefing['entity_type'] ?? '') === 'fit') {
            $fit = $fitsById[(int) ($briefing['entity_id'] ?? 0)] ?? [];
            $briefing['entity_name'] = (string) ($fit['fit_name'] ?? ('Fit #' . (int) ($briefing['entity_id'] ?? 0)));
            $briefing['context_name'] = implode(', ', (array) ($fit['group_names'] ?? []));
            $briefing['href'] = '/doctrine/fit?fit_id=' . (int) ($briefing['entity_id'] ?? 0);
        } else {
            $group = $groupsById[(int) ($briefing['entity_id'] ?? 0)] ?? [];
            $briefing['entity_name'] = (string) ($group['group_name'] ?? ('Group #' . (int) ($briefing['entity_id'] ?? 0)));
            $briefing['context_name'] = doctrine_format_quantity((int) ($group['fit_count'] ?? 0)) . ' fits';
            $briefing['href'] = '/doctrine/group?group_id=' . (int) ($briefing['entity_id'] ?? 0);
        }
    }
    unset($briefing);

    usort($briefings, static function (array $a, array $b): int {
        return ((int) ($b['priority_rank'] ?? 0) <=> (int) ($a['priority_rank'] ?? 0))
            ?: strcmp((string) ($b['computed_at'] ?? ''), (string) ($a['computed_at'] ?? ''));
    });

    return array_slice($briefings, 0, $resolvedLimit);
}

function activity_priority_snapshot_key(): string
{
    return 'activity_priority_summaries';
}

function activity_priority_snapshot_payload(): array
{
    $doctrineCached = supplycore_cached_json_read(supplycore_activity_doctrine_cache_key());
    $itemsCached = supplycore_cached_json_read(supplycore_activity_items_cache_key());
    if (is_array($doctrineCached) && is_array($itemsCached) && $doctrineCached !== [] && $itemsCached !== []) {
        return $doctrineCached + $itemsCached;
    }

    return supplycore_materialized_snapshot_read_or_bootstrap(
        activity_priority_snapshot_key(),
        static fn (): array => activity_priority_summary_build('bootstrap'),
        'activity-priority-bootstrap'
    );
}

function activity_priority_refresh_summary(string $reason = 'manual'): array
{
    supplycore_materialized_snapshot_mark_updating(activity_priority_snapshot_key(), $reason);
    $snapshot = activity_priority_summary_build($reason);
    $stored = supplycore_materialized_snapshot_store(activity_priority_snapshot_key(), $snapshot, [
        'reason' => $reason,
        'group_count' => count((array) ($snapshot['active_doctrines'] ?? [])),
        'item_count' => count((array) ($snapshot['priority_items'] ?? [])),
    ]);

    supplycore_cached_json_write(supplycore_activity_doctrine_cache_key(), [
        'summary_cards' => array_values((array) ($stored['summary_cards'] ?? [])),
        'active_doctrines' => array_values((array) ($stored['active_doctrines'] ?? [])),
        'active_fits' => array_values((array) ($stored['active_fits'] ?? [])),
        'trend_movement' => is_array($stored['trend_movement'] ?? null) ? $stored['trend_movement'] : [],
        '_freshness' => is_array($stored['_freshness'] ?? null) ? $stored['_freshness'] : [],
    ]);
    supplycore_cached_json_write(supplycore_activity_items_cache_key(), [
        'priority_items' => array_values((array) ($stored['priority_items'] ?? [])),
        'questions_answered' => is_array($stored['questions_answered'] ?? null) ? $stored['questions_answered'] : [],
        '_freshness' => is_array($stored['_freshness'] ?? null) ? $stored['_freshness'] : [],
    ]);

    return $stored;
}

function activity_priority_refresh_summary_job_result(string $reason = 'manual'): array
{
    $snapshot = activity_priority_refresh_summary($reason);
    $rowCount = count((array) ($snapshot['active_doctrines'] ?? [])) + count((array) ($snapshot['priority_items'] ?? []));
    $freshness = is_array($snapshot['_freshness'] ?? null) ? $snapshot['_freshness'] : [];

    return sync_result_shape() + [
        'rows_seen' => $rowCount,
        'rows_written' => $rowCount,
        'cursor' => 'activity_priority:' . gmdate('Y-m-d H:i:s'),
        'checksum' => sync_checksum([
            'rows' => $rowCount,
            'computed_at' => (string) ($freshness['computed_at'] ?? gmdate(DATE_ATOM)),
            'reason' => $reason,
        ]),
        'meta' => [
            'outcome_reason' => 'Doctrine activity and item-priority summaries were recomputed from tracked losses, doctrine definitions, and local market pressure signals.',
            'snapshot_generated_at' => (string) ($freshness['computed_at'] ?? ''),
        ],
    ];
}

function activity_priority_page_data(): array
{
    return activity_priority_snapshot_payload();
}

function activity_priority_level_from_score(float $score): string
{
    if ($score >= 78.0) {
        return 'highly active';
    }
    if ($score >= 56.0) {
        return 'active';
    }
    if ($score >= 32.0) {
        return 'moderate';
    }

    return 'low';
}

function activity_priority_item_band(float $score): string
{
    if ($score >= 82.0) {
        return 'critical';
    }
    if ($score >= 66.0) {
        return 'high';
    }
    if ($score >= 44.0) {
        return 'elevated';
    }

    return 'watch';
}

function activity_priority_level_tone(string $level): string
{
    return match ($level) {
        'highly active', 'critical' => 'border-rose-400/20 bg-rose-500/10 text-rose-100',
        'active', 'high' => 'border-orange-400/20 bg-orange-500/10 text-orange-100',
        'moderate', 'elevated' => 'border-amber-400/20 bg-amber-500/10 text-amber-100',
        default => 'border-slate-400/15 bg-slate-500/10 text-slate-200',
    };
}

function activity_priority_repeated_usage_weight(float $window24h, float $window3d, float $window7d): float
{
    $activeWindows = 0;
    foreach ([$window24h, $window3d, $window7d] as $value) {
        if ($value > 0) {
            $activeWindows++;
        }
    }

    $carryForward = max(0.0, $window3d - $window24h) + max(0.0, $window7d - $window3d);

    return round(min(16.0, ($activeWindows * 3.5) + ($carryForward * 2.2)), 2);
}

function activity_priority_recency_weight(float $window24h, float $window3d, float $window7d): float
{
    if ($window24h > 0) {
        return min(18.0, 10.0 + ($window24h * 2.2));
    }
    if ($window3d > 0) {
        return min(12.0, 6.0 + ($window3d * 1.1));
    }
    if ($window7d > 0) {
        return min(6.0, 2.5 + ($window7d * 0.4));
    }

    return 0.0;
}

function activity_priority_pressure_bonus(string $pressureState): float
{
    return match ($pressureState) {
        'urgent_resupply' => 8.0,
        'resupply_soon' => 6.0,
        'elevated' => 3.0,
        default => 0.0,
    };
}

function activity_priority_item_history_index(array $typeIds): array
{
    $normalizedTypeIds = array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $typeId): bool => $typeId > 0)));
    if ($normalizedTypeIds === []) {
        return [];
    }

    $allianceStructureId = configured_structure_destination_id_for_esi_sync();
    $hubRef = market_hub_setting_reference();
    $hubSourceId = sync_source_id_from_hub_ref($hubRef);

    try {
        if ($allianceStructureId > 0) {
            return doctrine_local_history_index(
                db_market_orders_history_stock_health_series(
                    'alliance_structure',
                    $allianceStructureId,
                    gmdate('Y-m-d', strtotime('-14 days')),
                    gmdate('Y-m-d'),
                    $normalizedTypeIds
                )
            );
        }

        if ($hubSourceId > 0) {
            return doctrine_local_history_index(
                db_market_hub_local_history_daily_window_by_type_ids(
                    market_hub_local_history_source(),
                    $hubSourceId,
                    $normalizedTypeIds,
                    14
                )
            );
        }
    } catch (Throwable) {
        return [];
    }

    return [];
}

function activity_priority_fit_item_windows(array $items, array $itemLossByType): array
{
    $moduleLosses24h = 0;
    $moduleLosses3d = 0;
    $moduleLosses7d = 0;
    $equivalent24h = 0.0;
    $equivalent3d = 0.0;
    $equivalent7d = 0.0;

    foreach ($items as $item) {
        $typeId = max(0, (int) ($item['type_id'] ?? 0));
        $requiredQty = max(1, (int) ($item['quantity'] ?? 1));
        if ($typeId <= 0) {
            continue;
        }

        $loss = $itemLossByType[$typeId] ?? [];
        $qty24h = max(0, (int) ($loss['quantity_24h'] ?? 0));
        $qty3d = max(0, (int) ($loss['quantity_3d'] ?? 0));
        $qty7d = max(0, (int) ($loss['quantity_7d'] ?? 0));

        $moduleLosses24h += $qty24h;
        $moduleLosses3d += $qty3d;
        $moduleLosses7d += $qty7d;
        $equivalent24h = max($equivalent24h, $qty24h / $requiredQty);
        $equivalent3d = max($equivalent3d, $qty3d / $requiredQty);
        $equivalent7d = max($equivalent7d, $qty7d / $requiredQty);
    }

    return [
        'module_losses_24h' => $moduleLosses24h,
        'module_losses_3d' => $moduleLosses3d,
        'module_losses_7d' => $moduleLosses7d,
        'fit_equivalent_losses_24h' => round($equivalent24h, 2),
        'fit_equivalent_losses_3d' => round($equivalent3d, 2),
        'fit_equivalent_losses_7d' => round($equivalent7d, 2),
    ];
}

function activity_priority_doctrine_explanation(array $row): string
{
    $parts = [];
    if ((int) ($row['hull_losses_24h'] ?? 0) > 0 || (int) ($row['hull_losses_7d'] ?? 0) > 0) {
        $parts[] = doctrine_format_quantity((int) ($row['hull_losses_24h'] ?? 0)) . ' hull losses in 24h and ' . doctrine_format_quantity((int) ($row['hull_losses_7d'] ?? 0)) . ' in 7d';
    }
    if ((float) ($row['fit_equivalent_losses_7d'] ?? 0.0) > 0) {
        $parts[] = number_format((float) ($row['fit_equivalent_losses_7d'] ?? 0.0), 1) . ' fit-equivalent module losses over 7d';
    }
    if ((int) ($row['readiness_gap_count'] ?? 0) > 0) {
        $parts[] = doctrine_format_quantity((int) ($row['readiness_gap_count'] ?? 0)) . ' fit gaps still open locally';
    }
    if (((string) ($row['resupply_pressure_state'] ?? 'stable')) !== 'stable') {
        $parts[] = 'resupply pressure is ' . strtolower((string) ($row['resupply_pressure'] ?? 'elevated'));
    }

    return ucfirst(implode(' · ', array_slice($parts, 0, 4))) . ($parts === [] ? 'No recent tracked loss pressure is pushing this doctrine higher right now.' : '.');
}

function activity_priority_item_explanation(array $row): string
{
    $parts = [];
    if ((bool) ($row['is_doctrine_linked'] ?? false)) {
        $parts[] = 'linked to ' . doctrine_format_quantity((int) ($row['linked_doctrine_count'] ?? 0)) . ' doctrine fits';
    }
    if ((int) ($row['linked_active_doctrine_count'] ?? 0) > 0) {
        $parts[] = doctrine_format_quantity((int) ($row['linked_active_doctrine_count'] ?? 0)) . ' linked doctrines are active now';
    }
    if ((int) ($row['recent_loss_qty_24h'] ?? 0) > 0 || (int) ($row['recent_loss_qty_7d'] ?? 0) > 0) {
        $parts[] = doctrine_format_quantity((int) ($row['recent_loss_qty_24h'] ?? 0)) . ' units lost in 24h and ' . doctrine_format_quantity((int) ($row['recent_loss_qty_7d'] ?? 0)) . ' in 7d';
    }
    if ((int) ($row['local_sell_volume'] ?? 0) <= 0) {
        $parts[] = 'no local sell stock currently listed';
    } elseif ((string) ($row['depletion_state'] ?? 'stable') === 'draining') {
        $parts[] = 'local stock is draining versus the prior window';
    }
    if ((int) ($row['bottleneck_fit_count'] ?? 0) > 0) {
        $parts[] = 'it is already blocking complete fits';
    }

    return ucfirst(implode(' · ', array_slice($parts, 0, 5))) . ($parts === [] ? 'Signals are steady, so the item remains on watch rather than elevated.' : '.');
}

function activity_priority_movement_label(int $rankDelta, float $scoreDelta): string
{
    if ($rankDelta >= 5 || $scoreDelta >= 12.0) {
        return 'Rising fast';
    }
    if ($rankDelta > 0 || $scoreDelta > 4.0) {
        return 'Moving up';
    }
    if ($rankDelta <= -5 || $scoreDelta <= -12.0) {
        return 'Cooling down';
    }
    if ($rankDelta < 0 || $scoreDelta < -4.0) {
        return 'Softening';
    }

    return 'Holding';
}

function activity_priority_summary_build(string $reason = 'manual'): array
{
    $doctrineSnapshot = doctrine_snapshot_cache_payload();
    if ($doctrineSnapshot === null) {
        $doctrineSnapshot = doctrine_refresh_intelligence($reason . ':bootstrap');
    }

    $marketOutcomes = market_comparison_outcomes();
    $marketRows = array_values((array) ($marketOutcomes['rows'] ?? []));
    $marketByTypeId = [];
    foreach ($marketRows as $row) {
        $typeId = (int) ($row['type_id'] ?? 0);
        if ($typeId > 0) {
            $marketByTypeId[$typeId] = $row;
        }
    }

    $fits = array_values((array) ($doctrineSnapshot['fits'] ?? []));
    $groups = array_values((array) ($doctrineSnapshot['groups'] ?? []));
    $fitIds = array_values(array_filter(array_map(static fn (array $fit): int => (int) ($fit['id'] ?? 0), $fits), static fn (int $fitId): bool => $fitId > 0));

    $itemsByFitId = [];
    $doctrineTypeIds = [];
    try {
        foreach (db_doctrine_fit_items_by_fit_ids($fitIds) as $item) {
            $fitId = (int) ($item['doctrine_fit_id'] ?? 0);
            $typeId = (int) ($item['type_id'] ?? 0);
            if ($fitId <= 0 || $typeId <= 0 || !item_scope_is_type_id_in_scope($typeId)) {
                continue;
            }

            $itemsByFitId[$fitId][] = $item;
            $doctrineTypeIds[] = $typeId;
        }
    } catch (Throwable) {
        $itemsByFitId = [];
    }

    $enabledTypeIds = array_values(array_unique(array_merge(array_keys($marketByTypeId), $doctrineTypeIds)));
    $itemLossRows = [];
    $hullLossRows = [];
    try {
        $itemLossRows = db_killmail_tracked_recent_item_activity_windows($enabledTypeIds, 24 * 7);
    } catch (Throwable) {
        $itemLossRows = [];
    }
    try {
        $hullLossRows = db_killmail_tracked_recent_hull_activity_windows(
            array_values(array_filter(array_map(static fn (array $fit): int => (int) ($fit['ship_type_id'] ?? 0), $fits), static fn (int $typeId): bool => $typeId > 0)),
            24 * 7
        );
    } catch (Throwable) {
        $hullLossRows = [];
    }

    $itemLossByType = doctrine_item_loss_index($itemLossRows);
    foreach ($itemLossRows as $row) {
        $typeId = (int) ($row['type_id'] ?? 0);
        if ($typeId <= 0) {
            continue;
        }
        $itemLossByType[$typeId]['quantity_3d'] = max(0, (int) ($row['quantity_3d'] ?? 0));
        $itemLossByType[$typeId]['losses_3d'] = max(0, (int) ($row['losses_3d'] ?? 0));
    }
    $hullLossByType = doctrine_hull_loss_index($hullLossRows);
    foreach ($hullLossRows as $row) {
        $typeId = (int) ($row['type_id'] ?? 0);
        if ($typeId <= 0) {
            continue;
        }
        $hullLossByType[$typeId]['losses_3d'] = max(0, (int) ($row['losses_3d'] ?? 0));
    }

    $historyByTypeId = activity_priority_item_history_index($enabledTypeIds);
    $depletionByType = doctrine_item_depletion_index($historyByTypeId);

    $previousFitSnapshots = [];
    foreach (db_doctrine_activity_latest_snapshots('fit', $fitIds) as $row) {
        $previousFitSnapshots[(int) ($row['entity_id'] ?? 0)] = $row;
    }

    $fitRows = [];
    foreach ($fits as $fit) {
        $fitId = (int) ($fit['id'] ?? 0);
        $supply = is_array($fit['supply'] ?? null) ? $fit['supply'] : [];
        $shipTypeId = (int) ($fit['ship_type_id'] ?? 0);
        $hull = $hullLossByType[$shipTypeId] ?? [];
        $itemWindows = activity_priority_fit_item_windows($itemsByFitId[$fitId] ?? [], $itemLossByType);

        $hullPressure = min(38.0, ((int) ($hull['losses_24h'] ?? 0) * 16.0) + max(0, ((int) ($hull['losses_3d'] ?? 0) - (int) ($hull['losses_24h'] ?? 0)) * 5.5) + max(0, ((int) ($hull['losses_7d'] ?? 0) - (int) ($hull['losses_3d'] ?? 0)) * 3.0));
        $modulePressure = min(24.0, ((float) ($itemWindows['fit_equivalent_losses_24h'] ?? 0.0) * 8.5) + max(0.0, ((float) ($itemWindows['fit_equivalent_losses_3d'] ?? 0.0) - (float) ($itemWindows['fit_equivalent_losses_24h'] ?? 0.0)) * 4.0) + max(0.0, ((float) ($itemWindows['fit_equivalent_losses_7d'] ?? 0.0) - (float) ($itemWindows['fit_equivalent_losses_3d'] ?? 0.0)) * 2.4));
        $recencyWeight = activity_priority_recency_weight(
            max((float) ($hull['losses_24h'] ?? 0), (float) ($itemWindows['fit_equivalent_losses_24h'] ?? 0.0)),
            max((float) ($hull['losses_3d'] ?? 0), (float) ($itemWindows['fit_equivalent_losses_3d'] ?? 0.0)),
            max((float) ($hull['losses_7d'] ?? 0), (float) ($itemWindows['fit_equivalent_losses_7d'] ?? 0.0))
        );
        $repeatedUsage = activity_priority_repeated_usage_weight(
            max((float) ($hull['losses_24h'] ?? 0), (float) ($itemWindows['fit_equivalent_losses_24h'] ?? 0.0)),
            max((float) ($hull['losses_3d'] ?? 0), (float) ($itemWindows['fit_equivalent_losses_3d'] ?? 0.0)),
            max((float) ($hull['losses_7d'] ?? 0), (float) ($itemWindows['fit_equivalent_losses_7d'] ?? 0.0))
        );
        $readinessGapWeight = min(16.0, ((int) ($supply['gap_to_target_fit_count'] ?? 0) * 3.0) + (((string) ($supply['readiness_state'] ?? 'market_ready')) === 'critical_gap' ? 6.0 : ((((string) ($supply['readiness_state'] ?? 'market_ready')) === 'partial_gap') ? 3.0 : 0.0)));
        $pressureWeight = activity_priority_pressure_bonus((string) ($supply['resupply_pressure_state'] ?? 'stable'));
        $activityScore = round(min(100.0, $hullPressure + $modulePressure + $recencyWeight + $repeatedUsage + $readinessGapWeight + $pressureWeight), 2);

        $components = [
            'hull_loss_pressure' => round($hullPressure, 2),
            'module_loss_pressure' => round($modulePressure, 2),
            'recency_weight' => round($recencyWeight, 2),
            'repeated_usage_weight' => round($repeatedUsage, 2),
            'readiness_gap_weight' => round($readinessGapWeight + $pressureWeight, 2),
        ];

        $previous = $previousFitSnapshots[$fitId] ?? [];
        $scoreDelta = round($activityScore - (float) ($previous['activity_score'] ?? 0.0), 2);
        $fitRows[] = $fit + [
            'entity_type' => 'fit',
            'entity_id' => $fitId,
            'doctrine_name' => (string) ($fit['fit_name'] ?? ('Fit #' . $fitId)),
            'activity_score' => $activityScore,
            'activity_level' => activity_priority_level_from_score($activityScore),
            'hull_losses_24h' => max(0, (int) ($hull['losses_24h'] ?? 0)),
            'hull_losses_3d' => max(0, (int) ($hull['losses_3d'] ?? 0)),
            'hull_losses_7d' => max(0, (int) ($hull['losses_7d'] ?? 0)),
            'module_losses_24h' => max(0, (int) ($itemWindows['module_losses_24h'] ?? 0)),
            'module_losses_3d' => max(0, (int) ($itemWindows['module_losses_3d'] ?? 0)),
            'module_losses_7d' => max(0, (int) ($itemWindows['module_losses_7d'] ?? 0)),
            'fit_equivalent_losses_24h' => (float) ($itemWindows['fit_equivalent_losses_24h'] ?? 0.0),
            'fit_equivalent_losses_3d' => (float) ($itemWindows['fit_equivalent_losses_3d'] ?? 0.0),
            'fit_equivalent_losses_7d' => (float) ($itemWindows['fit_equivalent_losses_7d'] ?? 0.0),
            'readiness_state' => (string) ($supply['readiness_state'] ?? 'market_ready'),
            'readiness_label' => (string) ($supply['readiness_label'] ?? 'Market ready'),
            'resupply_pressure_state' => (string) ($supply['resupply_pressure_state'] ?? 'stable'),
            'resupply_pressure' => (string) ($supply['resupply_pressure_label'] ?? 'Stable'),
            'readiness_gap_count' => max(0, (int) ($supply['gap_to_target_fit_count'] ?? 0)),
            'resupply_gap_isk' => (float) ($supply['restock_gap_isk'] ?? 0.0),
            'score_components' => $components,
            'score_delta' => $scoreDelta,
        ];
    }

    usort($fitRows, static fn (array $a, array $b): int => ((float) ($b['activity_score'] ?? 0.0) <=> (float) ($a['activity_score'] ?? 0.0)) ?: strcasecmp((string) ($a['doctrine_name'] ?? ''), (string) ($b['doctrine_name'] ?? '')));
    foreach ($fitRows as $index => &$row) {
        $previous = $previousFitSnapshots[(int) ($row['entity_id'] ?? 0)] ?? [];
        $row['rank_position'] = $index + 1;
        $row['previous_rank_position'] = isset($previous['rank_position']) ? (int) $previous['rank_position'] : null;
        $row['rank_delta'] = $row['previous_rank_position'] !== null ? ((int) $row['previous_rank_position'] - (int) $row['rank_position']) : 0;
        $row['movement_label'] = activity_priority_movement_label((int) $row['rank_delta'], (float) ($row['score_delta'] ?? 0.0));
        $row['explanation'] = activity_priority_doctrine_explanation($row);
    }
    unset($row);

    $fitById = [];
    foreach ($fitRows as $row) {
        $fitById[(int) ($row['entity_id'] ?? 0)] = $row;
    }

    $groupIds = array_values(array_filter(array_map(static fn (array $group): int => (int) ($group['id'] ?? 0), $groups), static fn (int $groupId): bool => $groupId > 0));
    $previousGroupSnapshots = [];
    foreach (db_doctrine_activity_latest_snapshots('group', $groupIds) as $row) {
        $previousGroupSnapshots[(int) ($row['entity_id'] ?? 0)] = $row;
    }

    $groupRows = [];
    foreach ($groups as $group) {
        $groupFits = array_values((array) ($group['fits'] ?? []));
        $scoredFits = [];
        foreach ($groupFits as $fit) {
            $fitId = (int) ($fit['id'] ?? 0);
            if (isset($fitById[$fitId])) {
                $scoredFits[] = $fitById[$fitId];
            }
        }
        if ($scoredFits === []) {
            continue;
        }

        usort($scoredFits, static fn (array $a, array $b): int => ((float) ($b['activity_score'] ?? 0.0) <=> (float) ($a['activity_score'] ?? 0.0)));
        $topFitScore = (float) ($scoredFits[0]['activity_score'] ?? 0.0);
        $averageFitScore = array_sum(array_map(static fn (array $fit): float => (float) ($fit['activity_score'] ?? 0.0), $scoredFits)) / max(1, count($scoredFits));
        $hull24h = array_sum(array_map(static fn (array $fit): int => (int) ($fit['hull_losses_24h'] ?? 0), $scoredFits));
        $hull3d = array_sum(array_map(static fn (array $fit): int => (int) ($fit['hull_losses_3d'] ?? 0), $scoredFits));
        $hull7d = array_sum(array_map(static fn (array $fit): int => (int) ($fit['hull_losses_7d'] ?? 0), $scoredFits));
        $module24h = array_sum(array_map(static fn (array $fit): int => (int) ($fit['module_losses_24h'] ?? 0), $scoredFits));
        $module3d = array_sum(array_map(static fn (array $fit): int => (int) ($fit['module_losses_3d'] ?? 0), $scoredFits));
        $module7d = array_sum(array_map(static fn (array $fit): int => (int) ($fit['module_losses_7d'] ?? 0), $scoredFits));
        $equivalent24h = array_sum(array_map(static fn (array $fit): float => (float) ($fit['fit_equivalent_losses_24h'] ?? 0.0), $scoredFits));
        $equivalent3d = array_sum(array_map(static fn (array $fit): float => (float) ($fit['fit_equivalent_losses_3d'] ?? 0.0), $scoredFits));
        $equivalent7d = array_sum(array_map(static fn (array $fit): float => (float) ($fit['fit_equivalent_losses_7d'] ?? 0.0), $scoredFits));
        $recencyWeight = activity_priority_recency_weight(max($hull24h, $equivalent24h), max($hull3d, $equivalent3d), max($hull7d, $equivalent7d));
        $repeatedWeight = activity_priority_repeated_usage_weight(max($hull24h, $equivalent24h), max($hull3d, $equivalent3d), max($hull7d, $equivalent7d));
        $readinessWeight = min(18.0, ((int) ($group['fit_gap_count'] ?? 0) * 2.5) + ((int) ($group['gap_fit_count'] ?? 0) * 2.0) + activity_priority_pressure_bonus((string) ($group['pressure_state'] ?? 'stable')));
        $hullPressure = min(30.0, ($hull24h * 7.0) + max(0, $hull3d - $hull24h) * 2.8 + max(0, $hull7d - $hull3d) * 1.6);
        $modulePressure = min(22.0, ($equivalent24h * 5.5) + max(0.0, $equivalent3d - $equivalent24h) * 2.4 + max(0.0, $equivalent7d - $equivalent3d) * 1.4);
        $score = round(min(100.0, ($topFitScore * 0.45) + ($averageFitScore * 0.25) + $hullPressure + $modulePressure + $recencyWeight + $repeatedWeight + $readinessWeight), 2);
        $components = [
            'hull_loss_pressure' => round($hullPressure, 2),
            'module_loss_pressure' => round($modulePressure, 2),
            'recency_weight' => round($recencyWeight, 2),
            'repeated_usage_weight' => round($repeatedWeight, 2),
            'readiness_gap_weight' => round($readinessWeight, 2),
        ];
        $previous = $previousGroupSnapshots[(int) ($group['id'] ?? 0)] ?? [];
        $scoreDelta = round($score - (float) ($previous['activity_score'] ?? 0.0), 2);

        $groupRows[] = $group + [
            'entity_type' => 'group',
            'entity_id' => (int) ($group['id'] ?? 0),
            'doctrine_name' => (string) ($group['group_name'] ?? 'Doctrine group'),
            'activity_score' => $score,
            'activity_level' => activity_priority_level_from_score($score),
            'hull_losses_24h' => $hull24h,
            'hull_losses_3d' => $hull3d,
            'hull_losses_7d' => $hull7d,
            'module_losses_24h' => $module24h,
            'module_losses_3d' => $module3d,
            'module_losses_7d' => $module7d,
            'fit_equivalent_losses_24h' => round($equivalent24h, 2),
            'fit_equivalent_losses_3d' => round($equivalent3d, 2),
            'fit_equivalent_losses_7d' => round($equivalent7d, 2),
            'readiness_state' => (string) ($group['status'] ?? 'market_ready'),
            'readiness_label' => (string) ($group['status_label'] ?? 'Market ready'),
            'resupply_pressure_state' => (string) ($group['pressure_state'] ?? 'stable'),
            'resupply_pressure' => (string) ($group['pressure_label'] ?? 'Stable'),
            'readiness_gap_count' => max(0, (int) ($group['fit_gap_count'] ?? 0)),
            'resupply_gap_isk' => (float) ($group['restock_gap_isk'] ?? 0.0),
            'score_components' => $components,
            'score_delta' => $scoreDelta,
            'top_fits' => array_slice(array_map(static fn (array $fit): array => [
                'fit_id' => (int) ($fit['entity_id'] ?? 0),
                'fit_name' => (string) ($fit['doctrine_name'] ?? ''),
                'activity_score' => (float) ($fit['activity_score'] ?? 0.0),
                'activity_level' => (string) ($fit['activity_level'] ?? 'low'),
            ], $scoredFits), 0, 3),
        ];
    }

    usort($groupRows, static fn (array $a, array $b): int => ((float) ($b['activity_score'] ?? 0.0) <=> (float) ($a['activity_score'] ?? 0.0)) ?: strcasecmp((string) ($a['doctrine_name'] ?? ''), (string) ($b['doctrine_name'] ?? '')));
    foreach ($groupRows as $index => &$row) {
        $previous = $previousGroupSnapshots[(int) ($row['entity_id'] ?? 0)] ?? [];
        $row['rank_position'] = $index + 1;
        $row['previous_rank_position'] = isset($previous['rank_position']) ? (int) $previous['rank_position'] : null;
        $row['rank_delta'] = $row['previous_rank_position'] !== null ? ((int) $row['previous_rank_position'] - (int) $row['rank_position']) : 0;
        $row['movement_label'] = activity_priority_movement_label((int) $row['rank_delta'], (float) ($row['score_delta'] ?? 0.0));
        $row['explanation'] = activity_priority_doctrine_explanation($row);
    }
    unset($row);

    $activeFitIds = array_fill_keys(array_map(static fn (array $fit): int => (int) ($fit['entity_id'] ?? 0), array_filter($fitRows, static fn (array $fit): bool => in_array((string) ($fit['activity_level'] ?? 'low'), ['active', 'highly active'], true))), true);
    $doctrineItemMeta = [];
    foreach ($fits as $fit) {
        $fitId = (int) ($fit['id'] ?? 0);
        $supply = is_array($fit['supply'] ?? null) ? $fit['supply'] : [];
        $groupNames = array_values((array) ($fit['group_names'] ?? []));
        foreach ($itemsByFitId[$fitId] ?? [] as $item) {
            $typeId = (int) ($item['type_id'] ?? 0);
            if ($typeId <= 0) {
                continue;
            }
            if (!isset($doctrineItemMeta[$typeId])) {
                $doctrineItemMeta[$typeId] = [
                    'linked_fit_ids' => [],
                    'linked_group_names' => [],
                    'active_fit_ids' => [],
                    'bottleneck_fit_count' => 0,
                    'readiness_gap_fit_count' => 0,
                    'max_doctrine_activity_score' => 0.0,
                    'avg_doctrine_activity_score' => 0.0,
                    'activity_score_total' => 0.0,
                    'activity_score_count' => 0,
                ];
            }
            $doctrineItemMeta[$typeId]['linked_fit_ids'][$fitId] = (string) ($fit['fit_name'] ?? 'Doctrine fit');
            foreach ($groupNames as $groupName) {
                $safeName = trim((string) $groupName);
                if ($safeName !== '') {
                    $doctrineItemMeta[$typeId]['linked_group_names'][$safeName] = true;
                }
            }
            if (isset($activeFitIds[$fitId])) {
                $doctrineItemMeta[$typeId]['active_fit_ids'][$fitId] = true;
            }
            if ((int) ($supply['bottleneck_type_id'] ?? 0) === $typeId && (($supply['bottleneck_is_stock_tracked'] ?? true) === true)) {
                $doctrineItemMeta[$typeId]['bottleneck_fit_count']++;
            }
            if ((int) ($supply['gap_to_target_fit_count'] ?? 0) > 0) {
                $doctrineItemMeta[$typeId]['readiness_gap_fit_count']++;
            }
            $fitScore = (float) (($fitById[$fitId]['activity_score'] ?? 0.0));
            $doctrineItemMeta[$typeId]['max_doctrine_activity_score'] = max((float) $doctrineItemMeta[$typeId]['max_doctrine_activity_score'], $fitScore);
            $doctrineItemMeta[$typeId]['activity_score_total'] += $fitScore;
            $doctrineItemMeta[$typeId]['activity_score_count']++;
        }
    }
    foreach ($doctrineItemMeta as $typeId => &$meta) {
        $count = max(1, (int) ($meta['activity_score_count'] ?? 1));
        $meta['avg_doctrine_activity_score'] = round(((float) ($meta['activity_score_total'] ?? 0.0)) / $count, 2);
    }
    unset($meta);

    $previousItemSnapshots = [];
    foreach (db_item_priority_latest_snapshots(array_keys($marketByTypeId)) as $row) {
        $previousItemSnapshots[(int) ($row['type_id'] ?? 0)] = $row;
    }

    $itemRows = [];
    foreach ($marketRows as $row) {
        $typeId = (int) ($row['type_id'] ?? 0);
        if ($typeId <= 0) {
            continue;
        }
        $loss = $itemLossByType[$typeId] ?? [];
        $depletion = $depletionByType[$typeId] ?? [];
        $doctrineMeta = $doctrineItemMeta[$typeId] ?? [];
        $isDoctrineLinked = isset($doctrineMeta['linked_fit_ids']) && $doctrineMeta['linked_fit_ids'] !== [];
        $linkedDoctrineCount = $isDoctrineLinked ? count((array) ($doctrineMeta['linked_fit_ids'] ?? [])) : 0;
        $linkedActiveDoctrineCount = $isDoctrineLinked ? count((array) ($doctrineMeta['active_fit_ids'] ?? [])) : 0;
        $doctrineActivityWeight = $isDoctrineLinked
            ? min(26.0, (((float) ($doctrineMeta['max_doctrine_activity_score'] ?? 0.0)) * 0.18) + (((float) ($doctrineMeta['avg_doctrine_activity_score'] ?? 0.0)) * 0.10) + ($linkedActiveDoctrineCount * 2.8))
            : 0.0;
        $directLossWeight = min(24.0,
            ((int) ($loss['quantity_24h'] ?? 0) * 2.2)
            + (max(0, (int) (($loss['quantity_3d'] ?? 0) - ($loss['quantity_24h'] ?? 0))) * 1.1)
            + (max(0, (int) (($loss['quantity_7d'] ?? 0) - ($loss['quantity_3d'] ?? 0))) * 0.6)
            + ((int) ($loss['losses_24h'] ?? 0) * 1.8)
        );
        $stockGapWeight = min(24.0,
            ((bool) ($row['missing_in_alliance'] ?? false) ? 18.0 : 0.0)
            + ((bool) ($row['weak_alliance_stock'] ?? false) ? 10.0 : 0.0)
            + min(12.0, (float) ($row['stock_score'] ?? 0) * 0.12)
        );
        $depletionWeight = match ((string) ($depletion['classification'] ?? 'stable')) {
            'draining' => min(14.0, 6.0 + max(0.0, (float) ($depletion['depletion_7d'] ?? 0))),
            'recovering' => -4.0,
            default => 0.0,
        };
        $bottleneckWeight = min(18.0, ((int) ($doctrineMeta['bottleneck_fit_count'] ?? 0) * 6.0) + ((int) ($doctrineMeta['readiness_gap_fit_count'] ?? 0) * 2.0));
        $doctrinePriorityBonus = $isDoctrineLinked ? (10.0 + min(8.0, $linkedDoctrineCount * 1.5)) : 0.0;
        $consumablePenalty = item_scope_type_is_durable_loss_relevant($typeId) ? 0.0 : 12.0;
        $priorityScore = round(max(0.0, min(100.0, $doctrineActivityWeight + $directLossWeight + $stockGapWeight + $depletionWeight + $bottleneckWeight + $doctrinePriorityBonus - $consumablePenalty)), 2);
        $components = [
            'doctrine_activity_weight' => round($doctrineActivityWeight, 2),
            'direct_loss_frequency_weight' => round($directLossWeight, 2),
            'stock_gap_weight' => round($stockGapWeight, 2),
            'depletion_weight' => round($depletionWeight, 2),
            'bottleneck_weight' => round($bottleneckWeight, 2),
            'doctrine_priority_bonus' => round($doctrinePriorityBonus, 2),
        ];
        $previous = $previousItemSnapshots[$typeId] ?? [];
        $scoreDelta = round($priorityScore - (float) ($previous['priority_score'] ?? 0.0), 2);
        $itemRows[] = $row + [
            'type_id' => $typeId,
            'item_name' => (string) ($row['type_name'] ?? ('Type #' . $typeId)),
            'priority_score' => $priorityScore,
            'priority_band' => activity_priority_item_band($priorityScore),
            'is_doctrine_linked' => $isDoctrineLinked,
            'linked_doctrine_count' => $linkedDoctrineCount,
            'linked_active_doctrine_count' => $linkedActiveDoctrineCount,
            'linked_doctrine_names' => array_slice(array_keys((array) ($doctrineMeta['linked_group_names'] ?? [])), 0, 5),
            'local_available_qty' => max(0, (int) ($row['alliance_total_sell_volume'] ?? 0)),
            'local_sell_orders' => max(0, (int) ($row['alliance_sell_order_count'] ?? 0)),
            'local_sell_volume' => max(0, (int) ($row['alliance_total_sell_volume'] ?? 0)),
            'recent_loss_qty_24h' => max(0, (int) ($loss['quantity_24h'] ?? 0)),
            'recent_loss_qty_3d' => max(0, (int) ($loss['quantity_3d'] ?? 0)),
            'recent_loss_qty_7d' => max(0, (int) ($loss['quantity_7d'] ?? 0)),
            'recent_loss_events_24h' => max(0, (int) ($loss['losses_24h'] ?? 0)),
            'recent_loss_events_3d' => max(0, (int) ($loss['losses_3d'] ?? 0)),
            'recent_loss_events_7d' => max(0, (int) ($loss['losses_7d'] ?? 0)),
            'readiness_gap_fit_count' => max(0, (int) ($doctrineMeta['readiness_gap_fit_count'] ?? 0)),
            'bottleneck_fit_count' => max(0, (int) ($doctrineMeta['bottleneck_fit_count'] ?? 0)),
            'depletion_state' => (string) ($depletion['classification'] ?? 'stable'),
            'score_components' => $components,
            'score_delta' => $scoreDelta,
        ];
    }

    usort($itemRows, static function (array $a, array $b): int {
        $doctrineWeight = ((bool) ($b['is_doctrine_linked'] ?? false) <=> (bool) ($a['is_doctrine_linked'] ?? false));
        if ($doctrineWeight !== 0 && abs((float) ($a['priority_score'] ?? 0.0) - (float) ($b['priority_score'] ?? 0.0)) <= 8.0) {
            return $doctrineWeight;
        }

        return ((float) ($b['priority_score'] ?? 0.0) <=> (float) ($a['priority_score'] ?? 0.0))
            ?: strcasecmp((string) ($a['item_name'] ?? ''), (string) ($b['item_name'] ?? ''));
    });
    foreach ($itemRows as $index => &$row) {
        $previous = $previousItemSnapshots[(int) ($row['type_id'] ?? 0)] ?? [];
        $row['rank_position'] = $index + 1;
        $row['previous_rank_position'] = isset($previous['rank_position']) ? (int) $previous['rank_position'] : null;
        $row['rank_delta'] = $row['previous_rank_position'] !== null ? ((int) $row['previous_rank_position'] - (int) $row['rank_position']) : 0;
        $row['movement_label'] = activity_priority_movement_label((int) $row['rank_delta'], (float) ($row['score_delta'] ?? 0.0));
        $row['explanation'] = activity_priority_item_explanation($row);
    }
    unset($row);

    $snapshotTime = gmdate('Y-m-d H:i:s');
    $groupSnapshotRows = array_map(static fn (array $row): array => [
        'entity_type' => 'group',
        'entity_id' => (int) ($row['entity_id'] ?? 0),
        'entity_name' => (string) ($row['doctrine_name'] ?? ''),
        'snapshot_time' => $snapshotTime,
        'rank_position' => (int) ($row['rank_position'] ?? 0),
        'previous_rank_position' => $row['previous_rank_position'] ?? null,
        'rank_delta' => (int) ($row['rank_delta'] ?? 0),
        'activity_score' => (float) ($row['activity_score'] ?? 0.0),
        'activity_level' => (string) ($row['activity_level'] ?? 'low'),
        'hull_losses_24h' => (int) ($row['hull_losses_24h'] ?? 0),
        'hull_losses_3d' => (int) ($row['hull_losses_3d'] ?? 0),
        'hull_losses_7d' => (int) ($row['hull_losses_7d'] ?? 0),
        'module_losses_24h' => (int) ($row['module_losses_24h'] ?? 0),
        'module_losses_3d' => (int) ($row['module_losses_3d'] ?? 0),
        'module_losses_7d' => (int) ($row['module_losses_7d'] ?? 0),
        'fit_equivalent_losses_24h' => (float) ($row['fit_equivalent_losses_24h'] ?? 0.0),
        'fit_equivalent_losses_3d' => (float) ($row['fit_equivalent_losses_3d'] ?? 0.0),
        'fit_equivalent_losses_7d' => (float) ($row['fit_equivalent_losses_7d'] ?? 0.0),
        'readiness_state' => (string) ($row['readiness_state'] ?? 'market_ready'),
        'resupply_pressure_state' => (string) ($row['resupply_pressure_state'] ?? 'stable'),
        'resupply_pressure' => (string) ($row['resupply_pressure'] ?? 'Stable'),
        'readiness_gap_count' => (int) ($row['readiness_gap_count'] ?? 0),
        'resupply_gap_isk' => (float) ($row['resupply_gap_isk'] ?? 0.0),
        'score_components_json' => json_encode((array) ($row['score_components'] ?? []), JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE),
        'explanation_text' => (string) ($row['explanation'] ?? ''),
    ], $groupRows);
    $fitSnapshotRows = array_map(static fn (array $row): array => [
        'entity_type' => 'fit',
        'entity_id' => (int) ($row['entity_id'] ?? 0),
        'entity_name' => (string) ($row['doctrine_name'] ?? ''),
        'snapshot_time' => $snapshotTime,
        'rank_position' => (int) ($row['rank_position'] ?? 0),
        'previous_rank_position' => $row['previous_rank_position'] ?? null,
        'rank_delta' => (int) ($row['rank_delta'] ?? 0),
        'activity_score' => (float) ($row['activity_score'] ?? 0.0),
        'activity_level' => (string) ($row['activity_level'] ?? 'low'),
        'hull_losses_24h' => (int) ($row['hull_losses_24h'] ?? 0),
        'hull_losses_3d' => (int) ($row['hull_losses_3d'] ?? 0),
        'hull_losses_7d' => (int) ($row['hull_losses_7d'] ?? 0),
        'module_losses_24h' => (int) ($row['module_losses_24h'] ?? 0),
        'module_losses_3d' => (int) ($row['module_losses_3d'] ?? 0),
        'module_losses_7d' => (int) ($row['module_losses_7d'] ?? 0),
        'fit_equivalent_losses_24h' => (float) ($row['fit_equivalent_losses_24h'] ?? 0.0),
        'fit_equivalent_losses_3d' => (float) ($row['fit_equivalent_losses_3d'] ?? 0.0),
        'fit_equivalent_losses_7d' => (float) ($row['fit_equivalent_losses_7d'] ?? 0.0),
        'readiness_state' => (string) ($row['readiness_state'] ?? 'market_ready'),
        'resupply_pressure_state' => (string) ($row['resupply_pressure_state'] ?? 'stable'),
        'resupply_pressure' => (string) ($row['resupply_pressure'] ?? 'Stable'),
        'readiness_gap_count' => (int) ($row['readiness_gap_count'] ?? 0),
        'resupply_gap_isk' => (float) ($row['resupply_gap_isk'] ?? 0.0),
        'score_components_json' => json_encode((array) ($row['score_components'] ?? []), JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE),
        'explanation_text' => (string) ($row['explanation'] ?? ''),
    ], $fitRows);
    $itemSnapshotRows = array_map(static fn (array $row): array => [
        'type_id' => (int) ($row['type_id'] ?? 0),
        'item_name' => (string) ($row['item_name'] ?? ''),
        'snapshot_time' => $snapshotTime,
        'rank_position' => (int) ($row['rank_position'] ?? 0),
        'previous_rank_position' => $row['previous_rank_position'] ?? null,
        'rank_delta' => (int) ($row['rank_delta'] ?? 0),
        'priority_score' => (float) ($row['priority_score'] ?? 0.0),
        'priority_band' => (string) ($row['priority_band'] ?? 'watch'),
        'is_doctrine_linked' => !empty($row['is_doctrine_linked']) ? 1 : 0,
        'linked_doctrine_count' => (int) ($row['linked_doctrine_count'] ?? 0),
        'linked_active_doctrine_count' => (int) ($row['linked_active_doctrine_count'] ?? 0),
        'local_available_qty' => (int) ($row['local_available_qty'] ?? 0),
        'local_sell_orders' => (int) ($row['local_sell_orders'] ?? 0),
        'local_sell_volume' => (int) ($row['local_sell_volume'] ?? 0),
        'recent_loss_qty_24h' => (int) ($row['recent_loss_qty_24h'] ?? 0),
        'recent_loss_qty_3d' => (int) ($row['recent_loss_qty_3d'] ?? 0),
        'recent_loss_qty_7d' => (int) ($row['recent_loss_qty_7d'] ?? 0),
        'recent_loss_events_24h' => (int) ($row['recent_loss_events_24h'] ?? 0),
        'recent_loss_events_3d' => (int) ($row['recent_loss_events_3d'] ?? 0),
        'recent_loss_events_7d' => (int) ($row['recent_loss_events_7d'] ?? 0),
        'readiness_gap_fit_count' => (int) ($row['readiness_gap_fit_count'] ?? 0),
        'bottleneck_fit_count' => (int) ($row['bottleneck_fit_count'] ?? 0),
        'depletion_state' => (string) ($row['depletion_state'] ?? 'stable'),
        'score_components_json' => json_encode((array) ($row['score_components'] ?? []), JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE),
        'linked_doctrines_json' => json_encode((array) ($row['linked_doctrine_names'] ?? []), JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE),
        'explanation_text' => (string) ($row['explanation'] ?? ''),
    ], $itemRows);

    try {
        db_doctrine_activity_snapshot_bulk_insert(array_merge($groupSnapshotRows, $fitSnapshotRows));
        db_item_priority_snapshot_bulk_insert($itemSnapshotRows);
    } catch (Throwable) {
    }

    try {
        db_time_series_store_doctrine_daily_rollups($fitRows, $groupRows, $itemsByFitId, $marketByTypeId);
    } catch (Throwable) {
    }

    $risingDoctrines = array_slice(array_values(array_filter($groupRows, static fn (array $row): bool => (int) ($row['rank_delta'] ?? 0) > 0 || (float) ($row['score_delta'] ?? 0.0) >= 6.0)), 0, 6);
    $newlyElevatedItems = array_slice(array_values(array_filter($itemRows, static fn (array $row): bool => in_array((string) ($row['priority_band'] ?? 'watch'), ['high', 'critical'], true) && ((int) ($row['rank_delta'] ?? 0) > 0 || (float) ($row['score_delta'] ?? 0.0) >= 6.0))), 0, 8);
    $coolingItems = array_slice(array_values(array_filter($itemRows, static fn (array $row): bool => (int) ($row['rank_delta'] ?? 0) < 0 || (float) ($row['score_delta'] ?? 0.0) <= -6.0)), 0, 8);

    return [
        'summary_cards' => [
            ['label' => 'Active doctrines', 'value' => (string) count(array_filter($groupRows, static fn (array $row): bool => in_array((string) ($row['activity_level'] ?? 'low'), ['active', 'highly active'], true))), 'context' => 'Doctrine groups showing repeated tracked loss pressure right now'],
            ['label' => 'Highly active fits', 'value' => (string) count(array_filter($fitRows, static fn (array $row): bool => (string) ($row['activity_level'] ?? 'low') === 'highly active')), 'context' => 'Doctrine fits with the strongest recent hull + module pressure'],
            ['label' => 'Elevated doctrine items', 'value' => (string) count(array_filter($itemRows, static fn (array $row): bool => !empty($row['is_doctrine_linked']) && in_array((string) ($row['priority_band'] ?? 'watch'), ['high', 'critical'], true))), 'context' => 'Doctrine-linked items now pinned above comparable generic signals'],
            ['label' => 'Items moving up', 'value' => (string) count(array_filter($itemRows, static fn (array $row): bool => (int) ($row['rank_delta'] ?? 0) > 0 || (float) ($row['score_delta'] ?? 0.0) > 0)), 'context' => 'Priority shifts versus the prior materialized snapshot'],
        ],
        'active_doctrines' => array_slice($groupRows, 0, 12),
        'active_fits' => array_slice($fitRows, 0, 10),
        'priority_items' => array_slice($itemRows, 0, 20),
        'trend_movement' => [
            'doctrines_moving_up' => $risingDoctrines,
            'items_newly_elevated' => $newlyElevatedItems,
            'items_cooling_down' => $coolingItems,
        ],
        'questions_answered' => [
            'which_doctrines_are_active' => array_map(static fn (array $row): string => (string) ($row['doctrine_name'] ?? ''), array_slice($groupRows, 0, 3)),
            'which_items_should_move_up' => array_map(static fn (array $row): string => (string) ($row['item_name'] ?? ''), array_slice($itemRows, 0, 5)),
        ],
        'fit_count' => count($fitRows),
        'group_count' => count($groupRows),
        'item_count' => count($itemRows),
    ];
}
