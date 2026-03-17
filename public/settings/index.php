<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$sections = setting_sections();
$section = active_section();
$title = 'Settings';

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    if (!validate_csrf($_POST['_token'] ?? null)) {
        http_response_code(419);
        exit('Invalid CSRF token');
    }

    $submittedSection = $_POST['section'] ?? 'general';

    $saved = false;

    switch ($submittedSection) {
        case 'general':
            $saved = save_settings([
                'app_name' => sanitize_app_name((string) ($_POST['app_name'] ?? app_name())),
                'app_timezone' => sanitize_timezone((string) ($_POST['app_timezone'] ?? 'UTC')),
                'default_currency' => sanitize_currency((string) ($_POST['default_currency'] ?? 'ISK')),
            ]);
            break;

        case 'trading-stations':
            $saved = save_settings([
                'market_station_id' => sanitize_station_selection($_POST['market_station_id'] ?? null, 'market'),
                'alliance_station_id' => sanitize_station_selection($_POST['alliance_station_id'] ?? null, 'alliance'),
            ]);
            break;

        case 'esi-login':
            $saved = save_settings([
                'esi_client_id' => trim($_POST['esi_client_id'] ?? ''),
                'esi_client_secret' => trim($_POST['esi_client_secret'] ?? ''),
                'esi_callback_url' => trim($_POST['esi_callback_url'] ?? ''),
                'esi_scopes' => trim($_POST['esi_scopes'] ?? implode(' ', esi_default_scopes())),
                'esi_enabled' => isset($_POST['esi_enabled']) ? '1' : '0',
            ]);
            break;

        case 'data-sync':
            $dataSyncAction = trim((string) ($_POST['data_sync_action'] ?? 'save'));

            if ($dataSyncAction === 'run-now') {
                $requestedJob = trim((string) ($_POST['run_now_job_key'] ?? ''));
                $runNow = run_data_sync_now($requestedJob === '' ? null : $requestedJob);
                $saved = (bool) ($runNow['ok'] ?? false);
                flash('success', (string) ($runNow['message'] ?? 'Run now completed.'));
                header('Location: /settings?section=' . urlencode($submittedSection));
                exit;
            }

            if ($dataSyncAction === 'static-data-import') {
                try {
                    $result = static_data_import_reference_data('auto', false);
                    $saved = (bool) ($result['ok'] ?? false);
                    $message = $saved
                        ? ('Static data import completed in ' . strtoupper((string) ($result['mode'] ?? 'auto')) . ' mode. Build ' . (string) ($result['build_id'] ?? '-') . '; changed=' . ((bool) ($result['changed'] ?? false) ? 'yes' : 'no') . '; rows=' . (int) ($result['rows_written'] ?? 0) . '.')
                        : 'Static data import did not complete.';
                    flash('success', $message);
                } catch (Throwable $exception) {
                    $saved = false;
                    flash('success', 'Static data import failed: ' . $exception->getMessage());
                }

                header('Location: /settings?section=' . urlencode($submittedSection));
                exit;
            }

            $settingsSaved = save_settings(data_sync_settings_from_request($_POST));
            $schedulesSaved = save_data_sync_schedule_settings($_POST);
            $saved = $settingsSaved && $schedulesSaved;
            break;
    }

    flash('success', $saved ? 'Settings saved successfully.' : 'Database unavailable. Settings were not persisted.');
    header('Location: /settings?section=' . urlencode($submittedSection));
    exit;
}

$settingValues = get_settings([
    'app_name',
    'app_timezone',
    'default_currency',
    'market_station_id',
    'alliance_station_id',
    'esi_client_id',
    'esi_client_secret',
    'esi_callback_url',
    'esi_scopes',
    'esi_enabled',
    'incremental_updates_enabled',
    'incremental_strategy',
    'incremental_delete_policy',
    'incremental_chunk_size',
    'alliance_current_pipeline_enabled',
    'alliance_history_pipeline_enabled',
    'hub_history_pipeline_enabled',
    'alliance_current_backfill_start_date',
    'alliance_history_backfill_start_date',
    'hub_history_backfill_start_date',
    'raw_order_snapshot_retention_days',
    'sync_automation_enabled_since',
    'static_data_source_url',
]);

$stations = grouped_station_options();

$dbStatus = db_connection_status();
$latestEsiToken = null;
$requiredStructureScopes = esi_required_market_structure_scopes();
$missingStructureScopes = [];
$syncStatusCards = [];
$syncScheduleCards = sync_schedule_settings_view_model();
$runNowJobOptions = [];
$staticDataState = null;
if ($dbStatus['ok']) {
    $latestEsiToken = db_latest_esi_oauth_token();
    if ($latestEsiToken !== null) {
        $missingStructureScopes = esi_missing_scopes($latestEsiToken, $requiredStructureScopes);
    }

    $staticDataState = db_static_data_import_state_get(static_data_source_key());

    $syncStatusCards = [
        [
            'label' => 'Alliance Orders',
            'status' => sync_status_from_prefix('alliance.structure.', 6),
        ],
        [
            'label' => 'Hub History',
            'status' => sync_status_from_prefix('market.hub.', 4),
        ],
        [
            'label' => 'Maintenance',
            'status' => sync_status_from_prefix('maintenance.', 3),
        ],
    ];

}

foreach ($syncScheduleCards as $schedule) {
    $runNowJobOptions[] = [
        'job_key' => (string) ($schedule['job_key'] ?? ''),
        'label' => (string) ($schedule['label'] ?? ''),
    ];
}

include __DIR__ . '/../../src/views/partials/header.php';
?>
<div class="grid gap-6 xl:grid-cols-[260px_1fr]">
    <aside class="rounded-xl border border-border bg-card p-4">
        <h2 class="px-3 text-sm font-medium">Configuration Areas</h2>
        <div class="mt-3 space-y-1">
            <?php foreach ($sections as $key => $meta): ?>
                <a href="/settings?section=<?= urlencode($key) ?>"
                   class="block rounded-lg px-3 py-2 text-sm <?= $section === $key ? 'bg-accent/20 text-white' : 'text-muted hover:bg-white/5 hover:text-slate-100' ?>">
                    <?= htmlspecialchars($meta['title'], ENT_QUOTES) ?>
                </a>
            <?php endforeach; ?>
        </div>
    </aside>

    <section class="rounded-xl border border-border bg-card p-6">
        <h2 class="text-xl font-semibold"><?= htmlspecialchars($sections[$section]['title'], ENT_QUOTES) ?></h2>
        <p class="mt-1 text-sm text-muted"><?= htmlspecialchars($sections[$section]['description'], ENT_QUOTES) ?></p>

        <?php if (!$dbStatus['ok']): ?>
            <div class="mt-4 rounded-lg border border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm text-amber-200">
                Database is currently unreachable; showing fallback values.
            </div>
        <?php endif; ?>

        <?php if ($section === 'general'): ?>
            <form class="mt-6 space-y-4" method="post">
                <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                <input type="hidden" name="section" value="general">
                <label class="block space-y-2">
                    <span class="text-sm text-muted">Application Name</span>
                    <input name="app_name" value="<?= htmlspecialchars($settingValues['app_name'] ?? app_name(), ENT_QUOTES) ?>" class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring" />
                </label>
                <label class="block space-y-2">
                    <span class="text-sm text-muted">Timezone</span>
                    <input name="app_timezone" value="<?= htmlspecialchars($settingValues['app_timezone'] ?? app_timezone(), ENT_QUOTES) ?>" class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring" />
                </label>
                <label class="block space-y-2">
                    <span class="text-sm text-muted">Default Currency</span>
                    <input name="default_currency" value="<?= htmlspecialchars($settingValues['default_currency'] ?? default_currency(), ENT_QUOTES) ?>" class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring" />
                </label>
                <button class="rounded-lg bg-accent px-4 py-2 text-sm font-medium">Save General Settings</button>
            </form>
        <?php elseif ($section === 'trading-stations'): ?>
            <form class="mt-6 space-y-4" method="post">
                <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                <input type="hidden" name="section" value="trading-stations">
                <label class="block space-y-2" id="market-station-search-field">
                    <span class="text-sm text-muted">Market Station Selection</span>
                    <?php
                        $marketStationId = trim((string) ($settingValues['market_station_id'] ?? ''));
                        $marketStationName = selected_station_name('market_station_id');
                    ?>
                    <input type="hidden" name="market_station_id" id="market_station_id" value="<?= htmlspecialchars($marketStationId, ENT_QUOTES) ?>">
                    <input
                        type="text"
                        id="market_station_search"
                        autocomplete="off"
                        value="<?= htmlspecialchars($marketStationName ?? '', ENT_QUOTES) ?>"
                        placeholder="Search market stations by name"
                        class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring"
                    />
                    <p id="market_station_status" class="text-xs text-muted">
                        <?= htmlspecialchars($marketStationId === ''
                            ? 'Type at least 2 characters to search market stations.'
                            : ('Selected station: ' . ($marketStationName ?? ('Station #' . $marketStationId)) . ' (#' . $marketStationId . ').'), ENT_QUOTES) ?>
                    </p>
                    <ul id="market_station_results" class="hidden max-h-60 overflow-y-auto rounded-lg border border-border bg-black/40"></ul>
                </label>
                <label class="block space-y-2" id="alliance-structure-search-field">
                    <span class="text-sm text-muted">Alliance Structure Selection</span>
                    <?php
                        $allianceStationId = trim((string) ($settingValues['alliance_station_id'] ?? ''));
                        $allianceStationName = selected_station_name('alliance_station_id');
                    ?>
                    <input type="hidden" name="alliance_station_id" id="alliance_station_id" value="<?= htmlspecialchars($allianceStationId, ENT_QUOTES) ?>">
                    <input
                        type="text"
                        id="alliance_structure_search"
                        autocomplete="off"
                        value="<?= htmlspecialchars($allianceStationName ?? '', ENT_QUOTES) ?>"
                        placeholder="Search structures by name"
                        class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring"
                    />
                    <p id="alliance_structure_status" class="text-xs text-muted">
                        <?= htmlspecialchars($allianceStationId === ''
                            ? 'Search is scoped to the connected ESI character token.'
                            : ('Selected structure: ' . ($allianceStationName ?? ('Structure #' . $allianceStationId)) . ' (#' . $allianceStationId . ').'), ENT_QUOTES) ?>
                    </p>
                    <ul id="alliance_structure_results" class="hidden max-h-60 overflow-y-auto rounded-lg border border-border bg-black/40"></ul>
                </label>
                <?php if ($latestEsiToken !== null && $missingStructureScopes !== []): ?>
                    <div class="rounded-lg border border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
                        Missing required scopes for structure data: <span class="font-medium"><?= htmlspecialchars(implode(', ', $missingStructureScopes), ENT_QUOTES) ?></span>.
                        Update scopes to include <span class="font-medium">esi-universe.read_structures.v1</span> and <span class="font-medium">esi-markets.structure_markets.v1</span>, then reconnect your ESI character.
                    </div>
                <?php endif; ?>
                <button class="rounded-lg bg-accent px-4 py-2 text-sm font-medium">Save Trading Stations</button>
            </form>

            <script>
                (() => {
                    const marketItems = <?= json_encode(array_map(static function (array $station): array {
                        return [
                            'id' => (int) $station['id'],
                            'name' => (string) ($station['station_name'] ?? ''),
                            'system' => (string) ($station['system_name'] ?? ''),
                            'type' => (string) ($station['station_type_name'] ?? ''),
                        ];
                    }, $stations['market']), JSON_HEX_TAG | JSON_HEX_AMP | JSON_HEX_APOS | JSON_HEX_QUOT) ?>;

                    const initializeSearchField = ({
                        inputId,
                        hiddenId,
                        resultsId,
                        statusId,
                        minimumQueryLength,
                        searchingLabel,
                        selectionStatusPrefix,
                        emptyQueryMessage,
                        noResultsMessage,
                        fetchResults,
                    }) => {
                        const input = document.getElementById(inputId);
                        const hidden = document.getElementById(hiddenId);
                        const results = document.getElementById(resultsId);
                        const status = document.getElementById(statusId);

                        if (!input || !hidden || !results || !status) {
                            return;
                        }

                        let debounceTimer = null;

                        const clearResults = () => {
                            results.innerHTML = '';
                            results.classList.add('hidden');
                        };

                        const renderResults = (items) => {
                            clearResults();

                            if (!Array.isArray(items) || items.length === 0) {
                                status.textContent = noResultsMessage;
                                return;
                            }

                            const fragment = document.createDocumentFragment();

                            items.forEach((item) => {
                                const row = document.createElement('li');
                                const button = document.createElement('button');
                                const details = [];

                                if (item.system) {
                                    details.push('System ' + item.system);
                                }

                                if (item.type) {
                                    details.push('Type ' + item.type);
                                }

                                button.type = 'button';
                                button.className = 'flex w-full flex-col items-start gap-1 px-3 py-2 text-left text-sm hover:bg-white/5';
                                button.innerHTML = '<span class="text-slate-100"></span><span class="text-xs text-muted"></span>';
                                button.querySelector('span').textContent = item.name;
                                button.querySelectorAll('span')[1].textContent = '#' + item.id + (details.length ? ' · ' + details.join(' · ') : '');
                                button.addEventListener('click', () => {
                                    hidden.value = String(item.id);
                                    input.value = item.name;
                                    status.textContent = selectionStatusPrefix + item.name + ' (#' + item.id + ').';
                                    clearResults();
                                });

                                row.appendChild(button);
                                fragment.appendChild(row);
                            });

                            results.appendChild(fragment);
                            results.classList.remove('hidden');
                        };

                        input.addEventListener('input', () => {
                            const query = input.value.trim();

                            hidden.value = '';

                            if (debounceTimer !== null) {
                                clearTimeout(debounceTimer);
                            }

                            if (query.length < minimumQueryLength) {
                                status.textContent = emptyQueryMessage;
                                clearResults();
                                return;
                            }

                            debounceTimer = window.setTimeout(async () => {
                                status.textContent = searchingLabel;

                                try {
                                    const items = await fetchResults(query);
                                    status.textContent = 'Select an option from the list.';
                                    renderResults(items);
                                } catch (error) {
                                    status.textContent = error instanceof Error ? error.message : 'Lookup failed.';
                                    clearResults();
                                }
                            }, 250);
                        });
                    };

                    initializeSearchField({
                        inputId: 'market_station_search',
                        hiddenId: 'market_station_id',
                        resultsId: 'market_station_results',
                        statusId: 'market_station_status',
                        minimumQueryLength: 2,
                        searchingLabel: 'Searching…',
                        selectionStatusPrefix: 'Selected station: ',
                        emptyQueryMessage: 'Type at least 2 characters to search market stations.',
                        noResultsMessage: 'No matching market stations found.',
                        fetchResults: async (query) => {
                            const normalizedQuery = query.toLowerCase();

                            return marketItems
                                .filter((item) => item.name.toLowerCase().includes(normalizedQuery))
                                .slice(0, 20);
                        },
                    });

                    initializeSearchField({
                        inputId: 'alliance_structure_search',
                        hiddenId: 'alliance_station_id',
                        resultsId: 'alliance_structure_results',
                        statusId: 'alliance_structure_status',
                        minimumQueryLength: 2,
                        searchingLabel: 'Searching…',
                        selectionStatusPrefix: 'Selected structure: ',
                        emptyQueryMessage: 'Type at least 2 characters to search alliance structures.',
                        noResultsMessage: 'No matching structures found.',
                        fetchResults: async (query) => {
                            const response = await fetch('/settings/esi-structures.php?q=' + encodeURIComponent(query), {
                                headers: { 'Accept': 'application/json' },
                            });

                            const payload = await response.json();
                            if (!response.ok) {
                                throw new Error(payload.error || 'Lookup failed.');
                            }

                            return payload.results || [];
                        },
                    });
                })();
            </script>
        <?php elseif ($section === 'esi-login'): ?>
            <form class="mt-6 space-y-4" method="post">
                <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                <input type="hidden" name="section" value="esi-login">
                <label class="block space-y-2">
                    <span class="text-sm text-muted">ESI Client ID</span>
                    <input name="esi_client_id" value="<?= htmlspecialchars($settingValues['esi_client_id'] ?? '', ENT_QUOTES) ?>" class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring" />
                </label>
                <label class="block space-y-2">
                    <span class="text-sm text-muted">ESI Client Secret</span>
                    <input name="esi_client_secret" type="password" value="<?= htmlspecialchars($settingValues['esi_client_secret'] ?? '', ENT_QUOTES) ?>" class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring" />
                </label>
                <label class="block space-y-2">
                    <span class="text-sm text-muted">Callback URL</span>
                    <input name="esi_callback_url" value="<?= htmlspecialchars($settingValues['esi_callback_url'] ?? base_url('/callback'), ENT_QUOTES) ?>" class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring" />
                </label>
                <label class="block space-y-2">
                    <span class="text-sm text-muted">Enabled Scopes (space separated)</span>
                    <textarea name="esi_scopes" rows="4" class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring"><?= htmlspecialchars($settingValues['esi_scopes'] ?? implode(' ', esi_default_scopes()), ENT_QUOTES) ?></textarea>
                </label>
                <label class="flex items-center gap-3 rounded-lg border border-border bg-black/20 p-3">
                    <input type="checkbox" name="esi_enabled" value="1" <?= ($settingValues['esi_enabled'] ?? '0') === '1' ? 'checked' : '' ?> class="size-4 rounded border-border bg-black">
                    <span class="text-sm">Enable ESI OAuth login</span>
                </label>
                <div class="flex flex-wrap items-center gap-3">
                    <button class="rounded-lg bg-accent px-4 py-2 text-sm font-medium">Save ESI Login Settings</button>
                    <?php if (($settingValues['esi_enabled'] ?? '0') === '1' && ($settingValues['esi_client_id'] ?? '') !== ''): ?>
                        <a class="rounded-lg border border-border px-4 py-2 text-sm hover:bg-white/5" href="<?= htmlspecialchars(esi_sso_authorize_url(), ENT_QUOTES) ?>">Connect ESI Character</a>
                    <?php endif; ?>
                </div>
            </form>

            <div class="mt-6 rounded-lg border border-border bg-black/20 p-4 text-sm text-muted">
                <p class="font-medium text-slate-200">ESI OAuth Status</p>
                <?php if ($latestEsiToken === null): ?>
                    <p class="mt-2">No ESI token is stored yet.</p>
                <?php else: ?>
                    <p class="mt-2">Connected character: <span class="text-slate-100"><?= htmlspecialchars($latestEsiToken['character_name'], ENT_QUOTES) ?></span> (<?= (int) $latestEsiToken['character_id'] ?>)</p>
                    <p class="mt-1">Token expires at (UTC): <?= htmlspecialchars($latestEsiToken['expires_at'], ENT_QUOTES) ?></p>
                    <p class="mt-1">Scopes: <?= htmlspecialchars($latestEsiToken['scopes'], ENT_QUOTES) ?></p>
                    <?php if ($missingStructureScopes !== []): ?>
                        <div class="mt-3 rounded-lg border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-amber-100">
                            Required scopes are missing: <span class="font-medium"><?= htmlspecialchars(implode(', ', $missingStructureScopes), ENT_QUOTES) ?></span>.
                            Add <span class="font-medium">esi-universe.read_structures.v1</span> and <span class="font-medium">esi-markets.structure_markets.v1</span> in the scopes field, save, then reconnect your character.
                        </div>
                    <?php endif; ?>
                <?php endif; ?>
            </div>
        <?php else: ?>
            <?php if ($syncStatusCards !== []): ?>
                <div class="mt-6 grid gap-3 md:grid-cols-3">
                    <?php foreach ($syncStatusCards as $syncCard): ?>
                        <?php $status = $syncCard['status']; ?>
                        <article class="rounded-lg border border-border bg-black/20 p-4 text-sm">
                            <p class="text-xs uppercase tracking-[0.16em] text-muted"><?= htmlspecialchars($syncCard['label'], ENT_QUOTES) ?></p>
                            <p class="mt-2 text-sm text-slate-100">Last success: <?= htmlspecialchars($status['last_success_at'] ?? 'Never', ENT_QUOTES) ?></p>
                            <p class="mt-1 text-sm text-muted">Rows written (recent runs): <?= (int) ($status['recent_rows_written'] ?? 0) ?></p>
                            <p class="mt-1 text-xs text-rose-200">Last error: <?= htmlspecialchars($status['last_error_message'] ?? 'None', ENT_QUOTES) ?></p>
                        </article>
                    <?php endforeach; ?>
                </div>
            <?php endif; ?>

            <form class="mt-6 space-y-4" method="post">
                <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                <input type="hidden" name="section" value="data-sync">
                <label class="flex items-center gap-3 rounded-lg border border-border bg-black/20 p-3">
                    <input type="checkbox" name="incremental_updates_enabled" value="1" <?= ($settingValues['incremental_updates_enabled'] ?? '1') === '1' ? 'checked' : '' ?> class="size-4 rounded border-border bg-black">
                    <span class="text-sm">Enable incremental database updates</span>
                </label>
                <label class="block space-y-2">
                    <span class="text-sm text-muted">Incremental Strategy</span>
                    <select name="incremental_strategy" class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring">
                        <?php foreach (incremental_strategy_options() as $value => $label): ?>
                            <option value="<?= htmlspecialchars($value, ENT_QUOTES) ?>" <?= ($settingValues['incremental_strategy'] ?? 'watermark_upsert') === $value ? 'selected' : '' ?>>
                                <?= htmlspecialchars($label, ENT_QUOTES) ?>
                            </option>
                        <?php endforeach; ?>
                    </select>
                </label>
                <label class="block space-y-2">
                    <span class="text-sm text-muted">Delete Handling Policy</span>
                    <select name="incremental_delete_policy" class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring">
                        <?php foreach (incremental_delete_policy_options() as $value => $label): ?>
                            <option value="<?= htmlspecialchars($value, ENT_QUOTES) ?>" <?= ($settingValues['incremental_delete_policy'] ?? 'reconcile') === $value ? 'selected' : '' ?>>
                                <?= htmlspecialchars($label, ENT_QUOTES) ?>
                            </option>
                        <?php endforeach; ?>
                    </select>
                </label>
                <label class="block space-y-2">
                    <span class="text-sm text-muted">Chunk Size</span>
                    <input type="number" min="100" max="10000" step="100" name="incremental_chunk_size" value="<?= htmlspecialchars($settingValues['incremental_chunk_size'] ?? '1000', ENT_QUOTES) ?>" class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring" />
                </label>

                <div class="space-y-3">
                    <p class="text-sm text-muted">Per-job schedules</p>
                    <?php foreach ($syncScheduleCards as $schedule): ?>
                        <div class="grid gap-3 rounded-lg border border-border bg-black/20 p-3 md:grid-cols-[minmax(0,1fr)_auto_auto]">
                            <label class="flex items-center gap-3">
                                <input type="hidden" name="<?= htmlspecialchars($schedule['enabled_key'], ENT_QUOTES) ?>" value="0">
                                <input type="checkbox" name="<?= htmlspecialchars($schedule['enabled_key'], ENT_QUOTES) ?>" value="1" <?= (int) $schedule['enabled'] === 1 ? 'checked' : '' ?> class="size-4 rounded border-border bg-black">
                                <span class="text-sm">Enable <?= htmlspecialchars($schedule['label'], ENT_QUOTES) ?> job</span>
                            </label>
                            <input
                                type="number"
                                min="1"
                                max="86400"
                                step="1"
                                name="<?= htmlspecialchars($schedule['interval_value_key'], ENT_QUOTES) ?>"
                                value="<?= htmlspecialchars((string) $schedule['interval_value'], ENT_QUOTES) ?>"
                                class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring"
                            />
                            <select name="<?= htmlspecialchars($schedule['interval_unit_key'], ENT_QUOTES) ?>" class="rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring">
                                <option value="seconds" <?= ($schedule['interval_unit'] ?? 'minutes') === 'seconds' ? 'selected' : '' ?>>seconds</option>
                                <option value="minutes" <?= ($schedule['interval_unit'] ?? 'minutes') === 'minutes' ? 'selected' : '' ?>>minutes</option>
                            </select>
                        </div>
                    <?php endforeach; ?>
                </div>


                <div class="rounded-lg border border-border bg-black/20 p-3 text-sm text-muted">
                    <?php $syncEnabledSince = sanitize_backfill_start_date($settingValues['sync_automation_enabled_since'] ?? '') ?: gmdate('Y-m-d'); ?>
                    Backfill start is automatic. Pipelines begin from the date sync automation was enabled: <span class="font-medium text-slate-100"><?= htmlspecialchars($syncEnabledSince, ENT_QUOTES) ?></span>.
                </div>

                <label class="block space-y-2">
                    <span class="text-sm text-muted">Raw Order Snapshot Retention (days)</span>
                    <input type="number" min="1" max="3650" step="1" name="raw_order_snapshot_retention_days" value="<?= htmlspecialchars($settingValues['raw_order_snapshot_retention_days'] ?? '30', ENT_QUOTES) ?>" class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring" />
                </label>

                <label class="block space-y-2">
                    <span class="text-sm text-muted">Static Data JSONL ZIP Source URL</span>
                    <input type="url" name="static_data_source_url" value="<?= htmlspecialchars($settingValues['static_data_source_url'] ?? 'https://developers.eveonline.com/static-data/eve-online-static-data-latest-jsonl.zip', ENT_QUOTES) ?>" class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring" />
                    <p class="text-xs text-muted">Importer expects the official CCP JSONL ZIP payload (<span class="font-mono">.zip</span>) from developers.eveonline.com.</p>
                </label>

                <div class="space-y-3">
                    <p class="text-sm text-muted">Pipeline toggles</p>
                    <label class="flex items-center gap-3 rounded-lg border border-border bg-black/20 p-3">
                        <input type="hidden" name="alliance_current_pipeline_enabled" value="0">
                        <input type="checkbox" name="alliance_current_pipeline_enabled" value="1" <?= ($settingValues['alliance_current_pipeline_enabled'] ?? '1') === '1' ? 'checked' : '' ?> class="size-4 rounded border-border bg-black">
                        <span class="text-sm">Enable alliance current pipeline</span>
                    </label>
                    <label class="flex items-center gap-3 rounded-lg border border-border bg-black/20 p-3">
                        <input type="hidden" name="alliance_history_pipeline_enabled" value="0">
                        <input type="checkbox" name="alliance_history_pipeline_enabled" value="1" <?= ($settingValues['alliance_history_pipeline_enabled'] ?? '1') === '1' ? 'checked' : '' ?> class="size-4 rounded border-border bg-black">
                        <span class="text-sm">Enable alliance history/backfill pipeline</span>
                    </label>
                    <label class="flex items-center gap-3 rounded-lg border border-border bg-black/20 p-3">
                        <input type="hidden" name="hub_history_pipeline_enabled" value="0">
                        <input type="checkbox" name="hub_history_pipeline_enabled" value="1" <?= ($settingValues['hub_history_pipeline_enabled'] ?? '1') === '1' ? 'checked' : '' ?> class="size-4 rounded border-border bg-black">
                        <span class="text-sm">Enable market hub history pipeline</span>
                    </label>
                </div>

                <?php if ($staticDataState !== null): ?>
                    <div class="rounded-lg border border-border bg-black/20 p-3 text-sm text-muted space-y-1">
                        <p><span class="text-slate-100">Static Data Source:</span> <?= htmlspecialchars((string) ($staticDataState['source_url'] ?? ''), ENT_QUOTES) ?></p>
                        <p><span class="text-slate-100">Remote Build:</span> <?= htmlspecialchars((string) ($staticDataState['remote_build_id'] ?? '-'), ENT_QUOTES) ?></p>
                        <p><span class="text-slate-100">Imported Build:</span> <?= htmlspecialchars((string) ($staticDataState['imported_build_id'] ?? '-'), ENT_QUOTES) ?></p>
                        <p><span class="text-slate-100">Last Status:</span> <?= htmlspecialchars((string) ($staticDataState['status'] ?? 'idle'), ENT_QUOTES) ?> (<?= htmlspecialchars((string) ($staticDataState['imported_mode'] ?? '-'), ENT_QUOTES) ?>)</p>
                    </div>
                <?php endif; ?>

                <p class="text-sm text-muted">When enabled, future import/sync jobs will only process changed rows for better scalability.</p>
                <div class="flex flex-wrap items-center gap-2">
                    <button name="data_sync_action" value="save" class="rounded-lg bg-accent px-4 py-2 text-sm font-medium">Save Data Sync Settings</button>
                    <select name="run_now_job_key" class="rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring">
                        <?php foreach ($runNowJobOptions as $jobOption): ?>
                            <option value="<?= htmlspecialchars($jobOption['job_key'], ENT_QUOTES) ?>"><?= htmlspecialchars($jobOption['label'], ENT_QUOTES) ?></option>
                        <?php endforeach; ?>
                    </select>
                    <button name="data_sync_action" value="run-now" class="rounded-lg border border-border px-4 py-2 text-sm font-medium text-slate-100 hover:bg-white/5">Run selected now</button>
                    <button name="data_sync_action" value="static-data-import" class="rounded-lg border border-border px-4 py-2 text-sm font-medium text-slate-100 hover:bg-white/5">Import EVE Static Data</button>
                </div>
            </form>
        <?php endif; ?>
    </section>
</div>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
