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
            $saved = save_settings(data_sync_settings_from_request($_POST));
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
    'alliance_current_sync_interval_minutes',
    'alliance_history_sync_interval_minutes',
    'hub_history_sync_interval_minutes',
    'alliance_current_pipeline_enabled',
    'alliance_history_pipeline_enabled',
    'hub_history_pipeline_enabled',
    'alliance_current_backfill_start_date',
    'alliance_history_backfill_start_date',
    'hub_history_backfill_start_date',
    'raw_order_snapshot_retention_days',
]);

$stations = grouped_station_options();

$dbStatus = db_connection_status();
$latestEsiToken = null;
$requiredStructureScopes = esi_required_market_structure_scopes();
$missingStructureScopes = [];
$syncStatusCards = [];
if ($dbStatus['ok']) {
    $latestEsiToken = db_latest_esi_oauth_token();
    if ($latestEsiToken !== null) {
        $missingStructureScopes = esi_missing_scopes($latestEsiToken, $requiredStructureScopes);
    }

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
                <label class="block space-y-2">
                    <span class="text-sm text-muted">Market Station Selection</span>
                    <select name="market_station_id" class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring">
                        <option value="">Select a market station</option>
                        <?php foreach ($stations['market'] as $station): ?>
                            <option value="<?= $station['id'] ?>" <?= ($settingValues['market_station_id'] ?? '') === (string) $station['id'] ? 'selected' : '' ?>>
                                <?= htmlspecialchars($station['station_name'], ENT_QUOTES) ?>
                            </option>
                        <?php endforeach; ?>
                    </select>
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
                    const input = document.getElementById('alliance_structure_search');
                    const hidden = document.getElementById('alliance_station_id');
                    const results = document.getElementById('alliance_structure_results');
                    const status = document.getElementById('alliance_structure_status');

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
                            status.textContent = 'No matching structures found.';
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
                                status.textContent = 'Selected structure: ' + item.name + ' (#' + item.id + ').';
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

                        if (query.length < 2) {
                            status.textContent = 'Type at least 2 characters to search alliance structures.';
                            clearResults();
                            return;
                        }

                        debounceTimer = window.setTimeout(async () => {
                            status.textContent = 'Searching…';

                            try {
                                const response = await fetch('/settings/esi-structures.php?q=' + encodeURIComponent(query), {
                                    headers: { 'Accept': 'application/json' },
                                });

                                const payload = await response.json();
                                if (!response.ok) {
                                    status.textContent = payload.error || 'Lookup failed.';
                                    clearResults();
                                    return;
                                }

                                status.textContent = 'Select a structure from the list.';
                                renderResults(payload.results || []);
                            } catch (_error) {
                                status.textContent = 'Unable to query ESI structures right now.';
                                clearResults();
                            }
                        }, 250);
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
                    <span class="text-sm">Enable incremental SQL database updates</span>
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

                <div class="grid gap-4 md:grid-cols-3">
                    <label class="block space-y-2">
                        <span class="text-sm text-muted">Alliance Current Interval (minutes)</span>
                        <input type="number" min="1" max="1440" step="1" name="alliance_current_sync_interval_minutes" value="<?= htmlspecialchars($settingValues['alliance_current_sync_interval_minutes'] ?? '5', ENT_QUOTES) ?>" class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring" />
                    </label>
                    <label class="block space-y-2">
                        <span class="text-sm text-muted">Alliance History Interval (minutes)</span>
                        <input type="number" min="1" max="1440" step="1" name="alliance_history_sync_interval_minutes" value="<?= htmlspecialchars($settingValues['alliance_history_sync_interval_minutes'] ?? '60', ENT_QUOTES) ?>" class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring" />
                    </label>
                    <label class="block space-y-2">
                        <span class="text-sm text-muted">Hub History Interval (minutes)</span>
                        <input type="number" min="1" max="1440" step="1" name="hub_history_sync_interval_minutes" value="<?= htmlspecialchars($settingValues['hub_history_sync_interval_minutes'] ?? '15', ENT_QUOTES) ?>" class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring" />
                    </label>
                </div>


                <div class="grid gap-4 md:grid-cols-3">
                    <label class="block space-y-2">
                        <span class="text-sm text-muted">Alliance Current Backfill Start</span>
                        <input type="date" name="alliance_current_backfill_start_date" value="<?= htmlspecialchars($settingValues['alliance_current_backfill_start_date'] ?? '', ENT_QUOTES) ?>" class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring" />
                    </label>
                    <label class="block space-y-2">
                        <span class="text-sm text-muted">Alliance History Backfill Start</span>
                        <input type="date" name="alliance_history_backfill_start_date" value="<?= htmlspecialchars($settingValues['alliance_history_backfill_start_date'] ?? '', ENT_QUOTES) ?>" class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring" />
                    </label>
                    <label class="block space-y-2">
                        <span class="text-sm text-muted">Hub History Backfill Start</span>
                        <input type="date" name="hub_history_backfill_start_date" value="<?= htmlspecialchars($settingValues['hub_history_backfill_start_date'] ?? '', ENT_QUOTES) ?>" class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring" />
                    </label>
                </div>

                <label class="block space-y-2">
                    <span class="text-sm text-muted">Raw Order Snapshot Retention (days)</span>
                    <input type="number" min="1" max="3650" step="1" name="raw_order_snapshot_retention_days" value="<?= htmlspecialchars($settingValues['raw_order_snapshot_retention_days'] ?? '30', ENT_QUOTES) ?>" class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring" />
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

                <p class="text-sm text-muted">When enabled, future import/sync jobs will only process changed rows for better scalability.</p>
                <button class="rounded-lg bg-accent px-4 py-2 text-sm font-medium">Save Data Sync Settings</button>
            </form>
        <?php endif; ?>
    </section>
</div>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
