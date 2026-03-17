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
                'app_timezone' => trim($_POST['app_timezone'] ?? 'UTC'),
                'default_currency' => trim($_POST['default_currency'] ?? 'ISK'),
            ]);
            break;

        case 'trading-stations':
            $saved = save_settings([
                'market_station_id' => (string) ($_POST['market_station_id'] ?? ''),
                'alliance_station_id' => (string) ($_POST['alliance_station_id'] ?? ''),
            ]);
            break;

        case 'esi-login':
            $saved = save_settings([
                'esi_client_id' => trim($_POST['esi_client_id'] ?? ''),
                'esi_callback_url' => trim($_POST['esi_callback_url'] ?? ''),
                'esi_enabled' => isset($_POST['esi_enabled']) ? '1' : '0',
            ]);
            break;

        case 'data-sync':
            $saved = save_settings([
                'incremental_updates_enabled' => isset($_POST['incremental_updates_enabled']) ? '1' : '0',
            ]);
            break;
    }

    flash('success', $saved ? 'Settings saved successfully.' : 'Database unavailable. Settings were not persisted.');
    header('Location: /settings?section=' . urlencode($submittedSection));
    exit;
}

$settingValues = get_settings([
    'app_timezone',
    'default_currency',
    'market_station_id',
    'alliance_station_id',
    'esi_client_id',
    'esi_callback_url',
    'esi_enabled',
    'incremental_updates_enabled',
]);

$stations = grouped_station_options();

$dbStatus = db_connection_status();

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
                    <span class="text-sm text-muted">Timezone</span>
                    <input name="app_timezone" value="<?= htmlspecialchars($settingValues['app_timezone'] ?? 'UTC', ENT_QUOTES) ?>" class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring" />
                </label>
                <label class="block space-y-2">
                    <span class="text-sm text-muted">Default Currency</span>
                    <input name="default_currency" value="<?= htmlspecialchars($settingValues['default_currency'] ?? 'ISK', ENT_QUOTES) ?>" class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring" />
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
                <label class="block space-y-2">
                    <span class="text-sm text-muted">Alliance Station Selection</span>
                    <select name="alliance_station_id" class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring">
                        <option value="">Select an alliance station</option>
                        <?php foreach ($stations['alliance'] as $station): ?>
                            <option value="<?= $station['id'] ?>" <?= ($settingValues['alliance_station_id'] ?? '') === (string) $station['id'] ? 'selected' : '' ?>>
                                <?= htmlspecialchars($station['station_name'], ENT_QUOTES) ?>
                            </option>
                        <?php endforeach; ?>
                    </select>
                </label>
                <button class="rounded-lg bg-accent px-4 py-2 text-sm font-medium">Save Trading Stations</button>
            </form>
        <?php elseif ($section === 'esi-login'): ?>
            <form class="mt-6 space-y-4" method="post">
                <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                <input type="hidden" name="section" value="esi-login">
                <label class="block space-y-2">
                    <span class="text-sm text-muted">ESI Client ID</span>
                    <input name="esi_client_id" value="<?= htmlspecialchars($settingValues['esi_client_id'] ?? '', ENT_QUOTES) ?>" class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring" />
                </label>
                <label class="block space-y-2">
                    <span class="text-sm text-muted">Callback URL</span>
                    <input name="esi_callback_url" value="<?= htmlspecialchars($settingValues['esi_callback_url'] ?? base_url('/auth/esi/callback'), ENT_QUOTES) ?>" class="w-full rounded-lg border border-border bg-black/30 px-3 py-2 text-sm outline-none ring-accent focus:ring" />
                </label>
                <label class="flex items-center gap-3 rounded-lg border border-border bg-black/20 p-3">
                    <input type="checkbox" name="esi_enabled" value="1" <?= ($settingValues['esi_enabled'] ?? '0') === '1' ? 'checked' : '' ?> class="size-4 rounded border-border bg-black">
                    <span class="text-sm">Enable ESI OAuth login</span>
                </label>
                <button class="rounded-lg bg-accent px-4 py-2 text-sm font-medium">Save ESI Login Settings</button>
            </form>
        <?php else: ?>
            <form class="mt-6 space-y-4" method="post">
                <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                <input type="hidden" name="section" value="data-sync">
                <label class="flex items-center gap-3 rounded-lg border border-border bg-black/20 p-3">
                    <input type="checkbox" name="incremental_updates_enabled" value="1" <?= ($settingValues['incremental_updates_enabled'] ?? '1') === '1' ? 'checked' : '' ?> class="size-4 rounded border-border bg-black">
                    <span class="text-sm">Enable incremental SQL database updates</span>
                </label>
                <p class="text-sm text-muted">When enabled, future import/sync jobs will only process changed rows for better scalability.</p>
                <button class="rounded-lg bg-accent px-4 py-2 text-sm font-medium">Save Data Sync Settings</button>
            </form>
        <?php endif; ?>
    </section>
</div>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
