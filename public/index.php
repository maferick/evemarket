<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/bootstrap.php';

$title = 'Dashboard';

$dbStatus = db_connection_status();

$stats = [
    ['label' => 'Tracked Markets', 'value' => '12', 'context' => 'Regions with active pull schedules'],
    ['label' => 'Trade Stations', 'value' => (string) count(station_options()), 'context' => 'Saved market + alliance stations'],
    ['label' => 'ESI Status', 'value' => get_setting('esi_enabled', 'disabled') === '1' ? 'Connected' : 'Pending', 'context' => 'SSO configuration health'],
    ['label' => 'Incremental SQL', 'value' => get_setting('incremental_updates_enabled', '1') === '1' ? 'Enabled' : 'Disabled', 'context' => 'Future sync/import optimizer'],
];

include __DIR__ . '/../src/views/partials/header.php';
?>
<section class="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
    <?php foreach ($stats as $card): ?>
        <article class="rounded-xl border border-border bg-card p-5 shadow-lg shadow-black/20">
            <p class="text-xs uppercase tracking-[0.2em] text-muted"><?= htmlspecialchars($card['label'], ENT_QUOTES) ?></p>
            <p class="mt-2 text-3xl font-semibold"><?= htmlspecialchars($card['value'], ENT_QUOTES) ?></p>
            <p class="mt-2 text-sm text-muted"><?= htmlspecialchars($card['context'], ENT_QUOTES) ?></p>
        </article>
    <?php endforeach; ?>
</section>

<section class="mt-6 grid gap-4 xl:grid-cols-3">
    <article class="rounded-xl border border-border bg-card p-6 xl:col-span-2">
        <h2 class="text-lg font-medium">Operations Snapshot</h2>
        <p class="mt-2 text-sm text-muted">EveMarket is ready for sync jobs, pricing analytics, and strategy modules. Use Settings to complete configuration before enabling import automation.</p>
        <div class="mt-6 grid gap-3 md:grid-cols-2">
            <div class="rounded-lg border border-border bg-black/20 p-4">
                <p class="text-sm font-medium">Setup checklist</p>
                <ul class="mt-2 space-y-2 text-sm text-muted">
                    <li>• Configure general app behavior</li>
                    <li>• Select market + alliance stations</li>
                    <li>• Add ESI SSO credentials</li>
                    <li>• Choose incremental SQL strategy</li>
                </ul>
            </div>
            <div class="rounded-lg border border-border bg-black/20 p-4">
                <p class="text-sm font-medium">Architecture highlights</p>
                <ul class="mt-2 space-y-2 text-sm text-muted">
                    <li>• Centralized DB layer</li>
                    <li>• Shared helper/services layer</li>
                    <li>• Section-based settings modules</li>
                    <li>• Expandable sidebar navigation</li>
                </ul>
            </div>
        </div>
    </article>

    <article class="rounded-xl border border-border bg-card p-6">
        <h2 class="text-lg font-medium">Quick Access</h2>
        <div class="mt-4 space-y-2 text-sm">
            <a class="block rounded-lg border border-border px-3 py-2 hover:bg-white/5" href="/settings?section=general">General Settings</a>
            <a class="block rounded-lg border border-border px-3 py-2 hover:bg-white/5" href="/settings?section=trading-stations">Trading Stations</a>
            <a class="block rounded-lg border border-border px-3 py-2 hover:bg-white/5" href="/settings?section=esi-login">ESI Login</a>
            <a class="block rounded-lg border border-border px-3 py-2 hover:bg-white/5" href="/settings?section=data-sync">Data Sync</a>
        </div>
    </article>
</section>
<?php include __DIR__ . '/../src/views/partials/footer.php'; ?>
