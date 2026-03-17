<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/bootstrap.php';

$title = 'Dashboard';
$dbStatus = db_connection_status();
$intel = dashboard_intelligence_data();

include __DIR__ . '/../src/views/partials/header.php';
?>
<section class="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
    <?php foreach (($intel['kpis'] ?? []) as $card): ?>
        <article class="rounded-xl border border-border bg-card p-5 shadow-lg shadow-black/20">
            <p class="text-xs uppercase tracking-[0.2em] text-muted"><?= htmlspecialchars((string) ($card['label'] ?? ''), ENT_QUOTES) ?></p>
            <p class="mt-2 text-3xl font-semibold"><?= htmlspecialchars((string) ($card['value'] ?? '—'), ENT_QUOTES) ?></p>
            <p class="mt-2 text-sm text-muted"><?= htmlspecialchars((string) ($card['context'] ?? ''), ENT_QUOTES) ?></p>
        </article>
    <?php endforeach; ?>
</section>

<section class="mt-6 grid gap-4 xl:grid-cols-2">
    <article class="rounded-xl border border-border bg-card p-6">
        <div class="flex items-center justify-between gap-3">
            <h2 class="text-lg font-medium">Top Opportunity Queue</h2>
            <span class="text-xs text-muted">Prioritize restock + repricing</span>
        </div>
        <?php $opportunities = $intel['priority_queues']['opportunities'] ?? []; ?>
        <?php if ($opportunities === []): ?>
            <p class="mt-4 rounded-lg border border-dashed border-border bg-black/20 p-4 text-sm text-muted">No opportunity signals yet. Run alliance and market sync jobs to generate overlap and pricing insights.</p>
        <?php else: ?>
            <div class="mt-4 space-y-2">
                <?php foreach ($opportunities as $row): ?>
                    <div class="rounded-lg border border-border bg-black/20 px-4 py-3">
                        <p class="text-sm font-medium"><?= htmlspecialchars((string) ($row['module'] ?? ''), ENT_QUOTES) ?></p>
                        <p class="mt-1 text-xs text-muted"><?= htmlspecialchars((string) ($row['signal'] ?? ''), ENT_QUOTES) ?></p>
                        <p class="mt-2 text-xs uppercase tracking-wide text-muted">Score: <?= htmlspecialchars((string) ($row['score'] ?? '0'), ENT_QUOTES) ?></p>
                    </div>
                <?php endforeach; ?>
            </div>
        <?php endif; ?>
    </article>

    <article class="rounded-xl border border-border bg-card p-6">
        <div class="flex items-center justify-between gap-3">
            <h2 class="text-lg font-medium">Top Risk Queue</h2>
            <span class="text-xs text-muted">Price + liquidity warnings</span>
        </div>
        <?php $risks = $intel['priority_queues']['risks'] ?? []; ?>
        <?php if ($risks === []): ?>
            <p class="mt-4 rounded-lg border border-dashed border-border bg-black/20 p-4 text-sm text-muted">No risk alerts detected. Sync more data to continuously monitor weak stock and deviation risk.</p>
        <?php else: ?>
            <div class="mt-4 space-y-2">
                <?php foreach ($risks as $row): ?>
                    <div class="rounded-lg border border-border bg-black/20 px-4 py-3">
                        <p class="text-sm font-medium"><?= htmlspecialchars((string) ($row['module'] ?? ''), ENT_QUOTES) ?></p>
                        <p class="mt-1 text-xs text-muted"><?= htmlspecialchars((string) ($row['signal'] ?? ''), ENT_QUOTES) ?></p>
                        <p class="mt-2 text-xs uppercase tracking-wide text-muted">Score: <?= htmlspecialchars((string) ($row['score'] ?? '0'), ENT_QUOTES) ?></p>
                    </div>
                <?php endforeach; ?>
            </div>
        <?php endif; ?>
    </article>
</section>

<section class="mt-6 grid gap-4 xl:grid-cols-3">
    <?php
    $healthPanels = [
        'Alliance Market Freshness' => $intel['health_panels']['alliance_freshness'] ?? [],
        'Sync Health' => $intel['health_panels']['sync_health'] ?? [],
        'Data Completeness' => $intel['health_panels']['data_completeness'] ?? [],
    ];
    ?>
    <?php foreach ($healthPanels as $titleText => $panel): ?>
        <article class="rounded-xl border border-border bg-card p-5">
            <h3 class="text-base font-medium"><?= htmlspecialchars($titleText, ENT_QUOTES) ?></h3>
            <p class="mt-2 text-sm text-muted">Status: <?= htmlspecialchars((string) ($panel['status'] ?? 'Awaiting sync'), ENT_QUOTES) ?></p>
            <div class="mt-3 space-y-1 text-xs text-muted">
                <?php if (isset($panel['last_success_at'])): ?><p>Last success: <?= htmlspecialchars((string) $panel['last_success_at'], ENT_QUOTES) ?></p><?php endif; ?>
                <?php if (isset($panel['recent_rows_written'])): ?><p>Recent rows written: <?= htmlspecialchars((string) $panel['recent_rows_written'], ENT_QUOTES) ?></p><?php endif; ?>
                <?php if (isset($panel['rows_compared'])): ?><p>Compared rows: <?= htmlspecialchars((string) $panel['rows_compared'], ENT_QUOTES) ?></p><?php endif; ?>
                <?php if (isset($panel['history_points'])): ?><p>History points: <?= htmlspecialchars((string) $panel['history_points'], ENT_QUOTES) ?></p><?php endif; ?>
                <?php if (isset($panel['last_error']) && (string) $panel['last_error'] !== 'None'): ?><p class="text-amber-300">Last error: <?= htmlspecialchars((string) $panel['last_error'], ENT_QUOTES) ?></p><?php endif; ?>
            </div>
        </article>
    <?php endforeach; ?>
</section>

<section class="mt-6 rounded-xl border border-border bg-card p-6">
    <div class="flex items-center justify-between gap-3">
        <h2 class="text-lg font-medium">Trend Snippets</h2>
        <span class="text-xs text-muted">Short-period movement from daily history</span>
    </div>
    <?php $snippets = $intel['trend_snippets'] ?? []; ?>
    <?php if ($snippets === []): ?>
        <p class="mt-4 rounded-lg border border-dashed border-border bg-black/20 p-4 text-sm text-muted">Trend snippets will appear after market history is synced. Enable hub history sync in Settings → Data Sync and run at least two daily snapshots.</p>
    <?php else: ?>
        <div class="mt-4 grid gap-3 md:grid-cols-2 xl:grid-cols-3">
            <?php foreach ($snippets as $snippet): ?>
                <article class="rounded-lg border border-border bg-black/20 p-4">
                    <p class="text-sm font-medium"><?= htmlspecialchars((string) ($snippet['module'] ?? ''), ENT_QUOTES) ?></p>
                    <p class="mt-2 text-lg font-semibold"><?= htmlspecialchars((string) ($snippet['movement'] ?? '0.0%'), ENT_QUOTES) ?></p>
                    <p class="text-xs text-muted"><?= htmlspecialchars((string) ($snippet['direction'] ?? 'Flat'), ENT_QUOTES) ?> · Close <?= htmlspecialchars((string) ($snippet['latest_close'] ?? '—'), ENT_QUOTES) ?></p>
                    <p class="mt-1 text-xs text-muted">Volume: <?= htmlspecialchars((string) ($snippet['latest_volume'] ?? '0'), ENT_QUOTES) ?></p>
                </article>
            <?php endforeach; ?>
        </div>
    <?php endif; ?>
</section>

<section class="mt-6 rounded-xl border border-border bg-card p-4 text-sm text-muted">
    <p>Database: <?= htmlspecialchars($dbStatus['connected'] ? 'Connected' : 'Unavailable', ENT_QUOTES) ?><?php if (!$dbStatus['connected'] && isset($dbStatus['error'])): ?> · <?= htmlspecialchars((string) $dbStatus['error'], ENT_QUOTES) ?><?php endif; ?></p>
</section>
<?php include __DIR__ . '/../src/views/partials/footer.php'; ?>
