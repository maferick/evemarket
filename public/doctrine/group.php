<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$groupId = max(0, (int) ($_GET['group_id'] ?? 0));
$data = doctrine_group_detail_data($groupId);
$group = $data['group'] ?? null;
$fits = $data['fits'] ?? [];
$title = $group !== null ? ((string) ($group['group_name'] ?? 'Doctrine Group')) : 'Doctrine Group';

include __DIR__ . '/../../src/views/partials/header.php';
?>
<?php if ($group === null): ?>
    <section class="surface-secondary">
        <div class="section-header">
            <div>
                <p class="eyebrow">Doctrine registry</p>
                <h2 class="mt-2 section-title">Doctrine group not found</h2>
            </div>
            <a href="/doctrine" class="btn-secondary">Back to groups</a>
        </div>
        <p class="text-sm text-slate-400">The requested doctrine group does not exist yet or the database is unavailable.</p>
    </section>
<?php else: ?>
    <section class="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <article class="surface-secondary">
            <p class="eyebrow">Doctrine fits</p>
            <p class="mt-3 text-3xl metric-value"><?= doctrine_format_quantity((int) ($group['fit_count'] ?? 0)) ?></p>
            <p class="mt-2 text-sm text-slate-300">Imported fits stored in this group.</p>
        </article>
        <article class="surface-secondary">
            <p class="eyebrow">Tracked items</p>
            <p class="mt-3 text-3xl metric-value"><?= doctrine_format_quantity((int) ($group['item_count'] ?? 0)) ?></p>
            <p class="mt-2 text-sm text-slate-300">Normalized doctrine lines ready for market mapping.</p>
        </article>
        <article class="surface-secondary">
            <p class="eyebrow">Last updated</p>
            <p class="mt-3 text-3xl metric-value"><?= htmlspecialchars(killmail_relative_datetime($group['last_fit_updated_at'] ?? null), ENT_QUOTES) ?></p>
            <p class="mt-2 text-sm text-slate-300">Most recent fit refresh inside this doctrine group.</p>
        </article>
        <article class="surface-secondary">
            <p class="eyebrow">Baseline goal</p>
            <p class="mt-3 text-3xl metric-value">Ready</p>
            <p class="mt-2 text-sm text-slate-300">Gap detection, restocks, and hauling suggestions build from these fits.</p>
        </article>
    </section>

    <section class="surface-secondary mt-8">
        <div class="section-header">
            <div>
                <p class="eyebrow">Fit inventory</p>
                <h2 class="mt-2 section-title"><?= htmlspecialchars((string) ($group['group_name'] ?? ''), ENT_QUOTES) ?></h2>
                <p class="mt-2 text-sm text-slate-400"><?= htmlspecialchars((string) ($group['description'] ?? 'Doctrine fit collection for SupplyCore.'), ENT_QUOTES) ?></p>
            </div>
            <div class="flex flex-wrap gap-3">
                <a href="/doctrine/import?group_id=<?= (int) ($group['id'] ?? 0) ?>" class="btn-primary">Import another fit</a>
                <a href="/doctrine" class="btn-secondary">All groups</a>
            </div>
        </div>

        <?php if ($fits === []): ?>
            <div class="surface-tertiary text-sm text-slate-400">No fits have been imported into this group yet.</div>
        <?php else: ?>
            <div class="grid gap-4 lg:grid-cols-2">
                <?php foreach ($fits as $fit): ?>
                    <a href="/doctrine/fit?fit_id=<?= (int) ($fit['id'] ?? 0) ?>" class="block rounded-[1.3rem] border border-white/8 bg-white/[0.03] p-4 transition hover:bg-white/[0.05]">
                        <div class="flex items-start justify-between gap-4">
                            <div class="flex items-center gap-4">
                                <div class="h-14 w-14 overflow-hidden rounded-2xl border border-white/10 bg-slate-950/70 p-2">
                                    <?php if (!empty($fit['ship_image_url'])): ?>
                                        <img src="<?= htmlspecialchars((string) $fit['ship_image_url'], ENT_QUOTES) ?>" alt="<?= htmlspecialchars((string) ($fit['ship_name'] ?? ''), ENT_QUOTES) ?>" class="h-full w-full object-contain">
                                    <?php else: ?>
                                        <div class="flex h-full w-full items-center justify-center text-xs text-slate-500">N/A</div>
                                    <?php endif; ?>
                                </div>
                                <div>
                                    <h3 class="text-lg font-semibold text-white"><?= htmlspecialchars((string) ($fit['fit_name'] ?? ''), ENT_QUOTES) ?></h3>
                                    <p class="mt-1 text-sm text-slate-400"><?= htmlspecialchars((string) ($fit['ship_name'] ?? ''), ENT_QUOTES) ?></p>
                                </div>
                            </div>
                            <span class="badge border-sky-400/18 bg-sky-500/10 text-sky-100"><?= htmlspecialchars(strtoupper((string) ($fit['source_format'] ?? 'buyall')), ENT_QUOTES) ?></span>
                        </div>
                        <div class="mt-4 grid gap-3 sm:grid-cols-3">
                            <div class="surface-tertiary">
                                <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Item lines</p>
                                <p class="mt-2 text-xl font-semibold text-slate-100"><?= doctrine_format_quantity((int) ($fit['item_count'] ?? 0)) ?></p>
                            </div>
                            <div class="surface-tertiary">
                                <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Required qty</p>
                                <p class="mt-2 text-xl font-semibold text-slate-100"><?= htmlspecialchars((string) ($fit['required_quantity_label'] ?? '0'), ENT_QUOTES) ?></p>
                            </div>
                            <div class="surface-tertiary">
                                <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Updated</p>
                                <p class="mt-2 text-sm font-medium text-slate-100"><?= htmlspecialchars(killmail_relative_datetime($fit['updated_at'] ?? null), ENT_QUOTES) ?></p>
                            </div>
                        </div>
                    </a>
                <?php endforeach; ?>
            </div>
        <?php endif; ?>
    </section>
<?php endif; ?>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
