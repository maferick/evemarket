<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Doctrine Groups';
$data = doctrine_groups_overview_data();
$summary = $data['summary'] ?? [];
$groups = $data['groups'] ?? [];
$notReadyFits = $data['not_ready_fits'] ?? [];
$topMissingItems = $data['top_missing_items'] ?? [];
$ungroupedFits = $data['ungrouped_fits'] ?? [];

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    if (!validate_csrf($_POST['_token'] ?? null)) {
        http_response_code(419);
        exit('Invalid CSRF token');
    }

    $groupName = doctrine_sanitize_group_name((string) ($_POST['group_name'] ?? ''));
    $description = doctrine_sanitize_description($_POST['description'] ?? null);

    try {
        $groupId = db_doctrine_group_create($groupName, $description);
        supplycore_cache_bust(['doctrine', 'dashboard']);
        flash('success', 'Doctrine group saved successfully.');
        header('Location: /doctrine/group?group_id=' . $groupId);
        exit;
    } catch (Throwable $exception) {
        flash('success', 'Doctrine group save failed: ' . $exception->getMessage());
        header('Location: /doctrine');
        exit;
    }
}

include __DIR__ . '/../../src/views/partials/header.php';
?>
<section class="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
    <?php foreach ($summary as $card): ?>
        <article class="surface-secondary">
            <p class="eyebrow"><?= htmlspecialchars((string) ($card['label'] ?? ''), ENT_QUOTES) ?></p>
            <p class="mt-3 text-3xl metric-value"><?= htmlspecialchars((string) ($card['value'] ?? ''), ENT_QUOTES) ?></p>
            <p class="mt-2 text-sm text-slate-300"><?= htmlspecialchars((string) ($card['context'] ?? ''), ENT_QUOTES) ?></p>
        </article>
    <?php endforeach; ?>
</section>

<section class="mt-8 grid gap-6 xl:grid-cols-[minmax(0,1.35fr)_minmax(320px,0.9fr)]">
    <article class="surface-secondary">
        <div class="section-header">
            <div>
                <p class="eyebrow">Doctrine registry</p>
                <h2 class="mt-2 section-title">Alliance doctrine groups</h2>
                <p class="mt-2 text-sm text-slate-400">Every card now exposes complete-fit availability, target stock, readiness trend, and loss-aware doctrine pressure before you even open the fit.</p>
            </div>
            <a href="/doctrine/import" class="btn-primary">Import fit</a>
        </div>

        <?php if ($groups === []): ?>
            <div class="surface-tertiary text-sm text-slate-400">No doctrine groups yet. Create one, then import an EFT or BuyAll payload.</div>
        <?php else: ?>
            <div class="space-y-4">
                <?php foreach ($groups as $group): ?>
                    <a href="/doctrine/group?group_id=<?= (int) ($group['id'] ?? 0) ?>" class="block rounded-[1.3rem] border border-white/8 bg-white/[0.03] p-4 transition hover:bg-white/[0.05]">
                        <div class="flex flex-wrap items-start justify-between gap-4">
                            <div>
                                <div class="flex flex-wrap items-center gap-3">
                                    <h3 class="text-lg font-semibold text-white"><?= htmlspecialchars((string) ($group['group_name'] ?? ''), ENT_QUOTES) ?></h3>
                                    <span class="badge <?= htmlspecialchars(doctrine_supply_status_tone((string) ($group['status'] ?? 'critical')), ENT_QUOTES) ?>"><?= htmlspecialchars((string) ($group['status_label'] ?? 'Gap active'), ENT_QUOTES) ?></span>
                                </div>
                                <p class="mt-2 max-w-2xl text-sm text-slate-400"><?= htmlspecialchars((string) ($group['description'] ?? 'Doctrine fit collection for SupplyCore workflows.'), ENT_QUOTES) ?></p>
                            </div>
                            <span class="badge border-cyan-400/18 bg-cyan-500/10 text-cyan-100"><?= doctrine_format_quantity((int) ($group['fit_count'] ?? 0)) ?> fits</span>
                        </div>
                        <div class="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
                            <div class="surface-tertiary">
                                <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Complete fits</p>
                                <p class="mt-2 text-xl font-semibold text-slate-100"><?= doctrine_format_quantity((int) ($group['complete_fits_available'] ?? 0)) ?></p>
                            </div>
                            <div class="surface-tertiary">
                                <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Target fits</p>
                                <p class="mt-2 text-xl font-semibold text-slate-100"><?= doctrine_format_quantity((int) ($group['target_fit_count'] ?? 0)) ?></p>
                            </div>
                            <div class="surface-tertiary">
                                <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Fit gap</p>
                                <p class="mt-2 text-xl font-semibold text-rose-200"><?= doctrine_format_quantity((int) ($group['fit_gap_count'] ?? 0)) ?></p>
                            </div>
                            <div class="surface-tertiary">
                                <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Trend</p>
                                <p class="mt-2 text-xl font-semibold text-slate-100"><?= htmlspecialchars((string) ($group['readiness_trend'] ?? 'Stable'), ENT_QUOTES) ?></p>
                                <p class="mt-1 text-xs text-slate-500"><?= doctrine_format_quantity((int) ($group['trending_down_fit_count'] ?? 0)) ?> fits trending down</p>
                            </div>
                            <div class="surface-tertiary">
                                <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Loss pressure</p>
                                <p class="mt-2 text-sm font-semibold text-sky-100"><?= htmlspecialchars(market_format_isk((float) ($group['restock_gap_isk'] ?? 0.0)), ENT_QUOTES) ?></p>
                                <p class="mt-1 text-xs text-slate-500"><?= doctrine_format_quantity((int) ($group['loss_pressure_fit_count'] ?? 0)) ?> fits not yet sufficient vs recent tracked losses</p>
                            </div>
                        </div>
                    </a>
                <?php endforeach; ?>
            </div>
        <?php endif; ?>
    </article>

    <aside class="space-y-6">
        <article class="surface-secondary">
            <div class="section-header">
                <div>
                    <p class="eyebrow">New group</p>
                    <h2 class="mt-2 section-title">Create doctrine group</h2>
                </div>
                <span class="badge border-sky-400/18 bg-sky-500/10 text-sky-100">CRUD ready</span>
            </div>
            <form method="post" class="space-y-4">
                <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                <label class="block">
                    <span class="mb-2 block field-label">Group name</span>
                    <input type="text" name="group_name" class="field-input" maxlength="190" placeholder="e.g. Muninn Mainline">
                </label>
                <label class="block">
                    <span class="mb-2 block field-label">Description</span>
                    <textarea name="description" class="field-input" style="min-height: 8rem;" placeholder="What this doctrine group covers, when it is used, or who owns the restock workflow."></textarea>
                </label>
                <button type="submit" class="btn-primary w-full justify-center">Save group</button>
            </form>
        </article>

        <article class="surface-secondary">
            <div class="section-header">
                <div>
                    <p class="eyebrow">Operational queue</p>
                    <h2 class="mt-2 section-title">Top doctrine shortages</h2>
                </div>
                <span class="badge border-orange-400/20 bg-orange-500/10 text-orange-100">Restock first</span>
            </div>
            <div class="space-y-3">
                <?php if ($topMissingItems === []): ?>
                    <div class="surface-tertiary text-sm text-slate-400">No doctrine shortages are currently tracked.</div>
                <?php else: ?>
                    <?php foreach ($topMissingItems as $row): ?>
                        <div class="surface-tertiary">
                            <div class="flex items-start justify-between gap-3">
                                <div>
                                    <p class="font-semibold text-slate-100"><?= htmlspecialchars((string) ($row['item_name'] ?? ''), ENT_QUOTES) ?></p>
                                    <p class="mt-1 text-xs text-slate-500"><?= doctrine_format_quantity((int) ($row['fit_count'] ?? 0)) ?> doctrine fits impacted</p>
                                </div>
                                <div class="text-right">
                                    <p class="text-sm font-semibold text-orange-100"><?= doctrine_format_quantity((int) ($row['missing_qty'] ?? 0)) ?></p>
                                    <p class="text-xs text-slate-500">missing units</p>
                                </div>
                            </div>
                            <p class="mt-2 text-xs text-sky-200"><?= htmlspecialchars(market_format_isk((float) ($row['restock_gap_isk'] ?? 0.0)), ENT_QUOTES) ?> estimated hub spend</p>
                        </div>
                    <?php endforeach; ?>
                <?php endif; ?>
            </div>
        </article>
    </aside>
</section>

<section class="mt-8 grid gap-6 xl:grid-cols-2">
    <article class="surface-secondary">
        <div class="section-header">
                <div>
                    <p class="eyebrow">Readiness exceptions</p>
                <h2 class="mt-2 section-title">Fits that need doctrine attention</h2>
                <p class="mt-2 text-sm text-slate-400">These fits are below target, trending down, or already under pressure from recent tracked losses.</p>
                </div>
                <span class="badge border-rose-400/20 bg-rose-500/10 text-rose-200"><?= doctrine_format_quantity(count($notReadyFits)) ?> active</span>
            </div>
        <div class="space-y-3">
            <?php if ($notReadyFits === []): ?>
                <div class="surface-tertiary text-sm text-slate-400">All tracked doctrine fits are currently market-ready.</div>
            <?php else: ?>
                <?php foreach (array_slice($notReadyFits, 0, 8) as $fit): ?>
                    <a href="/doctrine/fit?fit_id=<?= (int) ($fit['id'] ?? 0) ?>" class="intelligence-row group">
                        <div class="min-w-0 flex-1">
                            <p class="truncate text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) ($fit['fit_name'] ?? ''), ENT_QUOTES) ?></p>
                            <p class="mt-1 text-xs text-slate-500"><?= htmlspecialchars((string) implode(', ', (array) ($fit['group_names'] ?? [])), ENT_QUOTES) ?></p>
                        </div>
                        <div class="text-right">
                            <p class="text-sm font-semibold text-rose-200"><?= doctrine_format_quantity((int) (($fit['supply']['complete_fits_available'] ?? 0))) ?> / <?= doctrine_format_quantity((int) (($fit['supply']['recommended_target_fit_count'] ?? 0))) ?></p>
                            <p class="text-xs text-slate-500"><?= htmlspecialchars((string) (($fit['supply']['status_label'] ?? 'Watch closely')), ENT_QUOTES) ?></p>
                        </div>
                        <div class="text-slate-500 transition group-hover:text-slate-200">›</div>
                    </a>
                <?php endforeach; ?>
            <?php endif; ?>
        </div>
    </article>

    <article class="surface-secondary">
        <div class="section-header">
            <div>
                <p class="eyebrow">Ungrouped cleanup</p>
                <h2 class="mt-2 section-title">Fits without group membership</h2>
                <p class="mt-2 text-sm text-slate-400">Deleting a doctrine group removes only the membership links. Any fit left without another group stays intact and appears here until reassigned.</p>
            </div>
            <span class="badge border-amber-400/20 bg-amber-500/10 text-amber-100"><?= doctrine_format_quantity(count($ungroupedFits)) ?> ungrouped</span>
        </div>
        <div class="space-y-3">
            <?php if ($ungroupedFits === []): ?>
                <div class="surface-tertiary text-sm text-slate-400">No orphaned doctrine fits need cleanup.</div>
            <?php else: ?>
                <?php foreach ($ungroupedFits as $fit): ?>
                    <a href="/doctrine/fit?fit_id=<?= (int) ($fit['id'] ?? 0) ?>" class="intelligence-row group">
                        <div class="min-w-0 flex-1">
                            <p class="truncate text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) ($fit['fit_name'] ?? ''), ENT_QUOTES) ?></p>
                            <p class="mt-1 text-xs text-slate-500"><?= htmlspecialchars((string) ($fit['ship_name'] ?? ''), ENT_QUOTES) ?></p>
                        </div>
                        <div class="text-slate-500 transition group-hover:text-slate-200">Reassign ›</div>
                    </a>
                <?php endforeach; ?>
            <?php endif; ?>
        </div>
    </article>
</section>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
