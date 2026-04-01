<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Doctrine Groups';
$data = doctrine_groups_overview_data();
$summary = $data['summary'] ?? [];
$groups = $data['groups'] ?? [];
$notReadyFits = $data['not_ready_fits'] ?? [];
$topMissingItems = $data['top_missing_items'] ?? [];
$topBottlenecks = $data['top_bottlenecks'] ?? [];
$highestPriorityRestockItems = $data['highest_priority_restock_items'] ?? [];
$ungroupedFits = $data['ungrouped_fits'] ?? [];
$pageFreshness = supplycore_page_freshness_view_model((array) ($data['freshness'] ?? []));
$liveRefreshConfig = supplycore_live_refresh_page_config('doctrine_index');
$liveRefreshSummary = supplycore_live_refresh_summary($liveRefreshConfig);
$pageHeaderBadge = 'Doctrine readiness';
$pageHeaderSummary = 'Keep doctrine pages centered on fieldable hulls, blockers, missing items, and support dependencies.';
$pageHeaderMeta = [
    [
        'label' => 'Last updated',
        'value' => $pageFreshness['computed_relative'],
        'caption' => $pageFreshness['computed_at'],
    ],
    [
        'label' => 'Live updates',
        'value' => $liveRefreshSummary['mode_label'],
        'caption' => $liveRefreshSummary['health_message'],
    ],
];

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    if (!validate_csrf($_POST['_token'] ?? null)) {
        http_response_code(419);
        exit('Invalid CSRF token');
    }

    $groupName = doctrine_sanitize_group_name((string) ($_POST['group_name'] ?? ''));
    $description = doctrine_sanitize_description($_POST['description'] ?? null);

    try {
        $groupId = db_doctrine_group_create($groupName, $description);
        doctrine_schedule_intelligence_refresh('group-create');
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
<!-- ui-section:doctrine-index-main:start -->
<section class="grid gap-4 sm:grid-cols-2 xl:grid-cols-4" data-ui-section="doctrine-index-main">
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
                <p class="mt-2 text-sm text-slate-400">See what can field now, what is blocked, and where support stock is limiting fleet readiness.</p>
            </div>
            <div class="flex flex-wrap gap-3">
                <a href="/doctrine/fits" class="btn-secondary">Fit overview</a>
                <a href="/doctrine/import" class="btn-primary">Bulk import</a>
                <button type="button" class="btn-secondary" onclick="document.getElementById('doctrine-create-group').toggleAttribute('open')">+ New group</button>
            </div>
        </div>

        <details id="doctrine-create-group" class="mt-4 rounded-[1.4rem] border border-cyan-400/20 bg-cyan-500/5 p-4">
            <summary class="cursor-pointer list-none text-sm font-semibold text-cyan-100">Create new doctrine group</summary>
            <form method="post" class="mt-4 grid gap-4 md:grid-cols-[1fr_1fr_auto]">
                <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                <label class="block">
                    <span class="mb-2 block field-label">Group name</span>
                    <input type="text" name="group_name" class="field-input" maxlength="190" placeholder="e.g. Muninn Mainline">
                </label>
                <label class="block">
                    <span class="mb-2 block field-label">Description</span>
                    <input type="text" name="description" class="field-input" placeholder="What this doctrine group covers">
                </label>
                <div class="flex items-end">
                    <button type="submit" class="btn-primary">Create group</button>
                </div>
            </form>
        </details>

        <?php if ($groups === []): ?>
            <div class="surface-tertiary text-sm text-slate-400">No doctrine groups yet. Create one above, then import Winter Coalition HTML pages or EFT fallback payloads in bulk.</div>
        <?php else: ?>
            <div class="space-y-4">
                <?php foreach ($groups as $group): ?>
                    <a href="/doctrine/group?group_id=<?= (int) ($group['id'] ?? 0) ?>" class="block rounded-[1.3rem] border border-white/8 bg-white/[0.03] p-4 transition hover:bg-white/[0.05]">
                        <div class="flex flex-wrap items-start justify-between gap-4">
                            <div>
                                <div class="flex flex-wrap items-center gap-3">
                                    <h3 class="text-lg font-semibold text-white"><?= htmlspecialchars((string) ($group['group_name'] ?? ''), ENT_QUOTES) ?></h3>
                                    <span class="badge <?= htmlspecialchars(doctrine_supply_status_tone((string) ($group['status'] ?? 'market_ready')), ENT_QUOTES) ?>"><?= htmlspecialchars((string) ($group['status_label'] ?? 'Market ready'), ENT_QUOTES) ?></span>
                                    <span class="badge <?= htmlspecialchars(doctrine_resupply_pressure_tone((string) ($group['pressure_state'] ?? 'stable')), ENT_QUOTES) ?>"><?= htmlspecialchars((string) ($group['pressure_label'] ?? 'Stable'), ENT_QUOTES) ?></span>
                                </div>
                                <p class="mt-2 max-w-2xl text-sm text-slate-400"><?= htmlspecialchars((string) ($group['description'] ?? 'Doctrine fit collection for SupplyCore workflows.'), ENT_QUOTES) ?></p>
                            </div>
                            <span class="badge border-cyan-400/18 bg-cyan-500/10 text-cyan-100"><?= doctrine_format_quantity((int) ($group['fit_count'] ?? 0)) ?> primary fits</span>
                        </div>
                        <?php
                        $readyCount = (int) ($group['complete_fits_available'] ?? 0);
                        $targetCount = max(1, (int) ($group['target_fit_count'] ?? 1));
                        $readinessPercent = min(100, round(($readyCount / $targetCount) * 100, 1));
                        $readinessColor = $readinessPercent >= 90 ? 'bg-emerald-400/70' : ($readinessPercent >= 70 ? 'bg-amber-400/70' : 'bg-rose-400/70');
                        $readinessText = $readinessPercent >= 90 ? 'text-emerald-300' : ($readinessPercent >= 70 ? 'text-amber-300' : 'text-rose-300');
                        ?>
                        <div class="mt-3 flex items-center gap-3">
                            <div class="flex-1 h-2 rounded-full bg-white/8 overflow-hidden">
                                <div class="h-full rounded-full transition-all <?= $readinessColor ?>" style="width: <?= max(1, $readinessPercent) ?>%"></div>
                            </div>
                            <span class="text-sm font-semibold tabular-nums <?= $readinessText ?>"><?= $readinessPercent ?>%</span>
                        </div>
                        <div class="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
                            <div class="surface-tertiary">
                                <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Fleet ready</p>
                                <p class="mt-2 text-xl font-semibold text-slate-100"><?= doctrine_format_quantity((int) ($group['complete_fits_available'] ?? 0)) ?></p>
                            </div>
                            <div class="surface-tertiary">
                                <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Target fleet</p>
                                <p class="mt-2 text-xl font-semibold text-slate-100"><?= doctrine_format_quantity((int) ($group['target_fit_count'] ?? 0)) ?></p>
                            </div>
                            <div class="surface-tertiary">
                                <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Fleet gap</p>
                                <p class="mt-2 text-xl font-semibold text-rose-200"><?= doctrine_format_quantity((int) ($group['fit_gap_count'] ?? 0)) ?></p>
                            </div>
                            <div class="surface-tertiary">
                                <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Trend</p>
                                <p class="mt-2 text-xl font-semibold text-slate-100"><?= htmlspecialchars((string) ($group['readiness_trend'] ?? 'Stable'), ENT_QUOTES) ?></p>
                                <p class="mt-1 text-xs text-slate-500"><?= doctrine_format_quantity((int) ($group['trending_down_fit_count'] ?? 0)) ?> fits trending down</p>
                            </div>
                            <div class="surface-tertiary">
                                <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Resupply pressure</p>
                                <p class="mt-2 text-sm font-semibold text-sky-100"><?= htmlspecialchars((string) ($group['pressure_label'] ?? 'Stable'), ENT_QUOTES) ?></p>
                                <p class="mt-1 text-xs text-slate-500"><?= doctrine_format_quantity((int) ($group['pressure_fit_count'] ?? 0)) ?> primary fits show elevated-or-worse replenishment pressure · <?= doctrine_format_quantity((int) ($group['support_fit_count'] ?? 0)) ?> support fits attached</p>
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

        <article class="surface-secondary">
            <div class="section-header">
                <div>
                    <p class="eyebrow">Bottleneck queue</p>
                    <h2 class="mt-2 section-title">Top doctrine bottlenecks</h2>
                </div>
                <span class="badge border-rose-400/20 bg-rose-500/10 text-rose-200">Critical path</span>
            </div>
            <div class="space-y-3">
                <?php if ($topBottlenecks === []): ?>
                    <div class="surface-tertiary text-sm text-slate-400">No current doctrine bottlenecks were detected.</div>
                <?php else: ?>
                    <?php foreach ($topBottlenecks as $row): ?>
                        <div class="surface-tertiary">
                            <div class="flex items-start justify-between gap-3">
                                <div>
                                    <p class="font-semibold text-slate-100"><?= htmlspecialchars((string) ($row['type_name'] ?? ''), ENT_QUOTES) ?></p>
                                    <p class="mt-1 text-xs text-slate-500"><?= doctrine_format_quantity((int) ($row['doctrine_fit_count'] ?? 0)) ?> doctrines depend on this item · <?= doctrine_format_quantity((int) ($row['bottleneck_ship_impact'] ?? 0)) ?> ships blocked<?= !empty($row['is_external_bottleneck']) ? ' · External bottleneck' : '' ?></p>
                                </div>
                                <div class="text-right">
                                    <p class="text-sm font-semibold text-rose-100"><?= htmlspecialchars((string) ($row['priority_score'] ?? 0), ENT_QUOTES) ?></p>
                                    <p class="text-xs text-slate-500">priority</p>
                                </div>
                            </div>
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
                <p class="mt-2 text-sm text-slate-400">These fits still have readiness gaps right now, even if replenishment pressure is tracked separately.</p>
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
                            <p class="mt-1 text-xs text-slate-500"><?= htmlspecialchars((string) implode(', ', (array) ($fit['group_names'] ?? [])), ENT_QUOTES) ?><?= !empty($fit['supply']['externally_managed']) ? ' · Externally managed hull' : '' ?></p>
                        </div>
                        <div class="text-right">
                            <p class="text-sm font-semibold text-rose-200"><?= doctrine_format_quantity((int) (($fit['supply']['complete_fits_available'] ?? 0))) ?> / <?= doctrine_format_quantity((int) (($fit['supply']['recommended_target_fit_count'] ?? 0))) ?></p>
                            <p class="text-xs text-slate-500"><?= htmlspecialchars((string) (($fit['supply']['combined_status_label'] ?? (($fit['supply']['readiness_label'] ?? 'Market ready') . ' · ' . ($fit['supply']['resupply_pressure_label'] ?? 'Stable')))), ENT_QUOTES) ?></p>
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
                    <p class="eyebrow">Support dependencies</p>
                    <h2 class="mt-2 section-title">Highest priority support items</h2>
                    <p class="mt-2 text-sm text-slate-400">Keep shared support stock visible without letting it crowd out doctrine-critical blockers.</p>
                </div>
            <span class="badge border-sky-400/18 bg-sky-500/10 text-sky-100">Support watchlist</span>
        </div>
        <div class="space-y-3">
            <?php if ($highestPriorityRestockItems === []): ?>
                <div class="surface-tertiary text-sm text-slate-400">No enabled-item restock opportunities are currently ranked.</div>
            <?php else: ?>
                <?php foreach (array_slice($highestPriorityRestockItems, 0, 6) as $row): ?>
                    <div class="intelligence-row">
                        <div class="min-w-0 flex-1">
                            <p class="truncate text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) ($row['type_name'] ?? ''), ENT_QUOTES) ?></p>
                            <p class="mt-1 text-xs text-slate-500"><?= !empty($row['is_doctrine_item']) ? 'Doctrine-linked' : 'Enabled item' ?> · depletion <?= htmlspecialchars((string) ($row['depletion_state'] ?? 'stable'), ENT_QUOTES) ?></p>
                        </div>
                        <div class="text-right">
                            <p class="text-sm font-semibold text-sky-100"><?= htmlspecialchars((string) ($row['priority_score'] ?? 0), ENT_QUOTES) ?></p>
                            <p class="text-xs text-slate-500">priority</p>
                        </div>
                    </div>
                <?php endforeach; ?>
            <?php endif; ?>
        </div>
    </article>

    <details class="surface-secondary">
        <summary class="flex cursor-pointer list-none items-center justify-between gap-3">
            <div>
                <p class="eyebrow">Doctrine admin</p>
                <h2 class="mt-2 section-title">Advanced doctrine setup</h2>
                <p class="mt-2 text-sm text-slate-400">Create groups and clean up unmapped fits without putting admin tasks in the main readiness view.</p>
            </div>
            <span class="badge border-amber-400/20 bg-amber-500/10 text-amber-100"><?= doctrine_format_quantity(count($ungroupedFits)) ?> cleanup items</span>
        </summary>
        <div class="mt-6 grid gap-6 xl:grid-cols-2">
            <article class="surface-tertiary">
                <div class="section-header">
                    <div>
                        <p class="eyebrow">New group</p>
                        <h3 class="mt-2 text-lg font-semibold text-white">Create doctrine group</h3>
                        <p class="mt-2 text-sm text-slate-400">Use the <button type="button" class="text-cyan-200 underline" onclick="document.getElementById('doctrine-create-group').setAttribute('open','');window.scrollTo({top:0,behavior:'smooth'})">+ New group</button> button at the top of the page.</p>
                    </div>
                </div>
            </article>
            <article class="surface-tertiary">
                <div class="section-header">
                    <div>
                        <p class="eyebrow">Unowned cleanup</p>
                        <h3 class="mt-2 text-lg font-semibold text-white">Unmapped fits</h3>
                    </div>
                </div>
                <div class="space-y-3">
                    <?php if ($ungroupedFits === []): ?>
                        <div class="surface-tertiary text-sm text-slate-400">No excluded fits need ownership cleanup.</div>
                    <?php else: ?>
                        <?php foreach ($ungroupedFits as $fit): ?>
                            <a href="/doctrine/fit?fit_id=<?= (int) ($fit['id'] ?? 0) ?>" class="intelligence-row group">
                                <div class="min-w-0 flex-1">
                                    <p class="truncate text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) ($fit['fit_name'] ?? ''), ENT_QUOTES) ?></p>
                                    <p class="mt-1 text-xs text-slate-500"><?= htmlspecialchars((string) ($fit['ship_name'] ?? ''), ENT_QUOTES) ?> · <?= htmlspecialchars(implode(', ', array_map(static fn (array $membership): string => (string) ($membership['group_name'] ?? ''), (array) ($fit['memberships'] ?? []))) ?: 'No memberships', ENT_QUOTES) ?></p>
                                </div>
                                <div class="text-slate-500 transition group-hover:text-slate-200">Assign primary ›</div>
                            </a>
                        <?php endforeach; ?>
                    <?php endif; ?>
                </div>
            </article>
        </div>
    </details>
</section>
<!-- ui-section:doctrine-index-main:end -->
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
