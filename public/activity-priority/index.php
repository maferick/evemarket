<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Activity Priority';
$data = activity_priority_page_data();
$pageFreshness = supplycore_page_freshness_view_model((array) ($data['_freshness'] ?? []));
$summaryCards = array_values((array) ($data['summary_cards'] ?? []));
$activeDoctrines = array_values((array) ($data['active_doctrines'] ?? []));
$activeFits = array_values((array) ($data['active_fits'] ?? []));
$priorityItems = array_values((array) ($data['priority_items'] ?? []));
$movement = is_array($data['trend_movement'] ?? null) ? $data['trend_movement'] : [];
$liveRefreshConfig = supplycore_live_refresh_page_config('activity_priority');

include __DIR__ . '/../../src/views/partials/header.php';
?>
<!-- ui-section:activity-summary:start -->
<section class="grid gap-4 xl:grid-cols-4" data-ui-section="activity-summary">
    <?php foreach ($summaryCards as $card): ?>
        <article class="kpi-card">
            <div class="relative z-10 flex h-full flex-col gap-4">
                <div>
                    <p class="eyebrow"><?= htmlspecialchars((string) ($card['label'] ?? ''), ENT_QUOTES) ?></p>
                    <p class="mt-3 metric-value text-[2.3rem] leading-none text-cyan-100"><?= htmlspecialchars((string) ($card['value'] ?? '0'), ENT_QUOTES) ?></p>
                </div>
                <p class="text-sm text-slate-300"><?= htmlspecialchars((string) ($card['context'] ?? ''), ENT_QUOTES) ?></p>
            </div>
        </article>
    <?php endforeach; ?>
</section>
<!-- ui-section:activity-summary:end -->

<!-- ui-section:activity-doctrines:start -->
<section class="mt-8 grid gap-5 xl:grid-cols-[minmax(0,1.35fr)_minmax(320px,0.95fr)]" data-ui-section="activity-doctrines">
    <article class="surface-primary">
        <div class="section-header border-b border-white/8 pb-4">
            <div>
                <p class="eyebrow">Doctrine activity</p>
                <h2 class="mt-2 section-title">Active Doctrines</h2>
                <p class="mt-2 section-copy">Doctrine groups rank from deterministic hull-loss pressure, module-loss pressure, recency, repeated usage, and current readiness drag.</p>
            </div>
            <span class="badge border-cyan-400/20 bg-cyan-500/10 text-cyan-100"><?= htmlspecialchars((string) count($activeDoctrines), ENT_QUOTES) ?> tracked groups</span>
        </div>

        <div class="mt-5 space-y-4">
            <?php foreach ($activeDoctrines as $row): ?>
                <?php $tone = activity_priority_level_tone((string) ($row['activity_level'] ?? 'low')); ?>
                <article class="surface-tertiary">
                    <div class="flex flex-wrap items-start justify-between gap-3">
                        <div class="min-w-0 flex-1">
                            <div class="flex flex-wrap items-center gap-2">
                                <h3 class="truncate text-lg font-semibold text-white"><?= htmlspecialchars((string) ($row['doctrine_name'] ?? ''), ENT_QUOTES) ?></h3>
                                <span class="badge <?= htmlspecialchars($tone, ENT_QUOTES) ?>"><?= htmlspecialchars(ucfirst((string) ($row['activity_level'] ?? 'low')), ENT_QUOTES) ?></span>
                                <span class="badge border-white/10 bg-white/5 text-slate-200">#<?= (int) ($row['rank_position'] ?? 0) ?></span>
                                <span class="badge border-white/10 bg-white/5 text-slate-300"><?= htmlspecialchars((string) ($row['movement_label'] ?? 'Holding'), ENT_QUOTES) ?></span>
                            </div>
                            <p class="mt-2 text-sm text-slate-300"><?= htmlspecialchars((string) ($row['explanation'] ?? ''), ENT_QUOTES) ?></p>
                        </div>
                        <div class="text-right">
                            <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Activity score</p>
                            <p class="mt-2 text-2xl font-semibold text-white"><?= htmlspecialchars(number_format((float) ($row['activity_score'] ?? 0.0), 1), ENT_QUOTES) ?></p>
                            <p class="mt-1 text-xs text-slate-500">Δ <?= htmlspecialchars(number_format((float) ($row['score_delta'] ?? 0.0), 1), ENT_QUOTES) ?> · rank <?= (int) ($row['rank_delta'] ?? 0) >= 0 ? '+' : '' ?><?= (int) ($row['rank_delta'] ?? 0) ?></p>
                        </div>
                    </div>

                    <div class="mt-4 grid gap-3 md:grid-cols-4">
                        <div class="rounded-2xl border border-white/8 bg-slate-950/50 px-3 py-3">
                            <p class="text-xs uppercase tracking-[0.14em] text-slate-500">Hull losses</p>
                            <p class="mt-2 text-lg font-semibold text-slate-50"><?= doctrine_format_quantity((int) ($row['hull_losses_24h'] ?? 0)) ?> / <?= doctrine_format_quantity((int) ($row['hull_losses_3d'] ?? 0)) ?> / <?= doctrine_format_quantity((int) ($row['hull_losses_7d'] ?? 0)) ?></p>
                            <p class="mt-1 text-xs text-slate-500">24h · 3d · 7d</p>
                        </div>
                        <div class="rounded-2xl border border-white/8 bg-slate-950/50 px-3 py-3">
                            <p class="text-xs uppercase tracking-[0.14em] text-slate-500">Module losses</p>
                            <p class="mt-2 text-lg font-semibold text-slate-50"><?= doctrine_format_quantity((int) ($row['module_losses_24h'] ?? 0)) ?> / <?= doctrine_format_quantity((int) ($row['module_losses_3d'] ?? 0)) ?> / <?= doctrine_format_quantity((int) ($row['module_losses_7d'] ?? 0)) ?></p>
                            <p class="mt-1 text-xs text-slate-500">Doctrine-linked modules in tracked losses</p>
                        </div>
                        <div class="rounded-2xl border border-white/8 bg-slate-950/50 px-3 py-3">
                            <p class="text-xs uppercase tracking-[0.14em] text-slate-500">Fit-equivalent pressure</p>
                            <p class="mt-2 text-lg font-semibold text-slate-50"><?= htmlspecialchars(number_format((float) ($row['fit_equivalent_losses_24h'] ?? 0.0), 1), ENT_QUOTES) ?> / <?= htmlspecialchars(number_format((float) ($row['fit_equivalent_losses_7d'] ?? 0.0), 1), ENT_QUOTES) ?></p>
                            <p class="mt-1 text-xs text-slate-500">24h / 7d fit-equivalent module losses</p>
                        </div>
                        <div class="rounded-2xl border border-white/8 bg-slate-950/50 px-3 py-3">
                            <p class="text-xs uppercase tracking-[0.14em] text-slate-500">Readiness state</p>
                            <p class="mt-2 text-lg font-semibold text-slate-50"><?= htmlspecialchars((string) ($row['readiness_label'] ?? 'Market ready'), ENT_QUOTES) ?></p>
                            <p class="mt-1 text-xs text-slate-500"><?= htmlspecialchars((string) ($row['resupply_pressure'] ?? 'Stable'), ENT_QUOTES) ?> · <?= doctrine_format_quantity((int) ($row['readiness_gap_count'] ?? 0)) ?> fit gaps</p>
                        </div>
                    </div>

                    <div class="mt-4 grid gap-3 lg:grid-cols-[minmax(0,1fr)_minmax(280px,0.9fr)]">
                        <div class="rounded-2xl border border-white/8 bg-slate-950/50 px-3 py-3">
                            <p class="text-xs uppercase tracking-[0.14em] text-slate-500">Explainable score components</p>
                            <div class="mt-3 grid gap-2 sm:grid-cols-2 xl:grid-cols-3">
                                <?php foreach ((array) ($row['score_components'] ?? []) as $label => $value): ?>
                                    <div class="rounded-xl border border-white/8 bg-black/20 px-3 py-2">
                                        <p class="text-[11px] uppercase tracking-[0.12em] text-slate-500"><?= htmlspecialchars(str_replace('_', ' ', (string) $label), ENT_QUOTES) ?></p>
                                        <p class="mt-1 text-sm font-semibold text-slate-100"><?= htmlspecialchars(number_format((float) $value, 1), ENT_QUOTES) ?></p>
                                    </div>
                                <?php endforeach; ?>
                            </div>
                        </div>
                        <div class="rounded-2xl border border-white/8 bg-slate-950/50 px-3 py-3">
                            <p class="text-xs uppercase tracking-[0.14em] text-slate-500">Most active fits in this group</p>
                            <div class="mt-3 space-y-2">
                                <?php foreach ((array) ($row['top_fits'] ?? []) as $fit): ?>
                                    <div class="intelligence-row">
                                        <div class="min-w-0 flex-1">
                                            <p class="truncate text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) ($fit['fit_name'] ?? ''), ENT_QUOTES) ?></p>
                                            <p class="mt-1 text-xs text-slate-500"><?= htmlspecialchars(ucfirst((string) ($fit['activity_level'] ?? 'low')), ENT_QUOTES) ?></p>
                                        </div>
                                        <p class="text-sm font-semibold text-cyan-100"><?= htmlspecialchars(number_format((float) ($fit['activity_score'] ?? 0.0), 1), ENT_QUOTES) ?></p>
                                    </div>
                                <?php endforeach; ?>
                            </div>
                        </div>
                    </div>
                </article>
            <?php endforeach; ?>
            <?php if ($activeDoctrines === []): ?>
                <div class="surface-tertiary text-sm text-slate-400">No doctrine activity snapshot has been materialized yet. Wait for the next background recompute.</div>
            <?php endif; ?>
        </div>
    </article>

    <!-- ui-section:activity-sidebar:start -->
    <div class="space-y-5" data-ui-section="activity-sidebar">
        <article class="surface-primary">
            <div class="section-header border-b border-white/8 pb-4">
                <div>
                    <p class="eyebrow">Active fits</p>
                    <h2 class="mt-2 section-title">Highest-pressure Fits</h2>
                    <p class="mt-2 section-copy">Fit-level doctrine activity remains visible so operators can separate a hot doctrine group from the exact fit variant being burned.</p>
                </div>
            </div>
            <div class="mt-4 space-y-3">
                <?php foreach ($activeFits as $fit): ?>
                    <a href="/doctrine/fit?fit_id=<?= (int) ($fit['entity_id'] ?? 0) ?>" class="intelligence-row group">
                        <div class="min-w-0 flex-1">
                            <p class="truncate text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) ($fit['doctrine_name'] ?? ''), ENT_QUOTES) ?></p>
                            <p class="mt-1 text-xs text-slate-500"><?= htmlspecialchars((string) ($fit['readiness_label'] ?? 'Market ready'), ENT_QUOTES) ?> · <?= htmlspecialchars((string) ($fit['resupply_pressure'] ?? 'Stable'), ENT_QUOTES) ?></p>
                        </div>
                        <div class="text-right">
                            <p class="text-sm font-semibold text-cyan-100"><?= htmlspecialchars(number_format((float) ($fit['activity_score'] ?? 0.0), 1), ENT_QUOTES) ?></p>
                            <p class="text-xs text-slate-500"><?= htmlspecialchars((string) ($fit['movement_label'] ?? 'Holding'), ENT_QUOTES) ?></p>
                        </div>
                    </a>
                <?php endforeach; ?>
                <?php if ($activeFits === []): ?>
                    <div class="surface-tertiary text-sm text-slate-400">Fit-level activity data will appear after the next materialized refresh.</div>
                <?php endif; ?>
            </div>
        </article>

        <article class="surface-primary">
            <div class="section-header border-b border-white/8 pb-4">
                <div>
                    <p class="eyebrow">Trend / movement</p>
                    <h2 class="mt-2 section-title">What Changed Recently</h2>
                    <p class="mt-2 section-copy">Movement compares the current materialized ranking against the prior stored snapshot, while each row still shows its 24h / 7d loss windows.</p>
                </div>
            </div>
            <div class="mt-4 space-y-4">
                <?php $trendPanels = [
                    'Doctrines moving up' => $movement['doctrines_moving_up'] ?? [],
                    'Items newly elevated' => $movement['items_newly_elevated'] ?? [],
                    'Items cooling down' => $movement['items_cooling_down'] ?? [],
                ]; ?>
                <?php foreach ($trendPanels as $label => $rows): ?>
                    <div class="rounded-2xl border border-white/8 bg-slate-950/50 px-4 py-4">
                        <div class="flex items-center justify-between gap-3">
                            <p class="text-sm font-semibold text-slate-100"><?= htmlspecialchars($label, ENT_QUOTES) ?></p>
                            <span class="badge border-white/10 bg-white/5 text-slate-300"><?= htmlspecialchars((string) count($rows), ENT_QUOTES) ?></span>
                        </div>
                        <div class="mt-3 space-y-2">
                            <?php foreach (array_slice((array) $rows, 0, 4) as $row): ?>
                                <div class="intelligence-row">
                                    <div class="min-w-0 flex-1">
                                        <p class="truncate text-sm font-medium text-slate-100"><?= htmlspecialchars((string) (($row['doctrine_name'] ?? $row['item_name']) ?? ''), ENT_QUOTES) ?></p>
                                        <p class="mt-1 text-xs text-slate-500"><?= htmlspecialchars((string) ($row['movement_label'] ?? 'Holding'), ENT_QUOTES) ?> · Δ <?= htmlspecialchars(number_format((float) ($row['score_delta'] ?? 0.0), 1), ENT_QUOTES) ?></p>
                                    </div>
                                    <p class="text-sm font-semibold text-slate-200"><?= htmlspecialchars((string) ($row['rank_delta'] ?? 0) >= 0 ? '+' : '', ENT_QUOTES) ?><?= (int) ($row['rank_delta'] ?? 0) ?></p>
                                </div>
                            <?php endforeach; ?>
                            <?php if ((array) $rows === []): ?>
                                <p class="text-sm text-slate-500">No material movement recorded yet.</p>
                            <?php endif; ?>
                        </div>
                    </div>
                <?php endforeach; ?>
            </div>
        </article>
    </div>
    <!-- ui-section:activity-sidebar:end -->
</section>
<!-- ui-section:activity-doctrines:end -->

<!-- ui-section:activity-items:start -->
<section class="mt-8 surface-primary" data-ui-section="activity-items">
    <div class="section-header border-b border-white/8 pb-4">
        <div>
            <p class="eyebrow">Priority-shifting items</p>
            <h2 class="mt-2 section-title">Doctrine-first Restock Board</h2>
            <p class="mt-2 section-copy">Doctrine-linked items stay pinned above comparable enabled-item signals whenever doctrine activity, tracked losses, and local stock pressure line up.</p>
        </div>
        <span class="badge border-orange-400/20 bg-orange-500/10 text-orange-100"><?= htmlspecialchars((string) count($priorityItems), ENT_QUOTES) ?> ranked items</span>
    </div>

    <div class="mt-5 space-y-4">
        <?php foreach ($priorityItems as $row): ?>
            <?php $tone = activity_priority_level_tone((string) ($row['priority_band'] ?? 'watch')); ?>
            <article class="surface-tertiary">
                <div class="flex flex-wrap items-start justify-between gap-3">
                    <div class="min-w-0 flex-1">
                        <div class="flex flex-wrap items-center gap-2">
                            <h3 class="truncate text-lg font-semibold text-white"><?= htmlspecialchars((string) ($row['item_name'] ?? ''), ENT_QUOTES) ?></h3>
                            <span class="badge <?= htmlspecialchars($tone, ENT_QUOTES) ?>"><?= htmlspecialchars(ucfirst((string) ($row['priority_band'] ?? 'watch')), ENT_QUOTES) ?></span>
                            <?php if (!empty($row['is_doctrine_linked'])): ?>
                                <span class="badge border-cyan-400/20 bg-cyan-500/10 text-cyan-100">Doctrine-linked</span>
                            <?php endif; ?>
                            <span class="badge border-white/10 bg-white/5 text-slate-300">#<?= (int) ($row['rank_position'] ?? 0) ?></span>
                        </div>
                        <p class="mt-2 text-sm text-slate-300"><?= htmlspecialchars((string) ($row['explanation'] ?? ''), ENT_QUOTES) ?></p>
                    </div>
                    <div class="text-right">
                        <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Priority score</p>
                        <p class="mt-2 text-2xl font-semibold text-white"><?= htmlspecialchars(number_format((float) ($row['priority_score'] ?? 0.0), 1), ENT_QUOTES) ?></p>
                        <p class="mt-1 text-xs text-slate-500">Δ <?= htmlspecialchars(number_format((float) ($row['score_delta'] ?? 0.0), 1), ENT_QUOTES) ?> · rank <?= (int) ($row['rank_delta'] ?? 0) >= 0 ? '+' : '' ?><?= (int) ($row['rank_delta'] ?? 0) ?></p>
                    </div>
                </div>

                <div class="mt-4 grid gap-3 md:grid-cols-5">
                    <div class="rounded-2xl border border-white/8 bg-slate-950/50 px-3 py-3">
                        <p class="text-xs uppercase tracking-[0.14em] text-slate-500">Linked doctrines</p>
                        <p class="mt-2 text-lg font-semibold text-slate-50"><?= doctrine_format_quantity((int) ($row['linked_doctrine_count'] ?? 0)) ?></p>
                        <p class="mt-1 text-xs text-slate-500"><?= doctrine_format_quantity((int) ($row['linked_active_doctrine_count'] ?? 0)) ?> active now</p>
                    </div>
                    <div class="rounded-2xl border border-white/8 bg-slate-950/50 px-3 py-3">
                        <p class="text-xs uppercase tracking-[0.14em] text-slate-500">Recent losses</p>
                        <p class="mt-2 text-lg font-semibold text-slate-50"><?= doctrine_format_quantity((int) ($row['recent_loss_qty_24h'] ?? 0)) ?> / <?= doctrine_format_quantity((int) ($row['recent_loss_qty_7d'] ?? 0)) ?></p>
                        <p class="mt-1 text-xs text-slate-500">24h / 7d tracked victim-side quantity</p>
                    </div>
                    <div class="rounded-2xl border border-white/8 bg-slate-950/50 px-3 py-3">
                        <p class="text-xs uppercase tracking-[0.14em] text-slate-500">Local stock</p>
                        <p class="mt-2 text-lg font-semibold text-slate-50"><?= doctrine_format_quantity((int) ($row['local_sell_volume'] ?? 0)) ?></p>
                        <p class="mt-1 text-xs text-slate-500"><?= doctrine_format_quantity((int) ($row['local_sell_orders'] ?? 0)) ?> sell orders</p>
                    </div>
                    <div class="rounded-2xl border border-white/8 bg-slate-950/50 px-3 py-3">
                        <p class="text-xs uppercase tracking-[0.14em] text-slate-500">Readiness impact</p>
                        <p class="mt-2 text-lg font-semibold text-slate-50"><?= doctrine_format_quantity((int) ($row['readiness_gap_fit_count'] ?? 0)) ?></p>
                        <p class="mt-1 text-xs text-slate-500">fits with open target gaps</p>
                    </div>
                    <div class="rounded-2xl border border-white/8 bg-slate-950/50 px-3 py-3">
                        <p class="text-xs uppercase tracking-[0.14em] text-slate-500">Bottleneck status</p>
                        <p class="mt-2 text-lg font-semibold text-slate-50"><?= doctrine_format_quantity((int) ($row['bottleneck_fit_count'] ?? 0)) ?></p>
                        <p class="mt-1 text-xs text-slate-500"><?= htmlspecialchars(ucfirst((string) ($row['depletion_state'] ?? 'stable')), ENT_QUOTES) ?> depletion</p>
                    </div>
                </div>

                <div class="mt-4 grid gap-3 lg:grid-cols-[minmax(0,1fr)_minmax(320px,0.9fr)]">
                    <div class="rounded-2xl border border-white/8 bg-slate-950/50 px-3 py-3">
                        <p class="text-xs uppercase tracking-[0.14em] text-slate-500">Explainable score components</p>
                        <div class="mt-3 grid gap-2 sm:grid-cols-2 xl:grid-cols-3">
                            <?php foreach ((array) ($row['score_components'] ?? []) as $label => $value): ?>
                                <div class="rounded-xl border border-white/8 bg-black/20 px-3 py-2">
                                    <p class="text-[11px] uppercase tracking-[0.12em] text-slate-500"><?= htmlspecialchars(str_replace('_', ' ', (string) $label), ENT_QUOTES) ?></p>
                                    <p class="mt-1 text-sm font-semibold text-slate-100"><?= htmlspecialchars(number_format((float) $value, 1), ENT_QUOTES) ?></p>
                                </div>
                            <?php endforeach; ?>
                        </div>
                    </div>
                    <div class="rounded-2xl border border-white/8 bg-slate-950/50 px-3 py-3">
                        <p class="text-xs uppercase tracking-[0.14em] text-slate-500">Linked doctrine groups</p>
                        <div class="mt-3 flex flex-wrap gap-2">
                            <?php foreach ((array) ($row['linked_doctrine_names'] ?? []) as $name): ?>
                                <span class="badge border-white/10 bg-white/5 text-slate-200"><?= htmlspecialchars((string) $name, ENT_QUOTES) ?></span>
                            <?php endforeach; ?>
                            <?php if ((array) ($row['linked_doctrine_names'] ?? []) === []): ?>
                                <span class="text-sm text-slate-500">No doctrine link recorded.</span>
                            <?php endif; ?>
                        </div>
                    </div>
                </div>
            </article>
        <?php endforeach; ?>
        <?php if ($priorityItems === []): ?>
            <div class="surface-tertiary text-sm text-slate-400">No item-priority snapshot has been materialized yet.</div>
        <?php endif; ?>
    </div>
</section>
<!-- ui-section:activity-items:end -->

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
