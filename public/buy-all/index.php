<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Buy All Planner';
$planner = buy_all_planner_data($_GET);
$request = is_array($planner['request'] ?? null) ? $planner['request'] : buy_all_request();
$summary = is_array($planner['summary'] ?? null) ? $planner['summary'] : [];
$pages = array_values((array) ($planner['pages'] ?? []));
$activePage = (int) ($planner['active_page'] ?? 1);
$activePageData = is_array($planner['active_page_data'] ?? null) ? $planner['active_page_data'] : null;
$modeOptions = is_array($planner['mode_options'] ?? null) ? $planner['mode_options'] : [];
$sortOptions = is_array($planner['sort_options'] ?? null) ? $planner['sort_options'] : [];
$freshness = is_array($planner['freshness'] ?? null) ? $planner['freshness'] : [];
$priceBasis = is_array($planner['price_basis'] ?? null) ? $planner['price_basis'] : [];
$hauling = is_array($planner['hauling'] ?? null) ? $planner['hauling'] : [];
$filters = is_array($request['filters'] ?? null) ? $request['filters'] : [];
$baseQuery = $_GET;

$buildPageUrl = static function (int $pageNumber) use ($baseQuery): string {
    $params = $baseQuery;
    $params['page'] = $pageNumber;

    return '/buy-all?' . http_build_query($params);
};
$liveRefreshConfig = supplycore_live_refresh_page_config('buy_all');
$liveRefreshSummary = supplycore_live_refresh_summary($liveRefreshConfig);
$pageHeaderBadge = 'Buying plan';
$pageHeaderSummary = 'Keep this page on actionable buys: margin, hauling impact, doctrine urgency, and why each recommendation made the cut.';
$pageHeaderMeta = [
    [
        'label' => 'Plan freshness',
        'value' => (string) ($freshness['generated_relative'] ?? 'Unknown'),
        'caption' => (string) ($freshness['generated_at'] ?? 'Unavailable'),
    ],
    [
        'label' => 'Live updates',
        'value' => $liveRefreshSummary['mode_label'],
        'caption' => $liveRefreshSummary['health_message'],
    ],
];

include __DIR__ . '/../../src/views/partials/header.php';
?>
<!-- ui-section:buyall-overview:start -->
<section class="grid gap-5 xl:grid-cols-[minmax(0,1.45fr)_minmax(320px,0.9fr)]" data-ui-section="buyall-overview">
    <article class="surface-primary">
        <div class="section-header border-b border-white/8 pb-4">
            <div>
                <p class="eyebrow">Procurement workflow</p>
                <h1 class="mt-2 section-title">Buy All Planner</h1>
                <p class="mt-2 section-copy">Turn shortages and price spreads into a buy run you can execute immediately.</p>
            </div>
            <span class="badge border-cyan-400/20 bg-cyan-500/10 text-cyan-100"><?= htmlspecialchars((string) ($summary['mode_label'] ?? 'Blended'), ENT_QUOTES) ?></span>
        </div>

        <div class="mt-5 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <div class="rounded-[1.2rem] border border-white/8 bg-slate-950/50 px-4 py-4">
                <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Generated pages</p>
                <p class="mt-3 text-3xl font-semibold text-white"><?= htmlspecialchars((string) ($summary['page_count'] ?? 0), ENT_QUOTES) ?></p>
                <p class="mt-1 text-sm text-slate-400"><?= htmlspecialchars(number_format((float) ($hauling['page_volume_limit'] ?? 350000.0), 0), ENT_QUOTES) ?> m³ cap per page.</p>
            </div>
            <div class="rounded-[1.2rem] border border-white/8 bg-slate-950/50 px-4 py-4">
                <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Planned volume</p>
                <p class="mt-3 text-3xl font-semibold text-white"><?= htmlspecialchars(number_format((float) ($summary['total_volume'] ?? 0.0), 0), ENT_QUOTES) ?> m³</p>
                <p class="mt-1 text-sm text-slate-400"><?= htmlspecialchars((string) ($summary['total_item_types'] ?? 0), ENT_QUOTES) ?> ranked item types across <?= htmlspecialchars((string) ($summary['total_units'] ?? 0), ENT_QUOTES) ?> total units.</p>
            </div>
            <div class="rounded-[1.2rem] border border-white/8 bg-slate-950/50 px-4 py-4">
                <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Expected net profit</p>
                <p class="mt-3 text-3xl font-semibold <?= (float) ($summary['total_net_profit'] ?? 0.0) >= 0.0 ? 'text-emerald-100' : 'text-rose-200' ?>"><?= htmlspecialchars(market_format_isk((float) ($summary['total_net_profit'] ?? 0.0)), ENT_QUOTES) ?></p>
                <p class="mt-1 text-sm text-slate-400">After <?= htmlspecialchars(number_format((float) ($hauling['cost_per_m3'] ?? 250.0), 2), ENT_QUOTES) ?> ISK/m³ hauling.</p>
            </div>
            <div class="rounded-[1.2rem] border border-white/8 bg-slate-950/50 px-4 py-4">
                <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Doctrine-critical</p>
                <p class="mt-3 text-3xl font-semibold text-orange-100"><?= htmlspecialchars((string) ($summary['doctrine_critical_count'] ?? 0), ENT_QUOTES) ?></p>
                <p class="mt-1 text-sm text-slate-400">Top theme: <?= htmlspecialchars((string) ($summary['top_reason_theme'] ?? 'Mixed signals'), ENT_QUOTES) ?></p>
            </div>
        </div>

        <form method="get" class="mt-5 space-y-4 rounded-[1.4rem] border border-white/8 bg-white/[0.03] p-4">
            <div class="grid gap-4 lg:grid-cols-[minmax(0,1.2fr)_minmax(0,0.8fr)]">
                <div>
                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Modes</p>
                    <div class="mt-3 grid gap-3 md:grid-cols-2 xl:grid-cols-3">
                        <?php foreach ($modeOptions as $modeKey => $mode): ?>
                            <label class="rounded-[1.1rem] border <?= $request['mode'] === $modeKey ? 'border-cyan-400/30 bg-cyan-500/10 text-cyan-50' : 'border-white/8 bg-slate-950/40 text-slate-200' ?> p-3 cursor-pointer">
                                <input type="radio" name="mode" value="<?= htmlspecialchars((string) $modeKey, ENT_QUOTES) ?>" class="sr-only" <?= $request['mode'] === $modeKey ? 'checked' : '' ?>>
                                <span class="block text-sm font-semibold"><?= htmlspecialchars((string) ($mode['label'] ?? $modeKey), ENT_QUOTES) ?></span>
                                <span class="mt-1 block text-xs text-slate-400"><?= htmlspecialchars((string) ($mode['description'] ?? ''), ENT_QUOTES) ?></span>
                            </label>
                        <?php endforeach; ?>
                    </div>
                </div>
                <div>
                    <label class="block text-xs uppercase tracking-[0.16em] text-slate-500" for="sort">Sort</label>
                    <select id="sort" name="sort" class="field-input mt-3 w-full">
                        <?php foreach ($sortOptions as $sortKey => $sortLabel): ?>
                            <option value="<?= htmlspecialchars((string) $sortKey, ENT_QUOTES) ?>" <?= $request['sort'] === $sortKey ? 'selected' : '' ?>><?= htmlspecialchars((string) $sortLabel, ENT_QUOTES) ?></option>
                        <?php endforeach; ?>
                    </select>

                    <div class="mt-4 grid gap-3 sm:grid-cols-2">
                        <label class="flex items-center gap-2 text-sm text-slate-300"><input type="checkbox" name="doctrine_linked_only" value="1" <?= !empty($filters['doctrine_linked_only']) ? 'checked' : '' ?>> Doctrine-linked only</label>
                        <label class="flex items-center gap-2 text-sm text-slate-300"><input type="checkbox" name="positive_net_margin_only" value="1" <?= !empty($filters['positive_net_margin_only']) ? 'checked' : '' ?>> Positive net margin only</label>
                        <label class="flex items-center gap-2 text-sm text-slate-300"><input type="checkbox" name="allow_low_margin_doctrine_critical" value="1" <?= !empty($filters['allow_low_margin_doctrine_critical']) ? 'checked' : '' ?>> Allow low/negative margin if doctrine critical</label>
                        <label class="flex items-center gap-2 text-sm text-slate-300"><input type="checkbox" name="exclude_incomplete_pricing" value="1" <?= !empty($filters['exclude_incomplete_pricing']) ? 'checked' : '' ?>> Exclude incomplete pricing</label>
                        <label class="flex items-center gap-2 text-sm text-slate-300"><input type="checkbox" name="exclude_oversized_low_efficiency" value="1" <?= !empty($filters['exclude_oversized_low_efficiency']) ? 'checked' : '' ?>> Exclude oversized low-efficiency items</label>
                    </div>
                </div>
            </div>

            <div class="grid gap-4 md:grid-cols-3">
                <label class="block text-sm text-slate-300">Minimum priority threshold
                    <input type="number" step="1" min="0" max="100" name="min_priority_threshold" value="<?= htmlspecialchars((string) ($filters['min_priority_threshold'] ?? 0), ENT_QUOTES) ?>" class="field-input mt-2 w-full">
                </label>
                <label class="block text-sm text-slate-300">Minimum net margin %
                    <input type="number" step="0.1" name="min_net_margin_threshold" value="<?= htmlspecialchars((string) ($filters['min_net_margin_threshold'] ?? 0), ENT_QUOTES) ?>" class="field-input mt-2 w-full">
                </label>
                <label class="block text-sm text-slate-300">Minimum net profit total
                    <input type="number" step="100000" name="min_net_profit_threshold" value="<?= htmlspecialchars((string) ($filters['min_net_profit_threshold'] ?? 0), ENT_QUOTES) ?>" class="field-input mt-2 w-full">
                </label>
            </div>

            <div class="flex flex-wrap items-center gap-3">
                <input type="hidden" name="page" value="1">
                <button type="submit" class="btn-primary">Rebuild plan</button>
                <a href="/buy-all?mode=blended&page=1" class="btn-secondary">Reset to smart defaults</a>
            </div>
        </form>
    </article>

    <div class="space-y-5">
        <article class="surface-secondary">
            <div class="section-header border-b border-white/8 pb-4">
                <div>
                    <p class="eyebrow">Freshness / trust</p>
                    <h2 class="mt-2 section-title">Planner inputs</h2>
                    <p class="mt-2 section-copy">Pricing, stock, and doctrine inputs behind the current recommendation.</p>
                </div>
            </div>
            <div class="mt-4 space-y-3">
                <?php foreach (['hub_pricing' => 'Hub pricing', 'alliance_pricing' => 'Alliance pricing', 'stock' => 'Stock', 'doctrine' => 'Doctrine'] as $key => $label): ?>
                    <?php $card = is_array($freshness[$key] ?? null) ? $freshness[$key] : []; ?>
                    <div class="rounded-[1.1rem] border border-white/8 bg-slate-950/50 px-4 py-3">
                        <div class="flex items-center justify-between gap-3">
                            <p class="text-sm font-semibold text-slate-100"><?= htmlspecialchars($label, ENT_QUOTES) ?></p>
                            <span class="badge <?= htmlspecialchars((string) ($card['tone'] ?? 'border-amber-400/20 bg-amber-500/10 text-amber-100'), ENT_QUOTES) ?>"><?= htmlspecialchars((string) ($card['label'] ?? 'Unknown'), ENT_QUOTES) ?></span>
                        </div>
                        <p class="mt-2 text-sm text-slate-300"><?= htmlspecialchars((string) ($card['computed_relative'] ?? 'Unknown'), ENT_QUOTES) ?></p>
                        <p class="mt-1 text-xs text-slate-500"><?= htmlspecialchars((string) ($card['computed_at'] ?? 'Unavailable'), ENT_QUOTES) ?></p>
                    </div>
                <?php endforeach; ?>
                <div class="rounded-[1.1rem] border border-white/8 bg-slate-950/50 px-4 py-3">
                    <p class="text-sm font-semibold text-slate-100">Generated</p>
                    <p class="mt-2 text-sm text-slate-300"><?= htmlspecialchars((string) ($freshness['generated_relative'] ?? 'Unknown'), ENT_QUOTES) ?></p>
                    <p class="mt-1 text-xs text-slate-500"><?= htmlspecialchars((string) ($freshness['generated_at'] ?? 'Unavailable'), ENT_QUOTES) ?></p>
                </div>
            </div>
        </article>

        <article class="surface-secondary">
            <div class="section-header border-b border-white/8 pb-4">
                <div>
                    <p class="eyebrow">Price basis</p>
                    <h2 class="mt-2 section-title">Profit assumptions</h2>
                </div>
            </div>
            <div class="mt-4 space-y-3 text-sm text-slate-300">
                <p><?= htmlspecialchars((string) ($priceBasis['buy'] ?? ''), ENT_QUOTES) ?></p>
                <p><?= htmlspecialchars((string) ($priceBasis['sell'] ?? ''), ENT_QUOTES) ?></p>
                <p>Hauling cost is always <span class="font-semibold text-white"><?= htmlspecialchars(number_format((float) ($hauling['cost_per_m3'] ?? 250.0), 2), ENT_QUOTES) ?> ISK per m³</span> and is baked into ranking through net unit spread, net list profit, margin, and efficiency.</p>
            </div>
        </article>
    </div>
</section>
<!-- ui-section:buyall-overview:end -->

<!-- ui-section:buyall-results:start -->
<section class="mt-8 grid gap-5 xl:grid-cols-[minmax(0,1.4fr)_minmax(320px,0.9fr)]" data-ui-section="buyall-results">
    <article class="surface-primary">
        <div class="section-header border-b border-white/8 pb-4">
            <div>
                <p class="eyebrow">Buy-all pages</p>
                <h2 class="mt-2 section-title">Page <?= htmlspecialchars((string) $activePage, ENT_QUOTES) ?> of <?= htmlspecialchars((string) max(1, count($pages)), ENT_QUOTES) ?></h2>
                <p class="mt-2 section-copy">Buy in this order. The planner keeps the best action list together, then splits by page size and hauling volume.</p>
            </div>
            <div class="flex flex-wrap items-center gap-2">
                <?php if ($activePage > 1): ?>
                    <a href="<?= htmlspecialchars($buildPageUrl($activePage - 1), ENT_QUOTES) ?>" class="btn-secondary">← Back</a>
                <?php endif; ?>
                <?php if ($activePage < count($pages)): ?>
                    <a href="<?= htmlspecialchars($buildPageUrl($activePage + 1), ENT_QUOTES) ?>" class="btn-secondary">Next →</a>
                <?php endif; ?>
            </div>
        </div>

        <?php if ($activePageData === null): ?>
            <div class="surface-tertiary mt-5 text-sm text-slate-400">No buy-all pages were produced from the current inputs. Loosen the filters or switch to Blended mode.</div>
        <?php else: ?>
            <div class="mt-5 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                <div class="rounded-[1.1rem] border border-white/8 bg-slate-950/50 px-4 py-4">
                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Item types</p>
                    <p class="mt-2 text-2xl font-semibold text-white"><?= htmlspecialchars((string) ($activePageData['item_count'] ?? 0), ENT_QUOTES) ?></p>
                </div>
                <div class="rounded-[1.1rem] border border-white/8 bg-slate-950/50 px-4 py-4">
                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Total units</p>
                    <p class="mt-2 text-2xl font-semibold text-white"><?= htmlspecialchars((string) ($activePageData['total_units'] ?? 0), ENT_QUOTES) ?></p>
                </div>
                <div class="rounded-[1.1rem] border border-white/8 bg-slate-950/50 px-4 py-4">
                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Total volume</p>
                    <p class="mt-2 text-2xl font-semibold text-white"><?= htmlspecialchars(number_format((float) ($activePageData['total_volume'] ?? 0.0), 0), ENT_QUOTES) ?> m³</p>
                </div>
                <div class="rounded-[1.1rem] border border-white/8 bg-slate-950/50 px-4 py-4">
                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Doctrine-critical</p>
                    <p class="mt-2 text-2xl font-semibold text-orange-100"><?= htmlspecialchars((string) ($activePageData['doctrine_critical_count'] ?? 0), ENT_QUOTES) ?></p>
                    <p class="mt-1 text-xs text-slate-500"><?= htmlspecialchars((string) ($activePageData['necessity_mix_summary'] ?? ''), ENT_QUOTES) ?></p>
                </div>
            </div>

            <div class="mt-5 overflow-x-auto rounded-[1.2rem] border border-white/8 bg-slate-950/40">
                <table class="min-w-full divide-y divide-white/8 text-sm">
                    <thead class="bg-white/[0.03] text-left text-xs uppercase tracking-[0.16em] text-slate-500">
                        <tr>
                            <th class="px-4 py-3"><input type="checkbox" checked data-buyall-select-all title="Select / deselect all"></th>
                            <th class="px-4 py-3">Item</th>
                            <th class="px-4 py-3">Planner qty</th>
                            <th class="px-4 py-3">Priority</th>
                            <th class="px-4 py-3">Buy</th>
                            <th class="px-4 py-3">Sell</th>
                            <th class="px-4 py-3">Volume</th>
                            <th class="px-4 py-3">Hauling</th>
                            <th class="px-4 py-3">Net profit</th>
                            <th class="px-4 py-3">Reason</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-white/8 text-slate-200">
                        <?php foreach ((array) ($activePageData['items'] ?? []) as $item): ?>
                            <tr data-buyall-item data-buyall-name="<?= htmlspecialchars((string) ($item['item_name'] ?? ''), ENT_QUOTES) ?>" data-buyall-qty="<?= (int) ($item['final_planner_quantity'] ?? $item['quantity'] ?? 0) ?>" data-buyall-sell="<?= htmlspecialchars(isset($item['sell_price']) && $item['sell_price'] !== null ? number_format((float) $item['sell_price'], 2, '.', '') : '', ENT_QUOTES) ?>">
                                <td class="px-4 py-3 align-top"><input type="checkbox" checked class="buyall-item-check"></td>
                                <td class="px-4 py-3 align-top">
                                    <p class="font-semibold text-white"><?= htmlspecialchars((string) ($item['item_name'] ?? ''), ENT_QUOTES) ?></p>
                                    <p class="mt-1 text-xs text-slate-500"><?= htmlspecialchars((string) ($item['hull_class_label'] ?? 'Subcap'), ENT_QUOTES) ?> · <?= htmlspecialchars((string) ($item['valid_doctrine_count'] ?? 0), ENT_QUOTES) ?> doctrines · <?= htmlspecialchars((string) ($item['valid_fits_count'] ?? 0), ENT_QUOTES) ?> fits</p>
                                </td>
                                <td class="px-4 py-3 align-top">
                                    <p class="font-semibold text-white"><?= htmlspecialchars((string) ($item['final_planner_quantity'] ?? $item['quantity'] ?? 0), ENT_QUOTES) ?></p>
                                    <p class="mt-1 text-xs text-slate-500">Short by <?= htmlspecialchars((string) ($item['exact_deficit_quantity'] ?? 0), ENT_QUOTES) ?> · target <?= htmlspecialchars((string) ($item['operational_recommended_quantity'] ?? 0), ENT_QUOTES) ?></p>
                                </td>
                                <td class="px-4 py-3 align-top">
                                    <p class="font-semibold text-white"><?= htmlspecialchars(number_format((float) ($item['final_priority_score'] ?? 0.0), 1), ENT_QUOTES) ?></p>
                                    <p class="mt-1 text-xs text-slate-500"><?= (int) ($item['deterministic_blocked_fits'] ?? $item['blocked_fit_impact'] ?? 0) > 0 ? htmlspecialchars((string) ($item['deterministic_blocked_fits'] ?? $item['blocked_fit_impact'] ?? 0), ENT_QUOTES) . ' fits blocked' : 'Opportunity-led buy' ?></p>
                                    <p class="mt-1 text-xs text-slate-500">Target-ready fits <?= htmlspecialchars((string) ($item['target_ready_fits'] ?? 0), ENT_QUOTES) ?></p>
                                </td>
                                <td class="px-4 py-3 align-top">
                                    <p class="font-semibold text-white"><?= htmlspecialchars(market_format_isk(isset($item['buy_price']) ? (float) $item['buy_price'] : null), ENT_QUOTES) ?></p>
                                    <p class="mt-1 text-xs text-slate-500"><?= htmlspecialchars((string) ($item['buy_price_basis'] ?? 'Unknown'), ENT_QUOTES) ?></p>
                                </td>
                                <td class="px-4 py-3 align-top">
                                    <p class="font-semibold text-white"><?= htmlspecialchars(market_format_isk(isset($item['sell_price']) ? (float) $item['sell_price'] : null), ENT_QUOTES) ?></p>
                                    <p class="mt-1 text-xs text-slate-500"><?= htmlspecialchars((string) ($item['sell_price_basis'] ?? 'Unknown'), ENT_QUOTES) ?></p>
                                </td>
                                <td class="px-4 py-3 align-top">
                                    <p class="font-semibold text-white"><?= htmlspecialchars(number_format((float) ($item['unit_volume'] ?? 0.0), 2), ENT_QUOTES) ?> m³</p>
                                    <p class="mt-1 text-xs text-slate-500">Total <?= htmlspecialchars(number_format((float) ($item['total_volume'] ?? 0.0), 2), ENT_QUOTES) ?> m³</p>
                                </td>
                                <td class="px-4 py-3 align-top">
                                    <p class="font-semibold text-white"><?= htmlspecialchars(market_format_isk((float) ($item['hauling_cost_total'] ?? 0.0)), ENT_QUOTES) ?></p>
                                    <p class="mt-1 text-xs text-slate-500">Unit <?= htmlspecialchars(market_format_isk((float) ($item['hauling_cost_per_unit'] ?? 0.0)), ENT_QUOTES) ?></p>
                                </td>
                                <td class="px-4 py-3 align-top">
                                    <p class="font-semibold <?= isset($item['net_profit_total']) && (float) ($item['net_profit_total'] ?? 0.0) >= 0.0 ? 'text-emerald-100' : 'text-rose-200' ?>"><?= htmlspecialchars(market_format_isk(isset($item['net_profit_total']) ? (float) $item['net_profit_total'] : null), ENT_QUOTES) ?></p>
                                    <p class="mt-1 text-xs text-slate-500">Net margin <?= htmlspecialchars(isset($item['net_margin_percent']) ? market_format_percentage((float) $item['net_margin_percent'], 1) : 'Unknown', ENT_QUOTES) ?></p>
                                </td>
                                <td class="px-4 py-3 align-top">
                                    <p class="text-sm text-slate-200"><?= htmlspecialchars((string) ($item['reason_text'] ?? ''), ENT_QUOTES) ?></p>
                                    <p class="mt-1 text-xs text-slate-500">Doctrine impact <?= htmlspecialchars((string) ($item['doctrine_fit_impact'] ?? 0), ENT_QUOTES) ?> fits · pricing <?= htmlspecialchars((string) ($item['pricing_completeness'] ?? 'partial'), ENT_QUOTES) ?></p>
                                    <p class="mt-1 text-xs text-cyan-200/85">Graph dependency <?= htmlspecialchars(number_format((float) ($item['dependency_score'] ?? 0.0), 1), ENT_QUOTES) ?> · used by <?= htmlspecialchars((string) ($item['valid_doctrine_count'] ?? 0), ENT_QUOTES) ?> doctrines / <?= htmlspecialchars((string) ($item['valid_fits_count'] ?? 0), ENT_QUOTES) ?> fits</p>
                                    <?php $affectedFits = array_values((array) ($item['affected_fits'] ?? [])); ?>
                                    <details class="mt-2 rounded-lg border border-white/8 bg-white/[0.03] px-3 py-2">
                                        <summary class="cursor-pointer list-none text-xs font-medium text-slate-100">More detail</summary>
                                        <div class="mt-2 space-y-2">
                                            <p class="text-xs text-slate-500">Reason code <?= htmlspecialchars((string) ($item['reason_code'] ?? ''), ENT_QUOTES) ?> · necessity <?= htmlspecialchars(number_format((float) ($item['necessity_score'] ?? 0.0), 1), ENT_QUOTES) ?> · profit <?= htmlspecialchars(number_format((float) ($item['profit_score'] ?? 0.0), 1), ENT_QUOTES) ?> · blended <?= htmlspecialchars(number_format((float) ($item['blended_score'] ?? 0.0), 1), ENT_QUOTES) ?></p>
                                            <?php foreach ($affectedFits as $affectedFit): ?>
                                                <div class="rounded-lg border border-white/8 bg-white/[0.03] px-3 py-2">
                                                    <p class="text-xs font-semibold text-slate-100"><?= htmlspecialchars((string) ($affectedFit['fit_name'] ?? 'Doctrine fit'), ENT_QUOTES) ?></p>
                                                    <p class="mt-1 text-xs text-slate-400"><?= htmlspecialchars(implode(', ', array_values((array) ($affectedFit['group_names'] ?? []))) ?: 'No doctrine group recorded', ENT_QUOTES) ?></p>
                                                    <p class="mt-1 text-xs text-slate-500"><?= htmlspecialchars((string) ($affectedFit['hull_class_label'] ?? 'Subcap'), ENT_QUOTES) ?> · Req <?= htmlspecialchars((string) ($affectedFit['required_quantity'] ?? 0), ENT_QUOTES) ?> · Local <?= htmlspecialchars((string) ($affectedFit['local_stock'] ?? 0), ENT_QUOTES) ?></p>
                                                    <p class="mt-1 text-xs text-slate-500">Ready <?= htmlspecialchars((string) ($affectedFit['fit_ready_capacity'] ?? 0), ENT_QUOTES) ?> / Target <?= htmlspecialchars((string) ($affectedFit['target_ready_fits'] ?? 0), ENT_QUOTES) ?> · Blocked <?= htmlspecialchars((string) ($affectedFit['deterministic_blocked_fits'] ?? 0), ENT_QUOTES) ?></p>
                                                </div>
                                            <?php endforeach; ?>
                                            <?php if ($affectedFits === []): ?>
                                                <p class="text-xs text-slate-500">No affected fits were attached to this row.</p>
                                            <?php endif; ?>
                                        </div>
                                    </details>
                                </td>
                            </tr>
                        <?php endforeach; ?>
                    </tbody>
                </table>
            </div>
        <?php endif; ?>
    </article>

    <div class="space-y-5">
        <article class="surface-secondary">
            <div class="section-header border-b border-white/8 pb-4">
                <div>
                    <p class="eyebrow">Clipboard</p>
                    <h2 class="mt-2 section-title">Copy Buy All</h2>
                    <p class="mt-2 section-copy">Exact in-game import format: item name, space, quantity, one item per line, with no prices or annotations.</p>
                </div>
            </div>
            <?php if ($activePageData !== null): ?>
                <div class="mt-4 space-y-3">
                    <div class="flex flex-wrap items-center gap-2">
                        <button type="button" class="btn-primary" data-copy-target="buy-all-current">Copy current page</button>
                        <?php foreach ($pages as $page): ?>
                            <button type="button" class="btn-secondary" data-copy-text="<?= htmlspecialchars((string) ($page['clipboard_text'] ?? ''), ENT_QUOTES) ?>">Copy page <?= (int) ($page['number'] ?? 0) ?></button>
                        <?php endforeach; ?>
                    </div>
                    <textarea id="buy-all-current" class="field-input h-72 w-full font-mono text-sm" readonly><?= htmlspecialchars((string) ($activePageData['clipboard_text'] ?? ''), ENT_QUOTES) ?></textarea>
                </div>
            <?php else: ?>
                <div class="surface-tertiary mt-4 text-sm text-slate-400">No clipboard content is available until the planner produces at least one page.</div>
            <?php endif; ?>
        </article>

        <article class="surface-secondary">
            <div class="section-header border-b border-white/8 pb-4">
                <div>
                    <p class="eyebrow">Pricing export</p>
                    <h2 class="mt-2 section-title">Copy with sell prices</h2>
                    <p class="mt-2 section-copy">Tab-separated format with sell price and total sell value per item. Paste directly into a spreadsheet.</p>
                </div>
            </div>
            <?php if ($activePageData !== null): ?>
                <div class="mt-4 space-y-3">
                    <div class="flex flex-wrap items-center gap-2">
                        <button type="button" class="btn-primary" data-copy-target="buy-all-pricing-current">Copy pricing</button>
                    </div>
                    <textarea id="buy-all-pricing-current" class="field-input h-72 w-full font-mono text-sm" readonly><?= htmlspecialchars((string) ($activePageData['clipboard_pricing_text'] ?? ''), ENT_QUOTES) ?></textarea>
                </div>
            <?php else: ?>
                <div class="surface-tertiary mt-4 text-sm text-slate-400">No pricing data is available until the planner produces at least one page.</div>
            <?php endif; ?>
        </article>

        <article class="surface-secondary">
            <div class="section-header border-b border-white/8 pb-4">
                <div>
                    <p class="eyebrow">Page totals</p>
                    <h2 class="mt-2 section-title">Current page economics</h2>
                </div>
            </div>
            <?php if ($activePageData !== null): ?>
                <div class="mt-4 space-y-3 text-sm">
                    <div class="info-kv"><span class="text-slate-400">Buy cost</span><span class="font-medium text-slate-100"><?= htmlspecialchars(market_format_isk((float) ($activePageData['total_buy_cost'] ?? 0.0)), ENT_QUOTES) ?></span></div>
                    <div class="info-kv"><span class="text-slate-400">Expected sell value</span><span class="font-medium text-slate-100"><?= htmlspecialchars(market_format_isk((float) ($activePageData['total_expected_sell_value'] ?? 0.0)), ENT_QUOTES) ?></span></div>
                    <div class="info-kv"><span class="text-slate-400">Hauling cost</span><span class="font-medium text-slate-100"><?= htmlspecialchars(market_format_isk((float) ($activePageData['total_hauling_cost'] ?? 0.0)), ENT_QUOTES) ?></span></div>
                    <div class="info-kv"><span class="text-slate-400">Gross profit</span><span class="font-medium text-slate-100"><?= htmlspecialchars(market_format_isk((float) ($activePageData['total_gross_profit'] ?? 0.0)), ENT_QUOTES) ?></span></div>
                    <div class="info-kv"><span class="text-slate-400">Net profit</span><span class="font-medium <?= (float) ($activePageData['total_net_profit'] ?? 0.0) >= 0.0 ? 'text-emerald-100' : 'text-rose-200' ?>"><?= htmlspecialchars(market_format_isk((float) ($activePageData['total_net_profit'] ?? 0.0)), ENT_QUOTES) ?></span></div>
                    <div class="info-kv"><span class="text-slate-400">Necessity mix</span><span class="font-medium text-slate-100"><?= htmlspecialchars((string) ($activePageData['necessity_mix_summary'] ?? ''), ENT_QUOTES) ?></span></div>
                </div>
            <?php endif; ?>
        </article>
    </div>
</section>
<!-- ui-section:buyall-results:end -->

<script>
(function () {
    // Mode selection visual feedback
    var modeRadios = document.querySelectorAll('input[type="radio"][name="mode"]');
    modeRadios.forEach(function (radio) {
        radio.addEventListener('change', function () {
            modeRadios.forEach(function (r) {
                var label = r.closest('label');
                if (!label) return;
                if (r.checked) {
                    label.className = label.className
                        .replace(/border-white\/8/g, 'border-cyan-400/30')
                        .replace(/bg-slate-950\/40/g, 'bg-cyan-500/10')
                        .replace(/text-slate-200/g, 'text-cyan-50');
                } else {
                    label.className = label.className
                        .replace(/border-cyan-400\/30/g, 'border-white/8')
                        .replace(/bg-cyan-500\/10/g, 'bg-slate-950/40')
                        .replace(/text-cyan-50/g, 'text-slate-200');
                }
            });
        });
    });

    function rebuildClipboardFromSelection() {
        const rows = document.querySelectorAll('[data-buyall-item]');
        const lines = [];
        const pricingLines = ['Item\tQty\tSell Price\tTotal Sell'];
        rows.forEach(function (row) {
            const cb = row.querySelector('.buyall-item-check');
            if (cb && cb.checked) {
                var name = row.getAttribute('data-buyall-name');
                var qty = row.getAttribute('data-buyall-qty');
                var sell = row.getAttribute('data-buyall-sell');
                lines.push(name + ' ' + qty);
                var sellTotal = (sell && parseFloat(sell) > 0) ? (parseFloat(sell) * parseInt(qty, 10)).toFixed(2) : '';
                pricingLines.push(name + '\t' + qty + '\t' + (sell || '') + '\t' + sellTotal);
            }
        });
        const textarea = document.getElementById('buy-all-current');
        if (textarea) {
            textarea.value = lines.join('\n');
        }
        const pricingTextarea = document.getElementById('buy-all-pricing-current');
        if (pricingTextarea) {
            pricingTextarea.value = pricingLines.join('\n');
        }
    }

    // Select-all checkbox
    document.addEventListener('change', function (event) {
        if (event.target.hasAttribute('data-buyall-select-all')) {
            var checked = event.target.checked;
            document.querySelectorAll('.buyall-item-check').forEach(function (cb) { cb.checked = checked; });
            rebuildClipboardFromSelection();
        }
        if (event.target.classList.contains('buyall-item-check')) {
            rebuildClipboardFromSelection();
            // Update select-all state
            var allChecks = document.querySelectorAll('.buyall-item-check');
            var selectAll = document.querySelector('[data-buyall-select-all]');
            if (selectAll && allChecks.length > 0) {
                selectAll.checked = Array.from(allChecks).every(function (c) { return c.checked; });
            }
        }
    });

    // Copy-to-clipboard
    document.addEventListener('click', function (event) {
        var button = event.target.closest('[data-copy-target], [data-copy-text]');
        if (!button || !navigator.clipboard) {
            return;
        }

        var text = '';
        var targetId = button.getAttribute('data-copy-target');
        if (targetId) {
            var target = document.getElementById(targetId);
            text = target ? target.value : '';
        } else {
            text = button.getAttribute('data-copy-text') || '';
        }

        if (text === '') {
            return;
        }

        navigator.clipboard.writeText(text);
        var original = button.textContent;
        button.textContent = 'Copied';
        window.setTimeout(function () {
            button.textContent = original;
        }, 1200);
    });
})();
</script>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
