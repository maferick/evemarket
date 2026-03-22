<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$fitId = max(0, (int) ($_GET['fit_id'] ?? $_POST['fit_id'] ?? 0));
$groupOptions = doctrine_group_options();
$errorMessage = null;
$editDraft = null;

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    if (!validate_csrf($_POST['_token'] ?? null)) {
        http_response_code(419);
        exit('Invalid CSRF token');
    }

    $action = (string) ($_POST['action'] ?? '');
    if ($action === 'save_fit') {
        $result = doctrine_update_fit_from_request($fitId, $_POST);
        if (($result['ok'] ?? false) === true) {
            flash('success', (string) ($result['message'] ?? 'Doctrine fit updated successfully.'));
            header('Location: /doctrine/fit?fit_id=' . $fitId);
            exit;
        }
        $errorMessage = (string) ($result['message'] ?? 'Doctrine fit update failed.');
        $editDraft = $result['draft'] ?? null;
    }

    if ($action === 'delete_fit') {
        if (($_POST['confirm_delete'] ?? '') !== 'yes') {
            flash('success', 'Confirm doctrine fit deletion before continuing.');
            header('Location: /doctrine/fit?fit_id=' . $fitId . '&confirm_delete=1');
            exit;
        }

        try {
            db_doctrine_fit_delete($fitId);
            doctrine_schedule_intelligence_refresh('fit-delete');
            flash('success', 'Doctrine fit deleted successfully.');
            header('Location: /doctrine');
            exit;
        } catch (Throwable $exception) {
            flash('success', 'Doctrine fit delete failed: ' . $exception->getMessage());
            header('Location: /doctrine/fit?fit_id=' . $fitId);
            exit;
        }
    }
}

$data = doctrine_fit_detail_view_model($fitId);
$pageFreshness = supplycore_page_freshness_view_model((array) ($data['freshness'] ?? []));
$fit = $data['fit'] ?? null;
$categories = $data['categories'] ?? [];
$summary = $data['summary'] ?? [];
$snapshotHistory = $data['snapshot_history'] ?? [];
$title = $fit !== null ? ((string) ($fit['fit_name'] ?? 'Doctrine Fit')) : 'Doctrine Fit';
$showDeleteConfirm = isset($_GET['confirm_delete']) && $_GET['confirm_delete'] === '1';

if ($fit !== null && $editDraft === null) {
    $items = $data['items'] ?? [];
    $hullItem = doctrine_find_hull_item((array) $items);
    $editDraft = [
        'fit' => [
            'fit_name' => (string) ($fit['fit_name'] ?? ''),
            'ship_name' => (string) ($fit['ship_name'] ?? ''),
            'source_format' => (string) ($fit['source_format'] ?? 'buyall'),
            'import_body' => (string) ($fit['import_body'] ?? ''),
            'target_fleet_size_override' => (string) (($fit['target_fleet_size_override'] ?? '') ?: ''),
        ],
        'item_lines_text' => doctrine_render_editable_item_lines((array) $items),
        'group_ids' => (array) ($fit['group_ids'] ?? []),
        'unresolved' => [],
        'hull_is_stock_tracked' => !is_array($hullItem) || !array_key_exists('is_stock_tracked', $hullItem) || (bool) $hullItem['is_stock_tracked'],
        'hull_tracking_default_reason' => (string) ($fit['supply']['hull_tracking_note'] ?? ''),
    ];
}

$statusTone = static function (string $status): string {
    return match ($status) {
        'ok' => 'border-emerald-400/20 bg-emerald-500/10 text-emerald-100',
        'external' => 'border-cyan-400/20 bg-cyan-500/10 text-cyan-100',
        'low' => 'border-amber-400/20 bg-amber-500/10 text-amber-100',
        default => 'border-rose-400/20 bg-rose-500/10 text-rose-200',
    };
};
$liveRefreshConfig = supplycore_live_refresh_page_config('doctrine_fit');

include __DIR__ . '/../../src/views/partials/header.php';
?>
<?php if ($fit === null): ?>
    <section class="surface-secondary">
        <div class="section-header">
            <div>
                <p class="eyebrow">Doctrine fit</p>
                <h2 class="mt-2 section-title">Doctrine fit not found</h2>
            </div>
            <a href="/doctrine" class="btn-secondary">Back to groups</a>
        </div>
        <p class="text-sm text-slate-400">The requested doctrine fit does not exist yet or the database is unavailable.</p>
    </section>
<?php else: ?>
    <?php $supply = (array) ($fit['supply'] ?? []); ?>
    <?php $statusBadges = doctrine_combined_status_badges($supply); ?>
    <?php $history = is_array($snapshotHistory) ? $snapshotHistory : []; ?>
    <?php $latestSnapshot = is_array($history['latest'] ?? null) ? $history['latest'] : null; ?>
    <?php $previousSnapshot = is_array($history['previous'] ?? null) ? $history['previous'] : null; ?>
    <?php $change = (array) ($history['change'] ?? []); ?>
    <?php $trendPoints = (array) ($history['trend_points'] ?? []); ?>
    <?php $changeTimeline = (array) ($history['timeline'] ?? []); ?>
    <!-- ui-section:doctrine-fit-summary:start -->
    <section class="grid gap-4 sm:grid-cols-2 xl:grid-cols-4" data-ui-section="doctrine-fit-summary">
        <?php foreach ($summary as $card): ?>
            <article class="surface-secondary">
                <p class="eyebrow"><?= htmlspecialchars((string) ($card['label'] ?? ''), ENT_QUOTES) ?></p>
                <p class="mt-3 text-3xl metric-value"><?= htmlspecialchars((string) ($card['value'] ?? ''), ENT_QUOTES) ?></p>
                <p class="mt-2 text-sm text-slate-300"><?= htmlspecialchars((string) ($card['context'] ?? ''), ENT_QUOTES) ?></p>
            </article>
        <?php endforeach; ?>
    </section>
    <!-- ui-section:doctrine-fit-summary:end -->

    <!-- ui-section:doctrine-fit-history:start -->
    <section class="mt-6 grid gap-4 xl:grid-cols-3" data-ui-section="doctrine-fit-history">
        <article class="surface-secondary">
            <p class="eyebrow">Readiness + pressure change</p>
            <h2 class="mt-2 section-title">Why the doctrine moved</h2>
            <p class="mt-3 text-sm text-slate-300">
                <?= htmlspecialchars((string) ($change['summary'] ?? 'No doctrine snapshot history has been recorded yet.'), ENT_QUOTES) ?>
            </p>
            <div class="mt-4 grid gap-3 sm:grid-cols-2">
                <div class="surface-tertiary">
                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Target delta</p>
                    <p class="mt-2 text-xl font-semibold text-slate-100"><?= ($change['target_delta'] ?? 0) >= 0 ? '+' : '' ?><?= doctrine_format_quantity((int) ($change['target_delta'] ?? 0)) ?></p>
                    <p class="mt-1 text-xs text-slate-500">Previous <?= doctrine_format_quantity((int) ($previousSnapshot['target_fits'] ?? $supply['recommended_target_fit_count'] ?? 0)) ?> → current <?= doctrine_format_quantity((int) ($latestSnapshot['target_fits'] ?? $supply['recommended_target_fit_count'] ?? 0)) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Combined outlook</p>
                    <p class="mt-2 text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) ($previousSnapshot['recommendation_text'] ?? 'No prior snapshot'), ENT_QUOTES) ?> → <?= htmlspecialchars((string) ($latestSnapshot['recommendation_text'] ?? ($supply['recommendation_text'] ?? 'Unavailable')), ENT_QUOTES) ?></p>
                    <p class="mt-1 text-xs text-slate-500">Readiness <?= htmlspecialchars(doctrine_readiness_label_from_state((string) ($previousSnapshot['readiness_state'] ?? ($supply['readiness_state'] ?? 'market_ready'))), ENT_QUOTES) ?> → <?= htmlspecialchars(doctrine_readiness_label_from_state((string) ($latestSnapshot['readiness_state'] ?? ($supply['readiness_state'] ?? 'market_ready'))), ENT_QUOTES) ?> · Pressure <?= htmlspecialchars((string) ($previousSnapshot['resupply_pressure_text'] ?? ($supply['resupply_pressure_label'] ?? 'Stable')), ENT_QUOTES) ?> → <?= htmlspecialchars((string) ($latestSnapshot['resupply_pressure_text'] ?? ($supply['resupply_pressure_label'] ?? 'Stable')), ENT_QUOTES) ?></p>
                </div>
            </div>
        </article>
        <article class="surface-secondary">
            <p class="eyebrow">Driver breakdown</p>
            <h2 class="mt-2 section-title">Explainable doctrine scoring</h2>
            <div class="mt-4 space-y-3">
                <?php foreach ([
                    'Loss pressure' => (float) (($supply['driver_scores']['loss_pressure'] ?? 0.0)),
                    'Stock gap' => (float) (($supply['driver_scores']['stock_gap'] ?? 0.0)),
                    'Depletion' => (float) (($supply['driver_scores']['depletion'] ?? 0.0)),
                    'Bottleneck' => (float) (($supply['driver_scores']['bottleneck'] ?? 0.0)),
                ] as $label => $value): ?>
                    <?php $width = max(6, min(100, (int) round($value))); ?>
                    <div>
                        <div class="flex items-center justify-between gap-3 text-sm">
                            <span class="text-slate-300"><?= htmlspecialchars($label, ENT_QUOTES) ?></span>
                            <span class="font-semibold text-slate-100"><?= number_format($value, 1) ?></span>
                        </div>
                        <div class="mt-2 h-2.5 rounded-full bg-slate-900/80">
                            <div class="h-2.5 rounded-full bg-gradient-to-r from-cyan-500 via-sky-400 to-indigo-400" style="width: <?= $width ?>%"></div>
                        </div>
                    </div>
                <?php endforeach; ?>
            </div>
            <p class="mt-4 text-xs text-slate-500">Total doctrine score <?= number_format((float) (($supply['driver_scores']['total'] ?? 0.0)), 1) ?> · loss 24h <?= doctrine_format_quantity((int) ($supply['recent_hull_losses_24h'] ?? 0)) ?> / 7d <?= doctrine_format_quantity((int) ($supply['recent_hull_losses_7d'] ?? 0)) ?>.</p>
        </article>
        <article class="surface-secondary">
            <p class="eyebrow">Trend visualization</p>
            <h2 class="mt-2 section-title">Doctrine adaptation over time</h2>
            <?php if ($trendPoints === []): ?>
                <div class="mt-4 surface-tertiary text-sm text-slate-400">Doctrine snapshots will populate after the next market, killmail, or scheduler refresh.</div>
            <?php else: ?>
                <div class="mt-4 space-y-3">
                    <?php foreach (array_slice(array_reverse($trendPoints), 0, 6) as $point): ?>
                        <?php $target = max(1, (int) ($point['target_fits'] ?? 0)); ?>
                        <?php $complete = max(0, (int) ($point['complete_fits_available'] ?? 0)); ?>
                        <?php $width = min(100, (int) round(($complete / $target) * 100)); ?>
                        <div class="surface-tertiary">
                            <div class="flex items-center justify-between gap-3">
                                <p class="text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) ($point['snapshot_time'] ?? ''), ENT_QUOTES) ?></p>
                                <p class="text-xs text-slate-500">Loss 24h <?= doctrine_format_quantity((int) ($point['loss_24h'] ?? 0)) ?></p>
                            </div>
                            <div class="mt-3 h-2.5 rounded-full bg-slate-950/80">
                                <div class="h-2.5 rounded-full <?= $complete >= $target ? 'bg-emerald-400' : 'bg-amber-400' ?>" style="width: <?= $width ?>%"></div>
                            </div>
                            <p class="mt-2 text-xs text-slate-400"><?= doctrine_format_quantity($complete) ?> complete / <?= doctrine_format_quantity($target) ?> target · gap <?= doctrine_format_quantity((int) ($point['fit_gap'] ?? 0)) ?> · score <?= number_format((float) ($point['total_score'] ?? 0.0), 1) ?></p>
                        </div>
                    <?php endforeach; ?>
                </div>
            <?php endif; ?>
        </article>
    </section>
    <!-- ui-section:doctrine-fit-history:end -->

    <!-- ui-section:doctrine-fit-items:start -->
    <section class="mt-8 grid gap-6 xl:grid-cols-[minmax(280px,360px)_minmax(0,1fr)]" data-ui-section="doctrine-fit-items">
        <aside class="space-y-6">
            <article class="surface-secondary">
                <div class="section-header">
                    <div>
                        <p class="eyebrow">Hull profile</p>
                        <div class="mt-2 flex flex-wrap items-center gap-3">
                            <h2 class="section-title"><?= htmlspecialchars((string) ($fit['ship_name'] ?? ''), ENT_QUOTES) ?></h2>
                            <?php foreach ($statusBadges as $badge): ?>
                                <span class="badge <?= htmlspecialchars((string) ($badge['tone'] ?? ''), ENT_QUOTES) ?>"><?= htmlspecialchars((string) ($badge['title'] ?? '') . ': ' . (string) ($badge['label'] ?? ''), ENT_QUOTES) ?></span>
                            <?php endforeach; ?>
                            <?php if (!empty($supply['externally_managed'])): ?>
                                <span class="badge border-cyan-400/20 bg-cyan-500/10 text-cyan-100">Excluded from stocking</span>
                            <?php endif; ?>
                        </div>
                        <p class="mt-2 text-sm text-slate-400"><?= htmlspecialchars((string) ($fit['fit_name'] ?? ''), ENT_QUOTES) ?></p>
                    </div>
                    <span class="badge border-cyan-400/18 bg-cyan-500/10 text-cyan-100"><?= htmlspecialchars((string) strtoupper((string) ($fit['source_format'] ?? 'buyall')), ENT_QUOTES) ?></span>
                </div>
                <div class="overflow-hidden rounded-[1.35rem] border border-white/8 bg-slate-950/70 p-6">
                    <?php if (!empty($fit['ship_image_url'])): ?>
                        <img src="<?= htmlspecialchars((string) $fit['ship_image_url'], ENT_QUOTES) ?>" alt="<?= htmlspecialchars((string) ($fit['ship_name'] ?? ''), ENT_QUOTES) ?>" class="mx-auto h-48 w-48 object-contain">
                    <?php else: ?>
                        <div class="flex h-48 items-center justify-center text-sm text-slate-500">Ship image unavailable</div>
                    <?php endif; ?>
                </div>
                <div class="mt-4 grid gap-3">
                    <div class="surface-tertiary">
                        <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Doctrine groups</p>
                        <p class="mt-2 font-semibold text-slate-100"><?= htmlspecialchars(implode(', ', (array) ($fit['group_names'] ?? [])) ?: 'Ungrouped', ENT_QUOTES) ?></p>
                    </div>
                    <div class="surface-tertiary">
                        <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Import status</p>
                        <p class="mt-2 font-semibold text-slate-100"><?= htmlspecialchars((string) ($fit['readiness_status'] ?? 'Ready'), ENT_QUOTES) ?></p>
                        <p class="mt-1 text-xs text-slate-500">Source <?= htmlspecialchars(strtoupper((string) ($fit['source_type'] ?? 'manual')), ENT_QUOTES) ?> · warnings <?= (int) ($fit['warning_count'] ?? 0) ?> · unresolved <?= (int) ($fit['unresolved_count'] ?? 0) ?></p>
                        <?php if (($fit['conflict_state'] ?? 'none') !== 'none'): ?>
                            <p class="mt-2 text-xs text-fuchsia-100">Conflict: <?= htmlspecialchars((string) ($fit['conflict_state'] ?? ''), ENT_QUOTES) ?></p>
                        <?php endif; ?>
                    </div>
                    <?php if (!empty($fit['notes'])): ?>
                        <div class="surface-tertiary">
                            <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Fitting notes</p>
                            <p class="mt-2 whitespace-pre-wrap text-sm text-slate-300"><?= htmlspecialchars((string) ($fit['notes'] ?? ''), ENT_QUOTES) ?></p>
                        </div>
                    <?php endif; ?>
                    <?php if ((array) ($fit['parse_warnings'] ?? []) !== []): ?>
                        <div class="rounded-[1.25rem] border border-amber-400/20 bg-amber-500/10 p-4">
                            <p class="text-xs uppercase tracking-[0.16em] text-amber-100/80">Parse warnings</p>
                            <ul class="mt-3 space-y-2 text-sm text-amber-100">
                                <?php foreach ((array) ($fit['parse_warnings'] ?? []) as $warning): ?>
                                    <li class="rounded-xl border border-amber-400/15 bg-black/10 px-3 py-2"><?= htmlspecialchars((string) $warning, ENT_QUOTES) ?></li>
                                <?php endforeach; ?>
                            </ul>
                        </div>
                    <?php endif; ?>
                    <?php if (is_array($fit['ai_briefing'] ?? null)): ?>
                        <?php $aiBriefing = (array) $fit['ai_briefing']; ?>
                        <div class="rounded-[1.25rem] border border-violet-400/20 bg-violet-500/10 p-4">
                            <div class="flex flex-wrap items-start justify-between gap-3">
                                <div>
                                    <p class="text-xs uppercase tracking-[0.16em] text-violet-100/80">AI notes</p>
                                    <p class="mt-2 text-sm font-semibold text-white"><?= htmlspecialchars((string) ($aiBriefing['headline'] ?? 'Operational briefing'), ENT_QUOTES) ?></p>
                                </div>
                                <span class="badge <?= htmlspecialchars((string) ($aiBriefing['priority_tone'] ?? 'border-violet-400/20 bg-violet-500/10 text-violet-100'), ENT_QUOTES) ?>"><?= htmlspecialchars(strtoupper((string) ($aiBriefing['priority_level'] ?? 'medium')), ENT_QUOTES) ?></span>
                            </div>
                            <p class="mt-3 text-sm text-slate-200"><?= htmlspecialchars((string) ($aiBriefing['summary'] ?? ''), ENT_QUOTES) ?></p>
                            <p class="mt-3 text-sm font-medium text-violet-50">Next action: <?= htmlspecialchars((string) ($aiBriefing['action_text'] ?? ''), ENT_QUOTES) ?></p>
                            <p class="mt-2 text-xs text-violet-100/70">Generated <?= htmlspecialchars((string) ($aiBriefing['computed_relative'] ?? 'Unknown'), ENT_QUOTES) ?><?= (($aiBriefing['generation_status'] ?? 'ready') !== 'ready') ? ' · deterministic fallback' : '' ?></p>
                        </div>
                    <?php endif; ?>
                    <div class="surface-tertiary">
                        <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Complete fits available</p>
                        <p class="mt-2 font-semibold text-slate-100"><?= doctrine_format_quantity((int) ($supply['complete_fits_available'] ?? 0)) ?></p>
                        <p class="mt-1 text-xs text-slate-500"><?= htmlspecialchars((string) ($supply['constraint_label'] ?? ''), ENT_QUOTES) ?></p>
                    </div>
                    <div class="surface-tertiary">
                        <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Hull stocking mode</p>
                        <p class="mt-2 font-semibold text-slate-100"><?= !empty($supply['externally_managed']) ? 'Externally managed' : 'Tracked by SupplyCore' ?></p>
                        <p class="mt-1 text-xs text-slate-500"><?= htmlspecialchars((string) ($supply['hull_tracking_note'] ?? ''), ENT_QUOTES) ?></p>
                    </div>
                    <div class="surface-tertiary">
                        <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Target fit count</p>
                        <p class="mt-2 font-semibold text-slate-100"><?= doctrine_format_quantity((int) ($supply['recommended_target_fit_count'] ?? 0)) ?></p>
                        <p class="mt-1 text-xs text-slate-500"><?= doctrine_format_quantity((int) ($supply['gap_to_target_fit_count'] ?? 0)) ?> fits short of target<?= !empty($supply['externally_managed']) ? ' (readiness-only; no hull restock urgency)' : '' ?></p>
                    </div>
                    <div class="surface-tertiary border <?= htmlspecialchars(doctrine_supply_status_tone((string) ($supply['readiness_state'] ?? 'market_ready')), ENT_QUOTES) ?>">
                        <p class="text-xs uppercase tracking-[0.16em]">Combined outlook</p>
                        <p class="mt-2 text-sm text-slate-100"><?= htmlspecialchars((string) ($supply['combined_status_label'] ?? 'Market ready · Stable'), ENT_QUOTES) ?></p>
                        <p class="mt-2 text-sm text-slate-300"><?= htmlspecialchars((string) ($supply['readiness_context'] ?? 'Operational readiness unavailable.'), ENT_QUOTES) ?></p>
                        <p class="mt-2 text-sm text-slate-300"><?= htmlspecialchars((string) ($supply['resupply_pressure_context'] ?? 'Resupply pressure unavailable.'), ENT_QUOTES) ?></p>
                        <p class="mt-2 text-xs text-slate-300">Trend: <?= htmlspecialchars((string) ($supply['readiness_trend'] ?? 'Trend unavailable'), ENT_QUOTES) ?> · Restock: <?= htmlspecialchars((string) ($supply['restock_trend'] ?? 'Unavailable'), ENT_QUOTES) ?> · Depletion: <?= htmlspecialchars((string) ucfirst((string) ($supply['depletion_state'] ?? 'stable')), ENT_QUOTES) ?> · 7d hull losses: <?= doctrine_format_quantity((int) ($supply['recent_hull_losses_7d'] ?? 0)) ?>.</p>
                    </div>
                    <div class="flex gap-3">
                        <?php $primaryGroupId = (int) ($fit['doctrine_group_id'] ?? 0); ?>
                        <?php if ($primaryGroupId > 0): ?>
                            <a href="/doctrine/group?group_id=<?= $primaryGroupId ?>" class="btn-secondary">Back to group</a>
                        <?php endif; ?>
                        <a href="/doctrine/import" class="btn-primary">Import fit</a>
                    </div>
                </div>
            </article>

            <article class="surface-secondary">
                <div class="section-header">
                    <div>
                        <p class="eyebrow">Fit management</p>
                        <h2 class="mt-2 section-title">Edit doctrine fit</h2>
                    </div>
                    <span class="badge border-sky-400/18 bg-sky-500/10 text-sky-100">Correct without reimport</span>
                </div>
                <?php if ($errorMessage !== null): ?>
                    <div class="mb-4 rounded-2xl border border-amber-500/30 bg-amber-500/10 px-4 py-3 text-sm text-amber-100"><?= htmlspecialchars($errorMessage, ENT_QUOTES) ?></div>
                <?php endif; ?>
                <form method="post" class="space-y-4">
                    <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                    <input type="hidden" name="action" value="save_fit">
                    <input type="hidden" name="fit_id" value="<?= (int) $fitId ?>">
                    <div class="grid gap-4 md:grid-cols-2">
                        <label class="block md:col-span-2">
                            <span class="mb-2 block field-label">Fit name</span>
                            <input type="text" name="fit_name" class="field-input" maxlength="190" value="<?= htmlspecialchars((string) (($editDraft['fit']['fit_name'] ?? '')), ENT_QUOTES) ?>">
                        </label>
                        <label class="block">
                            <span class="mb-2 block field-label">Ship name</span>
                            <input type="text" name="ship_name" class="field-input" maxlength="255" value="<?= htmlspecialchars((string) (($editDraft['fit']['ship_name'] ?? '')), ENT_QUOTES) ?>">
                        </label>
                        <label class="block">
                            <span class="mb-2 block field-label">Source format</span>
                            <select name="source_format" class="field-select">
                                <?php foreach (['eft' => 'EFT', 'buyall' => 'BuyAll'] as $value => $label): ?>
                                    <option value="<?= $value ?>" <?= (($editDraft['fit']['source_format'] ?? 'buyall') === $value) ? 'selected' : '' ?>><?= $label ?></option>
                                <?php endforeach; ?>
                            </select>
                        </label>
                        <label class="block">
                            <span class="mb-2 block field-label">Target fleet override</span>
                            <input type="number" name="target_fleet_size_override" min="0" class="field-input" value="<?= htmlspecialchars((string) (($editDraft['fit']['target_fleet_size_override'] ?? '')), ENT_QUOTES) ?>" placeholder="Class default">
                        </label>
                    </div>
                    <div>
                        <span class="mb-2 block field-label">Doctrine groups</span>
                        <div class="space-y-2 rounded-[1.2rem] border border-white/8 bg-slate-950/50 p-4">
                            <?php foreach ($groupOptions as $group): ?>
                                <?php $groupId = (int) ($group['id'] ?? 0); ?>
                                <label class="flex items-start gap-3 text-sm text-slate-300">
                                    <input type="checkbox" name="group_ids[]" value="<?= $groupId ?>" class="mt-1" <?= in_array($groupId, (array) ($editDraft['group_ids'] ?? []), true) ? 'checked' : '' ?>>
                                    <span><?= htmlspecialchars((string) ($group['group_name'] ?? ''), ENT_QUOTES) ?></span>
                                </label>
                            <?php endforeach; ?>
                        </div>
                    </div>
                    <label class="flex items-start gap-3 rounded-[1.2rem] border border-white/8 bg-slate-950/50 p-4 text-sm text-slate-300">
                        <input type="hidden" name="hull_stock_tracked" value="0">
                        <input type="checkbox" name="hull_stock_tracked" value="1" class="mt-1" <?= !empty($editDraft['hull_is_stock_tracked']) ? 'checked' : '' ?>>
                        <span>
                            <span class="font-semibold text-slate-100">Track hull for stocking</span>
                            <span class="mt-1 block text-xs text-slate-400">Disable this for externally managed or specialty hulls. The hull will still cap complete fits and readiness, but it will stop affecting restock urgency, spend, and missing-item lists.</span>
                            <?php if (!empty($editDraft['hull_tracking_default_reason'])): ?>
                                <span class="mt-2 block text-xs <?= !empty($editDraft['hull_is_stock_tracked']) ? 'text-cyan-100' : 'text-amber-100' ?>"><?= htmlspecialchars((string) ($editDraft['hull_tracking_default_reason'] ?? ''), ENT_QUOTES) ?></span>
                            <?php endif; ?>
                        </span>
                    </label>
                    <label class="block">
                        <span class="mb-2 block field-label">Raw import text</span>
                        <textarea name="import_body" class="field-input font-mono text-sm" style="min-height: 12rem;" spellcheck="false"><?= htmlspecialchars((string) (($editDraft['fit']['import_body'] ?? '')), ENT_QUOTES) ?></textarea>
                    </label>
                    <label class="block">
                        <span class="mb-2 block field-label">Normalized item lines</span>
                        <textarea name="item_lines_text" class="field-input font-mono text-sm" style="min-height: 16rem;" spellcheck="false"><?= htmlspecialchars((string) (($editDraft['item_lines_text'] ?? '')), ENT_QUOTES) ?></textarea>
                    </label>
                    <?php if (($editDraft['unresolved'] ?? []) !== []): ?>
                        <div class="surface-tertiary text-sm text-amber-100">Resolve these names before saving: <?= htmlspecialchars(implode(', ', (array) ($editDraft['unresolved'] ?? [])), ENT_QUOTES) ?></div>
                    <?php endif; ?>
                    <button type="submit" class="btn-primary w-full justify-center">Save fit changes</button>
                </form>
            </article>

            <article class="surface-secondary">
                <div class="section-header">
                    <div>
                        <p class="eyebrow">Delete fit</p>
                        <h2 class="mt-2 section-title">Remove doctrine fit</h2>
                    </div>
                </div>
                <form method="post" class="space-y-4">
                    <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                    <input type="hidden" name="action" value="delete_fit">
                    <input type="hidden" name="fit_id" value="<?= (int) $fitId ?>">
                    <label class="flex items-start gap-3 text-sm text-slate-300">
                        <input type="checkbox" name="confirm_delete" value="yes" class="mt-1" <?= $showDeleteConfirm ? '' : '' ?>>
                        <span>I understand this removes the fit, its normalized doctrine item rows, and all group memberships.</span>
                    </label>
                    <button type="submit" class="btn-secondary w-full justify-center border-rose-400/30 text-rose-200 hover:bg-rose-500/10">Delete doctrine fit</button>
                </form>
            </article>
        </aside>

        <article class="surface-secondary">
            <div class="section-header">
                <div>
                    <p class="eyebrow">Market mapping</p>
                    <h2 class="mt-2 section-title">Required items by category</h2>
                    <p class="mt-2 text-sm text-slate-400">Fleet readiness now tracks only hard blockers, while sustainment and recent losses stay visible as separate operator signals.</p>
                </div>
                <div class="flex flex-wrap gap-2">
                    <?php foreach ($statusBadges as $badge): ?>
                        <span class="badge <?= htmlspecialchars((string) ($badge['tone'] ?? ''), ENT_QUOTES) ?>"><?= htmlspecialchars((string) ($badge['title'] ?? '') . ': ' . (string) ($badge['label'] ?? ''), ENT_QUOTES) ?></span>
                    <?php endforeach; ?>
                </div>
            </div>

            <div class="mb-6 grid gap-4 xl:grid-cols-2">
                <div class="surface-tertiary">
                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500"><?= htmlspecialchars((string) ($supply['bottleneck_label'] ?? 'Bottleneck item'), ENT_QUOTES) ?></p>
                    <p class="mt-2 text-lg font-semibold text-slate-50"><?= htmlspecialchars((string) ($supply['bottleneck_item_name'] ?? 'Unavailable'), ENT_QUOTES) ?></p>
                    <p class="mt-1 text-sm text-slate-400"><?= doctrine_format_quantity((int) ($supply['bottleneck_quantity'] ?? 0)) ?> local units for <?= doctrine_format_quantity((int) ($supply['bottleneck_required_quantity'] ?? 0)) ?> required per ship · fleet ceiling <?= doctrine_format_quantity((int) ($supply['bottleneck_capacity'] ?? $supply['minimum_stock_constraint'] ?? 0)) ?> · blocks <?= doctrine_format_quantity((int) ($supply['bottleneck_impact'] ?? 0)) ?> ships.</p>
                    <?php if (!empty($supply['external_bottleneck'])): ?>
                        <p class="mt-2 text-xs text-cyan-100">This bottleneck is externally managed, so it does not create restock urgency.</p>
                    <?php endif; ?>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Fleet model</p>
                    <p class="mt-2 text-lg font-semibold text-slate-50"><?= doctrine_format_quantity((int) ($supply['fleet_ready'] ?? 0)) ?> ready / <?= doctrine_format_quantity((int) ($supply['doctrine_target_fleet_size'] ?? 0)) ?> target</p>
                    <p class="mt-1 text-sm text-slate-400">Gap <?= doctrine_format_quantity((int) ($supply['fleet_gap'] ?? 0)) ?> · overcapacity <?= doctrine_format_quantity((int) ($supply['overcapacity'] ?? 0)) ?> · <?= htmlspecialchars((string) ($supply['readiness_trend_context'] ?? ''), ENT_QUOTES) ?></p>
                </div>
            </div>

            <div class="mb-6 grid gap-4 xl:grid-cols-2">
                <div class="surface-tertiary">
                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Depletion signal</p>
                    <p class="mt-2 text-lg font-semibold text-slate-50"><?= htmlspecialchars((string) ucfirst((string) ($supply['depletion_state'] ?? 'stable')), ENT_QUOTES) ?></p>
                    <p class="mt-1 text-sm text-slate-400"><?= htmlspecialchars((string) ($supply['depletion_context'] ?? 'Local depletion unavailable.'), ENT_QUOTES) ?></p>
                    <p class="mt-2 text-xs text-slate-500">24h <?= doctrine_format_quantity((int) ($supply['depletion_24h'] ?? 0)) ?> units · 7d <?= doctrine_format_quantity((int) ($supply['depletion_7d'] ?? 0)) ?> units · fit-equivalent drain <?= htmlspecialchars((string) number_format((float) ($supply['depletion_fit_equivalent_7d'] ?? 0.0), 2), ENT_QUOTES) ?></p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Sustainment + losses</p>
                    <p class="mt-2 text-lg font-semibold text-slate-50"><?= htmlspecialchars((string) ($supply['sustainment_risk_label'] ?? 'Stable'), ENT_QUOTES) ?></p>
                    <p class="mt-1 text-sm text-slate-400"><?= htmlspecialchars((string) ($supply['sustainment_risk_context'] ?? 'No separate sustainment pressure detected.'), ENT_QUOTES) ?></p>
                    <p class="mt-2 text-xs text-slate-500"><?= doctrine_format_quantity((int) ($supply['recent_hull_losses_24h'] ?? 0)) ?> hull losses (24h) · <?= doctrine_format_quantity((int) ($supply['recent_hull_losses_7d'] ?? 0)) ?> hull losses (7d) · <?= doctrine_format_quantity((int) ($supply['recent_item_fit_losses_7d'] ?? 0)) ?> fit-equivalent support losses (7d)</p>
                </div>
            </div>

            <div class="mb-6 surface-tertiary">
                <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Change timeline</p>
                <?php if ($changeTimeline === []): ?>
                    <p class="mt-2 text-sm text-slate-400">No doctrine changes logged yet.</p>
                <?php else: ?>
                    <div class="mt-3 space-y-3">
                        <?php foreach (array_slice($changeTimeline, 0, 4) as $event): ?>
                            <div class="rounded-2xl border border-white/8 bg-slate-950/60 px-3 py-2.5">
                                <div class="flex items-center justify-between gap-3">
                                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500"><?= htmlspecialchars((string) ($event['snapshot_time'] ?? ''), ENT_QUOTES) ?></p>
                                    <span class="badge <?= htmlspecialchars(doctrine_supply_status_tone((string) ($event['readiness_state'] ?? $latestSnapshot['readiness_state'] ?? $supply['readiness_state'] ?? 'market_ready')), ENT_QUOTES) ?>"><?= htmlspecialchars((string) ($event['readiness_text'] ?? ($supply['readiness_label'] ?? 'Market ready')), ENT_QUOTES) ?></span>
                                    <span class="badge <?= htmlspecialchars(doctrine_resupply_pressure_tone((string) ($event['resupply_pressure_state'] ?? $latestSnapshot['resupply_pressure_state'] ?? $supply['resupply_pressure_state'] ?? 'stable')), ENT_QUOTES) ?>"><?= htmlspecialchars((string) ($event['resupply_pressure_text'] ?? ($supply['resupply_pressure_label'] ?? 'Stable')), ENT_QUOTES) ?></span>
                                </div>
                                <p class="mt-2 text-sm text-slate-300"><?= htmlspecialchars((string) ($event['summary'] ?? ''), ENT_QUOTES) ?></p>
                            </div>
                        <?php endforeach; ?>
                    </div>
                <?php endif; ?>
            </div>

            <?php if ($categories === []): ?>
                <div class="surface-tertiary text-sm text-slate-400">No doctrine items were stored for this fit.</div>
            <?php else: ?>
                <div class="space-y-6">
                    <?php foreach ($categories as $category => $rows): ?>
                        <section>
                            <div class="mb-3 flex items-center justify-between gap-3">
                                <h3 class="text-base font-semibold text-slate-100"><?= htmlspecialchars((string) $category, ENT_QUOTES) ?></h3>
                                <span class="text-xs uppercase tracking-[0.16em] text-slate-500"><?= doctrine_format_quantity(count($rows)) ?> lines</span>
                            </div>
                            <div class="overflow-hidden rounded-[1.25rem] border border-white/8">
                                <table class="table-ui">
                                    <thead>
                                        <tr>
                                            <th>Item</th>
                                            <th class="text-right">Required</th>
                                            <th class="text-right">Local</th>
                                            <th class="text-right">Fits</th>
                                            <th class="text-right">Missing</th>
                                            <th class="text-right">Hub Price</th>
                                            <th class="text-right">Local Price</th>
                                            <th class="text-right">Restock Gap</th>
                                            <th>Status</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <?php foreach ($rows as $row): ?>
                                            <?php $isBottleneck = (int) ($row['type_id'] ?? 0) > 0 && (int) ($row['type_id'] ?? 0) === (int) ($supply['bottleneck_type_id'] ?? 0); ?>
                                            <?php $rowIsExternallyManaged = !($row['is_stock_tracked'] ?? true); ?>
                                            <tr class="<?= $rowIsExternallyManaged ? 'bg-cyan-900/10' : (((int) ($row['missing_qty'] ?? 0) > 0) ? 'bg-rose-900/10' : ($isBottleneck ? 'bg-amber-900/10' : '')) ?>">
                                                <td class="font-semibold text-slate-50">
                                                    <?= htmlspecialchars((string) ($row['item_name'] ?? ''), ENT_QUOTES) ?>
                                                    <span class="ml-2 rounded-full border border-white/10 bg-white/5 px-2 py-0.5 text-[11px] uppercase tracking-[0.08em] text-slate-300"><?= htmlspecialchars((string) ($row['item_block_type_label'] ?? 'Hard blocker'), ENT_QUOTES) ?></span>
                                                    <?php if ($rowIsExternallyManaged): ?>
                                                        <span class="ml-2 rounded-full border border-cyan-400/20 bg-cyan-500/10 px-2 py-0.5 text-[11px] uppercase tracking-[0.08em] text-cyan-100">Externally managed</span>
                                                    <?php endif; ?>
                                                </td>
                                                <td class="text-right tabular-nums"><?= htmlspecialchars((string) ($row['required_qty_label'] ?? '0'), ENT_QUOTES) ?></td>
                                                <td class="text-right tabular-nums"><?= htmlspecialchars((string) ($row['local_available_qty_label'] ?? '0'), ENT_QUOTES) ?></td>
                                                <td class="text-right tabular-nums <?= $isBottleneck ? 'text-amber-100' : 'text-slate-200' ?>"><?= doctrine_format_quantity(intdiv(max(0, (int) ($row['local_available_qty'] ?? 0)), max(1, (int) ($row['quantity'] ?? 1)))) ?></td>
                                                <td class="text-right tabular-nums <?= (!$rowIsExternallyManaged && (int) ($row['missing_qty'] ?? 0) > 0) ? 'text-rose-200' : ($rowIsExternallyManaged ? 'text-cyan-100' : '') ?>"><?= htmlspecialchars((string) ($row['missing_qty_label'] ?? '0'), ENT_QUOTES) ?></td>
                                                <td class="text-right tabular-nums text-sky-300"><?= htmlspecialchars((string) ($row['hub_price_label'] ?? '—'), ENT_QUOTES) ?></td>
                                                <td class="text-right tabular-nums text-sky-300"><?= htmlspecialchars((string) ($row['local_price_label'] ?? '—'), ENT_QUOTES) ?></td>
                                                <td class="text-right tabular-nums <?= $rowIsExternallyManaged ? 'text-cyan-100' : 'text-sky-100' ?>"><?= htmlspecialchars((string) ($row['restock_gap_label'] ?? '—'), ENT_QUOTES) ?></td>
                                                <td>
                                                    <span class="rounded-full border px-2 py-0.5 text-[11px] uppercase tracking-[0.08em] <?= $statusTone((string) ($row['market_status'] ?? 'missing')) ?>">
                                                        <?= htmlspecialchars((string) ($rowIsExternallyManaged && $isBottleneck ? 'External bottleneck' : ($isBottleneck ? 'Bottleneck' : (($row['item_block_type'] ?? 'hard_blocker') === 'hard_blocker' ? ($row['market_label'] ?? 'Missing') : 'Support tracked'))), ENT_QUOTES) ?>
                                                    </span>
                                                </td>
                                            </tr>
                                        <?php endforeach; ?>
                                    </tbody>
                                </table>
                            </div>
                        </section>
                    <?php endforeach; ?>
                </div>
            <?php endif; ?>

            <div class="mt-8 grid gap-4 xl:grid-cols-3">
                <section class="surface-tertiary">
                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Raw Buy All</p>
                    <pre class="mt-3 overflow-x-auto whitespace-pre-wrap text-xs text-slate-300"><?= htmlspecialchars((string) ($fit['raw_buyall'] ?? 'Unavailable'), ENT_QUOTES) ?></pre>
                </section>
                <section class="surface-tertiary">
                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Raw EFT</p>
                    <pre class="mt-3 overflow-x-auto whitespace-pre-wrap text-xs text-slate-300"><?= htmlspecialchars((string) ($fit['raw_eft'] ?? 'Unavailable'), ENT_QUOTES) ?></pre>
                </section>
                <section class="surface-tertiary">
                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Source metadata</p>
                    <pre class="mt-3 overflow-x-auto whitespace-pre-wrap text-xs text-slate-300"><?= htmlspecialchars(json_encode((array) ($fit['metadata'] ?? []), JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE) ?: '{}', ENT_QUOTES) ?></pre>
                </section>
            </div>
        </article>
    </section>
    <!-- ui-section:doctrine-fit-items:end -->
<?php endif; ?>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
