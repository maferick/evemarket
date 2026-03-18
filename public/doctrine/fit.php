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
            supplycore_cache_bust(['doctrine', 'dashboard']);
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
$fit = $data['fit'] ?? null;
$categories = $data['categories'] ?? [];
$summary = $data['summary'] ?? [];
$title = $fit !== null ? ((string) ($fit['fit_name'] ?? 'Doctrine Fit')) : 'Doctrine Fit';
$showDeleteConfirm = isset($_GET['confirm_delete']) && $_GET['confirm_delete'] === '1';

if ($fit !== null && $editDraft === null) {
    $items = $data['items'] ?? [];
    $editDraft = [
        'fit' => [
            'fit_name' => (string) ($fit['fit_name'] ?? ''),
            'ship_name' => (string) ($fit['ship_name'] ?? ''),
            'source_format' => (string) ($fit['source_format'] ?? 'buyall'),
            'import_body' => (string) ($fit['import_body'] ?? ''),
        ],
        'item_lines_text' => doctrine_render_editable_item_lines((array) $items),
        'group_ids' => (array) ($fit['group_ids'] ?? []),
        'unresolved' => [],
    ];
}

$statusTone = static function (string $status): string {
    return match ($status) {
        'ok' => 'border-emerald-400/20 bg-emerald-500/10 text-emerald-100',
        'low' => 'border-amber-400/20 bg-amber-500/10 text-amber-100',
        default => 'border-rose-400/20 bg-rose-500/10 text-rose-200',
    };
};

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
    <section class="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <?php foreach ($summary as $card): ?>
            <article class="surface-secondary">
                <p class="eyebrow"><?= htmlspecialchars((string) ($card['label'] ?? ''), ENT_QUOTES) ?></p>
                <p class="mt-3 text-3xl metric-value"><?= htmlspecialchars((string) ($card['value'] ?? ''), ENT_QUOTES) ?></p>
                <p class="mt-2 text-sm text-slate-300"><?= htmlspecialchars((string) ($card['context'] ?? ''), ENT_QUOTES) ?></p>
            </article>
        <?php endforeach; ?>
    </section>

    <section class="mt-8 grid gap-6 xl:grid-cols-[minmax(280px,360px)_minmax(0,1fr)]">
        <aside class="space-y-6">
            <article class="surface-secondary">
                <div class="section-header">
                    <div>
                        <p class="eyebrow">Hull profile</p>
                        <div class="mt-2 flex flex-wrap items-center gap-3">
                            <h2 class="section-title"><?= htmlspecialchars((string) ($fit['ship_name'] ?? ''), ENT_QUOTES) ?></h2>
                            <span class="badge <?= htmlspecialchars(doctrine_supply_status_tone((string) ($supply['status'] ?? 'critical')), ENT_QUOTES) ?>"><?= htmlspecialchars((string) ($supply['status_label'] ?? 'Supply gap'), ENT_QUOTES) ?></span>
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
                        <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Complete fits available</p>
                        <p class="mt-2 font-semibold text-slate-100"><?= doctrine_format_quantity((int) ($supply['complete_fits_available'] ?? 0)) ?></p>
                        <p class="mt-1 text-xs text-slate-500"><?= htmlspecialchars((string) ($supply['constraint_label'] ?? ''), ENT_QUOTES) ?></p>
                    </div>
                    <div class="surface-tertiary">
                        <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Target fit count</p>
                        <p class="mt-2 font-semibold text-slate-100"><?= doctrine_format_quantity((int) ($supply['recommended_target_fit_count'] ?? 0)) ?></p>
                        <p class="mt-1 text-xs text-slate-500"><?= doctrine_format_quantity((int) ($supply['gap_to_target_fit_count'] ?? 0)) ?> fits short of target</p>
                    </div>
                    <div class="surface-tertiary border <?= htmlspecialchars(doctrine_supply_status_tone((string) ($supply['status'] ?? 'critical')), ENT_QUOTES) ?>">
                        <p class="text-xs uppercase tracking-[0.16em]">Operational outcome</p>
                        <p class="mt-2 text-sm <?= ($supply['likely_enough_based_on_recent_losses'] ?? false) ? 'text-emerald-100' : 'text-rose-100' ?>"><?= htmlspecialchars((string) ($supply['planning_context'] ?? 'Operational recommendation unavailable.'), ENT_QUOTES) ?></p>
                        <p class="mt-2 text-xs text-slate-300">Trend: <?= htmlspecialchars((string) ($supply['readiness_trend'] ?? 'Trend unavailable'), ENT_QUOTES) ?> · Restock: <?= htmlspecialchars((string) ($supply['restock_trend'] ?? 'Unavailable'), ENT_QUOTES) ?> · 7d hull losses: <?= doctrine_format_quantity((int) ($supply['recent_hull_losses_7d'] ?? 0)) ?>.</p>
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
                    <p class="mt-2 text-sm text-slate-400">Doctrine readiness now highlights the bottleneck item, complete-fit ceiling, and whether local stock is keeping pace with recent tracked losses.</p>
                </div>
                <span class="badge <?= htmlspecialchars(doctrine_supply_status_tone((string) ($supply['status'] ?? 'critical')), ENT_QUOTES) ?>"><?= htmlspecialchars((string) ($supply['status_label'] ?? 'Supply gap'), ENT_QUOTES) ?></span>
            </div>

            <div class="mb-6 grid gap-4 xl:grid-cols-2">
                <div class="surface-tertiary">
                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Bottleneck item</p>
                    <p class="mt-2 text-lg font-semibold text-slate-50"><?= htmlspecialchars((string) ($supply['bottleneck_item_name'] ?? 'Unavailable'), ENT_QUOTES) ?></p>
                    <p class="mt-1 text-sm text-slate-400"><?= doctrine_format_quantity((int) ($supply['bottleneck_quantity'] ?? 0)) ?> local units for <?= doctrine_format_quantity((int) ($supply['bottleneck_required_quantity'] ?? 0)) ?> required per fit · minimum stock constraint <?= doctrine_format_quantity((int) ($supply['minimum_stock_constraint'] ?? 0)) ?> complete fits.</p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Loss-aware planning</p>
                    <p class="mt-2 text-lg font-semibold text-slate-50"><?= doctrine_format_quantity((int) ($supply['recent_hull_losses_24h'] ?? 0)) ?> hull losses (24h) · <?= doctrine_format_quantity((int) ($supply['recent_hull_losses_7d'] ?? 0)) ?> hull losses (7d)</p>
                    <p class="mt-1 text-sm text-slate-400"><?= doctrine_format_quantity((int) ($supply['recent_item_fit_losses_24h'] ?? 0)) ?> / <?= doctrine_format_quantity((int) ($supply['recent_item_fit_losses_7d'] ?? 0)) ?> fit-equivalent item losses from tracked victim killmails. <?= htmlspecialchars((string) ($supply['readiness_trend_context'] ?? ''), ENT_QUOTES) ?></p>
                </div>
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
                                            <tr class="<?= ((int) ($row['missing_qty'] ?? 0) > 0) ? 'bg-rose-900/10' : ($isBottleneck ? 'bg-amber-900/10' : '') ?>">
                                                <td class="font-semibold text-slate-50"><?= htmlspecialchars((string) ($row['item_name'] ?? ''), ENT_QUOTES) ?></td>
                                                <td class="text-right tabular-nums"><?= htmlspecialchars((string) ($row['required_qty_label'] ?? '0'), ENT_QUOTES) ?></td>
                                                <td class="text-right tabular-nums"><?= htmlspecialchars((string) ($row['local_available_qty_label'] ?? '0'), ENT_QUOTES) ?></td>
                                                <td class="text-right tabular-nums <?= $isBottleneck ? 'text-amber-100' : 'text-slate-200' ?>"><?= doctrine_format_quantity(intdiv(max(0, (int) ($row['local_available_qty'] ?? 0)), max(1, (int) ($row['quantity'] ?? 1)))) ?></td>
                                                <td class="text-right tabular-nums <?= ((int) ($row['missing_qty'] ?? 0) > 0) ? 'text-rose-200' : '' ?>"><?= htmlspecialchars((string) ($row['missing_qty_label'] ?? '0'), ENT_QUOTES) ?></td>
                                                <td class="text-right tabular-nums text-sky-300"><?= htmlspecialchars((string) ($row['hub_price_label'] ?? '—'), ENT_QUOTES) ?></td>
                                                <td class="text-right tabular-nums text-sky-300"><?= htmlspecialchars((string) ($row['local_price_label'] ?? '—'), ENT_QUOTES) ?></td>
                                                <td class="text-right tabular-nums text-sky-100"><?= htmlspecialchars((string) ($row['restock_gap_label'] ?? '—'), ENT_QUOTES) ?></td>
                                                <td>
                                                    <span class="rounded-full border px-2 py-0.5 text-[11px] uppercase tracking-[0.08em] <?= $statusTone((string) ($row['market_status'] ?? 'missing')) ?>">
                                                        <?= htmlspecialchars((string) ($isBottleneck ? 'Bottleneck' : ($row['market_label'] ?? 'Missing')), ENT_QUOTES) ?>
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
        </article>
    </section>
<?php endif; ?>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
