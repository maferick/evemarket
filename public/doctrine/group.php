<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$groupId = max(0, (int) ($_GET['group_id'] ?? $_POST['group_id'] ?? 0));
if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    if (!validate_csrf($_POST['_token'] ?? null)) {
        http_response_code(419);
        exit('Invalid CSRF token');
    }

    $action = (string) ($_POST['action'] ?? '');
    if ($action === 'save_group') {
        try {
            db_doctrine_group_update(
                $groupId,
                doctrine_sanitize_group_name((string) ($_POST['group_name'] ?? '')),
                doctrine_sanitize_description($_POST['description'] ?? null)
            );
            doctrine_refresh_intelligence('group-update');
            flash('success', 'Doctrine group updated successfully.');
        } catch (Throwable $exception) {
            flash('success', 'Doctrine group update failed: ' . $exception->getMessage());
        }
        header('Location: /doctrine/group?group_id=' . $groupId);
        exit;
    }

    if ($action === 'delete_group') {
        if (($_POST['confirm_delete'] ?? '') !== 'yes') {
            flash('success', 'Confirm doctrine group deletion before continuing.');
            header('Location: /doctrine/group?group_id=' . $groupId . '&confirm_delete=1');
            exit;
        }

        try {
            $result = db_doctrine_group_delete($groupId);
            doctrine_refresh_intelligence('group-delete');
            flash('success', 'Doctrine group deleted. Removed ' . doctrine_format_quantity((int) ($result['removed_memberships'] ?? 0)) . ' memberships; ' . doctrine_format_quantity((int) ($result['orphaned_fits'] ?? 0)) . ' fits remain as ungrouped records.');
            header('Location: /doctrine');
            exit;
        } catch (Throwable $exception) {
            flash('success', 'Doctrine group delete failed: ' . $exception->getMessage());
            header('Location: /doctrine/group?group_id=' . $groupId);
            exit;
        }
    }
}

$data = doctrine_group_detail_data($groupId);
$group = $data['group'] ?? null;
$fits = $data['fits'] ?? [];
$title = $group !== null ? ((string) ($group['group_name'] ?? 'Doctrine Group')) : 'Doctrine Group';
$showDeleteConfirm = isset($_GET['confirm_delete']) && $_GET['confirm_delete'] === '1';

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
            <p class="eyebrow">Readiness</p>
            <p class="mt-3 text-3xl metric-value"><?= htmlspecialchars((string) ($group['status_label'] ?? 'Market ready'), ENT_QUOTES) ?></p>
            <p class="mt-2 text-sm text-slate-300"><?= doctrine_format_quantity((int) ($group['gap_fit_count'] ?? 0)) ?> fits are below fieldable target coverage right now.</p>
        </article>
        <article class="surface-secondary">
            <p class="eyebrow">Resupply pressure</p>
            <p class="mt-3 text-3xl metric-value"><?= htmlspecialchars((string) ($group['pressure_label'] ?? 'Stable'), ENT_QUOTES) ?></p>
            <p class="mt-2 text-sm text-slate-300"><?= doctrine_format_quantity((int) ($group['loss_pressure_fit_count'] ?? 0)) ?> fits need replenishment soon or urgently.</p>
        </article>
        <article class="surface-secondary">
            <p class="eyebrow">Complete fits</p>
            <p class="mt-3 text-3xl metric-value"><?= doctrine_format_quantity((int) ($group['complete_fits_available'] ?? 0)) ?></p>
            <p class="mt-2 text-sm text-slate-300">Fleet-ready hulls that can be fielded right now from local stock.</p>
        </article>
        <article class="surface-secondary">
            <p class="eyebrow">Target fits</p>
            <p class="mt-3 text-3xl metric-value"><?= doctrine_format_quantity((int) ($group['target_fit_count'] ?? 0)) ?></p>
            <p class="mt-2 text-sm text-slate-300">Rule-based stock target across linked fits using losses and readiness trend.</p>
        </article>
        <article class="surface-secondary">
            <p class="eyebrow">Fit gap</p>
            <p class="mt-3 text-3xl metric-value"><?= doctrine_format_quantity((int) ($group['fit_gap_count'] ?? 0)) ?></p>
            <p class="mt-2 text-sm text-slate-300"><?= htmlspecialchars((string) ($group['readiness_trend'] ?? 'Stable'), ENT_QUOTES) ?> across linked fits · <?= htmlspecialchars(market_format_isk((float) ($group['restock_gap_isk'] ?? 0.0)), ENT_QUOTES) ?> restock gap.</p>
        </article>
    </section>

    <section class="mt-8 grid gap-6 xl:grid-cols-[minmax(0,1.35fr)_minmax(320px,0.85fr)]">
        <article class="surface-secondary">
            <div class="section-header">
                <div>
                    <p class="eyebrow">Fit inventory</p>
                    <div class="mt-2 flex flex-wrap items-center gap-3">
                        <h2 class="section-title"><?= htmlspecialchars((string) ($group['group_name'] ?? ''), ENT_QUOTES) ?></h2>
                        <span class="badge <?= htmlspecialchars(doctrine_supply_status_tone((string) ($group['status'] ?? 'market_ready')), ENT_QUOTES) ?>"><?= htmlspecialchars((string) ($group['status_label'] ?? 'Market ready'), ENT_QUOTES) ?></span>
                        <span class="badge <?= htmlspecialchars(doctrine_resupply_pressure_tone((string) ($group['pressure_state'] ?? 'stable')), ENT_QUOTES) ?>"><?= htmlspecialchars((string) ($group['pressure_label'] ?? 'Stable'), ENT_QUOTES) ?></span>
                    </div>
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
                        <?php $supply = (array) ($fit['supply'] ?? []); ?>
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
                                        <div class="flex flex-wrap items-center gap-3">
                                            <h3 class="text-lg font-semibold text-white"><?= htmlspecialchars((string) ($fit['fit_name'] ?? ''), ENT_QUOTES) ?></h3>
                                            <span class="rounded-full border px-2 py-0.5 text-[11px] uppercase tracking-[0.08em] <?= htmlspecialchars(doctrine_supply_status_tone((string) ($supply['readiness_state'] ?? $supply['status'] ?? 'market_ready')), ENT_QUOTES) ?>"><?= htmlspecialchars((string) ($supply['readiness_label'] ?? $supply['status_label'] ?? 'Market ready'), ENT_QUOTES) ?></span>
                                            <span class="rounded-full border px-2 py-0.5 text-[11px] uppercase tracking-[0.08em] <?= htmlspecialchars(doctrine_resupply_pressure_tone((string) ($supply['resupply_pressure_state'] ?? 'stable')), ENT_QUOTES) ?>"><?= htmlspecialchars((string) ($supply['resupply_pressure_label'] ?? 'Stable'), ENT_QUOTES) ?></span>
                                            <?php if (!empty($supply['externally_managed'])): ?>
                                                <span class="rounded-full border border-cyan-400/20 bg-cyan-500/10 px-2 py-0.5 text-[11px] uppercase tracking-[0.08em] text-cyan-100">Externally managed</span>
                                            <?php endif; ?>
                                        </div>
                                        <p class="mt-1 text-sm text-slate-400"><?= htmlspecialchars((string) ($fit['ship_name'] ?? ''), ENT_QUOTES) ?></p>
                                    </div>
                                </div>
                                <span class="badge border-sky-400/18 bg-sky-500/10 text-sky-100"><?= htmlspecialchars(strtoupper((string) ($fit['source_format'] ?? 'buyall')), ENT_QUOTES) ?></span>
                            </div>
                            <div class="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                                <div class="surface-tertiary">
                                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Complete fits</p>
                                    <p class="mt-2 text-xl font-semibold text-slate-100"><?= doctrine_format_quantity((int) ($supply['complete_fits_available'] ?? 0)) ?></p>
                                </div>
                                <div class="surface-tertiary">
                                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Target fits</p>
                                    <p class="mt-2 text-xl font-semibold text-slate-100"><?= doctrine_format_quantity((int) ($supply['recommended_target_fit_count'] ?? 0)) ?></p>
                                </div>
                                <div class="surface-tertiary">
                                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Fit gap</p>
                                    <p class="mt-2 text-xl font-semibold text-rose-200"><?= doctrine_format_quantity((int) ($supply['gap_to_target_fit_count'] ?? 0)) ?></p>
                                </div>
                                <div class="surface-tertiary">
                                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Trend</p>
                                    <p class="mt-2 text-sm font-semibold text-sky-100"><?= htmlspecialchars((string) ($supply['readiness_trend'] ?? 'Trend unavailable'), ENT_QUOTES) ?></p>
                                    <p class="mt-1 text-xs text-slate-500"><?= htmlspecialchars((string) ($supply['planning_context'] ?? ''), ENT_QUOTES) ?></p>
                                </div>
                            </div>
                            <div class="mt-3 grid gap-3 xl:grid-cols-2">
                                <div class="surface-tertiary">
                                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Bottleneck</p>
                                    <p class="mt-2 text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) ($supply['bottleneck_item_name'] ?? 'Unavailable'), ENT_QUOTES) ?></p>
                                    <p class="mt-1 text-xs text-slate-500"><?= doctrine_format_quantity((int) ($supply['bottleneck_quantity'] ?? 0)) ?> local for <?= doctrine_format_quantity((int) ($supply['bottleneck_required_quantity'] ?? 0)) ?> required per fit<?= !empty($supply['external_bottleneck']) ? ' · External bottleneck' : '' ?></p>
                                </div>
                                <div class="surface-tertiary">
                                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Loss-aware view</p>
                                    <p class="mt-2 text-sm font-semibold text-slate-100"><?= doctrine_format_quantity((int) ($supply['recent_hull_losses_7d'] ?? 0)) ?> hull losses · <?= doctrine_format_quantity((int) ($supply['recent_item_fit_losses_7d'] ?? 0)) ?> fit-equivalent item losses</p>
                                    <p class="mt-1 text-xs text-slate-500"><?= htmlspecialchars((string) ($supply['restock_trend'] ?? 'Restock trend unavailable'), ENT_QUOTES) ?></p>
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
                        <p class="eyebrow">Group management</p>
                        <h2 class="mt-2 section-title">Edit doctrine group</h2>
                    </div>
                    <span class="badge border-cyan-400/18 bg-cyan-500/10 text-cyan-100">Full CRUD</span>
                </div>
                <form method="post" class="space-y-4">
                    <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                    <input type="hidden" name="action" value="save_group">
                    <input type="hidden" name="group_id" value="<?= (int) ($group['id'] ?? 0) ?>">
                    <label class="block">
                        <span class="mb-2 block field-label">Group name</span>
                        <input type="text" name="group_name" class="field-input" maxlength="190" value="<?= htmlspecialchars((string) ($group['group_name'] ?? ''), ENT_QUOTES) ?>">
                    </label>
                    <label class="block">
                        <span class="mb-2 block field-label">Description</span>
                        <textarea name="description" class="field-input" style="min-height: 10rem;"><?= htmlspecialchars((string) ($group['description'] ?? ''), ENT_QUOTES) ?></textarea>
                    </label>
                    <button type="submit" class="btn-primary w-full justify-center">Save changes</button>
                </form>
            </article>

            <article class="surface-secondary">
                <div class="section-header">
                    <div>
                        <p class="eyebrow">Delete group</p>
                        <h2 class="mt-2 section-title">Remove doctrine group</h2>
                        <p class="mt-2 text-sm text-slate-400">Deleting this group removes only the fit-to-group memberships for this doctrine group. Fits that still belong to other groups remain there; fits with no remaining memberships are preserved as ungrouped fits for cleanup.</p>
                    </div>
                </div>
                <form method="post" class="space-y-4">
                    <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                    <input type="hidden" name="action" value="delete_group">
                    <input type="hidden" name="group_id" value="<?= (int) ($group['id'] ?? 0) ?>">
                    <label class="flex items-start gap-3 text-sm text-slate-300">
                        <input type="checkbox" name="confirm_delete" value="yes" class="mt-1" <?= $showDeleteConfirm ? '' : '' ?>>
                        <span>I understand this will remove this group and detach its memberships.</span>
                    </label>
                    <button type="submit" class="btn-secondary w-full justify-center border-rose-400/30 text-rose-200 hover:bg-rose-500/10">Delete doctrine group</button>
                </form>
            </article>
        </aside>
    </section>
<?php endif; ?>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
