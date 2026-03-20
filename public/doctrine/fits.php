<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Doctrine Fit Overview';
$errorMessage = null;

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    if (!validate_csrf($_POST['_token'] ?? null)) {
        http_response_code(419);
        exit('Invalid CSRF token');
    }

    $fitIds = array_values(array_unique(array_filter(array_map('intval', (array) ($_POST['fit_ids'] ?? [])), static fn (int $id): bool => $id > 0)));
    $bulkAction = (string) ($_POST['bulk_action'] ?? '');
    $selectedGroupIds = array_values(array_unique(array_filter(array_map('intval', (array) ($_POST['bulk_group_ids'] ?? [])), static fn (int $id): bool => $id > 0)));
    $bulkNotes = trim((string) ($_POST['bulk_notes'] ?? ''));

    try {
        if ($fitIds === [] && $bulkAction !== '') {
            throw new RuntimeException('Select at least one fit before running a bulk action.');
        }

        switch ($bulkAction) {
            case 'delete':
                db_doctrine_fit_bulk_delete($fitIds);
                doctrine_schedule_intelligence_refresh('fit-bulk-delete');
                flash('success', 'Deleted ' . count($fitIds) . ' doctrine fits.');
                break;
            case 'assign_groups':
                db_doctrine_fit_bulk_assign_groups($fitIds, $selectedGroupIds, false);
                doctrine_schedule_intelligence_refresh('fit-bulk-assign-groups');
                flash('success', 'Assigned groups to ' . count($fitIds) . ' doctrine fits.');
                break;
            case 'replace_groups':
                db_doctrine_fit_bulk_assign_groups($fitIds, $selectedGroupIds, true);
                doctrine_schedule_intelligence_refresh('fit-bulk-replace-groups');
                flash('success', 'Replaced doctrine group memberships for ' . count($fitIds) . ' fits.');
                break;
            case 'remove_groups':
                db_doctrine_fit_bulk_remove_groups($fitIds, $selectedGroupIds);
                doctrine_schedule_intelligence_refresh('fit-bulk-remove-groups');
                flash('success', 'Removed selected doctrine groups from ' . count($fitIds) . ' fits.');
                break;
            case 'mark_review':
                db_doctrine_fit_bulk_update_metadata($fitIds, ['parse_status' => 'review', 'review_status' => 'needs_review']);
                flash('success', 'Marked ' . count($fitIds) . ' fits for review.');
                break;
            case 'mark_reparse':
                db_doctrine_fit_bulk_update_metadata($fitIds, ['parse_status' => 'review', 'review_status' => 'reparse_requested']);
                flash('success', 'Marked ' . count($fitIds) . ' fits for reparse.');
                break;
            case 'append_notes':
                if ($bulkNotes === '') {
                    throw new RuntimeException('Enter a note before using the bulk metadata update.');
                }
                db_doctrine_fit_bulk_update_metadata($fitIds, ['notes' => $bulkNotes]);
                flash('success', 'Updated notes for ' . count($fitIds) . ' fits.');
                break;
        }

        if ($bulkAction !== '') {
            header('Location: /doctrine/fits?' . http_build_query($_GET));
            exit;
        }
    } catch (Throwable $exception) {
        $errorMessage = $exception->getMessage();
    }
}

$data = doctrine_fit_overview_data($_GET);
$fits = (array) ($data['fits'] ?? []);
$filters = (array) ($data['filters'] ?? []);
$sort = (string) ($data['sort'] ?? 'updated_desc');
$groupOptions = (array) ($data['groups'] ?? []);
$summary = (array) ($data['summary'] ?? []);

include __DIR__ . '/../../src/views/partials/header.php';
?>
<section class="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
    <article class="surface-secondary"><p class="eyebrow">Imported fits</p><p class="mt-3 text-3xl metric-value"><?= (int) ($summary['total'] ?? 0) ?></p><p class="mt-2 text-sm text-slate-400">Searchable doctrine-fit registry.</p></article>
    <article class="surface-secondary"><p class="eyebrow">Needs review</p><p class="mt-3 text-3xl metric-value text-amber-200"><?= (int) ($summary['review'] ?? 0) ?></p><p class="mt-2 text-sm text-slate-400">Rows carrying warnings, mismatches, or explicit review flags.</p></article>
    <article class="surface-secondary"><p class="eyebrow">Unresolved</p><p class="mt-3 text-3xl metric-value text-rose-200"><?= (int) ($summary['unresolved'] ?? 0) ?></p><p class="mt-2 text-sm text-slate-400">Fits with unresolved hull or item names.</p></article>
    <article class="surface-secondary"><p class="eyebrow">Conflicts</p><p class="mt-3 text-3xl metric-value text-fuchsia-200"><?= (int) ($summary['conflicts'] ?? 0) ?></p><p class="mt-2 text-sm text-slate-400">Potential duplicates, version drift, or source mismatches.</p></article>
</section>

<section class="mt-8 surface-secondary">
    <div class="section-header">
        <div>
            <p class="eyebrow">Fit management surface</p>
            <h2 class="mt-2 section-title">Doctrine fit overview</h2>
            <p class="mt-2 text-sm text-slate-400">Filter and operate on large batches of imported doctrine fits without dropping into one-by-one edits.</p>
        </div>
        <a href="/doctrine/import" class="btn-primary">Open bulk importer</a>
    </div>

    <?php if ($errorMessage !== null): ?>
        <div class="mb-5 rounded-2xl border border-amber-500/30 bg-amber-500/10 px-4 py-3 text-sm text-amber-100"><?= htmlspecialchars($errorMessage, ENT_QUOTES) ?></div>
    <?php endif; ?>

    <form method="get" class="grid gap-4 rounded-[1.4rem] border border-white/8 bg-white/[0.03] p-4 md:grid-cols-3 xl:grid-cols-6">
        <label class="block xl:col-span-2">
            <span class="mb-2 block field-label">Search</span>
            <input type="text" name="q" class="field-input" value="<?= htmlspecialchars((string) ($filters['search'] ?? ''), ENT_QUOTES) ?>" placeholder="Fit name, hull, note, source file">
        </label>
        <label class="block">
            <span class="mb-2 block field-label">Doctrine group</span>
            <select name="group_id" class="field-input">
                <option value="0">All groups</option>
                <?php foreach ($groupOptions as $group): ?>
                    <option value="<?= (int) ($group['id'] ?? 0) ?>" <?= (int) ($filters['group_id'] ?? 0) === (int) ($group['id'] ?? 0) ? 'selected' : '' ?>><?= htmlspecialchars((string) ($group['group_name'] ?? ''), ENT_QUOTES) ?></option>
                <?php endforeach; ?>
            </select>
        </label>
        <label class="block">
            <span class="mb-2 block field-label">Source type</span>
            <select name="source_type" class="field-input">
                <?php foreach (['' => 'All', 'html' => 'HTML', 'eft' => 'EFT', 'buyall' => 'BuyAll', 'manual' => 'Manual'] as $value => $label): ?>
                    <option value="<?= htmlspecialchars($value, ENT_QUOTES) ?>" <?= (string) ($filters['source_type'] ?? '') === $value ? 'selected' : '' ?>><?= htmlspecialchars($label, ENT_QUOTES) ?></option>
                <?php endforeach; ?>
            </select>
        </label>
        <label class="block">
            <span class="mb-2 block field-label">Parse status</span>
            <select name="parse_status" class="field-input">
                <?php foreach (['' => 'All', 'ready' => 'Ready', 'review' => 'Review'] as $value => $label): ?>
                    <option value="<?= htmlspecialchars($value, ENT_QUOTES) ?>" <?= (string) ($filters['parse_status'] ?? '') === $value ? 'selected' : '' ?>><?= htmlspecialchars($label, ENT_QUOTES) ?></option>
                <?php endforeach; ?>
            </select>
        </label>
        <label class="block">
            <span class="mb-2 block field-label">Conflict state</span>
            <select name="conflict_state" class="field-input">
                <?php foreach (['' => 'All', 'has_conflict' => 'Any conflict', 'duplicate_name' => 'Duplicate name', 'duplicate_items' => 'Duplicate items', 'version_conflict' => 'Version conflict', 'source_mismatch' => 'Source mismatch'] as $value => $label): ?>
                    <option value="<?= htmlspecialchars($value, ENT_QUOTES) ?>" <?= (string) ($filters['conflict_state'] ?? '') === $value ? 'selected' : '' ?>><?= htmlspecialchars($label, ENT_QUOTES) ?></option>
                <?php endforeach; ?>
            </select>
        </label>
        <label class="block">
            <span class="mb-2 block field-label">Hull</span>
            <input type="text" name="hull" class="field-input" value="<?= htmlspecialchars((string) ($filters['hull'] ?? ''), ENT_QUOTES) ?>" placeholder="Hull filter">
        </label>
        <label class="flex items-end gap-3">
            <input type="checkbox" name="unresolved_only" value="1" <?= !empty($filters['unresolved_only']) ? 'checked' : '' ?>>
            <span class="text-sm text-slate-300">Unresolved only</span>
        </label>
        <label class="block">
            <span class="mb-2 block field-label">Sort</span>
            <select name="sort" class="field-input">
                <?php foreach (['updated_desc' => 'Last updated', 'fit_name_asc' => 'Fit name', 'hull_asc' => 'Hull', 'groups_desc' => 'Group count', 'items_desc' => 'Item count', 'warnings_desc' => 'Warnings', 'status_asc' => 'Status'] as $value => $label): ?>
                    <option value="<?= htmlspecialchars($value, ENT_QUOTES) ?>" <?= $sort === $value ? 'selected' : '' ?>><?= htmlspecialchars($label, ENT_QUOTES) ?></option>
                <?php endforeach; ?>
            </select>
        </label>
        <div class="flex items-end gap-3 xl:col-span-2">
            <button type="submit" class="btn-primary">Apply filters</button>
            <a href="/doctrine/fits" class="btn-secondary">Reset</a>
        </div>
    </form>

    <form method="post" class="mt-6 space-y-4">
        <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">

        <div class="grid gap-4 rounded-[1.4rem] border border-white/8 bg-white/[0.03] p-4 xl:grid-cols-[minmax(220px,0.7fr)_minmax(220px,0.7fr)_minmax(220px,1fr)_auto]">
            <label class="block">
                <span class="mb-2 block field-label">Bulk action</span>
                <select name="bulk_action" class="field-input">
                    <option value="">Choose action</option>
                    <option value="delete">Bulk delete selected fits</option>
                    <option value="assign_groups">Bulk assign doctrine groups</option>
                    <option value="replace_groups">Bulk move/replace doctrine groups</option>
                    <option value="remove_groups">Bulk remove doctrine groups</option>
                    <option value="append_notes">Bulk edit metadata notes</option>
                    <option value="mark_review">Bulk mark fits for review</option>
                    <option value="mark_reparse">Bulk mark fits for reparse</option>
                </select>
            </label>
            <label class="block">
                <span class="mb-2 block field-label">Doctrine groups</span>
                <select name="bulk_group_ids[]" multiple class="field-input min-h-[8rem]">
                    <?php foreach ($groupOptions as $group): ?>
                        <option value="<?= (int) ($group['id'] ?? 0) ?>"><?= htmlspecialchars((string) ($group['group_name'] ?? ''), ENT_QUOTES) ?></option>
                    <?php endforeach; ?>
                </select>
            </label>
            <label class="block">
                <span class="mb-2 block field-label">Metadata note</span>
                <textarea name="bulk_notes" class="field-input" style="min-height: 8rem;" placeholder="Use for practical bulk metadata edits such as reviewer notes or next-step instructions."></textarea>
            </label>
            <div class="flex items-end">
                <button type="submit" class="btn-primary">Run bulk action</button>
            </div>
        </div>

        <div class="overflow-x-auto">
            <table class="min-w-full divide-y divide-white/10 text-sm text-slate-200">
                <thead>
                    <tr class="text-left text-xs uppercase tracking-[0.16em] text-slate-500">
                        <th class="px-3 py-3"><input type="checkbox" onclick="document.querySelectorAll('input[name=&quot;fit_ids[]&quot;]').forEach((el)=>el.checked=this.checked)"></th>
                        <th class="px-3 py-3">Fit</th>
                        <th class="px-3 py-3">Hull</th>
                        <th class="px-3 py-3">Groups</th>
                        <th class="px-3 py-3">Items</th>
                        <th class="px-3 py-3">Source</th>
                        <th class="px-3 py-3">Status</th>
                        <th class="px-3 py-3">Updated</th>
                        <th class="px-3 py-3">Actions</th>
                    </tr>
                </thead>
                <tbody class="divide-y divide-white/5 align-top">
                    <?php foreach ($fits as $fit): ?>
                        <tr>
                            <td class="px-3 py-4"><input type="checkbox" name="fit_ids[]" value="<?= (int) ($fit['id'] ?? 0) ?>"></td>
                            <td class="px-3 py-4">
                                <p class="font-semibold text-white"><?= htmlspecialchars((string) ($fit['fit_name'] ?? ''), ENT_QUOTES) ?></p>
                                <p class="mt-1 text-xs text-slate-500">#<?= (int) ($fit['id'] ?? 0) ?><?= !empty($fit['source_reference']) ? ' · ' . htmlspecialchars((string) $fit['source_reference'], ENT_QUOTES) : '' ?></p>
                            </td>
                            <td class="px-3 py-4"><?= htmlspecialchars((string) ($fit['ship_name'] ?? ''), ENT_QUOTES) ?></td>
                            <td class="px-3 py-4">
                                <div class="flex flex-wrap gap-2">
                                    <?php foreach ((array) ($fit['group_names'] ?? []) as $groupName): ?>
                                        <span class="badge border-white/10 bg-white/[0.04] text-slate-200"><?= htmlspecialchars((string) $groupName, ENT_QUOTES) ?></span>
                                    <?php endforeach; ?>
                                    <?php if ((array) ($fit['group_names'] ?? []) === []): ?>
                                        <span class="text-xs text-slate-500">Ungrouped</span>
                                    <?php endif; ?>
                                </div>
                            </td>
                            <td class="px-3 py-4 text-xs text-slate-400">
                                <p><?= (int) ($fit['item_count'] ?? 0) ?> declared</p>
                                <p><?= (int) ($fit['normalized_item_rows'] ?? 0) ?> stored</p>
                                <p><?= (int) ($fit['group_count'] ?? 0) ?> groups</p>
                            </td>
                            <td class="px-3 py-4">
                                <span class="badge border-sky-400/20 bg-sky-500/10 text-sky-100"><?= htmlspecialchars((string) ($fit['source_type'] ?? 'manual'), ENT_QUOTES) ?></span>
                            </td>
                            <td class="px-3 py-4">
                                <div class="space-y-2">
                                    <span class="badge <?= (($fit['parse_status'] ?? 'ready') === 'ready') ? 'border-emerald-400/20 bg-emerald-500/10 text-emerald-100' : 'border-amber-400/20 bg-amber-500/10 text-amber-100' ?>"><?= htmlspecialchars((string) ($fit['readiness_status'] ?? 'Ready'), ENT_QUOTES) ?></span>
                                    <?php if (((int) ($fit['warning_count'] ?? 0)) > 0): ?>
                                        <p class="text-xs text-amber-100"><?= (int) ($fit['warning_count'] ?? 0) ?> warnings</p>
                                    <?php endif; ?>
                                    <?php if (((int) ($fit['unresolved_count'] ?? 0)) > 0): ?>
                                        <p class="text-xs text-rose-100"><?= (int) ($fit['unresolved_count'] ?? 0) ?> unresolved</p>
                                    <?php endif; ?>
                                    <?php if (($fit['conflict_state'] ?? 'none') !== 'none'): ?>
                                        <p class="text-xs text-fuchsia-100"><?= htmlspecialchars((string) ($fit['conflict_state'] ?? ''), ENT_QUOTES) ?></p>
                                    <?php endif; ?>
                                </div>
                            </td>
                            <td class="px-3 py-4 text-xs text-slate-400"><?= htmlspecialchars((string) ($fit['updated_at'] ?? ''), ENT_QUOTES) ?></td>
                            <td class="px-3 py-4">
                                <div class="flex flex-wrap gap-2">
                                    <a href="/doctrine/fit?fit_id=<?= (int) ($fit['id'] ?? 0) ?>" class="btn-secondary">Inspect</a>
                                </div>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                    <?php if ($fits === []): ?>
                        <tr><td colspan="9" class="px-3 py-8 text-center text-sm text-slate-400">No doctrine fits matched the current filters.</td></tr>
                    <?php endif; ?>
                </tbody>
            </table>
        </div>
    </form>
</section>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
