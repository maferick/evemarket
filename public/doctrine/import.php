<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Bulk Doctrine Import';
$errorMessage = null;
$saveResult = null;

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    if (!validate_csrf($_POST['_token'] ?? null)) {
        http_response_code(419);
        exit('Invalid CSRF token');
    }

    $action = (string) ($_POST['action'] ?? 'preview');

    try {
        if ($action === 'preview') {
            $preview = doctrine_bulk_import_build_preview();
            doctrine_bulk_import_preview_store($preview);
            flash('success', 'Import preview built for ' . (int) ($preview['counts']['total'] ?? 0) . ' fits.');
            header('Location: /doctrine/import');
            exit;
        }

        if ($action === 'save_preview') {
            $saveResult = doctrine_bulk_import_save_from_request($_POST);
            if (($saveResult['ok'] ?? false) === true) {
                $results = (array) ($saveResult['results'] ?? []);
                flash('success', sprintf(
                    'Bulk import complete. %d created · %d updated · %d flagged for review · %d skipped.',
                    (int) ($results['created'] ?? 0),
                    (int) ($results['updated'] ?? 0),
                    (int) ($results['review'] ?? 0),
                    (int) ($results['skipped'] ?? 0)
                ));
                header('Location: /doctrine/fits');
                exit;
            }

            $errorMessage = (string) ($saveResult['message'] ?? 'Bulk import failed.');
        }

        if ($action === 'clear_preview') {
            doctrine_bulk_import_preview_clear();
            flash('success', 'Bulk import preview cleared.');
            header('Location: /doctrine/import');
            exit;
        }
    } catch (Throwable $exception) {
        $errorMessage = $exception->getMessage();
    }
}

$preview = doctrine_bulk_import_preview_fetch();
$groupOptions = doctrine_group_options();
$summary = (array) (($preview['counts'] ?? []));
$rows = (array) (($preview['rows'] ?? []));

include __DIR__ . '/../../src/views/partials/header.php';
?>
<section class="grid gap-6 xl:grid-cols-[minmax(0,0.95fr)_minmax(360px,1.05fr)]">
    <article class="surface-secondary">
        <div class="section-header">
            <div>
                <p class="eyebrow">HTML-first doctrine ingest</p>
                <h2 class="mt-2 section-title">Bulk Winter Coalition fit importer</h2>
                <p class="mt-2 text-sm text-slate-400">Upload Winter Coalition fit HTML pages as the primary source. SupplyCore pulls fit title, hull, doctrine memberships, notes, Buy All payloads, visible items, and EFT fallbacks into a single bulk preview before save.</p>
            </div>
            <span class="badge border-cyan-400/20 bg-cyan-500/10 text-cyan-100">Bulk control surface</span>
        </div>

        <?php if ($errorMessage !== null): ?>
            <div class="mb-5 rounded-2xl border border-amber-500/30 bg-amber-500/10 px-4 py-3 text-sm text-amber-100"><?= htmlspecialchars($errorMessage, ENT_QUOTES) ?></div>
        <?php endif; ?>

        <form method="post" enctype="multipart/form-data" class="space-y-5">
            <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
            <input type="hidden" name="action" value="preview">

            <div class="grid gap-4 md:grid-cols-2">
                <label class="block">
                    <span class="mb-2 block field-label">Winter Coalition HTML pages</span>
                    <input type="file" name="html_files[]" accept=".html,.htm,text/html" multiple class="field-input py-3">
                    <span class="mt-2 block text-xs text-slate-500">Upload one or many doctrine fit pages. HTML is treated as the source of truth for title, groups, notes, and Buy All metadata.</span>
                </label>
                <label class="block">
                    <span class="mb-2 block field-label">Optional EFT fallback files</span>
                    <input type="file" name="eft_files[]" accept=".txt,.eft,text/plain" multiple class="field-input py-3">
                    <span class="mt-2 block text-xs text-slate-500">Fallback EFT files are matched by filename or fit name and used to validate hull identity when HTML metadata is incomplete.</span>
                </label>
            </div>

            <div class="grid gap-3 md:grid-cols-3">
                <div class="surface-tertiary">
                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">1 · Upload</p>
                    <p class="mt-2 text-sm text-slate-200">HTML pages, plus optional EFT fallback files.</p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">2 · Preview</p>
                    <p class="mt-2 text-sm text-slate-200">Review conflicts, detected groups, unresolved names, and duplicate signals before anything is written.</p>
                </div>
                <div class="surface-tertiary">
                    <p class="text-xs uppercase tracking-[0.16em] text-slate-500">3 · Save</p>
                    <p class="mt-2 text-sm text-slate-200">Create, update, skip, or save flagged rows for manual review without reuploading everything.</p>
                </div>
            </div>

            <button type="submit" class="btn-primary">Build preview</button>
        </form>
    </article>

    <aside class="space-y-6">
        <article class="surface-secondary">
            <div class="section-header">
                <div>
                    <p class="eyebrow">Normalization rules</p>
                    <h2 class="mt-2 section-title">What SupplyCore persists</h2>
                </div>
            </div>
            <ul class="space-y-3 text-sm text-slate-300">
                <li class="surface-tertiary">Every fit gets exactly one persisted <span class="font-semibold text-white">Hull</span> row, even if the source omits it from visible modules.</li>
                <li class="surface-tertiary">Buy All payloads are preferred over visible item text. EFT is used as fallback and hull/identity validation.</li>
                <li class="surface-tertiary">Detected doctrine labels are preserved as many-to-many group memberships and can auto-create missing groups at save time.</li>
                <li class="surface-tertiary">Duplicate-name, duplicate-item, version, unresolved, and source-mismatch states are surfaced in preview instead of silently overwriting data.</li>
            </ul>
        </article>

        <article class="surface-secondary">
            <div class="section-header">
                <div>
                    <p class="eyebrow">After import</p>
                    <h2 class="mt-2 section-title">Bulk management</h2>
                </div>
                <a href="/doctrine/fits" class="btn-secondary">Open fit overview</a>
            </div>
            <p class="text-sm text-slate-400">The fit overview provides search, filters, sortable columns, bulk delete, bulk group assignment, review marking, and per-fit inspection of raw source and parse warnings.</p>
        </article>
    </aside>
</section>

<section class="mt-8 surface-secondary">
    <div class="section-header">
        <div>
            <p class="eyebrow">Preview queue</p>
            <h2 class="mt-2 section-title">Bulk parse results</h2>
            <p class="mt-2 text-sm text-slate-400">Preview clean fits and only spend time on the rows that need review.</p>
        </div>
        <?php if ($rows !== []): ?>
            <div class="flex flex-wrap gap-2">
                <span class="badge border-emerald-400/20 bg-emerald-500/10 text-emerald-100"><?= (int) ($summary['ready'] ?? 0) ?> ready</span>
                <span class="badge border-amber-400/20 bg-amber-500/10 text-amber-100"><?= (int) ($summary['review'] ?? 0) ?> review</span>
                <span class="badge border-cyan-400/20 bg-cyan-500/10 text-cyan-100"><?= (int) ($summary['total'] ?? 0) ?> total</span>
            </div>
        <?php endif; ?>
    </div>

    <?php if ($rows === []): ?>
        <div class="surface-tertiary text-sm text-slate-400">No import preview is active. Upload Winter Coalition HTML pages to stage a bulk ingest batch.</div>
    <?php else: ?>
        <form method="post" class="space-y-4">
            <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
            <input type="hidden" name="action" value="save_preview">

            <div class="overflow-x-auto">
                <table class="min-w-full divide-y divide-white/10 text-sm text-slate-200">
                    <thead>
                        <tr class="text-left text-xs uppercase tracking-[0.16em] text-slate-500">
                            <th class="px-3 py-3">Source</th>
                            <th class="px-3 py-3">Fit</th>
                            <th class="px-3 py-3">Groups</th>
                            <th class="px-3 py-3">Items</th>
                            <th class="px-3 py-3">Warnings</th>
                            <th class="px-3 py-3">Action</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-white/5 align-top">
                    <?php foreach ($rows as $row): ?>
                        <?php
                        $fit = (array) ($row['fit'] ?? []);
                        $warnings = (array) ($row['warnings'] ?? []);
                        $conflicts = (array) ($row['conflicts'] ?? []);
                        $sourceKey = (string) ($row['source_filename'] ?? md5((string) (($fit['fit_name'] ?? '') . '|' . ($fit['ship_name'] ?? ''))));
                        $defaultAction = (($fit['parse_status'] ?? 'ready') === 'ready' && $conflicts === []) ? 'create' : 'review';
                        ?>
                        <tr>
                            <td class="px-3 py-4">
                                <div class="space-y-2">
                                    <div class="font-medium text-white"><?= htmlspecialchars((string) ($row['source_filename'] ?? 'Uploaded fit'), ENT_QUOTES) ?></div>
                                    <span class="badge border-sky-400/20 bg-sky-500/10 text-sky-100"><?= htmlspecialchars((string) ($fit['source_type'] ?? 'html'), ENT_QUOTES) ?></span>
                                    <div class="text-xs text-slate-500"><?= htmlspecialchars((string) ($fit['source_format'] ?? 'buyall'), ENT_QUOTES) ?> normalization</div>
                                </div>
                            </td>
                            <td class="px-3 py-4">
                                <div class="space-y-2">
                                    <div>
                                        <p class="font-semibold text-white"><?= htmlspecialchars((string) ($fit['fit_name'] ?? 'Untitled fit'), ENT_QUOTES) ?></p>
                                        <p class="text-xs text-slate-400"><?= htmlspecialchars((string) ($fit['ship_name'] ?? 'Unknown hull'), ENT_QUOTES) ?></p>
                                    </div>
                                    <div class="flex flex-wrap gap-2">
                                        <span class="badge <?= (($fit['parse_status'] ?? 'ready') === 'ready') ? 'border-emerald-400/20 bg-emerald-500/10 text-emerald-100' : 'border-amber-400/20 bg-amber-500/10 text-amber-100' ?>"><?= htmlspecialchars((string) (($fit['parse_status'] ?? 'ready') === 'ready' ? 'Ready' : 'Review'), ENT_QUOTES) ?></span>
                                        <?php if (((int) ($fit['unresolved_count'] ?? 0)) > 0): ?>
                                            <span class="badge border-rose-400/20 bg-rose-500/10 text-rose-100"><?= (int) ($fit['unresolved_count'] ?? 0) ?> unresolved</span>
                                        <?php endif; ?>
                                        <?php if (($fit['conflict_state'] ?? 'none') !== 'none'): ?>
                                            <span class="badge border-fuchsia-400/20 bg-fuchsia-500/10 text-fuchsia-100"><?= htmlspecialchars((string) ($fit['conflict_state'] ?? 'conflict'), ENT_QUOTES) ?></span>
                                        <?php endif; ?>
                                    </div>
                                    <?php if ($conflicts !== []): ?>
                                        <div class="rounded-2xl border border-fuchsia-400/15 bg-fuchsia-500/5 p-3 text-xs text-fuchsia-100">
                                            <p class="font-semibold">Existing matches</p>
                                            <ul class="mt-2 space-y-2">
                                                <?php foreach ($conflicts as $conflict): ?>
                                                    <li>
                                                        <label class="flex items-start gap-2">
                                                            <input type="radio" name="row_target_fit_id[<?= htmlspecialchars($sourceKey, ENT_QUOTES) ?>]" value="<?= (int) ($conflict['id'] ?? 0) ?>" class="mt-1">
                                                            <span>#<?= (int) ($conflict['id'] ?? 0) ?> · <?= htmlspecialchars((string) ($conflict['fit_name'] ?? ''), ENT_QUOTES) ?> · <?= htmlspecialchars(implode(', ', (array) ($conflict['group_names'] ?? [])) ?: 'Ungrouped', ENT_QUOTES) ?></span>
                                                        </label>
                                                    </li>
                                                <?php endforeach; ?>
                                            </ul>
                                        </div>
                                    <?php endif; ?>
                                </div>
                            </td>
                            <td class="px-3 py-4">
                                <div class="space-y-2">
                                    <div class="flex flex-wrap gap-2">
                                        <?php foreach ((array) ($row['detected_group_labels'] ?? []) as $label): ?>
                                            <span class="badge border-white/10 bg-white/[0.04] text-slate-200"><?= htmlspecialchars((string) $label, ENT_QUOTES) ?></span>
                                        <?php endforeach; ?>
                                        <?php if ((array) ($row['detected_group_labels'] ?? []) === []): ?>
                                            <span class="text-xs text-slate-500">No HTML group labels detected</span>
                                        <?php endif; ?>
                                    </div>
                                    <select multiple name="row_group_ids[<?= htmlspecialchars($sourceKey, ENT_QUOTES) ?>][]" class="field-input min-h-[8rem]">
                                        <?php foreach ($groupOptions as $group): ?>
                                            <?php $groupId = (int) ($group['id'] ?? 0); ?>
                                            <option value="<?= $groupId ?>" <?= in_array($groupId, (array) ($row['group_ids'] ?? []), true) ? 'selected' : '' ?>><?= htmlspecialchars((string) ($group['group_name'] ?? ''), ENT_QUOTES) ?></option>
                                        <?php endforeach; ?>
                                    </select>
                                </div>
                            </td>
                            <td class="px-3 py-4">
                                <div class="space-y-2 text-xs text-slate-400">
                                    <p><span class="font-semibold text-slate-200"><?= (int) ($fit['item_count'] ?? 0) ?></span> normalized rows</p>
                                    <p><span class="font-semibold text-slate-200"><?= count((array) ($row['items'] ?? [])) ?></span> parsed lines</p>
                                    <?php if (!empty($fit['notes'])): ?>
                                        <p class="rounded-2xl border border-white/8 bg-white/[0.03] p-3 text-slate-300"><?= htmlspecialchars((string) $fit['notes'], ENT_QUOTES) ?></p>
                                    <?php endif; ?>
                                </div>
                            </td>
                            <td class="px-3 py-4">
                                <?php if ($warnings === [] && (array) ($row['unresolved'] ?? []) === []): ?>
                                    <span class="text-xs text-emerald-200">No parse warnings</span>
                                <?php else: ?>
                                    <ul class="space-y-2 text-xs text-amber-100">
                                        <?php foreach ($warnings as $warning): ?>
                                            <li class="rounded-2xl border border-amber-400/15 bg-amber-500/5 px-3 py-2"><?= htmlspecialchars((string) $warning, ENT_QUOTES) ?></li>
                                        <?php endforeach; ?>
                                        <?php foreach ((array) ($row['unresolved'] ?? []) as $unresolved): ?>
                                            <li class="rounded-2xl border border-rose-400/15 bg-rose-500/5 px-3 py-2">Unresolved: <?= htmlspecialchars((string) $unresolved, ENT_QUOTES) ?></li>
                                        <?php endforeach; ?>
                                    </ul>
                                <?php endif; ?>
                            </td>
                            <td class="px-3 py-4">
                                <div class="space-y-3">
                                    <select name="row_action[<?= htmlspecialchars($sourceKey, ENT_QUOTES) ?>]" class="field-input">
                                        <option value="create" <?= $defaultAction === 'create' ? 'selected' : '' ?>>Create new</option>
                                        <option value="update">Update existing</option>
                                        <option value="review" <?= $defaultAction === 'review' ? 'selected' : '' ?>>Save for review</option>
                                        <option value="skip">Skip</option>
                                    </select>
                                    <p class="text-xs text-slate-500">Use <span class="font-semibold text-slate-300">Save for review</span> for mismatches, unresolved names, or version conflicts you want to fix later from the fit overview.</p>
                                </div>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                    </tbody>
                </table>
            </div>

            <div class="flex flex-wrap justify-between gap-3">
                <button type="submit" class="btn-primary">Save selected actions</button>
            </div>
        </form>

        <form method="post" class="mt-4">
            <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
            <input type="hidden" name="action" value="clear_preview">
            <button type="submit" class="btn-secondary">Clear preview</button>
        </form>
    <?php endif; ?>
</section>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
