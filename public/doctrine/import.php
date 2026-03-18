<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Import Doctrine Fit';
$groupOptions = doctrine_group_options();
$defaultGroupId = max(0, (int) ($_GET['group_id'] ?? 0));
if ($defaultGroupId <= 0 && $groupOptions !== []) {
    $defaultGroupId = (int) ($groupOptions[0]['id'] ?? 0);
}

$formValues = [
    'group_ids' => $defaultGroupId > 0 ? [$defaultGroupId] : [],
    'new_group_name' => '',
    'new_group_description' => '',
    'fit_payload' => '',
    'fit_name' => '',
    'ship_name' => '',
    'source_format' => 'buyall',
    'import_body' => '',
    'item_lines_text' => '',
];
$previewDraft = null;
$errorMessage = null;

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    if (!validate_csrf($_POST['_token'] ?? null)) {
        http_response_code(419);
        exit('Invalid CSRF token');
    }

    $formValues = [
        'group_ids' => array_values(array_map('intval', (array) ($_POST['group_ids'] ?? []))),
        'new_group_name' => (string) ($_POST['new_group_name'] ?? ''),
        'new_group_description' => (string) ($_POST['new_group_description'] ?? ''),
        'fit_payload' => (string) ($_POST['fit_payload'] ?? ''),
        'fit_name' => (string) ($_POST['fit_name'] ?? ''),
        'ship_name' => (string) ($_POST['ship_name'] ?? ''),
        'source_format' => (string) ($_POST['source_format'] ?? 'buyall'),
        'import_body' => (string) ($_POST['import_body'] ?? ''),
        'item_lines_text' => (string) ($_POST['item_lines_text'] ?? ''),
    ];

    if (($_POST['action'] ?? 'preview') === 'save') {
        $result = doctrine_import_fit_from_request($_POST);
        if (($result['ok'] ?? false) === true) {
            flash('success', (string) ($result['message'] ?? 'Doctrine fit imported successfully.'));
            header('Location: /doctrine/fit?fit_id=' . (int) ($result['fit_id'] ?? 0));
            exit;
        }

        $errorMessage = (string) ($result['message'] ?? 'Doctrine import failed.');
        $previewDraft = $result['draft'] ?? null;
    } else {
        try {
            $previewDraft = doctrine_build_draft_from_import_request($_POST);
            $formValues['fit_name'] = (string) ($previewDraft['fit']['fit_name'] ?? '');
            $formValues['ship_name'] = (string) ($previewDraft['fit']['ship_name'] ?? '');
            $formValues['source_format'] = (string) ($previewDraft['fit']['source_format'] ?? 'buyall');
            $formValues['import_body'] = (string) ($previewDraft['fit']['import_body'] ?? $formValues['fit_payload']);
            $formValues['item_lines_text'] = (string) ($previewDraft['item_lines_text'] ?? '');
            $formValues['group_ids'] = (array) ($previewDraft['group_ids'] ?? $formValues['group_ids']);
        } catch (Throwable $exception) {
            $errorMessage = $exception->getMessage();
        }
    }
}

include __DIR__ . '/../../src/views/partials/header.php';
?>
<section class="grid gap-6 xl:grid-cols-[minmax(0,1.15fr)_minmax(320px,0.85fr)]">
    <article class="surface-secondary">
        <div class="section-header">
            <div>
                <p class="eyebrow">Doctrine ingest</p>
                <h2 class="mt-2 section-title">Import alliance fitting payload</h2>
                <p class="mt-2 text-sm text-slate-400">Stage the fit first, then confirm the resolved hull, doctrine groups, and normalized item lines before anything is saved.</p>
            </div>
            <span class="badge border-emerald-400/20 bg-emerald-500/10 text-emerald-100">Two-step import</span>
        </div>

        <?php if ($errorMessage !== null): ?>
            <div class="mb-5 rounded-2xl border border-amber-500/30 bg-amber-500/10 px-4 py-3 text-sm text-amber-100"><?= htmlspecialchars($errorMessage, ENT_QUOTES) ?></div>
        <?php endif; ?>

        <form method="post" class="space-y-4">
            <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
            <input type="hidden" name="action" value="preview">
            <div class="grid gap-4 md:grid-cols-2">
                <div>
                    <span class="mb-2 block field-label">Doctrine groups</span>
                    <div class="space-y-2 rounded-[1.2rem] border border-white/8 bg-slate-950/50 p-4">
                        <?php foreach ($groupOptions as $group): ?>
                            <?php $groupId = (int) ($group['id'] ?? 0); ?>
                            <label class="flex items-start gap-3 text-sm text-slate-300">
                                <input type="checkbox" name="group_ids[]" value="<?= $groupId ?>" class="mt-1" <?= in_array($groupId, (array) $formValues['group_ids'], true) ? 'checked' : '' ?>>
                                <span>
                                    <span class="font-medium text-slate-100"><?= htmlspecialchars((string) ($group['group_name'] ?? ''), ENT_QUOTES) ?></span>
                                    <span class="mt-1 block text-xs text-slate-500"><?= htmlspecialchars((string) ($group['description'] ?? 'Doctrine group'), ENT_QUOTES) ?></span>
                                </span>
                            </label>
                        <?php endforeach; ?>
                    </div>
                </div>
                <div class="space-y-4">
                    <label class="block">
                        <span class="mb-2 block field-label">New group name</span>
                        <input type="text" name="new_group_name" class="field-input" maxlength="190" value="<?= htmlspecialchars((string) $formValues['new_group_name'], ENT_QUOTES) ?>" placeholder="Optional new doctrine group">
                    </label>
                    <label class="block">
                        <span class="mb-2 block field-label">New group description</span>
                        <input type="text" name="new_group_description" class="field-input" maxlength="1000" value="<?= htmlspecialchars((string) $formValues['new_group_description'], ENT_QUOTES) ?>" placeholder="Operational note for the new doctrine group">
                    </label>
                </div>
            </div>
            <label class="block">
                <span class="mb-2 block field-label">Fit payload</span>
                <textarea name="fit_payload" class="field-input font-mono text-sm" style="min-height: 24rem;" spellcheck="false" placeholder="[Scimitar, Shield Scimi]\nDamage Control II\nNanofiber Internal Structure II\n\nLarge Shield Extender II\n10MN Afterburner II\n\nLarge Remote Shield Booster II x4\n\nMedium Core Defense Field Extender II x2\n\nWarrior II x5"><?= htmlspecialchars((string) $formValues['fit_payload'], ENT_QUOTES) ?></textarea>
            </label>
            <div class="flex flex-wrap items-center justify-between gap-3">
                <div class="text-sm text-slate-400">EFT hulls come from <code>[Ship, Fit Name]</code>. BuyAll hulls come from the first line. If anything looks ambiguous, correct it in the confirmation step before saving.</div>
                <button type="submit" class="btn-primary">Preview import</button>
            </div>
        </form>
    </article>

    <aside class="surface-secondary">
        <div class="section-header">
            <div>
                <p class="eyebrow">Import behavior</p>
                <h2 class="mt-2 section-title">Confirmation control</h2>
            </div>
            <span class="badge border-cyan-400/18 bg-cyan-500/10 text-cyan-100">No silent saves</span>
        </div>
        <div class="space-y-3 text-sm text-slate-300">
            <div class="surface-tertiary">
                <p class="font-semibold text-slate-100">1. Parse the hull correctly</p>
                <p class="mt-2 text-slate-400">EFT imports use the ship from the header. BuyAll imports use the first line as the hull and keep it visible for correction.</p>
            </div>
            <div class="surface-tertiary">
                <p class="font-semibold text-slate-100">2. Confirm every decision</p>
                <p class="mt-2 text-slate-400">Before saving you can edit the fit name, ship name, groups, raw payload, and normalized line list.</p>
            </div>
            <div class="surface-tertiary">
                <p class="font-semibold text-slate-100">3. Save only clean data</p>
                <p class="mt-2 text-slate-400">Broken hull resolution or unresolved items keep the fit in preview instead of silently storing a bad doctrine record.</p>
            </div>
        </div>
    </aside>
</section>

<?php if (is_array($previewDraft)): ?>
    <?php $readyToSave = (bool) ($previewDraft['ready_to_save'] ?? false); ?>
    <section class="surface-secondary mt-8">
        <div class="section-header">
            <div>
                <p class="eyebrow">Confirmation step</p>
                <h2 class="mt-2 section-title">Review and finalize doctrine fit</h2>
                <p class="mt-2 text-sm text-slate-400">Correct anything below before saving. Suggested multi-group matches are preselected when SupplyCore finds existing doctrines with the same hull or fit name.</p>
            </div>
            <span class="badge <?= htmlspecialchars(doctrine_supply_status_tone($readyToSave ? 'ready' : 'critical'), ENT_QUOTES) ?>"><?= $readyToSave ? 'Ready to save' : 'Needs correction' ?></span>
        </div>

        <form method="post" class="space-y-5">
            <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
            <input type="hidden" name="action" value="save">
            <input type="hidden" name="fit_payload" value="<?= htmlspecialchars((string) $formValues['fit_payload'], ENT_QUOTES) ?>">
            <div class="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                <label class="block xl:col-span-2">
                    <span class="mb-2 block field-label">Fit name</span>
                    <input type="text" name="fit_name" class="field-input" maxlength="190" value="<?= htmlspecialchars((string) ($formValues['fit_name'] ?: ($previewDraft['fit']['fit_name'] ?? '')), ENT_QUOTES) ?>">
                </label>
                <label class="block">
                    <span class="mb-2 block field-label">Ship name</span>
                    <input type="text" name="ship_name" class="field-input" maxlength="255" value="<?= htmlspecialchars((string) ($formValues['ship_name'] ?: ($previewDraft['fit']['ship_name'] ?? '')), ENT_QUOTES) ?>">
                </label>
                <label class="block">
                    <span class="mb-2 block field-label">Source format</span>
                    <select name="source_format" class="field-select">
                        <?php foreach (['eft' => 'EFT', 'buyall' => 'BuyAll'] as $value => $label): ?>
                            <option value="<?= $value ?>" <?= (($formValues['source_format'] ?: ($previewDraft['fit']['source_format'] ?? 'buyall')) === $value) ? 'selected' : '' ?>><?= $label ?></option>
                        <?php endforeach; ?>
                    </select>
                </label>
            </div>

            <div class="grid gap-4 xl:grid-cols-[minmax(280px,0.8fr)_minmax(0,1fr)]">
                <div class="space-y-4">
                    <div class="rounded-[1.2rem] border border-white/8 bg-slate-950/50 p-4">
                        <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Doctrine groups</p>
                        <div class="mt-3 space-y-2">
                            <?php foreach ($groupOptions as $group): ?>
                                <?php $groupId = (int) ($group['id'] ?? 0); ?>
                                <label class="flex items-start gap-3 text-sm text-slate-300">
                                    <input type="checkbox" name="group_ids[]" value="<?= $groupId ?>" class="mt-1" <?= in_array($groupId, (array) ($previewDraft['group_ids'] ?? []), true) ? 'checked' : '' ?>>
                                    <span><?= htmlspecialchars((string) ($group['group_name'] ?? ''), ENT_QUOTES) ?></span>
                                </label>
                            <?php endforeach; ?>
                        </div>
                        <?php if (($previewDraft['suggested_groups'] ?? []) !== []): ?>
                            <p class="mt-3 text-xs text-cyan-200/80">Suggested from existing hull / fit matches.</p>
                        <?php endif; ?>
                    </div>
                    <label class="block">
                        <span class="mb-2 block field-label">New group name</span>
                        <input type="text" name="new_group_name" class="field-input" maxlength="190" value="<?= htmlspecialchars((string) $formValues['new_group_name'], ENT_QUOTES) ?>">
                    </label>
                    <label class="block">
                        <span class="mb-2 block field-label">New group description</span>
                        <input type="text" name="new_group_description" class="field-input" maxlength="1000" value="<?= htmlspecialchars((string) $formValues['new_group_description'], ENT_QUOTES) ?>">
                    </label>
                    <div class="surface-tertiary text-sm text-slate-300">
                        <?php if (($previewDraft['unresolved'] ?? []) !== []): ?>
                            <p class="font-semibold text-amber-100">Resolve these names before saving:</p>
                            <p class="mt-2 text-amber-100"><?= htmlspecialchars(implode(', ', (array) ($previewDraft['unresolved'] ?? [])), ENT_QUOTES) ?></p>
                        <?php else: ?>
                            <p class="font-semibold text-emerald-100">Resolution looks clean.</p>
                            <p class="mt-2 text-slate-400">This fit will save with a mapped hull image, normalized items, and multi-group memberships.</p>
                        <?php endif; ?>
                    </div>
                </div>

                <div class="space-y-4">
                    <label class="block">
                        <span class="mb-2 block field-label">Raw import text</span>
                        <textarea name="import_body" class="field-input font-mono text-sm" style="min-height: 14rem;" spellcheck="false"><?= htmlspecialchars((string) ($formValues['import_body'] ?: ($previewDraft['fit']['import_body'] ?? '')), ENT_QUOTES) ?></textarea>
                    </label>
                    <label class="block">
                        <span class="mb-2 block field-label">Normalized item lines</span>
                        <textarea name="item_lines_text" class="field-input font-mono text-sm" style="min-height: 16rem;" spellcheck="false"><?= htmlspecialchars((string) ($formValues['item_lines_text'] ?: ($previewDraft['item_lines_text'] ?? '')), ENT_QUOTES) ?></textarea>
                    </label>
                </div>
            </div>

            <div class="flex flex-wrap items-center justify-between gap-3">
                <div class="text-sm text-slate-400">Use <code>Category :: Item xQty</code> when editing normalized lines directly.</div>
                <button type="submit" class="btn-primary" <?= $readyToSave ? '' : 'disabled' ?>>Finalize doctrine fit</button>
            </div>
        </form>
    </section>
<?php endif; ?>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
