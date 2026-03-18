<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Import Doctrine Fit';
$groupOptions = doctrine_group_options();
$defaultGroupId = max(0, (int) ($_GET['group_id'] ?? 0));
foreach ($groupOptions as $groupOption) {
    $candidateId = (int) ($groupOption['id'] ?? 0);
    if ($defaultGroupId <= 0 && $candidateId > 0) {
        $defaultGroupId = $candidateId;
        break;
    }
}

$formValues = [
    'group_id' => (string) $defaultGroupId,
    'new_group_name' => '',
    'new_group_description' => '',
    'fit_payload' => '',
];
$preview = null;
$errorMessage = null;

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    if (!validate_csrf($_POST['_token'] ?? null)) {
        http_response_code(419);
        exit('Invalid CSRF token');
    }

    $formValues = [
        'group_id' => (string) ($_POST['group_id'] ?? ''),
        'new_group_name' => (string) ($_POST['new_group_name'] ?? ''),
        'new_group_description' => (string) ($_POST['new_group_description'] ?? ''),
        'fit_payload' => (string) ($_POST['fit_payload'] ?? ''),
    ];

    $result = doctrine_import_fit_from_request($_POST);
    if (($result['ok'] ?? false) === true) {
        flash('success', (string) ($result['message'] ?? 'Doctrine fit imported successfully.'));
        header('Location: /doctrine/fit?fit_id=' . (int) ($result['fit_id'] ?? 0));
        exit;
    }

    $errorMessage = (string) ($result['message'] ?? 'Doctrine import failed.');

    try {
        $previewParsed = doctrine_parse_import_text((string) $formValues['fit_payload']);
        $preview = doctrine_resolve_parsed_fit($previewParsed, (string) $formValues['fit_payload']);
    } catch (Throwable) {
        $preview = $result['resolved'] ?? null;
    }
}

include __DIR__ . '/../../src/views/partials/header.php';
?>
<section class="grid gap-6 xl:grid-cols-[minmax(0,1.2fr)_minmax(320px,0.8fr)]">
    <article class="surface-secondary">
        <div class="section-header">
            <div>
                <p class="eyebrow">Doctrine ingest</p>
                <h2 class="mt-2 section-title">Import alliance fitting payload</h2>
                <p class="mt-2 text-sm text-slate-400">Paste EFT or BuyAll text. SupplyCore auto-detects the format, normalizes ship + item quantities, resolves real type names, and stores the fit once every line maps cleanly.</p>
            </div>
            <span class="badge border-emerald-400/20 bg-emerald-500/10 text-emerald-100">EFT + BuyAll</span>
        </div>

        <?php if ($errorMessage !== null): ?>
            <div class="mb-5 rounded-2xl border border-amber-500/30 bg-amber-500/10 px-4 py-3 text-sm text-amber-100"><?= htmlspecialchars($errorMessage, ENT_QUOTES) ?></div>
        <?php endif; ?>

        <form method="post" class="space-y-4">
            <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
            <div class="grid gap-4 md:grid-cols-2">
                <label class="block">
                    <span class="mb-2 block field-label">Doctrine group</span>
                    <select name="group_id" class="field-select">
                        <option value="">Select a group</option>
                        <?php foreach ($groupOptions as $group): ?>
                            <option value="<?= (int) ($group['id'] ?? 0) ?>" <?= (string) ($group['id'] ?? '') === (string) $formValues['group_id'] ? 'selected' : '' ?>>
                                <?= htmlspecialchars((string) ($group['group_name'] ?? ''), ENT_QUOTES) ?>
                            </option>
                        <?php endforeach; ?>
                    </select>
                </label>
                <div class="surface-tertiary text-sm text-slate-400">
                    Pick an existing group or enter a new one below. New group data wins when you fill both.
                </div>
            </div>
            <div class="grid gap-4 md:grid-cols-2">
                <label class="block">
                    <span class="mb-2 block field-label">New group name</span>
                    <input type="text" name="new_group_name" class="field-input" maxlength="190" value="<?= htmlspecialchars((string) $formValues['new_group_name'], ENT_QUOTES) ?>" placeholder="Optional new doctrine group">
                </label>
                <label class="block">
                    <span class="mb-2 block field-label">New group description</span>
                    <input type="text" name="new_group_description" class="field-input" maxlength="1000" value="<?= htmlspecialchars((string) $formValues['new_group_description'], ENT_QUOTES) ?>" placeholder="Optional context for the doctrine group">
                </label>
            </div>
            <label class="block">
                <span class="mb-2 block field-label">Fit payload</span>
                <textarea name="fit_payload" class="field-input font-mono text-sm" style="min-height: 26rem;" spellcheck="false" placeholder="[Scimitar, Shield Scimi]\nDamage Control II\nNanofiber Internal Structure II\n\nLarge Shield Extender II\n10MN Afterburner II\n\nLarge Remote Shield Booster II x4\n\nMedium Core Defense Field Extender II x2\n\nWarrior II x5"><?= htmlspecialchars((string) $formValues['fit_payload'], ENT_QUOTES) ?></textarea>
            </label>
            <div class="flex flex-wrap items-center justify-between gap-3">
                <div class="text-sm text-slate-400">
                    Quantity defaults to <span class="text-slate-100">1</span>, supports <span class="text-slate-100">xN</span>, ignores empty lines, and trims whitespace automatically.
                </div>
                <button type="submit" class="btn-primary">Import doctrine fit</button>
            </div>
        </form>
    </article>

    <aside class="surface-secondary">
        <div class="section-header">
            <div>
                <p class="eyebrow">Import behavior</p>
                <h2 class="mt-2 section-title">Resolution pipeline</h2>
            </div>
            <span class="badge border-cyan-400/18 bg-cyan-500/10 text-cyan-100">Local first</span>
        </div>
        <div class="space-y-3 text-sm text-slate-300">
            <div class="surface-tertiary">
                <p class="font-semibold text-slate-100">1. Parse</p>
                <p class="mt-2 text-slate-400">Detect EFT vs BuyAll, extract hull, and normalize line quantities.</p>
            </div>
            <div class="surface-tertiary">
                <p class="font-semibold text-slate-100">2. Resolve</p>
                <p class="mt-2 text-slate-400">Check <code>item_name_cache</code>, fall back to local reference types, then try ESI lookups for any missing lines.</p>
            </div>
            <div class="surface-tertiary">
                <p class="font-semibold text-slate-100">3. Store</p>
                <p class="mt-2 text-slate-400">Persist the fit only when every imported line maps to a real item name.</p>
            </div>
        </div>

        <?php if (is_array($preview) && ($preview['items'] ?? []) !== []): ?>
            <div class="mt-6">
                <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Last preview</p>
                <div class="mt-3 surface-tertiary space-y-3">
                    <div>
                        <p class="text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) (($preview['fit']['fit_name'] ?? '') ?: 'Import preview'), ENT_QUOTES) ?></p>
                        <p class="mt-1 text-sm text-slate-400"><?= htmlspecialchars((string) (($preview['fit']['ship_name'] ?? '') ?: 'Unknown hull'), ENT_QUOTES) ?></p>
                    </div>
                    <div class="grid gap-3 sm:grid-cols-2">
                        <div>
                            <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Resolved lines</p>
                            <p class="mt-2 text-lg font-semibold text-emerald-100"><?= doctrine_format_quantity(count((array) ($preview['items'] ?? []))) ?></p>
                        </div>
                        <div>
                            <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Unresolved</p>
                            <p class="mt-2 text-lg font-semibold text-amber-100"><?= doctrine_format_quantity(count((array) ($preview['unresolved'] ?? []))) ?></p>
                        </div>
                    </div>
                </div>
            </div>
        <?php endif; ?>
    </aside>
</section>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
