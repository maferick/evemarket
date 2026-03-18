<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Doctrine Groups';
$data = doctrine_groups_overview_data();
$summary = $data['summary'] ?? [];
$groups = $data['groups'] ?? [];

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    if (!validate_csrf($_POST['_token'] ?? null)) {
        http_response_code(419);
        exit('Invalid CSRF token');
    }

    $groupName = doctrine_sanitize_group_name((string) ($_POST['group_name'] ?? ''));
    $description = doctrine_sanitize_description($_POST['description'] ?? null);

    try {
        $groupId = db_doctrine_group_create($groupName, $description);
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
<section class="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
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
                <p class="mt-2 text-sm text-slate-400">Use groups to organize fleet comps, staging doctrines, and import batches before gap detection workflows kick in.</p>
            </div>
            <a href="/doctrine/import" class="btn-primary">Import fit</a>
        </div>

        <?php if ($groups === []): ?>
            <div class="surface-tertiary text-sm text-slate-400">No doctrine groups yet. Create one, then import an EFT or BuyAll payload.</div>
        <?php else: ?>
            <div class="space-y-4">
                <?php foreach ($groups as $group): ?>
                    <a href="/doctrine/group?group_id=<?= (int) ($group['id'] ?? 0) ?>" class="block rounded-[1.3rem] border border-white/8 bg-white/[0.03] p-4 transition hover:bg-white/[0.05]">
                        <div class="flex flex-wrap items-start justify-between gap-4">
                            <div>
                                <h3 class="text-lg font-semibold text-white"><?= htmlspecialchars((string) ($group['group_name'] ?? ''), ENT_QUOTES) ?></h3>
                                <p class="mt-2 max-w-2xl text-sm text-slate-400"><?= htmlspecialchars((string) ($group['description'] ?? 'Doctrine fit collection for SupplyCore workflows.'), ENT_QUOTES) ?></p>
                            </div>
                            <span class="badge border-cyan-400/18 bg-cyan-500/10 text-cyan-100"><?= doctrine_format_quantity((int) ($group['fit_count'] ?? 0)) ?> fits</span>
                        </div>
                        <div class="mt-4 grid gap-3 sm:grid-cols-3">
                            <div class="surface-tertiary">
                                <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Tracked fits</p>
                                <p class="mt-2 text-xl font-semibold text-slate-100"><?= doctrine_format_quantity((int) ($group['fit_count'] ?? 0)) ?></p>
                            </div>
                            <div class="surface-tertiary">
                                <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Tracked items</p>
                                <p class="mt-2 text-xl font-semibold text-slate-100"><?= doctrine_format_quantity((int) ($group['item_count'] ?? 0)) ?></p>
                            </div>
                            <div class="surface-tertiary">
                                <p class="text-xs uppercase tracking-[0.16em] text-slate-500">Last update</p>
                                <p class="mt-2 text-sm font-medium text-slate-100"><?= htmlspecialchars(killmail_relative_datetime($group['last_fit_updated_at'] ?? null), ENT_QUOTES) ?></p>
                            </div>
                        </div>
                    </a>
                <?php endforeach; ?>
            </div>
        <?php endif; ?>
    </article>

    <aside class="surface-secondary">
        <div class="section-header">
            <div>
                <p class="eyebrow">New group</p>
                <h2 class="mt-2 section-title">Create doctrine group</h2>
            </div>
            <span class="badge border-sky-400/18 bg-sky-500/10 text-sky-100">Lean workflow</span>
        </div>
        <form method="post" class="space-y-4">
            <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
            <label class="block">
                <span class="mb-2 block field-label">Group name</span>
                <input type="text" name="group_name" class="field-input" maxlength="190" placeholder="e.g. Muninn Mainline">
            </label>
            <label class="block">
                <span class="mb-2 block field-label">Description</span>
                <textarea name="description" class="field-input" style="min-height: 8rem;" placeholder="What this doctrine group covers, when it is used, or who owns the restock workflow."></textarea>
            </label>
            <div class="surface-tertiary text-sm text-slate-400">
                This group becomes the baseline bucket for imported fittings, supply gap reports, and future hauling suggestions.
            </div>
            <button type="submit" class="btn-primary w-full justify-center">Save group</button>
        </form>
    </aside>
</section>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
