<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$typeId = (int) ($_GET['type_id'] ?? 0);

if ($typeId <= 0) {
    header('Location: /economic-warfare/');
    exit;
}

$title = 'Module Drilldown';
$drilldown = db_economic_warfare_module_drilldown($typeId);
$score = is_array($drilldown['score'] ?? null) ? $drilldown['score'] : [];
$families = (array) ($drilldown['families'] ?? []);
$substitutes = (array) ($drilldown['substitutes'] ?? []);
$moduleName = (string) ($score['type_name'] ?? ('Type #' . $typeId));

include __DIR__ . '/../../src/views/partials/header.php';
?>
<section class="surface-primary">
    <div class="flex items-center justify-between gap-4">
        <div>
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Economic warfare &middot; Module drilldown</p>
            <h1 class="mt-1 text-2xl font-semibold text-slate-50"><?= htmlspecialchars($moduleName, ENT_QUOTES) ?></h1>
        </div>
        <a href="/economic-warfare/" class="btn-secondary text-sm">Back to overview</a>
    </div>

    <?php if ($score === []): ?>
        <div class="mt-6 rounded-xl border border-border bg-black/20 px-4 py-6 text-center text-sm text-muted">
            No economic warfare data for this module. It may not appear in any observed hostile fits.
        </div>
    <?php else: ?>
        <!-- Score Breakdown -->
        <div class="mt-5 grid gap-3 sm:grid-cols-3 lg:grid-cols-6">
            <?php
                $scoreFields = [
                    ['EW Score', 'economic_warfare_score', true],
                    ['Fit Constraint', 'fit_constraint_score', false],
                    ['Substitution', 'substitution_penalty_score', false],
                    ['Replacement', 'replacement_friction_score', false],
                    ['Doctrine Pen.', 'doctrine_penetration_score', false],
                    ['Loss Pressure', 'loss_pressure_score', false],
                ];
            ?>
            <?php foreach ($scoreFields as [$label, $key, $isPrimary]): ?>
                <?php $val = (float) ($score[$key] ?? 0); ?>
                <div class="rounded-xl border border-border bg-black/20 px-4 py-3">
                    <p class="text-xs text-muted"><?= $label ?></p>
                    <p class="mt-1 text-lg font-semibold <?= $isPrimary ? ($val >= 0.5 ? 'text-red-400' : ($val >= 0.3 ? 'text-amber-300' : 'text-zinc-400')) : 'text-slate-100' ?>">
                        <?= number_format($val, 3) ?>
                    </p>
                </div>
            <?php endforeach; ?>
        </div>

        <div class="mt-4 grid gap-3 sm:grid-cols-4">
            <div class="rounded-xl border border-border bg-black/20 px-4 py-3">
                <p class="text-xs text-muted">Meta Group</p>
                <p class="mt-1 text-sm font-medium text-slate-100"><?= htmlspecialchars(ew_meta_group_label((int) ($score['meta_group_id'] ?? 0)), ENT_QUOTES) ?></p>
            </div>
            <div class="rounded-xl border border-border bg-black/20 px-4 py-3">
                <p class="text-xs text-muted">Fitting Variant</p>
                <p class="mt-1 text-sm font-medium <?= (int) ($score['is_fitting_variant'] ?? 0) ? 'text-amber-300' : 'text-zinc-400' ?>"><?= (int) ($score['is_fitting_variant'] ?? 0) ? 'Yes' : 'No' ?></p>
            </div>
            <div class="rounded-xl border border-border bg-black/20 px-4 py-3">
                <p class="text-xs text-muted">Cross-Fit Persistence</p>
                <p class="mt-1 text-sm font-medium text-slate-100"><?= number_format((float) ($score['cross_fit_persistence'] ?? 0) * 100, 1) ?>%</p>
            </div>
            <div class="rounded-xl border border-border bg-black/20 px-4 py-3">
                <p class="text-xs text-muted">Substitutes Available</p>
                <p class="mt-1 text-sm font-medium text-slate-100"><?= number_format((int) ($score['substitute_count'] ?? 0)) ?></p>
            </div>
        </div>

        <!-- Hostile Families Using This Module -->
        <?php if ($families !== []): ?>
        <div class="mt-8">
            <h2 class="text-lg font-semibold text-slate-100">Used in <?= count($families) ?> hostile fit families</h2>
            <div class="mt-3 table-shell">
                <table class="table-ui">
                    <thead>
                    <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                        <th class="px-3 py-2 text-left">Hull</th>
                        <th class="px-3 py-2 text-center">Slot</th>
                        <th class="px-3 py-2 text-right">Frequency</th>
                        <th class="px-3 py-2 text-center">Core?</th>
                        <th class="px-3 py-2 text-right">Observations</th>
                        <th class="px-3 py-2 text-right">Confidence</th>
                        <th class="px-3 py-2 text-right">Alliances</th>
                    </tr>
                    </thead>
                    <tbody>
                    <?php foreach ($families as $fam): ?>
                        <?php $allianceIds = json_decode((string) ($fam['alliance_ids_json'] ?? '[]'), true) ?: []; ?>
                        <tr class="border-b border-border/30 hover:bg-white/[0.02]">
                            <td class="px-3 py-2 text-sm text-slate-100"><?= htmlspecialchars((string) ($fam['hull_name'] ?? ('Hull #' . ($fam['hull_type_id'] ?? '?'))), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-center text-xs text-muted"><?= htmlspecialchars((string) ($fam['flag_category'] ?? '-'), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-right text-sm text-slate-300"><?= number_format((float) ($fam['frequency'] ?? 0) * 100, 0) ?>%</td>
                            <td class="px-3 py-2 text-center text-xs"><?= (int) ($fam['is_core'] ?? 0) ? '<span class="text-amber-300">Core</span>' : '<span class="text-zinc-500">-</span>' ?></td>
                            <td class="px-3 py-2 text-right text-sm text-muted"><?= number_format((int) ($fam['observation_count'] ?? 0)) ?></td>
                            <td class="px-3 py-2 text-right text-sm <?= (float) ($fam['confidence'] ?? 0) >= 0.9 ? 'text-green-400' : 'text-slate-300' ?>"><?= number_format((float) ($fam['confidence'] ?? 0) * 100, 0) ?>%</td>
                            <td class="px-3 py-2 text-right text-sm text-muted"><?= count($allianceIds) ?></td>
                        </tr>
                    <?php endforeach; ?>
                    </tbody>
                </table>
            </div>
        </div>
        <?php endif; ?>

        <!-- Substitutes -->
        <?php if ($substitutes !== []): ?>
        <div class="mt-8">
            <h2 class="text-lg font-semibold text-slate-100">Substitutes (same group)</h2>
            <p class="mt-1 text-sm text-muted">Other modules in the same item group that could replace this module. Fewer viable substitutes = higher pressure.</p>
            <div class="mt-3 table-shell">
                <table class="table-ui">
                    <thead>
                    <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                        <th class="px-3 py-2 text-left">Module</th>
                        <th class="px-3 py-2 text-center">Meta</th>
                        <th class="px-3 py-2 text-right">EW Score</th>
                        <th class="px-3 py-2 text-right">Fit Constraint</th>
                    </tr>
                    </thead>
                    <tbody>
                    <?php foreach ($substitutes as $sub): ?>
                        <tr class="border-b border-border/30 hover:bg-white/[0.02]">
                            <td class="px-3 py-2 text-sm text-slate-100">
                                <a href="/economic-warfare/module.php?type_id=<?= (int) ($sub['type_id'] ?? 0) ?>" class="hover:text-blue-400"><?= htmlspecialchars((string) ($sub['type_name'] ?? ('Type #' . ($sub['type_id'] ?? '?'))), ENT_QUOTES) ?></a>
                            </td>
                            <td class="px-3 py-2 text-center text-xs text-muted"><?= htmlspecialchars(ew_meta_group_label((int) ($sub['meta_group_id'] ?? 0)), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-right text-sm text-slate-300"><?= $sub['economic_warfare_score'] !== null ? number_format((float) $sub['economic_warfare_score'], 3) : '-' ?></td>
                            <td class="px-3 py-2 text-right text-sm text-slate-300"><?= $sub['fit_constraint_score'] !== null ? number_format((float) $sub['fit_constraint_score'], 3) : '-' ?></td>
                        </tr>
                    <?php endforeach; ?>
                    </tbody>
                </table>
            </div>
        </div>
        <?php endif; ?>
    <?php endif; ?>
</section>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
