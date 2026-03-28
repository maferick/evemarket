<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Economic Warfare';
$filters = [
    'hostile_alliance_id' => (int) ($_GET['hostile_alliance_id'] ?? 0),
    'min_score' => (float) ($_GET['min_score'] ?? 0),
    'meta_group_id' => (int) ($_GET['meta_group_id'] ?? 0),
    'limit' => (int) ($_GET['limit'] ?? 100),
    'offset' => (int) ($_GET['offset'] ?? 0),
];

$data = economic_warfare_page_data($filters);
$summary = (array) ($data['summary'] ?? []);
$scores = (array) ($data['scores'] ?? []);
$families = (array) ($data['families'] ?? []);
$hostileAlliances = (array) ($data['hostile_alliances'] ?? []);
$computedAt = (string) ($summary['computed_at'] ?? '');

include __DIR__ . '/../../src/views/partials/header.php';
?>
<section class="surface-primary">
    <div class="flex items-center justify-between gap-4">
        <div>
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Supply intelligence</p>
            <h1 class="mt-1 text-2xl font-semibold text-slate-50">Economic Warfare</h1>
            <p class="mt-2 text-sm text-muted">Identifies fitting-constrained modules in hostile doctrines where supply pressure has maximum impact.</p>
        </div>
    </div>
    <p class="mt-4 text-xs text-muted">Computed at <?= htmlspecialchars($computedAt !== '' ? $computedAt : 'not yet computed', ENT_QUOTES) ?></p>

    <!-- Summary Cards -->
    <div class="mt-5 grid gap-3 sm:grid-cols-2 lg:grid-cols-5">
        <div class="rounded-xl border border-border bg-black/20 px-4 py-3">
            <p class="text-xs text-muted">Kills Analyzed</p>
            <p class="mt-1 text-lg font-semibold text-slate-100"><?= number_format((int) ($summary['total_observations'] ?? 0)) ?></p>
        </div>
        <div class="rounded-xl border border-border bg-black/20 px-4 py-3">
            <p class="text-xs text-muted">Fit Families</p>
            <p class="mt-1 text-lg font-semibold text-slate-100"><?= number_format((int) ($summary['fit_families'] ?? 0)) ?></p>
        </div>
        <div class="rounded-xl border border-border bg-black/20 px-4 py-3">
            <p class="text-xs text-muted">Hostile Alliances</p>
            <p class="mt-1 text-lg font-semibold text-slate-100"><?= number_format((int) ($summary['hostile_alliances'] ?? 0)) ?></p>
        </div>
        <div class="rounded-xl border border-border bg-black/20 px-4 py-3">
            <p class="text-xs text-muted">Top Pressure Target</p>
            <p class="mt-1 text-sm font-semibold text-slate-100 truncate"><?= htmlspecialchars((string) ($summary['top_target'] ?? 'N/A'), ENT_QUOTES) ?></p>
        </div>
        <div class="rounded-xl border border-border bg-black/20 px-4 py-3">
            <p class="text-xs text-muted">Avg Replacement Friction</p>
            <p class="mt-1 text-lg font-semibold text-slate-100"><?= number_format((float) ($summary['avg_replacement_friction'] ?? 0), 3) ?></p>
        </div>
    </div>

    <!-- Filters -->
    <form class="mt-5 flex flex-wrap items-end gap-3" method="get">
        <label class="block space-y-1">
            <span class="text-xs text-muted">Hostile Alliance</span>
            <select name="hostile_alliance_id" class="field-input text-sm">
                <option value="0">All alliances</option>
                <?php foreach ($hostileAlliances as $ha): ?>
                    <option value="<?= (int) ($ha['alliance_id'] ?? 0) ?>" <?= (int) ($filters['hostile_alliance_id'] ?? 0) === (int) ($ha['alliance_id'] ?? 0) ? 'selected' : '' ?>>
                        <?= htmlspecialchars((string) ($ha['label'] ?? ('Alliance #' . ($ha['alliance_id'] ?? '?'))), ENT_QUOTES) ?>
                    </option>
                <?php endforeach; ?>
            </select>
        </label>
        <label class="block space-y-1">
            <span class="text-xs text-muted">Min EW Score</span>
            <input type="number" name="min_score" step="0.01" min="0" max="1" value="<?= htmlspecialchars((string) ($filters['min_score'] ?? ''), ENT_QUOTES) ?>" placeholder="0.00" class="field-input text-sm w-24" />
        </label>
        <label class="block space-y-1">
            <span class="text-xs text-muted">Meta Group</span>
            <select name="meta_group_id" class="field-input text-sm">
                <option value="0">All</option>
                <option value="1" <?= (int) ($filters['meta_group_id'] ?? 0) === 1 ? 'selected' : '' ?>>T1</option>
                <option value="2" <?= (int) ($filters['meta_group_id'] ?? 0) === 2 ? 'selected' : '' ?>>T2</option>
                <option value="4" <?= (int) ($filters['meta_group_id'] ?? 0) === 4 ? 'selected' : '' ?>>Faction</option>
                <option value="6" <?= (int) ($filters['meta_group_id'] ?? 0) === 6 ? 'selected' : '' ?>>Deadspace</option>
                <option value="14" <?= (int) ($filters['meta_group_id'] ?? 0) === 14 ? 'selected' : '' ?>>T3</option>
            </select>
        </label>
        <button type="submit" class="btn-primary text-sm">Filter</button>
        <a href="/economic-warfare/" class="btn-secondary text-sm">Reset</a>
    </form>

    <!-- Main Table: Economic Warfare Targets -->
    <div class="mt-5 table-shell">
        <table class="table-ui">
            <thead>
            <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                <th class="px-3 py-2 text-left">#</th>
                <th class="px-3 py-2 text-left">Module</th>
                <th class="px-3 py-2 text-center">Meta</th>
                <th class="px-3 py-2 text-right">EW Score</th>
                <th class="px-3 py-2 text-right">Fit Constraint</th>
                <th class="px-3 py-2 text-right">Substitution</th>
                <th class="px-3 py-2 text-right">Replacement</th>
                <th class="px-3 py-2 text-right">Doctrine Pen.</th>
                <th class="px-3 py-2 text-right">Loss Pressure</th>
                <th class="px-3 py-2 text-right">Families</th>
                <th class="px-3 py-2 text-right">Alliances</th>
                <th class="px-3 py-2 text-right">Destroyed (30d)</th>
                <th class="px-3 py-2 text-center">Fitting?</th>
                <th class="px-3 py-2 text-right">Detail</th>
            </tr>
            </thead>
            <tbody>
            <?php if ($scores === []): ?>
                <tr><td colspan="14" class="px-3 py-6 text-sm text-muted">No economic warfare data yet. Run compute_economic_warfare to analyze opponent fits.</td></tr>
            <?php else: ?>
                <?php $rank = ((int) ($filters['offset'] ?? 0)) + 1; ?>
                <?php foreach ($scores as $row): ?>
                    <?php
                        $ewScore = (float) ($row['economic_warfare_score'] ?? 0);
                        $fitScore = (float) ($row['fit_constraint_score'] ?? 0);
                        $constraintClass = ew_constraint_color_class($fitScore);
                    ?>
                    <tr class="border-b border-border/30 hover:bg-white/[0.02]">
                        <td class="px-3 py-2 text-sm text-muted"><?= $rank++ ?></td>
                        <td class="px-3 py-2 text-sm text-slate-100"><?= htmlspecialchars((string) ($row['type_name'] ?? ('Type #' . ($row['type_id'] ?? '?'))), ENT_QUOTES) ?></td>
                        <td class="px-3 py-2 text-center text-xs text-muted"><?= htmlspecialchars(ew_meta_group_label((int) ($row['meta_group_id'] ?? 0)), ENT_QUOTES) ?></td>
                        <td class="px-3 py-2 text-right text-sm font-semibold <?= $ewScore >= 0.5 ? 'text-red-400' : ($ewScore >= 0.3 ? 'text-amber-300' : 'text-zinc-400') ?>"><?= number_format($ewScore, 3) ?></td>
                        <td class="px-3 py-2 text-right text-sm <?= $constraintClass ?>"><?= number_format($fitScore, 3) ?></td>
                        <td class="px-3 py-2 text-right text-sm text-slate-300"><?= number_format((float) ($row['substitution_penalty_score'] ?? 0), 3) ?></td>
                        <td class="px-3 py-2 text-right text-sm text-slate-300"><?= number_format((float) ($row['replacement_friction_score'] ?? 0), 3) ?></td>
                        <td class="px-3 py-2 text-right text-sm text-slate-300"><?= number_format((float) ($row['doctrine_penetration_score'] ?? 0), 3) ?></td>
                        <td class="px-3 py-2 text-right text-sm text-slate-300"><?= number_format((float) ($row['loss_pressure_score'] ?? 0), 3) ?></td>
                        <td class="px-3 py-2 text-right text-sm text-muted"><?= number_format((int) ($row['hostile_family_count'] ?? 0)) ?></td>
                        <td class="px-3 py-2 text-right text-sm text-muted"><?= number_format((int) ($row['hostile_alliance_count'] ?? 0)) ?></td>
                        <td class="px-3 py-2 text-right text-sm text-muted"><?= number_format((int) ($row['total_destroyed_30d'] ?? 0)) ?></td>
                        <td class="px-3 py-2 text-center text-xs"><?= (int) ($row['is_fitting_variant'] ?? 0) ? '<span class="text-amber-300">FIT</span>' : '<span class="text-zinc-500">-</span>' ?></td>
                        <td class="px-3 py-2 text-right">
                            <a href="/economic-warfare/module.php?type_id=<?= (int) ($row['type_id'] ?? 0) ?>" class="text-xs text-blue-400 hover:text-blue-300">Drilldown</a>
                        </td>
                    </tr>
                <?php endforeach; ?>
            <?php endif; ?>
            </tbody>
        </table>
    </div>

    <!-- Fit Family Browser -->
    <?php if ($families !== []): ?>
    <div class="mt-8">
        <h2 class="text-lg font-semibold text-slate-100">Hostile Fit Families</h2>
        <p class="mt-1 text-sm text-muted">Reconstructed doctrine archetypes from observed opponent losses, grouped by hull.</p>

        <div class="mt-4 space-y-3">
            <?php foreach ($families as $fam): ?>
                <?php
                    $familyId = (int) ($fam['id'] ?? 0);
                    $hullName = (string) ($fam['hull_name'] ?? ('Hull #' . ($fam['hull_type_id'] ?? '?')));
                    $obs = (int) ($fam['observation_count'] ?? 0);
                    $conf = (float) ($fam['confidence'] ?? 0);
                    $allianceIds = json_decode((string) ($fam['alliance_ids_json'] ?? '[]'), true) ?: [];
                ?>
                <details class="rounded-xl border border-border bg-black/20">
                    <summary class="cursor-pointer list-none px-4 py-3 hover:bg-white/[0.02]">
                        <div class="flex items-center justify-between gap-4">
                            <div class="flex items-center gap-3">
                                <span class="text-sm font-medium text-slate-100"><?= htmlspecialchars($hullName, ENT_QUOTES) ?></span>
                                <span class="text-xs text-muted"><?= $obs ?> observations</span>
                                <span class="text-xs <?= $conf >= 0.9 ? 'text-green-400' : ($conf >= 0.7 ? 'text-amber-300' : 'text-zinc-500') ?>">
                                    <?= number_format($conf * 100, 0) ?>% confidence
                                </span>
                            </div>
                            <span class="text-xs text-muted"><?= count($allianceIds) ?> alliance<?= count($allianceIds) !== 1 ? 's' : '' ?></span>
                        </div>
                    </summary>
                    <div class="border-t border-border px-4 py-3" id="family-<?= $familyId ?>">
                        <p class="text-xs text-muted mb-2">Loading modules...</p>
                        <script>
                            (function() {
                                const container = document.getElementById('family-<?= $familyId ?>');
                                fetch('/economic-warfare/family-modules.php?family_id=<?= $familyId ?>')
                                    .then(r => r.json())
                                    .then(data => {
                                        if (!Array.isArray(data.modules) || data.modules.length === 0) {
                                            container.innerHTML = '<p class="text-xs text-muted">No module data available.</p>';
                                            return;
                                        }
                                        let html = '<div class="grid gap-1">';
                                        data.modules.forEach(m => {
                                            const freq = (parseFloat(m.frequency) * 100).toFixed(0);
                                            const coreClass = parseInt(m.is_core) ? 'text-amber-300' : 'text-zinc-400';
                                            const barWidth = Math.max(4, freq);
                                            html += '<div class="flex items-center gap-3 text-sm">' +
                                                '<span class="w-5 text-right text-xs ' + coreClass + '">' + (parseInt(m.is_core) ? 'C' : '') + '</span>' +
                                                '<span class="w-48 truncate text-slate-200">' + (m.type_name || ('Type #' + m.item_type_id)) + '</span>' +
                                                '<span class="text-xs text-muted w-14">' + m.flag_category + '</span>' +
                                                '<div class="flex-1 h-2 rounded bg-black/30">' +
                                                    '<div class="h-2 rounded bg-blue-500/60" style="width:' + barWidth + '%"></div>' +
                                                '</div>' +
                                                '<span class="text-xs text-muted w-10 text-right">' + freq + '%</span>' +
                                            '</div>';
                                        });
                                        html += '</div>';
                                        container.innerHTML = html;
                                    })
                                    .catch(() => {
                                        container.innerHTML = '<p class="text-xs text-red-400">Failed to load modules.</p>';
                                    });
                            })();
                        </script>
                    </div>
                </details>
            <?php endforeach; ?>
        </div>
    </div>
    <?php endif; ?>

    <!-- Scoring Methodology -->
    <details class="mt-8 rounded-xl border border-border bg-black/20 p-4">
        <summary class="cursor-pointer list-none text-sm font-medium text-slate-100">Scoring Methodology</summary>
        <div class="mt-4 space-y-3 text-sm text-muted">
            <p>Each module is scored on five dimensions, weighted to prioritize <span class="text-slate-100">fitting constraint</span> over raw price:</p>
            <ul class="space-y-1 ml-4 list-disc">
                <li><span class="text-slate-100">Fit Constraint (30%)</span> - Is this a compact/faction/deadspace module chosen because standard T2 doesn't fit? High cross-fit persistence means mandatory.</li>
                <li><span class="text-slate-100">Substitution Penalty (25%)</span> - How many viable alternatives exist in the same item group? Fewer = harder to replace.</li>
                <li><span class="text-slate-100">Replacement Friction (20%)</span> - Market thinness and price. Low volume = supply pressure works faster.</li>
                <li><span class="text-slate-100">Doctrine Penetration (15%)</span> - How broadly is this module used across hostile fit families and alliances?</li>
                <li><span class="text-slate-100">Loss Pressure (10%)</span> - Recent attrition velocity. Higher burn rate = more demand for replacements.</li>
            </ul>
            <p class="mt-2">The <span class="text-amber-300">FIT</span> flag indicates modules with faction/deadspace/compact meta groups or fitting-variant type names, which are strong indicators of fitting constraint.</p>
        </div>
    </details>
</section>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
