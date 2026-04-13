<?php

declare(strict_types=1);
require_once __DIR__ . '/../../src/bootstrap.php';

$caseId = isset($_GET['case_id']) ? (int) $_GET['case_id'] : 0;
$title = $caseId > 0 ? "Spy Network Case #{$caseId}" : 'Spy Network Case';

$tablesReadyError = null;
$case = null;
$members = [];
$edges = [];
$ciScores = [];

if ($caseId > 0) {
    try {
        $case = db_spy_network_case_detail($caseId);
        if ($case !== null) {
            $members = db_spy_network_case_members($caseId, 500);
            $edges = db_spy_network_case_edges($caseId, 500);

            $memberIds = [];
            foreach ($members as $m) {
                $mid = (int) $m['character_id'];
                if ($mid > 0) {
                    $memberIds[] = $mid;
                }
            }
            if ($memberIds !== []) {
                try {
                    $ciScores = db_counterintel_scores_for_characters($memberIds);
                } catch (Throwable $e) {
                    // Non-fatal — CI table may not be populated yet.
                }
            }
        }
    } catch (Throwable $e) {
        $tablesReadyError = $e->getMessage();
    }
}

$severityBadge = static function (string $tier): string {
    return match ($tier) {
        'critical' => 'bg-red-900/60 text-red-200 border-red-700/70',
        'high' => 'bg-orange-900/50 text-orange-200 border-orange-700/60',
        'medium' => 'bg-amber-900/40 text-amber-200 border-amber-700/50',
        'monitor' => 'bg-sky-900/40 text-sky-200 border-sky-700/50',
        default => 'bg-slate-800 text-slate-300 border-slate-700',
    };
};

$statusBadge = static function (string $status): string {
    return match ($status) {
        'open' => 'bg-red-900/40 text-red-200 border-red-700/50',
        'reviewing' => 'bg-amber-900/40 text-amber-200 border-amber-700/50',
        'closed' => 'bg-slate-800 text-slate-400 border-slate-700',
        'reopened' => 'bg-purple-900/40 text-purple-200 border-purple-700/50',
        default => 'bg-slate-800 text-slate-300 border-slate-700',
    };
};

$roleBadge = static function (string $role): string {
    return match (strtolower($role)) {
        'ringleader', 'leader' => 'bg-red-900/40 text-red-200 border-red-700/50',
        'bridge' => 'bg-cyan-900/40 text-cyan-200 border-cyan-700/50',
        'suspect' => 'bg-orange-900/40 text-orange-200 border-orange-700/50',
        'peripheral' => 'bg-slate-800 text-slate-400 border-slate-700',
        default => 'bg-slate-800 text-slate-300 border-slate-700',
    };
};

$edgeTypeBadge = static function (string $edgeType): string {
    return match ($edgeType) {
        'identity' => 'bg-purple-900/40 text-purple-200 border-purple-700/50',
        'copresence' => 'bg-sky-900/40 text-sky-200 border-sky-700/50',
        'cross_side' => 'bg-red-900/40 text-red-200 border-red-700/50',
        'temporal' => 'bg-amber-900/40 text-amber-200 border-amber-700/50',
        default => 'bg-slate-800 text-slate-300 border-slate-700',
    };
};

include __DIR__ . '/../../src/views/partials/header.php';
?>

<section class="surface-primary">
    <div class="flex items-start justify-between gap-4">
        <div>
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Counterintel &middot; Spy Detection</p>
            <h1 class="mt-1 text-2xl font-semibold text-slate-50">
                <?php if ($caseId > 0 && $case !== null): ?>
                    Spy Network Case <span class="text-slate-400 font-mono">#<?= $caseId ?></span>
                <?php else: ?>
                    Spy Network Case
                <?php endif; ?>
            </h1>
            <?php if ($case !== null): ?>
                <p class="mt-2 text-xs text-muted">
                    Community
                    <span class="font-mono text-slate-300"><?= (int) ($case['community_id'] ?? 0) ?></span>
                    &middot; Source <?= htmlspecialchars((string) ($case['community_source'] ?? '')) ?>
                    &middot; Model <?= htmlspecialchars((string) ($case['model_version'] ?? '')) ?>
                    &middot; Run <code class="text-xs"><?= htmlspecialchars((string) ($case['source_run_id'] ?? '')) ?></code>
                </p>
            <?php endif; ?>
        </div>
        <div class="flex items-center gap-2">
            <a href="/spy-detection/" class="btn-secondary">&larr; All Cases</a>
        </div>
    </div>
</section>

<?php if ($tablesReadyError !== null): ?>
<section class="surface-primary mt-4 border border-amber-500/30 bg-amber-500/10">
    <p class="text-sm text-amber-200">
        <strong>Spy detection tables not ready.</strong>
        <?= htmlspecialchars($tablesReadyError, ENT_QUOTES) ?>
    </p>
</section>
<?php endif; ?>

<?php if ($caseId <= 0): ?>
    <section class="surface-primary mt-4">
        <p class="text-sm text-muted">No case selected. Return to the <a href="/spy-detection/" class="text-cyan-300">case list</a>.</p>
    </section>
<?php elseif ($case === null): ?>
    <section class="surface-primary mt-4">
        <p class="text-sm text-muted">Case #<?= $caseId ?> not found.</p>
    </section>
<?php else:
    $sev = (string) ($case['severity_tier'] ?? 'monitor');
    $st = (string) ($case['status'] ?? 'open');
    $feature = $case['feature_breakdown_json'] ?? null;
    if (is_string($feature)) {
        $featureData = json_decode($feature, true);
        $feature = is_array($featureData) ? $featureData : null;
    }
?>
    <!-- Headline stats -->
    <section class="mt-4 grid grid-cols-2 gap-4 md:grid-cols-5">
        <div class="surface-primary">
            <p class="text-xs uppercase tracking-wider text-muted">Severity</p>
            <div class="mt-1">
                <span class="inline-block rounded border px-2 py-0.5 text-xs uppercase <?= $severityBadge($sev) ?>">
                    <?= htmlspecialchars(strtoupper($sev)) ?>
                </span>
            </div>
            <p class="mt-2 text-xs text-muted">
                Status
                <span class="inline-block rounded border px-1.5 py-0.5 text-[10px] uppercase <?= $statusBadge($st) ?>">
                    <?= htmlspecialchars(strtoupper($st)) ?>
                </span>
            </p>
        </div>
        <div class="surface-primary">
            <p class="text-xs uppercase tracking-wider text-muted">Ring Score</p>
            <p class="mt-1 text-2xl font-bold text-slate-100"><?= number_format((float) $case['ring_score'], 3) ?></p>
            <p class="mt-1 text-xs text-muted">Confidence <?= number_format((float) $case['confidence_score'], 3) ?></p>
        </div>
        <div class="surface-primary">
            <p class="text-xs uppercase tracking-wider text-muted">Members</p>
            <p class="mt-1 text-2xl font-bold text-slate-100"><?= (int) $case['member_count'] ?></p>
            <p class="mt-1 text-xs text-muted">Suspicious ratio <?= number_format((float) $case['suspicious_member_ratio'], 3) ?></p>
        </div>
        <div class="surface-primary">
            <p class="text-xs uppercase tracking-wider text-muted">Bridge Concentration</p>
            <p class="mt-1 text-2xl font-bold text-cyan-200"><?= number_format((float) $case['bridge_concentration'], 3) ?></p>
            <p class="mt-1 text-xs text-muted">Hostile overlap <?= number_format((float) $case['hostile_overlap_density'], 3) ?></p>
        </div>
        <div class="surface-primary">
            <p class="text-xs uppercase tracking-wider text-muted">Identity Density</p>
            <p class="mt-1 text-2xl font-bold text-purple-200"><?= number_format((float) $case['identity_density'], 3) ?></p>
            <p class="mt-1 text-xs text-muted">Recurrence <?= number_format((float) $case['recurrence_stability'], 3) ?></p>
        </div>
    </section>

    <!-- Timing -->
    <section class="surface-primary mt-4">
        <div class="grid grid-cols-2 gap-4 md:grid-cols-4 text-xs">
            <div>
                <p class="uppercase tracking-wider text-muted">First Detected</p>
                <p class="mt-1 text-slate-200"><?= htmlspecialchars((string) $case['first_detected_at']) ?></p>
            </div>
            <div>
                <p class="uppercase tracking-wider text-muted">Last Reinforced</p>
                <p class="mt-1 text-slate-200"><?= htmlspecialchars((string) $case['last_reinforced_at']) ?></p>
            </div>
            <div>
                <p class="uppercase tracking-wider text-muted">Status Changed</p>
                <p class="mt-1 text-slate-200"><?= htmlspecialchars((string) ($case['status_changed_at'] ?? '—')) ?></p>
            </div>
            <div>
                <p class="uppercase tracking-wider text-muted">Computed</p>
                <p class="mt-1 text-slate-200"><?= htmlspecialchars((string) $case['computed_at']) ?></p>
            </div>
        </div>
    </section>

    <!-- Feature breakdown -->
    <?php if (is_array($feature) && $feature !== []): ?>
        <section class="surface-primary mt-4">
            <h2 class="text-lg font-semibold text-slate-100 mb-3">Feature Breakdown</h2>
            <div class="grid gap-3 md:grid-cols-2">
                <?php foreach ($feature as $key => $value):
                    if (is_array($value) || is_object($value)) {
                        $display = json_encode($value, JSON_UNESCAPED_SLASHES);
                    } else {
                        $display = (string) $value;
                    }
                ?>
                    <div class="flex items-start justify-between gap-3 rounded border border-border/40 bg-slate-800/30 px-3 py-2">
                        <span class="text-xs text-muted uppercase tracking-wider"><?= htmlspecialchars((string) $key) ?></span>
                        <span class="text-sm font-mono text-slate-200 text-right"><?= htmlspecialchars($display) ?></span>
                    </div>
                <?php endforeach; ?>
            </div>
        </section>
    <?php endif; ?>

    <!-- Members -->
    <section class="surface-primary mt-4">
        <div class="flex items-center justify-between mb-3">
            <h2 class="text-lg font-semibold text-slate-100">Members</h2>
            <span class="text-xs text-muted"><?= count($members) ?> shown</span>
        </div>

        <?php if ($members): ?>
            <div class="table-shell">
                <table class="table-ui">
                    <thead>
                        <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                            <th class="px-3 py-2 text-left">Character</th>
                            <th class="px-3 py-2 text-left">Role</th>
                            <th class="px-3 py-2 text-right">Contribution</th>
                            <th class="px-3 py-2 text-right">CI Priority</th>
                            <th class="px-3 py-2 text-right">CI Percentile</th>
                        </tr>
                    </thead>
                    <tbody>
                        <?php foreach ($members as $m):
                            $cid = (int) $m['character_id'];
                            $role = (string) ($m['role_label'] ?? 'member');
                            $ci = $ciScores[$cid] ?? null;
                        ?>
                            <tr class="border-b border-border/30 hover:bg-slate-800/40 transition-colors">
                                <td class="px-3 py-2 text-sm">
                                    <a href="/killmail-intelligence/?character_id=<?= $cid ?>" class="text-cyan-300 hover:text-cyan-100">
                                        <?= htmlspecialchars((string) $m['character_name']) ?>
                                    </a>
                                    <span class="ml-1 text-[10px] text-muted font-mono">#<?= $cid ?></span>
                                </td>
                                <td class="px-3 py-2">
                                    <span class="inline-block rounded border px-1.5 py-0.5 text-[10px] uppercase <?= $roleBadge($role) ?>">
                                        <?= htmlspecialchars(strtoupper($role)) ?>
                                    </span>
                                </td>
                                <td class="px-3 py-2 text-right text-sm font-mono text-slate-100">
                                    <?= number_format((float) $m['member_contribution_score'], 3) ?>
                                </td>
                                <td class="px-3 py-2 text-right text-sm font-mono">
                                    <?= $ci !== null ? number_format((float) $ci['review_priority_score'], 3) : '<span class="text-muted">—</span>' ?>
                                </td>
                                <td class="px-3 py-2 text-right text-xs font-mono text-muted">
                                    <?= $ci !== null ? number_format((float) $ci['percentile_rank'] * 100, 1) . '%' : '—' ?>
                                </td>
                            </tr>
                        <?php endforeach; ?>
                    </tbody>
                </table>
            </div>
        <?php else: ?>
            <p class="text-sm text-muted">No member rows.</p>
        <?php endif; ?>
    </section>

    <!-- Edges -->
    <section class="surface-primary mt-4">
        <div class="flex items-center justify-between mb-3">
            <h2 class="text-lg font-semibold text-slate-100">Relationship Edges</h2>
            <span class="text-xs text-muted"><?= count($edges) ?> shown</span>
        </div>

        <?php if ($edges): ?>
            <div class="table-shell">
                <table class="table-ui">
                    <thead>
                        <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                            <th class="px-3 py-2 text-left">Character A</th>
                            <th class="px-3 py-2 text-left">Character B</th>
                            <th class="px-3 py-2 text-left">Type</th>
                            <th class="px-3 py-2 text-right">Weight</th>
                            <th class="px-3 py-2 text-left">Components</th>
                        </tr>
                    </thead>
                    <tbody>
                        <?php foreach ($edges as $e):
                            $aid = (int) $e['character_id_a'];
                            $bid = (int) $e['character_id_b'];
                            $etype = (string) $e['edge_type'];
                            $components = $e['component_weights_json'] ?? null;
                            if (is_string($components)) {
                                $componentsData = json_decode($components, true);
                                $components = is_array($componentsData) ? $componentsData : null;
                            }
                        ?>
                            <tr class="border-b border-border/30 hover:bg-slate-800/40 transition-colors">
                                <td class="px-3 py-2 text-sm font-mono">
                                    <a href="/killmail-intelligence/?character_id=<?= $aid ?>" class="text-cyan-300 hover:text-cyan-100">#<?= $aid ?></a>
                                </td>
                                <td class="px-3 py-2 text-sm font-mono">
                                    <a href="/killmail-intelligence/?character_id=<?= $bid ?>" class="text-cyan-300 hover:text-cyan-100">#<?= $bid ?></a>
                                </td>
                                <td class="px-3 py-2">
                                    <span class="inline-block rounded border px-1.5 py-0.5 text-[10px] uppercase <?= $edgeTypeBadge($etype) ?>">
                                        <?= htmlspecialchars($etype) ?>
                                    </span>
                                </td>
                                <td class="px-3 py-2 text-right text-sm font-mono text-slate-100">
                                    <?= number_format((float) $e['edge_weight'], 3) ?>
                                </td>
                                <td class="px-3 py-2 text-xs text-muted">
                                    <?php if (is_array($components)): ?>
                                        <?php
                                        $parts = [];
                                        foreach ($components as $ck => $cv) {
                                            if (is_numeric($cv)) {
                                                $parts[] = htmlspecialchars((string) $ck) . ': ' . number_format((float) $cv, 3);
                                            }
                                        }
                                        echo implode(' · ', $parts);
                                        ?>
                                    <?php else: ?>
                                        —
                                    <?php endif; ?>
                                </td>
                            </tr>
                        <?php endforeach; ?>
                    </tbody>
                </table>
            </div>
        <?php else: ?>
            <p class="text-sm text-muted">No edge rows.</p>
        <?php endif; ?>
    </section>
<?php endif; ?>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
