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
        'identity', 'LIKELY_SAME_OPERATOR' => 'bg-purple-900/40 text-purple-200 border-purple-700/50',
        'copresence' => 'bg-sky-900/40 text-sky-200 border-sky-700/50',
        'cross_side' => 'bg-red-900/40 text-red-200 border-red-700/50',
        'temporal' => 'bg-amber-900/40 text-amber-200 border-amber-700/50',
        default => 'bg-slate-800 text-slate-300 border-slate-700',
    };
};

/**
 * Plain-English interpretation of each scoring component. Values are on a
 * 0.0–1.0 scale unless otherwise noted.
 */
$metricCatalog = [
    'suspicious_member_ratio' => [
        'label' => 'Suspicious Members',
        'short' => 'Fraction of members already flagged by counterintel review.',
        'interpret' => static function (float $v): string {
            if ($v <= 0.001) return 'None of the members are individually flagged yet — this group was grouped purely by graph structure.';
            if ($v < 0.15)   return 'A handful of members carry existing CI flags.';
            if ($v < 0.40)   return 'A meaningful minority of members are already on the CI radar.';
            if ($v < 0.70)   return 'Most members are already flagged individually — strong signal.';
            return 'Nearly every member is already flagged — this cluster is dense with known suspects.';
        },
    ],
    'bridge_concentration' => [
        'label' => 'Bridge Concentration',
        'short' => 'Fraction of members acting as bridges between otherwise-separate communities.',
        'interpret' => static function (float $v): string {
            if ($v <= 0.001) return 'No members sit on bridge edges — behaves like an ordinary, self-contained group.';
            if ($v < 0.25)   return 'A few members link out to other communities.';
            if ($v < 0.60)   return 'Many members bridge out to other groups — classic spy-ring topology.';
            if ($v < 0.99)   return 'Most members bridge out — the cluster is heavily intermeshed with outsiders.';
            return 'Every tracked member sits on at least one bridge edge — this community is entirely "glue" between other groups.';
        },
    ],
    'hostile_overlap_density' => [
        'label' => 'Hostile Overlap',
        'short' => 'How often members resolve to the same real operators as known hostiles.',
        'interpret' => static function (float $v): string {
            if ($v <= 0.001) return 'No cross-side identity links observed — no evidence members share operators with hostiles.';
            if ($v < 0.10)   return 'A small number of members share operator identities with hostiles.';
            if ($v < 0.30)   return 'Non-trivial cross-side identity sharing — worth a closer look.';
            return 'Heavy cross-side identity overlap — strong coordination signal.';
        },
    ],
    'identity_density' => [
        'label' => 'Identity Density',
        'short' => 'Density of identity-link pairs inside the cluster (same real pilot, multiple accounts).',
        'interpret' => static function (float $v): string {
            if ($v <= 0.001) return 'No identity links between members — no evidence of shared real operators.';
            if ($v < 0.05)   return 'A few pairs look like they could be the same real pilot.';
            if ($v < 0.20)   return 'A meaningful set of member pairs resolve to the same operator.';
            return 'Dense identity sharing — many members appear to be the same real pilot on alts.';
        },
    ],
    'recurrence_stability' => [
        'label' => 'Recurrence',
        'short' => 'How stable the cluster has been across pipeline runs.',
        'interpret' => static function (float $v): string {
            if ($v < 0.01)   return 'Cluster has not recurred — first appearance or fleeting.';
            if ($v > 0.49 && $v < 0.51) return 'Neutral default — not enough pipeline history yet to score stability.';
            if ($v < 0.40)   return 'Cluster is unstable across runs — members shift each pass.';
            if ($v < 0.75)   return 'Cluster is moderately persistent.';
            return 'Cluster recurs consistently — a durable group, not a coincidence.';
        },
    ],
    'recent_growth_score' => [
        'label' => 'Recent Growth',
        'short' => 'Whether the cluster gained members in the recent window.',
        'interpret' => static function (float $v): string {
            if ($v > 0.49 && $v < 0.51) return 'Neutral default — not enough snapshot history yet to score growth.';
            if ($v < 0.20)   return 'Cluster is shrinking or dormant.';
            if ($v < 0.50)   return 'Slight attrition compared to previous runs.';
            if ($v < 0.75)   return 'Growing — picking up new members recently.';
            return 'Rapid recent growth — actively recruiting or new operators joining.';
        },
    ],
];

/** Human label for a role token used inside the members table. */
$roleCatalog = [
    'anchor'     => ['label' => 'Anchor',     'desc' => 'Highest individual CI priority in the cluster — the centre of gravity.'],
    'ringleader' => ['label' => 'Ringleader', 'desc' => 'Strongest signal of coordinating the group.'],
    'bridge'     => ['label' => 'Bridge',     'desc' => 'Links this cluster to other communities — common spy-ring position.'],
    'suspect'    => ['label' => 'Suspect',    'desc' => 'Already flagged individually by counterintel.'],
    'member'     => ['label' => 'Member',     'desc' => 'Ordinary member — no special graph role assigned.'],
    'peripheral' => ['label' => 'Peripheral', 'desc' => 'On the edge of the cluster; weak attachment.'],
];

$severityLanguage = static function (string $tier, float $ringScore, float $confidence): string {
    $scorePct = number_format($ringScore * 100, 0) . '%';
    $confPct  = number_format($confidence * 100, 0) . '%';
    return match ($tier) {
        'critical' => "Critical — the ring-score model is highly confident ({$scorePct}, conf {$confPct}) that this group is coordinating hostile activity. Review as top priority.",
        'high'     => "High — strong combined signal ({$scorePct}, conf {$confPct}). Warrants a dedicated review and probably escalation.",
        'medium'   => "Medium — meaningful signal ({$scorePct}, conf {$confPct}). Worth a structured review but not an emergency.",
        'monitor'  => "Monitor — weak overall signal ({$scorePct}, conf {$confPct}). Keep an eye on it; no immediate action required.",
        default    => "Ring score {$scorePct}, confidence {$confPct}.",
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
    $ringScore = (float) $case['ring_score'];
    $confidence = (float) $case['confidence_score'];
    $memberCount = (int) $case['member_count'];

    // Component values from the top-level columns (authoritative) with JSON fallback.
    $components = [
        'suspicious_member_ratio' => (float) ($case['suspicious_member_ratio'] ?? 0),
        'bridge_concentration'    => (float) ($case['bridge_concentration'] ?? 0),
        'hostile_overlap_density' => (float) ($case['hostile_overlap_density'] ?? 0),
        'identity_density'        => (float) ($case['identity_density'] ?? 0),
        'recurrence_stability'    => (float) ($case['recurrence_stability'] ?? 0),
        'recent_growth_score'     => (float) ($case['recent_growth_score'] ?? 0),
    ];
    $weights = [];
    if (is_array($feature) && isset($feature['weights']) && is_array($feature['weights'])) {
        foreach ($feature['weights'] as $wk => $wv) {
            if (is_numeric($wv)) {
                $weights[(string) $wk] = (float) $wv;
            }
        }
    }

    // Build a narrative of the top and bottom signals, weighted by their contribution.
    $contribs = [];
    foreach ($components as $k => $v) {
        $w = $weights[$k] ?? 0.0;
        $contribs[$k] = ['value' => $v, 'weight' => $w, 'contribution' => $v * $w];
    }
    uasort($contribs, static fn($a, $b) => $b['contribution'] <=> $a['contribution']);
    $topSignals = [];
    $silentSignals = [];
    foreach ($contribs as $k => $c) {
        // Skip neutral defaults (exactly 0.5 for recurrence/growth) when listing strong signals.
        $isNeutralDefault = in_array($k, ['recurrence_stability', 'recent_growth_score'], true)
            && $c['value'] > 0.49 && $c['value'] < 0.51;
        if ($c['value'] > 0.15 && !$isNeutralDefault && count($topSignals) < 3) {
            $topSignals[$k] = $c;
        } elseif ($c['value'] <= 0.001) {
            $silentSignals[$k] = $c;
        }
    }

    // Members: count how many have real contribution / CI data.
    $memberWithContribution = 0;
    $memberWithCi = 0;
    foreach ($members as $m) {
        if ((float) ($m['member_contribution_score'] ?? 0) > 0.001) {
            $memberWithContribution++;
        }
        $mid = (int) $m['character_id'];
        if (isset($ciScores[$mid])) {
            $memberWithCi++;
        }
    }
?>
    <!-- Plain-English summary -->
    <section class="surface-primary mt-4 border-l-2 <?= match ($sev) {
        'critical' => 'border-red-500/70',
        'high' => 'border-orange-500/70',
        'medium' => 'border-amber-500/70',
        default => 'border-sky-500/60',
    } ?>">
        <h2 class="text-sm uppercase tracking-[0.18em] text-muted">What this case is</h2>
        <p class="mt-2 text-sm text-slate-200 leading-relaxed">
            A cluster of <strong class="text-slate-50"><?= $memberCount ?></strong>
            character<?= $memberCount === 1 ? '' : 's' ?> grouped together by the
            spy-ring graph projection
            <?php if (!empty($case['community_id'])): ?>
                (community <span class="font-mono text-slate-300">#<?= (int) $case['community_id'] ?></span>)
            <?php endif; ?>.
            <?= htmlspecialchars($severityLanguage($sev, $ringScore, $confidence)) ?>
        </p>
        <?php if ($topSignals !== []): ?>
            <p class="mt-3 text-sm text-slate-300 leading-relaxed">
                <span class="text-muted uppercase tracking-wider text-xs">Strongest signals:</span>
                <?php $first = true; foreach ($topSignals as $k => $c):
                    $label = $metricCatalog[$k]['label'] ?? $k;
                    $note = isset($metricCatalog[$k]['interpret'])
                        ? ($metricCatalog[$k]['interpret'])((float) $c['value'])
                        : '';
                ?>
                    <?= $first ? '' : ' ' ?>
                    <span class="block mt-1"><strong class="text-slate-100"><?= htmlspecialchars($label) ?>
                    (<?= number_format((float) $c['value'], 2) ?>)</strong> — <?= htmlspecialchars($note) ?></span>
                <?php $first = false; endforeach; ?>
            </p>
        <?php endif; ?>
        <?php if ($silentSignals !== []): ?>
            <p class="mt-3 text-xs text-muted leading-relaxed">
                <span class="uppercase tracking-wider">Not seen yet:</span>
                <?php $names = [];
                foreach ($silentSignals as $k => $c) {
                    $names[] = $metricCatalog[$k]['label'] ?? $k;
                }
                echo htmlspecialchars(implode(', ', $names));
                ?>.
                These signals returned zero for this cluster — either the evidence genuinely isn't there,
                or the upstream data (identity links, CI review scores, snapshot history) hasn't been
                populated yet.
            </p>
        <?php endif; ?>
        <?php if ($memberCount > 0 && $memberWithContribution === 0): ?>
            <p class="mt-3 text-xs text-amber-200/80 leading-relaxed">
                <strong>Heads up:</strong> member contribution scores are all zero, which typically means
                <code class="text-xs">compute_spy_risk_profiles</code> hasn't run (or hasn't populated
                review priorities) for these characters yet. The cluster structure below is valid, but
                per-member rankings will light up once that job completes.
            </p>
        <?php endif; ?>
    </section>

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
            <p class="mt-2 text-[11px] text-muted leading-snug">
                Tier boundaries: monitor &lt; 0.50 · medium 0.50 · high 0.70 · critical 0.85.
            </p>
        </div>
        <div class="surface-primary">
            <p class="text-xs uppercase tracking-wider text-muted">Ring Score</p>
            <p class="mt-1 text-2xl font-bold text-slate-100"><?= number_format($ringScore, 3) ?></p>
            <p class="mt-1 text-xs text-muted">Confidence <?= number_format($confidence, 3) ?></p>
            <p class="mt-2 text-[11px] text-muted leading-snug">
                Weighted combination of six signals below. 0 = no signal, 1 = maximum.
            </p>
        </div>
        <div class="surface-primary">
            <p class="text-xs uppercase tracking-wider text-muted">Members</p>
            <p class="mt-1 text-2xl font-bold text-slate-100"><?= $memberCount ?></p>
            <p class="mt-1 text-xs text-muted">
                <?= number_format($components['suspicious_member_ratio'] * 100, 0) ?>% already CI-flagged
            </p>
            <p class="mt-2 text-[11px] text-muted leading-snug">
                <?= htmlspecialchars(($metricCatalog['suspicious_member_ratio']['interpret'])($components['suspicious_member_ratio'])) ?>
            </p>
        </div>
        <div class="surface-primary">
            <p class="text-xs uppercase tracking-wider text-muted">Bridge Concentration</p>
            <p class="mt-1 text-2xl font-bold text-cyan-200"><?= number_format($components['bridge_concentration'], 3) ?></p>
            <p class="mt-1 text-xs text-muted">Hostile overlap <?= number_format($components['hostile_overlap_density'], 3) ?></p>
            <p class="mt-2 text-[11px] text-muted leading-snug">
                <?= htmlspecialchars(($metricCatalog['bridge_concentration']['interpret'])($components['bridge_concentration'])) ?>
            </p>
        </div>
        <div class="surface-primary">
            <p class="text-xs uppercase tracking-wider text-muted">Identity Density</p>
            <p class="mt-1 text-2xl font-bold text-purple-200"><?= number_format($components['identity_density'], 3) ?></p>
            <p class="mt-1 text-xs text-muted">Recurrence <?= number_format($components['recurrence_stability'], 3) ?></p>
            <p class="mt-2 text-[11px] text-muted leading-snug">
                <?= htmlspecialchars(($metricCatalog['identity_density']['interpret'])($components['identity_density'])) ?>
            </p>
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
    <section class="surface-primary mt-4">
        <div class="flex items-center justify-between mb-3">
            <h2 class="text-lg font-semibold text-slate-100">How the ring score was computed</h2>
            <span class="text-xs text-muted">Model <?= htmlspecialchars((string) ($case['model_version'] ?? '—')) ?></span>
        </div>
        <p class="text-xs text-muted mb-3 leading-relaxed">
            Six factors are each measured on a 0&ndash;1 scale, multiplied by a weight, and summed to
            produce the ring score. The largest "Contribution" values are the main drivers of this
            case's severity.
        </p>
        <div class="table-shell">
            <table class="table-ui">
                <thead>
                    <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                        <th class="px-3 py-2 text-left">Factor</th>
                        <th class="px-3 py-2 text-right">Value</th>
                        <th class="px-3 py-2 text-right">Weight</th>
                        <th class="px-3 py-2 text-right">Contribution</th>
                        <th class="px-3 py-2 text-left">What it means here</th>
                    </tr>
                </thead>
                <tbody>
                    <?php
                    // Order: largest contribution first.
                    $sorted = $contribs;
                    foreach ($sorted as $k => $row):
                        $label = $metricCatalog[$k]['label'] ?? $k;
                        $short = $metricCatalog[$k]['short'] ?? '';
                        $note = isset($metricCatalog[$k]['interpret'])
                            ? ($metricCatalog[$k]['interpret'])((float) $row['value'])
                            : '';
                        $v = (float) $row['value'];
                        $w = (float) $row['weight'];
                        $c = (float) $row['contribution'];
                        $valueClass = $v > 0.001 ? 'text-slate-100' : 'text-muted';
                        $contribClass = $c > 0.05 ? 'text-amber-200' : ($c > 0.001 ? 'text-slate-200' : 'text-muted');
                    ?>
                        <tr class="border-b border-border/30 align-top">
                            <td class="px-3 py-2">
                                <div class="text-sm text-slate-100"><?= htmlspecialchars($label) ?></div>
                                <?php if ($short !== ''): ?>
                                    <div class="text-[11px] text-muted leading-snug mt-0.5"><?= htmlspecialchars($short) ?></div>
                                <?php endif; ?>
                            </td>
                            <td class="px-3 py-2 text-right text-sm font-mono <?= $valueClass ?>">
                                <?= number_format($v, 3) ?>
                            </td>
                            <td class="px-3 py-2 text-right text-xs font-mono text-muted">
                                <?= $w > 0 ? number_format($w * 100, 0) . '%' : '—' ?>
                            </td>
                            <td class="px-3 py-2 text-right text-sm font-mono <?= $contribClass ?>">
                                <?= number_format($c, 3) ?>
                            </td>
                            <td class="px-3 py-2 text-xs text-slate-300 leading-snug">
                                <?= htmlspecialchars($note) ?>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                </tbody>
                <tfoot>
                    <tr class="text-xs uppercase tracking-[0.15em] text-muted">
                        <td class="px-3 py-2 text-right" colspan="3">Ring score (sum of contributions)</td>
                        <td class="px-3 py-2 text-right font-mono text-slate-100"><?= number_format($ringScore, 3) ?></td>
                        <td class="px-3 py-2 text-xs text-muted">
                            → <?= htmlspecialchars(strtoupper($sev)) ?> tier
                        </td>
                    </tr>
                </tfoot>
            </table>
        </div>
    </section>

    <!-- Members -->
    <?php
    // Summarise affiliations so an analyst can tell at a glance which
    // alliances / corps this cluster spans. Without this it's just a wall of
    // unfamiliar character names.
    $allianceCounts = [];
    $corpCounts = [];
    $noAffiliationCount = 0;
    foreach ($members as $m) {
        $allyId = (int) ($m['current_alliance_id'] ?? 0);
        $corpId = (int) ($m['current_corporation_id'] ?? 0);
        $allyName = trim((string) ($m['alliance_name'] ?? ''));
        $corpName = trim((string) ($m['corporation_name'] ?? ''));
        if ($allyId > 0 && $allyName !== '') {
            $allianceCounts[$allyId] = ($allianceCounts[$allyId] ?? ['name' => $allyName, 'count' => 0]);
            $allianceCounts[$allyId]['count']++;
        }
        if ($corpId > 0 && $corpName !== '') {
            $corpCounts[$corpId] = ($corpCounts[$corpId] ?? ['name' => $corpName, 'count' => 0]);
            $corpCounts[$corpId]['count']++;
        }
        if ($allyId === 0 && $corpId === 0) {
            $noAffiliationCount++;
        }
    }
    uasort($allianceCounts, static fn($a, $b) => $b['count'] <=> $a['count']);
    uasort($corpCounts, static fn($a, $b) => $b['count'] <=> $a['count']);
    ?>
    <section class="surface-primary mt-4">
        <div class="flex items-center justify-between mb-3">
            <h2 class="text-lg font-semibold text-slate-100">Members</h2>
            <span class="text-xs text-muted"><?= count($members) ?> shown</span>
        </div>

        <?php if ($allianceCounts !== [] || $corpCounts !== [] || $noAffiliationCount > 0): ?>
            <div class="mb-3 rounded border border-border/40 bg-slate-800/30 px-3 py-2">
                <p class="text-[11px] uppercase tracking-wider text-muted mb-1">Who's in this cluster</p>
                <?php if ($allianceCounts !== []): ?>
                    <div class="flex flex-wrap gap-1.5 mb-1">
                        <?php foreach (array_slice($allianceCounts, 0, 8, true) as $aid => $row): ?>
                            <span class="inline-flex items-center gap-1 rounded bg-slate-900/60 border border-border/50 px-2 py-0.5 text-xs">
                                <span class="text-slate-200"><?= htmlspecialchars($row['name']) ?></span>
                                <span class="text-muted font-mono">×<?= (int) $row['count'] ?></span>
                            </span>
                        <?php endforeach; ?>
                        <?php if (count($allianceCounts) > 8): ?>
                            <span class="text-xs text-muted self-center">+<?= count($allianceCounts) - 8 ?> more alliances</span>
                        <?php endif; ?>
                    </div>
                <?php endif; ?>
                <?php if ($corpCounts !== []): ?>
                    <div class="flex flex-wrap gap-1.5">
                        <?php foreach (array_slice($corpCounts, 0, 8, true) as $cid => $row): ?>
                            <span class="inline-flex items-center gap-1 rounded bg-slate-900/40 border border-border/30 px-2 py-0.5 text-[11px]">
                                <span class="text-slate-300"><?= htmlspecialchars($row['name']) ?></span>
                                <span class="text-muted font-mono">×<?= (int) $row['count'] ?></span>
                            </span>
                        <?php endforeach; ?>
                        <?php if (count($corpCounts) > 8): ?>
                            <span class="text-[11px] text-muted self-center">+<?= count($corpCounts) - 8 ?> more corps</span>
                        <?php endif; ?>
                    </div>
                <?php endif; ?>
                <?php if ($noAffiliationCount > 0): ?>
                    <p class="text-[11px] text-muted mt-1">
                        <?= $noAffiliationCount ?> member<?= $noAffiliationCount === 1 ? '' : 's' ?> without cached org history
                        — run <code class="text-[11px]">fetch_character_org_history</code> for affiliation data.
                    </p>
                <?php endif; ?>
            </div>
        <?php endif; ?>

        <!-- Role legend -->
        <div class="mb-3 flex flex-wrap gap-2 text-[11px] text-muted">
            <span class="uppercase tracking-wider">Roles:</span>
            <?php foreach (['anchor', 'bridge', 'member'] as $roleKey):
                $roleInfo = $roleCatalog[$roleKey] ?? ['label' => ucfirst($roleKey), 'desc' => ''];
            ?>
                <span class="inline-flex items-center gap-1">
                    <span class="inline-block rounded border px-1.5 py-0.5 text-[10px] uppercase <?= $roleBadge($roleKey) ?>">
                        <?= htmlspecialchars($roleInfo['label']) ?>
                    </span>
                    <span class="text-muted"><?= htmlspecialchars($roleInfo['desc']) ?></span>
                </span>
            <?php endforeach; ?>
        </div>

        <?php if ($members): ?>
            <div class="table-shell">
                <table class="table-ui">
                    <thead>
                        <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                            <th class="px-3 py-2 text-left">Character</th>
                            <th class="px-3 py-2 text-left">Affiliation</th>
                            <th class="px-3 py-2 text-left">Role</th>
                            <th class="px-3 py-2 text-right">Contribution</th>
                            <th class="px-3 py-2 text-right">CI Priority</th>
                            <th class="px-3 py-2 text-right">CI %ile</th>
                        </tr>
                    </thead>
                    <tbody>
                        <?php foreach ($members as $m):
                            $cid = (int) $m['character_id'];
                            $role = (string) ($m['role_label'] ?? 'member');
                            $ci = $ciScores[$cid] ?? null;
                            $allyName = trim((string) ($m['alliance_name'] ?? ''));
                            $corpName = trim((string) ($m['corporation_name'] ?? ''));
                        ?>
                            <tr class="border-b border-border/30 hover:bg-slate-800/40 transition-colors">
                                <td class="px-3 py-2 text-sm">
                                    <a href="/killmail-intelligence/?character_id=<?= $cid ?>" class="text-cyan-300 hover:text-cyan-100">
                                        <?= htmlspecialchars((string) $m['character_name']) ?>
                                    </a>
                                    <span class="ml-1 text-[10px] text-muted font-mono">#<?= $cid ?></span>
                                </td>
                                <td class="px-3 py-2 text-xs">
                                    <?php if ($allyName !== '' || $corpName !== ''): ?>
                                        <?php if ($allyName !== ''): ?>
                                            <div class="text-slate-200"><?= htmlspecialchars($allyName) ?></div>
                                        <?php endif; ?>
                                        <?php if ($corpName !== ''): ?>
                                            <div class="text-muted"><?= htmlspecialchars($corpName) ?></div>
                                        <?php endif; ?>
                                    <?php else: ?>
                                        <span class="text-muted italic">no org data</span>
                                    <?php endif; ?>
                                </td>
                                <td class="px-3 py-2">
                                    <span class="inline-block rounded border px-1.5 py-0.5 text-[10px] uppercase <?= $roleBadge($role) ?>"
                                          title="<?= htmlspecialchars(($roleCatalog[strtolower($role)]['desc'] ?? '')) ?>">
                                        <?= htmlspecialchars(strtoupper($role)) ?>
                                    </span>
                                </td>
                                <td class="px-3 py-2 text-right text-sm font-mono <?= (float) $m['member_contribution_score'] > 0.001 ? 'text-slate-100' : 'text-muted' ?>">
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
            <?php if ($memberCount > 0 && $memberWithCi === 0): ?>
                <p class="mt-3 text-xs text-muted leading-snug">
                    No CI priority scores available for any member of this case. CI scores come from
                    <code class="text-xs">character_counterintel_scores</code> — run the counterintel
                    scoring pipeline to populate.
                </p>
            <?php endif; ?>
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
        <p class="text-xs text-muted mb-3 leading-relaxed">
            Pairs of members with an explicit evidence link (shared-operator identity, copresence in
            hostile fleets, etc.). An empty list means members were only grouped by the broader graph
            structure — no pairwise "smoking gun" has been recorded.
        </p>

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
                            $edgeComponents = $e['component_weights_json'] ?? null;
                            if (is_string($edgeComponents)) {
                                $componentsData = json_decode($edgeComponents, true);
                                $edgeComponents = is_array($componentsData) ? $componentsData : null;
                            }
                            $nameA = trim((string) ($e['character_name_a'] ?? '')) ?: ('Character #' . $aid);
                            $nameB = trim((string) ($e['character_name_b'] ?? '')) ?: ('Character #' . $bid);
                            $allyA = trim((string) ($e['alliance_name_a'] ?? ''));
                            $allyB = trim((string) ($e['alliance_name_b'] ?? ''));
                        ?>
                            <tr class="border-b border-border/30 hover:bg-slate-800/40 transition-colors">
                                <td class="px-3 py-2 text-sm">
                                    <a href="/killmail-intelligence/?character_id=<?= $aid ?>" class="text-cyan-300 hover:text-cyan-100">
                                        <?= htmlspecialchars($nameA) ?>
                                    </a>
                                    <?php if ($allyA !== ''): ?>
                                        <div class="text-[11px] text-muted"><?= htmlspecialchars($allyA) ?></div>
                                    <?php endif; ?>
                                </td>
                                <td class="px-3 py-2 text-sm">
                                    <a href="/killmail-intelligence/?character_id=<?= $bid ?>" class="text-cyan-300 hover:text-cyan-100">
                                        <?= htmlspecialchars($nameB) ?>
                                    </a>
                                    <?php if ($allyB !== ''): ?>
                                        <div class="text-[11px] text-muted"><?= htmlspecialchars($allyB) ?></div>
                                    <?php endif; ?>
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
                                    <?php if (is_array($edgeComponents)): ?>
                                        <?php
                                        $parts = [];
                                        foreach ($edgeComponents as $ck => $cv) {
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
            <p class="text-sm text-muted">
                No pairwise evidence links recorded for this case. The cluster was formed purely from
                community-detection (Leiden) structure; specific member-to-member links would be added
                by identity resolution or cross-side copresence jobs.
            </p>
        <?php endif; ?>
    </section>
<?php endif; ?>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
