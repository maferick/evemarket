<?php

declare(strict_types=1);
require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Spy Detection — Ring Cases & Risk Profiles';

$tablesReadyError = null;
$summary = ['cases' => [], 'risk_profiles' => [], 'identity_links' => []];
$cases = [];
$topRisks = [];

$severityFilter = isset($_GET['severity']) ? trim((string) $_GET['severity']) : '';
$statusFilter = isset($_GET['status']) ? trim((string) $_GET['status']) : '';
$severityParam = in_array($severityFilter, ['monitor', 'medium', 'high', 'critical'], true) ? $severityFilter : null;
$statusParam = in_array($statusFilter, ['open', 'reviewing', 'closed', 'reopened'], true) ? $statusFilter : null;

try {
    $summary = db_spy_detection_summary();
    $cases = db_spy_network_cases_recent(50, $severityParam, $statusParam);
    $topRisks = db_character_spy_risk_top(25, $severityParam);
} catch (Throwable $e) {
    $tablesReadyError = $e->getMessage();
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

$confidenceBadge = static function (string $tier): string {
    return match ($tier) {
        'high' => 'bg-emerald-900/40 text-emerald-200 border-emerald-700/50',
        'medium' => 'bg-sky-900/40 text-sky-200 border-sky-700/50',
        'low' => 'bg-slate-800 text-slate-400 border-slate-700',
        default => 'bg-slate-800 text-slate-300 border-slate-700',
    };
};

include __DIR__ . '/../../src/views/partials/header.php';

$caseStats = $summary['cases'] ?? [];
$riskStats = $summary['risk_profiles'] ?? [];
$linkStats = $summary['identity_links'] ?? [];
?>

<section class="surface-primary">
    <div class="flex items-start justify-between gap-4">
        <div>
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Counterintel</p>
            <h1 class="mt-1 text-2xl font-semibold text-slate-50">Spy Detection</h1>
            <p class="mt-2 text-sm text-muted">
                Ring-scored network cases from Leiden community detection, per-character spy risk profiles,
                and identity-resolution links. Built by the <code class="text-xs">graph_spy_ring_projection</code> &rarr;
                <code class="text-xs">compute_spy_network_cases</code> &rarr; <code class="text-xs">compute_spy_risk_profiles</code>
                pipeline.
            </p>
        </div>
        <div class="flex items-center gap-2">
            <a href="/battle-intelligence/" class="btn-secondary">Battle Intel</a>
            <a href="/threat-corridors/" class="btn-secondary">Threat Corridors</a>
        </div>
    </div>
</section>

<?php if ($tablesReadyError !== null): ?>
<section class="surface-primary mt-4 border border-amber-500/30 bg-amber-500/10">
    <p class="text-sm text-amber-200">
        <strong>Spy detection tables not ready.</strong>
        Run the <code>20260411_spy_network_cases.sql</code>, <code>20260411_character_spy_risk_profiles.sql</code>
        and <code>20260411_identity_resolution.sql</code> migrations, then execute the spy pipeline jobs
        (<code>graph_spy_ring_projection</code>, <code>compute_spy_network_cases</code>,
        <code>compute_spy_risk_profiles</code>, <code>compute_character_identity_links</code>).
    </p>
    <p class="mt-2 text-xs text-amber-200/70">Error: <?= htmlspecialchars($tablesReadyError, ENT_QUOTES) ?></p>
</section>
<?php endif; ?>

<!-- KPI Summary Cards -->
<section class="mt-4 grid grid-cols-2 gap-4 md:grid-cols-4">
    <div class="surface-primary">
        <p class="text-xs uppercase tracking-wider text-muted">Ring Cases</p>
        <p class="mt-1 text-2xl font-bold text-slate-100"><?= number_format((int) ($caseStats['total'] ?? 0)) ?></p>
        <p class="mt-1 text-xs text-muted">
            <span class="text-red-300"><?= (int) ($caseStats['critical'] ?? 0) ?> crit</span> ·
            <span class="text-orange-300"><?= (int) ($caseStats['high'] ?? 0) ?> high</span> ·
            <span class="text-amber-300"><?= (int) ($caseStats['medium'] ?? 0) ?> med</span> ·
            <span class="text-sky-300"><?= (int) ($caseStats['monitor'] ?? 0) ?> mon</span>
        </p>
    </div>
    <div class="surface-primary">
        <p class="text-xs uppercase tracking-wider text-muted">Open / Reviewing</p>
        <p class="mt-1 text-2xl font-bold text-slate-100">
            <?= (int) ($caseStats['open_cases'] ?? 0) ?>
            <span class="text-sm text-muted">/ <?= (int) ($caseStats['reviewing_cases'] ?? 0) ?></span>
        </p>
        <p class="mt-1 text-xs text-muted">
            Last reinforced:
            <?= htmlspecialchars((string) ($caseStats['last_reinforced_at'] ?? '—')) ?>
        </p>
    </div>
    <div class="surface-primary">
        <p class="text-xs uppercase tracking-wider text-muted">Risk Profiles</p>
        <p class="mt-1 text-2xl font-bold text-slate-100"><?= number_format((int) ($riskStats['total_profiles'] ?? 0)) ?></p>
        <p class="mt-1 text-xs text-muted">
            <span class="text-red-300"><?= (int) ($riskStats['critical_profiles'] ?? 0) ?> crit</span> ·
            <span class="text-orange-300"><?= (int) ($riskStats['high_profiles'] ?? 0) ?> high</span> ·
            <span class="text-emerald-300"><?= (int) ($riskStats['high_confidence'] ?? 0) ?> hi-conf</span>
        </p>
    </div>
    <div class="surface-primary">
        <p class="text-xs uppercase tracking-wider text-muted">Identity Links</p>
        <p class="mt-1 text-2xl font-bold text-slate-100"><?= number_format((int) ($linkStats['total_links'] ?? 0)) ?></p>
        <p class="mt-1 text-xs text-muted">
            <span class="text-emerald-300"><?= (int) ($linkStats['high_confidence'] ?? 0) ?> high-confidence</span>
        </p>
    </div>
</section>

<!-- Filters -->
<section class="surface-primary mt-4">
    <form method="get" class="flex flex-wrap items-end gap-3">
        <div>
            <label class="block text-xs uppercase tracking-wider text-muted mb-1">Severity</label>
            <select name="severity" class="bg-slate-800 border border-slate-700 text-slate-100 rounded px-2 py-1 text-sm">
                <option value="" <?= $severityParam === null ? 'selected' : '' ?>>All</option>
                <?php foreach (['critical', 'high', 'medium', 'monitor'] as $tier): ?>
                    <option value="<?= $tier ?>" <?= $severityParam === $tier ? 'selected' : '' ?>><?= ucfirst($tier) ?></option>
                <?php endforeach; ?>
            </select>
        </div>
        <div>
            <label class="block text-xs uppercase tracking-wider text-muted mb-1">Status</label>
            <select name="status" class="bg-slate-800 border border-slate-700 text-slate-100 rounded px-2 py-1 text-sm">
                <option value="" <?= $statusParam === null ? 'selected' : '' ?>>All</option>
                <?php foreach (['open', 'reviewing', 'closed', 'reopened'] as $st): ?>
                    <option value="<?= $st ?>" <?= $statusParam === $st ? 'selected' : '' ?>><?= ucfirst($st) ?></option>
                <?php endforeach; ?>
            </select>
        </div>
        <button type="submit" class="btn-primary text-sm">Apply</button>
        <?php if ($severityParam !== null || $statusParam !== null): ?>
            <a href="/spy-detection/" class="text-xs text-muted hover:text-slate-200">Clear</a>
        <?php endif; ?>
    </form>
</section>

<!-- Spy Network Cases -->
<section class="surface-primary mt-4">
    <div class="flex items-center justify-between mb-3">
        <h2 class="text-lg font-semibold text-slate-100">Spy Network Cases</h2>
        <span class="text-xs text-muted">Top <?= count($cases) ?> by ring score</span>
    </div>

    <?php if ($cases): ?>
        <div class="table-shell">
            <table class="table-ui">
                <thead>
                    <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                        <th class="px-3 py-2 text-left">Case</th>
                        <th class="px-3 py-2 text-left">Severity</th>
                        <th class="px-3 py-2 text-left">Status</th>
                        <th class="px-3 py-2 text-right">Ring</th>
                        <th class="px-3 py-2 text-right">Confidence</th>
                        <th class="px-3 py-2 text-right">Members</th>
                        <th class="px-3 py-2 text-right">Bridges</th>
                        <th class="px-3 py-2 text-right">Hostile</th>
                        <th class="px-3 py-2 text-right">Identity</th>
                        <th class="px-3 py-2 text-left">Last Reinforced</th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach ($cases as $c):
                        $caseId = (int) $c['case_id'];
                        $sev = (string) ($c['severity_tier'] ?? 'monitor');
                        $st = (string) ($c['status'] ?? 'open');
                    ?>
                        <tr class="border-b border-border/30 hover:bg-slate-800/40 transition-colors">
                            <td class="px-3 py-2 text-sm">
                                <a href="/spy-detection/case.php?case_id=<?= $caseId ?>" class="text-cyan-300 hover:text-cyan-100 font-mono">
                                    #<?= $caseId ?>
                                </a>
                                <?php if (!empty($c['community_id'])): ?>
                                    <span class="ml-1 text-[10px] text-muted">comm <?= (int) $c['community_id'] ?></span>
                                <?php endif; ?>
                            </td>
                            <td class="px-3 py-2">
                                <span class="inline-block rounded border px-1.5 py-0.5 text-[10px] uppercase <?= $severityBadge($sev) ?>">
                                    <?= htmlspecialchars(strtoupper($sev)) ?>
                                </span>
                            </td>
                            <td class="px-3 py-2">
                                <span class="inline-block rounded border px-1.5 py-0.5 text-[10px] uppercase <?= $statusBadge($st) ?>">
                                    <?= htmlspecialchars(strtoupper($st)) ?>
                                </span>
                            </td>
                            <td class="px-3 py-2 text-right text-sm font-mono text-slate-100"><?= number_format((float) $c['ring_score'], 3) ?></td>
                            <td class="px-3 py-2 text-right text-sm font-mono text-slate-300"><?= number_format((float) $c['confidence_score'], 3) ?></td>
                            <td class="px-3 py-2 text-right text-sm font-mono"><?= (int) $c['member_count'] ?></td>
                            <td class="px-3 py-2 text-right text-sm font-mono text-muted"><?= number_format((float) $c['bridge_concentration'], 3) ?></td>
                            <td class="px-3 py-2 text-right text-sm font-mono text-muted"><?= number_format((float) $c['hostile_overlap_density'], 3) ?></td>
                            <td class="px-3 py-2 text-right text-sm font-mono text-muted"><?= number_format((float) $c['identity_density'], 3) ?></td>
                            <td class="px-3 py-2 text-xs text-muted"><?= htmlspecialchars((string) $c['last_reinforced_at']) ?></td>
                        </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
    <?php else: ?>
        <p class="text-sm text-muted">No ring cases match the current filters.</p>
    <?php endif; ?>
</section>

<!-- Top risk profiles -->
<section class="surface-primary mt-4">
    <div class="flex items-center justify-between mb-3">
        <h2 class="text-lg font-semibold text-slate-100">Highest Individual Risk</h2>
        <span class="text-xs text-muted">Top <?= count($topRisks) ?> by spy risk score</span>
    </div>

    <?php if ($topRisks): ?>
        <div class="table-shell">
            <table class="table-ui">
                <thead>
                    <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                        <th class="px-3 py-2 text-left">Character</th>
                        <th class="px-3 py-2 text-left">Severity</th>
                        <th class="px-3 py-2 text-left">Confidence</th>
                        <th class="px-3 py-2 text-right">Risk</th>
                        <th class="px-3 py-2 text-right">Percentile</th>
                        <th class="px-3 py-2 text-left">Top Case</th>
                        <th class="px-3 py-2 text-left">Computed</th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach ($topRisks as $r):
                        $cid = (int) $r['character_id'];
                        $sev = (string) ($r['severity_tier'] ?? 'monitor');
                        $conf = (string) ($r['confidence_tier'] ?? 'low');
                        $topCase = (int) ($r['top_case_id'] ?? 0);
                    ?>
                        <tr class="border-b border-border/30 hover:bg-slate-800/40 transition-colors">
                            <td class="px-3 py-2 text-sm">
                                <a href="/killmail-intelligence/?character_id=<?= $cid ?>" class="text-cyan-300 hover:text-cyan-100">
                                    <?= htmlspecialchars((string) $r['character_name']) ?>
                                </a>
                                <span class="ml-1 text-[10px] text-muted font-mono">#<?= $cid ?></span>
                            </td>
                            <td class="px-3 py-2">
                                <span class="inline-block rounded border px-1.5 py-0.5 text-[10px] uppercase <?= $severityBadge($sev) ?>">
                                    <?= htmlspecialchars(strtoupper($sev)) ?>
                                </span>
                            </td>
                            <td class="px-3 py-2">
                                <span class="inline-block rounded border px-1.5 py-0.5 text-[10px] uppercase <?= $confidenceBadge($conf) ?>">
                                    <?= htmlspecialchars(strtoupper($conf)) ?>
                                </span>
                            </td>
                            <td class="px-3 py-2 text-right text-sm font-mono text-slate-100"><?= number_format((float) $r['spy_risk_score'], 3) ?></td>
                            <td class="px-3 py-2 text-right text-sm font-mono text-muted"><?= number_format((float) $r['risk_percentile'] * 100, 1) ?>%</td>
                            <td class="px-3 py-2 text-xs">
                                <?php if ($topCase > 0): ?>
                                    <a href="/spy-detection/case.php?case_id=<?= $topCase ?>" class="text-cyan-300 hover:text-cyan-100 font-mono">#<?= $topCase ?></a>
                                <?php else: ?>
                                    <span class="text-muted">—</span>
                                <?php endif; ?>
                            </td>
                            <td class="px-3 py-2 text-xs text-muted"><?= htmlspecialchars((string) $r['computed_at']) ?></td>
                        </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
    <?php else: ?>
        <p class="text-sm text-muted">No risk profiles available.</p>
    <?php endif; ?>
</section>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
