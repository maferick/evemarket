<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Intelligence Events';

// Filters
$filterState    = (string) ($_GET['state'] ?? 'active');
$filterFamily   = (string) ($_GET['family'] ?? '');
$filterSeverity = (string) ($_GET['severity'] ?? '');
$sortBy         = (string) ($_GET['sort'] ?? 'impact_score');
$sortDir        = (string) ($_GET['dir'] ?? 'DESC');
$page           = max(1, (int) ($_GET['page'] ?? 1));
$perPage        = 50;
$offset         = ($page - 1) * $perPage;

// Data
$calibration = db_intelligence_calibration_latest();
$summary = db_intelligence_events_summary();
$totalCount = db_intelligence_events_count($filterFamily, $filterSeverity, $filterState);
$events = db_intelligence_events_queue($perPage, $offset, $filterFamily, $filterSeverity, $filterState, '', 0, $sortBy, $sortDir);
$totalPages = max(1, (int) ceil($totalCount / $perPage));
$digest = db_intelligence_event_digest_latest();

// Handle bulk acknowledge
if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['bulk_acknowledge'])) {
    $ids = array_map('intval', (array) ($_POST['event_ids'] ?? []));
    $analyst = 'analyst'; // placeholder — would come from session in production
    $count = db_intelligence_events_bulk_acknowledge($ids, $analyst, 'Bulk acknowledged from queue');
    header('Location: /intelligence-events/?state=' . urlencode($filterState) . '&family=' . urlencode($filterFamily) . '&ack=' . $count);
    exit;
}

$ackMessage = isset($_GET['ack']) ? ((int) $_GET['ack']) . ' event(s) acknowledged.' : '';

/**
 * Build a query string preserving current filters.
 */
function ie_filter_url(array $overrides = []): string
{
    $params = [
        'state' => $_GET['state'] ?? 'active',
        'family' => $_GET['family'] ?? '',
        'severity' => $_GET['severity'] ?? '',
        'sort' => $_GET['sort'] ?? 'impact_score',
        'dir' => $_GET['dir'] ?? 'DESC',
        'page' => $_GET['page'] ?? '1',
    ];
    $params = array_merge($params, $overrides);
    $params = array_filter($params, static fn($v): bool => $v !== '' && $v !== '0' && $v !== 0);
    return '/intelligence-events/?' . http_build_query($params);
}

/**
 * Format a datetime string as a relative age (e.g. "3d 5h", "2h 14m", "< 1m").
 */
function ie_relative_age(string $datetime): string
{
    if ($datetime === '') {
        return '—';
    }
    $ts = strtotime($datetime);
    if ($ts === false) {
        return '—';
    }
    $diff = time() - $ts;
    if ($diff < 60) {
        return '< 1m';
    }
    if ($diff < 3600) {
        return (int) ($diff / 60) . 'm';
    }
    if ($diff < 86400) {
        $h = (int) ($diff / 3600);
        $m = (int) (($diff % 3600) / 60);
        return $h . 'h ' . $m . 'm';
    }
    $d = (int) ($diff / 86400);
    $h = (int) (($diff % 86400) / 3600);
    return $d . 'd ' . $h . 'h';
}

include __DIR__ . '/../../src/views/partials/header.php';
?>

<section class="surface-primary">
    <div class="flex items-center justify-between gap-4">
        <div>
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Character Intelligence Profile</p>
            <h1 class="mt-1 text-2xl font-semibold text-slate-50">Intelligence Event Queue</h1>
            <p class="mt-2 text-sm text-muted">Lifecycle-managed events from the CIP delta engine. Triage, acknowledge, and resolve detected threats and profile changes.</p>
        </div>
    </div>

    <?php if ($ackMessage !== ''): ?>
        <div class="mt-3 rounded bg-emerald-900/40 border border-emerald-700/50 px-4 py-2 text-sm text-emerald-300"><?= htmlspecialchars($ackMessage, ENT_QUOTES) ?></div>
    <?php endif; ?>

    <!-- Summary counters -->
    <div class="mt-5 grid gap-3 sm:grid-cols-2 md:grid-cols-4 lg:grid-cols-6">
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Active events</p>
            <p class="mt-1 text-2xl font-semibold text-slate-100"><?= number_format($summary['total_active']) ?></p>
            <p class="text-[10px] text-muted mt-1">Open events awaiting triage or action</p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Critical severity</p>
            <p class="mt-1 text-2xl font-semibold <?= $summary['active_critical'] > 0 ? 'text-red-400' : 'text-slate-500' ?>"><?= number_format($summary['active_critical']) ?></p>
            <p class="text-[10px] text-muted mt-1">Highest urgency, escalated threats</p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">High severity</p>
            <p class="mt-1 text-2xl font-semibold <?= $summary['active_high'] > 0 ? 'text-orange-400' : 'text-slate-500' ?>"><?= number_format($summary['active_high']) ?></p>
            <p class="text-[10px] text-muted mt-1">Rank entries, multi-domain, compounds</p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Threat events</p>
            <p class="mt-1 text-2xl font-semibold text-slate-100"><?= number_format($summary['active_threat']) ?></p>
            <p class="text-[10px] text-muted mt-1">Who needs attention?</p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Profile quality</p>
            <p class="mt-1 text-2xl font-semibold text-slate-100"><?= number_format($summary['active_quality']) ?></p>
            <p class="text-[10px] text-muted mt-1">How much should we trust the picture?</p>
        </div>
        <div class="surface-tertiary">
            <p class="text-xs text-muted">Resolved (24h)</p>
            <p class="mt-1 text-2xl font-semibold text-emerald-400"><?= number_format($summary['resolved_24h']) ?></p>
            <p class="text-[10px] text-muted mt-1">Events cleared in the last day</p>
        </div>
    </div>

    <!-- Digest banner -->
    <?php if ($digest !== null): ?>
    <div class="mt-4 surface-tertiary">
        <div class="flex items-center justify-between">
            <div>
                <p class="text-xs text-muted">Latest digest &mdash; <?= htmlspecialchars((string) ($digest['digest_type'] ?? 'daily'), ENT_QUOTES) ?></p>
                <p class="mt-1 text-sm text-slate-200">
                    <?= (int) $digest['new_events'] ?> new,
                    <?= (int) $digest['escalated_events'] ?> escalated,
                    <?= (int) $digest['resolved_events'] ?> resolved
                    <span class="text-muted ml-2">(<?= htmlspecialchars((string) $digest['period_start'], ENT_QUOTES) ?> &ndash; <?= htmlspecialchars((string) $digest['period_end'], ENT_QUOTES) ?>)</span>
                </p>
            </div>
            <?php if ((int) ($digest['threat_critical'] ?? 0) > 0): ?>
                <span class="inline-flex items-center rounded-full bg-red-900/60 text-red-300 border border-red-800/50 px-2.5 py-1 text-xs font-semibold"><?= (int) $digest['threat_critical'] ?> critical threats</span>
            <?php endif; ?>
        </div>
    </div>
    <?php endif; ?>

    <!-- Filters -->
    <div class="mt-4 flex flex-wrap items-center gap-2 text-xs">
        <span class="text-muted">State:</span>
        <?php foreach (['active', 'acknowledged', 'suppressed', 'resolved', 'expired', 'all'] as $s): ?>
            <a href="<?= ie_filter_url(['state' => $s, 'page' => '1']) ?>"
               class="rounded px-2 py-1 <?= $filterState === $s ? 'bg-accent/20 text-accent font-semibold' : 'text-slate-400 hover:text-slate-200' ?>"><?= ucfirst($s) ?></a>
        <?php endforeach; ?>

        <span class="text-muted ml-3">Family:</span>
        <a href="<?= ie_filter_url(['family' => '', 'page' => '1']) ?>"
           class="rounded px-2 py-1 <?= $filterFamily === '' ? 'bg-accent/20 text-accent font-semibold' : 'text-slate-400 hover:text-slate-200' ?>">All</a>
        <a href="<?= ie_filter_url(['family' => 'threat', 'page' => '1']) ?>"
           class="rounded px-2 py-1 <?= $filterFamily === 'threat' ? 'bg-accent/20 text-accent font-semibold' : 'text-slate-400 hover:text-slate-200' ?>">Threat</a>
        <a href="<?= ie_filter_url(['family' => 'profile_quality', 'page' => '1']) ?>"
           class="rounded px-2 py-1 <?= $filterFamily === 'profile_quality' ? 'bg-accent/20 text-accent font-semibold' : 'text-slate-400 hover:text-slate-200' ?>">Profile Quality</a>

        <span class="text-muted ml-3">Severity:</span>
        <a href="<?= ie_filter_url(['severity' => '', 'page' => '1']) ?>"
           class="rounded px-2 py-1 <?= $filterSeverity === '' ? 'bg-accent/20 text-accent font-semibold' : 'text-slate-400 hover:text-slate-200' ?>">All</a>
        <?php foreach (['critical', 'high', 'medium', 'low', 'info'] as $sev): ?>
            <a href="<?= ie_filter_url(['severity' => $sev, 'page' => '1']) ?>"
               class="rounded px-2 py-1 <?= $filterSeverity === $sev ? 'bg-accent/20 text-accent font-semibold' : 'text-slate-400 hover:text-slate-200' ?>"><?= ucfirst($sev) ?></a>
        <?php endforeach; ?>
    </div>

    <!-- Event table -->
    <form method="post" action="/intelligence-events/">
        <input type="hidden" name="state" value="<?= htmlspecialchars($filterState, ENT_QUOTES) ?>">
        <input type="hidden" name="family" value="<?= htmlspecialchars($filterFamily, ENT_QUOTES) ?>">

        <div class="mt-4 table-shell">
            <table class="table-ui">
                <thead>
                <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                    <?php if ($filterState === 'active'): ?><th class="px-3 py-2 w-8"><input type="checkbox" id="select-all" class="accent-accent"></th><?php endif; ?>
                    <th class="px-3 py-2 text-left">
                        <a href="<?= ie_filter_url(['sort' => 'last_updated_at', 'dir' => ($sortBy === 'last_updated_at' && $sortDir === 'DESC') ? 'ASC' : 'DESC']) ?>" class="hover:text-slate-200">Event</a>
                    </th>
                    <th class="px-3 py-2 text-left">Entity</th>
                    <th class="px-3 py-2 text-center">Severity</th>
                    <th class="px-3 py-2 text-right">
                        <a href="<?= ie_filter_url(['sort' => 'impact_score', 'dir' => ($sortBy === 'impact_score' && $sortDir === 'DESC') ? 'ASC' : 'DESC']) ?>" class="hover:text-slate-200">Impact</a>
                    </th>
                    <th class="px-3 py-2 text-center">Family</th>
                    <th class="px-3 py-2 text-center" title="Priority band from calibrated risk score thresholds">Band</th>
                    <th class="px-3 py-2 text-right">
                        <a href="<?= ie_filter_url(['sort' => 'escalation_count', 'dir' => ($sortBy === 'escalation_count' && $sortDir === 'DESC') ? 'ASC' : 'DESC']) ?>" class="hover:text-slate-200">Esc.</a>
                    </th>
                    <th class="px-3 py-2 text-right">
                        <a href="<?= ie_filter_url(['sort' => 'first_detected_at', 'dir' => ($sortBy === 'first_detected_at' && $sortDir === 'DESC') ? 'ASC' : 'DESC']) ?>" class="hover:text-slate-200">Detected</a>
                    </th>
                    <th class="px-3 py-2 text-right" title="How long this event has existed / been in current state">Age</th>
                    <th class="px-3 py-2 text-right">Inspect</th>
                </tr>
                </thead>
                <tbody>
                <?php if ($events === []): ?>
                    <tr><td colspan="11" class="px-3 py-6 text-sm text-muted">No events matching filters.</td></tr>
                <?php else: ?>
                    <?php foreach ($events as $ev): ?>
                        <?php
                        $sev = (string) ($ev['severity'] ?? 'medium');
                        $sevClasses = match ($sev) {
                            'critical' => 'bg-red-900/60 text-red-300 border border-red-800/50',
                            'high'     => 'bg-orange-900/60 text-orange-300 border border-orange-800/50',
                            'medium'   => 'bg-amber-900/60 text-amber-300 border border-amber-800/50',
                            'low'      => 'bg-yellow-900/60 text-yellow-400 border border-yellow-800/50',
                            default    => 'bg-slate-700 text-slate-300 border border-slate-600/50',
                        };
                        $familyLabel = ($ev['event_family'] ?? '') === 'threat' ? 'Threat' : 'Quality';
                        $familyClass = ($ev['event_family'] ?? '') === 'threat' ? 'text-red-400' : 'text-cyan-400';
                        $stateIcon = match ($ev['state'] ?? '') {
                            'acknowledged' => '<span class="text-amber-400" title="Acknowledged">&#9679;</span>',
                            'suppressed'   => '<span class="text-slate-400" title="Suppressed">&#8856;</span>',
                            'resolved'     => '<span class="text-emerald-400" title="Resolved">&#10003;</span>',
                            'expired'      => '<span class="text-slate-500" title="Expired">&#10005;</span>',
                            default        => '',
                        };
                        ?>
                        <tr class="border-b border-border/50">
                            <?php if ($filterState === 'active'): ?>
                                <td class="px-3 py-2"><input type="checkbox" name="event_ids[]" value="<?= (int) $ev['id'] ?>" class="event-checkbox accent-accent"></td>
                            <?php endif; ?>
                            <td class="px-3 py-2">
                                <div class="text-slate-100 text-sm"><?= $stateIcon ?> <?= htmlspecialchars((string) ($ev['title'] ?? $ev['event_type']), ENT_QUOTES) ?></div>
                                <div class="text-[10px] text-muted mt-0.5"><?= htmlspecialchars((string) ($ev['event_type'] ?? ''), ENT_QUOTES) ?></div>
                            </td>
                            <td class="px-3 py-2 text-sm">
                                <?php if (($ev['entity_name'] ?? '') !== ''): ?>
                                    <a class="text-accent" href="/battle-intelligence/character.php?character_id=<?= urlencode((string) ((int) ($ev['entity_id'] ?? 0))) ?>"><?= htmlspecialchars((string) $ev['entity_name'], ENT_QUOTES) ?></a>
                                <?php else: ?>
                                    <span class="text-muted"><?= htmlspecialchars((string) ($ev['entity_type'] ?? ''), ENT_QUOTES) ?> #<?= (int) ($ev['entity_id'] ?? 0) ?></span>
                                <?php endif; ?>
                            </td>
                            <td class="px-3 py-2 text-center">
                                <span class="inline-flex items-center rounded px-2 py-0.5 text-xs font-medium <?= $sevClasses ?>"><?= strtoupper($sev) ?></span>
                            </td>
                            <td class="px-3 py-2 text-right text-sm">
                                <?php
                                $impact = (float) ($ev['impact_score'] ?? 0);
                                $impactClass = $impact >= 0.7 ? 'text-red-400 font-semibold' : ($impact >= 0.4 ? 'text-orange-400' : 'text-slate-300');
                                ?>
                                <span class="<?= $impactClass ?>"><?= number_format($impact, 4) ?></span>
                            </td>
                            <td class="px-3 py-2 text-center text-xs <?= $familyClass ?>"><?= $familyLabel ?></td>
                            <?php
                            $riskAtEvent = (float) ($ev['risk_score_at_event'] ?? 0);
                            $band = intelligence_priority_band($riskAtEvent, $calibration);
                            $bandClass = match ($band) {
                                'critical' => 'text-red-400 font-semibold',
                                'high'     => 'text-orange-400',
                                'moderate' => 'text-amber-400',
                                'low'      => 'text-slate-400',
                                default    => 'text-slate-500',
                            };
                            ?>
                            <td class="px-3 py-2 text-center text-xs <?= $bandClass ?>"><?= $band !== '' ? ucfirst($band) : '—' ?></td>
                            <td class="px-3 py-2 text-right text-sm">
                                <?php $esc = (int) ($ev['escalation_count'] ?? 1); ?>
                                <?php if ($esc > 1): ?>
                                    <span class="text-orange-400 font-medium"><?= $esc ?>x</span>
                                <?php else: ?>
                                    <span class="text-slate-400">1</span>
                                <?php endif; ?>
                            </td>
                            <td class="px-3 py-2 text-right text-xs text-muted"><?= htmlspecialchars((string) ($ev['first_detected_at'] ?? ''), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-right text-xs">
                                <?php
                                $age = ie_relative_age((string) ($ev['first_detected_at'] ?? ''));
                                $ageD = ($ev['first_detected_at'] ?? '') !== '' ? (int) ((time() - strtotime((string) $ev['first_detected_at'])) / 86400) : 0;
                                $ageClass = $ageD >= 7 ? 'text-orange-400' : ($ageD >= 3 ? 'text-amber-400' : 'text-slate-400');
                                ?>
                                <span class="<?= $ageClass ?>"><?= $age ?></span>
                            </td>
                            <td class="px-3 py-2 text-right"><a class="text-accent" href="/intelligence-events/view.php?id=<?= (int) $ev['id'] ?>">Detail</a></td>
                        </tr>
                    <?php endforeach; ?>
                <?php endif; ?>
                </tbody>
            </table>
        </div>

        <?php if ($filterState === 'active' && $events !== []): ?>
        <div class="mt-3 flex items-center gap-3">
            <button type="submit" name="bulk_acknowledge" value="1" class="btn-secondary text-xs">Acknowledge selected</button>
            <span class="text-[10px] text-muted">Select events above, then click to acknowledge.</span>
        </div>
        <?php endif; ?>
    </form>

    <!-- Pagination -->
    <?php if ($totalPages > 1): ?>
    <div class="mt-4 flex items-center justify-between text-xs text-muted">
        <span>Showing <?= number_format($offset + 1) ?>&ndash;<?= number_format(min($offset + $perPage, $totalCount)) ?> of <?= number_format($totalCount) ?></span>
        <div class="flex gap-1">
            <?php if ($page > 1): ?>
                <a href="<?= ie_filter_url(['page' => (string) ($page - 1)]) ?>" class="rounded px-2 py-1 hover:bg-slate-700 text-slate-400">&laquo; Prev</a>
            <?php endif; ?>
            <?php
            $startPage = max(1, $page - 2);
            $endPage = min($totalPages, $page + 2);
            for ($p = $startPage; $p <= $endPage; $p++): ?>
                <a href="<?= ie_filter_url(['page' => (string) $p]) ?>"
                   class="rounded px-2 py-1 <?= $p === $page ? 'bg-accent/20 text-accent font-semibold' : 'hover:bg-slate-700 text-slate-400' ?>"><?= $p ?></a>
            <?php endfor; ?>
            <?php if ($page < $totalPages): ?>
                <a href="<?= ie_filter_url(['page' => (string) ($page + 1)]) ?>" class="rounded px-2 py-1 hover:bg-slate-700 text-slate-400">Next &raquo;</a>
            <?php endif; ?>
        </div>
    </div>
    <?php endif; ?>
</section>

<script>
document.getElementById('select-all')?.addEventListener('change', function() {
    document.querySelectorAll('.event-checkbox').forEach(cb => cb.checked = this.checked);
});
</script>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
