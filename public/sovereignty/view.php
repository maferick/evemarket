<?php

declare(strict_types=1);
require_once __DIR__ . '/../../src/bootstrap.php';

$systemId = (int) ($_GET['system_id'] ?? 0);
if ($systemId <= 0) {
    header('Location: /sovereignty/');
    exit;
}

$detail = db_sovereignty_system_detail($systemId);
$owner = $detail['owner'];
$structures = $detail['structures'];
$campaigns = $detail['campaigns'];
$mapHistory = $detail['map_history'];
$structureHistory = $detail['structure_history'];

$systemName = $owner['system_name'] ?? 'Unknown System';
$title = "{$systemName} — Sovereignty Detail";

include __DIR__ . '/../../src/views/partials/header.php';
?>

<section class="surface-primary">
    <div class="flex items-center gap-3">
        <a href="/sovereignty/" class="text-accent hover:underline text-sm">&larr; Sovereignty</a>
    </div>
    <div class="mt-3">
        <p class="text-xs uppercase tracking-[0.16em] text-muted">System Sovereignty</p>
        <h1 class="mt-1 text-2xl font-semibold text-slate-50"><?= htmlspecialchars($systemName) ?></h1>
        <?php if ($owner): ?>
            <?php
                $sec = (float) ($owner['system_security'] ?? 0);
                $secColor = $sec >= 0.5 ? 'text-green-400' : ($sec > 0.0 ? 'text-amber-400' : 'text-red-400');
                $ownerStanding = (float) ($owner['owner_standing'] ?? 0);
                $standingClass = $ownerStanding > 0 ? 'text-cyan-400' : ($ownerStanding < 0 ? 'text-red-400' : 'text-slate-300');
                $standingLabel = $ownerStanding > 0 ? 'Friendly' : ($ownerStanding < 0 ? 'Hostile' : 'Neutral');
            ?>
            <div class="mt-2 flex items-center gap-4 text-sm">
                <span class="text-muted"><?= htmlspecialchars($owner['constellation_name'] ?? '') ?></span>
                <span class="text-muted"><?= htmlspecialchars($owner['region_name'] ?? '') ?></span>
                <span class="<?= $secColor ?>"><?= number_format($sec, 1) ?></span>
                <span class="text-muted">Owner:</span>
                <span class="<?= $standingClass ?> font-medium"><?= htmlspecialchars($owner['owner_name'] ?? 'Unknown') ?></span>
                <span class="inline-block px-1.5 py-0.5 rounded text-xs <?= $ownerStanding > 0 ? 'bg-cyan-900/50 text-cyan-300' : ($ownerStanding < 0 ? 'bg-red-900/50 text-red-300' : 'bg-slate-700/50 text-slate-400') ?>"><?= $standingLabel ?></span>
            </div>
        <?php else: ?>
            <p class="mt-2 text-sm text-muted">No sovereignty data available for this system.</p>
        <?php endif; ?>
    </div>
</section>

<!-- Sovereignty Structures -->
<section class="surface-primary mt-4">
    <p class="text-xs uppercase tracking-[0.16em] text-muted mb-3">Sovereignty Structures</p>
    <?php if ($structures): ?>
        <div class="table-shell">
            <table class="table-ui">
                <thead>
                    <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                        <th class="px-3 py-2 text-left">Type</th>
                        <th class="px-3 py-2 text-left">Role</th>
                        <th class="px-3 py-2 text-left">Owner</th>
                        <th class="px-3 py-2 text-right">ADM</th>
                        <th class="px-3 py-2 text-left">Vuln Window</th>
                        <th class="px-3 py-2 text-center">Status</th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach ($structures as $s):
                        $admStatusColors = [
                            'critical' => 'text-red-400',
                            'weak'     => 'text-amber-400',
                            'stable'   => 'text-green-400',
                            'strong'   => 'text-cyan-400',
                        ];
                        $admColor = $admStatusColors[$s['adm_status'] ?? ''] ?? 'text-slate-400';
                        $isSovHub = in_array($s['structure_role'], ['sov_hub'], true);
                        $roleLabel = ucfirst(str_replace('_', ' ', $s['structure_role'] ?? 'unknown'));
                        $isVulnNow = (int) ($s['is_vulnerable_now'] ?? 0);
                        $vulnIn = (int) ($s['vulnerable_in_minutes'] ?? 0);
                    ?>
                        <tr class="border-b border-border/40 <?= $isSovHub ? 'bg-cyan-950/10' : '' ?>">
                            <td class="px-3 py-2 text-sm"><?= htmlspecialchars($s['structure_type_name'] ?? 'Type #' . $s['structure_type_id']) ?></td>
                            <td class="px-3 py-2 text-sm <?= $isSovHub ? 'text-cyan-400 font-medium' : 'text-muted' ?>"><?= $roleLabel ?></td>
                            <td class="px-3 py-2 text-sm"><?= htmlspecialchars($s['alliance_name'] ?? 'Unknown') ?></td>
                            <td class="px-3 py-2 text-right">
                                <?php if ($s['vulnerability_occupancy_level'] !== null): ?>
                                    <span class="<?= $admColor ?> font-medium"><?= number_format((float) $s['vulnerability_occupancy_level'], 1) ?></span>
                                    <span class="text-xs text-muted ml-1"><?= ucfirst($s['adm_status'] ?? '') ?></span>
                                <?php else: ?>
                                    <span class="text-muted">—</span>
                                <?php endif; ?>
                            </td>
                            <td class="px-3 py-2 text-sm">
                                <?php if ($s['vulnerable_start_time']): ?>
                                    <span class="text-muted"><?= substr($s['vulnerable_start_time'], 11, 5) ?> — <?= substr($s['vulnerable_end_time'], 11, 5) ?></span>
                                    <?php if ($vulnIn > 0 && !$isVulnNow): ?>
                                        <span class="text-xs text-amber-400 ml-1">in <?= $vulnIn ?>m</span>
                                    <?php endif; ?>
                                <?php else: ?>
                                    <span class="text-muted">—</span>
                                <?php endif; ?>
                            </td>
                            <td class="px-3 py-2 text-center">
                                <?php if ($isVulnNow): ?>
                                    <span class="inline-block px-1.5 py-0.5 rounded text-xs font-medium bg-red-900/60 text-red-300">VULNERABLE</span>
                                <?php else: ?>
                                    <span class="text-xs text-green-400">Protected</span>
                                <?php endif; ?>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
    <?php else: ?>
        <p class="text-sm text-muted">No sovereignty structures in this system.</p>
    <?php endif; ?>
</section>

<!-- Active Campaigns -->
<?php if ($campaigns): ?>
<section class="surface-primary mt-4">
    <p class="text-xs uppercase tracking-[0.16em] text-muted mb-3">Active Campaigns</p>
    <?php foreach ($campaigns as $c):
        $atkPct = max(0, min(100, round((float) $c['attackers_score'] * 100)));
        $defPct = max(0, min(100, round((float) $c['defender_score'] * 100)));
    ?>
        <div class="mb-3 p-3 rounded border border-border/50 bg-slate-800/30">
            <div class="flex items-center justify-between text-sm mb-2">
                <span class="font-medium text-slate-100"><?= htmlspecialchars($c['event_type']) ?></span>
                <span class="text-muted">Defender: <?= htmlspecialchars($c['defender_name'] ?? 'Alliance #' . $c['defender_id']) ?></span>
            </div>
            <div class="flex gap-4 items-center">
                <div class="flex-1">
                    <p class="text-xs text-red-400 mb-1">Attackers <?= $atkPct ?>%</p>
                    <div class="w-full h-3 bg-slate-700 rounded-full"><div class="h-3 bg-red-500 rounded-full" style="width: <?= $atkPct ?>%"></div></div>
                </div>
                <div class="flex-1">
                    <p class="text-xs text-cyan-400 mb-1">Defenders <?= $defPct ?>%</p>
                    <div class="w-full h-3 bg-slate-700 rounded-full"><div class="h-3 bg-cyan-500 rounded-full" style="width: <?= $defPct ?>%"></div></div>
                </div>
            </div>
        </div>
    <?php endforeach; ?>
</section>
<?php endif; ?>

<!-- Ownership Timeline -->
<section class="surface-primary mt-4">
    <p class="text-xs uppercase tracking-[0.16em] text-muted mb-3">Ownership Timeline</p>
    <?php if ($mapHistory): ?>
        <div class="space-y-2">
            <?php foreach ($mapHistory as $h): ?>
                <div class="flex items-center gap-3 text-sm border-l-2 border-border/50 pl-3 py-1">
                    <span class="text-muted min-w-[120px]"><?= htmlspecialchars(substr($h['changed_at'], 0, 16)) ?></span>
                    <span class="text-red-400"><?= htmlspecialchars($h['previous_owner_name'] ?? ($h['previous_owner_entity_id'] ? 'Entity #' . $h['previous_owner_entity_id'] : 'None')) ?></span>
                    <span class="text-muted">&rarr;</span>
                    <span class="text-cyan-400"><?= htmlspecialchars($h['new_owner_name'] ?? ($h['new_owner_entity_id'] ? 'Entity #' . $h['new_owner_entity_id'] : 'None')) ?></span>
                </div>
            <?php endforeach; ?>
        </div>
    <?php else: ?>
        <p class="text-sm text-muted">No ownership changes recorded yet.</p>
    <?php endif; ?>
</section>

<!-- Structure History -->
<section class="surface-primary mt-4">
    <p class="text-xs uppercase tracking-[0.16em] text-muted mb-3">Structure History</p>
    <?php if ($structureHistory): ?>
        <div class="table-shell">
            <table class="table-ui">
                <thead>
                    <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                        <th class="px-3 py-2 text-left">Time</th>
                        <th class="px-3 py-2 text-left">Type</th>
                        <th class="px-3 py-2 text-left">Event</th>
                        <th class="px-3 py-2 text-left">Details</th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach ($structureHistory as $sh):
                        $eventColors = [
                            'appeared'      => 'text-green-400',
                            'disappeared'   => 'text-red-400',
                            'adm_changed'   => 'text-amber-400',
                            'owner_changed' => 'text-purple-400',
                            'vuln_changed'  => 'text-slate-400',
                        ];
                        $eventColor = $eventColors[$sh['event_type']] ?? 'text-slate-400';

                        $detailText = '';
                        if ($sh['event_type'] === 'adm_changed') {
                            $detailText = ($sh['previous_adm'] ?? '?') . ' → ' . ($sh['new_adm'] ?? '?');
                        } elseif ($sh['event_type'] === 'owner_changed') {
                            $detailText = 'Alliance changed';
                        } elseif ($sh['event_type'] === 'appeared') {
                            $detailText = 'Structure deployed';
                        } elseif ($sh['event_type'] === 'disappeared') {
                            $detailText = 'Structure removed';
                        } else {
                            $detailText = 'Vulnerability window shifted';
                        }
                    ?>
                        <tr class="border-b border-border/40">
                            <td class="px-3 py-2 text-sm text-muted"><?= htmlspecialchars(substr($sh['recorded_at'], 0, 16)) ?></td>
                            <td class="px-3 py-2 text-sm"><?= htmlspecialchars($sh['structure_type_name'] ?? ucfirst(str_replace('_', ' ', $sh['structure_role']))) ?></td>
                            <td class="px-3 py-2 text-sm <?= $eventColor ?>"><?= ucfirst(str_replace('_', ' ', $sh['event_type'])) ?></td>
                            <td class="px-3 py-2 text-sm text-muted"><?= htmlspecialchars($detailText) ?></td>
                        </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
    <?php else: ?>
        <p class="text-sm text-muted">No structure history recorded yet.</p>
    <?php endif; ?>
</section>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
