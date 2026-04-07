<?php

declare(strict_types=1);
require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Battle Intelligence — Create Manual Theater';

$error = null;
$success = null;
$formSystem = trim((string) ($_POST['system_name'] ?? $_GET['system_name'] ?? ''));
$formStart = trim((string) ($_POST['start_time'] ?? $_GET['start_time'] ?? ''));
$formEnd = trim((string) ($_POST['end_time'] ?? $_GET['end_time'] ?? ''));
$formLabel = trim((string) ($_POST['label'] ?? ''));
$formConstellation = isset($_POST['include_constellation']) && $_POST['include_constellation'] === '1';
$previewBattles = [];
$isPreview = isset($_POST['preview']);
$isCreate = isset($_POST['create']);

// ── Resolve system ──────────────────────────────────────────────────────────
$resolvedSystem = null;
if ($formSystem !== '') {
    $candidates = db_ref_system_search($formSystem, 5);
    // Prefer exact match
    foreach ($candidates as $c) {
        if (strcasecmp((string) $c['system_name'], $formSystem) === 0) {
            $resolvedSystem = $c;
            break;
        }
    }
    if ($resolvedSystem === null && $candidates !== []) {
        $resolvedSystem = $candidates[0];
    }
}

// ── Preview or Create ───────────────────────────────────────────────────────
if (($isPreview || $isCreate) && $_SERVER['REQUEST_METHOD'] === 'POST') {
    if ($resolvedSystem === null) {
        $error = 'System not found. Please enter a valid system name.';
    } elseif ($formStart === '' || $formEnd === '') {
        $error = 'Start time and end time are required.';
    } elseif (strtotime($formStart) === false || strtotime($formEnd) === false) {
        $error = 'Invalid date/time format. Use YYYY-MM-DD HH:MM format.';
    } elseif (strtotime($formEnd) <= strtotime($formStart)) {
        $error = 'End time must be after start time.';
    } else {
        $startFormatted = date('Y-m-d H:i:s', strtotime($formStart));
        $endFormatted = date('Y-m-d H:i:s', strtotime($formEnd));
        $previewBattles = db_battles_in_range(
            (int) $resolvedSystem['system_id'],
            $startFormatted,
            $endFormatted,
            $formConstellation
        );

        if ($previewBattles === []) {
            $error = 'No battles found in ' . htmlspecialchars((string) $resolvedSystem['system_name'], ENT_QUOTES)
                   . ($formConstellation ? ' (constellation)' : '')
                   . ' between ' . $startFormatted . ' and ' . $endFormatted . '.';
        } elseif ($isCreate) {
            $theaterId = db_create_manual_theater($previewBattles, $formLabel !== '' ? $formLabel : null);
            if ($theaterId !== null) {
                // Queue theater_analysis to populate summaries immediately
                db_worker_job_force_available_by_job_keys(['theater_analysis']);
                flash('success', 'Manual theater created with ' . count($previewBattles) . ' battles. Analysis job queued — data will populate shortly.');
                header('Location: /theater-intelligence/view.php?theater_id=' . urlencode($theaterId));
                exit;
            }
            $error = 'Failed to create theater. Please try again.';
        }
    }
}

include __DIR__ . '/../../src/views/partials/header.php';
?>

<section class="surface-primary">
    <div class="flex items-center justify-between gap-4">
        <div>
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Battle Intelligence</p>
            <h1 class="mt-1 text-2xl font-semibold text-slate-50">Create Manual Theater</h1>
            <p class="mt-2 text-sm text-muted">Manually group battles into a theater by specifying a system, time range, and optional label. Useful for extended fights that span multiple auto-clustering windows.</p>
        </div>
        <div class="flex gap-2">
            <a href="/theater-intelligence" class="btn-secondary">Back to Theaters</a>
        </div>
    </div>
</section>

<?php if ($error !== null): ?>
    <section class="surface-primary mt-4">
        <div class="rounded border border-red-700/50 bg-red-900/20 px-4 py-3 text-sm text-red-300"><?= htmlspecialchars($error, ENT_QUOTES) ?></div>
    </section>
<?php endif; ?>

<section class="surface-primary mt-4">
    <form method="POST" class="space-y-4">
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
                <label class="text-xs text-muted block mb-1">System Name</label>
                <input type="text" name="system_name" required
                       value="<?= htmlspecialchars($formSystem, ENT_QUOTES) ?>"
                       placeholder="e.g. X47L-Q"
                       class="w-full rounded bg-slate-800 border border-border px-3 py-2 text-sm text-slate-100 placeholder-slate-500">
                <?php if ($resolvedSystem !== null && ($isPreview || $isCreate)): ?>
                    <p class="text-[11px] text-green-400 mt-1">
                        Matched: <?= htmlspecialchars((string) $resolvedSystem['system_name'], ENT_QUOTES) ?>
                        <span class="text-muted">(<?= htmlspecialchars((string) ($resolvedSystem['constellation_name'] ?? ''), ENT_QUOTES) ?>, <?= htmlspecialchars((string) ($resolvedSystem['region_name'] ?? ''), ENT_QUOTES) ?>)</span>
                    </p>
                <?php endif; ?>
            </div>
            <div>
                <label class="text-xs text-muted block mb-1">Label <span class="text-slate-500">(optional)</span></label>
                <input type="text" name="label"
                       value="<?= htmlspecialchars($formLabel, ENT_QUOTES) ?>"
                       placeholder="e.g. The Big One - 12h Siege"
                       class="w-full rounded bg-slate-800 border border-border px-3 py-2 text-sm text-slate-100 placeholder-slate-500">
            </div>
        </div>
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
                <label class="text-xs text-muted block mb-1">Start Time <span class="text-slate-500">(EVE time)</span></label>
                <input type="datetime-local" name="start_time" required
                       value="<?= htmlspecialchars($formStart, ENT_QUOTES) ?>"
                       class="w-full rounded bg-slate-800 border border-border px-3 py-2 text-sm text-slate-100">
            </div>
            <div>
                <label class="text-xs text-muted block mb-1">End Time <span class="text-slate-500">(EVE time)</span></label>
                <input type="datetime-local" name="end_time" required
                       value="<?= htmlspecialchars($formEnd, ENT_QUOTES) ?>"
                       class="w-full rounded bg-slate-800 border border-border px-3 py-2 text-sm text-slate-100">
            </div>
        </div>
        <div class="flex items-center gap-2">
            <input type="checkbox" name="include_constellation" value="1" id="include_constellation"
                   <?= $formConstellation ? 'checked' : '' ?>
                   class="rounded bg-slate-800 border-border text-accent">
            <label for="include_constellation" class="text-sm text-slate-300">Include entire constellation</label>
            <span class="text-[11px] text-muted">(captures fights that spilled into adjacent systems)</span>
        </div>
        <div class="flex gap-2 pt-2">
            <button type="submit" name="preview" value="1" class="btn-secondary">Preview Battles</button>
            <?php if ($previewBattles !== []): ?>
                <button type="submit" name="create" value="1" class="btn-primary">Create Theater (<?= count($previewBattles) ?> battles)</button>
            <?php endif; ?>
        </div>
    </form>
</section>

<?php if ($previewBattles !== []): ?>
<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-50 mb-3">Preview: <?= count($previewBattles) ?> Battles Found</h2>
    <?php
        $previewTotalParticipants = array_sum(array_column($previewBattles, 'participant_count'));
        $previewStart = min(array_column($previewBattles, 'started_at'));
        $previewEnd = max(array_column($previewBattles, 'ended_at'));
        $previewDuration = max(1, (int) (strtotime($previewEnd) - strtotime($previewStart)));
        $previewDurationLabel = $previewDuration >= 3600 ? number_format($previewDuration / 3600, 1) . 'h' : number_format($previewDuration / 60, 0) . 'm';
        $previewSystems = array_unique(array_column($previewBattles, 'system_name'));
    ?>
    <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4">
        <div class="rounded bg-slate-800/50 border border-border/50 px-3 py-2">
            <p class="text-[10px] uppercase tracking-wider text-muted">Battles</p>
            <p class="text-lg font-semibold text-slate-100"><?= count($previewBattles) ?></p>
        </div>
        <div class="rounded bg-slate-800/50 border border-border/50 px-3 py-2">
            <p class="text-[10px] uppercase tracking-wider text-muted">Total Participants</p>
            <p class="text-lg font-semibold text-slate-100"><?= number_format($previewTotalParticipants) ?></p>
        </div>
        <div class="rounded bg-slate-800/50 border border-border/50 px-3 py-2">
            <p class="text-[10px] uppercase tracking-wider text-muted">Duration</p>
            <p class="text-lg font-semibold text-slate-100"><?= $previewDurationLabel ?></p>
        </div>
        <div class="rounded bg-slate-800/50 border border-border/50 px-3 py-2">
            <p class="text-[10px] uppercase tracking-wider text-muted">Systems</p>
            <p class="text-lg font-semibold text-slate-100"><?= count($previewSystems) ?></p>
            <p class="text-[10px] text-muted"><?= htmlspecialchars(implode(', ', $previewSystems), ENT_QUOTES) ?></p>
        </div>
    </div>

    <div class="table-shell">
        <table class="table-ui">
            <thead>
                <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                    <th class="px-3 py-2 text-left">System</th>
                    <th class="px-3 py-2 text-left">Started</th>
                    <th class="px-3 py-2 text-left">Ended</th>
                    <th class="px-3 py-2 text-right">Participants</th>
                    <th class="px-3 py-2 text-left">Size</th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($previewBattles as $b): ?>
                    <tr class="border-b border-border/50">
                        <td class="px-3 py-2 text-sm text-slate-100"><?= htmlspecialchars((string) ($b['system_name'] ?? '-'), ENT_QUOTES) ?></td>
                        <td class="px-3 py-2 text-sm text-slate-300"><?= htmlspecialchars((string) ($b['started_at'] ?? ''), ENT_QUOTES) ?></td>
                        <td class="px-3 py-2 text-sm text-slate-300"><?= htmlspecialchars((string) ($b['ended_at'] ?? ''), ENT_QUOTES) ?></td>
                        <td class="px-3 py-2 text-sm text-slate-100 text-right"><?= number_format((int) ($b['participant_count'] ?? 0)) ?></td>
                        <td class="px-3 py-2 text-sm text-slate-400"><?= htmlspecialchars((string) ($b['battle_size_class'] ?? '-'), ENT_QUOTES) ?></td>
                    </tr>
                <?php endforeach; ?>
            </tbody>
        </table>
    </div>
</section>
<?php endif; ?>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
