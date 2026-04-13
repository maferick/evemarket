<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

// ── Input parsing ────────────────────────────────────────────────────────────
$requestedName = isset($_GET['name']) ? (string) $_GET['name'] : '';
$requestedLevel = isset($_GET['level']) ? strtolower(trim((string) $_GET['level'])) : '';
$requestedQuery = isset($_GET['q']) ? trim((string) $_GET['q']) : '';
$requestedLines = isset($_GET['lines']) ? (int) $_GET['lines'] : 300;

$allowedLevels = ['', 'debug', 'info', 'warning', 'error'];
if (!in_array($requestedLevel, $allowedLevels, true)) {
    $requestedLevel = '';
}

$allowedLineCounts = [100, 300, 1000, 3000];
if (!in_array($requestedLines, $allowedLineCounts, true)) {
    $requestedLines = 300;
}

// ── Data ─────────────────────────────────────────────────────────────────────
$availableFiles = log_viewer_available_files();
$detail = log_viewer_file_detail($requestedName, [
    'max_lines' => $requestedLines,
    'level' => $requestedLevel,
    'q' => $requestedQuery,
]);

$title = 'Log Viewer · ' . ($detail['filename'] ?? $requestedName);
$pageHeaderBadge = 'Structured scheduler logs';
$pageHeaderSummary = 'Pretty-printed lane-aware log viewer. JSONL files are parsed and grouped by level; legacy text logs fall through to a raw tail.';

// Build a relative freshness caption from the file's modified time.
$pageFreshness = [];
if (($detail['error'] ?? null) === null && ($detail['modified_at'] ?? null) !== null) {
    $pageFreshness = [
        'label' => 'Log file',
        'computed_relative' => $detail['modified_relative'] ?? 'Unknown',
        'computed_at' => $detail['modified_at'] ?? 'Unavailable',
        'tone' => 'border-emerald-400/20 bg-emerald-500/10 text-emerald-100',
    ];
}

// Preserve the current filter in querystrings for the control buttons.
$baseQuery = [
    'name' => $requestedName,
    'lines' => $requestedLines,
];
if ($requestedQuery !== '') {
    $baseQuery['q'] = $requestedQuery;
}
$levelBase = $baseQuery;

include __DIR__ . '/../../src/views/partials/header.php';
?>

<div class="mb-4 flex flex-wrap items-center gap-3">
    <a href="/log-viewer" class="text-sm text-sky-300 hover:text-sky-200">← Back to log viewer</a>
    <?php if (($detail['error'] ?? null) === null): ?>
        <span class="text-xs text-slate-400">
            <?= htmlspecialchars((string) ($detail['filename'] ?? $requestedName), ENT_QUOTES) ?>
            · <?= htmlspecialchars((string) ($detail['size_human'] ?? ''), ENT_QUOTES) ?>
            · modified <?= htmlspecialchars((string) ($detail['modified_relative'] ?? 'unknown'), ENT_QUOTES) ?>
        </span>
    <?php endif; ?>
</div>

<?php if (($detail['error'] ?? null) !== null): ?>
    <section class="rounded-xl border border-rose-500/40 bg-rose-500/10 px-4 py-3 text-sm text-rose-100">
        <p class="font-semibold">Could not load log file.</p>
        <p class="mt-1"><?= htmlspecialchars((string) $detail['error'], ENT_QUOTES) ?></p>
        <?php if ($requestedName !== ''): ?>
            <p class="mt-1 text-xs text-rose-200/80">Requested: <?= htmlspecialchars($requestedName, ENT_QUOTES) ?></p>
        <?php endif; ?>
    </section>

    <?php if ($availableFiles !== []): ?>
    <section class="mt-6">
        <h2 class="section-title mb-4">Available log files</h2>
        <div class="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
            <?php foreach ($availableFiles as $lf): ?>
                <?php
                $href = '/log-viewer/file?' . http_build_query(['name' => $lf['filename']]);
                ?>
                <a href="<?= htmlspecialchars($href, ENT_QUOTES) ?>"
                   class="block rounded-xl border border-white/10 bg-white/[0.03] px-4 py-3 text-sm hover:bg-white/[0.06]">
                    <p class="font-medium text-white"><?= htmlspecialchars($lf['filename'], ENT_QUOTES) ?></p>
                    <p class="mt-1 text-xs text-slate-400">
                        <?= htmlspecialchars($lf['size_human'], ENT_QUOTES) ?> · modified <?= htmlspecialchars($lf['modified_relative'], ENT_QUOTES) ?>
                    </p>
                </a>
            <?php endforeach; ?>
        </div>
    </section>
    <?php endif; ?>
<?php else: ?>

    <!-- ═══════════════════════════════════════════════════════════════════════
         Controls — file picker, level filter, tail length, search
         ═══════════════════════════════════════════════════════════════════ -->
    <section class="mb-4 rounded-2xl border border-white/8 bg-white/[0.02] p-4">
        <form method="get" action="/log-viewer/file" class="flex flex-wrap items-end gap-3">
            <label class="flex min-w-[16rem] flex-1 flex-col gap-1 text-xs uppercase tracking-wider text-slate-400">
                Log file
                <select name="name" class="rounded-lg border border-white/10 bg-slate-900 px-3 py-2 text-sm text-white">
                    <?php foreach ($availableFiles as $lf): ?>
                        <option value="<?= htmlspecialchars($lf['filename'], ENT_QUOTES) ?>"
                            <?= $lf['filename'] === $detail['filename'] ? 'selected' : '' ?>>
                            <?= htmlspecialchars($lf['filename'], ENT_QUOTES) ?>
                            (<?= htmlspecialchars($lf['size_human'], ENT_QUOTES) ?> · <?= htmlspecialchars($lf['modified_relative'], ENT_QUOTES) ?>)
                        </option>
                    <?php endforeach; ?>
                </select>
            </label>

            <label class="flex flex-col gap-1 text-xs uppercase tracking-wider text-slate-400">
                Tail lines
                <select name="lines" class="rounded-lg border border-white/10 bg-slate-900 px-3 py-2 text-sm text-white">
                    <?php foreach ($allowedLineCounts as $lc): ?>
                        <option value="<?= $lc ?>" <?= $requestedLines === $lc ? 'selected' : '' ?>><?= $lc ?></option>
                    <?php endforeach; ?>
                </select>
            </label>

            <label class="flex flex-col gap-1 text-xs uppercase tracking-wider text-slate-400">
                Minimum level
                <select name="level" class="rounded-lg border border-white/10 bg-slate-900 px-3 py-2 text-sm text-white">
                    <option value="" <?= $requestedLevel === '' ? 'selected' : '' ?>>All</option>
                    <option value="debug" <?= $requestedLevel === 'debug' ? 'selected' : '' ?>>Debug+</option>
                    <option value="info" <?= $requestedLevel === 'info' ? 'selected' : '' ?>>Info+</option>
                    <option value="warning" <?= $requestedLevel === 'warning' ? 'selected' : '' ?>>Warning+</option>
                    <option value="error" <?= $requestedLevel === 'error' ? 'selected' : '' ?>>Error only</option>
                </select>
            </label>

            <label class="flex flex-1 min-w-[12rem] flex-col gap-1 text-xs uppercase tracking-wider text-slate-400">
                Search (substring)
                <input type="text" name="q" value="<?= htmlspecialchars($requestedQuery, ENT_QUOTES) ?>"
                    placeholder="e.g. job_key, error snippet, event name"
                    class="rounded-lg border border-white/10 bg-slate-900 px-3 py-2 text-sm text-white placeholder-slate-500">
            </label>

            <div class="flex gap-2">
                <button type="submit" class="rounded-lg border border-sky-400/40 bg-sky-500/15 px-4 py-2 text-sm font-medium text-sky-100 hover:bg-sky-500/25">
                    Apply
                </button>
                <a href="/log-viewer/file?<?= htmlspecialchars(http_build_query(['name' => $detail['filename']]), ENT_QUOTES) ?>"
                   class="rounded-lg border border-white/10 bg-white/5 px-4 py-2 text-sm text-slate-300 hover:bg-white/10">
                    Reset
                </a>
            </div>
        </form>

        <div class="mt-3 flex flex-wrap items-center gap-4 text-xs text-slate-400">
            <span>
                <span class="text-slate-300"><?= number_format($detail['lines_returned']) ?></span> of
                <span class="text-slate-300"><?= number_format($detail['lines_scanned']) ?></span>
                lines match (reading tail of <?= htmlspecialchars($detail['size_human'], ENT_QUOTES) ?>).
            </span>
            <?php if ($detail['is_jsonl']): ?>
                <span class="inline-flex items-center gap-1 rounded-full border border-sky-400/25 bg-sky-500/10 px-2 py-0.5 text-[0.7rem] text-sky-200">
                    JSONL parsed
                </span>
            <?php endif; ?>
            <span class="text-rose-300">error: <?= $detail['level_counts']['error'] ?></span>
            <span class="text-amber-300">warn: <?= $detail['level_counts']['warning'] ?></span>
            <span class="text-slate-300">info: <?= $detail['level_counts']['info'] ?></span>
            <?php if ($detail['level_counts']['debug'] > 0): ?>
                <span class="text-slate-500">debug: <?= $detail['level_counts']['debug'] ?></span>
            <?php endif; ?>
        </div>
    </section>

    <!-- ═══════════════════════════════════════════════════════════════════════
         Log entries
         ═══════════════════════════════════════════════════════════════════ -->
    <section>
        <?php if ($detail['entries'] === []): ?>
            <div class="rounded-2xl border border-white/8 bg-white/[0.02] p-8 text-center text-slate-400">
                No entries match the current filters.
            </div>
        <?php else: ?>
            <ul class="space-y-1.5">
                <?php foreach ($detail['entries'] as $entry):
                    $levelTone = match ($entry['level']) {
                        'error', 'critical' => 'border-rose-400/30 bg-rose-500/10 text-rose-100',
                        'warning', 'warn'   => 'border-amber-400/30 bg-amber-500/10 text-amber-100',
                        'info', 'notice'    => 'border-sky-400/25 bg-sky-500/8 text-sky-100',
                        'debug'             => 'border-slate-400/20 bg-slate-500/8 text-slate-300',
                        default             => 'border-white/10 bg-white/[0.03] text-slate-200',
                    };
                    $rowTone = match ($entry['level']) {
                        'error', 'critical' => 'border-l-2 border-l-rose-400/60',
                        'warning', 'warn'   => 'border-l-2 border-l-amber-400/60',
                        default             => 'border-l border-l-white/5',
                    };
                ?>
                    <li class="rounded-lg bg-white/[0.02] <?= $rowTone ?> px-3 py-2">
                        <?php if ($entry['is_json']): ?>
                            <div class="flex flex-wrap items-baseline gap-2">
                                <span class="inline-flex items-center rounded-md border px-1.5 py-0.5 text-[0.65rem] font-semibold uppercase tracking-wider <?= $levelTone ?>">
                                    <?= htmlspecialchars($entry['level'], ENT_QUOTES) ?>
                                </span>
                                <?php if ($entry['ts_display'] !== null): ?>
                                    <span class="font-mono text-[0.7rem] text-slate-500"><?= htmlspecialchars((string) $entry['ts_display'], ENT_QUOTES) ?></span>
                                <?php endif; ?>
                                <?php if ($entry['event'] !== null): ?>
                                    <span class="rounded-md bg-slate-800/80 px-1.5 py-0.5 font-mono text-[0.7rem] text-slate-300"><?= htmlspecialchars($entry['event'], ENT_QUOTES) ?></span>
                                <?php endif; ?>
                                <?php if ($entry['job_key'] !== null): ?>
                                    <span class="rounded-md border border-indigo-400/25 bg-indigo-500/10 px-1.5 py-0.5 font-mono text-[0.7rem] text-indigo-100"><?= htmlspecialchars($entry['job_key'], ENT_QUOTES) ?></span>
                                <?php endif; ?>
                                <?php if ($entry['logger'] !== null): ?>
                                    <span class="ml-auto text-[0.7rem] text-slate-500"><?= htmlspecialchars($entry['logger'], ENT_QUOTES) ?></span>
                                <?php endif; ?>
                            </div>
                            <p class="mt-1 text-sm text-slate-100"><?= htmlspecialchars($entry['message'], ENT_QUOTES) ?></p>
                            <?php if ($entry['payload'] !== null || $entry['exception'] !== null): ?>
                                <details class="mt-1">
                                    <summary class="cursor-pointer text-[0.7rem] text-slate-400 hover:text-slate-200">
                                        payload<?= $entry['exception'] !== null ? ' / exception' : '' ?>
                                    </summary>
                                    <?php if ($entry['payload'] !== null): ?>
                                        <pre class="mt-2 max-h-64 overflow-auto rounded-lg bg-black/40 p-3 text-[0.7rem] leading-relaxed text-slate-300"><?= htmlspecialchars(json_encode($entry['payload'], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE) ?: '{}', ENT_QUOTES) ?></pre>
                                    <?php endif; ?>
                                    <?php if ($entry['exception'] !== null): ?>
                                        <pre class="mt-2 max-h-64 overflow-auto rounded-lg bg-rose-950/40 p-3 text-[0.7rem] leading-relaxed text-rose-200"><?= htmlspecialchars($entry['exception'], ENT_QUOTES) ?></pre>
                                    <?php endif; ?>
                                </details>
                            <?php endif; ?>
                        <?php else: ?>
                            <pre class="whitespace-pre-wrap font-mono text-[0.75rem] leading-relaxed text-slate-300"><?= htmlspecialchars($entry['raw'], ENT_QUOTES) ?></pre>
                        <?php endif; ?>
                    </li>
                <?php endforeach; ?>
            </ul>

            <?php if ($detail['truncated']): ?>
                <p class="mt-3 text-xs text-slate-500">
                    Showing the most recent <?= number_format($detail['lines_returned']) ?> entries.
                    Older content is not loaded — raise <em>Tail lines</em> to see more.
                </p>
            <?php endif; ?>
        <?php endif; ?>
    </section>

<?php endif; ?>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
