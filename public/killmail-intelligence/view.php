<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Killmail Loss Detail';
$data = killmail_detail_data();
$error = $data['error'] ?? null;
$detail = $data['detail'] ?? null;

include __DIR__ . '/../../src/views/partials/header.php';
?>
<div class="mb-6 flex items-center justify-between gap-3">
    <div>
        <a href="/killmail-intelligence" class="text-sm text-accent hover:text-blue-300">← Back to loss overview</a>
        <p class="mt-2 text-sm text-muted">Inspect the locally stored victim-side loss record and verify the item data foundation for future market comparison.</p>
    </div>
</div>

<?php if (is_string($error) && trim($error) !== ''): ?>
    <section class="rounded-xl border border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
        <?= htmlspecialchars($error, ENT_QUOTES) ?>
    </section>
<?php elseif (is_array($detail)): ?>
    <?php $victim = $detail['victim'] ?? []; ?>
    <?php $ship = $detail['ship'] ?? []; ?>
    <?php $location = $detail['location'] ?? []; ?>
    <?php $attackers = $detail['attackers'] ?? []; ?>
    <?php $items = $detail['items'] ?? []; ?>
    <?php $zkb = $detail['zkb'] ?? []; ?>

    <section class="rounded-xl border border-border bg-card p-5 shadow-lg shadow-black/20">
        <div class="flex flex-wrap items-start justify-between gap-4">
            <div>
                <p class="text-xs uppercase tracking-[0.2em] text-muted">Stored victim loss</p>
                <h2 class="mt-1 text-xl font-semibold text-slate-50"><?= htmlspecialchars((string) ($ship['name'] ?? 'Killmail loss'), ENT_QUOTES) ?></h2>
                <p class="mt-2 text-sm text-muted"><?= htmlspecialchars((string) ($victim['corporation_display'] ?? 'Victim corporation unavailable'), ENT_QUOTES) ?> · <?= htmlspecialchars((string) ($location['system_display'] ?? 'System unavailable'), ENT_QUOTES) ?></p>
            </div>
            <div class="flex flex-wrap gap-2">
                <span class="rounded-full border px-3 py-1 text-xs uppercase tracking-[0.15em] <?= ($detail['tracked_victim_loss'] ?? false) ? 'border-emerald-500/40 bg-emerald-500/10 text-emerald-200' : 'border-slate-500/40 bg-slate-500/10 text-slate-300' ?>">
                    <?= ($detail['tracked_victim_loss'] ?? false) ? 'Tracked victim loss' : 'Stored loss' ?>
                </span>
                <span class="rounded-full border border-border bg-black/20 px-3 py-1 text-xs uppercase tracking-[0.15em] text-slate-200">Sequence #<?= htmlspecialchars(number_format((int) ($detail['sequence_id'] ?? 0)), ENT_QUOTES) ?></span>
            </div>
        </div>
        <p class="mt-4 max-w-4xl text-sm text-muted"><?= htmlspecialchars((string) ($detail['match_context'] ?? ''), ENT_QUOTES) ?></p>
    </section>

    <section class="mt-6 grid gap-4 xl:grid-cols-[minmax(0,1.45fr)_minmax(320px,0.95fr)]">
        <article class="rounded-xl border border-border bg-card p-5">
            <div class="grid gap-4 md:grid-cols-2">
                <div class="rounded-lg border border-border bg-black/20 p-4">
                    <p class="text-xs uppercase tracking-[0.15em] text-muted">Killmail time</p>
                    <p class="mt-2 text-lg font-semibold text-slate-50"><?= htmlspecialchars((string) ($detail['killmail_time_display'] ?? '—'), ENT_QUOTES) ?></p>
                    <p class="mt-1 text-sm text-muted">Uploaded <?= htmlspecialchars((string) ($detail['uploaded_at_display'] ?? '—'), ENT_QUOTES) ?></p>
                </div>
                <div class="rounded-lg border border-border bg-black/20 p-4">
                    <p class="text-xs uppercase tracking-[0.15em] text-muted">Stored references</p>
                    <p class="mt-2 text-lg font-semibold text-slate-50">Killmail <?= htmlspecialchars((string) ($detail['killmail_id'] ?? '—'), ENT_QUOTES) ?></p>
                    <p class="mt-1 text-sm text-muted">Sequence <?= htmlspecialchars((string) ($detail['sequence_id'] ?? '—'), ENT_QUOTES) ?> · Hash <?= htmlspecialchars((string) ($detail['killmail_hash'] ?? '—'), ENT_QUOTES) ?></p>
                </div>
                <div class="rounded-lg border border-border bg-black/20 p-4">
                    <p class="text-xs uppercase tracking-[0.15em] text-muted">Victim details</p>
                    <p class="mt-2 text-base font-semibold text-slate-50"><?= htmlspecialchars((string) ($victim['corporation_display'] ?? '—'), ENT_QUOTES) ?></p>
                    <p class="mt-1 text-sm text-slate-300"><?= htmlspecialchars((string) ($victim['alliance_display'] ?? '—'), ENT_QUOTES) ?></p>
                    <div class="mt-3 space-y-1 text-xs text-muted">
                        <p><?= htmlspecialchars((string) ($victim['character_id_display'] ?? '—'), ENT_QUOTES) ?></p>
                        <p><?= htmlspecialchars((string) ($victim['corporation_id_display'] ?? '—'), ENT_QUOTES) ?></p>
                        <p><?= htmlspecialchars((string) ($victim['alliance_id_display'] ?? '—'), ENT_QUOTES) ?></p>
                        <p>Damage taken <?= htmlspecialchars((string) ($victim['damage_taken'] ?? '0'), ENT_QUOTES) ?></p>
                    </div>
                </div>
                <div class="rounded-lg border border-border bg-black/20 p-4">
                    <p class="text-xs uppercase tracking-[0.15em] text-muted">Ship and location</p>
                    <p class="mt-2 text-base font-semibold text-slate-50"><?= htmlspecialchars((string) ($ship['name'] ?? '—'), ENT_QUOTES) ?></p>
                    <p class="mt-1 text-sm text-slate-300"><?= htmlspecialchars((string) ($location['system_display'] ?? '—'), ENT_QUOTES) ?> · <?= htmlspecialchars((string) ($location['region_display'] ?? '—'), ENT_QUOTES) ?></p>
                    <div class="mt-3 space-y-1 text-xs text-muted">
                        <p><?= htmlspecialchars((string) ($ship['type_id_display'] ?? '—'), ENT_QUOTES) ?></p>
                        <p><?= htmlspecialchars((string) ($location['system_id_display'] ?? '—'), ENT_QUOTES) ?></p>
                        <p><?= htmlspecialchars((string) ($location['region_id_display'] ?? '—'), ENT_QUOTES) ?></p>
                    </div>
                </div>
            </div>
        </article>

        <article class="rounded-xl border border-border bg-card p-5">
            <div class="flex items-center justify-between gap-3">
                <h2 class="text-base font-medium text-slate-50">Attackers summary</h2>
                <span class="text-xs uppercase tracking-[0.15em] text-muted"><?= number_format((int) ($attackers['count'] ?? 0)) ?> attackers</span>
            </div>
            <div class="mt-4 space-y-3">
                <div class="rounded-lg border border-border bg-black/20 p-4">
                    <p class="text-xs uppercase tracking-[0.15em] text-muted">Final blow</p>
                    <?php if (is_array($attackers['final_blow'] ?? null)): ?>
                        <p class="mt-2 text-base font-semibold text-slate-50"><?= htmlspecialchars((string) (($attackers['final_blow']['corporation_display'] ?? 'Unknown attacker')), ENT_QUOTES) ?></p>
                        <p class="mt-1 text-sm text-slate-300"><?= htmlspecialchars((string) (($attackers['final_blow']['ship_display'] ?? 'Unknown ship') . ' · ' . ($attackers['final_blow']['weapon_display'] ?? 'Unknown weapon')), ENT_QUOTES) ?></p>
                    <?php else: ?>
                        <p class="mt-2 text-sm text-muted">No final blow row stored.</p>
                    <?php endif; ?>
                </div>
                <div class="rounded-lg border border-border bg-black/10 p-4">
                    <p class="text-xs uppercase tracking-[0.15em] text-muted">Top attacker rows</p>
                    <div class="mt-3 space-y-3">
                        <?php foreach ((array) ($attackers['top_rows'] ?? []) as $attacker): ?>
                            <div class="rounded-lg border border-border/70 bg-black/20 p-3">
                                <div class="flex items-start justify-between gap-3">
                                    <div>
                                        <p class="font-medium text-slate-50"><?= htmlspecialchars((string) ($attacker['corporation_display'] ?? '—'), ENT_QUOTES) ?></p>
                                        <p class="mt-1 text-xs text-slate-300"><?= htmlspecialchars((string) ($attacker['alliance_display'] ?? '—'), ENT_QUOTES) ?></p>
                                    </div>
                                    <?php if ($attacker['final_blow'] ?? false): ?>
                                        <span class="rounded-full border border-emerald-500/40 bg-emerald-500/10 px-2 py-0.5 text-[11px] uppercase tracking-[0.08em] text-emerald-200">Final blow</span>
                                    <?php endif; ?>
                                </div>
                                <p class="mt-2 text-sm text-muted"><?= htmlspecialchars((string) (($attacker['ship_display'] ?? '—') . ' · ' . ($attacker['weapon_display'] ?? '—')), ENT_QUOTES) ?></p>
                                <p class="mt-2 text-xs text-muted"><?= htmlspecialchars((string) ($attacker['corporation_id_display'] ?? '—'), ENT_QUOTES) ?> · <?= htmlspecialchars((string) ($attacker['alliance_id_display'] ?? '—'), ENT_QUOTES) ?> · Sec <?= htmlspecialchars((string) ($attacker['security_status'] ?? '—'), ENT_QUOTES) ?></p>
                            </div>
                        <?php endforeach; ?>
                    </div>
                </div>
                <div class="rounded-lg border border-dashed border-border bg-black/10 px-4 py-3 text-sm text-muted">
                    Attacker rows are secondary context only. The primary intelligence target remains the victim-side loss and its locally stored item rows.
                </div>
            </div>
        </article>
    </section>

    <section class="mt-6 rounded-xl border border-border bg-card p-5 shadow-lg shadow-black/20">
        <div class="flex flex-wrap items-center justify-between gap-3">
            <div>
                <p class="text-xs uppercase tracking-[0.2em] text-muted">Loss item intelligence</p>
                <h2 class="mt-1 text-lg font-medium text-slate-50">Stored loss items ready for analysis</h2>
                <p class="mt-2 max-w-4xl text-sm text-muted">This section is organized around the locally stored item rows so future market availability, hub pricing, and restock-worthiness comparisons can plug in cleanly.</p>
            </div>
            <div class="grid gap-2 sm:grid-cols-3">
                <div class="rounded-lg border border-border bg-black/20 px-4 py-3 text-center">
                    <p class="text-xs uppercase tracking-[0.15em] text-muted">Dropped qty</p>
                    <p class="mt-1 text-lg font-semibold text-slate-50"><?= number_format((int) ($detail['item_totals']['dropped'] ?? 0)) ?></p>
                </div>
                <div class="rounded-lg border border-border bg-black/20 px-4 py-3 text-center">
                    <p class="text-xs uppercase tracking-[0.15em] text-muted">Destroyed qty</p>
                    <p class="mt-1 text-lg font-semibold text-slate-50"><?= number_format((int) ($detail['item_totals']['destroyed'] ?? 0)) ?></p>
                </div>
                <div class="rounded-lg border border-border bg-black/20 px-4 py-3 text-center">
                    <p class="text-xs uppercase tracking-[0.15em] text-muted">Stored item rows</p>
                    <p class="mt-1 text-lg font-semibold text-slate-50"><?= number_format((int) ($detail['stored_item_count'] ?? 0)) ?></p>
                </div>
            </div>
        </div>

        <div class="mt-6 grid gap-4 xl:grid-cols-3">
            <?php foreach ((array) $items as $groupKey => $group): ?>
                <article class="rounded-xl border border-border/80 bg-black/20 p-4">
                    <div class="flex items-start justify-between gap-3">
                        <div>
                            <h3 class="text-base font-semibold text-slate-50"><?= htmlspecialchars((string) ($group['label'] ?? $groupKey), ENT_QUOTES) ?></h3>
                            <p class="mt-1 text-sm text-muted"><?= htmlspecialchars((string) ($group['description'] ?? ''), ENT_QUOTES) ?></p>
                        </div>
                        <span class="rounded-full border border-border px-2 py-0.5 text-xs text-slate-300"><?= number_format((int) ($group['total_quantity'] ?? 0)) ?></span>
                    </div>
                    <?php if (((array) ($group['rows'] ?? [])) === []): ?>
                        <div class="mt-4 rounded-lg border border-dashed border-border bg-black/10 px-4 py-5 text-sm text-muted">No <?= htmlspecialchars(strtolower((string) ($group['label'] ?? $groupKey)), ENT_QUOTES) ?> stored for this loss.</div>
                    <?php else: ?>
                        <div class="mt-4 space-y-3">
                            <?php foreach ((array) ($group['rows'] ?? []) as $itemRow): ?>
                                <div class="rounded-lg border border-border/70 bg-black/30 p-3">
                                    <div class="flex items-start justify-between gap-3">
                                        <div>
                                            <p class="font-medium text-slate-50"><?= htmlspecialchars((string) ($itemRow['item_name'] ?? 'Unknown item'), ENT_QUOTES) ?></p>
                                            <p class="mt-1 text-xs text-muted"><?= htmlspecialchars((string) ('Type ID ' . number_format((int) ($itemRow['item_type_id'] ?? 0))), ENT_QUOTES) ?></p>
                                        </div>
                                        <span class="rounded-full border border-accent/40 bg-accent/10 px-2 py-0.5 text-xs text-slate-100">Qty <?= number_format((int) ($itemRow['quantity'] ?? 0)) ?></span>
                                    </div>
                                    <div class="mt-3 grid gap-2 text-xs text-muted sm:grid-cols-3">
                                        <p>Flag <?= htmlspecialchars((string) (($itemRow['item_flag'] ?? null) !== null ? (string) $itemRow['item_flag'] : '—'), ENT_QUOTES) ?></p>
                                        <p>Singleton <?= htmlspecialchars((string) (($itemRow['singleton'] ?? null) !== null ? (string) $itemRow['singleton'] : '—'), ENT_QUOTES) ?></p>
                                        <p>Stored <?= htmlspecialchars((string) ($itemRow['stored_at_display'] ?? '—'), ENT_QUOTES) ?></p>
                                    </div>
                                </div>
                            <?php endforeach; ?>
                        </div>
                    <?php endif; ?>
                </article>
            <?php endforeach; ?>
        </div>

        <div class="mt-6 rounded-xl border border-dashed border-border bg-black/10 px-4 py-4 text-sm text-muted">
            Future extension points: compare these stored loss items against alliance market availability, reference-hub pricing, and restock thresholds without redesigning the loss detail layout.
        </div>
    </section>

    <section class="mt-6 grid gap-4 xl:grid-cols-[minmax(0,1.2fr)_minmax(320px,0.8fr)]">
        <article class="rounded-xl border border-border bg-card p-5">
            <h2 class="text-base font-medium text-slate-50">Storage proof</h2>
            <div class="mt-4 grid gap-3 md:grid-cols-2">
                <div class="rounded-lg border border-border bg-black/20 p-4">
                    <p class="text-xs uppercase tracking-[0.15em] text-muted">Locally written</p>
                    <p class="mt-2 text-lg font-semibold text-slate-50"><?= htmlspecialchars((string) ($detail['created_at_display'] ?? '—'), ENT_QUOTES) ?></p>
                    <p class="mt-1 text-sm text-muted">Updated <?= htmlspecialchars((string) ($detail['updated_at_display'] ?? '—'), ENT_QUOTES) ?></p>
                </div>
                <div class="rounded-lg border border-border bg-black/20 p-4">
                    <p class="text-xs uppercase tracking-[0.15em] text-muted">Item rows stored</p>
                    <p class="mt-2 text-lg font-semibold text-slate-50"><?= number_format((int) ($detail['stored_item_count'] ?? 0)) ?></p>
                    <p class="mt-1 text-sm text-muted">Ready for downstream market and price analytics.</p>
                </div>
            </div>
        </article>

        <article class="rounded-xl border border-border bg-card p-5">
            <h2 class="text-base font-medium text-slate-50">zKill metadata</h2>
            <div class="mt-4 space-y-3 text-sm text-slate-200">
                <div class="rounded-lg border border-border bg-black/20 px-4 py-3">
                    <p class="text-xs uppercase tracking-[0.15em] text-muted">Estimated value</p>
                    <p class="mt-1 text-base font-semibold text-slate-50"><?= htmlspecialchars((string) ($zkb['total_value_display'] ?? 'Unavailable'), ENT_QUOTES) ?></p>
                </div>
                <div class="rounded-lg border border-border bg-black/20 px-4 py-3">
                    <p class="text-xs uppercase tracking-[0.15em] text-muted">Metadata flags</p>
                    <p class="mt-1 text-sm text-slate-300">Points <?= htmlspecialchars((string) ($zkb['points_display'] ?? 'Unavailable'), ENT_QUOTES) ?> · NPC <?= !empty($zkb['npc']) ? 'Yes' : 'No' ?> · Solo <?= !empty($zkb['solo']) ? 'Yes' : 'No' ?> · Awox <?= !empty($zkb['awox']) ? 'Yes' : 'No' ?></p>
                    <p class="mt-1 text-xs text-muted"><?= htmlspecialchars((string) ($zkb['location_id_display'] ?? 'Unavailable'), ENT_QUOTES) ?></p>
                </div>
                <?php if ((string) ($zkb['href'] ?? '') !== ''): ?>
                    <a href="<?= htmlspecialchars((string) $zkb['href'], ENT_QUOTES) ?>" target="_blank" rel="noreferrer" class="inline-flex items-center rounded-lg border border-border px-3 py-2 text-sm text-slate-200 transition hover:bg-white/5">Open zKill reference</a>
                <?php else: ?>
                    <div class="rounded-lg border border-dashed border-border bg-black/10 px-4 py-3 text-sm text-muted">No zKill reference URL was stored for this loss.</div>
                <?php endif; ?>
            </div>
        </article>
    </section>
<?php endif; ?>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
