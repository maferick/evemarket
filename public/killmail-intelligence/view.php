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
        <p class="mt-2 text-sm text-muted">Readable loss intelligence for logistics, doctrine review, and replacement planning.</p>
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
    <?php $signalStrength = $detail['signal_strength'] ?? []; ?>
    <?php $supplyImpact = $detail['supply_impact'] ?? []; ?>
    <?php $lossSummary = $detail['loss_summary'] ?? []; ?>

    <section class="overflow-hidden surface-primary">
        <div class="grid gap-6 p-6 xl:grid-cols-[minmax(320px,430px)_minmax(0,1fr)]">
            <div class="surface-tertiary">
                <?php if ((string) ($ship['render_url'] ?? '') !== ''): ?>
                    <img
                        src="<?= htmlspecialchars((string) $ship['render_url'], ENT_QUOTES) ?>"
                        alt="<?= htmlspecialchars((string) ($ship['name'] ?? 'Victim ship'), ENT_QUOTES) ?>"
                        class="mx-auto aspect-square w-full max-w-sm rounded-2xl object-contain"
                        loading="eager"
                    >
                <?php else: ?>
                    <div class="surface-tertiary flex aspect-square items-center justify-center text-sm text-slate-400">
                        Ship render unavailable
                    </div>
                <?php endif; ?>
            </div>

            <div class="flex flex-col gap-6">
                <div class="flex flex-wrap items-start justify-between gap-4">
                    <div class="max-w-3xl">
                        <p class="text-xs uppercase tracking-[0.2em] text-muted">Loss</p>
                        <h2 class="mt-2 text-3xl font-semibold tracking-tight text-slate-50"><?= htmlspecialchars((string) ($ship['name'] ?? 'Killmail loss'), ENT_QUOTES) ?></h2>
                        <p class="mt-2 text-base text-slate-300"><?= htmlspecialchars((string) ($ship['class'] ?? 'Ship class unavailable'), ENT_QUOTES) ?></p>
                    </div>
                    <div class="flex flex-wrap gap-2">
                        <span class="rounded-full border px-3 py-1 text-xs uppercase tracking-[0.15em] <?= ($signalStrength['tone'] ?? 'border-slate-500/40 bg-slate-500/10 text-slate-300') ?>">
                            <?= htmlspecialchars((string) ($signalStrength['label'] ?? 'Signal'), ENT_QUOTES) ?>
                        </span>
                        <span class="rounded-full border px-3 py-1 text-xs uppercase tracking-[0.15em] <?= ($supplyImpact['tone'] ?? 'border-slate-500/40 bg-slate-500/10 text-slate-300') ?>">
                            <?= htmlspecialchars((string) ($supplyImpact['label'] ?? 'Impact'), ENT_QUOTES) ?>
                        </span>
                    </div>
                </div>

                <div class="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
                    <div class="surface-tertiary md:col-span-2 xl:col-span-1">
                        <p class="text-xs uppercase tracking-[0.15em] text-muted">Victim organization</p>
                        <div class="mt-3 space-y-3">
                            <div class="flex items-center gap-3">
                                <?php if ((string) ($victim['corporation_logo_url'] ?? '') !== ''): ?>
                                    <img src="<?= htmlspecialchars((string) $victim['corporation_logo_url'], ENT_QUOTES) ?>" alt="" class="h-12 w-12 rounded-xl object-cover">
                                <?php endif; ?>
                                <div>
                                    <p class="font-medium text-slate-50"><?= htmlspecialchars((string) ($victim['corporation_display'] ?? 'Unknown corporation'), ENT_QUOTES) ?></p>
                                    <p class="mt-1 text-xs text-muted">Corporation</p>
                                </div>
                            </div>
                            <div class="flex items-center gap-3">
                                <?php if ((string) ($victim['alliance_logo_url'] ?? '') !== ''): ?>
                                    <img src="<?= htmlspecialchars((string) $victim['alliance_logo_url'], ENT_QUOTES) ?>" alt="" class="h-12 w-12 rounded-xl object-cover">
                                <?php endif; ?>
                                <div>
                                    <p class="font-medium text-slate-50"><?= htmlspecialchars((string) ($victim['alliance_display'] ?? 'Unknown alliance'), ENT_QUOTES) ?></p>
                                    <p class="mt-1 text-xs text-muted">Alliance</p>
                                </div>
                            </div>
                        </div>
                    </div>

                    <div class="surface-tertiary">
                        <p class="text-xs uppercase tracking-[0.15em] text-muted">Location</p>
                        <p class="mt-3 text-lg font-semibold text-slate-50"><?= htmlspecialchars((string) ($location['system_display'] ?? 'Unknown system'), ENT_QUOTES) ?></p>
                        <p class="mt-1 text-sm text-slate-300"><?= htmlspecialchars((string) ($location['region_display'] ?? 'Unknown region'), ENT_QUOTES) ?></p>
                        <?php if ((string) ($location['security_status'] ?? '') !== ''): ?>
                            <p class="mt-2 text-xs text-muted">Security <?= htmlspecialchars((string) $location['security_status'], ENT_QUOTES) ?></p>
                        <?php endif; ?>
                    </div>

                    <div class="surface-tertiary">
                        <p class="text-xs uppercase tracking-[0.15em] text-muted">Time</p>
                        <p class="mt-3 text-lg font-semibold text-slate-50"><?= htmlspecialchars((string) ($detail['killmail_time_display'] ?? '—'), ENT_QUOTES) ?></p>
                        <p class="mt-1 text-sm text-muted">Recorded <?= htmlspecialchars((string) ($detail['created_at_display'] ?? '—'), ENT_QUOTES) ?></p>
                    </div>
                </div>

                <div class="grid gap-4 md:grid-cols-3">
                    <div class="surface-tertiary">
                        <p class="text-xs uppercase tracking-[0.15em] text-muted">Victim</p>
                        <div class="mt-3 flex items-center gap-3">
                            <?php if ((string) ($victim['character_portrait_url'] ?? '') !== ''): ?>
                                <img src="<?= htmlspecialchars((string) $victim['character_portrait_url'], ENT_QUOTES) ?>" alt="" class="h-12 w-12 rounded-xl object-cover">
                            <?php endif; ?>
                            <div>
                                <p class="font-medium text-slate-50"><?= htmlspecialchars((string) ($victim['character_name'] ?? 'Unknown character'), ENT_QUOTES) ?></p>
                                <p class="mt-1 text-xs text-muted">Damage taken <?= htmlspecialchars((string) ($victim['damage_taken'] ?? '0'), ENT_QUOTES) ?></p>
                            </div>
                        </div>
                    </div>
                    <div class="surface-tertiary">
                        <p class="text-xs uppercase tracking-[0.15em] text-muted">Signal context</p>
                        <p class="mt-3 text-sm text-slate-200"><?= htmlspecialchars((string) ($signalStrength['context'] ?? 'No signal context available.'), ENT_QUOTES) ?></p>
                    </div>
                    <div class="surface-tertiary">
                        <p class="text-xs uppercase tracking-[0.15em] text-muted">Supply impact</p>
                        <p class="mt-3 text-sm text-slate-200"><?= htmlspecialchars((string) ($supplyImpact['context'] ?? 'No supply impact context available.'), ENT_QUOTES) ?></p>
                    </div>
                </div>
            </div>
        </div>
    </section>

    <section class="mt-6 surface-secondary shadow-lg shadow-black/20">
        <div class="flex flex-wrap items-start justify-between gap-4">
            <div>
                <p class="text-xs uppercase tracking-[0.2em] text-muted">Loss summary</p>
                <h2 class="mt-1 text-xl font-semibold text-slate-50">What matters for logistics</h2>
                <p class="mt-2 max-w-3xl text-sm text-muted">Start with value and extracted item volume, then move directly into dropped, destroyed, and fitted inventory.</p>
            </div>
            <div class="flex flex-wrap gap-2">
                <span class="rounded-full border px-3 py-1 text-xs uppercase tracking-[0.15em] <?= ($detail['tracked_victim_loss'] ?? false) ? 'border-emerald-500/40 bg-emerald-500/10 text-emerald-200' : 'border-slate-500/40 bg-slate-500/10 text-slate-300' ?>">
                    <?= ($detail['tracked_victim_loss'] ?? false) ? 'Tracked victim loss' : 'Recorded loss' ?>
                </span>
                <span class="rounded-full border border-border bg-black/20 px-3 py-1 text-xs uppercase tracking-[0.15em] text-slate-300">Attackers <?= htmlspecialchars(number_format((int) ($attackers['count'] ?? 0)), ENT_QUOTES) ?></span>
            </div>
        </div>

        <div class="mt-6 grid gap-4 md:grid-cols-3">
            <article class="surface-tertiary">
                <p class="text-xs uppercase tracking-[0.15em] text-muted">Estimated value</p>
                <p class="mt-2 text-2xl font-semibold text-slate-50"><?= htmlspecialchars((string) ($lossSummary['estimated_value_display'] ?? 'Value unavailable'), ENT_QUOTES) ?></p>
                <p class="mt-2 text-sm text-muted">zKill estimate for decision support.</p>
            </article>
            <article class="surface-tertiary">
                <p class="text-xs uppercase tracking-[0.15em] text-muted">Items extracted</p>
                <p class="mt-2 text-2xl font-semibold text-slate-50"><?= htmlspecialchars((string) ($lossSummary['item_count_display'] ?? '0 extracted items'), ENT_QUOTES) ?></p>
                <p class="mt-2 text-sm text-muted">Structured inventory available for follow-up analysis.</p>
            </article>
            <article class="surface-tertiary">
                <p class="text-xs uppercase tracking-[0.15em] text-muted">Impact</p>
                <p class="mt-2 text-lg font-semibold text-slate-50"><?= htmlspecialchars((string) ($supplyImpact['label'] ?? 'Impact unavailable'), ENT_QUOTES) ?></p>
                <p class="mt-2 text-sm text-muted"><?= htmlspecialchars((string) ($lossSummary['impact_summary'] ?? 'No impact summary available.'), ENT_QUOTES) ?></p>
            </article>
        </div>
    </section>

    <section class="mt-6 surface-primary shadow-[0_0_30px_rgba(59,130,246,0.18)]">
        <div class="flex flex-wrap items-center justify-between gap-3">
            <div>
                <p class="text-xs uppercase tracking-[0.2em] text-cyan/80">Primary intelligence</p>
                <h2 class="mt-1 text-xl font-semibold text-slate-50">Extracted items</h2>
                <p class="mt-2 max-w-4xl text-sm text-muted">Dropped items indicate immediate recovery or redistribution potential. Destroyed and fitted items show replacement pressure and doctrine exposure.</p>
            </div>
            <div class="grid gap-2 sm:grid-cols-3">
                <div class="surface-tertiary text-center">
                    <p class="text-xs uppercase tracking-[0.15em] text-muted">Dropped</p>
                    <p class="mt-1 text-lg font-semibold text-slate-50"><?= number_format((int) ($detail['item_totals']['dropped'] ?? 0)) ?></p>
                </div>
                <div class="surface-tertiary text-center">
                    <p class="text-xs uppercase tracking-[0.15em] text-muted">Destroyed</p>
                    <p class="mt-1 text-lg font-semibold text-slate-50"><?= number_format((int) ($detail['item_totals']['destroyed'] ?? 0)) ?></p>
                </div>
                <div class="surface-tertiary text-center">
                    <p class="text-xs uppercase tracking-[0.15em] text-muted">Fitted</p>
                    <p class="mt-1 text-lg font-semibold text-slate-50"><?= number_format((int) ($detail['item_totals']['fitted'] ?? 0)) ?></p>
                </div>
            </div>
        </div>

        <div class="mt-6 grid gap-4 xl:grid-cols-3">
            <?php foreach ((array) $items as $groupKey => $group): ?>
                <article class="surface-secondary">
                    <div class="flex items-start justify-between gap-3">
                        <div>
                            <h3 class="text-lg font-semibold text-slate-50"><?= htmlspecialchars((string) ($group['label'] ?? $groupKey), ENT_QUOTES) ?></h3>
                            <p class="mt-1 text-sm text-muted"><?= htmlspecialchars((string) ($group['description'] ?? ''), ENT_QUOTES) ?></p>
                        </div>
                        <span class="rounded-full border border-accent/40 bg-accent/10 px-2 py-0.5 text-xs text-slate-100"><?= number_format((int) ($group['total_quantity'] ?? 0)) ?></span>
                    </div>
                    <?php if (((array) ($group['rows'] ?? [])) === []): ?>
                        <div class="mt-4 rounded-xl border border-dashed border-border bg-black/20 px-4 py-5 text-sm text-slate-400">
                            <?= htmlspecialchars(killmail_item_empty_message((string) $groupKey), ENT_QUOTES) ?>
                        </div>
                    <?php else: ?>
                        <div class="mt-4 space-y-3">
                            <?php foreach ((array) ($group['rows'] ?? []) as $itemRow): ?>
                                <div class="surface-tertiary">
                                    <div class="flex items-start gap-3">
                                        <?php if ((string) ($itemRow['item_icon_url'] ?? '') !== ''): ?>
                                            <img src="<?= htmlspecialchars((string) $itemRow['item_icon_url'], ENT_QUOTES) ?>" alt="" class="h-14 w-14 rounded-xl bg-black/30 object-contain p-1">
                                        <?php endif; ?>
                                        <div class="min-w-0 flex-1">
                                            <div class="flex flex-wrap items-start justify-between gap-2">
                                                <div>
                                                    <p class="text-base font-semibold text-slate-50"><?= htmlspecialchars((string) ($itemRow['item_name'] ?? 'Unknown item'), ENT_QUOTES) ?></p>
                                                    <p class="mt-1 text-xs text-muted"><?= htmlspecialchars((string) ($itemRow['state_label'] ?? ''), ENT_QUOTES) ?></p>
                                                </div>
                                                <span class="rounded-full border border-accent/40 bg-accent/10 px-2 py-0.5 text-xs text-slate-100"><?= htmlspecialchars((string) ($itemRow['quantity_label'] ?? 'Qty 0'), ENT_QUOTES) ?></span>
                                            </div>
                                            <div class="mt-3 flex flex-wrap gap-2 text-xs text-muted">
                                                <span class="rounded-full border border-border bg-black/20 px-2 py-0.5">Flag <?= htmlspecialchars((string) (($itemRow['item_flag'] ?? null) !== null ? (string) $itemRow['item_flag'] : '—'), ENT_QUOTES) ?></span>
                                                <span class="rounded-full border border-border bg-black/20 px-2 py-0.5">Singleton <?= htmlspecialchars((string) (($itemRow['singleton'] ?? null) !== null ? (string) $itemRow['singleton'] : '—'), ENT_QUOTES) ?></span>
                                                <span class="rounded-full border border-border bg-black/20 px-2 py-0.5">Recorded <?= htmlspecialchars((string) ($itemRow['stored_at_display'] ?? '—'), ENT_QUOTES) ?></span>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            <?php endforeach; ?>
                        </div>
                    <?php endif; ?>
                </article>
            <?php endforeach; ?>
        </div>
    </section>

    <section class="mt-6">
        <details class="surface-secondary group">
            <summary class="flex cursor-pointer list-none items-center justify-between gap-3">
                <div>
                    <p class="text-xs uppercase tracking-[0.2em] text-muted">Secondary context</p>
                    <h2 class="mt-1 text-lg font-medium text-slate-50">Attackers</h2>
                </div>
                <span class="rounded-full border border-border bg-black/20 px-3 py-1 text-xs uppercase tracking-[0.15em] text-slate-300">Collapsed</span>
            </summary>

            <div class="mt-4 space-y-4">
                <div class="surface-tertiary">
                    <p class="text-xs uppercase tracking-[0.15em] text-muted">Final blow</p>
                    <?php if (is_array($attackers['final_blow'] ?? null)): ?>
                        <div class="mt-3 flex items-center gap-3">
                            <?php if ((string) (($attackers['final_blow']['ship_icon_url'] ?? '')) !== ''): ?>
                                <img src="<?= htmlspecialchars((string) $attackers['final_blow']['ship_icon_url'], ENT_QUOTES) ?>" alt="" class="h-11 w-11 rounded-lg bg-black/30 object-contain p-1">
                            <?php endif; ?>
                            <div>
                                <p class="font-medium text-slate-50"><?= htmlspecialchars((string) (($attackers['final_blow']['character_name'] ?? 'Unknown attacker')), ENT_QUOTES) ?></p>
                                <p class="mt-1 text-sm text-slate-300"><?= htmlspecialchars((string) (($attackers['final_blow']['corporation_display'] ?? 'Unknown corporation') . ' · ' . ($attackers['final_blow']['alliance_display'] ?? 'Unknown alliance')), ENT_QUOTES) ?></p>
                                <p class="mt-1 text-xs text-muted"><?= htmlspecialchars((string) (($attackers['final_blow']['ship_display'] ?? 'Unknown ship') . ' · ' . ($attackers['final_blow']['weapon_display'] ?? 'Unknown weapon')), ENT_QUOTES) ?></p>
                            </div>
                        </div>
                    <?php else: ?>
                        <p class="mt-2 text-sm text-muted">No final blow row stored.</p>
                    <?php endif; ?>
                </div>

                <div class="grid gap-3 lg:grid-cols-2">
                    <?php foreach ((array) ($attackers['top_rows'] ?? []) as $attacker): ?>
                        <div class="surface-tertiary opacity-90">
                            <div class="flex items-start gap-3">
                                <?php if ((string) ($attacker['ship_icon_url'] ?? '') !== ''): ?>
                                    <img src="<?= htmlspecialchars((string) $attacker['ship_icon_url'], ENT_QUOTES) ?>" alt="" class="h-10 w-10 rounded-lg bg-black/30 object-contain p-1">
                                <?php endif; ?>
                                <div class="min-w-0 flex-1">
                                    <div class="flex items-start justify-between gap-3">
                                        <div>
                                            <p class="font-medium text-slate-50"><?= htmlspecialchars((string) ($attacker['character_name'] ?? 'Unknown attacker'), ENT_QUOTES) ?></p>
                                            <div class="mt-1 flex flex-wrap items-center gap-2 text-xs text-slate-300">
                                                <?php if ((string) ($attacker['corporation_logo_url'] ?? '') !== ''): ?>
                                                    <img src="<?= htmlspecialchars((string) $attacker['corporation_logo_url'], ENT_QUOTES) ?>" alt="" class="h-5 w-5 rounded-md object-cover">
                                                <?php endif; ?>
                                                <span><?= htmlspecialchars((string) ($attacker['corporation_display'] ?? 'Unknown corporation'), ENT_QUOTES) ?></span>
                                            </div>
                                            <div class="mt-1 flex flex-wrap items-center gap-2 text-xs text-muted">
                                                <?php if ((string) ($attacker['alliance_logo_url'] ?? '') !== ''): ?>
                                                    <img src="<?= htmlspecialchars((string) $attacker['alliance_logo_url'], ENT_QUOTES) ?>" alt="" class="h-5 w-5 rounded-md object-cover">
                                                <?php endif; ?>
                                                <span><?= htmlspecialchars((string) ($attacker['alliance_display'] ?? 'Unknown alliance'), ENT_QUOTES) ?></span>
                                            </div>
                                        </div>
                                        <?php if ($attacker['final_blow'] ?? false): ?>
                                            <span class="rounded-full border border-emerald-500/40 bg-emerald-500/10 px-2 py-0.5 text-[11px] uppercase tracking-[0.08em] text-emerald-200">Final blow</span>
                                        <?php endif; ?>
                                    </div>
                                    <p class="mt-2 text-xs text-muted"><?= htmlspecialchars((string) (($attacker['ship_display'] ?? '—') . ' · ' . ($attacker['weapon_display'] ?? '—') . ' · Sec ' . ($attacker['security_status'] ?? '—')), ENT_QUOTES) ?></p>
                                </div>
                            </div>
                        </div>
                    <?php endforeach; ?>
                </div>
            </div>
        </details>
    </section>

    <section class="mt-6">
        <details class="surface-secondary opacity-90">
            <summary class="flex cursor-pointer list-none items-center justify-between gap-3">
                <div>
                    <p class="text-xs uppercase tracking-[0.2em] text-muted">Sync and ingestion</p>
                    <h2 class="mt-1 text-lg font-medium text-slate-50">Show technical details</h2>
                </div>
                <span class="rounded-full border border-border bg-black/20 px-3 py-1 text-xs uppercase tracking-[0.15em] text-slate-300">Collapsed by default</span>
            </summary>

            <div class="mt-4 grid gap-4 xl:grid-cols-[minmax(0,1.2fr)_minmax(320px,0.8fr)]">
                <article class="surface-tertiary">
                    <h3 class="text-base font-medium text-slate-50">Sync status</h3>
                    <div class="mt-4 grid gap-3 md:grid-cols-2">
                        <div class="surface-tertiary">
                            <p class="text-xs uppercase tracking-[0.15em] text-muted">Recorded</p>
                            <p class="mt-2 text-lg font-semibold text-slate-50"><?= htmlspecialchars((string) ($detail['created_at_display'] ?? '—'), ENT_QUOTES) ?></p>
                            <p class="mt-1 text-sm text-muted">Updated <?= htmlspecialchars((string) ($detail['updated_at_display'] ?? '—'), ENT_QUOTES) ?></p>
                        </div>
                        <div class="surface-tertiary">
                            <p class="text-xs uppercase tracking-[0.15em] text-muted">Items extracted</p>
                            <p class="mt-2 text-lg font-semibold text-slate-50"><?= number_format((int) ($detail['stored_item_count'] ?? 0)) ?></p>
                            <p class="mt-1 text-sm text-muted">Ready for downstream analytics.</p>
                        </div>
                        <div class="surface-tertiary md:col-span-2">
                            <p class="text-xs uppercase tracking-[0.15em] text-muted">Context</p>
                            <p class="mt-2 text-sm text-slate-200"><?= htmlspecialchars((string) ($detail['match_context'] ?? 'No tracked match context available.'), ENT_QUOTES) ?></p>
                        </div>
                    </div>
                </article>

                <article class="surface-tertiary">
                    <h3 class="text-base font-medium text-slate-50">Killmail</h3>
                    <div class="mt-4 space-y-3 text-sm text-slate-200">
                        <div class="surface-tertiary">
                            <p class="text-xs uppercase tracking-[0.15em] text-muted">Estimated value</p>
                            <p class="mt-1 text-base font-semibold text-slate-50"><?= htmlspecialchars((string) ($zkb['total_value_display'] ?? 'Unavailable'), ENT_QUOTES) ?></p>
                        </div>
                        <div class="surface-tertiary">
                            <p class="text-xs uppercase tracking-[0.15em] text-muted">Metadata flags</p>
                            <p class="mt-1 text-sm text-slate-300">Points <?= htmlspecialchars((string) ($zkb['points_display'] ?? 'Unavailable'), ENT_QUOTES) ?> · NPC <?= !empty($zkb['npc']) ? 'Yes' : 'No' ?> · Solo <?= !empty($zkb['solo']) ? 'Yes' : 'No' ?> · Awox <?= !empty($zkb['awox']) ? 'Yes' : 'No' ?></p>
                        </div>
                        <?php if ((string) ($zkb['href'] ?? '') !== ''): ?>
                            <a href="<?= htmlspecialchars((string) $zkb['href'], ENT_QUOTES) ?>" target="_blank" rel="noreferrer" class="inline-flex items-center rounded-lg border border-border px-3 py-2 text-sm text-slate-200 transition hover:bg-white/5">Open zKill reference</a>
                        <?php else: ?>
                            <div class="surface-tertiary text-sm text-slate-400">No zKill reference URL was stored for this loss.</div>
                        <?php endif; ?>
                    </div>
                </article>

                <article class="surface-tertiary xl:col-span-2">
                    <h3 class="text-base font-medium text-slate-50">Internal IDs and hashes</h3>
                    <div class="mt-4 grid gap-3 md:grid-cols-3 text-sm text-slate-200">
                        <div class="surface-tertiary">
                            <p class="text-xs uppercase tracking-[0.15em] text-muted">Sequence</p>
                            <p class="mt-1 font-mono text-slate-50"><?= htmlspecialchars((string) ($detail['sequence_id'] ?? '—'), ENT_QUOTES) ?></p>
                        </div>
                        <div class="surface-tertiary">
                            <p class="text-xs uppercase tracking-[0.15em] text-muted">Killmail ID</p>
                            <p class="mt-1 font-mono text-slate-50"><?= htmlspecialchars((string) ($detail['killmail_id'] ?? '—'), ENT_QUOTES) ?></p>
                        </div>
                        <div class="surface-tertiary md:col-span-3 xl:col-span-1">
                            <p class="text-xs uppercase tracking-[0.15em] text-muted">Hash</p>
                            <p class="mt-1 break-all font-mono text-slate-50"><?= htmlspecialchars((string) ($detail['killmail_hash'] ?? '—'), ENT_QUOTES) ?></p>
                        </div>
                    </div>
                </article>
            </div>
        </details>
    </section>
<?php endif; ?>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
