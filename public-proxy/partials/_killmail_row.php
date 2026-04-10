<?php

declare(strict_types=1);

/**
 * Shared killmail row renderer.
 *
 * Expects: $__km (array) with keys from the /recent-killmails API and
 * optionally top-level victim fields. Used by recent.php, character.php,
 * and the theater sidebar.
 *
 * Parameters via $__kmVariant:
 *   'full'   (default) — pilot, ship, location, value
 *   'compact'          — single line (sidebar)
 */

$__km = $__km ?? [];
$__kmVariant = $__kmVariant ?? 'full';

$seq        = (int) ($__km['sequence_id'] ?? 0);
$value      = (float) ($__km['value'] ?? 0);
$points     = (int) ($__km['points'] ?? 0);
$time       = (string) ($__km['killmail_time'] ?? '');
$system     = (string) ($__km['system_name'] ?? '');
$region     = (string) ($__km['region_name'] ?? '');
$security   = $__km['security_status'] ?? $__km['security'] ?? null;

$victim = $__km['victim'] ?? $__km; // allow flat shape from character endpoint
$victimCharId   = (int)    ($victim['character_id'] ?? $__km['victim_character_id'] ?? 0);
$victimCharName = (string) ($victim['character_name'] ?? $__km['victim_name'] ?? '');
$victimCorpName = (string) ($victim['corporation_name'] ?? '');
$victimAlliName = (string) ($victim['alliance_name'] ?? '');
$shipTypeId     = (int)    ($victim['ship_type_id'] ?? $__km['victim_ship_type_id'] ?? 0);
$shipName       = (string) ($victim['ship_name'] ?? $__km['victim_ship_name'] ?? '');
$solo           = (bool)   ($__km['solo'] ?? false);
$finalBlow      = (bool)   ($__km['final_blow'] ?? false);

$secClass = 'text-slate-400';
if ($security !== null) {
    $s = (float) $security;
    if ($s >= 0.5)      $secClass = 'text-green-400';
    elseif ($s > 0.0)   $secClass = 'text-yellow-400';
    else                $secClass = 'text-red-400';
}

$linkHref = 'killmail.php?sequence_id=' . $seq;
?>
<?php if ($__kmVariant === 'compact'): ?>
<a class="km-row km-row-compact" href="<?= proxy_e($linkHref) ?>">
    <?php if ($shipTypeId > 0): ?>
        <img class="km-thumb" src="https://images.evetech.net/types/<?= $shipTypeId ?>/render?size=32" alt="" loading="lazy">
    <?php endif; ?>
    <div class="km-row-body">
        <div class="km-row-title">
            <span class="km-ship"><?= proxy_e($shipName ?: 'Ship') ?></span>
            <span class="km-value"><?= proxy_format_isk($value) ?></span>
        </div>
        <div class="km-row-meta">
            <span class="km-pilot"><?= proxy_e($victimCharName ?: 'Unknown') ?></span>
            <span class="<?= $secClass ?>"><?= proxy_e($system) ?></span>
        </div>
    </div>
</a>
<?php else: ?>
<tr class="border-b border-border/50 hover:bg-white/[0.02]">
    <td class="px-3 py-3">
        <a class="flex items-center gap-2" href="<?= proxy_e($linkHref) ?>">
            <?php if ($shipTypeId > 0): ?>
                <img class="w-12 h-12 rounded-md bg-slate-800/50 object-contain" src="https://images.evetech.net/types/<?= $shipTypeId ?>/render?size=64" alt="" loading="lazy">
            <?php endif; ?>
            <div>
                <div class="text-sm text-slate-100 font-semibold"><?= proxy_e($shipName ?: 'Unknown ship') ?></div>
                <?php if ($victimCharName !== ''): ?>
                    <div class="text-[11px] text-muted"><?= proxy_e($victimCharName) ?></div>
                <?php endif; ?>
            </div>
        </a>
    </td>
    <td class="px-3 py-3 text-xs text-slate-300">
        <div><?= proxy_e($victimCorpName) ?></div>
        <?php if ($victimAlliName !== ''): ?>
            <div class="text-[11px] text-muted"><?= proxy_e($victimAlliName) ?></div>
        <?php endif; ?>
    </td>
    <td class="px-3 py-3">
        <div class="text-xs">
            <span class="<?= $secClass ?>"><?= $security !== null ? number_format((float) $security, 1) : '—' ?></span>
            <span class="text-slate-200"><?= proxy_e($system) ?></span>
        </div>
        <?php if ($region !== ''): ?>
            <div class="text-[11px] text-muted"><?= proxy_e($region) ?></div>
        <?php endif; ?>
    </td>
    <td class="px-3 py-3 text-right text-sm text-slate-100"><?= proxy_format_isk($value) ?></td>
    <td class="px-3 py-3 text-right text-xs text-slate-300">
        <?php if ($points > 0): ?><?= number_format($points) ?> pts<?php endif; ?>
    </td>
    <td class="px-3 py-3 text-right text-[11px] text-muted"><?= proxy_e($time) ?></td>
    <td class="px-3 py-3 text-right">
        <?php if ($solo): ?><span class="km-flag km-flag-solo">solo</span><?php endif; ?>
        <?php if ($finalBlow): ?><span class="km-flag km-flag-fb">FB</span><?php endif; ?>
    </td>
</tr>
<?php endif; ?>
