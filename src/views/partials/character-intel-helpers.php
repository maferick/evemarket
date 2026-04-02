<?php

declare(strict_types=1);

/**
 * Shared helper functions for character intelligence display.
 * Used by both character.php and pilot-lookup.php.
 */

/** Return a risk-level label and Tailwind color classes based on review priority score. */
function ci_risk_level(float $score): array
{
    if ($score >= 0.75) {
        return ['label' => 'High', 'bg' => 'bg-red-900/60', 'text' => 'text-red-300', 'bar' => 'bg-red-500'];
    }
    if ($score >= 0.45) {
        return ['label' => 'Medium', 'bg' => 'bg-yellow-900/60', 'text' => 'text-yellow-300', 'bar' => 'bg-yellow-500'];
    }
    if ($score >= 0.20) {
        return ['label' => 'Low', 'bg' => 'bg-blue-900/60', 'text' => 'text-blue-300', 'bar' => 'bg-blue-500'];
    }
    return ['label' => 'Minimal', 'bg' => 'bg-green-900/60', 'text' => 'text-green-300', 'bar' => 'bg-green-500'];
}

/** Return color classes for a 0-1 metric (higher = more suspicious). */
function ci_metric_color(float $value, float $cautionAt = 0.4, float $dangerAt = 0.7): string
{
    if ($value >= $dangerAt) {
        return 'text-red-400';
    }
    if ($value >= $cautionAt) {
        return 'text-yellow-400';
    }
    return 'text-green-400';
}

/** Return color for lift values (>1 means enemy survives more than expected). */
function ci_lift_color(float $lift): string
{
    if ($lift >= 1.5) {
        return 'text-red-400';
    }
    if ($lift >= 1.15) {
        return 'text-yellow-400';
    }
    return 'text-green-400';
}

/** Color for overperformance scores. */
function ci_overperf_color(float $score): string
{
    if ($score >= 1.3) {
        return 'text-red-400';
    }
    if ($score >= 1.1) {
        return 'text-yellow-400';
    }
    if ($score >= 0.9) {
        return 'text-slate-100';
    }
    return 'text-blue-400';
}

/** Return a human-readable label for an anomaly class. */
function ci_anomaly_label(string $class): array
{
    return match ($class) {
        'high_enemy_overperformance' => ['label' => 'Suspicious', 'bg' => 'bg-red-900/60', 'text' => 'text-red-300'],
        'underperforming' => ['label' => 'Under-performing', 'bg' => 'bg-blue-900/60', 'text' => 'text-blue-300'],
        default => ['label' => 'Normal', 'bg' => 'bg-slate-700', 'text' => 'text-slate-300'],
    };
}

/** Convert snake_case evidence keys to human-readable labels. */
function ci_evidence_label(string $key): string
{
    $map = [
        'anomalous_battle_presence_count' => 'Anomalous battle appearances',
        'enemy_same_hull_survival_lift_detail' => 'Enemy hull survival lift',
        'enemy_sustain_lift' => 'Enemy sustain lift',
        'graph_copresence_cluster_proximity' => 'Network clustering',
        'repeatability_across_battles_windows' => 'Pattern repeatability',
        'anomalous_presence_rate' => 'Anomalous presence rate',
        'presence_rate_delta' => 'Presence rate delta',
        'org_history_movement_180d' => 'Corp movement (180 days)',
        'neo4j_cross_side_overlap' => 'Cross-side corp overlap',
        'neo4j_recent_defector' => 'Recent defector',
        'neo4j_hostile_adjacency' => 'Hostile corp adjacency',
        'footprint_expansion' => 'Footprint expansion',
        'footprint_contraction' => 'Footprint contraction',
        'new_area_entry' => 'New area entry',
        'hostile_overlap_change' => 'Hostile overlap change',
        'active_hour_shift' => 'Active hour shift',
        'weekday_profile_shift' => 'Weekday profile shift',
        'cadence_burstiness' => 'Cadence burstiness',
        'reactivation_after_dormancy' => 'Reactivation after dormancy',
    ];
    return $map[$key] ?? ucwords(str_replace('_', ' ', $key));
}

/** Render a slim progress bar (0-1 range). */
function ci_progress_bar(float $value, string $barColor = 'bg-cyan-500', float $max = 1.0): string
{
    $pct = min(100, max(0, ($value / $max) * 100));
    return '<div class="mt-1 h-1.5 w-full rounded-full bg-slate-700/60 overflow-hidden">'
         . '<div class="h-full rounded-full ' . $barColor . '" style="width:' . number_format($pct, 1) . '%"></div>'
         . '</div>';
}

/** Render an evidence path from structured node data, using resolved names. */
function ci_render_evidence_path(string $nodesJson, string $edgesJson, array $nameMap): string
{
    $nodes = json_decode($nodesJson, true);
    if (!is_array($nodes) || $nodes === []) {
        return '<span class="text-muted">No path data</span>';
    }

    $edges = json_decode($edgesJson, true);
    if (!is_array($edges)) {
        $edges = [];
    }

    $html = '';
    foreach ($nodes as $i => $node) {
        $type = (string) ($node['type'] ?? 'Unknown');
        $id = $node['id'] ?? null;
        $rawName = (string) ($node['name'] ?? '');
        $flagged = !empty($node['flagged']);

        // Try resolved name first, fall back to node name, then short ID
        if ($type === 'Character' && is_numeric($id) && isset($nameMap[(int) $id])) {
            $displayName = $nameMap[(int) $id];
        } elseif ($rawName !== '' && !is_numeric($rawName)) {
            $displayName = $rawName;
        } elseif (is_numeric($id)) {
            $displayName = $type . ' #' . $id;
        } else {
            $displayName = '?';
        }

        // Edge label between nodes
        if ($i > 0 && isset($edges[$i - 1])) {
            $edgeType = (string) ($edges[$i - 1]['type'] ?? '?');
            $readableEdge = strtolower(str_replace('_', ' ', $edgeType));
            $html .= ' <span class="text-muted">&rarr;</span> <span class="text-[11px] text-slate-500 italic">' . htmlspecialchars($readableEdge, ENT_QUOTES) . '</span> <span class="text-muted">&rarr;</span> ';
        }

        // Node display
        $nameClass = $flagged ? 'text-red-400 font-semibold' : ($i === 0 ? 'text-cyan-400' : 'text-slate-100');
        if ($type === 'Character' && is_numeric($id)) {
            $html .= '<a href="/battle-intelligence/character.php?character_id=' . (int) $id . '" class="' . $nameClass . ' hover:underline">' . htmlspecialchars($displayName, ENT_QUOTES) . '</a>';
        } else {
            $html .= '<span class="' . $nameClass . '">' . htmlspecialchars($displayName, ENT_QUOTES) . '</span>';
        }

        if ($flagged) {
            $html .= ' <span class="text-[10px] rounded-full bg-red-900/60 text-red-300 px-1.5 py-0.5">flagged</span>';
        }
    }

    return $html;
}

/** Format a date string like "2022/01/05 04:25" into a human-friendly relative duration. */
function ci_corp_duration(string $startDate, ?string $endDate): string
{
    $start = strtotime(str_replace('/', '-', $startDate));
    $end = $endDate !== null ? strtotime(str_replace('/', '-', $endDate)) : time();
    if ($start === false || $end === false || $end <= $start) {
        return '';
    }
    $days = (int) round(($end - $start) / 86400);
    if ($days < 1) {
        return '< 1 day';
    }
    if ($days < 30) {
        return $days . 'd';
    }
    $months = (int) round($days / 30.44);
    if ($months < 12) {
        return $months . 'mo';
    }
    $years = round($days / 365.25, 1);
    return rtrim(rtrim(number_format($years, 1), '0'), '.') . 'y';
}

/** Render a plain-English one-line verdict based on key metrics. */
function ci_verdict_summary(array $char): string
{
    $score = (float) ($char['review_priority_score'] ?? 0);
    $pctile = (float) ($char['percentile_rank'] ?? 0) * 100;
    $lift = (float) ($char['enemy_sustain_lift'] ?? 0);
    $repeat = (float) ($char['repeatability_score'] ?? 0);
    $hops = (float) ($char['corp_hop_frequency_180d'] ?? 0);

    $parts = [];

    if ($score >= 0.75) {
        $parts[] = 'This character has a <strong class="text-red-400">high</strong> review priority and warrants close investigation.';
    } elseif ($score >= 0.45) {
        $parts[] = 'This character shows <strong class="text-yellow-400">moderate</strong> suspicious signals.';
    } elseif ($score >= 0.20) {
        $parts[] = 'This character has <strong class="text-blue-400">low-level</strong> flags &mdash; likely not a priority.';
    } else {
        $parts[] = 'This character shows <strong class="text-green-400">minimal</strong> suspicious activity.';
    }

    if ($lift >= 1.15) {
        $parts[] = 'Enemies survive <strong class="text-yellow-400">' . number_format(($lift - 1) * 100, 0) . '% more</strong> than expected when this pilot is on the field.';
    }

    if ($repeat >= 0.5) {
        $parts[] = 'Suspicious patterns repeat across multiple time windows.';
    }

    if ($hops >= 3) {
        $parts[] = 'Frequent corp-hopping detected (' . number_format($hops, 0) . ' changes in 180 days).';
    }

    if ($pctile >= 95) {
        $parts[] = 'Ranks in the <strong>top ' . number_format(100 - $pctile, 1) . '%</strong> of all tracked characters.';
    }

    return implode(' ', $parts);
}
