/**
 * Pilot Lookup — Chart Visualizations
 * Uses Chart.js (loaded via CDN) for activity timeline, ship usage,
 * risk contribution, and CSS-grid heatmaps for temporal patterns.
 */
(function () {
    'use strict';

    const COLORS = {
        cyan:    'rgb(52, 214, 255)',
        cyanDim: 'rgba(52, 214, 255, 0.15)',
        red:     'rgb(248, 113, 113)',
        redDim:  'rgba(248, 113, 113, 0.15)',
        yellow:  'rgb(250, 204, 21)',
        yellowDim: 'rgba(250, 204, 21, 0.15)',
        green:   'rgb(74, 222, 128)',
        greenDim: 'rgba(74, 222, 128, 0.15)',
        slate:   'rgb(148, 163, 184)',
        slateDim: 'rgba(148, 163, 184, 0.08)',
        grid:    'rgba(148, 163, 184, 0.08)',
        gridTick:'rgba(148, 163, 184, 0.5)',
    };

    const chartDefaults = {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: { labels: { color: COLORS.slate, font: { size: 11 } } },
            tooltip: {
                backgroundColor: 'rgba(15, 23, 42, 0.95)',
                borderColor: 'rgba(255,255,255,0.1)',
                borderWidth: 1,
                titleColor: '#fff',
                bodyColor: COLORS.slate,
                cornerRadius: 8,
                padding: 10,
            },
        },
        scales: {
            x: { grid: { color: COLORS.grid }, ticks: { color: COLORS.gridTick, font: { size: 10 } } },
            y: { grid: { color: COLORS.grid }, ticks: { color: COLORS.gridTick, font: { size: 10 } } },
        },
    };

    // ── Activity Timeline ──────────────────────────────────
    function renderActivityTimeline(canvasId, dataAttr) {
        const canvas = document.getElementById(canvasId);
        if (!canvas) return;
        const raw = canvas.getAttribute(dataAttr);
        if (!raw) return;
        let data;
        try { data = JSON.parse(raw); } catch (e) { return; }
        if (!data.labels || !data.labels.length) {
            canvas.parentElement.innerHTML = '<p class="text-xs text-slate-500 py-8 text-center">No temporal data available</p>';
            return;
        }

        new Chart(canvas, {
            type: 'line',
            data: {
                labels: data.labels,
                datasets: [
                    {
                        label: 'Kills',
                        data: data.kills,
                        borderColor: COLORS.cyan,
                        backgroundColor: COLORS.cyanDim,
                        fill: true,
                        tension: 0.3,
                        pointRadius: 0,
                        pointHitRadius: 8,
                        borderWidth: 2,
                    },
                    {
                        label: 'Deaths',
                        data: data.deaths,
                        borderColor: COLORS.red,
                        backgroundColor: COLORS.redDim,
                        fill: true,
                        tension: 0.3,
                        pointRadius: 0,
                        pointHitRadius: 8,
                        borderWidth: 2,
                    },
                    {
                        label: 'Battles',
                        data: data.battles,
                        borderColor: COLORS.slate,
                        backgroundColor: COLORS.slateDim,
                        fill: false,
                        tension: 0.3,
                        pointRadius: 0,
                        pointHitRadius: 8,
                        borderWidth: 1,
                        borderDash: [4, 4],
                    },
                ],
            },
            options: {
                ...chartDefaults,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    ...chartDefaults.plugins,
                    legend: { display: true, position: 'top', labels: { color: COLORS.slate, boxWidth: 12, padding: 16, font: { size: 11 } } },
                },
                scales: {
                    x: { grid: { color: COLORS.grid }, ticks: { color: COLORS.gridTick, font: { size: 10 }, maxTicksLimit: 12 } },
                    y: { grid: { color: COLORS.grid }, ticks: { color: COLORS.gridTick, font: { size: 10 } }, beginAtZero: true },
                },
            },
        });
    }

    // ── Ship Usage Bar Chart ───────────────────────────────
    function renderShipChart(canvasId, dataAttr) {
        const canvas = document.getElementById(canvasId);
        if (!canvas) return;
        const raw = canvas.getAttribute(dataAttr);
        if (!raw) return;
        let data;
        try { data = JSON.parse(raw); } catch (e) { return; }
        if (!data.labels || !data.labels.length) return;

        new Chart(canvas, {
            type: 'bar',
            data: {
                labels: data.labels,
                datasets: [{
                    label: 'Times flown',
                    data: data.values,
                    backgroundColor: data.labels.map((_, i) => i === 0 ? COLORS.cyan : 'rgba(52, 214, 255, ' + (0.7 - i * 0.06) + ')'),
                    borderColor: 'rgba(52, 214, 255, 0.3)',
                    borderWidth: 1,
                    borderRadius: 4,
                    barThickness: 22,
                }],
            },
            options: {
                ...chartDefaults,
                indexAxis: 'y',
                plugins: { ...chartDefaults.plugins, legend: { display: false } },
                scales: {
                    x: { grid: { color: COLORS.grid }, ticks: { color: COLORS.gridTick, font: { size: 10 } }, beginAtZero: true },
                    y: { grid: { display: false }, ticks: { color: '#e2e8f0', font: { size: 11 } } },
                },
            },
        });
    }

    // ── Risk Contribution Chart ────────────────────────────
    function renderRiskContribution(canvasId, dataAttr) {
        const canvas = document.getElementById(canvasId);
        if (!canvas) return;
        const raw = canvas.getAttribute(dataAttr);
        if (!raw) return;
        let data;
        try { data = JSON.parse(raw); } catch (e) { return; }
        if (!data.labels || !data.labels.length) return;

        const barColors = data.values.map(v => {
            if (v >= 0.7) return COLORS.red;
            if (v >= 0.3) return COLORS.yellow;
            return COLORS.green;
        });

        new Chart(canvas, {
            type: 'bar',
            data: {
                labels: data.labels,
                datasets: [{
                    label: 'Contribution',
                    data: data.values,
                    backgroundColor: barColors.map(c => c.replace('rgb', 'rgba').replace(')', ', 0.6)')),
                    borderColor: barColors,
                    borderWidth: 1,
                    borderRadius: 4,
                    barThickness: 22,
                }],
            },
            options: {
                ...chartDefaults,
                indexAxis: 'y',
                plugins: {
                    ...chartDefaults.plugins,
                    legend: { display: false },
                    tooltip: {
                        ...chartDefaults.plugins.tooltip,
                        callbacks: {
                            label: function(ctx) {
                                const v = ctx.parsed.x;
                                if (v >= 0.7) return 'High — ' + (v * 100).toFixed(0) + '%';
                                if (v >= 0.3) return 'Moderate — ' + (v * 100).toFixed(0) + '%';
                                return 'Low — ' + (v * 100).toFixed(0) + '%';
                            }
                        }
                    },
                },
                scales: {
                    x: { grid: { color: COLORS.grid }, ticks: { color: COLORS.gridTick, font: { size: 10 }, callback: v => (v * 100) + '%' }, min: 0, max: 1 },
                    y: { grid: { display: false }, ticks: { color: '#e2e8f0', font: { size: 11 } } },
                },
            },
        });
    }

    // ── Activity Heatmap (CSS Grid) ────────────────────────
    function renderActivityHeatmap(containerId, dataAttr) {
        const container = document.getElementById(containerId);
        if (!container) return;
        const raw = container.getAttribute(dataAttr);
        if (!raw) return;
        let data;
        try { data = JSON.parse(raw); } catch (e) { return; }
        // data = { hours: [0..23], days: ['Sun'..'Sat'], grid: [[day0h0, day0h1, ...], ...] }
        if (!data.grid || !data.grid.length) return;

        const maxVal = Math.max(1, ...data.grid.flat());
        const days = data.days || ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
        const hours = data.hours || Array.from({length: 24}, (_, i) => i);

        let html = '<div class="grid gap-px" style="grid-template-columns: 40px repeat(' + hours.length + ', 1fr)">';
        // Header row
        html += '<div></div>';
        for (let h = 0; h < hours.length; h++) {
            html += '<div class="text-center text-[9px] text-slate-500 pb-1">' + (h % 3 === 0 ? hours[h] : '') + '</div>';
        }
        // Data rows
        for (let d = 0; d < days.length; d++) {
            html += '<div class="text-[10px] text-slate-400 pr-2 flex items-center justify-end">' + days[d] + '</div>';
            for (let h = 0; h < hours.length; h++) {
                const val = (data.grid[d] && data.grid[d][h]) || 0;
                const intensity = val / maxVal;
                const alpha = Math.max(0.04, intensity * 0.9);
                const color = intensity > 0.7 ? '52, 214, 255' : intensity > 0.3 ? '52, 214, 255' : '148, 163, 184';
                html += '<div class="rounded-sm aspect-square" style="background: rgba(' + color + ',' + alpha.toFixed(2) + ')" title="' + days[d] + ' ' + hours[h] + ':00 — ' + val + ' events"></div>';
            }
        }
        html += '</div>';
        container.innerHTML = html;
    }

    // ── Collapsible sections ───────────────────────────────
    function initCollapsibles() {
        document.querySelectorAll('[data-collapse-toggle]').forEach(btn => {
            btn.addEventListener('click', () => {
                const target = document.getElementById(btn.getAttribute('data-collapse-toggle'));
                if (!target) return;
                const expanded = target.classList.toggle('hidden');
                const icon = btn.querySelector('[data-collapse-icon]');
                if (icon) icon.style.transform = expanded ? '' : 'rotate(180deg)';
                btn.setAttribute('aria-expanded', String(!expanded));
            });
        });
    }

    // ── Init on DOMContentLoaded ───────────────────────────
    function init() {
        renderActivityTimeline('chart-activity-timeline', 'data-chart');
        renderShipChart('chart-ship-usage', 'data-chart');
        renderRiskContribution('chart-risk-contribution', 'data-chart');
        renderActivityHeatmap('heatmap-7d', 'data-heatmap');
        renderActivityHeatmap('heatmap-30d', 'data-heatmap');
        renderActivityHeatmap('heatmap-90d', 'data-heatmap');
        renderActivityHeatmap('heatmap-lifetime', 'data-heatmap');
        initCollapsibles();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
