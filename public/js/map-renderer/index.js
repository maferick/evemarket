/**
 * AegisCore / SupplyCore Map Renderer — ES Module
 *
 * Exports a single function: mountMapRenderer(root)
 *
 * The root element must carry these data attributes:
 *   data-url          Full URL to fetch the MapScene JSON from.
 *   data-scope        "universe" | "region" | "system" | …  (informational)
 *   data-label-mode   "hover" | "always" | "hidden"
 *   data-color-by     "security" | "region"
 *   data-interactive  "true" | "false"
 *   data-highlights   JSON array of highlighted system/region IDs
 *   data-instance-id  Unique string ID for SVG filter namespacing
 *
 * The MapScene JSON contract (from /internal/map/* or /api/map-graph.php):
 *   canvas   : { width, height, pad }
 *   nodes    : { [id]: { id, name, security, x, y, role, system_count?, … } }
 *   edges    : [ { from, to, tier } ]
 *   build_stats: { node_count, edge_count, … }
 *   layout   : "universe-aggregated-v1" | "universe-dense-v1" | "region-v1" | …
 *
 * IMPORTANT – coordinate system
 *   x and y in the scene are NORMALISED values in [0, 1].
 *   To obtain SVG pixel coordinates apply:
 *     px = canvas.pad + x * (canvas.width  - 2 * canvas.pad)
 *     py = canvas.pad + y * (canvas.height - 2 * canvas.pad)
 *   Skipping this transform is the most common cause of a blank/black canvas.
 */

// ── Constants ───────────────────────────────────────────────────────────────

const SVG_NS = 'http://www.w3.org/2000/svg';

// ── Colour helpers ───────────────────────────────────────────────────────────

function securityColor(sec) {
    if (sec >= 0.5) return '#10b981';
    if (sec >  0.0) return '#f59e0b';
    return '#ef4444';
}

// Deterministic region colour palette (hue wheel)
const REGION_PALETTE = [
    '#6366f1','#8b5cf6','#a855f7','#ec4899','#f43f5e',
    '#f97316','#eab308','#84cc16','#22c55e','#14b8a6',
    '#06b6d4','#3b82f6','#60a5fa','#818cf8','#c084fc',
    '#e879f9','#fb7185','#fdba74','#fde047','#a3e635',
    '#4ade80','#34d399','#2dd4bf','#38bdf8','#93c5fd',
];
function regionColor(regionId) {
    return REGION_PALETTE[regionId % REGION_PALETTE.length];
}

// ── SVG helpers ──────────────────────────────────────────────────────────────

function el(tag, attrs) {
    const e = document.createElementNS(SVG_NS, tag);
    for (const [k, v] of Object.entries(attrs || {})) e.setAttribute(k, String(v));
    return e;
}

function elText(tag, attrs, text) {
    const e = el(tag, attrs);
    e.textContent = String(text);
    return e;
}

// ── Coordinate transform ─────────────────────────────────────────────────────

function makeTransform(canvas) {
    const { width: w, height: h, pad: p } = canvas;
    return {
        sx: (x) => p + x * (w - 2 * p),
        sy: (y) => p + y * (h - 2 * p),
    };
}

// ── Header bar ───────────────────────────────────────────────────────────────

function renderHeader(root, scene, colorBy) {
    const stats  = scene.build_stats || {};
    const layout = scene.layout || '';
    const isUni  = layout.startsWith('universe');
    const nodeCount = stats.node_count || Object.keys(scene.nodes || {}).length;
    const edgeCount = stats.edge_count || (scene.edges || []).length;

    const scopeLabel = isUni
        ? (layout.includes('dense') ? 'UNIVERSE' : 'UNIVERSE')
        : (scene.scope?.type || 'MAP').toUpperCase();

    const unitLabel  = isUni && !layout.includes('dense') ? 'REGIONS' : 'SYSTEMS';

    const bar = document.createElement('div');
    bar.className = 'aegis-map-header';
    bar.innerHTML =
        `<span class="aegis-map-scope">${scopeLabel}</span>` +
        `<span class="aegis-map-stat">${nodeCount.toLocaleString()} ${unitLabel}</span>` +
        `<span class="aegis-map-sep">·</span>` +
        `<span class="aegis-map-stat">${edgeCount.toLocaleString()} JUMPS</span>`;
    root.insertBefore(bar, root.firstChild);
}

// ── Universe-aggregated renderer (region nodes as circles) ──────────────────

function renderUniverseAggregated(svg, scene, t, prefix, labelMode, colorBy, highlights) {
    const nodes = scene.nodes || {};
    const edges = scene.edges || [];
    const highlightSet = new Set(highlights.map(String));

    // Edge layer
    const edgeG = el('g', { class: 'map-edges', opacity: '0.55' });
    for (const edge of edges) {
        const a = nodes[edge.from], b = nodes[edge.to];
        if (!a || !b) continue;
        edgeG.appendChild(el('line', {
            x1: t.sx(a.x), y1: t.sy(a.y),
            x2: t.sx(b.x), y2: t.sy(b.y),
            stroke: '#374151', 'stroke-width': 1.2,
        }));
    }
    svg.appendChild(edgeG);

    // Node layer
    const nodeG = el('g', { class: 'map-nodes' });
    const nodeList = Object.values(nodes);

    for (const node of nodeList) {
        const px   = t.sx(node.x);
        const py   = t.sy(node.y);
        const r    = Math.max(5, Math.min(12, 4 + Math.sqrt(node.system_count || 1) * 0.6));
        const col  = colorBy === 'region' ? regionColor(node.id) : securityColor(node.security);
        const isHi = highlightSet.has(String(node.id));

        const g = el('g', {
            class: 'map-node',
            'data-region-id': node.id,
            style: 'cursor:pointer',
        });

        // Glow ring for highlighted nodes
        if (isHi) {
            g.appendChild(el('circle', {
                cx: px, cy: py, r: r + 5,
                fill: 'none', stroke: '#facc15', 'stroke-width': 2,
                opacity: 0.7, filter: `url(#${prefix}-nglow)`,
            }));
        }

        // Main circle
        g.appendChild(el('circle', {
            cx: px, cy: py, r,
            fill: col, 'fill-opacity': 0.85,
            stroke: col, 'stroke-width': 1.5, 'stroke-opacity': 1.0,
            filter: `url(#${prefix}-nglow)`,
        }));

        // Label
        const showLabel = labelMode === 'always' ||
            (labelMode !== 'hidden' && isHi);
        if (showLabel) {
            g.appendChild(elText('text', {
                x: px,
                y: py + r + 10,
                'text-anchor': 'middle',
                style: 'font:500 9px Inter,sans-serif;fill:#cbd5e1;pointer-events:none',
            }, node.name));
        }

        // Tooltip
        const title = el('title');
        title.textContent = `${node.name}\nSystems: ${node.system_count ?? '?'}\nAvg security: ${node.security.toFixed(2)}`;
        g.appendChild(title);

        nodeG.appendChild(g);
    }
    svg.appendChild(nodeG);
}

// ── Dense / system-level universe renderer ───────────────────────────────────

function renderUniverseDense(svg, scene, t, prefix, labelMode, colorBy, highlights) {
    const nodes = scene.nodes || {};
    const edges = scene.edges || [];
    const highlightSet = new Set(highlights.map(String));

    // Edges (thin, no label)
    const edgeG = el('g', { class: 'map-edges', opacity: '0.4' });
    for (const edge of edges) {
        const a = nodes[edge.from], b = nodes[edge.to];
        if (!a || !b) continue;
        edgeG.appendChild(el('line', {
            x1: t.sx(a.x), y1: t.sy(a.y),
            x2: t.sx(b.x), y2: t.sy(b.y),
            stroke: '#1e293b', 'stroke-width': 0.7,
        }));
    }
    svg.appendChild(edgeG);

    // Nodes as tiny dots
    const nodeG = el('g', { class: 'map-nodes' });
    for (const node of Object.values(nodes)) {
        const px  = t.sx(node.x);
        const py  = t.sy(node.y);
        const col = colorBy === 'region' ? regionColor(node.constellation_id || node.id)
                                         : securityColor(node.security);
        const r   = highlightSet.has(String(node.id)) ? 3.5 : 1.8;

        const g = el('g', {
            class: 'map-node',
            'data-system-id': node.id,
            style: 'cursor:pointer',
        });
        g.appendChild(el('circle', {
            cx: px, cy: py, r,
            fill: col, 'fill-opacity': 0.9,
        }));

        if (labelMode === 'always') {
            g.appendChild(elText('text', {
                x: px, y: py - r - 1,
                'text-anchor': 'middle',
                style: 'font:400 6px Inter,sans-serif;fill:#94a3b8;pointer-events:none',
            }, node.name));
        }

        const title = el('title');
        title.textContent = `${node.name} (${node.security.toFixed(1)})`;
        g.appendChild(title);

        nodeG.appendChild(g);
    }
    svg.appendChild(nodeG);
}

// ── Generic system/region map renderer (radial, corridor, theater, region-v1) ─

function renderGenericScene(svg, scene, t, prefix, labelMode, colorBy, highlights) {
    const nodes      = scene.nodes || {};
    const edges      = scene.edges || [];
    const highlightSet = new Set(highlights.map(String));

    // Edge layers
    const edgeLayers = { boundary_stub: [], gate: [], route: [] };
    for (const e of edges) {
        (edgeLayers[e.tier] || edgeLayers.gate).push(e);
    }

    // Pass 0: boundary stubs
    for (const e of edgeLayers.boundary_stub) {
        const a = nodes[e.from], b = nodes[e.to];
        if (!a || !b) continue;
        const rendered  = a.role !== 'boundary' ? a : b;
        const boundary  = a.role !== 'boundary' ? b : a;
        const rx = t.sx(rendered.x), ry = t.sy(rendered.y);
        const bx = t.sx(boundary.x), by = t.sy(boundary.y);
        svg.appendChild(el('line', {
            x1: rx, y1: ry,
            x2: rx + (bx - rx) * 0.55, y2: ry + (by - ry) * 0.55,
            stroke: '#374151', 'stroke-opacity': 0.35, 'stroke-width': 1.2,
            'stroke-dasharray': '4 3',
        }));
    }

    // Pass 1: gate edges
    for (const e of edgeLayers.gate) {
        const a = nodes[e.from], b = nodes[e.to];
        if (!a || !b) continue;
        const focal = a.role === 'focal' || b.role === 'focal';
        svg.appendChild(el('line', {
            x1: t.sx(a.x), y1: t.sy(a.y),
            x2: t.sx(b.x), y2: t.sy(b.y),
            stroke: focal ? '#3b6db5' : '#374151',
            'stroke-opacity': focal ? 0.85 : 0.7,
            'stroke-width': focal ? 1.8 : 1.5,
        }));
    }

    // Pass 2: route edges
    for (const e of edgeLayers.route) {
        const a = nodes[e.from], b = nodes[e.to];
        if (!a || !b) continue;
        const x1 = t.sx(a.x), y1 = t.sy(a.y), x2 = t.sx(b.x), y2 = t.sy(b.y);
        svg.appendChild(el('line', {
            x1, y1, x2, y2,
            stroke: '#92400e', 'stroke-opacity': 0.45, 'stroke-width': 9,
            'stroke-linecap': 'round',
        }));
        svg.appendChild(el('line', {
            x1, y1, x2, y2,
            stroke: '#fbbf24', 'stroke-opacity': 0.88, 'stroke-width': 2.6,
            'stroke-linecap': 'round', filter: `url(#${prefix}-rglow)`,
        }));
    }

    // Pass 3: nodes
    const nodeList = Object.values(nodes).filter(n => n.role !== 'boundary');
    const roleOrder = { surrounding: 0, route: 1, anchor: 2, focal: 3 };
    nodeList.sort((a, b) => (roleOrder[a.role] || 0) - (roleOrder[b.role] || 0));

    const nodeG = el('g', { class: 'map-nodes' });
    for (const node of nodeList) {
        const px     = t.sx(node.x);
        const py     = t.sy(node.y);
        const tl     = node.threat_level || '';
        const hasThr = tl !== '';
        const secCol = securityColor(node.security);
        const isHi   = highlightSet.has(String(node.id));

        // Pill sizing
        const len  = node.name.length;
        let pw, ph;
        switch (node.role) {
            case 'focal':  pw = Math.max(90, len * 8.2 + 30); ph = hasThr ? 42 : 28; break;
            case 'anchor': pw = Math.max(82, len * 7.8 + 28); ph = hasThr ? 38 : 26; break;
            case 'route':  pw = Math.max(70, len * 7.2 + 22); ph = 34; break;
            default:       pw = Math.max(70, len * 7.2 + 22); ph = hasThr ? 34 : 24;
        }
        const rx = Math.floor(ph / 2);

        let fill, stroke, sw, filterAttr;
        switch (node.role) {
            case 'focal':  fill = '#0f172a'; stroke = secCol;    sw = 2.5; filterAttr = `${prefix}-fglow`; break;
            case 'anchor': fill = '#1a1207'; stroke = '#fbbf24'; sw = 2.2; filterAttr = `${prefix}-rglow`; break;
            case 'route':  fill = '#111827'; stroke = '#fbbf24'; sw = 1.5; filterAttr = null; break;
            default:       fill = '#111827'; stroke = colorBy === 'region' ? regionColor(node.constellation_id || node.id) : secCol;
                           sw = 1.8; filterAttr = `${prefix}-nglow`;
        }
        if (isHi && node.role === 'surrounding') {
            stroke = '#facc15'; sw = 2.2;
        }

        const g = el('g', { class: 'map-node', 'data-system-id': node.id, style: 'cursor:pointer' });
        if (filterAttr) g.setAttribute('filter', `url(#${filterAttr})`);

        const rect = el('rect', {
            x: px - pw / 2, y: py - ph / 2,
            width: pw, height: ph, rx,
            fill, stroke, 'stroke-width': sw, 'stroke-opacity': 0.9,
        });
        if (node.role === 'route') {
            rect.setAttribute('stroke-dasharray', '5 3');
            rect.setAttribute('stroke-opacity', '0.8');
        }
        g.appendChild(rect);

        // Name label
        const labelY  = hasThr ? py - 3 : py + 4;
        const labelSt = node.role === 'focal'  ? 'font:700 12px Inter,sans-serif;fill:#f1f5f9'  :
                        node.role === 'anchor' ? 'font:700 11px Inter,sans-serif;fill:#fef3c7'  :
                                                 'font:500 9.5px Inter,sans-serif;fill:#cbd5e1';
        g.appendChild(elText('text', { x: px, y: labelY, 'text-anchor': 'middle', style: labelSt }, node.name));

        // Sub-label
        if (hasThr) {
            g.appendChild(elText('text', {
                x: px, y: py + 12, 'text-anchor': 'middle',
                style: 'font:600 8px Inter,sans-serif;letter-spacing:.04em',
                fill: node.role === 'anchor' ? '#92400e' : '#ef4444',
            }, tl.toUpperCase()));
        } else {
            g.appendChild(elText('text', {
                x: px, y: py + (ph > 30 ? 11 : 4), 'text-anchor': 'middle',
                style: 'font:600 8px Inter,sans-serif;letter-spacing:.04em',
                fill: secCol,
            }, node.security.toFixed(1)));
        }

        const title = el('title');
        title.textContent = node.name + ' | sec=' + node.security.toFixed(1) + (tl ? ' | threat=' + tl : '');
        g.appendChild(title);

        nodeG.appendChild(g);
    }
    svg.appendChild(nodeG);
}

// ── SVG defs (glow filters) ──────────────────────────────────────────────────

function buildDefs(prefix) {
    const defs = el('defs');
    defs.innerHTML =
        `<filter id="${prefix}-fglow" x="-100%" y="-100%" width="300%" height="300%">` +
        `<feGaussianBlur stdDeviation="5" result="blur"/>` +
        `<feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge></filter>` +

        `<filter id="${prefix}-nglow" x="-60%" y="-60%" width="220%" height="220%">` +
        `<feGaussianBlur stdDeviation="2.2" result="blur"/>` +
        `<feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge></filter>` +

        `<filter id="${prefix}-rglow" x="-40%" y="-40%" width="180%" height="180%">` +
        `<feGaussianBlur stdDeviation="3.5" result="blur"/>` +
        `<feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge></filter>`;
    return defs;
}

// ── Zoom & pan ───────────────────────────────────────────────────────────────

function attachZoomPan(svg, canvas) {
    let vb   = { x: 0, y: 0, w: canvas.width, h: canvas.height };
    let pan  = false;
    let ps   = { x: 0, y: 0 };
    let pvs  = { x: 0, y: 0 };

    function applyVb() {
        svg.setAttribute('viewBox',
            `${vb.x.toFixed(1)} ${vb.y.toFixed(1)} ${vb.w.toFixed(1)} ${vb.h.toFixed(1)}`);
    }

    svg.addEventListener('wheel', (e) => {
        e.preventDefault();
        const r  = svg.getBoundingClientRect();
        const mx = (e.clientX - r.left) / r.width;
        const my = (e.clientY - r.top)  / r.height;
        const f  = e.deltaY > 0 ? 1.15 : 0.87;
        const nw = Math.min(canvas.width * 3,  Math.max(canvas.width  * 0.15, vb.w * f));
        const nh = Math.min(canvas.height * 3, Math.max(canvas.height * 0.15, vb.h * f));
        vb.x += (vb.w - nw) * mx;
        vb.y += (vb.h - nh) * my;
        vb.w  = nw;
        vb.h  = nh;
        applyVb();
    }, { passive: false });

    svg.addEventListener('mousedown', (e) => {
        if (e.button !== 0) return;
        pan = true;
        ps  = { x: e.clientX, y: e.clientY };
        pvs = { x: vb.x, y: vb.y };
        svg.style.cursor = 'grabbing';
    });

    window.addEventListener('mousemove', (e) => {
        if (!pan) return;
        const r  = svg.getBoundingClientRect();
        const dx = (e.clientX - ps.x) / r.width  * vb.w;
        const dy = (e.clientY - ps.y) / r.height * vb.h;
        vb.x = pvs.x - dx;
        vb.y = pvs.y - dy;
        applyVb();
    });

    window.addEventListener('mouseup', () => {
        if (pan) { pan = false; svg.style.cursor = 'grab'; }
    });
}

// ── Hover tooltip ────────────────────────────────────────────────────────────

function attachTooltip(svg, container, nodes) {
    const tip = document.createElement('div');
    tip.className = 'aegis-map-tooltip';
    tip.style.cssText =
        'position:absolute;display:none;pointer-events:none;z-index:50;' +
        'background:#0f172a;border:1px solid rgba(255,255,255,0.12);border-radius:8px;' +
        'padding:6px 10px;font-size:11px;color:#e2e8f0;box-shadow:0 4px 12px rgba(0,0,0,.5);' +
        'white-space:nowrap';
    container.appendChild(tip);

    const nodeAttr = 'data-system-id';
    const nodeAttr2 = 'data-region-id';

    svg.addEventListener('mouseover', (e) => {
        const n = e.target.closest('.map-node');
        if (!n) { tip.style.display = 'none'; return; }
        const id   = n.getAttribute(nodeAttr) || n.getAttribute(nodeAttr2);
        const data = nodes[id];
        if (!data) return;
        const count = data.system_count != null
            ? `<br>Systems: ${data.system_count}` : '';
        tip.innerHTML =
            `<strong>${data.name}</strong>` +
            `<br>Security: <span style="color:${securityColor(data.security)}">${data.security.toFixed(2)}</span>` +
            count +
            (data.threat_level ? `<br>Threat: ${data.threat_level}` : '');
        tip.style.display = 'block';
    });

    svg.addEventListener('mousemove', (e) => {
        if (tip.style.display !== 'block') return;
        const r = container.getBoundingClientRect();
        tip.style.left = (e.clientX - r.left + 14) + 'px';
        tip.style.top  = (e.clientY - r.top  - 10) + 'px';
    });

    svg.addEventListener('mouseout', (e) => {
        if (!e.target.closest('.map-node')) tip.style.display = 'none';
    });
}

// ── Main entry point ─────────────────────────────────────────────────────────

/**
 * Mount the map renderer into the given root element.
 * @param {HTMLElement} root  Element carrying data-* configuration attributes.
 */
export function mountMapRenderer(root) {
    const url        = root.dataset.url        || '';
    const labelMode  = root.dataset.labelMode  || 'hover';
    const colorBy    = root.dataset.colorBy    || 'security';
    const interactive = root.dataset.interactive !== 'false';
    let   highlights = [];
    try { highlights = JSON.parse(root.dataset.highlights || '[]'); } catch (_) {}
    const instanceId = root.dataset.instanceId || ('map_' + Math.random().toString(36).slice(2, 9));

    if (!url) {
        root.textContent = 'Map error: no data-url specified.';
        return;
    }

    // Loading state
    root.innerHTML = '<div class="aegis-map-loading">Loading map data…</div>';

    fetch(url)
        .then((r) => {
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            return r.json();
        })
        .then((scene) => {
            if (scene.error) throw new Error(scene.error);

            const canvas = scene.canvas || { width: 1200, height: 800, pad: 40 };
            const layout = scene.layout || '';
            const prefix = scene.filter_prefix || instanceId;
            const t      = makeTransform(canvas);

            // Build SVG
            const svg = el('svg', {
                viewBox: `0 0 ${canvas.width} ${canvas.height}`,
                style: 'width:100%;height:auto;cursor:grab;user-select:none;display:block',
                role: 'img',
                'aria-label': 'AegisCore Universe Map',
            });
            svg.appendChild(buildDefs(prefix));

            // Dark background
            svg.appendChild(el('rect', {
                width: canvas.width, height: canvas.height, fill: '#04080f',
            }));

            // Delegate to the appropriate renderer
            if (layout === 'universe-aggregated-v1') {
                renderUniverseAggregated(svg, scene, t, prefix, labelMode, colorBy, highlights);
            } else if (layout === 'universe-dense-v1') {
                renderUniverseDense(svg, scene, t, prefix, labelMode, colorBy, highlights);
            } else {
                renderGenericScene(svg, scene, t, prefix, labelMode, colorBy, highlights);
            }

            // Mount into root
            root.style.position = 'relative';
            root.innerHTML = '';
            root.appendChild(svg);

            // Header bar
            renderHeader(root, scene, colorBy);

            // Interactive features
            if (interactive) {
                attachZoomPan(svg, canvas);
                attachTooltip(svg, root, scene.nodes || {});

                // Click navigation (system maps only)
                if (!layout.startsWith('universe')) {
                    svg.addEventListener('click', (e) => {
                        const n = e.target.closest('.map-node');
                        if (!n) return;
                        const sid = n.dataset.systemId;
                        if (sid) window.location.href = '/system-map?system_id=' + sid;
                    });
                }
            }
        })
        .catch((err) => {
            root.innerHTML =
                `<div class="aegis-map-error">Map unavailable: ${err.message}</div>`;
        });
}
