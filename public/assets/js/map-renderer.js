/**
 * SupplyCore Map Renderer — vanilla JS interactive SVG map.
 *
 * Consumes a MapScene JSON from /api/map-graph.php and renders an inline SVG
 * with zoom (scroll), pan (drag), hover tooltips, click navigation, and
 * progressive label disclosure.
 *
 * Usage:
 *   <div id="map-container" data-map-type="system" data-map-system-id="30002537" data-map-hops="2"></div>
 *   <script src="/assets/js/map-renderer.js"></script>
 */
(function () {
    'use strict';

    // ── Color helpers ────────────────────────────────────────────────────
    function securityColor(sec) {
        if (sec >= 0.5) return '#10b981';
        if (sec > 0.0)  return '#f59e0b';
        return '#ef4444';
    }

    function threatColor(tl) {
        switch (tl) {
            case 'critical': return '#ef4444';
            case 'high':     return '#f97316';
            case 'medium':   return '#eab308';
            case 'low':      return '#3b82f6';
            default:         return '#94a3b8';
        }
    }

    // ── SVG element helpers ──────────────────────────────────────────────
    const SVG_NS = 'http://www.w3.org/2000/svg';

    function svgEl(tag, attrs) {
        const el = document.createElementNS(SVG_NS, tag);
        for (const [k, v] of Object.entries(attrs || {})) {
            el.setAttribute(k, String(v));
        }
        return el;
    }

    function svgText(tag, attrs, text) {
        const el = svgEl(tag, attrs);
        el.textContent = text;
        return el;
    }

    // ── Coordinate transform ─────────────────────────────────────────────
    function makeTransform(canvas) {
        const w = canvas.width, h = canvas.height, p = canvas.pad;
        return {
            sx: (x) => p + x * (w - p * 2),
            sy: (y) => p + y * (h - p * 2),
        };
    }

    // ── Pill sizing ──────────────────────────────────────────────────────
    function pillSize(name, role, hasThreat) {
        const len = name.length;
        switch (role) {
            case 'focal':
                return { w: Math.max(90, len * 8.2 + 30), h: hasThreat ? 42 : 28 };
            case 'anchor':
                return { w: Math.max(82, len * 7.8 + 28), h: hasThreat ? 38 : 26 };
            case 'route':
                return { w: Math.max(70, len * 7.2 + 22), h: 34 };
            default:
                return { w: Math.max(70, len * 7.2 + 22), h: hasThreat ? 34 : 24 };
        }
    }

    // ── Label visibility by zoom ─────────────────────────────────────────
    function shouldShowLabel(node, zoomLevel) {
        if (node.role === 'focal' || node.role === 'anchor') return true;
        if (node.role === 'boundary') return false;
        if (zoomLevel >= 1.5) return true;
        if (node.role === 'route' && zoomLevel >= 1.0) return true;
        const priority = node.label_priority || 0;
        return priority > (1.0 - zoomLevel * 0.8);
    }

    // ── Render scene to SVG ──────────────────────────────────────────────
    function renderScene(scene, container) {
        const canvas = scene.canvas;
        const t = makeTransform(canvas);
        const prefix = scene.filter_prefix || 'map';
        const nodes = scene.nodes || {};
        const edges = scene.edges || [];

        const svg = svgEl('svg', {
            viewBox: '0 0 ' + canvas.width + ' ' + canvas.height,
            style: 'width:100%;height:auto;cursor:grab;user-select:none',
            role: 'img',
            'aria-label': 'SupplyCore Map',
        });

        // Defs
        const defs = svgEl('defs');
        defs.innerHTML =
            '<filter id="' + prefix + '-fglow" x="-100%" y="-100%" width="300%" height="300%">' +
            '<feGaussianBlur stdDeviation="5" result="blur"/>' +
            '<feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge>' +
            '</filter>' +
            '<filter id="' + prefix + '-nglow" x="-60%" y="-60%" width="220%" height="220%">' +
            '<feGaussianBlur stdDeviation="2.2" result="blur"/>' +
            '<feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge>' +
            '</filter>' +
            '<filter id="' + prefix + '-rglow" x="-40%" y="-40%" width="180%" height="180%">' +
            '<feGaussianBlur stdDeviation="3.5" result="blur"/>' +
            '<feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge>' +
            '</filter>';
        svg.appendChild(defs);

        // Background
        svg.appendChild(svgEl('rect', { width: canvas.width, height: canvas.height, fill: '#04080f' }));

        // Edge layers
        const edgeLayers = { boundary_stub: [], gate: [], route: [] };
        for (const edge of edges) {
            const tier = edge.tier || 'gate';
            (edgeLayers[tier] || edgeLayers.gate).push(edge);
        }

        // Pass 0: boundary stubs
        for (const e of edgeLayers.boundary_stub) {
            const a = nodes[e.from], b = nodes[e.to];
            if (!a || !b) continue;
            const rendered = a.role !== 'boundary' ? a : b;
            const boundary = a.role !== 'boundary' ? b : a;
            const rx = t.sx(rendered.x), ry = t.sy(rendered.y);
            const bx = t.sx(boundary.x), by = t.sy(boundary.y);
            svg.appendChild(svgEl('line', {
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
            const isFocal = a.role === 'focal' || b.role === 'focal';
            svg.appendChild(svgEl('line', {
                x1: t.sx(a.x), y1: t.sy(a.y),
                x2: t.sx(b.x), y2: t.sy(b.y),
                stroke: isFocal ? '#3b6db5' : '#374151',
                'stroke-opacity': isFocal ? 0.85 : 0.7,
                'stroke-width': isFocal ? 1.8 : 1.5,
            }));
        }

        // Pass 2: route edges (golden glow)
        for (const e of edgeLayers.route) {
            const a = nodes[e.from], b = nodes[e.to];
            if (!a || !b) continue;
            const x1 = t.sx(a.x), y1 = t.sy(a.y), x2 = t.sx(b.x), y2 = t.sy(b.y);
            // Wide ambient glow
            svg.appendChild(svgEl('line', {
                x1, y1, x2, y2,
                stroke: '#92400e', 'stroke-opacity': 0.45, 'stroke-width': 9,
                'stroke-linecap': 'round',
            }));
            // Bright core
            svg.appendChild(svgEl('line', {
                x1, y1, x2, y2,
                stroke: '#fbbf24', 'stroke-opacity': 0.88, 'stroke-width': 2.6,
                'stroke-linecap': 'round', filter: 'url(#' + prefix + '-rglow)',
            }));
        }

        // Pass 3: nodes
        const nodeGroup = svgEl('g', { class: 'map-nodes' });
        const nodeEntries = Object.values(nodes).filter(n => n.role !== 'boundary');

        // Sort: surrounding first, then route, anchor, focal (focal on top)
        const roleOrder = { surrounding: 0, route: 1, anchor: 2, focal: 3 };
        nodeEntries.sort((a, b) => (roleOrder[a.role] || 0) - (roleOrder[b.role] || 0));

        for (const node of nodeEntries) {
            const px = t.sx(node.x), py = t.sy(node.y);
            const tl = node.threat_level || '';
            const hasThreat = tl !== '';
            const pill = pillSize(node.name, node.role, hasThreat);
            const rx = Math.floor(pill.h / 2);
            const secCol = securityColor(node.security);

            const g = svgEl('g', {
                class: 'map-node',
                'data-system-id': node.id,
                style: 'cursor:pointer',
            });

            let fillColor, strokeColor, strokeWidth, filterAttr;
            switch (node.role) {
                case 'focal':
                    fillColor = '#0f172a'; strokeColor = secCol; strokeWidth = 2.5;
                    filterAttr = prefix + '-fglow';
                    break;
                case 'anchor':
                    fillColor = '#1a1207'; strokeColor = '#fbbf24'; strokeWidth = 2.2;
                    filterAttr = prefix + '-rglow';
                    break;
                case 'route':
                    fillColor = '#111827'; strokeColor = '#fbbf24'; strokeWidth = 1.5;
                    filterAttr = null;
                    break;
                default:
                    fillColor = '#111827'; strokeColor = secCol; strokeWidth = 1.8;
                    filterAttr = prefix + '-nglow';
            }

            if (filterAttr) g.setAttribute('filter', 'url(#' + filterAttr + ')');

            const rect = svgEl('rect', {
                x: px - pill.w / 2, y: py - pill.h / 2,
                width: pill.w, height: pill.h, rx,
                fill: fillColor, stroke: strokeColor,
                'stroke-width': strokeWidth, 'stroke-opacity': 0.9,
            });
            if (node.role === 'route') {
                rect.setAttribute('stroke-dasharray', '5 3');
                rect.setAttribute('stroke-opacity', '0.8');
            }
            g.appendChild(rect);

            // Name label
            const labelY = hasThreat ? py - 3 : py + 4;
            const labelClass = node.role === 'focal' ? 'font:700 12px Inter,sans-serif;fill:#f1f5f9' :
                node.role === 'anchor' ? 'font:700 11px Inter,sans-serif;fill:#fef3c7' :
                    'font:500 9.5px Inter,sans-serif;fill:#cbd5e1';
            g.appendChild(svgText('text', {
                x: px, y: labelY, 'text-anchor': 'middle', style: labelClass,
            }, node.name));

            // Security/threat sub-label
            if (hasThreat) {
                g.appendChild(svgText('text', {
                    x: px, y: py + 12, 'text-anchor': 'middle',
                    style: 'font:600 8px Inter,sans-serif;letter-spacing:.04em',
                    fill: node.role === 'anchor' ? '#92400e' : threatColor(tl),
                }, tl.toUpperCase()));
            } else {
                const secFmt = node.security.toFixed(1);
                g.appendChild(svgText('text', {
                    x: px, y: py + (pill.h > 30 ? 11 : 4), 'text-anchor': 'middle',
                    style: 'font:600 8px Inter,sans-serif;letter-spacing:.04em',
                    fill: secCol,
                }, secFmt));
            }

            // Tooltip title
            const title = svgEl('title');
            title.textContent = node.name + ' | sec=' + node.security.toFixed(1) +
                (tl ? ' | threat=' + tl : '');
            g.appendChild(title);

            nodeGroup.appendChild(g);
        }
        svg.appendChild(nodeGroup);

        // ── Tooltip ──────────────────────────────────────────────────────
        const tooltip = document.createElement('div');
        tooltip.className = 'map-tooltip';
        tooltip.style.cssText = 'position:absolute;display:none;pointer-events:none;z-index:50;' +
            'background:#0f172a;border:1px solid rgba(255,255,255,0.1);border-radius:8px;' +
            'padding:6px 10px;font-size:11px;color:#e2e8f0;box-shadow:0 4px 12px rgba(0,0,0,0.5);' +
            'white-space:nowrap';

        // ── Zoom & Pan state ─────────────────────────────────────────────
        let viewBox = { x: 0, y: 0, w: canvas.width, h: canvas.height };
        let isPanning = false;
        let panStart = { x: 0, y: 0 };
        let panViewStart = { x: 0, y: 0 };

        function updateViewBox() {
            svg.setAttribute('viewBox',
                viewBox.x.toFixed(1) + ' ' + viewBox.y.toFixed(1) + ' ' +
                viewBox.w.toFixed(1) + ' ' + viewBox.h.toFixed(1));
        }

        // Zoom
        svg.addEventListener('wheel', function (e) {
            e.preventDefault();
            const rect = svg.getBoundingClientRect();
            const mx = (e.clientX - rect.left) / rect.width;
            const my = (e.clientY - rect.top) / rect.height;

            const factor = e.deltaY > 0 ? 1.15 : 0.87;
            const newW = Math.min(canvas.width * 3, Math.max(canvas.width * 0.15, viewBox.w * factor));
            const newH = Math.min(canvas.height * 3, Math.max(canvas.height * 0.15, viewBox.h * factor));

            viewBox.x += (viewBox.w - newW) * mx;
            viewBox.y += (viewBox.h - newH) * my;
            viewBox.w = newW;
            viewBox.h = newH;
            updateViewBox();
        }, { passive: false });

        // Pan
        svg.addEventListener('mousedown', function (e) {
            if (e.button !== 0) return;
            isPanning = true;
            panStart = { x: e.clientX, y: e.clientY };
            panViewStart = { x: viewBox.x, y: viewBox.y };
            svg.style.cursor = 'grabbing';
        });

        window.addEventListener('mousemove', function (e) {
            if (!isPanning) return;
            const rect = svg.getBoundingClientRect();
            const dx = (e.clientX - panStart.x) / rect.width * viewBox.w;
            const dy = (e.clientY - panStart.y) / rect.height * viewBox.h;
            viewBox.x = panViewStart.x - dx;
            viewBox.y = panViewStart.y - dy;
            updateViewBox();
        });

        window.addEventListener('mouseup', function () {
            if (isPanning) {
                isPanning = false;
                svg.style.cursor = 'grab';
            }
        });

        // Hover tooltip
        svg.addEventListener('mouseover', function (e) {
            const node = e.target.closest('.map-node');
            if (!node) { tooltip.style.display = 'none'; return; }
            const sysId = node.dataset.systemId;
            const data = nodes[sysId];
            if (!data) return;
            tooltip.innerHTML =
                '<strong>' + data.name + '</strong>' +
                '<br>Security: <span style="color:' + securityColor(data.security) + '">' + data.security.toFixed(1) + '</span>' +
                (data.threat_level ? '<br>Threat: <span style="color:' + threatColor(data.threat_level) + '">' + data.threat_level + '</span>' : '');
            tooltip.style.display = 'block';
        });

        svg.addEventListener('mousemove', function (e) {
            if (tooltip.style.display === 'block') {
                const rect = container.getBoundingClientRect();
                tooltip.style.left = (e.clientX - rect.left + 12) + 'px';
                tooltip.style.top = (e.clientY - rect.top - 10) + 'px';
            }
        });

        svg.addEventListener('mouseout', function (e) {
            if (!e.target.closest('.map-node')) tooltip.style.display = 'none';
        });

        // Click navigation
        svg.addEventListener('click', function (e) {
            if (isPanning) return;
            const node = e.target.closest('.map-node');
            if (!node) return;
            const sysId = node.dataset.systemId;
            if (sysId) {
                window.location.href = '/system-map?system_id=' + sysId;
            }
        });

        // Mount
        container.style.position = 'relative';
        container.innerHTML = '';
        container.appendChild(svg);
        container.appendChild(tooltip);

        return svg;
    }

    // ── Auto-init from data attributes ───────────────────────────────────
    function initMapContainers() {
        const containers = document.querySelectorAll('[data-map-type]');
        containers.forEach(function (container) {
            const type = container.dataset.mapType;
            const params = new URLSearchParams();
            params.set('type', type);

            if (type === 'system') {
                params.set('system_id', container.dataset.mapSystemId || '0');
                params.set('hops', container.dataset.mapHops || '2');
            } else if (type === 'corridor') {
                params.set('corridor_id', container.dataset.mapCorridorId || '0');
                params.set('system_ids', container.dataset.mapSystemIds || '');
                params.set('hops', container.dataset.mapHops || '1');
            } else if (type === 'theater') {
                params.set('theater_id', container.dataset.mapTheaterId || '');
                params.set('system_ids', container.dataset.mapSystemIds || '');
                params.set('hops', container.dataset.mapHops || '1');
            }

            fetch('/api/map-graph.php?' + params.toString())
                .then(function (r) { return r.json(); })
                .then(function (scene) {
                    if (scene.error) {
                        container.innerHTML = '<p class="text-muted text-center py-6">Map unavailable.</p>';
                        return;
                    }
                    renderScene(scene, container);
                })
                .catch(function () {
                    container.innerHTML = '<p class="text-muted text-center py-6">Failed to load map data.</p>';
                });
        });
    }

    // Expose for programmatic use
    window.SupplyCoreMap = { renderScene: renderScene, initMapContainers: initMapContainers };

    // Auto-init on DOM ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initMapContainers);
    } else {
        initMapContainers();
    }
})();
