(function () {
  const root = document.querySelector('[data-live-refresh-config]');
  if (!root) {
    return;
  }

  let config;
  try {
    config = JSON.parse(root.getAttribute('data-live-refresh-config') || '{}');
  } catch (error) {
    console.warn('Live refresh config parse failed.', error);
    return;
  }

  if (!config || !config.page_id) {
    return;
  }

  const state = {
    currentVersions: config.current_versions || {},
    lastPublishedEvent: config.latest_event || null,
    lastSectionRefreshAt: {},
    transport: 'sse',
    pendingVersionKeys: new Set(),
    refreshTimer: null,
    pollingTimer: null,
    autoRefreshTimer: null,
    stream: null,
  };

  const diagnostics = {
    mode: document.querySelector('[data-live-refresh-mode]'),
    health: document.querySelector('[data-live-refresh-health]'),
    healthBadge: document.querySelector('[data-live-refresh-health-badge]'),
    advanced: document.querySelector('[data-live-refresh-advanced]'),
    transport: document.querySelector('[data-live-refresh-transport]'),
    event: document.querySelector('[data-live-refresh-last-event]'),
    refresh: document.querySelector('[data-live-refresh-last-refresh]'),
    versions: document.querySelector('[data-live-refresh-versions]'),
  };

  const pageSections = config.sections || {};
  const debounceMs = Number(config.debounce_ms || 1200);
  const pollIntervalMs = Number(config.poll_interval_ms || 30000);
  const autoRefreshMs = Number(config.auto_refresh_ms || 60000);

  function relativeTime(isoValue) {
    if (!isoValue) {
      return 'Never';
    }

    const target = new Date(isoValue);
    if (Number.isNaN(target.getTime())) {
      return 'Unknown';
    }

    const deltaSeconds = Math.max(0, Math.round((Date.now() - target.getTime()) / 1000));
    if (deltaSeconds <= 10) {
      return 'just now';
    }
    if (deltaSeconds < 60) {
      return `${deltaSeconds}s ago`;
    }
    if (deltaSeconds < 3600) {
      return `${Math.round(deltaSeconds / 60)}m ago`;
    }
    return `${Math.round(deltaSeconds / 3600)}h ago`;
  }

  function flashSection(sectionKey) {
    const element = document.querySelector(`[data-ui-section="${sectionKey}"]`);
    if (!element) {
      return;
    }

    element.classList.remove('ui-refresh-flash');
    void element.offsetWidth;
    element.classList.add('ui-refresh-flash');
  }

  function updateFreshnessBadges(sectionKey) {
    document.querySelectorAll(`[data-ui-freshness-target~="${sectionKey}"]`).forEach((node) => {
      node.textContent = 'Updated just now';
      node.classList.add('text-emerald-200');
    });
  }

  function renderDiagnostics() {
    const refreshTimes = Object.values(state.lastSectionRefreshAt);
    const latestSectionRefresh = refreshTimes.sort().slice(-1)[0] || null;
    const lastRefreshTime = latestSectionRefresh || state.lastPublishedEvent?.finished_at || null;
    const lastRefreshRelative = relativeTime(lastRefreshTime);
    let healthState = 'degraded';

    if (lastRefreshTime) {
      const deltaMinutes = Math.max(0, Math.round((Date.now() - new Date(lastRefreshTime).getTime()) / 60000));
      if (deltaMinutes <= 15) {
        healthState = 'fresh';
      } else if (deltaMinutes <= 45) {
        healthState = 'updating';
      } else if (deltaMinutes <= 120) {
        healthState = 'degraded';
      } else {
        healthState = 'stale';
      }
    }

    const modeLabel = state.transport === 'sse'
      ? 'On'
      : state.transport === 'polling'
        ? 'Polling fallback'
        : 'Off';
    const healthLabel = healthState === 'fresh'
      ? 'Live updates are healthy.'
      : healthState === 'updating'
        ? 'A newer refresh is still landing.'
        : healthState === 'stale'
          ? 'Refresh health is stale.'
          : 'Refresh health is degraded.';
    const healthBadge = healthState === 'fresh'
      ? 'Fresh'
      : healthState === 'updating'
        ? 'Updating'
        : healthState === 'stale'
          ? 'Stale'
          : 'Delayed';

    if (diagnostics.mode) {
      diagnostics.mode.textContent = modeLabel;
    }

    if (diagnostics.health) {
      diagnostics.health.textContent = healthLabel;
    }

    if (diagnostics.healthBadge) {
      diagnostics.healthBadge.textContent = healthBadge;
      diagnostics.healthBadge.className = `badge ${
        healthState === 'fresh'
          ? 'border-emerald-400/20 bg-emerald-500/10 text-emerald-100'
          : healthState === 'updating'
            ? 'border-sky-400/20 bg-sky-500/10 text-sky-100'
            : healthState === 'stale'
              ? 'border-rose-400/20 bg-rose-500/10 text-rose-100'
              : 'border-amber-400/20 bg-amber-500/10 text-amber-100'
      }`;
    }

    if (diagnostics.advanced && (healthState === 'degraded' || healthState === 'stale')) {
      diagnostics.advanced.open = true;
    }

    if (diagnostics.transport) {
      diagnostics.transport.textContent = state.transport === 'sse'
        ? 'Server-sent events'
        : state.transport === 'polling'
          ? 'Polling fallback'
          : 'Off';
    }

    if (diagnostics.event) {
      diagnostics.event.textContent = state.lastPublishedEvent
        ? `${state.lastPublishedEvent.job_name} · ${state.lastPublishedEvent.state} · ${relativeTime(state.lastPublishedEvent.finished_at)}`
        : 'No published refresh event yet';
    }

    if (diagnostics.refresh) {
      diagnostics.refresh.textContent = lastRefreshTime ? `Last updated ${lastRefreshRelative}` : 'Never';
    }

    if (diagnostics.versions) {
      const labels = Object.entries(state.currentVersions)
        .map(([key, value]) => `${key}:${value.version}`)
        .join(' · ');
      diagnostics.versions.textContent = labels || 'No version markers available';
    }
  }

  function sectionsForVersionKeys(versionKeys) {
    return Object.entries(pageSections)
      .filter(([, sectionConfig]) => {
        const subscribed = sectionConfig.version_keys || [];
        return subscribed.some((key) => versionKeys.has(key));
      })
      .map(([sectionKey]) => sectionKey);
  }

  function updateVersionState(nextVersions) {
    Object.entries(nextVersions || {}).forEach(([versionKey, versionValue]) => {
      state.currentVersions[versionKey] = versionValue;
    });
  }

  /**
   * Swap a section's DOM with a crossfade using the View Transitions API.
   * Falls back to an instant swap + CSS flash for older browsers.
   */
  function transitionSwap(current, replacement) {
    if (document.startViewTransition) {
      current.style.viewTransitionName = 'ui-section-' + (current.dataset.uiSection || 'default');
      replacement.style.viewTransitionName = 'ui-section-' + (current.dataset.uiSection || 'default');
      document.startViewTransition(() => {
        current.replaceWith(replacement);
      });
    } else {
      current.replaceWith(replacement);
    }
  }

  async function refreshSection(sectionKey) {
    const params = new URLSearchParams({
      section: sectionKey,
      page_query: window.location.search.replace(/^\?/, ''),
    });

    const response = await fetch(`${config.fragment_url}&${params.toString()}`, {
      headers: { Accept: 'application/json' },
      credentials: 'same-origin',
      cache: 'no-store',
    });

    if (!response.ok) {
      throw new Error(`Fragment refresh failed for ${sectionKey}`);
    }

    const payload = await response.json();
    const current = document.querySelector(`[data-ui-section="${sectionKey}"]`);
    if (!current || typeof payload.html !== 'string' || payload.html.trim() === '') {
      return;
    }

    const template = document.createElement('template');
    template.innerHTML = payload.html.trim();
    const replacement = template.content.firstElementChild;
    if (!replacement) {
      return;
    }

    transitionSwap(current, replacement);
    state.lastSectionRefreshAt[sectionKey] = new Date().toISOString();
    flashSection(sectionKey);
    updateFreshnessBadges(sectionKey);
    updateVersionState(payload.current_versions || {});
    renderDiagnostics();
  }

  async function flushPendingRefreshes() {
    if (state.pendingVersionKeys.size === 0) {
      return;
    }

    const pending = new Set(state.pendingVersionKeys);
    state.pendingVersionKeys.clear();
    const targets = sectionsForVersionKeys(pending);
    for (const sectionKey of targets) {
      try {
        await refreshSection(sectionKey);
      } catch (error) {
        console.warn('Live refresh section update failed.', sectionKey, error);
      }
    }
    renderDiagnostics();
  }

  function scheduleRefresh(versionKeys) {
    versionKeys.forEach((key) => state.pendingVersionKeys.add(key));
    if (state.refreshTimer) {
      window.clearTimeout(state.refreshTimer);
    }
    state.refreshTimer = window.setTimeout(flushPendingRefreshes, debounceMs);
  }

  function applyEvent(eventPayload) {
    if (!eventPayload || !eventPayload.changed_versions) {
      return;
    }

    state.lastPublishedEvent = eventPayload;
    updateVersionState(eventPayload.freshness_versions || {});
    const changedVersionKeys = Object.keys(eventPayload.changed_versions || {}).filter((versionKey) => {
      const nextVersion = Number(eventPayload.changed_versions[versionKey]?.version || 0);
      const currentVersion = Number(state.currentVersions[versionKey]?.version || 0);
      return nextVersion >= currentVersion;
    });

    scheduleRefresh(changedVersionKeys);
    renderDiagnostics();
  }

  async function pollState() {
    try {
      const response = await fetch(config.poll_url, {
        headers: { Accept: 'application/json' },
        credentials: 'same-origin',
        cache: 'no-store',
      });
      if (!response.ok) {
        throw new Error('Polling failed');
      }

      const payload = await response.json();
      state.transport = 'polling';
      const changed = [];
      Object.entries(payload.current_versions || {}).forEach(([versionKey, value]) => {
        const previous = Number(state.currentVersions[versionKey]?.version || 0);
        const next = Number(value?.version || 0);
        if (next > previous) {
          changed.push(versionKey);
        }
      });
      updateVersionState(payload.current_versions || {});
      state.lastPublishedEvent = payload.last_published_event || state.lastPublishedEvent;
      if (changed.length > 0) {
        scheduleRefresh(changed);
      }
      renderDiagnostics();
    } catch (error) {
      state.transport = 'off';
      renderDiagnostics();
      console.warn('Live refresh polling failed.', error);
    }
  }

  function startPolling() {
    state.transport = 'polling';
    renderDiagnostics();
    if (state.pollingTimer) {
      window.clearInterval(state.pollingTimer);
    }
    state.pollingTimer = window.setInterval(pollState, pollIntervalMs);
    pollState();
  }

  function startStream() {
    if (!window.EventSource) {
      startPolling();
      return;
    }

    try {
      state.stream = new EventSource(config.stream_url, { withCredentials: true });
      state.stream.addEventListener('init', (event) => {
        const payload = JSON.parse(event.data || '{}');
        state.transport = 'sse';
        updateVersionState(payload.current_versions || {});
        state.lastPublishedEvent = payload.last_published_event || state.lastPublishedEvent;
        renderDiagnostics();
      });
      state.stream.addEventListener('ui-refresh', (event) => {
        const payload = JSON.parse(event.data || '{}');
        state.transport = 'sse';
        applyEvent(payload);
      });
      state.stream.onerror = function () {
        if (state.stream) {
          state.stream.close();
          state.stream = null;
        }
        startPolling();
      };
    } catch (error) {
      console.warn('Live refresh stream failed to initialize.', error);
      startPolling();
    }
  }

  /**
   * Periodic auto-refresh: unconditionally refreshes all page sections on an
   * interval so data stays current even when the backend hasn't published a
   * version bump (e.g. relative timestamps, freshness badges).
   * Only runs while the tab is visible.
   */
  function refreshAllSections() {
    if (document.hidden) {
      return;
    }

    const allSections = Object.keys(pageSections);
    (async () => {
      for (const sectionKey of allSections) {
        try {
          await refreshSection(sectionKey);
        } catch (error) {
          console.warn('Auto-refresh failed for section.', sectionKey, error);
        }
      }
    })();
  }

  function startAutoRefresh() {
    if (state.autoRefreshTimer) {
      window.clearInterval(state.autoRefreshTimer);
    }
    state.autoRefreshTimer = window.setInterval(refreshAllSections, autoRefreshMs);
  }

  // Pause/resume auto-refresh when tab visibility changes
  document.addEventListener('visibilitychange', () => {
    if (!document.hidden) {
      refreshAllSections();
    }
  });

  renderDiagnostics();
  startStream();
  startAutoRefresh();
})();
