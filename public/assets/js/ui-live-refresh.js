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
    stream: null,
  };

  const diagnostics = {
    transport: document.querySelector('[data-live-refresh-transport]'),
    event: document.querySelector('[data-live-refresh-last-event]'),
    refresh: document.querySelector('[data-live-refresh-last-refresh]'),
    versions: document.querySelector('[data-live-refresh-versions]'),
  };

  const pageSections = config.sections || {};
  const debounceMs = Number(config.debounce_ms || 1200);
  const pollIntervalMs = Number(config.poll_interval_ms || 30000);

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
    if (diagnostics.transport) {
      diagnostics.transport.textContent = state.transport === 'sse' ? 'Live stream' : 'Polling fallback';
    }

    if (diagnostics.event) {
      diagnostics.event.textContent = state.lastPublishedEvent
        ? `${state.lastPublishedEvent.job_name} · ${state.lastPublishedEvent.state} · ${relativeTime(state.lastPublishedEvent.finished_at)}`
        : 'No published refresh event yet';
    }

    if (diagnostics.refresh) {
      const refreshTimes = Object.values(state.lastSectionRefreshAt);
      const latest = refreshTimes.sort().slice(-1)[0] || null;
      diagnostics.refresh.textContent = latest ? `Last section refresh ${relativeTime(latest)}` : 'No section refreshed yet';
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

    current.replaceWith(replacement);
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

  renderDiagnostics();
  startStream();
})();
