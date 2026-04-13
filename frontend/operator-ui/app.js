(() => {
  const API = {
    runtimeStatus: "/health/runtime",
    sources: "/v1/sources",
    runs: "/v1/extraction-runs",
    researchRuns: "/v1/research/runs",
    researchRun: (runId) => `/v1/research/runs/${runId}`,
    stageResearchRun: (runId) => `/v1/research/runs/${runId}/stage`,
    extractResearchRun: (runId) => `/v1/research/runs/${runId}/extract`,
    researchPrograms: "/v1/research/programs",
    pullSources: "/v1/ingest/zotero/pull",
    extractCandidates: "/v1/ingest/extract-candidates",
    candidates: "/v1/candidates",
    reviewCandidate: (candidateId) => `/v1/candidates/${candidateId}/review`,
    claims: "/v1/claims",
    query: "/v1/query",
  };

  const seed = window.SOURCEBOUND_SEED_DATA;
  const knownScreens = new Set(["sources", "research", "runs", "review", "claims", "ask"]);
  const state = {
    apiBase: normalizeBase(window.SOURCEBOUND_API_BASE || ""),
    activeScreen: currentScreen(),
    apiOnline: false,
    lastSync: "Idle",
    banner: null,
    loading: false,
    runtimeStatus: null,
    sources: clone(seed.sources),
    evidence: clone(seed.evidence),
    candidates: clone(seed.candidates),
    claims: clone(seed.claims),
    runs: clone(seed.extractionRuns),
    researchRuns: clone(seed.researchRuns || []),
    researchPrograms: clone(seed.researchPrograms || []),
    selectedSourceId: seed.sources[0]?.source_id ?? null,
    selectedRunId: seed.extractionRuns[0]?.run_id ?? null,
    selectedResearchRunId: seed.researchRuns?.[0]?.run_id ?? null,
    researchRunDetail: null,
    selectedCandidateId: seed.candidates.find((candidate) => candidate.review_state === "pending")
      ?.candidate_id ?? seed.candidates[0]?.candidate_id ?? null,
    selectedClaimId: seed.claims[0]?.claim_id ?? null,
    filters: {
      candidates: "pending",
      claims: "all",
      researchDecision: "all",
      researchReason: "all",
      researchSourceType: "all",
      researchFacet: "all",
      researchProvider: "all",
      researchQueryProfile: "all",
      researchSemantic: "all",
      researchSort: "accepted_first",
    },
    query: {
      question: seed.queryPresets[0],
      mode: "strict_facts",
      status: "",
      claimKind: "",
      place: "",
      viewpoint: "",
    },
    researchDraft: {
      topic: "",
      focal_year: "",
      time_start: "",
      time_end: "",
      locale: "",
      audience: "",
      adapter_id: "web_open",
      domain_hints: "",
      desired_facets: "",
      preferred_source_types: "",
      excluded_source_types: "",
      coverage_targets: "",
      curated_title: "",
      curated_text: "",
      curated_url: "",
      max_queries: "12",
      max_results_per_query: "5",
      max_findings: "20",
      max_per_facet: "2",
      total_fetch_time_seconds: "90",
      per_host_fetch_cap: "3",
      retry_attempts: "3",
      retry_backoff_base_ms: "250",
      retry_backoff_max_ms: "2000",
      allow_domains: "",
      deny_domains: "",
      respect_robots: true,
      program_id: "default",
    },
    queryResult: null,
  };

  const nodes = {
    app: document.getElementById("app"),
    banner: document.getElementById("banner"),
    title: document.getElementById("screen-title"),
    summary: document.getElementById("screen-summary"),
    apiBase: document.getElementById("api-base"),
    lastSync: document.getElementById("last-sync"),
    modeBadge: document.getElementById("mode-badge"),
    connectionSummary: document.getElementById("connection-summary"),
    refreshAll: document.getElementById("refresh-all"),
    metricSources: document.getElementById("metric-sources"),
    metricPending: document.getElementById("metric-pending"),
    metricClaims: document.getElementById("metric-claims"),
    metricEvidence: document.getElementById("metric-evidence"),
    runtimePanel: document.getElementById("runtime-panel"),
    nav: Array.from(document.querySelectorAll("[data-nav]")),
  };

  const screenConfig = {
    sources: {
      title: "Sources",
      summary:
        "Pulled sources feed extraction and review. This view mirrors the live source catalog and the normalized text feeding extraction.",
    },
    runs: {
      title: "Extraction Runs",
      summary:
        "Track intake cycles, candidate yield, and backend-reported extraction history.",
    },
    research: {
      title: "Research",
      summary:
        "Scout broad subjects and eras into staged, provenance-rich excerpts before normalization and candidate extraction.",
    },
    review: {
      title: "Review Queue",
      summary:
        "Approve, reject, or override extracted candidates before they become canonical claims.",
    },
    claims: {
      title: "Claims",
      summary:
        "Inspect approved claims and their evidence links as the canonical output of the review gate.",
    },
    ask: {
      title: "Ask",
      summary:
        "Query approved claims with explicit mode controls and surfaced provenance, not hidden retrieval magic.",
    },
  };

  boot().catch((error) => {
    console.error(error);
    setBanner("failed", "Startup failed", error.message || "The operator console could not initialize.");
  });

  async function boot() {
    bindGlobalEvents();
    hydrateFromStorage();
    await refreshLiveData({ quiet: true });
    if (state.activeScreen === "research" && state.selectedResearchRunId) {
      await refreshResearchDetail(state.selectedResearchRunId, { quiet: true });
    }
    render();
  }

  function bindGlobalEvents() {
    window.addEventListener("hashchange", () => {
      state.activeScreen = currentScreen();
      if (
        state.activeScreen === "research" &&
        state.selectedResearchRunId &&
        state.researchRunDetail?.run?.run_id !== state.selectedResearchRunId
      ) {
        render();
        refreshResearchDetail(state.selectedResearchRunId, { quiet: true });
        return;
      }
      render();
    });

    nodes.refreshAll.addEventListener("click", () => refreshLiveData());

    nodes.app.addEventListener("click", async (event) => {
      const sourceButton = event.target.closest("[data-select-source]");
      const runButton = event.target.closest("[data-select-run]");
      const researchRunButton = event.target.closest("[data-select-research-run]");
      const candidateButton = event.target.closest("[data-select-candidate]");
      const claimButton = event.target.closest("[data-select-claim]");
      const actionButton = event.target.closest("[data-action]");
      const presetButton = event.target.closest("[data-preset]");
      const filterButton = event.target.closest("[data-filter]");
      const queryModeButton = event.target.closest("[data-query-mode]");

      if (sourceButton) {
        state.selectedSourceId = sourceButton.dataset.selectSource;
        render();
        return;
      }

      if (runButton) {
        state.selectedRunId = runButton.dataset.selectRun;
        render();
        return;
      }

      if (researchRunButton) {
        state.selectedResearchRunId = researchRunButton.dataset.selectResearchRun;
        state.researchRunDetail = null;
        render();
        await refreshResearchDetail(state.selectedResearchRunId, { quiet: true });
        return;
      }

      if (candidateButton) {
        state.selectedCandidateId = candidateButton.dataset.selectCandidate;
        render();
        return;
      }

      if (claimButton) {
        state.selectedClaimId = claimButton.dataset.selectClaim;
        render();
        return;
      }

      if (presetButton) {
        state.query.question = presetButton.dataset.preset;
        state.activeScreen = "ask";
        location.hash = "#ask";
        render();
        return;
      }

      if (filterButton) {
        const { filterGroup, filterValue } = filterButton.dataset;
        state.filters[filterGroup] = filterValue;
        render();
        return;
      }

      if (queryModeButton) {
        state.query.mode = queryModeButton.dataset.queryMode;
        render();
        return;
      }

      if (!actionButton) {
        return;
      }

      const action = actionButton.dataset.action;

      if (action === "pull-sources") {
        await pullSources();
        return;
      }

      if (action === "run-extraction") {
        await runExtraction();
        return;
      }

      if (action === "refresh-research") {
        await refreshResearchDetail(state.selectedResearchRunId);
        return;
      }

      if (action === "stage-research") {
        await stageResearchRun(state.selectedResearchRunId);
        return;
      }

      if (action === "extract-research") {
        await extractResearchRun(state.selectedResearchRunId);
        return;
      }

      // Submit actions are handled by the form submit listener to avoid duplicate posts.
    });

    nodes.app.addEventListener("submit", async (event) => {
      if (event.target.dataset.form === "review") {
        event.preventDefault();
        await submitReview(event.target);
      }

      if (event.target.dataset.form === "query") {
        event.preventDefault();
        await submitQuery(event.target);
      }

      if (event.target.dataset.form === "research-run") {
        event.preventDefault();
        await submitResearchRun(event.target);
      }
    });

    nodes.app.addEventListener("change", (event) => {
      const researchFilter = event.target.closest("[data-research-filter]");
      if (!researchFilter) {
        return;
      }
      state.filters[researchFilter.dataset.researchFilter] = researchFilter.value;
      persistState();
      render();
    });
  }

  function hydrateFromStorage() {
    try {
      const raw = localStorage.getItem("sourcebound.operator.state");
      if (!raw) {
        return;
      }
      const saved = JSON.parse(raw);
      if (saved.query) {
        state.query = { ...state.query, ...saved.query };
      }
      if (saved.filters) {
        state.filters = { ...state.filters, ...saved.filters };
      }
      if (Array.isArray(saved.runs) && saved.runs.length) {
        state.runs = saved.runs;
      }
      if (saved.researchDraft) {
        state.researchDraft = { ...state.researchDraft, ...saved.researchDraft };
      }
      if (saved.selectedResearchRunId) {
        state.selectedResearchRunId = saved.selectedResearchRunId;
      }
    } catch (error) {
      console.warn("Could not hydrate operator state", error);
    }
  }

  function persistState() {
    try {
      localStorage.setItem(
        "sourcebound.operator.state",
        JSON.stringify({
          query: state.query,
          filters: state.filters,
          runs: state.runs,
          researchDraft: state.researchDraft,
          selectedResearchRunId: state.selectedResearchRunId,
        })
      );
    } catch (error) {
      console.warn("Could not persist operator state", error);
    }
  }

  function currentScreen() {
    const hash = window.location.hash.replace("#", "").trim();
    return knownScreens.has(hash) ? hash : "sources";
  }

  function normalizeBase(base) {
    if (!base) return "";
    return base.endsWith("/") ? base.slice(0, -1) : base;
  }

  function clone(value) {
    return JSON.parse(JSON.stringify(value));
  }

  function escapeHtml(value) {
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function formatDate(value) {
    if (!value || value === "seeded" || value === "awaiting pull") {
      return value || "n/a";
    }
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
      return value;
    }
    return new Intl.DateTimeFormat(undefined, {
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
    }).format(date);
  }

  function countByStatus(items, key) {
    return items.reduce((accumulator, item) => {
      const value = item[key];
      accumulator[value] = (accumulator[value] || 0) + 1;
      return accumulator;
    }, {});
  }

  function applyLoading(isLoading) {
    state.loading = isLoading;
    document.body.classList.toggle("is-loading", isLoading);
  }

  function setBanner(kind, title, message) {
    state.banner = { kind, title, message };
    renderBanner();
  }

  function renderBanner() {
    if (!state.banner) {
      nodes.banner.innerHTML = "";
      return;
    }

    nodes.banner.innerHTML = `
      <div class="banner-card ${escapeHtml(state.banner.kind)}">
        <div class="banner-text">
          <strong>${escapeHtml(state.banner.title)}</strong>
          <span>${escapeHtml(state.banner.message)}</span>
        </div>
        <div class="pill ${escapeHtml(state.banner.kind)}">${escapeHtml(state.banner.kind)}</div>
      </div>
    `;
  }

  function setApiStatus(online, message) {
    state.apiOnline = online;
    const runtime = state.runtimeStatus;
    let badgeClass = "status-pill status-pill-muted";
    let badgeLabel = online ? "Live API" : "Seed fallback";

    if (online && runtime?.overall_status === "ready") {
      badgeClass = "status-pill status-pill-live";
      badgeLabel = "Runtime ready";
    } else if (online && runtime) {
      badgeClass = "status-pill status-pill-warning";
      badgeLabel = "Needs setup";
    }

    nodes.modeBadge.className = badgeClass;
    nodes.modeBadge.textContent = badgeLabel;
    nodes.connectionSummary.textContent = message;
    nodes.apiBase.textContent = state.apiBase || "Same origin";
  }

  async function refreshLiveData({ quiet = false } = {}) {
    applyLoading(true);
    if (!quiet) {
      setBanner("pending", "Refreshing", "Pulling live sources, research runs, extraction runs, candidates, and claims from the API.");
    }

    try {
      const [sourcesResult, researchRunsResult, researchProgramsResult, runsResult, candidatesResult, claimsResult, runtimeResult] =
        await Promise.allSettled([
          fetchJson(API.sources),
          fetchJson(API.researchRuns),
          fetchJson(API.researchPrograms),
          fetchJson(API.runs),
          fetchJson(API.candidates),
          fetchJson(API.claims),
          fetchJson(API.runtimeStatus),
        ]);

      if (sourcesResult.status === "fulfilled") {
        state.sources = sourcesResult.value;
      }

      if (runsResult.status === "fulfilled") {
        state.runs = runsResult.value;
      }

      if (researchRunsResult.status === "fulfilled") {
        state.researchRuns = researchRunsResult.value;
      }

      if (researchProgramsResult.status === "fulfilled") {
        state.researchPrograms = researchProgramsResult.value;
      }

      if (candidatesResult.status === "fulfilled") {
        state.candidates = candidatesResult.value;
      }

      if (claimsResult.status === "fulfilled") {
        state.claims = claimsResult.value;
      }

      if (runtimeResult?.status === "fulfilled") {
        state.runtimeStatus = runtimeResult.value;
      }

      const online = [sourcesResult, researchRunsResult, researchProgramsResult, runsResult, candidatesResult, claimsResult, runtimeResult].some(
        (result) => result.status === "fulfilled"
      );
      const runtimeSummary = summarizeRuntime();
      setApiStatus(
        online,
        online
          ? runtimeSummary || "Live sources, research runs, extraction runs, candidates, claims, and runtime status loaded."
          : "Using seed data until the API responds."
      );
      updateMetrics();
      updateSelectionFallbacks();
      state.lastSync = new Date().toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
      nodes.lastSync.textContent = state.lastSync;
      if (!quiet) {
        setBanner(
          online ? "live" : "queued",
          online ? "Live data synced" : "Seed data retained",
          online
            ? "Sources, research runs, extraction runs, candidates, and claims refreshed from the backend."
            : "The UI stayed operational with local seed records."
        );
      }
    } catch (error) {
      setApiStatus(false, "Using seed data. Live refresh failed.");
      setBanner("failed", "Refresh failed", error.message || "Could not refresh live data.");
    } finally {
      applyLoading(false);
      render();
      persistState();
    }
  }

  function updateSelectionFallbacks() {
    if (!state.selectedSourceId && state.sources.length) {
      state.selectedSourceId = state.sources[0].source_id;
    }

    if (
      state.selectedResearchRunId &&
      !state.researchRuns.some((run) => run.run_id === state.selectedResearchRunId)
    ) {
      state.selectedResearchRunId = null;
      state.researchRunDetail = null;
    }

    if (!state.selectedResearchRunId && state.researchRuns.length) {
      state.selectedResearchRunId = state.researchRuns[0].run_id;
    }

    if (!state.selectedRunId && state.runs.length) {
      state.selectedRunId = state.runs[0].run_id;
    }

    if (!state.selectedCandidateId && state.candidates.length) {
      state.selectedCandidateId = state.candidates.find((candidate) => candidate.review_state === "pending")
        ?.candidate_id ?? state.candidates[0].candidate_id;
    }

    if (!state.selectedClaimId && state.claims.length) {
      state.selectedClaimId = state.claims[0].claim_id;
    }
  }

  function updateMetrics() {
    const pendingCount = state.candidates.filter((candidate) => candidate.review_state === "pending").length;
    nodes.metricSources.textContent = String(state.sources.length);
    nodes.metricPending.textContent = String(pendingCount);
    nodes.metricClaims.textContent = String(state.claims.length);
    nodes.metricEvidence.textContent = String(state.evidence.length);
  }

  function render() {
    updateNavigation();
    updateHeader();
    updateMetrics();
    renderRuntimeOverview();
    renderBanner();
    nodes.app.innerHTML = renderScreen();
    wireScreenTitle();
  }

  function renderRuntimeOverview() {
    if (!state.runtimeStatus) {
      nodes.runtimePanel.innerHTML = `
        <div class="runtime-shell">
          <div class="runtime-summary">
            <p class="runtime-kicker">Runtime overview</p>
            <h2>Waiting for backend status</h2>
            <p>The operator console is still usable with seed data, but the runtime report has not loaded yet.</p>
          </div>
          <div class="helper">The API can expose a live readiness report at <code>/health/runtime</code>.</div>
        </div>
      `;
      return;
    }

    const readyCount = state.runtimeStatus.services.filter((service) => service.ready).length;
    const overallTone = state.runtimeStatus.overall_status === "ready" ? "live" : "queued";
    const summaryLine = summarizeRuntime() || "Runtime details are available.";

    nodes.runtimePanel.innerHTML = `
      <div class="runtime-shell">
        <div class="runtime-summary">
          <p class="runtime-kicker">Runtime overview</p>
          <h2>${escapeHtml(titleize(state.runtimeStatus.overall_status.replaceAll("_", " ")))}</h2>
          <p>${escapeHtml(summaryLine)}</p>
          <div class="service-meta">
            <span class="pill ${escapeHtml(overallTone)}">${readyCount}/${state.runtimeStatus.services.length} services ready</span>
            <span class="pill ${escapeHtml(state.runtimeStatus.state_backend)}">${escapeHtml(state.runtimeStatus.state_backend)} state</span>
            <span class="pill ${escapeHtml(state.runtimeStatus.truth_backend)}">${escapeHtml(state.runtimeStatus.truth_backend)} truth</span>
            <span class="pill ${escapeHtml(state.runtimeStatus.extraction_backend)}">${escapeHtml(state.runtimeStatus.extraction_backend)}</span>
          </div>
          ${
            state.runtimeStatus.next_steps.length
              ? `
                <div class="field">
                  <label>Next steps</label>
                  <ol class="next-step-list">
                    ${state.runtimeStatus.next_steps
                      .map((step) => `<li>${escapeHtml(step)}</li>`)
                      .join("")}
                  </ol>
                </div>
              `
              : "<div class='helper'>No follow-up steps are reported.</div>"
          }
        </div>
        <div class="service-grid">
          ${state.runtimeStatus.services
            .map((service) => {
              const tone = service.ready ? "live" : service.configured ? "failed" : "queued";
              return `
                <article class="service-card">
                  <div class="service-head">
                    <div>
                      <strong>${escapeHtml(titleize(service.name.replaceAll("_", " ")))}</strong>
                      <div class="detail-note">${escapeHtml(service.role)}</div>
                    </div>
                    <span class="pill ${escapeHtml(tone)}">${service.ready ? "ready" : "attention"}</span>
                  </div>
                  <div class="service-meta">
                    <span class="code">${escapeHtml(service.mode)}</span>
                    <span class="detail-note">configured: ${service.configured ? "yes" : "no"}</span>
                    <span class="detail-note">reachable: ${renderReachability(service.reachable)}</span>
                  </div>
                  <div>${escapeHtml(service.detail)}</div>
                </article>
              `;
            })
            .join("")}
        </div>
      </div>
    `;
  }

  function updateNavigation() {
    nodes.nav.forEach((item) => {
      const active = item.dataset.nav === state.activeScreen;
      if (active) {
        item.setAttribute("aria-current", "page");
      } else {
        item.removeAttribute("aria-current");
      }
    });
  }

  function updateHeader() {
    const config = screenConfig[state.activeScreen];
    nodes.title.textContent = config.title;
    nodes.summary.textContent = config.summary;
  }

  function wireScreenTitle() {
    const title = document.querySelector("[data-active-screen]");
    if (title) {
      title.textContent = screenConfig[state.activeScreen].title;
    }
  }

  function renderScreen() {
    switch (state.activeScreen) {
      case "research":
        return renderResearchScreen();
      case "runs":
        return renderRunsScreen();
      case "review":
        return renderReviewScreen();
      case "claims":
        return renderClaimsScreen();
      case "ask":
        return renderAskScreen();
      case "sources":
      default:
        return renderSourcesScreen();
    }
  }

  function renderSourcesScreen() {
    const selected = state.sources.find((source) => source.source_id === state.selectedSourceId) ?? state.sources[0];
    const linkedEvidence = state.evidence.filter((snippet) => snippet.source_id === selected?.source_id);
    const linkedCandidates = state.candidates.filter((candidate) =>
      candidate.evidence_ids.some((evidenceId) => linkedEvidence.some((snippet) => snippet.evidence_id === evidenceId))
    );

    return `
      <article class="screen fade-in">
        <div class="screen-head">
          <div>
            <h2 data-active-screen>Sources</h2>
            <p>Source intake is the operator starting point. Pull from Zotero when you want live source records; use the seed list when the backend is still in file-backed mode.</p>
          </div>
          <div class="screen-actions">
            <button class="secondary-button" type="button" data-action="pull-sources">Pull Zotero sources</button>
          </div>
        </div>

        <div class="split">
          <div class="surface list">
            ${state.sources
              .map(
                (source) => `
                  <button class="list-row ${source.source_id === selected?.source_id ? "is-selected" : ""}" type="button" data-select-source="${escapeHtml(source.source_id)}">
                    <div>
                      <div class="row-title">${escapeHtml(source.title)}</div>
                      <div class="row-subtitle">${escapeHtml(source.author || "Unknown author")} · ${escapeHtml(source.year || "n.d.")}</div>
                    </div>
                    <div class="row-meta">
                      <span class="pill ${escapeHtml(source.source_type)}">${escapeHtml(source.source_type)}</span>
                      <span class="code">${escapeHtml(source.source_id)}</span>
                    </div>
                    <div class="row-meta">${escapeHtml(source.locator_hint || "No locator hint")}</div>
                    <div class="row-meta">${escapeHtml((state.evidence.filter((e) => e.source_id === source.source_id).length || 0) + " evidence")}</div>
                  </button>
                `
              )
              .join("")}
          </div>

          <aside class="detail">
            ${selected ? renderSourceDetail(selected, linkedEvidence, linkedCandidates) : "<div class='helper'>No sources are available.</div>"}
          </aside>
        </div>
      </article>
    `;
  }

  function renderSourceDetail(source, linkedEvidence, linkedCandidates) {
    return `
      <div class="detail-head">
        <div>
          <h3>${escapeHtml(source.title)}</h3>
          <div class="detail-note">${escapeHtml(source.author || "Unknown author")} · ${escapeHtml(source.year || "n.d.")}</div>
        </div>
        <span class="pill ${escapeHtml(source.source_type)}">${escapeHtml(source.source_type)}</span>
      </div>
      <div class="detail-grid">
        <div class="field">
          <label>Source ID</label>
          <div class="code">${escapeHtml(source.source_id)}</div>
        </div>
        <div class="field">
          <label>Locator hint</label>
          <div>${escapeHtml(source.locator_hint || "n/a")}</div>
        </div>
        <div class="field">
          <label>Zotero item key</label>
          <div>${escapeHtml(source.zotero_item_key || "not yet linked")}</div>
        </div>
        <div class="field">
          <label>Evidence count</label>
          <div>${linkedEvidence.length}</div>
        </div>
      </div>
      <div class="detail-stack">
        <div class="field">
          <label>Linked evidence</label>
          <div class="detail-list">
            ${linkedEvidence.length
              ? linkedEvidence
                  .map(
                    (snippet) => `
                      <div class="mini">
                        <div class="code">${escapeHtml(snippet.evidence_id)} · ${escapeHtml(snippet.locator)}</div>
                        <div>${escapeHtml(snippet.text)}</div>
                        <div class="detail-note">${escapeHtml(snippet.notes || "No note")}</div>
                      </div>
                    `
                  )
                  .join("")
              : "<div class='helper'>No evidence snippets are linked to this source yet.</div>"}
          </div>
        </div>
        <div class="field">
          <label>Linked candidates</label>
          <div class="detail-list">
            ${linkedCandidates.length
              ? linkedCandidates
                  .map(
                    (candidate) => `
                      <div class="mini">
                        <div class="code">${escapeHtml(candidate.candidate_id)}</div>
                        <div>${escapeHtml(candidate.subject)} — ${escapeHtml(candidate.predicate)} — ${escapeHtml(candidate.value)}</div>
                        <div class="detail-note">${escapeHtml(candidate.review_state)} · ${escapeHtml(candidate.status_suggestion)}</div>
                      </div>
                    `
                  )
                  .join("")
              : "<div class='helper'>No candidates reference evidence from this source yet.</div>"}
          </div>
        </div>
      </div>
    `;
  }

  function renderResearchScreen() {
    const selectedRun = state.researchRuns.find((run) => run.run_id === state.selectedResearchRunId) ?? state.researchRuns[0];
    const selectedDetail = state.researchRunDetail?.run?.run_id === selectedRun?.run_id ? state.researchRunDetail : null;
    const availablePrograms = state.researchPrograms.length
      ? state.researchPrograms
      : [{ program_id: "default", name: "Generic Subject / Era Research", built_in: true }];
    const selectedProgramId = state.researchDraft.program_id || "default";

    return `
      <article class="screen fade-in">
        <div class="screen-head">
          <div>
            <h2 data-active-screen>Research</h2>
            <p>Start a bounded scout run from a brief, then stage accepted findings as text-backed sources before normal extraction and review.</p>
          </div>
          <div class="screen-actions">
            <button class="secondary-button" type="button" data-action="refresh-research">Refresh selected run</button>
          </div>
        </div>

        <div class="split">
          <div class="detail-stack">
            <form class="detail" data-form="research-run">
              <div class="detail-head">
                <div>
                  <h3>New research run</h3>
                  <div class="detail-note">Generic by default: topic, era, optional locale, and a reusable program.</div>
                </div>
                <span class="pill queued">brief</span>
              </div>
              <div class="detail-grid">
                <div class="field">
                  <label for="research-topic">Topic</label>
                  <input id="research-topic" name="topic" value="${escapeHtml(state.researchDraft.topic)}" placeholder="2003 DJ and music scene" required />
                </div>
                <div class="field">
                  <label for="research-program">Research program</label>
                  <select id="research-program" name="program_id">
                    ${availablePrograms
                      .map(
                        (program) => `
                          <option value="${escapeHtml(program.program_id)}" ${program.program_id === selectedProgramId ? "selected" : ""}>
                            ${escapeHtml(program.name)}${program.built_in ? " (built-in)" : ""}
                          </option>
                        `
                      )
                      .join("")}
                  </select>
                </div>
                <div class="field">
                  <label for="research-adapter">Scout adapter</label>
                  <select id="research-adapter" name="adapter_id">
                    <option value="web_open" ${state.researchDraft.adapter_id === "web_open" ? "selected" : ""}>web_open</option>
                    <option value="curated_inputs" ${state.researchDraft.adapter_id === "curated_inputs" ? "selected" : ""}>curated_inputs</option>
                  </select>
                </div>
                <div class="field">
                  <label for="research-focal-year">Focal year</label>
                  <input id="research-focal-year" name="focal_year" value="${escapeHtml(state.researchDraft.focal_year)}" placeholder="2003" />
                </div>
                <div class="field">
                  <label for="research-locale">Locale</label>
                  <input id="research-locale" name="locale" value="${escapeHtml(state.researchDraft.locale)}" placeholder="Berlin, UK garage clubs, Bay Area" />
                </div>
                <div class="field">
                  <label for="research-time-start">Time start</label>
                  <input id="research-time-start" name="time_start" value="${escapeHtml(state.researchDraft.time_start)}" placeholder="2002-01-01" />
                </div>
                <div class="field">
                  <label for="research-time-end">Time end</label>
                  <input id="research-time-end" name="time_end" value="${escapeHtml(state.researchDraft.time_end)}" placeholder="2004-12-31" />
                </div>
                <div class="field">
                  <label for="research-audience">Audience / lens</label>
                  <input id="research-audience" name="audience" value="${escapeHtml(state.researchDraft.audience)}" placeholder="clubgoers, record collectors, city historians" />
                </div>
                <div class="field">
                  <label for="research-domain-hints">Domain hints</label>
                  <input id="research-domain-hints" name="domain_hints" value="${escapeHtml(state.researchDraft.domain_hints)}" placeholder="vinyl, mixtapes, nightlife" />
                </div>
                <div class="field">
                  <label for="research-desired-facets">Desired facets</label>
                  <input id="research-desired-facets" name="desired_facets" value="${escapeHtml(state.researchDraft.desired_facets)}" placeholder="people, practices, objects_technology, media_culture" />
                </div>
                <div class="field">
                  <label for="research-preferred-source-types">Preferred source types</label>
                  <input id="research-preferred-source-types" name="preferred_source_types" value="${escapeHtml(state.researchDraft.preferred_source_types)}" placeholder="news, archive, educational" />
                </div>
                <div class="field">
                  <label for="research-excluded-source-types">Excluded source types</label>
                  <input id="research-excluded-source-types" name="excluded_source_types" value="${escapeHtml(state.researchDraft.excluded_source_types)}" placeholder="social, forum" />
                </div>
                <div class="field">
                  <label for="research-coverage-targets">Coverage targets</label>
                  <input id="research-coverage-targets" name="coverage_targets" value="${escapeHtml(state.researchDraft.coverage_targets)}" placeholder="people:2, events:2, objects_technology:3" />
                </div>
                <div class="field">
                  <label for="research-max-queries">Max queries</label>
                  <input id="research-max-queries" name="max_queries" type="number" min="1" value="${escapeHtml(state.researchDraft.max_queries)}" />
                </div>
                <div class="field">
                  <label for="research-max-results-per-query">Results / query</label>
                  <input id="research-max-results-per-query" name="max_results_per_query" type="number" min="1" value="${escapeHtml(state.researchDraft.max_results_per_query)}" />
                </div>
                <div class="field">
                  <label for="research-max-findings">Max findings</label>
                  <input id="research-max-findings" name="max_findings" type="number" min="1" value="${escapeHtml(state.researchDraft.max_findings)}" />
                </div>
                <div class="field">
                  <label for="research-max-per-facet">Max / facet</label>
                  <input id="research-max-per-facet" name="max_per_facet" type="number" min="1" value="${escapeHtml(state.researchDraft.max_per_facet)}" />
                </div>
                <div class="field">
                  <label for="research-total-fetch-time">Fetch budget (s)</label>
                  <input id="research-total-fetch-time" name="total_fetch_time_seconds" type="number" min="1" value="${escapeHtml(state.researchDraft.total_fetch_time_seconds)}" />
                </div>
                <div class="field">
                  <label for="research-per-host-fetch-cap">Per-host cap</label>
                  <input id="research-per-host-fetch-cap" name="per_host_fetch_cap" type="number" min="1" value="${escapeHtml(state.researchDraft.per_host_fetch_cap)}" />
                </div>
                <div class="field">
                  <label for="research-retry-attempts">Retry attempts</label>
                  <input id="research-retry-attempts" name="retry_attempts" type="number" min="1" value="${escapeHtml(state.researchDraft.retry_attempts)}" />
                </div>
                <div class="field">
                  <label for="research-retry-backoff-base-ms">Backoff base (ms)</label>
                  <input id="research-retry-backoff-base-ms" name="retry_backoff_base_ms" type="number" min="1" value="${escapeHtml(state.researchDraft.retry_backoff_base_ms)}" />
                </div>
                <div class="field">
                  <label for="research-retry-backoff-max-ms">Backoff max (ms)</label>
                  <input id="research-retry-backoff-max-ms" name="retry_backoff_max_ms" type="number" min="1" value="${escapeHtml(state.researchDraft.retry_backoff_max_ms)}" />
                </div>
                <div class="field">
                  <label for="research-allow-domains">Allow domains</label>
                  <input id="research-allow-domains" name="allow_domains" value="${escapeHtml(state.researchDraft.allow_domains)}" placeholder="archive.example.org, bbc.co.uk" />
                </div>
                <div class="field">
                  <label for="research-deny-domains">Deny domains</label>
                  <input id="research-deny-domains" name="deny_domains" value="${escapeHtml(state.researchDraft.deny_domains)}" placeholder="social.example.org, blocked.example.org" />
                </div>
                <div class="field">
                  <label for="research-curated-title">Curated title</label>
                  <input id="research-curated-title" name="curated_title" value="${escapeHtml(state.researchDraft.curated_title)}" placeholder="Optional text input title" />
                </div>
              <div class="field">
                <label for="research-curated-url">Curated URL</label>
                <input id="research-curated-url" name="curated_url" value="${escapeHtml(state.researchDraft.curated_url)}" placeholder="Optional URL input for no-search curated mode" />
              </div>
            </div>
              <div class="field">
                <label for="research-curated-text">Curated text</label>
                <textarea id="research-curated-text" name="curated_text" placeholder="Optional pasted evidence for curated_inputs. Text stays local evidence; curated URLs may still fetch if policy allows it.">${escapeHtml(state.researchDraft.curated_text)}</textarea>
              </div>
              <div class="toolbar">
                <label class="chip">
                  <input type="checkbox" name="respect_robots" ${state.researchDraft.respect_robots ? "checked" : ""} />
                  <span style="margin-left:8px;">Respect robots</span>
                </label>
              </div>
              <div class="toolbar">
                <button class="primary-button" type="submit">Run research scout</button>
                <span class="helper">Accepted findings stay provisional until you stage them, extract candidates, and review those candidates normally.</span>
              </div>
            </form>

            <div class="detail">
              <div class="detail-head">
                <div>
                  <h3>Programs</h3>
                  <div class="detail-note">The runner uses these instruction sets to choose facets, source policy, and quality thresholds.</div>
                </div>
                <span class="pill probable">${escapeHtml(availablePrograms.length)}</span>
              </div>
              <div class="detail-list">
                ${availablePrograms.length
                  ? availablePrograms
                      .map(
                        (program) => `
                          <div class="mini">
                            <div class="toolbar">
                              <strong>${escapeHtml(program.name)}</strong>
                              <span class="pill ${program.built_in ? "verified" : "probable"}">${program.built_in ? "built-in" : "custom"}</span>
                            </div>
                            <div class="detail-note">${escapeHtml(program.program_id)}</div>
                            <div>${escapeHtml(program.description || "No description supplied.")}</div>
                            <div class="detail-note">
                              facets: ${escapeHtml((program.default_facets || []).join(", ") || "none")}
                              · adapter: ${escapeHtml(program.default_adapter_id || "web_open")}
                            </div>
                            <div class="detail-note">
                              preferred: ${escapeHtml((program.preferred_source_classes || []).join(", ") || "none")}
                              · robots: ${program.default_execution_policy?.respect_robots ? "on" : "off"}
                            </div>
                          </div>
                        `
                      )
                      .join("")
                  : "<div class='helper'>No research programs are loaded yet.</div>"}
              </div>
            </div>

            <div class="surface list">
              ${state.researchRuns.length
                ? state.researchRuns
                    .map(
                      (run) => `
                        <button class="list-row ${run.run_id === selectedRun?.run_id ? "is-selected" : ""}" type="button" data-select-research-run="${escapeHtml(run.run_id)}">
                          <div>
                            <div class="row-title">${escapeHtml(run.brief.topic)}</div>
                            <div class="row-subtitle">${escapeHtml(run.run_id)}</div>
                          </div>
                          <div class="row-meta">
                            <span class="pill ${escapeHtml(run.status)}">${escapeHtml(run.status)}</span>
                            <span class="code">${escapeHtml(run.program_id)}</span>
                          </div>
                          <div class="row-meta">${escapeHtml(run.accepted_count)} accepted · ${escapeHtml(run.rejected_count)} rejected</div>
                          <div class="row-meta">${escapeHtml(run.staged_count)} staged · ${escapeHtml(run.query_count)} queries</div>
                        </button>
                      `
                    )
                    .join("")
                : "<div class='helper' style='padding:14px;'>No research runs yet. Start one from the brief form above.</div>"}
            </div>
          </div>

          <aside class="detail">
            ${selectedRun ? renderResearchRunDetail(selectedRun, selectedDetail) : "<div class='helper'>No research run selected.</div>"}
          </aside>
        </div>
      </article>
    `;
  }

  function renderResearchRunDetail(run, detail) {
    const findings = detail?.findings || [];
    const program = detail?.program || state.researchPrograms.find((item) => item.program_id === run.program_id);
    const facetCoverage = detail?.facet_coverage || [];
    const filteredFindings = filterAndSortResearchFindings(findings, state.filters);
    const filterOptions = buildResearchFilterOptions(findings);

    return `
      <div class="detail-head">
        <div>
          <h3>${escapeHtml(run.brief.topic)}</h3>
          <div class="detail-note">${escapeHtml(run.run_id)} · ${escapeHtml(program?.name || run.program_id)}</div>
        </div>
        <span class="pill ${escapeHtml(run.status)}">${escapeHtml(run.status)}</span>
      </div>
      <div class="inline-metrics">
        <span>${escapeHtml(run.accepted_count)} accepted</span>
        <span>${escapeHtml(run.rejected_count)} rejected</span>
        <span>${escapeHtml(run.staged_count)} staged</span>
        <span>${escapeHtml(run.finding_count)} findings</span>
        <span>${escapeHtml(run.query_count)} queries</span>
      </div>
      <div class="toolbar">
        <button class="secondary-button" type="button" data-action="refresh-research">Refresh detail</button>
        <button class="secondary-button" type="button" data-action="stage-research">Stage accepted findings</button>
        <button class="primary-button" type="button" data-action="extract-research">Stage + extract</button>
      </div>
      <div class="detail-grid">
        <div class="field">
          <label>Time window</label>
          <div>${escapeHtml(renderTimeWindow(run.brief))}</div>
        </div>
        <div class="field">
          <label>Locale</label>
          <div>${escapeHtml(run.brief.locale || "n/a")}</div>
        </div>
        <div class="field">
          <label>Audience</label>
          <div>${escapeHtml(run.brief.audience || "n/a")}</div>
        </div>
        <div class="field">
          <label>Extraction run</label>
          <div class="code">${escapeHtml(run.extraction_run_id || "not yet extracted")}</div>
        </div>
      </div>
      <div class="field">
        <label>Facet coverage</label>
        <div class="detail-list">
          ${(facetCoverage.length ? facetCoverage : run.facets).length
            ? (facetCoverage.length ? facetCoverage : run.facets)
                .map(
                  (facet) => `
                    <div class="mini">
                      <div class="toolbar">
                        <strong>${escapeHtml(facet.label)}</strong>
                        <span class="pill ${escapeHtml(facet.coverage_status || "probable")}">${escapeHtml(facet.accepted_count)}/${escapeHtml(facet.target_count)} target</span>
                      </div>
                      <div class="detail-note">
                        ${escapeHtml(facet.facet_id)}
                        · queries ${escapeHtml(facet.queries_attempted ?? 0)}
                        · hits ${escapeHtml(facet.hits_seen ?? 0)}
                        · rejected ${escapeHtml(facet.rejected_count)}
                        · skipped ${escapeHtml(facet.skipped_count ?? 0)}
                      </div>
                      <div>
                        ${escapeHtml(facet.coverage_status ? `coverage ${facet.coverage_status}` : facet.query_hint)}
                        ${facet.coverage_gap_reason ? ` · gap ${escapeHtml(facet.coverage_gap_reason)}` : ""}
                      </div>
                      ${facet.diagnostic_summary
                        ? `<div class="detail-note">diagnostics ${escapeHtml(facet.diagnostic_summary)} · duplicates ${escapeHtml(facet.duplicate_rejections ?? 0)} · threshold ${escapeHtml(facet.threshold_rejections ?? 0)} · excluded ${escapeHtml(facet.excluded_source_rejections ?? 0)} · fetch ${escapeHtml(facet.fetch_failures ?? 0)}</div>`
                        : ""}
                      ${facet.accepted_sources_by_type
                        ? `<div class="detail-note">accepted sources ${escapeHtml(renderKeyValueMap(facet.accepted_sources_by_type))}</div>`
                        : ""}
                    </div>
                  `
                )
                .join("")
            : "<div class='helper'>No facets were recorded for this run.</div>"}
        </div>
      </div>
      <div class="field">
        <label>Warnings</label>
        <div class="warning-list">
          ${run.warnings.length
            ? run.warnings.map((warning) => `<div class="warning">${escapeHtml(warning)}</div>`).join("")
            : "<div class='helper'>No warnings recorded.</div>"}
        </div>
      </div>
      <div class="field">
        <label>Telemetry</label>
        <div class="detail-list">
          <div class="mini">
            <div class="detail-note">queries ${escapeHtml(run.telemetry?.queries_attempted ?? 0)} / ${escapeHtml(run.telemetry?.total_queries ?? 0)}</div>
            <div>fetches ${escapeHtml(run.telemetry?.successful_fetches ?? 0)} successful / ${escapeHtml(run.telemetry?.fetch_attempts ?? 0)} attempts</div>
            <div class="detail-note">retries ${escapeHtml(run.telemetry?.retries ?? 0)} · dedupe ${escapeHtml(run.telemetry?.dedupe_count ?? 0)}</div>
          </div>
          <div class="mini">
            <div>robots blocks ${escapeHtml(run.telemetry?.blocked_by_robots_count ?? 0)} · policy blocks ${escapeHtml(run.telemetry?.blocked_by_policy_count ?? 0)}</div>
            <div class="detail-note">run ${escapeHtml(run.telemetry?.elapsed_run_time_ms ?? 0)}ms · fetch ${escapeHtml(run.telemetry?.elapsed_fetch_time_ms ?? 0)}ms</div>
          </div>
          <div class="mini">
            <div class="detail-note">per-host fetch counts</div>
            <div>${escapeHtml(renderKeyValueMap(run.telemetry?.per_host_fetch_counts || {}))}</div>
          </div>
          <div class="mini">
            <div class="detail-note">skipped hosts</div>
            <div>${escapeHtml(renderKeyValueMap(run.telemetry?.skipped_host_counts || {}))}</div>
          </div>
          <div class="mini">
            <div class="detail-note">failure categories</div>
            <div>${escapeHtml(renderKeyValueMap(run.telemetry?.fetch_failures_by_category || {}))}</div>
          </div>
          <div class="mini">
            <div class="detail-note">fallback flags</div>
            <div>${escapeHtml((run.telemetry?.fallback_flags || []).join(", ") || "none")}</div>
          </div>
          <div class="mini">
            <div class="detail-note">search providers</div>
            <div>${escapeHtml((run.telemetry?.search?.providers_used || []).join(", ") || "none")}</div>
            <div class="detail-note">
              queries ${escapeHtml(renderKeyValueMap(run.telemetry?.search?.queries_by_provider || {}))}
              · hits ${escapeHtml(renderKeyValueMap(run.telemetry?.search?.hits_by_provider || {}))}
            </div>
            <div class="detail-note">
              accepted ${escapeHtml(renderKeyValueMap(run.telemetry?.search?.accepted_by_provider || {}))}
              · zero-hit profiles ${escapeHtml(renderKeyValueMap(run.telemetry?.search?.zero_hit_queries_by_profile || {}))}
            </div>
            <div class="detail-note">
              ${run.telemetry?.search?.fallback_used ? `provider fallback ${escapeHtml(run.telemetry?.search?.fallback_reason || "used")}` : "provider fallback none"}
            </div>
          </div>
          <div class="mini">
            <div class="detail-note">semantic backend</div>
            <div>
              ${escapeHtml(run.telemetry?.semantic?.backend || "n/a")}
              ${run.telemetry?.semantic?.fallback_used ? ` · fallback ${escapeHtml(run.telemetry?.semantic?.fallback_reason || "used")}` : ""}
            </div>
            <div class="detail-note">
              vectors ${escapeHtml(run.telemetry?.semantic?.vectors_upserted ?? 0)}
              · comparisons ${escapeHtml(run.telemetry?.semantic?.comparisons_performed ?? 0)}
              · duplicate hints ${escapeHtml(run.telemetry?.semantic?.duplicate_hints_emitted ?? 0)}
            </div>
          </div>
        </div>
      </div>
      <div class="field">
        <label>Findings</label>
        <div class="detail-grid">
          <div class="field">
            <label for="research-filter-decision">Decision</label>
            <select id="research-filter-decision" data-research-filter="researchDecision">
              ${renderSelectOptions([
                ["all", "all"],
                ...filterOptions.decisions.map((value) => [value, value]),
              ], state.filters.researchDecision)}
            </select>
          </div>
          <div class="field">
            <label for="research-filter-reason">Reason</label>
            <select id="research-filter-reason" data-research-filter="researchReason">
              ${renderSelectOptions([
                ["all", "all"],
                ...filterOptions.reasons.map((value) => [value, value]),
              ], state.filters.researchReason)}
            </select>
          </div>
          <div class="field">
            <label for="research-filter-source-type">Source type</label>
            <select id="research-filter-source-type" data-research-filter="researchSourceType">
              ${renderSelectOptions([
                ["all", "all"],
                ...filterOptions.sourceTypes.map((value) => [value, value]),
              ], state.filters.researchSourceType)}
            </select>
          </div>
          <div class="field">
            <label for="research-filter-facet">Facet</label>
            <select id="research-filter-facet" data-research-filter="researchFacet">
              ${renderSelectOptions([
                ["all", "all"],
                ...filterOptions.facets.map((value) => [value, value]),
              ], state.filters.researchFacet)}
            </select>
          </div>
          <div class="field">
            <label for="research-filter-provider">Provider</label>
            <select id="research-filter-provider" data-research-filter="researchProvider">
              ${renderSelectOptions([
                ["all", "all"],
                ...filterOptions.providers.map((value) => [value, value]),
              ], state.filters.researchProvider)}
            </select>
          </div>
          <div class="field">
            <label for="research-filter-profile">Query profile</label>
            <select id="research-filter-profile" data-research-filter="researchQueryProfile">
              ${renderSelectOptions([
                ["all", "all"],
                ...filterOptions.queryProfiles.map((value) => [value, value]),
              ], state.filters.researchQueryProfile)}
            </select>
          </div>
          <div class="field">
            <label for="research-filter-sort">Sort</label>
            <select id="research-filter-sort" data-research-filter="researchSort">
              ${renderSelectOptions([
                ["accepted_first", "accepted first"],
                ["weakest_first", "weakest first"],
              ], state.filters.researchSort)}
            </select>
          </div>
          <div class="field">
            <label for="research-filter-semantic">Semantic</label>
            <select id="research-filter-semantic" data-research-filter="researchSemantic">
              ${renderSelectOptions([
                ["all", "all"],
                ["duplicate_hint", "duplicate hint"],
                ["fallback_only", "fallback only"],
              ], state.filters.researchSemantic)}
            </select>
          </div>
        </div>
        <div class="detail-list">
          ${filteredFindings.length
            ? filteredFindings
                .map(
                  (finding) => `
                    <div class="mini">
                      <div class="toolbar">
                        <strong>${escapeHtml(finding.title)}</strong>
                        <span class="pill ${escapeHtml(finding.decision)}">${escapeHtml(finding.decision)}</span>
                      </div>
                      <div class="detail-note">${escapeHtml(renderResearchWhySummary(finding))}</div>
                      <div>${escapeHtml(finding.snippet_text)}</div>
                      <div class="detail-note">
                        ${escapeHtml(finding.publisher || "unknown publisher")}
                        ${finding.published_at ? ` · ${escapeHtml(formatDate(finding.published_at))}` : ""}
                        ${finding.rejection_reason ? ` · ${escapeHtml(finding.rejection_reason)}` : ""}
                      </div>
                      <details class="detail-note" style="margin-top:8px;">
                        <summary>Why this finding</summary>
                        <div style="margin-top:8px;">${renderResearchFindingProvenance(finding)}</div>
                      </details>
                    </div>
                  `
                )
                .join("")
            : "<div class='helper'>No findings matched the current filters for this run.</div>"}
        </div>
      </div>
      <div class="field">
        <label>Run log</label>
        <div class="detail-list">
          ${run.logs.length
            ? run.logs.map((line) => `<div class="mini"><div>${escapeHtml(line)}</div></div>`).join("")
            : "<div class='helper'>No run log entries were recorded.</div>"}
        </div>
      </div>
    `;
  }

  function renderRunsScreen() {
    const selected = state.runs.find((run) => run.run_id === state.selectedRunId) ?? state.runs[0];

    return `
      <article class="screen fade-in">
        <div class="screen-head">
          <div>
            <h2 data-active-screen>Extraction Runs</h2>
            <p>Use this view to track the intake cycle. The backend currently exposes extraction as an action, so this screen keeps local history of completed and queued runs.</p>
          </div>
          <div class="screen-actions">
            <button class="primary-button" type="button" data-action="run-extraction">Run extraction</button>
          </div>
        </div>

        <div class="split">
          <div class="surface list">
            ${state.runs
              .map(
                (run) => `
                  <button class="list-row ${run.run_id === selected?.run_id ? "is-selected" : ""}" type="button" data-select-run="${escapeHtml(run.run_id)}">
                    <div>
                      <div class="row-title">${escapeHtml(run.run_id)}</div>
                      <div class="row-subtitle">${escapeHtml(run.notes || run.note || "Extraction run")}</div>
                    </div>
                    <div class="row-meta">
                      <span class="pill ${escapeHtml(run.status)}">${escapeHtml(run.status)}</span>
                      <span class="code">${escapeHtml(run.started_at || "n/a")}</span>
                    </div>
                    <div class="row-meta">${escapeHtml(run.source_count)} sources</div>
                    <div class="row-meta">${escapeHtml(run.candidate_count)} candidates</div>
                  </button>
                `
              )
              .join("")}
          </div>

          <aside class="detail">
            ${selected ? renderRunDetail(selected) : "<div class='helper'>No extraction runs yet.</div>"}
          </aside>
        </div>
      </article>
    `;
  }

  function renderRunDetail(run) {
    const relatedCandidates = state.candidates.filter((candidate) => candidate.extractor_run_id === run.run_id);

    return `
      <div class="detail-head">
        <div>
          <h3>${escapeHtml(run.run_id)}</h3>
          <div class="detail-note">${escapeHtml(run.notes || run.note || "No run note")}</div>
        </div>
        <span class="pill ${escapeHtml(run.status)}">${escapeHtml(run.status)}</span>
      </div>
      <div class="inline-metrics">
        <span>${escapeHtml(run.source_count)} sources</span>
        <span>${escapeHtml(run.candidate_count)} candidates</span>
        <span>Started: ${escapeHtml(formatDate(run.started_at))}</span>
        <span>Finished: ${escapeHtml(run.completed_at ? formatDate(run.completed_at) : run.finished_at ? formatDate(run.finished_at) : "n/a")}</span>
      </div>
      <div class="field">
        <label>Run note</label>
        <div>${escapeHtml(run.notes || run.note || "n/a")}</div>
      </div>
      <div class="field">
        <label>Candidates from this run</label>
        <div class="detail-list">
          ${relatedCandidates.length
            ? relatedCandidates
                .map(
                  (candidate) => `
                    <div class="mini">
                      <div class="code">${escapeHtml(candidate.candidate_id)}</div>
                      <div>${escapeHtml(candidate.subject)} — ${escapeHtml(candidate.value)}</div>
                      <div class="detail-note">${escapeHtml(candidate.review_state)} · ${escapeHtml(candidate.status_suggestion)}</div>
                    </div>
                  `
                )
                .join("")
            : "<div class='helper'>No candidates are linked to this run yet.</div>"}
        </div>
      </div>
    `;
  }

  function renderReviewScreen() {
    const filteredCandidates = state.candidates.filter((candidate) => {
      if (state.filters.candidates === "all") return true;
      return candidate.review_state === state.filters.candidates;
    });
    const selected = filteredCandidates.find((candidate) => candidate.candidate_id === state.selectedCandidateId) ?? filteredCandidates[0];

    return `
      <article class="screen fade-in">
        <div class="screen-head">
          <div>
            <h2 data-active-screen>Review Queue</h2>
            <p>Review is the trust gate. Pending candidates are queued here until they are approved into the canonical claim store or rejected as noise.</p>
          </div>
          <div class="screen-actions">
            <div class="chip-bar" role="tablist" aria-label="Candidate filters">
              ${renderFilterChip("candidates", "all", "All")}
              ${renderFilterChip("candidates", "pending", "Pending")}
              ${renderFilterChip("candidates", "approved", "Approved")}
              ${renderFilterChip("candidates", "rejected", "Rejected")}
            </div>
          </div>
        </div>

        <div class="split">
          <div class="surface list">
            ${filteredCandidates.length
              ? filteredCandidates
                  .map(
                    (candidate) => `
                      <button class="list-row ${candidate.candidate_id === selected?.candidate_id ? "is-selected" : ""}" type="button" data-select-candidate="${escapeHtml(candidate.candidate_id)}">
                        <div>
                          <div class="row-title">${escapeHtml(candidate.subject)}</div>
                          <div class="row-subtitle">${escapeHtml(candidate.predicate)} · ${escapeHtml(candidate.value)}</div>
                        </div>
                        <div class="row-meta">
                          <span class="pill ${escapeHtml(candidate.review_state)}">${escapeHtml(candidate.review_state)}</span>
                          <span class="code">${escapeHtml(candidate.candidate_id)}</span>
                        </div>
                        <div class="row-meta">${escapeHtml(candidate.status_suggestion)}</div>
                        <div class="row-meta">${escapeHtml(candidate.evidence_ids.length)} evidence</div>
                      </button>
                    `
                  )
                  .join("")
              : "<div class='helper' style='padding:14px;'>No candidates match the current filter.</div>"}
          </div>

          <aside class="detail">
            ${selected ? renderCandidateDetail(selected) : "<div class='helper'>No candidate selected.</div>"}
          </aside>
        </div>
      </article>
    `;
  }

  function renderFilterChip(group, value, label) {
    const active = state.filters[group] === value;
    return `
      <button
        class="chip ${active ? "is-active" : ""}"
        type="button"
        data-filter-group="${escapeHtml(group)}"
        data-filter-value="${escapeHtml(value)}"
        data-filter="${escapeHtml(group)}"
      >
        ${escapeHtml(label)}
      </button>
    `;
  }

  function renderCandidateDetail(candidate) {
    const matchingEvidence = candidate.evidence_ids
      .map((id) => state.evidence.find((snippet) => snippet.evidence_id === id))
      .filter(Boolean);

    return `
      <div class="detail-head">
        <div>
          <h3>${escapeHtml(candidate.subject)}</h3>
          <div class="detail-note">${escapeHtml(candidate.predicate)} · ${escapeHtml(candidate.value)}</div>
        </div>
        <span class="pill ${escapeHtml(candidate.review_state)}">${escapeHtml(candidate.review_state)}</span>
      </div>
      <div class="detail-grid">
        <div class="field">
          <label>Candidate ID</label>
          <div class="code">${escapeHtml(candidate.candidate_id)}</div>
        </div>
        <div class="field">
          <label>Suggested status</label>
          <div>${escapeHtml(candidate.status_suggestion)}</div>
        </div>
        <div class="field">
          <label>Claim kind</label>
          <div>${escapeHtml(candidate.claim_kind)}</div>
        </div>
        <div class="field">
          <label>Viewpoint scope</label>
          <div>${escapeHtml(candidate.viewpoint_scope || "n/a")}</div>
        </div>
      </div>
      <div class="field">
        <label>Evidence</label>
        <div class="detail-list">
          ${matchingEvidence.length
            ? matchingEvidence
                .map(
                  (snippet) => `
                    <div class="mini">
                      <div class="code">${escapeHtml(snippet.evidence_id)} · ${escapeHtml(snippet.locator)}</div>
                      <div class="detail-note">
                        ${escapeHtml(snippet.text_unit_id || "n/a")} ·
                        [${escapeHtml(snippet.span_start ?? "n/a")}, ${escapeHtml(snippet.span_end ?? "n/a")}]
                      </div>
                      <div>${escapeHtml(snippet.text)}</div>
                      <div class="detail-note">${escapeHtml(snippet.notes || "No note")}</div>
                    </div>
                  `
                )
                .join("")
            : "<div class='helper'>No evidence linked to this candidate.</div>"}
        </div>
      </div>
      <form class="detail-stack" data-form="review">
        <div class="detail-grid">
          <div class="field">
            <label>Decision</label>
            <select name="decision">
              <option value="approve">Approve</option>
              <option value="reject">Reject</option>
            </select>
          </div>
          <div class="field">
            <label>Override status</label>
            <select name="override_status">
              <option value="">Use suggestion</option>
              <option value="verified">Verified</option>
              <option value="probable">Probable</option>
              <option value="contested">Contested</option>
              <option value="rumor">Rumor</option>
              <option value="legend">Legend</option>
              <option value="author_choice">Author choice</option>
            </select>
          </div>
        </div>
        <div class="field">
          <label>Review notes</label>
          <textarea name="notes" placeholder="Add rationale, source concerns, or an editorial note.">${escapeHtml(candidate.notes || "")}</textarea>
        </div>
        <div class="toolbar">
          <button class="primary-button" type="submit" data-action="review-submit">Submit review</button>
          <span class="helper">Submitting approval writes to the truth store; rejection only updates the queue.</span>
        </div>
      </form>
    `;
  }

  function renderClaimsScreen() {
    const filteredClaims = state.claims.filter((claim) => {
      if (state.filters.claims === "all") return true;
      return claim.status === state.filters.claims;
    });
    const selected = filteredClaims.find((claim) => claim.claim_id === state.selectedClaimId) ?? filteredClaims[0];

    return `
      <article class="screen fade-in">
        <div class="screen-head">
          <div>
            <h2 data-active-screen>Claims</h2>
            <p>Approved claims are the canonical output. This screen keeps the operator focused on status, provenance, and what evidence actually supports each statement.</p>
          </div>
          <div class="screen-actions">
            <div class="chip-bar" role="tablist" aria-label="Claim filters">
              ${renderClaimFilter("all", "All")}
              ${renderClaimFilter("verified", "Verified")}
              ${renderClaimFilter("probable", "Probable")}
              ${renderClaimFilter("contested", "Contested")}
              ${renderClaimFilter("rumor", "Rumor")}
            </div>
          </div>
        </div>

        <div class="split">
          <div class="surface list">
            ${filteredClaims.length
              ? filteredClaims
                  .map(
                    (claim) => `
                      <button class="list-row ${claim.claim_id === selected?.claim_id ? "is-selected" : ""}" type="button" data-select-claim="${escapeHtml(claim.claim_id)}">
                        <div>
                          <div class="row-title">${escapeHtml(claim.subject)}</div>
                          <div class="row-subtitle">${escapeHtml(claim.predicate)} · ${escapeHtml(claim.value)}</div>
                        </div>
                        <div class="row-meta">
                          <span class="pill ${escapeHtml(claim.status)}">${escapeHtml(claim.status)}</span>
                          <span class="code">${escapeHtml(claim.claim_id)}</span>
                        </div>
                        <div class="row-meta">${escapeHtml(claim.claim_kind)}</div>
                        <div class="row-meta">${escapeHtml(claim.evidence_ids.length)} evidence</div>
                      </button>
                    `
                  )
                  .join("")
              : "<div class='helper' style='padding:14px;'>No claims match the current filter.</div>"}
          </div>

          <aside class="detail">
            ${selected ? renderClaimDetail(selected) : "<div class='helper'>No claim selected.</div>"}
          </aside>
        </div>
      </article>
    `;
  }

  function renderClaimFilter(value, label) {
    const active = state.filters.claims === value;
    return `
      <button class="chip ${active ? "is-active" : ""}" type="button" data-filter-group="claims" data-filter-value="${escapeHtml(value)}" data-filter="claims">
        ${escapeHtml(label)}
      </button>
    `;
  }

  function renderClaimDetail(claim) {
    const evidence = claim.evidence_ids
      .map((id) => state.evidence.find((snippet) => snippet.evidence_id === id))
      .filter(Boolean);

    return `
      <div class="detail-head">
        <div>
          <h3>${escapeHtml(claim.subject)}</h3>
          <div class="detail-note">${escapeHtml(claim.predicate)} · ${escapeHtml(claim.value)}</div>
        </div>
        <span class="pill ${escapeHtml(claim.status)}">${escapeHtml(claim.status)}</span>
      </div>
      <div class="detail-grid">
        <div class="field">
          <label>Claim ID</label>
          <div class="code">${escapeHtml(claim.claim_id)}</div>
        </div>
        <div class="field">
          <label>Kind</label>
          <div>${escapeHtml(claim.claim_kind)}</div>
        </div>
        <div class="field">
          <label>Place</label>
          <div>${escapeHtml(claim.place || "n/a")}</div>
        </div>
        <div class="field">
          <label>Author choice</label>
          <div>${claim.author_choice ? "yes" : "no"}</div>
        </div>
      </div>
      <div class="field">
        <label>Evidence</label>
        <div class="detail-list">
          ${evidence.length
            ? evidence
                .map(
                  (snippet) => `
                    <div class="mini">
                      <div class="code">${escapeHtml(snippet.evidence_id)} · ${escapeHtml(snippet.locator)}</div>
                      <div>${escapeHtml(snippet.text)}</div>
                      <div class="detail-note">${escapeHtml(snippet.notes || "No note")}</div>
                    </div>
                  `
                )
                .join("")
            : "<div class='helper'>No evidence linked to this claim.</div>"}
        </div>
      </div>
      <div class="field">
        <label>Notes</label>
        <div>${escapeHtml(claim.notes || "n/a")}</div>
      </div>
    `;
  }

  function renderAskScreen() {
    const queryResult = state.queryResult;

    return `
      <article class="screen fade-in">
        <div class="screen-head">
          <div>
            <h2 data-active-screen>Ask</h2>
            <p>Ask only against approved claims. The query screen keeps mode selection visible so the operator always knows what kind of answer surface they are invoking.</p>
          </div>
          <div class="screen-actions">
            ${seed.queryPresets
              .map(
                (preset) => `
                  <button class="secondary-button" type="button" data-preset="${escapeHtml(preset)}">${escapeHtml(preset)}</button>
                `
              )
              .join("")}
          </div>
        </div>

        <div class="split">
          <form class="detail query-form" data-form="query">
            <div class="query-row">
              <div class="field">
                <label for="question">Question</label>
                <textarea id="question" name="question" placeholder="Ask about the approved record set.">${escapeHtml(state.query.question)}</textarea>
              </div>
              <div class="detail-stack">
                <div class="field">
                  <label>Mode</label>
                  <div class="chip-bar">
                    ${renderModeChip("strict_facts", "Strict facts")}
                    ${renderModeChip("contested_views", "Contested views")}
                    ${renderModeChip("rumor_and_legend", "Rumor + legend")}
                    ${renderModeChip("character_knowledge", "Character knowledge")}
                    ${renderModeChip("open_exploration", "Open exploration")}
                  </div>
                </div>
                <div class="field">
                  <label for="status">Status filter</label>
                  <input id="status" name="status" value="${escapeHtml(state.query.status)}" placeholder="verified, probable, ..." />
                </div>
                <div class="field">
                  <label for="claimKind">Claim kind filter</label>
                  <input id="claimKind" name="claimKind" value="${escapeHtml(state.query.claimKind)}" placeholder="practice, belief, ..." />
                </div>
                <div class="field">
                  <label for="place">Place filter</label>
                  <input id="place" name="place" value="${escapeHtml(state.query.place)}" placeholder="Rouen" />
                </div>
                <div class="field">
                  <label for="viewpoint">Viewpoint filter</label>
                  <input id="viewpoint" name="viewpoint" value="${escapeHtml(state.query.viewpoint)}" placeholder="townspeople" />
                </div>
              </div>
            </div>
            <div class="toolbar">
              <button class="primary-button" type="submit" data-action="query-submit">Ask the record</button>
              <span class="helper">This uses the current `/v1/query` contract and returns supporting claims, evidence, and warnings.</span>
            </div>
          </form>

          <aside class="detail query-result">
            ${renderQueryResult(queryResult)}
          </aside>
        </div>
      </article>
    `;
  }

  function renderModeChip(value, label) {
    const active = state.query.mode === value;
    return `
      <button class="chip ${active ? "is-active" : ""}" type="button" data-query-mode="${escapeHtml(value)}">
        ${escapeHtml(label)}
      </button>
    `;
  }

  function renderQueryResult(result) {
    if (!result) {
      return `
        <div class="detail-head">
          <div>
            <h3>No query yet</h3>
            <div class="detail-note">Run a question to populate answer, evidence, and warnings.</div>
          </div>
        </div>
        <div class="helper">The answer surface is designed for provenance first. It should never feel like a guess box.</div>
      `;
    }

    return `
      <div class="detail-head">
        <div>
          <h3>Answer</h3>
          <div class="detail-note">${escapeHtml(result.question)}</div>
        </div>
        <span class="pill ${escapeHtml(result.mode)}">${escapeHtml(result.mode)}</span>
      </div>
      <div class="answer-block">
        <pre>${escapeHtml(result.answer)}</pre>
      </div>
      <div class="warning-list">
        ${result.warnings.length
          ? result.warnings.map((warning) => `<div class="warning">${escapeHtml(warning)}</div>`).join("")
          : "<div class='helper'>No warnings returned.</div>"}
      </div>
      <div class="field">
        <label>Supporting claims</label>
        <div class="detail-list">
          ${result.supporting_claims.length
            ? result.supporting_claims
                .map(
                  (claim) => `
                    <div class="mini">
                      <div class="code">${escapeHtml(claim.claim_id)}</div>
                      <div>${escapeHtml(claim.subject)} — ${escapeHtml(claim.predicate)} — ${escapeHtml(claim.value)}</div>
                      <div class="detail-note">${escapeHtml(claim.status)} · ${escapeHtml(claim.claim_kind)}</div>
                    </div>
                  `
                )
                .join("")
            : "<div class='helper'>No supporting claims were returned.</div>"}
        </div>
      </div>
      <div class="field">
        <label>Evidence</label>
        <div class="detail-list">
          ${result.evidence.length
            ? result.evidence
                .map(
                  (snippet) => `
                    <div class="mini">
                      <div class="code">${escapeHtml(snippet.evidence_id)} · ${escapeHtml(snippet.locator)}</div>
                      <div>${escapeHtml(snippet.text)}</div>
                      <div class="detail-note">${escapeHtml(snippet.notes || "No note")}</div>
                    </div>
                  `
                )
                .join("")
            : "<div class='helper'>No evidence snippets were returned.</div>"}
        </div>
      </div>
    `;
  }

  async function pullSources() {
    applyLoading(true);
    try {
      const payload = await fetchJson(API.pullSources, { method: "POST" });
      if (Array.isArray(payload.sources)) {
        state.sources = payload.sources;
      }
      try {
        state.sources = await fetchJson(API.sources);
      } catch (error) {
        console.warn("Could not refresh sources after pull", error);
      }
      state.lastSync = new Date().toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
      nodes.lastSync.textContent = state.lastSync;
      await refreshRuntimeStatus();
      setApiStatus(true, `Pulled ${payload.count ?? state.sources.length} sources from Zotero.`);
      setBanner("live", "Sources pulled", `Received ${payload.count ?? state.sources.length} source records from the live endpoint.`);
      render();
    } catch (error) {
      setApiStatus(false, "Source pull failed. Seed records remain in place.");
      setBanner("failed", "Could not pull sources", error.message || "The source pull endpoint is unavailable.");
    } finally {
      applyLoading(false);
      persistState();
    }
  }

  async function runExtraction() {
    applyLoading(true);
    try {
      const payload = await fetchJson(API.extractCandidates, { method: "POST" });
      if (Array.isArray(payload.candidates)) {
        state.candidates = payload.candidates;
      }
      if (Array.isArray(payload.evidence)) {
        state.evidence = payload.evidence;
      }
      if (payload.run) {
        state.runs = [payload.run, ...state.runs.filter((run) => run.run_id !== payload.run.run_id)];
        state.selectedRunId = payload.run.run_id;
      } else {
        try {
          state.runs = await fetchJson(API.runs);
          state.selectedRunId = state.runs[0]?.run_id ?? state.selectedRunId;
        } catch (error) {
          console.warn("Could not refresh runs after extraction", error);
        }
      }
      state.selectedCandidateId = state.candidates.find((candidate) => candidate.review_state === "pending")?.candidate_id ?? state.candidates[0]?.candidate_id ?? null;
      state.lastSync = new Date().toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
      nodes.lastSync.textContent = state.lastSync;
      await refreshRuntimeStatus();
      setApiStatus(true, `Extraction returned ${payload.count ?? state.candidates.length} candidates.`);
      setBanner("live", "Extraction complete", `Queued ${payload.count ?? state.candidates.length} candidates from the live endpoint.`);
      render();
    } catch (error) {
      const nextRunId = `local-${Date.now()}`;
      state.runs = [
        {
          run_id: nextRunId,
          status: "failed",
          source_count: state.sources.length,
          candidate_count: 0,
          started_at: new Date().toISOString(),
          finished_at: new Date().toISOString(),
          note: "Extraction could not reach the live backend, so the console preserved the local run log.",
        },
        ...state.runs,
      ];
      state.selectedRunId = nextRunId;
      setApiStatus(false, "Extraction failed. Seed data remains available.");
      setBanner("failed", "Extraction failed", error.message || "The extraction endpoint is unavailable.");
      render();
    } finally {
      applyLoading(false);
      persistState();
    }
  }

  async function submitReview(form) {
    const candidateId = state.selectedCandidateId;
    if (!candidateId) {
      setBanner("failed", "No candidate selected", "Pick a candidate before submitting a review.");
      return;
    }

    const formData = new FormData(form);
    const decision = formData.get("decision");
    const overrideStatus = formData.get("override_status");
    const notes = String(formData.get("notes") || "").trim();

    applyLoading(true);
    try {
      const payload = await fetchJson(API.reviewCandidate(candidateId), {
        method: "POST",
        body: {
          decision,
          override_status: overrideStatus || null,
          notes: notes || null,
        },
      });

      if (payload.status === "rejected") {
        state.candidates = state.candidates.map((candidate) =>
          candidate.candidate_id === candidateId ? { ...candidate, review_state: "rejected" } : candidate
        );
        setBanner("live", "Candidate rejected", `Candidate ${candidateId} is now marked rejected.`);
      } else if (payload.claim) {
        state.candidates = state.candidates.map((candidate) =>
          candidate.candidate_id === candidateId ? { ...candidate, review_state: "approved" } : candidate
        );
        state.claims = [payload.claim, ...state.claims.filter((claim) => claim.claim_id !== payload.claim.claim_id)];
        setBanner("live", "Candidate approved", `Claim ${payload.claim.claim_id} was written to the truth store.`);
      }

      await refreshAfterReview(candidateId);
    } catch (error) {
      const candidate = state.candidates.find((item) => item.candidate_id === candidateId);
      if (candidate) {
        const reviewState = decision === "reject" ? "rejected" : "approved";
        candidate.review_state = reviewState;
      }
      if (decision !== "reject" && candidateId) {
        state.claims = [
          {
            claim_id: `local-${Date.now()}`,
            subject: candidate?.subject || "Unknown",
            predicate: candidate?.predicate || "unknown",
            value: candidate?.value || "unknown",
            claim_kind: candidate?.claim_kind || "belief",
            status: overrideStatus || candidate?.status_suggestion || "probable",
            place: candidate?.place || null,
            time_start: candidate?.time_start || null,
            time_end: candidate?.time_end || null,
            viewpoint_scope: candidate?.viewpoint_scope || null,
            author_choice: overrideStatus === "author_choice",
            evidence_ids: candidate?.evidence_ids || [],
            notes: notes || candidate?.notes || null,
          },
          ...state.claims,
        ];
      }
      setApiStatus(false, "Review endpoint unavailable. Local state was updated.");
      setBanner("failed", "Review could not sync", error.message || "The review endpoint is unavailable.");
      render();
    } finally {
      applyLoading(false);
      persistState();
    }
  }

  async function refreshAfterReview(candidateId) {
    try {
      const [candidates, claims] = await Promise.all([fetchJson(API.candidates), fetchJson(API.claims)]);
      state.candidates = candidates;
      state.claims = claims;
      await refreshRuntimeStatus();
      state.selectedCandidateId =
        state.candidates.find((candidate) => candidate.review_state === "pending")?.candidate_id ??
        state.candidates.find((candidate) => candidate.candidate_id === candidateId)?.candidate_id ??
        state.candidates[0]?.candidate_id ??
        null;
      updateSelectionFallbacks();
      setApiStatus(true, "Review sync complete.");
    } catch (error) {
      console.warn("Could not refresh review state", error);
    }
    render();
  }

  async function submitQuery(form) {
    const formData = new FormData(form);
    const request = {
      question: String(formData.get("question") || "").trim(),
      mode: state.query.mode,
      filters: {
        status: String(formData.get("status") || "").trim() || null,
        claim_kind: String(formData.get("claimKind") || "").trim() || null,
        place: String(formData.get("place") || "").trim() || null,
        viewpoint_scope: String(formData.get("viewpoint") || "").trim() || null,
      },
    };

    state.query = {
      question: request.question,
      mode: request.mode,
      status: request.filters.status || "",
      claimKind: request.filters.claim_kind || "",
      place: request.filters.place || "",
      viewpoint: request.filters.viewpoint_scope || "",
    };
    persistState();

    applyLoading(true);
    try {
      const payload = await fetchJson(API.query, {
        method: "POST",
        body: request,
      });
      state.queryResult = payload;
      await refreshRuntimeStatus();
      setBanner("live", "Query answered", "The question was resolved against approved claims and their evidence.");
      setApiStatus(true, "Query response received.");
      render();
    } catch (error) {
      state.queryResult = {
        question: request.question,
        mode: request.mode,
        answer:
          "The live query endpoint was unavailable, so the console retained the operator form instead of fabricating an answer.",
        supporting_claims: [],
        evidence: [],
        warnings: [error.message || "Query endpoint unavailable."],
      };
      setApiStatus(false, "Query failed. Seed and local state remain available.");
      setBanner("failed", "Query failed", error.message || "Could not reach the query endpoint.");
      render();
    } finally {
      applyLoading(false);
    }
  }

  async function submitResearchRun(form) {
    const formData = new FormData(form);
    const draft = {
      topic: String(formData.get("topic") || "").trim(),
      focal_year: String(formData.get("focal_year") || "").trim(),
      time_start: String(formData.get("time_start") || "").trim(),
      time_end: String(formData.get("time_end") || "").trim(),
      locale: String(formData.get("locale") || "").trim(),
      audience: String(formData.get("audience") || "").trim(),
      adapter_id: String(formData.get("adapter_id") || "web_open").trim() || "web_open",
      domain_hints: String(formData.get("domain_hints") || "").trim(),
      desired_facets: String(formData.get("desired_facets") || "").trim(),
      preferred_source_types: String(formData.get("preferred_source_types") || "").trim(),
      excluded_source_types: String(formData.get("excluded_source_types") || "").trim(),
      coverage_targets: String(formData.get("coverage_targets") || "").trim(),
      curated_title: String(formData.get("curated_title") || "").trim(),
      curated_text: String(formData.get("curated_text") || "").trim(),
      curated_url: String(formData.get("curated_url") || "").trim(),
      max_queries: String(formData.get("max_queries") || "").trim(),
      max_results_per_query: String(formData.get("max_results_per_query") || "").trim(),
      max_findings: String(formData.get("max_findings") || "").trim(),
      max_per_facet: String(formData.get("max_per_facet") || "").trim(),
      total_fetch_time_seconds: String(formData.get("total_fetch_time_seconds") || "").trim(),
      per_host_fetch_cap: String(formData.get("per_host_fetch_cap") || "").trim(),
      retry_attempts: String(formData.get("retry_attempts") || "").trim(),
      retry_backoff_base_ms: String(formData.get("retry_backoff_base_ms") || "").trim(),
      retry_backoff_max_ms: String(formData.get("retry_backoff_max_ms") || "").trim(),
      allow_domains: String(formData.get("allow_domains") || "").trim(),
      deny_domains: String(formData.get("deny_domains") || "").trim(),
      respect_robots: formData.get("respect_robots") === "on",
      program_id: String(formData.get("program_id") || "default").trim() || "default",
    };
    state.researchDraft = draft;
    persistState();

    const request = {
      brief: {
        topic: draft.topic,
        focal_year: nullableString(draft.focal_year),
        time_start: nullableString(draft.time_start),
        time_end: nullableString(draft.time_end),
        locale: nullableString(draft.locale),
        audience: nullableString(draft.audience),
        adapter_id: nullableString(draft.adapter_id),
        domain_hints: splitList(draft.domain_hints),
        desired_facets: splitList(draft.desired_facets).length ? splitList(draft.desired_facets) : null,
        preferred_source_types: splitList(draft.preferred_source_types),
        excluded_source_types: splitList(draft.excluded_source_types),
        coverage_targets: parseCoverageTargets(draft.coverage_targets),
        curated_inputs: buildCuratedInputs(draft),
        execution_policy: {
          total_fetch_time_seconds: parsePositiveInteger(draft.total_fetch_time_seconds, 90),
          per_host_fetch_cap: parsePositiveInteger(draft.per_host_fetch_cap, 3),
          retry_attempts: parsePositiveInteger(draft.retry_attempts, 3),
          retry_backoff_base_ms: parsePositiveInteger(draft.retry_backoff_base_ms, 250),
          retry_backoff_max_ms: parsePositiveInteger(draft.retry_backoff_max_ms, 2000),
          respect_robots: draft.respect_robots,
          allow_domains: splitList(draft.allow_domains),
          deny_domains: splitList(draft.deny_domains),
        },
        max_queries: parsePositiveInteger(draft.max_queries, 12),
        max_results_per_query: parsePositiveInteger(draft.max_results_per_query, 5),
        max_findings: parsePositiveInteger(draft.max_findings, 20),
        max_per_facet: parsePositiveInteger(draft.max_per_facet, 2),
      },
      program_id: draft.program_id === "default" ? null : draft.program_id,
    };

    applyLoading(true);
    try {
      const payload = await fetchJson(API.researchRuns, {
        method: "POST",
        body: request,
      });
      state.researchRuns = [payload.run, ...state.researchRuns.filter((run) => run.run_id !== payload.run.run_id)];
      state.selectedResearchRunId = payload.run.run_id;
      state.researchRunDetail = payload;
      state.activeScreen = "research";
      location.hash = "#research";
      await refreshRuntimeStatus();
      setApiStatus(true, `Research run ${payload.run.run_id} completed and is ready for staging.`);
      setBanner("live", "Research run created", `Collected ${payload.run.accepted_count} accepted findings across ${payload.run.facets.length} facets.`);
      render();
    } catch (error) {
      setBanner("failed", "Research run failed", error.message || "The research run could not be created.");
      render();
    } finally {
      applyLoading(false);
      persistState();
    }
  }

  async function refreshResearchDetail(runId, { quiet = false } = {}) {
    if (!runId) {
      if (!quiet) {
        setBanner("failed", "No research run selected", "Pick a research run before requesting its detail.");
      }
      return;
    }

    applyLoading(true);
    try {
      const payload = await fetchJson(API.researchRun(runId));
      state.researchRunDetail = payload;
      state.researchRuns = [
        payload.run,
        ...state.researchRuns.filter((run) => run.run_id !== payload.run.run_id),
      ];
      state.selectedResearchRunId = payload.run.run_id;
      await refreshRuntimeStatus();
      if (!quiet) {
        setBanner("live", "Research detail refreshed", `Loaded ${payload.findings.length} findings for ${payload.run.run_id}.`);
      }
      render();
    } catch (error) {
      if (!quiet) {
        setBanner("failed", "Research detail failed", error.message || "Could not load the selected research run.");
        render();
      }
    } finally {
      applyLoading(false);
      persistState();
    }
  }

  async function stageResearchRun(runId) {
    if (!runId) {
      setBanner("failed", "No research run selected", "Pick a research run before staging findings.");
      return;
    }

    applyLoading(true);
    try {
      const payload = await fetchJson(API.stageResearchRun(runId), { method: "POST" });
      state.researchRuns = [
        payload.run,
        ...state.researchRuns.filter((run) => run.run_id !== payload.run.run_id),
      ];
      await refreshLiveData({ quiet: true });
      await refreshResearchDetail(runId, { quiet: true });
      setApiStatus(true, `Staged ${payload.staged_source_ids.length} research findings into source records.`);
      setBanner(
        "live",
        "Research staged",
        payload.staged_source_ids.length
          ? `Created ${payload.staged_source_ids.length} staged sources and ${payload.staged_document_ids.length} text documents.`
          : payload.warnings[0] || "No additional accepted findings needed staging."
      );
      render();
    } catch (error) {
      setBanner("failed", "Staging failed", error.message || "Could not stage the selected research run.");
      render();
    } finally {
      applyLoading(false);
      persistState();
    }
  }

  async function extractResearchRun(runId) {
    if (!runId) {
      setBanner("failed", "No research run selected", "Pick a research run before extracting staged findings.");
      return;
    }

    applyLoading(true);
    try {
      const payload = await fetchJson(API.extractResearchRun(runId), { method: "POST" });
      const researchRun = payload.stage_result?.run;
      if (researchRun) {
        state.researchRuns = [
          researchRun,
          ...state.researchRuns.filter((run) => run.run_id !== researchRun.run_id),
        ];
      }
      if (payload.extraction?.candidates) {
        state.candidates = payload.extraction.candidates;
      }
      if (payload.extraction?.evidence) {
        state.evidence = payload.extraction.evidence;
      }
      if (payload.extraction?.run) {
        state.runs = [
          payload.extraction.run,
          ...state.runs.filter((run) => run.run_id !== payload.extraction.run.run_id),
        ];
        state.selectedRunId = payload.extraction.run.run_id;
      }
      await refreshLiveData({ quiet: true });
      await refreshResearchDetail(runId, { quiet: true });
      state.selectedCandidateId =
        state.candidates.find((candidate) => candidate.review_state === "pending")?.candidate_id ??
        state.candidates[0]?.candidate_id ??
        null;
      setApiStatus(true, `Research extraction created ${payload.extraction?.candidates?.length || 0} candidates.`);
      setBanner(
        "live",
        "Research extracted",
        `Normalized ${payload.normalization?.document_count ?? 0} documents into ${payload.normalization?.text_unit_count ?? 0} text units and produced ${payload.extraction?.candidates?.length || 0} candidates.`
      );
      render();
    } catch (error) {
      setBanner("failed", "Research extraction failed", error.message || "Could not stage and extract the selected research run.");
      render();
    } finally {
      applyLoading(false);
      persistState();
    }
  }

  function nullableString(value) {
    const normalized = String(value || "").trim();
    return normalized || null;
  }

  function splitList(value) {
    return String(value || "")
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean);
  }

  function parseCoverageTargets(value) {
    return splitList(value).reduce((accumulator, item) => {
      const [key, rawCount] = item.split(":").map((part) => part.trim());
      const count = Number.parseInt(rawCount || "", 10);
      if (key && Number.isFinite(count) && count > 0) {
        accumulator[key] = count;
      }
      return accumulator;
    }, {});
  }

  function parsePositiveInteger(value, fallback) {
    const parsed = Number.parseInt(String(value || "").trim(), 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
  }

  function buildCuratedInputs(draft) {
    const inputs = [];
    if (draft.curated_text) {
      inputs.push({
        input_type: "text",
        title: draft.curated_title || "Curated Text Input",
        text: draft.curated_text,
      });
    }
    if (draft.curated_url) {
      inputs.push({
        input_type: "url",
        url: draft.curated_url,
        title: draft.curated_title || null,
      });
    }
    return inputs;
  }

  function renderResearchWhySummary(finding) {
    const provenance = finding.provenance || null;
    const host = provenance?.canonical_url
      ? safeHostFromUrl(provenance.canonical_url)
      : safeHostFromUrl(finding.canonical_url || finding.url || "");
    const reason =
      provenance?.acceptance_reason ||
      provenance?.rejection_reason ||
      finding.rejection_reason ||
      "provenance_unavailable";
    const threshold = provenance?.scoring?.quality_threshold;
    const scoreSummary =
      typeof threshold === "number" && Number.isFinite(threshold)
        ? `score ${finding.score} / threshold ${threshold}`
        : `score ${finding.score}`;
    const semantic =
      provenance?.semantic_duplicate_hint
        ? `semantic duplicate ${provenance.scoring?.semantic_duplicate_similarity ?? "n/a"}`
        : provenance?.scoring?.semantic_fallback_used
          ? `semantic fallback`
          : null;
    return [
      provenance?.facet_label || finding.facet_id,
      provenance?.search_provider_id ? `provider ${provenance.search_provider_id}` : null,
      provenance?.query_profile ? `profile ${provenance.query_profile}` : null,
      `query ${provenance?.originating_query || finding.query}`,
      host ? `host ${host}` : null,
      provenance?.fetch_outcome ? `fetch ${provenance.fetch_outcome}` : null,
      scoreSummary,
      semantic,
      `why ${reason}`,
    ]
      .filter(Boolean)
      .join(" · ");
  }

  function renderResearchFindingProvenance(finding) {
    const provenance = finding.provenance || null;
    if (!provenance) {
      return `<div>Structured provenance is unavailable for this older finding.</div>`;
    }
    const lines = [
      `adapter: ${provenance.adapter_id || "n/a"}`,
      `facet: ${provenance.facet_label || finding.facet_id} (${provenance.facet_id})`,
      `provider: ${provenance.search_provider_id || "n/a"} rank ${provenance.provider_rank ?? "n/a"} matched ${(provenance.matched_providers || []).join(", ") || "none"}`,
      `query profile: ${provenance.query_profile || "n/a"}${provenance.fusion_score != null ? ` · fusion ${provenance.fusion_score}` : ""}`,
      `query: ${provenance.originating_query || finding.query}`,
      `search rank: ${provenance.search_rank ?? "n/a"}`,
      `canonical url: ${provenance.canonical_url || finding.canonical_url || finding.url}`,
      `fetch outcome: ${provenance.fetch_outcome || "n/a"}`,
      `fetch status: ${provenance.fetch_status || "n/a"}`,
      `fetch error: ${provenance.fetch_error_category || "none"}`,
      `decision reason: ${provenance.acceptance_reason || provenance.rejection_reason || "n/a"}`,
      `duplicate rule: ${provenance.duplicate_rule || "none"}`,
      `policy flags: ${(provenance.policy_flags || []).join(", ") || "none"}`,
      `semantic duplicate hint: ${provenance.semantic_duplicate_hint ? "yes" : "no"}`,
      `semantic notes: ${provenance.semantic_decision_notes || "none"}`,
      `normalized title: ${provenance.scoring?.normalized_title || "n/a"}`,
      `canonical host: ${provenance.scoring?.canonical_host || "n/a"}`,
      `scores: overall ${provenance.scoring?.overall_score ?? finding.score}, relevance ${provenance.scoring?.relevance_score ?? finding.relevance_score}, quality ${provenance.scoring?.quality_score ?? finding.quality_score}, novelty ${provenance.scoring?.novelty_score ?? finding.novelty_score}`,
      `components: structural ${provenance.scoring?.structural_score ?? 0}, source class ${provenance.scoring?.source_class_score ?? 0}, era ${provenance.scoring?.era_score ?? 0}, coverage ${provenance.scoring?.coverage_score ?? 0}`,
      `semantic: score ${provenance.scoring?.semantic_score ?? 0}, novelty ${provenance.scoring?.semantic_novelty_score ?? 0}, rerank ${provenance.scoring?.semantic_rerank_delta ?? 0}`,
      `semantic top match: ${provenance.scoring?.semantic_duplicate_candidate_id || "none"} (${provenance.scoring?.semantic_duplicate_similarity ?? "n/a"})`,
      `semantic fallback: ${provenance.scoring?.semantic_fallback_used ? provenance.scoring?.semantic_fallback_reason || "used" : "no"}`,
      `threshold: ${provenance.scoring?.quality_threshold ?? "n/a"} (${provenance.scoring?.threshold_passed ? "passed" : "not passed"})`,
    ];
    for (const match of provenance.semantic_matches || []) {
      lines.push(`semantic match: ${match.finding_id} · ${match.similarity} · ${match.title}`);
    }
    return lines.map((line) => `<div>${escapeHtml(line)}</div>`).join("");
  }

  function buildResearchFilterOptions(findings) {
    return {
      decisions: uniqueSorted(findings.map((item) => item.decision).filter(Boolean)),
      reasons: uniqueSorted(
        findings
          .map((item) => item.provenance?.acceptance_reason || item.provenance?.rejection_reason || item.rejection_reason)
          .filter(Boolean)
      ),
      sourceTypes: uniqueSorted(findings.map((item) => item.source_type || "unknown").filter(Boolean)),
      facets: uniqueSorted(findings.map((item) => item.facet_id).filter(Boolean)),
      providers: uniqueSorted(findings.map((item) => item.provenance?.search_provider_id || "unknown").filter(Boolean)),
      queryProfiles: uniqueSorted(findings.map((item) => item.provenance?.query_profile || "unknown").filter(Boolean)),
    };
  }

  function filterAndSortResearchFindings(findings, filters) {
    const filtered = findings.filter((finding) => {
      const reason = finding.provenance?.acceptance_reason || finding.provenance?.rejection_reason || finding.rejection_reason || "";
      const sourceType = finding.source_type || "unknown";
      if (filters.researchDecision !== "all" && finding.decision !== filters.researchDecision) {
        return false;
      }
      if (filters.researchReason !== "all" && reason !== filters.researchReason) {
        return false;
      }
      if (filters.researchSourceType !== "all" && sourceType !== filters.researchSourceType) {
        return false;
      }
      if (filters.researchFacet !== "all" && finding.facet_id !== filters.researchFacet) {
        return false;
      }
      if (filters.researchProvider !== "all" && (finding.provenance?.search_provider_id || "unknown") !== filters.researchProvider) {
        return false;
      }
      if (filters.researchQueryProfile !== "all" && (finding.provenance?.query_profile || "unknown") !== filters.researchQueryProfile) {
        return false;
      }
      if (filters.researchSemantic === "duplicate_hint" && !finding.provenance?.semantic_duplicate_hint) {
        return false;
      }
      if (filters.researchSemantic === "fallback_only" && !finding.provenance?.scoring?.semantic_fallback_used) {
        return false;
      }
      return true;
    });

    return filtered.sort((left, right) => {
      if (filters.researchSort === "weakest_first") {
        return (left.score - right.score) || left.title.localeCompare(right.title);
      }
      const decisionOrder = (left.decision === "accepted" ? 0 : 1) - (right.decision === "accepted" ? 0 : 1);
      if (decisionOrder !== 0) {
        return decisionOrder;
      }
      return (right.score - left.score) || left.title.localeCompare(right.title);
    });
  }

  function renderSelectOptions(options, selectedValue) {
    return options
      .map(
        ([value, label]) => `
          <option value="${escapeHtml(value)}" ${value === selectedValue ? "selected" : ""}>${escapeHtml(label)}</option>
        `
      )
      .join("");
  }

  function uniqueSorted(values) {
    return [...new Set(values)].sort((left, right) => String(left).localeCompare(String(right)));
  }

  function safeHostFromUrl(value) {
    try {
      if (!value) {
        return "";
      }
      return new URL(value).hostname.replace(/^www\./, "");
    } catch {
      return "";
    }
  }

  function renderKeyValueMap(values) {
    const entries = Object.entries(values || {});
    if (!entries.length) {
      return "none";
    }
    return entries.map(([key, value]) => `${key}: ${value}`).join(", ");
  }

  function renderTimeWindow(brief) {
    if (brief.time_start || brief.time_end) {
      return [brief.time_start || "?", brief.time_end || "?"].join(" -> ");
    }
    return brief.focal_year || "n/a";
  }

  async function fetchJson(path, options = {}) {
    const url = `${state.apiBase}${path}`;
    const init = {
      headers: {
        "Content-Type": "application/json",
        ...(options.headers || {}),
      },
      ...options,
    };
    if (options.body && typeof options.body !== "string") {
      init.body = JSON.stringify(options.body);
    }

    const response = await fetch(url, init);
    if (!response.ok) {
      let detail = response.statusText;
      try {
        const json = await response.json();
        detail = json.detail || json.message || JSON.stringify(json);
      } catch {
        detail = await response.text();
      }
      throw new Error(`${response.status} ${detail}`.trim());
    }

    if (response.status === 204) {
      return null;
    }

    return response.json();
  }

  async function refreshRuntimeStatus() {
    try {
      state.runtimeStatus = await fetchJson(API.runtimeStatus);
    } catch (error) {
      console.warn("Could not refresh runtime status", error);
    }
  }

  function summarizeRuntime() {
    const runtime = state.runtimeStatus;
    if (!runtime) {
      return "";
    }

    const readyCount = runtime.services.filter((service) => service.ready).length;
    return `${readyCount}/${runtime.services.length} services ready. Extraction backend: ${runtime.extraction_backend}.`;
  }

  function renderReachability(value) {
    if (value === true) return "yes";
    if (value === false) return "no";
    return "n/a";
  }

  function titleize(value) {
    return String(value)
      .split(/\s+/)
      .filter(Boolean)
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
      .join(" ");
  }
})();
