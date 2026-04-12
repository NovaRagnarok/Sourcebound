(() => {
  const API = {
    sources: "/v1/sources",
    runs: "/v1/extraction-runs",
    pullSources: "/v1/ingest/zotero/pull",
    extractCandidates: "/v1/ingest/extract-candidates",
    candidates: "/v1/candidates",
    reviewCandidate: (candidateId) => `/v1/candidates/${candidateId}/review`,
    claims: "/v1/claims",
    query: "/v1/query",
  };

  const seed = window.SOURCEBOUND_SEED_DATA;
  const state = {
    apiBase: normalizeBase(window.SOURCEBOUND_API_BASE || ""),
    activeScreen: currentScreen(),
    apiOnline: false,
    lastSync: "Idle",
    banner: null,
    loading: false,
    sources: clone(seed.sources),
    evidence: clone(seed.evidence),
    candidates: clone(seed.candidates),
    claims: clone(seed.claims),
    runs: clone(seed.extractionRuns),
    selectedSourceId: seed.sources[0]?.source_id ?? null,
    selectedRunId: seed.extractionRuns[0]?.run_id ?? null,
    selectedCandidateId: seed.candidates.find((candidate) => candidate.review_state === "pending")
      ?.candidate_id ?? seed.candidates[0]?.candidate_id ?? null,
    selectedClaimId: seed.claims[0]?.claim_id ?? null,
    filters: {
      candidates: "pending",
      claims: "all",
    },
    query: {
      question: seed.queryPresets[0],
      mode: "strict_facts",
      status: "",
      claimKind: "",
      place: "",
      viewpoint: "",
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
    render();
  }

  function bindGlobalEvents() {
    window.addEventListener("hashchange", () => {
      state.activeScreen = currentScreen();
      render();
    });

    nodes.refreshAll.addEventListener("click", () => refreshLiveData());

    nodes.app.addEventListener("click", async (event) => {
      const sourceButton = event.target.closest("[data-select-source]");
      const runButton = event.target.closest("[data-select-run]");
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
        })
      );
    } catch (error) {
      console.warn("Could not persist operator state", error);
    }
  }

  function currentScreen() {
    const hash = window.location.hash.replace("#", "").trim();
    return screenConfig[hash] ? hash : "sources";
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
    nodes.modeBadge.className = online ? "status-pill status-pill-live" : "status-pill status-pill-muted";
    nodes.modeBadge.textContent = online ? "Live API" : "Seed fallback";
    nodes.connectionSummary.textContent = message;
    nodes.apiBase.textContent = state.apiBase || "Same origin";
  }

  async function refreshLiveData({ quiet = false } = {}) {
    applyLoading(true);
    if (!quiet) {
      setBanner("pending", "Refreshing", "Pulling live sources, runs, candidates, and claims from the API.");
    }

    try {
      const [sourcesResult, runsResult, candidatesResult, claimsResult] = await Promise.allSettled([
        fetchJson(API.sources),
        fetchJson(API.runs),
        fetchJson(API.candidates),
        fetchJson(API.claims),
      ]);

      if (sourcesResult.status === "fulfilled") {
        state.sources = sourcesResult.value;
      }

      if (runsResult.status === "fulfilled") {
        state.runs = runsResult.value;
      }

      if (candidatesResult.status === "fulfilled") {
        state.candidates = candidatesResult.value;
      }

      if (claimsResult.status === "fulfilled") {
        state.claims = claimsResult.value;
      }

      const online = [sourcesResult, runsResult, candidatesResult, claimsResult].some(
        (result) => result.status === "fulfilled"
      );
      setApiStatus(
        online,
        online ? "Live sources, runs, candidates, and claims loaded." : "Using seed data until the API responds."
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
            ? "Sources, runs, candidates, and claims refreshed from the backend."
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
    renderBanner();
    nodes.app.innerHTML = renderScreen();
    wireScreenTitle();
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
                      <div class="row-subtitle">${escapeHtml(run.note)}</div>
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
          <div class="detail-note">${escapeHtml(run.note)}</div>
        </div>
        <span class="pill ${escapeHtml(run.status)}">${escapeHtml(run.status)}</span>
      </div>
      <div class="inline-metrics">
        <span>${escapeHtml(run.source_count)} sources</span>
        <span>${escapeHtml(run.candidate_count)} candidates</span>
        <span>Started: ${escapeHtml(formatDate(run.started_at))}</span>
        <span>Finished: ${escapeHtml(run.finished_at ? formatDate(run.finished_at) : "n/a")}</span>
      </div>
      <div class="field">
        <label>Run note</label>
        <div>${escapeHtml(run.note)}</div>
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
})();
