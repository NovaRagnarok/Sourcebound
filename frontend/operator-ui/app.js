(() => {
  const API = {
    workspaceSummary: "/v1/workspace/summary",
    runtimeStatus: "/health/runtime",
    jobs: "/v1/jobs",
    job: (jobId) => `/v1/jobs/${jobId}`,
    cancelJob: (jobId) => `/v1/jobs/${jobId}/cancel`,
    retryJob: (jobId) => `/v1/jobs/${jobId}/retry`,
    bibleProfiles: "/v1/bible/profiles",
    bibleProfile: (projectId) => `/v1/bible/profiles/${projectId}`,
    bibleSections: "/v1/bible/sections",
    bibleSection: (sectionId) => `/v1/bible/sections/${sectionId}`,
    bibleSectionProvenance: (sectionId) => `/v1/bible/sections/${sectionId}/provenance`,
    regenerateBibleSection: (sectionId) => `/v1/bible/sections/${sectionId}/regenerate`,
    queueBibleExport: (projectId) => `/v1/bible/exports/${projectId}`,
    exportBibleProject: (projectId) => `/v1/bible/exports/${projectId}`,
    sources: "/v1/sources",
    source: (sourceId) => `/v1/sources/${sourceId}`,
    runs: "/v1/extraction-runs",
    researchRuns: "/v1/research/runs",
    researchRun: (runId) => `/v1/research/runs/${runId}`,
    stageResearchRun: (runId) => `/v1/research/runs/${runId}/stage`,
    extractResearchRun: (runId) => `/v1/research/runs/${runId}/extract`,
    researchPrograms: "/v1/research/programs",
    intakeText: "/v1/intake/text",
    intakeUrl: "/v1/intake/url",
    intakeFile: "/v1/intake/file",
    pullSources: "/v1/ingest/zotero/pull",
    normalizeDocuments: "/v1/ingest/normalize-documents",
    extractCandidates: "/v1/ingest/extract-candidates",
    candidates: "/v1/candidates",
    reviewQueue: "/v1/candidates/review-queue",
    reviewCandidate: (candidateId) => `/v1/candidates/${candidateId}/review`,
    claims: "/v1/claims",
    query: "/v1/query",
  };
  const STORAGE_KEY = "sourcebound.writer-workspace.state";
  const LEGACY_STORAGE_KEY = "sourcebound.operator.state";

  const seed = window.SOURCEBOUND_SEED_DATA;
  const knownScreens = new Set(["workspace", "sources", "research", "bible", "runs", "review", "claims", "ask"]);
  const advancedOnlyScreens = new Set(["sources", "runs", "claims"]);
  const knownQueryModes = new Set([
    "strict_facts",
    "contested_views",
    "rumor_and_legend",
    "character_knowledge",
    "open_exploration",
  ]);
  const writerQueryModes = new Set(["strict_facts", "contested_views", "rumor_and_legend"]);
  const state = {
    apiBase: normalizeBase(window.SOURCEBOUND_API_BASE || ""),
    activeScreen: currentScreen(),
    workspaceMode: "writer",
    apiOnline: false,
    apiStatusMessage: "",
    lastSync: "Idle",
    banner: null,
    loading: false,
    workspaceSummary: null,
    runtimeStatus: null,
    jobs: [],
    sources: clone(seed.sources),
    evidence: clone(seed.evidence),
    candidates: clone(seed.candidates),
    reviewQueue: clone(seed.reviewQueue || []),
    claims: clone(seed.claims),
    runs: clone(seed.extractionRuns),
    researchRuns: clone(seed.researchRuns || []),
    researchPrograms: clone(seed.researchPrograms || []),
    bible: {
      projectId: seed.bibleProfile?.project_id || "project-greyport",
      profile: clone(seed.bibleProfile || null),
      sections: clone(seed.bibleSections || []),
      selectedSectionId: seed.bibleSections?.[0]?.section_id ?? null,
      selectedParagraphId: null,
      selectedProvenance: null,
      exportBundle: null,
      exportJobId: null,
      draft: {
        section_type: "setting_overview",
        title: "",
        focus: seed.bibleProfile?.composition_defaults?.focus || "",
        statuses: (seed.bibleProfile?.composition_defaults?.include_statuses || ["verified", "probable"]).join(", "),
        source_types: (seed.bibleProfile?.composition_defaults?.source_types || []).join(", "),
        place: seed.bibleProfile?.geography || "",
        time_start: seed.bibleProfile?.time_start || "",
        time_end: seed.bibleProfile?.time_end || "",
      },
    },
    selectedSourceId: seed.sources[0]?.source_id ?? null,
    selectedSourceDetail: null,
    selectedRunId: seed.extractionRuns[0]?.run_id ?? null,
    selectedResearchRunId: seed.researchRuns?.[0]?.run_id ?? null,
    researchRunDetail: null,
    selectedCandidateId: getReviewCards(seed).find((candidate) => isUnresolvedReviewState(candidate.review_state))
      ?.candidate_id ?? getReviewCards(seed)[0]?.candidate_id ?? null,
    editingCandidateId: null,
    selectedClaimId: seed.claims[0]?.claim_id ?? null,
    filters: {
      candidates: "unresolved",
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
      projectId: seed.bibleProfile?.project_id || "project-greyport",
      status: "",
      claimKind: "",
      place: "",
      viewpoint: "",
    },
    intakeDraft: {
      mode: "text",
      title: "",
      text: "",
      author: "",
      year: "",
      url: "",
      notes: "",
      source_type: "document",
      collection_key: "",
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
    apiMeta: document.getElementById("api-meta"),
    lastSync: document.getElementById("last-sync"),
    modeBadge: document.getElementById("mode-badge"),
    modeToggle: document.getElementById("mode-toggle"),
    connectionSummary: document.getElementById("connection-summary"),
    refreshAll: document.getElementById("refresh-all"),
    advancedNav: document.getElementById("advanced-nav"),
    metricSources: document.getElementById("metric-sources"),
    metricLabelSources: document.getElementById("metric-label-sources"),
    metricPending: document.getElementById("metric-pending"),
    metricLabelPending: document.getElementById("metric-label-pending"),
    metricClaims: document.getElementById("metric-claims"),
    metricLabelClaims: document.getElementById("metric-label-claims"),
    metricEvidence: document.getElementById("metric-evidence"),
    metricLabelEvidence: document.getElementById("metric-label-evidence"),
    runtimePanel: document.getElementById("runtime-panel"),
    nav: Array.from(document.querySelectorAll("[data-nav]")),
  };

  const screenConfig = {
    workspace: {
      title: "Workspace",
      summary:
        "See the active writing project, what canon is ready, where the thin spots are, and what to do next.",
    },
    sources: {
      title: "Sources",
      summary:
        "Supporting utility for source intake, linked evidence, and staged material feeding extraction.",
    },
    runs: {
      title: "Background Work",
      summary:
        "Supporting workflow status for extraction, bible composition, export, and other jobs running behind the writing flow.",
    },
    research: {
      title: "Research",
      summary:
        "Fill a writing gap with a focused brief, then send the strongest accepted leads toward review.",
    },
    bible: {
      title: "Bible",
      summary:
        "Shape reviewed canon into editable notebook pages with visible trust boundaries, uncertainty, and provenance.",
    },
    review: {
      title: "Review New Facts",
      summary:
        "Approve or reject candidate facts before they cross the canon trust boundary.",
    },
    claims: {
      title: "Canon Ledger",
      summary:
        "Supporting utility for inspecting reviewed claims and their evidence links in detail.",
    },
    ask: {
      title: "Ask Canon",
      summary:
        "Pressure-test the approved record for the current project without crossing the trust boundary.",
    },
  };

  boot().catch((error) => {
    console.error(error);
    setBanner("failed", "Startup failed", error.message || "The writer workspace could not initialize.");
  });

  async function boot() {
    bindGlobalEvents();
    hydrateFromStorage();
    syncWorkspaceView();
    replaceHash(state.activeScreen);
    await refreshLiveData({ quiet: true });
    if (state.bible.projectId) {
      await refreshBibleWorkspace({ quiet: true, projectId: state.bible.projectId });
    }
    if (state.activeScreen === "research" && state.selectedResearchRunId) {
      await refreshResearchDetail(state.selectedResearchRunId, { quiet: true });
    }
    render();
  }

  function bindGlobalEvents() {
    window.addEventListener("hashchange", () => {
      state.activeScreen = normalizeScreen(currentScreen(), state.workspaceMode);
      replaceHash(state.activeScreen);
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
    nodes.modeToggle.addEventListener("click", () => {
      state.workspaceMode = state.workspaceMode === "writer" ? "advanced" : "writer";
      syncWorkspaceView();
      replaceHash(state.activeScreen);
      persistState();
      render();
    });

    nodes.app.addEventListener("click", async (event) => {
      const sourceButton = event.target.closest("[data-select-source]");
      const runButton = event.target.closest("[data-select-run]");
      const researchRunButton = event.target.closest("[data-select-research-run]");
      const bibleSectionButton = event.target.closest("[data-select-bible-section]");
      const bibleParagraphButton = event.target.closest("[data-select-bible-paragraph]");
      const candidateButton = event.target.closest("[data-select-candidate]");
      const claimButton = event.target.closest("[data-select-claim]");
      const actionButton = event.target.closest("[data-action]");
      const presetButton = event.target.closest("[data-preset]");
      const filterButton = event.target.closest("[data-filter]");
      const queryModeButton = event.target.closest("[data-query-mode]");

      if (sourceButton) {
        state.selectedSourceId = sourceButton.dataset.selectSource;
        render();
        refreshSourceDetail(state.selectedSourceId, { quiet: true });
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

      if (bibleSectionButton) {
        state.bible.selectedSectionId = bibleSectionButton.dataset.selectBibleSection;
        state.bible.selectedParagraphId = null;
        await loadBibleProvenance(state.bible.selectedSectionId);
        persistState();
        render();
        return;
      }

      if (bibleParagraphButton) {
        state.bible.selectedParagraphId = bibleParagraphButton.dataset.selectBibleParagraph;
        await loadBibleProvenance(state.bible.selectedSectionId);
        persistState();
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
        state.query.mode = normalizeQueryMode(queryModeButton.dataset.queryMode, state.workspaceMode);
        persistState();
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

      if (action === "repull-source") {
        const selectedSource = state.sources.find((source) => source.source_id === state.selectedSourceId);
        if (!selectedSource || selectedSource.external_source !== "zotero" || !selectedSource.zotero_item_key) {
          setBanner("pending", "Local source", "This source lives only in the local workspace, so there is nothing to re-pull from Zotero.");
          render();
          return;
        }
        await pullSources({ sourceIds: [state.selectedSourceId], forceRefresh: true });
        return;
      }

      if (action === "retry-source-normalization") {
        await retrySourceNormalization(state.selectedSourceId);
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

      if (action === "refresh-live-data") {
        await refreshLiveData();
        return;
      }

      if (action === "copy-command") {
        const command = actionButton.dataset.command || "";
        const copied = await copyTextToClipboard(command);
        setBanner(
          copied ? "live" : "queued",
          copied ? "Command copied" : "Command ready",
          copied
            ? `${command} is on your clipboard. Run it in the repo root, then refresh the workspace.`
            : command
        );
        render();
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

      if (action === "export-bible") {
        await exportBibleProject();
        return;
      }

      if (action === "cancel-job") {
        await cancelJob(actionButton.dataset.jobId);
        return;
      }

      if (action === "retry-job") {
        await retryJob(actionButton.dataset.jobId);
        return;
      }

      if (action === "regenerate-bible-section") {
        await regenerateBibleSection(state.bible.selectedSectionId);
        return;
      }

      if (action === "launch-gap-research") {
        seedResearchFromCurrentGap();
        state.activeScreen = "research";
        location.hash = "#research";
        persistState();
        render();
        return;
      }

      if (action === "compose-from-query") {
        state.bible.draft.focus = state.query.question || state.queryResult?.question || "";
        state.bible.draft.place = state.query.place || state.bible.profile?.geography || "";
        state.activeScreen = "bible";
        location.hash = "#bible";
        persistState();
        render();
        return;
      }

      if (action === "compose-from-claim") {
        const claim = state.claims.find((item) => item.claim_id === state.selectedClaimId);
        state.bible.draft.focus = claim ? `${claim.subject} ${claim.value}` : "";
        state.bible.draft.place = claim?.place || state.bible.profile?.geography || "";
        state.activeScreen = "bible";
        location.hash = "#bible";
        persistState();
        render();
        return;
      }

      if (action === "compose-from-research") {
        const run = state.researchRuns.find((item) => item.run_id === state.selectedResearchRunId);
        state.bible.draft.focus = run?.brief?.topic || "";
        state.activeScreen = "bible";
        location.hash = "#bible";
        persistState();
        render();
        return;
      }

      if (action === "toggle-candidate-context") {
        const candidateId = actionButton.dataset.candidateId;
        state.selectedCandidateId = state.selectedCandidateId === candidateId ? null : candidateId;
        if (state.editingCandidateId && state.editingCandidateId !== state.selectedCandidateId) {
          state.editingCandidateId = null;
        }
        persistState();
        render();
        return;
      }

      if (action === "edit-candidate-review") {
        const candidateId = actionButton.dataset.candidateId;
        state.selectedCandidateId = candidateId;
        state.editingCandidateId = candidateId;
        persistState();
        render();
        return;
      }

      if (action === "cancel-candidate-edit") {
        state.editingCandidateId = null;
        persistState();
        render();
        return;
      }

      // Submit actions are handled by the form submit listener to avoid duplicate posts.
    });

    nodes.app.addEventListener("submit", async (event) => {
      if (event.target.dataset.form === "intake-source") {
        event.preventDefault();
        await submitIntakeSource(event.target);
      }

      if (event.target.dataset.form === "review") {
        event.preventDefault();
        await submitReview(event.target, event.submitter);
      }

      if (event.target.dataset.form === "query") {
        event.preventDefault();
        await submitQuery(event.target);
      }

      if (event.target.dataset.form === "research-run") {
        event.preventDefault();
        await submitResearchRun(event.target);
      }

      if (event.target.dataset.form === "bible-profile") {
        event.preventDefault();
        await submitBibleProfile(event.target);
      }

      if (event.target.dataset.form === "bible-section") {
        event.preventDefault();
        await submitBibleSection(event.target);
      }

      if (event.target.dataset.form === "bible-section-edit") {
        event.preventDefault();
        await submitBibleSectionEdit(event.target);
      }
    });

    nodes.app.addEventListener("change", (event) => {
      const intakeModeSelect = event.target.closest("[data-intake-mode]");
      if (intakeModeSelect) {
        state.intakeDraft.mode = intakeModeSelect.value || "text";
        persistState();
        render();
        return;
      }

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
      const raw = localStorage.getItem(STORAGE_KEY) || localStorage.getItem(LEGACY_STORAGE_KEY);
      if (!raw) {
        return;
      }
      const saved = JSON.parse(raw);
      if (saved.query) {
        state.query = { ...state.query, ...saved.query };
      }
      if (saved.workspaceMode) {
        state.workspaceMode = saved.workspaceMode;
      }
      if (saved.filters) {
        state.filters = { ...state.filters, ...saved.filters };
      }
      if (Array.isArray(saved.runs) && saved.runs.length) {
        state.runs = saved.runs;
      }
      if (saved.intakeDraft) {
        state.intakeDraft = { ...state.intakeDraft, ...saved.intakeDraft };
      }
      if (saved.researchDraft) {
        state.researchDraft = { ...state.researchDraft, ...saved.researchDraft };
      }
      if (saved.selectedResearchRunId) {
        state.selectedResearchRunId = saved.selectedResearchRunId;
      }
      if (saved.bible) {
        state.bible = {
          ...state.bible,
          ...saved.bible,
          draft: { ...state.bible.draft, ...(saved.bible.draft || {}) },
        };
      }
    } catch (error) {
      console.warn("Could not hydrate workspace state", error);
    }
  }

  function persistState() {
    try {
      localStorage.setItem(
        STORAGE_KEY,
        JSON.stringify({
          query: state.query,
          workspaceMode: state.workspaceMode,
          filters: state.filters,
          runs: state.runs,
          intakeDraft: state.intakeDraft,
          researchDraft: state.researchDraft,
          selectedResearchRunId: state.selectedResearchRunId,
          bible: state.bible,
        })
      );
      localStorage.removeItem(LEGACY_STORAGE_KEY);
    } catch (error) {
      console.warn("Could not persist workspace state", error);
    }
  }

  function currentScreen() {
    const hash = window.location.hash.replace("#", "").trim();
    return knownScreens.has(hash) ? hash : "workspace";
  }

  function normalizeWorkspaceMode(mode) {
    return mode === "advanced" ? "advanced" : "writer";
  }

  function normalizeQueryMode(mode, workspaceMode = state.workspaceMode) {
    if (!knownQueryModes.has(mode)) {
      return "strict_facts";
    }
    if (workspaceMode !== "advanced" && !writerQueryModes.has(mode)) {
      return "strict_facts";
    }
    return mode;
  }

  function normalizeScreen(screen, workspaceMode = state.workspaceMode) {
    if (!knownScreens.has(screen)) {
      return "workspace";
    }
    if (workspaceMode !== "advanced" && advancedOnlyScreens.has(screen)) {
      return "workspace";
    }
    return screen;
  }

  function syncWorkspaceView() {
    state.workspaceMode = normalizeWorkspaceMode(state.workspaceMode);
    state.query.mode = normalizeQueryMode(state.query.mode, state.workspaceMode);
    state.activeScreen = normalizeScreen(state.activeScreen, state.workspaceMode);
  }

  function replaceHash(screen) {
    const target = `#${screen}`;
    if (!window.location.hash && screen === "workspace") {
      return;
    }
    if (window.location.hash === target) {
      return;
    }
    window.history.replaceState(
      window.history.state,
      "",
      `${window.location.pathname}${window.location.search}${target}`
    );
  }

  function normalizeBase(base) {
    if (!base) return "";
    return base.endsWith("/") ? base.slice(0, -1) : base;
  }

  function clone(value) {
    return JSON.parse(JSON.stringify(value));
  }

  function seedResearchFromCurrentGap() {
    const selectedSection =
      state.bible.sections.find((section) => section.section_id === state.bible.selectedSectionId)
      ?? state.bible.sections[0]
      ?? null;
    const gapPrompt = selectedSection?.recommended_next_research?.[0]
      || state.workspaceSummary?.current_section?.recommended_next_research?.[0]
      || selectedSection?.coverage_gaps?.[0]
      || state.workspaceSummary?.current_section?.coverage_gaps?.[0]
      || state.bible.profile?.narrative_focus
      || "";
    state.researchDraft.topic = gapPrompt;
    state.researchDraft.locale = selectedSection?.generation_filters?.place || state.bible.profile?.geography || state.researchDraft.locale;
    state.researchDraft.audience = state.bible.profile?.social_lens || state.researchDraft.audience;
    state.researchDraft.time_start = selectedSection?.generation_filters?.time_start || state.bible.profile?.time_start || state.researchDraft.time_start;
    state.researchDraft.time_end = selectedSection?.generation_filters?.time_end || state.bible.profile?.time_end || state.researchDraft.time_end;
    state.researchDraft.desired_facets = (state.bible.profile?.desired_facets || []).join(", ");
    state.researchDraft.domain_hints = selectedSection?.title || state.bible.profile?.narrative_focus || state.researchDraft.domain_hints;
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

  async function copyTextToClipboard(text) {
    if (!text) {
      return false;
    }

    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(text);
        return true;
      }
    } catch (error) {
      // Fall back to a temporary textarea for browsers that reject clipboard writes.
    }

    const probe = document.createElement("textarea");
    probe.value = text;
    probe.setAttribute("readonly", "");
    probe.style.position = "absolute";
    probe.style.left = "-9999px";
    document.body.appendChild(probe);
    probe.select();
    const copied = document.execCommand("copy");
    document.body.removeChild(probe);
    return copied;
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
    state.apiStatusMessage = message;
    nodes.apiBase.textContent = state.apiBase || "Same origin";
  }

  async function refreshLiveData({ quiet = false } = {}) {
    applyLoading(true);
    if (!quiet) {
      setBanner("pending", "Refreshing", "Pulling the current project, review queue, bible workspace, research runs, and supporting utilities from the API.");
    }

    try {
      const [workspaceSummaryResult, sourcesResult, researchRunsResult, researchProgramsResult, runsResult, candidatesResult, reviewQueueResult, claimsResult, runtimeResult, bibleProfilesResult, jobsResult] =
        await Promise.allSettled([
          fetchJson(API.workspaceSummary),
          fetchJson(API.sources),
          fetchJson(API.researchRuns),
          fetchJson(API.researchPrograms),
          fetchJson(API.runs),
          fetchJson(API.candidates),
          fetchJson(API.reviewQueue),
          fetchJson(API.claims),
          fetchJson(API.runtimeStatus),
          fetchJson(API.bibleProfiles),
          fetchJson(API.jobs),
        ]);

      if (workspaceSummaryResult.status === "fulfilled") {
        state.workspaceSummary = workspaceSummaryResult.value;
      }

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

      if (reviewQueueResult.status === "fulfilled") {
        state.reviewQueue = reviewQueueResult.value;
      }

      if (claimsResult.status === "fulfilled") {
        state.claims = claimsResult.value;
      }

      if (runtimeResult?.status === "fulfilled") {
        state.runtimeStatus = runtimeResult.value;
      }

      if (jobsResult?.status === "fulfilled") {
        state.jobs = jobsResult.value;
      }

      if (bibleProfilesResult?.status === "fulfilled") {
        const profiles = bibleProfilesResult.value;
        if (profiles.length) {
          state.bible.projectId = state.bible.projectId || profiles[0].project_id;
          await refreshBibleWorkspace({ quiet: true, projectId: state.bible.projectId });
        }
      }

      const online = [workspaceSummaryResult, sourcesResult, researchRunsResult, researchProgramsResult, runsResult, candidatesResult, reviewQueueResult, claimsResult, runtimeResult, bibleProfilesResult, jobsResult].some(
        (result) => result.status === "fulfilled"
      );
      const runtimeSummary = summarizeRuntime();
      setApiStatus(
        online,
        online
          ? runtimeSummary || "Live project, bible workspace, review queue, research runs, and utilities loaded."
          : "Using seed data until the API responds."
      );
      updateMetrics();
      updateSelectionFallbacks();
      if (state.selectedSourceId) {
        await refreshSourceDetail(state.selectedSourceId, { quiet: true });
      }
      state.lastSync = new Date().toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
      nodes.lastSync.textContent = state.lastSync;
      if (!quiet) {
        setBanner(
          online ? "live" : "queued",
          online ? "Live data synced" : "Seed data retained",
          online
            ? "Project summary, bible workspace, research runs, review queue, and utilities refreshed from the backend."
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
      state.selectedSourceDetail?.source?.source_id &&
      !state.sources.some((source) => source.source_id === state.selectedSourceDetail.source.source_id)
    ) {
      state.selectedSourceDetail = null;
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

    const reviewCards = state.reviewQueue.length ? state.reviewQueue : state.candidates;
    if (
      state.selectedCandidateId &&
      !reviewCards.some((candidate) => candidate.candidate_id === state.selectedCandidateId)
    ) {
      state.selectedCandidateId = null;
    }

    if (!state.selectedCandidateId && reviewCards.length) {
      state.selectedCandidateId = reviewCards.find((candidate) => isUnresolvedReviewState(candidate.review_state))
        ?.candidate_id ?? reviewCards[0].candidate_id;
    }

    if (
      state.editingCandidateId &&
      !reviewCards.some((candidate) => candidate.candidate_id === state.editingCandidateId)
    ) {
      state.editingCandidateId = null;
    }

    if (!state.selectedClaimId && state.claims.length) {
      state.selectedClaimId = state.claims[0].claim_id;
    }

    if (
      state.bible.selectedSectionId &&
      !state.bible.sections.some((section) => section.section_id === state.bible.selectedSectionId)
    ) {
      state.bible.selectedSectionId = null;
    }

    if (!state.bible.selectedSectionId && state.bible.sections.length) {
      state.bible.selectedSectionId = state.bible.sections[0].section_id;
    }
  }

  function updateMetrics() {
    const pendingCount = state.workspaceSummary?.pending_review_count
      ?? state.candidates.filter((candidate) => isUnresolvedReviewState(candidate.review_state)).length;
    nodes.metricLabelSources.textContent = "Bible sections";
    nodes.metricSources.textContent = String(state.workspaceSummary?.bible_section_count ?? state.bible.sections.length);
    nodes.metricLabelPending.textContent = "Needs review";
    nodes.metricPending.textContent = String(pendingCount);
    nodes.metricLabelClaims.textContent = "Reviewed canon";
    nodes.metricClaims.textContent = String(state.workspaceSummary?.reviewed_canon_count ?? state.claims.length);
    nodes.metricLabelEvidence.textContent = "Evidence snippets";
    nodes.metricEvidence.textContent = String(state.workspaceSummary?.evidence_count ?? state.evidence.length);
  }

  function render() {
    renderChrome();
    updateNavigation();
    updateHeader();
    updateMetrics();
    renderRuntimeOverview();
    renderBanner();
    nodes.app.innerHTML = renderScreen();
    wireScreenTitle();
  }

  function renderChrome() {
    const isAdvanced = state.workspaceMode === "advanced";
    document.body.dataset.workspaceMode = state.workspaceMode;
    nodes.advancedNav.hidden = !isAdvanced;
    nodes.apiMeta.hidden = !isAdvanced;
    nodes.modeBadge.className = isAdvanced ? "status-pill status-pill-warning" : "status-pill status-pill-live";
    nodes.modeBadge.textContent = isAdvanced ? "Advanced mode" : "Writer mode";
    nodes.modeToggle.textContent = isAdvanced ? "Hide advanced tools" : "Show advanced tools";
    if (isAdvanced) {
      nodes.connectionSummary.textContent = state.apiStatusMessage || "Advanced utilities are visible for auditing, troubleshooting, and policy controls.";
    } else if (!state.apiOnline) {
      nodes.connectionSummary.textContent = "Live data is unavailable right now. The workspace stays usable with seed data.";
    } else {
      nodes.connectionSummary.textContent = "Writer mode keeps the daily path focused on research, review, bible work, and asking canon.";
    }
  }

  function renderRuntimeOverview() {
    const runtimeNeedsAttention =
      state.runtimeStatus && state.runtimeStatus.overall_status !== "ready";
    if (
      state.activeScreen !== "workspace"
      || (state.workspaceMode !== "advanced" && !runtimeNeedsAttention)
    ) {
      nodes.runtimePanel.innerHTML = "";
      return;
    }
    if (!state.runtimeStatus) {
      nodes.runtimePanel.innerHTML = `
        <div class="runtime-shell">
          <div class="runtime-summary">
            <p class="runtime-kicker">Runtime overview</p>
            <h2>Waiting for backend status</h2>
            <p>The workspace is still usable with seed data, but the runtime report has not loaded yet.</p>
          </div>
          <div class="helper">The API can expose a live readiness report at <code>/health/runtime</code>.</div>
        </div>
      `;
      return;
    }

    const readyCount = state.runtimeStatus.services.filter((service) => service.ready).length;
    const overallTone = runtimeStatusTone(state.runtimeStatus.overall_status);
    const summaryLine = summarizeRuntime() || "Runtime details are available.";

    nodes.runtimePanel.innerHTML = `
      <div class="runtime-shell">
        <div class="runtime-summary">
          <p class="runtime-kicker">Runtime overview</p>
          <h2>${escapeHtml(titleize(state.runtimeStatus.overall_status.replaceAll("_", " ")))}</h2>
          <p>${escapeHtml(summaryLine)}</p>
          <div class="service-meta">
            <span class="pill ${escapeHtml(overallTone)}">${escapeHtml(titleize(state.runtimeStatus.overall_status.replaceAll("_", " ")))} runtime</span>
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
              const tone = runtimeServiceTone(service);
              const statusLabel = runtimeServiceLabel(service);
              return `
                <article class="service-card">
                  <div class="service-head">
                    <div>
                      <strong>${escapeHtml(titleize(service.name.replaceAll("_", " ")))}</strong>
                      <div class="detail-note">${escapeHtml(service.role)}</div>
                    </div>
                    <span class="pill ${escapeHtml(tone)}">${escapeHtml(statusLabel)}</span>
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
      case "workspace":
        return renderWorkspaceScreen();
      case "research":
        return renderResearchScreen();
      case "bible":
        return renderBibleScreen();
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

  function renderWorkspaceScreen() {
    const advanced = state.workspaceMode === "advanced";
    const profile = state.workspaceSummary?.project || state.bible.profile;
    const runtimeBlocked = state.runtimeStatus && state.runtimeStatus.overall_status !== "ready";
    const setupMode = !profile && !!runtimeBlocked;
    const coverage = buildBibleCoverage(profile, state.bible.sections, state.claims);
    const fallbackSection =
      state.bible.sections.find((section) => section.section_id === state.bible.selectedSectionId) ??
      state.bible.sections[0] ??
      null;
    const selectedSection = state.workspaceSummary?.current_section || (
      fallbackSection
        ? {
            section_id: fallbackSection.section_id,
            title: fallbackSection.title,
            generation_status: fallbackSection.generation_status,
            ready_for_writer: fallbackSection.ready_for_writer,
            has_manual_edits: fallbackSection.has_manual_edits,
            claim_count: fallbackSection.references.claim_ids.length,
            source_count: fallbackSection.references.source_ids.length,
            summary: fallbackSection.coverage_gaps?.[0] || renderBibleGenerationSummary(fallbackSection),
            coverage_gaps: fallbackSection.coverage_gaps || [],
            recommended_next_research: fallbackSection.recommended_next_research || [],
          }
        : null
    );
    const pendingCandidates = state.candidates.filter((candidate) => isUnresolvedReviewState(candidate.review_state));
    const thinCoverage = coverage.filter((item) => item.summary !== "ready");
    const researchRunsNeedingAttention = state.researchRuns.filter((run) =>
      ["queued", "running", "partial"].includes(run.status)
    );
    const writerActions = state.workspaceSummary?.next_actions?.length
      ? state.workspaceSummary.next_actions
      : buildWorkspaceActions({
          profile,
          pendingCandidates,
          thinCoverage,
          selectedSection: fallbackSection,
          researchRunsNeedingAttention,
        }).map((item, index) => ({
          action_id: `fallback-${index}`,
          title: item.title,
          summary: item.summary,
          screen: item.href,
          tone: item.tone,
          badge: item.badge,
        }));
    const visibleWriterActions = advanced
      ? writerActions
      : writerActions.filter((item) => !advancedOnlyScreens.has(item.screen));
    const activeJobs = state.workspaceSummary?.background_items?.length
      ? state.workspaceSummary.background_items
      : collectWorkspaceJobs().map((item) => ({
          item_id: item.job.job_id,
          title: item.label,
          summary: item.summary,
          status_label: item.job.status_label || item.job.status,
          screen: "runs",
        }));
    const projectPeriod =
      profile?.era || [profile?.time_start, profile?.time_end].filter(Boolean).join(" to ") || "Not set";
    const nextMove = visibleWriterActions[0];
    const sampleProjectLoaded = isSeededSampleProject(profile);
    const setupChecklist = buildWorkspaceSetupChecklist(profile);
    const remainingSetupSteps = setupChecklist.filter((item) => !item.ready).length;
    const workspaceTitle = setupMode
      ? "Finish the local stack setup"
      : (profile?.project_name || "Set up your first writing project");
    const workspaceLead = setupMode
      ? "Bring Postgres and Qdrant online, seed the sample project, and the workspace will shift into the normal research-review-compose loop."
      : sampleProjectLoaded
        ? (
            profile?.narrative_focus ||
            "The default Rouen Winter sample is live. Review canon, inspect the seeded bible, and use the workspace to learn the intended research-review-compose loop."
          )
      : (
          profile?.narrative_focus ||
          "Define the project focus, review canon, and turn evidence-backed material into usable notebook pages."
        );
    const heroEyebrow = setupMode
      ? "First run"
      : (sampleProjectLoaded ? "Sample project loaded" : "Active writing project");
    const heroActionMarkup = renderWorkspaceHeroActions({
      setupMode,
      sampleProjectLoaded,
      pendingCandidates,
      thinCoverage,
      selectedSection,
      researchRunsNeedingAttention,
    });
    const actionHeading = setupMode ? "First-run checklist" : "Next actions";
    const actionLead = setupMode
      ? "Run these in order for the default Postgres + Qdrant stack. The workspace will settle into the normal loop once the sample project appears."
      : "The shortest route to stronger pages is research, review, compose, then edit.";
    const placeSummary = setupMode ? "Rouen sample project" : (profile?.geography || "Not set");
    const eraSummary = setupMode ? "Postgres + Qdrant default path" : projectPeriod;
    const attentionSummary = setupMode
      ? `${remainingSetupSteps} ${remainingSetupSteps === 1 ? "step" : "steps"} left`
      : (nextMove?.badge || `${state.workspaceSummary?.pending_review_count ?? pendingCandidates.length} waiting`);

    return `
      <article class="screen fade-in">
        <section class="workspace-hero">
          <div class="workspace-hero-copy">
            <p class="eyebrow">${escapeHtml(heroEyebrow)}</p>
            <h2 data-active-screen>${escapeHtml(workspaceTitle)}</h2>
            <p class="workspace-lead">
              ${escapeHtml(workspaceLead)}
            </p>
            ${
              nextMove
                ? `
                  <div class="workspace-next-move">
                    <span class="rail-label">Recommended next move</span>
                    <strong>${escapeHtml(nextMove.title)}</strong>
                    <div class="detail-note">${escapeHtml(nextMove.summary)}</div>
                  </div>
                `
                : ""
            }
            <div class="workspace-hero-actions">${heroActionMarkup}</div>
            ${setupMode ? renderWorkspaceSetupChecklist(setupChecklist) : ""}
          </div>
          <div class="workspace-hero-meta">
            <div class="workspace-keyline">
              <span>Place</span>
              <strong>${escapeHtml(placeSummary)}</strong>
            </div>
            <div class="workspace-keyline">
              <span>Era</span>
              <strong>${escapeHtml(eraSummary)}</strong>
            </div>
            <div class="workspace-keyline">
              <span>Trust boundary</span>
              <strong>Review gates canon; manual text stays separate</strong>
            </div>
            <div class="workspace-keyline">
              <span>What needs attention</span>
              <strong>${escapeHtml(attentionSummary)}</strong>
            </div>
          </div>
        </section>

        <section class="workspace-grid">
          <div class="detail workspace-panel-primary">
            <div class="detail-head">
              <div>
                <h3>${escapeHtml(actionHeading)}</h3>
                <div class="detail-note">${escapeHtml(actionLead)}</div>
              </div>
              <span class="pill probable">${escapeHtml(writerActions.length)}</span>
            </div>
            <div class="workspace-action-list">
              ${visibleWriterActions.map((item) => renderWorkspaceActionCard(item, { setupMode })).join("")}
            </div>
          </div>

          <div class="detail">
            <div class="detail-head">
              <div>
                <h3>Canon readiness</h3>
                <div class="detail-note">One glance at what is strong, thin, or still risky.</div>
              </div>
              <span class="pill ${thinCoverage.length ? "contested" : "verified"}">${thinCoverage.length ? "gaps visible" : "usable"}</span>
            </div>
            <div class="detail-list">
              ${coverage
                .map(
                  (item) => `
                    <div class="mini">
                      <div class="toolbar">
                        <strong>${escapeHtml(item.label)}</strong>
                        <span class="pill ${escapeHtml(item.tone)}">${escapeHtml(item.summary)}</span>
                      </div>
                      <div class="detail-note">${escapeHtml(item.detail)}</div>
                    </div>
                  `
                )
                .join("")}
            </div>
          </div>
        </section>

        <section class="workspace-grid">
          <div class="detail">
            <div class="detail-head">
              <div>
                <h3>Current bible section</h3>
                <div class="detail-note">Generated canon stays auditable; editable text stays clearly separate.</div>
              </div>
              <a class="secondary-button" href="#bible">Open Bible</a>
            </div>
              ${
                selectedSection
                  ? `
                  <div class="inline-metrics">
                    <span>${escapeHtml(selectedSection.claim_count)} claims</span>
                    <span>${escapeHtml(selectedSection.source_count)} sources</span>
                    <span>${escapeHtml(selectedSection.ready_for_writer ? "writer-ready" : "needs support")}</span>
                  </div>
                  <div class="workspace-section-preview">
                    <h4>${escapeHtml(selectedSection.title)}</h4>
                    <p>${escapeHtml(selectedSection.summary)}</p>
                  </div>
                  <div class="detail-list">
                    <div class="mini">
                      <div class="toolbar">
                        <strong>Generated baseline</strong>
                        <span class="pill ${escapeHtml(renderBibleGenerationTone(selectedSection.generation_status))}">${escapeHtml(selectedSection.ready_for_writer ? "writer-ready" : "thin baseline")}</span>
                      </div>
                      <div class="detail-note">${escapeHtml(selectedSection.has_manual_edits ? "Manual text is already in play for this section." : "The current section is still mostly generated." )}</div>
                    </div>
                    ${
                      (selectedSection.coverage_gaps || []).length
                        ? selectedSection.coverage_gaps
                            .slice(0, 3)
                            .map((item) => `<div class="warning">${escapeHtml(item)}</div>`)
                            .join("")
                        : ((selectedSection.recommended_next_research || []).length
                            ? selectedSection.recommended_next_research
                                .slice(0, 2)
                                .map((item) => `<div class="warning">${escapeHtml(item)}</div>`)
                                .join("")
                            : "<div class='helper'>No coverage gaps recorded for the selected section.</div>")
                    }
                  </div>
                `
                : "<div class='helper'>No bible section exists yet. Compose one once reviewed canon is ready.</div>"
            }
          </div>

          <div class="detail">
            <div class="detail-head">
              <div>
                <h3>Workflow status</h3>
                <div class="detail-note">Jobs stay visible here as support, not the main event.</div>
              </div>
              ${advanced ? '<a class="secondary-button" href="#runs">Open utilities</a>' : ""}
            </div>
            <div class="detail-list">
              ${
                activeJobs.length
                  ? activeJobs
                      .slice(0, 5)
                      .map(
                        (item) => `
                          <div class="mini">
                            <div class="toolbar">
                              <strong>${escapeHtml(item.title)}</strong>
                              <span class="pill probable">${escapeHtml(item.status_label)}</span>
                            </div>
                            <div class="detail-note">${escapeHtml(item.summary)}</div>
                          </div>
                        `
                      )
                      .join("")
                  : "<div class='helper'>No active or recent jobs need attention.</div>"
              }
            </div>
          </div>
        </section>

        <section class="workspace-grid">
          ${renderIntakePanel({ surface: "workspace" })}
          <div class="detail">
            <div class="detail-head">
              <div>
                <h3>Source Intake Loop</h3>
                <div class="detail-note">A real onboarding path should end in review, not in a dead-end import log.</div>
              </div>
              <span class="pill probable">${escapeHtml((state.reviewQueue || []).length || state.candidates.length)}</span>
            </div>
            <div class="detail-list">
              <div class="mini">
                <div class="toolbar">
                  <strong>1. Add source material</strong>
                  <span class="pill queued">intake</span>
                </div>
                <div class="detail-note">Paste text is the fastest first-run path. File upload works best with text-like files right now.</div>
              </div>
              <div class="mini">
                <div class="toolbar">
                  <strong>2. Normalize and extract</strong>
                  <span class="pill probable">automatic</span>
                </div>
                <div class="detail-note">Sourcebound will queue text units and extract candidate facts only for the newly added source.</div>
              </div>
              <div class="mini">
                <div class="toolbar">
                  <strong>3. Review before canon</strong>
                  <span class="pill verified">trust boundary</span>
                </div>
                <div class="detail-note">The loop lands you in Review so nothing enters canon without a human decision.</div>
              </div>
            </div>
          </div>
        </section>
      </article>
    `;
  }

  function buildWorkspaceActions({ profile, pendingCandidates, thinCoverage, selectedSection, researchRunsNeedingAttention }) {
    const actions = [];
    if (!profile?.project_name) {
      actions.push({
        title: "Set the active project profile",
        summary: "Add place, era, and narrative focus so research and composition aim at the same book.",
        href: "bible",
        tone: "queued",
        badge: "setup",
      });
    }
    if (pendingCandidates.length) {
      actions.push({
        title: "Review pending canon candidates",
        summary: `${pendingCandidates.length} extracted claims are waiting at the trust boundary before they can feed the bible.`,
        href: "review",
        tone: "probable",
        badge: `${pendingCandidates.length} pending`,
      });
    }
    if (thinCoverage.length) {
      actions.push({
        title: "Close the thinnest research gaps",
        summary: `${thinCoverage[0].label} is still thin. Run research before the next compose pass to get fuller scene material.`,
        href: "research",
        tone: "contested",
        badge: `${thinCoverage.length} thin`,
      });
    }
    if (selectedSection) {
      actions.push({
        title: "Regenerate or edit the live bible section",
        summary: `${selectedSection.title} already has canon attached. Refresh the generated draft or continue manual shaping without losing provenance.`,
        href: "bible",
        tone: selectedSection.has_manual_edits ? "author_choice" : "verified",
        badge: selectedSection.has_manual_edits ? "manual text" : "ready",
      });
    } else {
      actions.push({
        title: "Compose the first bible section",
        summary: "Turn reviewed canon into a writer-facing section with visible uncertainty and provenance.",
        href: "bible",
        tone: "queued",
        badge: "compose",
      });
    }
    if (researchRunsNeedingAttention.length) {
      actions.push({
        title: "Check in-flight research",
        summary: `${researchRunsNeedingAttention.length} research run${researchRunsNeedingAttention.length === 1 ? "" : "s"} still need staging, extraction, or review follow-through.`,
        href: "research",
        tone: "queued",
        badge: "running",
      });
    }
    if (!actions.length) {
      actions.push({
        title: "Ask canon a drafting question",
        summary: "The project is in a healthy state. Use Ask to pressure-test the approved record before drafting a scene.",
        href: "ask",
        tone: "verified",
        badge: "ready",
      });
    }
    return actions.slice(0, 4);
  }

  function isSeededSampleProject(profile) {
    return profile?.project_id === "project-rouen-winter";
  }

  function buildWorkspaceSetupChecklist(profile) {
    const services = Object.fromEntries(
      (state.runtimeStatus?.services || []).map((service) => [service.name, service])
    );
    const projectionMode = services.projection?.mode || "";
    const postgresReady = Boolean(services.app_state?.ready) && Boolean(services.truth_store?.ready);
    const qdrantReady = ["qdrant:uninitialized", "qdrant:ready"].includes(projectionMode);
    const sampleReady = Boolean(profile) || projectionMode === "qdrant:ready";

    return [
      {
        step: "1",
        title: "Start Postgres",
        summary: "Bring workflow state and reviewed canon online before the rest of the onboarding path.",
        command: "docker compose up -d postgres",
        ready: postgresReady,
        tone: postgresReady ? "verified" : "queued",
        badge: postgresReady ? "ready" : "required",
      },
      {
        step: "2",
        title: "Start Qdrant",
        summary: "Boot the default retrieval service so Sourcebound can initialize the projection-backed path.",
        command: "docker compose up -d qdrant",
        ready: qdrantReady,
        tone: qdrantReady ? "verified" : "queued",
        badge: qdrantReady ? "ready" : (postgresReady ? "next" : "after Postgres"),
      },
      {
        step: "3",
        title: "Seed dev data",
        summary: "Load the Rouen Winter sample project and initialize the default newcomer collections.",
        command: ".venv/bin/saw seed-dev-data",
        ready: sampleReady,
        tone: sampleReady ? "verified" : "probable",
        badge: sampleReady ? "loaded" : (qdrantReady ? "next" : "after Qdrant"),
      },
    ];
  }

  function renderWorkspaceSetupChecklist(checklist) {
    return `
      <div class="workspace-setup-strip">
        ${checklist
          .map(
            (item) => `
              <div class="setup-step">
                <div class="toolbar">
                  <strong>${escapeHtml(item.step)}. ${escapeHtml(item.title)}</strong>
                  <span class="pill ${escapeHtml(item.tone)}">${escapeHtml(item.badge)}</span>
                </div>
                <div class="detail-note">${escapeHtml(item.summary)}</div>
                <code class="inline-command">${escapeHtml(item.command)}</code>
                <button
                  class="secondary-button"
                  type="button"
                  data-action="copy-command"
                  data-command="${escapeHtml(item.command)}"
                >
                  Copy command
                </button>
              </div>
            `
          )
          .join("")}
      </div>
    `;
  }

  function renderWorkspaceHeroActions({
    setupMode,
    sampleProjectLoaded,
    pendingCandidates,
    thinCoverage,
    selectedSection,
    researchRunsNeedingAttention,
  }) {
    if (setupMode) {
      return `
        <button class="primary-button" type="button" data-action="refresh-live-data">Refresh setup status</button>
        <a class="secondary-button" href="#workspace">View runtime checklist</a>
      `;
    }

    const actions = [];
    if (pendingCandidates.length) {
      actions.push(`<a class="primary-button" href="#review">Review ${escapeHtml(String(pendingCandidates.length))} pending</a>`);
    } else if (selectedSection) {
      actions.push(`<a class="primary-button" href="#bible">${escapeHtml(sampleProjectLoaded ? "Open sample Bible" : "Open Bible")}</a>`);
    } else {
      actions.push('<a class="primary-button" href="#bible">Compose first section</a>');
    }

    if (thinCoverage.length || researchRunsNeedingAttention.length) {
      actions.push('<button class="secondary-button" type="button" data-action="launch-gap-research">Fill gap</button>');
    } else {
      actions.push('<a class="secondary-button" href="#research">Open research</a>');
    }

    actions.push(`<a class="secondary-button" href="#ask">${escapeHtml(sampleProjectLoaded ? "Ask sample canon" : "Ask canon")}</a>`);

    if (!pendingCandidates.length) {
      actions.push('<a class="secondary-button" href="#review">Review</a>');
    }

    return actions.slice(0, 4).join("");
  }

  function renderWorkspaceActionCard(item, { setupMode = false } = {}) {
    if (item.command) {
      return `
        <div class="workspace-action workspace-action-setup">
          <div class="workspace-action-body">
            <div class="toolbar">
              <strong>${escapeHtml(item.title)}</strong>
              <span class="pill ${escapeHtml(item.tone)}">${escapeHtml(item.badge || "setup")}</span>
            </div>
            <div class="detail-note">${escapeHtml(item.summary)}</div>
            <code class="inline-command">${escapeHtml(item.command)}</code>
          </div>
          <div class="workspace-action-cta">
            <button
              class="secondary-button"
              type="button"
              data-action="copy-command"
              data-command="${escapeHtml(item.command)}"
            >
              Copy command
            </button>
          </div>
        </div>
      `;
    }

    return `
      <a class="workspace-action" href="#${escapeHtml(item.screen)}">
        <div>
          <strong>${escapeHtml(item.title)}</strong>
          <div class="detail-note">${escapeHtml(item.summary)}</div>
        </div>
        <span class="pill ${escapeHtml(item.tone)}">${escapeHtml(item.badge || "open")}</span>
      </a>
    `;
  }

  function collectWorkspaceJobs() {
    const jobs = [];
    const seen = new Set();
    for (const job of state.jobs || []) {
      if (!job?.job_id || seen.has(job.job_id)) continue;
      seen.add(job.job_id);
      jobs.push({
        label: titleize((job.job_type || "background_job").replaceAll("_", " ")),
        summary: renderJobHeadline(job),
        job,
      });
    }
    for (const section of state.bible.sections || []) {
      const job = section.latest_job;
      if (!job?.job_id || seen.has(job.job_id)) continue;
      seen.add(job.job_id);
      jobs.push({
        label: `${section.title} ${titleize((job.job_type || "job").replaceAll("_", " "))}`.trim(),
        summary: renderJobHeadline(job),
        job,
      });
    }
    for (const run of state.researchRuns || []) {
      const job = run.latest_job;
      if (!job?.job_id || seen.has(job.job_id)) continue;
      seen.add(job.job_id);
      jobs.push({
        label: run.brief?.topic || run.run_id || "Research run",
        summary: renderJobHeadline(job),
        job,
      });
    }
    return jobs.sort((left, right) => {
      const leftTime = Date.parse(left.job.updated_at || left.job.finished_at || left.job.started_at || "") || 0;
      const rightTime = Date.parse(right.job.updated_at || right.job.finished_at || right.job.started_at || "") || 0;
      return rightTime - leftTime;
    });
  }

  function renderIntakePanel({ surface = "workspace" } = {}) {
    const mode = state.intakeDraft.mode || "text";
    const corpusService = state.runtimeStatus?.services?.find((service) => service.name === "corpus");
    const localOnly = corpusService?.mode === "stub";
    const title = surface === "workspace" ? "Bring In New Material" : "Add Source";
    const summary =
      surface === "workspace"
        ? "Start from your own material, then let Sourcebound carry it into review."
        : "Create a source without leaving the product, then inspect the audit trail below.";
    const helper =
      mode === "text"
        ? "Best first-run path. Pasted text can move straight into review even before Zotero is configured."
        : mode === "file"
          ? "Text-like files work best today. Binary attachments still need richer extraction support."
          : localOnly
            ? "Without Zotero, URL intake stores the link and your notes locally instead of fetching the page body."
            : "When Zotero is configured, the URL is created upstream and then pulled back into the workspace.";

    return `
      <div class="detail ${surface === "workspace" ? "workspace-panel-primary" : ""}">
        <div class="detail-head">
          <div>
            <h3>${escapeHtml(title)}</h3>
            <div class="detail-note">${escapeHtml(summary)}</div>
          </div>
          <span class="pill ${localOnly ? "queued" : "verified"}">${escapeHtml(localOnly ? "local" : "live")}</span>
        </div>
        ${localOnly ? "<div class='helper'>Fresh install mode is active. Text intake works locally; Zotero remains optional until you want live library sync.</div>" : ""}
        <form class="detail-stack" data-form="intake-source">
          <div class="detail-grid">
            <div class="field">
              <label for="intake-mode">Intake mode</label>
              <select id="intake-mode" name="intake_mode" data-intake-mode>
                <option value="text" ${mode === "text" ? "selected" : ""}>Paste text</option>
                <option value="file" ${mode === "file" ? "selected" : ""}>Upload file</option>
                <option value="url" ${mode === "url" ? "selected" : ""}>Save URL</option>
              </select>
            </div>
            <div class="field">
              <label for="intake-source-type">Source type</label>
              <select id="intake-source-type" name="source_type">
                ${renderSourceTypeOptions(state.intakeDraft.source_type)}
              </select>
            </div>
          </div>

          ${mode === "text" ? `
            <div class="detail-grid">
              <div class="field">
                <label for="intake-title">Title</label>
                <input id="intake-title" name="title" value="${escapeHtml(state.intakeDraft.title)}" placeholder="Municipal price ledger excerpt" required />
              </div>
              <div class="field">
                <label for="intake-author">Author</label>
                <input id="intake-author" name="author" value="${escapeHtml(state.intakeDraft.author)}" placeholder="City clerk" />
              </div>
              <div class="field">
                <label for="intake-year">Year or era</label>
                <input id="intake-year" name="year" value="${escapeHtml(state.intakeDraft.year)}" placeholder="1422" />
              </div>
              <div class="field">
                <label for="intake-collection-key">Collection key</label>
                <input id="intake-collection-key" name="collection_key" value="${escapeHtml(state.intakeDraft.collection_key)}" placeholder="Optional Zotero collection key" />
              </div>
            </div>
            <div class="field">
              <label for="intake-text">Source text</label>
              <textarea id="intake-text" name="text" placeholder="Paste the excerpt, note, or transcription you want Sourcebound to analyze." required>${escapeHtml(state.intakeDraft.text)}</textarea>
            </div>
            <div class="field">
              <label for="intake-notes">Notes</label>
              <textarea id="intake-notes" name="notes" placeholder="Optional context for yourself or the reviewer.">${escapeHtml(state.intakeDraft.notes)}</textarea>
            </div>
          ` : ""}

          ${mode === "file" ? `
            <div class="detail-grid">
              <div class="field">
                <label for="intake-file">File</label>
                <input id="intake-file" name="file" type="file" required />
              </div>
              <div class="field">
                <label for="intake-file-title">Title</label>
                <input id="intake-file-title" name="title" value="${escapeHtml(state.intakeDraft.title)}" placeholder="Optional display title" />
              </div>
              <div class="field">
                <label for="intake-file-collection-key">Collection key</label>
                <input id="intake-file-collection-key" name="collection_key" value="${escapeHtml(state.intakeDraft.collection_key)}" placeholder="Optional Zotero collection key" />
              </div>
            </div>
            <div class="field">
              <label for="intake-file-notes">Notes</label>
              <textarea id="intake-file-notes" name="notes" placeholder="Optional context, provenance, or reminders.">${escapeHtml(state.intakeDraft.notes)}</textarea>
            </div>
          ` : ""}

          ${mode === "url" ? `
            <div class="detail-grid">
              <div class="field">
                <label for="intake-url">URL</label>
                <input id="intake-url" name="url" type="url" value="${escapeHtml(state.intakeDraft.url)}" placeholder="https://example.org/source" required />
              </div>
              <div class="field">
                <label for="intake-url-title">Title</label>
                <input id="intake-url-title" name="title" value="${escapeHtml(state.intakeDraft.title)}" placeholder="Optional title override" />
              </div>
              <div class="field">
                <label for="intake-url-collection-key">Collection key</label>
                <input id="intake-url-collection-key" name="collection_key" value="${escapeHtml(state.intakeDraft.collection_key)}" placeholder="Optional Zotero collection key" />
              </div>
            </div>
            <div class="field">
              <label for="intake-url-notes">Notes or captured detail</label>
              <textarea id="intake-url-notes" name="notes" placeholder="Paste the key detail you care about if you want better local extraction.">${escapeHtml(state.intakeDraft.notes)}</textarea>
            </div>
          ` : ""}

          <div class="toolbar">
            <button class="primary-button" type="submit">${escapeHtml(mode === "file" ? "Upload and Process" : "Save and Process")}</button>
            <span class="helper">${escapeHtml(helper)}</span>
          </div>
        </form>
      </div>
    `;
  }

  function renderSourcesScreen() {
    const selected = state.sources.find((source) => source.source_id === state.selectedSourceId) ?? state.sources[0];
    const selectedDetail = state.selectedSourceDetail?.source?.source_id === selected?.source_id ? state.selectedSourceDetail : null;
    const linkedEvidence = state.evidence.filter((snippet) => snippet.source_id === selected?.source_id);
    const linkedCandidates = state.candidates.filter((candidate) =>
      candidate.evidence_ids.some((evidenceId) => linkedEvidence.some((snippet) => snippet.evidence_id === evidenceId))
    );
    const canRepullSelected = !!(selected && selected.external_source === "zotero" && selected.zotero_item_key);

    return `
      <article class="screen fade-in">
        <div class="screen-head">
          <div>
            <h2 data-active-screen>Sources</h2>
            <p>Advanced utility for source intake and auditing. Use this when you need to inspect imported material feeding the writing workflow.</p>
          </div>
          <div class="screen-actions">
            <button class="secondary-button" type="button" data-action="pull-sources">Pull Zotero sources</button>
            <button class="secondary-button" type="button" data-action="repull-source" ${canRepullSelected ? "" : "disabled"}>Re-pull selected source</button>
            <button class="secondary-button" type="button" data-action="retry-source-normalization" ${selected ? "" : "disabled"}>Retry normalization</button>
          </div>
        </div>

        <div class="split">
          ${renderIntakePanel({ surface: "sources" })}
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
                      <span class="pill ${escapeHtml(source.workflow_stage || source.sync_status)}">${escapeHtml(source.workflow_stage || source.sync_status)}</span>
                      <span class="pill ${escapeHtml(source.source_type)}">${escapeHtml(source.source_type)}</span>
                      <span class="code">${escapeHtml(source.source_id)}</span>
                    </div>
                    <div class="row-meta">${escapeHtml(source.locator_hint || "No locator hint")}</div>
                    <div class="row-meta">${escapeHtml(renderSourceStageSummary(source.stage_summary || {}))}</div>
                    <div class="row-meta">${escapeHtml((state.evidence.filter((e) => e.source_id === source.source_id).length || 0) + " evidence")}</div>
                  </button>
                `
              )
              .join("")}
          </div>

          <aside class="detail">
            ${selected ? renderSourceDetail(selectedDetail || { source: selected, source_documents: [], text_units: [], stage_summary: selected.stage_summary || {}, stage_errors: selected.stage_errors || [] }, linkedEvidence, linkedCandidates) : "<div class='helper'>No sources are available.</div>"}
          </aside>
        </div>
      </article>
    `;
  }

  function renderSourceDetail(detail, linkedEvidence, linkedCandidates) {
    const source = detail.source;
    const documents = detail.source_documents || [];
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
        <div class="field">
          <label>Workflow stage</label>
          <div>${escapeHtml(source.workflow_stage || source.sync_status || "n/a")}</div>
        </div>
        <div class="field">
          <label>Last sync</label>
          <div>${escapeHtml(source.last_synced_at || "n/a")}</div>
        </div>
      </div>
      <div class="detail-stack">
        <div class="field">
          <label>Stage summary</label>
          <div>${escapeHtml(renderSourceStageSummary(detail.stage_summary || source.stage_summary || {}))}</div>
          <div class="detail-note">${escapeHtml((detail.stage_errors || source.stage_errors || []).join(" | ") || "No source-level errors.")}</div>
        </div>
        <div class="field">
          <label>Source documents</label>
          <div class="detail-list">
            ${documents.length
              ? documents
                  .map(
                    (document) => `
                      <div class="mini">
                        <div class="code">${escapeHtml(document.document_id)} · ${escapeHtml(document.filename || document.locator || document.document_kind)}</div>
                        <div class="row-meta">
                          <span class="pill ${escapeHtml(document.document_kind)}">${escapeHtml(document.document_kind)}</span>
                          <span class="pill ${escapeHtml(document.attachment_fetch_status || document.ingest_status)}">${escapeHtml(document.attachment_fetch_status || document.ingest_status)}</span>
                          <span class="pill ${escapeHtml(document.text_extraction_status || document.raw_text_status)}">${escapeHtml(document.text_extraction_status || document.raw_text_status)}</span>
                          <span class="pill ${escapeHtml(document.normalization_status || document.claim_extraction_status)}">${escapeHtml(document.normalization_status || document.claim_extraction_status)}</span>
                        </div>
                        <div>${escapeHtml(renderDocumentStageLine(document))}</div>
                        <div class="detail-note">${escapeHtml(document.storage_path || "No stored attachment path.")}</div>
                        <div class="detail-note">${escapeHtml((document.stage_errors || []).join(" | ") || "No document errors.")}</div>
                      </div>
                    `
                  )
                  .join("")
              : "<div class='helper'>No document-level intake details have been loaded yet.</div>"}
          </div>
        </div>
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
    const advanced = state.workspaceMode === "advanced";

    return `
      <article class="screen fade-in">
        <div class="screen-head">
          <div>
            <h2 data-active-screen>Research</h2>
            <p>Start with the gap you need to fill for the book, then send the strongest accepted leads through the normal review gate.</p>
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
                  <h3>Fill a writing gap</h3>
                  <div class="detail-note">Describe the scene problem, period, place, and audience lens. Advanced scouting controls stay available when you need them.</div>
                </div>
                <span class="pill queued">brief</span>
              </div>
              <div class="detail-grid">
                <div class="field">
                  <label for="research-topic">Topic</label>
                  <input id="research-topic" name="topic" value="${escapeHtml(state.researchDraft.topic)}" placeholder="What detail do I need for the next scene?" required />
                </div>
                <div class="field">
                  <label for="research-focal-year">Focal year or period</label>
                  <input id="research-focal-year" name="focal_year" value="${escapeHtml(state.researchDraft.focal_year)}" placeholder="1422" />
                </div>
                <div class="field">
                  <label for="research-locale">Place</label>
                  <input id="research-locale" name="locale" value="${escapeHtml(state.researchDraft.locale)}" placeholder="Rouen" />
                </div>
                <div class="field">
                  <label for="research-time-start">Time start</label>
                  <input id="research-time-start" name="time_start" value="${escapeHtml(state.researchDraft.time_start)}" placeholder="1421-12-01" />
                </div>
                <div class="field">
                  <label for="research-time-end">Time end</label>
                  <input id="research-time-end" name="time_end" value="${escapeHtml(state.researchDraft.time_end)}" placeholder="1422-02-28" />
                </div>
                <div class="field">
                  <label for="research-audience">Audience / lens</label>
                  <input id="research-audience" name="audience" value="${escapeHtml(state.researchDraft.audience)}" placeholder="bakers, dockworkers, clerks, households in ration lines" />
                </div>
                <div class="field">
                  <label for="research-domain-hints">Scene need</label>
                  <input id="research-domain-hints" name="domain_hints" value="${escapeHtml(state.researchDraft.domain_hints)}" placeholder="market movement, bell timing, ration tokens, shrine ritual" />
                </div>
                <div class="field">
                  <label for="research-desired-facets">Desired facets</label>
                  <input id="research-desired-facets" name="desired_facets" value="${escapeHtml(state.researchDraft.desired_facets)}" placeholder="people, practices, institutions, objects_technology" />
                </div>
                <div class="field">
                  <label for="research-curated-title">Optional source title</label>
                  <input id="research-curated-title" name="curated_title" value="${escapeHtml(state.researchDraft.curated_title)}" placeholder="Pasted note or known source title" />
                </div>
                <div class="field">
                  <label for="research-curated-url">Optional source URL</label>
                  <input id="research-curated-url" name="curated_url" value="${escapeHtml(state.researchDraft.curated_url)}" placeholder="Optional URL for curated mode or a known lead" />
                </div>
              </div>
              <div class="field">
                <label for="research-curated-text">Notes or pasted evidence</label>
                <textarea id="research-curated-text" name="curated_text" placeholder="Paste a source excerpt, a note to yourself, or a known detail you want the run to build around.">${escapeHtml(state.researchDraft.curated_text)}</textarea>
              </div>
              <details class="detail disclosure" ${advanced ? "open" : ""}>
                <summary>Advanced research settings</summary>
                <div class="detail-grid" style="margin-top:12px;">
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
                    <label for="research-preferred-source-types">Preferred source types</label>
                    <input id="research-preferred-source-types" name="preferred_source_types" value="${escapeHtml(state.researchDraft.preferred_source_types)}" placeholder="archive, educational, record" />
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
                    <input id="research-deny-domains" name="deny_domains" value="${escapeHtml(state.researchDraft.deny_domains)}" placeholder="blocked.example.org" />
                  </div>
                </div>
              </details>
              <div class="toolbar">
                <label class="chip">
                  <input type="checkbox" name="respect_robots" ${state.researchDraft.respect_robots ? "checked" : ""} />
                  <span style="margin-left:8px;">Respect robots</span>
                </label>
              </div>
              <div class="toolbar">
                <button class="primary-button" type="submit">Run research</button>
                <span class="helper">Accepted leads still stay outside canon until they are staged, extracted, and reviewed normally.</span>
              </div>
            </form>

            ${advanced
              ? `
                <div class="detail">
                  <div class="detail-head">
                    <div>
                      <h3>Programs</h3>
                      <div class="detail-note">These instruction sets shape facet targets, source policy, and quality thresholds.</div>
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
                                <div>${escapeHtml(program.description || "No description supplied.")}</div>
                                <div class="detail-note">facets: ${escapeHtml((program.default_facets || []).join(", ") || "none")} · adapter: ${escapeHtml(program.default_adapter_id || "web_open")}</div>
                              </div>
                            `
                          )
                          .join("")
                      : "<div class='helper'>No research programs are loaded yet.</div>"}
                  </div>
                </div>
              `
              : ""}

            <div class="surface list">
              ${state.researchRuns.length
                ? state.researchRuns
                    .map(
                      (run) => `
                        <button class="list-row ${run.run_id === selectedRun?.run_id ? "is-selected" : ""}" type="button" data-select-research-run="${escapeHtml(run.run_id)}">
                          <div>
                            <div class="row-title">${escapeHtml(run.brief.topic)}</div>
                            <div class="row-subtitle">${escapeHtml(run.brief.locale || run.brief.audience || "Research run")}</div>
                          </div>
                          <div class="row-meta">
                            <span class="pill ${escapeHtml(run.status)}">${escapeHtml(run.status)}</span>
                            ${advanced ? `${renderJobPill(run.latest_job)}<span class="code">${escapeHtml(run.program_id)}</span>` : ""}
                          </div>
                          <div class="row-meta">${escapeHtml(run.accepted_count)} usable leads · ${escapeHtml(run.rejected_count)} rejected</div>
                          <div class="row-meta">${escapeHtml(run.staged_count)} sent onward${advanced ? ` · ${escapeHtml(run.query_count)} queries` : ""}</div>
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
    const advanced = state.workspaceMode === "advanced";

    return `
      <div class="detail-head">
        <div>
          <h3>${escapeHtml(run.brief.topic)}</h3>
          <div class="detail-note">${escapeHtml(program?.name || run.program_id)}${advanced ? ` · ${escapeHtml(run.run_id)}` : ""}</div>
        </div>
        <div class="toolbar">
          ${advanced ? renderJobPill(run.latest_job) : ""}
          <span class="pill ${escapeHtml(run.status)}">${escapeHtml(run.status)}</span>
        </div>
      </div>
      <div class="inline-metrics">
        <span>${escapeHtml(run.accepted_count)} usable leads</span>
        <span>${escapeHtml(run.rejected_count)} rejected</span>
        <span>${escapeHtml(run.staged_count)} sent onward</span>
        <span>${escapeHtml(run.finding_count)} findings</span>
        ${advanced ? `<span>${escapeHtml(run.query_count)} queries</span>` : ""}
      </div>
      <div class="toolbar">
        <button class="secondary-button" type="button" data-action="refresh-research">Refresh detail</button>
        ${advanced ? `<button class="secondary-button" type="button" data-action="stage-research">Stage accepted findings</button>` : ""}
        <button class="primary-button" type="button" data-action="extract-research">Send accepted leads to review</button>
        ${advanced ? renderJobControls(run.latest_job) : ""}
      </div>
      ${advanced ? renderJobDiagnostic(run.latest_job) : ""}
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
          <label>Review handoff</label>
          <div>${escapeHtml(run.extraction_run_id ? "Candidates created and waiting for review." : "Not yet sent onward to review.")}</div>
        </div>
      </div>
      <div class="field">
        <label>Coverage</label>
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
                      ${advanced && facet.diagnostic_summary
                        ? `<div class="detail-note">diagnostics ${escapeHtml(facet.diagnostic_summary)} · duplicates ${escapeHtml(facet.duplicate_rejections ?? 0)} · threshold ${escapeHtml(facet.threshold_rejections ?? 0)} · excluded ${escapeHtml(facet.excluded_source_rejections ?? 0)} · fetch ${escapeHtml(facet.fetch_failures ?? 0)}</div>`
                        : ""}
                      ${advanced && facet.accepted_sources_by_type
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
        <label>Usable leads</label>
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
          ${advanced ? `
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
          ` : `
            <div class="field">
              <label for="research-filter-sort">Sort</label>
              <select id="research-filter-sort" data-research-filter="researchSort">
                ${renderSelectOptions([
                  ["accepted_first", "accepted first"],
                  ["weakest_first", "weakest first"],
                ], state.filters.researchSort)}
              </select>
            </div>
          `}
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
                      <details class="detail-note" style="margin-top:8px;" ${advanced ? "open" : ""}>
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
      ${advanced ? `
        <details class="field disclosure">
          <summary>Advanced diagnostics</summary>
          <div class="detail-list" style="margin-top:12px;">
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
              <div class="detail-note">search providers</div>
              <div>${escapeHtml((run.telemetry?.search?.providers_used || []).join(", ") || "none")}</div>
            </div>
            <div class="mini">
              <div class="detail-note">semantic backend</div>
              <div>${escapeHtml(run.telemetry?.semantic?.backend || "n/a")}</div>
            </div>
            <div class="mini">
              <div class="detail-note">run log</div>
              <div>${run.logs.length ? run.logs.map((line) => escapeHtml(line)).join(" | ") : "No run log entries were recorded."}</div>
            </div>
          </div>
        </details>
      ` : ""}
    `;
  }

  function renderBibleScreen() {
    const profile = state.bible.profile;
    const selected = state.bible.sections.find((section) => section.section_id === state.bible.selectedSectionId) ?? state.bible.sections[0];
    const coverage = buildBibleCoverage(profile, state.bible.sections, state.claims);
    const readyForBible = state.claims.filter((claim) => claim.status !== "rumor" && claim.status !== "legend").length;
    const nonReadyCount = coverage.filter((item) => item.summary !== "ready").length;
    const advanced = state.workspaceMode === "advanced";

    return `
      <article class="screen fade-in">
        <div class="screen-head">
          <div>
            <h2 data-active-screen>Bible</h2>
            <p>The bible workspace turns reviewed canon into writer-facing sections. Every section keeps its provenance visible, keeps uncertainty explicit, and can be regenerated without silently overwriting manual edits.</p>
          </div>
          <div class="screen-actions">
            <button class="secondary-button" type="button" data-action="export-bible">Export saved bible</button>
          </div>
        </div>

        <section class="hero-surface bible-hero">
          <div>
            <p class="eyebrow">Solo author workflow</p>
            <h3>${escapeHtml(profile?.project_name || "No bible profile yet")}</h3>
            <p>${escapeHtml(profile?.narrative_focus || "Anchor the period, track what is true, and keep rumors and author choices visibly separate.")}</p>
          </div>
          <div class="funnel">
            <div><strong>${state.candidates.filter((candidate) => isUnresolvedReviewState(candidate.review_state)).length}</strong><span>Needs review</span></div>
            <div><strong>${readyForBible}</strong><span>Eligible canon</span></div>
            <div><strong>${nonReadyCount}</strong><span>Sections not ready</span></div>
          </div>
        </section>

        ${advanced && state.bible.exportBundle
          ? `
            <section class="detail">
              <div class="detail-head">
                <div>
                  <h3>Latest Export Bundle</h3>
                  <div class="detail-note">Exports now run as background jobs. This panel shows the latest completed bundle returned by the worker.</div>
                </div>
                <span class="pill verified">${escapeHtml(state.bible.exportBundle.sections?.length || 0)} sections</span>
              </div>
              <div class="detail-grid">
                <div class="field">
                  <label>Project</label>
                  <div>${escapeHtml(state.bible.exportBundle.profile?.project_name || "n/a")}</div>
                </div>
                <div class="field">
                  <label>Generated</label>
                  <div>${escapeHtml(state.bible.exportBundle.generated_at || "n/a")}</div>
                </div>
              </div>
            </section>
          `
          : ""}

        <div class="split">
          <div class="detail-stack">
            <form class="detail" data-form="bible-profile">
              <div class="detail-head">
                <div>
                  <h3>Project profile</h3>
                  <div class="detail-note">This drives section defaults, tone, and coverage expectations for one active historical-fiction project.</div>
                </div>
                <span class="pill probable">profile</span>
              </div>
              <div class="detail-grid">
                <div class="field">
                  <label for="bible-project-name">Project name</label>
                  <input id="bible-project-name" name="project_name" value="${escapeHtml(profile?.project_name || "")}" placeholder="Greyport Bible" required />
                </div>
                <div class="field">
                  <label for="bible-era">Era</label>
                  <input id="bible-era" name="era" value="${escapeHtml(profile?.era || "")}" placeholder="1421-1422 winter shortage" />
                </div>
                <div class="field">
                  <label for="bible-geography">Geography</label>
                  <input id="bible-geography" name="geography" value="${escapeHtml(profile?.geography || "")}" placeholder="Rouen" />
                </div>
                <div class="field">
                  <label for="bible-social-lens">Social lens</label>
                  <input id="bible-social-lens" name="social_lens" value="${escapeHtml(profile?.social_lens || "")}" placeholder="market workers, clerks, dockhands" />
                </div>
                <div class="field">
                  <label for="bible-focus">Narrative focus</label>
                  <input id="bible-focus" name="narrative_focus" value="${escapeHtml(profile?.narrative_focus || "")}" placeholder="scarcity, ritual, rumor" />
                </div>
                <div class="field">
                  <label for="bible-tone">Tone</label>
                  <select id="bible-tone" name="tone">
                    ${renderSelectOptions([
                      ["documentary", "documentary"],
                      ["grounded_literary", "grounded literary"],
                      ["rumor_rich_folkloric", "rumor-rich / folkloric"],
                      ["mixed_historical_fiction", "mixed historical-fiction"],
                    ], profile?.tone || "grounded_literary")}
                  </select>
                </div>
                <div class="field">
                  <label for="bible-time-start">Time start</label>
                  <input id="bible-time-start" name="time_start" value="${escapeHtml(profile?.time_start || "")}" placeholder="1421-12-01" />
                </div>
                <div class="field">
                  <label for="bible-time-end">Time end</label>
                  <input id="bible-time-end" name="time_end" value="${escapeHtml(profile?.time_end || "")}" placeholder="1422-02-28" />
                </div>
                <div class="field">
                  <label for="bible-facets">Desired facets</label>
                  <input id="bible-facets" name="desired_facets" value="${escapeHtml((profile?.desired_facets || []).join(", "))}" placeholder="daily life, institutions, economics" />
                </div>
                <div class="field">
                  <label for="bible-taboo">Taboo / excluded areas</label>
                  <input id="bible-taboo" name="taboo_topics" value="${escapeHtml((profile?.taboo_topics || []).join(", "))}" placeholder="anachronistic slang, clean myths" />
                </div>
                <div class="field">
                  <label for="bible-statuses">Default certainty buckets</label>
                  <input id="bible-statuses" name="include_statuses" value="${escapeHtml((profile?.composition_defaults?.include_statuses || ["verified", "probable"]).join(", "))}" placeholder="verified, probable" />
                </div>
                <div class="field">
                  <label for="bible-source-types">Default source types</label>
                  <input id="bible-source-types" name="source_types" value="${escapeHtml((profile?.composition_defaults?.source_types || []).join(", "))}" placeholder="record, chronicle, archive" />
                </div>
              </div>
              <div class="toolbar">
                <button class="primary-button" type="submit">Save project profile</button>
              </div>
            </form>

            <div class="detail">
              <div class="detail-head">
                <div>
                  <h3>Coverage map</h3>
                  <div class="detail-note">Thin, strong, and rumor-heavy areas stay visible so the writer can decide whether to research more or draft anyway.</div>
                </div>
                <span class="pill ${coverage.some((item) => item.summary !== "ready") ? "contested" : "verified"}">${escapeHtml(coverage.length)}</span>
              </div>
              <div class="detail-list">
                ${coverage.map((item) => `
                  <div class="mini">
                    <div class="toolbar">
                      <strong>${escapeHtml(item.label)}</strong>
                      <span class="pill ${escapeHtml(item.tone)}">${escapeHtml(item.summary)}</span>
                    </div>
                    <div class="detail-note">${escapeHtml(item.detail)}</div>
                  </div>
                `).join("")}
              </div>
            </div>

            <form class="detail" data-form="bible-section">
              <div class="detail-head">
                <div>
                  <h3>Compose section</h3>
                  <div class="detail-note">Use section type, certainty, and source filters to generate a writer-facing draft from approved claims.</div>
                </div>
                <span class="pill queued">compose</span>
              </div>
              <div class="detail-grid">
                <div class="field">
                  <label for="bible-section-type">Section type</label>
                  <select id="bible-section-type" name="section_type">
                    ${renderSelectOptions([
                      ["setting_overview", "setting overview"],
                      ["chronology", "chronology / timeline"],
                      ["people_and_factions", "people / factions"],
                      ["daily_life", "daily life / practices"],
                      ["institutions_and_politics", "institutions / politics"],
                      ["economics_and_material_culture", "economics / material culture"],
                      ["rumors_and_contested_accounts", "rumors / contested accounts"],
                      ["author_decisions", "author decisions"],
                    ], state.bible.draft.section_type)}
                  </select>
                </div>
                <div class="field">
                  <label for="bible-section-title">Custom title</label>
                  <input id="bible-section-title" name="title" value="${escapeHtml(state.bible.draft.title)}" placeholder="Optional custom title" />
                </div>
                <div class="field">
                  <label for="bible-section-focus">Focus</label>
                  <input id="bible-section-focus" name="focus" value="${escapeHtml(state.bible.draft.focus)}" placeholder="bread prices, dock rumors, shrine rituals" />
                </div>
                <div class="field">
                  <label for="bible-section-statuses">Certainty buckets</label>
                  <input id="bible-section-statuses" name="statuses" value="${escapeHtml(state.bible.draft.statuses)}" placeholder="verified, probable" />
                </div>
                <div class="field">
                  <label for="bible-section-source-types">Source types</label>
                  <input id="bible-section-source-types" name="source_types" value="${escapeHtml(state.bible.draft.source_types)}" placeholder="record, chronicle, oral_history" />
                </div>
                <div class="field">
                  <label for="bible-section-place">Place</label>
                  <input id="bible-section-place" name="place" value="${escapeHtml(state.bible.draft.place)}" placeholder="Rouen" />
                </div>
                <div class="field">
                  <label for="bible-section-time-start">Time start</label>
                  <input id="bible-section-time-start" name="time_start" value="${escapeHtml(state.bible.draft.time_start)}" placeholder="1421-12-01" />
                </div>
                <div class="field">
                  <label for="bible-section-time-end">Time end</label>
                  <input id="bible-section-time-end" name="time_end" value="${escapeHtml(state.bible.draft.time_end)}" placeholder="1422-02-28" />
                </div>
              </div>
              <div class="toolbar">
                <button class="primary-button" type="submit">Compose section</button>
                <span class="helper">Approved canon only. Rumor, contested material, and author choices stay visibly labeled.</span>
              </div>
            </form>

            <div class="surface list">
              ${state.bible.sections.length
                ? state.bible.sections.map((section) => `
                  <button class="list-row ${section.section_id === selected?.section_id ? "is-selected" : ""}" type="button" data-select-bible-section="${escapeHtml(section.section_id)}">
                    <div>
                      <div class="row-title">${escapeHtml(section.title)}</div>
                      <div class="row-subtitle">${escapeHtml(section.section_type.replaceAll("_", " "))}</div>
                    </div>
                    <div class="row-meta">
                      <span class="pill ${escapeHtml(renderBibleGenerationTone(section.generation_status))}">${escapeHtml(renderBibleGenerationLabel(section))}</span>
                      ${section.has_manual_edits ? `<span class="pill author_choice">${escapeHtml(renderBibleManualState(section))}</span>` : ""}
                      ${renderJobPill(section.latest_job)}
                      <span class="code">${escapeHtml(section.section_id)}</span>
                    </div>
                    <div class="row-meta">${escapeHtml(section.references.claim_ids.length)} claims · ${escapeHtml(section.references.source_ids.length)} sources · ${escapeHtml(renderBibleGenerationSummary(section))}</div>
                  </button>
                `).join("")
                : "<div class='helper' style='padding:14px;'>No bible sections yet. Compose the first one from approved claims.</div>"}
            </div>
          </div>

          <aside class="detail">
            ${selected ? renderBibleSectionDetail(selected) : "<div class='helper'>No bible section selected.</div>"}
          </aside>
        </div>
      </article>
    `;
  }

  function renderBibleSectionDetail(section) {
    const provenance = state.bible.selectedProvenance;
    const selectedParagraph =
      provenance?.paragraphs?.find((item) => item.paragraph?.paragraph_id === state.bible.selectedParagraphId) ||
      provenance?.paragraphs?.find((item) => (item.paragraph?.claim_ids || []).length) ||
      null;
    const manualMode = section.has_manual_edits ? "manual override active" : "generated working copy";
    const manualCallout = section.has_manual_edits
      ? "The editable text below is the author's working override. Regeneration refreshes the generated draft above and preserves this manual text."
      : "The editable text below currently mirrors the generated draft. Once you change it, the editor becomes a manual override that can diverge from regenerated canon synthesis.";
    const generationTone = renderBibleGenerationTone(section.generation_status);
    const generationLabel = renderBibleGenerationLabel(section);
    return `
      <div class="detail-head">
        <div>
          <h3>${escapeHtml(section.title)}</h3>
          <div class="detail-note">${escapeHtml(section.section_type.replaceAll("_", " "))} · ${escapeHtml(section.project_id)}</div>
        </div>
        <div class="toolbar">
          ${renderJobPill(section.latest_job)}
          <span class="pill ${escapeHtml(generationTone)}">${escapeHtml(generationLabel)}</span>
          ${section.has_manual_edits ? `<span class="pill author_choice">${escapeHtml(renderBibleManualState(section))}</span>` : ""}
        </div>
      </div>
      <div class="inline-metrics">
        <span>${escapeHtml(section.references.claim_ids.length)} claims</span>
        <span>${escapeHtml(section.references.source_ids.length)} sources</span>
        <span>${escapeHtml(section.references.evidence_ids.length)} evidence</span>
        <span>${section.ready_for_writer ? "generated baseline usable" : "generated baseline not yet dependable"}</span>
      </div>
      <div class="toolbar">
        <button class="secondary-button" type="button" data-action="launch-gap-research">Fill a gap</button>
        <button class="secondary-button" type="button" data-action="regenerate-bible-section">Regenerate section</button>
        ${advanced ? renderJobControls(section.latest_job) : ""}
      </div>
      ${advanced ? renderJobDiagnostic(section.latest_job) : ""}
      <div class="field">
        <label>Generation posture</label>
        <div class="detail-list">
          <div class="mini">
            <div class="detail-note">retrieval</div>
            <div>${escapeHtml(renderRetrievalSummary(section.retrieval_metadata || {}))}</div>
          </div>
          <div class="mini">
            <div class="detail-note">diagnostic</div>
            <div>${escapeHtml(section.coverage_analysis?.diagnostic_summary || "No diagnostic summary.")}</div>
          </div>
          <div class="mini">
            <div class="detail-note">certainty mix</div>
            <div>${escapeHtml(renderKeyValueMap(section.coverage_analysis?.certainty_mix || {}))}</div>
          </div>
          <div class="mini">
            <div class="detail-note">composition</div>
            <div>${escapeHtml(renderBibleCompositionSummary(section.composition_metrics || {}))}</div>
          </div>
        </div>
      </div>
      <section class="trust-zone trust-zone-generated">
        <div class="trust-zone-head">
          <div>
            <h4>Generated Canon Synthesis</h4>
            <div class="detail-note">These cards and the draft snapshot are generated from approved canon. Paragraph provenance only explains this generated synthesis.</div>
          </div>
          <span class="pill ${escapeHtml(generationTone)}">${escapeHtml(generationLabel)}</span>
        </div>
        <div class="trust-callout trust-callout-generated">${escapeHtml(renderBibleGenerationSummary(section))} Regeneration refreshes this evidence-backed draft and never silently rewrites manual author text.</div>
        <div class="field">
          <label>Generated draft snapshot</label>
          <div class="draft-preview">${escapeHtml(section.generated_markdown || "No generated draft yet.")}</div>
        </div>
        <div class="field">
          <label>Generated reading view</label>
          ${renderBibleGeneratedReadingView(section)}
        </div>
        <div class="field">
          <label>Generated paragraph cards</label>
          ${renderBibleGeneratedParagraphCards(section)}
        </div>
      </section>
      <section class="trust-zone trust-zone-manual">
        <div class="trust-zone-head">
          <div>
            <h4>${section.has_manual_edits ? "Manual Override Text" : "Editable Working Text"}</h4>
            <div class="detail-note">This is the author's writable surface. It may stay aligned with the generated draft or intentionally diverge from it.</div>
          </div>
          <span class="pill ${section.has_manual_edits ? "author_choice" : "probable"}">${escapeHtml(renderBibleManualState(section))}</span>
        </div>
        <div class="trust-callout ${section.has_manual_edits ? "trust-callout-manual" : "trust-callout-neutral"}">${escapeHtml(manualCallout)}</div>
        <form data-form="bible-section-edit">
          <input type="hidden" name="section_id" value="${escapeHtml(section.section_id)}" />
          <div class="field" style="margin-bottom:10px;">
            <label for="bible-edit-title">Title</label>
            <input id="bible-edit-title" name="title" value="${escapeHtml(section.title)}" />
          </div>
          <div class="field">
            <label>${section.has_manual_edits ? "Author-edited full text" : "Working text editor"}</label>
            <textarea name="content" class="bible-editor">${escapeHtml(section.content)}</textarea>
          </div>
          <div class="toolbar">
            <button class="primary-button" type="submit">Save edits</button>
            <span class="helper">${section.has_manual_edits ? `Manual edits are preserved even when the generated draft refreshes. Current state: ${renderBibleManualState(section)}.` : "Saving here creates an explicit manual override."}</span>
          </div>
        </form>
      </section>
      <section class="trust-zone trust-zone-provenance">
        <div class="trust-zone-head">
          <div>
            <h4>Why This Section Says This</h4>
            <div class="detail-note">This drill-down explains why a selected generated paragraph exists. It does not justify arbitrary manual rewrites in the editor.</div>
          </div>
          <span class="pill queued">generated only</span>
        </div>
        <div class="provenance-scope-note">Select a generated paragraph card above to inspect supporting claims, evidence snippets, source titles, and contradictions or supersessions.</div>
        ${selectedParagraph ? renderBibleParagraphProvenance(selectedParagraph, provenance) : "<div class='helper'>Select a generated paragraph to inspect why it exists.</div>"}
      </section>
      <div class="field">
        <label>Coverage and trust</label>
        <div class="warning-list">
          <div class="mini">
            <div class="toolbar">
              <strong>Section readiness</strong>
              <span class="pill ${escapeHtml(generationTone)}">${escapeHtml(generationLabel)}</span>
            </div>
            <div class="detail-note">${escapeHtml(renderBibleGenerationSummary(section))}</div>
          </div>
          ${section.composition_metrics?.thin_section ? `<div class="warning">Thin section warning: ${escapeHtml(renderBibleCompositionSummary(section.composition_metrics || {}))}</div>` : ""}
          ${(section.composition_metrics?.skipped_beat_ids || []).map((beatId, index) => `<div class="warning">${escapeHtml(renderBibleBeatLabel(beatId))}: ${escapeHtml(section.composition_metrics?.skipped_reasons?.[index] || "skipped")}</div>`).join("")}
          ${(section.coverage_gaps || []).map((item) => `<div class="warning">${escapeHtml(item)}</div>`).join("") || "<div class='helper'>No coverage gaps recorded.</div>"}
          ${(section.contradiction_flags || []).map((item) => `<div class="warning">${escapeHtml(item)}</div>`).join("")}
        </div>
      </div>
      ${advanced ? `
        <div class="field">
          <label>Coverage analysis</label>
          <div class="detail-list">
            <div class="mini">
              <div class="detail-note">facet distribution</div>
              <div>${escapeHtml(renderKeyValueMap(section.coverage_analysis?.facet_distribution || {}))}</div>
            </div>
            <div class="mini">
              <div class="detail-note">certainty mix</div>
              <div>${escapeHtml(renderKeyValueMap(section.coverage_analysis?.certainty_mix || {}))}</div>
            </div>
            <div class="mini">
              <div class="detail-note">diagnostic</div>
              <div>${escapeHtml(section.coverage_analysis?.diagnostic_summary || "No diagnostic summary.")}</div>
            </div>
            <div class="mini">
              <div class="detail-note">retrieval</div>
              <div>${escapeHtml(renderRetrievalSummary(section.retrieval_metadata || {}))}</div>
            </div>
          </div>
        </div>
      ` : ""}
      <div class="field">
        <label>Recommended next research</label>
        <div class="detail-list">
          ${(section.recommended_next_research || []).length
            ? section.recommended_next_research.map((item) => `<div class="mini">${escapeHtml(item)}</div>`).join("")
            : "<div class='helper'>No follow-up research recommendations.</div>"}
        </div>
      </div>
    `;
  }

  function renderBibleBeatLabel(value) {
    return (value || "")
      .replace(/:.+$/, "")
      .replaceAll("_", " ");
  }

  function renderBibleParagraphLabel(paragraph) {
    return paragraph.heading || renderBibleBeatLabel(paragraph.paragraph_kind || "paragraph");
  }

  function renderBibleParagraphRole(paragraph) {
    return (paragraph.paragraph_role || paragraph.paragraph_kind || "generated")
      .replaceAll("_", " ");
  }

  function renderBibleCompositionSummary(metrics) {
    const target = Number(metrics?.target_beats || 0);
    const produced = Number(metrics?.produced_beats || 0);
    const claimDensity = Number(metrics?.claim_density || 0);
    const evidenceDensity = Number(metrics?.evidence_density || 0);
    const contradiction = metrics?.contradiction_presence ? "contradictions present" : "no contradictions surfaced";
    return `${produced}/${target} beats produced · claim density ${claimDensity.toFixed(1)} · evidence density ${evidenceDensity.toFixed(1)} · ${contradiction}`;
  }

  function renderBibleGenerationTone(status) {
    if (status === "ready") return "verified";
    if (status === "thin") return "contested";
    if (status === "failed") return "failed";
    return "queued";
  }

  function renderBibleGenerationLabel(section) {
    const status = section?.generation_status || "queued";
    if (status === "ready") return "writer-ready";
    if (status === "thin") return "thin baseline";
    if (status === "failed") return "generation failed";
    return "generation queued";
  }

  function renderBibleGenerationSummary(section) {
    const status = section?.generation_status || "queued";
    if (status === "ready") {
      return `${section.references?.claim_ids?.length || 0} linked claims with a usable generated baseline.`;
    }
    if (status === "thin") {
      return section.generation_error || section.coverage_analysis?.diagnostic_summary || "Generated baseline is still too thin for dependable drafting.";
    }
    if (status === "failed") {
      return section.generation_error || section.latest_job?.error_detail || "The latest generation attempt failed.";
    }
    return "Generation is queued or waiting on the latest background job.";
  }

  function renderBibleManualState(section) {
    if (!section?.has_manual_edits) {
      return "manual layer not started";
    }
    const generated = (section.generated_markdown || "").trim();
    const manual = (section.manual_markdown || section.content || "").trim();
    if (!generated) {
      return "manual text only";
    }
    return generated === manual ? "manual matches generated" : "manual diverges from generated";
  }

  function renderBibleGeneratedParagraphCards(section) {
    const paragraphs = (section.paragraphs || []).filter((paragraph) => (paragraph.claim_ids || []).length);
    if (!paragraphs.length) {
      return "<div class='helper'>No claim-backed generated paragraphs are available for provenance yet.</div>";
    }
    return `
      <div class="paragraph-grid">
        ${paragraphs.map((paragraph) => `
          <button class="paragraph-card ${paragraph.paragraph_id === state.bible.selectedParagraphId ? "is-selected" : ""}" type="button" data-select-bible-paragraph="${escapeHtml(paragraph.paragraph_id)}">
            <div class="paragraph-card-head">
              <div class="row-title">${escapeHtml(renderBibleParagraphLabel(paragraph))}</div>
              <span class="pill verified">${escapeHtml(renderBibleParagraphRole(paragraph))}</span>
            </div>
            <div class="paragraph-card-text">${escapeHtml(paragraph.text)}</div>
            <div class="paragraph-card-meta">
              <span>${escapeHtml(paragraph.claim_ids.length)} claims</span>
              <span>${escapeHtml(paragraph.source_ids.length)} sources</span>
              <span>${escapeHtml(paragraph.evidence_ids.length)} evidence</span>
            </div>
          </button>
        `).join("")}
      </div>
    `;
  }

  function renderBibleGeneratedReadingView(section) {
    const paragraphs = (section.paragraphs || []).filter((paragraph) => (paragraph.claim_ids || []).length);
    if (!paragraphs.length) {
      return "<div class='helper'>No claim-backed generated reading view is available yet.</div>";
    }
    return `
      <div class="linked-draft">
        ${paragraphs.map((paragraph) => `
          <button class="linked-draft-paragraph ${paragraph.paragraph_id === state.bible.selectedParagraphId ? "is-selected" : ""}" type="button" data-select-bible-paragraph="${escapeHtml(paragraph.paragraph_id)}">
            <div class="linked-draft-heading">${escapeHtml(renderBibleParagraphLabel(paragraph))}</div>
            <div class="linked-draft-text">${escapeHtml(paragraph.text)}</div>
            <div class="linked-draft-meta">
              <span>${escapeHtml(renderBibleParagraphRole(paragraph))}</span>
              <span>${escapeHtml(paragraph.claim_ids.length)} claims</span>
              <span>${escapeHtml(paragraph.source_ids.length)} sources</span>
              <span>${escapeHtml(paragraph.evidence_ids.length)} evidence</span>
            </div>
          </button>
        `).join("")}
      </div>
    `;
  }

  function renderBibleParagraphProvenance(selectedParagraph, provenance) {
    const paragraph = selectedParagraph.paragraph || {};
    if (!(paragraph.claim_ids || []).length) {
      return "<div class='helper'>This generated placeholder has no linked claims yet, so provenance cannot justify it as a drafting baseline.</div>";
    }
    const scopeTone =
      selectedParagraph.provenance_scope === "author_guidance"
        ? "author_choice"
        : selectedParagraph.provenance_scope === "contested_context"
          ? "contested"
          : "verified";
    return `
      <div class="detail-list">
        <div class="mini">
          <div class="toolbar">
            <div class="detail-note">generated paragraph</div>
            <span class="pill ${escapeHtml(scopeTone)}">${escapeHtml((selectedParagraph.provenance_scope || "canon_support").replaceAll("_", " "))}</span>
          </div>
          <div>${escapeHtml(paragraph.text || "")}</div>
        </div>
        <div class="mini">
          <div class="detail-note">why this paragraph exists</div>
          <div>${escapeHtml(selectedParagraph.why_this_paragraph_exists || "No summary recorded.")}</div>
        </div>
        <div class="mini">
          <div class="detail-note">supporting claims</div>
          <div class="detail-list">
            ${selectedParagraph.claim_details?.length
              ? selectedParagraph.claim_details.map((claim) => `
                  <div class="mini">
                    <div class="toolbar">
                      <strong>${escapeHtml(claim.summary || `${claim.subject} ${claim.value}`)}</strong>
                      <span class="pill ${escapeHtml(claim.status || "probable")}">${escapeHtml((claim.status || "unknown").replaceAll("_", " "))}</span>
                    </div>
                    <div class="detail-note">${escapeHtml((claim.claim_kind || "claim").replaceAll("_", " "))}${claim.place ? ` · ${escapeHtml(claim.place)}` : ""}${claim.time_start ? ` · ${escapeHtml(claim.time_start)}` : ""}${claim.viewpoint_scope ? ` · ${escapeHtml(claim.viewpoint_scope)}` : ""}</div>
                    ${claim.notes ? `<div>${escapeHtml(claim.notes)}</div>` : ""}
                  </div>
                `).join("")
              : "<div class='helper'>No claim detail recorded.</div>"}
          </div>
        </div>
        <div class="mini">
          <div class="detail-note">evidence snippets</div>
          <div class="detail-list">
            ${selectedParagraph.evidence_details?.length
              ? selectedParagraph.evidence_details.map((item) => `
                  <div class="mini">
                    <div class="toolbar">
                      <strong>${escapeHtml(item.source_title || item.source_id || "source")}</strong>
                      <span class="pill probable">${escapeHtml(item.source_type || "source")}</span>
                    </div>
                    <div class="detail-note">${escapeHtml(item.locator || "locator unavailable")}</div>
                    <div>${escapeHtml(item.snippet || "")}</div>
                  </div>
                `).join("")
              : "<div class='helper'>No evidence snippets linked.</div>"}
          </div>
        </div>
        <div class="mini">
          <div class="detail-note">contradictions and supersessions</div>
          <div class="detail-list">
            ${renderRelationshipDetails(selectedParagraph, provenance)}
          </div>
        </div>
      </div>
    `;
  }

  function renderRelationshipDetails(selectedParagraph, provenance) {
    const contradictionItems = selectedParagraph.contradiction_details || [];
    const supersessionItems = selectedParagraph.supersession_details || [];
    const fallbackContext = [
      ...(selectedParagraph.contradiction_context || []),
      ...(selectedParagraph.supersession_context || []),
      ...(provenance?.relationships || []),
    ];
    if (!contradictionItems.length && !supersessionItems.length && !fallbackContext.length) {
      return "<div class='helper'>No contradictions or supersessions linked.</div>";
    }
    return [
      ...contradictionItems.map((item) => `
        <div class="mini">
          <div class="toolbar">
            <strong>Contradiction</strong>
            <span class="pill contested">contradicts</span>
          </div>
          <div>${escapeHtml(item.related_claim_summary || item.related_claim_id || "related claim")}</div>
          ${item.notes ? `<div class="detail-note">${escapeHtml(item.notes)}</div>` : ""}
        </div>
      `),
      ...supersessionItems.map((item) => `
        <div class="mini">
          <div class="toolbar">
            <strong>Supersession</strong>
            <span class="pill queued">${escapeHtml((item.relationship_type || "supersedes").replaceAll("_", " "))}</span>
          </div>
          <div>${escapeHtml(item.related_claim_summary || item.related_claim_id || "related claim")}</div>
          ${item.notes ? `<div class="detail-note">${escapeHtml(item.notes)}</div>` : ""}
        </div>
      `),
      ...(!contradictionItems.length && !supersessionItems.length
        ? fallbackContext.map((item) => `<div class="mini"><div>${escapeHtml(item)}</div></div>`)
        : []),
    ].join("");
  }

  function renderRetrievalSummary(metadata) {
    if (!metadata || !Object.keys(metadata).length) {
      return "memory ranking";
    }
    const backend = metadata.retrieval_backend || "memory";
    const quality = metadata.retrieval_quality_tier || (backend === "qdrant" ? "projection" : "memory_ranked");
    const boundary = metadata.answer_boundary || "research_gap";
    const rawStrategy = metadata.ranking_strategy || "lexical";
    const strategy = rawStrategy === "intent_blended" ? "topic-first blended" : rawStrategy;
    const fallback = metadata.fallback_used ? ` · fallback ${metadata.fallback_reason || "used"}` : "";
    const nearby = metadata.used_nearby_context ? " · nearby canon visible" : "";
    return `${renderAnswerBoundaryLabel(boundary)} · ${backend} · ${quality.replaceAll("_", " ")} · ${strategy}${nearby}${fallback}`;
  }

  function renderJobControls(job) {
    if (!job) {
      return "";
    }
    if (["queued", "running", "cancel_requested"].includes(job.worker_state || job.status_label || job.status)) {
      return `<button class="secondary-button" type="button" data-action="cancel-job" data-job-id="${escapeHtml(job.job_id)}">Cancel job</button>`;
    }
    if ((job.status_label || job.status) === "failed" && job.retryable) {
      return `<button class="secondary-button" type="button" data-action="retry-job" data-job-id="${escapeHtml(job.job_id)}">Retry job</button>`;
    }
    return "";
  }

  function renderJobDiagnostic(job) {
    if (!job) {
      return "";
    }
    const warnings = job.warnings || [];
    const diagnostic = [];
    if (job.progress_message) {
      diagnostic.push(`<div class="mini"><strong>Stage</strong><div class="detail-note">${escapeHtml(job.progress_message)}</div></div>`);
    }
    if (job.stalled_reason) {
      diagnostic.push(`<div class="warning">${escapeHtml(job.stalled_reason)}</div>`);
    }
    if (job.degraded_reason) {
      diagnostic.push(`<div class="warning">${escapeHtml(job.degraded_reason)}</div>`);
    }
    if ((job.status_label || job.status) === "failed") {
      diagnostic.push(`<div class="warning">${escapeHtml(job.error_detail || job.error || "Background job failed.")}</div>`);
    }
    if (warnings.length) {
      diagnostic.push(...warnings.map((warning) => `<div class="warning">${escapeHtml(warning)}</div>`));
    }
    return diagnostic.join("");
  }

  function buildBibleCoverage(profile, sections, claims) {
    const sectionTypes = [
      ["setting_overview", "Setting"],
      ["chronology", "Chronology"],
      ["people_and_factions", "People"],
      ["daily_life", "Daily life"],
      ["institutions_and_politics", "Institutions"],
      ["economics_and_material_culture", "Economics"],
      ["rumors_and_contested_accounts", "Rumor ledger"],
      ["author_decisions", "Author decisions"],
    ];
    return sectionTypes.map(([type, label]) => {
      const section = sections.find((item) => item.section_type === type);
      const matchingClaims = claims.filter((claim) => {
        if (type === "rumors_and_contested_accounts") {
          return ["rumor", "legend", "contested"].includes(claim.status);
        }
        if (type === "author_decisions") {
          return claim.status === "author_choice" || claim.author_choice;
        }
        return true;
      });
      const status = section?.generation_status || (!section ? "queued" : "thin");
      const weak = !section || status !== "ready";
      return {
        label,
        tone: section ? renderBibleGenerationTone(status) : "queued",
        summary: section ? status.replaceAll("_", " ") : "missing",
        detail: section
          ? `${section.references.claim_ids.length} claims tracked · ${renderBibleGenerationSummary(section)}${section.latest_job ? ` · ${renderJobSummary(section.latest_job)}` : ""}`
          : `No saved section yet. ${matchingClaims.length} claims could support this area for ${profile?.project_name || "the current project"}.`,
      };
    });
  }

  function renderRunsScreen() {
    const selected = state.runs.find((run) => run.run_id === state.selectedRunId) ?? state.runs[0];

    return `
      <article class="screen fade-in">
        <div class="screen-head">
          <div>
            <h2 data-active-screen>Background Work</h2>
            <p>Advanced utility for jobs, extraction runs, and troubleshooting. This work should support the writing loop rather than define it.</p>
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
    const reviewCards = getReviewCards();
    const filteredCandidates = reviewCards.filter((candidate) => {
      if (state.filters.candidates === "all") return true;
      if (state.filters.candidates === "unresolved") return isUnresolvedReviewState(candidate.review_state);
      return candidate.review_state === state.filters.candidates;
    });

    return `
      <article class="screen fade-in">
        <div class="screen-head">
          <div>
            <h2 data-active-screen>Review New Facts</h2>
            <p>Review is still the trust gate. Each card shows the claim, evidence span, and nearby context so nobody has to approve canon blind.</p>
          </div>
          <div class="screen-actions">
            <div class="chip-bar" role="tablist" aria-label="Candidate filters">
              ${renderFilterChip("candidates", "all", "All")}
              ${renderFilterChip("candidates", "unresolved", "Needs review")}
              ${renderFilterChip("candidates", "pending", "Pending")}
              ${renderFilterChip("candidates", "needs_edit", "Needs edit")}
              ${renderFilterChip("candidates", "needs_split", "Needs split")}
              ${renderFilterChip("candidates", "approved", "Approved")}
              ${renderFilterChip("candidates", "rejected", "Rejected")}
            </div>
          </div>
        </div>

        <div class="review-card-list">
          ${filteredCandidates.length
            ? filteredCandidates.map((candidate) => renderReviewCard(candidate)).join("")
            : "<div class='detail'><div class='helper'>No candidates match the current filter.</div></div>"}
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

  function renderReviewCard(candidate) {
    const expanded = candidate.candidate_id === state.selectedCandidateId;
    const editing = candidate.candidate_id === state.editingCandidateId;
    const primary = candidate.primary_evidence;
    const extraEvidence = (candidate.evidence_items || []).slice(1);
    const allowSuggestedApproval = canApproveCandidateAsSuggested(candidate);

    return `
      <form class="detail review-card ${expanded ? "is-expanded" : ""}" data-form="review" data-candidate-id="${escapeHtml(candidate.candidate_id)}">
        <div class="detail-head">
          <div>
            <h3>${escapeHtml(candidate.claim_text || `${candidate.subject} ${candidate.value}`)}</h3>
            <div class="detail-note">${escapeHtml(primary?.source_title || "No source attached")}</div>
          </div>
          <div class="toolbar">
            <span class="pill ${escapeHtml(candidate.review_state)}">${escapeHtml(candidate.review_state)}</span>
            <span class="pill ${escapeHtml(candidate.evidence_quality)}">${escapeHtml(candidate.evidence_quality)}</span>
          </div>
        </div>

        <div class="detail-grid">
          <div class="field">
            <label>Evidence span</label>
            <div class="review-excerpt">${escapeHtml(primary?.excerpt || "No evidence excerpt available.")}</div>
          </div>
          <div class="field">
            <label>Review summary</label>
            <div class="detail-stack">
              <div>${escapeHtml(candidate.location_summary || "Location metadata unavailable")}</div>
              <div>${escapeHtml(candidate.certainty_suggestion || candidate.status_suggestion)}</div>
              <div>${escapeHtml((candidate.extra_evidence_count || 0) > 0 ? `+${candidate.extra_evidence_count} more evidence span${candidate.extra_evidence_count === 1 ? "" : "s"}` : `${candidate.evidence_ids.length} linked evidence span${candidate.evidence_ids.length === 1 ? "" : "s"}`)}</div>
            </div>
          </div>
        </div>

        ${candidate.weakness_reasons?.length ? `
          <div class="inline-metrics review-weaknesses">
            ${candidate.weakness_reasons.map((reason) => `<span>${escapeHtml(formatReviewWeakness(reason))}</span>`).join("")}
          </div>
        ` : ""}

        <div class="toolbar review-actions">
          <button class="secondary-button" type="button" data-action="toggle-candidate-context" data-candidate-id="${escapeHtml(candidate.candidate_id)}">
            ${expanded ? "Hide context" : "Expand context"}
          </button>
          ${allowSuggestedApproval
            ? '<button class="primary-button" type="submit" name="review_action" value="approve_suggested">Approve as suggested</button>'
            : '<span class="helper">Deferred cards need an edited approval before they can enter canon.</span>'}
          <button class="secondary-button" type="button" data-action="edit-candidate-review" data-candidate-id="${escapeHtml(candidate.candidate_id)}">Approve with edits</button>
          <button class="secondary-button" type="submit" name="review_action" value="needs_edit">Needs edit</button>
          <button class="secondary-button" type="submit" name="review_action" value="needs_split">Needs split</button>
          <button class="secondary-button" type="submit" name="review_action" value="reject">Reject</button>
        </div>

        ${expanded ? `
          <div class="detail-stack review-expanded">
            <div class="field">
              <label>Context around evidence span</label>
              <div class="review-context">
                <span class="review-context-muted">${escapeHtml(primary?.context_before || "")}</span><mark>${escapeHtml(primary?.excerpt || "No span excerpt available.")}</mark><span class="review-context-muted">${escapeHtml(primary?.context_after || "")}</span>
              </div>
              <div class="detail-note">${escapeHtml(primary?.locator || "locator unavailable")}${primary?.text_unit_id ? ` · text unit ${escapeHtml(primary.text_unit_id)}` : ""}</div>
            </div>

            ${extraEvidence.length ? `
              <div class="field">
                <label>Additional evidence</label>
                <div class="detail-list">
                  ${extraEvidence.map((snippet) => `
                    <div class="mini">
                      <div class="toolbar">
                        <strong>${escapeHtml(snippet.source_title || snippet.source_id)}</strong>
                        <span class="detail-note">${escapeHtml(snippet.locator || "locator unavailable")}</span>
                      </div>
                      <div>${escapeHtml(snippet.excerpt || "No excerpt available.")}</div>
                    </div>
                  `).join("")}
                </div>
              </div>
            ` : ""}

            <div class="detail-grid">
              <div class="field">
                <label>Suggested certainty</label>
                <select name="override_status">
                  <option value="">Use suggestion (${escapeHtml(candidate.status_suggestion)})</option>
                  <option value="verified">Verified</option>
                  <option value="probable">Probable</option>
                  <option value="contested">Contested</option>
                  <option value="rumor">Rumor</option>
                  <option value="legend">Legend</option>
                  <option value="author_choice">Author choice</option>
                </select>
              </div>
              <div class="field">
                <label>Claim kind</label>
                <div>${escapeHtml(candidate.claim_kind)}</div>
              </div>
            </div>

            <div class="field">
              <label>Review notes</label>
              <textarea name="notes" placeholder="Add rationale, source concerns, or a follow-up instruction.">${escapeHtml(candidate.notes || "")}</textarea>
            </div>

            ${editing ? `
              <div class="review-edit-grid">
                <div class="field">
                  <label>Subject</label>
                  <input name="patch_subject" value="${escapeHtml(candidate.subject || "")}" />
                </div>
                <div class="field">
                  <label>Predicate</label>
                  <input name="patch_predicate" value="${escapeHtml(candidate.predicate || "")}" />
                </div>
                <div class="field">
                  <label>Value</label>
                  <textarea name="patch_value">${escapeHtml(candidate.value || "")}</textarea>
                </div>
                <div class="field">
                  <label>Place</label>
                  <input name="patch_place" value="${escapeHtml(candidate.place || "")}" />
                </div>
                <div class="field">
                  <label>Time start</label>
                  <input name="patch_time_start" value="${escapeHtml(candidate.time_start || "")}" />
                </div>
                <div class="field">
                  <label>Time end</label>
                  <input name="patch_time_end" value="${escapeHtml(candidate.time_end || "")}" />
                </div>
                <div class="field">
                  <label>Viewpoint scope</label>
                  <input name="patch_viewpoint_scope" value="${escapeHtml(candidate.viewpoint_scope || "")}" />
                </div>
              </div>
              <div class="toolbar">
                <button class="primary-button" type="submit" name="review_action" value="approve_with_edits">Submit edited approval</button>
                <button class="secondary-button" type="button" data-action="cancel-candidate-edit">Cancel edit mode</button>
              </div>
            ` : ""}
          </div>
        ` : ""}
      </form>
    `;
  }

  function formatReviewWeakness(reason) {
    return reason.replaceAll("_", " ");
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
            <h2 data-active-screen>Canon Ledger</h2>
            <p>Advanced utility for inspecting approved claims, status, provenance, and supporting evidence when you need the raw record.</p>
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
      <div class="toolbar">
        <button class="secondary-button" type="button" data-action="compose-from-claim">Compose into bible</button>
      </div>
    `;
  }

  function renderAskScreen() {
    const queryResult = state.queryResult;
    const activeSection =
      state.bible.sections.find((section) => section.section_id === state.bible.selectedSectionId)
      ?? state.bible.sections[0]
      ?? null;
    const advanced = state.workspaceMode === "advanced";

    return `
      <article class="screen fade-in">
        <div class="screen-head">
          <div>
            <h2 data-active-screen>Ask Canon</h2>
            <p>Ask only against approved canon. The answer should help you pressure-test the next scene, not guess past the trust boundary.${state.bible.profile?.project_name ? ` Active project: ${escapeHtml(state.bible.profile.project_name)}.` : ""}${activeSection ? ` Current section: ${escapeHtml(activeSection.title)}.` : ""}</p>
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
                <textarea id="question" name="question" placeholder="Ask about the approved record in a way that helps the next draft decision.">${escapeHtml(state.query.question)}</textarea>
              </div>
              <div class="detail-stack">
                <div class="field">
                  <label>Answer posture</label>
                  <div class="chip-bar">
                    ${renderModeChip("strict_facts", "Strict facts")}
                    ${renderModeChip("contested_views", "Include contested")}
                    ${renderModeChip("rumor_and_legend", "Include rumor")}
                  </div>
                </div>
                <div class="field">
                  <label for="place">Place filter</label>
                  <input id="place" name="place" value="${escapeHtml(state.query.place || state.bible.profile?.geography || "")}" placeholder="Rouen" />
                </div>
                <div class="field">
                  <label for="viewpoint">Viewpoint filter</label>
                  <input id="viewpoint" name="viewpoint" value="${escapeHtml(state.query.viewpoint || state.bible.profile?.social_lens || "")}" placeholder="townspeople, bakers, clergy" />
                </div>
                ${advanced ? `
                  <details class="field disclosure">
                    <summary>Advanced query controls</summary>
                    <div class="detail-stack" style="margin-top:12px;">
                      <div class="field">
                        <label>Full mode set</label>
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
                    </div>
                  </details>
                ` : ""}
              </div>
            </div>
            <div class="toolbar">
              <button class="primary-button" type="submit" data-action="query-submit">Ask canon</button>
              <span class="helper">Answers stay grounded in approved canon. When the record cannot answer directly, the UI should show the gap instead of improvising.${state.bible.projectId ? ` Project context is sent as ${escapeHtml(state.bible.projectId)} when available.` : ""}</span>
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
    const advanced = state.workspaceMode === "advanced";
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
      <div class="field">
        <label>Answer posture</label>
        <div class="detail-list">
          <div class="mini">
            <div class="detail-note">answer boundary</div>
            <div>${escapeHtml(renderAnswerBoundaryLabel(result.metadata?.answer_boundary || "research_gap"))}</div>
          </div>
          <div class="mini">
            <div class="detail-note">coverage gaps</div>
            <div>${escapeHtml((result.coverage_gaps || []).join(" | ") || "none")}</div>
          </div>
          ${advanced ? `
            <div class="mini">
              <div class="detail-note">retrieval</div>
              <div>${escapeHtml(renderRetrievalSummary(result.metadata || {}))}</div>
            </div>
            <div class="mini">
              <div class="detail-note">certainty summary</div>
              <div>${escapeHtml(renderKeyValueMap(result.certainty_summary || {}))}</div>
            </div>
          ` : ""}
        </div>
      </div>
      <div class="answer-block">
        <pre>${escapeHtml(result.answer)}</pre>
      </div>
      <div class="warning-list">
        ${result.warnings.length
          ? result.warnings.map((warning) => `<div class="warning">${escapeHtml(warning)}</div>`).join("")
          : "<div class='helper'>No warnings returned.</div>"}
      </div>
      <div class="toolbar">
        <button class="secondary-button" type="button" data-action="compose-from-query">Compose into bible</button>
      </div>
      <div class="field">
        <label>Next moves</label>
        <div class="detail-list">
          <div class="mini">
            <div class="detail-note">recommended next research</div>
            <div>${escapeHtml((result.recommended_next_research || []).join(" | ") || "none")}</div>
          </div>
          <div class="mini">
            <div class="detail-note">suggested follow-up questions</div>
            <div>${escapeHtml((result.suggested_follow_ups || []).join(" | ") || "none")}</div>
          </div>
        </div>
      </div>
      ${(result.nearby_claims || []).length
        ? `
          <div class="field">
            <label>Nearby approved canon</label>
            <div class="detail-list">
              ${result.nearby_claims
                .map(
                  (claim) => `
                    <div class="mini">
                      <div class="toolbar">
                        <strong>${escapeHtml(claim.subject)}</strong>
                        <span class="pill ${escapeHtml(claim.status)}">${escapeHtml(claim.status)}</span>
                      </div>
                      <div>${escapeHtml(claim.predicate)} · ${escapeHtml(claim.value)}</div>
                      <div class="detail-note">${escapeHtml(claim.place || "no place")} ${claim.time_start ? `· ${escapeHtml(claim.time_start)}` : ""}</div>
                    </div>
                  `
                )
                .join("")}
            </div>
          </div>
        `
        : ""}
      <div class="field">
        <label>Supporting claims</label>
        <div class="detail-list">
          ${result.supporting_claims.length
            ? result.supporting_claims
                .map(
                  (claim) => `
                    <div class="mini">
                      <div class="toolbar">
                        ${advanced ? `<div class="code">${escapeHtml(claim.claim_id)}</div>` : `<strong>${escapeHtml(claim.subject)}</strong>`}
                        <span class="pill ${escapeHtml(renderClaimLaneTone(claim.claim_id, result))}">${escapeHtml(renderClaimLaneLabel(claim.claim_id, result))}</span>
                      </div>
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

  function renderSourceStageSummary(summary) {
    const total = summary.total ?? 0;
    const extracted = summary.extracted ?? 0;
    const normalized = summary.normalized ?? 0;
    const missing = summary.missing ?? 0;
    const failed = summary.failed ?? 0;
    return `${total} docs · ${extracted} extracted · ${normalized} normalized · ${missing} missing · ${failed} failed`;
  }

  function renderDocumentStageLine(document) {
    return [
      `present ${document.present_in_latest_pull ? "yes" : "no"}`,
      `fetch ${document.attachment_fetch_status || "n/a"}`,
      `text ${document.text_extraction_status || "n/a"}`,
      `normalize ${document.normalization_status || "n/a"}`,
      `synced ${document.last_synced_at || "n/a"}`,
    ].join(" · ");
  }

  function renderSourceTypeOptions(selectedValue) {
    const options = [
      ["document", "Document"],
      ["record", "Record"],
      ["letter", "Letter"],
      ["chronicle", "Chronicle"],
      ["petition", "Petition"],
      ["ordinance", "Ordinance"],
      ["oral_history", "Oral history"],
      ["webpage", "Web page"],
    ];
    return options
      .map(([value, label]) => `<option value="${escapeHtml(value)}" ${selectedValue === value ? "selected" : ""}>${escapeHtml(label)}</option>`)
      .join("");
  }

  function buildIntakeFileFormData({ file, title, notes, sourceType, collectionKey }) {
    const payload = new FormData();
    payload.set("file", file);
    if (title) payload.set("title", title);
    if (notes) payload.set("notes", notes);
    if (sourceType) payload.set("source_type", sourceType);
    if (collectionKey) payload.set("collection_key", collectionKey);
    return payload;
  }

  function canAutoProcessIntake(result) {
    return (result.source_documents || []).some((document) =>
      document.raw_text
      || document.text_extraction_status === "extracted"
      || document.raw_text_status === "ready"
    );
  }

  async function refreshSourceDetail(sourceId, { quiet = false } = {}) {
    if (!sourceId) {
      state.selectedSourceDetail = null;
      return;
    }
    try {
      state.selectedSourceDetail = await fetchJson(API.source(sourceId));
      if (!quiet) {
        render();
      }
    } catch (error) {
      if (!quiet) {
        setBanner("failed", "Could not load source detail", error.message || "The source detail endpoint is unavailable.");
        render();
      }
    }
  }

  async function pullSources({ sourceIds = [], itemKeys = [], forceRefresh = false } = {}) {
    applyLoading(true);
    try {
      const payload = await fetchJson(API.pullSources, {
        method: "POST",
        body: {
          source_ids: sourceIds,
          item_keys: itemKeys,
          force_refresh: forceRefresh,
        },
      });
      if (Array.isArray(payload.sources)) {
        state.sources = payload.sources;
      }
      try {
        state.sources = await fetchJson(API.sources);
      } catch (error) {
        console.warn("Could not refresh sources after pull", error);
      }
      await refreshSourceDetail(state.selectedSourceId, { quiet: true });
      state.lastSync = new Date().toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
      nodes.lastSync.textContent = state.lastSync;
      await refreshRuntimeStatus();
      setApiStatus(true, `Pulled ${payload.count ?? state.sources.length} sources from Zotero.`);
      setBanner(
        payload.failed_document_count ? "queued" : "live",
        "Sources pulled",
        `Sources ${payload.inserted_source_count ?? 0} new, ${payload.updated_source_count ?? 0} refreshed, ${payload.unchanged_source_count ?? 0} unchanged. Documents ${payload.failed_document_count ?? 0} failed.`
      );
      render();
    } catch (error) {
      setApiStatus(false, "Source pull failed. Seed records remain in place.");
      setBanner("failed", "Could not pull sources", error.message || "The source pull endpoint is unavailable.");
    } finally {
      applyLoading(false);
      persistState();
    }
  }

  async function submitIntakeSource(form) {
    const formData = new FormData(form);
    const mode = String(formData.get("intake_mode") || "text").trim() || "text";
    const sourceType = String(formData.get("source_type") || "document").trim() || "document";
    const title = String(formData.get("title") || "").trim();
    const author = String(formData.get("author") || "").trim();
    const year = String(formData.get("year") || "").trim();
    const text = String(formData.get("text") || "");
    const url = String(formData.get("url") || "").trim();
    const notes = String(formData.get("notes") || "");
    const collectionKey = String(formData.get("collection_key") || "").trim();

    state.intakeDraft = {
      ...state.intakeDraft,
      mode,
      title,
      author,
      year,
      text,
      url,
      notes,
      source_type: sourceType,
      collection_key: collectionKey,
    };
    persistState();

    const file = formData.get("file");
    if (mode === "file" && (!(file instanceof File) || !file.size)) {
      setBanner("failed", "No file selected", "Choose a file before starting source intake.");
      render();
      return;
    }

    applyLoading(true);
    try {
      setBanner("queued", "Processing source", "Saving the source, preparing text, and moving the strongest new facts toward review.");
      render();

      let intakeResult;
      if (mode === "text") {
        intakeResult = await fetchJson(API.intakeText, {
          method: "POST",
          body: {
            title,
            text,
            author: nullableString(author),
            year: nullableString(year),
            source_type: sourceType,
            notes: nullableString(notes),
            collection_key: nullableString(collectionKey),
          },
        });
      } else if (mode === "url") {
        intakeResult = await fetchJson(API.intakeUrl, {
          method: "POST",
          body: {
            url,
            title: nullableString(title),
            notes: nullableString(notes),
            collection_key: nullableString(collectionKey),
          },
        });
      } else {
        intakeResult = await fetchJson(API.intakeFile, {
          method: "POST",
          body: buildIntakeFileFormData({
            file,
            title,
            notes,
            sourceType,
            collectionKey,
          }),
        });
      }

      const sourceIds = (intakeResult.pulled_sources || []).map((source) => source.source_id).filter(Boolean);
      if (sourceIds.length) {
        state.selectedSourceId = sourceIds[0];
      }

      await refreshLiveData({ quiet: true });
      await refreshSourceDetail(state.selectedSourceId, { quiet: true });
      state.intakeDraft = {
        ...state.intakeDraft,
        title: "",
        text: "",
        author: "",
        year: "",
        url: "",
        notes: "",
      };

      if (!sourceIds.length) {
        setBanner(
          "pending",
          "Source saved",
          "The source was created, but nothing was returned for normalization or extraction yet."
        );
        render();
        return;
      }

      const autoProcess = canAutoProcessIntake(intakeResult);
      if (!autoProcess) {
        setBanner(
          "pending",
          "Source saved",
          "The source was added, but no extractable text was available yet. Check the source detail for warnings."
        );
        render();
        return;
      }

      const normalization = await fetchJson(API.normalizeDocuments, {
        method: "POST",
        body: {
          source_ids: sourceIds,
          retry_failed: false,
        },
      });

      await refreshLiveData({ quiet: true });
      await refreshSourceDetail(state.selectedSourceId, { quiet: true });

      if (!(normalization.text_unit_count > 0)) {
        setBanner(
          normalization.warnings?.length ? "pending" : "live",
          "Source normalized",
          normalization.warnings?.[0]
            || "The source was saved, but no text units were created for extraction yet."
        );
        render();
        return;
      }

      const extraction = await runExtraction({
        sourceIds,
        quietSuccess: true,
        navigateToReview: true,
      });
      if (!extraction) {
        return;
      }

      const extractedCount = extraction.count ?? extraction.candidates?.length ?? 0;
      const warningCount = (intakeResult.warnings || []).length + (normalization.warnings || []).length;
      setBanner(
        warningCount ? "pending" : "live",
        "Source ready for review",
        extractedCount
          ? `Created ${extractedCount} candidate fact${extractedCount === 1 ? "" : "s"} and opened the review queue.`
          : "The source is saved and normalized, but extraction did not yield any candidate facts yet."
      );
      render();
    } catch (error) {
      setBanner("failed", "Source intake failed", error.message || "Could not process the new source.");
      render();
    } finally {
      applyLoading(false);
      persistState();
    }
  }

  async function retrySourceNormalization(sourceId) {
    if (!sourceId) {
      return;
    }
    applyLoading(true);
    try {
      const payload = await fetchJson(API.normalizeDocuments, {
        method: "POST",
        body: {
          source_ids: [sourceId],
          retry_failed: true,
        },
      });
      await refreshSourceDetail(sourceId, { quiet: true });
      state.sources = await fetchJson(API.sources);
      setBanner(
        payload.warnings?.length ? "queued" : "live",
        "Normalization retried",
        `Touched ${payload.document_count ?? 0} documents and created ${payload.text_unit_count ?? 0} text units.`
      );
      render();
    } catch (error) {
      setBanner("failed", "Could not retry normalization", error.message || "The normalization endpoint is unavailable.");
    } finally {
      applyLoading(false);
      persistState();
    }
  }

  async function runExtraction({
    sourceIds = [],
    quietSuccess = false,
    navigateToReview = false,
  } = {}) {
    applyLoading(true);
    try {
      const payload = await fetchJson(API.extractCandidates, {
        method: "POST",
        body: { source_ids: sourceIds },
      });
      if (Array.isArray(payload.candidates)) {
        state.candidates = mergeByKey(state.candidates, payload.candidates, "candidate_id");
      }
      if (Array.isArray(payload.evidence)) {
        state.evidence = mergeByKey(state.evidence, payload.evidence, "evidence_id");
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
      try {
        state.reviewQueue = await fetchJson(API.reviewQueue);
      } catch (error) {
        console.warn("Could not refresh review queue after extraction", error);
      }
      try {
        const [sources, workspaceSummary] = await Promise.all([
          fetchJson(API.sources),
          fetchJson(API.workspaceSummary),
        ]);
        state.sources = sources;
        state.workspaceSummary = workspaceSummary;
      } catch (error) {
        console.warn("Could not refresh workspace state after extraction", error);
      }
      if (state.selectedSourceId) {
        await refreshSourceDetail(state.selectedSourceId, { quiet: true });
      }
      state.selectedCandidateId =
        state.reviewQueue.find((candidate) => isUnresolvedReviewState(candidate.review_state))?.candidate_id ??
        state.candidates.find((candidate) => isUnresolvedReviewState(candidate.review_state))?.candidate_id ??
        state.reviewQueue[0]?.candidate_id ??
        state.candidates[0]?.candidate_id ??
        null;
      if (navigateToReview) {
        state.activeScreen = "review";
        location.hash = "#review";
      }
      state.lastSync = new Date().toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
      nodes.lastSync.textContent = state.lastSync;
      await refreshRuntimeStatus();
      setApiStatus(true, `Extraction returned ${payload.count ?? state.candidates.length} candidates.`);
      if (!quietSuccess) {
        setBanner("live", "Extraction complete", `Queued ${payload.count ?? state.candidates.length} candidates from the live endpoint.`);
      }
      render();
      return payload;
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
          note: "Extraction could not reach the live backend, so the workspace preserved the local run log.",
        },
        ...state.runs,
      ];
      state.selectedRunId = nextRunId;
      setApiStatus(false, "Extraction failed. Seed data remains available.");
      setBanner("failed", "Extraction failed", error.message || "The extraction endpoint is unavailable.");
      render();
      return null;
    } finally {
      applyLoading(false);
      persistState();
    }
  }

  async function submitReview(form, submitter) {
    const candidateId = form.dataset.candidateId || state.selectedCandidateId;
    if (!candidateId) {
      setBanner("failed", "No candidate selected", "Pick a candidate before submitting a review.");
      return;
    }

    const candidate = findReviewCandidate(candidateId);
    const formData = new FormData(form);
    const action = submitter?.value || "approve_suggested";
    const overrideStatus = formData.get("override_status");
    const notes = String(formData.get("notes") || "").trim();
    const reviewBody = {
      decision: action === "approve_suggested" || action === "approve_with_edits" ? "approve" : "reject",
      override_status:
        action === "approve_suggested" || action === "approve_with_edits"
          ? (overrideStatus || null)
          : null,
      defer_state:
        action === "needs_edit"
          ? "needs_edit"
          : action === "needs_split"
            ? "needs_split"
            : null,
      claim_patch:
        action === "approve_with_edits"
          ? compactReviewPatch({
              subject: formData.get("patch_subject"),
              predicate: formData.get("patch_predicate"),
              value: formData.get("patch_value"),
              place: formData.get("patch_place"),
              time_start: formData.get("patch_time_start"),
              time_end: formData.get("patch_time_end"),
              viewpoint_scope: formData.get("patch_viewpoint_scope"),
            }, candidate)
          : null,
      notes: notes || null,
    };

    applyLoading(true);
    try {
      const payload = await fetchJson(API.reviewCandidate(candidateId), {
        method: "POST",
        body: reviewBody,
      });

      if (payload.status === "rejected") {
        const nextState = reviewBody.defer_state || "rejected";
        state.candidates = state.candidates.map((candidate) =>
          candidate.candidate_id === candidateId ? { ...candidate, review_state: nextState } : candidate
        );
        setBanner(
          "live",
          nextState === "rejected" ? "Candidate rejected" : "Candidate deferred",
          nextState === "rejected"
            ? `Candidate ${candidateId} is now marked rejected.`
            : `Candidate ${candidateId} is now flagged ${nextState.replaceAll("_", " ")}.`
        );
      } else if (payload.claim) {
        state.candidates = state.candidates.map((candidate) =>
          candidate.candidate_id === candidateId ? { ...candidate, review_state: "approved" } : candidate
        );
        state.claims = [payload.claim, ...state.claims.filter((claim) => claim.claim_id !== payload.claim.claim_id)];
        setBanner(
          "live",
          action === "approve_with_edits" ? "Edited candidate approved" : "Candidate approved",
          `Claim ${payload.claim.claim_id} was written to the truth store.`
        );
      }

      await refreshAfterReview(candidateId);
    } catch (error) {
      const syncRejected = /^\d{3}\b/.test(String(error.message || ""));
      setApiStatus(syncRejected || state.apiOnline, "Review sync failed. Canon was not changed.");
      setBanner(
        "failed",
        "Review could not sync",
        error.message || "The review did not cross the trust boundary. Approved canon was not updated."
      );
      render();
    } finally {
      applyLoading(false);
      persistState();
    }
  }

  async function refreshAfterReview(candidateId) {
    try {
      const [candidates, reviewQueue, claims, workspaceSummary] = await Promise.all([
        fetchJson(API.candidates),
        fetchJson(API.reviewQueue),
        fetchJson(API.claims),
        fetchJson(API.workspaceSummary),
      ]);
      state.candidates = candidates;
      state.reviewQueue = reviewQueue;
      state.claims = claims;
      state.workspaceSummary = workspaceSummary;
      await refreshRuntimeStatus();
      state.selectedCandidateId =
        state.reviewQueue.find((candidate) => isUnresolvedReviewState(candidate.review_state))?.candidate_id ??
        state.reviewQueue.find((candidate) => candidate.candidate_id === candidateId)?.candidate_id ??
        state.reviewQueue[0]?.candidate_id ??
        null;
      state.editingCandidateId = null;
      updateSelectionFallbacks();
      setApiStatus(true, "Review sync complete.");
    } catch (error) {
      console.warn("Could not refresh review state", error);
    }
    render();
  }

  async function submitQuery(form) {
    const formData = new FormData(form);
    const mode = normalizeQueryMode(state.query.mode, state.workspaceMode);
    const request = {
      question: String(formData.get("question") || "").trim(),
      mode,
      project_id: state.bible.projectId || null,
      filters: {
        status: String(formData.get("status") || "").trim() || null,
        claim_kind: String(formData.get("claimKind") || "").trim() || null,
        place: String(formData.get("place") || "").trim() || null,
        viewpoint_scope: String(formData.get("viewpoint") || "").trim() || null,
      },
    };

    state.query = {
      question: request.question,
      mode,
      projectId: request.project_id || "",
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
      setBanner(
        "live",
        "Query answered",
        request.project_id
          ? "The question was resolved against approved claims using the active project context."
          : "The question was resolved against approved claims and their evidence."
      );
      setApiStatus(true, "Query response received.");
      render();
    } catch (error) {
      state.queryResult = {
        question: request.question,
        mode: request.mode,
        answer:
          "The live query endpoint was unavailable, so the workspace retained the ask form instead of fabricating an answer.",
        supporting_claims: [],
        evidence: [],
        warnings: [error.message || "Query endpoint unavailable."],
        certainty_summary: {},
        coverage_gaps: [],
        recommended_next_research: [],
        metadata: {
          retrieval_backend: "memory",
          fallback_used: true,
          fallback_reason: error.message || "Query endpoint unavailable.",
          ranking_strategy: request.project_id ? "intent_blended" : "lexical",
          retrieval_quality_tier: "memory_ranked",
          answer_boundary: "research_gap",
          used_nearby_context: false,
        },
        direct_match_claim_ids: [],
        adjacent_context_claim_ids: [],
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
      const job = await fetchJson(API.researchRuns, {
        method: "POST",
        body: request,
      });
      state.selectedResearchRunId = job.result_ref?.run_id || state.selectedResearchRunId;
      state.activeScreen = "research";
      location.hash = "#research";
      setBanner("queued", "Research queued", `Queued job ${job.job_id} for ${job.result_ref?.run_id || "research run"}.`);
      render();
      const settledJob = await pollJobUntilSettled(job.job_id, {
        onProgress: async (activeJob) => {
          setBanner("queued", "Research running", `Job ${activeJob.job_id} is ${renderJobSummary(activeJob)}.`);
          await refreshLiveData({ quiet: true });
          render();
        },
        onComplete: async (finishedJob) => {
          await refreshLiveData({ quiet: true });
          if (finishedJob.result_ref?.run_id) {
            await refreshResearchDetail(finishedJob.result_ref.run_id, { quiet: true });
          }
          if (finishedJob.status === "completed") {
            setBanner("live", "Research complete", `${finishedJob.result_ref?.run_id || "Research run"} finished in the background.`);
          } else {
            setBanner("failed", "Research failed", finishedJob.error || "Background research job failed.");
          }
        },
      });
      if (settledJob?.pollTimedOut) {
        setBanner("queued", "Research still running", `Job ${settledJob.job_id} is still running in the background. You can keep working and check Background Work anytime.`);
      }
      render();
    } catch (error) {
      setBanner("failed", "Research run failed", error.message || "The research run could not be created.");
      render();
    } finally {
      applyLoading(false);
      persistState();
    }
  }

  async function refreshBibleWorkspace({ quiet = false, projectId } = {}) {
    const activeProjectId = projectId || state.bible.projectId;
    if (!activeProjectId) {
      return;
    }
    try {
      const [profile, sections] = await Promise.all([
        fetchJson(API.bibleProfile(activeProjectId)),
        fetchJson(`${API.bibleSections}?project_id=${encodeURIComponent(activeProjectId)}`),
      ]);
      state.bible.projectId = activeProjectId;
      state.bible.profile = profile;
      state.bible.sections = sections;
      state.bible.selectedProvenance = null;
      updateSelectionFallbacks();
      if (state.bible.selectedSectionId) {
        await loadBibleProvenance(state.bible.selectedSectionId);
      }
      try {
        state.workspaceSummary = await fetchJson(`${API.workspaceSummary}?project_id=${encodeURIComponent(activeProjectId)}`);
      } catch (error) {
        console.warn("Could not refresh workspace summary from bible refresh", error);
      }
      if (!quiet) {
        setBanner("live", "Bible refreshed", `Loaded ${sections.length} saved bible sections for ${profile.project_name}.`);
      }
    } catch (error) {
      if (!quiet) {
        setBanner("failed", "Bible refresh failed", error.message || "Could not load the bible workspace.");
      }
    } finally {
      persistState();
      render();
    }
  }

  async function loadBibleProvenance(sectionId) {
    if (!sectionId) {
      state.bible.selectedProvenance = null;
      return;
    }
    try {
      state.bible.selectedProvenance = await fetchJson(API.bibleSectionProvenance(sectionId));
      const paragraphIds = (state.bible.selectedProvenance?.paragraphs || []).map((item) => item.paragraph?.paragraph_id).filter(Boolean);
      if (!paragraphIds.includes(state.bible.selectedParagraphId)) {
        state.bible.selectedParagraphId = paragraphIds[0] || null;
      }
    } catch (error) {
      console.warn("Could not load bible provenance", error);
    }
  }

  async function submitBibleProfile(form) {
    const formData = new FormData(form);
    const request = {
      project_name: String(formData.get("project_name") || "").trim(),
      era: nullableString(formData.get("era")),
      geography: nullableString(formData.get("geography")),
      social_lens: nullableString(formData.get("social_lens")),
      narrative_focus: nullableString(formData.get("narrative_focus")),
      tone: String(formData.get("tone") || "grounded_literary"),
      time_start: nullableString(formData.get("time_start")),
      time_end: nullableString(formData.get("time_end")),
      desired_facets: splitList(formData.get("desired_facets")),
      taboo_topics: splitList(formData.get("taboo_topics")),
      composition_defaults: {
        include_statuses: splitList(formData.get("include_statuses")),
        source_types: splitList(formData.get("source_types")),
        focus: state.bible.draft.focus || null,
      },
    };

    applyLoading(true);
    try {
      const payload = await fetchJson(API.bibleProfile(state.bible.projectId), {
        method: "PUT",
        body: request,
      });
      state.bible.profile = payload;
      state.bible.draft.place = payload.geography || state.bible.draft.place;
      state.bible.draft.time_start = payload.time_start || state.bible.draft.time_start;
      state.bible.draft.time_end = payload.time_end || state.bible.draft.time_end;
      setBanner("live", "Bible profile saved", `Updated ${payload.project_name}.`);
      render();
    } catch (error) {
      setBanner("failed", "Bible profile failed", error.message || "Could not save the bible profile.");
      render();
    } finally {
      applyLoading(false);
      persistState();
    }
  }

  async function submitBibleSection(form) {
    const formData = new FormData(form);
    state.bible.draft = {
      section_type: String(formData.get("section_type") || "setting_overview"),
      title: String(formData.get("title") || "").trim(),
      focus: String(formData.get("focus") || "").trim(),
      statuses: String(formData.get("statuses") || "").trim(),
      source_types: String(formData.get("source_types") || "").trim(),
      place: String(formData.get("place") || "").trim(),
      time_start: String(formData.get("time_start") || "").trim(),
      time_end: String(formData.get("time_end") || "").trim(),
    };

    const request = {
      project_id: state.bible.projectId,
      section_type: state.bible.draft.section_type,
      title: state.bible.draft.title || null,
      filters: {
        focus: nullableString(state.bible.draft.focus),
        statuses: splitList(state.bible.draft.statuses),
        source_types: splitList(state.bible.draft.source_types),
        place: nullableString(state.bible.draft.place),
        time_start: nullableString(state.bible.draft.time_start),
        time_end: nullableString(state.bible.draft.time_end),
      },
    };

    applyLoading(true);
    try {
      const job = await fetchJson(API.bibleSections, {
        method: "POST",
        body: request,
      });
      state.bible.selectedSectionId = job.result_ref?.section_id || state.bible.selectedSectionId;
      setBanner("queued", "Bible composition queued", `Queued job ${job.job_id} for ${job.result_ref?.section_id || "new section"}.`);
      await refreshBibleWorkspace({ quiet: true });
      render();
      const settledJob = await pollJobUntilSettled(job.job_id, {
        onProgress: async (activeJob) => {
          setBanner("queued", "Bible composition running", `Job ${activeJob.job_id} is ${renderJobSummary(activeJob)}.`);
          await refreshBibleWorkspace({ quiet: true });
        },
        onComplete: async (finishedJob) => {
          await refreshBibleWorkspace({ quiet: true });
          if (finishedJob.result_ref?.section_id) {
            state.bible.selectedSectionId = finishedJob.result_ref.section_id;
          }
          if (finishedJob.status === "completed") {
            const section = state.bible.sections.find((item) => item.section_id === finishedJob.result_ref?.section_id);
            setBanner("live", "Bible section composed", section ? `Saved ${section.title} with ${section.references.claim_ids.length} linked claims.` : "Bible composition finished.");
          } else {
            setBanner("failed", "Bible composition failed", finishedJob.error || "Background composition job failed.");
          }
        },
      });
      if (settledJob?.pollTimedOut) {
        setBanner("queued", "Bible composition still running", `Job ${settledJob.job_id} is still running in the background. The section will stay visible in Workspace and Background Work.`);
      }
      render();
    } catch (error) {
      setBanner("failed", "Bible composition failed", error.message || "Could not compose the bible section.");
      render();
    } finally {
      applyLoading(false);
      persistState();
    }
  }

  async function submitBibleSectionEdit(form) {
    const formData = new FormData(form);
    const sectionId = String(formData.get("section_id") || "").trim();
    if (!sectionId) {
      return;
    }
    applyLoading(true);
    try {
      const payload = await fetchJson(API.bibleSection(sectionId), {
        method: "PUT",
        body: {
          title: String(formData.get("title") || "").trim() || null,
          content: String(formData.get("content") || ""),
        },
      });
      state.bible.sections = state.bible.sections.map((section) =>
        section.section_id === sectionId ? payload : section
      );
      state.bible.selectedSectionId = sectionId;
      setBanner("live", "Bible edits saved", `Manual edits were saved for ${payload.title}.`);
      render();
    } catch (error) {
      setBanner("failed", "Bible save failed", error.message || "Could not save section edits.");
      render();
    } finally {
      applyLoading(false);
      persistState();
    }
  }

  async function regenerateBibleSection(sectionId) {
    if (!sectionId) {
      setBanner("failed", "No bible section selected", "Pick a bible section before regenerating it.");
      return;
    }
    const section = state.bible.sections.find((item) => item.section_id === sectionId);
    applyLoading(true);
    try {
      const job = await fetchJson(API.regenerateBibleSection(sectionId), {
        method: "POST",
        body: {
          filters: section?.generation_filters || {},
        },
      });
      setBanner("queued", "Regeneration queued", `Queued job ${job.job_id} for ${sectionId}.`);
      const settledJob = await pollJobUntilSettled(job.job_id, {
        onProgress: async (activeJob) => {
          setBanner("queued", "Regeneration running", `Job ${activeJob.job_id} is ${renderJobSummary(activeJob)}.`);
          await refreshBibleWorkspace({ quiet: true });
        },
        onComplete: async (finishedJob) => {
          await refreshBibleWorkspace({ quiet: true });
          const refreshed = state.bible.sections.find((item) => item.section_id === sectionId);
          if (finishedJob.status === "completed") {
            setBanner("live", "Section regenerated", refreshed?.has_manual_edits ? "Generated draft refreshed while manual edits stayed intact." : `Regenerated ${refreshed?.title || "section"}.`);
          } else {
            setBanner("failed", "Regeneration failed", finishedJob.error || "Background regeneration job failed.");
          }
        },
      });
      if (settledJob?.pollTimedOut) {
        setBanner("queued", "Regeneration still running", `Job ${settledJob.job_id} is still running in the background. Manual text remains safe while the generated draft refreshes.`);
      }
      render();
    } catch (error) {
      setBanner("failed", "Regeneration failed", error.message || "Could not regenerate the selected bible section.");
      render();
    } finally {
      applyLoading(false);
      persistState();
    }
  }

  async function exportBibleProject() {
    if (!state.bible.projectId) {
      setBanner("failed", "No active bible project", "Save a bible project profile before exporting.");
      return;
    }
    applyLoading(true);
    try {
      const job = await fetchJson(API.queueBibleExport(state.bible.projectId), { method: "POST" });
      state.bible.exportJobId = job.job_id;
      setBanner("queued", "Bible export queued", `Queued job ${job.job_id} for ${state.bible.projectId}.`);
      render();
      const settledJob = await pollJobUntilSettled(job.job_id, {
        onProgress: async (activeJob) => {
          state.bible.exportJobId = activeJob.job_id;
          setBanner("queued", "Bible export running", `Job ${activeJob.job_id} is ${renderJobSummary(activeJob)}.`);
        },
        onComplete: async (finishedJob) => {
          state.bible.exportJobId = finishedJob.job_id;
          if (finishedJob.result_payload?.profile) {
            state.bible.exportBundle = finishedJob.result_payload;
          } else {
            state.bible.exportBundle = await fetchJson(API.exportBibleProject(state.bible.projectId));
          }
          if ((finishedJob.status_label || finishedJob.status) === "failed") {
            setBanner("failed", "Bible export failed", finishedJob.error_detail || finishedJob.error || "Background export failed.");
            return;
          }
          if ((finishedJob.status_label || finishedJob.status) === "partial") {
            setBanner("pending", "Bible export partial", (finishedJob.warnings || []).join(" | ") || "Export completed with warnings.");
            return;
          }
          setBanner(
            "live",
            "Bible exported",
            `Prepared ${state.bible.exportBundle.sections.length} saved sections for ${state.bible.exportBundle.profile.project_name}.`
          );
        },
      });
      if (settledJob?.pollTimedOut) {
        setBanner("queued", "Bible export still running", `Job ${settledJob.job_id} is still preparing the export bundle in the background.`);
      }
      render();
    } catch (error) {
      setBanner("failed", "Bible export failed", error.message || "Could not export the saved bible.");
    } finally {
      applyLoading(false);
      persistState();
      render();
    }
  }

  async function cancelJob(jobId) {
    if (!jobId) {
      return;
    }
    try {
      const job = await fetchJson(API.cancelJob(jobId), { method: "POST" });
      state.jobs = [job, ...state.jobs.filter((item) => item.job_id !== job.job_id)];
      await refreshLiveData({ quiet: true });
      if (state.bible.projectId) {
        await refreshBibleWorkspace({ quiet: true });
      }
      if (state.selectedResearchRunId) {
        await refreshResearchDetail(state.selectedResearchRunId, { quiet: true });
      }
      setBanner("pending", "Cancellation requested", `Job ${job.job_id} is now ${job.status_label || job.status}.`);
    } catch (error) {
      setBanner("failed", "Cancel failed", error.message || "Could not cancel the selected job.");
    } finally {
      render();
    }
  }

  async function retryJob(jobId) {
    if (!jobId) {
      return;
    }
    try {
      const job = await fetchJson(API.retryJob(jobId), { method: "POST" });
      setBanner("queued", "Retry queued", `Queued retry job ${job.job_id}.`);
      const settledJob = await pollJobUntilSettled(job.job_id, {
        onProgress: async (activeJob) => {
          setBanner("queued", "Retry running", `Job ${activeJob.job_id} is ${renderJobSummary(activeJob)}.`);
        },
        onComplete: async (finishedJob) => {
          await refreshLiveData({ quiet: true });
          if (state.bible.projectId) {
            await refreshBibleWorkspace({ quiet: true });
          }
          if (state.selectedResearchRunId) {
            await refreshResearchDetail(state.selectedResearchRunId, { quiet: true });
          }
          if ((finishedJob.status_label || finishedJob.status) === "failed") {
            setBanner("failed", "Retry failed", finishedJob.error_detail || finishedJob.error || "Retry job failed.");
          } else {
            setBanner("live", "Retry completed", `Job ${finishedJob.job_id} finished as ${finishedJob.status_label || finishedJob.status}.`);
          }
        },
      });
      if (settledJob?.pollTimedOut) {
        setBanner("queued", "Retry still running", `Job ${settledJob.job_id} is still running in the background. You can keep working and revisit the job list later.`);
      }
    } catch (error) {
      setBanner("failed", "Retry failed", error.message || "Could not retry the selected job.");
    } finally {
      render();
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
      const job = await fetchJson(API.stageResearchRun(runId), { method: "POST" });
      setBanner("queued", "Research staging queued", `Queued job ${job.job_id} for ${runId}.`);
      const settledJob = await pollJobUntilSettled(job.job_id, {
        onProgress: async (activeJob) => {
          setBanner("queued", "Research staging running", `Job ${activeJob.job_id} is ${renderJobSummary(activeJob)}.`);
          await refreshResearchDetail(runId, { quiet: true });
        },
        onComplete: async (finishedJob) => {
          await refreshLiveData({ quiet: true });
          await refreshResearchDetail(runId, { quiet: true });
          if (finishedJob.status === "completed") {
            setApiStatus(true, `Staged accepted research findings for ${runId}.`);
            setBanner("live", "Research staged", `Accepted findings for ${runId} were staged into source records.`);
          } else {
            setBanner("failed", "Staging failed", finishedJob.error || "Background staging job failed.");
          }
        },
      });
      if (settledJob?.pollTimedOut) {
        setBanner("queued", "Research staging still running", `Job ${settledJob.job_id} is still staging in the background. Background Work will keep tracking it.`);
      }
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
      const job = await fetchJson(API.extractResearchRun(runId), { method: "POST" });
      setBanner("queued", "Research extraction queued", `Queued job ${job.job_id} for ${runId}.`);
      const settledJob = await pollJobUntilSettled(job.job_id, {
        onProgress: async (activeJob) => {
          setBanner("queued", "Research extraction running", `Job ${activeJob.job_id} is ${renderJobSummary(activeJob)}.`);
          await refreshResearchDetail(runId, { quiet: true });
        },
        onComplete: async (finishedJob) => {
          await refreshLiveData({ quiet: true });
          await refreshResearchDetail(runId, { quiet: true });
          state.selectedCandidateId =
            state.candidates.find((candidate) => isUnresolvedReviewState(candidate.review_state))?.candidate_id ??
            state.candidates[0]?.candidate_id ??
            null;
          if (finishedJob.status === "completed") {
            setApiStatus(true, `Research extraction completed for ${runId}.`);
            setBanner("live", "Research extracted", `Background extraction finished for ${runId}.`);
          } else {
            setBanner("failed", "Research extraction failed", finishedJob.error || "Background extraction job failed.");
          }
        },
      });
      if (settledJob?.pollTimedOut) {
        setBanner("queued", "Research extraction still running", `Job ${settledJob.job_id} is still extracting in the background. You can keep working while it finishes.`);
      }
      render();
    } catch (error) {
      setBanner("failed", "Research extraction failed", error.message || "Could not stage and extract the selected research run.");
      render();
    } finally {
      applyLoading(false);
      persistState();
    }
  }

  async function pollJobUntilSettled(jobId, { onProgress, onComplete } = {}) {
    if (!jobId) {
      return null;
    }
    const startedAt = Date.now();
    let lastJob = null;
    while (Date.now() - startedAt < 600000) {
      const job = await fetchJson(API.job(jobId));
      lastJob = job;
      state.jobs = [job, ...state.jobs.filter((item) => item.job_id !== job.job_id)];
      if (onProgress) {
        await onProgress(job);
      }
      if (["completed", "failed", "cancelled", "partial"].includes(job.status_label || job.status)) {
        if (onComplete) {
          await onComplete(job);
        }
        return job;
      }
      const elapsed = Date.now() - startedAt;
      const interval = elapsed < 20000 ? 500 : 2000;
      await new Promise((resolve) => window.setTimeout(resolve, interval));
    }
    return lastJob ? { ...lastJob, pollTimedOut: true } : null;
  }

  function renderJobSummary(job) {
    if (!job) {
      return "idle";
    }
    const total = Number(job.progress_total || 100);
    const current = Number(job.progress_current || 0);
    const percent = total > 0 ? Math.max(0, Math.min(100, Math.round((current / total) * 100))) : 0;
    const label = (job.worker_state || job.status_label || job.status || "queued").replaceAll("_", " ");
    return `${label}${job.progress_stage ? ` · ${job.progress_stage.replaceAll("_", " ")}` : ""}${["running", "queued", "cancel_requested"].includes(job.worker_state || job.status_label || job.status) ? ` · ${percent}%` : ""}`;
  }

  function renderJobPill(job) {
    if (!job) {
      return `<span class="pill verified">idle</span>`;
    }
    const state = job.worker_state || job.status_label || job.status;
    const tone =
      state === "completed"
        ? "verified"
        : state === "failed" || state === "stalled"
          ? "contested"
          : state === "partial"
            ? "probable"
            : state === "cancelled"
              ? "author_choice"
              : state === "cancel_requested"
                ? "probable"
                : "queued";
    return `<span class="pill ${escapeHtml(tone)}">${escapeHtml(renderJobSummary(job))}</span>`;
  }

  function renderJobHeadline(job) {
    if (!job) {
      return "Background workflow status is available.";
    }
    return (
      job.progress_message ||
      job.stalled_reason ||
      job.degraded_reason ||
      job.error_detail ||
      job.error ||
      renderJobSummary(job)
    );
  }

  function renderAnswerBoundaryLabel(boundary) {
    if (boundary === "direct_answer") return "Direct canon answer";
    if (boundary === "adjacent_context") return "Nearby canon only";
    return "Research gap";
  }

  function renderClaimLaneLabel(claimId, result) {
    const direct = new Set(result.direct_match_claim_ids || []);
    const adjacent = new Set(result.adjacent_context_claim_ids || []);
    if (direct.has(claimId)) return "approved answer";
    if (adjacent.has(claimId)) return "nearby canon";
    return "supporting context";
  }

  function renderClaimLaneTone(claimId, result) {
    const direct = new Set(result.direct_match_claim_ids || []);
    const adjacent = new Set(result.adjacent_context_claim_ids || []);
    if (direct.has(claimId)) return "verified";
    if (adjacent.has(claimId)) return "probable";
    return "queued";
  }

  function isUnresolvedReviewState(reviewState) {
    return ["pending", "needs_split", "needs_edit"].includes(reviewState);
  }

  function isDeferredReviewState(reviewState) {
    return ["needs_split", "needs_edit"].includes(reviewState);
  }

  function canApproveCandidateAsSuggested(candidate) {
    return !isDeferredReviewState(candidate?.review_state);
  }

  function getReviewCards(sourceState = state) {
    return sourceState.reviewQueue?.length ? sourceState.reviewQueue : sourceState.candidates;
  }

  function findReviewCandidate(candidateId) {
    return getReviewCards().find((candidate) => candidate.candidate_id === candidateId)
      ?? state.candidates.find((candidate) => candidate.candidate_id === candidateId)
      ?? null;
  }

  function compactReviewPatch(patch, candidate = null) {
    const entries = Object.entries(patch || {}).reduce((accumulator, [key, rawValue]) => {
      const value = nullableString(rawValue);
      if (value && value !== nullableString(candidate?.[key])) {
        accumulator.push([key, value]);
      }
      return accumulator;
    }, []);
    return entries.length ? Object.fromEntries(entries) : null;
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

  function mergeByKey(existing, incoming, key) {
    const merged = new Map((existing || []).map((item) => [item?.[key], item]));
    for (const item of incoming || []) {
      if (!item?.[key]) {
        continue;
      }
      merged.set(item[key], item);
    }
    return Array.from(merged.values());
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
    const isFormData = options.body instanceof FormData;
    const init = {
      headers: { ...(options.headers || {}) },
      ...options,
    };
    if (!isFormData && !("Content-Type" in init.headers)) {
      init.headers["Content-Type"] = "application/json";
    }
    if (!isFormData && options.body && typeof options.body !== "string") {
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
    const projection = runtime.services.find((service) => service.name === "projection");
    const projectionSummary =
      projection?.mode === "qdrant:uninitialized"
        ? " Projection is uninitialized, so query and composition are using memory ranking until the collection exists."
        : projection && (projection.mode === "disabled" || !projection.ready)
          ? " Projection is degraded, so query and composition are falling back to memory ranking."
          : "";
    const statusLabel =
      runtime.overall_status === "ready"
        ? "healthy"
        : runtime.overall_status === "degraded"
          ? "degraded"
          : "blocked";
    return `Runtime is ${statusLabel}. ${readyCount}/${runtime.services.length} services are currently usable. Extraction backend: ${runtime.extraction_backend}.${projectionSummary}`;
  }

  function runtimeStatusTone(status) {
    if (status === "ready") return "live";
    if (status === "degraded") return "probable";
    return "queued";
  }

  function runtimeServiceTone(service) {
    if (service.mode === "disabled" || service.mode === "stub" || service.mode === "heuristic") {
      return "probable";
    }
    if (service.ready) {
      return "live";
    }
    if (service.mode === "qdrant:uninitialized") {
      return "queued";
    }
    return service.configured ? "failed" : "queued";
  }

  function runtimeServiceLabel(service) {
    if (service.mode === "disabled" || service.mode === "stub" || service.mode === "heuristic") {
      return "degraded";
    }
    if (service.mode === "qdrant:uninitialized" && !service.ready) {
      return "uninitialized";
    }
    return service.ready ? "healthy" : "attention";
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
