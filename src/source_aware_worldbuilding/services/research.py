from __future__ import annotations

import posixpath
import re
import time
from difflib import SequenceMatcher
from hashlib import sha1
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from uuid import uuid4

import httpx
from tenacity import Retrying, retry_if_exception, stop_after_attempt, wait_exponential_jitter

from source_aware_worldbuilding.adapters.web_research_scout import ResearchScoutRegistry
from source_aware_worldbuilding.domain.enums import ResearchFindingDecision, ResearchRunStatus
from source_aware_worldbuilding.domain.enums import (
    ResearchCoverageStatus,
    ResearchFetchOutcome,
    ResearchFindingReason,
)
from source_aware_worldbuilding.domain.models import (
    ResearchBrief,
    ResearchCuratedInput,
    ResearchExecutionPolicy,
    ResearchExtractResult,
    ResearchFacet,
    ResearchFacetCoverage,
    ResearchFetchedPage,
    ResearchFinding,
    ResearchFindingProvenance,
    ResearchFindingScoring,
    ResearchProgram,
    ResearchProgramCreateRequest,
    ResearchRun,
    ResearchRunDetail,
    ResearchRunRequest,
    ResearchSemanticMatch,
    ResearchSemanticResult,
    ResearchRunStageResult,
    ResearchRunTelemetry,
    ResearchScoutCapabilities,
    ResearchSearchHit,
    SourceDocumentRecord,
    SourceRecord,
    utc_now,
)
from source_aware_worldbuilding.ports import (
    ResearchFindingStorePort,
    ResearchProgramStorePort,
    ResearchRunStorePort,
    ResearchScoutAdapterPort,
    ResearchSemanticPort,
    SourceDocumentStorePort,
    SourceStorePort,
)
from source_aware_worldbuilding.services.ingestion import IngestionService
from source_aware_worldbuilding.services.normalization import NormalizationService

_TOKEN_RE = re.compile(r"[a-z0-9]{3,}")
_STOPWORDS = {
    "about",
    "after",
    "around",
    "before",
    "between",
    "from",
    "history",
    "into",
    "more",
    "that",
    "their",
    "these",
    "this",
    "those",
    "what",
    "when",
    "where",
    "which",
    "with",
}
_DEFAULT_PROGRAM_ID = "default-generic"
_DEFAULT_FACETS: dict[str, tuple[str, str]] = {
    "people": ("People", "participants eyewitnesses figures biographies"),
    "places": ("Places", "locations venues neighborhoods geographies"),
    "institutions": ("Institutions", "organizations labels networks clubs publishers"),
    "practices": ("Practices", "habits routines workflows customs methods"),
    "events": ("Events", "events milestones incidents timelines"),
    "objects_technology": ("Objects / Technology", "tools gear formats artifacts technology"),
    "language_slang": ("Language / Slang", "phrases terminology slang discourse"),
    "economics_commercial": ("Economics / Commercial Context", "prices business trade money markets commerce"),
    "media_culture": ("Media / Culture", "coverage magazines radio press television culture"),
    "regional_context": ("Region-Specific Context", "local scene regional context local history"),
}
_PREFERRED_CLASS_BOOSTS = {
    "government": 0.2,
    "educational": 0.2,
    "archive": 0.18,
    "news": 0.16,
    "magazine": 0.08,
}
_PENALIZED_CLASSES = {
    "forum": 0.18,
    "social": 0.2,
    "shopping": 0.12,
    "blog": 0.06,
}
_TRACKING_PARAMS = {
    "fbclid",
    "gclid",
    "igshid",
    "mc_cid",
    "mc_eid",
    "mkt_tok",
    "ref",
    "ref_src",
    "source",
}
_GENERIC_QUERY_PARAMS = {
    "amp",
    "output",
    "print",
    "referrer",
    "replytocom",
    "view",
}
_TITLE_SUFFIX_NOISE = (
    "official site",
    "homepage",
    "home page",
    "article",
    "news",
    "feature",
)
_DEFAULT_PAGE_NAMES = {
    "index",
    "index.html",
    "index.htm",
    "default",
    "default.html",
    "default.htm",
    "default.aspx",
}
_RETRYABLE_STATUSES = {408, 409, 425, 429, 500, 502, 503, 504}


class ResearchService:
    def __init__(
        self,
        scout_registry: ResearchScoutRegistry,
        run_store: ResearchRunStorePort,
        finding_store: ResearchFindingStorePort,
        program_store: ResearchProgramStorePort,
        source_store: SourceStorePort,
        source_document_store: SourceDocumentStorePort,
        normalization_service: NormalizationService,
        ingestion_service: IngestionService,
        research_semantic: ResearchSemanticPort,
        *,
        default_program_markdown: str,
        default_execution_policy: ResearchExecutionPolicy,
        default_adapter_id: str,
        research_user_agent: str,
        semantic_duplicate_threshold: float,
        semantic_novelty_floor: float,
        semantic_rerank_weight: float,
    ) -> None:
        self.scout_registry = scout_registry
        self.run_store = run_store
        self.finding_store = finding_store
        self.program_store = program_store
        self.source_store = source_store
        self.source_document_store = source_document_store
        self.normalization_service = normalization_service
        self.ingestion_service = ingestion_service
        self.research_semantic = research_semantic
        self.default_program_markdown = default_program_markdown
        self.default_execution_policy = default_execution_policy
        self.default_adapter_id = default_adapter_id
        self.research_user_agent = research_user_agent
        self.semantic_duplicate_threshold = semantic_duplicate_threshold
        self.semantic_novelty_floor = semantic_novelty_floor
        self.semantic_rerank_weight = semantic_rerank_weight

    def list_runs(self) -> list[ResearchRun]:
        return self.run_store.list_runs()

    def list_programs(self) -> list[ResearchProgram]:
        programs = {program.program_id: program for program in self.program_store.list_programs()}
        programs[self._default_program().program_id] = self._default_program()
        return sorted(programs.values(), key=lambda item: (not item.built_in, item.name.lower()))

    def create_program(self, request: ResearchProgramCreateRequest) -> ResearchProgram:
        now = utc_now()
        program = ResearchProgram(
            program_id=request.program_id or f"program-{uuid4().hex[:12]}",
            name=request.name,
            description=request.description,
            markdown=request.markdown,
            built_in=False,
            default_facets=request.default_facets,
            default_adapter_id=request.default_adapter_id,
            default_execution_policy=request.default_execution_policy
            or self.default_execution_policy.model_copy(deep=True),
            preferred_source_classes=request.preferred_source_classes,
            excluded_source_classes=request.excluded_source_classes,
            quality_threshold=request.quality_threshold,
            dedupe_similarity_threshold=request.dedupe_similarity_threshold,
            created_at=now,
            updated_at=now,
        )
        self.program_store.save_program(program)
        return program

    def get_run_detail(self, run_id: str) -> ResearchRunDetail | None:
        run = self.run_store.get_run(run_id)
        if run is None:
            return None
        findings = sorted(
            self.finding_store.list_findings(run_id=run_id),
            key=lambda item: (item.decision != ResearchFindingDecision.ACCEPTED, -item.score, item.title),
        )
        return ResearchRunDetail(
            run=run,
            findings=findings,
            program=self._resolve_program(run.program_id),
            facet_coverage=self._build_facet_coverage(run.facets, findings),
        )

    def run_research(self, request: ResearchRunRequest) -> ResearchRunDetail:
        program = self._resolve_program(request.program_id)
        facets = self._expand_facets(request.brief, program)
        execution_policy = self._resolve_execution_policy(request.brief, program)
        adapter_id = request.brief.adapter_id or program.default_adapter_id or self.default_adapter_id
        run = ResearchRun(
            run_id=f"research-{uuid4().hex[:12]}",
            status=ResearchRunStatus.RUNNING,
            brief=request.brief,
            program_id=program.program_id,
            facets=facets,
            telemetry=ResearchRunTelemetry(),
            logs=[f"Using research program {program.program_id}.", f"Using adapter {adapter_id}."],
        )
        run.telemetry.semantic.backend = "qdrant"
        self.run_store.save_run(run)

        adapter = self.scout_registry.get(adapter_id)
        if adapter is None:
            return self._finish_policy_failure(
                run,
                facets,
                f"Research adapter {adapter_id} was not found.",
            )

        validation_error = self._validate_adapter_request(adapter, request.brief, execution_policy)
        if validation_error:
            return self._finish_policy_failure(run, facets, validation_error)

        started_monotonic = time.monotonic()
        accepted_signatures: dict[str, dict[str, str]] = {}
        findings: list[ResearchFinding] = []
        stop_reason: str | None = None

        try:
            if request.brief.curated_inputs:
                run.query_count = 0
                run.telemetry.total_queries = 0
                run.logs.append(
                    f"Processing {len(request.brief.curated_inputs)} curated input(s)."
                )
                stop_reason = self._process_curated_inputs(
                    adapter_id=adapter_id,
                    adapter=adapter,
                    brief=request.brief,
                    program=program,
                    facets=facets,
                    findings=findings,
                    accepted_signatures=accepted_signatures,
                    run=run,
                    policy=execution_policy,
                    started_monotonic=started_monotonic,
                )
            else:
                queries = self._build_queries(request.brief, facets, program)
                run.query_count = len(queries)
                run.telemetry.total_queries = len(queries)
                run.logs.append(f"Planned {len(queries)} query/facet combinations.")
                stop_reason = self._process_search_queries(
                    adapter_id=adapter_id,
                    adapter=adapter,
                    brief=request.brief,
                    program=program,
                    facets=facets,
                    findings=findings,
                    accepted_signatures=accepted_signatures,
                    run=run,
                    policy=execution_policy,
                    queries=queries,
                    started_monotonic=started_monotonic,
                )

            self._sync_run_progress(run, facets, findings, started_monotonic)
            self._index_research_findings(
                run,
                [
                    item
                    for item in findings
                    if item.decision != ResearchFindingDecision.ACCEPTED
                ],
            )
            run.completed_at = utc_now()
            if stop_reason:
                run.logs.append(stop_reason)
                run.status = ResearchRunStatus.COMPLETED_PARTIAL
            else:
                run.status = ResearchRunStatus.COMPLETED
            if (
                run.status == ResearchRunStatus.COMPLETED
                and (run.telemetry.fallback_flags or run.telemetry.semantic.fallback_used)
            ):
                run.status = ResearchRunStatus.DEGRADED_FALLBACK
            self.run_store.update_run(run)
            return self.get_run_detail(run.run_id)  # type: ignore[return-value]
        except Exception as exc:
            run.status = ResearchRunStatus.FAILED_RUNTIME
            run.error = str(exc)
            run.completed_at = utc_now()
            run.logs.append(f"Run failed at runtime: {exc}")
            self._record_failure(run.telemetry, "runtime_error")
            self._sync_run_progress(run, facets, findings, started_monotonic)
            self.run_store.update_run(run)
            raise

    def stage_run(self, run_id: str) -> ResearchRunStageResult:
        run = self.run_store.get_run(run_id)
        if run is None:
            raise ValueError("Research run not found.")

        findings = self.finding_store.list_findings(run_id=run_id)
        accepted = [
            item
            for item in findings
            if item.decision == ResearchFindingDecision.ACCEPTED and item.staged_source_id is None
        ]
        staged_source_ids: list[str] = []
        staged_document_ids: list[str] = []
        warnings: list[str] = []
        source_records: list[SourceRecord] = []
        documents: list[SourceDocumentRecord] = []

        for finding in accepted:
            source_id = f"research-source-{sha1(finding.finding_id.encode()).hexdigest()[:12]}"
            document_id = f"research-doc-{sha1(finding.finding_id.encode()).hexdigest()[:12]}"
            source_records.append(
                SourceRecord(
                    source_id=source_id,
                    external_source="research_scout",
                    external_id=finding.finding_id,
                    title=finding.title,
                    author=finding.publisher,
                    year=self._year_from_date(finding.published_at) or self._year_from_brief(run.brief),
                    source_type=finding.source_type or "webpage",
                    locator_hint=finding.locator or finding.query,
                    abstract=finding.snippet_text,
                    url=finding.canonical_url or finding.url,
                    sync_status="awaiting_text_extraction",
                    raw_metadata_json={
                        "run_id": run.run_id,
                        "query": finding.query,
                        "facet_id": finding.facet_id,
                        "publisher": finding.publisher,
                        "published_at": finding.published_at,
                        "score": finding.score,
                        "canonical_url": finding.canonical_url,
                    },
                )
            )
            documents.append(
                SourceDocumentRecord(
                    document_id=document_id,
                    source_id=source_id,
                    document_kind="manual_text",
                    external_id=finding.canonical_url or finding.url,
                    filename=f"{finding.finding_id}.txt",
                    mime_type="text/plain",
                    ingest_status="imported",
                    raw_text_status="ready",
                    claim_extraction_status="queued",
                    locator=finding.locator or finding.url,
                    raw_text=self._build_staged_text(run, finding),
                    raw_metadata_json={
                        "run_id": run.run_id,
                        "finding_id": finding.finding_id,
                        "query": finding.query,
                    },
                )
            )
            finding.staged_source_id = source_id
            finding.staged_document_id = document_id
            staged_source_ids.append(source_id)
            staged_document_ids.append(document_id)

        if source_records:
            self.source_store.save_sources(source_records)
            self.source_document_store.save_source_documents(documents)
            for finding in accepted:
                self.finding_store.update_finding(finding)
        else:
            warnings.append("No accepted findings needed staging.")

        run.staged_count = len(
            [
                item
                for item in self.finding_store.list_findings(run_id=run_id)
                if item.staged_source_id is not None
            ]
        )
        self.run_store.update_run(run)
        return ResearchRunStageResult(
            run=run,
            staged_source_ids=staged_source_ids,
            staged_document_ids=staged_document_ids,
            warnings=warnings,
        )

    def extract_run(self, run_id: str) -> ResearchExtractResult:
        stage_result = self.stage_run(run_id)
        staged_findings = self.finding_store.list_findings(run_id=run_id)
        staged_source_ids = [
            item.staged_source_id for item in staged_findings if item.staged_source_id is not None
        ]
        staged_document_ids = [
            item.staged_document_id for item in staged_findings if item.staged_document_id is not None
        ]
        normalization = self.normalization_service.normalize_documents(
            document_ids=staged_document_ids,
            source_ids=staged_source_ids,
        )
        extraction = self.ingestion_service.extract_candidates(source_ids=staged_source_ids)
        run = self.run_store.get_run(run_id)
        if run is None:
            raise ValueError("Research run not found after extraction.")
        run.extraction_run_id = extraction.run.run_id
        self.run_store.update_run(run)
        stage_result.run = run
        return ResearchExtractResult(
            stage_result=stage_result,
            normalization=normalization,
            extraction=extraction,
        )

    def _default_program(self) -> ResearchProgram:
        now = utc_now()
        return ResearchProgram(
            program_id=_DEFAULT_PROGRAM_ID,
            name="Generic Subject / Era Research",
            description="Default generic research program for broad subjects and eras.",
            markdown=self.default_program_markdown,
            built_in=True,
            default_facets=list(_DEFAULT_FACETS),
            default_adapter_id=self.default_adapter_id,
            default_execution_policy=self.default_execution_policy.model_copy(deep=True),
            preferred_source_classes=["government", "educational", "archive", "news"],
            excluded_source_classes=["social"],
            quality_threshold=0.45,
            dedupe_similarity_threshold=0.9,
            created_at=now,
            updated_at=now,
        )

    def _resolve_program(self, program_id: str | None) -> ResearchProgram:
        if not program_id or program_id == _DEFAULT_PROGRAM_ID:
            return self._default_program()
        program = self.program_store.get_program(program_id)
        if program is None:
            raise ValueError(f"Research program {program_id} was not found.")
        return program

    def _resolve_execution_policy(
        self,
        brief: ResearchBrief,
        program: ResearchProgram,
    ) -> ResearchExecutionPolicy:
        base = self.default_execution_policy.model_dump(mode="python")
        base.update(program.default_execution_policy.model_dump(mode="python"))
        if brief.execution_policy is not None:
            base.update(brief.execution_policy.model_dump(mode="python"))
        return ResearchExecutionPolicy(**base)

    def _validate_adapter_request(
        self,
        adapter: ResearchScoutAdapterPort,
        brief: ResearchBrief,
        policy: ResearchExecutionPolicy,
    ) -> str | None:
        capabilities: ResearchScoutCapabilities = adapter.capabilities
        if brief.curated_inputs:
            if not capabilities.supports_text_inputs:
                return f"Adapter {adapter.adapter_id} does not support curated inputs."
            if any(item.input_type == "url" for item in brief.curated_inputs) and not capabilities.supports_fetch:
                return f"Adapter {adapter.adapter_id} cannot fetch curated URL inputs."
        elif not capabilities.supports_search:
            return (
                f"Adapter {adapter.adapter_id} is a no-search adapter and requires curated inputs."
            )
        if policy.respect_robots and not capabilities.supports_robots:
            return f"Adapter {adapter.adapter_id} does not support robots checks."
        if (policy.allow_domains or policy.deny_domains) and not capabilities.supports_domain_policy:
            return f"Adapter {adapter.adapter_id} does not support domain policy."
        return None

    def _finish_policy_failure(
        self,
        run: ResearchRun,
        facets: list[ResearchFacet],
        error: str,
    ) -> ResearchRunDetail:
        run.status = ResearchRunStatus.FAILED_POLICY
        run.error = error
        run.completed_at = utc_now()
        run.logs.append(error)
        self._sync_run_progress(run, facets, [], time.monotonic())
        self.run_store.update_run(run)
        return self.get_run_detail(run.run_id)  # type: ignore[return-value]

    def _process_search_queries(
        self,
        *,
        adapter_id: str,
        adapter: ResearchScoutAdapterPort,
        brief: ResearchBrief,
        program: ResearchProgram,
        facets: list[ResearchFacet],
        findings: list[ResearchFinding],
        accepted_signatures: dict[str, dict[str, str]],
        run: ResearchRun,
        policy: ResearchExecutionPolicy,
        queries: list[tuple[str, str]],
        started_monotonic: float,
    ) -> str | None:
        for facet_id, query in queries:
            if len(findings) >= brief.max_findings:
                return "Stopped early because max_findings was reached."
            if self._deadline_exceeded(started_monotonic, policy):
                return "Stopped early because total fetch time was exhausted."
            facet = next(item for item in facets if item.facet_id == facet_id)
            facet.queries_attempted += 1
            run.telemetry.queries_attempted += 1
            try:
                hits = self._run_retrying_search(adapter, query, brief.max_results_per_query, run, policy)
            except Exception as exc:
                run.logs.append(f"Query [{facet_id}] failed: {exc}")
                run.warnings.append(f"Query [{facet_id}] failed and was skipped: {exc}")
                self._sync_run_progress(run, facets, findings, started_monotonic)
                continue
            facet.hits_seen += len(hits)
            run.logs.append(f"Query [{facet_id}] returned {len(hits)} candidate hits.")
            for hit in hits:
                if len(findings) >= brief.max_findings:
                    return "Stopped early because max_findings was reached."
                if self._deadline_exceeded(started_monotonic, policy):
                    return "Stopped early because total fetch time was exhausted."
                self._process_hit(
                    adapter_id=adapter_id,
                    adapter=adapter,
                    brief=brief,
                    program=program,
                    facet=facet,
                    query=query,
                    hit=hit,
                    run=run,
                    policy=policy,
                    findings=findings,
                    accepted_signatures=accepted_signatures,
                    started_monotonic=started_monotonic,
                )
        return None

    def _process_curated_inputs(
        self,
        *,
        adapter_id: str,
        adapter: ResearchScoutAdapterPort,
        brief: ResearchBrief,
        program: ResearchProgram,
        facets: list[ResearchFacet],
        findings: list[ResearchFinding],
        accepted_signatures: dict[str, dict[str, str]],
        run: ResearchRun,
        policy: ResearchExecutionPolicy,
        started_monotonic: float,
    ) -> str | None:
        for item in brief.curated_inputs:
            if len(findings) >= brief.max_findings:
                return "Stopped early because max_findings was reached."
            if item.input_type == "text":
                facet = self._best_facet_for_text(facets, brief, item.title or "", item.text or "")
                facet.hits_seen += 1
                finding = self._build_finding(
                    adapter_id=adapter_id,
                    run=run,
                    brief=brief,
                    facet=facet,
                    query="curated_input",
                    hit=ResearchSearchHit(
                        query="curated_input",
                        url=item.url or f"curated://{sha1((item.title or 'text').encode()).hexdigest()[:12]}",
                        title=item.title or "Curated Text Input",
                        snippet=(item.notes or item.text or "")[:240] or None,
                        rank=1,
                    ),
                    page=ResearchFetchedPage(
                        url=item.url or f"curated://{sha1((item.title or 'text').encode()).hexdigest()[:12]}",
                        final_url=item.url or None,
                        title=item.title,
                        publisher=item.publisher,
                        published_at=item.published_at,
                        locator=item.locator,
                        source_type=item.source_type or "curated_text",
                        text=item.text or "",
                    ),
                    fetch_outcome=ResearchFetchOutcome.CURATED_TEXT,
                    fetch_status="curated_text",
                )
                finding.source_type = item.source_type or finding.source_type or "curated_text"
                self._finalize_finding(
                    brief=brief,
                    program=program,
                    facet=facet,
                    finding=finding,
                    findings=findings,
                    accepted_signatures=accepted_signatures,
                    run=run,
                    started_monotonic=started_monotonic,
                )
                continue

            hit = ResearchSearchHit(
                query="curated_input",
                url=item.url or "",
                title=item.title or item.url or "Curated URL Input",
                snippet=(item.notes or item.title or item.url or "")[:240] or None,
                rank=1,
            )
            facet = self._best_facet_for_text(facets, brief, hit.title, hit.snippet or "")
            facet.hits_seen += 1
            self._process_hit(
                adapter_id=adapter_id,
                adapter=adapter,
                brief=brief,
                program=program,
                facet=facet,
                query="curated_input",
                hit=hit,
                run=run,
                policy=policy,
                findings=findings,
                accepted_signatures=accepted_signatures,
                started_monotonic=started_monotonic,
            )
        return None

    def _process_hit(
        self,
        *,
        adapter_id: str,
        adapter: ResearchScoutAdapterPort,
        brief: ResearchBrief,
        program: ResearchProgram,
        facet: ResearchFacet,
        query: str,
        hit: ResearchSearchHit,
        run: ResearchRun,
        policy: ResearchExecutionPolicy,
        findings: list[ResearchFinding],
        accepted_signatures: dict[str, dict[str, str]],
        started_monotonic: float,
    ) -> None:
        canonical_url = self._canonical_url(hit.url)
        host = self._host_for_url(canonical_url)
        if self._is_blocked_by_domain_policy(canonical_url, policy):
            facet.skipped_count += 1
            run.telemetry.blocked_by_policy_count += 1
            run.logs.append(f"Skipped {canonical_url} because it was blocked by domain policy.")
            self._sync_run_progress(run, run.facets, findings, started_monotonic)
            return
        if host and run.telemetry.per_host_fetch_counts.get(host, 0) >= policy.per_host_fetch_cap:
            facet.skipped_count += 1
            run.telemetry.skipped_host_counts[host] = run.telemetry.skipped_host_counts.get(host, 0) + 1
            run.logs.append(f"Skipped {canonical_url} because host cap for {host} was reached.")
            self._sync_run_progress(run, run.facets, findings, started_monotonic)
            return
        if policy.respect_robots:
            allowed = adapter.allows_fetch(canonical_url, user_agent=self.research_user_agent)
            if allowed is False:
                facet.skipped_count += 1
                run.telemetry.blocked_by_robots_count += 1
                run.logs.append(f"Skipped {canonical_url} because robots disallowed fetch.")
                self._sync_run_progress(run, run.facets, findings, started_monotonic)
                return
            if allowed is None and "robots_unavailable" not in run.telemetry.fallback_flags:
                run.telemetry.fallback_flags.append("robots_unavailable")
                run.logs.append("Robots check was unavailable for one or more hosts; continuing in degraded mode.")

        try:
            page = self._run_retrying_fetch(adapter, canonical_url, run, policy)
            if host:
                run.telemetry.per_host_fetch_counts[host] = run.telemetry.per_host_fetch_counts.get(host, 0) + 1
                run.telemetry.successful_fetches += 1
            finding = self._build_finding(
                adapter_id=adapter_id,
                run=run,
                brief=brief,
                facet=facet,
                query=query,
                hit=hit,
                page=page,
                fetch_outcome=ResearchFetchOutcome.FETCHED,
                fetch_status="fetched",
            )
        except Exception as exc:
            facet.rejected_count += 1
            category = self._failure_category(exc)
            self._record_failure(run.telemetry, category)
            finding = ResearchFinding(
                finding_id=self._finding_id(run.run_id, facet.facet_id, query, hit.url, hit.rank),
                run_id=run.run_id,
                facet_id=facet.facet_id,
                query=query,
                url=canonical_url,
                canonical_url=canonical_url,
                title=hit.title or canonical_url,
                publisher=None,
                published_at=None,
                locator=None,
                snippet_text=(hit.snippet or hit.title or canonical_url).strip(),
                page_excerpt=None,
                source_type=None,
                score=0.0,
                relevance_score=0.0,
                quality_score=0.0,
                novelty_score=0.0,
                decision=ResearchFindingDecision.REJECTED,
                rejection_reason=f"Fetch failed: {exc}",
                provenance=self._build_failed_fetch_provenance(
                    adapter_id=adapter_id,
                    facet=facet,
                    query=query,
                    hit=hit,
                    canonical_url=canonical_url,
                    category=category,
                ),
            )
            findings.append(finding)
            self.finding_store.save_findings([finding])
            run.logs.append(f"Fetch failed for [{facet.facet_id}] {canonical_url}: {exc}")
            self._sync_run_progress(run, run.facets, findings, started_monotonic)
            return

        self._finalize_finding(
            brief=brief,
            program=program,
            facet=facet,
            finding=finding,
            findings=findings,
            accepted_signatures=accepted_signatures,
            run=run,
            started_monotonic=started_monotonic,
        )

    def _finalize_finding(
        self,
        *,
        brief: ResearchBrief,
        program: ResearchProgram,
        facet: ResearchFacet,
        finding: ResearchFinding,
        findings: list[ResearchFinding],
        accepted_signatures: dict[str, dict[str, str]],
        run: ResearchRun,
        started_monotonic: float,
    ) -> None:
        signature = self._dedupe_signature(finding.canonical_url or finding.url, finding.title)
        provenance = finding.provenance or ResearchFindingProvenance(
            facet_id=facet.facet_id,
            facet_label=facet.label,
            originating_query=finding.query,
        )
        provenance.dedupe_signature = signature
        provenance.scoring.quality_threshold = program.quality_threshold
        provenance.scoring.threshold_passed = finding.score >= program.quality_threshold
        if self._is_excluded_source(finding.source_type, brief, program):
            finding.decision = ResearchFindingDecision.REJECTED
            finding.rejection_reason = "Source class is excluded by brief or program."
            provenance.rejection_reason = ResearchFindingReason.REJECTED_EXCLUDED_SOURCE
            provenance.policy_flags.append("excluded_source")
        else:
            duplicate_reason, duplicate_rule = self._duplicate_reason(
                signature,
                finding.canonical_url or finding.url,
                finding.title,
                accepted_signatures,
                program.dedupe_similarity_threshold,
            )
            if duplicate_reason:
                run.telemetry.dedupe_count += 1
                finding.decision = ResearchFindingDecision.REJECTED
                finding.rejection_reason = duplicate_reason
                provenance.rejection_reason = ResearchFindingReason.REJECTED_DUPLICATE
                provenance.duplicate_rule = duplicate_rule
            elif facet.accepted_count >= min(facet.target_count, brief.max_per_facet):
                finding.decision = ResearchFindingDecision.REJECTED
                finding.rejection_reason = "Facet coverage target already met."
                provenance.rejection_reason = ResearchFindingReason.REJECTED_FACET_TARGET_MET
            else:
                self._apply_semantic_advisory(
                    finding=finding,
                    findings=findings,
                    run=run,
                    quality_threshold=program.quality_threshold,
                )
            if duplicate_reason or facet.accepted_count >= min(facet.target_count, brief.max_per_facet):
                pass
            elif finding.score < program.quality_threshold:
                finding.decision = ResearchFindingDecision.REJECTED
                finding.rejection_reason = "Finding score fell below the quality threshold."
                provenance.rejection_reason = ResearchFindingReason.REJECTED_QUALITY_THRESHOLD
            else:
                finding.decision = ResearchFindingDecision.ACCEPTED
                accepted_signatures[signature] = {
                    "title": finding.title,
                    "normalized_title": self._normalize_title(finding.title),
                    "host": self._host_for_url(finding.canonical_url or finding.url),
                    "canonical_url": self._canonical_url(finding.canonical_url or finding.url),
                    "path": self._path_for_url(finding.canonical_url or finding.url),
                }
                facet.accepted_count += 1
                provenance.acceptance_reason = ResearchFindingReason.ACCEPTED_QUALITY_THRESHOLD
                self._index_research_findings(run, [finding])

        if finding.decision == ResearchFindingDecision.REJECTED:
            facet.rejected_count += 1
        finding.provenance = provenance
        findings.append(finding)
        self.finding_store.save_findings([finding])
        self._sync_run_progress(run, run.facets, findings, started_monotonic)

    def _run_retrying_search(
        self,
        adapter: ResearchScoutAdapterPort,
        query: str,
        limit: int,
        run: ResearchRun,
        policy: ResearchExecutionPolicy,
    ) -> list[ResearchSearchHit]:
        result: list[ResearchSearchHit] = []

        def _before_sleep(retry_state) -> None:
            run.telemetry.retries += 1
            run.logs.append(f"Retrying search after error: {retry_state.outcome.exception()}")

        for attempt in Retrying(
            stop=stop_after_attempt(policy.retry_attempts),
            wait=wait_exponential_jitter(
                initial=max(policy.retry_backoff_base_ms / 1000.0, 0.01),
                max=max(policy.retry_backoff_max_ms / 1000.0, 0.05),
            ),
            retry=retry_if_exception(self._is_retryable_exception),
            before_sleep=_before_sleep,
            reraise=True,
        ):
            with attempt:
                fetch_started = time.monotonic()
                result = adapter.search(query, limit=limit)
                run.telemetry.elapsed_fetch_time_ms += int((time.monotonic() - fetch_started) * 1000)
        return result

    def _run_retrying_fetch(
        self,
        adapter: ResearchScoutAdapterPort,
        url: str,
        run: ResearchRun,
        policy: ResearchExecutionPolicy,
    ) -> ResearchFetchedPage:
        page: ResearchFetchedPage | None = None

        def _before_sleep(retry_state) -> None:
            run.telemetry.retries += 1
            run.logs.append(f"Retrying fetch for {url}: {retry_state.outcome.exception()}")

        for attempt in Retrying(
            stop=stop_after_attempt(policy.retry_attempts),
            wait=wait_exponential_jitter(
                initial=max(policy.retry_backoff_base_ms / 1000.0, 0.01),
                max=max(policy.retry_backoff_max_ms / 1000.0, 0.05),
            ),
            retry=retry_if_exception(self._is_retryable_exception),
            before_sleep=_before_sleep,
            reraise=True,
        ):
            with attempt:
                run.telemetry.fetch_attempts += 1
                fetch_started = time.monotonic()
                page = adapter.fetch_page(url)
                run.telemetry.elapsed_fetch_time_ms += int((time.monotonic() - fetch_started) * 1000)
        if page is None:
            raise RuntimeError(f"Fetch produced no page for {url}")
        return page

    def _expand_facets(self, brief: ResearchBrief, program: ResearchProgram) -> list[ResearchFacet]:
        requested = brief.desired_facets or program.default_facets or list(_DEFAULT_FACETS)
        facets: list[ResearchFacet] = []
        for facet_id in requested:
            label, query_hint = _DEFAULT_FACETS.get(
                facet_id,
                (" ".join(part.capitalize() for part in facet_id.split("_")), facet_id.replace("_", " ")),
            )
            facets.append(
                ResearchFacet(
                    facet_id=facet_id,
                    label=label,
                    query_hint=query_hint,
                    target_count=brief.coverage_targets.get(facet_id, 1),
                )
            )
        return facets

    def _build_queries(
        self,
        brief: ResearchBrief,
        facets: list[ResearchFacet],
        program: ResearchProgram,
    ) -> list[tuple[str, str]]:
        queries: list[tuple[str, str]] = []
        preferred_hint = " ".join(program.preferred_source_classes[:2])
        time_hint = self._time_hint(brief)
        for facet in facets:
            parts = [brief.topic, facet.query_hint, time_hint, brief.locale, brief.audience]
            if brief.domain_hints:
                parts.extend(brief.domain_hints[:2])
            if preferred_hint:
                parts.append(preferred_hint)
            query = " ".join(part.strip() for part in parts if part and part.strip())
            queries.append((facet.facet_id, query))
        return queries[: brief.max_queries]

    def _build_finding(
        self,
        adapter_id: str,
        run: ResearchRun,
        brief: ResearchBrief,
        facet: ResearchFacet,
        query: str,
        hit: ResearchSearchHit,
        page: ResearchFetchedPage,
        *,
        fetch_outcome: ResearchFetchOutcome,
        fetch_status: str,
    ) -> ResearchFinding:
        title = page.title or hit.title or hit.url
        canonical_url = self._canonical_url(page.final_url or hit.url)
        canonical_host = self._host_for_url(canonical_url)
        normalized_title = self._normalize_title(title)
        source_type = page.source_type or self._classify_source(canonical_url)
        excerpt = self._best_excerpt(
            page.text,
            self._scoring_tokens(brief.topic, facet.label, brief.locale, self._time_hint(brief)),
        )
        snippet = (hit.snippet or excerpt or title).strip()
        publisher = page.publisher or self._host_for_url(canonical_url)
        relevance = self._relevance_score(
            " ".join(filter(None, [title, hit.snippet or "", excerpt])),
            self._scoring_tokens(brief.topic, facet.label, brief.locale, self._time_hint(brief)),
        )
        structural_score = self._structural_score(title, snippet, page.published_at)
        source_class_score, boost, penalty = self._source_class_score(source_type)
        era_score = self._era_score(page.published_at, brief)
        coverage_score = self._coverage_score(facet, brief)
        quality = max(
            0.0,
            min(structural_score + source_class_score + era_score + coverage_score, 1.0),
        )
        novelty = 1.0
        score = round((0.50 * relevance) + (0.50 * quality), 4)
        return ResearchFinding(
            finding_id=self._finding_id(run.run_id, facet.facet_id, query, hit.url, hit.rank),
            run_id=run.run_id,
            facet_id=facet.facet_id,
            query=query,
            url=page.final_url or hit.url,
            canonical_url=canonical_url,
            title=title,
            publisher=publisher,
            published_at=page.published_at,
            access_date=utc_now(),
            locator=page.locator,
            snippet_text=snippet,
            page_excerpt=excerpt or None,
            source_type=source_type,
            score=score,
            relevance_score=round(relevance, 4),
            quality_score=round(quality, 4),
            novelty_score=novelty,
            decision=ResearchFindingDecision.REJECTED,
            provenance=ResearchFindingProvenance(
                adapter_id=adapter_id,
                facet_id=facet.facet_id,
                facet_label=facet.label,
                originating_query=query,
                search_rank=hit.rank,
                hit_url=hit.url,
                canonical_url=canonical_url,
                fetch_outcome=fetch_outcome,
                fetch_final_url=page.final_url or hit.url,
                fetch_status=fetch_status,
                scoring=ResearchFindingScoring(
                    overall_score=score,
                    relevance_score=round(relevance, 4),
                    quality_score=round(quality, 4),
                    novelty_score=novelty,
                    structural_score=round(structural_score, 4),
                    source_class_score=round(source_class_score, 4),
                    era_score=round(era_score, 4),
                    coverage_score=round(coverage_score, 4),
                    source_type=source_type,
                    source_class_boost_applied=boost,
                    source_class_penalty_applied=penalty,
                    near_era_bias_applied=era_score > 0.0,
                    normalized_title=normalized_title,
                    canonical_host=canonical_host or None,
                    semantic_score=0.0,
                    semantic_novelty_score=novelty,
                    semantic_rerank_delta=0.0,
                    semantic_backend="qdrant",
                    semantic_fallback_used=False,
                    semantic_fallback_reason=None,
                    semantic_duplicate_similarity=None,
                    semantic_duplicate_candidate_id=None,
                ),
            ),
        )

    def _build_failed_fetch_provenance(
        self,
        *,
        adapter_id: str,
        facet: ResearchFacet,
        query: str,
        hit: ResearchSearchHit,
        canonical_url: str,
        category: str,
    ) -> ResearchFindingProvenance:
        return ResearchFindingProvenance(
            adapter_id=adapter_id,
            facet_id=facet.facet_id,
            facet_label=facet.label,
            originating_query=query,
            search_rank=hit.rank,
            hit_url=hit.url,
            canonical_url=canonical_url,
            fetch_outcome=ResearchFetchOutcome.FAILED,
            fetch_final_url=canonical_url,
            fetch_status="failed",
            fetch_error_category=category,
            rejection_reason=ResearchFindingReason.REJECTED_FETCH_FAILURE,
            scoring=ResearchFindingScoring(
                overall_score=0.0,
                relevance_score=0.0,
                quality_score=0.0,
                novelty_score=1.0,
                structural_score=0.0,
                source_class_score=0.0,
                era_score=0.0,
                coverage_score=0.0,
                quality_threshold=0.0,
                threshold_passed=False,
                source_type=None,
                source_class_boost_applied=0.0,
                source_class_penalty_applied=0.0,
                near_era_bias_applied=False,
                normalized_title=self._normalize_title(hit.title or canonical_url),
                canonical_host=self._host_for_url(canonical_url) or None,
                semantic_score=0.0,
                semantic_novelty_score=1.0,
                semantic_rerank_delta=0.0,
                semantic_backend="qdrant",
                semantic_fallback_used=False,
                semantic_fallback_reason=None,
                semantic_duplicate_similarity=None,
                semantic_duplicate_candidate_id=None,
            ),
        )

    def _apply_semantic_advisory(
        self,
        *,
        finding: ResearchFinding,
        findings: list[ResearchFinding],
        run: ResearchRun,
        quality_threshold: float,
    ) -> None:
        accepted_ids = [
            item.finding_id
            for item in findings
            if item.decision == ResearchFindingDecision.ACCEPTED and item.facet_id == finding.facet_id
        ]
        scoring = finding.provenance.scoring if finding.provenance else ResearchFindingScoring()
        provenance = finding.provenance or ResearchFindingProvenance(
            facet_id=finding.facet_id,
            originating_query=finding.query,
        )
        if not accepted_ids:
            finding.novelty_score = 1.0
            scoring.novelty_score = 1.0
            scoring.semantic_novelty_score = 1.0
            scoring.semantic_rerank_delta = 0.0
            finding.provenance = provenance
            return

        result = self.research_semantic.search_similar_findings(
            finding,
            accepted_ids,
            run_id=run.run_id,
            limit=3,
        )
        run.telemetry.semantic.backend = result.retrieval_backend
        run.telemetry.semantic.comparisons_performed += len(accepted_ids)
        scoring.semantic_backend = result.retrieval_backend
        if result.fallback_used:
            run.telemetry.semantic.fallback_used = True
            run.telemetry.semantic.fallback_reason = result.fallback_reason
            scoring.semantic_fallback_used = True
            scoring.semantic_fallback_reason = result.fallback_reason
            provenance.semantic_decision_notes = (
                f"Semantic fallback: {result.fallback_reason}" if result.fallback_reason else "Semantic fallback used."
            )
            finding.provenance = provenance
            return

        top_match = result.matches[0] if result.matches else None
        top_similarity = round(top_match.similarity, 4) if top_match else 0.0
        novelty = max(self.semantic_novelty_floor, round(1.0 - top_similarity, 4))
        rerank_delta = 0.0
        if result.matches:
            rerank_delta = round(self.semantic_rerank_weight * (novelty - 0.5), 4)
            base_score = finding.score
            adjusted_score = max(0.0, min(finding.score + rerank_delta, 1.0))
            if base_score >= quality_threshold and adjusted_score < quality_threshold and rerank_delta < 0:
                adjusted_score = quality_threshold
            finding.score = round(adjusted_score, 4)
            if finding.provenance:
                finding.provenance.scoring.overall_score = finding.score

        scoring.semantic_score = top_similarity
        scoring.novelty_score = novelty
        scoring.semantic_novelty_score = novelty
        scoring.semantic_rerank_delta = rerank_delta
        scoring.semantic_duplicate_similarity = top_similarity if top_match else None
        scoring.semantic_duplicate_candidate_id = top_match.finding_id if top_match else None
        scoring.semantic_fallback_used = False
        scoring.semantic_fallback_reason = None
        finding.novelty_score = novelty
        provenance.semantic_matches = result.matches[:3]
        if top_match and top_similarity >= self.semantic_duplicate_threshold:
            provenance.semantic_duplicate_hint = True
            provenance.semantic_decision_notes = (
                f"Semantically near {top_match.finding_id} at similarity {top_similarity}."
            )
            run.telemetry.semantic.duplicate_hints_emitted += 1
        elif top_match:
            provenance.semantic_decision_notes = (
                f"Top semantic match {top_match.finding_id} at similarity {top_similarity}."
            )
        finding.provenance = provenance

    def _index_research_findings(
        self,
        run: ResearchRun,
        findings: list[ResearchFinding],
    ) -> None:
        if not findings:
            return
        try:
            indexed_count = self.research_semantic.upsert_findings(findings, run_id=run.run_id)
            run.telemetry.semantic.vectors_upserted += indexed_count
        except Exception as exc:
            run.telemetry.semantic.fallback_used = True
            run.telemetry.semantic.fallback_reason = str(exc)

    def _build_facet_coverage(
        self,
        facets: list[ResearchFacet],
        findings: list[ResearchFinding],
    ) -> list[ResearchFacetCoverage]:
        coverage: list[ResearchFacetCoverage] = []
        for facet in facets:
            facet_findings = [item for item in findings if item.facet_id == facet.facet_id]
            rejected_for_facet = [
                item for item in facet_findings
                if item.decision == ResearchFindingDecision.REJECTED
            ]
            accepted_for_facet = [
                item for item in facet_findings
                if item.decision == ResearchFindingDecision.ACCEPTED
            ]
            duplicate_rejections = sum(
                bool(
                    item.provenance
                    and item.provenance.rejection_reason == ResearchFindingReason.REJECTED_DUPLICATE
                )
                for item in rejected_for_facet
            )
            threshold_rejections = sum(
                bool(
                    item.provenance
                    and item.provenance.rejection_reason == ResearchFindingReason.REJECTED_QUALITY_THRESHOLD
                )
                for item in rejected_for_facet
            )
            excluded_source_rejections = sum(
                bool(
                    item.provenance
                    and item.provenance.rejection_reason == ResearchFindingReason.REJECTED_EXCLUDED_SOURCE
                )
                for item in rejected_for_facet
            )
            fetch_failures = sum(
                bool(
                    item.provenance
                    and item.provenance.rejection_reason == ResearchFindingReason.REJECTED_FETCH_FAILURE
                )
                for item in rejected_for_facet
            )
            accepted_sources_by_type: dict[str, int] = {}
            for item in accepted_for_facet:
                source_type = item.source_type or "unknown"
                accepted_sources_by_type[source_type] = accepted_sources_by_type.get(source_type, 0) + 1
            if facet.accepted_count > facet.target_count:
                status = ResearchCoverageStatus.OVERSUBSCRIBED
                gap_reason = None
            elif facet.accepted_count >= facet.target_count and facet.target_count > 0:
                status = ResearchCoverageStatus.MET
                gap_reason = None
            elif facet.accepted_count > 0:
                status = ResearchCoverageStatus.PARTIAL
                gap_reason = "target_not_met"
            else:
                status = ResearchCoverageStatus.EMPTY
                gap_reason = self._coverage_gap_reason(facet, rejected_for_facet)
            coverage.append(
                ResearchFacetCoverage(
                    facet_id=facet.facet_id,
                    label=facet.label,
                    target_count=facet.target_count,
                    queries_attempted=facet.queries_attempted,
                    hits_seen=facet.hits_seen,
                    accepted_count=facet.accepted_count,
                    rejected_count=facet.rejected_count,
                    skipped_count=facet.skipped_count,
                    duplicate_rejections=duplicate_rejections,
                    threshold_rejections=threshold_rejections,
                    excluded_source_rejections=excluded_source_rejections,
                    fetch_failures=fetch_failures,
                    accepted_sources_by_type=accepted_sources_by_type,
                    diagnostic_summary=self._diagnostic_summary(
                        facet,
                        gap_reason,
                        duplicate_rejections=duplicate_rejections,
                        threshold_rejections=threshold_rejections,
                        excluded_source_rejections=excluded_source_rejections,
                        fetch_failures=fetch_failures,
                    ),
                    coverage_status=status,
                    coverage_gap_reason=gap_reason,
                )
            )
        return coverage

    def _coverage_gap_reason(
        self,
        facet: ResearchFacet,
        rejected_for_facet: list[ResearchFinding],
    ) -> str | None:
        if facet.hits_seen == 0:
            return "no_hits"
        if facet.skipped_count >= facet.hits_seen and facet.skipped_count > 0:
            return "policy_blocked"
        if rejected_for_facet:
            reasons = {
                item.provenance.rejection_reason
                for item in rejected_for_facet
                if item.provenance and item.provenance.rejection_reason is not None
            }
            if reasons == {ResearchFindingReason.REJECTED_FETCH_FAILURE}:
                return "fetch_failures_only"
            if reasons == {ResearchFindingReason.REJECTED_DUPLICATE}:
                return "duplicates_only"
            if reasons == {ResearchFindingReason.REJECTED_QUALITY_THRESHOLD}:
                return "threshold_rejections"
            return "mixed_rejections"
        if facet.accepted_count < facet.target_count:
            return "target_not_met"
        return None

    def _diagnostic_summary(
        self,
        facet: ResearchFacet,
        gap_reason: str | None,
        *,
        duplicate_rejections: int,
        threshold_rejections: int,
        excluded_source_rejections: int,
        fetch_failures: int,
    ) -> str:
        if facet.accepted_count >= facet.target_count and facet.target_count > 0:
            return "target_met"
        if gap_reason:
            return gap_reason
        if duplicate_rejections or threshold_rejections or excluded_source_rejections or fetch_failures:
            return "mixed_rejections"
        return "target_not_met"

    def _finding_id(
        self,
        run_id: str,
        facet_id: str,
        query: str,
        hit_url: str,
        rank: int | None,
    ) -> str:
        raw = f"{run_id}:{facet_id}:{query}:{hit_url}:{rank or 0}"
        return f"finding-{sha1(raw.encode()).hexdigest()[:12]}"

    def _dedupe_signature(self, url: str, title: str) -> str:
        canonical_url = self._canonical_url(url)
        normalized_title = self._normalize_title(title)
        return sha1(f"{canonical_url}|{normalized_title}".encode()).hexdigest()

    def _duplicate_reason(
        self,
        signature: str,
        url: str,
        title: str,
        accepted_signatures: dict[str, dict[str, str]],
        similarity_threshold: float,
    ) -> tuple[str | None, str | None]:
        if signature in accepted_signatures:
            return "Duplicate of an already accepted finding.", "exact_signature"
        canonical_url = self._canonical_url(url)
        normalized_title = self._normalize_title(title)
        host = self._host_for_url(url)
        path = self._path_for_url(url)
        for existing in accepted_signatures.values():
            existing_title_raw = existing.get("title", "")
            existing_title = existing.get("normalized_title", "")
            existing_host = existing.get("host", "")
            existing_url = existing.get("canonical_url", "")
            existing_path = existing.get("path", "")
            if existing_url == canonical_url and existing_title_raw and existing_title_raw != title:
                return "Duplicate URL with a title variant of an already accepted finding.", "same_canonical_url_different_title"
            if existing_host and host and existing_host == host:
                if SequenceMatcher(None, existing_title, normalized_title).ratio() >= similarity_threshold:
                    return "Near-duplicate of an already accepted finding.", "same_host_similar_title"
            if (
                existing_title
                and existing_title == normalized_title
                and existing_host
                and host
                and existing_host != host
                and self._is_specific_title(normalized_title)
                and self._has_weak_identity_path(path)
                and self._has_weak_identity_path(existing_path)
            ):
                return "Cross-host duplicate of a highly specific titled finding.", "same_title_cross_host"
        return None, None

    def _is_excluded_source(
        self,
        source_type: str | None,
        brief: ResearchBrief,
        program: ResearchProgram,
    ) -> bool:
        if not source_type:
            return False
        return source_type in set(brief.excluded_source_types) | set(program.excluded_source_classes)

    def _coverage_warnings(
        self,
        facets: list[ResearchFacet],
        findings: list[ResearchFinding],
    ) -> list[str]:
        warnings: list[str] = []
        facet_coverage = self._build_facet_coverage(facets, findings)
        if not findings:
            warnings.append("No findings were captured for this run.")
            warnings.append("No accepted findings met the acceptance threshold.")
            return warnings
        accepted = [item for item in findings if item.decision == ResearchFindingDecision.ACCEPTED]
        if not accepted:
            warnings.append("No findings met the acceptance threshold.")
        for facet in facet_coverage:
            if facet.accepted_count == 0:
                if facet.coverage_gap_reason:
                    warnings.append(
                        f"Coverage gap: facet {facet.label} ended with {facet.coverage_gap_reason}."
                    )
                else:
                    warnings.append(f"Coverage gap: no accepted findings for facet {facet.label}.")
        return warnings

    def _build_staged_text(self, run: ResearchRun, finding: ResearchFinding) -> str:
        _ = run
        primary = (finding.page_excerpt or "").strip()
        secondary = finding.snippet_text.strip()
        if primary and secondary and secondary not in primary:
            return f"{primary}\n\n{secondary}".strip()
        return primary or secondary

    def _best_excerpt(self, text: str, desired_tokens: set[str]) -> str:
        if not text:
            return ""
        paragraphs = [segment.strip() for segment in re.split(r"\n{2,}", text) if segment.strip()]
        if not paragraphs:
            paragraphs = [text.strip()]
        ranked = sorted(
            paragraphs,
            key=lambda paragraph: (
                self._relevance_score(paragraph, desired_tokens),
                min(len(paragraph), 700),
            ),
            reverse=True,
        )
        return ranked[0][:700] if ranked else ""

    def _best_facet_for_text(
        self,
        facets: list[ResearchFacet],
        brief: ResearchBrief,
        title: str,
        text: str,
    ) -> ResearchFacet:
        desired_tokens_by_facet = {
            facet.facet_id: self._scoring_tokens(brief.topic, facet.label, brief.locale, self._time_hint(brief))
            for facet in facets
        }
        return max(
            facets,
            key=lambda facet: self._relevance_score(
                " ".join(filter(None, [title, text])),
                desired_tokens_by_facet[facet.facet_id],
            ),
        )

    def _relevance_score(self, text: str, desired_tokens: set[str]) -> float:
        if not text or not desired_tokens:
            return 0.0
        haystack = self._normalize_text(text)
        matches = sum(1 for token in desired_tokens if token in haystack)
        return min(matches / max(len(desired_tokens), 1), 1.0)

    def _structural_score(
        self,
        title: str,
        snippet: str,
        published_at: str | None,
    ) -> float:
        score = 0.2
        if title:
            score += 0.1
        if len(snippet) >= 120:
            score += 0.15
        if published_at:
            score += 0.1
        return max(0.0, min(score, 1.0))

    def _source_class_score(self, source_type: str | None) -> tuple[float, float, float]:
        boost = _PREFERRED_CLASS_BOOSTS.get(source_type or "", 0.0)
        penalty = _PENALIZED_CLASSES.get(source_type or "", 0.0)
        return boost - penalty, boost, penalty

    def _era_score(self, published_at: str | None, brief: ResearchBrief) -> float:
        published_year = self._year_from_date(published_at)
        if not published_year:
            return 0.0
        target_year = self._target_year(brief)
        if target_year is None:
            return 0.0
        delta = abs(int(published_year) - target_year)
        if delta == 0:
            return 0.12
        if delta <= 1:
            return 0.08
        if delta <= 3:
            return 0.04
        return 0.0

    def _coverage_score(self, facet: ResearchFacet, brief: ResearchBrief) -> float:
        target = max(1, min(facet.target_count, brief.max_per_facet))
        if facet.accepted_count == 0:
            return 0.08
        if facet.accepted_count < target:
            return 0.04
        return 0.0

    def _classify_source(self, url: str) -> str:
        host = self._host_for_url(url)
        lower = host.lower()
        if lower.endswith(".gov"):
            return "government"
        if lower.endswith(".edu"):
            return "educational"
        if any(part in lower for part in ("archive", "library", "museum")):
            return "archive"
        if any(part in lower for part in ("news", "newspaper", "times", "post", "guardian", "bbc")):
            return "news"
        if any(part in lower for part in ("magazine", "rollingstone", "billboard", "vice")):
            return "magazine"
        if any(part in lower for part in ("forum", "board")):
            return "forum"
        if any(part in lower for part in ("twitter", "x.com", "facebook", "instagram", "tiktok")):
            return "social"
        if any(part in lower for part in ("shop", "store", "ebay", "amazon")):
            return "shopping"
        if any(part in lower for part in ("blog", "substack")):
            return "blog"
        return "web"

    def _canonical_url(self, url: str) -> str:
        parsed = urlparse(url)
        scheme = (parsed.scheme or "https").lower()
        netloc = parsed.netloc.lower().removeprefix("www.")
        if ":" in netloc:
            host, _, port = netloc.partition(":")
            if (scheme == "http" and port == "80") or (scheme == "https" and port == "443"):
                netloc = host
        path = posixpath.normpath(parsed.path or "/")
        if path == ".":
            path = "/"
        path = self._normalize_canonical_path(path)
        query_items = [
            (key, value)
            for key, value in parse_qsl(parsed.query, keep_blank_values=True)
            if value
            and key.lower() not in _TRACKING_PARAMS
            and not key.lower().startswith("utm_")
            and key.lower() not in _GENERIC_QUERY_PARAMS
        ]
        query_items.sort()
        query = urlencode(query_items, doseq=True)
        return urlunparse((scheme, netloc, path, "", query, ""))

    def _host_for_url(self, url: str) -> str:
        parsed = urlparse(url)
        return parsed.netloc.lower().removeprefix("www.")

    def _normalize_text(self, value: str) -> str:
        return " ".join(_TOKEN_RE.findall(value.lower()))

    def _normalize_title(self, value: str) -> str:
        normalized = value.strip().lower()
        normalized = re.sub(r"\[[^\]]+\]|\([^)]+\)$", " ", normalized)
        parts = re.split(r"\s*(?:\||-|—|:|::|»|/)\s*", normalized)
        if len(parts) > 1:
            normalized = self._pick_title_core(parts)
        normalized = re.sub(r"[^a-z0-9\s]", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        for suffix in _TITLE_SUFFIX_NOISE:
            if normalized.endswith(f" {suffix}"):
                normalized = normalized[: -len(suffix) - 1].strip()
        return normalized

    def _pick_title_core(self, parts: list[str]) -> str:
        cleaned = [part.strip() for part in parts if part and part.strip()]
        if not cleaned:
            return ""
        ranked = sorted(
            cleaned,
            key=lambda part: (self._is_specific_title(part), len(part), part.count(" ")),
            reverse=True,
        )
        return ranked[0]

    def _normalize_canonical_path(self, path: str) -> str:
        normalized = path or "/"
        if normalized != "/" and normalized.endswith("/"):
            normalized = normalized.rstrip("/")
        lowered = normalized.lower()
        if lowered in {f"/{item}" for item in _DEFAULT_PAGE_NAMES}:
            return "/"
        for page_name in _DEFAULT_PAGE_NAMES:
            if lowered.endswith(f"/{page_name}"):
                normalized = normalized[: -(len(page_name) + 1)] or "/"
                break
        return normalized or "/"

    def _path_for_url(self, url: str) -> str:
        return urlparse(self._canonical_url(url)).path or "/"

    def _has_weak_identity_path(self, path: str) -> bool:
        normalized = self._normalize_canonical_path(path or "/")
        if normalized == "/":
            return True
        tokens = [token for token in normalized.split("/") if token]
        return len(tokens) <= 1

    def _is_specific_title(self, normalized_title: str) -> bool:
        tokens = normalized_title.split()
        return len(tokens) >= 4 and len(normalized_title) >= 24

    def _target_year(self, brief: ResearchBrief) -> int | None:
        if brief.focal_year and brief.focal_year.isdigit():
            return int(brief.focal_year)
        years = [
            self._year_from_date(brief.time_start),
            self._year_from_date(brief.time_end),
        ]
        numeric = [int(year) for year in years if year and year.isdigit()]
        if not numeric:
            return None
        return round(sum(numeric) / len(numeric))

    def _scoring_tokens(self, *values: str | None) -> set[str]:
        tokens: set[str] = set()
        for value in values:
            if not value:
                continue
            for token in _TOKEN_RE.findall(value.lower()):
                if token in _STOPWORDS:
                    continue
                tokens.add(token)
        return tokens

    def _time_hint(self, brief: ResearchBrief) -> str:
        if brief.focal_year:
            return brief.focal_year
        if brief.time_start and brief.time_end:
            return f"{brief.time_start} {brief.time_end}"
        return brief.time_start or brief.time_end or ""

    def _year_from_date(self, value: str | None) -> str | None:
        if not value:
            return None
        match = re.search(r"\b(\d{4})\b", value)
        return match.group(1) if match else None

    def _year_from_brief(self, brief: ResearchBrief) -> str | None:
        return brief.focal_year or self._year_from_date(brief.time_start) or self._year_from_date(brief.time_end)

    def _deadline_exceeded(
        self,
        started_monotonic: float,
        policy: ResearchExecutionPolicy,
    ) -> bool:
        return (time.monotonic() - started_monotonic) >= policy.total_fetch_time_seconds

    def _is_blocked_by_domain_policy(self, url: str, policy: ResearchExecutionPolicy) -> bool:
        host = self._host_for_url(url)
        if not host:
            return True
        if any(host == domain or host.endswith(f".{domain}") for domain in policy.deny_domains):
            return True
        if policy.allow_domains:
            return not any(host == domain or host.endswith(f".{domain}") for domain in policy.allow_domains)
        return False

    def _record_failure(self, telemetry: ResearchRunTelemetry, category: str) -> None:
        telemetry.fetch_failures_by_category[category] = telemetry.fetch_failures_by_category.get(category, 0) + 1

    def _failure_category(self, exc: Exception) -> str:
        if isinstance(exc, httpx.TimeoutException):
            return "timeout"
        if isinstance(exc, httpx.HTTPStatusError):
            return f"http_{exc.response.status_code}"
        if isinstance(exc, httpx.TransportError):
            return "transport_error"
        if isinstance(exc, NotImplementedError):
            return "unsupported"
        return "runtime_error"

    def _is_retryable_exception(self, exc: BaseException) -> bool:
        if isinstance(exc, httpx.TimeoutException):
            return True
        if isinstance(exc, httpx.NetworkError | httpx.RemoteProtocolError):
            return True
        if isinstance(exc, httpx.HTTPStatusError):
            return exc.response.status_code in _RETRYABLE_STATUSES
        return False

    def _sync_run_progress(
        self,
        run: ResearchRun,
        facets: list[ResearchFacet],
        findings: list[ResearchFinding],
        started_monotonic: float,
    ) -> None:
        run.finding_count = len(findings)
        run.accepted_count = sum(
            item.decision == ResearchFindingDecision.ACCEPTED for item in findings
        )
        run.rejected_count = sum(
            item.decision == ResearchFindingDecision.REJECTED for item in findings
        )
        run.warnings = self._coverage_warnings(facets, findings)
        run.telemetry.elapsed_run_time_ms = int((time.monotonic() - started_monotonic) * 1000)
        self.run_store.update_run(run)
