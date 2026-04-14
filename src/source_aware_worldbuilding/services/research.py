from __future__ import annotations

import posixpath
import re
import time
from collections.abc import Callable
from difflib import SequenceMatcher
from hashlib import sha1
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from uuid import uuid4

import httpx
from tenacity import Retrying, retry_if_exception, stop_after_attempt, wait_exponential_jitter

from source_aware_worldbuilding.adapters.web_research_scout import ResearchScoutRegistry
from source_aware_worldbuilding.domain.enums import (
    ResearchCoverageStatus,
    ResearchFetchOutcome,
    ResearchFindingDecision,
    ResearchFindingReason,
    ResearchRunStatus,
)
from source_aware_worldbuilding.domain.models import (
    ResearchBrief,
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
    ResearchQueryPlan,
    ResearchRun,
    ResearchRunDetail,
    ResearchRunRequest,
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
    "practices": (
        "Practices",
        "habits routines workflows customs methods promotion distribution "
        "residencies flyers playlists",
    ),
    "events": ("Events", "events milestones incidents timelines"),
    "objects_technology": ("Objects / Technology", "tools gear formats artifacts technology"),
    "language_slang": ("Language / Slang", "phrases terminology slang discourse"),
    "economics_commercial": (
        "Economics / Commercial Context",
        "prices business trade money markets commerce",
    ),
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
    "reference": 0.18,
    "video": 0.2,
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
_GUIDE_TITLE_PATTERNS = (
    "history of",
    "guide to",
    "your guide to",
    "origins of",
    "origins and evolution",
    "evolution of",
    "birth of",
    "top 10",
    "top 20",
    "top 100",
    "most influential",
    "documentaries",
    "documentary",
    "best ",
    "essentials",
    "condensed history",
    "explores",
    "archive of",
)
_WEAK_SOURCE_SHAPE_PATTERNS = (
    "abstract",
    "introduction",
    "overview",
    "about this project",
    "about the archive",
    "visitor guide",
    "travel guide",
    "things to do",
    "nightlife guide",
    "documentaries",
    "documentary",
    "playlist",
    "glossary",
)
_RETROSPECTIVE_TITLE_PATTERNS = (
    "timeline",
    "legacy lives",
    "since the first",
    "years since",
    "tribute to",
    "important artists",
    "key moments",
    "shaped the",
    "birth of",
    "origins of",
)
_PROMO_TEXT_PATTERNS = (
    "listen to",
    "shop ",
    "buy ",
    "sign up",
    "watch now",
    "read more",
    "photo by",
    "tickets",
    "podcast",
    "newsletter",
)
_LOW_VALUE_EXCERPT_PATTERNS = (
    "retrieved from",
    "read more",
    "click here",
    "photo by",
)
_VAGUE_STAGE_PATTERNS = (
    "what started as",
    "throughout europe and beyond",
    "worldwide phenomenon",
    "changed the clubbing world",
)
_LOW_VALUE_RESULT_PATTERNS = (
    "glossary",
    "playlist",
    "tracklist",
    "watch?v=",
    "youtube",
    "artistdirect",
    "archive of electronic music",
    "top 20",
    "top 10",
)
_SOURCE_SEEKING_TERMS = (
    "interview review listing flyer playlist radio residency archive promoter venue club mix set"
)
_ANCHOR_TERMS = (
    "2002",
    "2003",
    "2004",
    "vinyl",
    "cdj",
    "mixtape",
    "record pool",
    "residency",
    "warehouse",
    "club",
    "radio",
    "label",
    "promoter",
    "flyer",
    "neighborhood",
    "chicago",
)
_PRACTICE_TERMS = (
    "routine",
    "workflow",
    "habit",
    "practice",
    "residency",
    "flyer",
    "record pool",
    "door policy",
    "promoter",
    "promotion",
    "distribution",
    "playlist",
    "listing",
    "club night",
    "club",
    "venue",
    "radio",
    "mix",
    "set",
)
_CONCRETENESS_TERMS = (
    "vinyl",
    "cdj",
    "turntable",
    "mixtape",
    "record pool",
    "radio show",
    "radio",
    "label",
    "promoter",
    "flyer",
    "residency",
    "club",
    "venue",
    "warehouse",
    "nightclub",
    "dj booth",
    "neighborhood",
    "street",
)


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
        for program in self._built_in_programs():
            programs[program.program_id] = program
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
            key=lambda item: (
                item.decision != ResearchFindingDecision.ACCEPTED,
                -item.score,
                item.title,
            ),
        )
        return ResearchRunDetail(
            run=run,
            findings=findings,
            program=self._resolve_program(run.program_id),
            facet_coverage=self._build_facet_coverage(run.facets, findings),
        )

    def prepare_run(self, request: ResearchRunRequest) -> ResearchRun:
        program = self._resolve_program(request.program_id)
        facets = self._expand_facets(request.brief, program)
        run = ResearchRun(
            run_id=f"research-{uuid4().hex[:12]}",
            status=ResearchRunStatus.PENDING,
            brief=request.brief,
            program_id=program.program_id,
            facets=facets,
            telemetry=ResearchRunTelemetry(),
            logs=[f"Queued research program {program.program_id}."],
        )
        run.telemetry.semantic.backend = "qdrant"
        self.run_store.save_run(run)
        return run

    def run_research(self, request: ResearchRunRequest) -> ResearchRunDetail:
        run = self.prepare_run(request)
        return self.execute_run(run.run_id)

    def execute_run(
        self,
        run_id: str,
        *,
        checkpoint: Callable[[], None] | None = None,
    ) -> ResearchRunDetail:
        run = self.run_store.get_run(run_id)
        if run is None:
            raise ValueError("Research run not found.")
        program = self._resolve_program(run.program_id)
        facets = run.facets
        execution_policy = self._resolve_execution_policy(run.brief, program)
        adapter_id = run.brief.adapter_id or program.default_adapter_id or self.default_adapter_id
        run.status = ResearchRunStatus.RUNNING
        run.error = None
        run.completed_at = None
        run.logs.append(f"Using research program {program.program_id}.")
        run.logs.append(f"Using adapter {adapter_id}.")
        self.run_store.update_run(run)

        adapter = self.scout_registry.get(adapter_id)
        if adapter is None:
            return self._finish_policy_failure(
                run,
                facets,
                f"Research adapter {adapter_id} was not found.",
            )

        validation_error = self._validate_adapter_request(adapter, run.brief, execution_policy)
        if validation_error:
            return self._finish_policy_failure(run, facets, validation_error)

        started_monotonic = time.monotonic()
        accepted_signatures: dict[str, dict[str, str]] = {}
        findings: list[ResearchFinding] = []
        page_cache: dict[str, ResearchFetchedPage] = {}
        failed_fetch_cache: dict[str, tuple[str, str]] = {}
        stop_reason: str | None = None

        try:
            self._checkpoint(checkpoint)
            if run.brief.curated_inputs:
                run.query_count = 0
                run.telemetry.total_queries = 0
                run.logs.append(f"Processing {len(run.brief.curated_inputs)} curated input(s).")
                stop_reason = self._process_curated_inputs(
                    adapter_id=adapter_id,
                    adapter=adapter,
                    brief=run.brief,
                    program=program,
                    facets=facets,
                    findings=findings,
                    accepted_signatures=accepted_signatures,
                    page_cache=page_cache,
                    failed_fetch_cache=failed_fetch_cache,
                    run=run,
                    policy=execution_policy,
                    started_monotonic=started_monotonic,
                    checkpoint=checkpoint,
                )
            else:
                queries = self._build_queries(run.brief, facets, program)
                run.query_count = len(queries)
                run.telemetry.total_queries = len(queries)
                run.logs.append(f"Planned {len(queries)} query/facet combinations.")
                stop_reason = self._process_search_queries(
                    adapter_id=adapter_id,
                    adapter=adapter,
                    brief=run.brief,
                    program=program,
                    facets=facets,
                    findings=findings,
                    accepted_signatures=accepted_signatures,
                    page_cache=page_cache,
                    failed_fetch_cache=failed_fetch_cache,
                    run=run,
                    policy=execution_policy,
                    queries=queries,
                    started_monotonic=started_monotonic,
                    checkpoint=checkpoint,
                )

            self._checkpoint(checkpoint)
            self._sync_run_progress(run, facets, findings, started_monotonic)
            self._index_research_findings(
                run,
                [item for item in findings if item.decision != ResearchFindingDecision.ACCEPTED],
            )
            run.completed_at = utc_now()
            if stop_reason:
                run.logs.append(stop_reason)
                run.status = ResearchRunStatus.COMPLETED_PARTIAL
            else:
                run.status = ResearchRunStatus.COMPLETED
            if run.status == ResearchRunStatus.COMPLETED and (
                run.telemetry.fallback_flags or run.telemetry.semantic.fallback_used
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

    def stage_run(
        self,
        run_id: str,
        *,
        checkpoint: Callable[[], None] | None = None,
    ) -> ResearchRunStageResult:
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
            self._checkpoint(checkpoint)
            source_id = f"research-source-{sha1(finding.finding_id.encode()).hexdigest()[:12]}"
            document_id = f"research-doc-{sha1(finding.finding_id.encode()).hexdigest()[:12]}"
            source_records.append(
                SourceRecord(
                    source_id=source_id,
                    external_source="research_scout",
                    external_id=finding.finding_id,
                    title=finding.title,
                    author=finding.publisher,
                    year=self._year_from_date(finding.published_at)
                    or self._year_from_brief(run.brief),
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
            self._checkpoint(checkpoint)
            self.source_store.save_sources(source_records)
            self.source_document_store.save_source_documents(documents)
            for finding in accepted:
                self._checkpoint(checkpoint)
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

    def extract_run(
        self,
        run_id: str,
        *,
        checkpoint: Callable[[], None] | None = None,
    ) -> ResearchExtractResult:
        stage_result = self.stage_run(run_id, checkpoint=checkpoint)
        staged_findings = self.finding_store.list_findings(run_id=run_id)
        staged_source_ids = [
            item.staged_source_id for item in staged_findings if item.staged_source_id is not None
        ]
        staged_document_ids = [
            item.staged_document_id
            for item in staged_findings
            if item.staged_document_id is not None
        ]
        self._checkpoint(checkpoint)
        normalization = self.normalization_service.normalize_documents(
            document_ids=staged_document_ids,
            source_ids=staged_source_ids,
            checkpoint=checkpoint,
        )
        self._checkpoint(checkpoint)
        extraction = self.ingestion_service.extract_candidates(
            source_ids=staged_source_ids,
            checkpoint=checkpoint,
        )
        self._checkpoint(checkpoint)
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

    def _built_in_programs(self) -> list[ResearchProgram]:
        now = utc_now()
        defaults = self.default_execution_policy.model_copy(deep=True)
        return [
            self._default_program(),
            ResearchProgram(
                program_id="historical-period-grounding",
                name="Historical Fiction / Period Grounding",
                description=(
                    "Broad grounding across people, places, institutions, practices, "
                    "and dated events."
                ),
                markdown=self.default_program_markdown,
                built_in=True,
                default_facets=["people", "places", "institutions", "practices", "events"],
                default_adapter_id=self.default_adapter_id,
                default_execution_policy=defaults,
                preferred_source_classes=["archive", "government", "educational", "news"],
                excluded_source_classes=["social", "shopping"],
                quality_threshold=0.5,
                dedupe_similarity_threshold=0.9,
                created_at=now,
                updated_at=now,
            ),
            ResearchProgram(
                program_id="historical-daily-life",
                name="Historical Fiction / Daily Life And Material Culture",
                description=(
                    "Find routines, tools, domestic details, labor, prices, and "
                    "objects that make scenes concrete."
                ),
                markdown=self.default_program_markdown,
                built_in=True,
                default_facets=["practices", "objects_technology", "economics_commercial"],
                default_adapter_id=self.default_adapter_id,
                default_execution_policy=defaults,
                preferred_source_classes=["archive", "educational", "news"],
                excluded_source_classes=["social", "video"],
                quality_threshold=0.5,
                dedupe_similarity_threshold=0.88,
                created_at=now,
                updated_at=now,
            ),
            ResearchProgram(
                program_id="historical-institutions-power",
                name="Historical Fiction / Institutions And Power",
                description=(
                    "Bias toward government, legal, religious, and institutional "
                    "context for a setting."
                ),
                markdown=self.default_program_markdown,
                built_in=True,
                default_facets=["institutions", "events", "regional_context"],
                default_adapter_id=self.default_adapter_id,
                default_execution_policy=defaults,
                preferred_source_classes=["government", "archive", "educational"],
                excluded_source_classes=["social", "shopping"],
                quality_threshold=0.52,
                dedupe_similarity_threshold=0.9,
                created_at=now,
                updated_at=now,
            ),
            ResearchProgram(
                program_id="historical-slang-discourse",
                name="Historical Fiction / Speech, Slang, And Discourse",
                description=(
                    "Hunt language, discourse, and period-specific phrases without "
                    "flattening everything into modern summary text."
                ),
                markdown=self.default_program_markdown,
                built_in=True,
                default_facets=["language_slang", "media_culture", "people"],
                default_adapter_id=self.default_adapter_id,
                default_execution_policy=defaults,
                preferred_source_classes=["archive", "news", "magazine"],
                excluded_source_classes=["shopping", "social"],
                quality_threshold=0.48,
                dedupe_similarity_threshold=0.88,
                created_at=now,
                updated_at=now,
            ),
            ResearchProgram(
                program_id="historical-rumor-folklore",
                name="Historical Fiction / Rumor And Folklore",
                description=(
                    "Collect contested narratives, hearsay, and legend-like material "
                    "while preserving their lower certainty."
                ),
                markdown=self.default_program_markdown,
                built_in=True,
                default_facets=["regional_context", "media_culture", "events"],
                default_adapter_id=self.default_adapter_id,
                default_execution_policy=defaults,
                preferred_source_classes=["archive", "news", "magazine"],
                excluded_source_classes=["shopping"],
                quality_threshold=0.42,
                dedupe_similarity_threshold=0.87,
                created_at=now,
                updated_at=now,
            ),
        ]

    def _resolve_program(self, program_id: str | None) -> ResearchProgram:
        built_ins = {program.program_id: program for program in self._built_in_programs()}
        if not program_id:
            return built_ins[_DEFAULT_PROGRAM_ID]
        if program_id in built_ins:
            return built_ins[program_id]
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
            if (
                any(item.input_type == "url" for item in brief.curated_inputs)
                and not capabilities.supports_fetch
            ):
                return f"Adapter {adapter.adapter_id} cannot fetch curated URL inputs."
        elif not capabilities.supports_search:
            return (
                f"Adapter {adapter.adapter_id} is a no-search adapter and requires curated inputs."
            )
        if policy.respect_robots and not capabilities.supports_robots:
            return f"Adapter {adapter.adapter_id} does not support robots checks."
        if (
            policy.allow_domains or policy.deny_domains
        ) and not capabilities.supports_domain_policy:
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
        page_cache: dict[str, ResearchFetchedPage],
        failed_fetch_cache: dict[str, tuple[str, str]],
        run: ResearchRun,
        policy: ResearchExecutionPolicy,
        queries: list[ResearchQueryPlan],
        started_monotonic: float,
        checkpoint: Callable[[], None] | None = None,
    ) -> str | None:
        queries_by_facet: dict[str, list[ResearchQueryPlan]] = {}
        for plan in queries:
            queries_by_facet.setdefault(plan.facet_id, []).append(plan)

        pending_by_facet: dict[str, list[ResearchFinding]] = {
            facet.facet_id: [] for facet in facets
        }
        max_rounds = max((len(items) for items in queries_by_facet.values()), default=0)

        def _pending_count() -> int:
            return sum(len(items) for items in pending_by_facet.values())

        def _stop(reason: str) -> str:
            self._finalize_pending_candidates(
                brief=brief,
                program=program,
                facets=facets,
                pending_by_facet=pending_by_facet,
                findings=findings,
                accepted_signatures=accepted_signatures,
                run=run,
                started_monotonic=started_monotonic,
            )
            return reason

        for round_index in range(max_rounds):
            self._checkpoint(checkpoint)
            for facet in facets:
                self._checkpoint(checkpoint)
                if len(findings) + _pending_count() >= brief.max_findings:
                    return _stop("Stopped early because max_findings was reached.")
                if self._deadline_exceeded(started_monotonic, policy):
                    return _stop("Stopped early because total fetch time was exhausted.")
                facet_queries = queries_by_facet.get(facet.facet_id, [])
                if round_index >= len(facet_queries):
                    continue
                query_plan = facet_queries[round_index]
                query = query_plan.query
                facet.queries_attempted += 1
                run.telemetry.queries_attempted += 1
                try:
                    raw_hits = self._run_retrying_search(
                        adapter, query, brief.max_results_per_query, run, policy
                    )
                    self._update_search_telemetry(
                        run, adapter.get_last_search_metadata(), query_plan.profile
                    )
                    hits = self._rank_search_hits_for_fetch(
                        brief,
                        facet,
                        raw_hits,
                    )
                except Exception as exc:
                    run.logs.append(f"Query [{facet.facet_id}] failed: {exc}")
                    run.warnings.append(f"Query [{facet.facet_id}] failed and was skipped: {exc}")
                    self._sync_run_progress(run, facets, findings, started_monotonic)
                    continue
                if not hits:
                    run.telemetry.search.zero_hit_queries_by_profile[query_plan.profile] = (
                        run.telemetry.search.zero_hit_queries_by_profile.get(
                            query_plan.profile, 0
                        )
                        + 1
                    )
                facet.hits_seen += len(hits)
                run.logs.append(
                    f"Query [{facet.facet_id}/{query_plan.profile}] returned "
                    f"{len(hits)} candidate hits."
                )
                for hit in hits:
                    self._checkpoint(checkpoint)
                    if len(findings) + _pending_count() >= brief.max_findings:
                        return _stop("Stopped early because max_findings was reached.")
                    if self._deadline_exceeded(started_monotonic, policy):
                        return _stop("Stopped early because total fetch time was exhausted.")
                    finding = self._prepare_finding_from_hit(
                        adapter_id=adapter_id,
                        adapter=adapter,
                        brief=brief,
                        program=program,
                        facet=facet,
                        query_plan=query_plan,
                        hit=hit,
                        page_cache=page_cache,
                        failed_fetch_cache=failed_fetch_cache,
                        run=run,
                        policy=policy,
                        findings=findings,
                        started_monotonic=started_monotonic,
                    )
                    if finding is not None:
                        pending_by_facet[facet.facet_id].append(finding)
        self._finalize_pending_candidates(
            brief=brief,
            program=program,
            facets=facets,
            pending_by_facet=pending_by_facet,
            findings=findings,
            accepted_signatures=accepted_signatures,
            run=run,
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
        page_cache: dict[str, ResearchFetchedPage],
        failed_fetch_cache: dict[str, tuple[str, str]],
        run: ResearchRun,
        policy: ResearchExecutionPolicy,
        started_monotonic: float,
        checkpoint: Callable[[], None] | None = None,
    ) -> str | None:
        pending_by_facet: dict[str, list[ResearchFinding]] = {
            facet.facet_id: [] for facet in facets
        }
        for item in brief.curated_inputs:
            self._checkpoint(checkpoint)
            pending_count = sum(len(items) for items in pending_by_facet.values())
            if len(findings) + pending_count >= brief.max_findings:
                self._finalize_pending_candidates(
                    brief=brief,
                    program=program,
                    facets=facets,
                    pending_by_facet=pending_by_facet,
                    findings=findings,
                    accepted_signatures=accepted_signatures,
                    run=run,
                    started_monotonic=started_monotonic,
                )
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
                    query_profile="curated_input",
                    hit=ResearchSearchHit(
                        query="curated_input",
                        url=item.url
                        or f"curated://{sha1((item.title or 'text').encode()).hexdigest()[:12]}",
                        title=item.title or "Curated Text Input",
                        snippet=(item.notes or item.text or "")[:240] or None,
                        rank=1,
                    ),
                    page=ResearchFetchedPage(
                        url=item.url
                        or f"curated://{sha1((item.title or 'text').encode()).hexdigest()[:12]}",
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
                pending_by_facet[facet.facet_id].append(finding)
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
            finding = self._prepare_finding_from_hit(
                adapter_id=adapter_id,
                adapter=adapter,
                brief=brief,
                program=program,
                facet=facet,
                query_plan=ResearchQueryPlan(
                    facet_id=facet.facet_id,
                    query="curated_input",
                    profile="curated_input",
                ),
                hit=hit,
                page_cache=page_cache,
                failed_fetch_cache=failed_fetch_cache,
                run=run,
                policy=policy,
                findings=findings,
                started_monotonic=started_monotonic,
            )
            if finding is not None:
                pending_by_facet[facet.facet_id].append(finding)
        for facet in facets:
            self._checkpoint(checkpoint)
            self._finalize_facet_candidates(
                brief=brief,
                program=program,
                facet=facet,
                facet_candidates=pending_by_facet[facet.facet_id],
                findings=findings,
                accepted_signatures=accepted_signatures,
                run=run,
                started_monotonic=started_monotonic,
            )
        return None

    def _checkpoint(self, checkpoint: Callable[[], None] | None) -> None:
        if checkpoint is not None:
            checkpoint()

    def _finalize_pending_candidates(
        self,
        *,
        brief: ResearchBrief,
        program: ResearchProgram,
        facets: list[ResearchFacet],
        pending_by_facet: dict[str, list[ResearchFinding]],
        findings: list[ResearchFinding],
        accepted_signatures: dict[str, dict[str, str]],
        run: ResearchRun,
        started_monotonic: float,
    ) -> None:
        for facet in facets:
            facet_candidates = pending_by_facet.get(facet.facet_id, [])
            if not facet_candidates:
                continue
            self._finalize_facet_candidates(
                brief=brief,
                program=program,
                facet=facet,
                facet_candidates=facet_candidates,
                findings=findings,
                accepted_signatures=accepted_signatures,
                run=run,
                started_monotonic=started_monotonic,
            )
            pending_by_facet[facet.facet_id] = []

    def _prepare_finding_from_hit(
        self,
        *,
        adapter_id: str,
        adapter: ResearchScoutAdapterPort,
        brief: ResearchBrief,
        program: ResearchProgram,
        facet: ResearchFacet,
        query_plan: ResearchQueryPlan,
        hit: ResearchSearchHit,
        page_cache: dict[str, ResearchFetchedPage],
        failed_fetch_cache: dict[str, tuple[str, str]],
        run: ResearchRun,
        policy: ResearchExecutionPolicy,
        findings: list[ResearchFinding],
        started_monotonic: float,
    ) -> ResearchFinding | None:
        query = query_plan.query
        canonical_url = self._canonical_url(hit.url)
        host = self._host_for_url(canonical_url)
        if self._is_blocked_by_domain_policy(canonical_url, policy):
            facet.skipped_count += 1
            run.telemetry.blocked_by_policy_count += 1
            run.logs.append(f"Skipped {canonical_url} because it was blocked by domain policy.")
            self._sync_run_progress(run, run.facets, findings, started_monotonic)
            return None
        if canonical_url in page_cache:
            page = page_cache[canonical_url]
            finding = self._build_finding(
                adapter_id=adapter_id,
                run=run,
                brief=brief,
                facet=facet,
                query=query,
                query_profile=query_plan.profile,
                hit=hit,
                page=page,
                fetch_outcome=ResearchFetchOutcome.FETCHED,
                fetch_status="cached",
            )
            finding.provenance.fetch_status = "cached"
            return finding
        if canonical_url in failed_fetch_cache:
            _, message = failed_fetch_cache[canonical_url]
            facet.skipped_count += 1
            run.logs.append(
                f"Reused cached fetch failure for [{facet.facet_id}] {canonical_url}: {message}"
            )
            self._sync_run_progress(run, run.facets, findings, started_monotonic)
            return None
        if host and run.telemetry.per_host_fetch_counts.get(host, 0) >= policy.per_host_fetch_cap:
            facet.skipped_count += 1
            run.telemetry.skipped_host_counts[host] = (
                run.telemetry.skipped_host_counts.get(host, 0) + 1
            )
            run.logs.append(f"Skipped {canonical_url} because host cap for {host} was reached.")
            self._sync_run_progress(run, run.facets, findings, started_monotonic)
            return None
        if policy.respect_robots:
            allowed = adapter.allows_fetch(canonical_url, user_agent=self.research_user_agent)
            if allowed is False:
                facet.skipped_count += 1
                run.telemetry.blocked_by_robots_count += 1
                run.logs.append(f"Skipped {canonical_url} because robots disallowed fetch.")
                self._sync_run_progress(run, run.facets, findings, started_monotonic)
                return None
            if allowed is None and "robots_unavailable" not in run.telemetry.fallback_flags:
                run.telemetry.fallback_flags.append("robots_unavailable")
                run.logs.append(
                    "Robots check was unavailable for one or more hosts; "
                    "continuing in degraded mode."
                )
        if self._should_skip_hit_before_fetch(brief, facet, hit):
            facet.skipped_count += 1
            run.logs.append(
                f"Skipped {canonical_url} before fetch because it looked low-value for this brief."
            )
            self._sync_run_progress(run, run.facets, findings, started_monotonic)
            return None

        try:
            if host:
                run.telemetry.per_host_fetch_counts[host] = (
                    run.telemetry.per_host_fetch_counts.get(host, 0) + 1
                )
            page = self._run_retrying_fetch(adapter, canonical_url, run, policy)
            page_cache[canonical_url] = page
            run.telemetry.successful_fetches += 1
            finding = self._build_finding(
                adapter_id=adapter_id,
                run=run,
                brief=brief,
                facet=facet,
                query=query,
                query_profile=query_plan.profile,
                hit=hit,
                page=page,
                fetch_outcome=ResearchFetchOutcome.FETCHED,
                fetch_status="fetched",
            )
        except Exception as exc:
            facet.rejected_count += 1
            category = self._failure_category(exc)
            failed_fetch_cache[canonical_url] = (category, str(exc))
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
                    query_profile=query_plan.profile,
                    hit=hit,
                    canonical_url=canonical_url,
                    category=category,
                ),
            )
            findings.append(finding)
            self.finding_store.save_findings([finding])
            run.logs.append(f"Fetch failed for [{facet.facet_id}] {canonical_url}: {exc}")
            self._sync_run_progress(run, run.facets, findings, started_monotonic)
            return None
        return finding

    def _rank_search_hits_for_fetch(
        self,
        brief: ResearchBrief,
        facet: ResearchFacet,
        hits: list[ResearchSearchHit],
    ) -> list[ResearchSearchHit]:
        return sorted(
            hits,
            key=lambda item: (
                self._prefetch_hit_score(brief, facet, item),
                item.provider_hit_count or 1,
                -(item.rank or 9999),
            ),
            reverse=True,
        )

    def _prefetch_hit_score(
        self,
        brief: ResearchBrief,
        facet: ResearchFacet,
        hit: ResearchSearchHit,
    ) -> float:
        title = hit.title or ""
        snippet = hit.snippet or ""
        url = self._canonical_url(hit.url)
        source_type = self._classify_source(url)
        relevance = self._relevance_score(
            " ".join(filter(None, [title, snippet])),
            self._scoring_tokens(
                brief.topic,
                facet.label,
                brief.locale,
                self._time_hint(brief),
                *(brief.domain_hints or []),
            ),
        )
        score = relevance
        score += (hit.fusion_score or 0.0) * 0.35
        score += max((hit.provider_hit_count or 1) - 1, 0) * 0.12
        score += self._source_class_score(source_type)[0]
        score += self._source_shape_score(url, title, snippet, "", source_type, brief)
        score += self._prefetch_era_score(url, title, snippet, brief)
        if source_type in {"shopping", "social"}:
            score -= 0.25
        return round(score, 4)

    def _update_search_telemetry(
        self,
        run: ResearchRun,
        metadata: dict[str, object] | None,
        query_profile: str,
    ) -> None:
        if not metadata:
            run.telemetry.search.zero_hit_queries_by_profile[query_profile] = (
                run.telemetry.search.zero_hit_queries_by_profile.get(query_profile, 0) + 1
            )
            return
        providers_used = metadata.get("providers_used") or []
        for provider_id in providers_used:
            if provider_id not in run.telemetry.search.providers_used:
                run.telemetry.search.providers_used.append(provider_id)
        for provider_id, count in (metadata.get("queries_by_provider") or {}).items():
            run.telemetry.search.queries_by_provider[provider_id] = (
                run.telemetry.search.queries_by_provider.get(provider_id, 0) + int(count)
            )
        for provider_id, count in (metadata.get("hits_by_provider") or {}).items():
            run.telemetry.search.hits_by_provider[provider_id] = (
                run.telemetry.search.hits_by_provider.get(provider_id, 0) + int(count)
            )
        if metadata.get("fallback_used"):
            run.telemetry.search.fallback_used = True
            run.telemetry.search.fallback_reason = (
                metadata.get("fallback_reason") or "provider fallback used"
            )

    def _should_skip_hit_before_fetch(
        self,
        brief: ResearchBrief,
        facet: ResearchFacet,
        hit: ResearchSearchHit,
    ) -> bool:
        url = self._canonical_url(hit.url)
        title = hit.title or ""
        snippet = hit.snippet or ""
        source_type = self._classify_source(url)
        combined = " ".join(filter(None, [url, title, snippet])).lower()
        score = self._prefetch_hit_score(brief, facet, hit)
        has_time_anchor = self._text_mentions_requested_time(combined, brief)
        if source_type in {"social", "shopping", "video"}:
            return True
        if source_type == "reference" and not has_time_anchor:
            return True
        if any(pattern in combined for pattern in _LOW_VALUE_RESULT_PATTERNS):
            if has_time_anchor:
                return score < 0.1
            return score < 0.55
        return score < -0.05

    def _prefetch_era_score(
        self,
        url: str,
        title: str,
        snippet: str,
        brief: ResearchBrief,
    ) -> float:
        score = 0.0
        text = " ".join(filter(None, [url, title, snippet]))
        if self._text_mentions_requested_time(text, brief):
            score += 0.2
        elif self._text_mentions_contextual_time(text, brief):
            score += 0.08
        years = [int(year) for year in re.findall(r"\b((?:19|20)\d{2})\b", text)]
        for year in years:
            era_band = self._era_band_for_year(year, brief)
            if era_band == "core":
                score += 0.18
            elif era_band == "historical":
                score += 0.05
            elif era_band in {"future", "distant_future"}:
                score -= 0.12
        return score

    def _is_retrospective_or_guide_title(self, title: str) -> bool:
        lowered = title.lower()
        if any(pattern in lowered for pattern in _GUIDE_TITLE_PATTERNS):
            return True
        if any(pattern in lowered for pattern in _RETROSPECTIVE_TITLE_PATTERNS):
            return True
        return any(pattern in lowered for pattern in ("guide", "history of", "most influential"))

    def _acceptance_preference_delta(self, finding: ResearchFinding) -> float:
        provenance = finding.provenance
        if provenance is None:
            return 0.0
        scoring = provenance.scoring
        delta = 0.0
        penalty_flags: list[str] = []

        if scoring.period_native:
            delta += 0.12
        elif scoring.period_evidenced:
            delta += 0.08
        elif scoring.historical_contextual:
            delta += 0.04

        if scoring.era_band in {"future", "distant_future"} and not scoring.period_evidenced:
            delta -= 0.10
            penalty_flags.append("acceptance_penalty:future_without_period_evidence")
        if (
            provenance.query_profile == "broad"
            and not scoring.period_native
            and not scoring.period_evidenced
        ):
            delta -= 0.08
            penalty_flags.append("acceptance_penalty:broad_without_period_evidence")
        if (
            scoring.era_band != "core"
            and not scoring.period_evidenced
            and self._is_retrospective_or_guide_title(finding.title)
        ):
            delta -= 0.10
            penalty_flags.append("acceptance_penalty:retrospective_title")

        provenance.policy_flags = [
            flag for flag in provenance.policy_flags if not flag.startswith("acceptance_penalty:")
        ]
        provenance.policy_flags.extend(penalty_flags)
        return round(max(-0.18, min(delta, 0.18)), 4)

    def _fails_future_soft_gate(self, finding: ResearchFinding) -> bool:
        provenance = finding.provenance
        if provenance is None:
            return False
        scoring = provenance.scoring
        return (
            scoring.era_band in {"future", "distant_future"}
            and not scoring.period_evidenced
            and scoring.anchor_score < 0.24
            and scoring.concreteness_score < 0.16
        )

    def _finalize_facet_candidates(
        self,
        *,
        brief: ResearchBrief,
        program: ResearchProgram,
        facet: ResearchFacet,
        facet_candidates: list[ResearchFinding],
        findings: list[ResearchFinding],
        accepted_signatures: dict[str, dict[str, str]],
        run: ResearchRun,
        started_monotonic: float,
    ) -> None:
        accepted_source_usage = self._accepted_source_usage(findings)
        for finding in facet_candidates:
            self._apply_source_saturation(finding, accepted_source_usage)
        acceptance_deltas = {
            finding.finding_id: self._acceptance_preference_delta(finding)
            for finding in facet_candidates
        }
        ranked = sorted(
            facet_candidates,
            key=lambda item: (
                item.score + acceptance_deltas.get(item.finding_id, 0.0),
                item.provenance.scoring.period_native if item.provenance else False,
                item.provenance.scoring.period_evidenced if item.provenance else False,
                item.provenance.query_profile in {"anchored", "source_seeking"}
                if item.provenance
                else False,
                item.provenance.scoring.anchor_score if item.provenance else 0.0,
                item.provenance.scoring.concreteness_score if item.provenance else 0.0,
                item.provenance.scoring.facet_fit_score if item.provenance else 0.0,
                item.relevance_score,
                item.quality_score,
                len(item.page_excerpt or item.snippet_text),
            ),
            reverse=True,
        )
        for finding in ranked:
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
                facet.facet_id,
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
            provenance.scoring.overall_score = finding.score
            provenance.scoring.threshold_passed = finding.score >= program.quality_threshold
            if duplicate_reason or facet.accepted_count >= min(
                facet.target_count, brief.max_per_facet
            ):
                pass
            elif not self._passes_facet_fit(facet, provenance.scoring.facet_fit_score):
                finding.decision = ResearchFindingDecision.REJECTED
                finding.rejection_reason = "Finding did not meet the facet-fit threshold."
                provenance.rejection_reason = ResearchFindingReason.REJECTED_QUALITY_THRESHOLD
                provenance.policy_flags.append("facet_fit")
            elif self._fails_future_soft_gate(finding):
                finding.decision = ResearchFindingDecision.REJECTED
                finding.rejection_reason = (
                    "Future-era finding lacked period evidence or strong anchors."
                )
                provenance.rejection_reason = ResearchFindingReason.REJECTED_QUALITY_THRESHOLD
                if "future_soft_gate" not in provenance.policy_flags:
                    provenance.policy_flags.append("future_soft_gate")
            elif finding.score < program.quality_threshold:
                finding.decision = ResearchFindingDecision.REJECTED
                finding.rejection_reason = "Finding score fell below the quality threshold."
                provenance.rejection_reason = ResearchFindingReason.REJECTED_QUALITY_THRESHOLD
            else:
                finding.decision = ResearchFindingDecision.ACCEPTED
                accepted_signatures[signature] = {
                    "facet_id": facet.facet_id,
                    "title": finding.title,
                    "normalized_title": self._normalize_title(finding.title),
                    "host": self._host_for_url(finding.canonical_url or finding.url),
                    "canonical_url": self._canonical_url(finding.canonical_url or finding.url),
                    "path": self._path_for_url(finding.canonical_url or finding.url),
                }
                facet.accepted_count += 1
                provenance.acceptance_reason = ResearchFindingReason.ACCEPTED_QUALITY_THRESHOLD
                provider_id = provenance.search_provider_id or "unknown"
                run.telemetry.search.accepted_by_provider[provider_id] = (
                    run.telemetry.search.accepted_by_provider.get(provider_id, 0) + 1
                )
                profile_id = provenance.query_profile or "unknown"
                run.telemetry.search.accepted_by_profile[profile_id] = (
                    run.telemetry.search.accepted_by_profile.get(profile_id, 0) + 1
                )
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
                run.telemetry.elapsed_fetch_time_ms += int(
                    (time.monotonic() - fetch_started) * 1000
                )
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
                run.telemetry.elapsed_fetch_time_ms += int(
                    (time.monotonic() - fetch_started) * 1000
                )
        if page is None:
            raise RuntimeError(f"Fetch produced no page for {url}")
        return page

    def _expand_facets(self, brief: ResearchBrief, program: ResearchProgram) -> list[ResearchFacet]:
        requested = brief.desired_facets or program.default_facets or list(_DEFAULT_FACETS)
        facets: list[ResearchFacet] = []
        for facet_id in requested:
            label, query_hint = _DEFAULT_FACETS.get(
                facet_id,
                (
                    " ".join(part.capitalize() for part in facet_id.split("_")),
                    facet_id.replace("_", " "),
                ),
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
    ) -> list[ResearchQueryPlan]:
        broad_queries: list[ResearchQueryPlan] = []
        anchored_queries: list[ResearchQueryPlan] = []
        source_queries: list[ResearchQueryPlan] = []
        topic_phrase = self._compact_query_phrase(self._query_topic_phrase(brief), max_words=6)
        locale_phrase = self._compact_query_phrase(brief.locale or "", max_words=3)
        audience_phrase = self._compact_query_phrase(brief.audience or "", max_words=3)
        domain_phrase = self._quoted_domain_phrase(brief)
        domain_tail = self._domain_tail_phrase(brief)
        for facet in facets:
            facet_phrase = self._compact_query_phrase(facet.query_hint, max_words=4)
            primary_parts = [
                self._time_hint(brief),
                topic_phrase,
                facet_phrase,
                locale_phrase,
                audience_phrase,
            ]
            if domain_phrase:
                primary_parts.append(domain_phrase)
            primary_query = " ".join(
                part.strip() for part in primary_parts if part and part.strip()
            )
            broad_queries.append(
                ResearchQueryPlan(
                    facet_id=facet.facet_id,
                    query=primary_query,
                    profile="broad",
                )
            )

            anchored_parts = [
                self._expanded_query_time_phrase(brief),
                topic_phrase,
                facet_phrase,
                locale_phrase,
            ]
            if domain_phrase:
                anchored_parts.append(domain_phrase)
            anchored_query = " ".join(
                part.strip() for part in anchored_parts if part and part.strip()
            )
            if anchored_query and anchored_query != primary_query:
                anchored_queries.append(
                    ResearchQueryPlan(
                        facet_id=facet.facet_id,
                        query=anchored_query,
                        profile="anchored",
                    )
                )

            source_parts = [
                self._query_time_phrase(brief),
                topic_phrase,
                locale_phrase,
                _SOURCE_SEEKING_TERMS,
            ]
            if facet_phrase:
                source_parts.append(facet_phrase)
            if domain_tail:
                source_parts.append(domain_tail)
            source_query = " ".join(part.strip() for part in source_parts if part and part.strip())
            if source_query and source_query not in {primary_query, anchored_query}:
                source_queries.append(
                    ResearchQueryPlan(
                        facet_id=facet.facet_id,
                        query=source_query,
                        profile="source_seeking",
                    )
                )
        return (anchored_queries + source_queries + broad_queries)[: brief.max_queries]

    def _build_finding(
        self,
        adapter_id: str,
        run: ResearchRun,
        brief: ResearchBrief,
        facet: ResearchFacet,
        query: str,
        query_profile: str | None,
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
            brief,
        )
        snippet = (hit.snippet or excerpt or title).strip()
        publisher = page.publisher or self._host_for_url(canonical_url)
        relevance = self._relevance_score(
            " ".join(filter(None, [title, hit.snippet or "", excerpt])),
            self._scoring_tokens(
                brief.topic,
                facet.label,
                brief.locale,
                self._time_hint(brief),
            ),
        )
        structural_score = self._structural_score(title, snippet, page.published_at)
        source_class_score, boost, penalty = self._source_class_score(source_type)
        era_score = self._era_score(page.published_at, brief)
        anchor_score = self._anchor_score(
            title=title, snippet=snippet, excerpt=excerpt, brief=brief
        )
        concreteness_score = self._concreteness_score(
            title=title, snippet=snippet, excerpt=excerpt, brief=brief
        )
        era_band, period_native, period_evidenced, historical_contextual = (
            self._classify_period_context(
                title=title,
                snippet=snippet,
                excerpt=excerpt,
                published_at=page.published_at,
                brief=brief,
                concreteness_score=concreteness_score,
            )
        )
        facet_fit_score = self._facet_fit_score(
            facet,
            title=title,
            snippet=snippet,
            excerpt=excerpt,
            brief=brief,
        )
        coverage_score = self._coverage_score(facet, brief)
        shape_score = self._source_shape_score(
            canonical_url,
            title,
            snippet,
            excerpt,
            source_type,
            brief,
        )
        shape_score += self._facet_specificity_score(
            facet,
            title=title,
            snippet=snippet,
            excerpt=excerpt,
        )
        if era_band in {"future", "distant_future"} and not period_evidenced:
            shape_score -= 0.12 if era_band == "future" else 0.2
        elif era_band == "historical" and not period_evidenced:
            shape_score -= 0.06
        profile_score = self._query_profile_score(query_profile)
        quality = max(
            0.0,
            min(
                structural_score
                + source_class_score
                + era_score
                + anchor_score
                + concreteness_score
                + coverage_score
                + shape_score
                + profile_score,
                1.0,
            ),
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
                query_profile=query_profile,
                search_provider_id=hit.search_provider_id,
                matched_providers=hit.matched_providers,
                provider_rank=hit.provider_rank,
                fusion_score=hit.fusion_score,
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
                    anchor_score=round(anchor_score, 4),
                    concreteness_score=round(concreteness_score, 4),
                    shape_score=round(shape_score + profile_score, 4),
                    facet_fit_score=round(facet_fit_score, 4),
                    source_saturation_score=0.0,
                    coverage_score=round(coverage_score, 4),
                    source_type=source_type,
                    source_class_boost_applied=boost,
                    source_class_penalty_applied=penalty,
                    near_era_bias_applied=(era_score + anchor_score) > 0.0,
                    era_band=era_band,
                    period_native=period_native,
                    period_evidenced=period_evidenced,
                    historical_contextual=historical_contextual,
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
        query_profile: str | None,
        hit: ResearchSearchHit,
        canonical_url: str,
        category: str,
    ) -> ResearchFindingProvenance:
        return ResearchFindingProvenance(
            adapter_id=adapter_id,
            facet_id=facet.facet_id,
            facet_label=facet.label,
            originating_query=query,
            query_profile=query_profile,
            search_provider_id=hit.search_provider_id,
            matched_providers=hit.matched_providers,
            provider_rank=hit.provider_rank,
            fusion_score=hit.fusion_score,
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
                anchor_score=0.0,
                concreteness_score=0.0,
                shape_score=0.0,
                facet_fit_score=0.0,
                source_saturation_score=0.0,
                coverage_score=0.0,
                quality_threshold=0.0,
                threshold_passed=False,
                source_type=None,
                source_class_boost_applied=0.0,
                source_class_penalty_applied=0.0,
                near_era_bias_applied=False,
                era_band="unknown",
                period_native=False,
                period_evidenced=False,
                historical_contextual=False,
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
            if item.decision == ResearchFindingDecision.ACCEPTED
            and item.facet_id == finding.facet_id
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
                f"Semantic fallback: {result.fallback_reason}"
                if result.fallback_reason
                else "Semantic fallback used."
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
            if (
                base_score >= quality_threshold
                and adjusted_score < quality_threshold
                and rerank_delta < 0
            ):
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
                item for item in facet_findings if item.decision == ResearchFindingDecision.REJECTED
            ]
            accepted_for_facet = [
                item for item in facet_findings if item.decision == ResearchFindingDecision.ACCEPTED
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
                    and item.provenance.rejection_reason
                    == ResearchFindingReason.REJECTED_QUALITY_THRESHOLD
                )
                for item in rejected_for_facet
            )
            excluded_source_rejections = sum(
                bool(
                    item.provenance
                    and item.provenance.rejection_reason
                    == ResearchFindingReason.REJECTED_EXCLUDED_SOURCE
                )
                for item in rejected_for_facet
            )
            fetch_failures = sum(
                bool(
                    item.provenance
                    and item.provenance.rejection_reason
                    == ResearchFindingReason.REJECTED_FETCH_FAILURE
                )
                for item in rejected_for_facet
            )
            accepted_sources_by_type: dict[str, int] = {}
            for item in accepted_for_facet:
                source_type = item.source_type or "unknown"
                accepted_sources_by_type[source_type] = (
                    accepted_sources_by_type.get(source_type, 0) + 1
                )
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
        if (
            duplicate_rejections
            or threshold_rejections
            or excluded_source_rejections
            or fetch_failures
        ):
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
        facet_id: str,
        accepted_signatures: dict[str, dict[str, str]],
        similarity_threshold: float,
    ) -> tuple[str | None, str | None]:
        if signature in accepted_signatures:
            if accepted_signatures[signature].get("facet_id") == facet_id:
                return "Duplicate of an already accepted finding.", "exact_signature"
        canonical_url = self._canonical_url(url)
        normalized_title = self._normalize_title(title)
        host = self._host_for_url(url)
        path = self._path_for_url(url)
        for existing in accepted_signatures.values():
            if existing.get("facet_id") != facet_id:
                continue
            existing_title_raw = existing.get("title", "")
            existing_title = existing.get("normalized_title", "")
            existing_host = existing.get("host", "")
            existing_url = existing.get("canonical_url", "")
            existing_path = existing.get("path", "")
            if existing_url == canonical_url and existing_title_raw and existing_title_raw != title:
                return (
                    "Duplicate URL with a title variant of an already accepted finding.",
                    "same_canonical_url_different_title",
                )
            if existing_host and host and existing_host == host:
                if (
                    SequenceMatcher(None, existing_title, normalized_title).ratio()
                    >= similarity_threshold
                ):
                    return (
                        "Near-duplicate of an already accepted finding.",
                        "same_host_similar_title",
                    )
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
                return (
                    "Cross-host duplicate of a highly specific titled finding.",
                    "same_title_cross_host",
                )
        return None, None

    def _is_excluded_source(
        self,
        source_type: str | None,
        brief: ResearchBrief,
        program: ResearchProgram,
    ) -> bool:
        if not source_type:
            return False
        return source_type in set(brief.excluded_source_types) | set(
            program.excluded_source_classes
        )

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
        combined = "\n".join(
            part.strip()
            for part in [finding.page_excerpt or "", finding.snippet_text]
            if part and part.strip()
        ).strip()
        sentences = self._candidate_stage_sentences(combined)
        secondary_sentences = self._candidate_secondary_stage_sentences(combined)
        for sentence in secondary_sentences:
            if sentence not in sentences:
                sentences.append(sentence)
        high_anchor_sentences = [
            sentence
            for sentence in sentences
            if self._stage_sentence_anchor_score(sentence, run.brief) >= 0.28
        ]
        ranked = sorted(
            sentences,
            key=lambda sentence: (
                self._stage_sentence_score(sentence, run.brief, finding),
                len(sentence),
            ),
            reverse=True,
        )
        chosen: list[str] = []
        seen: set[str] = set()
        chosen_anchor_types: set[str] = set()
        for sentence in ranked:
            normalized = self._normalize_text(sentence)
            if not normalized or normalized in seen:
                continue
            anchor_type = self._stage_anchor_type(sentence)
            if anchor_type in chosen_anchor_types and len(chosen) < 2:
                continue
            seen.add(normalized)
            chosen.append(sentence)
            chosen_anchor_types.add(anchor_type)
            if len(chosen) == 4:
                break
        if len(chosen) < 4:
            for sentence in ranked:
                normalized = self._normalize_text(sentence)
                if not normalized or normalized in seen:
                    continue
                if self._stage_sentence_score(sentence, run.brief, finding) < 0.18:
                    continue
                seen.add(normalized)
                chosen.append(sentence)
                if len(chosen) == 4:
                    break
        if high_anchor_sentences and not any(
            sentence in chosen for sentence in high_anchor_sentences
        ):
            anchor_sentence = max(
                high_anchor_sentences,
                key=lambda sentence: self._stage_sentence_score(sentence, run.brief, finding),
            )
            if chosen:
                chosen[-1] = anchor_sentence
            else:
                chosen.append(anchor_sentence)
        if finding.provenance:
            flags = [flag for flag in finding.provenance.policy_flags if flag != "weak_anchor"]
            if not high_anchor_sentences:
                flags.append("weak_anchor")
            finding.provenance.policy_flags = flags
        if chosen:
            return "\n\n".join(chosen)
        return combined

    def _best_excerpt(
        self,
        text: str,
        desired_tokens: set[str],
        brief: ResearchBrief,
    ) -> str:
        if not text:
            return ""
        candidates = self._candidate_stage_sentences(text)
        if not candidates:
            candidates = [
                segment.strip()
                for segment in re.split(r"\n{2,}", text)
                if segment and segment.strip()
            ]
        ranked = sorted(
            candidates,
            key=lambda segment: (
                self._excerpt_segment_score(segment, desired_tokens, brief),
                len(segment),
            ),
            reverse=True,
        )
        chosen: list[str] = []
        seen: set[str] = set()
        for segment in ranked:
            normalized = self._normalize_text(segment)
            if not normalized or normalized in seen:
                continue
            score = self._excerpt_segment_score(segment, desired_tokens, brief)
            if score < 0.08:
                continue
            seen.add(normalized)
            chosen.append(" ".join(segment.split()))
            if len(chosen) == 3:
                break
        return " ".join(chosen)[:900] if chosen else ""

    def _excerpt_segment_score(
        self,
        segment: str,
        desired_tokens: set[str],
        brief: ResearchBrief,
    ) -> float:
        cleaned = " ".join(segment.split()).strip()
        lower = cleaned.lower()
        score = self._relevance_score(cleaned, desired_tokens)
        if self._text_mentions_requested_time(cleaned, brief):
            score += 0.28
        elif self._text_mentions_contextual_time(cleaned, brief):
            score += 0.12
        if re.search(r"\b(19|20)\d{2}\b", cleaned):
            score += 0.12
        if self._has_specific_date_anchor(cleaned):
            score += 0.12
        concreteness = self._concreteness_score(title="", snippet=cleaned, excerpt="", brief=brief)
        if concreteness >= 0.16:
            score += 0.12
            if self._text_mentions_contextual_time(cleaned, brief):
                score += 0.08
        if re.search(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3}\b", cleaned):
            score += 0.08
        if re.search(r"\b\d+\b", cleaned):
            score += 0.04
        if len(cleaned.split()) >= 12:
            score += 0.05
        if any(pattern in lower for pattern in _LOW_VALUE_EXCERPT_PATTERNS):
            score -= 0.45
        if any(pattern in lower for pattern in _PROMO_TEXT_PATTERNS):
            score -= 0.3
        if any(pattern in lower for pattern in _VAGUE_STAGE_PATTERNS):
            score -= 0.15
        return score

    def _best_facet_for_text(
        self,
        facets: list[ResearchFacet],
        brief: ResearchBrief,
        title: str,
        text: str,
    ) -> ResearchFacet:
        desired_tokens_by_facet = {
            facet.facet_id: self._scoring_tokens(
                brief.topic, facet.label, brief.locale, self._time_hint(brief)
            )
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

    def _era_score(
        self,
        published_at: str | None,
        brief: ResearchBrief,
    ) -> float:
        published_year = self._year_from_date(published_at)
        if published_year is None or not published_year.isdigit():
            return 0.0
        year = int(published_year)
        era_band = self._era_band_for_year(year, brief)
        if era_band == "core":
            return 0.34
        if era_band == "historical":
            return 0.1
        if era_band == "future":
            return 0.02
        if era_band == "distant_future":
            return -0.24
        if era_band == "distant_past":
            return -0.08
        return 0.0

    def _period_evidence_strength(
        self,
        *,
        title: str,
        snippet: str,
        excerpt: str,
        brief: ResearchBrief,
        concreteness_score: float,
    ) -> float:
        combined = " ".join(filter(None, [title, snippet, excerpt]))
        lower = combined.lower()
        explicit_years = {int(year) for year in self._years_mentioned(combined)}
        core_start, core_end = self._core_year_range(brief)
        historical_start, historical_end = self._historical_year_range(brief)
        score = 0.0
        if core_start is None or core_end is None:
            return 0.0
        if any(core_start <= year <= core_end for year in explicit_years):
            score += 0.34
        elif (
            historical_start is not None
            and historical_end is not None
            and any(historical_start <= year <= historical_end for year in explicit_years)
        ):
            score += 0.12
        if self._has_specific_date_anchor(combined):
            score += 0.08
        if concreteness_score >= 0.16:
            score += 0.08
        if any(term in lower for term in _ANCHOR_TERMS):
            score += 0.08
        return score

    def _classify_period_context(
        self,
        *,
        title: str,
        snippet: str,
        excerpt: str,
        published_at: str | None,
        brief: ResearchBrief,
        concreteness_score: float,
    ) -> tuple[str, bool, bool, bool]:
        published_year = self._year_from_date(published_at)
        year = int(published_year) if published_year and published_year.isdigit() else None
        era_band = self._era_band_for_year(year, brief)
        period_native = era_band == "core"
        evidence_strength = self._period_evidence_strength(
            title=title,
            snippet=snippet,
            excerpt=excerpt,
            brief=brief,
            concreteness_score=concreteness_score,
        )
        period_evidenced = not period_native and evidence_strength >= 0.32
        historical_contextual = era_band == "historical"
        return era_band, period_native, period_evidenced, historical_contextual

    def _anchor_score(
        self,
        *,
        title: str,
        snippet: str,
        excerpt: str,
        brief: ResearchBrief,
    ) -> float:
        combined = " ".join(filter(None, [title, snippet, excerpt]))
        lower = combined.lower()
        score = 0.0
        if self._text_mentions_requested_time(combined, brief):
            score += 0.22
        elif self._text_mentions_contextual_time(combined, brief):
            score += 0.08
        explicit_years = self._years_mentioned(combined)
        core_years = self._core_year_tokens(brief)
        contextual_years = self._contextual_year_tokens(brief)
        if core_years.intersection(explicit_years):
            score += 0.18
        elif contextual_years.intersection(explicit_years):
            score += 0.08
        elif explicit_years:
            score += 0.05
        if self._has_specific_date_anchor(combined):
            score += 0.08
        if any(term in lower for term in _ANCHOR_TERMS):
            score += 0.08
        return score

    def _concreteness_score(
        self,
        *,
        title: str,
        snippet: str,
        excerpt: str,
        brief: ResearchBrief,
    ) -> float:
        combined = " ".join(filter(None, [title, snippet, excerpt]))
        lower = combined.lower()
        score = 0.0
        if self._years_mentioned(combined):
            score += 0.08
        if re.search(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3}\b", combined):
            score += 0.08
        if self._has_specific_date_anchor(combined):
            score += 0.08
        if brief.locale and brief.locale.lower() in lower:
            score += 0.05
        score += min(sum(lower.count(term) for term in _CONCRETENESS_TERMS) * 0.035, 0.18)
        return score

    def _facet_fit_score(
        self,
        facet: ResearchFacet,
        *,
        title: str,
        snippet: str,
        excerpt: str,
        brief: ResearchBrief,
    ) -> float:
        combined = " ".join(filter(None, [title, snippet, excerpt]))
        lower = combined.lower()
        score = max(
            self._facet_specificity_score(facet, title=title, snippet=snippet, excerpt=excerpt),
            -0.2,
        )
        score += min(
            self._relevance_score(combined, self._scoring_tokens(facet.label, facet.query_hint))
            * 0.2,
            0.2,
        )
        if facet.facet_id == "people":
            if re.search(r"\bDJ\b", combined) or re.search(
                r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2}\b", combined
            ):
                score += 0.08
            return score
        facet_anchor_bonus = 0.0
        if facet.facet_id == "objects_technology" and any(
            term in lower
            for term in ("vinyl", "cdj", "turntable", "mixtape", "record pool", "gear", "format")
        ):
            facet_anchor_bonus = 0.16
        elif facet.facet_id == "media_culture" and any(
            term in lower
            for term in ("radio", "press", "magazine", "coverage", "media", "television", "feature")
        ):
            facet_anchor_bonus = 0.16
        elif facet.facet_id == "regional_context" and any(
            term in lower
            for term in (
                "chicago",
                "neighborhood",
                "south side",
                "north side",
                "wicker park",
                "local",
                "regional",
            )
        ):
            facet_anchor_bonus = 0.16
        elif facet.facet_id == "practices" and any(term in lower for term in _PRACTICE_TERMS):
            facet_anchor_bonus = 0.16
        score += facet_anchor_bonus
        if self._text_mentions_requested_time(combined, brief):
            score += 0.04
        return score

    def _facet_fit_threshold(self, facet: ResearchFacet) -> float:
        if facet.facet_id == "people":
            return 0.0
        if facet.facet_id == "practices":
            return 0.02
        return 0.05

    def _passes_facet_fit(self, facet: ResearchFacet, facet_fit_score: float) -> bool:
        return facet_fit_score >= self._facet_fit_threshold(facet)

    def _source_identity(self, finding: ResearchFinding) -> str:
        return self._canonical_url(finding.canonical_url or finding.url)

    def _accepted_source_usage(self, findings: list[ResearchFinding]) -> dict[str, int]:
        usage: dict[str, int] = {}
        for item in findings:
            if item.decision != ResearchFindingDecision.ACCEPTED:
                continue
            identity = self._source_identity(item)
            usage[identity] = usage.get(identity, 0) + 1
        return usage

    def _source_saturation_penalty(self, accepted_count: int) -> float:
        if accepted_count <= 0:
            return 0.0
        if accepted_count == 1:
            return -0.14
        if accepted_count == 2:
            return -0.24
        return -0.34

    def _apply_source_saturation(
        self,
        finding: ResearchFinding,
        accepted_source_usage: dict[str, int],
    ) -> None:
        provenance = finding.provenance
        if provenance is None:
            return
        identity = self._source_identity(finding)
        accepted_count = accepted_source_usage.get(identity, 0)
        saturation_score = self._source_saturation_penalty(accepted_count)
        provenance.scoring.source_saturation_score = round(saturation_score, 4)
        if accepted_count > 0:
            flags = [
                flag for flag in provenance.policy_flags if not flag.startswith("source_saturation")
            ]
            flags.append(f"source_saturation:{accepted_count}")
            provenance.policy_flags = flags
        if saturation_score:
            finding.score = round(max(0.0, min(finding.score + saturation_score, 1.0)), 4)
            provenance.scoring.overall_score = finding.score

    def _query_profile_score(self, query_profile: str | None) -> float:
        if query_profile == "anchored":
            return 0.06
        if query_profile == "source_seeking":
            return 0.05
        return 0.0

    def _coverage_score(self, facet: ResearchFacet, brief: ResearchBrief) -> float:
        target = max(1, min(facet.target_count, brief.max_per_facet))
        if facet.accepted_count == 0:
            return 0.08
        if facet.accepted_count < target:
            return 0.04
        return 0.0

    def _source_shape_score(
        self,
        url: str,
        title: str,
        snippet: str,
        excerpt: str,
        source_type: str | None,
        brief: ResearchBrief,
    ) -> float:
        score = 0.0
        path = self._path_for_url(url)
        title_lower = title.lower()
        combined_lower = " ".join(filter(None, [title, snippet, excerpt])).lower()
        has_time_anchor = self._finding_text_has_time_anchor(title, snippet, excerpt, brief)
        has_specific_date_anchor = self._has_specific_date_anchor(
            " ".join(filter(None, [url, title, snippet, excerpt]))
        )
        has_query_identity = self._url_has_strong_query_identity(url)
        has_concrete_anchor = (
            self._concreteness_score(
                title=title,
                snippet=snippet,
                excerpt=excerpt,
                brief=brief,
            )
            >= 0.16
        )

        if path == "/":
            score -= 0.2
        elif self._has_weak_identity_path(path) and not has_query_identity:
            score -= 0.1
        if any(pattern in title_lower for pattern in _GUIDE_TITLE_PATTERNS):
            score -= 0.22
        if any(pattern in title_lower for pattern in _WEAK_SOURCE_SHAPE_PATTERNS):
            score -= 0.16
        if any(pattern in title_lower for pattern in _RETROSPECTIVE_TITLE_PATTERNS):
            score -= 0.26
        if any(pattern in combined_lower for pattern in _PROMO_TEXT_PATTERNS):
            score -= 0.12
        if any(pattern in combined_lower for pattern in _VAGUE_STAGE_PATTERNS):
            score -= 0.12
        if any(pattern in combined_lower for pattern in _LOW_VALUE_RESULT_PATTERNS):
            score -= 0.14
        if not has_time_anchor:
            score -= 0.08
        if has_specific_date_anchor:
            score += 0.14
        elif has_time_anchor and has_query_identity:
            score += 0.08
        if has_concrete_anchor:
            score += 0.12
        if source_type in {"shopping", "social"}:
            score -= 0.16
        elif source_type == "archive" and path == "/" and not has_time_anchor:
            score -= 0.1
        elif source_type == "web" and any(
            pattern in title_lower for pattern in ("guide", "history", "top ", "about ")
        ):
            score -= 0.14
        elif source_type == "educational" and any(
            pattern in title_lower for pattern in ("abstract", "introduction", "paper", "thesis")
        ):
            score -= 0.14
        if any(
            pattern in combined_lower
            for pattern in (
                "interview",
                "review",
                "residency",
                "flyer",
                "radio show",
                "track listing",
                "event listing",
            )
        ):
            score += 0.1
        return score

    def _facet_specificity_score(
        self,
        facet: ResearchFacet,
        *,
        title: str,
        snippet: str,
        excerpt: str,
    ) -> float:
        combined = " ".join(filter(None, [title, snippet, excerpt]))
        lower = combined.lower()
        facet_tokens = self._scoring_tokens(facet.query_hint, facet.label)
        matches = sum(1 for token in facet_tokens if token in lower)
        score = min(matches * 0.03, 0.12)
        if facet.facet_id == "people":
            if re.search(r"\bDJ\b", combined) or re.search(
                r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2}\b", combined
            ):
                score += 0.05
            return score
        if matches == 0:
            score -= 0.12
        elif matches == 1:
            score -= 0.02
        if facet.facet_id == "objects_technology" and not any(
            term in lower
            for term in ("vinyl", "cdj", "turntable", "mixtape", "record pool", "gear", "format")
        ):
            score -= 0.08
        if facet.facet_id == "media_culture" and not any(
            term in lower
            for term in ("radio", "press", "magazine", "coverage", "media", "television", "feature")
        ):
            score -= 0.08
        if facet.facet_id == "regional_context" and not any(
            term in lower
            for term in (
                "chicago",
                "neighborhood",
                "south side",
                "north side",
                "wicker park",
                "local",
                "regional",
            )
        ):
            score -= 0.08
        if facet.facet_id == "practices" and not any(term in lower for term in _PRACTICE_TERMS):
            score -= 0.08
        return score

    def _classify_source(self, url: str) -> str:
        host = self._host_for_url(url)
        lower = host.lower()
        if lower.endswith(".gov"):
            return "government"
        if lower.endswith(".edu"):
            return "educational"
        if any(part in lower for part in ("archive", "library", "museum")):
            return "archive"
        if any(part in lower for part in ("wikipedia", "britannica", "encyclopedia")):
            return "reference"
        if any(
            part in lower
            for part in (
                "news",
                "newspaper",
                "times",
                "post",
                "guardian",
                "bbc",
                "tribune",
                "reader",
                "journal",
                "npr",
                "fox",
                "abc",
                "cbs",
                "nbc",
                "wttw",
                "kutx",
            )
        ):
            return "news"
        if any(
            part in lower for part in ("youtube", "youtu.be", "vimeo", "soundcloud", "mixcloud")
        ):
            return "video"
        if any(
            part in lower
            for part in (
                "magazine",
                "rollingstone",
                "billboard",
                "vice",
                "djmag",
                "5mag",
                "chicagomag",
            )
        ):
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

    def _query_time_phrase(self, brief: ResearchBrief) -> str:
        if brief.focal_year:
            return f'"{brief.focal_year}"'
        return self._time_hint(brief)

    def _query_topic_phrase(self, brief: ResearchBrief) -> str:
        return brief.topic.strip()

    def _expanded_time_hint(self, brief: ResearchBrief) -> str:
        years = [
            year
            for year in [
                brief.focal_year,
                self._year_from_date(brief.time_start),
                self._year_from_date(brief.time_end),
            ]
            if year
        ]
        unique_years: list[str] = []
        for year in years:
            if year not in unique_years:
                unique_years.append(year)
        if len(unique_years) >= 2:
            return " ".join(unique_years)
        return self._time_hint(brief)

    def _expanded_query_time_phrase(self, brief: ResearchBrief) -> str:
        if brief.focal_year:
            return f'"{brief.focal_year}"'
        start_year = self._year_from_date(brief.time_start)
        end_year = self._year_from_date(brief.time_end)
        if start_year and end_year and start_year != end_year:
            return f'"{start_year}" "{end_year}"'
        return self._query_time_phrase(brief)

    def _compact_query_phrase(self, value: str, *, max_words: int) -> str:
        if not value:
            return ""
        words = [part.strip(" ,.;:()[]{}\"'") for part in value.split()]
        compacted: list[str] = []
        seen: set[str] = set()
        for word in words:
            if not word:
                continue
            lowered = word.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            compacted.append(word)
            if len(compacted) >= max_words:
                break
        return " ".join(compacted)

    def _quoted_domain_phrase(self, brief: ResearchBrief) -> str:
        compacted = self._compact_query_phrase(" ".join(brief.domain_hints or []), max_words=2)
        if not compacted:
            return ""
        if len(compacted.split()) >= 2:
            return f'"{compacted}"'
        return compacted

    def _domain_tail_phrase(self, brief: ResearchBrief) -> str:
        raw = " ".join(brief.domain_hints or [])
        if not raw:
            return ""
        words = [
            part.strip(" ,.;:()[]{}\"'") for part in raw.split() if part.strip(" ,.;:()[]{}\"'")
        ]
        if len(words) <= 2:
            return ""
        return self._compact_query_phrase(" ".join(words[2:]), max_words=4)

    def _year_from_date(self, value: str | None) -> str | None:
        if not value:
            return None
        match = re.search(r"\b(\d{4})\b", value)
        return match.group(1) if match else None

    def _target_year_range(self, brief: ResearchBrief) -> tuple[int | None, int | None]:
        start = self._year_from_date(brief.time_start)
        end = self._year_from_date(brief.time_end)
        if start and end:
            return int(start), int(end)
        if brief.focal_year and brief.focal_year.isdigit():
            year = int(brief.focal_year)
            return year, year
        return (int(start), int(start)) if start and start.isdigit() else (None, None)

    def _years_mentioned(self, text: str) -> set[str]:
        return set(re.findall(r"\b((?:19|20)\d{2})\b", text))

    def _year_from_brief(self, brief: ResearchBrief) -> str | None:
        return (
            brief.focal_year
            or self._year_from_date(brief.time_start)
            or self._year_from_date(brief.time_end)
        )

    def _core_year_range(self, brief: ResearchBrief) -> tuple[int | None, int | None]:
        target_year = self._target_year(brief)
        if target_year is not None:
            return target_year - 5, target_year + 5
        return self._target_year_range(brief)

    def _historical_year_range(self, brief: ResearchBrief) -> tuple[int | None, int | None]:
        core_start, _ = self._core_year_range(brief)
        if core_start is None:
            return None, None
        return core_start - 50, core_start - 1

    def _future_year_range(self, brief: ResearchBrief) -> tuple[int | None, int | None]:
        _, core_end = self._core_year_range(brief)
        if core_end is None:
            return None, None
        return core_end + 1, core_end + 10

    def _core_year_tokens(self, brief: ResearchBrief) -> set[str]:
        start, end = self._core_year_range(brief)
        if start is None or end is None:
            year = self._year_from_brief(brief)
            return {year} if year else set()
        return {str(year) for year in range(start, end + 1)}

    def _contextual_year_tokens(self, brief: ResearchBrief) -> set[str]:
        historical_start, historical_end = self._historical_year_range(brief)
        core_start, core_end = self._core_year_range(brief)
        years: set[str] = set()
        if historical_start is not None and historical_end is not None:
            years.update(str(year) for year in range(historical_start, historical_end + 1))
        if core_start is not None and core_end is not None:
            years.update(str(year) for year in range(core_start, core_end + 1))
        if not years:
            year = self._year_from_brief(brief)
            if year:
                years.add(year)
        return years

    def _requested_year_tokens(self, brief: ResearchBrief) -> set[str]:
        return self._core_year_tokens(brief)

    def _era_band_for_year(self, year: int | None, brief: ResearchBrief) -> str:
        if year is None:
            return "unknown"
        core_start, core_end = self._core_year_range(brief)
        historical_start, historical_end = self._historical_year_range(brief)
        future_start, future_end = self._future_year_range(brief)
        if core_start is not None and core_end is not None and core_start <= year <= core_end:
            return "core"
        if (
            historical_start is not None
            and historical_end is not None
            and historical_start <= year <= historical_end
        ):
            return "historical"
        if (
            future_start is not None
            and future_end is not None
            and future_start <= year <= future_end
        ):
            return "future"
        if future_end is not None and year > future_end:
            return "distant_future"
        if historical_start is not None and year < historical_start:
            return "distant_past"
        return "unknown"

    def _text_mentions_requested_time(self, text: str, brief: ResearchBrief) -> bool:
        if not text:
            return False
        return any(year in text for year in self._core_year_tokens(brief))

    def _text_mentions_contextual_time(self, text: str, brief: ResearchBrief) -> bool:
        if not text:
            return False
        return any(year in text for year in self._contextual_year_tokens(brief))

    def _has_specific_date_anchor(self, text: str) -> bool:
        if not text:
            return False
        return bool(
            re.search(r"\b(?:19|20)\d{2}[-/](?:0?[1-9]|1[0-2])[-/](?:0?[1-9]|[12]\d|3[01])\b", text)
            or re.search(
                r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\.?\s+\d{1,2},?\s+(?:19|20)\d{2}\b",
                text,
                flags=re.IGNORECASE,
            )
        )

    def _url_has_strong_query_identity(self, url: str) -> bool:
        parsed = urlparse(self._canonical_url(url))
        if not parsed.query:
            return False
        for key, value in parse_qsl(parsed.query, keep_blank_values=True):
            lowered = key.lower()
            if lowered in {"id", "item", "doc", "title", "set", "page"} and len(value) >= 12:
                return True
            if self._has_specific_date_anchor(value):
                return True
        return False

    def _finding_text_has_time_anchor(
        self,
        title: str,
        snippet: str,
        excerpt: str,
        brief: ResearchBrief,
    ) -> bool:
        return self._text_mentions_contextual_time(
            " ".join(filter(None, [title, snippet, excerpt])), brief
        )

    def _finding_has_requested_time_anchor(
        self, finding: ResearchFinding, brief: ResearchBrief
    ) -> bool:
        return self._finding_text_has_time_anchor(
            finding.title,
            finding.snippet_text,
            finding.page_excerpt or "",
            brief,
        )

    def _candidate_stage_sentences(self, text: str) -> list[str]:
        if not text:
            return []
        candidates = [
            fragment.strip()
            for fragment in re.split(r"(?<=[.!?])\s+|\n+", text)
            if fragment and fragment.strip()
        ]
        return [sentence for sentence in candidates if self._is_stage_sentence_usable(sentence)]

    def _candidate_secondary_stage_sentences(self, text: str) -> list[str]:
        if not text:
            return []
        candidates = [
            fragment.strip()
            for fragment in re.split(r"(?<=[.!?])\s+|\n+", text)
            if fragment and fragment.strip()
        ]
        return [
            sentence
            for sentence in candidates
            if self._is_secondary_stage_sentence_usable(sentence)
        ]

    def _is_stage_sentence_usable(self, sentence: str) -> bool:
        cleaned = " ".join(sentence.split()).strip(" -")
        lower = cleaned.lower()
        if len(cleaned) < 55 or len(cleaned.split()) < 8:
            return False
        if len(cleaned) > 320:
            return False
        if cleaned.endswith((":", "—", "-", "“", '"')):
            return False
        if lower.startswith(
            (
                "after defining",
                "this paper",
                "this article",
                "this essay",
                "this project",
                "in this paper",
            )
        ):
            return False
        if any(pattern in lower for pattern in _PROMO_TEXT_PATTERNS):
            return False
        if any(pattern in lower for pattern in _VAGUE_STAGE_PATTERNS):
            return False
        if cleaned.count('"') % 2 == 1 and not cleaned.endswith('"'):
            return False
        return True

    def _is_secondary_stage_sentence_usable(self, sentence: str) -> bool:
        cleaned = " ".join(sentence.split()).strip(" -")
        lower = cleaned.lower()
        if len(cleaned) < 34 or len(cleaned.split()) < 5:
            return False
        if len(cleaned) > 220:
            return False
        if cleaned.endswith((":", "—", "-", "“", '"')):
            return False
        if lower.startswith(
            (
                "after defining",
                "this paper",
                "this article",
                "this essay",
                "this project",
                "in this paper",
            )
        ):
            return False
        if any(pattern in lower for pattern in _PROMO_TEXT_PATTERNS):
            return False
        if any(pattern in lower for pattern in _VAGUE_STAGE_PATTERNS):
            return False
        if cleaned.count('"') % 2 == 1 and not cleaned.endswith('"'):
            return False
        return bool(
            self._stage_anchor_type(cleaned) != "other"
            or re.search(r"\b(19|20)\d{2}\b", cleaned)
            or self._has_specific_date_anchor(cleaned)
        )

    def _stage_sentence_anchor_score(self, sentence: str, brief: ResearchBrief) -> float:
        cleaned = " ".join(sentence.split()).strip()
        score = 0.0
        if self._text_mentions_requested_time(cleaned, brief):
            score += 0.22
        elif self._text_mentions_contextual_time(cleaned, brief):
            score += 0.1
        if self._has_specific_date_anchor(cleaned):
            score += 0.12
        concreteness = self._concreteness_score(title="", snippet=cleaned, excerpt="", brief=brief)
        if concreteness >= 0.16:
            score += 0.12
            if self._text_mentions_contextual_time(cleaned, brief):
                score += 0.06
        if re.search(r"\b(19|20)\d{2}\b", cleaned):
            score += 0.08
        return score

    def _stage_anchor_type(self, sentence: str) -> str:
        lower = sentence.lower()
        if re.search(r"\b(19|20)\d{2}\b", sentence) or self._has_specific_date_anchor(sentence):
            return "time"
        if any(
            term in lower
            for term in (
                "wicker park",
                "south side",
                "north side",
                "chicago",
                "neighborhood",
                "street",
            )
        ):
            return "place"
        if any(
            term in lower
            for term in ("vinyl", "cdj", "turntable", "mixtape", "record pool", "gear", "format")
        ):
            return "gear"
        if any(term in lower for term in ("radio", "magazine", "press", "feature", "coverage")):
            return "media"
        if any(
            term in lower
            for term in (
                "residency",
                "flyer",
                "door policy",
                "workflow",
                "practice",
                "routine",
                "promoter",
            )
        ):
            return "practice"
        if re.search(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2}\b", sentence):
            return "person"
        return "other"

    def _stage_sentence_score(
        self, sentence: str, brief: ResearchBrief, finding: ResearchFinding
    ) -> float:
        cleaned = " ".join(sentence.split()).strip()
        score = self._stage_sentence_anchor_score(cleaned, brief)
        if re.search(r"\b(19|20)\d{2}\b", cleaned):
            score += 0.1
        if re.search(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3}\b", cleaned):
            score += 0.12
        if re.search(r"\b\d+\b", cleaned):
            score += 0.08
        if self._concreteness_score(title="", snippet=cleaned, excerpt="", brief=brief) >= 0.16:
            score += 0.12
        if len(cleaned.split()) >= 12:
            score += 0.08
        if cleaned.endswith((".", "!", "?")):
            score += 0.05
        if finding.publisher and finding.publisher.lower() not in cleaned.lower():
            score += 0.02
        if cleaned.lower().startswith(
            ("after defining", "this paper", "this article", "in this paper")
        ):
            score -= 0.25
        if any(pattern in cleaned.lower() for pattern in _PROMO_TEXT_PATTERNS):
            score -= 0.25
        if any(pattern in cleaned.lower() for pattern in _VAGUE_STAGE_PATTERNS):
            score -= 0.15
        return score

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
            return not any(
                host == domain or host.endswith(f".{domain}") for domain in policy.allow_domains
            )
        return False

    def _record_failure(self, telemetry: ResearchRunTelemetry, category: str) -> None:
        telemetry.fetch_failures_by_category[category] = (
            telemetry.fetch_failures_by_category.get(category, 0) + 1
        )

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
