from __future__ import annotations

from pathlib import Path
from typing import Iterable

from source_aware_worldbuilding.adapters.file_backed import (
    FileCandidateStore,
    FileEvidenceStore,
    FileExtractionRunStore,
    FileResearchFindingStore,
    FileResearchProgramStore,
    FileResearchRunStore,
    FileSourceDocumentStore,
    FileSourceStore,
    FileTextUnitStore,
)
from source_aware_worldbuilding.adapters.web_research_scout import ResearchScoutRegistry
from source_aware_worldbuilding.domain.enums import (
    ResearchCoverageStatus,
    ResearchFetchOutcome,
    ResearchFindingDecision,
    ResearchFindingReason,
)
from source_aware_worldbuilding.domain.models import (
    ResearchBrief,
    ResearchCuratedInput,
    ResearchExecutionPolicy,
    ResearchFetchedPage,
    ResearchFinding,
    ResearchSemanticMatch,
    ResearchSemanticResult,
    ResearchProgramCreateRequest,
    ResearchRunRequest,
    ResearchScoutCapabilities,
    ResearchSearchHit,
    SourceDocumentRecord,
    SourceRecord,
)
from source_aware_worldbuilding.services.ingestion import IngestionService
from source_aware_worldbuilding.services.normalization import NormalizationService
from source_aware_worldbuilding.services.research import ResearchService


class DummyCorpus:
    def pull_sources(self):
        raise AssertionError("staged research extraction should not need to pull external sources")

    def discover_source_documents(self, sources):
        return []

    def pull_text_units(self, sources):
        return []

    def pull_sources_by_item_keys(self, item_keys):
        return []

    def create_text_source(self, request):
        raise AssertionError("research staging bypasses corpus writes")

    def create_url_source(self, request):
        raise AssertionError("research staging bypasses corpus writes")

    def create_file_source(self, **kwargs):
        raise AssertionError("research staging bypasses corpus writes")


class FakeScout:
    adapter_id = "web_open"
    capabilities = ResearchScoutCapabilities(
        supports_search=True,
        supports_fetch=True,
        supports_text_inputs=False,
        supports_robots=True,
        supports_domain_policy=True,
    )

    def __init__(self) -> None:
        self.search_queries: list[str] = []

    def search(self, query: str, *, limit: int = 5) -> list[ResearchSearchHit]:
        self.search_queries.append(query)
        lowered = query.lower()
        if "participants" in lowered:
            return [
                ResearchSearchHit(
                    query=query,
                    url="https://archive.example.org/people-profile",
                    title="Scene Participants Oral History",
                    snippet="Participants described the local scene and key figures in detail.",
                    rank=1,
                )
            ]
        if "locations" in lowered:
            return [
                ResearchSearchHit(
                    query=query,
                    url="https://archive.example.org/people-profile",
                    title="Scene Participants Oral History",
                    snippet="Duplicate archive record describing the same scene.",
                    rank=1,
                )
            ]
        if "coverage" in lowered:
            return [
                ResearchSearchHit(
                    query=query,
                    url="https://news.example.com/feature",
                    title="Feature Coverage of the Local Scene",
                    snippet="A magazine feature covered the local scene with named venues.",
                    rank=1,
                )
            ]
        return []

    def fetch_page(self, url: str) -> ResearchFetchedPage:
        if "people-profile" in url:
            return ResearchFetchedPage(
                url=url,
                final_url=url,
                title="Scene Participants Oral History",
                publisher="City Archive",
                published_at="2003-05-12",
                source_type="archive",
                text=(
                    "Participants described the scene, named venues, and documented the local habits. "
                    "The 2003 oral history mentions equipment, promoters, and neighborhood changes."
                ),
            )
        return ResearchFetchedPage(
            url=url,
            final_url=url,
            title="Feature Coverage of the Local Scene",
            publisher="Regional News",
            published_at="2004-02-18",
            source_type="news",
            text=(
                "The feature described local venues, radio habits, prices, and regional context "
                "for the broader scene in the era."
            ),
        )

    def allows_fetch(self, url: str, *, user_agent: str) -> bool | None:
        _ = user_agent
        return "blocked" not in url


class FakeResearchSemantic:
    def __init__(self, *, fallback_reason: str | None = None, match_feature: bool = False) -> None:
        self.fallback_reason = fallback_reason
        self.match_feature = match_feature
        self.upserts: list[tuple[str, list[str]]] = []
        self.searches: list[tuple[str, list[str]]] = []

    def upsert_findings(self, findings: list[ResearchFinding], *, run_id: str) -> int:
        self.upserts.append((run_id, [item.finding_id for item in findings]))
        if self.fallback_reason:
            raise RuntimeError(self.fallback_reason)
        return len(findings)

    def search_similar_findings(
        self,
        finding: ResearchFinding,
        allowed_finding_ids: list[str],
        *,
        run_id: str,
        limit: int = 3,
    ) -> ResearchSemanticResult:
        _ = limit
        self.searches.append((run_id, allowed_finding_ids))
        if self.fallback_reason:
            return ResearchSemanticResult(fallback_used=True, fallback_reason=self.fallback_reason)
        matches: list[ResearchSemanticMatch] = []
        if self.match_feature and (
            "Feature Coverage" in finding.title or "Scene Magazine Overview" in finding.title
        ) and allowed_finding_ids:
            matches.append(
                ResearchSemanticMatch(
                    finding_id=allowed_finding_ids[0],
                    similarity=0.93,
                    title="Scene Participants Oral History",
                    canonical_url="https://archive.example.org/people-profile",
                    decision="accepted",
                )
            )
        return ResearchSemanticResult(matches=matches)


def build_service(
    data_dir: Path,
    scout: FakeScout | Iterable[FakeScout],
    *,
    default_adapter_id: str | None = None,
    research_semantic: FakeResearchSemantic | None = None,
) -> ResearchService:
    source_store = FileSourceStore(data_dir)
    source_document_store = FileSourceDocumentStore(data_dir)
    text_unit_store = FileTextUnitStore(data_dir)
    normalization = NormalizationService(
        source_document_store=source_document_store,
        text_unit_store=text_unit_store,
    )
    ingestion = IngestionService(
        corpus=DummyCorpus(),
        extractor=__import__(
            "source_aware_worldbuilding.adapters.heuristic_extraction",
            fromlist=["HeuristicExtractionAdapter"],
        ).HeuristicExtractionAdapter(),
        source_store=source_store,
        text_unit_store=text_unit_store,
        source_document_store=source_document_store,
        run_store=FileExtractionRunStore(data_dir),
        candidate_store=FileCandidateStore(data_dir),
        evidence_store=FileEvidenceStore(data_dir),
    )
    default_markdown = (Path(__file__).resolve().parents[1] / "docs" / "research" / "default_program.md").read_text(
        encoding="utf-8"
    )
    scouts = list(scout) if not isinstance(scout, FakeScout) else [scout]
    return ResearchService(
        scout_registry=ResearchScoutRegistry(
            scouts,
            default_adapter_id=default_adapter_id or scouts[0].adapter_id,
        ),
        run_store=FileResearchRunStore(data_dir),
        finding_store=FileResearchFindingStore(data_dir),
        program_store=FileResearchProgramStore(data_dir),
        source_store=source_store,
        source_document_store=source_document_store,
        normalization_service=normalization,
        ingestion_service=ingestion,
        research_semantic=research_semantic or FakeResearchSemantic(),
        default_program_markdown=default_markdown,
        default_execution_policy=ResearchExecutionPolicy(),
        default_adapter_id=default_adapter_id or scouts[0].adapter_id,
        research_user_agent="SourceboundResearchScout/Test",
        semantic_duplicate_threshold=0.9,
        semantic_novelty_floor=0.1,
        semantic_rerank_weight=0.05,
    )


def test_research_brief_expansion_and_query_generation_are_generic(temp_data_dir: Path) -> None:
    scout = FakeScout()
    service = build_service(temp_data_dir, scout)

    detail = service.run_research(
        ResearchRunRequest(
            brief=ResearchBrief(
                topic="urban transit strikes",
                time_start="1910",
                time_end="1912",
                locale="Glasgow",
                desired_facets=["people", "media_culture"],
                max_queries=2,
                max_results_per_query=1,
            )
        )
    )

    assert [facet.facet_id for facet in detail.run.facets] == ["people", "media_culture"]
    assert len(scout.search_queries) == 2
    assert all("urban transit strikes" in query for query in scout.search_queries)
    assert all("glasgow" in query.lower() for query in scout.search_queries)
    assert all("1910 1912" in query for query in scout.search_queries)


def test_research_run_allows_same_source_to_support_multiple_facets(temp_data_dir: Path) -> None:
    service = build_service(temp_data_dir, FakeScout())

    detail = service.run_research(
        ResearchRunRequest(
            brief=ResearchBrief(
                topic="local music scenes",
                focal_year="2003",
                desired_facets=["people", "places"],
                max_queries=2,
                max_results_per_query=1,
            )
        )
    )

    accepted = [item for item in detail.findings if item.decision == ResearchFindingDecision.ACCEPTED]
    rejected = [item for item in detail.findings if item.decision == ResearchFindingDecision.REJECTED]
    assert len(accepted) == 2
    assert len(rejected) == 0
    assert accepted[0].provenance is not None
    assert accepted[0].provenance.acceptance_reason == ResearchFindingReason.ACCEPTED_QUALITY_THRESHOLD


def test_stage_and_extract_flow_creates_text_backed_sources_and_candidates(
    temp_data_dir: Path,
) -> None:
    service = build_service(temp_data_dir, FakeScout())
    detail = service.run_research(
        ResearchRunRequest(
            brief=ResearchBrief(
                topic="2003 DJ scene",
                focal_year="2003",
                locale="Chicago",
                desired_facets=["people", "media_culture"],
                max_queries=2,
                max_results_per_query=1,
                max_per_facet=1,
            )
        )
    )

    stage_result = service.stage_run(detail.run.run_id)
    assert len(stage_result.staged_source_ids) == 1
    sources = FileSourceStore(temp_data_dir).list_sources()
    assert {item.external_source for item in sources} == {"research_scout"}
    documents = FileSourceDocumentStore(temp_data_dir).list_source_documents()
    assert all(item.document_kind == "manual_text" for item in documents)
    assert all("Query:" not in (item.raw_text or "") for item in documents)
    assert all("Topic:" not in (item.raw_text or "") for item in documents)

    unrelated_source = FileSourceStore(temp_data_dir)
    unrelated_source.save_sources(
        [
            SourceRecord(
                source_id="unrelated-source",
                external_source="manual",
                title="Unrelated source",
                source_type="note",
                sync_status="awaiting_text_extraction",
            )
        ]
    )
    FileSourceDocumentStore(temp_data_dir).save_source_documents(
        [
            SourceDocumentRecord(
                document_id="unrelated-doc",
                source_id="unrelated-source",
                document_kind="manual_text",
                ingest_status="imported",
                raw_text_status="ready",
                claim_extraction_status="queued",
                locator="manual",
                raw_text="Unrelated notes that should not be extracted by the research run.",
            )
        ]
    )

    extract_result = service.extract_run(detail.run.run_id)
    assert extract_result.normalization["text_unit_count"] >= 1
    assert extract_result.extraction.candidates
    assert extract_result.stage_result.run.extraction_run_id is not None
    assert {item.source_id for item in extract_result.extraction.evidence} <= set(stage_result.staged_source_ids)


def test_program_creation_and_empty_runs_surface_coverage_gaps(temp_data_dir: Path) -> None:
    class EmptyScout(FakeScout):
        def search(self, query: str, *, limit: int = 5) -> list[ResearchSearchHit]:
            self.search_queries.append(query)
            return []

    service = build_service(temp_data_dir, EmptyScout())
    custom = service.create_program(
        ResearchProgramCreateRequest(
            name="Archive First",
            markdown="# Archive First\nPrefer archives.",
            default_facets=["people"],
            preferred_source_classes=["archive"],
        )
    )

    programs = service.list_programs()
    assert any(item.program_id == "default-generic" for item in programs)
    assert any(item.program_id == custom.program_id for item in programs)

    detail = service.run_research(
        ResearchRunRequest(
            program_id=custom.program_id,
            brief=ResearchBrief(topic="cinema ticket pricing", focal_year="1978", max_queries=1),
        )
    )

    assert detail.run.warnings
    assert any("no accepted findings" in item.lower() or "coverage gap" in item.lower() for item in detail.run.warnings)
    assert detail.facet_coverage
    assert detail.facet_coverage[0].coverage_status == ResearchCoverageStatus.EMPTY
    assert detail.facet_coverage[0].coverage_gap_reason == "no_hits"


def test_research_run_continues_when_one_fetch_fails(temp_data_dir: Path) -> None:
    class PartiallyFailingScout(FakeScout):
        def search(self, query: str, *, limit: int = 5) -> list[ResearchSearchHit]:
            lowered = query.lower()
            if "participants" in lowered:
                return [
                    ResearchSearchHit(
                        query=query,
                        url="https://archive.example.org/bad-hit",
                        title="Broken Archive Entry",
                        snippet="This hit will fail during fetch.",
                        rank=1,
                    ),
                    ResearchSearchHit(
                        query=query,
                        url="https://archive.example.org/people-profile",
                        title="Scene Participants Oral History",
                        snippet="Participants described the local scene and key figures in detail.",
                        rank=2,
                    ),
                ]
            return []

        def fetch_page(self, url: str) -> ResearchFetchedPage:
            if "bad-hit" in url:
                raise RuntimeError("network timeout")
            return super().fetch_page(url)

    service = build_service(temp_data_dir, PartiallyFailingScout())
    detail = service.run_research(
        ResearchRunRequest(
            brief=ResearchBrief(
                topic="2003 DJ scene",
                focal_year="2003",
                desired_facets=["people"],
                max_queries=1,
                max_results_per_query=2,
            )
        )
    )

    accepted = [item for item in detail.findings if item.decision == ResearchFindingDecision.ACCEPTED]
    rejected = [item for item in detail.findings if item.decision == ResearchFindingDecision.REJECTED]
    assert len(accepted) == 1
    assert any((item.rejection_reason or "").startswith("Fetch failed:") for item in rejected)
    assert any("fetch failed" in log.lower() for log in detail.run.logs)
    failed = next(item for item in rejected if (item.rejection_reason or "").startswith("Fetch failed:"))
    assert failed.provenance is not None
    assert failed.provenance.fetch_outcome == ResearchFetchOutcome.FAILED
    assert failed.provenance.rejection_reason == ResearchFindingReason.REJECTED_FETCH_FAILURE
    assert failed.provenance.fetch_error_category == "runtime_error"


def test_research_run_uses_curated_text_inputs_without_search(temp_data_dir: Path) -> None:
    class CuratedScout(FakeScout):
        adapter_id = "curated_inputs"
        capabilities = ResearchScoutCapabilities(
            supports_search=False,
            supports_fetch=True,
            supports_text_inputs=True,
            supports_robots=True,
            supports_domain_policy=True,
        )

        def search(self, query: str, *, limit: int = 5) -> list[ResearchSearchHit]:
            raise AssertionError("curated text inputs should not invoke search")

        def fetch_page(self, url: str) -> ResearchFetchedPage:
            raise AssertionError("curated text inputs should not fetch pages")

    service = build_service(temp_data_dir, CuratedScout(), default_adapter_id="curated_inputs")
    detail = service.run_research(
        ResearchRunRequest(
            brief=ResearchBrief(
                topic="local music scenes",
                focal_year="2003",
                adapter_id="curated_inputs",
                curated_inputs=[
                    ResearchCuratedInput(
                        input_type="text",
                        title="Curated oral history",
                        text="The local music scene promoter described neighborhood venues, mixtapes, and weekly residencies.",
                        source_type="archive",
                        published_at="2003-04-01",
                    )
                ],
            )
        )
    )

    assert detail.run.status == "completed"
    assert detail.run.accepted_count == 1
    assert detail.findings[0].source_type == "archive"
    assert detail.findings[0].provenance is not None
    assert detail.findings[0].provenance.fetch_outcome == ResearchFetchOutcome.CURATED_TEXT
    assert detail.findings[0].provenance.originating_query == "curated_input"


def test_research_run_marks_failed_policy_for_impossible_adapter_request(temp_data_dir: Path) -> None:
    class CuratedScout(FakeScout):
        adapter_id = "curated_inputs"
        capabilities = ResearchScoutCapabilities(
            supports_search=False,
            supports_fetch=True,
            supports_text_inputs=True,
            supports_robots=True,
            supports_domain_policy=True,
        )

    service = build_service(temp_data_dir, CuratedScout(), default_adapter_id="curated_inputs")
    detail = service.run_research(
        ResearchRunRequest(
            brief=ResearchBrief(
                topic="local music scenes",
                focal_year="2003",
                adapter_id="curated_inputs",
            )
        )
    )

    assert detail.run.status == "failed_policy"
    assert "requires curated inputs" in (detail.run.error or "").lower()


def test_title_normalization_strips_boilerplate_and_publisher_suffixes(temp_data_dir: Path) -> None:
    service = build_service(temp_data_dir, FakeScout())

    assert service._normalize_title("Scene Participants Oral History | City Archive") == "scene participants oral history"
    assert service._normalize_title("Feature Coverage of the Local Scene - Regional News") == "feature coverage of the local scene"
    assert service._normalize_title("2003 DJ Scene: Official Site") == "2003 dj scene"


def test_url_canonicalization_collapses_tracking_and_default_page_variants(temp_data_dir: Path) -> None:
    service = build_service(temp_data_dir, FakeScout())

    first = service._canonical_url("https://www.example.org/index.html?utm_source=test&id=5")
    second = service._canonical_url("https://example.org/?id=5&utm_medium=email")
    third = service._canonical_url("https://example.org/default.aspx?id=5")

    assert first == "https://example.org/?id=5"
    assert second == "https://example.org/?id=5"
    assert third == "https://example.org/?id=5"


def test_duplicate_reason_distinguishes_canonical_url_and_cross_host_title_cases(temp_data_dir: Path) -> None:
    service = build_service(temp_data_dir, FakeScout())
    accepted = {
        "sig-a": {
            "facet_id": "people",
            "title": "Scene Participants Oral History",
            "normalized_title": "scene participants oral history",
            "host": "archive.example.org",
            "canonical_url": "https://archive.example.org/people-profile",
            "path": "/people-profile",
        }
    }

    same_url_reason, same_url_rule = service._duplicate_reason(
        "sig-b",
        "https://archive.example.org/people-profile?utm_source=test",
        "Scene Participants Oral History | City Archive",
        "people",
        accepted,
        0.9,
    )
    assert same_url_rule == "same_canonical_url_different_title"
    assert same_url_reason is not None

    accepted["sig-c"] = {
        "facet_id": "people",
        "title": "Rare Local Record Pool Newsletter 2003 Chicago",
        "normalized_title": "rare local record pool newsletter 2003 chicago",
        "host": "archive.example.org",
        "canonical_url": "https://archive.example.org/",
        "path": "/",
    }
    cross_host_reason, cross_host_rule = service._duplicate_reason(
        "sig-d",
        "https://mirror.example.net/",
        "Rare Local Record Pool Newsletter 2003 Chicago",
        "people",
        accepted,
        0.9,
    )
    assert cross_host_rule == "same_title_cross_host"
    assert cross_host_reason is not None

    distinct_reason, distinct_rule = service._duplicate_reason(
        "sig-e",
        "https://mag.example.net/deep/report",
        "Rare Local Record Pool Newsletter 2003 Chicago",
        "people",
        accepted,
        0.9,
    )
    assert distinct_reason is None
    assert distinct_rule is None


def test_research_run_tracks_host_caps_and_domain_policy(temp_data_dir: Path) -> None:
    class HostCapScout(FakeScout):
        def search(self, query: str, *, limit: int = 5) -> list[ResearchSearchHit]:
            return [
                ResearchSearchHit(
                    query=query,
                    url="https://archive.example.org/people-profile?utm_source=test",
                    title="Scene Participants Oral History",
                    snippet="Participants described the local scene and key figures in detail.",
                    rank=1,
                ),
                ResearchSearchHit(
                    query=query,
                    url="https://archive.example.org/people-profile?utm_medium=email",
                    title="Scene Participants Oral History | City Archive",
                    snippet="Duplicate archive record describing the same scene.",
                    rank=2,
                ),
                ResearchSearchHit(
                    query=query,
                    url="https://blocked.example.org/scene",
                    title="Blocked domain",
                    snippet="Should be blocked by deny domains.",
                    rank=3,
                ),
            ]

    service = build_service(temp_data_dir, HostCapScout())
    detail = service.run_research(
        ResearchRunRequest(
            brief=ResearchBrief(
                topic="local music scenes",
                focal_year="2003",
                desired_facets=["people"],
                max_queries=1,
                max_results_per_query=3,
                execution_policy=ResearchExecutionPolicy(
                    per_host_fetch_cap=1,
                    deny_domains=["blocked.example.org"],
                ),
            )
        )
    )

    assert detail.run.telemetry.per_host_fetch_counts["archive.example.org"] == 1
    assert detail.run.telemetry.skipped_host_counts.get("archive.example.org", 0) == 0
    assert detail.run.telemetry.blocked_by_policy_count == 1
    assert detail.facet_coverage[0].duplicate_rejections == 1


def test_research_run_respects_robots_and_marks_degraded_fallback(temp_data_dir: Path) -> None:
    class RobotsScout(FakeScout):
        def search(self, query: str, *, limit: int = 5) -> list[ResearchSearchHit]:
            return [
                ResearchSearchHit(
                    query=query,
                    url="https://blocked.example.org/scene",
                    title="Blocked by robots",
                    snippet="This should never fetch.",
                    rank=1,
                ),
                ResearchSearchHit(
                    query=query,
                    url="https://archive.example.org/people-profile",
                    title="Scene Participants Oral History",
                    snippet="Participants described the local scene and key figures in detail.",
                    rank=2,
                ),
            ]

        def allows_fetch(self, url: str, *, user_agent: str) -> bool | None:
            _ = user_agent
            if "blocked.example.org" in url:
                return False
            return None

    service = build_service(temp_data_dir, RobotsScout())
    detail = service.run_research(
        ResearchRunRequest(
            brief=ResearchBrief(
                topic="local music scenes",
                focal_year="2003",
                desired_facets=["people"],
                max_queries=1,
                max_results_per_query=2,
            )
        )
    )

    assert detail.run.status == "degraded_fallback"
    assert detail.run.telemetry.blocked_by_robots_count == 1
    assert "robots_unavailable" in detail.run.telemetry.fallback_flags


def test_research_run_can_complete_partially_when_time_budget_is_exhausted(
    temp_data_dir: Path,
    monkeypatch,
) -> None:
    service = build_service(temp_data_dir, FakeScout())
    calls = {"count": 0}

    def fake_deadline(_started, _policy):
        calls["count"] += 1
        return calls["count"] > 1

    monkeypatch.setattr(service, "_deadline_exceeded", fake_deadline)
    detail = service.run_research(
        ResearchRunRequest(
            brief=ResearchBrief(
                topic="local music scenes",
                focal_year="2003",
                desired_facets=["people", "media_culture"],
                max_queries=2,
                max_results_per_query=1,
            )
        )
    )

    assert detail.run.status == "completed_partial"
    assert any("total fetch time" in log.lower() for log in detail.run.logs)


def test_research_run_derives_quality_threshold_rejection_and_facet_coverage(
    temp_data_dir: Path,
) -> None:
    service = build_service(temp_data_dir, FakeScout())
    program = service.create_program(
        ResearchProgramCreateRequest(
            name="Strict Quality",
            markdown="# Strict\nHigh quality threshold.",
            default_facets=["people", "media_culture"],
            quality_threshold=0.95,
        )
    )

    detail = service.run_research(
        ResearchRunRequest(
            program_id=program.program_id,
            brief=ResearchBrief(
                topic="2003 DJ scene",
                focal_year="2003",
                desired_facets=["people", "media_culture"],
                max_queries=2,
                max_results_per_query=1,
            )
        )
    )

    assert detail.findings
    assert all(item.decision == ResearchFindingDecision.REJECTED for item in detail.findings)
    assert all(item.provenance is not None for item in detail.findings)
    assert all(
        item.provenance.rejection_reason == ResearchFindingReason.REJECTED_QUALITY_THRESHOLD
        for item in detail.findings
    )
    assert all(item.provenance.scoring.threshold_passed is False for item in detail.findings)
    assert {item.coverage_gap_reason for item in detail.facet_coverage} == {"threshold_rejections"}
    assert all(item.threshold_rejections >= 1 for item in detail.facet_coverage)


def test_research_run_detail_handles_older_findings_without_provenance(temp_data_dir: Path) -> None:
    service = build_service(temp_data_dir, FakeScout())
    detail = service.run_research(
        ResearchRunRequest(
            brief=ResearchBrief(
                topic="2003 DJ scene",
                focal_year="2003",
                desired_facets=["people"],
                max_queries=1,
                max_results_per_query=1,
            )
        )
    )

    store = FileResearchFindingStore(temp_data_dir)
    legacy = ResearchFinding(
        finding_id=detail.findings[0].finding_id,
        run_id=detail.findings[0].run_id,
        facet_id=detail.findings[0].facet_id,
        query=detail.findings[0].query,
        url=detail.findings[0].url,
        canonical_url=detail.findings[0].canonical_url,
        title=detail.findings[0].title,
        publisher=detail.findings[0].publisher,
        published_at=detail.findings[0].published_at,
        access_date=detail.findings[0].access_date,
        locator=detail.findings[0].locator,
        snippet_text=detail.findings[0].snippet_text,
        page_excerpt=detail.findings[0].page_excerpt,
        source_type=detail.findings[0].source_type,
        score=detail.findings[0].score,
        relevance_score=detail.findings[0].relevance_score,
        quality_score=detail.findings[0].quality_score,
        novelty_score=detail.findings[0].novelty_score,
        decision=detail.findings[0].decision,
        rejection_reason=detail.findings[0].rejection_reason,
        staged_source_id=detail.findings[0].staged_source_id,
        staged_document_id=detail.findings[0].staged_document_id,
        provenance=None,
    )
    store.update_finding(legacy)

    reloaded = service.get_run_detail(detail.run.run_id)
    assert reloaded is not None
    assert reloaded.findings[0].provenance is None
    assert reloaded.facet_coverage


def test_scoring_exposes_era_and_coverage_components(temp_data_dir: Path) -> None:
    service = build_service(temp_data_dir, FakeScout())
    brief = ResearchBrief(topic="2003 DJ scene", focal_year="2003", max_per_facet=2)
    facet = service._expand_facets(brief, service.list_programs()[0])[0]
    seed_run = service.run_research(
        ResearchRunRequest(
            brief=ResearchBrief(
                topic="seed",
                focal_year="2003",
                desired_facets=["people"],
                max_queries=1,
                max_results_per_query=1,
            )
        )
    ).run

    closer = service._build_finding(
        adapter_id="web_open",
        run=seed_run,
        brief=brief,
        facet=facet,
        query="test",
        hit=ResearchSearchHit(query="test", url="https://archive.example.org/close", title="Close source", rank=1),
        page=ResearchFetchedPage(
            url="https://archive.example.org/close",
            final_url="https://archive.example.org/close",
            title="Close source",
            published_at="2003-05-01",
            source_type="archive",
            text="Detailed scene account with dates, venues, and equipment.",
        ),
        fetch_outcome=ResearchFetchOutcome.FETCHED,
        fetch_status="fetched",
    )
    retrospective = service._build_finding(
        adapter_id="web_open",
        run=seed_run.model_copy(update={"run_id": "research-other"}),
        brief=brief,
        facet=facet,
        query="test",
        hit=ResearchSearchHit(query="test", url="https://archive.example.org/retro", title="Retrospective source", rank=2),
        page=ResearchFetchedPage(
            url="https://archive.example.org/retro",
            final_url="https://archive.example.org/retro",
            title="Retrospective source",
            published_at="2015-05-01",
            source_type="archive",
            text="Detailed scene account with dates, venues, and equipment.",
        ),
        fetch_outcome=ResearchFetchOutcome.FETCHED,
        fetch_status="fetched",
    )

    assert closer.provenance is not None
    assert retrospective.provenance is not None
    assert closer.provenance.scoring.era_score > retrospective.provenance.scoring.era_score
    assert closer.score > retrospective.score
    assert closer.provenance.scoring.coverage_score > 0
    assert closer.provenance.scoring.normalized_title is not None
    assert closer.provenance.scoring.canonical_host == "archive.example.org"


def test_year_anchor_and_landing_page_penalties_shape_scores(temp_data_dir: Path) -> None:
    service = build_service(temp_data_dir, FakeScout())
    brief = ResearchBrief(topic="Chicago DJ scene", focal_year="2003")
    facet = service._expand_facets(brief, service.list_programs()[0])[0]
    seed_run = service.run_research(
        ResearchRunRequest(
            brief=ResearchBrief(
                topic="seed",
                focal_year="2003",
                desired_facets=["people"],
                max_queries=1,
                max_results_per_query=1,
            )
        )
    ).run

    anchored = service._build_finding(
        adapter_id="web_open",
        run=seed_run,
        brief=brief,
        facet=facet,
        query="test",
        hit=ResearchSearchHit(
            query="test",
            url="https://archive.example.org/articles/chicago-2003",
            title="Chicago DJ scene in 2003",
            snippet="A 2003 club report covering mixers, flyers, and residencies.",
            rank=1,
        ),
        page=ResearchFetchedPage(
            url="https://archive.example.org/articles/chicago-2003",
            final_url="https://archive.example.org/articles/chicago-2003",
            title="Chicago DJ scene in 2003",
            published_at="2003-08-10",
            source_type="archive",
            text="A 2003 club report covering mixers, flyers, and residencies.",
        ),
        fetch_outcome=ResearchFetchOutcome.FETCHED,
        fetch_status="fetched",
    )
    landing = service._build_finding(
        adapter_id="web_open",
        run=seed_run.model_copy(update={"run_id": "research-landing"}),
        brief=brief,
        facet=facet,
        query="test",
        hit=ResearchSearchHit(
            query="test",
            url="https://archive.example.org/",
            title="Archive of Chicago DJ history",
            snippet="Listen to our documentary about the scene.",
            rank=2,
        ),
        page=ResearchFetchedPage(
            url="https://archive.example.org/",
            final_url="https://archive.example.org/",
            title="Archive of Chicago DJ history",
            published_at="2024-05-10",
            source_type="archive",
            text="Listen to our documentary about the scene.",
        ),
        fetch_outcome=ResearchFetchOutcome.FETCHED,
        fetch_status="fetched",
    )

    assert anchored.provenance is not None
    assert landing.provenance is not None
    assert anchored.provenance.scoring.era_score > landing.provenance.scoring.era_score
    assert anchored.score > landing.score


def test_facet_acceptance_reranks_before_accepting(temp_data_dir: Path) -> None:
    class RerankScout(FakeScout):
        def search(self, query: str, *, limit: int = 5) -> list[ResearchSearchHit]:
            return [
                ResearchSearchHit(
                    query=query,
                    url="https://guide.example.org/",
                    title="Guide to Chicago DJ history",
                    snippet="A modern overview of the scene.",
                    rank=1,
                ),
                ResearchSearchHit(
                    query=query,
                    url="https://archive.example.org/2003-scene-report",
                    title="Chicago DJ scene report from 2003",
                    snippet="A 2003 report on DJs, club flyers, and record pools.",
                    rank=2,
                ),
            ]

        def fetch_page(self, url: str) -> ResearchFetchedPage:
            if "guide.example.org" in url:
                return ResearchFetchedPage(
                    url=url,
                    final_url=url,
                    title="Guide to Chicago DJ history",
                    publisher="Guide Example",
                    published_at="2024-05-10",
                    source_type="web",
                    text="A modern overview of the scene. Listen to our podcast.",
                )
            return ResearchFetchedPage(
                url=url,
                final_url=url,
                title="Chicago DJ scene report from 2003",
                publisher="Scene Archive",
                published_at="2003-08-10",
                source_type="archive",
                text="In 2003, record pools, club flyers, and weekly residencies shaped the Chicago DJ scene.",
            )

    service = build_service(temp_data_dir, RerankScout())
    detail = service.run_research(
        ResearchRunRequest(
            brief=ResearchBrief(
                topic="Chicago DJ scene",
                focal_year="2003",
                desired_facets=["practices"],
                max_queries=1,
                max_results_per_query=2,
            )
        )
    )

    accepted = [item for item in detail.findings if item.decision == ResearchFindingDecision.ACCEPTED]
    assert len(accepted) == 1
    assert accepted[0].title == "Chicago DJ scene report from 2003"


def test_stage_text_prefers_factual_complete_sentences(temp_data_dir: Path) -> None:
    service = build_service(temp_data_dir, FakeScout())
    detail = service.run_research(
        ResearchRunRequest(
            brief=ResearchBrief(
                topic="Chicago DJ scene",
                focal_year="2003",
                desired_facets=["people"],
                max_queries=1,
                max_results_per_query=1,
            )
        )
    )
    finding = detail.findings[0].model_copy(
        update={
            "page_excerpt": (
                "Listen to our documentary about the scene. In 2003, DJs rotated between Wicker Park loft parties "
                "and South Side club residencies. Promoters enforced door policies to keep the parties safe."
            ),
            "snippet_text": "Read more about the scene. Photo by archive staff.",
        }
    )

    staged = service._build_staged_text(detail.run, finding)

    assert "Listen to our documentary" not in staged
    assert "Photo by" not in staged
    assert "In 2003, DJs rotated between Wicker Park loft parties" in staged


def test_best_excerpt_prefers_anchored_factual_sentences_over_boilerplate(temp_data_dir: Path) -> None:
    service = build_service(temp_data_dir, FakeScout())
    brief = ResearchBrief(topic="Chicago DJ scene", focal_year="2003")

    excerpt = service._best_excerpt(
        (
            'Retrieved from "https://example.org/archive". '
            "Read more about the exhibition. "
            "In 2003, DJs rotated between Wicker Park loft parties and South Side club residencies. "
            "Promoters posted weekly flyers and coordinated record pool drops for Friday sets."
        ),
        service._scoring_tokens("Chicago DJ scene", "people", "Chicago", "2003"),
        brief,
    )

    assert "Retrieved from" not in excerpt
    assert "Read more" not in excerpt
    assert "In 2003, DJs rotated between Wicker Park loft parties" in excerpt


def test_facet_coverage_derives_diagnostic_counts(temp_data_dir: Path) -> None:
    class MixedScout(FakeScout):
        def search(self, query: str, *, limit: int = 5) -> list[ResearchSearchHit]:
            return [
                ResearchSearchHit(
                    query=query,
                    url="https://archive.example.org/people-profile?utm_source=test",
                    title="Scene Participants Oral History",
                    snippet="Participants described the local scene and key figures in detail.",
                    rank=1,
                ),
                ResearchSearchHit(
                    query=query,
                    url="https://archive.example.org/people-profile?utm_medium=email",
                    title="Scene Participants Oral History | City Archive",
                    snippet="Duplicate archive record describing the same scene.",
                    rank=2,
                ),
                ResearchSearchHit(
                    query=query,
                    url="https://archive.example.org/failure",
                    title="Broken archive",
                    snippet="This one fails.",
                    rank=3,
                ),
            ]

        def fetch_page(self, url: str) -> ResearchFetchedPage:
            if "failure" in url:
                raise RuntimeError("boom")
            return super().fetch_page(url)

    service = build_service(temp_data_dir, MixedScout())
    detail = service.run_research(
        ResearchRunRequest(
            brief=ResearchBrief(
                topic="local music scenes",
                focal_year="2003",
                desired_facets=["people"],
                max_queries=1,
                max_results_per_query=3,
            )
        )
    )

    facet = detail.facet_coverage[0]
    assert facet.duplicate_rejections == 1
    assert facet.fetch_failures == 1
    assert facet.accepted_sources_by_type["archive"] == 1
    assert facet.diagnostic_summary == "target_met"


def test_curated_url_inputs_remain_no_search_optional_fetch(temp_data_dir: Path) -> None:
    calls = {"search": 0, "fetch": 0}

    class CuratedUrlScout(FakeScout):
        adapter_id = "curated_inputs"
        capabilities = ResearchScoutCapabilities(
            supports_search=False,
            supports_fetch=True,
            supports_text_inputs=True,
            supports_robots=True,
            supports_domain_policy=True,
        )

        def search(self, query: str, *, limit: int = 5) -> list[ResearchSearchHit]:
            calls["search"] += 1
            raise AssertionError("curated url mode should not search")

        def fetch_page(self, url: str) -> ResearchFetchedPage:
            calls["fetch"] += 1
            return super().fetch_page("https://archive.example.org/people-profile")

    service = build_service(temp_data_dir, CuratedUrlScout(), default_adapter_id="curated_inputs")
    detail = service.run_research(
        ResearchRunRequest(
            brief=ResearchBrief(
                topic="local music scenes",
                focal_year="2003",
                adapter_id="curated_inputs",
                curated_inputs=[
                    ResearchCuratedInput(
                        input_type="url",
                        url="https://archive.example.org/people-profile?utm_source=test",
                        title="Curated archive URL",
                    )
                ],
            )
        )
    )

    assert detail.run.accepted_count == 1
    assert calls["search"] == 0
    assert calls["fetch"] == 1


def test_specific_date_artifact_url_can_outrank_retrospective_timeline(temp_data_dir: Path) -> None:
    service = build_service(temp_data_dir, FakeScout())
    brief = ResearchBrief(topic="Chicago DJ scene", focal_year="2003")
    facet = service._expand_facets(brief, service.list_programs()[0])[0]
    seed_run = service.run_research(
        ResearchRunRequest(
            brief=ResearchBrief(
                topic="seed",
                focal_year="2003",
                desired_facets=["people"],
                max_queries=1,
                max_results_per_query=1,
            )
        )
    ).run

    artifact = service._build_finding(
        adapter_id="web_open",
        run=seed_run,
        brief=brief,
        facet=facet,
        query="test",
        hit=ResearchSearchHit(
            query="test",
            url="https://mix.example.org/w?title=2003-02-07_DJ_Set_Chicago",
            title="2003-02-07 DJ Set Chicago",
            snippet="2003-02-07 DJ set listing for Chicago venue.",
            rank=1,
        ),
        page=ResearchFetchedPage(
            url="https://mix.example.org/w?title=2003-02-07_DJ_Set_Chicago",
            final_url="https://mix.example.org/w?title=2003-02-07_DJ_Set_Chicago",
            title="2003-02-07 DJ Set Chicago",
            published_at="2003",
            source_type="web",
            text="2003-02-07 DJ set listing for a Chicago venue with resident DJs and flyer support.",
        ),
        fetch_outcome=ResearchFetchOutcome.FETCHED,
        fetch_status="fetched",
    )
    retrospective = service._build_finding(
        adapter_id="web_open",
        run=seed_run.model_copy(update={"run_id": "research-retro"}),
        brief=brief,
        facet=facet,
        query="test",
        hit=ResearchSearchHit(
            query="test",
            url="https://music.example.org/timeline",
            title="Timeline of house music key moments and artists",
            snippet="A retrospective overview of the genre's legacy.",
            rank=2,
        ),
        page=ResearchFetchedPage(
            url="https://music.example.org/timeline",
            final_url="https://music.example.org/timeline",
            title="Timeline of house music key moments and artists",
            published_at="2024-01-10",
            source_type="web",
            text="A retrospective overview of the genre's legacy and important artists.",
        ),
        fetch_outcome=ResearchFetchOutcome.FETCHED,
        fetch_status="fetched",
    )

    assert artifact.provenance is not None
    assert retrospective.provenance is not None
    assert artifact.provenance.scoring.era_score >= retrospective.provenance.scoring.era_score
    assert artifact.score > retrospective.score


def test_semantic_similarity_records_duplicate_hints_without_auto_rejecting(
    temp_data_dir: Path,
) -> None:
    class SemanticFacetScout(FakeScout):
        def search(self, query: str, *, limit: int = 5) -> list[ResearchSearchHit]:
            self.search_queries.append(query)
            lowered = query.lower()
            if "coverage" in lowered:
                return [
                        ResearchSearchHit(
                            query=query,
                            url="https://zine.example.com/scene-overview",
                            title="Scene Magazine Feature Coverage",
                            snippet="A 2003 local zine feature covered the scene's radio ecosystem, named venues, and club-night reporting.",
                            rank=1,
                        ),
                        ResearchSearchHit(
                            query=query,
                            url="https://news.example.com/feature",
                            title="2003 Chicago Radio Coverage Notes",
                            snippet="A 2003 Chicago magazine report covered local radio culture, named venues, and club-night promotion practices.",
                            rank=2,
                        ),
                ]
            return []

        def fetch_page(self, url: str) -> ResearchFetchedPage:
            if "scene-overview" in url:
                return ResearchFetchedPage(
                    url=url,
                    final_url=url,
                    title="Scene Magazine Feature Coverage",
                    publisher="Local Zine",
                    published_at="2003-03-10",
                    source_type="news",
                    text=(
                        "A 2003 local zine feature described promoters, venues, neighborhood shifts, and "
                        "scene reporting across local radio and print."
                    ),
                )
            if "news.example.com/feature" in url:
                return ResearchFetchedPage(
                    url=url,
                    final_url=url,
                    title="2003 Chicago Radio Coverage Notes",
                    publisher="Regional News",
                    published_at="2003-04-18",
                    source_type="news",
                    text=(
                        "A 2003 Chicago magazine report described late-night radio play, venue flyers, and "
                        "promotion practices tied to the local DJ scene."
                    ),
                )
            return super().fetch_page(url)

    semantic = FakeResearchSemantic(match_feature=True)
    service = build_service(temp_data_dir, SemanticFacetScout(), research_semantic=semantic)

    detail = service.run_research(
        ResearchRunRequest(
            brief=ResearchBrief(
                topic="2003 DJ scene",
                focal_year="2003",
                locale="Chicago",
                desired_facets=["media_culture"],
                coverage_targets={"media_culture": 2},
                max_queries=1,
                max_results_per_query=2,
                max_per_facet=2,
            )
        )
    )

    accepted = [item for item in detail.findings if item.decision == ResearchFindingDecision.ACCEPTED]
    assert len(accepted) == 2
    hinted = next(item for item in accepted if item.provenance and item.provenance.semantic_duplicate_hint)
    assert hinted.provenance is not None
    assert hinted.provenance.semantic_matches
    assert hinted.provenance.scoring.semantic_duplicate_similarity is not None
    assert hinted.novelty_score < 1.0
    assert hinted.rejection_reason is None
    assert detail.run.telemetry.semantic.duplicate_hints_emitted == 1


def test_semantic_backend_fallback_preserves_lexical_behavior(temp_data_dir: Path) -> None:
    class SemanticFacetScout(FakeScout):
        def search(self, query: str, *, limit: int = 5) -> list[ResearchSearchHit]:
            self.search_queries.append(query)
            if "coverage" in query.lower():
                return [
                    ResearchSearchHit(
                        query=query,
                        url="https://zine.example.com/scene-overview",
                        title="Scene Magazine Overview",
                        snippet="A local zine overview of the scene and its coverage ecosystem.",
                        rank=1,
                    ),
                        ResearchSearchHit(
                            query=query,
                            url="https://news.example.com/feature",
                            title="Feature Coverage of the Local Scene",
                            snippet="A 2003 Chicago magazine feature covered local radio culture with named venues.",
                            rank=2,
                        ),
                ]
            return []

        def fetch_page(self, url: str) -> ResearchFetchedPage:
            if "scene-overview" in url:
                return ResearchFetchedPage(
                    url=url,
                    final_url=url,
                    title="Scene Magazine Overview",
                    publisher="Local Zine",
                    published_at="2003-03-10",
                    source_type="news",
                    text=(
                        "The overview described promoters, venues, neighborhood shifts, and scene reporting "
                        "across local radio and print."
                    ),
                )
            return super().fetch_page(url)

    semantic = FakeResearchSemantic(fallback_reason="qdrant unavailable")
    service = build_service(temp_data_dir, SemanticFacetScout(), research_semantic=semantic)

    detail = service.run_research(
        ResearchRunRequest(
            brief=ResearchBrief(
                topic="2003 DJ scene",
                focal_year="2003",
                locale="Chicago",
                desired_facets=["media_culture"],
                coverage_targets={"media_culture": 2},
                max_queries=1,
                max_results_per_query=2,
                max_per_facet=2,
            )
        )
    )

    assert detail.run.status == "degraded_fallback"
    assert detail.run.telemetry.semantic.fallback_used is True
    assert "qdrant unavailable" in (detail.run.telemetry.semantic.fallback_reason or "")
    assert any(item.provenance and item.provenance.scoring.semantic_fallback_used for item in detail.findings)


def test_semantic_indexing_happens_for_accepted_and_completed_findings(temp_data_dir: Path) -> None:
    semantic = FakeResearchSemantic()
    service = build_service(temp_data_dir, FakeScout(), research_semantic=semantic)

    detail = service.run_research(
        ResearchRunRequest(
            brief=ResearchBrief(
                topic="2003 DJ scene",
                focal_year="2003",
                locale="Chicago",
                desired_facets=["people", "media_culture"],
                max_queries=2,
                max_results_per_query=1,
                max_per_facet=2,
            )
        )
    )

    assert semantic.upserts
    assert semantic.upserts[-1][0] == detail.run.run_id
    assert detail.run.telemetry.semantic.vectors_upserted == detail.run.finding_count


def test_semantic_advisory_only_compares_within_same_facet(temp_data_dir: Path) -> None:
    semantic = FakeResearchSemantic(match_feature=True)
    service = build_service(temp_data_dir, FakeScout(), research_semantic=semantic)

    service.run_research(
        ResearchRunRequest(
            brief=ResearchBrief(
                topic="2003 DJ scene",
                focal_year="2003",
                locale="Chicago",
                desired_facets=["people", "media_culture"],
                max_queries=2,
                max_results_per_query=1,
                max_per_facet=2,
            )
        )
    )

    assert not semantic.searches
