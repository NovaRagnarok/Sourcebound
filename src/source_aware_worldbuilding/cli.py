from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import typer
import uvicorn
from rich import print
from rich.table import Table

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
from source_aware_worldbuilding.adapters.heuristic_extraction import HeuristicExtractionAdapter
from source_aware_worldbuilding.adapters.postgres_backed import (
    PostgresCandidateStore,
    PostgresEvidenceStore,
    PostgresExtractionRunStore,
    PostgresReviewStore,
    PostgresSourceDocumentStore,
    PostgresSourceStore,
    PostgresTextUnitStore,
)
from source_aware_worldbuilding.adapters.sqlite_backed import (
    SqliteCandidateStore,
    SqliteEvidenceStore,
    SqliteExtractionRunStore,
    SqliteReviewStore,
    SqliteSourceDocumentStore,
    SqliteSourceStore,
    SqliteTextUnitStore,
)
from source_aware_worldbuilding.adapters.qdrant_adapter import QdrantResearchSemanticAdapter
from source_aware_worldbuilding.adapters.web_research_scout import (
    ResearchScoutRegistry,
    WebOpenResearchScout,
)
from source_aware_worldbuilding.adapters.zotero_adapter import ZoteroCorpusAdapter
from source_aware_worldbuilding.api.dependencies import (
    get_intake_service,
    get_normalization_service,
)
from source_aware_worldbuilding.domain.enums import (
    ClaimKind,
    ClaimStatus,
    ExtractionRunStatus,
    ResearchFindingDecision,
    ResearchFetchOutcome,
    ReviewState,
)
from source_aware_worldbuilding.domain.models import (
    CandidateClaim,
    EvidenceSnippet,
    ExtractionRun,
    IntakeTextRequest,
    IntakeUrlRequest,
    ResearchBrief,
    ResearchExecutionPolicy,
    ResearchRunRequest,
    RuntimeStatus,
    SourceRecord,
    TextUnit,
)
from source_aware_worldbuilding.services.ingestion import IngestionService
from source_aware_worldbuilding.services.research import ResearchService
from source_aware_worldbuilding.services.status import build_runtime_status
from source_aware_worldbuilding.services.normalization import NormalizationService
from source_aware_worldbuilding.settings import settings

app = typer.Typer(help="Source-Aware Worldbuilding CLI")


def _write_json(path: Path, payload: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


class _BenchmarkCorpus:
    def pull_sources(self) -> list[SourceRecord]:
        return []

    def discover_source_documents(self, sources: list[SourceRecord]):
        _ = sources
        return []

    def pull_text_units(self, sources: list[SourceRecord]) -> list[TextUnit]:
        _ = sources
        return []

    def pull_sources_by_item_keys(self, item_keys: list[str]) -> list[SourceRecord]:
        _ = item_keys
        return []


def _seed_sources() -> list[SourceRecord]:
    return [
        SourceRecord(
            source_id="src-1",
            title="Municipal price records of Rouen",
            author="City clerk",
            year="1421",
            source_type="record",
            locator_hint="folios 10-14",
            abstract="Bread prices rose sharply during the winter shortage.",
        ),
        SourceRecord(
            source_id="src-2",
            title="Later chronicle of unrest",
            author="Anonymous chronicler",
            year="1450",
            source_type="chronicle",
            locator_hint="chapter 7",
            abstract="Townspeople whispered that merchants were withholding grain.",
        ),
    ]


def _seed_text_units() -> list[TextUnit]:
    return [
        TextUnit(
            text_unit_id="txt-1",
            source_id="src-1",
            locator="folio 12r",
            text="Bread prices rose sharply during the winter shortage.",
            ordinal=1,
            checksum="seed-1",
        ),
        TextUnit(
            text_unit_id="txt-2",
            source_id="src-2",
            locator="chapter 7",
            text="Townspeople whispered that merchants were withholding grain.",
            ordinal=1,
            checksum="seed-2",
        ),
    ]


def _seed_evidence() -> list[EvidenceSnippet]:
    return [
        EvidenceSnippet(
            evidence_id="evi-1",
            source_id="src-1",
            locator="folio 12r",
            text="Bread prices rose sharply during the winter shortage.",
            text_unit_id="txt-1",
            span_start=0,
            span_end=48,
            notes="Economic record",
        ),
        EvidenceSnippet(
            evidence_id="evi-2",
            source_id="src-2",
            locator="chapter 7",
            text="Townspeople whispered that merchants were withholding grain.",
            text_unit_id="txt-2",
            span_start=0,
            span_end=59,
            notes="Later chronicle, lower certainty",
        ),
    ]


def _seed_candidates() -> list[CandidateClaim]:
    return [
        CandidateClaim(
            candidate_id="cand-1",
            subject="Rouen bread prices",
            predicate="rose_during",
            value="winter shortage",
            claim_kind=ClaimKind.PRACTICE,
            status_suggestion=ClaimStatus.PROBABLE,
            review_state=ReviewState.PENDING,
            place="Rouen",
            time_start="1421-12-01",
            time_end="1422-02-28",
            evidence_ids=["evi-1"],
            extractor_run_id="seed-run",
            notes="Derived from municipal records.",
        ),
        CandidateClaim(
            candidate_id="cand-2",
            subject="Merchant grain hoarding",
            predicate="rumored_in",
            value="Rouen",
            claim_kind=ClaimKind.BELIEF,
            status_suggestion=ClaimStatus.RUMOR,
            review_state=ReviewState.PENDING,
            place="Rouen",
            time_start="1422-01-01",
            time_end="1422-01-31",
            viewpoint_scope="townspeople",
            evidence_ids=["evi-2"],
            extractor_run_id="seed-run",
            notes="Rumor, not a verified economic fact.",
        ),
    ]


def _seed_run() -> ExtractionRun:
    return ExtractionRun(
        run_id="seed-run",
        status=ExtractionRunStatus.COMPLETED,
        source_count=2,
        text_unit_count=2,
        candidate_count=2,
        started_at="2026-04-12T00:00:00+00:00",
        completed_at="2026-04-12T00:00:00+00:00",
        notes="Seeded development run.",
    )


@app.command()
def serve(reload: bool = False) -> None:
    uvicorn.run(
        "source_aware_worldbuilding.api.main:app",
        host=settings.app_host,
        port=settings.app_port,
        reload=reload,
        factory=False,
    )


@app.command()
def seed_dev_data() -> None:
    data_dir = settings.app_data_dir
    sources = _seed_sources()
    text_units = _seed_text_units()
    evidence = _seed_evidence()
    candidates = _seed_candidates()
    extraction_runs = [_seed_run()]
    review_events: list[dict] = []

    _write_json(data_dir / "sources.json", [item.model_dump(mode="json") for item in sources])
    _write_json(data_dir / "text_units.json", [item.model_dump(mode="json") for item in text_units])
    _write_json(data_dir / "evidence.json", [item.model_dump(mode="json") for item in evidence])
    _write_json(data_dir / "candidates.json", [item.model_dump(mode="json") for item in candidates])
    _write_json(
        data_dir / "extraction_runs.json",
        [item.model_dump(mode="json") for item in extraction_runs],
    )
    _write_json(data_dir / "review_events.json", review_events)
    _write_json(data_dir / "claims.json", [])
    _write_json(data_dir / "claim_relationships.json", [])
    _write_json(data_dir / "source_documents.json", [])

    if settings.app_state_backend == "postgres":
        source_store = PostgresSourceStore(
            settings.app_postgres_dsn,
            settings.app_postgres_schema,
        )
        source_store.store.clear_all()
        source_store.save_sources(sources)
        PostgresSourceDocumentStore(
            settings.app_postgres_dsn,
            settings.app_postgres_schema,
        ).save_source_documents([])
        PostgresTextUnitStore(
            settings.app_postgres_dsn,
            settings.app_postgres_schema,
        ).save_text_units(text_units)
        PostgresEvidenceStore(
            settings.app_postgres_dsn,
            settings.app_postgres_schema,
        ).save_evidence(evidence)
        PostgresCandidateStore(
            settings.app_postgres_dsn,
            settings.app_postgres_schema,
        ).save_candidates(candidates)
        PostgresExtractionRunStore(
            settings.app_postgres_dsn,
            settings.app_postgres_schema,
        ).save_run(extraction_runs[0])
        PostgresReviewStore(settings.app_postgres_dsn, settings.app_postgres_schema)

    if settings.app_state_backend == "sqlite":
        if settings.app_sqlite_path.exists():
            settings.app_sqlite_path.unlink()
        SqliteSourceStore(settings.app_sqlite_path).save_sources(sources)
        SqliteSourceDocumentStore(settings.app_sqlite_path).save_source_documents([])
        SqliteTextUnitStore(settings.app_sqlite_path).save_text_units(text_units)
        SqliteEvidenceStore(settings.app_sqlite_path).save_evidence(evidence)
        SqliteCandidateStore(settings.app_sqlite_path).save_candidates(candidates)
        SqliteExtractionRunStore(settings.app_sqlite_path).save_run(extraction_runs[0])
        SqliteReviewStore(settings.app_sqlite_path)

    print(f"[green]Seeded development data in {data_dir}[/green]")


@app.command()
def status(json_output: bool = False) -> None:
    runtime_status = build_runtime_status()
    if json_output:
        typer.echo(json.dumps(runtime_status.model_dump(mode="json"), indent=2))
        return

    _print_runtime_status(runtime_status)


def _print_runtime_status(runtime_status: RuntimeStatus) -> None:
    print(
        f"[bold]{runtime_status.app_name}[/bold] "
        f"({runtime_status.app_env}) - overall status: "
        f"[cyan]{runtime_status.overall_status}[/cyan]"
    )
    print(
        "State backend: "
        f"{runtime_status.state_backend} | Truth backend: {runtime_status.truth_backend} | "
        f"Extraction: {runtime_status.extraction_backend} | "
        f"Operator UI: {'enabled' if runtime_status.operator_ui_enabled else 'disabled'}"
    )

    table = Table(title="Runtime Services")
    table.add_column("Name")
    table.add_column("Mode")
    table.add_column("Ready")
    table.add_column("Detail")

    for service in runtime_status.services:
        ready_label = "yes" if service.ready else "no"
        table.add_row(service.name, service.mode, ready_label, service.detail)
    print(table)

    if runtime_status.next_steps:
        print("[bold]Next Steps[/bold]")
        for step in runtime_status.next_steps:
            print(f"- {step}")


@app.command("zotero-check")
def zotero_check(
    json_output: bool = False,
    source_limit: int = 3,
    include_text_units: bool = True,
) -> None:
    report = _build_zotero_report(
        source_limit=max(1, source_limit),
        include_text_units=include_text_units,
    )
    if json_output:
        typer.echo(json.dumps(report, indent=2))
        return

    _print_zotero_report(report)


def _build_zotero_report(*, source_limit: int, include_text_units: bool) -> dict:
    missing: list[str] = []
    if not settings.zotero_library_id:
        missing.append("ZOTERO_LIBRARY_ID")

    report: dict = {
        "configured": not missing,
        "library_type": settings.zotero_library_type,
        "library_id_present": bool(settings.zotero_library_id),
        "collection_key_present": bool(settings.zotero_collection_key),
        "api_key_present": bool(settings.zotero_api_key),
        "base_url": settings.zotero_base_url,
        "source_limit": source_limit,
        "include_text_units": include_text_units,
        "missing": missing,
        "success": False,
        "source_count": 0,
        "text_unit_count": 0,
        "sources_preview": [],
        "text_units_preview": [],
    }
    if missing:
        report["detail"] = "Zotero is not configured yet."
        return report

    adapter = ZoteroCorpusAdapter()
    try:
        sources = adapter.pull_sources()
        report["source_count"] = len(sources)
        report["sources_preview"] = [
            source.model_dump(mode="json") for source in sources[:source_limit]
        ]

        if include_text_units and sources:
            text_units = adapter.pull_text_units(sources[:1])
            report["text_unit_count"] = len(text_units)
            report["text_units_preview"] = [
                text_unit.model_dump(mode="json") for text_unit in text_units[:source_limit]
            ]

        report["success"] = True
        report["detail"] = "Zotero pull succeeded."
        return report
    except Exception as exc:
        report["detail"] = f"Zotero pull failed: {exc}"
        return report


def _print_zotero_report(report: dict) -> None:
    print("[bold]Zotero Check[/bold]")
    if report["configured"]:
        print(
            "Library type: "
            f"{report['library_type']} | "
            f"Collection key set: {'yes' if report['collection_key_present'] else 'no'} | "
            f"API key set: {'yes' if report['api_key_present'] else 'no'}"
        )
    else:
        missing_line = (
            "Missing: " + ", ".join(report["missing"])
            if report["missing"]
            else "Zotero config present."
        )
        print(missing_line)

    print(report["detail"])

    if report["success"]:
        print(
            f"Sources pulled: {report['source_count']} | "
            f"Text units previewed: {report['text_unit_count']}"
        )
        preview_table = Table(title="Zotero Source Preview")
        preview_table.add_column("Source ID")
        preview_table.add_column("Title")
        preview_table.add_column("Author")
        preview_table.add_column("Year")
        for source in report["sources_preview"]:
            preview_table.add_row(
                source["source_id"],
                source["title"],
                source.get("author") or "n/a",
                source.get("year") or "n/a",
            )
        print(preview_table)


@app.command("intake-text")
def intake_text(
    title: str,
    text: str,
    author: str | None = None,
    year: str | None = None,
    source_type: str = "document",
    notes: str | None = None,
    collection_key: str | None = None,
    json_output: bool = False,
) -> None:
    result = get_intake_service().intake_text(
        IntakeTextRequest(
            title=title,
            text=text,
            author=author,
            year=year,
            source_type=source_type,
            notes=notes,
            collection_key=collection_key,
        )
    )
    if json_output:
        typer.echo(json.dumps(result.model_dump(mode="json"), indent=2))
        return
    print(
        f"[green]Created Zotero item {result.created_item.zotero_item_key}[/green] | "
        f"Sources: {len(result.pulled_sources)} | "
        f"Documents queued: {len(result.source_documents)}"
    )
    for warning in result.warnings:
        print(f"[yellow]{warning}[/yellow]")


@app.command("intake-url")
def intake_url(
    url: str,
    title: str | None = None,
    notes: str | None = None,
    collection_key: str | None = None,
    json_output: bool = False,
) -> None:
    result = get_intake_service().intake_url(
        IntakeUrlRequest(
            url=url,
            title=title,
            notes=notes,
            collection_key=collection_key,
        )
    )
    if json_output:
        typer.echo(json.dumps(result.model_dump(mode="json"), indent=2))
        return
    print(
        f"[green]Created Zotero item {result.created_item.zotero_item_key}[/green] | "
        f"Sources: {len(result.pulled_sources)} | "
        f"Documents queued: {len(result.source_documents)}"
    )
    for warning in result.warnings:
        print(f"[yellow]{warning}[/yellow]")


@app.command("intake-file")
def intake_file(
    path: Path,
    title: str | None = None,
    notes: str | None = None,
    source_type: str = "document",
    collection_key: str | None = None,
    json_output: bool = False,
) -> None:
    result = get_intake_service().intake_file(
        filename=path.name,
        content_type=None,
        content=path.read_bytes(),
        title=title,
        source_type=source_type,
        notes=notes,
        collection_key=collection_key,
    )
    if json_output:
        typer.echo(json.dumps(result.model_dump(mode="json"), indent=2))
        return
    print(
        f"[green]Created Zotero item {result.created_item.zotero_item_key}[/green] | "
        f"Sources: {len(result.pulled_sources)} | "
        f"Documents queued: {len(result.source_documents)}"
    )
    for warning in result.warnings:
        print(f"[yellow]{warning}[/yellow]")


@app.command("normalize-documents")
def normalize_documents(json_output: bool = False) -> None:
    result = get_normalization_service().normalize_documents()
    if json_output:
        typer.echo(json.dumps(result, indent=2))
        return
    print(
        f"[green]Normalized documents[/green] | "
        f"Documents touched: {result['document_count']} | "
        f"Text units created: {result['text_unit_count']}"
    )
    for warning in result["warnings"]:
        print(f"[yellow]{warning}[/yellow]")


def _benchmark_brief_2003_dj() -> ResearchBrief:
    return ResearchBrief(
        topic="Chicago house DJ and club scene",
        focal_year="2003",
        time_start="2002",
        time_end="2004",
        locale="Chicago",
        domain_hints=[
            "house music",
            "vinyl CDJ mixtape flyers radio record pools residencies",
        ],
        desired_facets=[
            "objects_technology",
            "media_culture",
            "regional_context",
            "people",
            "practices",
        ],
        excluded_source_types=["social", "shopping"],
        coverage_targets={
            "objects_technology": 1,
            "media_culture": 1,
            "regional_context": 1,
            "people": 1,
            "practices": 1,
        },
        max_queries=10,
        max_results_per_query=10,
        max_findings=50,
        max_per_facet=1,
        execution_policy=ResearchExecutionPolicy(
            total_fetch_time_seconds=90,
            per_host_fetch_cap=2,
            retry_attempts=2,
            retry_backoff_base_ms=250,
            retry_backoff_max_ms=1500,
            respect_robots=True,
        ),
    )


def _build_benchmark_research_service(state_dir: Path) -> ResearchService:
    source_store = FileSourceStore(state_dir)
    source_document_store = FileSourceDocumentStore(state_dir)
    text_unit_store = FileTextUnitStore(state_dir)
    normalization_service = NormalizationService(
        source_document_store=source_document_store,
        text_unit_store=text_unit_store,
    )
    ingestion_service = IngestionService(
        corpus=_BenchmarkCorpus(),
        extractor=HeuristicExtractionAdapter(),
        source_store=source_store,
        text_unit_store=text_unit_store,
        source_document_store=source_document_store,
        run_store=FileExtractionRunStore(state_dir),
        candidate_store=FileCandidateStore(state_dir),
        evidence_store=FileEvidenceStore(state_dir),
    )
    default_program_markdown = (
        Path(__file__).resolve().parents[2] / "docs" / "research" / "default_program.md"
    ).read_text(encoding="utf-8")
    return ResearchService(
        scout_registry=ResearchScoutRegistry(
            [WebOpenResearchScout(user_agent=settings.app_research_user_agent)],
            default_adapter_id="web_open",
        ),
        run_store=FileResearchRunStore(state_dir),
        finding_store=FileResearchFindingStore(state_dir),
        program_store=FileResearchProgramStore(state_dir),
        source_store=source_store,
        source_document_store=source_document_store,
        normalization_service=normalization_service,
        ingestion_service=ingestion_service,
        research_semantic=QdrantResearchSemanticAdapter(),
        default_program_markdown=default_program_markdown,
        default_execution_policy=ResearchExecutionPolicy(
            total_fetch_time_seconds=settings.app_research_total_fetch_time_seconds,
            per_host_fetch_cap=settings.app_research_per_host_fetch_cap,
            retry_attempts=settings.app_research_retry_attempts,
            retry_backoff_base_ms=settings.app_research_retry_backoff_base_ms,
            retry_backoff_max_ms=settings.app_research_retry_backoff_max_ms,
            respect_robots=settings.app_research_respect_robots,
        ),
        default_adapter_id=settings.app_research_default_adapter_id,
        research_user_agent=settings.app_research_user_agent,
        semantic_duplicate_threshold=settings.research_semantic_duplicate_threshold,
        semantic_novelty_floor=settings.research_semantic_novelty_floor,
        semantic_rerank_weight=settings.research_semantic_rerank_weight,
    )


def _manual_review_slots(run_detail, extract_result) -> dict:
    return {
        "accepted_findings": [
            {
                "finding_id": finding.finding_id,
                "title": finding.title,
                "facet_id": finding.facet_id,
                "marks": [],
                "allowed_marks": [
                    "story-useful",
                    "too retrospective",
                    "too generic",
                    "wrong facet",
                    "low-value source",
                ],
                "notes": None,
            }
            for finding in run_detail.findings
            if finding.decision == ResearchFindingDecision.ACCEPTED
        ],
        "top_candidates": [
            {
                "candidate_id": candidate.candidate_id,
                "subject": candidate.subject,
                "predicate": candidate.predicate,
                "value": candidate.value,
                "marks": [],
                "allowed_marks": [
                    "reviewable factual lead",
                    "too vague",
                    "fragment/broken",
                    "wrong subject",
                    "marketing/noise",
                ],
                "notes": None,
            }
            for candidate in extract_result.extraction.candidates[:10]
        ],
    }


def _benchmark_candidate_quality(candidate: CandidateClaim) -> tuple[bool, bool]:
    value = " ".join(candidate.value.split()).strip()
    subject = " ".join(candidate.subject.split()).strip().lower()
    broken = (
        len(value) < 24
        or len(value.split()) < 4
        or value.endswith((":", "—", "-", "“", "\""))
        or subject in {"people", "person", "they", "it", "this", "that"}
    )
    noisy = any(
        pattern in value.lower()
        for pattern in ("photo by", "listen to", "read more", "shop ", "buy ", "sign up")
    )
    reviewable = not broken and not noisy
    return reviewable, broken


def _build_benchmark_scorecard(run_detail, extract_result) -> dict:
    accepted = [item for item in run_detail.findings if item.decision == ResearchFindingDecision.ACCEPTED]
    top_candidates = extract_result.extraction.candidates[:10]
    target_years = {"2002", "2003", "2004"}
    near_era_or_anchored = 0
    late_sources = 0
    root_path_count = 0
    disallowed_source_count = 0
    fetch_failed_accepted_count = 0

    for finding in accepted:
        published_year = ""
        if finding.published_at:
            for year in target_years | {str(year) for year in range(1900, 2101)}:
                if year in finding.published_at:
                    published_year = year
                    break
        staged_text = " ".join(filter(None, [finding.page_excerpt or "", finding.snippet_text]))
        anchored = any(year in staged_text for year in target_years)
        if published_year in target_years or (
            finding.source_type in {"archive", "news", "magazine", "government", "educational", "web"}
            and anchored
        ):
            near_era_or_anchored += 1
        if published_year and published_year.isdigit() and int(published_year) > 2015:
            late_sources += 1
        if finding.canonical_url and (urlparse(finding.canonical_url).path or "/") == "/":
            root_path_count += 1
        if finding.source_type in {"social", "shopping"}:
            disallowed_source_count += 1
        if finding.provenance and finding.provenance.fetch_outcome == ResearchFetchOutcome.FAILED:
            fetch_failed_accepted_count += 1

    reviewable_count = 0
    broken_count = 0
    for candidate in top_candidates:
        reviewable, broken = _benchmark_candidate_quality(candidate)
        if reviewable:
            reviewable_count += 1
        if broken:
            broken_count += 1

    auto_checks = {
        "coverage_all_facets": all(item.accepted_count >= 1 for item in run_detail.facet_coverage),
        "accepted_findings_total": len(accepted) == 5,
        "near_era_or_anchored_count": near_era_or_anchored,
        "near_era_or_anchored_pass": near_era_or_anchored >= 4,
        "root_path_count": root_path_count,
        "root_path_pass": root_path_count == 0,
        "late_source_count": late_sources,
        "late_source_pass": late_sources <= 1,
        "disallowed_source_count": disallowed_source_count,
        "disallowed_source_pass": disallowed_source_count == 0,
        "candidate_count": len(extract_result.extraction.candidates),
        "candidate_count_pass": len(extract_result.extraction.candidates) >= 10,
        "top_candidate_proxy_reviewable_count": reviewable_count,
        "top_candidate_proxy_reviewable_ratio": round(reviewable_count / max(len(top_candidates), 1), 3),
        "top_candidate_proxy_reviewable_pass": reviewable_count >= 7,
        "top_candidate_proxy_broken_count": broken_count,
        "top_candidate_proxy_broken_pass": broken_count <= 2,
        "runtime_failure_pass": run_detail.run.status not in {"failed_runtime", "failed"},
        "degraded_status_pass": run_detail.run.status != "degraded_fallback"
        or "robots_unavailable" in run_detail.run.telemetry.fallback_flags,
        "semantic_fallback_pass": not run_detail.run.telemetry.semantic.fallback_used,
        "accepted_fetch_failure_pass": fetch_failed_accepted_count == 0,
    }
    auto_pass = all(
        value
        for key, value in auto_checks.items()
        if key.endswith("_pass") or key in {"coverage_all_facets", "accepted_findings_total"}
    )
    return {
        "benchmark_id": "2003_dj_chicago",
        "auto_checks": auto_checks,
        "auto_pass": auto_pass,
        "manual_review_required": True,
    }


def _build_benchmark_report(run_detail, extract_result, artifact_dir: Path, *, label: str | None) -> dict:
    report = {
        "benchmark_id": "2003_dj_chicago",
        "label": label,
        "generated_at": datetime.now(UTC).isoformat(),
        "artifact_dir": str(artifact_dir),
        "run": run_detail.run.model_dump(mode="json"),
        "facet_coverage": [item.model_dump(mode="json") for item in run_detail.facet_coverage],
        "accepted_findings": [
            {
                "finding_id": finding.finding_id,
                "facet_id": finding.facet_id,
                "title": finding.title,
                "publisher": finding.publisher,
                "published_at": finding.published_at,
                "source_type": finding.source_type,
                "score": finding.score,
                "relevance_score": finding.relevance_score,
                "quality_score": finding.quality_score,
                "novelty_score": finding.novelty_score,
                "url": finding.canonical_url or finding.url,
                "snippet_text": finding.snippet_text,
                "page_excerpt": finding.page_excerpt,
                "staged_source_id": finding.staged_source_id,
                "provenance": finding.provenance.model_dump(mode="json") if finding.provenance else None,
            }
            for finding in run_detail.findings
            if finding.decision == ResearchFindingDecision.ACCEPTED
        ],
        "rejected_findings": [
            {
                "finding_id": finding.finding_id,
                "facet_id": finding.facet_id,
                "title": finding.title,
                "reason": finding.rejection_reason,
                "score": finding.score,
                "url": finding.canonical_url or finding.url,
            }
            for finding in run_detail.findings
            if finding.decision == ResearchFindingDecision.REJECTED
        ],
        "stage_result": {
            "staged_source_ids": extract_result.stage_result.staged_source_ids,
            "staged_document_ids": extract_result.stage_result.staged_document_ids,
            "warnings": extract_result.stage_result.warnings,
        },
        "normalization": extract_result.normalization,
        "extraction": {
            "run": extract_result.extraction.run.model_dump(mode="json"),
            "candidate_count": len(extract_result.extraction.candidates),
            "evidence_count": len(extract_result.extraction.evidence),
            "candidates": [
                candidate.model_dump(mode="json")
                for candidate in extract_result.extraction.candidates
            ],
        },
        "scorecard": _build_benchmark_scorecard(run_detail, extract_result),
        "manual_review": _manual_review_slots(run_detail, extract_result),
    }
    return report


def _run_benchmark_2003_dj(output_root: Path, *, label: str | None = None) -> dict:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    artifact_name = timestamp if not label else f"{timestamp}-{label.replace(' ', '-').lower()}"
    artifact_dir = output_root / artifact_name
    state_dir = artifact_dir / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    service = _build_benchmark_research_service(state_dir)
    run_detail = service.run_research(ResearchRunRequest(brief=_benchmark_brief_2003_dj()))
    extract_result = service.extract_run(run_detail.run.run_id)
    report = _build_benchmark_report(run_detail, extract_result, artifact_dir, label=label)
    report_path = artifact_dir / "report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


@app.command("benchmark-2003-dj")
def benchmark_2003_dj(
    output_root: Path = Path("runtime/research_benchmarks/2003_dj_chicago"),
    label: str | None = None,
    json_output: bool = False,
) -> None:
    report = _run_benchmark_2003_dj(output_root, label=label)
    if json_output:
        typer.echo(json.dumps(report, indent=2))
        return
    scorecard = report["scorecard"]["auto_checks"]
    print("[bold]2003 DJ Benchmark[/bold]")
    print(f"Artifact: {report['artifact_dir']}")
    print(
        f"Accepted findings: {len(report['accepted_findings'])} | "
        f"Candidates: {report['extraction']['candidate_count']} | "
        f"Auto pass: {'yes' if report['scorecard']['auto_pass'] else 'no'}"
    )
    print(
        "Coverage: "
        f"{'ok' if scorecard['coverage_all_facets'] else 'missing facets'} | "
        f"Near-era accepted: {scorecard['near_era_or_anchored_count']} | "
        f"Late accepted: {scorecard['late_source_count']} | "
        f"Top-10 proxy reviewable: {scorecard['top_candidate_proxy_reviewable_count']}"
    )


if __name__ == "__main__":
    app()
