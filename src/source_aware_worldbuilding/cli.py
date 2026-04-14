from __future__ import annotations

import json
import re
import statistics
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import typer
import uvicorn
from psycopg import connect
from psycopg.sql import SQL, Identifier
from rich import print
from rich.table import Table

from source_aware_worldbuilding.adapters.file_backed import (
    FileBibleProjectProfileStore,
    FileBibleSectionStore,
    FileCandidateStore,
    FileEvidenceStore,
    FileExtractionRunStore,
    FileJobStore,
    FileResearchFindingStore,
    FileResearchProgramStore,
    FileResearchRunStore,
    FileSourceDocumentStore,
    FileSourceStore,
    FileTextUnitStore,
    FileTruthStore,
)
from source_aware_worldbuilding.adapters.heuristic_extraction import HeuristicExtractionAdapter
from source_aware_worldbuilding.adapters.postgres_backed import (
    PostgresBibleProjectProfileStore,
    PostgresBibleSectionStore,
    PostgresCandidateStore,
    PostgresEvidenceStore,
    PostgresExtractionRunStore,
    PostgresJobStore,
    PostgresResearchFindingStore,
    PostgresResearchProgramStore,
    PostgresResearchRunStore,
    PostgresReviewStore,
    PostgresSourceDocumentStore,
    PostgresSourceStore,
    PostgresTextUnitStore,
    PostgresTruthStore,
)
from source_aware_worldbuilding.adapters.qdrant_adapter import QdrantResearchSemanticAdapter
from source_aware_worldbuilding.adapters.sqlite_backed import (
    SqliteBibleProjectProfileStore,
    SqliteBibleSectionStore,
    SqliteCandidateStore,
    SqliteEvidenceStore,
    SqliteExtractionRunStore,
    SqliteJobStore,
    SqliteResearchFindingStore,
    SqliteResearchProgramStore,
    SqliteResearchRunStore,
    SqliteReviewStore,
    SqliteSourceDocumentStore,
    SqliteSourceStore,
    SqliteTextUnitStore,
)
from source_aware_worldbuilding.adapters.web_research_scout import (
    BraveSearchApiProvider,
    DuckDuckGoHtmlSearchProvider,
    ResearchScoutRegistry,
    ResearchSearchProviderRegistry,
    WebOpenResearchScout,
)
from source_aware_worldbuilding.adapters.zotero_adapter import ZoteroCorpusAdapter
from source_aware_worldbuilding.api.dependencies import (
    get_evidence_store,
    get_intake_service,
    get_normalization_service,
    get_projection,
    get_research_semantic,
    get_truth_store,
)
from source_aware_worldbuilding.domain.enums import (
    BibleSectionType,
    BibleTone,
    ClaimKind,
    ClaimStatus,
    ExtractionRunStatus,
    JobStatus,
    ResearchFetchOutcome,
    ResearchFindingDecision,
    ResearchFindingReason,
    ResearchRunStatus,
    ReviewDecision,
    ReviewState,
)
from source_aware_worldbuilding.domain.models import (
    ApprovedClaim,
    BibleCompositionDefaults,
    BibleProjectProfile,
    BibleSection,
    BibleSectionFilters,
    CandidateClaim,
    ClaimRelationship,
    EvidenceSnippet,
    ExtractionRun,
    IntakeTextRequest,
    IntakeUrlRequest,
    JobRecord,
    JobResultRef,
    ResearchBrief,
    ResearchExecutionPolicy,
    ResearchFacet,
    ResearchFinding,
    ResearchFindingProvenance,
    ResearchFindingScoring,
    ResearchProgram,
    ResearchRun,
    ResearchRunRequest,
    ResearchRunTelemetry,
    ReviewEvent,
    RuntimeStatus,
    SourceDocumentRecord,
    SourceRecord,
    TextUnit,
)
from source_aware_worldbuilding.services.bible import BibleWorkspaceService
from source_aware_worldbuilding.services.ingestion import IngestionService
from source_aware_worldbuilding.services.normalization import NormalizationService
from source_aware_worldbuilding.services.research import ResearchService
from source_aware_worldbuilding.services.status import (
    build_runtime_status,
    enforce_runtime_startup_checks,
)
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


_SEED_PROJECT_ID = "project-rouen-winter"
_SEED_CORE_RUN_ID = "extract-rouen-core"
_SEED_RESEARCH_RUN_ID = "research-rouen-winter"
_SEED_RESEARCH_EXTRACT_RUN_ID = "extract-research-rouen"
_SEED_ECON_SECTION_ID = "section-rouen-economics"
_SEED_RUMOR_SECTION_ID = "section-rouen-rumors"
_SEED_AUTHOR_SECTION_ID = "section-rouen-author-decisions"
_SEED_CREATED_AT = "2026-04-12T08:00:00+00:00"
_SEED_UPDATED_AT = "2026-04-12T13:15:00+00:00"


def _seed_sources() -> list[SourceRecord]:
    return [
        SourceRecord(
            source_id="src-price-ledger",
            title="Municipal price ledger of Rouen",
            author="Bread office clerks",
            year="1421",
            source_type="record",
            locator_hint="folio 12r",
            abstract="Clerks tracked the winter rise in bread prices at the Rouen market.",
            sync_status="ready_for_extraction",
        ),
        SourceRecord(
            source_id="src-bakers-petition",
            title="Petition of the Rouen bakers",
            author="Guild wardens",
            year="1422",
            source_type="petition",
            locator_hint="petition 3",
            abstract=(
                "Bakers described household bread tokens and ration loaves "
                "during the shortage."
            ),
            sync_status="ready_for_extraction",
        ),
        SourceRecord(
            source_id="src-port-roll",
            title="Saint Sever customs roll",
            author="River toll collector",
            year="1422",
            source_type="record",
            locator_hint="gate roll 4",
            abstract="Night barges unloaded grain outside Saint Sever gate before dawn.",
            sync_status="ready_for_extraction",
        ),
        SourceRecord(
            source_id="src-council-ordinance",
            title="Council grain ordinance",
            author="Rouen council",
            year="1422",
            source_type="ordinance",
            locator_hint="article 7",
            abstract="The grain bell schedule was recorded for January market control.",
            sync_status="ready_for_extraction",
        ),
        SourceRecord(
            source_id="src-council-addendum",
            title="Council addendum on the grain bell",
            author="Rouen council",
            year="1422",
            source_type="ordinance",
            locator_hint="addendum 2",
            abstract="A later ordinance revised the hour of the grain bell.",
            sync_status="ready_for_extraction",
        ),
        SourceRecord(
            source_id="src-chronicle",
            title="Later chronicle of the shortage winter",
            author="Anonymous chronicler",
            year="1450",
            source_type="chronicle",
            locator_hint="chapter 7",
            abstract="Citizens whispered that merchants hid grain behind shuttered lofts.",
            sync_status="ready_for_extraction",
        ),
        SourceRecord(
            source_id="src-abbey-accounts",
            title="Abbey barley accounts",
            author="Abbey bursar",
            year="1422",
            source_type="account_book",
            locator_hint="leaf 9v",
            abstract="Wax tally tablets tracked barley and salt debts by household.",
            sync_status="ready_for_extraction",
        ),
        SourceRecord(
            source_id="src-ballad",
            title="Dockside ballad of Saint Romain",
            author="Unknown singer",
            year="1435",
            source_type="broadside",
            locator_hint="verse 4",
            abstract="Sailors said shrine lanterns burned blue before the thaw.",
            sync_status="ready_for_extraction",
        ),
        SourceRecord(
            source_id="research-source-bread-scrip",
            external_source="research_scout",
            external_id="finding-bread-scrip",
            title="Curated archive note on bakers' scrip",
            author="Archive clerk",
            year="1422",
            source_type="archive",
            locator_hint="curated input",
            abstract="A curated note described bread scrip circulating at the market gate.",
            url="https://demo.sourcebound.test/archive/bread-scrip",
            sync_status="ready_for_extraction",
        ),
        SourceRecord(
            source_id="research-source-grain-bell",
            external_source="research_scout",
            external_id="finding-grain-bell",
            title="Curated parish note on grain bell beadles",
            author="Parish clerk",
            year="1422",
            source_type="archive",
            locator_hint="curated input",
            abstract="A curated note described beadles counting households at the grain bell.",
            url="https://demo.sourcebound.test/archive/grain-bell",
            sync_status="ready_for_extraction",
        ),
    ]


def _seed_source_documents() -> list[SourceDocumentRecord]:
    return [
        SourceDocumentRecord(
            document_id="doc-price-ledger",
            source_id="src-price-ledger",
            document_kind="attachment",
            filename="price-ledger.txt",
            mime_type="text/plain",
            ingest_status="imported",
            raw_text_status="ready",
            claim_extraction_status="completed",
            locator="folio 12r",
            raw_text=(
                "Rouen bread prices rose during the winter shortage in Rouen in 1421, "
                "and clerks marked the increase in the municipal ledger."
            ),
        ),
        SourceDocumentRecord(
            document_id="doc-bakers-petition",
            source_id="src-bakers-petition",
            document_kind="attachment",
            filename="bakers-petition.txt",
            mime_type="text/plain",
            ingest_status="imported",
            raw_text_status="ready",
            claim_extraction_status="completed",
            locator="petition 3",
            raw_text=(
                "Bakers in Rouen used household bread tokens during January 1422, "
                "and each token marked one ration loaf for a registered hearth."
            ),
        ),
        SourceDocumentRecord(
            document_id="doc-port-roll",
            source_id="src-port-roll",
            document_kind="attachment",
            filename="port-roll.txt",
            mime_type="text/plain",
            ingest_status="imported",
            raw_text_status="ready",
            claim_extraction_status="completed",
            locator="gate roll 4",
            raw_text=(
                "Night barges unloaded grain outside Rouen near Saint Sever gate in 1422, "
                "and porters moved sacks before dawn to avoid crowding."
            ),
        ),
        SourceDocumentRecord(
            document_id="doc-council-ordinance",
            source_id="src-council-ordinance",
            document_kind="attachment",
            filename="grain-ordinance.txt",
            mime_type="text/plain",
            ingest_status="imported",
            raw_text_status="ready",
            claim_extraction_status="completed",
            locator="article 7",
            raw_text=(
                "The council ordinance in Rouen recorded that the grain bell was rung after "
                "prime in early January 1422."
            ),
        ),
        SourceDocumentRecord(
            document_id="doc-council-addendum",
            source_id="src-council-addendum",
            document_kind="attachment",
            filename="grain-addendum.txt",
            mime_type="text/plain",
            ingest_status="imported",
            raw_text_status="ready",
            claim_extraction_status="completed",
            locator="addendum 2",
            raw_text=(
                "A revised council ordinance in Rouen recorded that the grain bell was rung "
                "after terce in late January 1422."
            ),
        ),
        SourceDocumentRecord(
            document_id="doc-chronicle",
            source_id="src-chronicle",
            document_kind="attachment",
            filename="later-chronicle.txt",
            mime_type="text/plain",
            ingest_status="imported",
            raw_text_status="ready",
            claim_extraction_status="completed",
            locator="chapter 7",
            raw_text=(
                "Citizens in Rouen whispered that merchants withheld grain behind shuttered lofts "
                "during the winter of 1422."
            ),
        ),
        SourceDocumentRecord(
            document_id="doc-abbey-accounts",
            source_id="src-abbey-accounts",
            document_kind="attachment",
            filename="abbey-accounts.txt",
            mime_type="text/plain",
            ingest_status="imported",
            raw_text_status="ready",
            claim_extraction_status="completed",
            locator="leaf 9v",
            raw_text=(
                "Abbey clerks in Rouen recorded wax tally tablets for barley and salt in "
                "February 1422, keeping each market debt by household mark."
            ),
        ),
        SourceDocumentRecord(
            document_id="doc-ballad",
            source_id="src-ballad",
            document_kind="attachment",
            filename="dockside-ballad.txt",
            mime_type="text/plain",
            ingest_status="imported",
            raw_text_status="ready",
            claim_extraction_status="queued",
            locator="verse 4",
            raw_text=(
                "Sailors in Rouen said the Saint Romain shrine lanterns burned blue before "
                "the thaw of 1422."
            ),
        ),
        SourceDocumentRecord(
            document_id="research-doc-bread-scrip",
            source_id="research-source-bread-scrip",
            document_kind="manual_text",
            external_id="https://demo.sourcebound.test/archive/bread-scrip",
            filename="bread-scrip.txt",
            mime_type="text/plain",
            ingest_status="imported",
            raw_text_status="ready",
            claim_extraction_status="completed",
            locator="curated input",
            raw_text=(
                "Rouen bakers were paid in bread scrip during the winter of 1422, "
                "and neighbors compared the stamped tokens at the market gate."
            ),
        ),
        SourceDocumentRecord(
            document_id="research-doc-grain-bell",
            source_id="research-source-grain-bell",
            document_kind="manual_text",
            external_id="https://demo.sourcebound.test/archive/grain-bell",
            filename="grain-bell.txt",
            mime_type="text/plain",
            ingest_status="imported",
            raw_text_status="ready",
            claim_extraction_status="completed",
            locator="curated input",
            raw_text=(
                "Rouen parish beadles were posted at the grain bell in 1422, and witnesses "
                "described how they counted households before opening the market."
            ),
        ),
    ]


def _seed_text_units() -> list[TextUnit]:
    documents = {item.document_id: item for item in _seed_source_documents()}
    mapping = [
        (
            "txt-price-ledger",
            "src-price-ledger",
            documents["doc-price-ledger"].locator,
            documents["doc-price-ledger"].raw_text,
        ),
        (
            "txt-bakers-petition",
            "src-bakers-petition",
            documents["doc-bakers-petition"].locator,
            documents["doc-bakers-petition"].raw_text,
        ),
        (
            "txt-port-roll",
            "src-port-roll",
            documents["doc-port-roll"].locator,
            documents["doc-port-roll"].raw_text,
        ),
        (
            "txt-council-ordinance",
            "src-council-ordinance",
            documents["doc-council-ordinance"].locator,
            documents["doc-council-ordinance"].raw_text,
        ),
        (
            "txt-council-addendum",
            "src-council-addendum",
            documents["doc-council-addendum"].locator,
            documents["doc-council-addendum"].raw_text,
        ),
        (
            "txt-chronicle",
            "src-chronicle",
            documents["doc-chronicle"].locator,
            documents["doc-chronicle"].raw_text,
        ),
        (
            "txt-abbey-accounts",
            "src-abbey-accounts",
            documents["doc-abbey-accounts"].locator,
            documents["doc-abbey-accounts"].raw_text,
        ),
        (
            "txt-ballad",
            "src-ballad",
            documents["doc-ballad"].locator,
            documents["doc-ballad"].raw_text,
        ),
        (
            "txt-research-bread-scrip",
            "research-source-bread-scrip",
            documents["research-doc-bread-scrip"].locator,
            documents["research-doc-bread-scrip"].raw_text,
        ),
        (
            "txt-research-grain-bell",
            "research-source-grain-bell",
            documents["research-doc-grain-bell"].locator,
            documents["research-doc-grain-bell"].raw_text,
        ),
    ]
    return [
        TextUnit(
            text_unit_id=text_unit_id,
            source_id=source_id,
            locator=locator or "unknown",
            text=text or "",
            ordinal=1,
            checksum=f"seed-{index:02d}",
        )
        for index, (text_unit_id, source_id, locator, text) in enumerate(mapping, start=1)
    ]


def _seed_evidence() -> list[EvidenceSnippet]:
    return [
        EvidenceSnippet(
            evidence_id="evi-price-rise",
            source_id="src-price-ledger",
            locator="folio 12r",
            text=(
                "Rouen bread prices rose during the winter shortage in Rouen in 1421, "
                "and clerks marked the increase in the municipal ledger."
            ),
            text_unit_id="txt-price-ledger",
            span_start=0,
            span_end=117,
            notes="Core economics evidence.",
        ),
        EvidenceSnippet(
            evidence_id="evi-bread-tokens",
            source_id="src-bakers-petition",
            locator="petition 3",
            text=(
                "Bakers in Rouen used household bread tokens during January 1422, "
                "and each token marked one ration loaf for a registered hearth."
            ),
            text_unit_id="txt-bakers-petition",
            span_start=0,
            span_end=124,
            notes="Ration-token detail for daily-life and economics sections.",
        ),
        EvidenceSnippet(
            evidence_id="evi-night-barges",
            source_id="src-port-roll",
            locator="gate roll 4",
            text=(
                "Night barges unloaded grain outside Rouen near Saint Sever gate in 1422, "
                "and porters moved sacks before dawn to avoid crowding."
            ),
            text_unit_id="txt-port-roll",
            span_start=0,
            span_end=126,
            notes="Strong setting and logistics detail.",
        ),
        EvidenceSnippet(
            evidence_id="evi-bell-prime",
            source_id="src-council-ordinance",
            locator="article 7",
            text=(
                "The council ordinance in Rouen recorded that the grain bell was rung after "
                "prime in early January 1422."
            ),
            text_unit_id="txt-council-ordinance",
            span_start=0,
            span_end=103,
            notes="First bell-time account.",
        ),
        EvidenceSnippet(
            evidence_id="evi-bell-terce",
            source_id="src-council-addendum",
            locator="addendum 2",
            text=(
                "A revised council ordinance in Rouen recorded that the grain bell was rung "
                "after terce in late January 1422."
            ),
            text_unit_id="txt-council-addendum",
            span_start=0,
            span_end=111,
            notes="Later bell-time revision.",
        ),
        EvidenceSnippet(
            evidence_id="evi-hoarding-rumor",
            source_id="src-chronicle",
            locator="chapter 7",
            text=(
                "Citizens in Rouen whispered that merchants withheld grain behind shuttered lofts "
                "during the winter of 1422."
            ),
            text_unit_id="txt-chronicle",
            span_start=0,
            span_end=110,
            notes="Later and lower-certainty rumor source.",
        ),
        EvidenceSnippet(
            evidence_id="evi-wax-tablets",
            source_id="src-abbey-accounts",
            locator="leaf 9v",
            text=(
                "Abbey clerks in Rouen recorded wax tally tablets for barley and salt in "
                "February 1422, keeping each market debt by household mark."
            ),
            text_unit_id="txt-abbey-accounts",
            span_start=0,
            span_end=130,
            notes="Material-culture evidence.",
        ),
        EvidenceSnippet(
            evidence_id="evi-blue-lanterns",
            source_id="src-ballad",
            locator="verse 4",
            text=(
                "Sailors in Rouen said the Saint Romain shrine lanterns burned blue before "
                "the thaw of 1422."
            ),
            text_unit_id="txt-ballad",
            span_start=0,
            span_end=93,
            notes="Folkloric atmosphere, not settled fact.",
        ),
        EvidenceSnippet(
            evidence_id="evi-bread-scrip",
            source_id="research-source-bread-scrip",
            locator="curated input",
            text=(
                "Rouen bakers were paid in bread scrip during the winter of 1422, "
                "and neighbors compared the stamped tokens at the market gate."
            ),
            text_unit_id="txt-research-bread-scrip",
            span_start=0,
            span_end=125,
            notes="Seeded accepted research finding staged into canon extraction.",
        ),
        EvidenceSnippet(
            evidence_id="evi-grain-bell-beadles",
            source_id="research-source-grain-bell",
            locator="curated input",
            text=(
                "Rouen parish beadles were posted at the grain bell in 1422, and witnesses "
                "described how they counted households before opening the market."
            ),
            text_unit_id="txt-research-grain-bell",
            span_start=0,
            span_end=137,
            notes="Accepted research finding awaiting review.",
        ),
    ]


def _seed_candidates() -> list[CandidateClaim]:
    return [
        CandidateClaim(
            candidate_id="cand-market-price",
            subject="Rouen bread prices",
            predicate="rose_during",
            value="the winter shortage in Rouen in 1421",
            claim_kind=ClaimKind.PRACTICE,
            status_suggestion=ClaimStatus.VERIFIED,
            review_state=ReviewState.APPROVED,
            place="Rouen",
            time_start="1421-12-01",
            time_end="1422-02-28",
            evidence_ids=["evi-price-rise"],
            extractor_run_id=_SEED_CORE_RUN_ID,
            notes="Approved from the municipal price ledger.",
        ),
        CandidateClaim(
            candidate_id="cand-bread-tokens",
            subject="Bakers in Rouen",
            predicate="used",
            value="household bread tokens during January 1422",
            claim_kind=ClaimKind.PRACTICE,
            status_suggestion=ClaimStatus.PROBABLE,
            review_state=ReviewState.APPROVED,
            place="Rouen",
            time_start="1422-01-01",
            time_end="1422-01-31",
            evidence_ids=["evi-bread-tokens"],
            extractor_run_id=_SEED_CORE_RUN_ID,
            notes="Approved because it grounds daily rationing behavior.",
        ),
        CandidateClaim(
            candidate_id="cand-bread-scrip",
            subject="Rouen bakers",
            predicate="were_paid_in",
            value="bread scrip during the winter of 1422",
            claim_kind=ClaimKind.PRACTICE,
            status_suggestion=ClaimStatus.PROBABLE,
            review_state=ReviewState.APPROVED,
            place="Rouen",
            time_start="1422-01-01",
            time_end="1422-02-28",
            evidence_ids=["evi-bread-scrip"],
            extractor_run_id=_SEED_RESEARCH_EXTRACT_RUN_ID,
            notes="Approved from the staged research run.",
        ),
        CandidateClaim(
            candidate_id="cand-hidden-grain",
            subject="Citizens in Rouen",
            predicate="rumored_that",
            value="merchants withheld grain behind shuttered lofts",
            claim_kind=ClaimKind.BELIEF,
            status_suggestion=ClaimStatus.RUMOR,
            review_state=ReviewState.REJECTED,
            place="Rouen",
            time_start="1422-01-01",
            time_end="1422-02-28",
            viewpoint_scope="citizens",
            evidence_ids=["evi-hoarding-rumor"],
            extractor_run_id=_SEED_CORE_RUN_ID,
            notes="Kept as a rumor note, not approved as canon fact.",
        ),
        CandidateClaim(
            candidate_id="cand-blue-lanterns",
            subject="Sailors in Rouen",
            predicate="said",
            value="the Saint Romain shrine lanterns burned blue before the thaw",
            claim_kind=ClaimKind.BELIEF,
            status_suggestion=ClaimStatus.RUMOR,
            review_state=ReviewState.PENDING,
            place="Rouen",
            time_start="1422-02-01",
            time_end="1422-02-28",
            viewpoint_scope="sailors",
            evidence_ids=["evi-blue-lanterns"],
            extractor_run_id=_SEED_CORE_RUN_ID,
            notes="Pending because it is vivid but folkloric.",
        ),
        CandidateClaim(
            candidate_id="cand-grain-bell-beadles",
            subject="Rouen parish beadles",
            predicate="were_posted_at",
            value="the grain bell in 1422",
            claim_kind=ClaimKind.INSTITUTION,
            status_suggestion=ClaimStatus.PROBABLE,
            review_state=ReviewState.PENDING,
            place="Rouen",
            time_start="1422-01-01",
            time_end="1422-02-28",
            evidence_ids=["evi-grain-bell-beadles"],
            extractor_run_id=_SEED_RESEARCH_EXTRACT_RUN_ID,
            notes="Strong stage/extract result that still needs review.",
        ),
    ]


def _seed_extraction_runs() -> list[ExtractionRun]:
    return [
        ExtractionRun(
            run_id=_SEED_CORE_RUN_ID,
            status=ExtractionRunStatus.COMPLETED,
            source_count=8,
            text_unit_count=8,
            candidate_count=4,
            started_at="2026-04-12T08:05:00+00:00",
            completed_at="2026-04-12T08:07:00+00:00",
            notes="Core offline fixture extraction across the historical source pack.",
        ),
        ExtractionRun(
            run_id=_SEED_RESEARCH_EXTRACT_RUN_ID,
            status=ExtractionRunStatus.COMPLETED,
            source_count=2,
            text_unit_count=2,
            candidate_count=2,
            started_at="2026-04-12T10:55:00+00:00",
            completed_at="2026-04-12T10:57:00+00:00",
            notes="Extraction of staged findings from the accepted research run.",
        ),
    ]


def _seed_review_events() -> list[ReviewEvent]:
    return [
        ReviewEvent(
            review_id="review-market-price",
            candidate_id="cand-market-price",
            decision=ReviewDecision.APPROVE,
            reviewed_at="2026-04-12T08:15:00+00:00",
            approved_claim_id="claim-market-price",
            notes="Core economic anchor for the project.",
        ),
        ReviewEvent(
            review_id="review-bread-tokens",
            candidate_id="cand-bread-tokens",
            decision=ReviewDecision.APPROVE,
            reviewed_at="2026-04-12T08:18:00+00:00",
            override_status=ClaimStatus.PROBABLE,
            approved_claim_id="claim-bread-tokens",
            notes="Useful routine detail despite narrow sourcing.",
        ),
        ReviewEvent(
            review_id="review-bread-scrip",
            candidate_id="cand-bread-scrip",
            decision=ReviewDecision.APPROVE,
            reviewed_at="2026-04-12T11:02:00+00:00",
            override_status=ClaimStatus.PROBABLE,
            approved_claim_id="claim-bread-scrip",
            notes="Accepted from staged research because it reinforces the token economy.",
        ),
        ReviewEvent(
            review_id="review-hidden-grain",
            candidate_id="cand-hidden-grain",
            decision=ReviewDecision.REJECT,
            reviewed_at="2026-04-12T08:21:00+00:00",
            notes="Keep as rumor texture only; do not turn into settled canon.",
        ),
    ]


def _seed_claims() -> list[ApprovedClaim]:
    return [
        ApprovedClaim(
            claim_id="claim-market-price",
            subject="Rouen bread prices",
            predicate="rose_during",
            value="the winter shortage",
            claim_kind=ClaimKind.PRACTICE,
            status=ClaimStatus.VERIFIED,
            place="Rouen",
            time_start="1421-12-01",
            time_end="1422-02-28",
            evidence_ids=["evi-price-rise"],
            created_from_run_id=_SEED_CORE_RUN_ID,
            notes="Strong anchor from the municipal ledger.",
        ),
        ApprovedClaim(
            claim_id="claim-bread-tokens",
            subject="Bakers in Rouen",
            predicate="used",
            value="household bread tokens",
            claim_kind=ClaimKind.PRACTICE,
            status=ClaimStatus.PROBABLE,
            place="Rouen",
            time_start="1422-01-01",
            time_end="1422-01-31",
            evidence_ids=["evi-bread-tokens"],
            created_from_run_id=_SEED_CORE_RUN_ID,
            notes="Grounds rationing scenes but still comes from a narrow petition source.",
        ),
        ApprovedClaim(
            claim_id="claim-bread-scrip",
            subject="Rouen bakers",
            predicate="were_paid_in",
            value="bread scrip",
            claim_kind=ClaimKind.PRACTICE,
            status=ClaimStatus.PROBABLE,
            place="Rouen",
            time_start="1422-01-01",
            time_end="1422-02-28",
            evidence_ids=["evi-bread-scrip"],
            created_from_run_id=_SEED_RESEARCH_EXTRACT_RUN_ID,
            notes="Accepted from the staged research run to deepen the local economy.",
        ),
        ApprovedClaim(
            claim_id="claim-night-barges",
            subject="Night barges",
            predicate="unloaded_at",
            value="Saint Sever gate before dawn",
            claim_kind=ClaimKind.EVENT,
            status=ClaimStatus.VERIFIED,
            place="Rouen",
            time_start="1422-01-01",
            time_end="1422-02-28",
            evidence_ids=["evi-night-barges"],
            created_from_run_id=_SEED_CORE_RUN_ID,
            notes="Supports setting logistics and harbor movement.",
        ),
        ApprovedClaim(
            claim_id="claim-bell-prime",
            subject="Rouen grain bell",
            predicate="rang_after",
            value="prime",
            claim_kind=ClaimKind.INSTITUTION,
            status=ClaimStatus.CONTESTED,
            place="Rouen",
            time_start="1422-01-01",
            time_end="1422-01-15",
            evidence_ids=["evi-bell-prime"],
            created_from_run_id=_SEED_CORE_RUN_ID,
            notes="Earlier ordinance reading kept as contested context.",
        ),
        ApprovedClaim(
            claim_id="claim-bell-terce",
            subject="Rouen grain bell",
            predicate="rang_after",
            value="terce",
            claim_kind=ClaimKind.INSTITUTION,
            status=ClaimStatus.PROBABLE,
            place="Rouen",
            time_start="1422-01-16",
            time_end="1422-02-28",
            evidence_ids=["evi-bell-terce"],
            created_from_run_id=_SEED_CORE_RUN_ID,
            notes="Later ordinance appears to supersede the earlier hour.",
        ),
        ApprovedClaim(
            claim_id="claim-wax-tablets",
            subject="Rouen market debt",
            predicate="was_tracked_with",
            value="wax tally tablets",
            claim_kind=ClaimKind.OBJECT,
            status=ClaimStatus.VERIFIED,
            place="Rouen",
            time_start="1422-02-01",
            time_end="1422-02-28",
            evidence_ids=["evi-wax-tablets"],
            created_from_run_id=_SEED_CORE_RUN_ID,
            notes="Concrete object detail for material culture.",
        ),
        ApprovedClaim(
            claim_id="claim-blue-lanterns",
            subject="Saint Romain shrine lanterns",
            predicate="were_said_to_burn",
            value="blue before the thaw",
            claim_kind=ClaimKind.BELIEF,
            status=ClaimStatus.RUMOR,
            place="Rouen",
            time_start="1422-02-01",
            time_end="1422-02-28",
            evidence_ids=["evi-blue-lanterns"],
            created_from_run_id=_SEED_CORE_RUN_ID,
            notes="Useful atmosphere, still explicitly rumor.",
        ),
        ApprovedClaim(
            claim_id="claim-docks-author-choice",
            subject="Rouen docks",
            predicate="should_be_depicted_as",
            value="narrow, freezing, and lantern-lit",
            claim_kind=ClaimKind.PLACE,
            status=ClaimStatus.AUTHOR_CHOICE,
            author_choice=True,
            place="Rouen",
            notes="Author decision to unify harbor scenes even when evidence is thin.",
        ),
    ]


def _seed_relationships() -> list[ClaimRelationship]:
    return [
        ClaimRelationship(
            relationship_id="rel-bread-tokens-support-price",
            claim_id="claim-bread-tokens",
            related_claim_id="claim-market-price",
            relationship_type="supports",
            source_kind="derived",
            notes="Ration tokens reinforce the price-spike reading.",
        ),
        ClaimRelationship(
            relationship_id="rel-bell-prime-contradicts-terce",
            claim_id="claim-bell-prime",
            related_claim_id="claim-bell-terce",
            relationship_type="contradicts",
            source_kind="derived",
            notes="The two bell-time accounts cannot both be primary canon without qualification.",
        ),
        ClaimRelationship(
            relationship_id="rel-bell-terce-contradicts-prime",
            claim_id="claim-bell-terce",
            related_claim_id="claim-bell-prime",
            relationship_type="contradicts",
            source_kind="derived",
            notes="The later addendum directly contests the earlier ordinance reading.",
        ),
        ClaimRelationship(
            relationship_id="rel-bell-terce-supersedes-prime",
            claim_id="claim-bell-terce",
            related_claim_id="claim-bell-prime",
            relationship_type="supersedes",
            source_kind="derived",
            notes=(
                "The addendum is treated as the better operational guide for scenes "
                "after mid-January."
            ),
        ),
        ClaimRelationship(
            relationship_id="rel-bell-prime-superseded-by-terce",
            claim_id="claim-bell-prime",
            related_claim_id="claim-bell-terce",
            relationship_type="superseded_by",
            source_kind="derived",
            notes=(
                "The earlier bell hour remains visible for provenance, not as the "
                "preferred synthesis."
            ),
        ),
    ]


def _seed_research_runs() -> list[ResearchRun]:
    return [
        ResearchRun(
            run_id=_SEED_RESEARCH_RUN_ID,
            status=ResearchRunStatus.COMPLETED,
            brief=ResearchBrief(
                topic="Rouen winter shortage daily life",
                focal_year="1422",
                time_start="1421-12-01",
                time_end="1422-02-28",
                locale="Rouen",
                audience="historical fiction authors",
                desired_facets=["practices", "institutions", "regional_context"],
                preferred_source_types=["archive", "record", "petition"],
                adapter_id="curated_inputs",
                max_queries=0,
                max_results_per_query=0,
                max_findings=4,
                max_per_facet=2,
            ),
            program_id="historical-daily-life",
            facets=[
                ResearchFacet(
                    facet_id="practices",
                    label="Practices",
                    query_hint="routines, household rationing, payment habits",
                    target_count=1,
                    queries_attempted=0,
                    hits_seen=2,
                    accepted_count=1,
                    rejected_count=1,
                    skipped_count=0,
                ),
                ResearchFacet(
                    facet_id="institutions",
                    label="Institutions",
                    query_hint="council enforcement, bells, wardens, beadles",
                    target_count=1,
                    queries_attempted=0,
                    hits_seen=1,
                    accepted_count=1,
                    rejected_count=0,
                    skipped_count=0,
                ),
                ResearchFacet(
                    facet_id="regional_context",
                    label="Region-Specific Context",
                    query_hint="broader winter context and neighboring market pressure",
                    target_count=1,
                    queries_attempted=0,
                    hits_seen=1,
                    accepted_count=0,
                    rejected_count=1,
                    skipped_count=0,
                ),
            ],
            query_count=0,
            finding_count=4,
            accepted_count=2,
            rejected_count=2,
            staged_count=2,
            extraction_run_id=_SEED_RESEARCH_EXTRACT_RUN_ID,
            telemetry=ResearchRunTelemetry(
                total_queries=0,
                queries_attempted=0,
                fetch_attempts=0,
                successful_fetches=0,
                retries=0,
                elapsed_run_time_ms=4200,
                elapsed_fetch_time_ms=0,
            ),
            warnings=["Regional context is still thin compared with daily-life detail."],
            logs=[
                "Queued research program historical-daily-life.",
                "Using adapter curated_inputs.",
                "Processing 2 curated input(s).",
                "Accepted one practice finding and one institutional finding.",
            ],
            started_at="2026-04-12T10:45:00+00:00",
            completed_at="2026-04-12T10:49:00+00:00",
        )
    ]


def _finding_provenance(
    *,
    facet_id: str,
    facet_label: str,
    url: str,
    score: float,
    relevance: float,
    quality: float,
    novelty: float,
    source_type: str,
    title: str,
    decision: ResearchFindingDecision,
    reason: ResearchFindingReason,
) -> ResearchFindingProvenance:
    return ResearchFindingProvenance(
        adapter_id="curated_inputs",
        facet_id=facet_id,
        facet_label=facet_label,
        originating_query="curated_input",
        query_profile="curated",
        search_provider_id="curated_inputs",
        matched_providers=["curated_inputs"],
        provider_rank=1,
        fusion_score=1.0,
        search_rank=1,
        hit_url=url,
        canonical_url=url,
        fetch_outcome=ResearchFetchOutcome.CURATED_TEXT,
        fetch_final_url=url,
        fetch_status="curated_text",
        acceptance_reason=reason if decision == ResearchFindingDecision.ACCEPTED else None,
        rejection_reason=reason if decision == ResearchFindingDecision.REJECTED else None,
        scoring=ResearchFindingScoring(
            overall_score=score,
            relevance_score=relevance,
            quality_score=quality,
            novelty_score=novelty,
            quality_threshold=0.45,
            threshold_passed=decision == ResearchFindingDecision.ACCEPTED,
            source_type=source_type,
            normalized_title=title,
            canonical_host=urlparse(url).netloc,
        ),
    )


def _seed_research_findings() -> list[ResearchFinding]:
    return [
        ResearchFinding(
            finding_id="finding-bread-scrip",
            run_id=_SEED_RESEARCH_RUN_ID,
            facet_id="practices",
            query="curated_input",
            url="https://demo.sourcebound.test/archive/bread-scrip",
            title="Curated archive note on bakers' scrip",
            canonical_url="https://demo.sourcebound.test/archive/bread-scrip",
            publisher="Archive clerk",
            published_at="1422-01-18",
            access_date="2026-04-12T10:45:10+00:00",
            locator="curated input",
            snippet_text=(
                "Rouen bakers were paid in bread scrip during the winter of 1422, "
                "and neighbors compared the stamped tokens at the market gate."
            ),
            page_excerpt=(
                "Rouen bakers were paid in bread scrip during the winter of 1422. "
                "Neighbors compared the stamped tokens at the market gate."
            ),
            source_type="archive",
            score=0.84,
            relevance_score=0.87,
            quality_score=0.82,
            novelty_score=0.79,
            decision=ResearchFindingDecision.ACCEPTED,
            staged_source_id="research-source-bread-scrip",
            staged_document_id="research-doc-bread-scrip",
            provenance=_finding_provenance(
                facet_id="practices",
                facet_label="Practices",
                url="https://demo.sourcebound.test/archive/bread-scrip",
                score=0.84,
                relevance=0.87,
                quality=0.82,
                novelty=0.79,
                source_type="archive",
                title="Curated archive note on bakers' scrip",
                decision=ResearchFindingDecision.ACCEPTED,
                reason=ResearchFindingReason.ACCEPTED_QUALITY_THRESHOLD,
            ),
        ),
        ResearchFinding(
            finding_id="finding-grain-bell",
            run_id=_SEED_RESEARCH_RUN_ID,
            facet_id="institutions",
            query="curated_input",
            url="https://demo.sourcebound.test/archive/grain-bell",
            title="Curated parish note on grain bell beadles",
            canonical_url="https://demo.sourcebound.test/archive/grain-bell",
            publisher="Parish clerk",
            published_at="1422-01-22",
            access_date="2026-04-12T10:45:20+00:00",
            locator="curated input",
            snippet_text=(
                "Rouen parish beadles were posted at the grain bell in 1422, and witnesses "
                "described how they counted households before opening the market."
            ),
            page_excerpt=(
                "Rouen parish beadles were posted at the grain bell in 1422. "
                "Witnesses described how they counted households before opening the market."
            ),
            source_type="archive",
            score=0.78,
            relevance_score=0.8,
            quality_score=0.76,
            novelty_score=0.73,
            decision=ResearchFindingDecision.ACCEPTED,
            staged_source_id="research-source-grain-bell",
            staged_document_id="research-doc-grain-bell",
            provenance=_finding_provenance(
                facet_id="institutions",
                facet_label="Institutions",
                url="https://demo.sourcebound.test/archive/grain-bell",
                score=0.78,
                relevance=0.8,
                quality=0.76,
                novelty=0.73,
                source_type="archive",
                title="Curated parish note on grain bell beadles",
                decision=ResearchFindingDecision.ACCEPTED,
                reason=ResearchFindingReason.ACCEPTED_QUALITY_THRESHOLD,
            ),
        ),
        ResearchFinding(
            finding_id="finding-food-history",
            run_id=_SEED_RESEARCH_RUN_ID,
            facet_id="practices",
            query="curated_input",
            url="https://demo.sourcebound.test/essays/food-history",
            title="Retrospective food history of medieval Rouen",
            canonical_url="https://demo.sourcebound.test/essays/food-history",
            publisher="Museum essay",
            published_at="2019-01-01",
            access_date="2026-04-12T10:45:30+00:00",
            locator="essay",
            snippet_text=(
                "A retrospective essay repeated the same token economy detail "
                "without adding period-specific texture."
            ),
            page_excerpt=(
                "The retrospective essay repeated the same token economy detail "
                "without adding new anchors."
            ),
            source_type="essay",
            score=0.54,
            relevance_score=0.68,
            quality_score=0.49,
            novelty_score=0.21,
            decision=ResearchFindingDecision.REJECTED,
            rejection_reason=ResearchFindingReason.REJECTED_DUPLICATE.value,
            provenance=_finding_provenance(
                facet_id="practices",
                facet_label="Practices",
                url="https://demo.sourcebound.test/essays/food-history",
                score=0.54,
                relevance=0.68,
                quality=0.49,
                novelty=0.21,
                source_type="essay",
                title="Retrospective food history of medieval Rouen",
                decision=ResearchFindingDecision.REJECTED,
                reason=ResearchFindingReason.REJECTED_DUPLICATE,
            ),
        ),
        ResearchFinding(
            finding_id="finding-regional-overview",
            run_id=_SEED_RESEARCH_RUN_ID,
            facet_id="regional_context",
            query="curated_input",
            url="https://demo.sourcebound.test/overview/winter-shortage",
            title="General overview of the winter shortage",
            canonical_url="https://demo.sourcebound.test/overview/winter-shortage",
            publisher="Reference overview",
            published_at="2015-01-01",
            access_date="2026-04-12T10:45:40+00:00",
            locator="overview",
            snippet_text=(
                "A broad overview mentioned hardship but offered no local Rouen "
                "anchors or operational detail."
            ),
            page_excerpt=(
                "The overview was broad, undated for Rouen, and too generic for "
                "scene construction."
            ),
            source_type="reference",
            score=0.32,
            relevance_score=0.4,
            quality_score=0.35,
            novelty_score=0.51,
            decision=ResearchFindingDecision.REJECTED,
            rejection_reason=ResearchFindingReason.REJECTED_QUALITY_THRESHOLD.value,
            provenance=_finding_provenance(
                facet_id="regional_context",
                facet_label="Region-Specific Context",
                url="https://demo.sourcebound.test/overview/winter-shortage",
                score=0.32,
                relevance=0.4,
                quality=0.35,
                novelty=0.51,
                source_type="reference",
                title="General overview of the winter shortage",
                decision=ResearchFindingDecision.REJECTED,
                reason=ResearchFindingReason.REJECTED_QUALITY_THRESHOLD,
            ),
        ),
    ]


def _seed_profile() -> BibleProjectProfile:
    return BibleProjectProfile(
        project_id=_SEED_PROJECT_ID,
        project_name="Rouen Winter Shortage Bible",
        era="1421-1422 winter shortage",
        time_start="1421-12-01",
        time_end="1422-02-28",
        geography="Rouen",
        social_lens="bakers, porters, clerks, and households waiting on ration lines",
        narrative_focus="market control, winter scarcity, rumor, and ritualized public order",
        taboo_topics=["modern slang", "clean heroic famine myths"],
        desired_facets=["economics", "daily life", "institutions", "ritual", "rumor"],
        tone=BibleTone.GROUNDED_LITERARY,
        composition_defaults=BibleCompositionDefaults(
            include_statuses=[ClaimStatus.VERIFIED, ClaimStatus.PROBABLE],
            source_types=["record", "petition", "archive", "account_book"],
            focus="bread prices, ration tokens, and bell-controlled market flow",
        ),
        created_at=_SEED_CREATED_AT,
        updated_at=_SEED_UPDATED_AT,
    )


def _seed_sections(data_dir: Path, profile: BibleProjectProfile) -> list[BibleSection]:
    service = BibleWorkspaceService(
        profile_store=FileBibleProjectProfileStore(data_dir),
        section_store=FileBibleSectionStore(data_dir),
        truth_store=FileTruthStore(data_dir),
        evidence_store=FileEvidenceStore(data_dir),
        source_store=FileSourceStore(data_dir),
    )

    economics_filters = BibleSectionFilters(
        focus="bread prices, ration tokens, debt tracking, and market movement",
        statuses=[ClaimStatus.VERIFIED, ClaimStatus.PROBABLE],
        source_types=["record", "petition", "archive", "account_book"],
        place="Rouen",
        time_start=profile.time_start,
        time_end=profile.time_end,
    )
    economics_draft = service._compose_section(
        profile.project_id,
        BibleSectionType.ECONOMICS_AND_MATERIAL_CULTURE,
        economics_filters,
    )

    rumor_filters = BibleSectionFilters(
        focus="grain bell disputes, hoarding stories, and eerie shrine gossip",
        statuses=[
            ClaimStatus.CONTESTED,
            ClaimStatus.RUMOR,
            ClaimStatus.LEGEND,
            ClaimStatus.PROBABLE,
        ],
        source_types=["ordinance", "chronicle", "broadside"],
        place="Rouen",
        time_start=profile.time_start,
        time_end=profile.time_end,
        relationship_types=["contradicts", "supersedes", "superseded_by"],
    )
    rumor_draft = service._compose_section(
        profile.project_id,
        BibleSectionType.RUMORS_AND_CONTESTED,
        rumor_filters,
    )
    author_filters = BibleSectionFilters(
        focus="narrative defaults for queues, cold weather, and shrine atmosphere",
        place="Rouen",
    )
    author_draft = service._compose_section(
        profile.project_id,
        BibleSectionType.AUTHOR_DECISIONS,
        author_filters,
    )

    manual_rumor_text = (
        "# What the City Said vs. What the Ledgers Say\n\n"
        "The working version for the novel keeps the grain-bell timing dispute "
        "visible as public confusion, not as a solved footnote. "
        "The latest generated synthesis stays below as the evidence-backed baseline.\n\n"
        "- Treat the prime-versus-terce contradiction as a queue-management "
        "problem the characters can feel.\n"
        "- Keep merchant-hoarding talk in dialogue and rumor, never in omniscient narration.\n"
        "- Use the blue-lantern shrine story for tone, not confirmation."
    )

    return [
        BibleSection(
            section_id=_SEED_ECON_SECTION_ID,
            project_id=profile.project_id,
            section_type=BibleSectionType.ECONOMICS_AND_MATERIAL_CULTURE,
            title="Markets, Tokens, and Grain Flow",
            content=economics_draft.generated_markdown,
            generated_markdown=economics_draft.generated_markdown,
            paragraphs=economics_draft.paragraphs,
            generation_filters=economics_filters,
            references=economics_draft.references,
            certainty_summary=economics_draft.certainty_summary,
            coverage_gaps=economics_draft.coverage_gaps,
            contradiction_flags=economics_draft.contradiction_flags,
            recommended_next_research=economics_draft.recommended_next_research,
            coverage_analysis=economics_draft.coverage_analysis,
            retrieval_metadata=economics_draft.retrieval_metadata,
            composition_metrics=economics_draft.composition_metrics,
            generation_status=economics_draft.generation_status,
            generation_error=economics_draft.generation_error,
            ready_for_writer=economics_draft.ready_for_writer,
            has_manual_edits=False,
            created_at="2026-04-12T12:20:00+00:00",
            updated_at="2026-04-12T12:20:00+00:00",
            last_generated_at="2026-04-12T12:20:00+00:00",
        ),
        BibleSection(
            section_id=_SEED_RUMOR_SECTION_ID,
            project_id=profile.project_id,
            section_type=BibleSectionType.RUMORS_AND_CONTESTED,
            title="What the City Said vs. What the Ledgers Say",
            content=manual_rumor_text,
            generated_markdown=rumor_draft.generated_markdown,
            manual_markdown=manual_rumor_text,
            paragraphs=rumor_draft.paragraphs,
            generation_filters=rumor_filters,
            references=rumor_draft.references,
            certainty_summary=rumor_draft.certainty_summary,
            coverage_gaps=rumor_draft.coverage_gaps,
            contradiction_flags=rumor_draft.contradiction_flags,
            recommended_next_research=rumor_draft.recommended_next_research,
            coverage_analysis=rumor_draft.coverage_analysis,
            retrieval_metadata=rumor_draft.retrieval_metadata,
            composition_metrics=rumor_draft.composition_metrics,
            generation_status=rumor_draft.generation_status,
            generation_error=rumor_draft.generation_error,
            ready_for_writer=False,
            has_manual_edits=True,
            created_at="2026-04-12T12:45:00+00:00",
            updated_at="2026-04-12T13:15:00+00:00",
            last_generated_at="2026-04-12T13:00:00+00:00",
            last_edited_at="2026-04-12T13:15:00+00:00",
        ),
        BibleSection(
            section_id=_SEED_AUTHOR_SECTION_ID,
            project_id=profile.project_id,
            section_type=BibleSectionType.AUTHOR_DECISIONS,
            title="House Style Decisions",
            content=author_draft.generated_markdown,
            generated_markdown=author_draft.generated_markdown,
            paragraphs=author_draft.paragraphs,
            generation_filters=author_filters,
            references=author_draft.references,
            certainty_summary=author_draft.certainty_summary,
            coverage_gaps=author_draft.coverage_gaps,
            contradiction_flags=author_draft.contradiction_flags,
            recommended_next_research=author_draft.recommended_next_research,
            coverage_analysis=author_draft.coverage_analysis,
            retrieval_metadata=author_draft.retrieval_metadata,
            composition_metrics=author_draft.composition_metrics,
            generation_status=author_draft.generation_status,
            generation_error=author_draft.generation_error,
            ready_for_writer=author_draft.ready_for_writer,
            has_manual_edits=False,
            created_at="2026-04-12T13:05:00+00:00",
            updated_at="2026-04-12T13:05:00+00:00",
            last_generated_at="2026-04-12T13:05:00+00:00",
        ),
    ]


def _seed_jobs(sections: list[BibleSection], profile: BibleProjectProfile) -> list[JobRecord]:
    economics_section = next(item for item in sections if item.section_id == _SEED_ECON_SECTION_ID)
    rumor_section = next(item for item in sections if item.section_id == _SEED_RUMOR_SECTION_ID)
    author_section = next(item for item in sections if item.section_id == _SEED_AUTHOR_SECTION_ID)
    return [
        JobRecord(
            job_id="job-research-create",
            job_type="research_run_create",
            status=JobStatus.COMPLETED,
            payload={
                "run_id": _SEED_RESEARCH_RUN_ID,
                "request": {
                    "brief": _seed_research_runs()[0].brief.model_dump(mode="json"),
                    "program_id": "historical-daily-life",
                },
            },
            progress_stage="completed",
            progress_current=100,
            progress_total=100,
            result_ref=JobResultRef(run_id=_SEED_RESEARCH_RUN_ID),
            created_at="2026-04-12T10:45:00+00:00",
            started_at="2026-04-12T10:45:02+00:00",
            completed_at="2026-04-12T10:49:00+00:00",
            updated_at="2026-04-12T10:49:00+00:00",
        ),
        JobRecord(
            job_id="job-research-stage",
            job_type="research_run_stage",
            status=JobStatus.COMPLETED,
            payload={"run_id": _SEED_RESEARCH_RUN_ID},
            progress_stage="completed",
            progress_current=100,
            progress_total=100,
            result_ref=JobResultRef(run_id=_SEED_RESEARCH_RUN_ID),
            created_at="2026-04-12T10:50:00+00:00",
            started_at="2026-04-12T10:50:03+00:00",
            completed_at="2026-04-12T10:52:00+00:00",
            updated_at="2026-04-12T10:52:00+00:00",
        ),
        JobRecord(
            job_id="job-research-extract",
            job_type="research_run_extract",
            status=JobStatus.COMPLETED,
            payload={"run_id": _SEED_RESEARCH_RUN_ID},
            progress_stage="completed",
            progress_current=100,
            progress_total=100,
            result_ref=JobResultRef(run_id=_SEED_RESEARCH_RUN_ID),
            created_at="2026-04-12T10:54:00+00:00",
            started_at="2026-04-12T10:54:02+00:00",
            completed_at="2026-04-12T10:57:00+00:00",
            updated_at="2026-04-12T10:57:00+00:00",
        ),
        JobRecord(
            job_id="job-bible-compose-economics",
            job_type="bible_section_compose",
            status=JobStatus.COMPLETED,
            payload={
                "section_id": economics_section.section_id,
                "request": {
                    "project_id": profile.project_id,
                    "section_type": economics_section.section_type.value,
                    "title": economics_section.title,
                    "filters": economics_section.generation_filters.model_dump(mode="json"),
                },
            },
            progress_stage="completed",
            progress_current=100,
            progress_total=100,
            result_ref=JobResultRef(section_id=economics_section.section_id),
            created_at="2026-04-12T12:18:00+00:00",
            started_at="2026-04-12T12:18:03+00:00",
            completed_at="2026-04-12T12:20:00+00:00",
            updated_at="2026-04-12T12:20:00+00:00",
        ),
        JobRecord(
            job_id="job-bible-regenerate-rumors",
            job_type="bible_section_regenerate",
            status=JobStatus.COMPLETED,
            payload={
                "section_id": rumor_section.section_id,
                "request": {"filters": rumor_section.generation_filters.model_dump(mode="json")},
            },
            progress_stage="completed",
            progress_current=100,
            progress_total=100,
            result_ref=JobResultRef(section_id=rumor_section.section_id),
            created_at="2026-04-12T12:58:00+00:00",
            started_at="2026-04-12T12:58:04+00:00",
            completed_at="2026-04-12T13:00:00+00:00",
            updated_at="2026-04-12T13:00:00+00:00",
        ),
        JobRecord(
            job_id="job-bible-export-latest",
            job_type="bible_project_export",
            status=JobStatus.PENDING,
            payload={"project_id": profile.project_id},
            progress_stage="queued",
            progress_current=0,
            progress_total=100,
            result_ref=JobResultRef(project_id=profile.project_id),
            created_at="2026-04-12T13:10:00+00:00",
            updated_at="2026-04-12T13:10:00+00:00",
        ),
        JobRecord(
            job_id="job-bible-compose-author-failed",
            job_type="bible_section_compose",
            status=JobStatus.FAILED,
            payload={
                "section_id": author_section.section_id,
                "request": {
                    "project_id": profile.project_id,
                    "section_type": author_section.section_type.value,
                    "title": author_section.title,
                    "filters": author_section.generation_filters.model_dump(mode="json"),
                },
            },
            progress_stage="failed",
            progress_current=40,
            progress_total=100,
            result_ref=JobResultRef(section_id=author_section.section_id),
            error="Background worker restarted before this compose job finished.",
            error_detail="Background worker restarted before this compose job finished.",
            retryable=True,
            warnings=["Retrying this job is safe; author manual text is not involved."],
            created_at="2026-04-12T13:06:00+00:00",
            started_at="2026-04-12T13:06:05+00:00",
            completed_at="2026-04-12T13:07:10+00:00",
            updated_at="2026-04-12T13:07:10+00:00",
        ),
    ]


def _seed_research_programs() -> list[dict]:
    return []


def _write_seed_files(
    *,
    data_dir: Path,
    sources: list[SourceRecord],
    source_documents: list[SourceDocumentRecord],
    text_units: list[TextUnit],
    evidence: list[EvidenceSnippet],
    candidates: list[CandidateClaim],
    extraction_runs: list[ExtractionRun],
    review_events: list[ReviewEvent],
    claims: list[ApprovedClaim],
    relationships: list[ClaimRelationship],
    research_runs: list[ResearchRun],
    research_findings: list[ResearchFinding],
    jobs: list[JobRecord],
    profile: BibleProjectProfile,
    sections: list[BibleSection],
) -> None:
    _write_json(data_dir / "sources.json", [item.model_dump(mode="json") for item in sources])
    _write_json(
        data_dir / "source_documents.json",
        [item.model_dump(mode="json") for item in source_documents],
    )
    _write_json(data_dir / "text_units.json", [item.model_dump(mode="json") for item in text_units])
    _write_json(data_dir / "evidence.json", [item.model_dump(mode="json") for item in evidence])
    _write_json(data_dir / "candidates.json", [item.model_dump(mode="json") for item in candidates])
    _write_json(
        data_dir / "extraction_runs.json",
        [item.model_dump(mode="json") for item in extraction_runs],
    )
    _write_json(
        data_dir / "review_events.json",
        [item.model_dump(mode="json") for item in review_events],
    )
    _write_json(data_dir / "claims.json", [item.model_dump(mode="json") for item in claims])
    _write_json(
        data_dir / "claim_relationships.json",
        [item.model_dump(mode="json") for item in relationships],
    )
    _write_json(
        data_dir / "research_runs.json",
        [item.model_dump(mode="json") for item in research_runs],
    )
    _write_json(
        data_dir / "research_findings.json",
        [item.model_dump(mode="json") for item in research_findings],
    )
    _write_json(data_dir / "research_programs.json", _seed_research_programs())
    _write_json(data_dir / "jobs.json", [item.model_dump(mode="json") for item in jobs])
    _write_json(
        data_dir / "bible_project_profiles.json",
        [profile.model_dump(mode="json")],
    )
    _write_json(
        data_dir / "bible_sections.json",
        [item.model_dump(mode="json") for item in sections],
    )


def _mirror_truth_to_postgres(
    *,
    evidence: list[EvidenceSnippet],
    review_events: list[ReviewEvent],
    claims: list[ApprovedClaim],
    relationships: list[ClaimRelationship],
) -> None:
    truth_store = PostgresTruthStore(settings.app_postgres_dsn, settings.app_postgres_schema)
    evidence_by_id = {item.evidence_id: item for item in evidence}
    reviews_by_claim = {
        review.approved_claim_id: review
        for review in review_events
        if review.approved_claim_id is not None
    }
    for claim in claims:
        claim_evidence = [
            evidence_by_id[evidence_id]
            for evidence_id in claim.evidence_ids
            if evidence_id in evidence_by_id
        ]
        truth_store.save_claim(
            claim, evidence=claim_evidence, review=reviews_by_claim.get(claim.claim_id)
        )
    for relationship in relationships:
        truth_store.upsert_relationship(
            relationship.claim_id,
            relationship.related_claim_id,
            relationship.relationship_type,
            notes=relationship.notes,
            source_kind=relationship.source_kind,
        )


def _initialize_qdrant_runtime() -> dict[str, object]:
    if not settings.qdrant_enabled and not settings.research_semantic_enabled:
        raise RuntimeError(
            "Qdrant projection and research semantics are both disabled; enable "
            "at least one collection before initialization."
        )

    projection = get_projection()
    projection_created = projection.initialize_collection() if settings.qdrant_enabled else False

    research_created = False
    if settings.research_semantic_enabled:
        research_created = get_research_semantic().initialize_collection()

    return {
        "qdrant_enabled": settings.qdrant_enabled,
        "projection_collection": settings.qdrant_collection,
        "projection_created": projection_created,
        "research_semantic_enabled": settings.research_semantic_enabled,
        "research_collection": settings.research_qdrant_collection,
        "research_created": research_created,
    }


def _rebuild_qdrant_projection() -> dict[str, object]:
    if not settings.qdrant_enabled:
        raise RuntimeError(
            "Qdrant projection is disabled; set QDRANT_ENABLED=true before rebuilding."
        )

    projection = get_projection()
    truth_store = get_truth_store()
    evidence_store = get_evidence_store()

    claims = truth_store.list_claims()
    evidence = evidence_store.list_evidence()
    projection_created = projection.initialize_collection()
    projection.upsert_claims(claims, evidence)

    return {
        "qdrant_enabled": settings.qdrant_enabled,
        "projection_collection": settings.qdrant_collection,
        "projection_created": projection_created,
        "claim_count": len(claims),
        "evidence_count": len(evidence),
    }


def _ensure_seed_prerequisites() -> dict[str, object] | None:
    if settings.app_state_backend == "postgres" or settings.app_truth_backend == "postgres":
        if not settings.app_postgres_dsn:
            raise RuntimeError(
                "APP_POSTGRES_DSN is required for the default Postgres newcomer path. "
                "Run `cp .env.example .env` or set "
                "`APP_POSTGRES_DSN=postgresql://saw:saw@localhost:5432/saw`."
            )
        try:
            with connect(
                settings.app_postgres_dsn,
                connect_timeout=2,
                autocommit=True,
            ) as connection:
                connection.execute("SELECT 1")
        except Exception as exc:
            raise RuntimeError(
                "Postgres connection failed before seeding: "
                f"{exc}. Start it with `docker compose up -d postgres` and verify "
                f"`APP_POSTGRES_DSN={settings.app_postgres_dsn}`."
            ) from exc

    if not settings.qdrant_enabled and not settings.research_semantic_enabled:
        return None

    try:
        return _initialize_qdrant_runtime()
    except RuntimeError as exc:
        raise RuntimeError(
            f"{exc} Start Qdrant with `docker compose up -d qdrant`. "
            "If you intentionally want a non-default degraded mode, set "
            "`QDRANT_ENABLED=false` and `RESEARCH_SEMANTIC_ENABLED=false` before seeding."
        ) from exc


@app.command()
def serve(
    reload: bool = False,
    strict_runtime_checks: bool = typer.Option(
        False,
        "--strict-runtime-checks",
        help="Refuse to serve when runtime dependencies required for retrieval are degraded.",
    ),
) -> None:
    effective_strict = strict_runtime_checks or settings.app_strict_startup_checks
    try:
        enforce_runtime_startup_checks(strict_runtime_checks=effective_strict)
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc

    uvicorn.run(
        "source_aware_worldbuilding.api.main:app",
        host=settings.app_host,
        port=settings.app_port,
        reload=reload,
        factory=False,
    )


@app.command("qdrant-init")
def qdrant_init(json_output: bool = False) -> None:
    try:
        report = _initialize_qdrant_runtime()
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    if json_output:
        typer.echo(json.dumps(report, indent=2))
        return

    print(
        f"[green]Qdrant projection collection ready:[/green] "
        f"{report['projection_collection']} "
        f"(created={'yes' if report['projection_created'] else 'no'})"
    )
    if report["research_semantic_enabled"]:
        print(
            f"[green]Research semantic collection ready:[/green] "
            f"{report['research_collection']} "
            f"(created={'yes' if report['research_created'] else 'no'})"
        )


@app.command("qdrant-rebuild")
def qdrant_rebuild(json_output: bool = False) -> None:
    try:
        report = _rebuild_qdrant_projection()
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    if json_output:
        typer.echo(json.dumps(report, indent=2))
        return

    print(
        f"[green]Qdrant projection rebuilt:[/green] {report['projection_collection']} "
        f"with {report['claim_count']} claims and {report['evidence_count']} evidence snippets "
        f"(created={'yes' if report['projection_created'] else 'no'})."
    )


@app.command()
def seed_dev_data() -> None:
    try:
        qdrant_report, data_dir = _seed_dev_data_impl()
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc

    print(f"[green]Seeded development data in {data_dir}[/green]")
    if qdrant_report and qdrant_report.get("qdrant_enabled"):
        print(
            f"[green]Qdrant projection ready:[/green] "
            f"{qdrant_report['projection_collection']} "
            f"with {qdrant_report['claim_count']} claims and {qdrant_report['evidence_count']} "
            "evidence snippets."
        )
    if qdrant_report and qdrant_report.get("research_semantic_enabled"):
        print(
            f"[green]Research semantic collection ready:[/green] "
            f"{qdrant_report['research_collection']} "
            f"(created={'yes' if qdrant_report['research_created'] else 'no'})"
        )


def _seed_dev_data_impl() -> tuple[dict[str, object] | None, Path]:
    qdrant_init_report = _ensure_seed_prerequisites()
    data_dir = settings.app_data_dir
    sources = _seed_sources()
    source_documents = _seed_source_documents()
    text_units = _seed_text_units()
    evidence = _seed_evidence()
    candidates = _seed_candidates()
    extraction_runs = _seed_extraction_runs()
    review_events = _seed_review_events()
    claims = _seed_claims()
    relationships = _seed_relationships()
    research_runs = _seed_research_runs()
    research_findings = _seed_research_findings()
    profile = _seed_profile()

    # Write the canonical JSON fixtures first so the file-backed composition pass can
    # synthesize deterministic bible sections from the same reviewed canon we ship.
    _write_seed_files(
        data_dir=data_dir,
        sources=sources,
        source_documents=source_documents,
        text_units=text_units,
        evidence=evidence,
        candidates=candidates,
        extraction_runs=extraction_runs,
        review_events=review_events,
        claims=claims,
        relationships=relationships,
        research_runs=research_runs,
        research_findings=research_findings,
        jobs=[],
        profile=profile,
        sections=[],
    )
    sections = _seed_sections(data_dir, profile)
    jobs = _seed_jobs(sections, profile)
    _write_seed_files(
        data_dir=data_dir,
        sources=sources,
        source_documents=source_documents,
        text_units=text_units,
        evidence=evidence,
        candidates=candidates,
        extraction_runs=extraction_runs,
        review_events=review_events,
        claims=claims,
        relationships=relationships,
        research_runs=research_runs,
        research_findings=research_findings,
        jobs=jobs,
        profile=profile,
        sections=sections,
    )

    if settings.app_state_backend == "postgres" or settings.app_truth_backend == "postgres":
        _reset_postgres_schema()

    if settings.app_state_backend == "postgres":
        PostgresSourceStore(
            settings.app_postgres_dsn,
            settings.app_postgres_schema,
        ).save_sources(sources)
        PostgresSourceDocumentStore(
            settings.app_postgres_dsn,
            settings.app_postgres_schema,
        ).save_source_documents(source_documents)
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
        extraction_run_store = PostgresExtractionRunStore(
            settings.app_postgres_dsn,
            settings.app_postgres_schema,
        )
        for run in extraction_runs:
            extraction_run_store.save_run(run)
        review_store = PostgresReviewStore(settings.app_postgres_dsn, settings.app_postgres_schema)
        for review in review_events:
            review_store.save_review(review)
        research_run_store = PostgresResearchRunStore(
            settings.app_postgres_dsn,
            settings.app_postgres_schema,
        )
        for run in research_runs:
            research_run_store.save_run(run)
        PostgresResearchFindingStore(
            settings.app_postgres_dsn,
            settings.app_postgres_schema,
        ).save_findings(research_findings)
        research_program_store = PostgresResearchProgramStore(
            settings.app_postgres_dsn,
            settings.app_postgres_schema,
        )
        for program in _seed_research_programs():
            research_program_store.save_program(ResearchProgram.model_validate(program))
        job_store = PostgresJobStore(
            settings.app_postgres_dsn,
            settings.app_postgres_schema,
        )
        for job in jobs:
            job_store.save_job(job)
        PostgresBibleProjectProfileStore(
            settings.app_postgres_dsn,
            settings.app_postgres_schema,
        ).save_profile(profile)
        section_store = PostgresBibleSectionStore(
            settings.app_postgres_dsn,
            settings.app_postgres_schema,
        )
        for section in sections:
            section_store.save_section(section)

    if settings.app_truth_backend == "postgres":
        _mirror_truth_to_postgres(
            evidence=evidence,
            review_events=review_events,
            claims=claims,
            relationships=relationships,
        )

    if settings.app_state_backend == "sqlite":
        if settings.app_sqlite_path.exists():
            settings.app_sqlite_path.unlink()
        SqliteSourceStore(settings.app_sqlite_path).save_sources(sources)
        SqliteSourceDocumentStore(settings.app_sqlite_path).save_source_documents(source_documents)
        SqliteTextUnitStore(settings.app_sqlite_path).save_text_units(text_units)
        SqliteEvidenceStore(settings.app_sqlite_path).save_evidence(evidence)
        SqliteCandidateStore(settings.app_sqlite_path).save_candidates(candidates)
        extraction_run_store = SqliteExtractionRunStore(settings.app_sqlite_path)
        for run in extraction_runs:
            extraction_run_store.save_run(run)
        review_store = SqliteReviewStore(settings.app_sqlite_path)
        for review in review_events:
            review_store.save_review(review)
        research_run_store = SqliteResearchRunStore(settings.app_sqlite_path)
        for run in research_runs:
            research_run_store.save_run(run)
        SqliteResearchFindingStore(settings.app_sqlite_path).save_findings(research_findings)
        research_program_store = SqliteResearchProgramStore(settings.app_sqlite_path)
        for program in _seed_research_programs():
            research_program_store.save_program(ResearchProgram.model_validate(program))
        job_store = SqliteJobStore(settings.app_sqlite_path)
        for job in jobs:
            job_store.save_job(job)
        SqliteBibleProjectProfileStore(settings.app_sqlite_path).save_profile(profile)
        section_store = SqliteBibleSectionStore(settings.app_sqlite_path)
        for section in sections:
            section_store.save_section(section)

    if settings.app_state_backend == "file":
        FileJobStore(settings.app_data_dir)
    if settings.app_truth_backend == "file":
        truth_store = FileTruthStore(settings.app_data_dir)
        evidence_by_id = {item.evidence_id: item for item in evidence}
        reviews_by_claim = {
            review.approved_claim_id: review
            for review in review_events
            if review.approved_claim_id is not None
        }
        for claim in claims:
            truth_store.save_claim(
                claim,
                evidence=[
                    evidence_by_id[evidence_id]
                    for evidence_id in claim.evidence_ids
                    if evidence_id in evidence_by_id
                ],
                review=reviews_by_claim.get(claim.claim_id),
            )
        for relationship in relationships:
            truth_store.upsert_relationship(
                relationship.claim_id,
                relationship.related_claim_id,
                relationship.relationship_type,
                notes=relationship.notes,
                source_kind=relationship.source_kind,
            )

    qdrant_report: dict[str, object] | None = None
    if qdrant_init_report:
        qdrant_report = dict(qdrant_init_report)
        if settings.qdrant_enabled:
            qdrant_report.update(_rebuild_qdrant_projection())

    return qdrant_report, data_dir


def _reset_postgres_schema() -> None:
    with connect(settings.app_postgres_dsn, autocommit=True) as connection:
        connection.execute(
            SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(Identifier(settings.app_postgres_schema))
        )
        connection.execute(SQL("CREATE SCHEMA {}").format(Identifier(settings.app_postgres_schema)))


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
        max_queries=15,
        max_results_per_query=10,
        max_findings=80,
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
    search_provider_registry = ResearchSearchProviderRegistry(
        _benchmark_search_providers(),
        default_order=_benchmark_search_provider_ids(),
    )
    return ResearchService(
        scout_registry=ResearchScoutRegistry(
            [
                WebOpenResearchScout(
                    user_agent=settings.app_research_user_agent,
                    search_provider_registry=search_provider_registry,
                    search_provider_ids=_benchmark_search_provider_ids(),
                )
            ],
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


def _benchmark_search_provider_ids() -> list[str]:
    configured = [
        item.strip() for item in settings.app_research_search_providers.split(",") if item.strip()
    ]
    if configured:
        return configured
    if settings.brave_search_api_key:
        return ["brave_search_api", "duckduckgo_html"]
    return ["duckduckgo_html"]


def _benchmark_search_providers() -> list[object]:
    providers: list[object] = [
        DuckDuckGoHtmlSearchProvider(user_agent=settings.app_research_user_agent)
    ]
    if settings.brave_search_api_key:
        providers.insert(
            0,
            BraveSearchApiProvider(
                api_key=settings.brave_search_api_key,
                base_url=settings.brave_search_base_url,
                user_agent=settings.app_research_user_agent,
            ),
        )
    return providers


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
        or value.endswith((":", "—", "-", "“", '"'))
        or subject in {"people", "person", "they", "it", "this", "that"}
    )
    noisy = any(
        pattern in value.lower()
        for pattern in ("photo by", "listen to", "read more", "shop ", "buy ", "sign up")
    )
    reviewable = not broken and not noisy
    return reviewable, broken


def _benchmark_core_year_range(brief) -> tuple[int | None, int | None]:
    focal_year = getattr(brief, "focal_year", None)
    if focal_year and str(focal_year).isdigit():
        year = int(focal_year)
        return year - 5, year + 5
    years = []
    for value in (getattr(brief, "time_start", None), getattr(brief, "time_end", None)):
        if not value:
            continue
        match = re.search(r"\b(\d{4})\b", str(value))
        if match:
            years.append(int(match.group(1)))
    if len(years) >= 2:
        return min(years), max(years)
    if years:
        return years[0], years[0]
    return None, None


def _benchmark_historical_year_range(brief) -> tuple[int | None, int | None]:
    core_start, _ = _benchmark_core_year_range(brief)
    if core_start is None:
        return None, None
    return core_start - 50, core_start - 1


def _benchmark_future_year_range(brief) -> tuple[int | None, int | None]:
    _, core_end = _benchmark_core_year_range(brief)
    if core_end is None:
        return None, None
    return core_end + 1, core_end + 10


def _benchmark_year_from_value(value: str | None) -> int | None:
    if not value:
        return None
    match = re.search(r"\b(\d{4})\b", str(value))
    return int(match.group(1)) if match else None


def _benchmark_era_band_for_year(year: int | None, brief) -> str:
    if year is None:
        return "unknown"
    core_start, core_end = _benchmark_core_year_range(brief)
    historical_start, historical_end = _benchmark_historical_year_range(brief)
    future_start, future_end = _benchmark_future_year_range(brief)
    if core_start is not None and core_end is not None and core_start <= year <= core_end:
        return "core"
    if (
        historical_start is not None
        and historical_end is not None
        and historical_start <= year <= historical_end
    ):
        return "historical"
    if future_start is not None and future_end is not None and future_start <= year <= future_end:
        return "future"
    if future_end is not None and year > future_end:
        return "distant_future"
    if historical_start is not None and year < historical_start:
        return "distant_past"
    return "unknown"


def _benchmark_core_year_tokens(brief) -> set[str]:
    start, end = _benchmark_core_year_range(brief)
    if start is None or end is None:
        year = _benchmark_year_from_value(getattr(brief, "focal_year", None))
        return {str(year)} if year is not None else set()
    return {str(year) for year in range(start, end + 1)}


def _benchmark_finding_era_state(finding: ResearchFinding, brief) -> dict[str, object]:
    scoring = finding.provenance.scoring if finding.provenance else None
    staged_text = " ".join(filter(None, [finding.page_excerpt or "", finding.snippet_text]))
    core_tokens = _benchmark_core_year_tokens(brief)
    published_year = _benchmark_year_from_value(finding.published_at)
    era_band = (
        scoring.era_band
        if scoring and scoring.era_band
        else _benchmark_era_band_for_year(published_year, brief)
    )
    period_native = scoring.period_native if scoring else era_band == "core"
    period_evidenced = (
        scoring.period_evidenced if scoring else any(year in staged_text for year in core_tokens)
    )
    historical_contextual = scoring.historical_contextual if scoring else era_band == "historical"
    return {
        "published_year": published_year,
        "era_band": era_band,
        "period_native": period_native,
        "period_evidenced": period_evidenced,
        "historical_contextual": historical_contextual,
    }


def _build_benchmark_scorecard(run_detail, extract_result) -> dict:
    accepted = [
        item for item in run_detail.findings if item.decision == ResearchFindingDecision.ACCEPTED
    ]
    top_candidates = extract_result.extraction.candidates[:10]
    brief = run_detail.run.brief
    core_era_count = 0
    period_evidenced_count = 0
    historical_context_count = 0
    late_retrospective_count = 0
    anchored_profile_count = 0
    concrete_anchor_count = 0
    source_counts: dict[str, int] = {}
    root_path_count = 0
    disallowed_source_count = 0
    fetch_failed_accepted_count = 0

    for finding in accepted:
        era_state = _benchmark_finding_era_state(finding, brief)
        source_identity = finding.canonical_url or finding.url
        source_counts[source_identity] = source_counts.get(source_identity, 0) + 1
        if finding.provenance and finding.provenance.query_profile in {
            "anchored",
            "source_seeking",
        }:
            anchored_profile_count += 1
        scoring = finding.provenance.scoring if finding.provenance else None
        if (
            scoring
            and scoring.concreteness_score >= 0.16
            and (scoring.anchor_score >= 0.16 or era_state["period_evidenced"])
        ):
            concrete_anchor_count += 1
        if era_state["period_native"]:
            core_era_count += 1
        elif era_state["period_evidenced"]:
            period_evidenced_count += 1
        elif era_state["historical_contextual"]:
            historical_context_count += 1
        elif era_state["era_band"] in {"future", "distant_future"}:
            late_retrospective_count += 1
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
        "core_era_count": core_era_count,
        "period_evidenced_count": period_evidenced_count,
        "historical_context_count": historical_context_count,
        "core_or_period_evidenced_count": core_era_count + period_evidenced_count,
        "core_or_period_evidenced_pass": (core_era_count + period_evidenced_count) >= 4,
        "anchored_profile_count": anchored_profile_count,
        "concrete_anchor_count": concrete_anchor_count,
        "unique_accepted_source_count": len(source_counts),
        "unique_accepted_source_pass": len(source_counts) >= 3,
        "max_facets_per_source": max(source_counts.values(), default=0),
        "max_facets_per_source_pass": max(source_counts.values(), default=0) <= 2,
        "accepted_findings_all_same_source": len(source_counts) == 1 and bool(source_counts),
        "root_path_count": root_path_count,
        "root_path_pass": root_path_count == 0,
        "late_retrospective_count": late_retrospective_count,
        "late_retrospective_pass": late_retrospective_count <= 1,
        "disallowed_source_count": disallowed_source_count,
        "disallowed_source_pass": disallowed_source_count == 0,
        "candidate_count": len(extract_result.extraction.candidates),
        "candidate_count_pass": len(extract_result.extraction.candidates) >= 10,
        "top_candidate_proxy_reviewable_count": reviewable_count,
        "top_candidate_proxy_reviewable_ratio": round(
            reviewable_count / max(len(top_candidates), 1), 3
        ),
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


def _build_benchmark_report(
    run_detail, extract_result, artifact_dir: Path, *, label: str | None
) -> dict:
    accepted_provider_profile: dict[str, int] = {}
    accepted_by_profile: dict[str, int] = {}
    accepted_anchor_class: dict[str, int] = {
        "core_era": 0,
        "period_evidenced": 0,
        "historical_context": 0,
        "late_retrospective": 0,
        "weak_anchor": 0,
    }
    accepted_by_source: dict[str, dict[str, object]] = {}
    brief = run_detail.run.brief
    for finding in run_detail.findings:
        if finding.decision != ResearchFindingDecision.ACCEPTED:
            continue
        provider = finding.provenance.search_provider_id if finding.provenance else None
        profile = finding.provenance.query_profile if finding.provenance else None
        key = f"{provider or 'unknown'}::{profile or 'unknown'}"
        accepted_provider_profile[key] = accepted_provider_profile.get(key, 0) + 1
        accepted_by_profile[profile or "unknown"] = (
            accepted_by_profile.get(profile or "unknown", 0) + 1
        )
        era_state = _benchmark_finding_era_state(finding, brief)
        if era_state["period_native"]:
            accepted_anchor_class["core_era"] += 1
        elif era_state["period_evidenced"]:
            accepted_anchor_class["period_evidenced"] += 1
        elif era_state["historical_contextual"]:
            accepted_anchor_class["historical_context"] += 1
        elif era_state["era_band"] in {"future", "distant_future"}:
            accepted_anchor_class["late_retrospective"] += 1
        else:
            accepted_anchor_class["weak_anchor"] += 1
        source_key = finding.canonical_url or finding.url
        group = accepted_by_source.setdefault(
            source_key,
            {
                "url": source_key,
                "title": finding.title,
                "count": 0,
                "facets": [],
                "query_profiles": [],
            },
        )
        group["count"] = int(group["count"]) + 1
        group["facets"].append(finding.facet_id)
        group["era_bands"] = list(
            dict.fromkeys([*(group.get("era_bands") or []), str(era_state["era_band"])])
        )
        if profile:
            group["query_profiles"].append(profile)
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
                "facet_fit_score": (
                    finding.provenance.scoring.facet_fit_score if finding.provenance else None
                ),
                "era_band": (finding.provenance.scoring.era_band if finding.provenance else None),
                "period_native": (
                    finding.provenance.scoring.period_native if finding.provenance else None
                ),
                "period_evidenced": (
                    finding.provenance.scoring.period_evidenced if finding.provenance else None
                ),
                "historical_contextual": (
                    finding.provenance.scoring.historical_contextual if finding.provenance else None
                ),
                "source_saturation_score": (
                    finding.provenance.scoring.source_saturation_score
                    if finding.provenance
                    else None
                ),
                "url": finding.canonical_url or finding.url,
                "snippet_text": finding.snippet_text,
                "page_excerpt": finding.page_excerpt,
                "staged_source_id": finding.staged_source_id,
                "search_provider_id": finding.provenance.search_provider_id
                if finding.provenance
                else None,
                "query_profile": finding.provenance.query_profile if finding.provenance else None,
                "provenance": finding.provenance.model_dump(mode="json")
                if finding.provenance
                else None,
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
        "provider_contribution": {
            "queries_by_provider": run_detail.run.telemetry.search.queries_by_provider,
            "hits_by_provider": run_detail.run.telemetry.search.hits_by_provider,
            "accepted_by_provider": run_detail.run.telemetry.search.accepted_by_provider,
            "accepted_by_profile": run_detail.run.telemetry.search.accepted_by_profile,
            "accepted_by_provider_profile": accepted_provider_profile,
            "accepted_anchor_class": accepted_anchor_class,
            "accepted_by_source": list(accepted_by_source.values()),
            "zero_hit_queries_by_profile": (
                run_detail.run.telemetry.search.zero_hit_queries_by_profile
            ),
        },
        "manual_review": _manual_review_slots(run_detail, extract_result),
    }
    return report


def _run_benchmark_2003_dj_once(output_root: Path, *, label: str | None = None) -> dict:
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


def _aggregate_benchmark_reports(
    reports: list[dict], artifact_dir: Path, *, label: str | None
) -> dict:
    scorecards = [report["scorecard"]["auto_checks"] for report in reports]
    accepted_counts = [len(report["accepted_findings"]) for report in reports]
    candidate_counts = [report["extraction"]["candidate_count"] for report in reports]
    core_or_period_counts = [
        report["scorecard"]["auto_checks"]["core_or_period_evidenced_count"] for report in reports
    ]
    late_retrospective_counts = [
        report["scorecard"]["auto_checks"]["late_retrospective_count"] for report in reports
    ]
    unique_source_counts = [
        report["scorecard"]["auto_checks"]["unique_accepted_source_count"] for report in reports
    ]
    max_facets_per_source_counts = [
        report["scorecard"]["auto_checks"]["max_facets_per_source"] for report in reports
    ]
    aggregate = {
        "benchmark_id": "2003_dj_chicago",
        "label": label,
        "generated_at": datetime.now(UTC).isoformat(),
        "artifact_dir": str(artifact_dir),
        "runs": [
            {
                "artifact_dir": report["artifact_dir"],
                "auto_pass": report["scorecard"]["auto_pass"],
                "status": report["run"]["status"],
                "accepted_findings": len(report["accepted_findings"]),
                "candidate_count": report["extraction"]["candidate_count"],
                "core_or_period_evidenced_count": report["scorecard"]["auto_checks"][
                    "core_or_period_evidenced_count"
                ],
            }
            for report in reports
        ],
        "summary": {
            "repeat_count": len(reports),
            "pass_count": sum(1 for report in reports if report["scorecard"]["auto_pass"]),
            "accepted_findings": {
                "best": max(accepted_counts, default=0),
                "worst": min(accepted_counts, default=0),
                "median": statistics.median(accepted_counts) if accepted_counts else 0,
            },
            "candidate_count": {
                "best": max(candidate_counts, default=0),
                "worst": min(candidate_counts, default=0),
                "median": statistics.median(candidate_counts) if candidate_counts else 0,
            },
            "core_or_period_evidenced_count": {
                "best": max(core_or_period_counts, default=0),
                "worst": min(core_or_period_counts, default=0),
                "median": statistics.median(core_or_period_counts) if core_or_period_counts else 0,
            },
            "late_retrospective_count": {
                "best": max(late_retrospective_counts, default=0),
                "worst": min(late_retrospective_counts, default=0),
                "median": statistics.median(late_retrospective_counts)
                if late_retrospective_counts
                else 0,
            },
            "unique_accepted_source_count": {
                "best": max(unique_source_counts, default=0),
                "worst": min(unique_source_counts, default=0),
                "median": statistics.median(unique_source_counts) if unique_source_counts else 0,
            },
            "max_facets_per_source": {
                "best": max(max_facets_per_source_counts, default=0),
                "worst": min(max_facets_per_source_counts, default=0),
                "median": statistics.median(max_facets_per_source_counts)
                if max_facets_per_source_counts
                else 0,
            },
            "coverage_all_facets_passes": sum(
                1 for item in scorecards if item["coverage_all_facets"]
            ),
            "candidate_count_passes": sum(1 for item in scorecards if item["candidate_count_pass"]),
            "core_or_period_evidenced_passes": sum(
                1 for item in scorecards if item["core_or_period_evidenced_pass"]
            ),
            "late_retrospective_passes": sum(
                1 for item in scorecards if item["late_retrospective_pass"]
            ),
            "unique_accepted_source_passes": sum(
                1 for item in scorecards if item["unique_accepted_source_pass"]
            ),
            "max_facets_per_source_passes": sum(
                1 for item in scorecards if item["max_facets_per_source_pass"]
            ),
        },
    }
    return aggregate


def _run_benchmark_2003_dj(output_root: Path, *, label: str | None = None, repeat: int = 1) -> dict:
    if repeat <= 1:
        return _run_benchmark_2003_dj_once(output_root, label=label)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    batch_name = timestamp if not label else f"{timestamp}-{label.replace(' ', '-').lower()}"
    batch_dir = output_root / batch_name
    batch_dir.mkdir(parents=True, exist_ok=True)
    reports = [
        _run_benchmark_2003_dj_once(batch_dir, label=f"run-{index + 1}") for index in range(repeat)
    ]
    summary = _aggregate_benchmark_reports(reports, batch_dir, label=label)
    (batch_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


@app.command("benchmark-2003-dj")
def benchmark_2003_dj(
    output_root: Path = Path("runtime/research_benchmarks/2003_dj_chicago"),
    label: str | None = None,
    repeat: int = 1,
    json_output: bool = False,
) -> None:
    report = _run_benchmark_2003_dj(output_root, label=label, repeat=repeat)
    if json_output:
        typer.echo(json.dumps(report, indent=2))
        return
    if repeat > 1:
        print("[bold]2003 DJ Benchmark[/bold]")
        print(f"Batch artifact: {report['artifact_dir']}")
        print(
            f"Runs: {report['summary']['repeat_count']} | "
            f"Passes: {report['summary']['pass_count']} | "
            f"Median accepted: {report['summary']['accepted_findings']['median']} | "
            f"Median candidates: {report['summary']['candidate_count']['median']} | "
            f"Median core/period-evidenced accepted: "
            f"{report['summary']['core_or_period_evidenced_count']['median']} | "
            f"Median unique sources: "
            f"{report['summary']['unique_accepted_source_count']['median']} | "
            f"Median max facets/source: {report['summary']['max_facets_per_source']['median']}"
        )
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
        f"Core/period-evidenced accepted: {scorecard['core_or_period_evidenced_count']} | "
        f"Unique sources: {scorecard['unique_accepted_source_count']} | "
        f"Late retrospectives: {scorecard['late_retrospective_count']} | "
        f"Top-10 proxy reviewable: {scorecard['top_candidate_proxy_reviewable_count']}"
    )


if __name__ == "__main__":
    app()
