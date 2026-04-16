from __future__ import annotations

import json
from hashlib import sha1
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from source_aware_worldbuilding.adapters.file_backed import (
    FileBibleProjectProfileStore,
    FileBibleSectionStore,
    FileCandidateStore,
    FileEvidenceStore,
    FileExtractionRunStore,
    FileReviewStore,
    FileSourceDocumentStore,
    FileSourceStore,
    FileTextUnitStore,
    FileTruthStore,
)
from source_aware_worldbuilding.adapters.heuristic_extraction import HeuristicExtractionAdapter
from source_aware_worldbuilding.domain.enums import (
    BibleSectionGenerationStatus,
    BibleSectionType,
    ClaimStatus,
    ReviewDecision,
)
from source_aware_worldbuilding.domain.models import (
    AuthenticatedActor,
    BibleProjectProfileUpdateRequest,
    BibleSectionCreateRequest,
    BibleSectionFilters,
    IntakeTextRequest,
    ReviewClaimPatch,
    ReviewRequest,
    SourceDocumentRecord,
    SourceRecord,
    ZoteroCreatedItem,
)
from source_aware_worldbuilding.services.bible import BibleWorkspaceService
from source_aware_worldbuilding.services.ingestion import IngestionService
from source_aware_worldbuilding.services.intake import IntakeService
from source_aware_worldbuilding.services.normalization import NormalizationService
from source_aware_worldbuilding.services.review import ReviewService


class DemoCorpusDocumentSpec(BaseModel):
    document_id: str
    document_kind: Literal["attachment", "note", "manual_text"]
    path: str
    locator: str
    filename: str | None = None
    mime_type: str = "text/plain"


class DemoCorpusApprovalSpec(BaseModel):
    predicate: str | None = None
    subject_contains: str | None = None
    value_contains: str
    override_status: ClaimStatus | None = None
    claim_patch: ReviewClaimPatch | None = None
    notes: str | None = None


class DemoCorpusManifest(BaseModel):
    corpus_id: str
    corpus_name: str
    source: SourceRecord
    documents: list[DemoCorpusDocumentSpec] = Field(default_factory=list)
    approvals: list[DemoCorpusApprovalSpec] = Field(default_factory=list)
    project_id: str
    profile: BibleProjectProfileUpdateRequest
    section_type: BibleSectionType
    section_filters: BibleSectionFilters = Field(default_factory=BibleSectionFilters)


class DemoCorpusRunSummary(BaseModel):
    corpus_id: str
    corpus_name: str
    data_dir: str
    source_id: str
    source_document_count: int
    text_unit_count: int
    candidate_count: int
    pending_candidate_count: int
    approved_claim_count: int
    approved_claim_ids: list[str] = Field(default_factory=list)
    review_preview_candidate_id: str | None = None
    review_preview_excerpt: str | None = None
    review_preview_span_start: int | None = None
    review_preview_span_end: int | None = None
    review_preview_locator: str | None = None
    section_id: str
    section_type: BibleSectionType
    section_title: str
    section_generation_status: BibleSectionGenerationStatus
    section_ready_for_writer: bool
    section_claim_ids: list[str] = Field(default_factory=list)
    section_excerpt: str
    summary_path: str
    markdown_path: str


class DemoCorpusAdapter:
    def __init__(self, manifest: DemoCorpusManifest, corpus_dir: Path) -> None:
        self.manifest = manifest
        self.corpus_dir = corpus_dir
        self.item_key = f"demo-{manifest.corpus_id}"

    def pull_sources(self):
        return []

    def discover_source_documents(self, sources, *, existing_documents=None, force_refresh=False):
        _ = existing_documents, force_refresh
        source_ids = {source.source_id for source in sources}
        if self.manifest.source.source_id not in source_ids:
            return []
        return [
            _build_source_document_record(
                self.manifest,
                self.corpus_dir,
                self.manifest.source.source_id,
                spec,
            )
            for spec in self.manifest.documents
        ]

    def pull_text_units(self, sources):
        _ = sources
        return []

    def pull_sources_by_item_keys(self, item_keys):
        if self.item_key not in set(item_keys):
            return []
        source = self.manifest.source.model_copy(deep=True)
        source.external_source = "demo"
        source.external_id = self.manifest.corpus_id
        source.zotero_item_key = self.item_key
        source.raw_metadata_json = {"demo_corpus_id": self.manifest.corpus_id}
        return [source]

    def create_text_source(self, request: IntakeTextRequest) -> ZoteroCreatedItem:
        _ = request
        return ZoteroCreatedItem(
            zotero_item_key=self.item_key,
            title=self.manifest.source.title,
            item_type=self.manifest.source.source_type,
        )

    def create_url_source(self, request):
        raise NotImplementedError("Demo corpus adapter only supports manifest-backed text intake.")

    def create_file_source(self, **kwargs):
        raise NotImplementedError("Demo corpus adapter only supports manifest-backed text intake.")


_DATA_FILES = (
    "sources.json",
    "source_documents.json",
    "text_units.json",
    "extraction_runs.json",
    "candidates.json",
    "evidence.json",
    "review_events.json",
    "claims.json",
    "claim_relationships.json",
    "bible_project_profiles.json",
    "bible_sections.json",
)


def available_demo_corpora() -> list[str]:
    root = _demo_corpora_root()
    if not root.exists():
        return []
    return sorted(item.name for item in root.iterdir() if (item / "manifest.json").exists())


def run_demo_corpus(corpus_id: str, *, data_dir: Path) -> DemoCorpusRunSummary:
    manifest, corpus_dir = load_demo_corpus_manifest(corpus_id)
    output_dir = data_dir.resolve()
    _reset_output_dir(output_dir)
    corpus = DemoCorpusAdapter(manifest, corpus_dir)

    source_store = FileSourceStore(output_dir)
    source_document_store = FileSourceDocumentStore(output_dir)
    text_unit_store = FileTextUnitStore(output_dir)
    run_store = FileExtractionRunStore(output_dir)
    candidate_store = FileCandidateStore(output_dir)
    evidence_store = FileEvidenceStore(output_dir)
    review_store = FileReviewStore(output_dir)
    truth_store = FileTruthStore(output_dir)
    bible_profile_store = FileBibleProjectProfileStore(output_dir)
    bible_section_store = FileBibleSectionStore(output_dir)

    intake_service = IntakeService(
        corpus=corpus,
        source_store=source_store,
        source_document_store=source_document_store,
    )
    intake_result = intake_service.intake_text(
        IntakeTextRequest(
            title=manifest.source.title,
            text=(
                f"Demo corpus manifest intake for {manifest.corpus_id}. "
                "The checked-in note and attachment documents are the discoverable evidence."
            ),
            source_type=manifest.source.source_type,
            notes=f"demo_corpus_id={manifest.corpus_id}",
        )
    )
    if not intake_result.pulled_sources:
        raise ValueError(f"Demo corpus '{manifest.corpus_id}' did not yield a pulled source.")
    source = intake_result.pulled_sources[0]

    normalization_service = NormalizationService(
        source_document_store=source_document_store,
        text_unit_store=text_unit_store,
        source_store=source_store,
    )
    normalization_service.normalize_documents(source_ids=[source.source_id])

    ingestion_service = IngestionService(
        corpus=corpus,
        extractor=HeuristicExtractionAdapter(),
        source_store=source_store,
        text_unit_store=text_unit_store,
        source_document_store=source_document_store,
        run_store=run_store,
        candidate_store=candidate_store,
        evidence_store=evidence_store,
    )
    extraction_output = ingestion_service.extract_candidates(source_ids=[source.source_id])

    review_service = ReviewService(
        candidate_store=candidate_store,
        truth_store=truth_store,
        review_store=review_store,
        evidence_store=evidence_store,
        source_store=source_store,
        text_unit_store=text_unit_store,
    )
    review_cards = review_service.list_review_queue()
    preview_card = next(
        (item for item in review_cards if item.primary_evidence is not None),
        review_cards[0] if review_cards else None,
    )
    approved_claim_ids: list[str] = []
    consumed_candidate_ids: set[str] = set()
    for approval in manifest.approvals:
        candidate = _find_candidate_for_approval(
            candidate_store.list_candidates(),
            approval,
            consumed_candidate_ids=consumed_candidate_ids,
        )
        approved = review_service.review_candidate(
            candidate.candidate_id,
            ReviewRequest(
                decision=ReviewDecision.APPROVE,
                override_status=approval.override_status,
                claim_patch=approval.claim_patch,
                notes=approval.notes,
            ),
            actor=AuthenticatedActor(actor_id="system-demo-corpus", role="operator"),
        )
        if approved is not None:
            consumed_candidate_ids.add(candidate.candidate_id)
            approved_claim_ids.append(approved.claim_id)

    bible_service = BibleWorkspaceService(
        profile_store=bible_profile_store,
        section_store=bible_section_store,
        truth_store=truth_store,
        evidence_store=evidence_store,
        source_store=source_store,
    )
    bible_service.save_profile(manifest.project_id, manifest.profile)
    section = bible_service.create_section(
        BibleSectionCreateRequest(
            project_id=manifest.project_id,
            section_type=manifest.section_type,
            filters=manifest.section_filters,
        )
    )

    summary = DemoCorpusRunSummary(
        corpus_id=manifest.corpus_id,
        corpus_name=manifest.corpus_name,
        data_dir=str(output_dir),
        source_id=source.source_id,
        source_document_count=len(source_document_store.list_source_documents()),
        text_unit_count=len(text_unit_store.list_text_units()),
        candidate_count=len(extraction_output.candidates),
        pending_candidate_count=len(candidate_store.list_candidates(review_state="pending")),
        approved_claim_count=len(truth_store.list_claims()),
        approved_claim_ids=approved_claim_ids,
        review_preview_candidate_id=preview_card.candidate_id if preview_card is not None else None,
        review_preview_excerpt=(
            preview_card.primary_evidence.excerpt
            if preview_card is not None and preview_card.primary_evidence is not None
            else None
        ),
        review_preview_span_start=(
            preview_card.primary_evidence.span_start
            if preview_card is not None and preview_card.primary_evidence is not None
            else None
        ),
        review_preview_span_end=(
            preview_card.primary_evidence.span_end
            if preview_card is not None and preview_card.primary_evidence is not None
            else None
        ),
        review_preview_locator=(
            preview_card.primary_evidence.locator
            if preview_card is not None and preview_card.primary_evidence is not None
            else None
        ),
        section_id=section.section_id,
        section_type=section.section_type,
        section_title=section.title,
        section_generation_status=section.generation_status,
        section_ready_for_writer=section.ready_for_writer,
        section_claim_ids=section.references.claim_ids,
        section_excerpt=_first_content_paragraph(section.content),
        summary_path="",
        markdown_path="",
    )

    summary_path = output_dir / "demo_summary.json"
    markdown_path = output_dir / f"{manifest.section_type.value}.md"
    markdown_path.write_text(section.content, encoding="utf-8")
    summary.summary_path = str(summary_path)
    summary.markdown_path = str(markdown_path)
    summary_path.write_text(json.dumps(summary.model_dump(mode="json"), indent=2), encoding="utf-8")
    return summary


def load_demo_corpus_manifest(corpus_id: str) -> tuple[DemoCorpusManifest, Path]:
    corpus_dir = _demo_corpora_root() / corpus_id
    manifest_path = corpus_dir / "manifest.json"
    if not manifest_path.exists():
        available = ", ".join(available_demo_corpora()) or "none"
        raise ValueError(f"Unknown demo corpus '{corpus_id}'. Available demo corpora: {available}.")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    return DemoCorpusManifest.model_validate(payload), corpus_dir


def _demo_corpora_root() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "demo" / "corpora"


def _reset_output_dir(data_dir: Path) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    for filename in _DATA_FILES:
        path = data_dir / filename
        if path.exists():
            path.unlink()


def _build_source_document_record(
    manifest: DemoCorpusManifest,
    corpus_dir: Path,
    source_id: str,
    spec: DemoCorpusDocumentSpec,
) -> SourceDocumentRecord:
    path = corpus_dir / spec.path
    raw_text = path.read_text(encoding="utf-8").strip()
    checksum = sha1(raw_text.encode("utf-8")).hexdigest()
    return SourceDocumentRecord(
        document_id=spec.document_id,
        source_id=source_id,
        document_kind=spec.document_kind,
        external_id=f"{manifest.corpus_id}:{spec.document_id}",
        filename=spec.filename or path.name,
        mime_type=spec.mime_type,
        storage_path=str(path.resolve()) if spec.document_kind == "attachment" else None,
        metadata_import_status="imported",
        attachment_discovery_status=(
            "discovered" if spec.document_kind == "attachment" else "not_applicable"
        ),
        attachment_fetch_status=(
            "fetched" if spec.document_kind == "attachment" else "not_applicable"
        ),
        text_extraction_status="extracted",
        normalization_status="queued",
        content_checksum=checksum,
        locator=spec.locator,
        raw_text=raw_text,
        raw_metadata_json={
            "demo_corpus_id": manifest.corpus_id,
            "relative_path": spec.path,
        },
    )


def _find_candidate_for_approval(
    candidates,
    approval: DemoCorpusApprovalSpec,
    *,
    consumed_candidate_ids: set[str] | None = None,
):
    matches = []
    consumed_candidate_ids = consumed_candidate_ids or set()
    for candidate in candidates:
        if candidate.candidate_id in consumed_candidate_ids:
            continue
        if approval.predicate is not None and candidate.predicate != approval.predicate:
            continue
        if approval.subject_contains is not None and (
            approval.subject_contains.lower() not in candidate.subject.lower()
        ):
            continue
        if approval.value_contains.lower() not in candidate.value.lower():
            continue
        matches.append(candidate)
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise ValueError(
            "Approval rule matched multiple candidates; make it more specific "
            f"(value_contains={approval.value_contains!r})."
        )
    raise ValueError(
        "Could not find a candidate matching approval rule "
        f"value_contains={approval.value_contains!r}."
    )


def _first_content_paragraph(content: str) -> str:
    for block in content.split("\n\n"):
        stripped = block.strip()
        if stripped and not stripped.startswith("#"):
            return stripped
    return content.strip()
