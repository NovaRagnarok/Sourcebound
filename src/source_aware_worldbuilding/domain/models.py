from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from source_aware_worldbuilding.domain.enums import (
    BibleSectionGenerationStatus,
    BibleSectionType,
    BibleTone,
    ClaimKind,
    ClaimStatus,
    ExtractionRunStatus,
    JobStatus,
    QueryMode,
    ResearchCoverageStatus,
    ResearchFetchOutcome,
    ResearchFindingDecision,
    ResearchFindingReason,
    ResearchRunStatus,
    ReviewDecision,
    ReviewState,
)


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


SourceWorkflowStage = Literal[
    "metadata_imported",
    "attachment_discovered",
    "attachment_fetched",
    "text_extracted",
    "normalization_queued",
    "normalization_completed",
    "attention_required",
]
MetadataImportStatus = Literal["imported", "failed"]
AttachmentDiscoveryStatus = Literal["not_applicable", "discovered", "missing"]
AttachmentFetchStatus = Literal["not_applicable", "pending", "fetched", "failed"]
TextExtractionStatus = Literal["not_applicable", "pending", "extracted", "failed"]
NormalizationStatus = Literal["not_applicable", "queued", "completed", "failed"]
SourceDocumentKind = Literal["attachment", "note", "snapshot", "manual_text"]
ClaimExtractionStatus = Literal["queued", "ready", "running", "completed", "failed"]
QueryRankingStrategy = Literal["lexical", "blended", "projection_only", "intent_blended"]
QueryAnswerBoundary = Literal["direct_answer", "adjacent_context", "research_gap"]
ClaimRelationshipType = Literal["supports", "contradicts", "supersedes", "superseded_by"]
ClaimRelationshipSourceKind = Literal["derived", "manual"]
ClaimClusterKind = Literal["reinforcing", "contested", "supersession"]
BibleParagraphRole = Literal[
    "descriptive_synthesis",
    "interpretive_synthesis",
    "uncertainty_framing",
    "writer_guidance",
]
BibleParagraphProvenanceScope = Literal[
    "canon_support",
    "contested_context",
    "author_guidance",
]
JobType = Literal[
    "research_run_create",
    "research_run_stage",
    "research_run_extract",
    "bible_section_compose",
    "bible_section_regenerate",
    "bible_project_export",
]
JobWorkerState = Literal[
    "queued",
    "running",
    "cancel_requested",
    "stalled",
    "completed",
    "failed",
    "cancelled",
    "partial",
]


def _legacy_source_workflow_stage(sync_status: str) -> SourceWorkflowStage:
    mapping: dict[str, SourceWorkflowStage] = {
        "imported": "metadata_imported",
        "attachments_missing": "attention_required",
        "awaiting_text_extraction": "text_extracted",
        "ready_for_extraction": "normalization_completed",
        "extraction_failed": "attention_required",
    }
    return mapping.get(sync_status, "metadata_imported")


def _legacy_document_stage_defaults(
    *,
    document_kind: str,
    ingest_status: str,
    raw_text_status: str,
    claim_extraction_status: str,
    storage_path: str | None,
    raw_text: str | None,
) -> tuple[
    AttachmentDiscoveryStatus,
    AttachmentFetchStatus,
    TextExtractionStatus,
    NormalizationStatus,
]:
    if document_kind in {"note", "manual_text"}:
        discovery_status: AttachmentDiscoveryStatus = "not_applicable"
        fetch_status: AttachmentFetchStatus = "not_applicable"
    else:
        discovery_status = "discovered"
        fetch_status = "pending"

    if ingest_status == "attachments_missing":
        discovery_status = "missing"
    elif storage_path:
        fetch_status = "fetched"

    if raw_text_status == "ready":
        extraction_status: TextExtractionStatus = "extracted"
    elif raw_text_status == "failed" or ingest_status == "extraction_failed":
        extraction_status = "failed"
    elif raw_text_status == "queued":
        extraction_status = "pending"
    else:
        extraction_status = "pending" if document_kind not in {"note", "manual_text"} else "pending"

    if raw_text and raw_text.strip():
        extraction_status = "extracted"

    if claim_extraction_status in {"ready", "completed"} or ingest_status == "ready_for_extraction":
        normalization_status: NormalizationStatus = "completed"
    elif claim_extraction_status == "failed":
        normalization_status = "failed"
    else:
        normalization_status = "queued"

    if document_kind in {"note", "manual_text"} and not raw_text and extraction_status != "failed":
        extraction_status = "pending"

    return discovery_status, fetch_status, extraction_status, normalization_status


class SourceRecord(BaseModel):
    source_id: str
    external_source: str = "zotero"
    external_id: str | None = None
    title: str
    author: str | None = None
    year: str | None = None
    source_type: str = "document"
    locator_hint: str | None = None
    zotero_item_key: str | None = None
    collection_key: str | None = None
    abstract: str | None = None
    url: str | None = None
    sync_status: Literal[
        "imported",
        "attachments_missing",
        "awaiting_text_extraction",
        "ready_for_extraction",
        "extraction_failed",
    ] = "imported"
    workflow_stage: SourceWorkflowStage = "metadata_imported"
    last_synced_at: str | None = None
    last_zotero_version: str | None = None
    last_source_checksum: str | None = None
    stage_errors: list[str] = Field(default_factory=list)
    stage_summary: dict[str, int] = Field(default_factory=dict)
    raw_metadata_json: dict | None = None

    @model_validator(mode="after")
    def derive_compatibility_fields(self) -> SourceRecord:
        if self.last_synced_at is None:
            self.last_synced_at = utc_now()
        if not self.workflow_stage or (
            self.workflow_stage == "metadata_imported" and self.sync_status != "imported"
        ):
            self.workflow_stage = _legacy_source_workflow_stage(self.sync_status)
        if self.workflow_stage == "attention_required":
            if self.sync_status not in {"attachments_missing", "extraction_failed"}:
                self.sync_status = (
                    "extraction_failed"
                    if any("extract" in error.lower() for error in self.stage_errors)
                    else "attachments_missing"
                )
        elif self.workflow_stage in {"normalization_queued", "normalization_completed"}:
            self.sync_status = "ready_for_extraction"
        elif self.workflow_stage in {"text_extracted", "attachment_fetched"}:
            self.sync_status = "awaiting_text_extraction"
        else:
            self.sync_status = "imported"
        return self


class SourceDocumentRecord(BaseModel):
    document_id: str
    source_id: str
    document_kind: SourceDocumentKind
    external_id: str | None = None
    filename: str | None = None
    mime_type: str | None = None
    storage_path: str | None = None
    ingest_status: Literal[
        "imported",
        "attachments_missing",
        "awaiting_text_extraction",
        "ready_for_extraction",
        "extraction_failed",
    ] = "imported"
    raw_text_status: Literal["missing", "queued", "ready", "failed"] = "missing"
    claim_extraction_status: ClaimExtractionStatus = "queued"
    metadata_import_status: MetadataImportStatus = "imported"
    attachment_discovery_status: AttachmentDiscoveryStatus = "not_applicable"
    attachment_fetch_status: AttachmentFetchStatus = "not_applicable"
    text_extraction_status: TextExtractionStatus = "pending"
    normalization_status: NormalizationStatus = "queued"
    content_checksum: str | None = None
    last_synced_at: str | None = None
    present_in_latest_pull: bool = True
    stage_errors: list[str] = Field(default_factory=list)
    zotero_parent_item_key: str | None = None
    zotero_child_item_key: str | None = None
    locator: str | None = None
    raw_text: str | None = None
    raw_metadata_json: dict | None = None

    @model_validator(mode="after")
    def derive_stage_and_compatibility_fields(self) -> SourceDocumentRecord:
        if self.last_synced_at is None:
            self.last_synced_at = utc_now()
        (
            discovery_status,
            fetch_status,
            extraction_status,
            normalization_status,
        ) = _legacy_document_stage_defaults(
            document_kind=self.document_kind,
            ingest_status=self.ingest_status,
            raw_text_status=self.raw_text_status,
            claim_extraction_status=self.claim_extraction_status,
            storage_path=self.storage_path,
            raw_text=self.raw_text,
        )

        if self.document_kind in {"note", "manual_text"}:
            discovery_status = "not_applicable"
            fetch_status = "not_applicable"
        elif (self.raw_metadata_json or {}).get("local_only"):
            fetch_status = "not_applicable"
        elif self.attachment_discovery_status == "missing":
            discovery_status = "missing"
        elif self.attachment_discovery_status == "discovered":
            discovery_status = "discovered"

        if self.attachment_fetch_status in {"pending", "fetched", "failed"}:
            fetch_status = self.attachment_fetch_status
        if self.text_extraction_status in {"extracted", "failed", "not_applicable"}:
            extraction_status = self.text_extraction_status
        if self.normalization_status in {"completed", "failed", "not_applicable"}:
            normalization_status = self.normalization_status
        elif self.claim_extraction_status in {"ready", "completed"}:
            normalization_status = "completed"
        elif self.claim_extraction_status == "failed":
            normalization_status = "failed"

        if self.metadata_import_status == "failed":
            self.ingest_status = "extraction_failed"
        elif discovery_status == "missing" or not self.present_in_latest_pull:
            self.ingest_status = "attachments_missing"
        elif fetch_status == "failed" or extraction_status == "failed":
            self.ingest_status = "extraction_failed"
        elif normalization_status == "completed":
            self.ingest_status = "ready_for_extraction"
        elif extraction_status == "extracted" or fetch_status == "fetched":
            self.ingest_status = "awaiting_text_extraction"
        else:
            self.ingest_status = "imported"

        self.attachment_discovery_status = discovery_status
        self.attachment_fetch_status = fetch_status
        self.text_extraction_status = extraction_status
        self.normalization_status = normalization_status
        self.raw_text_status = (
            "ready"
            if extraction_status == "extracted"
            else "failed"
            if extraction_status == "failed"
            else "queued"
            if extraction_status == "pending" and self.attachment_discovery_status != "missing"
            else "missing"
        )
        if self.claim_extraction_status not in {"running", "completed"}:
            self.claim_extraction_status = (
                "failed"
                if normalization_status == "failed"
                else "ready"
                if normalization_status == "completed"
                else "queued"
            )
        return self


def summarize_source_documents(source_documents: list[SourceDocumentRecord]) -> dict[str, int]:
    summary = {
        "total": len(source_documents),
        "present": 0,
        "missing": 0,
        "fetched": 0,
        "extracted": 0,
        "normalized": 0,
        "failed": 0,
    }
    for document in source_documents:
        if document.present_in_latest_pull:
            summary["present"] += 1
        if not document.present_in_latest_pull or document.attachment_discovery_status == "missing":
            summary["missing"] += 1
        if document.attachment_fetch_status == "fetched":
            summary["fetched"] += 1
        if document.text_extraction_status == "extracted":
            summary["extracted"] += 1
        if document.normalization_status == "completed":
            summary["normalized"] += 1
        if (
            document.metadata_import_status == "failed"
            or document.attachment_fetch_status == "failed"
            or document.text_extraction_status == "failed"
            or document.normalization_status == "failed"
        ):
            summary["failed"] += 1
    return summary


def derive_source_workflow_stage(
    source_documents: list[SourceDocumentRecord],
) -> SourceWorkflowStage:
    if not source_documents:
        return "metadata_imported"
    if any(
        not document.present_in_latest_pull
        or document.attachment_discovery_status == "missing"
        or document.attachment_fetch_status == "failed"
        or document.text_extraction_status == "failed"
        or document.normalization_status == "failed"
        or document.metadata_import_status == "failed"
        for document in source_documents
    ):
        return "attention_required"
    if all(document.normalization_status == "completed" for document in source_documents):
        return "normalization_completed"
    if any(document.normalization_status == "queued" for document in source_documents):
        return "normalization_queued"
    if all(
        document.text_extraction_status == "extracted"
        for document in source_documents
        if document.text_extraction_status != "not_applicable"
    ):
        return "text_extracted"
    if any(document.attachment_fetch_status == "fetched" for document in source_documents):
        return "attachment_fetched"
    if any(document.attachment_discovery_status == "discovered" for document in source_documents):
        return "attachment_discovered"
    return "metadata_imported"


def sync_source_with_documents(
    source: SourceRecord,
    source_documents: list[SourceDocumentRecord],
) -> SourceRecord:
    summary = summarize_source_documents(source_documents)
    errors: list[str] = []
    for document in source_documents:
        errors.extend(document.stage_errors)
    source.stage_summary = summary
    source.stage_errors = list(dict.fromkeys(errors))
    source.workflow_stage = derive_source_workflow_stage(source_documents)
    source.last_synced_at = utc_now()
    return source


class ZoteroCreatedItem(BaseModel):
    zotero_item_key: str
    parent_item_key: str | None = None
    title: str
    item_type: str
    collection_key: str | None = None
    url: str | None = None


class TextUnit(BaseModel):
    text_unit_id: str
    source_id: str
    locator: str
    text: str
    ordinal: int = 0
    checksum: str | None = None
    notes: str | None = None


class EvidenceSnippet(BaseModel):
    evidence_id: str
    source_id: str
    locator: str
    text: str
    text_unit_id: str | None = None
    span_start: int | None = None
    span_end: int | None = None
    notes: str | None = None
    checksum: str | None = None


ReviewEvidenceQuality = Literal["supported", "thin", "blind"]
ReviewWeaknessReason = Literal[
    "missing_evidence",
    "missing_span_context",
    "short_excerpt",
    "single_snippet",
    "missing_locator",
]
ReviewDeferState = Literal["needs_split", "needs_edit"]


class CandidateClaim(BaseModel):
    candidate_id: str
    subject: str
    predicate: str
    value: str
    claim_kind: ClaimKind
    status_suggestion: ClaimStatus
    review_state: ReviewState = ReviewState.PENDING
    place: str | None = None
    time_start: str | None = None
    time_end: str | None = None
    viewpoint_scope: str | None = None
    evidence_ids: list[str] = Field(default_factory=list)
    extractor_run_id: str | None = None
    notes: str | None = None


class ApprovedClaim(BaseModel):
    claim_id: str
    subject: str
    predicate: str
    value: str
    claim_kind: ClaimKind
    status: ClaimStatus
    place: str | None = None
    time_start: str | None = None
    time_end: str | None = None
    viewpoint_scope: str | None = None
    author_choice: bool = False
    evidence_ids: list[str] = Field(default_factory=list)
    created_from_run_id: str | None = None
    notes: str | None = None


class ExtractionRun(BaseModel):
    run_id: str
    status: ExtractionRunStatus = ExtractionRunStatus.PENDING
    source_count: int = 0
    text_unit_count: int = 0
    candidate_count: int = 0
    started_at: str = Field(default_factory=utc_now)
    completed_at: str | None = None
    error: str | None = None
    notes: str | None = None


class ReviewEvent(BaseModel):
    review_id: str
    candidate_id: str
    decision: ReviewDecision
    reviewed_at: str = Field(default_factory=utc_now)
    override_status: ClaimStatus | None = None
    notes: str | None = None
    approved_claim_id: str | None = None


class ReviewClaimPatch(BaseModel):
    subject: str | None = None
    predicate: str | None = None
    value: str | None = None
    place: str | None = None
    time_start: str | None = None
    time_end: str | None = None
    viewpoint_scope: str | None = None

    @field_validator("*", mode="before")
    @classmethod
    def normalize_blank_strings(cls, value):
        if isinstance(value, str):
            value = value.strip()
            return value or None
        return value


class ReviewEvidencePreview(BaseModel):
    evidence_id: str
    source_id: str
    source_title: str
    source_type: str | None = None
    locator: str | None = None
    excerpt: str
    context_before: str = ""
    context_after: str = ""
    span_start: int | None = None
    span_end: int | None = None
    text_unit_id: str | None = None
    notes: str | None = None


class ReviewQueueCard(CandidateClaim):
    claim_text: str
    certainty_suggestion: ClaimStatus
    location_summary: str | None = None
    evidence_quality: ReviewEvidenceQuality
    weakness_reasons: list[ReviewWeaknessReason] = Field(default_factory=list)
    primary_evidence: ReviewEvidencePreview | None = None
    extra_evidence_count: int = 0
    evidence_items: list[ReviewEvidencePreview] = Field(default_factory=list)


class ExtractionOutput(BaseModel):
    run: ExtractionRun
    candidates: list[CandidateClaim] = Field(default_factory=list)
    evidence: list[EvidenceSnippet] = Field(default_factory=list)


class IntakeTextRequest(BaseModel):
    title: str
    text: str
    author: str | None = None
    year: str | None = None
    source_type: str = "document"
    notes: str | None = None
    collection_key: str | None = None


class IntakeUrlRequest(BaseModel):
    url: str
    title: str | None = None
    notes: str | None = None
    collection_key: str | None = None


class ZoteroPullRequest(BaseModel):
    item_keys: list[str] = Field(default_factory=list)
    source_ids: list[str] = Field(default_factory=list)
    force_refresh: bool = False


class ZoteroPullResult(BaseModel):
    count: int = 0
    sources: list[SourceRecord] = Field(default_factory=list)
    source_documents: list[SourceDocumentRecord] = Field(default_factory=list)
    inserted_source_count: int = 0
    updated_source_count: int = 0
    unchanged_source_count: int = 0
    inserted_document_count: int = 0
    updated_document_count: int = 0
    unchanged_document_count: int = 0
    failed_document_count: int = 0
    warnings: list[str] = Field(default_factory=list)


class NormalizeDocumentsRequest(BaseModel):
    source_ids: list[str] = Field(default_factory=list)
    document_ids: list[str] = Field(default_factory=list)
    retry_failed: bool = False


class ExtractCandidatesRequest(BaseModel):
    source_ids: list[str] = Field(default_factory=list)


class SourceDetailResponse(BaseModel):
    source: SourceRecord
    source_documents: list[SourceDocumentRecord] = Field(default_factory=list)
    text_units: list[TextUnit] = Field(default_factory=list)
    stage_summary: dict[str, int] = Field(default_factory=dict)
    stage_errors: list[str] = Field(default_factory=list)


class IntakeResult(BaseModel):
    created_item: ZoteroCreatedItem
    pulled_sources: list[SourceRecord] = Field(default_factory=list)
    source_documents: list[SourceDocumentRecord] = Field(default_factory=list)
    pulled_text_units: list[TextUnit] = Field(default_factory=list)
    extraction_run: ExtractionRun | None = None
    candidate_count: int = 0
    evidence_count: int = 0
    warnings: list[str] = Field(default_factory=list)


class ReviewRequest(BaseModel):
    decision: ReviewDecision
    override_status: ClaimStatus | None = None
    claim_patch: ReviewClaimPatch | None = None
    defer_state: ReviewDeferState | None = None
    notes: str | None = None

    @model_validator(mode="after")
    def validate_review_shape(self) -> ReviewRequest:
        if self.defer_state is not None and self.decision != ReviewDecision.REJECT:
            raise ValueError("defer_state can only be used with reject decisions")
        if self.claim_patch is not None and self.decision != ReviewDecision.APPROVE:
            raise ValueError("claim_patch can only be used with approve decisions")
        return self


class QueryFilter(BaseModel):
    status: ClaimStatus | None = None
    include_statuses: list[ClaimStatus] = Field(default_factory=list)
    claim_kind: ClaimKind | None = None
    place: str | None = None
    viewpoint_scope: str | None = None
    source_types: list[str] = Field(default_factory=list)
    time_start: str | None = None
    time_end: str | None = None
    relationship_types: list[Literal["supports", "contradicts", "supersedes", "superseded_by"]] = (
        Field(default_factory=list)
    )


class QueryRequest(BaseModel):
    question: str
    mode: QueryMode = QueryMode.STRICT_FACTS
    project_id: str | None = None
    filters: QueryFilter | None = None


class LorePacketRequest(BaseModel):
    project_name: str
    focus: str | None = None
    files: list[Literal["basic-lore.md", "characters.md", "timeline.md", "notes.md"]] | None = None
    filters: QueryFilter | None = None
    include_statuses: list[ClaimStatus] | None = None
    include_evidence_footnotes: bool = True


class LorePacketFile(BaseModel):
    filename: str
    content: str
    claim_ids: list[str] = Field(default_factory=list)
    source_ids: list[str] = Field(default_factory=list)


class LorePacketMetadata(BaseModel):
    claim_count: int = 0
    source_count: int = 0
    evidence_count: int = 0


class LorePacketResponse(BaseModel):
    project_name: str
    generated_at: str = Field(default_factory=utc_now)
    focus: str | None = None
    filters: QueryFilter | None = None
    files: list[LorePacketFile] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    metadata: LorePacketMetadata = Field(default_factory=LorePacketMetadata)


class QueryResultMetadata(BaseModel):
    retrieval_backend: Literal["memory", "qdrant"] = "memory"
    fallback_used: bool = False
    fallback_reason: str | None = None
    ranking_strategy: QueryRankingStrategy = "lexical"
    answer_boundary: QueryAnswerBoundary = "research_gap"
    retrieval_quality_tier: Literal["projection", "memory_ranked"] = "memory_ranked"
    used_nearby_context: bool = False


class ProjectionSearchResult(BaseModel):
    claim_ids: list[str] = Field(default_factory=list)
    retrieval_backend: Literal["qdrant"] = "qdrant"
    fallback_used: bool = False
    fallback_reason: str | None = None


class ResearchSemanticMatch(BaseModel):
    finding_id: str
    similarity: float
    title: str
    canonical_url: str | None = None
    decision: str | None = None


class ResearchSemanticResult(BaseModel):
    matches: list[ResearchSemanticMatch] = Field(default_factory=list)
    retrieval_backend: Literal["qdrant"] = "qdrant"
    fallback_used: bool = False
    fallback_reason: str | None = None


class ResearchSemanticTelemetry(BaseModel):
    backend: str | None = None
    fallback_used: bool = False
    fallback_reason: str | None = None
    vectors_upserted: int = 0
    comparisons_performed: int = 0
    duplicate_hints_emitted: int = 0


class ClaimRelationship(BaseModel):
    relationship_id: str
    claim_id: str
    related_claim_id: str
    relationship_type: ClaimRelationshipType
    source_kind: ClaimRelationshipSourceKind = "derived"
    notes: str | None = None


class ClaimRelationshipRequest(BaseModel):
    related_claim_id: str
    relationship_type: ClaimRelationshipType
    notes: str | None = None


class ClaimCluster(BaseModel):
    cluster_id: str
    lead_claim_id: str
    claim_ids: list[str] = Field(default_factory=list)
    relationship_types: list[ClaimRelationshipType] = Field(default_factory=list)
    cluster_kind: ClaimClusterKind
    summary: str


class AnswerSection(BaseModel):
    cluster_id: str
    heading: str
    text: str
    claim_ids: list[str] = Field(default_factory=list)
    cluster_kind: ClaimClusterKind


class QueryResult(BaseModel):
    question: str
    mode: QueryMode
    answer: str
    supporting_claims: list[ApprovedClaim] = Field(default_factory=list)
    nearby_claims: list[ApprovedClaim] = Field(default_factory=list)
    related_claims: list[ClaimRelationship] = Field(default_factory=list)
    claim_clusters: list[ClaimCluster] = Field(default_factory=list)
    answer_sections: list[AnswerSection] = Field(default_factory=list)
    evidence: list[EvidenceSnippet] = Field(default_factory=list)
    sources: list[SourceRecord] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    certainty_summary: dict[str, int] = Field(default_factory=dict)
    coverage_gaps: list[str] = Field(default_factory=list)
    contradiction_flags: list[str] = Field(default_factory=list)
    recommended_next_research: list[str] = Field(default_factory=list)
    suggested_follow_ups: list[str] = Field(default_factory=list)
    direct_match_claim_ids: list[str] = Field(default_factory=list)
    adjacent_context_claim_ids: list[str] = Field(default_factory=list)
    metadata: QueryResultMetadata = Field(default_factory=QueryResultMetadata)


class WorkspaceAction(BaseModel):
    action_id: str
    title: str
    summary: str
    screen: Literal["workspace", "bible", "research", "review", "ask", "sources", "runs", "claims"]
    tone: Literal["verified", "probable", "contested", "queued", "author_choice"] = "queued"
    badge: str | None = None
    command: str | None = None


class WorkspaceBackgroundItem(BaseModel):
    item_id: str
    title: str
    summary: str
    status_label: str
    screen: Literal["runs", "research", "bible"] = "runs"


class WorkspaceSectionSnapshot(BaseModel):
    section_id: str
    title: str
    section_type: BibleSectionType
    generation_status: BibleSectionGenerationStatus
    ready_for_writer: bool = False
    has_manual_edits: bool = False
    claim_count: int = 0
    source_count: int = 0
    evidence_count: int = 0
    summary: str
    coverage_gaps: list[str] = Field(default_factory=list)
    recommended_next_research: list[str] = Field(default_factory=list)
    latest_job: JobSummary | None = None


class WorkspaceSummary(BaseModel):
    project: BibleProjectProfile | None = None
    current_section: WorkspaceSectionSnapshot | None = None
    next_actions: list[WorkspaceAction] = Field(default_factory=list)
    background_items: list[WorkspaceBackgroundItem] = Field(default_factory=list)
    pending_review_count: int = 0
    reviewed_canon_count: int = 0
    bible_section_count: int = 0
    evidence_count: int = 0
    active_background_count: int = 0
    thin_section_count: int = 0


class BibleCompositionDefaults(BaseModel):
    include_statuses: list[ClaimStatus] = Field(
        default_factory=lambda: [ClaimStatus.VERIFIED, ClaimStatus.PROBABLE]
    )
    source_types: list[str] = Field(default_factory=list)
    focus: str | None = None


class BibleProjectProfile(BaseModel):
    project_id: str
    project_name: str
    era: str | None = None
    time_start: str | None = None
    time_end: str | None = None
    geography: str | None = None
    social_lens: str | None = None
    narrative_focus: str | None = None
    taboo_topics: list[str] = Field(default_factory=list)
    desired_facets: list[str] = Field(default_factory=list)
    tone: BibleTone = BibleTone.GROUNDED_LITERARY
    composition_defaults: BibleCompositionDefaults = Field(default_factory=BibleCompositionDefaults)
    created_at: str = Field(default_factory=utc_now)
    updated_at: str = Field(default_factory=utc_now)


class BibleProjectProfileUpdateRequest(BaseModel):
    project_name: str
    era: str | None = None
    time_start: str | None = None
    time_end: str | None = None
    geography: str | None = None
    social_lens: str | None = None
    narrative_focus: str | None = None
    taboo_topics: list[str] = Field(default_factory=list)
    desired_facets: list[str] = Field(default_factory=list)
    tone: BibleTone = BibleTone.GROUNDED_LITERARY
    composition_defaults: BibleCompositionDefaults = Field(default_factory=BibleCompositionDefaults)


class BibleSectionReference(BaseModel):
    claim_ids: list[str] = Field(default_factory=list)
    evidence_ids: list[str] = Field(default_factory=list)
    source_ids: list[str] = Field(default_factory=list)
    certainty_buckets: list[ClaimStatus] = Field(default_factory=list)


class BibleCoverageBucket(BaseModel):
    label: str
    count: int = 0


class BibleCoverageAnalysis(BaseModel):
    desired_facets: list[str] = Field(default_factory=list)
    facet_distribution: dict[str, int] = Field(default_factory=dict)
    missing_facets: list[str] = Field(default_factory=list)
    certainty_mix: dict[str, int] = Field(default_factory=dict)
    time_coverage: list[BibleCoverageBucket] = Field(default_factory=list)
    place_coverage: list[BibleCoverageBucket] = Field(default_factory=list)
    missing_named_actors: bool = False
    missing_material_detail: bool = False
    missing_dated_anchors: bool = False
    diagnostic_summary: str | None = None


class BibleSectionParagraph(BaseModel):
    paragraph_id: str
    heading: str | None = None
    text: str
    paragraph_kind: str = "summary"
    paragraph_role: BibleParagraphRole | None = None
    claim_ids: list[str] = Field(default_factory=list)
    evidence_ids: list[str] = Field(default_factory=list)
    source_ids: list[str] = Field(default_factory=list)
    contradiction_flags: list[str] = Field(default_factory=list)
    supersession_flags: list[str] = Field(default_factory=list)


class BibleSectionCompositionMetrics(BaseModel):
    thin_section: bool = False
    target_beats: int = 0
    produced_beats: int = 0
    skipped_beat_ids: list[str] = Field(default_factory=list)
    skipped_reasons: list[str] = Field(default_factory=list)
    claim_density: float = 0.0
    evidence_density: float = 0.0
    contradiction_presence: bool = False


class BibleParagraphProvenance(BaseModel):
    paragraph: BibleSectionParagraph
    claims: list[ApprovedClaim] = Field(default_factory=list)
    evidence: list[EvidenceSnippet] = Field(default_factory=list)
    sources: list[SourceRecord] = Field(default_factory=list)
    contradiction_context: list[str] = Field(default_factory=list)
    supersession_context: list[str] = Field(default_factory=list)
    provenance_scope: BibleParagraphProvenanceScope = "canon_support"
    why_this_paragraph_exists: str | None = None
    claim_details: list[dict[str, object]] = Field(default_factory=list)
    evidence_details: list[dict[str, object]] = Field(default_factory=list)
    contradiction_details: list[dict[str, object]] = Field(default_factory=list)
    supersession_details: list[dict[str, object]] = Field(default_factory=list)


class BibleSectionProvenanceDetail(BaseModel):
    section_id: str
    title: str
    section_type: BibleSectionType
    references: BibleSectionReference = Field(default_factory=BibleSectionReference)
    paragraphs: list[BibleParagraphProvenance] = Field(default_factory=list)
    relationships: list[str] = Field(default_factory=list)


class BibleSectionFilters(BaseModel):
    focus: str | None = None
    statuses: list[ClaimStatus] = Field(default_factory=list)
    source_types: list[str] = Field(default_factory=list)
    place: str | None = None
    viewpoint_scope: str | None = None
    claim_kind: ClaimKind | None = None
    time_start: str | None = None
    time_end: str | None = None
    relationship_types: list[Literal["supports", "contradicts", "supersedes", "superseded_by"]] = (
        Field(default_factory=list)
    )


class BibleSectionDraft(BaseModel):
    section_type: BibleSectionType
    title: str
    generated_markdown: str
    paragraphs: list[BibleSectionParagraph] = Field(default_factory=list)
    references: BibleSectionReference = Field(default_factory=BibleSectionReference)
    certainty_summary: dict[str, int] = Field(default_factory=dict)
    coverage_gaps: list[str] = Field(default_factory=list)
    contradiction_flags: list[str] = Field(default_factory=list)
    recommended_next_research: list[str] = Field(default_factory=list)
    coverage_analysis: BibleCoverageAnalysis = Field(default_factory=BibleCoverageAnalysis)
    retrieval_metadata: dict[str, object] = Field(default_factory=dict)
    composition_metrics: BibleSectionCompositionMetrics = Field(
        default_factory=BibleSectionCompositionMetrics
    )
    generation_status: BibleSectionGenerationStatus = BibleSectionGenerationStatus.THIN
    generation_error: str | None = None
    ready_for_writer: bool = False


class BibleSection(BaseModel):
    section_id: str
    project_id: str
    section_type: BibleSectionType
    title: str
    content: str
    generated_markdown: str
    manual_markdown: str | None = None
    paragraphs: list[BibleSectionParagraph] = Field(default_factory=list)
    generation_filters: BibleSectionFilters = Field(default_factory=BibleSectionFilters)
    references: BibleSectionReference = Field(default_factory=BibleSectionReference)
    certainty_summary: dict[str, int] = Field(default_factory=dict)
    coverage_gaps: list[str] = Field(default_factory=list)
    contradiction_flags: list[str] = Field(default_factory=list)
    recommended_next_research: list[str] = Field(default_factory=list)
    coverage_analysis: BibleCoverageAnalysis = Field(default_factory=BibleCoverageAnalysis)
    retrieval_metadata: dict[str, object] = Field(default_factory=dict)
    composition_metrics: BibleSectionCompositionMetrics = Field(
        default_factory=BibleSectionCompositionMetrics
    )
    generation_status: BibleSectionGenerationStatus = BibleSectionGenerationStatus.QUEUED
    generation_error: str | None = None
    ready_for_writer: bool = False
    has_manual_edits: bool = False
    latest_job: JobSummary | None = None
    created_at: str = Field(default_factory=utc_now)
    updated_at: str = Field(default_factory=utc_now)
    last_generated_at: str | None = None
    last_edited_at: str | None = None


class BibleSectionCreateRequest(BaseModel):
    project_id: str
    section_type: BibleSectionType
    title: str | None = None
    filters: BibleSectionFilters = Field(default_factory=BibleSectionFilters)


class BibleSectionUpdateRequest(BaseModel):
    title: str | None = None
    content: str


class BibleSectionRegenerateRequest(BaseModel):
    filters: BibleSectionFilters | None = None


class BibleProjectExportResponse(BaseModel):
    profile: BibleProjectProfile
    sections: list[BibleSection] = Field(default_factory=list)
    generated_at: str = Field(default_factory=utc_now)


class JobResultRef(BaseModel):
    run_id: str | None = None
    section_id: str | None = None
    project_id: str | None = None


class JobRecord(BaseModel):
    job_id: str
    job_type: JobType
    status: JobStatus = JobStatus.PENDING
    status_label: str | None = None
    completion_state: Literal["completed", "partial"] | None = None
    payload: dict[str, object] = Field(default_factory=dict)
    result_payload: dict[str, object] | None = None
    progress_stage: str = "queued"
    progress_current: int = 0
    progress_total: int = 100
    progress_message: str | None = None
    result_ref: JobResultRef = Field(default_factory=JobResultRef)
    error: str | None = None
    error_code: str | None = None
    error_detail: str | None = None
    warnings: list[str] = Field(default_factory=list)
    worker_state: JobWorkerState | None = None
    stalled_reason: str | None = None
    degraded_reason: str | None = None
    retryable: bool = False
    attempt_count: int = 1
    max_attempts: int = 1
    retry_of_job_id: str | None = None
    cancel_requested_at: str | None = None
    last_heartbeat_at: str | None = None
    last_checkpoint_at: str | None = None
    created_at: str = Field(default_factory=utc_now)
    started_at: str | None = None
    completed_at: str | None = None
    updated_at: str = Field(default_factory=utc_now)

    @model_validator(mode="after")
    def populate_status_label(self) -> JobRecord:
        if not self.status_label:
            if self.completion_state == "partial":
                self.status_label = "partial"
            elif self.status == JobStatus.PARTIAL:
                self.status_label = "partial"
            elif self.status == JobStatus.PENDING:
                self.status_label = "queued"
            else:
                self.status_label = self.status.value
        if not self.worker_state:
            if self.status_label in {"queued", "pending"}:
                self.worker_state = "queued"
            elif self.status_label == "running":
                self.worker_state = "running"
            elif self.status_label == "partial":
                self.worker_state = "partial"
            elif self.status_label == "cancelled":
                self.worker_state = "cancelled"
            elif self.status_label == "failed":
                self.worker_state = "failed"
            elif self.status_label == "completed":
                self.worker_state = "completed"
            elif self.cancel_requested_at:
                self.worker_state = "cancel_requested"
        if self.progress_message is None and self.progress_stage:
            self.progress_message = self.progress_stage.replace("_", " ")
        if self.error_detail is None and self.error is not None:
            self.error_detail = self.error
        return self


class JobSummary(BaseModel):
    job_id: str
    job_type: str
    status: JobStatus
    status_label: str | None = None
    completion_state: Literal["completed", "partial"] | None = None
    progress_stage: str = "queued"
    progress_current: int = 0
    progress_total: int = 100
    progress_message: str | None = None
    updated_at: str = Field(default_factory=utc_now)
    retryable: bool = False
    warnings: list[str] = Field(default_factory=list)
    worker_state: (
        Literal[
            "queued",
            "running",
            "cancel_requested",
            "stalled",
            "completed",
            "failed",
            "cancelled",
            "partial",
        ]
        | None
    ) = None
    stalled_reason: str | None = None
    degraded_reason: str | None = None
    last_checkpoint_at: str | None = None

    @model_validator(mode="after")
    def populate_status_label(self) -> JobSummary:
        if not self.status_label:
            if self.completion_state == "partial":
                self.status_label = "partial"
            elif self.status == JobStatus.PARTIAL:
                self.status_label = "partial"
            elif self.status == JobStatus.PENDING:
                self.status_label = "queued"
            else:
                self.status_label = self.status.value
        if not self.worker_state:
            if self.status_label in {"queued", "pending"}:
                self.worker_state = "queued"
            elif self.status_label == "running":
                self.worker_state = "running"
            elif self.status_label == "partial":
                self.worker_state = "partial"
            elif self.status_label == "cancelled":
                self.worker_state = "cancelled"
            elif self.status_label == "failed":
                self.worker_state = "failed"
            elif self.status_label == "completed":
                self.worker_state = "completed"
        if self.progress_message is None and self.progress_stage:
            self.progress_message = self.progress_stage.replace("_", " ")
        return self


class ResearchFacet(BaseModel):
    facet_id: str
    label: str
    query_hint: str
    target_count: int = 1
    queries_attempted: int = 0
    hits_seen: int = 0
    accepted_count: int = 0
    rejected_count: int = 0
    skipped_count: int = 0


class ResearchExecutionPolicy(BaseModel):
    total_fetch_time_seconds: int = 90
    per_host_fetch_cap: int = 3
    retry_attempts: int = 3
    retry_backoff_base_ms: int = 250
    retry_backoff_max_ms: int = 2000
    respect_robots: bool = True
    allow_domains: list[str] = Field(default_factory=list)
    deny_domains: list[str] = Field(default_factory=list)


class ResearchCuratedInput(BaseModel):
    input_type: Literal["url", "text"]
    url: str | None = None
    title: str | None = None
    text: str | None = None
    publisher: str | None = None
    published_at: str | None = None
    source_type: str | None = None
    locator: str | None = None
    notes: str | None = None

    @model_validator(mode="after")
    def validate_shape(self) -> ResearchCuratedInput:
        if self.input_type == "url" and not self.url:
            raise ValueError("Curated URL inputs require a url.")
        if self.input_type == "text":
            if not self.title:
                raise ValueError("Curated text inputs require a title.")
            if not self.text:
                raise ValueError("Curated text inputs require text.")
        return self


class ResearchScoutCapabilities(BaseModel):
    supports_search: bool = False
    supports_fetch: bool = False
    supports_text_inputs: bool = False
    supports_robots: bool = False
    supports_domain_policy: bool = False


class ResearchSearchTelemetry(BaseModel):
    providers_used: list[str] = Field(default_factory=list)
    queries_by_provider: dict[str, int] = Field(default_factory=dict)
    hits_by_provider: dict[str, int] = Field(default_factory=dict)
    accepted_by_provider: dict[str, int] = Field(default_factory=dict)
    accepted_by_profile: dict[str, int] = Field(default_factory=dict)
    zero_hit_queries_by_profile: dict[str, int] = Field(default_factory=dict)
    fallback_used: bool = False
    fallback_reason: str | None = None


class ResearchRunTelemetry(BaseModel):
    total_queries: int = 0
    queries_attempted: int = 0
    fetch_attempts: int = 0
    successful_fetches: int = 0
    retries: int = 0
    fetch_failures_by_category: dict[str, int] = Field(default_factory=dict)
    blocked_by_robots_count: int = 0
    blocked_by_policy_count: int = 0
    dedupe_count: int = 0
    per_host_fetch_counts: dict[str, int] = Field(default_factory=dict)
    skipped_host_counts: dict[str, int] = Field(default_factory=dict)
    elapsed_run_time_ms: int = 0
    elapsed_fetch_time_ms: int = 0
    fallback_flags: list[str] = Field(default_factory=list)
    search: ResearchSearchTelemetry = Field(default_factory=ResearchSearchTelemetry)
    semantic: ResearchSemanticTelemetry = Field(default_factory=ResearchSemanticTelemetry)


class ResearchFindingScoring(BaseModel):
    overall_score: float = 0.0
    relevance_score: float = 0.0
    quality_score: float = 0.0
    novelty_score: float = 0.0
    structural_score: float = 0.0
    source_class_score: float = 0.0
    era_score: float = 0.0
    anchor_score: float = 0.0
    concreteness_score: float = 0.0
    shape_score: float = 0.0
    facet_fit_score: float = 0.0
    source_saturation_score: float = 0.0
    coverage_score: float = 0.0
    quality_threshold: float = 0.0
    threshold_passed: bool = False
    source_type: str | None = None
    source_class_boost_applied: float = 0.0
    source_class_penalty_applied: float = 0.0
    near_era_bias_applied: bool = False
    era_band: str | None = None
    period_native: bool = False
    period_evidenced: bool = False
    historical_contextual: bool = False
    normalized_title: str | None = None
    canonical_host: str | None = None
    semantic_score: float = 0.0
    semantic_novelty_score: float = 0.0
    semantic_rerank_delta: float = 0.0
    semantic_backend: str | None = None
    semantic_fallback_used: bool = False
    semantic_fallback_reason: str | None = None
    semantic_duplicate_similarity: float | None = None
    semantic_duplicate_candidate_id: str | None = None


class ResearchFindingProvenance(BaseModel):
    adapter_id: str | None = None
    facet_id: str
    facet_label: str | None = None
    originating_query: str
    query_profile: str | None = None
    search_provider_id: str | None = None
    matched_providers: list[str] = Field(default_factory=list)
    provider_rank: int | None = None
    fusion_score: float | None = None
    search_rank: int | None = None
    hit_url: str | None = None
    canonical_url: str | None = None
    fetch_outcome: ResearchFetchOutcome | None = None
    fetch_final_url: str | None = None
    fetch_status: str | None = None
    fetch_error_category: str | None = None
    dedupe_signature: str | None = None
    duplicate_rule: str | None = None
    acceptance_reason: ResearchFindingReason | None = None
    rejection_reason: ResearchFindingReason | None = None
    policy_flags: list[str] = Field(default_factory=list)
    semantic_duplicate_hint: bool = False
    semantic_matches: list[ResearchSemanticMatch] = Field(default_factory=list)
    semantic_decision_notes: str | None = None
    scoring: ResearchFindingScoring = Field(default_factory=ResearchFindingScoring)


class ResearchBrief(BaseModel):
    topic: str
    time_start: str | None = None
    time_end: str | None = None
    focal_year: str | None = None
    locale: str | None = None
    audience: str | None = None
    domain_hints: list[str] = Field(default_factory=list)
    desired_facets: list[str] | None = None
    preferred_source_types: list[str] = Field(default_factory=list)
    excluded_source_types: list[str] = Field(default_factory=list)
    coverage_targets: dict[str, int] = Field(default_factory=dict)
    adapter_id: str | None = None
    curated_inputs: list[ResearchCuratedInput] = Field(default_factory=list)
    execution_policy: ResearchExecutionPolicy | None = None
    max_queries: int = 12
    max_results_per_query: int = 5
    max_findings: int = 20
    max_per_facet: int = 2


class ResearchProgram(BaseModel):
    program_id: str
    name: str
    description: str | None = None
    markdown: str
    built_in: bool = False
    default_facets: list[str] = Field(default_factory=list)
    default_adapter_id: str = "web_open"
    default_execution_policy: ResearchExecutionPolicy = Field(
        default_factory=ResearchExecutionPolicy
    )
    preferred_source_classes: list[str] = Field(default_factory=list)
    excluded_source_classes: list[str] = Field(default_factory=list)
    quality_threshold: float = 0.45
    dedupe_similarity_threshold: float = 0.9
    created_at: str = Field(default_factory=utc_now)
    updated_at: str = Field(default_factory=utc_now)


class ResearchRun(BaseModel):
    run_id: str
    status: ResearchRunStatus = ResearchRunStatus.PENDING
    brief: ResearchBrief
    program_id: str
    facets: list[ResearchFacet] = Field(default_factory=list)
    query_count: int = 0
    finding_count: int = 0
    accepted_count: int = 0
    rejected_count: int = 0
    staged_count: int = 0
    extraction_run_id: str | None = None
    telemetry: ResearchRunTelemetry = Field(default_factory=ResearchRunTelemetry)
    warnings: list[str] = Field(default_factory=list)
    logs: list[str] = Field(default_factory=list)
    latest_job: JobSummary | None = None
    started_at: str = Field(default_factory=utc_now)
    completed_at: str | None = None
    error: str | None = None


class ResearchFinding(BaseModel):
    finding_id: str
    run_id: str
    facet_id: str
    query: str
    url: str
    title: str
    canonical_url: str | None = None
    publisher: str | None = None
    published_at: str | None = None
    access_date: str = Field(default_factory=utc_now)
    locator: str | None = None
    snippet_text: str
    page_excerpt: str | None = None
    source_type: str | None = None
    score: float
    relevance_score: float
    quality_score: float
    novelty_score: float
    decision: ResearchFindingDecision
    rejection_reason: str | None = None
    staged_source_id: str | None = None
    staged_document_id: str | None = None
    provenance: ResearchFindingProvenance | None = None


class ResearchSearchHit(BaseModel):
    query: str
    url: str
    title: str
    snippet: str | None = None
    rank: int = 0
    search_provider_id: str | None = None
    provider_rank: int | None = None
    provider_hit_count: int = 1
    matched_providers: list[str] = Field(default_factory=list)
    query_profile: str | None = None
    fusion_score: float | None = None


class ResearchSearchProviderResult(BaseModel):
    provider_id: str
    hits: list[ResearchSearchHit] = Field(default_factory=list)
    fallback_used: bool = False
    fallback_reason: str | None = None


class ResearchQueryPlan(BaseModel):
    facet_id: str
    query: str
    profile: str


class ResearchFetchedPage(BaseModel):
    url: str
    final_url: str | None = None
    title: str | None = None
    publisher: str | None = None
    published_at: str | None = None
    locator: str | None = None
    source_type: str | None = None
    text: str = ""


class ResearchRunRequest(BaseModel):
    brief: ResearchBrief
    program_id: str | None = None


class ResearchProgramCreateRequest(BaseModel):
    program_id: str | None = None
    name: str
    description: str | None = None
    markdown: str
    default_facets: list[str] = Field(default_factory=list)
    default_adapter_id: str = "web_open"
    default_execution_policy: ResearchExecutionPolicy | None = None
    preferred_source_classes: list[str] = Field(default_factory=list)
    excluded_source_classes: list[str] = Field(default_factory=list)
    quality_threshold: float = 0.45
    dedupe_similarity_threshold: float = 0.9


class ResearchRunStageResult(BaseModel):
    run: ResearchRun
    staged_source_ids: list[str] = Field(default_factory=list)
    staged_document_ids: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class ResearchExtractResult(BaseModel):
    stage_result: ResearchRunStageResult
    normalization: dict[str, object]
    extraction: ExtractionOutput


class ResearchRunDetail(BaseModel):
    run: ResearchRun
    findings: list[ResearchFinding] = Field(default_factory=list)
    program: ResearchProgram
    facet_coverage: list[ResearchFacetCoverage] = Field(default_factory=list)


class ResearchFacetCoverage(BaseModel):
    facet_id: str
    label: str
    target_count: int = 1
    queries_attempted: int = 0
    hits_seen: int = 0
    accepted_count: int = 0
    rejected_count: int = 0
    skipped_count: int = 0
    duplicate_rejections: int = 0
    threshold_rejections: int = 0
    excluded_source_rejections: int = 0
    fetch_failures: int = 0
    accepted_sources_by_type: dict[str, int] = Field(default_factory=dict)
    diagnostic_summary: str | None = None
    coverage_status: ResearchCoverageStatus = ResearchCoverageStatus.EMPTY
    coverage_gap_reason: str | None = None


class AuthenticatedActor(BaseModel):
    actor_id: str = "trusted-operator"
    role: Literal["operator"] = "operator"


class RuntimeDependencyStatus(BaseModel):
    name: str
    role: str
    mode: str
    configured: bool = True
    reachable: bool | None = None
    ready: bool = False
    detail: str


class RuntimeStatus(BaseModel):
    app_name: str
    app_env: str
    operator_ui_enabled: bool
    state_backend: str
    truth_backend: str
    extraction_backend: str
    overall_status: str
    services: list[RuntimeDependencyStatus] = Field(default_factory=list)
    next_steps: list[str] = Field(default_factory=list)
