from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, Field

from source_aware_worldbuilding.domain.enums import (
    ClaimKind,
    ClaimStatus,
    ExtractionRunStatus,
    QueryMode,
    ReviewDecision,
    ReviewState,
)


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


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
    raw_metadata_json: dict | None = None


class SourceDocumentRecord(BaseModel):
    document_id: str
    source_id: str
    document_kind: Literal["attachment", "note", "snapshot", "manual_text"]
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
    claim_extraction_status: Literal["queued", "ready", "running", "completed", "failed"] = (
        "queued"
    )
    locator: str | None = None
    raw_text: str | None = None
    raw_metadata_json: dict | None = None


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
    notes: str | None = None


class QueryFilter(BaseModel):
    status: ClaimStatus | None = None
    claim_kind: ClaimKind | None = None
    place: str | None = None
    viewpoint_scope: str | None = None


class QueryRequest(BaseModel):
    question: str
    mode: QueryMode = QueryMode.STRICT_FACTS
    filters: QueryFilter | None = None


class QueryResultMetadata(BaseModel):
    retrieval_backend: Literal["memory", "qdrant"] = "memory"
    fallback_used: bool = False
    fallback_reason: str | None = None


class ProjectionSearchResult(BaseModel):
    claim_ids: list[str] = Field(default_factory=list)
    retrieval_backend: Literal["qdrant"] = "qdrant"
    fallback_used: bool = False
    fallback_reason: str | None = None


class ClaimRelationship(BaseModel):
    relationship_id: str
    claim_id: str
    related_claim_id: str
    relationship_type: Literal["supports", "contradicts", "supersedes", "superseded_by"]
    source_kind: Literal["derived", "manual"] = "derived"
    notes: str | None = None


class ClaimRelationshipRequest(BaseModel):
    related_claim_id: str
    relationship_type: Literal["supports", "contradicts", "supersedes", "superseded_by"]
    notes: str | None = None


class ClaimCluster(BaseModel):
    cluster_id: str
    lead_claim_id: str
    claim_ids: list[str] = Field(default_factory=list)
    relationship_types: list[Literal["supports", "contradicts", "supersedes", "superseded_by"]] = (
        Field(default_factory=list)
    )
    cluster_kind: Literal["reinforcing", "contested", "supersession"]
    summary: str


class AnswerSection(BaseModel):
    cluster_id: str
    heading: str
    text: str
    claim_ids: list[str] = Field(default_factory=list)
    cluster_kind: Literal["reinforcing", "contested", "supersession"]


class QueryResult(BaseModel):
    question: str
    mode: QueryMode
    answer: str
    supporting_claims: list[ApprovedClaim] = Field(default_factory=list)
    related_claims: list[ClaimRelationship] = Field(default_factory=list)
    claim_clusters: list[ClaimCluster] = Field(default_factory=list)
    answer_sections: list[AnswerSection] = Field(default_factory=list)
    evidence: list[EvidenceSnippet] = Field(default_factory=list)
    sources: list[SourceRecord] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    metadata: QueryResultMetadata = Field(default_factory=QueryResultMetadata)


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
