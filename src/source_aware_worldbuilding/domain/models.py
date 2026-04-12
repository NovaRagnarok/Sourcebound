from __future__ import annotations

from pydantic import BaseModel, Field

from source_aware_worldbuilding.domain.enums import (
    ClaimKind,
    ClaimStatus,
    QueryMode,
    ReviewDecision,
    ReviewState,
)


class SourceRecord(BaseModel):
    source_id: str
    title: str
    author: str | None = None
    year: str | None = None
    source_type: str = "document"
    locator_hint: str | None = None
    zotero_item_key: str | None = None


class EvidenceSnippet(BaseModel):
    evidence_id: str
    source_id: str
    locator: str
    text: str
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
    notes: str | None = None


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


class QueryResult(BaseModel):
    question: str
    mode: QueryMode
    answer: str
    supporting_claims: list[ApprovedClaim] = Field(default_factory=list)
    evidence: list[EvidenceSnippet] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
