from __future__ import annotations

from typing import Protocol

from source_aware_worldbuilding.domain.models import (
    ApprovedClaim,
    CandidateClaim,
    EvidenceSnippet,
    ExtractionOutput,
    ExtractionRun,
    ProjectionSearchResult,
    QueryRequest,
    QueryResult,
    ReviewEvent,
    SourceRecord,
    TextUnit,
)


class CorpusPort(Protocol):
    def pull_sources(self) -> list[SourceRecord]: ...
    def pull_text_units(self, sources: list[SourceRecord]) -> list[TextUnit]: ...


class ExtractionPort(Protocol):
    def extract_candidates(
        self,
        run: ExtractionRun,
        sources: list[SourceRecord],
        text_units: list[TextUnit],
    ) -> ExtractionOutput: ...


class SourceStorePort(Protocol):
    def list_sources(self) -> list[SourceRecord]: ...
    def get_source(self, source_id: str) -> SourceRecord | None: ...
    def save_sources(self, sources: list[SourceRecord]) -> None: ...


class TextUnitStorePort(Protocol):
    def list_text_units(self, source_id: str | None = None) -> list[TextUnit]: ...
    def save_text_units(self, text_units: list[TextUnit]) -> None: ...


class ExtractionRunStorePort(Protocol):
    def list_runs(self) -> list[ExtractionRun]: ...
    def get_run(self, run_id: str) -> ExtractionRun | None: ...
    def save_run(self, run: ExtractionRun) -> None: ...
    def update_run(self, run: ExtractionRun) -> None: ...


class CandidateStorePort(Protocol):
    def list_candidates(self, review_state: str | None = None) -> list[CandidateClaim]: ...
    def get_candidate(self, candidate_id: str) -> CandidateClaim | None: ...
    def save_candidates(self, candidates: list[CandidateClaim]) -> None: ...
    def update_candidate(self, candidate: CandidateClaim) -> None: ...


class TruthStorePort(Protocol):
    def list_claims(self) -> list[ApprovedClaim]: ...
    def get_claim(self, claim_id: str) -> ApprovedClaim | None: ...
    def save_claim(
        self,
        claim: ApprovedClaim,
        evidence: list[EvidenceSnippet] | None = None,
    ) -> None: ...


class EvidenceStorePort(Protocol):
    def list_evidence(self, source_id: str | None = None) -> list[EvidenceSnippet]: ...
    def get_evidence(self, evidence_id: str) -> EvidenceSnippet | None: ...
    def save_evidence(self, evidence: list[EvidenceSnippet]) -> None: ...


class ReviewStorePort(Protocol):
    def list_reviews(self, candidate_id: str | None = None) -> list[ReviewEvent]: ...
    def save_review(self, review: ReviewEvent) -> None: ...


class ProjectionPort(Protocol):
    def upsert_claims(
        self,
        claims: list[ApprovedClaim],
        evidence: list[EvidenceSnippet],
    ) -> None: ...
    def search_claim_ids(
        self,
        question: str,
        allowed_claim_ids: list[str],
        *,
        limit: int = 10,
    ) -> ProjectionSearchResult: ...


class QueryPort(Protocol):
    def answer(self, request: QueryRequest) -> QueryResult: ...
