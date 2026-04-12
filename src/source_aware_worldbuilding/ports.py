from __future__ import annotations

from typing import Protocol

from source_aware_worldbuilding.domain.models import (
    ApprovedClaim,
    CandidateClaim,
    EvidenceSnippet,
    QueryRequest,
    QueryResult,
    SourceRecord,
)


class CorpusPort(Protocol):
    def pull_sources(self) -> list[SourceRecord]: ...


class ExtractionPort(Protocol):
    def extract_candidates(self, sources: list[SourceRecord]) -> list[CandidateClaim]: ...


class CandidateStorePort(Protocol):
    def list_candidates(self) -> list[CandidateClaim]: ...
    def get_candidate(self, candidate_id: str) -> CandidateClaim | None: ...
    def save_candidates(self, candidates: list[CandidateClaim]) -> None: ...
    def update_candidate(self, candidate: CandidateClaim) -> None: ...


class TruthStorePort(Protocol):
    def list_claims(self) -> list[ApprovedClaim]: ...
    def save_claim(self, claim: ApprovedClaim) -> None: ...


class EvidenceStorePort(Protocol):
    def list_evidence(self) -> list[EvidenceSnippet]: ...
    def get_evidence(self, evidence_id: str) -> EvidenceSnippet | None: ...


class QueryPort(Protocol):
    def answer(self, request: QueryRequest) -> QueryResult: ...
