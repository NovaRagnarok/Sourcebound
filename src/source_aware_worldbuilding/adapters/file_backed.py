from __future__ import annotations

from pathlib import Path

from source_aware_worldbuilding.domain.models import (
    ApprovedClaim,
    CandidateClaim,
    EvidenceSnippet,
    ExtractionRun,
    ReviewEvent,
    SourceRecord,
    TextUnit,
)
from source_aware_worldbuilding.storage.json_store import JsonListStore


class FileSourceStore:
    def __init__(self, data_dir: Path):
        self.store = JsonListStore(data_dir / "sources.json")

    def list_sources(self) -> list[SourceRecord]:
        return self.store.read_models(SourceRecord)

    def get_source(self, source_id: str) -> SourceRecord | None:
        return next((item for item in self.list_sources() if item.source_id == source_id), None)

    def save_sources(self, sources: list[SourceRecord]) -> None:
        self.store.write_models(sources)


class FileTextUnitStore:
    def __init__(self, data_dir: Path):
        self.store = JsonListStore(data_dir / "text_units.json")

    def list_text_units(self, source_id: str | None = None) -> list[TextUnit]:
        text_units = self.store.read_models(TextUnit)
        if source_id is None:
            return text_units
        return [item for item in text_units if item.source_id == source_id]

    def save_text_units(self, text_units: list[TextUnit]) -> None:
        existing = {item.text_unit_id: item for item in self.store.read_models(TextUnit)}
        for item in text_units:
            existing[item.text_unit_id] = item
        self.store.write_models(existing.values())


class FileExtractionRunStore:
    def __init__(self, data_dir: Path):
        self.store = JsonListStore(data_dir / "extraction_runs.json")

    def list_runs(self) -> list[ExtractionRun]:
        return list(reversed(self.store.read_models(ExtractionRun)))

    def get_run(self, run_id: str) -> ExtractionRun | None:
        return next(
            (item for item in self.store.read_models(ExtractionRun) if item.run_id == run_id),
            None,
        )

    def save_run(self, run: ExtractionRun) -> None:
        self.update_run(run)

    def update_run(self, run: ExtractionRun) -> None:
        runs = {item.run_id: item for item in self.store.read_models(ExtractionRun)}
        runs[run.run_id] = run
        self.store.write_models(runs.values())


class FileCandidateStore:
    def __init__(self, data_dir: Path):
        self.store = JsonListStore(data_dir / "candidates.json")

    def list_candidates(self, review_state: str | None = None) -> list[CandidateClaim]:
        candidates = self.store.read_models(CandidateClaim)
        if review_state is None:
            return candidates
        return [item for item in candidates if item.review_state.value == review_state]

    def get_candidate(self, candidate_id: str) -> CandidateClaim | None:
        return next(
            (item for item in self.list_candidates() if item.candidate_id == candidate_id),
            None,
        )

    def save_candidates(self, candidates: list[CandidateClaim]) -> None:
        existing = {item.candidate_id: item for item in self.store.read_models(CandidateClaim)}
        for item in candidates:
            existing[item.candidate_id] = item
        self.store.write_models(existing.values())

    def update_candidate(self, candidate: CandidateClaim) -> None:
        candidates = self.list_candidates()
        updated = []
        found = False
        for current in candidates:
            if current.candidate_id == candidate.candidate_id:
                updated.append(candidate)
                found = True
            else:
                updated.append(current)
        if not found:
            updated.append(candidate)
        self.store.write_models(updated)


class FileTruthStore:
    def __init__(self, data_dir: Path):
        self.store = JsonListStore(data_dir / "claims.json")

    def list_claims(self) -> list[ApprovedClaim]:
        return self.store.read_models(ApprovedClaim)

    def get_claim(self, claim_id: str) -> ApprovedClaim | None:
        return next((item for item in self.list_claims() if item.claim_id == claim_id), None)

    def save_claim(
        self,
        claim: ApprovedClaim,
        evidence: list[EvidenceSnippet] | None = None,
    ) -> None:
        _ = evidence
        claims = self.list_claims()
        existing = {item.claim_id: item for item in claims}
        existing[claim.claim_id] = claim
        self.store.write_models(existing.values())


class FileEvidenceStore:
    def __init__(self, data_dir: Path):
        self.store = JsonListStore(data_dir / "evidence.json")

    def list_evidence(self, source_id: str | None = None) -> list[EvidenceSnippet]:
        evidence = self.store.read_models(EvidenceSnippet)
        if source_id is None:
            return evidence
        return [item for item in evidence if item.source_id == source_id]

    def get_evidence(self, evidence_id: str) -> EvidenceSnippet | None:
        return next(
            (item for item in self.list_evidence() if item.evidence_id == evidence_id),
            None,
        )

    def save_evidence(self, evidence: list[EvidenceSnippet]) -> None:
        existing = {item.evidence_id: item for item in self.store.read_models(EvidenceSnippet)}
        for item in evidence:
            existing[item.evidence_id] = item
        self.store.write_models(existing.values())


class FileReviewStore:
    def __init__(self, data_dir: Path):
        self.store = JsonListStore(data_dir / "review_events.json")

    def list_reviews(self, candidate_id: str | None = None) -> list[ReviewEvent]:
        reviews = list(reversed(self.store.read_models(ReviewEvent)))
        if candidate_id is None:
            return reviews
        return [item for item in reviews if item.candidate_id == candidate_id]

    def save_review(self, review: ReviewEvent) -> None:
        existing = {item.review_id: item for item in self.store.read_models(ReviewEvent)}
        existing[review.review_id] = review
        self.store.write_models(existing.values())
