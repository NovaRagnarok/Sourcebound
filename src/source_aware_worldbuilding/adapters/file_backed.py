from __future__ import annotations

from pathlib import Path

from source_aware_worldbuilding.domain.models import ApprovedClaim, CandidateClaim, EvidenceSnippet
from source_aware_worldbuilding.storage.json_store import JsonListStore


class FileCandidateStore:
    def __init__(self, data_dir: Path):
        self.store = JsonListStore(data_dir / "candidates.json")

    def list_candidates(self) -> list[CandidateClaim]:
        return self.store.read_models(CandidateClaim)

    def get_candidate(self, candidate_id: str) -> CandidateClaim | None:
        return next((item for item in self.list_candidates() if item.candidate_id == candidate_id), None)

    def save_candidates(self, candidates: list[CandidateClaim]) -> None:
        self.store.write_models(candidates)

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

    def save_claim(self, claim: ApprovedClaim) -> None:
        claims = self.list_claims()
        claims.append(claim)
        self.store.write_models(claims)


class FileEvidenceStore:
    def __init__(self, data_dir: Path):
        self.store = JsonListStore(data_dir / "evidence.json")

    def list_evidence(self) -> list[EvidenceSnippet]:
        return self.store.read_models(EvidenceSnippet)

    def get_evidence(self, evidence_id: str) -> EvidenceSnippet | None:
        return next((item for item in self.list_evidence() if item.evidence_id == evidence_id), None)
