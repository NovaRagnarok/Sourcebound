from __future__ import annotations

from hashlib import sha1

from source_aware_worldbuilding.domain.enums import ClaimKind, ClaimStatus, ReviewState
from source_aware_worldbuilding.domain.models import (
    CandidateClaim,
    EvidenceSnippet,
    ExtractionOutput,
    ExtractionRun,
    SourceRecord,
    TextUnit,
)


class GraphRAGExtractionAdapter:
    """A pragmatic extraction layer that preserves the GraphRAG seam."""

    def extract_candidates(
        self,
        run: ExtractionRun,
        sources: list[SourceRecord],
        text_units: list[TextUnit],
    ) -> ExtractionOutput:
        source_by_id = {source.source_id: source for source in sources}
        evidence: list[EvidenceSnippet] = []
        candidates: list[CandidateClaim] = []

        for text_unit in text_units:
            source = source_by_id.get(text_unit.source_id)
            if source is None:
                continue

            evidence_id = f"evi-{run.run_id}-{text_unit.ordinal}"
            evidence.append(
                EvidenceSnippet(
                    evidence_id=evidence_id,
                    source_id=text_unit.source_id,
                    locator=text_unit.locator,
                    text=text_unit.text,
                    notes=f"Generated during extraction run {run.run_id}.",
                    checksum=text_unit.checksum,
                )
            )

            status = self._suggest_status(text_unit.text)
            predicate, value = self._extract_predicate_value(text_unit.text)
            candidate_seed = sha1(f"{run.run_id}:{text_unit.text}".encode()).hexdigest()[:12]
            candidates.append(
                CandidateClaim(
                    candidate_id=f"cand-{candidate_seed}",
                    subject=source.title,
                    predicate=predicate,
                    value=value,
                    claim_kind=self._claim_kind(source),
                    status_suggestion=status,
                    review_state=ReviewState.PENDING,
                    place=source.locator_hint,
                    evidence_ids=[evidence_id],
                    extractor_run_id=run.run_id,
                    notes=(
                        "Heuristic extraction result. Replace or augment with a richer "
                        "GraphRAG pipeline later."
                    ),
                )
            )

        run.candidate_count = len(candidates)
        run.text_unit_count = len(text_units)
        run.source_count = len(sources)
        return ExtractionOutput(run=run, candidates=candidates, evidence=evidence)

    def _suggest_status(self, text: str) -> ClaimStatus:
        text_lower = text.lower()
        if any(word in text_lower for word in ("rumor", "whisper", "legend", "myth")):
            return ClaimStatus.RUMOR
        if any(word in text_lower for word in ("contested", "disputed", "claimed")):
            return ClaimStatus.CONTESTED
        return ClaimStatus.PROBABLE

    def _extract_predicate_value(self, text: str) -> tuple[str, str]:
        normalized = " ".join(text.split())
        if " rose " in f" {normalized.lower()} ":
            return ("rose_during", normalized[:120])
        if " whispered " in f" {normalized.lower()} ":
            return ("rumored_in", normalized[:120])
        return ("described_as", normalized[:120])

    def _claim_kind(self, source: SourceRecord) -> ClaimKind:
        source_type = source.source_type.lower()
        if source_type in {"person", "biography"}:
            return ClaimKind.PERSON
        if source_type in {"place", "map"}:
            return ClaimKind.PLACE
        if source_type in {"chronicle", "belief"}:
            return ClaimKind.BELIEF
        if source_type in {"event"}:
            return ClaimKind.EVENT
        return ClaimKind.OBJECT
