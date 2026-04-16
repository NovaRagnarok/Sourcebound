from __future__ import annotations

from uuid import uuid4

from source_aware_worldbuilding.domain.enums import (
    UNRESOLVED_REVIEW_STATES,
    ReviewDecision,
    ReviewState,
)
from source_aware_worldbuilding.domain.errors import ReviewConflictError
from source_aware_worldbuilding.domain.models import (
    ApprovedClaim,
    AuthenticatedActor,
    EvidenceSnippet,
    ReviewClaimPatch,
    ReviewEvent,
    ReviewEvidencePreview,
    ReviewEvidenceQuality,
    ReviewQueueCard,
    ReviewRequest,
    ReviewWeaknessReason,
    SourceRecord,
    TextUnit,
)
from source_aware_worldbuilding.ports import (
    CandidateStorePort,
    EvidenceStorePort,
    ProjectionPort,
    ReviewStorePort,
    SourceStorePort,
    TextUnitStorePort,
    TruthStorePort,
)


class ReviewService:
    _CERTAINTY_RANK = {
        "verified": 5,
        "author_choice": 4,
        "probable": 3,
        "contested": 2,
        "rumor": 1,
        "legend": 0,
    }
    _EVIDENCE_QUALITY_RANK = {"supported": 2, "thin": 1, "blind": 0}
    _CONTEXT_WINDOW = 96
    _SHORT_EXCERPT_MIN_LENGTH = 72

    def __init__(
        self,
        candidate_store: CandidateStorePort,
        truth_store: TruthStorePort,
        review_store: ReviewStorePort,
        evidence_store: EvidenceStorePort,
        source_store: SourceStorePort | None = None,
        text_unit_store: TextUnitStorePort | None = None,
        projection: ProjectionPort | None = None,
    ):
        self.candidate_store = candidate_store
        self.truth_store = truth_store
        self.review_store = review_store
        self.evidence_store = evidence_store
        self.source_store = source_store
        self.text_unit_store = text_unit_store
        self.projection = projection

    def list_candidates(self, review_state: str | None = None):
        candidates = self.candidate_store.list_candidates(review_state=review_state)
        return sorted(
            candidates,
            key=lambda item: (
                len(item.evidence_ids),
                -self._CERTAINTY_RANK.get(item.status_suggestion.value, -1),
                item.candidate_id,
            ),
        )

    def list_review_queue(self) -> list[ReviewQueueCard]:
        source_by_id = self._sources_by_id()
        text_units_by_id = self._text_units_by_id()
        cards = [
            self._build_review_queue_card(candidate, source_by_id, text_units_by_id)
            for candidate in self.candidate_store.list_candidates()
        ]
        return sorted(
            cards,
            key=lambda item: (
                0 if item.review_state in UNRESOLVED_REVIEW_STATES else 1,
                -self._EVIDENCE_QUALITY_RANK.get(item.evidence_quality, -1),
                -len(item.evidence_ids),
                -self._CERTAINTY_RANK.get(item.status_suggestion.value, -1),
                item.candidate_id,
            ),
        )

    def list_reviews(self, candidate_id: str | None = None):
        return self.review_store.list_reviews(candidate_id=candidate_id)

    def review_candidate(
        self,
        candidate_id: str,
        request: ReviewRequest,
        *,
        actor: AuthenticatedActor,
    ) -> ApprovedClaim | None:
        candidate = self.candidate_store.get_candidate(candidate_id)
        if candidate is None:
            return None
        if request.decision == ReviewDecision.APPROVE and (
            candidate.review_state == ReviewState.APPROVED
        ):
            raise ReviewConflictError("Candidate has already been approved.")

        review = ReviewEvent(
            review_id=f"rev-{uuid4().hex[:12]}",
            candidate_id=candidate_id,
            decision=request.decision,
            override_status=request.override_status,
            notes=request.notes,
            actor_id=actor.actor_id,
            actor_role=actor.role,
        )

        if request.decision.value == "reject":
            candidate.review_state = (
                ReviewState(request.defer_state)
                if request.defer_state is not None
                else ReviewState.REJECTED
            )
            self.candidate_store.update_candidate(candidate)
            self.review_store.save_review(review)
            return None

        evidence = [
            snippet
            for evidence_id in candidate.evidence_ids
            if (snippet := self.evidence_store.get_evidence(evidence_id)) is not None
        ]
        claim_patch = request.claim_patch or ReviewClaimPatch()
        self._validate_approval(candidate.review_state, candidate, claim_patch)

        approved = ApprovedClaim(
            claim_id=f"claim-{uuid4().hex[:12]}",
            subject=claim_patch.subject or candidate.subject,
            predicate=claim_patch.predicate or candidate.predicate,
            value=claim_patch.value or candidate.value,
            claim_kind=candidate.claim_kind,
            status=request.override_status or candidate.status_suggestion,
            place=claim_patch.place if claim_patch.place is not None else candidate.place,
            time_start=(
                claim_patch.time_start
                if claim_patch.time_start is not None
                else candidate.time_start
            ),
            time_end=(
                claim_patch.time_end if claim_patch.time_end is not None else candidate.time_end
            ),
            viewpoint_scope=(
                claim_patch.viewpoint_scope
                if claim_patch.viewpoint_scope is not None
                else candidate.viewpoint_scope
            ),
            author_choice=(
                request.override_status is not None
                and request.override_status.value == "author_choice"
            ),
            evidence_ids=candidate.evidence_ids,
            created_from_run_id=candidate.extractor_run_id,
            notes=request.notes or candidate.notes,
        )
        review.approved_claim_id = approved.claim_id
        self.truth_store.save_claim(approved, evidence=evidence, review=review)
        candidate.review_state = ReviewState.APPROVED
        self.candidate_store.update_candidate(candidate)
        self.review_store.save_review(review)
        if self.projection is not None:
            self.projection.upsert_claims([approved], evidence)
        return approved

    def _validate_approval(
        self,
        review_state: ReviewState,
        candidate,
        claim_patch: ReviewClaimPatch,
    ) -> None:
        if review_state not in {ReviewState.NEEDS_EDIT, ReviewState.NEEDS_SPLIT}:
            return
        if self._has_meaningful_patch(candidate, claim_patch):
            return
        raise ReviewConflictError(
            "Deferred review candidates require edits before they can be approved."
        )

    def _has_meaningful_patch(self, candidate, claim_patch: ReviewClaimPatch) -> bool:
        for field in (
            "subject",
            "predicate",
            "value",
            "place",
            "time_start",
            "time_end",
            "viewpoint_scope",
        ):
            patched_value = getattr(claim_patch, field)
            if patched_value is None:
                continue
            if patched_value != getattr(candidate, field):
                return True
        return False

    def _sources_by_id(self) -> dict[str, SourceRecord]:
        if self.source_store is None:
            return {}
        return {source.source_id: source for source in self.source_store.list_sources()}

    def _text_units_by_id(self) -> dict[str, TextUnit]:
        if self.text_unit_store is None:
            return {}
        return {
            text_unit.text_unit_id: text_unit
            for text_unit in self.text_unit_store.list_text_units()
        }

    def _build_review_queue_card(
        self,
        candidate,
        source_by_id: dict[str, SourceRecord],
        text_units_by_id: dict[str, TextUnit],
    ) -> ReviewQueueCard:
        evidence_items = [
            preview
            for evidence_id in candidate.evidence_ids
            if (snippet := self.evidence_store.get_evidence(evidence_id)) is not None
            if (preview := self._build_evidence_preview(snippet, source_by_id, text_units_by_id))
            is not None
        ]
        primary_evidence = evidence_items[0] if evidence_items else None

        weakness_reasons: list[ReviewWeaknessReason] = []
        if not evidence_items:
            weakness_reasons.append("missing_evidence")
        else:
            if len(evidence_items) == 1:
                weakness_reasons.append("single_snippet")
            if not (primary_evidence.locator if primary_evidence else None):
                weakness_reasons.append("missing_locator")
            if not self._has_expandable_context(primary_evidence):
                weakness_reasons.append("missing_span_context")
            if self._is_short_excerpt(primary_evidence):
                weakness_reasons.append("short_excerpt")

        evidence_quality: ReviewEvidenceQuality = self._evidence_quality(weakness_reasons)
        payload = candidate.model_dump(mode="python")
        return ReviewQueueCard(
            **payload,
            claim_text=self._claim_text(candidate.subject, candidate.predicate, candidate.value),
            certainty_suggestion=candidate.status_suggestion,
            location_summary=self._location_summary(candidate, primary_evidence),
            evidence_quality=evidence_quality,
            weakness_reasons=list(dict.fromkeys(weakness_reasons)),
            primary_evidence=primary_evidence,
            extra_evidence_count=max(len(evidence_items) - 1, 0),
            evidence_items=evidence_items,
        )

    def _build_evidence_preview(
        self,
        snippet: EvidenceSnippet,
        source_by_id: dict[str, SourceRecord],
        text_units_by_id: dict[str, TextUnit],
    ) -> ReviewEvidencePreview:
        source = source_by_id.get(snippet.source_id)
        text_unit = (
            text_units_by_id.get(snippet.text_unit_id) if snippet.text_unit_id is not None else None
        )
        span = self._resolve_span(snippet, text_unit)
        excerpt = (snippet.text or "").strip()
        context_before = ""
        context_after = ""
        span_start = snippet.span_start
        span_end = snippet.span_end

        if text_unit is not None and span is not None:
            span_start, span_end = span
            excerpt = text_unit.text[span_start:span_end].strip() or excerpt
            context_before = text_unit.text[max(0, span_start - self._CONTEXT_WINDOW) : span_start]
            context_after = text_unit.text[
                span_end : min(len(text_unit.text), span_end + self._CONTEXT_WINDOW)
            ]

        return ReviewEvidencePreview(
            evidence_id=snippet.evidence_id,
            source_id=snippet.source_id,
            source_title=source.title if source is not None else snippet.source_id,
            source_type=source.source_type if source is not None else None,
            locator=snippet.locator or (text_unit.locator if text_unit is not None else None),
            excerpt=excerpt,
            context_before=context_before,
            context_after=context_after,
            span_start=span_start,
            span_end=span_end,
            text_unit_id=snippet.text_unit_id,
            notes=snippet.notes,
        )

    def _resolve_span(
        self,
        snippet: EvidenceSnippet,
        text_unit: TextUnit | None,
    ) -> tuple[int, int] | None:
        if text_unit is None or not text_unit.text:
            return None

        haystack = text_unit.text
        if (
            snippet.span_start is not None
            and snippet.span_end is not None
            and 0 <= snippet.span_start < snippet.span_end <= len(haystack)
        ):
            excerpt = haystack[snippet.span_start : snippet.span_end]
            if self._normalized_excerpt(excerpt) == self._normalized_excerpt(snippet.text):
                return snippet.span_start, snippet.span_end

        needle = (snippet.text or "").strip()
        if not needle:
            return None
        exact = haystack.find(needle)
        if exact >= 0:
            return exact, exact + len(needle)

        normalized_needle = self._normalized_excerpt(needle)
        if not normalized_needle:
            return None

        for start in range(len(haystack)):
            end = start + len(needle)
            if end > len(haystack):
                break
            if self._normalized_excerpt(haystack[start:end]) == normalized_needle:
                return start, end
        return None

    def _normalized_excerpt(self, value: str | None) -> str:
        if value is None:
            return ""
        return " ".join(value.split()).strip()

    def _has_expandable_context(
        self,
        preview: ReviewEvidencePreview | None,
    ) -> bool:
        if preview is None:
            return False
        return bool((preview.context_before or "").strip() or (preview.context_after or "").strip())

    def _is_short_excerpt(self, preview: ReviewEvidencePreview | None) -> bool:
        if preview is None:
            return False
        normalized = self._normalized_excerpt(preview.excerpt)
        return len(normalized) < self._SHORT_EXCERPT_MIN_LENGTH

    def _evidence_quality(
        self,
        weakness_reasons: list[ReviewWeaknessReason],
    ) -> ReviewEvidenceQuality:
        weakness = set(weakness_reasons)
        if "missing_evidence" in weakness:
            return "blind"
        if weakness & {"missing_span_context", "short_excerpt", "missing_locator"}:
            return "thin"
        return "supported"

    def _claim_text(self, subject: str, predicate: str, value: str) -> str:
        return f"{subject} {predicate.replace('_', ' ')} {value}".strip()

    def _location_summary(
        self, candidate, primary_evidence: ReviewEvidencePreview | None
    ) -> str | None:
        parts = [
            primary_evidence.locator if primary_evidence is not None else None,
            candidate.place,
            self._time_summary(candidate.time_start, candidate.time_end),
        ]
        summary = " · ".join(part for part in parts if part)
        return summary or None

    def _time_summary(self, time_start: str | None, time_end: str | None) -> str | None:
        if time_start and time_end and time_start != time_end:
            return f"{time_start} to {time_end}"
        return time_start or time_end
