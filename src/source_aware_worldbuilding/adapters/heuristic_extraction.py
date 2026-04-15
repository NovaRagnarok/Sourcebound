from __future__ import annotations

import re
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
from source_aware_worldbuilding.domain.normalization import (
    infer_place,
    infer_time_range,
    normalized_candidate_key,
)

SENTENCE_FRAGMENT_RE = re.compile(r'.+?(?:[.!?](?:["\'])?(?=\s|$)|\n+|$)', re.S)
GENERIC_SUBJECTS = {"people", "person", "they", "it", "this", "that", "these", "those"}
NOISY_TEXT_PATTERNS = (
    "photo by",
    "listen to",
    "sign up",
    "read more",
    "shop ",
    "buy ",
)
BROKEN_TEXT_PATTERNS = (
    "after defining",
    "this paper will",
    "this article will",
    "in this paper",
)


class HeuristicExtractionAdapter:
    """Local sentence-based extractor used for MVP and offline development."""

    backend_name = "heuristic"

    def extract_candidates(
        self,
        run: ExtractionRun,
        sources: list[SourceRecord],
        text_units: list[TextUnit],
    ) -> ExtractionOutput:
        source_by_id = {source.source_id: source for source in sources}
        evidence: list[EvidenceSnippet] = []
        candidate_by_key: dict[tuple[str, str, str], CandidateClaim] = {}

        for text_unit in text_units:
            source = source_by_id.get(text_unit.source_id)
            if source is None:
                continue

            sentences = self._sentence_spans(text_unit.text)
            for sentence_index, (sentence, span_start, span_end) in enumerate(sentences, start=1):
                if not self._is_usable_sentence(sentence):
                    continue
                evidence_id = f"evi-{run.run_id}-{text_unit.text_unit_id}-{sentence_index}"
                evidence.append(
                    EvidenceSnippet(
                        evidence_id=evidence_id,
                        source_id=text_unit.source_id,
                        locator=f"{text_unit.locator}#s{sentence_index}",
                        text=sentence,
                        text_unit_id=text_unit.text_unit_id,
                        span_start=span_start,
                        span_end=span_end,
                        notes=f"Generated during extraction run {run.run_id}.",
                        checksum=sha1(sentence.encode()).hexdigest(),
                    )
                )

                for predicate, value in self._extract_claims_from_sentence(sentence):
                    subject = self._claim_subject(sentence, source)
                    if not subject or self._is_generic_subject(subject):
                        continue
                    if not self._is_usable_claim_value(value):
                        continue
                    candidate_key = normalized_candidate_key(subject, predicate, value)
                    existing = candidate_by_key.get(candidate_key)
                    if existing is not None:
                        if evidence_id not in existing.evidence_ids:
                            existing.evidence_ids.append(evidence_id)
                        continue

                    time_start, time_end = infer_time_range(sentence, source.year)
                    candidate_seed = sha1(
                        f"{run.run_id}:{'|'.join(candidate_key)}".encode()
                    ).hexdigest()[:12]
                    candidate_by_key[candidate_key] = CandidateClaim(
                        candidate_id=f"cand-{candidate_seed}",
                        subject=subject,
                        predicate=predicate,
                        value=value[:160],
                        claim_kind=self._claim_kind(sentence, source),
                        status_suggestion=self._suggest_status(sentence),
                        review_state=ReviewState.PENDING,
                        place=infer_place(sentence, source),
                        time_start=time_start,
                        time_end=time_end,
                        evidence_ids=[evidence_id],
                        extractor_run_id=run.run_id,
                        notes="Sentence-derived extraction candidate.",
                    )

        candidates = list(candidate_by_key.values())
        run.candidate_count = len(candidates)
        run.text_unit_count = len(text_units)
        run.source_count = len(sources)
        return ExtractionOutput(run=run, candidates=candidates, evidence=evidence)

    def _sentence_spans(self, text: str) -> list[tuple[str, int, int]]:
        if not text.strip():
            return []

        spans: list[tuple[str, int, int]] = []
        for match in SENTENCE_FRAGMENT_RE.finditer(text):
            chunk = match.group(0)
            if not chunk.strip():
                continue
            start_offset = len(chunk) - len(chunk.lstrip(" -\n"))
            end_offset = len(chunk.rstrip(" -\n"))
            sentence = chunk[start_offset:end_offset]
            if sentence:
                spans.append(
                    (
                        sentence,
                        match.start() + start_offset,
                        match.start() + end_offset,
                    )
                )

        return spans

    def _is_usable_sentence(self, sentence: str) -> bool:
        normalized = " ".join(sentence.split()).strip()
        lower = normalized.lower()
        boundary_safe = normalized.rstrip('"”’\'')
        if len(normalized) < 35 or len(normalized.split()) < 6:
            return False
        if len(normalized) > 320:
            return False
        if boundary_safe.endswith((":", "—", "-", "“")):
            return False
        if "…" in normalized or normalized.endswith("..."):
            return False
        if lower.startswith(BROKEN_TEXT_PATTERNS):
            return False
        if any(pattern in lower for pattern in NOISY_TEXT_PATTERNS):
            return False
        if normalized.count('"') % 2 == 1 and not normalized.endswith('"'):
            return False
        return True

    def _extract_claims_from_sentence(self, sentence: str) -> list[tuple[str, str]]:
        normalized = " ".join(sentence.split())
        lower = normalized.lower()

        if " whispered that " in lower:
            return [("rumored_that", self._after_phrase(normalized, "whispered that"))]
        if " at " in lower and re.search(
            r"\b(?:club|venue|warehouse|radio|residency|record pool|label)\b", lower
        ):
            return [("occurred_at", normalized)]
        if " record pool " in f" {lower} ":
            return [("distributed_via", normalized)]
        if " radio " in f" {lower} " or " radio show " in f" {lower} ":
            return [("aired_on", normalized)]
        if any(term in lower for term in ("vinyl", "cdj", "turntable", "mixtape")):
            return [("used", normalized)]
        if " flyer " in f" {lower} " or " flyers " in f" {lower} ":
            return [("promoted_with", normalized)]
        if " hosted " in lower or " host " in lower:
            return [("hosted", normalized)]
        if " featured " in lower or " features " in lower:
            return [("featured", normalized)]
        if " withholding " in lower or " withheld " in lower:
            return [("withheld", normalized)]
        if " rose " in f" {lower} ":
            return [("rose_during", normalized)]
        if " was said to " in lower:
            return [("was_said_to", self._after_phrase(normalized, "was said to"))]
        if " during " in lower:
            return [("occurred_during", self._after_phrase(normalized, "during"))]
        if " includes " in lower or " include " in lower:
            return [("includes", normalized)]
        if " required " in lower or " requires " in lower:
            return [("required", normalized)]
        if " declined " in lower and " thanks to " in lower:
            return [("declined_due_to", normalized)]
        if " focus on " in lower or " focuses on " in lower:
            return [("focuses_on", self._after_phrase(normalized, "focus on"))]
        if len(normalized.split()) >= 5:
            return [("described_as", normalized)]
        return []

    def _after_phrase(self, sentence: str, phrase: str) -> str:
        lower_sentence = sentence.lower()
        index = lower_sentence.find(phrase)
        if index < 0:
            return sentence
        return sentence[index + len(phrase) :].strip(" .,:;")

    def _claim_subject(self, sentence: str, source: SourceRecord) -> str:
        match = re.match(
            r"([A-Z][^,.;]{2,80}?)\s+(rose|was|were|whispered|withheld|withholding)\b",
            sentence,
        )
        if match:
            subject = match.group(1).strip()
            if self._is_generic_subject(subject):
                return source.title
            return subject
        return source.title

    def _is_generic_subject(self, subject: str) -> bool:
        normalized = " ".join(subject.split()).strip().lower()
        return normalized in GENERIC_SUBJECTS

    def _is_usable_claim_value(self, value: str) -> bool:
        cleaned = " ".join(value.split()).strip(" .,:;")
        lower = cleaned.lower()
        boundary_safe = cleaned.rstrip('"”’\'')
        if len(cleaned) < 24 or len(cleaned.split()) < 4:
            return False
        if boundary_safe.endswith((":", "—", "-", "“")):
            return False
        if cleaned.startswith(("After defining", "This paper", "This article", "In this paper")):
            return False
        if any(pattern in lower for pattern in NOISY_TEXT_PATTERNS):
            return False
        return True

    def _suggest_status(self, text: str) -> ClaimStatus:
        text_lower = text.lower()
        if any(word in text_lower for word in ("rumor", "whisper", "legend", "myth", "said to")):
            return ClaimStatus.RUMOR
        if any(word in text_lower for word in ("contested", "disputed", "claimed", "alleged")):
            return ClaimStatus.CONTESTED
        if any(word in text_lower for word in ("record", "ledger", "price", "documented")):
            return ClaimStatus.VERIFIED
        return ClaimStatus.PROBABLE

    def _claim_kind(self, sentence: str, source: SourceRecord) -> ClaimKind:
        sentence_lower = sentence.lower()
        if any(word in sentence_lower for word in ("town", "city", "market", "square")):
            return ClaimKind.PLACE
        if any(word in sentence_lower for word in ("merchant", "clerk", "townspeople")):
            return ClaimKind.PERSON
        if any(word in sentence_lower for word in ("price", "practice", "record", "ledger")):
            return ClaimKind.PRACTICE
        if source.source_type.lower() in {"chronicle", "belief"}:
            return ClaimKind.BELIEF
        return ClaimKind.OBJECT
