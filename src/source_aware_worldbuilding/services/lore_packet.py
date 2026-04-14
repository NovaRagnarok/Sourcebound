from __future__ import annotations

import re
from collections import defaultdict

from source_aware_worldbuilding.domain.enums import ClaimKind, ClaimStatus
from source_aware_worldbuilding.domain.models import (
    ApprovedClaim,
    EvidenceSnippet,
    LorePacketFile,
    LorePacketMetadata,
    LorePacketRequest,
    LorePacketResponse,
    QueryFilter,
    SourceRecord,
)
from source_aware_worldbuilding.ports import EvidenceStorePort, SourceStorePort, TruthStorePort


class LorePacketService:
    _DEFAULT_FILES = ["basic-lore.md", "characters.md", "timeline.md", "notes.md"]
    _DEFAULT_STATUSES = [
        ClaimStatus.VERIFIED,
        ClaimStatus.PROBABLE,
        ClaimStatus.CONTESTED,
        ClaimStatus.RUMOR,
        ClaimStatus.LEGEND,
        ClaimStatus.AUTHOR_CHOICE,
    ]
    _STATUS_HEADINGS = {
        ClaimStatus.VERIFIED: "Verified",
        ClaimStatus.PROBABLE: "Probable",
        ClaimStatus.CONTESTED: "Contested",
        ClaimStatus.RUMOR: "Rumor",
        ClaimStatus.LEGEND: "Legend",
        ClaimStatus.AUTHOR_CHOICE: "Author Choices",
    }
    _STATUS_ORDER = {
        ClaimStatus.VERIFIED: 0,
        ClaimStatus.PROBABLE: 1,
        ClaimStatus.CONTESTED: 2,
        ClaimStatus.RUMOR: 3,
        ClaimStatus.LEGEND: 4,
        ClaimStatus.AUTHOR_CHOICE: 5,
    }
    _KIND_HEADINGS = {
        ClaimKind.PERSON: "People",
        ClaimKind.PLACE: "Places",
        ClaimKind.INSTITUTION: "Institutions",
        ClaimKind.EVENT: "Events",
        ClaimKind.PRACTICE: "Practices",
        ClaimKind.BELIEF: "Beliefs",
        ClaimKind.RELATIONSHIP: "Relationships",
        ClaimKind.OBJECT: "Objects",
    }

    def __init__(
        self,
        truth_store: TruthStorePort,
        evidence_store: EvidenceStorePort,
        source_store: SourceStorePort,
    ) -> None:
        self.truth_store = truth_store
        self.evidence_store = evidence_store
        self.source_store = source_store

    def export(self, request: LorePacketRequest) -> LorePacketResponse:
        claims = self._filter_claims(self.truth_store.list_claims(), request)
        evidence_by_id = self._evidence_index(claims)
        source_by_id = self._source_index(evidence_by_id.values())
        warnings = self._derive_warnings(claims, evidence_by_id)

        files = []
        for filename in request.files or self._DEFAULT_FILES:
            if filename == "basic-lore.md":
                files.append(
                    self._build_file(
                        filename,
                        claims,
                        evidence_by_id,
                        source_by_id,
                        request.include_evidence_footnotes,
                        self._render_basic_lore,
                    )
                )
            elif filename == "characters.md":
                files.append(
                    self._build_file(
                        filename,
                        self._character_claims(claims),
                        evidence_by_id,
                        source_by_id,
                        request.include_evidence_footnotes,
                        self._render_characters,
                    )
                )
            elif filename == "timeline.md":
                files.append(
                    self._build_file(
                        filename,
                        self._timeline_claims(claims),
                        evidence_by_id,
                        source_by_id,
                        request.include_evidence_footnotes,
                        self._render_timeline,
                    )
                )
            elif filename == "notes.md":
                files.append(
                    self._build_file(
                        filename,
                        self._notes_claims(claims),
                        evidence_by_id,
                        source_by_id,
                        request.include_evidence_footnotes,
                        lambda selected, evi, src, include: self._render_notes(
                            selected, evi, src, include, warnings
                        ),
                    )
                )

        claim_ids = {claim.claim_id for claim in claims}
        source_ids = {
            evidence.source_id
            for claim in claims
            for evidence_id in claim.evidence_ids
            if (evidence := evidence_by_id.get(evidence_id)) is not None
        }
        evidence_ids = {
            evidence_id
            for claim in claims
            for evidence_id in claim.evidence_ids
            if evidence_id in evidence_by_id
        }
        return LorePacketResponse(
            project_name=request.project_name,
            focus=request.focus,
            filters=request.filters,
            files=files,
            warnings=warnings,
            metadata=LorePacketMetadata(
                claim_count=len(claim_ids),
                source_count=len(source_ids),
                evidence_count=len(evidence_ids),
            ),
        )

    def _filter_claims(
        self,
        claims: list[ApprovedClaim],
        request: LorePacketRequest,
    ) -> list[ApprovedClaim]:
        filtered = list(claims)
        allowed_statuses = set(request.include_statuses or self._DEFAULT_STATUSES)
        filtered = [claim for claim in filtered if claim.status in allowed_statuses]
        filtered = self._apply_filters(filtered, request.filters)
        if request.focus:
            filtered = self._apply_focus(filtered, request.focus)
        return sorted(
            filtered,
            key=lambda claim: (
                self._STATUS_ORDER[claim.status],
                claim.claim_kind.value,
                claim.subject.lower(),
                claim.predicate.lower(),
                claim.value.lower(),
            ),
        )

    def _apply_filters(
        self,
        claims: list[ApprovedClaim],
        filters: QueryFilter | None,
    ) -> list[ApprovedClaim]:
        if filters is None:
            return claims
        filtered = list(claims)
        if filters.status is not None:
            filtered = [claim for claim in filtered if claim.status == filters.status]
        if filters.claim_kind is not None:
            filtered = [claim for claim in filtered if claim.claim_kind == filters.claim_kind]
        if filters.place:
            filtered = [claim for claim in filtered if claim.place == filters.place]
        if filters.viewpoint_scope:
            filtered = [
                claim for claim in filtered if claim.viewpoint_scope == filters.viewpoint_scope
            ]
        return filtered

    def _apply_focus(self, claims: list[ApprovedClaim], focus: str) -> list[ApprovedClaim]:
        focus_tokens = self._tokens(focus)
        if not focus_tokens:
            return claims
        matched = []
        for claim in claims:
            haystack = self._normalize_text(
                " ".join(
                    [
                        claim.subject,
                        claim.predicate,
                        claim.value,
                        claim.place or "",
                        claim.viewpoint_scope or "",
                        claim.notes or "",
                    ]
                )
            )
            if all(token in haystack for token in focus_tokens):
                matched.append(claim)
        return matched

    def _evidence_index(self, claims: list[ApprovedClaim]) -> dict[str, EvidenceSnippet]:
        index: dict[str, EvidenceSnippet] = {}
        for claim in claims:
            for evidence_id in claim.evidence_ids:
                if evidence_id in index:
                    continue
                snippet = self.evidence_store.get_evidence(evidence_id)
                if snippet is not None:
                    index[evidence_id] = snippet
        return index

    def _source_index(self, evidence: list[EvidenceSnippet]) -> dict[str, SourceRecord]:
        index: dict[str, SourceRecord] = {}
        for snippet in evidence:
            if snippet.source_id in index:
                continue
            source = self.source_store.get_source(snippet.source_id)
            if source is not None:
                index[source.source_id] = source
        return index

    def _derive_warnings(
        self,
        claims: list[ApprovedClaim],
        evidence_by_id: dict[str, EvidenceSnippet],
    ) -> list[str]:
        warnings: list[str] = []
        if not claims:
            warnings.append("No approved claims matched the export request.")
            return warnings
        if any(not claim.evidence_ids for claim in claims):
            warnings.append("Some exported claims have no linked evidence snippets.")
        if any(
            claim.status in {ClaimStatus.CONTESTED, ClaimStatus.RUMOR, ClaimStatus.LEGEND}
            for claim in claims
        ):
            warnings.append("This packet includes contested or low-certainty material.")
        if not any(claim.time_start or claim.time_end for claim in claims):
            warnings.append("Temporal coverage is sparse; timeline output may be limited.")
        if any(
            evidence.notes and "contradict" in evidence.notes.lower()
            for evidence in evidence_by_id.values()
        ):
            warnings.append("Some supporting evidence is marked as contradictory.")
        return warnings

    def _build_file(
        self,
        filename: str,
        claims: list[ApprovedClaim],
        evidence_by_id: dict[str, EvidenceSnippet],
        source_by_id: dict[str, SourceRecord],
        include_evidence_footnotes: bool,
        renderer,
    ) -> LorePacketFile:
        selected_evidence = {
            evidence_id: evidence_by_id[evidence_id]
            for claim in claims
            for evidence_id in claim.evidence_ids
            if evidence_id in evidence_by_id
        }
        selected_sources = {
            source_id: source_by_id[source_id]
            for source_id in {snippet.source_id for snippet in selected_evidence.values()}
            if source_id in source_by_id
        }
        content = renderer(
            claims,
            selected_evidence,
            selected_sources,
            include_evidence_footnotes,
        )
        if not claims:
            content = self._placeholder_content(filename)
        return LorePacketFile(
            filename=filename,
            content=content,
            claim_ids=[claim.claim_id for claim in claims],
            source_ids=sorted(selected_sources),
        )

    def _render_basic_lore(
        self,
        claims: list[ApprovedClaim],
        evidence_by_id: dict[str, EvidenceSnippet],
        source_by_id: dict[str, SourceRecord],
        include_evidence_footnotes: bool,
    ) -> str:
        parts = ["# Basic Lore"]
        by_kind: dict[ClaimKind, list[ApprovedClaim]] = defaultdict(list)
        for claim in claims:
            by_kind[claim.claim_kind].append(claim)
        for kind in ClaimKind:
            kind_claims = by_kind.get(kind, [])
            if not kind_claims:
                continue
            parts.append(f"## {self._KIND_HEADINGS[kind]}")
            parts.extend(
                self._render_status_groups(
                    kind_claims, evidence_by_id, source_by_id, include_evidence_footnotes
                )
            )
        return "\n\n".join(parts)

    def _render_characters(
        self,
        claims: list[ApprovedClaim],
        evidence_by_id: dict[str, EvidenceSnippet],
        source_by_id: dict[str, SourceRecord],
        include_evidence_footnotes: bool,
    ) -> str:
        parts = ["# Characters"]
        by_subject: dict[str, list[ApprovedClaim]] = defaultdict(list)
        for claim in claims:
            by_subject[claim.subject].append(claim)
        for subject in sorted(by_subject):
            parts.append(f"## {subject}")
            parts.extend(
                self._render_status_groups(
                    by_subject[subject],
                    evidence_by_id,
                    source_by_id,
                    include_evidence_footnotes,
                )
            )
        return "\n\n".join(parts)

    def _render_timeline(
        self,
        claims: list[ApprovedClaim],
        evidence_by_id: dict[str, EvidenceSnippet],
        source_by_id: dict[str, SourceRecord],
        include_evidence_footnotes: bool,
    ) -> str:
        parts = ["# Timeline"]
        dated = [claim for claim in claims if claim.time_start or claim.time_end]
        undated = [claim for claim in claims if claim not in dated]
        dated = sorted(
            dated,
            key=lambda claim: (
                claim.time_start or claim.time_end or "zzzz",
                claim.time_end or "",
                claim.subject.lower(),
            ),
        )
        if dated:
            parts.append("## Dated Events")
            parts.extend(
                self._render_status_groups(
                    dated, evidence_by_id, source_by_id, include_evidence_footnotes
                )
            )
        if undated:
            parts.append("## Undated or Relative")
            parts.extend(
                self._render_status_groups(
                    undated, evidence_by_id, source_by_id, include_evidence_footnotes
                )
            )
        return "\n\n".join(parts)

    def _render_notes(
        self,
        claims: list[ApprovedClaim],
        evidence_by_id: dict[str, EvidenceSnippet],
        source_by_id: dict[str, SourceRecord],
        include_evidence_footnotes: bool,
        warnings: list[str],
    ) -> str:
        parts = ["# Notes"]
        if claims:
            parts.extend(
                self._render_status_groups(
                    claims, evidence_by_id, source_by_id, include_evidence_footnotes
                )
            )
        if warnings:
            parts.append("## Export Warnings")
            parts.extend(f"- {warning}" for warning in warnings)
        return "\n\n".join(parts)

    def _render_status_groups(
        self,
        claims: list[ApprovedClaim],
        evidence_by_id: dict[str, EvidenceSnippet],
        source_by_id: dict[str, SourceRecord],
        include_evidence_footnotes: bool,
    ) -> list[str]:
        parts: list[str] = []
        by_status: dict[ClaimStatus, list[ApprovedClaim]] = defaultdict(list)
        for claim in claims:
            by_status[claim.status].append(claim)
        for status in self._DEFAULT_STATUSES:
            status_claims = by_status.get(status, [])
            if not status_claims:
                continue
            parts.append(f"### {self._STATUS_HEADINGS[status]}")
            parts.extend(
                self._claim_bullet(
                    claim,
                    evidence_by_id,
                    source_by_id,
                    include_evidence_footnotes,
                )
                for claim in status_claims
            )
        return parts

    def _claim_bullet(
        self,
        claim: ApprovedClaim,
        evidence_by_id: dict[str, EvidenceSnippet],
        source_by_id: dict[str, SourceRecord],
        include_evidence_footnotes: bool,
    ) -> str:
        fragments = [
            f"**{claim.subject}** {self._humanize_predicate(claim.predicate)} {claim.value}"
        ]
        if claim.place:
            fragments.append(f"in {claim.place}")
        if claim.time_start or claim.time_end:
            if claim.time_start and claim.time_end:
                fragments.append(f"({claim.time_start} to {claim.time_end})")
            else:
                fragments.append(f"({claim.time_start or claim.time_end})")
        if claim.viewpoint_scope:
            fragments.append(f"[viewpoint: {claim.viewpoint_scope}]")
        if claim.notes:
            fragments.append(f"- {claim.notes}")
        line = " ".join(fragments)
        if include_evidence_footnotes:
            citations = self._citations_for_claim(claim, evidence_by_id, source_by_id)
            if citations:
                line = f"{line} Sources: {', '.join(citations)}"
        return f"- {line}"

    def _citations_for_claim(
        self,
        claim: ApprovedClaim,
        evidence_by_id: dict[str, EvidenceSnippet],
        source_by_id: dict[str, SourceRecord],
    ) -> list[str]:
        citations: list[str] = []
        for evidence_id in claim.evidence_ids:
            evidence = evidence_by_id.get(evidence_id)
            if evidence is None:
                continue
            source = source_by_id.get(evidence.source_id)
            label = source.title if source is not None else evidence.source_id
            locator = evidence.locator or (source.locator_hint if source is not None else None)
            if locator:
                citations.append(f"{label} ({locator})")
            else:
                citations.append(label)
        return citations

    def _character_claims(self, claims: list[ApprovedClaim]) -> list[ApprovedClaim]:
        return [
            claim
            for claim in claims
            if claim.claim_kind == ClaimKind.PERSON
            or claim.claim_kind == ClaimKind.RELATIONSHIP
            or "person" in self._normalize_text(claim.subject)
        ]

    def _timeline_claims(self, claims: list[ApprovedClaim]) -> list[ApprovedClaim]:
        return [
            claim
            for claim in claims
            if claim.claim_kind == ClaimKind.EVENT or claim.time_start or claim.time_end
        ]

    def _notes_claims(self, claims: list[ApprovedClaim]) -> list[ApprovedClaim]:
        return [
            claim
            for claim in claims
            if claim.status
            in {
                ClaimStatus.CONTESTED,
                ClaimStatus.RUMOR,
                ClaimStatus.LEGEND,
                ClaimStatus.AUTHOR_CHOICE,
            }
        ]

    def _placeholder_content(self, filename: str) -> str:
        title = filename.removesuffix(".md").replace("-", " ").title()
        return f"# {title}\n\nNo approved claims matched this export."

    def _humanize_predicate(self, predicate: str) -> str:
        return predicate.replace("_", " ").strip()

    def _normalize_text(self, text: str) -> str:
        return re.sub(r"\s+", " ", text.lower()).strip()

    def _tokens(self, text: str) -> list[str]:
        return [token for token in re.split(r"[^a-z0-9]+", self._normalize_text(text)) if token]
