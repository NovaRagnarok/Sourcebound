from __future__ import annotations

import json
import re
from collections import Counter
from hashlib import sha1
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

from source_aware_worldbuilding.adapters.file_backed import (
    FileBibleProjectProfileStore,
    FileBibleSectionStore,
    FileCandidateStore,
    FileEvidenceStore,
    FileExtractionRunStore,
    FileReviewStore,
    FileSourceDocumentStore,
    FileSourceStore,
    FileTextUnitStore,
    FileTruthStore,
)
from source_aware_worldbuilding.adapters.heuristic_extraction import HeuristicExtractionAdapter
from source_aware_worldbuilding.adapters.zotero_adapter import ZoteroCorpusAdapter
from source_aware_worldbuilding.domain.enums import (
    BibleSectionGenerationStatus,
    BibleSectionType,
    ClaimStatus,
    ReviewDecision,
)
from source_aware_worldbuilding.domain.errors import ZoteroConfigError
from source_aware_worldbuilding.domain.models import (
    BibleProjectProfileUpdateRequest,
    BibleSectionCreateRequest,
    BibleSectionFilters,
    IntakeTextRequest,
    ReviewClaimPatch,
    ReviewRequest,
    SourceDocumentRecord,
    SourceRecord,
    ZoteroCreatedItem,
    summarize_source_documents,
)
from source_aware_worldbuilding.services.bible import BibleWorkspaceService
from source_aware_worldbuilding.services.ingestion import IngestionService
from source_aware_worldbuilding.services.intake import IntakeService
from source_aware_worldbuilding.services.normalization import NormalizationService
from source_aware_worldbuilding.services.review import ReviewService
from source_aware_worldbuilding.settings import settings

_LANE_TOKEN_RE = re.compile(r"\bpilot_lane_id=([a-z0-9][a-z0-9_-]*)\b", re.I)
_DATA_FILES = (
    "sources.json",
    "source_documents.json",
    "text_units.json",
    "extraction_runs.json",
    "candidates.json",
    "evidence.json",
    "review_events.json",
    "claims.json",
    "claim_relationships.json",
    "bible_project_profiles.json",
    "bible_sections.json",
)


class PilotCorpusDocumentSpec(BaseModel):
    document_id: str
    document_kind: Literal["attachment", "note", "manual_text"]
    path: str | None = None
    locator: str
    filename: str | None = None
    mime_type: str = "text/plain"
    metadata_import_status: Literal["imported", "failed"] = "imported"
    attachment_discovery_status: Literal["not_applicable", "discovered", "missing"] = (
        "not_applicable"
    )
    attachment_fetch_status: Literal["not_applicable", "pending", "fetched", "failed"] = (
        "not_applicable"
    )
    text_extraction_status: Literal["pending", "extracted", "failed", "not_applicable"] = (
        "extracted"
    )
    normalization_status: Literal["queued", "completed", "failed", "not_applicable"] = "queued"
    present_in_latest_pull: bool = True
    stage_errors: list[str] = Field(default_factory=list)


class PilotCorpusExpectedOutcome(BaseModel):
    document_count: int
    stage_summary: dict[str, int] = Field(default_factory=dict)
    warning_contains: list[str] = Field(default_factory=list)


class PilotCorpusSourceSpec(BaseModel):
    lane_id: str
    proof_role: Literal["happy_path", "degraded"] = "happy_path"
    intake_mode: Literal["zotero_text", "manual_text", "manual_file"]
    title: str
    input_path: str
    author: str | None = None
    year: str | None = None
    source_type: str = "document"
    notes: str | None = None
    collection_key: str | None = None
    source: SourceRecord | None = None
    documents: list[PilotCorpusDocumentSpec] = Field(default_factory=list)
    expected_outcome: PilotCorpusExpectedOutcome


class PilotCorpusReviewSpec(BaseModel):
    decision: ReviewDecision
    predicate: str | None = None
    subject_contains: str | None = None
    value_contains: str
    override_status: ClaimStatus | None = None
    claim_patch: ReviewClaimPatch | None = None
    defer_state: Literal["needs_split", "needs_edit"] | None = None
    notes: str | None = None


class PilotCorpusSectionSpec(BaseModel):
    section_type: BibleSectionType
    filters: BibleSectionFilters = Field(default_factory=BibleSectionFilters)
    require_ready: bool = False


class PilotCorpusExtractionThresholds(BaseModel):
    important_fact_recall: float = 0.83
    claim_precision: float = 0.50
    avg_reviewer_actions: float = 1.50
    avg_anchor_focus: float = 0.60


class PilotCorpusThresholds(BaseModel):
    max_happy_path_failed_documents: int = 0
    max_blind_review_cards: int = 0
    max_unresolved_candidates: int = 0
    min_substantive_ready_sections: int = 1
    extraction_dataset_id: str | None = None
    extraction: PilotCorpusExtractionThresholds | None = None


class PilotCorpusLiveZoteroSpec(BaseModel):
    item_keys: list[str] = Field(default_factory=list)


class PilotCorpusManifest(BaseModel):
    corpus_id: str
    corpus_name: str
    project_id: str
    profile: BibleProjectProfileUpdateRequest
    sources: list[PilotCorpusSourceSpec] = Field(default_factory=list)
    reviews: list[PilotCorpusReviewSpec] = Field(default_factory=list)
    sections: list[PilotCorpusSectionSpec] = Field(default_factory=list)
    thresholds: PilotCorpusThresholds = Field(default_factory=PilotCorpusThresholds)
    live_zotero: PilotCorpusLiveZoteroSpec | None = None


class PilotCorpusSourceRunSummary(BaseModel):
    lane_id: str
    proof_role: Literal["happy_path", "degraded"]
    source_id: str | None = None
    title: str
    workflow_stage: str | None = None
    source_type: str | None = None
    document_count: int = 0
    failed_document_count: int = 0
    stage_summary: dict[str, int] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)
    gate_failures: list[str] = Field(default_factory=list)


class PilotCorpusSectionRunSummary(BaseModel):
    section_id: str
    section_type: BibleSectionType
    title: str
    generation_status: BibleSectionGenerationStatus
    ready_for_writer: bool
    claim_count: int = 0
    evidence_count: int = 0
    markdown_path: str


class PilotCorpusLiveZoteroSummary(BaseModel):
    status: Literal["skipped", "passed", "failed"]
    detail: str
    source_count: int = 0
    source_document_count: int = 0
    failed_document_count: int = 0
    warnings: list[str] = Field(default_factory=list)


class PilotCorpusRunSummary(BaseModel):
    corpus_id: str
    corpus_name: str
    data_dir: str
    gate_passed: bool
    gate_failures: list[str] = Field(default_factory=list)
    source_count: int = 0
    source_summaries: list[PilotCorpusSourceRunSummary] = Field(default_factory=list)
    document_count: int = 0
    failed_document_count: int = 0
    candidate_count: int = 0
    needs_split_count: int = 0
    needs_edit_count: int = 0
    evidence_quality_mix: dict[str, int] = Field(default_factory=dict)
    blind_review_card_count: int = 0
    approved_claim_count: int = 0
    unresolved_candidate_count: int = 0
    section_summaries: list[PilotCorpusSectionRunSummary] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    extraction_eval: dict[str, Any] | None = None
    live_zotero: PilotCorpusLiveZoteroSummary | None = None
    summary_path: str
    report_path: str


class PilotCorpusFixtureAdapter:
    def __init__(self, manifest: PilotCorpusManifest, corpus_dir: Path) -> None:
        self.manifest = manifest
        self.corpus_dir = corpus_dir
        self._spec_by_lane_id = {source.lane_id: source for source in manifest.sources}
        self._item_key_by_lane_id = {
            source.lane_id: f"PILOT-{manifest.corpus_id}-{source.lane_id}".upper()
            for source in manifest.sources
            if source.intake_mode.startswith("zotero_")
        }

    def pull_sources(self) -> list[SourceRecord]:
        return [
            self._build_remote_source(spec)
            for spec in self.manifest.sources
            if spec.intake_mode.startswith("zotero_")
        ]

    def pull_sources_by_item_keys(self, item_keys: list[str]) -> list[SourceRecord]:
        requested = set(item_keys)
        return [
            self._build_remote_source(spec)
            for spec in self.manifest.sources
            if self._item_key_by_lane_id.get(spec.lane_id) in requested
        ]

    def discover_source_documents(
        self,
        sources: list[SourceRecord],
        *,
        existing_documents: list[SourceDocumentRecord] | None = None,
        force_refresh: bool = False,
    ) -> list[SourceDocumentRecord]:
        _ = existing_documents, force_refresh
        documents: list[SourceDocumentRecord] = []
        for source in sources:
            lane_id = ((source.raw_metadata_json or {}).get("pilot_lane_id")) or ""
            spec = self._spec_by_lane_id.get(lane_id)
            if spec is None:
                continue
            for document_spec in spec.documents:
                documents.append(
                    _build_pilot_source_document_record(
                        manifest=self.manifest,
                        corpus_dir=self.corpus_dir,
                        source_id=source.source_id,
                        lane_id=lane_id,
                        spec=document_spec,
                    )
                )
        return documents

    def pull_text_units(self, sources) -> list[Any]:
        _ = sources
        return []

    def create_text_source(self, request: IntakeTextRequest) -> ZoteroCreatedItem:
        lane_id = _lane_id_from_text(request.notes)
        spec = self._spec_by_lane_id.get(lane_id)
        if spec is None:
            raise ZoteroConfigError("Unknown pilot lane for text intake.")
        if spec.intake_mode == "manual_text":
            raise ZoteroConfigError("Manual text lane intentionally uses local fallback.")
        return ZoteroCreatedItem(
            zotero_item_key=self._item_key_by_lane_id[lane_id],
            title=spec.title,
            item_type=spec.source_type,
            collection_key=spec.collection_key,
        )

    def create_url_source(self, request):
        _ = request
        raise NotImplementedError("Pilot corpus fixture does not use URL intake.")

    def create_file_source(self, **kwargs):
        lane_id = _lane_id_from_text(kwargs.get("notes"))
        spec = self._spec_by_lane_id.get(lane_id)
        if spec is None:
            raise ZoteroConfigError("Unknown pilot lane for file intake.")
        if spec.intake_mode == "manual_file":
            raise ZoteroConfigError("Manual file lane intentionally uses local fallback.")
        raise NotImplementedError("Pilot corpus fixture does not use Zotero file intake.")

    def _build_remote_source(self, spec: PilotCorpusSourceSpec) -> SourceRecord:
        if spec.source is None:
            raise ValueError(f"Pilot lane '{spec.lane_id}' is missing source metadata.")
        source = spec.source.model_copy(deep=True)
        source.external_source = "pilot_fixture"
        source.external_id = self._item_key_by_lane_id[spec.lane_id]
        source.zotero_item_key = self._item_key_by_lane_id[spec.lane_id]
        metadata = dict(source.raw_metadata_json or {})
        metadata.update(
            {
                "pilot_corpus_id": self.manifest.corpus_id,
                "pilot_lane_id": spec.lane_id,
            }
        )
        source.raw_metadata_json = metadata
        return source


def available_pilot_corpora() -> list[str]:
    root = _pilot_corpora_root()
    if not root.exists():
        return []
    return sorted(item.name for item in root.iterdir() if (item / "manifest.json").exists())


def load_pilot_corpus_manifest(corpus_id: str) -> tuple[PilotCorpusManifest, Path]:
    corpus_dir = _pilot_corpora_root() / corpus_id
    manifest_path = corpus_dir / "manifest.json"
    if not manifest_path.exists():
        available = ", ".join(available_pilot_corpora()) or "none"
        raise ValueError(
            f"Unknown pilot corpus '{corpus_id}'. Available pilot corpora: {available}."
        )
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    return PilotCorpusManifest.model_validate(payload), corpus_dir


def run_pilot_corpus(
    corpus_id: str,
    *,
    data_dir: Path,
    live_zotero: bool = False,
) -> PilotCorpusRunSummary:
    manifest, corpus_dir = load_pilot_corpus_manifest(corpus_id)
    output_dir = data_dir.resolve()
    _reset_output_dir(output_dir)

    source_store = FileSourceStore(output_dir)
    source_document_store = FileSourceDocumentStore(output_dir)
    text_unit_store = FileTextUnitStore(output_dir)
    run_store = FileExtractionRunStore(output_dir)
    candidate_store = FileCandidateStore(output_dir)
    evidence_store = FileEvidenceStore(output_dir)
    review_store = FileReviewStore(output_dir)
    truth_store = FileTruthStore(output_dir)
    bible_profile_store = FileBibleProjectProfileStore(output_dir)
    bible_section_store = FileBibleSectionStore(output_dir)

    intake_service = IntakeService(
        corpus=PilotCorpusFixtureAdapter(manifest, corpus_dir),
        source_store=source_store,
        source_document_store=source_document_store,
    )
    lane_to_source_id: dict[str, str] = {}
    warnings: list[str] = []
    for source_spec in manifest.sources:
        intake_result = _run_lane_intake(
            intake_service=intake_service,
            source_spec=source_spec,
            corpus_dir=corpus_dir,
        )
        warnings.extend(intake_result.warnings)
        if intake_result.pulled_sources:
            lane_to_source_id[source_spec.lane_id] = intake_result.pulled_sources[0].source_id

    normalization_service = NormalizationService(
        source_document_store=source_document_store,
        text_unit_store=text_unit_store,
        source_store=source_store,
    )
    normalized = normalization_service.normalize_documents(
        source_ids=list(lane_to_source_id.values())
    )
    warnings.extend(cast_list_str(normalized.get("warnings")))

    ingestion_service = IngestionService(
        corpus=PilotCorpusFixtureAdapter(manifest, corpus_dir),
        extractor=HeuristicExtractionAdapter(),
        source_store=source_store,
        text_unit_store=text_unit_store,
        source_document_store=source_document_store,
        run_store=run_store,
        candidate_store=candidate_store,
        evidence_store=evidence_store,
    )
    extraction_output = ingestion_service.extract_candidates(source_ids=list(lane_to_source_id.values()))

    review_service = ReviewService(
        candidate_store=candidate_store,
        truth_store=truth_store,
        review_store=review_store,
        evidence_store=evidence_store,
        source_store=source_store,
        text_unit_store=text_unit_store,
    )
    review_cards = review_service.list_review_queue()
    consumed_candidate_ids: set[str] = set()
    for review_spec in manifest.reviews:
        candidate = _find_candidate_for_review_action(
            candidate_store.list_candidates(),
            review_spec,
            consumed_candidate_ids=consumed_candidate_ids,
        )
        review_service.review_candidate(
            candidate.candidate_id,
            ReviewRequest(
                decision=review_spec.decision,
                override_status=review_spec.override_status,
                claim_patch=review_spec.claim_patch,
                defer_state=review_spec.defer_state,
                notes=review_spec.notes,
            ),
        )
        consumed_candidate_ids.add(candidate.candidate_id)

    bible_service = BibleWorkspaceService(
        profile_store=bible_profile_store,
        section_store=bible_section_store,
        truth_store=truth_store,
        evidence_store=evidence_store,
        source_store=source_store,
    )
    bible_service.save_profile(manifest.project_id, manifest.profile)
    section_summaries: list[PilotCorpusSectionRunSummary] = []
    for section_spec in manifest.sections:
        section = bible_service.create_section(
            BibleSectionCreateRequest(
                project_id=manifest.project_id,
                section_type=section_spec.section_type,
                filters=section_spec.filters,
            )
        )
        markdown_path = output_dir / f"{section.section_type.value}.md"
        markdown_path.write_text(section.content, encoding="utf-8")
        section_summaries.append(
            PilotCorpusSectionRunSummary(
                section_id=section.section_id,
                section_type=section.section_type,
                title=section.title,
                generation_status=section.generation_status,
                ready_for_writer=section.ready_for_writer,
                claim_count=len(section.references.claim_ids),
                evidence_count=len(section.references.evidence_ids),
                markdown_path=str(markdown_path),
            )
        )

    source_summaries = _build_source_summaries(
        manifest=manifest,
        lane_to_source_id=lane_to_source_id,
        source_store=source_store,
        source_document_store=source_document_store,
    )
    final_candidates = candidate_store.list_candidates()
    evidence_quality_mix = Counter(card.evidence_quality for card in review_cards)
    extraction_summary = None
    if manifest.thresholds.extraction_dataset_id:
        from source_aware_worldbuilding.extraction_eval import (
            evaluate_prepared_extraction_dataset,
        )

        extraction_output_root = output_dir / "extraction_eval"
        extraction_summary = evaluate_prepared_extraction_dataset(
            manifest.thresholds.extraction_dataset_id,
            output_root=extraction_output_root,
            sources=source_store.list_sources(),
            text_units=text_unit_store.list_text_units(),
        )

    live_summary = _run_live_zotero_smoke(manifest) if live_zotero else None
    gate_failures = _evaluate_pilot_thresholds(
        manifest=manifest,
        source_summaries=source_summaries,
        review_cards=review_cards,
        final_candidates=final_candidates,
        section_summaries=section_summaries,
        extraction_eval=extraction_summary,
    )
    warnings.extend(
        warning
        for source_summary in source_summaries
        for warning in source_summary.warnings
        if warning
    )
    warnings.extend(
        f"{source_summary.lane_id}: {failure}"
        for source_summary in source_summaries
        if source_summary.proof_role == "degraded"
        for failure in source_summary.gate_failures
    )
    warnings = list(dict.fromkeys(warnings))
    summary_path = output_dir / "pilot_summary.json"
    report_path = output_dir / "pilot_report.md"
    summary = PilotCorpusRunSummary(
        corpus_id=manifest.corpus_id,
        corpus_name=manifest.corpus_name,
        data_dir=str(output_dir),
        gate_passed=not gate_failures,
        gate_failures=gate_failures,
        source_count=len(source_summaries),
        source_summaries=source_summaries,
        document_count=len(source_document_store.list_source_documents()),
        failed_document_count=sum(
            item.failed_document_count for item in source_summaries
        ),
        candidate_count=len(extraction_output.candidates),
        needs_split_count=sum(1 for item in final_candidates if item.review_state == "needs_split"),
        needs_edit_count=sum(1 for item in final_candidates if item.review_state == "needs_edit"),
        evidence_quality_mix=dict(evidence_quality_mix),
        blind_review_card_count=evidence_quality_mix.get("blind", 0),
        approved_claim_count=len(truth_store.list_claims()),
        unresolved_candidate_count=sum(
            1
            for item in final_candidates
            if item.review_state in {"pending", "needs_split", "needs_edit"}
        ),
        section_summaries=section_summaries,
        warnings=warnings,
        extraction_eval=extraction_summary,
        live_zotero=live_summary,
        summary_path=str(summary_path),
        report_path=str(report_path),
    )
    summary_path.write_text(json.dumps(summary.model_dump(mode="json"), indent=2), encoding="utf-8")
    report_path.write_text(
        _build_pilot_report(summary, review_cards),
        encoding="utf-8",
    )
    return summary


def _run_lane_intake(
    *,
    intake_service: IntakeService,
    source_spec: PilotCorpusSourceSpec,
    corpus_dir: Path,
):
    lane_notes = _with_lane_note(source_spec.notes, source_spec.lane_id)
    if source_spec.intake_mode in {"zotero_text", "manual_text"}:
        text = (corpus_dir / source_spec.input_path).read_text(encoding="utf-8").strip()
        return intake_service.intake_text(
            IntakeTextRequest(
                title=source_spec.title,
                text=text,
                author=source_spec.author,
                year=source_spec.year,
                source_type=source_spec.source_type,
                notes=lane_notes,
                collection_key=source_spec.collection_key,
            )
        )
    if source_spec.intake_mode == "manual_file":
        path = corpus_dir / source_spec.input_path
        return intake_service.intake_file(
            filename=path.name,
            content_type=None,
            content=path.read_bytes(),
            title=source_spec.title,
            source_type=source_spec.source_type,
            notes=lane_notes,
            collection_key=source_spec.collection_key,
        )
    raise ValueError(f"Unsupported pilot intake mode '{source_spec.intake_mode}'.")


def _build_source_summaries(
    *,
    manifest: PilotCorpusManifest,
    lane_to_source_id: dict[str, str],
    source_store: FileSourceStore,
    source_document_store: FileSourceDocumentStore,
) -> list[PilotCorpusSourceRunSummary]:
    summaries: list[PilotCorpusSourceRunSummary] = []
    for source_spec in manifest.sources:
        source_id = lane_to_source_id.get(source_spec.lane_id)
        source = source_store.get_source(source_id) if source_id else None
        documents = (
            source_document_store.list_source_documents(source_id=source_id)
            if source_id
            else []
        )
        stage_summary = summarize_source_documents(documents)
        warnings = list(
            dict.fromkeys(
                error
                for document in documents
                for error in document.stage_errors
                if error
            )
        )
        gate_failures: list[str] = []
        if len(documents) != source_spec.expected_outcome.document_count:
            gate_failures.append(
                f"expected {source_spec.expected_outcome.document_count} documents, got {len(documents)}"
            )
        for key, value in source_spec.expected_outcome.stage_summary.items():
            if stage_summary.get(key, 0) != value:
                gate_failures.append(
                    f"expected stage_summary[{key}]={value}, got {stage_summary.get(key, 0)}"
                )
        for expected_warning in source_spec.expected_outcome.warning_contains:
            if not any(expected_warning in warning for warning in warnings):
                gate_failures.append(f"missing expected warning: {expected_warning}")
        summaries.append(
            PilotCorpusSourceRunSummary(
                lane_id=source_spec.lane_id,
                proof_role=source_spec.proof_role,
                source_id=source_id,
                title=(source.title if source is not None else source_spec.title),
                workflow_stage=(source.workflow_stage if source is not None else None),
                source_type=(source.source_type if source is not None else source_spec.source_type),
                document_count=len(documents),
                failed_document_count=stage_summary.get("failed", 0),
                stage_summary=stage_summary,
                warnings=warnings,
                gate_failures=gate_failures,
            )
        )
    return summaries


def _evaluate_pilot_thresholds(
    *,
    manifest: PilotCorpusManifest,
    source_summaries: list[PilotCorpusSourceRunSummary],
    review_cards,
    final_candidates,
    section_summaries: list[PilotCorpusSectionRunSummary],
    extraction_eval: dict[str, Any] | None,
) -> list[str]:
    failures: list[str] = []
    for source_summary in source_summaries:
        if source_summary.proof_role != "happy_path":
            continue
        failures.extend(
            f"{source_summary.lane_id}: {failure}" for failure in source_summary.gate_failures
        )
    happy_path_failed_documents = sum(
        source_summary.failed_document_count
        for source_summary in source_summaries
        if source_summary.proof_role == "happy_path"
    )
    if happy_path_failed_documents > manifest.thresholds.max_happy_path_failed_documents:
        failures.append(
            "happy-path document failures exceeded threshold "
            f"({happy_path_failed_documents} > {manifest.thresholds.max_happy_path_failed_documents})"
        )
    blind_review_cards = sum(1 for card in review_cards if card.evidence_quality == "blind")
    if blind_review_cards > manifest.thresholds.max_blind_review_cards:
        failures.append(
            "blind review cards exceeded threshold "
            f"({blind_review_cards} > {manifest.thresholds.max_blind_review_cards})"
        )
    unresolved_candidates = sum(
        1
        for candidate in final_candidates
        if candidate.review_state in {"pending", "needs_split", "needs_edit"}
    )
    if unresolved_candidates > manifest.thresholds.max_unresolved_candidates:
        failures.append(
            "unresolved candidate count exceeded threshold "
            f"({unresolved_candidates} > {manifest.thresholds.max_unresolved_candidates})"
        )
    ready_sections = sum(
        1
        for section in section_summaries
        if section.ready_for_writer and section.section_type != BibleSectionType.AUTHOR_DECISIONS
    )
    if ready_sections < manifest.thresholds.min_substantive_ready_sections:
        failures.append(
            "substantive ready sections fell below threshold "
            f"({ready_sections} < {manifest.thresholds.min_substantive_ready_sections})"
        )
    for section_spec in manifest.sections:
        if not section_spec.require_ready:
            continue
        summary = next(
            (
                item
                for item in section_summaries
                if item.section_type == section_spec.section_type
            ),
            None,
        )
        if summary is None or not summary.ready_for_writer:
            failures.append(f"{section_spec.section_type.value} did not end ready_for_writer.")
    if manifest.thresholds.extraction and extraction_eval is not None:
        heuristic = next(
            (item for item in extraction_eval.get("paths", []) if item.get("path") == "heuristic"),
            None,
        )
        if heuristic is None:
            failures.append("heuristic extraction eval result was missing.")
        else:
            metrics = heuristic.get("metrics", {})
            evidence = heuristic.get("evidence_span_quality", {})
            burden = heuristic.get("reviewer_edit_burden", {})
            thresholds = manifest.thresholds.extraction
            if metrics.get("important_fact_recall", 0.0) < thresholds.important_fact_recall:
                failures.append("important_fact_recall fell below pilot threshold.")
            if metrics.get("claim_precision", 0.0) < thresholds.claim_precision:
                failures.append("claim_precision fell below pilot threshold.")
            if burden.get("avg_actions_per_matched_candidate", 99.0) > thresholds.avg_reviewer_actions:
                failures.append("avg reviewer actions exceeded pilot threshold.")
            if evidence.get("avg_anchor_focus", 0.0) < thresholds.avg_anchor_focus:
                failures.append("avg anchor focus fell below pilot threshold.")
    return failures


def _run_live_zotero_smoke(manifest: PilotCorpusManifest) -> PilotCorpusLiveZoteroSummary:
    live_spec = manifest.live_zotero
    if live_spec is None or not live_spec.item_keys:
        return PilotCorpusLiveZoteroSummary(
            status="skipped",
            detail="No live Zotero item keys were declared for this pilot corpus.",
        )
    if not settings.zotero_library_id:
        return PilotCorpusLiveZoteroSummary(
            status="skipped",
            detail="Live Zotero smoke skipped because ZOTERO_LIBRARY_ID is not configured.",
        )

    adapter = ZoteroCorpusAdapter()
    try:
        sources = adapter.pull_sources_by_item_keys(live_spec.item_keys)
        documents = adapter.discover_source_documents(sources, existing_documents=[], force_refresh=True)
    except Exception as exc:
        return PilotCorpusLiveZoteroSummary(
            status="failed",
            detail=str(exc),
        )
    if not sources:
        return PilotCorpusLiveZoteroSummary(
            status="failed",
            detail="Live Zotero smoke did not return any sources for the declared item keys.",
        )
    if not documents:
        return PilotCorpusLiveZoteroSummary(
            status="failed",
            detail="Live Zotero smoke returned sources but no discoverable source documents.",
            source_count=len(sources),
        )
    warnings = list(
        dict.fromkeys(
            error
            for document in documents
            for error in document.stage_errors
            if error
        )
    )
    failed_document_count = sum(
        1
        for document in documents
        if document.metadata_import_status == "failed"
        or document.text_extraction_status == "failed"
        or document.normalization_status == "failed"
    )
    if failed_document_count:
        return PilotCorpusLiveZoteroSummary(
            status="failed",
            detail="Live Zotero smoke discovered documents, but one or more documents failed.",
            source_count=len(sources),
            source_document_count=len(documents),
            failed_document_count=failed_document_count,
            warnings=warnings,
        )
    return PilotCorpusLiveZoteroSummary(
        status="passed",
        detail="Live Zotero pull and document discovery succeeded.",
        source_count=len(sources),
        source_document_count=len(documents),
        failed_document_count=failed_document_count,
        warnings=warnings,
    )


def _build_pilot_source_document_record(
    *,
    manifest: PilotCorpusManifest,
    corpus_dir: Path,
    source_id: str,
    lane_id: str,
    spec: PilotCorpusDocumentSpec,
) -> SourceDocumentRecord:
    raw_text = None
    checksum = None
    storage_path = None
    if spec.path is not None:
        path = corpus_dir / spec.path
        raw_text = path.read_text(encoding="utf-8").strip()
        checksum = sha1(raw_text.encode("utf-8")).hexdigest()
        if spec.document_kind == "attachment":
            storage_path = str(path.resolve())
    return SourceDocumentRecord(
        document_id=spec.document_id,
        source_id=source_id,
        document_kind=spec.document_kind,
        external_id=f"{manifest.corpus_id}:{lane_id}:{spec.document_id}",
        filename=spec.filename or (Path(spec.path).name if spec.path else spec.document_id),
        mime_type=spec.mime_type,
        storage_path=storage_path,
        metadata_import_status=spec.metadata_import_status,
        attachment_discovery_status=spec.attachment_discovery_status,
        attachment_fetch_status=spec.attachment_fetch_status,
        text_extraction_status=spec.text_extraction_status,
        normalization_status=spec.normalization_status,
        present_in_latest_pull=spec.present_in_latest_pull,
        content_checksum=checksum,
        locator=spec.locator,
        raw_text=raw_text,
        stage_errors=list(spec.stage_errors),
        raw_metadata_json={
            "pilot_corpus_id": manifest.corpus_id,
            "pilot_lane_id": lane_id,
            "relative_path": spec.path,
        },
    )


def _build_pilot_report(summary: PilotCorpusRunSummary, review_cards) -> str:
    lines = [
        f"# {summary.corpus_name}",
        "",
        f"- Gate: {'pass' if summary.gate_passed else 'fail'}",
        f"- Sources: {summary.source_count}",
        f"- Documents: {summary.document_count} (failed: {summary.failed_document_count})",
        f"- Candidates: {summary.candidate_count}",
        f"- Approved claims: {summary.approved_claim_count}",
        f"- Unresolved candidates: {summary.unresolved_candidate_count}",
        "",
        "## Source Lanes",
    ]
    for source in summary.source_summaries:
        lines.append(
            f"- `{source.lane_id}` ({source.proof_role}): {source.workflow_stage or 'unknown'} | "
            f"documents={source.document_count} | failed={source.failed_document_count} | "
            f"stage_summary={json.dumps(source.stage_summary, sort_keys=True)}"
        )
        for warning in source.warnings[:3]:
            lines.append(f"  note: {warning}")
    lines.extend(
        [
            "",
            "## Review Queue",
            f"- Evidence quality mix: {json.dumps(summary.evidence_quality_mix, sort_keys=True)}",
            f"- Blind cards: {summary.blind_review_card_count}",
            f"- Needs split: {summary.needs_split_count}",
            f"- Needs edit: {summary.needs_edit_count}",
        ]
    )
    preview_cards = [card for card in review_cards if card.primary_evidence is not None][:3]
    if preview_cards:
        lines.append("- Review evidence preview:")
        for card in preview_cards:
            lines.append(
                "  "
                + f"{card.candidate_id}: {card.primary_evidence.locator} "
                + f"[{card.primary_evidence.span_start}, {card.primary_evidence.span_end}]"
            )
    lines.extend(["", "## Writer Outputs"])
    for section in summary.section_summaries:
        lines.append(
            f"- `{section.section_type.value}`: {section.generation_status.value} | "
            f"ready={section.ready_for_writer} | claims={section.claim_count} | "
            f"markdown={section.markdown_path}"
        )
    if summary.extraction_eval is not None:
        heuristic = next(
            (item for item in summary.extraction_eval.get("paths", []) if item.get("path") == "heuristic"),
            None,
        )
        if heuristic is not None:
            metrics = heuristic.get("metrics", {})
            evidence = heuristic.get("evidence_span_quality", {})
            burden = heuristic.get("reviewer_edit_burden", {})
            lines.extend(
                [
                    "",
                    "## Extraction Eval",
                    f"- important_fact_recall: {metrics.get('important_fact_recall')}",
                    f"- claim_precision: {metrics.get('claim_precision')}",
                    f"- avg_actions_per_matched_candidate: {burden.get('avg_actions_per_matched_candidate')}",
                    f"- avg_anchor_focus: {evidence.get('avg_anchor_focus')}",
                ]
            )
    if summary.live_zotero is not None:
        lines.extend(
            [
                "",
                "## Live Zotero",
                f"- Status: {summary.live_zotero.status}",
                f"- Detail: {summary.live_zotero.detail}",
            ]
        )
    if summary.gate_failures:
        lines.extend(["", "## Gate Failures"])
        lines.extend([f"- {failure}" for failure in summary.gate_failures])
    return "\n".join(lines).strip() + "\n"


def _find_candidate_for_review_action(
    candidates,
    review_spec: PilotCorpusReviewSpec,
    *,
    consumed_candidate_ids: set[str] | None = None,
):
    matches = []
    consumed_candidate_ids = consumed_candidate_ids or set()
    for candidate in candidates:
        if candidate.candidate_id in consumed_candidate_ids:
            continue
        if review_spec.predicate is not None and candidate.predicate != review_spec.predicate:
            continue
        if review_spec.subject_contains is not None and (
            review_spec.subject_contains.lower() not in candidate.subject.lower()
        ):
            continue
        if review_spec.value_contains.lower() not in candidate.value.lower():
            continue
        matches.append(candidate)
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise ValueError(
            "Review rule matched multiple candidates; make it more specific "
            f"(value_contains={review_spec.value_contains!r})."
        )
    raise ValueError(
        "Could not find a candidate matching review rule "
        f"value_contains={review_spec.value_contains!r}."
    )


def _pilot_corpora_root() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "pilot" / "corpora"


def _reset_output_dir(data_dir: Path) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    for filename in _DATA_FILES:
        path = data_dir / filename
        if path.exists():
            path.unlink()


def _with_lane_note(notes: str | None, lane_id: str) -> str:
    prefix = (notes or "").strip()
    token = f"pilot_lane_id={lane_id}"
    return f"{prefix}\n{token}".strip()


def _lane_id_from_text(value: str | None) -> str:
    match = _LANE_TOKEN_RE.search(value or "")
    if match is None:
        return ""
    return match.group(1)


def cast_list_str(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]
