from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from hashlib import sha1
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Literal, cast

from pydantic import BaseModel, Field

from source_aware_worldbuilding.adapters.file_backed import (
    FileSourceDocumentStore,
    FileSourceStore,
    FileTextUnitStore,
)
from source_aware_worldbuilding.adapters.graphrag_adapter import (
    GraphRAGArtifactBundle,
    GraphRAGExtractionAdapter,
)
from source_aware_worldbuilding.adapters.heuristic_extraction import HeuristicExtractionAdapter
from source_aware_worldbuilding.demo_corpus import (
    _build_source_document_record,
    load_demo_corpus_manifest,
)
from source_aware_worldbuilding.domain.enums import ExtractionRunStatus
from source_aware_worldbuilding.domain.models import (
    CandidateClaim,
    EvidenceSnippet,
    ExtractionOutput,
    ExtractionRun,
    SourceDocumentRecord,
    SourceRecord,
    TextUnit,
)
from source_aware_worldbuilding.pilot_corpus import (
    _build_pilot_source_document_record,
    available_pilot_corpora,
    load_pilot_corpus_manifest,
)
from source_aware_worldbuilding.services.normalization import NormalizationService

_NORMALIZE_RE = re.compile(r"[^0-9a-z]+")


@dataclass(frozen=True, slots=True)
class _EvalPathSpec:
    path: str
    kind: Literal["extraction", "mapping_fixture"]
    notes: str


class GoldClaimSpec(BaseModel):
    gold_claim_id: str
    label: str
    importance: Literal["high", "medium", "low"] = "medium"
    target_subject: str
    target_predicate: str
    target_value: str
    subject_patterns: list[str] = Field(default_factory=list)
    predicate_patterns: list[str] = Field(default_factory=list)
    value_patterns: list[str] = Field(default_factory=list)
    evidence_patterns: list[str] = Field(default_factory=list)
    notes: str | None = None


class GraphRAGFixtureClaimSpec(BaseModel):
    claim_id: str
    type: str
    description: str
    subject_id: str
    object_id: str
    status: str = "TRUE"
    source_text: str
    text_unit_locator: str
    start_date: str | None = None
    end_date: str | None = None


class ContradictionGroupSpec(BaseModel):
    group_id: str
    gold_claim_ids: list[str] = Field(default_factory=list)
    notes: str | None = None


class ExtractionEvalDataset(BaseModel):
    evaluation_id: str
    title: str
    corpus_id: str
    gold_claims: list[GoldClaimSpec] = Field(default_factory=list)
    contradiction_groups: list[ContradictionGroupSpec] = Field(default_factory=list)
    graphrag_fixture_claims: list[GraphRAGFixtureClaimSpec] = Field(default_factory=list)


@dataclass(slots=True)
class _PreparedCorpus:
    sources: list[SourceRecord]
    text_units: list[TextUnit]
    dataset_path: Path


@dataclass(slots=True)
class _MatchRecord:
    gold_claim_id: str
    candidate_id: str
    score: int
    subject_match: bool
    predicate_match: bool
    value_match: bool
    evidence_match: bool
    evidence_id: str | None
    anchor_text: str | None


@dataclass(slots=True)
class _PathRunResult:
    path: str
    kind: Literal["extraction", "mapping_fixture"]
    notes: str
    output: ExtractionOutput
    repeated_outputs: list[ExtractionOutput]


def available_extraction_eval_datasets() -> list[str]:
    root = _dataset_root()
    if not root.exists():
        return []
    return sorted(item.stem for item in root.glob("*.json"))


def load_extraction_eval_dataset(dataset_id: str) -> ExtractionEvalDataset:
    path = _dataset_root() / f"{dataset_id}.json"
    if not path.exists():
        available = ", ".join(available_extraction_eval_datasets()) or "none"
        raise ValueError(
            f"Unknown extraction evaluation dataset '{dataset_id}'. "
            f"Available datasets: {available}."
        )
    return ExtractionEvalDataset.model_validate_json(path.read_text(encoding="utf-8"))


def evaluate_extraction_dataset(
    dataset_id: str,
    *,
    output_root: Path,
    repeat: int = 3,
) -> dict[str, object]:
    if repeat < 1:
        raise ValueError("repeat must be >= 1.")
    dataset = load_extraction_eval_dataset(dataset_id)
    prepared = _prepare_corpus(dataset)
    return _evaluate_dataset_with_prepared(
        dataset=dataset,
        prepared=prepared,
        output_root=output_root,
        repeat=repeat,
    )


def evaluate_extraction_suite(
    *,
    output_root: Path,
    dataset_ids: list[str] | None = None,
    repeat: int = 3,
) -> dict[str, object]:
    if repeat < 1:
        raise ValueError("repeat must be >= 1.")
    selected_dataset_ids = (
        available_extraction_eval_datasets() if dataset_ids is None else list(dataset_ids)
    )
    if not selected_dataset_ids:
        raise ValueError("No extraction evaluation datasets are available.")

    output_root.mkdir(parents=True, exist_ok=True)
    dataset_summaries = [
        evaluate_extraction_dataset(
            dataset_id,
            output_root=output_root / dataset_id,
            repeat=repeat,
        )
        for dataset_id in selected_dataset_ids
    ]
    summary: dict[str, Any] = {
        "dataset_count": len(dataset_summaries),
        "repeat": repeat,
        "datasets": [
            {
                "evaluation_id": dataset_summary["evaluation_id"],
                "title": dataset_summary["title"],
                "corpus_id": dataset_summary["corpus_id"],
                "path_count": len(cast(list[dict[str, Any]], dataset_summary["paths"])),
                "artifact_dir": str(output_root / cast(str, dataset_summary["evaluation_id"])),
            }
            for dataset_summary in dataset_summaries
        ],
        "rows": _build_suite_rows(dataset_summaries),
    }
    (output_root / "suite-summary.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )
    (output_root / "suite-report.md").write_text(
        _build_suite_markdown_report(summary),
        encoding="utf-8",
    )
    return summary


def evaluate_prepared_extraction_dataset(
    dataset_id: str,
    *,
    output_root: Path,
    sources: list[SourceRecord],
    text_units: list[TextUnit],
    repeat: int = 3,
) -> dict[str, object]:
    if repeat < 1:
        raise ValueError("repeat must be >= 1.")
    dataset = load_extraction_eval_dataset(dataset_id)
    prepared = _PreparedCorpus(
        sources=sources,
        text_units=text_units,
        dataset_path=_dataset_root() / f"{dataset.evaluation_id}.json",
    )
    return _evaluate_dataset_with_prepared(
        dataset=dataset,
        prepared=prepared,
        output_root=output_root,
        repeat=repeat,
    )


def _evaluate_dataset_with_prepared(
    *,
    dataset: ExtractionEvalDataset,
    prepared: _PreparedCorpus,
    output_root: Path,
    repeat: int,
) -> dict[str, object]:
    output_root.mkdir(parents=True, exist_ok=True)

    path_reports: list[dict[str, Any]] = []
    skipped_paths: list[dict[str, str]] = []
    for path_spec in _available_eval_paths(dataset):
        try:
            initial_output = _run_extraction_path(
                dataset=dataset,
                prepared=prepared,
                path_spec=path_spec,
                run_suffix="run-1",
            )
            repeated_outputs = [initial_output] + [
                _run_extraction_path(
                    dataset=dataset,
                    prepared=prepared,
                    path_spec=path_spec,
                    run_suffix=f"run-{index + 2}",
                )
                for index in range(repeat - 1)
            ]
        except Exception as exc:
            skipped_paths.append(
                {
                    "path": path_spec.path,
                    "kind": path_spec.kind,
                    "reason": str(exc),
                }
            )
            if path_spec.path == "graphrag_live" and dataset.graphrag_fixture_claims:
                fixture_path = _EvalPathSpec(
                    path="graphrag_mapping_fixture",
                    kind="mapping_fixture",
                    notes=(
                        "Live GraphRAG extraction was not runnable in this environment, so the "
                        "benchmark fell back to a fixture-backed GraphRAG mapping check."
                    ),
                )
                initial_output = _run_extraction_path(
                    dataset=dataset,
                    prepared=prepared,
                    path_spec=fixture_path,
                    run_suffix="run-1",
                )
                repeated_outputs = [initial_output] + [
                    _run_extraction_path(
                        dataset=dataset,
                        prepared=prepared,
                        path_spec=fixture_path,
                        run_suffix=f"run-{index + 2}",
                    )
                    for index in range(repeat - 1)
                ]
                path_spec = fixture_path
            else:
                continue
        path_reports.append(
            _score_path_result(
                dataset=dataset,
                path_result=_PathRunResult(
                    path=path_spec.path,
                    kind=path_spec.kind,
                    notes=path_spec.notes,
                    output=initial_output,
                    repeated_outputs=repeated_outputs,
                ),
            )
        )

    summary: dict[str, Any] = {
        "evaluation_id": dataset.evaluation_id,
        "title": dataset.title,
        "corpus_id": dataset.corpus_id,
        "repeat": repeat,
        "paths": path_reports,
        "comparisons": _build_comparisons(path_reports),
        "skipped_paths": skipped_paths,
    }
    (output_root / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    (output_root / "report.md").write_text(
        _build_markdown_report(summary),
        encoding="utf-8",
    )
    return summary


def _dataset_root() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "evals" / "extraction"


def _prepare_corpus(dataset: ExtractionEvalDataset) -> _PreparedCorpus:
    with TemporaryDirectory(prefix="sourcebound-extraction-eval-") as temp_dir:
        data_dir = Path(temp_dir)
        source_store = FileSourceStore(data_dir)
        source_document_store = FileSourceDocumentStore(data_dir)
        text_unit_store = FileTextUnitStore(data_dir)

        sources: list[SourceRecord]
        source_documents: list[SourceDocumentRecord]
        source_ids: list[str]
        if dataset.corpus_id in set(available_pilot_corpora()):
            pilot_manifest, corpus_dir = load_pilot_corpus_manifest(dataset.corpus_id)
            sources = []
            source_documents = []
            for source_spec in pilot_manifest.sources:
                if source_spec.source is None:
                    continue
                source = source_spec.source.model_copy(deep=True)
                source.external_source = "pilot_fixture"
                source.external_id = (
                    f"PILOT-{pilot_manifest.corpus_id}-{source_spec.lane_id}".upper()
                )
                source.zotero_item_key = source.external_id
                source.raw_metadata_json = {
                    "pilot_corpus_id": pilot_manifest.corpus_id,
                    "pilot_lane_id": source_spec.lane_id,
                }
                sources.append(source)
                for document_spec in source_spec.documents:
                    source_documents.append(
                        _build_pilot_source_document_record(
                            manifest=pilot_manifest,
                            corpus_dir=corpus_dir,
                            source_id=source.source_id,
                            lane_id=source_spec.lane_id,
                            spec=document_spec,
                        )
                    )
            source_ids = [source.source_id for source in sources]
        else:
            demo_manifest, corpus_dir = load_demo_corpus_manifest(dataset.corpus_id)
            source = demo_manifest.source.model_copy(deep=True)
            source.external_source = "demo"
            source.external_id = dataset.corpus_id
            source.zotero_item_key = f"demo-{dataset.corpus_id}"
            source.raw_metadata_json = {"demo_corpus_id": dataset.corpus_id}
            sources = [source]
            source_documents = [
                _build_source_document_record(
                    demo_manifest,
                    corpus_dir,
                    source.source_id,
                    document_spec,
                )
                for document_spec in demo_manifest.documents
            ]
            source_ids = [source.source_id]

        source_store.save_sources(sources)
        source_document_store.save_source_documents(source_documents)
        NormalizationService(
            source_document_store=source_document_store,
            text_unit_store=text_unit_store,
            source_store=source_store,
        ).normalize_documents(source_ids=source_ids)
        text_units = text_unit_store.list_text_units()
        if not text_units:
            raise ValueError(
                f"Evaluation dataset '{dataset.evaluation_id}' "
                "did not yield any normalized text units."
            )
        return _PreparedCorpus(
            sources=sources,
            text_units=text_units,
            dataset_path=_dataset_root() / f"{dataset.evaluation_id}.json",
        )


def _available_eval_paths(dataset: ExtractionEvalDataset) -> list[_EvalPathSpec]:
    paths = [
        _EvalPathSpec(
            path="heuristic",
            kind="extraction",
            notes="Default local extraction adapter.",
        )
    ]
    graph_rag_runtime = GraphRAGExtractionAdapter.runtime_probe()
    if graph_rag_runtime.ready and _graph_rag_live_requested():
        paths.append(
            _EvalPathSpec(
                path="graphrag_live",
                kind="extraction",
                notes=graph_rag_runtime.detail,
            )
        )
    elif dataset.graphrag_fixture_claims:
        paths.append(
            _EvalPathSpec(
                path="graphrag_mapping_fixture",
                kind="mapping_fixture",
                notes=(
                    "Fixture-backed GraphRAG mapping check. This isolates the GraphRAG-to-"
                    "Sourcebound mapping layer and is not a live GraphRAG extraction run."
                ),
            )
        )
    return paths


def _graph_rag_live_requested() -> bool:
    api_key = (os.getenv("GRAPHRAG_API_KEY") or "").strip()
    if not api_key or api_key.startswith("<"):
        return False
    return True


def _run_extraction_path(
    *,
    dataset: ExtractionEvalDataset,
    prepared: _PreparedCorpus,
    path_spec: _EvalPathSpec,
    run_suffix: str,
) -> ExtractionOutput:
    run = ExtractionRun(
        run_id=f"{dataset.evaluation_id}-{path_spec.path}-{run_suffix}",
        status=ExtractionRunStatus.RUNNING,
    )
    if path_spec.path == "heuristic":
        return HeuristicExtractionAdapter().extract_candidates(
            run=run,
            sources=prepared.sources,
            text_units=prepared.text_units,
        )
    if path_spec.path == "graphrag_live":
        return GraphRAGExtractionAdapter().extract_candidates(
            run=run,
            sources=prepared.sources,
            text_units=prepared.text_units,
        )
    if path_spec.path == "graphrag_mapping_fixture":
        adapter = GraphRAGExtractionAdapter(mode="in_process")
        artifacts = _build_graphrag_fixture_bundle(dataset, prepared)
        output = adapter._map_artifacts_to_output(
            artifacts=artifacts,
            run=run,
            sources=prepared.sources,
            source_text_units=prepared.text_units,
        )
        output.run.notes = (
            f"{output.run.notes or ''} Fixture-backed GraphRAG comparison path.".strip()
        )
        return output
    raise ValueError(f"Unsupported extraction path '{path_spec.path}'.")


def _build_graphrag_fixture_bundle(
    dataset: ExtractionEvalDataset,
    prepared: _PreparedCorpus,
) -> GraphRAGArtifactBundle:
    text_unit_by_locator = {text_unit.locator: text_unit for text_unit in prepared.text_units}
    graph_text_units: list[dict[str, Any]] = []
    documents: list[dict[str, Any]] = []
    graph_text_unit_id_by_text_unit_id: dict[str, str] = {}

    for index, text_unit in enumerate(prepared.text_units, start=1):
        document_id = f"fixture-doc-{index}"
        graph_text_unit_id = f"fixture-text-unit-{index}"
        graph_text_unit_id_by_text_unit_id[text_unit.text_unit_id] = graph_text_unit_id
        graph_text_units.append({"id": graph_text_unit_id, "document_id": document_id})
        documents.append(
            {
                "id": document_id,
                "metadata": {
                    "text_unit_id": text_unit.text_unit_id,
                    "source_id": text_unit.source_id,
                    "locator": text_unit.locator,
                },
            }
        )

    covariates: list[dict[str, Any]] = []
    for fixture_claim in dataset.graphrag_fixture_claims:
        fixture_text_unit = text_unit_by_locator.get(fixture_claim.text_unit_locator)
        if fixture_text_unit is None:
            raise ValueError(
                "GraphRAG fixture claim "
                f"{fixture_claim.claim_id!r} referenced unknown locator "
                f"{fixture_claim.text_unit_locator!r}."
            )
        covariates.append(
            {
                "covariate_type": "claim",
                "type": fixture_claim.type,
                "description": fixture_claim.description,
                "subject_id": fixture_claim.subject_id,
                "object_id": fixture_claim.object_id,
                "status": fixture_claim.status,
                "start_date": fixture_claim.start_date,
                "end_date": fixture_claim.end_date,
                "source_text": fixture_claim.source_text,
                "text_unit_id": graph_text_unit_id_by_text_unit_id[fixture_text_unit.text_unit_id],
            }
        )

    return GraphRAGArtifactBundle(
        covariates=covariates,
        text_units=graph_text_units,
        documents=documents,
        output_dir=prepared.dataset_path.parent,
    )


def _score_path_result(
    *,
    dataset: ExtractionEvalDataset,
    path_result: _PathRunResult,
) -> dict[str, Any]:
    output = path_result.output
    evidence_by_id = {item.evidence_id: item for item in output.evidence}
    eligible_matches = _eligible_matches(dataset.gold_claims, output.candidates, evidence_by_id)
    assigned_by_candidate, assigned_by_gold = _assign_matches(eligible_matches)

    matched_candidates = len(assigned_by_candidate)
    matched_gold_claims = len(assigned_by_gold)
    unmatched_gold_claims = [
        gold.gold_claim_id
        for gold in dataset.gold_claims
        if gold.gold_claim_id not in assigned_by_gold
    ]
    exact_duplicate_count = _duplicate_count(output.candidates)
    semantic_duplicate_count = _semantic_duplicate_count(eligible_matches)
    reviewer_edit_burden = _reviewer_edit_burden(
        dataset.gold_claims,
        output.candidates,
        evidence_by_id,
        assigned_by_candidate,
        eligible_matches,
    )
    evidence_quality = _evidence_quality(
        dataset.gold_claims,
        output.candidates,
        evidence_by_id,
        assigned_by_candidate,
    )
    review_ready_candidate_count = sum(
        1 for item in reviewer_edit_burden["per_candidate"].values() if item["action_count"] == 0
    )
    contradiction = _contradiction_summary(dataset, set(assigned_by_gold))
    stability = _stability_summary(path_result.repeated_outputs)
    gold_claim_by_id = {gold.gold_claim_id: gold for gold in dataset.gold_claims}

    assigned_examples: list[dict[str, Any]] = []
    for candidate in output.candidates:
        assigned = assigned_by_candidate.get(candidate.candidate_id)
        if assigned is None:
            continue
        gold = gold_claim_by_id[assigned.gold_claim_id]
        assigned_examples.append(
            {
                "candidate_id": candidate.candidate_id,
                "gold_claim_id": gold.gold_claim_id,
                "gold_label": gold.label,
                "candidate_text": _claim_text(candidate),
                "secondary_gold_hits": sorted(
                    match.gold_claim_id
                    for match in eligible_matches
                    if match.candidate_id == candidate.candidate_id
                    and match.gold_claim_id != assigned.gold_claim_id
                ),
                "edit_actions": reviewer_edit_burden["per_candidate"].get(
                    candidate.candidate_id, {}
                ),
                "best_evidence_focus": evidence_quality["per_candidate"].get(
                    candidate.candidate_id, {}
                ),
            }
        )

    return {
        "path": path_result.path,
        "path_kind": path_result.kind,
        "path_notes": path_result.notes,
        "candidate_count": len(output.candidates),
        "evidence_count": len(output.evidence),
        "metrics": {
            "claim_precision": _safe_ratio(review_ready_candidate_count, len(output.candidates)),
            "factual_support_precision": _safe_ratio(matched_candidates, len(output.candidates)),
            "important_fact_recall": _safe_ratio(matched_gold_claims, len(dataset.gold_claims)),
            "review_ready_recall": _safe_ratio(
                review_ready_candidate_count, len(dataset.gold_claims)
            ),
            "exact_duplicate_rate": _safe_ratio(exact_duplicate_count, len(output.candidates)),
            "semantic_duplicate_rate": _safe_ratio(
                semantic_duplicate_count, len(output.candidates)
            ),
        },
        "evidence_span_quality": evidence_quality["summary"],
        "reviewer_edit_burden": reviewer_edit_burden["summary"],
        "contradiction_handling": contradiction,
        "stability": stability,
        "matched_gold_claim_ids": sorted(assigned_by_gold),
        "unmatched_gold_claims": [
            {
                "gold_claim_id": gold_claim_id,
                "label": gold_claim_by_id[gold_claim_id].label,
                "importance": gold_claim_by_id[gold_claim_id].importance,
            }
            for gold_claim_id in unmatched_gold_claims
        ],
        "failure_modes": _rank_failure_modes(
            output.candidates,
            assigned_by_candidate,
            eligible_matches,
            reviewer_edit_burden["per_candidate"],
            unmatched_gold_claims,
        ),
        "assigned_examples": assigned_examples,
    }


def _eligible_matches(
    gold_claims: list[GoldClaimSpec],
    candidates: list[CandidateClaim],
    evidence_by_id: dict[str, EvidenceSnippet],
) -> list[_MatchRecord]:
    matches: list[_MatchRecord] = []
    for gold in gold_claims:
        subject_patterns = _patterns_or_target(gold.subject_patterns, gold.target_subject)
        predicate_patterns = _patterns_or_target(gold.predicate_patterns, gold.target_predicate)
        value_patterns = _patterns_or_target(gold.value_patterns, gold.target_value)
        evidence_patterns = _patterns_or_target(gold.evidence_patterns, gold.target_value)

        for candidate in candidates:
            subject_match = _matches_patterns(candidate.subject, subject_patterns)
            predicate_match = _matches_predicate(candidate.predicate, predicate_patterns)
            value_match = _matches_patterns(candidate.value, value_patterns)
            evidence_match, evidence_id, anchor_text = _match_evidence(
                [evidence_by_id[item] for item in candidate.evidence_ids if item in evidence_by_id],
                evidence_patterns,
            )
            score = (
                (2 if subject_match else 0)
                + (1 if predicate_match else 0)
                + (4 if value_match else 0)
                + (3 if evidence_match else 0)
            )
            if score < 7:
                continue
            if not ((value_match and evidence_match) or (value_match and subject_match)):
                continue
            matches.append(
                _MatchRecord(
                    gold_claim_id=gold.gold_claim_id,
                    candidate_id=candidate.candidate_id,
                    score=score,
                    subject_match=subject_match,
                    predicate_match=predicate_match,
                    value_match=value_match,
                    evidence_match=evidence_match,
                    evidence_id=evidence_id,
                    anchor_text=anchor_text,
                )
            )
    return sorted(
        matches,
        key=lambda item: (
            -item.score,
            item.gold_claim_id,
            item.candidate_id,
        ),
    )


def _assign_matches(
    eligible_matches: list[_MatchRecord],
) -> tuple[dict[str, _MatchRecord], dict[str, _MatchRecord]]:
    assigned_by_candidate: dict[str, _MatchRecord] = {}
    assigned_by_gold: dict[str, _MatchRecord] = {}
    for match in eligible_matches:
        if match.candidate_id in assigned_by_candidate:
            continue
        if match.gold_claim_id in assigned_by_gold:
            continue
        assigned_by_candidate[match.candidate_id] = match
        assigned_by_gold[match.gold_claim_id] = match
    return assigned_by_candidate, assigned_by_gold


def _reviewer_edit_burden(
    gold_claims: list[GoldClaimSpec],
    candidates: list[CandidateClaim],
    evidence_by_id: dict[str, EvidenceSnippet],
    assigned_by_candidate: dict[str, _MatchRecord],
    eligible_matches: list[_MatchRecord],
) -> dict[str, Any]:
    gold_claim_by_id = {gold.gold_claim_id: gold for gold in gold_claims}
    candidate_by_id = {candidate.candidate_id: candidate for candidate in candidates}
    eligible_by_candidate: dict[str, list[_MatchRecord]] = {}
    for match in eligible_matches:
        eligible_by_candidate.setdefault(match.candidate_id, []).append(match)

    per_candidate: dict[str, dict[str, Any]] = {}
    action_totals: dict[str, int] = {
        "subject_rewrites": 0,
        "predicate_rewrites": 0,
        "value_rewrites": 0,
        "needs_split": 0,
        "total_actions": 0,
    }

    for candidate_id, assigned in assigned_by_candidate.items():
        candidate = candidate_by_id[candidate_id]
        gold = gold_claim_by_id[assigned.gold_claim_id]
        subject_rewrite = _normalize(candidate.subject) != _normalize(gold.target_subject)
        predicate_rewrite = _normalize(candidate.predicate) != _normalize(gold.target_predicate)
        value_rewrite = _normalize(candidate.value) != _normalize(gold.target_value)
        secondary_hits = [
            match.gold_claim_id
            for match in eligible_by_candidate.get(candidate_id, [])
            if match.gold_claim_id != assigned.gold_claim_id
        ]
        actions: dict[str, Any] = {
            "subject_rewrite": subject_rewrite,
            "predicate_rewrite": predicate_rewrite,
            "value_rewrite": value_rewrite,
            "needs_split": bool(secondary_hits),
            "secondary_gold_hits": sorted(secondary_hits),
            "action_count": int(subject_rewrite)
            + int(predicate_rewrite)
            + int(value_rewrite)
            + int(bool(secondary_hits)),
        }
        per_candidate[candidate_id] = actions
        action_totals["subject_rewrites"] += int(subject_rewrite)
        action_totals["predicate_rewrites"] += int(predicate_rewrite)
        action_totals["value_rewrites"] += int(value_rewrite)
        action_totals["needs_split"] += int(bool(secondary_hits))
        action_totals["total_actions"] += actions["action_count"]

    matched_count = len(assigned_by_candidate)
    return {
        "summary": {
            **action_totals,
            "avg_actions_per_matched_candidate": _safe_ratio(
                action_totals["total_actions"],
                matched_count,
            ),
        },
        "per_candidate": per_candidate,
    }


def _evidence_quality(
    gold_claims: list[GoldClaimSpec],
    candidates: list[CandidateClaim],
    evidence_by_id: dict[str, EvidenceSnippet],
    assigned_by_candidate: dict[str, _MatchRecord],
) -> dict[str, Any]:
    gold_claim_by_id = {gold.gold_claim_id: gold for gold in gold_claims}
    focus_scores: list[float] = []
    exact_anchor_hits = 0
    span_backed_hits = 0
    per_candidate: dict[str, dict[str, Any]] = {}

    for candidate_id, assigned in assigned_by_candidate.items():
        gold = gold_claim_by_id[assigned.gold_claim_id]
        evidence = (
            evidence_by_id.get(assigned.evidence_id) if assigned.evidence_id is not None else None
        )
        anchor = assigned.anchor_text or (
            gold.evidence_patterns[0] if gold.evidence_patterns else ""
        )
        evidence_text = evidence.text if evidence is not None else ""
        anchor_length = max(len(_normalize(anchor)), 1)
        evidence_length = max(len(_normalize(evidence_text)), 1)
        focus = min(1.0, anchor_length / evidence_length)
        focus_scores.append(focus)
        if evidence is not None and _normalize(anchor) == _normalize(evidence_text):
            exact_anchor_hits += 1
        if (
            evidence is not None
            and evidence.span_start is not None
            and evidence.span_end is not None
        ):
            span_backed_hits += 1
        per_candidate[candidate_id] = {
            "gold_claim_id": gold.gold_claim_id,
            "anchor_text": anchor,
            "evidence_id": evidence.evidence_id if evidence is not None else None,
            "evidence_excerpt": evidence_text,
            "anchor_focus": round(focus, 4),
            "span_backed": evidence is not None
            and evidence.span_start is not None
            and evidence.span_end is not None,
        }

    matched_count = len(assigned_by_candidate)
    return {
        "summary": {
            "avg_anchor_focus": round(_mean(focus_scores), 4),
            "exact_anchor_hit_rate": _safe_ratio(exact_anchor_hits, matched_count),
            "span_backed_rate": _safe_ratio(span_backed_hits, matched_count),
        },
        "per_candidate": per_candidate,
    }


def _contradiction_summary(
    dataset: ExtractionEvalDataset,
    matched_gold_claim_ids: set[str],
) -> dict[str, Any]:
    if not dataset.contradiction_groups:
        return {
            "opportunities": 0,
            "handled_groups": 0,
            "status": "not_exercised",
        }

    handled_groups = 0
    for group in dataset.contradiction_groups:
        if len(set(group.gold_claim_ids) & matched_gold_claim_ids) >= 2:
            handled_groups += 1
    return {
        "opportunities": len(dataset.contradiction_groups),
        "handled_groups": handled_groups,
        "status": "exercised",
    }


def _stability_summary(repeated_outputs: list[ExtractionOutput]) -> dict[str, Any]:
    if not repeated_outputs:
        return {"repeat_count": 0, "exact_match_runs": 0, "exact_match_rate": 0.0}
    baseline = _output_signature(repeated_outputs[0])
    exact_match_runs = sum(
        1 for output in repeated_outputs if _output_signature(output) == baseline
    )
    return {
        "repeat_count": len(repeated_outputs),
        "exact_match_runs": exact_match_runs,
        "exact_match_rate": _safe_ratio(exact_match_runs, len(repeated_outputs)),
    }


def _build_comparisons(path_reports: list[dict[str, Any]]) -> list[dict[str, Any]]:
    path_by_name = {item["path"]: item for item in path_reports}
    comparisons: list[dict[str, Any]] = []

    heuristic = path_by_name.get("heuristic")
    graphrag_live = path_by_name.get("graphrag_live")
    if heuristic is not None and graphrag_live is not None:
        heuristic_metrics = heuristic["metrics"]
        graphrag_metrics = graphrag_live["metrics"]
        comparisons.append(
            {
                "comparison_kind": "extraction",
                "paths": ["heuristic", "graphrag_live"],
                "claim_precision_delta": round(
                    graphrag_metrics["claim_precision"] - heuristic_metrics["claim_precision"],
                    4,
                ),
                "important_fact_recall_delta": round(
                    graphrag_metrics["important_fact_recall"]
                    - heuristic_metrics["important_fact_recall"],
                    4,
                ),
                "anchor_focus_delta": round(
                    graphrag_live["evidence_span_quality"]["avg_anchor_focus"]
                    - heuristic["evidence_span_quality"]["avg_anchor_focus"],
                    4,
                ),
                "reviewer_action_delta": round(
                    heuristic["reviewer_edit_burden"]["avg_actions_per_matched_candidate"]
                    - graphrag_live["reviewer_edit_burden"]["avg_actions_per_matched_candidate"],
                    4,
                ),
            }
        )

    graphrag_mapping_fixture = path_by_name.get("graphrag_mapping_fixture")
    if heuristic is not None and graphrag_mapping_fixture is not None:
        heuristic_metrics = heuristic["metrics"]
        fixture_metrics = graphrag_mapping_fixture["metrics"]
        comparisons.append(
            {
                "comparison_kind": "mapping_fixture",
                "paths": ["heuristic", "graphrag_mapping_fixture"],
                "claim_precision_delta": round(
                    fixture_metrics["claim_precision"] - heuristic_metrics["claim_precision"],
                    4,
                ),
                "important_fact_recall_delta": round(
                    fixture_metrics["important_fact_recall"]
                    - heuristic_metrics["important_fact_recall"],
                    4,
                ),
                "anchor_focus_delta": round(
                    graphrag_mapping_fixture["evidence_span_quality"]["avg_anchor_focus"]
                    - heuristic["evidence_span_quality"]["avg_anchor_focus"],
                    4,
                ),
                "reviewer_action_delta": round(
                    heuristic["reviewer_edit_burden"]["avg_actions_per_matched_candidate"]
                    - graphrag_mapping_fixture["reviewer_edit_burden"][
                        "avg_actions_per_matched_candidate"
                    ],
                    4,
                ),
                "notes": (
                    "Fixture mapping comparison only. This is not a live GraphRAG extraction delta."
                ),
            }
        )
    return comparisons


def _build_suite_rows(dataset_summaries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for dataset_summary in dataset_summaries:
        evaluation_id = cast(str, dataset_summary["evaluation_id"])
        title = cast(str, dataset_summary["title"])
        corpus_id = cast(str, dataset_summary["corpus_id"])
        for path_report in cast(list[dict[str, Any]], dataset_summary["paths"]):
            rows.append(
                {
                    "evaluation_id": evaluation_id,
                    "title": title,
                    "corpus_id": corpus_id,
                    "path": path_report["path"],
                    "path_kind": path_report["path_kind"],
                    "claim_precision": path_report["metrics"]["claim_precision"],
                    "important_fact_recall": path_report["metrics"]["important_fact_recall"],
                    "avg_anchor_focus": path_report["evidence_span_quality"][
                        "avg_anchor_focus"
                    ],
                    "avg_reviewer_actions": path_report["reviewer_edit_burden"][
                        "avg_actions_per_matched_candidate"
                    ],
                    "stability": path_report["stability"]["exact_match_rate"],
                }
            )
    rows.sort(key=lambda item: (item["evaluation_id"], item["path"]))
    return rows


def _rank_failure_modes(
    candidates: list[CandidateClaim],
    assigned_by_candidate: dict[str, _MatchRecord],
    eligible_matches: list[_MatchRecord],
    burden_by_candidate: dict[str, dict[str, Any]],
    unmatched_gold_claims: list[str],
) -> list[dict[str, Any]]:
    eligible_by_candidate: dict[str, list[_MatchRecord]] = {}
    for match in eligible_matches:
        eligible_by_candidate.setdefault(match.candidate_id, []).append(match)

    compound_candidates = []
    generic_predicates = []
    subject_rewrites = []
    for candidate in candidates:
        if candidate.candidate_id not in assigned_by_candidate:
            continue
        burden = burden_by_candidate.get(candidate.candidate_id, {})
        if burden.get("needs_split"):
            compound_candidates.append(candidate.candidate_id)
        if candidate.predicate == "described_as":
            generic_predicates.append(candidate.candidate_id)
        if burden.get("subject_rewrite"):
            subject_rewrites.append(candidate.candidate_id)

    raw_failure_modes: list[dict[str, Any]] = [
        {
            "failure_mode": "missed_expected_claims",
            "count": len(unmatched_gold_claims),
            "evidence": unmatched_gold_claims[:3],
            "priority": 4,
        },
        {
            "failure_mode": "compound_candidates_need_split",
            "count": len(compound_candidates),
            "evidence": compound_candidates[:3],
            "priority": 4,
        },
        {
            "failure_mode": "generic_described_as_predicates",
            "count": len(generic_predicates),
            "evidence": generic_predicates[:3],
            "priority": 2,
        },
        {
            "failure_mode": "subjects_need_rewrite",
            "count": len(subject_rewrites),
            "evidence": subject_rewrites[:3],
            "priority": 2,
        },
    ]
    ranked = [item for item in raw_failure_modes if item["count"] > 0]
    ranked.sort(key=lambda item: (-item["priority"], -item["count"], item["failure_mode"]))
    return ranked[:3]


def _duplicate_count(candidates: list[CandidateClaim]) -> int:
    seen: set[tuple[str, str, str]] = set()
    duplicates = 0
    for candidate in candidates:
        signature = (
            _normalize(candidate.subject),
            candidate.predicate,
            _normalize(candidate.value),
        )
        if signature in seen:
            duplicates += 1
            continue
        seen.add(signature)
    return duplicates


def _semantic_duplicate_count(eligible_matches: list[_MatchRecord]) -> int:
    candidate_ids_by_gold: dict[str, set[str]] = {}
    for match in eligible_matches:
        candidate_ids_by_gold.setdefault(match.gold_claim_id, set()).add(match.candidate_id)
    return sum(max(0, len(candidate_ids) - 1) for candidate_ids in candidate_ids_by_gold.values())


def _output_signature(output: ExtractionOutput) -> str:
    evidence_by_id = {item.evidence_id: item for item in output.evidence}
    candidate_rows = []
    for candidate in output.candidates:
        evidence_rows = []
        for evidence_id in candidate.evidence_ids:
            evidence = evidence_by_id.get(evidence_id)
            if evidence is None:
                continue
            evidence_rows.append(
                (
                    evidence.source_id,
                    evidence.locator,
                    evidence.text_unit_id,
                    evidence.span_start,
                    evidence.span_end,
                    _normalize(evidence.text),
                )
            )
        candidate_rows.append(
            (
                _normalize(candidate.subject),
                candidate.predicate,
                _normalize(candidate.value),
                tuple(sorted(evidence_rows)),
            )
        )
    payload = json.dumps(sorted(candidate_rows), sort_keys=True)
    return sha1(payload.encode("utf-8")).hexdigest()


def _claim_text(candidate: CandidateClaim) -> str:
    return f"{candidate.subject} {candidate.predicate} {candidate.value}".strip()


def _patterns_or_target(patterns: list[str], target: str) -> list[str]:
    return patterns or [target]


def _match_evidence(
    evidence_items: list[EvidenceSnippet],
    patterns: list[str],
) -> tuple[bool, str | None, str | None]:
    best_match: tuple[int, str | None, str | None] = (-1, None, None)
    for evidence in evidence_items:
        evidence_text = _normalize(evidence.text)
        for pattern in patterns:
            normalized_pattern = _normalize(pattern)
            if normalized_pattern and normalized_pattern in evidence_text:
                if len(normalized_pattern) > best_match[0]:
                    best_match = (len(normalized_pattern), evidence.evidence_id, pattern)
    return best_match[0] >= 0, best_match[1], best_match[2]


def _matches_patterns(text: str, patterns: list[str]) -> bool:
    normalized = _normalize(text)
    return any(_normalize(pattern) in normalized for pattern in patterns if _normalize(pattern))


def _matches_predicate(predicate: str, patterns: list[str]) -> bool:
    normalized_predicate = _normalize(predicate)
    return any(_normalize(pattern) == normalized_predicate for pattern in patterns if pattern)


def _normalize(value: str | None) -> str:
    lowered = (value or "").lower()
    collapsed = _NORMALIZE_RE.sub(" ", lowered).strip()
    return " ".join(collapsed.split())


def _safe_ratio(numerator: int | float, denominator: int | float) -> float:
    if not denominator:
        return 0.0
    return round(float(numerator) / float(denominator), 4)


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _build_markdown_report(summary: dict[str, Any]) -> str:
    lines = [
        f"# {summary['title']}",
        "",
        f"- Evaluation ID: `{summary['evaluation_id']}`",
        f"- Corpus: `{summary['corpus_id']}`",
        f"- Repeat count: `{summary['repeat']}`",
        "",
        "## Path Summary",
        "",
        "| Path | Kind | Claims | Evidence | Precision | "
        "Factual Support Precision | Recall | Avg Anchor Focus | "
        "Avg Reviewer Actions | Stability |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for path_report in summary["paths"]:
        lines.append(
            "| "
            f"{path_report['path']} | "
            f"{path_report['path_kind']} | "
            f"{path_report['candidate_count']} | "
            f"{path_report['evidence_count']} | "
            f"{path_report['metrics']['claim_precision']:.4f} | "
            f"{path_report['metrics']['factual_support_precision']:.4f} | "
            f"{path_report['metrics']['important_fact_recall']:.4f} | "
            f"{path_report['evidence_span_quality']['avg_anchor_focus']:.4f} | "
            f"{path_report['reviewer_edit_burden']['avg_actions_per_matched_candidate']:.4f} | "
            f"{path_report['stability']['exact_match_rate']:.4f} |"
        )
        lines.append("")
        lines.append(f"- {path_report['path']}: {path_report['path_notes']}")

    comparisons = summary.get("comparisons") or []
    if comparisons:
        lines.extend(["", "## Comparisons", ""])
        for comparison in comparisons:
            paths = " vs ".join(comparison["paths"])
            lines.append(
                f"- `{comparison['comparison_kind']}` `{paths}`: "
                f"claim_precision_delta={comparison['claim_precision_delta']:.4f}, "
                f"important_fact_recall_delta={comparison['important_fact_recall_delta']:.4f}, "
                f"anchor_focus_delta={comparison['anchor_focus_delta']:.4f}, "
                f"reviewer_action_delta={comparison['reviewer_action_delta']:.4f}"
            )
            if comparison.get("notes"):
                lines.append(f"- Note: {comparison['notes']}")

    lines.append("")
    lines.append("## Failure Modes")
    lines.append("")
    for path_report in summary["paths"]:
        lines.append(f"### {path_report['path']}")
        if not path_report["failure_modes"]:
            lines.append("- No ranked failure modes.")
            continue
        for failure_mode in path_report["failure_modes"]:
            evidence = ", ".join(failure_mode["evidence"]) if failure_mode["evidence"] else "none"
            lines.append(
                f"- `{failure_mode['failure_mode']}`: {failure_mode['count']} "
                f"(evidence: {evidence})"
            )
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def _build_suite_markdown_report(summary: dict[str, Any]) -> str:
    lines = [
        "# Extraction Evaluation Suite",
        "",
        f"- Dataset count: `{summary['dataset_count']}`",
        f"- Repeat count: `{summary['repeat']}`",
        "",
        "## Dataset Catalog",
        "",
    ]
    for dataset in summary["datasets"]:
        lines.append(
            f"- `{dataset['evaluation_id']}`: {dataset['title']} "
            f"(corpus=`{dataset['corpus_id']}`, paths=`{dataset['path_count']}`)"
        )

    lines.extend(
        [
            "",
            "## Comparison Grid",
            "",
            "| Dataset | Path | Kind | Claim Precision | Important Fact Recall | "
            "Avg Anchor Focus | Avg Reviewer Actions | Stability |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in summary["rows"]:
        lines.append(
            "| "
            f"{row['evaluation_id']} | "
            f"{row['path']} | "
            f"{row['path_kind']} | "
            f"{row['claim_precision']:.4f} | "
            f"{row['important_fact_recall']:.4f} | "
            f"{row['avg_anchor_focus']:.4f} | "
            f"{row['avg_reviewer_actions']:.4f} | "
            f"{row['stability']:.4f} |"
        )
    lines.extend(
        [
            "",
            "Use this grid as the lightweight over-time comparison artifact. "
            "The per-dataset `summary.json` and `report.md` files remain the detailed drill-down source.",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"
