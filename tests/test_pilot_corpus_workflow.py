from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from source_aware_worldbuilding.adapters.file_backed import FileBibleSectionStore, FileTruthStore
from source_aware_worldbuilding.cli import _build_zotero_report, app as cli_app
from source_aware_worldbuilding.domain.enums import (
    BibleSectionGenerationStatus,
    BibleSectionType,
)
from source_aware_worldbuilding.extraction_eval import evaluate_extraction_dataset
from source_aware_worldbuilding.pilot_corpus import (
    PilotCorpusManifest,
    PilotCorpusSectionRunSummary,
    PilotCorpusSourceRunSummary,
    _evaluate_pilot_thresholds,
    _run_live_zotero_smoke,
    load_pilot_corpus_manifest,
    run_pilot_corpus,
)
from source_aware_worldbuilding.domain.models import SourceRecord

runner = CliRunner()


def test_pilot_corpus_manifest_loads() -> None:
    manifest, corpus_dir = load_pilot_corpus_manifest("harbor-watch-proof-loop")

    assert isinstance(manifest, PilotCorpusManifest)
    assert manifest.corpus_name == "Harbor watch proof loop pilot"
    assert len(manifest.sources) == 4
    assert (corpus_dir / "watch_note.txt").exists()


def test_pilot_corpus_runs_end_to_end(temp_data_dir: Path) -> None:
    summary = run_pilot_corpus("harbor-watch-proof-loop", data_dir=temp_data_dir)

    assert summary.gate_passed is True
    assert summary.source_count == 4
    assert len(summary.source_summaries) == 4
    happy_path = [item for item in summary.source_summaries if item.proof_role == "happy_path"]
    assert len(happy_path) == 3
    assert all(item.failed_document_count == 0 for item in happy_path)
    assert summary.blind_review_card_count == 0
    assert summary.unresolved_candidate_count == 0
    assert summary.approved_claim_count == 4

    economics = next(
        item
        for item in summary.section_summaries
        if item.section_type == BibleSectionType.ECONOMICS_AND_MATERIAL_CULTURE
    )
    rumor = next(
        item
        for item in summary.section_summaries
        if item.section_type == BibleSectionType.RUMORS_AND_CONTESTED
    )
    assert economics.ready_for_writer is True
    assert economics.generation_status == BibleSectionGenerationStatus.READY
    assert rumor.generation_status in {
        BibleSectionGenerationStatus.READY,
        BibleSectionGenerationStatus.THIN,
    }

    claims = FileTruthStore(temp_data_dir).list_claims()
    claim_ids = {claim.claim_id for claim in claims}
    sections = FileBibleSectionStore(temp_data_dir).list_sections("project-harbor-watch-proof-loop")
    assert len(sections) == 2
    for section in sections:
        assert set(section.references.claim_ids) <= claim_ids


def test_pilot_thresholds_treat_degraded_lane_as_warning_only() -> None:
    manifest, _ = load_pilot_corpus_manifest("harbor-watch-proof-loop")
    failures = _evaluate_pilot_thresholds(
        manifest=manifest,
        source_summaries=[
            PilotCorpusSourceRunSummary(
                lane_id="happy",
                proof_role="happy_path",
                title="Happy lane",
                failed_document_count=0,
            ),
            PilotCorpusSourceRunSummary(
                lane_id="degraded",
                proof_role="degraded",
                title="Degraded lane",
                failed_document_count=1,
                warnings=["Scanned attachment could not be extracted into text."],
                gate_failures=["expected stage_summary[failed]=1, got 0"],
            ),
        ],
        review_cards=[],
        final_candidates=[],
        section_summaries=[
            PilotCorpusSectionRunSummary(
                section_id="sec-1",
                section_type=BibleSectionType.ECONOMICS_AND_MATERIAL_CULTURE,
                title="Economics",
                generation_status=BibleSectionGenerationStatus.READY,
                ready_for_writer=True,
                markdown_path="runtime/pilot/economics.md",
            )
        ],
        extraction_eval=None,
    )

    assert failures == []


def test_live_zotero_smoke_skips_without_configuration(monkeypatch) -> None:
    manifest, _ = load_pilot_corpus_manifest("harbor-watch-proof-loop")
    manifest = manifest.model_copy(
        update={
            "live_zotero": manifest.live_zotero.model_copy(update={"item_keys": ["ITEM-1"]})
        }
    )
    monkeypatch.setattr("source_aware_worldbuilding.settings.settings.zotero_library_id", None)

    summary = _run_live_zotero_smoke(manifest)

    assert summary.status == "skipped"
    assert "not configured" in summary.detail


def test_live_zotero_smoke_fails_when_item_keys_return_no_sources(monkeypatch) -> None:
    manifest, _ = load_pilot_corpus_manifest("harbor-watch-proof-loop")
    manifest = manifest.model_copy(
        update={
            "live_zotero": manifest.live_zotero.model_copy(update={"item_keys": ["ITEM-1"]})
        }
    )

    class EmptyAdapter:
        def pull_sources_by_item_keys(self, item_keys):
            _ = item_keys
            return []

        def discover_source_documents(self, sources, existing_documents=None, force_refresh=False):
            _ = sources, existing_documents, force_refresh
            return []

    monkeypatch.setattr("source_aware_worldbuilding.settings.settings.zotero_library_id", "12345")
    monkeypatch.setattr("source_aware_worldbuilding.pilot_corpus.ZoteroCorpusAdapter", EmptyAdapter)

    summary = _run_live_zotero_smoke(manifest)

    assert summary.status == "failed"
    assert "did not return any sources" in summary.detail


def test_pilot_extraction_eval_meets_thresholds(tmp_path: Path) -> None:
    summary = evaluate_extraction_dataset(
        "harbor-watch-proof-loop",
        output_root=tmp_path / "eval",
    )
    heuristic = next(item for item in summary["paths"] if item["path"] == "heuristic")

    assert heuristic["metrics"]["important_fact_recall"] >= 0.83
    assert heuristic["metrics"]["claim_precision"] >= 0.50
    assert heuristic["reviewer_edit_burden"]["avg_actions_per_matched_candidate"] <= 1.5
    assert heuristic["evidence_span_quality"]["avg_anchor_focus"] >= 0.60


def test_pilot_corpus_cli_succeeds(temp_data_dir: Path) -> None:
    result = runner.invoke(
        cli_app,
        ["pilot-corpus-run", "harbor-watch-proof-loop", "--data-dir", str(temp_data_dir)],
    )

    assert result.exit_code == 0
    assert "Ran pilot corpus harbor-watch-proof-loop" in result.stdout


def test_zotero_check_json_includes_live_smoke_fields() -> None:
    report = _build_zotero_report(source_limit=1, include_text_units=False)

    assert "live_smoke" in report
    assert "stage_breakdown" in report
    assert "document_warnings" in report
