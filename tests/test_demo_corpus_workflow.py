from __future__ import annotations

from typer.testing import CliRunner

from source_aware_worldbuilding.adapters.file_backed import (
    FileBibleSectionStore,
    FileCandidateStore,
    FileReviewStore,
    FileSourceDocumentStore,
    FileTextUnitStore,
    FileTruthStore,
)
from source_aware_worldbuilding.adapters.heuristic_extraction import HeuristicExtractionAdapter
from source_aware_worldbuilding.cli import app as cli_app
from source_aware_worldbuilding.demo_corpus import (
    DemoCorpusApprovalSpec,
    _find_candidate_for_approval,
    run_demo_corpus,
)
from source_aware_worldbuilding.domain.enums import ClaimKind, ClaimStatus, ExtractionRunStatus, ReviewState
from source_aware_worldbuilding.domain.models import CandidateClaim, ExtractionRun, SourceRecord, TextUnit

runner = CliRunner()


def test_heuristic_extractor_records_text_unit_spans() -> None:
    adapter = HeuristicExtractionAdapter()
    sentence = (
        'These dealers were privileged by law to receive thirteen batches for twelve, '
        'hence the expression "a baker\'s dozen."'
    )
    text = f"Intro. {sentence} Tail."
    output = adapter.extract_candidates(
        run=ExtractionRun(run_id="run-demo", status=ExtractionRunStatus.RUNNING),
        sources=[
            SourceRecord(
                source_id="src-demo",
                title="Bread-market excerpt",
                source_type="record",
                year="1896",
            )
        ],
        text_units=[
            TextUnit(
                text_unit_id="tu-demo",
                source_id="src-demo",
                locator="excerpt",
                text=text,
                ordinal=1,
            )
        ],
    )

    evidence = output.evidence[0]
    assert evidence.text_unit_id == "tu-demo"
    assert evidence.span_start == text.index(sentence)
    assert evidence.span_end == text.index(sentence) + len(sentence)
    assert text[evidence.span_start : evidence.span_end] == sentence


def test_demo_corpus_runs_end_to_end(temp_data_dir) -> None:
    summary = run_demo_corpus("wheatley-london-bread", data_dir=temp_data_dir)

    source_documents = FileSourceDocumentStore(temp_data_dir).list_source_documents()
    assert len(source_documents) == 2
    assert {item.document_kind for item in source_documents} == {"attachment", "note"}
    assert all(item.normalization_status == "completed" for item in source_documents)

    text_units = FileTextUnitStore(temp_data_dir).list_text_units()
    assert len(text_units) == 2
    assert all("source_document_id=" in (item.notes or "") for item in text_units)

    candidates = FileCandidateStore(temp_data_dir).list_candidates()
    assert summary.candidate_count >= 4
    assert len(candidates) == summary.candidate_count
    assert any(item.review_state == ReviewState.PENDING for item in candidates)

    reviews = FileReviewStore(temp_data_dir).list_reviews()
    claims = FileTruthStore(temp_data_dir).list_claims()
    assert len(reviews) == 3
    assert len(claims) == 3
    assert summary.approved_claim_count == 3
    assert summary.review_preview_span_start is not None
    assert summary.review_preview_span_end is not None
    assert summary.review_preview_span_end > summary.review_preview_span_start

    section = FileBibleSectionStore(temp_data_dir).get_section(summary.section_id)
    assert section is not None
    assert section.references.claim_ids
    assert set(section.references.claim_ids) <= {claim.claim_id for claim in claims}
    assert "Sources:" in section.content
    assert "London" in section.content or "bread" in section.content.lower()


def test_demo_corpus_cli_reports_clean_error_for_unknown_corpus() -> None:
    result = runner.invoke(cli_app, ["demo-corpus-run", "does-not-exist"])

    assert result.exit_code == 1
    assert "Unknown demo corpus 'does-not-exist'" in result.stdout
    assert "Traceback" not in result.stdout


def test_demo_corpus_approval_rules_must_match_exactly_one_candidate() -> None:
    candidates = [
        CandidateClaim(
            candidate_id="cand-1",
            subject="London bread regratresses",
            predicate="described_as",
            value="received thirteen batches for every twelve purchased",
            claim_kind=ClaimKind.PRACTICE,
            status_suggestion=ClaimStatus.PROBABLE,
            review_state=ReviewState.PENDING,
        ),
        CandidateClaim(
            candidate_id="cand-2",
            subject="London bread regratresses",
            predicate="described_as",
            value="received thirteen batches for every twelve purchased in some wards",
            claim_kind=ClaimKind.PRACTICE,
            status_suggestion=ClaimStatus.PROBABLE,
            review_state=ReviewState.PENDING,
        ),
    ]

    try:
        _find_candidate_for_approval(
            candidates,
            DemoCorpusApprovalSpec(
                predicate="described_as",
                value_contains="thirteen batches for every twelve",
            ),
        )
    except ValueError as exc:
        assert "matched multiple candidates" in str(exc)
    else:
        raise AssertionError("Expected duplicate approval match to fail cleanly.")
