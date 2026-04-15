from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from source_aware_worldbuilding.cli import app as cli_app
from source_aware_worldbuilding.extraction_eval import (
    ContradictionGroupSpec,
    ExtractionEvalDataset,
    _contradiction_summary,
    available_extraction_eval_datasets,
    evaluate_extraction_dataset,
    load_extraction_eval_dataset,
)

runner = CliRunner()


def test_extraction_eval_dataset_is_discoverable() -> None:
    datasets = available_extraction_eval_datasets()

    assert "wheatley-london-bread" in datasets
    dataset = load_extraction_eval_dataset("wheatley-london-bread")
    assert dataset.corpus_id == "wheatley-london-bread"
    assert len(dataset.gold_claims) == 6
    assert len(dataset.graphrag_fixture_claims) == 6


def test_extraction_eval_writes_reproducible_artifacts(tmp_path: Path) -> None:
    report = evaluate_extraction_dataset(
        "wheatley-london-bread",
        output_root=tmp_path,
        repeat=2,
    )

    summary_path = tmp_path / "summary.json"
    report_path = tmp_path / "report.md"
    assert summary_path.exists()
    assert report_path.exists()

    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert payload["evaluation_id"] == "wheatley-london-bread"
    assert {item["path"] for item in payload["paths"]} == {"heuristic", "graphrag_mapping_fixture"}

    graphrag_fixture = next(
        item for item in payload["paths"] if item["path"] == "graphrag_mapping_fixture"
    )
    heuristic = next(item for item in payload["paths"] if item["path"] == "heuristic")

    assert graphrag_fixture["candidate_count"] == 6
    assert graphrag_fixture["metrics"]["claim_precision"] == 1.0
    assert graphrag_fixture["metrics"]["factual_support_precision"] == 1.0
    assert graphrag_fixture["metrics"]["important_fact_recall"] == 1.0
    assert graphrag_fixture["stability"]["exact_match_rate"] == 1.0
    assert heuristic["stability"]["exact_match_rate"] == 1.0
    assert heuristic["contradiction_handling"]["status"] == "not_exercised"
    assert heuristic["metrics"]["claim_precision"] == 0.0
    assert heuristic["metrics"]["factual_support_precision"] == 1.0
    assert "Path Summary" in report_path.read_text(encoding="utf-8")

    assert report["comparisons"][0]["paths"] == ["heuristic", "graphrag_mapping_fixture"]
    assert report["comparisons"][0]["comparison_kind"] == "mapping_fixture"


def test_cli_evaluate_extraction_emits_json(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "source_aware_worldbuilding.cli.evaluate_extraction_dataset",
        lambda dataset, output_root, repeat=3: {
            "evaluation_id": dataset,
            "paths": [
                {
                    "path": "heuristic",
                    "path_kind": "extraction",
                    "metrics": {
                        "claim_precision": 0.5,
                        "factual_support_precision": 0.75,
                        "important_fact_recall": 0.5,
                    },
                    "evidence_span_quality": {"avg_anchor_focus": 0.4},
                    "reviewer_edit_burden": {"avg_actions_per_matched_candidate": 2.0},
                    "stability": {"exact_match_rate": 1.0},
                }
            ],
        },
    )

    result = runner.invoke(
        cli_app,
        [
            "evaluate-extraction",
            "--dataset",
            "wheatley-london-bread",
            "--output-root",
            str(tmp_path),
            "--json-output",
        ],
    )

    assert result.exit_code == 0
    assert '"evaluation_id": "wheatley-london-bread"' in result.stdout


def test_contradiction_summary_requires_distinct_assigned_gold_claims() -> None:
    dataset = ExtractionEvalDataset(
        evaluation_id="contradiction-test",
        title="Contradiction test",
        corpus_id="wheatley-london-bread",
        contradiction_groups=[
            ContradictionGroupSpec(
                group_id="group-1",
                gold_claim_ids=["gold-a", "gold-b"],
            )
        ],
    )

    summary = _contradiction_summary(dataset, {"gold-a"})
    assert summary["status"] == "exercised"
    assert summary["handled_groups"] == 0

    resolved = _contradiction_summary(dataset, {"gold-a", "gold-b"})
    assert resolved["handled_groups"] == 1


def test_cli_evaluate_extraction_rejects_non_positive_repeat(tmp_path: Path) -> None:
    result = runner.invoke(
        cli_app,
        [
            "evaluate-extraction",
            "--dataset",
            "wheatley-london-bread",
            "--output-root",
            str(tmp_path),
            "--repeat",
            "0",
        ],
    )

    assert result.exit_code != 0
    assert "repeat must be >= 1" in result.output
