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
    evaluate_extraction_suite,
    load_extraction_eval_dataset,
)

runner = CliRunner()


def test_extraction_eval_dataset_is_discoverable() -> None:
    datasets = available_extraction_eval_datasets()

    assert "wheatley-london-bread" in datasets
    assert "wheatley-london-bread-core-rules" in datasets
    assert "wheatley-london-bread-compound-clauses" in datasets
    assert "harbor-watch-proof-loop" in datasets
    assert "harbor-watch-proof-loop-comparison" in datasets
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


def test_extraction_eval_suite_writes_comparison_artifacts(tmp_path: Path) -> None:
    report = evaluate_extraction_suite(
        output_root=tmp_path,
        dataset_ids=[
            "wheatley-london-bread-core-rules",
            "wheatley-london-bread-compound-clauses",
            "harbor-watch-proof-loop-comparison",
        ],
        repeat=1,
    )

    summary_path = tmp_path / "suite-summary.json"
    report_path = tmp_path / "suite-report.md"
    assert summary_path.exists()
    assert report_path.exists()

    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert payload["dataset_count"] == 3
    dataset_ids = [item["evaluation_id"] for item in payload["datasets"]]
    assert dataset_ids == sorted(dataset_ids)
    assert dataset_ids == [
        "harbor-watch-proof-loop-comparison",
        "wheatley-london-bread-compound-clauses",
        "wheatley-london-bread-core-rules",
    ]
    assert [item["artifact_dir"] for item in payload["datasets"]] == dataset_ids

    row_keys = [(item["evaluation_id"], item["path"]) for item in payload["rows"]]
    assert row_keys == sorted(row_keys)
    assert {
        (item["evaluation_id"], item["path"])
        for item in payload["rows"]
    } >= {
        ("wheatley-london-bread-core-rules", "heuristic"),
        ("wheatley-london-bread-compound-clauses", "heuristic"),
        ("harbor-watch-proof-loop-comparison", "heuristic"),
    }
    assert report["dataset_count"] == 3
    assert "Comparison Grid" in report_path.read_text(encoding="utf-8")


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


def test_cli_evaluate_extraction_suite_emits_json(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "source_aware_worldbuilding.cli.evaluate_extraction_suite",
        lambda output_root, repeat=3: {
            "dataset_count": 2,
            "rows": [
                {
                    "evaluation_id": "wheatley-london-bread-core-rules",
                    "path": "heuristic",
                    "path_kind": "extraction",
                    "claim_precision": 0.5,
                    "important_fact_recall": 0.66,
                    "avg_anchor_focus": 0.4,
                    "avg_reviewer_actions": 1.5,
                    "stability": 1.0,
                }
            ],
        },
    )

    result = runner.invoke(
        cli_app,
        [
            "evaluate-extraction",
            "--dataset",
            "all",
            "--output-root",
            str(tmp_path),
            "--json-output",
        ],
    )

    assert result.exit_code == 0
    assert '"dataset_count": 2' in result.stdout
    assert '"evaluation_id": "wheatley-london-bread-core-rules"' in result.stdout


def test_extraction_eval_compound_clause_scenario_highlights_reviewability_gap(
    tmp_path: Path,
) -> None:
    report = evaluate_extraction_dataset(
        "wheatley-london-bread-compound-clauses",
        output_root=tmp_path,
        repeat=1,
    )

    summary_path = tmp_path / "summary.json"
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert {item["path"] for item in payload["paths"]} == {
        "heuristic",
        "graphrag_mapping_fixture",
    }

    heuristic = next(item for item in payload["paths"] if item["path"] == "heuristic")
    fixture = next(item for item in payload["paths"] if item["path"] == "graphrag_mapping_fixture")

    assert set(heuristic["matched_gold_claim_ids"]) == {
        "gold-hostellers-forbidden",
        "gold-tourte-bakers-white",
    }
    assert heuristic["metrics"]["claim_precision"] == 0.0
    assert heuristic["metrics"]["important_fact_recall"] == 1.0
    assert heuristic["metrics"]["review_ready_recall"] == 0.0
    assert heuristic["reviewer_edit_burden"]["avg_actions_per_matched_candidate"] == 3.0
    assert fixture["metrics"]["claim_precision"] == 1.0
    assert fixture["metrics"]["important_fact_recall"] == 1.0
    assert report["comparisons"][0]["comparison_kind"] == "mapping_fixture"
    assert report["comparisons"][0]["claim_precision_delta"] == 1.0
    assert report["comparisons"][0]["important_fact_recall_delta"] == 0.0
    assert report["comparisons"][0]["anchor_focus_delta"] > 0.5
    assert report["comparisons"][0]["reviewer_action_delta"] == 3.0


def test_extraction_eval_harbor_comparison_scenario_includes_mapping_fixture(
    tmp_path: Path,
) -> None:
    report = evaluate_extraction_dataset(
        "harbor-watch-proof-loop-comparison",
        output_root=tmp_path,
        repeat=1,
    )

    assert {item["path"] for item in report["paths"]} == {
        "heuristic",
        "graphrag_mapping_fixture",
    }
    assert report["comparisons"][0]["comparison_kind"] == "mapping_fixture"
    assert report["comparisons"][0]["claim_precision_delta"] == 0.0
    assert report["comparisons"][0]["important_fact_recall_delta"] == 0.0


def test_cli_evaluate_extraction_prints_comparison_summary(monkeypatch, tmp_path: Path) -> None:
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
            "comparisons": [
                {
                    "comparison_kind": "mapping_fixture",
                    "paths": ["heuristic", "graphrag_mapping_fixture"],
                    "claim_precision_delta": 0.25,
                    "important_fact_recall_delta": 0.5,
                    "anchor_focus_delta": 0.6,
                    "reviewer_action_delta": 2.0,
                    "notes": "Fixture mapping comparison only.",
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
        ],
    )

    assert result.exit_code == 0
    assert "Comparisons:" in result.stdout
    assert "heuristic vs graphrag_mapping_fixture" in result.stdout
    assert "claim_precision_delta=0.2500" in result.stdout


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
