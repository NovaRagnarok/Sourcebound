from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from source_aware_worldbuilding.cli import (
    _benchmark_brief_2003_dj,
    _run_benchmark_2003_dj,
    _run_benchmark_2003_dj_once,
    app as cli_app,
)
from source_aware_worldbuilding.domain.enums import (
    ClaimKind,
    ClaimStatus,
    ExtractionRunStatus,
    ResearchCoverageStatus,
    ResearchFindingDecision,
    ResearchRunStatus,
)
from source_aware_worldbuilding.domain.models import (
    CandidateClaim,
    ExtractionOutput,
    ExtractionRun,
    ResearchExtractResult,
    ResearchFacet,
    ResearchFacetCoverage,
    ResearchFinding,
    ResearchProgram,
    ResearchRun,
    ResearchRunDetail,
    ResearchRunStageResult,
)

runner = CliRunner()


def test_benchmark_brief_uses_fixed_2003_dj_preset() -> None:
    brief = _benchmark_brief_2003_dj()

    assert brief.topic == "Chicago house DJ and club scene"
    assert brief.focal_year == "2003"
    assert brief.time_start == "2002"
    assert brief.time_end == "2004"
    assert brief.desired_facets == [
        "objects_technology",
        "media_culture",
        "regional_context",
        "people",
        "practices",
    ]
    assert brief.max_results_per_query == 10
    assert brief.max_findings == 80


def test_benchmark_runner_writes_complete_report_artifact(tmp_path: Path, monkeypatch) -> None:
    class FakeResearchService:
        def run_research(self, request):
            _ = request
            run = ResearchRun(
                run_id="research-test",
                status=ResearchRunStatus.COMPLETED,
                brief=_benchmark_brief_2003_dj(),
                program_id="default-generic",
                facets=[ResearchFacet(facet_id="people", label="People", query_hint="participants", target_count=1)],
                accepted_count=1,
                rejected_count=0,
            )
            finding = ResearchFinding(
                finding_id="finding-1",
                run_id="research-test",
                facet_id="people",
                query="test",
                url="https://archive.example.org/2003-scene-report",
                canonical_url="https://archive.example.org/2003-scene-report",
                title="Chicago DJ scene report from 2003",
                publisher="Scene Archive",
                published_at="2003-08-10",
                snippet_text="In 2003, DJs rotated between loft parties and club residencies.",
                page_excerpt="In 2003, DJs rotated between loft parties and club residencies.",
                source_type="archive",
                score=0.8,
                relevance_score=0.7,
                quality_score=0.9,
                novelty_score=1.0,
                decision=ResearchFindingDecision.ACCEPTED,
            )
            return ResearchRunDetail(
                run=run,
                findings=[finding],
                program=ResearchProgram(
                    program_id="default-generic",
                    name="Default",
                    markdown="# Default",
                    built_in=True,
                ),
                facet_coverage=[
                    ResearchFacetCoverage(
                        facet_id="people",
                        label="People",
                        target_count=1,
                        accepted_count=1,
                        coverage_status=ResearchCoverageStatus.MET,
                        diagnostic_summary="target_met",
                    )
                ],
            )

        def extract_run(self, run_id: str):
            _ = run_id
            run = ResearchRun(
                run_id="research-test",
                status=ResearchRunStatus.COMPLETED,
                brief=_benchmark_brief_2003_dj(),
                program_id="default-generic",
            )
            extraction_run = ExtractionRun(
                run_id="extract-test",
                status=ExtractionRunStatus.COMPLETED,
                source_count=1,
                text_unit_count=1,
                candidate_count=1,
            )
            candidate = CandidateClaim(
                candidate_id="cand-1",
                subject="Chicago DJ scene",
                predicate="occurred_during",
                value="2003 loft parties and club residencies",
                claim_kind=ClaimKind.EVENT,
                status_suggestion=ClaimStatus.PROBABLE,
            )
            return ResearchExtractResult(
                stage_result=ResearchRunStageResult(
                    run=run,
                    staged_source_ids=["research-source-1"],
                    staged_document_ids=["research-doc-1"],
                ),
                normalization={"document_count": 1, "text_unit_count": 1, "warnings": []},
                extraction=ExtractionOutput(
                    run=extraction_run,
                    candidates=[candidate],
                    evidence=[],
                ),
            )

    monkeypatch.setattr(
        "source_aware_worldbuilding.cli._build_benchmark_research_service",
        lambda state_dir: FakeResearchService(),
    )

    report = _run_benchmark_2003_dj(tmp_path, label="test")
    report_path = Path(report["artifact_dir"]) / "report.json"

    assert report_path.exists()
    payload = json.loads(report_path.read_text())
    assert "run" in payload
    assert "accepted_findings" in payload
    assert "rejected_findings" in payload
    assert "stage_result" in payload
    assert "normalization" in payload
    assert "extraction" in payload
    assert "scorecard" in payload
    assert "manual_review" in payload
    assert "provider_contribution" in payload
    assert "core_or_period_evidenced_count" in payload["scorecard"]["auto_checks"]
    assert "unique_accepted_source_count" in payload["scorecard"]["auto_checks"]


def test_cli_benchmark_command_emits_json(monkeypatch) -> None:
    monkeypatch.setattr(
        "source_aware_worldbuilding.cli._run_benchmark_2003_dj",
        lambda output_root, label=None, repeat=1: {
            "artifact_dir": str(output_root / "artifact"),
            "accepted_findings": [],
            "extraction": {"candidate_count": 0},
            "scorecard": {"auto_pass": False, "auto_checks": {"coverage_all_facets": False, "core_or_period_evidenced_count": 0, "late_retrospective_count": 0, "top_candidate_proxy_reviewable_count": 0}},
        },
    )

    result = runner.invoke(cli_app, ["benchmark-2003-dj", "--json-output"])

    assert result.exit_code == 0
    assert '"artifact_dir"' in result.stdout


def test_benchmark_repeat_writes_summary_artifact(tmp_path: Path, monkeypatch) -> None:
    class FakeResearchService:
        def __init__(self) -> None:
            self.calls = 0

        def run_research(self, request):
            _ = request
            self.calls += 1
            run = ResearchRun(
                run_id=f"research-test-{self.calls}",
                status=ResearchRunStatus.COMPLETED,
                brief=_benchmark_brief_2003_dj(),
                program_id="default-generic",
                facets=[ResearchFacet(facet_id="people", label="People", query_hint="participants", target_count=1)],
                accepted_count=1,
                rejected_count=0,
            )
            finding = ResearchFinding(
                finding_id=f"finding-{self.calls}",
                run_id=run.run_id,
                facet_id="people",
                query="test",
                url="https://archive.example.org/2003-scene-report",
                canonical_url="https://archive.example.org/2003-scene-report",
                title="Chicago DJ scene report from 2003",
                publisher="Scene Archive",
                published_at="2003-08-10",
                snippet_text="In 2003, DJs rotated between loft parties and club residencies.",
                page_excerpt="In 2003, DJs rotated between loft parties and club residencies.",
                source_type="archive",
                score=0.8,
                relevance_score=0.7,
                quality_score=0.9,
                novelty_score=1.0,
                decision=ResearchFindingDecision.ACCEPTED,
            )
            return ResearchRunDetail(
                run=run,
                findings=[finding],
                program=ResearchProgram(program_id="default-generic", name="Default", markdown="# Default", built_in=True),
                facet_coverage=[
                    ResearchFacetCoverage(
                        facet_id="people",
                        label="People",
                        target_count=1,
                        accepted_count=1,
                        coverage_status=ResearchCoverageStatus.MET,
                        diagnostic_summary="target_met",
                    )
                ],
            )

        def extract_run(self, run_id: str):
            _ = run_id
            extraction_run = ExtractionRun(
                run_id=f"extract-{self.calls}",
                status=ExtractionRunStatus.COMPLETED,
                source_count=1,
                text_unit_count=1,
                candidate_count=1,
            )
            candidate = CandidateClaim(
                candidate_id=f"cand-{self.calls}",
                subject="Chicago DJ scene",
                predicate="occurred_during",
                value="2003 loft parties and club residencies",
                claim_kind=ClaimKind.EVENT,
                status_suggestion=ClaimStatus.PROBABLE,
            )
            return ResearchExtractResult(
                stage_result=ResearchRunStageResult(
                    run=ResearchRun(run_id=f"research-test-{self.calls}", status=ResearchRunStatus.COMPLETED, brief=_benchmark_brief_2003_dj(), program_id="default-generic"),
                    staged_source_ids=["research-source-1"],
                    staged_document_ids=["research-doc-1"],
                ),
                normalization={"document_count": 1, "text_unit_count": 1, "warnings": []},
                extraction=ExtractionOutput(run=extraction_run, candidates=[candidate], evidence=[]),
            )

    service = FakeResearchService()
    monkeypatch.setattr(
        "source_aware_worldbuilding.cli._build_benchmark_research_service",
        lambda state_dir: service,
    )

    summary = _run_benchmark_2003_dj(tmp_path, label="repeat", repeat=2)

    assert summary["summary"]["repeat_count"] == 2
    assert len(summary["runs"]) == 2
    assert (Path(summary["artifact_dir"]) / "summary.json").exists()
    assert "core_or_period_evidenced_count" in summary["summary"]
    assert "unique_accepted_source_count" in summary["summary"]
