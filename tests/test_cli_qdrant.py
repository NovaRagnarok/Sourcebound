from __future__ import annotations

from typer.testing import CliRunner

from source_aware_worldbuilding.cli import app as cli_app
from source_aware_worldbuilding.domain.enums import ClaimKind, ClaimStatus
from source_aware_worldbuilding.domain.models import ApprovedClaim, EvidenceSnippet
from source_aware_worldbuilding.settings import settings

runner = CliRunner()


def test_cli_qdrant_init_initializes_projection_and_research_collections(monkeypatch) -> None:
    monkeypatch.setattr(settings, "qdrant_enabled", True)
    monkeypatch.setattr(settings, "research_semantic_enabled", True)
    monkeypatch.setattr(settings, "qdrant_collection", "approved_claims")
    monkeypatch.setattr(settings, "research_qdrant_collection", "research_findings")

    class FakeProjection:
        def initialize_collection(self) -> bool:
            return True

    class FakeResearchSemantic:
        def initialize_collection(self) -> bool:
            return False

    monkeypatch.setattr("source_aware_worldbuilding.cli.get_projection", lambda: FakeProjection())
    monkeypatch.setattr(
        "source_aware_worldbuilding.cli.get_research_semantic",
        lambda: FakeResearchSemantic(),
    )

    result = runner.invoke(cli_app, ["qdrant-init", "--json-output"])

    assert result.exit_code == 0
    assert '"projection_created": true' in result.stdout
    assert '"research_created": false' in result.stdout


def test_cli_qdrant_rebuild_backfills_projection_from_truth_store(monkeypatch) -> None:
    monkeypatch.setattr(settings, "qdrant_enabled", True)
    monkeypatch.setattr(settings, "qdrant_collection", "approved_claims")

    claim = ApprovedClaim(
        claim_id="claim-1",
        subject="Rouen bread prices",
        predicate="rose_during",
        value="winter shortage",
        claim_kind=ClaimKind.PRACTICE,
        status=ClaimStatus.VERIFIED,
        evidence_ids=["evi-1"],
    )
    evidence = [
        EvidenceSnippet(
            evidence_id="evi-1",
            source_id="src-1",
            locator="folio 12r",
            text="Bread prices rose sharply during the winter shortage.",
        )
    ]

    class FakeProjection:
        def __init__(self) -> None:
            self.upserted: tuple[list[ApprovedClaim], list[EvidenceSnippet]] | None = None

        def initialize_collection(self) -> bool:
            return True

        def upsert_claims(
            self, claims: list[ApprovedClaim], snippets: list[EvidenceSnippet]
        ) -> None:
            self.upserted = (claims, snippets)

    class FakeTruthStore:
        def list_claims(self) -> list[ApprovedClaim]:
            return [claim]

    class FakeEvidenceStore:
        def list_evidence(self) -> list[EvidenceSnippet]:
            return evidence

    projection = FakeProjection()
    monkeypatch.setattr("source_aware_worldbuilding.cli.get_projection", lambda: projection)
    monkeypatch.setattr("source_aware_worldbuilding.cli.get_truth_store", lambda: FakeTruthStore())
    monkeypatch.setattr(
        "source_aware_worldbuilding.cli.get_evidence_store",
        lambda: FakeEvidenceStore(),
    )

    result = runner.invoke(cli_app, ["qdrant-rebuild", "--json-output"])

    assert result.exit_code == 0
    assert '"claim_count": 1' in result.stdout
    assert projection.upserted == ([claim], evidence)
