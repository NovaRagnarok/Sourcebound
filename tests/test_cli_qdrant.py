from __future__ import annotations

import json

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


def test_cli_upgrade_check_reports_postgres_schema_version(monkeypatch) -> None:
    monkeypatch.setattr(settings, "app_state_backend", "postgres")
    monkeypatch.setattr(settings, "app_truth_backend", "postgres")

    monkeypatch.setattr(
        "source_aware_worldbuilding.cli.PostgresAppStateStore.inspect_schema_compatibility",
        lambda dsn, schema: {
            "schema": schema,
            "schema_version": 1,
            "expected_schema_version": 1,
            "compatible": True,
            "detail": "Schema compatibility matches the current supported upgrade path.",
        },
    )

    result = runner.invoke(cli_app, ["upgrade-check", "--json-output"])

    assert result.exit_code == 0
    body = json.loads(result.stdout)
    assert body["postgres_enabled"] is True
    assert body["schema_version"] == 1
    assert body["expected_schema_version"] == 1
    assert body["compatible"] is True
    assert body["projection_rebuild_command"] == ".venv/bin/saw qdrant-rebuild --json-output"


def test_cli_seed_dev_data_initializes_and_backfills_qdrant(temp_data_dir, monkeypatch) -> None:
    monkeypatch.setattr(settings, "qdrant_enabled", True)
    monkeypatch.setattr(settings, "research_semantic_enabled", True)

    init_calls: list[bool] = []
    rebuild_calls: list[bool] = []

    def fake_init() -> dict[str, object]:
        init_calls.append(True)
        return {
            "qdrant_enabled": True,
            "projection_collection": "approved_claims",
            "projection_created": True,
            "research_semantic_enabled": True,
            "research_collection": "research_findings",
            "research_created": True,
        }

    def fake_rebuild() -> dict[str, object]:
        rebuild_calls.append(True)
        return {
            "qdrant_enabled": True,
            "projection_collection": "approved_claims",
            "projection_created": False,
            "claim_count": 9,
            "evidence_count": 10,
        }

    monkeypatch.setattr("source_aware_worldbuilding.cli._initialize_qdrant_runtime", fake_init)
    monkeypatch.setattr("source_aware_worldbuilding.cli._rebuild_qdrant_projection", fake_rebuild)

    result = runner.invoke(cli_app, ["seed-dev-data"])

    assert result.exit_code == 0
    assert init_calls == [True]
    assert rebuild_calls == [True]
    assert "Qdrant projection ready" in result.stdout


def test_cli_seed_dev_data_fails_fast_when_postgres_is_unavailable(monkeypatch) -> None:
    monkeypatch.setattr(settings, "app_state_backend", "postgres")
    monkeypatch.setattr(settings, "app_truth_backend", "postgres")
    monkeypatch.setattr(settings, "app_postgres_dsn", "postgresql://saw:saw@localhost:5432/saw")

    def fake_connect(*args, **kwargs):
        raise RuntimeError("connection refused")

    monkeypatch.setattr("source_aware_worldbuilding.cli.connect", fake_connect)

    result = runner.invoke(cli_app, ["seed-dev-data"])

    assert result.exit_code == 1
    assert "docker compose up -d postgres" in result.output
    assert "APP_POSTGRES_DSN" in result.output


def test_cli_seed_dev_data_fails_fast_when_qdrant_is_unavailable(
    temp_data_dir,
    monkeypatch,
) -> None:
    monkeypatch.setattr(settings, "qdrant_enabled", True)
    monkeypatch.setattr(settings, "research_semantic_enabled", True)

    def fake_init() -> dict[str, object]:
        raise RuntimeError("Failed to initialize Qdrant collection 'approved_claims': boom")

    monkeypatch.setattr("source_aware_worldbuilding.cli._initialize_qdrant_runtime", fake_init)

    result = runner.invoke(cli_app, ["seed-dev-data"])

    assert result.exit_code == 1
    assert "docker compose up -d qdrant" in result.output
    assert "non-default local mode" in result.output
