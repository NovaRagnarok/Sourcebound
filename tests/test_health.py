import socket

import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from source_aware_worldbuilding.adapters.qdrant_adapter import QdrantProjectionAdapter
from source_aware_worldbuilding.api.main import app
from source_aware_worldbuilding.cli import app as cli_app
from source_aware_worldbuilding.settings import settings

runner = CliRunner()


def test_health() -> None:
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_runtime_health_reports_local_mvp_mode(monkeypatch) -> None:
    monkeypatch.setattr(settings, "app_state_backend", "file")
    monkeypatch.setattr(settings, "app_truth_backend", "file")
    monkeypatch.setattr(settings, "graph_rag_enabled", False)
    monkeypatch.setattr(settings, "qdrant_enabled", False)
    monkeypatch.setattr(settings, "zotero_library_id", None)
    monkeypatch.setattr(settings, "wikibase_api_url", None)
    monkeypatch.setattr(settings, "wikibase_username", None)
    monkeypatch.setattr(settings, "wikibase_password", None)
    monkeypatch.setattr(settings, "wikibase_property_map", None)

    client = TestClient(app)
    response = client.get("/health/runtime")

    assert response.status_code == 200
    body = response.json()
    assert body["overall_status"] == "ready"
    assert body["state_backend"] == "file"
    assert body["truth_backend"] == "file"
    assert body["extraction_backend"] == "heuristic"
    assert any(
        service["name"] == "corpus" and service["mode"] == "stub" for service in body["services"]
    )
    assert any(
        service["name"] == "projection" and service["mode"] == "disabled"
        for service in body["services"]
    )
    assert any(
        service["name"] == "research_semantics" and service["mode"] == "disabled"
        for service in body["services"]
    )
    assert any(
        service["name"] == "bible_export" and service["mode"] == "job_backed"
        for service in body["services"]
    )
    assert any("APP_STATE_BACKEND=postgres" in step for step in body["next_steps"])
    assert any("APP_TRUTH_BACKEND=postgres" in step for step in body["next_steps"])
    assert any("Zotero" in step for step in body["next_steps"])
    assert any("Qdrant projection" in step for step in body["next_steps"])
    assert not any("WIKIBASE_API_URL" in step for step in body["next_steps"])


def test_cli_status_json_reports_runtime_state(monkeypatch) -> None:
    monkeypatch.setattr(settings, "app_state_backend", "file")
    monkeypatch.setattr(settings, "app_truth_backend", "file")
    monkeypatch.setattr(settings, "graph_rag_enabled", False)
    monkeypatch.setattr(settings, "qdrant_enabled", False)
    monkeypatch.setattr(settings, "zotero_library_id", None)
    monkeypatch.setattr(settings, "wikibase_api_url", None)
    monkeypatch.setattr(settings, "wikibase_username", None)
    monkeypatch.setattr(settings, "wikibase_password", None)
    monkeypatch.setattr(settings, "wikibase_property_map", None)

    result = runner.invoke(cli_app, ["status", "--json-output"])

    assert result.exit_code == 0
    assert '"overall_status": "ready"' in result.stdout
    assert '"extraction_backend": "heuristic"' in result.stdout
    assert '"truth_backend": "file"' in result.stdout


def test_cli_verify_default_stack_fails_for_non_default_local_mode(monkeypatch) -> None:
    monkeypatch.setattr(settings, "app_state_backend", "file")
    monkeypatch.setattr(settings, "app_truth_backend", "file")
    monkeypatch.setattr(settings, "graph_rag_enabled", False)
    monkeypatch.setattr(settings, "qdrant_enabled", False)
    monkeypatch.setattr(settings, "research_semantic_enabled", False)
    monkeypatch.setattr(settings, "zotero_library_id", None)
    monkeypatch.setattr(settings, "app_job_worker_enabled", True)

    result = runner.invoke(cli_app, ["verify-default-stack"])

    assert result.exit_code == 1
    assert "APP_STATE_BACKEND=postgres" in result.output
    assert "APP_TRUTH_BACKEND=postgres" in result.output
    assert "QDRANT_ENABLED=true" in result.output


def test_cli_verify_default_stack_succeeds_when_default_stack_is_ready(monkeypatch) -> None:
    monkeypatch.setattr(settings, "app_state_backend", "postgres")
    monkeypatch.setattr(settings, "app_truth_backend", "postgres")
    monkeypatch.setattr(settings, "graph_rag_enabled", False)
    monkeypatch.setattr(settings, "qdrant_enabled", True)
    monkeypatch.setattr(settings, "research_semantic_enabled", False)
    monkeypatch.setattr(settings, "zotero_library_id", None)
    monkeypatch.setattr(settings, "app_job_worker_enabled", True)
    monkeypatch.setattr(
        "source_aware_worldbuilding.services.status._probe_postgres",
        lambda dsn: (True, "Postgres connection succeeded."),
    )
    monkeypatch.setattr(
        QdrantProjectionAdapter,
        "runtime_probe",
        lambda self: (
            "qdrant",
            True,
            True,
            "Qdrant projection is ready.",
        ),
    )

    result = runner.invoke(cli_app, ["verify-default-stack"])

    assert result.exit_code == 0
    assert "verification passed" in result.output.lower()
    assert "recommended Postgres + Qdrant local path" in result.output


def test_runtime_health_reports_graphrag_when_enabled(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(settings, "app_truth_backend", "file")
    monkeypatch.setattr(settings, "graph_rag_enabled", True)
    monkeypatch.setattr(settings, "graph_rag_mode", "in_process")
    monkeypatch.setattr(settings, "graph_rag_root", tmp_path / "graphrag")
    monkeypatch.setattr(settings, "qdrant_enabled", False)
    monkeypatch.setattr(settings, "zotero_library_id", None)
    monkeypatch.setattr(settings, "wikibase_api_url", None)
    monkeypatch.setattr(settings, "wikibase_username", None)
    monkeypatch.setattr(settings, "wikibase_password", None)
    monkeypatch.setattr(settings, "wikibase_property_map", None)

    client = TestClient(app)
    response = client.get("/health/runtime")

    assert response.status_code == 200
    body = response.json()
    assert body["extraction_backend"] == "graphrag"
    extraction_service = next(
        service for service in body["services"] if service["name"] == "extraction"
    )
    assert extraction_service["mode"] == "graphrag:in_process"
    assert extraction_service["ready"] is False
    assert "graphrag" in extraction_service["detail"].lower()


def test_runtime_health_reports_postgres_truth_backend(monkeypatch) -> None:
    monkeypatch.setattr(settings, "app_state_backend", "postgres")
    monkeypatch.setattr(settings, "app_truth_backend", "postgres")
    monkeypatch.setattr(settings, "graph_rag_enabled", False)
    monkeypatch.setattr(settings, "qdrant_enabled", False)
    monkeypatch.setattr(settings, "zotero_library_id", None)

    client = TestClient(app)
    response = client.get("/health/runtime")

    assert response.status_code == 200
    body = response.json()
    truth_store = next(service for service in body["services"] if service["name"] == "truth_store")
    assert body["truth_backend"] == "postgres"
    assert truth_store["mode"] == "postgres"
    assert truth_store["configured"] is True


def test_runtime_health_marks_qdrant_uninitialized_when_collection_is_missing(monkeypatch) -> None:
    monkeypatch.setattr(settings, "app_state_backend", "file")
    monkeypatch.setattr(settings, "app_truth_backend", "file")
    monkeypatch.setattr(settings, "graph_rag_enabled", False)
    monkeypatch.setattr(settings, "qdrant_enabled", True)
    monkeypatch.setattr(settings, "zotero_library_id", None)
    monkeypatch.setattr(
        QdrantProjectionAdapter,
        "runtime_probe",
        lambda self: (
            "qdrant:uninitialized",
            True,
            False,
            "Qdrant is reachable, but collection 'approved_claims' is not "
            "initialized; query and composition fall back to memory ranking.",
        ),
    )

    client = TestClient(app)
    response = client.get("/health/runtime")

    assert response.status_code == 200
    body = response.json()
    projection = next(service for service in body["services"] if service["name"] == "projection")
    assert projection["mode"] == "qdrant:uninitialized"
    assert projection["ready"] is False
    assert body["overall_status"] == "degraded"
    assert any("saw seed-dev-data" in step.lower() for step in body["next_steps"])


def test_runtime_health_reports_default_stack_as_degraded_before_seed(monkeypatch) -> None:
    monkeypatch.setattr(settings, "app_state_backend", "postgres")
    monkeypatch.setattr(settings, "app_truth_backend", "postgres")
    monkeypatch.setattr(settings, "graph_rag_enabled", False)
    monkeypatch.setattr(settings, "qdrant_enabled", True)
    monkeypatch.setattr(settings, "research_semantic_enabled", False)
    monkeypatch.setattr(
        "source_aware_worldbuilding.services.status._probe_postgres",
        lambda dsn: (True, "Postgres connection succeeded."),
    )
    monkeypatch.setattr(
        QdrantProjectionAdapter,
        "runtime_probe",
        lambda self: (
            "qdrant:uninitialized",
            True,
            False,
            "Qdrant is reachable, but collection 'approved_claims' is not initialized.",
        ),
    )

    client = TestClient(app)
    response = client.get("/health/runtime")

    assert response.status_code == 200
    body = response.json()
    assert body["overall_status"] == "degraded"
    assert body["state_backend"] == "postgres"
    assert body["truth_backend"] == "postgres"
    projection = next(service for service in body["services"] if service["name"] == "projection")
    assert projection["mode"] == "qdrant:uninitialized"
    assert any("seed-dev-data" in step for step in body["next_steps"])


def test_runtime_health_reports_needs_setup_when_worker_is_disabled(monkeypatch) -> None:
    monkeypatch.setattr(settings, "app_state_backend", "file")
    monkeypatch.setattr(settings, "app_truth_backend", "file")
    monkeypatch.setattr(settings, "graph_rag_enabled", False)
    monkeypatch.setattr(settings, "qdrant_enabled", False)
    monkeypatch.setattr(settings, "zotero_library_id", None)
    monkeypatch.setattr(settings, "app_job_worker_enabled", False)

    client = TestClient(app)
    response = client.get("/health/runtime")

    assert response.status_code == 200
    body = response.json()
    assert body["overall_status"] == "needs_setup"
    worker = next(service for service in body["services"] if service["name"] == "job_worker")
    assert worker["ready"] is False
    assert any("required for local startup" in step.lower() for step in body["next_steps"])


def test_strict_startup_checks_fail_fast_when_qdrant_is_uninitialized(monkeypatch) -> None:
    monkeypatch.setattr(settings, "app_strict_startup_checks", True)
    monkeypatch.setattr(settings, "qdrant_enabled", True)
    monkeypatch.setattr(
        QdrantProjectionAdapter,
        "runtime_probe",
        lambda self: (
            "qdrant:uninitialized",
            True,
            False,
            "Qdrant is reachable, but collection 'approved_claims' is not "
            "initialized; query and composition fall back to memory ranking.",
        ),
    )

    with pytest.raises(RuntimeError, match="seed-dev-data"):
        with TestClient(app):
            pass


def test_cli_serve_strict_runtime_checks_fail_fast_when_projection_is_degraded(monkeypatch) -> None:
    monkeypatch.setattr("source_aware_worldbuilding.cli._serve_port_preflight_issue", lambda: None)
    monkeypatch.setattr(settings, "app_strict_startup_checks", False)
    monkeypatch.setattr(settings, "qdrant_enabled", True)
    monkeypatch.setattr(
        QdrantProjectionAdapter,
        "runtime_probe",
        lambda self: (
            "qdrant:uninitialized",
            True,
            False,
            "Qdrant is reachable, but collection 'approved_claims' is not "
            "initialized; query and composition fall back to memory ranking.",
        ),
    )

    result = runner.invoke(cli_app, ["serve", "--strict-runtime-checks"])

    assert result.exit_code == 1
    assert "seed-dev-data" in result.output


def test_cli_zotero_check_reports_missing_configuration(monkeypatch) -> None:
    monkeypatch.setattr(settings, "zotero_library_id", None)
    monkeypatch.setattr(settings, "zotero_collection_key", None)
    monkeypatch.setattr(settings, "zotero_api_key", None)

    result = runner.invoke(cli_app, ["zotero-check", "--json-output"])

    assert result.exit_code == 0
    assert '"configured": false' in result.stdout
    assert '"success": false' in result.stdout
    assert '"blocked_stage": "configuration"' in result.stdout
    assert '"routine_ready": false' in result.stdout
    assert '"verification_command": ".venv/bin/saw zotero-check --json-output"' in result.stdout
    assert '"ZOTERO_LIBRARY_ID"' in result.stdout


def test_runtime_health_reports_missing_postgres_dsn_with_fix(monkeypatch) -> None:
    monkeypatch.setattr(settings, "app_state_backend", "postgres")
    monkeypatch.setattr(settings, "app_truth_backend", "postgres")
    monkeypatch.setattr(settings, "app_postgres_dsn", "")
    monkeypatch.setattr(settings, "graph_rag_enabled", False)
    monkeypatch.setattr(settings, "qdrant_enabled", False)
    monkeypatch.setattr(settings, "zotero_library_id", None)

    client = TestClient(app)
    response = client.get("/health/runtime")

    assert response.status_code == 200
    body = response.json()
    app_state = next(service for service in body["services"] if service["name"] == "app_state")
    truth_store = next(service for service in body["services"] if service["name"] == "truth_store")
    assert "cp .env.example .env" in app_state["detail"]
    assert "APP_POSTGRES_DSN" in truth_store["detail"]
    assert any("docker compose up -d postgres" in step for step in body["next_steps"])


def test_cli_serve_fails_fast_for_enabled_but_unconfigured_wikibase(monkeypatch) -> None:
    monkeypatch.setattr("source_aware_worldbuilding.cli._serve_port_preflight_issue", lambda: None)
    monkeypatch.setattr(settings, "app_truth_backend", "wikibase")
    monkeypatch.setattr(settings, "wikibase_api_url", None)
    monkeypatch.setattr(settings, "wikibase_username", None)
    monkeypatch.setattr(settings, "wikibase_password", None)
    monkeypatch.setattr(settings, "wikibase_property_map", None)

    result = runner.invoke(cli_app, ["serve"])

    assert result.exit_code == 1
    assert "APP_TRUTH_BACKEND=wikibase" in result.output
    assert "WIKIBASE_API_URL" in result.output


def test_cli_serve_fails_fast_for_enabled_but_unready_graphrag(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("source_aware_worldbuilding.cli._serve_port_preflight_issue", lambda: None)
    monkeypatch.setattr(settings, "graph_rag_enabled", True)
    monkeypatch.setattr(settings, "graph_rag_mode", "in_process")
    monkeypatch.setattr(settings, "graph_rag_root", tmp_path / "missing-graphrag")

    result = runner.invoke(cli_app, ["serve"])

    assert result.exit_code == 1
    assert "GRAPH_RAG_ENABLED=true" in result.output
    assert "bootstrap-graphrag" in result.output


def test_cli_serve_fails_fast_when_app_port_is_occupied(monkeypatch) -> None:
    monkeypatch.setattr(settings, "app_state_backend", "file")
    monkeypatch.setattr(settings, "app_truth_backend", "file")
    monkeypatch.setattr(settings, "graph_rag_enabled", False)
    monkeypatch.setattr(settings, "qdrant_enabled", False)
    monkeypatch.setattr(settings, "research_semantic_enabled", False)
    monkeypatch.setattr(settings, "app_job_worker_enabled", True)
    monkeypatch.setattr(settings, "app_host", "127.0.0.1")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        monkeypatch.setattr(settings, "app_port", listener.getsockname()[1])

        result = runner.invoke(cli_app, ["serve"])

    assert result.exit_code == 1
    assert "already in use" in result.output
    assert "APP_PORT=" in result.output


def test_cli_serve_fails_fast_when_postgres_is_unavailable(monkeypatch) -> None:
    monkeypatch.setattr("source_aware_worldbuilding.cli._serve_port_preflight_issue", lambda: None)
    monkeypatch.setattr(settings, "app_state_backend", "postgres")
    monkeypatch.setattr(settings, "app_truth_backend", "postgres")
    monkeypatch.setattr(settings, "graph_rag_enabled", False)
    monkeypatch.setattr(settings, "qdrant_enabled", False)
    monkeypatch.setattr(settings, "research_semantic_enabled", False)
    monkeypatch.setattr(settings, "app_job_worker_enabled", True)
    monkeypatch.setattr(
        "source_aware_worldbuilding.services.status._probe_postgres",
        lambda dsn: (
            False,
            "Postgres connection failed: test. Start it with "
            "`docker compose up -d postgres`.",
        ),
    )

    result = runner.invoke(cli_app, ["serve"])

    assert result.exit_code == 1
    assert "Postgres connection failed" in result.output
    assert "docker compose up -d postgres" in result.output
    assert "verify-default-stack" in result.output
