from fastapi.testclient import TestClient
from typer.testing import CliRunner

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

    client = TestClient(app)
    response = client.get("/health/runtime")

    assert response.status_code == 200
    body = response.json()
    assert body["overall_status"] == "needs_setup"
    assert body["state_backend"] == "file"
    assert body["truth_backend"] == "file"
    assert body["extraction_backend"] == "heuristic"
    assert any(
        service["name"] == "corpus" and service["mode"] == "stub"
        for service in body["services"]
    )
    assert any(
        service["name"] == "projection" and service["mode"] == "disabled"
        for service in body["services"]
    )
    assert any("ZOTERO_LIBRARY_ID" in step for step in body["next_steps"])


def test_cli_status_json_reports_runtime_state(monkeypatch) -> None:
    monkeypatch.setattr(settings, "app_state_backend", "file")
    monkeypatch.setattr(settings, "app_truth_backend", "file")
    monkeypatch.setattr(settings, "graph_rag_enabled", False)
    monkeypatch.setattr(settings, "qdrant_enabled", False)
    monkeypatch.setattr(settings, "zotero_library_id", None)

    result = runner.invoke(cli_app, ["status", "--json-output"])

    assert result.exit_code == 0
    assert '"overall_status": "needs_setup"' in result.stdout
    assert '"extraction_backend": "heuristic"' in result.stdout


def test_runtime_health_reports_graphrag_preview_when_enabled(monkeypatch) -> None:
    monkeypatch.setattr(settings, "graph_rag_enabled", True)
    monkeypatch.setattr(settings, "qdrant_enabled", False)
    monkeypatch.setattr(settings, "zotero_library_id", None)

    client = TestClient(app)
    response = client.get("/health/runtime")

    assert response.status_code == 200
    body = response.json()
    assert body["extraction_backend"] == "graphrag_preview"
    extraction_service = next(
        service for service in body["services"] if service["name"] == "extraction"
    )
    assert extraction_service["mode"] == "graphrag_preview"
    assert extraction_service["ready"] is True


def test_cli_zotero_check_reports_missing_configuration(monkeypatch) -> None:
    monkeypatch.setattr(settings, "zotero_library_id", None)
    monkeypatch.setattr(settings, "zotero_collection_key", None)
    monkeypatch.setattr(settings, "zotero_api_key", None)

    result = runner.invoke(cli_app, ["zotero-check", "--json-output"])

    assert result.exit_code == 0
    assert '"configured": false' in result.stdout
    assert '"success": false' in result.stdout
    assert '"ZOTERO_LIBRARY_ID"' in result.stdout
