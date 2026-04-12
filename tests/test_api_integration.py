from __future__ import annotations

from fastapi.testclient import TestClient

from source_aware_worldbuilding.api.main import app
from source_aware_worldbuilding.cli import seed_dev_data


def test_openapi_includes_operator_mvp_routes() -> None:
    paths = set(app.openapi()["paths"])
    assert {
        "/health",
        "/v1/ingest/zotero/pull",
        "/v1/ingest/extract-candidates",
        "/v1/sources",
        "/v1/sources/{source_id}",
        "/v1/extraction-runs",
        "/v1/candidates",
        "/v1/candidates/{candidate_id}",
        "/v1/candidates/{candidate_id}/review",
        "/v1/claims",
        "/v1/claims/{claim_id}",
        "/v1/query",
    } <= paths


def test_operator_flow_routes_share_file_backed_state(temp_data_dir) -> None:
    seed_dev_data()

    with TestClient(app) as client:
        sources_before = client.get("/v1/sources")
        assert sources_before.status_code == 200
        assert len(sources_before.json()) == 2

        pull_response = client.post("/v1/ingest/zotero/pull")
        assert pull_response.status_code == 200
        assert pull_response.json()["count"] >= 1

        run_response = client.post("/v1/ingest/extract-candidates")
        assert run_response.status_code == 200
        run_body = run_response.json()
        assert run_body["count"] >= 1
        assert run_body["run"]["status"] == "completed"
        assert run_body["evidence"]

        runs = client.get("/v1/extraction-runs")
        assert runs.status_code == 200
        assert runs.json()[0]["status"] == "completed"

        candidates_before = client.get("/v1/candidates?review_state=pending")
        assert candidates_before.status_code == 200
        assert all(item["review_state"] == "pending" for item in candidates_before.json())

        first_candidate_id = candidates_before.json()[0]["candidate_id"]
        approve_response = client.post(
            f"/v1/candidates/{first_candidate_id}/review",
            json={"decision": "approve"},
        )
        assert approve_response.status_code == 200
        approved_claim_id = approve_response.json()["claim"]["claim_id"]

        source_detail = client.get("/v1/sources/src-1")
        assert source_detail.status_code == 200
        assert source_detail.json()["source"]["source_id"] == "src-1"
        assert source_detail.json()["text_units"]

        claim_detail = client.get(f"/v1/claims/{approved_claim_id}")
        assert claim_detail.status_code == 200
        assert claim_detail.json()["claim_id"] == approved_claim_id

        query_response = client.post(
            "/v1/query",
            json={"question": "Rouen bread prices", "mode": "strict_facts"},
        )
        assert query_response.status_code == 200
        query_body = query_response.json()
        assert query_body["supporting_claims"]
        assert query_body["evidence"]
        assert query_body["sources"]
        assert "Strict facts mode" in query_body["warnings"][0]
