from __future__ import annotations

from fastapi.testclient import TestClient

from source_aware_worldbuilding.api.dependencies import get_review_service
from source_aware_worldbuilding.api.main import app
from source_aware_worldbuilding.cli import seed_dev_data
from source_aware_worldbuilding.domain.errors import WikibaseSyncError


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
        "/v1/claims/{claim_id}/relationships",
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
        assert "text_unit_id" in run_body["evidence"][0]
        assert "span_start" in run_body["evidence"][0]
        assert "span_end" in run_body["evidence"][0]

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
        assert approve_response.json()["status"] == "approved"

        source_detail = client.get("/v1/sources/src-1")
        assert source_detail.status_code == 200
        assert source_detail.json()["source"]["source_id"] == "src-1"
        assert source_detail.json()["text_units"]

        candidate_detail = client.get(f"/v1/candidates/{first_candidate_id}")
        assert candidate_detail.status_code == 200
        assert candidate_detail.json()["review_state"] == "approved"

        claims_response = client.get("/v1/claims")
        assert claims_response.status_code == 200
        assert len(claims_response.json()) == 1

        relationships_before = client.get(
            f"/v1/claims/{claims_response.json()[0]['claim_id']}/relationships"
        )
        assert relationships_before.status_code == 200
        assert relationships_before.json() == []

        query_response = client.post(
            "/v1/query",
            json={"question": "Rouen bread prices", "mode": "strict_facts"},
        )
        assert query_response.status_code == 200
        assert query_response.json()["supporting_claims"]


def test_review_route_surfaces_wikibase_sync_failures(temp_data_dir) -> None:
    seed_dev_data()

    class FailingReviewService:
        def review_candidate(self, candidate_id: str, request):
            _ = candidate_id, request
            raise WikibaseSyncError("Wikibase sync failed: upstream unavailable")

    app.dependency_overrides[get_review_service] = lambda: FailingReviewService()

    with TestClient(app) as client:
        response = client.post(
            "/v1/candidates/cand-1/review",
            json={"decision": "approve"},
        )

    assert response.status_code == 502
    assert "Wikibase sync failed" in response.json()["detail"]
