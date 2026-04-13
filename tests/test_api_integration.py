from __future__ import annotations

from fastapi.testclient import TestClient

from source_aware_worldbuilding.api.dependencies import (
    get_intake_service,
    get_lore_packet_service,
    get_review_service,
)
from source_aware_worldbuilding.api.main import app
from source_aware_worldbuilding.cli import seed_dev_data
from source_aware_worldbuilding.domain.errors import (
    CanonUnavailableError,
    WikibaseSyncError,
    ZoteroWriteError,
)
from source_aware_worldbuilding.domain.models import (
    ExtractionRun,
    IntakeResult,
    ApprovedClaim,
    LorePacketRequest,
    LorePacketResponse,
    SourceDocumentRecord,
    EvidenceSnippet,
    SourceRecord,
    ZoteroCreatedItem,
)
from source_aware_worldbuilding.domain.enums import ClaimKind, ClaimStatus
from source_aware_worldbuilding.storage.json_store import JsonListStore


def populate_lore_packet_fixtures(data_dir) -> None:
    JsonListStore(data_dir / "sources.json").write_models(
        [
            SourceRecord(source_id="src-1", title="Town Register", source_type="record"),
            SourceRecord(source_id="src-2", title="Family Letters", source_type="letter"),
            SourceRecord(source_id="src-3", title="War Chronicle", source_type="chronicle"),
            SourceRecord(source_id="src-4", title="Tavern Rumors", source_type="oral_history"),
        ]
    )
    JsonListStore(data_dir / "evidence.json").write_models(
        [
            EvidenceSnippet(
                evidence_id="evi-1",
                source_id="src-1",
                locator="folio 2r",
                text="Alys served as harbor master.",
            ),
            EvidenceSnippet(
                evidence_id="evi-2",
                source_id="src-2",
                locator="letter 3",
                text="Beren remained loyal to Alys.",
            ),
            EvidenceSnippet(
                evidence_id="evi-3",
                source_id="src-3",
                locator="chapter 1",
                text="The Ember War began in 1201.",
            ),
            EvidenceSnippet(
                evidence_id="evi-4",
                source_id="src-4",
                locator="entry 9",
                text="Patrons whispered that the moon well sings.",
            ),
        ]
    )
    JsonListStore(data_dir / "claims.json").write_models(
        [
            ApprovedClaim(
                claim_id="claim-1",
                subject="Alys",
                predicate="serves_as",
                value="harbor master",
                claim_kind=ClaimKind.PERSON,
                status=ClaimStatus.VERIFIED,
                place="Greyport",
                evidence_ids=["evi-1"],
            ),
            ApprovedClaim(
                claim_id="claim-2",
                subject="Beren",
                predicate="is_allied_with",
                value="Alys",
                claim_kind=ClaimKind.RELATIONSHIP,
                status=ClaimStatus.PROBABLE,
                place="Greyport",
                evidence_ids=["evi-2"],
            ),
            ApprovedClaim(
                claim_id="claim-3",
                subject="Ember War",
                predicate="begins",
                value="regional conflict",
                claim_kind=ClaimKind.EVENT,
                status=ClaimStatus.VERIFIED,
                time_start="1201",
                evidence_ids=["evi-3"],
            ),
            ApprovedClaim(
                claim_id="claim-4",
                subject="Moon well",
                predicate="sings_to",
                value="travelers at midnight",
                claim_kind=ClaimKind.OBJECT,
                status=ClaimStatus.RUMOR,
                place="Greyport",
                evidence_ids=["evi-4"],
            ),
            ApprovedClaim(
                claim_id="claim-5",
                subject="Greyport docks",
                predicate="should_be_depicted_as",
                value="crowded and wind-cut",
                claim_kind=ClaimKind.PLACE,
                status=ClaimStatus.AUTHOR_CHOICE,
            ),
        ]
    )


def test_openapi_includes_operator_mvp_routes() -> None:
    paths = set(app.openapi()["paths"])
    assert {
        "/health",
        "/v1/ingest/zotero/pull",
        "/v1/ingest/normalize-documents",
        "/v1/ingest/extract-candidates",
        "/v1/intake/text",
        "/v1/intake/url",
        "/v1/intake/file",
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
        "/v1/exports/lore-packet",
        "/v1/research/runs",
        "/v1/research/runs/{run_id}",
        "/v1/research/runs/{run_id}/stage",
        "/v1/research/runs/{run_id}/extract",
        "/v1/research/programs",
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
        assert "related_claims" in query_response.json()
        assert "claim_clusters" in query_response.json()
        assert "answer_sections" in query_response.json()
        assert isinstance(query_response.json()["claim_clusters"], list)
        assert isinstance(query_response.json()["answer_sections"], list)


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


def test_lore_packet_export_route_returns_markdown_packet(temp_data_dir) -> None:
    populate_lore_packet_fixtures(temp_data_dir)

    with TestClient(app) as client:
        response = client.post(
            "/v1/exports/lore-packet",
            json={"project_name": "greyport", "files": ["basic-lore.md", "timeline.md"]},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["project_name"] == "greyport"
    assert [item["filename"] for item in body["files"]] == ["basic-lore.md", "timeline.md"]
    assert body["metadata"]["claim_count"] == 5
    assert "Basic Lore" in body["files"][0]["content"]
    assert "Timeline" in body["files"][1]["content"]


def test_lore_packet_route_surfaces_canon_unavailable_errors(temp_data_dir) -> None:
    class FailingLorePacketService:
        def export(self, request: LorePacketRequest) -> LorePacketResponse:
            _ = request
            raise CanonUnavailableError("canon unavailable")

    app.dependency_overrides[get_lore_packet_service] = lambda: FailingLorePacketService()

    with TestClient(app) as client:
        response = client.post("/v1/exports/lore-packet", json={"project_name": "greyport"})

    assert response.status_code == 503
    assert "canon unavailable" in response.json()["detail"]


def test_research_routes_accept_nested_execution_policy_and_curated_inputs(temp_data_dir) -> None:
    with TestClient(app) as client:
        response = client.post(
            "/v1/research/runs",
            json={
                "brief": {
                    "topic": "2003 DJ scene",
                    "focal_year": "2003",
                    "adapter_id": "curated_inputs",
                    "execution_policy": {
                        "total_fetch_time_seconds": 30,
                        "per_host_fetch_cap": 2,
                        "retry_attempts": 2,
                        "retry_backoff_base_ms": 100,
                        "retry_backoff_max_ms": 500,
                        "respect_robots": True,
                        "allow_domains": [],
                        "deny_domains": [],
                    },
                    "curated_inputs": [
                            {
                                "input_type": "text",
                                "title": "Flyer archive note",
                                "text": "Promoters in the local DJ scene described weekly residencies, vinyl crates, and local venue habits.",
                                "source_type": "archive",
                                "published_at": "2003-08-01",
                            }
                    ],
                }
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["run"]["status"] == "completed"
    assert body["run"]["brief"]["adapter_id"] == "curated_inputs"
    assert body["run"]["telemetry"]["total_queries"] == 0
    assert body["findings"]
    assert body["findings"][0]["provenance"]["fetch_outcome"] == "curated_text"
    assert "normalized_title" in body["findings"][0]["provenance"]["scoring"]
    assert "semantic_novelty_score" in body["findings"][0]["provenance"]["scoring"]
    assert body["facet_coverage"]
    assert "diagnostic_summary" in body["facet_coverage"][0]
    assert "semantic" in body["run"]["telemetry"]


def test_intake_routes_return_shared_result_shapes(temp_data_dir) -> None:
    created_item = ZoteroCreatedItem(
        zotero_item_key="ITEM-1",
        title="Test source",
        item_type="document",
    )
    result = IntakeResult(
        created_item=created_item,
        pulled_sources=[SourceRecord(source_id="zotero-ITEM-1", title="Test source")],
        source_documents=[
            SourceDocumentRecord(
                document_id="zdoc-ITEM-1-note",
                source_id="zotero-ITEM-1",
                document_kind="note",
                ingest_status="imported",
                raw_text_status="ready",
                claim_extraction_status="queued",
                locator="note",
                raw_text="hello world",
            )
        ],
    )

    class FakeIntakeService:
        def intake_text(self, payload):
            assert payload.title == "Test source"
            return result

        def intake_url(self, payload):
            assert payload.url == "https://example.test"
            return result

        def intake_file(self, **kwargs):
            assert kwargs["filename"] == "test.txt"
            return result.model_copy(update={"warnings": ["metadata only"]})

    app.dependency_overrides[get_intake_service] = lambda: FakeIntakeService()

    with TestClient(app) as client:
        text_response = client.post(
            "/v1/intake/text",
            json={"title": "Test source", "text": "hello world"},
        )
        assert text_response.status_code == 200
        assert text_response.json()["created_item"]["zotero_item_key"] == "ITEM-1"
        assert text_response.json()["source_documents"][0]["document_kind"] == "note"

        url_response = client.post(
            "/v1/intake/url",
            json={"url": "https://example.test"},
        )
        assert url_response.status_code == 200
        assert url_response.json()["source_documents"][0]["raw_text_status"] == "ready"

        file_response = client.post(
            "/v1/intake/file",
            data={"title": "Uploaded"},
            files={"file": ("test.txt", b"hello world", "text/plain")},
        )
        assert file_response.status_code == 200
        assert file_response.json()["warnings"] == ["metadata only"]


def test_intake_route_surfaces_zotero_write_failures(temp_data_dir) -> None:
    class FailingIntakeService:
        def intake_text(self, payload):
            _ = payload
            raise ZoteroWriteError("Zotero parent item creation failed")

    app.dependency_overrides[get_intake_service] = lambda: FailingIntakeService()

    with TestClient(app) as client:
        response = client.post(
            "/v1/intake/text",
            json={"title": "Bad source", "text": "body"},
        )

    assert response.status_code == 502
    assert "Zotero" in response.json()["detail"]
