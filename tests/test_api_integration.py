from __future__ import annotations

import time

from fastapi.testclient import TestClient

from source_aware_worldbuilding.api.dependencies import (
    get_intake_service,
    get_lore_packet_service,
    get_review_service,
)
from source_aware_worldbuilding.api.main import app
from source_aware_worldbuilding.cli import seed_dev_data
from source_aware_worldbuilding.domain.enums import ClaimKind, ClaimStatus
from source_aware_worldbuilding.domain.errors import (
    CanonUnavailableError,
    WikibaseSyncError,
    ZoteroWriteError,
)
from source_aware_worldbuilding.domain.models import (
    ApprovedClaim,
    EvidenceSnippet,
    IntakeResult,
    LorePacketRequest,
    LorePacketResponse,
    SourceDocumentRecord,
    SourceRecord,
    ZoteroCreatedItem,
)
from source_aware_worldbuilding.settings import settings
from source_aware_worldbuilding.storage.json_store import JsonListStore


def wait_for_job(client: TestClient, job_id: str, *, attempts: int = 40) -> dict:
    last_body: dict = {}
    for _ in range(attempts):
        response = client.get(f"/v1/jobs/{job_id}")
        assert response.status_code == 200
        last_body = response.json()
        if last_body.get("status_label") in {
            "completed",
            "failed",
            "cancelled",
            "partial",
        } or last_body["status"] in {"completed", "failed", "cancelled"}:
            return last_body
        time.sleep(0.05)
    return last_body


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
        "/v1/workspace/summary",
        "/v1/bible/profiles",
        "/v1/bible/profiles/{project_id}",
        "/v1/bible/sections",
        "/v1/bible/sections/{section_id}",
        "/v1/bible/sections/{section_id}/regenerate",
        "/v1/bible/sections/{section_id}/provenance",
        "/v1/bible/exports/{project_id}",
        "/v1/jobs/{job_id}/cancel",
        "/v1/jobs/{job_id}/retry",
        "/v1/ingest/zotero/pull",
        "/v1/ingest/normalize-documents",
        "/v1/ingest/extract-candidates",
        "/v1/intake/text",
        "/v1/intake/url",
        "/v1/intake/file",
        "/v1/jobs",
        "/v1/jobs/{job_id}",
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


def test_root_redirects_to_writer_workspace_alias() -> None:
    with TestClient(app) as client:
        response = client.get("/", follow_redirects=False)

    assert response.status_code in {302, 307}
    assert response.headers["location"] == "/workspace/"


def test_runtime_health_route_reports_degraded_when_quality_layers_are_missing(monkeypatch) -> None:
    monkeypatch.setattr(settings, "app_state_backend", "file")
    monkeypatch.setattr(settings, "app_truth_backend", "file")
    monkeypatch.setattr(settings, "graph_rag_enabled", False)
    monkeypatch.setattr(settings, "qdrant_enabled", False)
    monkeypatch.setattr(settings, "zotero_library_id", None)

    with TestClient(app) as client:
        response = client.get("/health/runtime")

    assert response.status_code == 200
    body = response.json()
    assert body["overall_status"] == "degraded"
    assert any(
        service["name"] == "projection" and service["mode"] == "disabled"
        for service in body["services"]
    )
    assert any("recommended for retrieval quality" in step.lower() for step in body["next_steps"])


def test_operator_flow_routes_share_file_backed_state(temp_data_dir) -> None:
    seed_dev_data()

    with TestClient(app) as client:
        workspace_summary = client.get("/v1/workspace/summary")
        assert workspace_summary.status_code == 200
        summary_body = workspace_summary.json()
        assert summary_body["project"]["project_id"] == "project-rouen-winter"
        assert summary_body["next_actions"]
        assert "screen" in summary_body["next_actions"][0]

        sources_before = client.get("/v1/sources")
        assert sources_before.status_code == 200
        assert len(sources_before.json()) == 10
        claims_before = client.get("/v1/claims")
        assert claims_before.status_code == 200
        initial_claim_count = len(claims_before.json())
        assert initial_claim_count == 9

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
        approved_claim_id = approve_response.json()["claim"]["claim_id"]

        source_detail = client.get("/v1/sources/src-price-ledger")
        assert source_detail.status_code == 200
        assert source_detail.json()["source"]["source_id"] == "src-price-ledger"
        assert source_detail.json()["text_units"]

        candidate_detail = client.get(f"/v1/candidates/{first_candidate_id}")
        assert candidate_detail.status_code == 200
        assert candidate_detail.json()["review_state"] == "approved"

        claims_response = client.get("/v1/claims")
        assert claims_response.status_code == 200
        assert len(claims_response.json()) == initial_claim_count + 1

        relationships_before = client.get(f"/v1/claims/{approved_claim_id}/relationships")
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


def test_query_route_keeps_bread_token_question_topic_first(temp_data_dir) -> None:
    seed_dev_data()

    with TestClient(app) as client:
        response = client.post(
            "/v1/query",
            json={
                "question": "How were bread tokens handled in Rouen?",
                "mode": "open_exploration",
            },
        )

    assert response.status_code == 200
    body = response.json()
    returned_ids = [claim["claim_id"] for claim in body["supporting_claims"]]
    assert returned_ids[:2] == ["claim-bread-tokens", "claim-bread-scrip"]
    assert body["metadata"]["answer_boundary"] == "direct_answer"
    assert body["metadata"]["retrieval_quality_tier"] in {"projection", "memory_ranked"}
    assert body["direct_match_claim_ids"][0] == "claim-bread-tokens"
    assert body["answer_sections"]
    assert body["claim_clusters"][0]["lead_claim_id"] == "claim-bread-tokens"


def test_review_route_surfaces_wikibase_sync_failures(temp_data_dir) -> None:
    seed_dev_data()

    class FailingReviewService:
        def review_candidate(self, candidate_id: str, request):
            _ = candidate_id, request
            raise WikibaseSyncError("Wikibase sync failed: upstream unavailable")

    try:
        with TestClient(app) as client:
            claims_before = client.get("/v1/claims")
            assert claims_before.status_code == 200
            claim_count_before = len(claims_before.json())
            candidates_before = client.get("/v1/candidates?review_state=pending")
            assert candidates_before.status_code == 200
            pending_ids_before = {item["candidate_id"] for item in candidates_before.json()}
            app.dependency_overrides[get_review_service] = lambda: FailingReviewService()
            response = client.post(
                "/v1/candidates/cand-grain-bell-beadles/review",
                json={"decision": "approve"},
            )
            app.dependency_overrides.pop(get_review_service, None)
            claims_after = client.get("/v1/claims")
            assert claims_after.status_code == 200
            candidates_after = client.get("/v1/candidates?review_state=pending")
            assert candidates_after.status_code == 200

        assert response.status_code == 502
        assert "Wikibase sync failed" in response.json()["detail"]
        assert len(claims_after.json()) == claim_count_before
        assert pending_ids_before == {item["candidate_id"] for item in candidates_after.json()}
    finally:
        app.dependency_overrides.pop(get_review_service, None)


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
    assert body["metadata"]["claim_count"] == 4
    assert "Basic Lore" in body["files"][0]["content"]
    assert "Timeline" in body["files"][1]["content"]


def test_bible_workspace_routes_support_profile_section_and_export(temp_data_dir) -> None:
    populate_lore_packet_fixtures(temp_data_dir)

    with TestClient(app) as client:
        profile = client.put(
            "/v1/bible/profiles/project-greyport",
            json={
                "project_name": "Greyport Bible",
                "era": "1201",
                "geography": "Greyport",
                "composition_defaults": {"include_statuses": ["verified", "probable"]},
            },
        )
        assert profile.status_code == 200
        assert profile.json()["project_name"] == "Greyport Bible"

        section = client.post(
            "/v1/bible/sections",
            json={
                "project_id": "project-greyport",
                "section_type": "setting_overview",
                "filters": {"place": "Greyport"},
            },
        )
        assert section.status_code == 202
        section_job = wait_for_job(client, section.json()["job_id"])
        assert section_job["status"] == "completed"
        section_id = section_job["result_ref"]["section_id"]
        section_record = client.get(f"/v1/bible/sections/{section_id}")
        assert section_record.status_code == 200
        assert section_record.json()["references"]["claim_ids"]
        assert "composition_metrics" in section_record.json()

        edited = client.put(
            f"/v1/bible/sections/{section_id}",
            json={"title": "Setting Notes", "content": "Manual setting notes"},
        )
        assert edited.status_code == 200
        assert edited.json()["has_manual_edits"] is True

        regenerated = client.post(
            f"/v1/bible/sections/{section_id}/regenerate",
            json={"filters": {"place": "Greyport"}},
        )
        assert regenerated.status_code == 202
        regenerate_job = wait_for_job(client, regenerated.json()["job_id"])
        assert regenerate_job["status"] == "completed"
        refreshed = client.get(f"/v1/bible/sections/{section_id}")
        assert refreshed.status_code == 200
        assert refreshed.json()["content"] == "Manual setting notes"
        assert refreshed.json()["generated_markdown"]
        assert refreshed.json()["composition_metrics"]["target_beats"] >= 1

        provenance = client.get(f"/v1/bible/sections/{section_id}/provenance")
        assert provenance.status_code == 200
        assert provenance.json()["paragraphs"]
        assert provenance.json()["paragraphs"][0]["why_this_paragraph_exists"]
        assert provenance.json()["paragraphs"][0]["claim_details"]
        assert provenance.json()["paragraphs"][0]["evidence_details"]
        assert provenance.json()["paragraphs"][0]["paragraph"]["paragraph_role"] is not None

        exported = client.get("/v1/bible/exports/project-greyport")
        assert exported.status_code == 200
        assert exported.json()["profile"]["project_name"] == "Greyport Bible"
        assert len(exported.json()["sections"]) == 1

        export_job = client.post("/v1/bible/exports/project-greyport")
        assert export_job.status_code == 202
        completed_export = wait_for_job(client, export_job.json()["job_id"])
        assert completed_export["status"] == "completed"
        assert completed_export["result_payload"]["profile"]["project_name"] == "Greyport Bible"


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
                            "text": (
                                "Promoters in the local DJ scene described weekly "
                                "residencies, vinyl crates, and local venue habits."
                            ),
                            "source_type": "archive",
                            "published_at": "2003-08-01",
                        }
                    ],
                }
            },
        )
        assert response.status_code == 202
        job = wait_for_job(client, response.json()["job_id"])
        assert job["status"] == "completed"
        detail = client.get(f"/v1/research/runs/{job['result_ref']['run_id']}")
        assert detail.status_code == 200
        body = detail.json()
        assert body["run"]["status"] in {"completed", "degraded_fallback"}
        assert body["run"]["brief"]["adapter_id"] == "curated_inputs"
        assert body["run"]["telemetry"]["total_queries"] == 0
        assert body["findings"]
        assert body["findings"][0]["provenance"]["fetch_outcome"] == "curated_text"
        assert "normalized_title" in body["findings"][0]["provenance"]["scoring"]
        assert "semantic_novelty_score" in body["findings"][0]["provenance"]["scoring"]
        assert body["facet_coverage"]
        assert "diagnostic_summary" in body["facet_coverage"][0]
        assert "semantic" in body["run"]["telemetry"]


def test_long_running_routes_return_503_when_worker_disabled(temp_data_dir, monkeypatch) -> None:
    populate_lore_packet_fixtures(temp_data_dir)
    monkeypatch.setattr(settings, "app_job_worker_enabled", False)
    monkeypatch.setattr(settings, "app_allow_queued_jobs_without_worker", False)

    with TestClient(app) as client:
        profile = client.put(
            "/v1/bible/profiles/project-greyport",
            json={"project_name": "Greyport Bible", "geography": "Greyport"},
        )
        assert profile.status_code == 200

        section = client.post(
            "/v1/bible/sections",
            json={"project_id": "project-greyport", "section_type": "setting_overview"},
        )
        assert section.status_code == 503
        assert "Background worker is disabled" in section.json()["detail"]

        research = client.post(
            "/v1/research/runs",
            json={"brief": {"topic": "Greyport docks", "adapter_id": "curated_inputs"}},
        )
        assert research.status_code == 503
        assert "Background worker is disabled" in research.json()["detail"]


def test_async_author_flow_uses_background_jobs_end_to_end(temp_data_dir) -> None:
    project_id = "project-author-flow"

    with TestClient(app) as client:
        profile = client.put(
            f"/v1/bible/profiles/{project_id}",
            json={
                "project_name": "Author Flow Demo",
                "era": "1421-1422 winter shortage",
                "geography": "Rouen",
                "narrative_focus": "market control and winter scarcity",
                "desired_facets": ["economics", "institutions"],
                "composition_defaults": {"include_statuses": ["verified", "probable"]},
            },
        )
        assert profile.status_code == 200

        create_run = client.post(
            "/v1/research/runs",
            json={
                "program_id": "historical-daily-life",
                "brief": {
                    "topic": "Rouen winter shortage daily life",
                    "focal_year": "1422",
                    "time_start": "1421-12-01",
                    "time_end": "1422-02-28",
                    "locale": "Rouen",
                    "audience": "historical fiction authors",
                    "adapter_id": "curated_inputs",
                    "desired_facets": ["practices"],
                    "curated_inputs": [
                        {
                            "input_type": "text",
                            "title": "Archive workflow note on bread scrip distribution",
                            "text": (
                                "Rouen bakers described the routine workflow for "
                                "stamped bread scrip distribution at the market gate "
                                "in 1422, and neighbors compared the tokens before dawn. "
                                "Rouen bakers were paid in bread scrip during the winter of 1422, "
                                "and neighbors compared the stamped tokens at the market gate."
                            ),
                            "source_type": "archive",
                            "published_at": "1422-01-18",
                        }
                    ],
                },
            },
        )
        assert create_run.status_code == 202
        create_job = wait_for_job(client, create_run.json()["job_id"])
        assert create_job["status"] == "completed"
        run_id = create_job["result_ref"]["run_id"]

        run_detail = client.get(f"/v1/research/runs/{run_id}")
        assert run_detail.status_code == 200
        detail_body = run_detail.json()
        assert detail_body["findings"]
        assert detail_body["facet_coverage"]
        assert detail_body["run"]["latest_job"]["job_id"] == create_job["job_id"]

        stage_response = client.post(f"/v1/research/runs/{run_id}/stage")
        assert stage_response.status_code == 202
        stage_job = wait_for_job(client, stage_response.json()["job_id"])
        assert stage_job["status"] == "completed"

        staged_detail = client.get(f"/v1/research/runs/{run_id}")
        assert staged_detail.status_code == 200
        staged_findings = staged_detail.json()["findings"]
        accepted_findings = [item for item in staged_findings if item["decision"] == "accepted"]
        assert accepted_findings
        assert all(item["staged_source_id"] for item in accepted_findings)
        assert all(item["staged_document_id"] for item in accepted_findings)

        extract_response = client.post(f"/v1/research/runs/{run_id}/extract")
        assert extract_response.status_code == 202
        extract_job = wait_for_job(client, extract_response.json()["job_id"])
        assert extract_job["status"] == "completed"

        extracted_detail = client.get(f"/v1/research/runs/{run_id}")
        assert extracted_detail.status_code == 200
        extracted_body = extracted_detail.json()
        extraction_run_id = extracted_body["run"]["extraction_run_id"]
        assert extraction_run_id

        pending_candidates = client.get("/v1/candidates?review_state=pending")
        assert pending_candidates.status_code == 200
        new_candidates = [
            item
            for item in pending_candidates.json()
            if item["extractor_run_id"] == extraction_run_id
        ]
        assert new_candidates
        candidate_to_approve = next(
            (item for item in new_candidates if item["claim_kind"] in {"practice", "object"}),
            new_candidates[0],
        )

        approve_response = client.post(
            f"/v1/candidates/{candidate_to_approve['candidate_id']}/review",
            json={"decision": "approve"},
        )
        assert approve_response.status_code == 200
        approved_claim_id = approve_response.json()["claim"]["claim_id"]

        compose_section = client.post(
            "/v1/bible/sections",
            json={
                "project_id": project_id,
                "section_type": "economics_and_material_culture",
                "filters": {
                    "statuses": ["verified", "probable"],
                },
            },
        )
        assert compose_section.status_code == 202
        section_job = wait_for_job(client, compose_section.json()["job_id"])
        assert section_job["status"] == "completed"
        section_id = section_job["result_ref"]["section_id"]

        section = client.get(f"/v1/bible/sections/{section_id}")
        assert section.status_code == 200
        section_body = section.json()
        assert section_body["references"]["claim_ids"]
        assert approved_claim_id in section_body["references"]["claim_ids"]
        assert section_body["latest_job"]["job_id"] == section_job["job_id"]
        assert "composition_metrics" in section_body

        provenance = client.get(f"/v1/bible/sections/{section_id}/provenance")
        assert provenance.status_code == 200
        provenance_body = provenance.json()
        assert provenance_body["paragraphs"]
        assert any(
            approved_claim_id in item["paragraph"]["claim_ids"]
            for item in provenance_body["paragraphs"]
        )
        assert any(item["sources"] for item in provenance_body["paragraphs"])

        export_job_response = client.post(f"/v1/bible/exports/{project_id}")
        assert export_job_response.status_code == 202
        export_job = wait_for_job(client, export_job_response.json()["job_id"])
        assert export_job["status"] == "completed"
        export_body = export_job["result_payload"]
        assert export_body["profile"]["project_id"] == project_id
        assert any(item["section_id"] == section_id for item in export_body["sections"])


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
