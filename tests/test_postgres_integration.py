from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient
from psycopg import connect
from psycopg.sql import SQL, Identifier

from source_aware_worldbuilding.adapters.postgres_backed import PostgresTruthStore
from source_aware_worldbuilding.api.main import app
from source_aware_worldbuilding.cli import seed_dev_data
from source_aware_worldbuilding.domain.enums import ClaimKind, ClaimStatus, ReviewDecision
from source_aware_worldbuilding.domain.models import ApprovedClaim, EvidenceSnippet, ReviewEvent


def _table_count(dsn: str, schema: str, table_name: str) -> int:
    query = SQL("SELECT COUNT(*) FROM {}.{}").format(
        Identifier(schema),
        Identifier(table_name),
    )
    with connect(dsn, autocommit=True) as connection:
        count = connection.execute(query).fetchone()
    assert count is not None
    return int(count[0])


def test_operator_flow_routes_share_postgres_backed_state(
    postgres_app_state: dict[str, str | Path],
) -> None:
    dsn = str(postgres_app_state["dsn"])
    schema = str(postgres_app_state["schema"])

    seed_dev_data()

    assert _table_count(dsn, schema, "sources") == 2
    assert _table_count(dsn, schema, "text_units") == 2
    assert _table_count(dsn, schema, "extraction_runs") == 1
    assert _table_count(dsn, schema, "candidates") == 2
    assert _table_count(dsn, schema, "evidence") == 2
    assert _table_count(dsn, schema, "review_events") == 0
    assert _table_count(dsn, schema, "claims") == 0
    assert _table_count(dsn, schema, "claim_evidence") == 0
    assert _table_count(dsn, schema, "claim_reviews") == 0
    assert _table_count(dsn, schema, "claim_versions") == 0
    assert _table_count(dsn, schema, "source_documents") == 0
    assert _table_count(dsn, schema, "source_chunks") == 0

    with TestClient(app) as client:
        sources_before = client.get("/v1/sources")
        assert sources_before.status_code == 200
        assert len(sources_before.json()) == 2

        pull_response = client.post("/v1/ingest/zotero/pull")
        assert pull_response.status_code == 200
        pull_body = pull_response.json()
        assert pull_body["count"] == 2
        assert _table_count(dsn, schema, "sources") == 2
        assert _table_count(dsn, schema, "text_units") == 6

        run_response = client.post("/v1/ingest/extract-candidates")
        assert run_response.status_code == 200
        run_body = run_response.json()
        assert run_body["count"] == 3
        assert run_body["run"]["status"] == "completed"
        assert len(run_body["evidence"]) == 12
        assert _table_count(dsn, schema, "extraction_runs") == 2
        assert _table_count(dsn, schema, "candidates") == 5
        assert _table_count(dsn, schema, "evidence") == 14

        runs = client.get("/v1/extraction-runs")
        assert runs.status_code == 200
        assert runs.json()[0]["status"] == "completed"

        candidates_before = client.get("/v1/candidates?review_state=pending")
        assert candidates_before.status_code == 200
        assert len(candidates_before.json()) == 5
        assert all(item["review_state"] == "pending" for item in candidates_before.json())

        first_candidate_id = candidates_before.json()[0]["candidate_id"]
        approve_response = client.post(
            f"/v1/candidates/{first_candidate_id}/review",
            json={"decision": "approve"},
        )
        assert approve_response.status_code == 200
        assert approve_response.json()["status"] == "approved"
        assert _table_count(dsn, schema, "review_events") == 1
        assert _table_count(dsn, schema, "claims") == 1
        assert _table_count(dsn, schema, "claim_evidence") == 1
        assert _table_count(dsn, schema, "claim_reviews") == 1
        assert _table_count(dsn, schema, "claim_versions") == 1
        assert _table_count(dsn, schema, "source_documents") == 1
        assert _table_count(dsn, schema, "source_chunks") == 1

        source_detail = client.get("/v1/sources/src-1")
        assert source_detail.status_code == 200
        assert source_detail.json()["source"]["source_id"] == "src-1"
        assert len(source_detail.json()["text_units"]) == 3

        candidate_detail = client.get(f"/v1/candidates/{first_candidate_id}")
        assert candidate_detail.status_code == 200
        assert candidate_detail.json()["review_state"] == "approved"

        claims_response = client.get("/v1/claims")
        assert claims_response.status_code == 200
        assert len(claims_response.json()) == 1

        query_response = client.post(
            "/v1/query",
            json={"question": "Rouen bread prices", "mode": "strict_facts"},
        )
        assert query_response.status_code == 200
        assert query_response.json()["supporting_claims"]


def test_postgres_truth_store_derives_claim_relationships(
    postgres_app_state: dict[str, str | Path],
) -> None:
    dsn = str(postgres_app_state["dsn"])
    schema = str(postgres_app_state["schema"])
    store = PostgresTruthStore(dsn, schema)

    store.save_claim(
        ApprovedClaim(
            claim_id="claim-1",
            subject="Rouen bread prices",
            predicate="rose_during",
            value="winter shortage",
            claim_kind=ClaimKind.PRACTICE,
            status=ClaimStatus.PROBABLE,
            place="Rouen",
            time_start="1421-12-01",
            time_end="1422-02-28",
            evidence_ids=["evi-1"],
            created_from_run_id="run-1",
        ),
        evidence=[
            EvidenceSnippet(
                evidence_id="evi-1",
                source_id="src-1",
                locator="folio 12r",
                text="Bread prices rose sharply during the winter shortage.",
                text_unit_id="txt-1",
            )
        ],
        review=ReviewEvent(
            review_id="rev-1",
            candidate_id="cand-1",
            decision=ReviewDecision.APPROVE,
            approved_claim_id="claim-1",
        ),
    )
    store.save_claim(
        ApprovedClaim(
            claim_id="claim-2",
            subject="Rouen bread prices",
            predicate="rose_during",
            value="winter shortage",
            claim_kind=ClaimKind.PRACTICE,
            status=ClaimStatus.VERIFIED,
            place="Rouen",
            time_start="1421-12-01",
            time_end="1422-02-28",
            evidence_ids=["evi-2"],
            created_from_run_id="run-2",
        ),
        evidence=[
            EvidenceSnippet(
                evidence_id="evi-2",
                source_id="src-2",
                locator="folio 14r",
                text="The winter shortage drove bread prices upward.",
                text_unit_id="txt-2",
            )
        ],
        review=ReviewEvent(
            review_id="rev-2",
            candidate_id="cand-2",
            decision=ReviewDecision.APPROVE,
            approved_claim_id="claim-2",
        ),
    )
    store.save_claim(
        ApprovedClaim(
            claim_id="claim-3",
            subject="Rouen bread prices",
            predicate="rose_during",
            value="winter shortage",
            claim_kind=ClaimKind.PRACTICE,
            status=ClaimStatus.PROBABLE,
            place="Rouen",
            evidence_ids=["evi-3"],
            created_from_run_id="run-3",
        ),
        evidence=[
            EvidenceSnippet(
                evidence_id="evi-3",
                source_id="src-3",
                locator="folio 15r",
                text="Regional accounts also describe the same winter shortage.",
                text_unit_id="txt-3",
            )
        ],
        review=ReviewEvent(
            review_id="rev-3",
            candidate_id="cand-3",
            decision=ReviewDecision.APPROVE,
            approved_claim_id="claim-3",
        ),
    )
    store.save_claim(
        ApprovedClaim(
            claim_id="claim-4",
            subject="Rouen bread prices",
            predicate="rose_during",
            value="harvest glut",
            claim_kind=ClaimKind.PRACTICE,
            status=ClaimStatus.CONTESTED,
            place="Rouen",
            time_start="1421-12-15",
            time_end="1422-01-15",
            evidence_ids=["evi-4"],
            created_from_run_id="run-4",
        ),
        evidence=[
            EvidenceSnippet(
                evidence_id="evi-4",
                source_id="src-4",
                locator="folio 16r",
                text="A later account instead blamed a harvest glut.",
                text_unit_id="txt-4",
            )
        ],
        review=ReviewEvent(
            review_id="rev-4",
            candidate_id="cand-4",
            decision=ReviewDecision.APPROVE,
            approved_claim_id="claim-4",
        ),
    )
    store.save_claim(
        ApprovedClaim(
            claim_id="claim-5",
            subject="Rouen bread prices",
            predicate="rose_during",
            value="another shortage",
            claim_kind=ClaimKind.PRACTICE,
            status=ClaimStatus.CONTESTED,
            place="Rouen",
            time_start="1500-01-01",
            time_end="1500-12-31",
            evidence_ids=["evi-5"],
            created_from_run_id="run-5",
        ),
        evidence=[
            EvidenceSnippet(
                evidence_id="evi-5",
                source_id="src-5",
                locator="folio 18r",
                text="A much later shortage is described elsewhere.",
                text_unit_id="txt-5",
            )
        ],
        review=ReviewEvent(
            review_id="rev-5",
            candidate_id="cand-5",
            decision=ReviewDecision.APPROVE,
            approved_claim_id="claim-5",
        ),
    )

    active_claim_ids = [item.claim_id for item in store.list_claims()]
    relationships = store.list_relationships()

    assert "claim-1" not in active_claim_ids
    assert {"claim-2", "claim-3", "claim-4", "claim-5"} <= set(active_claim_ids)
    assert any(
        item.claim_id == "claim-2"
        and item.related_claim_id == "claim-1"
        and item.relationship_type == "supersedes"
        and "Distinct provenance" in (item.notes or "")
        for item in relationships
    )
    assert any(
        item.claim_id == "claim-3"
        and item.related_claim_id == "claim-2"
        and item.relationship_type == "supports"
        for item in relationships
    )
    assert any(
        item.claim_id == "claim-4"
        and item.related_claim_id == "claim-2"
        and item.relationship_type == "contradicts"
        for item in relationships
    )
    assert not any(
        item.claim_id == "claim-5" and item.relationship_type == "contradicts"
        for item in relationships
    )
