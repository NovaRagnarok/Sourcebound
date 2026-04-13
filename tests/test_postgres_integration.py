from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient
from psycopg import connect
from psycopg.sql import SQL, Identifier

from source_aware_worldbuilding.api.main import app
from source_aware_worldbuilding.cli import seed_dev_data


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
        assert approve_response.status_code == 503
        assert "Canonical Wikibase truth store is not configured" in approve_response.json()[
            "detail"
        ]
        assert _table_count(dsn, schema, "review_events") == 0

        source_detail = client.get("/v1/sources/src-1")
        assert source_detail.status_code == 200
        assert source_detail.json()["source"]["source_id"] == "src-1"
        assert len(source_detail.json()["text_units"]) == 3

        candidate_detail = client.get(f"/v1/candidates/{first_candidate_id}")
        assert candidate_detail.status_code == 200
        assert candidate_detail.json()["review_state"] == "pending"

        claims_response = client.get("/v1/claims")
        assert claims_response.status_code == 503
        assert "Canonical Wikibase truth store is not configured" in claims_response.json()[
            "detail"
        ]

        query_response = client.post(
            "/v1/query",
            json={"question": "Rouen bread prices", "mode": "strict_facts"},
        )
        assert query_response.status_code == 503
        assert "Canonical Wikibase truth store is not configured" in query_response.json()[
            "detail"
        ]
