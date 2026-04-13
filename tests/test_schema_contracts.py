from __future__ import annotations

import json
from pathlib import Path

from source_aware_worldbuilding.cli import seed_dev_data
from source_aware_worldbuilding.domain.enums import ReviewState
from source_aware_worldbuilding.domain.models import (
    ApprovedClaim,
    CandidateClaim,
    EvidenceSnippet,
    SourceRecord,
    TextUnit,
)

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_DIR = ROOT / "docs" / "schemas"
DATA_DIR = ROOT / "data" / "dev"


def load_json_list(path: Path) -> list[dict]:
    return json.loads(path.read_text(encoding="utf-8"))


def assert_schema_matches_model(schema_path: Path, model_type, required_fields: set[str]) -> None:
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    assert schema["type"] == "object"
    assert set(schema["required"]) == required_fields
    assert set(schema["properties"]) == set(model_type.model_fields)
    assert schema["additionalProperties"] is False


def test_schema_files_match_domain_models() -> None:
    assert_schema_matches_model(
        SCHEMA_DIR / "candidate-claim.schema.json",
        CandidateClaim,
        {
            "candidate_id",
            "subject",
            "predicate",
            "value",
            "claim_kind",
            "status_suggestion",
            "review_state",
            "evidence_ids",
        },
    )
    assert_schema_matches_model(
        SCHEMA_DIR / "claim.schema.json",
        ApprovedClaim,
        {
            "claim_id",
            "subject",
            "predicate",
            "value",
            "claim_kind",
            "status",
            "evidence_ids",
        },
    )
    assert_schema_matches_model(
        SCHEMA_DIR / "evidence.schema.json",
        EvidenceSnippet,
        {
            "evidence_id",
            "source_id",
            "locator",
            "text",
        },
    )


def test_repository_fixtures_match_domain_models() -> None:
    sources = [
        SourceRecord.model_validate(item) for item in load_json_list(DATA_DIR / "sources.json")
    ]
    text_units = [
        TextUnit.model_validate(item) for item in load_json_list(DATA_DIR / "text_units.json")
    ]
    evidence = [
        EvidenceSnippet.model_validate(item) for item in load_json_list(DATA_DIR / "evidence.json")
    ]
    candidates = [
        CandidateClaim.model_validate(item) for item in load_json_list(DATA_DIR / "candidates.json")
    ]

    assert len(sources) == 2
    assert len(text_units) == 2
    assert len(evidence) == 2
    assert len(candidates) == 2

    evidence_ids = {item.evidence_id for item in evidence}
    source_ids = {item.source_id for item in sources}

    assert all(item.source_id in source_ids for item in text_units)
    assert all(item.source_id in source_ids for item in evidence)
    assert all(set(item.evidence_ids) <= evidence_ids for item in candidates)
    assert {item.review_state for item in candidates} == {ReviewState.PENDING}


def test_seed_dev_data_writes_reproducible_temp_fixtures(temp_data_dir) -> None:
    seed_dev_data()

    assert len(load_json_list(temp_data_dir / "sources.json")) == 2
    assert len(load_json_list(temp_data_dir / "text_units.json")) == 2
    assert len(load_json_list(temp_data_dir / "evidence.json")) == 2
    assert len(load_json_list(temp_data_dir / "candidates.json")) == 2
    assert len(load_json_list(temp_data_dir / "claims.json")) == 0
    assert len(load_json_list(temp_data_dir / "claim_relationships.json")) == 0
    assert len(load_json_list(temp_data_dir / "source_documents.json")) == 0
