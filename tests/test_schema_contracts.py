from __future__ import annotations

import json
from pathlib import Path

from source_aware_worldbuilding.cli import seed_dev_data
from source_aware_worldbuilding.domain.enums import ReviewState
from source_aware_worldbuilding.domain.models import (
    ApprovedClaim,
    BibleProjectProfile,
    BibleSection,
    CandidateClaim,
    ClaimRelationship,
    EvidenceSnippet,
    JobRecord,
    ResearchFinding,
    ResearchRun,
    ReviewEvent,
    SourceDocumentRecord,
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
    source_documents = [
        SourceDocumentRecord.model_validate(item)
        for item in load_json_list(DATA_DIR / "source_documents.json")
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
    claims = [
        ApprovedClaim.model_validate(item) for item in load_json_list(DATA_DIR / "claims.json")
    ]
    relationships = [
        ClaimRelationship.model_validate(item)
        for item in load_json_list(DATA_DIR / "claim_relationships.json")
    ]
    review_events = [
        ReviewEvent.model_validate(item) for item in load_json_list(DATA_DIR / "review_events.json")
    ]
    research_runs = [
        ResearchRun.model_validate(item) for item in load_json_list(DATA_DIR / "research_runs.json")
    ]
    research_findings = [
        ResearchFinding.model_validate(item)
        for item in load_json_list(DATA_DIR / "research_findings.json")
    ]
    jobs = [JobRecord.model_validate(item) for item in load_json_list(DATA_DIR / "jobs.json")]
    profiles = [
        BibleProjectProfile.model_validate(item)
        for item in load_json_list(DATA_DIR / "bible_project_profiles.json")
    ]
    sections = [
        BibleSection.model_validate(item)
        for item in load_json_list(DATA_DIR / "bible_sections.json")
    ]

    assert len(sources) == 10
    assert len(source_documents) == 10
    assert len(text_units) == 10
    assert len(evidence) == 10
    assert len(candidates) == 6
    assert len(claims) == 9
    assert len(relationships) == 5
    assert len(review_events) == 4
    assert len(research_runs) == 1
    assert len(research_findings) == 4
    assert len(jobs) == 7
    assert len(profiles) == 1
    assert len(sections) == 3

    evidence_ids = {item.evidence_id for item in evidence}
    source_ids = {item.source_id for item in sources}
    claim_ids = {item.claim_id for item in claims}

    assert all(item.source_id in source_ids for item in source_documents)
    assert all(item.source_id in source_ids for item in text_units)
    assert all(item.source_id in source_ids for item in evidence)
    assert all(set(item.evidence_ids) <= evidence_ids for item in candidates)
    assert all(set(item.evidence_ids) <= evidence_ids for item in claims)
    assert all(
        item.claim_id in claim_ids and item.related_claim_id in claim_ids for item in relationships
    )
    assert all(item.run_id == "research-rouen-winter" for item in research_findings)
    assert profiles[0].project_id == "project-rouen-winter"
    assert {item.project_id for item in sections} == {"project-rouen-winter"}
    assert {item.review_state for item in candidates} == {
        ReviewState.APPROVED,
        ReviewState.PENDING,
        ReviewState.REJECTED,
    }
    assert any(section.has_manual_edits for section in sections)
    assert any(not section.has_manual_edits for section in sections)


def test_seed_dev_data_writes_reproducible_temp_fixtures(temp_data_dir) -> None:
    seed_dev_data()

    assert len(load_json_list(temp_data_dir / "sources.json")) == 10
    assert len(load_json_list(temp_data_dir / "source_documents.json")) == 10
    assert len(load_json_list(temp_data_dir / "text_units.json")) == 10
    assert len(load_json_list(temp_data_dir / "evidence.json")) == 10
    assert len(load_json_list(temp_data_dir / "candidates.json")) == 6
    assert len(load_json_list(temp_data_dir / "review_events.json")) == 4
    assert len(load_json_list(temp_data_dir / "claims.json")) == 9
    assert len(load_json_list(temp_data_dir / "claim_relationships.json")) == 5
    assert len(load_json_list(temp_data_dir / "research_runs.json")) == 1
    assert len(load_json_list(temp_data_dir / "research_findings.json")) == 4
    assert len(load_json_list(temp_data_dir / "jobs.json")) == 7
    assert len(load_json_list(temp_data_dir / "bible_project_profiles.json")) == 1
    assert len(load_json_list(temp_data_dir / "bible_sections.json")) == 3
