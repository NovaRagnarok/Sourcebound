from __future__ import annotations

from pathlib import Path

from source_aware_worldbuilding.adapters.file_backed import (
    FileEvidenceStore,
    FileSourceStore,
    FileTruthStore,
)
from source_aware_worldbuilding.domain.enums import ClaimKind, ClaimStatus, QueryMode
from source_aware_worldbuilding.domain.models import (
    ApprovedClaim,
    EvidenceSnippet,
    QueryFilter,
    QueryRequest,
    SourceRecord,
)
from source_aware_worldbuilding.services.query import QueryService
from source_aware_worldbuilding.storage.json_store import JsonListStore


def populate_query_fixtures(data_dir: Path) -> None:
    JsonListStore(data_dir / "sources.json").write_models(
        [
            SourceRecord(source_id="src-1", title="Rouen records", source_type="record"),
            SourceRecord(source_id="src-2", title="Chronicle", source_type="chronicle"),
            SourceRecord(source_id="src-3", title="Market notes", source_type="chronicle"),
            SourceRecord(source_id="src-4", title="Ghost appendix", source_type="chronicle"),
            SourceRecord(source_id="src-5", title="Rouen market accounts", source_type="record"),
            SourceRecord(source_id="src-6", title="Paris market accounts", source_type="record"),
        ]
    )
    JsonListStore(data_dir / "evidence.json").write_models(
        [
            EvidenceSnippet(
                evidence_id="evi-1",
                source_id="src-1",
                locator="folio 12r",
                text="Bread prices rose sharply during the winter shortage.",
                notes="Verified municipal record",
            ),
            EvidenceSnippet(
                evidence_id="evi-2",
                source_id="src-2",
                locator="chapter 7",
                text="Townspeople whispered that merchants were withholding grain.",
                notes="Contested chronicle",
            ),
            EvidenceSnippet(
                evidence_id="evi-3",
                source_id="src-3",
                locator="leaf 4v",
                text="The market whispered about a hidden bread cartel.",
                notes="Rumor text",
            ),
            EvidenceSnippet(
                evidence_id="evi-4",
                source_id="src-4",
                locator="appendix",
                text="A ghost was said to haunt the market square.",
                notes="Legend text",
            ),
            EvidenceSnippet(
                evidence_id="evi-5",
                source_id="src-5",
                locator="folio 15r",
                text="Bread gossip concerned the Rouen market.",
                notes="Probable Rouen record",
            ),
            EvidenceSnippet(
                evidence_id="evi-6",
                source_id="src-6",
                locator="folio 16r",
                text="Bread gossip also concerned Paris.",
                notes="Probable Paris record",
            ),
        ]
    )
    JsonListStore(data_dir / "claims.json").write_models(
        [
            ApprovedClaim(
                claim_id="claim-verified-1",
                subject="Rouen bread prices",
                predicate="rose_during",
                value="winter shortage",
                claim_kind=ClaimKind.PRACTICE,
                status=ClaimStatus.VERIFIED,
                place="Rouen",
                evidence_ids=["evi-1"],
            ),
            ApprovedClaim(
                claim_id="claim-probable-1",
                subject="Market gossip about bread prices",
                predicate="is_reported_in",
                value="Rouen market accounts",
                claim_kind=ClaimKind.BELIEF,
                status=ClaimStatus.PROBABLE,
                place="Rouen",
                evidence_ids=["evi-5"],
            ),
            ApprovedClaim(
                claim_id="claim-probable-2",
                subject="Market gossip about bread prices",
                predicate="is_reported_in",
                value="Paris market accounts",
                claim_kind=ClaimKind.BELIEF,
                status=ClaimStatus.PROBABLE,
                place="Paris",
                evidence_ids=["evi-6"],
            ),
            ApprovedClaim(
                claim_id="claim-contested-1",
                subject="Merchant grain hoarding",
                predicate="is_reported_in",
                value="townspeople accounts",
                claim_kind=ClaimKind.BELIEF,
                status=ClaimStatus.CONTESTED,
                place="Rouen",
                viewpoint_scope="townspeople",
                evidence_ids=["evi-2"],
            ),
            ApprovedClaim(
                claim_id="claim-rumor-1",
                subject="Whispered rumor",
                predicate="circulates_in",
                value="the marketplace",
                claim_kind=ClaimKind.BELIEF,
                status=ClaimStatus.RUMOR,
                place="Rouen",
                viewpoint_scope="townspeople",
                evidence_ids=["evi-3"],
            ),
            ApprovedClaim(
                claim_id="claim-legend-1",
                subject="Market ghost",
                predicate="haunts",
                value="the square",
                claim_kind=ClaimKind.BELIEF,
                status=ClaimStatus.LEGEND,
                place="Rouen",
                evidence_ids=["evi-4"],
            ),
        ]
    )


def build_query_service(data_dir: Path) -> QueryService:
    return QueryService(
        truth_store=FileTruthStore(data_dir),
        evidence_store=FileEvidenceStore(data_dir),
        source_store=FileSourceStore(data_dir),
    )


def test_query_modes_return_expected_claims_and_warnings(temp_data_dir: Path) -> None:
    populate_query_fixtures(temp_data_dir)
    service = build_query_service(temp_data_dir)

    cases = [
        (
            QueryMode.STRICT_FACTS,
            "Rouen bread prices",
            ClaimStatus.VERIFIED,
            "Strict facts mode hides rumor and legend by design.",
            1,
            "evi-1",
            "src-1",
        ),
        (
            QueryMode.CONTESTED_VIEWS,
            "Merchant grain hoarding",
            ClaimStatus.CONTESTED,
            "Contested views mode prefers disputed claims.",
            1,
            "evi-2",
            "src-2",
        ),
        (
            QueryMode.RUMOR_AND_LEGEND,
            "Whispered rumor",
            ClaimStatus.RUMOR,
            "Rumor and legend mode surfaces low-certainty material intentionally.",
            1,
            "evi-3",
            "src-3",
        ),
    ]

    for (
        mode,
        question,
        expected_status,
        warning_fragment,
        expected_count,
        expected_evidence_id,
        expected_source_id,
    ) in cases:
        result = service.answer(QueryRequest(question=question, mode=mode))
        assert result.question == question
        assert result.mode == mode
        assert len(result.supporting_claims) == expected_count
        assert result.supporting_claims[0].status == expected_status
        assert result.evidence[0].evidence_id == expected_evidence_id
        assert result.sources[0].source_id == expected_source_id
        assert warning_fragment in result.warnings[0]
        assert result.answer.startswith(f"- {result.supporting_claims[0].subject}")


def test_query_filters_can_narrow_matching_claims(temp_data_dir: Path) -> None:
    populate_query_fixtures(temp_data_dir)
    service = build_query_service(temp_data_dir)

    result = service.answer(
        QueryRequest(
            question="Market gossip about bread prices",
            mode=QueryMode.OPEN_EXPLORATION,
            filters=QueryFilter(status=ClaimStatus.PROBABLE, place="Rouen"),
        )
    )

    assert len(result.supporting_claims) == 1
    assert result.supporting_claims[0].place == "Rouen"
    assert result.supporting_claims[0].status == ClaimStatus.PROBABLE
    assert result.evidence[0].evidence_id == "evi-5"
    assert result.sources[0].source_id == "src-5"


def test_query_strict_facts_reports_gap_when_only_uncertain_claims_remain(
    temp_data_dir: Path,
) -> None:
    JsonListStore(temp_data_dir / "sources.json").write_models(
        [
            SourceRecord(source_id="src-3", title="Rumor source", source_type="chronicle"),
            SourceRecord(source_id="src-4", title="Legend source", source_type="chronicle"),
        ]
    )
    JsonListStore(temp_data_dir / "evidence.json").write_models(
        [
            EvidenceSnippet(
                evidence_id="evi-3",
                source_id="src-3",
                locator="leaf 4v",
                text="The market whispered about a hidden bread cartel.",
                notes="Rumor text",
            ),
            EvidenceSnippet(
                evidence_id="evi-4",
                source_id="src-4",
                locator="appendix",
                text="A ghost was said to haunt the market square.",
                notes="Legend text",
            ),
        ]
    )
    JsonListStore(temp_data_dir / "claims.json").write_models(
        [
            ApprovedClaim(
                claim_id="claim-rumor-1",
                subject="Whispered rumor",
                predicate="circulates_in",
                value="the marketplace",
                claim_kind=ClaimKind.BELIEF,
                status=ClaimStatus.RUMOR,
                place="Rouen",
                evidence_ids=["evi-3"],
            ),
            ApprovedClaim(
                claim_id="claim-legend-1",
                subject="Market ghost",
                predicate="haunts",
                value="the square",
                claim_kind=ClaimKind.BELIEF,
                status=ClaimStatus.LEGEND,
                place="Rouen",
                evidence_ids=["evi-4"],
            ),
        ]
    )

    result = build_query_service(temp_data_dir).answer(
        QueryRequest(question="market", mode=QueryMode.STRICT_FACTS)
    )

    assert result.supporting_claims == []
    assert result.evidence == []
    assert result.sources == []
    assert result.answer.startswith("No approved claims matched")
