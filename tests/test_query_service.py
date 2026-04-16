from __future__ import annotations

from pathlib import Path

from source_aware_worldbuilding.adapters.file_backed import FileEvidenceStore, FileSourceStore
from source_aware_worldbuilding.domain.enums import ClaimKind, ClaimStatus, QueryMode
from source_aware_worldbuilding.domain.models import (
    ApprovedClaim,
    BibleProjectProfile,
    ClaimRelationship,
    EvidenceSnippet,
    ProjectionSearchResult,
    QueryFilter,
    QueryRequest,
    SourceRecord,
)
from source_aware_worldbuilding.services.query import QueryService
from source_aware_worldbuilding.storage.json_store import JsonListStore


class InMemoryTruthStore:
    def __init__(
        self,
        claims: list[ApprovedClaim],
        relationships: list[ClaimRelationship] | None = None,
    ) -> None:
        self.claims = {claim.claim_id: claim for claim in claims}
        self.relationships = relationships or []

    def list_claims(self) -> list[ApprovedClaim]:
        return list(self.claims.values())

    def get_claim(self, claim_id: str) -> ApprovedClaim | None:
        return self.claims.get(claim_id)

    def list_relationships(self, claim_id: str | None = None) -> list[ClaimRelationship]:
        if claim_id is None:
            return list(self.relationships)
        return [item for item in self.relationships if item.claim_id == claim_id]

    def save_claim(self, claim: ApprovedClaim, evidence=None, review=None) -> None:
        _ = evidence, review
        self.claims[claim.claim_id] = claim


class InMemoryProfileStore:
    def __init__(self, profiles: list[BibleProjectProfile] | None = None) -> None:
        self.profiles = {profile.project_id: profile for profile in profiles or []}

    def list_profiles(self) -> list[BibleProjectProfile]:
        return list(self.profiles.values())

    def get_profile(self, project_id: str) -> BibleProjectProfile | None:
        return self.profiles.get(project_id)

    def save_profile(self, profile: BibleProjectProfile) -> None:
        self.profiles[profile.project_id] = profile


def populate_query_fixtures(data_dir: Path) -> list[ApprovedClaim]:
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
    return [
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


class FakeProjection:
    def __init__(self, result: ProjectionSearchResult):
        self.result = result

    def upsert_claims(self, claims, evidence) -> None:
        _ = claims, evidence

    def search_claim_ids(self, question: str, allowed_claim_ids: list[str], *, limit: int = 10):
        _ = question, allowed_claim_ids, limit
        return self.result


def build_query_service(
    data_dir: Path,
    claims: list[ApprovedClaim],
    projection=None,
    relationships: list[ClaimRelationship] | None = None,
    profiles: list[BibleProjectProfile] | None = None,
) -> QueryService:
    return QueryService(
        truth_store=InMemoryTruthStore(claims, relationships=relationships),
        evidence_store=FileEvidenceStore(data_dir),
        source_store=FileSourceStore(data_dir),
        projection=projection,
        profile_store=InMemoryProfileStore(profiles),
    )


def test_query_modes_return_expected_claims_and_warnings(temp_data_dir: Path) -> None:
    claims = populate_query_fixtures(temp_data_dir)
    service = build_query_service(temp_data_dir, claims)

    cases = [
        (
            QueryMode.STRICT_FACTS,
            "Rouen bread prices",
            ClaimStatus.VERIFIED,
            "Strict facts mode hides rumor and legend by design.",
            2,
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
        assert result.metadata.answer_boundary == "direct_answer"
        assert result.direct_match_claim_ids
        assert result.metadata.retrieval_quality_tier in {"projection", "memory_ranked"}
        assert warning_fragment in result.warnings[0]
        assert result.claim_clusters
        assert result.answer_sections
        assert result.supporting_claims[0].subject in result.answer
        assert result.metadata.retrieval_backend == "memory"
        assert result.metadata.fallback_used is False
        assert isinstance(result.certainty_summary, dict)
        assert isinstance(result.coverage_gaps, list)
        assert isinstance(result.recommended_next_research, list)


def test_query_filters_can_narrow_matching_claims(temp_data_dir: Path) -> None:
    claims = populate_query_fixtures(temp_data_dir)
    service = build_query_service(temp_data_dir, claims)

    result = service.answer(
        QueryRequest(
            question="Market gossip about bread prices",
            mode=QueryMode.OPEN_EXPLORATION,
            filters=QueryFilter(
                status=ClaimStatus.PROBABLE,
                place="Rouen",
                source_types=["record"],
            ),
        )
    )

    assert len(result.supporting_claims) == 1
    assert result.supporting_claims[0].place == "Rouen"
    assert result.supporting_claims[0].status == ClaimStatus.PROBABLE
    assert result.evidence[0].evidence_id == "evi-5"
    assert result.sources[0].source_id == "src-5"
    assert result.metadata.retrieval_backend == "memory"


def test_query_keeps_focus_band_instead_of_single_strongest_claim(temp_data_dir: Path) -> None:
    claims = populate_query_fixtures(temp_data_dir)
    service = build_query_service(temp_data_dir, claims)

    result = service.answer(
        QueryRequest(
            question="What does Rouen market gossip say about bread prices?",
            mode=QueryMode.STRICT_FACTS,
        )
    )

    returned_ids = {claim.claim_id for claim in result.supporting_claims}
    assert "claim-verified-1" in returned_ids
    assert "claim-probable-1" in returned_ids
    assert "claim-probable-2" not in returned_ids
    assert result.answer_sections


def test_query_gap_first_mode_does_not_substitute_adjacent_canon_for_narrow_question(
    temp_data_dir: Path,
) -> None:
    claims = populate_query_fixtures(temp_data_dir)
    service = build_query_service(temp_data_dir, claims)

    result = service.answer(
        QueryRequest(
            question="How were bread tokens handled in Rouen during the winter shortage?",
            mode=QueryMode.STRICT_FACTS,
        )
    )

    assert result.supporting_claims == []
    assert result.evidence == []
    assert result.sources == []
    assert result.claim_clusters == []
    assert result.answer_sections == []
    assert "does not directly answer this question yet" in result.answer
    assert any("adjacent canon was not substituted" in warning for warning in result.warnings)
    assert result.coverage_gaps == ["Approved canon does not directly answer the question yet."]
    assert result.metadata.answer_boundary == "research_gap"
    assert result.direct_match_claim_ids == []
    assert result.adjacent_context_claim_ids == []
    assert [claim.claim_id for claim in result.nearby_claims] == [
        "claim-verified-1",
        "claim-probable-1",
    ]
    assert all(
        claim.status in {ClaimStatus.VERIFIED, ClaimStatus.PROBABLE}
        for claim in result.nearby_claims
    )
    assert result.recommended_next_research == [
        "Find directly documented evidence for: How were bread tokens handled in "
        "Rouen during the winter shortage?"
    ]
    assert result.suggested_follow_ups


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
    claims = [
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

    result = build_query_service(temp_data_dir, claims).answer(
        QueryRequest(question="market", mode=QueryMode.STRICT_FACTS)
    )

    assert result.supporting_claims == []
    assert result.evidence == []
    assert result.sources == []
    assert result.answer.startswith("Approved canon does not directly answer this question yet")
    assert result.metadata.retrieval_backend == "memory"
    assert result.nearby_claims == []
    assert result.suggested_follow_ups == []


def test_query_uses_projection_when_available(temp_data_dir: Path) -> None:
    claims = populate_query_fixtures(temp_data_dir)
    service = build_query_service(
        temp_data_dir,
        claims,
        projection=FakeProjection(
            ProjectionSearchResult(claim_ids=["claim-probable-2", "claim-probable-1"])
        ),
    )

    result = service.answer(
        QueryRequest(
            question="Market gossip about bread prices",
            mode=QueryMode.OPEN_EXPLORATION,
            filters=QueryFilter(status=ClaimStatus.PROBABLE),
        )
    )

    assert [claim.claim_id for claim in result.supporting_claims[:2]] == [
        "claim-probable-2",
        "claim-probable-1",
    ]
    assert result.metadata.retrieval_backend == "qdrant"
    assert result.metadata.fallback_used is False
    assert result.metadata.retrieval_quality_tier == "projection"
    assert result.metadata.ranking_strategy == "blended"


def test_query_uses_project_context_to_prefer_author_intent_matches(temp_data_dir: Path) -> None:
    claims = populate_query_fixtures(temp_data_dir)
    service = build_query_service(
        temp_data_dir,
        claims,
        profiles=[
            BibleProjectProfile(
                project_id="project-rouen",
                project_name="Rouen Winter Book",
                geography="Rouen",
                social_lens="townspeople",
                narrative_focus="winter shortage and bread queues",
                desired_facets=["beliefs", "practices"],
            )
        ],
    )

    result = service.answer(
        QueryRequest(
            question="Market gossip about bread prices",
            mode=QueryMode.OPEN_EXPLORATION,
            project_id="project-rouen",
        )
    )

    assert result.supporting_claims[0].claim_id == "claim-probable-1"
    assert result.metadata.ranking_strategy == "intent_blended"
    assert result.metadata.retrieval_backend == "memory"


def test_query_falls_back_to_memory_when_projection_fails(temp_data_dir: Path) -> None:
    claims = populate_query_fixtures(temp_data_dir)
    service = build_query_service(
        temp_data_dir,
        claims,
        projection=FakeProjection(
            ProjectionSearchResult(
                fallback_used=True,
                fallback_reason="Qdrant is disabled.",
            )
        ),
    )

    result = service.answer(
        QueryRequest(
            question="Rouen bread prices",
            mode=QueryMode.STRICT_FACTS,
        )
    )

    assert result.supporting_claims[0].claim_id == "claim-verified-1"
    assert result.metadata.retrieval_backend == "memory"
    assert result.metadata.fallback_used is True
    assert result.metadata.fallback_reason == "Qdrant is disabled."
    assert result.metadata.ranking_strategy == "lexical"
    assert result.metadata.retrieval_quality_tier == "memory_ranked"
    assert any("Qdrant fallback" in warning for warning in result.warnings)


def test_query_falls_back_to_memory_when_projection_returns_no_usable_hits(
    temp_data_dir: Path,
) -> None:
    claims = populate_query_fixtures(temp_data_dir)
    service = build_query_service(
        temp_data_dir,
        claims,
        projection=FakeProjection(
            ProjectionSearchResult(
                fallback_used=True,
                fallback_reason="Qdrant returned no usable hits.",
            )
        ),
    )

    result = service.answer(
        QueryRequest(
            question="Rouen bread prices",
            mode=QueryMode.STRICT_FACTS,
        )
    )

    assert result.supporting_claims[0].claim_id == "claim-verified-1"
    assert result.metadata.retrieval_backend == "memory"
    assert result.metadata.fallback_used is True
    assert result.metadata.fallback_reason == "Qdrant returned no usable hits."
    assert result.metadata.retrieval_quality_tier == "memory_ranked"
    assert any(
        warning == "Qdrant fallback: Qdrant returned no usable hits."
        for warning in result.warnings
    )


def test_query_surfaces_related_claims_and_relationship_warnings(temp_data_dir: Path) -> None:
    claims = populate_query_fixtures(temp_data_dir)
    service = build_query_service(
        temp_data_dir,
        claims,
        relationships=[
            ClaimRelationship(
                relationship_id="rel-1",
                claim_id="claim-probable-1",
                related_claim_id="claim-probable-2",
                relationship_type="contradicts",
                notes="Different object values for the same subject/predicate.",
            ),
            ClaimRelationship(
                relationship_id="rel-2",
                claim_id="claim-probable-2",
                related_claim_id="claim-probable-1",
                relationship_type="contradicts",
                notes="Different object values for the same subject/predicate.",
            ),
        ],
    )

    result = service.answer(
        QueryRequest(
            question="Market gossip about bread prices",
            mode=QueryMode.OPEN_EXPLORATION,
            filters=QueryFilter(status=ClaimStatus.PROBABLE),
        )
    )

    assert result.related_claims
    assert result.claim_clusters
    assert result.claim_clusters[0].cluster_kind == "contested"
    assert any(item.relationship_type == "contradicts" for item in result.related_claims)
    assert "Canonical claims disagree here" in result.answer
    assert any("contradictions" in warning for warning in result.warnings)


def test_query_surfaces_supersession_warning(temp_data_dir: Path) -> None:
    claims = populate_query_fixtures(temp_data_dir)
    service = build_query_service(
        temp_data_dir,
        claims,
        relationships=[
            ClaimRelationship(
                relationship_id="rel-3",
                claim_id="claim-verified-1",
                related_claim_id="claim-probable-1",
                relationship_type="supersedes",
                notes="Newer canonical claim with the same signature.",
            )
        ],
    )

    result = service.answer(
        QueryRequest(
            question="Rouen bread prices",
            mode=QueryMode.STRICT_FACTS,
        )
    )

    assert any(item.relationship_type == "supersedes" for item in result.related_claims)
    assert result.claim_clusters
    assert result.claim_clusters[0].cluster_kind == "supersession"
    assert "current canonical position" in result.answer
    assert any("supersede" in warning for warning in result.warnings)


def test_query_prefers_supported_claims_when_lexical_scores_tie(temp_data_dir: Path) -> None:
    claims = populate_query_fixtures(temp_data_dir)
    service = build_query_service(
        temp_data_dir,
        claims,
        relationships=[
            ClaimRelationship(
                relationship_id="rel-4",
                claim_id="claim-probable-1",
                related_claim_id="claim-verified-1",
                relationship_type="supports",
                source_kind="manual",
                notes="Reviewer confirmed support.",
            ),
            ClaimRelationship(
                relationship_id="rel-5",
                claim_id="claim-probable-2",
                related_claim_id="claim-probable-1",
                relationship_type="contradicts",
                notes="Different place-specific value.",
            ),
        ],
    )

    result = service.answer(
        QueryRequest(
            question="Market gossip about bread prices",
            mode=QueryMode.OPEN_EXPLORATION,
            filters=QueryFilter(status=ClaimStatus.PROBABLE),
        )
    )

    assert result.supporting_claims[0].claim_id == "claim-probable-1"


def test_query_groups_supporting_claims_into_one_reinforcing_cluster(temp_data_dir: Path) -> None:
    claims = populate_query_fixtures(temp_data_dir)
    service = build_query_service(
        temp_data_dir,
        claims,
        relationships=[
            ClaimRelationship(
                relationship_id="rel-support-1",
                claim_id="claim-probable-1",
                related_claim_id="claim-probable-2",
                relationship_type="supports",
                notes="These market-account claims reinforce the same point.",
            ),
            ClaimRelationship(
                relationship_id="rel-support-2",
                claim_id="claim-probable-2",
                related_claim_id="claim-probable-1",
                relationship_type="supports",
                notes="These market-account claims reinforce the same point.",
            ),
        ],
    )

    result = service.answer(
        QueryRequest(
            question="Market gossip about bread prices",
            mode=QueryMode.OPEN_EXPLORATION,
            filters=QueryFilter(status=ClaimStatus.PROBABLE),
        )
    )

    assert len(result.claim_clusters) == 2
    assert all(cluster.cluster_kind == "reinforcing" for cluster in result.claim_clusters)
    assert {cluster.lead_claim_id for cluster in result.claim_clusters} == {
        "claim-probable-1",
        "claim-probable-2",
    }
    assert "Rouen market accounts" in result.answer
    assert "Paris market accounts" in result.answer


def test_query_contradiction_pair_produces_contested_summary_text(temp_data_dir: Path) -> None:
    claims = populate_query_fixtures(temp_data_dir)
    service = build_query_service(
        temp_data_dir,
        claims,
        relationships=[
            ClaimRelationship(
                relationship_id="rel-contradict-1",
                claim_id="claim-probable-1",
                related_claim_id="claim-probable-2",
                relationship_type="contradicts",
            ),
            ClaimRelationship(
                relationship_id="rel-contradict-2",
                claim_id="claim-probable-2",
                related_claim_id="claim-probable-1",
                relationship_type="contradicts",
            ),
        ],
    )

    result = service.answer(
        QueryRequest(
            question="Market gossip about bread prices",
            mode=QueryMode.OPEN_EXPLORATION,
            filters=QueryFilter(status=ClaimStatus.PROBABLE),
        )
    )

    assert result.claim_clusters[0].cluster_kind == "contested"
    assert "Canonical claims disagree here" in result.answer
    assert "Rouen market accounts" in result.answer
    assert "Paris market accounts" in result.answer


def test_query_superseded_claim_does_not_become_cluster_lead(temp_data_dir: Path) -> None:
    claims = populate_query_fixtures(temp_data_dir)
    older_claim = ApprovedClaim(
        claim_id="claim-verified-older",
        subject="Rouen bread prices",
        predicate="rose_during",
        value="autumn shortage",
        claim_kind=ClaimKind.PRACTICE,
        status=ClaimStatus.VERIFIED,
        place="Rouen",
        evidence_ids=["evi-1"],
    )
    service = build_query_service(
        temp_data_dir,
        claims + [older_claim],
        relationships=[
            ClaimRelationship(
                relationship_id="rel-super-1",
                claim_id="claim-verified-1",
                related_claim_id="claim-verified-older",
                relationship_type="supersedes",
            ),
            ClaimRelationship(
                relationship_id="rel-super-2",
                claim_id="claim-verified-older",
                related_claim_id="claim-verified-1",
                relationship_type="superseded_by",
            ),
        ],
    )

    result = service.answer(
        QueryRequest(
            question="Rouen bread prices",
            mode=QueryMode.STRICT_FACTS,
        )
    )

    assert result.claim_clusters[0].cluster_kind == "supersession"
    assert result.claim_clusters[0].lead_claim_id == "claim-verified-1"
    assert "autumn shortage" in result.answer
    assert "winter shortage" in result.answer


def test_query_mixed_cluster_chooses_stable_lead_claim(temp_data_dir: Path) -> None:
    claims = populate_query_fixtures(temp_data_dir)
    service = build_query_service(
        temp_data_dir,
        claims,
        relationships=[
            ClaimRelationship(
                relationship_id="rel-mixed-1",
                claim_id="claim-verified-1",
                related_claim_id="claim-probable-1",
                relationship_type="supports",
                source_kind="manual",
            ),
            ClaimRelationship(
                relationship_id="rel-mixed-2",
                claim_id="claim-probable-1",
                related_claim_id="claim-probable-2",
                relationship_type="contradicts",
            ),
            ClaimRelationship(
                relationship_id="rel-mixed-3",
                claim_id="claim-probable-2",
                related_claim_id="claim-probable-1",
                relationship_type="contradicts",
            ),
        ],
    )

    result = service.answer(
        QueryRequest(
            question="bread",
            mode=QueryMode.STRICT_FACTS,
        )
    )

    contested_cluster = next(
        cluster for cluster in result.claim_clusters if cluster.cluster_kind == "contested"
    )
    assert contested_cluster.lead_claim_id in {"claim-probable-1", "claim-probable-2"}


def test_query_cluster_ids_are_stable_for_same_inputs(temp_data_dir: Path) -> None:
    claims = populate_query_fixtures(temp_data_dir)
    relationships = [
        ClaimRelationship(
            relationship_id="rel-stable-1",
            claim_id="claim-probable-1",
            related_claim_id="claim-probable-2",
            relationship_type="supports",
        ),
        ClaimRelationship(
            relationship_id="rel-stable-2",
            claim_id="claim-probable-2",
            related_claim_id="claim-probable-1",
            relationship_type="supports",
        ),
    ]
    service = build_query_service(
        temp_data_dir,
        claims,
        relationships=relationships,
    )

    first = service.answer(
        QueryRequest(
            question="Market gossip about bread prices",
            mode=QueryMode.OPEN_EXPLORATION,
            filters=QueryFilter(status=ClaimStatus.PROBABLE),
        )
    )
    second = service.answer(
        QueryRequest(
            question="Market gossip about bread prices",
            mode=QueryMode.OPEN_EXPLORATION,
            filters=QueryFilter(status=ClaimStatus.PROBABLE),
        )
    )

    assert [cluster.cluster_id for cluster in first.claim_clusters] == [
        cluster.cluster_id for cluster in second.claim_clusters
    ]


def test_query_can_return_multiple_cluster_sections(temp_data_dir: Path) -> None:
    claims = populate_query_fixtures(temp_data_dir)
    older_claim = ApprovedClaim(
        claim_id="claim-verified-older",
        subject="Rouen bread prices",
        predicate="rose_during",
        value="autumn shortage",
        claim_kind=ClaimKind.PRACTICE,
        status=ClaimStatus.PROBABLE,
        place="Rouen",
        evidence_ids=["evi-1"],
    )
    service = build_query_service(
        temp_data_dir,
        claims + [older_claim],
        relationships=[
            ClaimRelationship(
                relationship_id="rel-multi-1",
                claim_id="claim-verified-1",
                related_claim_id="claim-verified-older",
                relationship_type="supersedes",
            ),
            ClaimRelationship(
                relationship_id="rel-multi-2",
                claim_id="claim-verified-older",
                related_claim_id="claim-verified-1",
                relationship_type="superseded_by",
            ),
            ClaimRelationship(
                relationship_id="rel-multi-3",
                claim_id="claim-probable-1",
                related_claim_id="claim-probable-2",
                relationship_type="contradicts",
            ),
            ClaimRelationship(
                relationship_id="rel-multi-4",
                claim_id="claim-probable-2",
                related_claim_id="claim-probable-1",
                relationship_type="contradicts",
            ),
        ],
    )

    result = service.answer(
        QueryRequest(
            question="bread",
            mode=QueryMode.OPEN_EXPLORATION,
        )
    )

    assert len(result.claim_clusters) >= 2
    assert len(result.answer_sections) >= 2
    assert "\n\n" in result.answer
    assert {"supersession", "contested"} <= {
        cluster.cluster_kind for cluster in result.claim_clusters
    }


def test_query_prefers_supersession_cluster_for_current_canon_questions(
    temp_data_dir: Path,
) -> None:
    claims = populate_query_fixtures(temp_data_dir)
    older_claim = ApprovedClaim(
        claim_id="claim-current-older",
        subject="Rouen bread prices",
        predicate="rose_during",
        value="autumn shortage",
        claim_kind=ClaimKind.PRACTICE,
        status=ClaimStatus.PROBABLE,
        place="Rouen",
        evidence_ids=["evi-1"],
    )
    service = build_query_service(
        temp_data_dir,
        claims + [older_claim],
        relationships=[
            ClaimRelationship(
                relationship_id="rel-current-1",
                claim_id="claim-verified-1",
                related_claim_id="claim-current-older",
                relationship_type="supersedes",
            ),
            ClaimRelationship(
                relationship_id="rel-current-2",
                claim_id="claim-current-older",
                related_claim_id="claim-verified-1",
                relationship_type="superseded_by",
            ),
            ClaimRelationship(
                relationship_id="rel-current-3",
                claim_id="claim-probable-1",
                related_claim_id="claim-probable-2",
                relationship_type="contradicts",
            ),
            ClaimRelationship(
                relationship_id="rel-current-4",
                claim_id="claim-probable-2",
                related_claim_id="claim-probable-1",
                relationship_type="contradicts",
            ),
        ],
    )

    result = service.answer(
        QueryRequest(
            question="What is the current canonical view on bread prices?",
            mode=QueryMode.OPEN_EXPLORATION,
        )
    )

    assert result.answer_sections[0].cluster_kind == "supersession"


def test_query_prefers_contested_cluster_for_disagreement_questions(
    temp_data_dir: Path,
) -> None:
    claims = populate_query_fixtures(temp_data_dir)
    older_claim = ApprovedClaim(
        claim_id="claim-dispute-older",
        subject="Rouen bread prices",
        predicate="rose_during",
        value="autumn shortage",
        claim_kind=ClaimKind.PRACTICE,
        status=ClaimStatus.PROBABLE,
        place="Rouen",
        evidence_ids=["evi-1"],
    )
    service = build_query_service(
        temp_data_dir,
        claims + [older_claim],
        relationships=[
            ClaimRelationship(
                relationship_id="rel-dispute-1",
                claim_id="claim-verified-1",
                related_claim_id="claim-dispute-older",
                relationship_type="supersedes",
            ),
            ClaimRelationship(
                relationship_id="rel-dispute-2",
                claim_id="claim-dispute-older",
                related_claim_id="claim-verified-1",
                relationship_type="superseded_by",
            ),
            ClaimRelationship(
                relationship_id="rel-dispute-3",
                claim_id="claim-probable-1",
                related_claim_id="claim-probable-2",
                relationship_type="contradicts",
            ),
            ClaimRelationship(
                relationship_id="rel-dispute-4",
                claim_id="claim-probable-2",
                related_claim_id="claim-probable-1",
                relationship_type="contradicts",
            ),
        ],
    )

    result = service.answer(
        QueryRequest(
            question="Where do canonical claims disagree about bread?",
            mode=QueryMode.OPEN_EXPLORATION,
        )
    )

    assert result.answer_sections[0].cluster_kind == "contested"


def test_query_cluster_expansion_uses_more_than_top_five_matches(temp_data_dir: Path) -> None:
    JsonListStore(temp_data_dir / "sources.json").write_models(
        [SourceRecord(source_id="src-1", title="Bread notes", source_type="record")]
    )
    JsonListStore(temp_data_dir / "evidence.json").write_models(
        [
            EvidenceSnippet(
                evidence_id="evi-1",
                source_id="src-1",
                locator="folio 1r",
                text="Bread note.",
            )
        ]
    )
    claims = [
        ApprovedClaim(
            claim_id=f"claim-bread-{index}",
            subject="Bread record",
            predicate="appears_in",
            value=f"ledger {index}",
            claim_kind=ClaimKind.PRACTICE,
            status=ClaimStatus.PROBABLE,
            place="Rouen",
            evidence_ids=["evi-1"],
        )
        for index in range(1, 7)
    ]
    explainer_claim = ApprovedClaim(
        claim_id="claim-bread-explainer",
        subject="Bread record",
        predicate="is_contested_by",
        value="a missing ledger",
        claim_kind=ClaimKind.BELIEF,
        status=ClaimStatus.CONTESTED,
        place="Rouen",
        evidence_ids=["evi-1"],
    )
    service = build_query_service(
        temp_data_dir,
        claims + [explainer_claim],
        relationships=[
            ClaimRelationship(
                relationship_id="rel-top-six-1",
                claim_id="claim-bread-6",
                related_claim_id="claim-bread-explainer",
                relationship_type="contradicts",
            ),
            ClaimRelationship(
                relationship_id="rel-top-six-2",
                claim_id="claim-bread-explainer",
                related_claim_id="claim-bread-6",
                relationship_type="contradicts",
            ),
        ],
    )

    result = service.answer(
        QueryRequest(
            question="Bread record",
            mode=QueryMode.OPEN_EXPLORATION,
        )
    )

    assert any("claim-bread-explainer" in cluster.claim_ids for cluster in result.claim_clusters)


def test_query_narrow_topic_prefers_bread_token_claims_and_caps_unrelated_canon(
    temp_data_dir: Path,
) -> None:
    JsonListStore(temp_data_dir / "sources.json").write_models(
        [
            SourceRecord(source_id="src-1", title="Rouen ledger", source_type="record"),
            SourceRecord(source_id="src-2", title="Bakers petition", source_type="petition"),
            SourceRecord(source_id="src-3", title="Gate note", source_type="archive"),
        ]
    )
    JsonListStore(temp_data_dir / "evidence.json").write_models(
        [
            EvidenceSnippet(
                evidence_id="evi-1",
                source_id="src-1",
                locator="folio 1r",
                text="Bread prices rose during the shortage.",
            ),
            EvidenceSnippet(
                evidence_id="evi-2",
                source_id="src-2",
                locator="folio 2r",
                text="Bakers used household bread tokens at the market gate.",
            ),
            EvidenceSnippet(
                evidence_id="evi-3",
                source_id="src-3",
                locator="note 4",
                text="Rouen bakers traded stamped bread scrip before dawn.",
            ),
        ]
    )
    claims = [
        ApprovedClaim(
            claim_id="claim-price",
            subject="Rouen bread prices",
            predicate="rose_during",
            value="the shortage",
            claim_kind=ClaimKind.PRACTICE,
            status=ClaimStatus.VERIFIED,
            place="Rouen",
            evidence_ids=["evi-1"],
        ),
        ApprovedClaim(
            claim_id="claim-token",
            subject="Bakers in Rouen",
            predicate="used",
            value="household bread tokens",
            claim_kind=ClaimKind.PRACTICE,
            status=ClaimStatus.PROBABLE,
            place="Rouen",
            evidence_ids=["evi-2"],
        ),
        ApprovedClaim(
            claim_id="claim-scrip",
            subject="Rouen bakers",
            predicate="handled",
            value="stamped bread scrip before dawn",
            claim_kind=ClaimKind.PRACTICE,
            status=ClaimStatus.PROBABLE,
            place="Rouen",
            evidence_ids=["evi-3"],
        ),
    ]
    service = build_query_service(
        temp_data_dir,
        claims,
        relationships=[
            ClaimRelationship(
                relationship_id="rel-token-scrip",
                claim_id="claim-token",
                related_claim_id="claim-scrip",
                relationship_type="supports",
            ),
            ClaimRelationship(
                relationship_id="rel-scrip-token",
                claim_id="claim-scrip",
                related_claim_id="claim-token",
                relationship_type="supports",
            ),
        ],
    )

    result = service.answer(
        QueryRequest(
            question="How were bread tokens handled in Rouen?",
            mode=QueryMode.OPEN_EXPLORATION,
        )
    )

    returned_ids = [claim.claim_id for claim in result.supporting_claims]
    assert returned_ids[:2] == ["claim-token", "claim-scrip"]
    assert "claim-price" not in returned_ids
    assert result.metadata.answer_boundary == "direct_answer"
    assert result.direct_match_claim_ids[:1] == ["claim-token"]
    assert "claim-scrip" in result.adjacent_context_claim_ids
    assert [claim.claim_id for claim in result.nearby_claims] == ["claim-scrip"]
    assert all("claim-price" not in section.claim_ids for section in result.answer_sections)
    assert result.claim_clusters[0].lead_claim_id == "claim-token"


def test_query_rumor_question_stays_centered_on_rumor_material(
    temp_data_dir: Path,
) -> None:
    JsonListStore(temp_data_dir / "sources.json").write_models(
        [
            SourceRecord(source_id="src-1", title="Rumor chronicle", source_type="chronicle"),
            SourceRecord(source_id="src-2", title="Verified ledger", source_type="record"),
        ]
    )
    JsonListStore(temp_data_dir / "evidence.json").write_models(
        [
            EvidenceSnippet(
                evidence_id="evi-1",
                source_id="src-1",
                locator="chapter 2",
                text="Citizens whispered that merchants hid grain in shuttered lofts.",
            ),
            EvidenceSnippet(
                evidence_id="evi-2",
                source_id="src-1",
                locator="chapter 3",
                text="Some witnesses disputed whether the hoarding story was true.",
            ),
            EvidenceSnippet(
                evidence_id="evi-3",
                source_id="src-2",
                locator="folio 8r",
                text="Bread prices still rose in Rouen.",
            ),
        ]
    )
    claims = [
        ApprovedClaim(
            claim_id="claim-rumor",
            subject="Citizens in Rouen",
            predicate="rumored_that",
            value="merchants hid grain in shuttered lofts",
            claim_kind=ClaimKind.BELIEF,
            status=ClaimStatus.RUMOR,
            place="Rouen",
            viewpoint_scope="citizens",
            evidence_ids=["evi-1"],
        ),
        ApprovedClaim(
            claim_id="claim-contested",
            subject="Witnesses in Rouen",
            predicate="disputed",
            value="the grain-hoarding story",
            claim_kind=ClaimKind.BELIEF,
            status=ClaimStatus.CONTESTED,
            place="Rouen",
            viewpoint_scope="witnesses",
            evidence_ids=["evi-2"],
        ),
        ApprovedClaim(
            claim_id="claim-price",
            subject="Rouen bread prices",
            predicate="rose_during",
            value="the shortage",
            claim_kind=ClaimKind.PRACTICE,
            status=ClaimStatus.VERIFIED,
            place="Rouen",
            evidence_ids=["evi-3"],
        ),
    ]
    service = build_query_service(
        temp_data_dir,
        claims,
        relationships=[
            ClaimRelationship(
                relationship_id="rel-rumor-contested",
                claim_id="claim-rumor",
                related_claim_id="claim-contested",
                relationship_type="supports",
            )
        ],
    )

    result = service.answer(
        QueryRequest(
            question="Which Rouen claims are still contested or rumor about grain hoarding?",
            mode=QueryMode.OPEN_EXPLORATION,
        )
    )

    returned_ids = {claim.claim_id for claim in result.supporting_claims}
    assert {"claim-rumor", "claim-contested"} <= returned_ids
    assert "claim-price" not in returned_ids
    assert result.metadata.answer_boundary in {"direct_answer", "adjacent_context"}
    assert result.metadata.used_nearby_context is True
    assert "grain-hoarding" in result.answer or "shuttered lofts" in result.answer
