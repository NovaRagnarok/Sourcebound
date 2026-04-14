from __future__ import annotations

from pathlib import Path

from source_aware_worldbuilding.adapters.file_backed import (
    FileBibleProjectProfileStore,
    FileBibleSectionStore,
    FileEvidenceStore,
    FileSourceStore,
    FileTruthStore,
)
from source_aware_worldbuilding.domain.enums import (
    BibleSectionType,
    BibleTone,
    ClaimKind,
    ClaimStatus,
)
from source_aware_worldbuilding.domain.models import (
    ApprovedClaim,
    BibleCompositionDefaults,
    BibleProjectProfileUpdateRequest,
    BibleSectionCreateRequest,
    BibleSectionFilters,
    BibleSectionRegenerateRequest,
    BibleSectionUpdateRequest,
    ClaimRelationship,
    EvidenceSnippet,
    SourceRecord,
)
from source_aware_worldbuilding.services.bible import BibleWorkspaceService
from source_aware_worldbuilding.storage.json_store import JsonListStore


def populate_bible_fixtures(data_dir: Path) -> None:
    JsonListStore(data_dir / "sources.json").write_models(
        [
            SourceRecord(source_id="src-1", title="Town Register", source_type="record"),
            SourceRecord(source_id="src-2", title="Market Chronicle", source_type="chronicle"),
            SourceRecord(source_id="src-3", title="Dockside Rumors", source_type="oral_history"),
        ]
    )
    JsonListStore(data_dir / "evidence.json").write_models(
        [
            EvidenceSnippet(
                evidence_id="evi-1",
                source_id="src-1",
                locator="folio 2r",
                text="Greyport walls were repaired in 1201.",
            ),
            EvidenceSnippet(
                evidence_id="evi-2",
                source_id="src-2",
                locator="chapter 4",
                text="The council taxed salt and market bread.",
            ),
            EvidenceSnippet(
                evidence_id="evi-3",
                source_id="src-3",
                locator="entry 9",
                text="Patrons whispered the moon well sang to sailors.",
            ),
        ]
    )
    JsonListStore(data_dir / "claims.json").write_models(
        [
            ApprovedClaim(
                claim_id="claim-1",
                subject="Greyport",
                predicate="has_feature",
                value="stone walls",
                claim_kind=ClaimKind.PLACE,
                status=ClaimStatus.VERIFIED,
                place="Greyport",
                time_start="1201",
                evidence_ids=["evi-1"],
            ),
            ApprovedClaim(
                claim_id="claim-2",
                subject="Greyport council",
                predicate="taxes",
                value="salt and bread",
                claim_kind=ClaimKind.INSTITUTION,
                status=ClaimStatus.PROBABLE,
                place="Greyport",
                evidence_ids=["evi-2"],
            ),
            ApprovedClaim(
                claim_id="claim-3",
                subject="Moon well",
                predicate="sings_to",
                value="sailors",
                claim_kind=ClaimKind.BELIEF,
                status=ClaimStatus.RUMOR,
                place="Greyport",
                evidence_ids=["evi-3"],
            ),
            ApprovedClaim(
                claim_id="claim-4",
                subject="Greyport docks",
                predicate="should_be_depicted_as",
                value="wind-cut and crowded",
                claim_kind=ClaimKind.PLACE,
                status=ClaimStatus.AUTHOR_CHOICE,
                author_choice=True,
                place="Greyport",
            ),
        ]
    )
    JsonListStore(data_dir / "claim_relationships.json").write_models(
        [
            ClaimRelationship(
                relationship_id="rel-1",
                claim_id="claim-2",
                related_claim_id="claim-1",
                relationship_type="contradicts",
                notes="Chronicle dates differ from register.",
            )
        ]
    )


def build_service(data_dir: Path) -> BibleWorkspaceService:
    return BibleWorkspaceService(
        profile_store=FileBibleProjectProfileStore(data_dir),
        section_store=FileBibleSectionStore(data_dir),
        truth_store=FileTruthStore(data_dir),
        evidence_store=FileEvidenceStore(data_dir),
        source_store=FileSourceStore(data_dir),
    )


def test_bible_workspace_creates_sections_with_provenance_and_guidance(temp_data_dir: Path) -> None:
    populate_bible_fixtures(temp_data_dir)
    service = build_service(temp_data_dir)
    profile = service.save_profile(
        "project-greyport",
        BibleProjectProfileUpdateRequest(
            project_name="Greyport Bible",
            geography="Greyport",
            era="1201",
            desired_facets=["places", "institutions", "daily life"],
            tone=BibleTone.GROUNDED_LITERARY,
            composition_defaults=BibleCompositionDefaults(
                include_statuses=[ClaimStatus.VERIFIED, ClaimStatus.PROBABLE]
            ),
        ),
    )

    section = service.create_section(
        BibleSectionCreateRequest(
            project_id=profile.project_id,
            section_type=BibleSectionType.SETTING_OVERVIEW,
            filters=BibleSectionFilters(place="Greyport"),
        )
    )

    assert set(section.references.claim_ids) == {"claim-1"}
    assert section.references.evidence_ids == ["evi-1"]
    assert section.certainty_summary == {"verified": 1}
    assert section.contradiction_flags
    assert "Greyport" in section.content
    assert "Sources:" in section.content
    assert section.paragraphs
    assert section.composition_metrics.target_beats >= 1
    assert section.coverage_analysis.diagnostic_summary
    assert isinstance(section.recommended_next_research, list)


def test_bible_workspace_preserves_manual_edits_when_regenerated(temp_data_dir: Path) -> None:
    populate_bible_fixtures(temp_data_dir)
    service = build_service(temp_data_dir)
    service.save_profile(
        "project-greyport",
        BibleProjectProfileUpdateRequest(project_name="Greyport Bible"),
    )
    section = service.create_section(
        BibleSectionCreateRequest(
            project_id="project-greyport",
            section_type=BibleSectionType.RUMORS_AND_CONTESTED,
        )
    )

    edited = service.update_section(
        section.section_id,
        BibleSectionUpdateRequest(
            title="Rumor Ledger",
            content=section.content + "\n\nAuthor note: keep this eerie but unconfirmed.",
        ),
    )
    regenerated = service.regenerate_section(
        section.section_id,
        BibleSectionRegenerateRequest(
            filters=BibleSectionFilters(statuses=[ClaimStatus.RUMOR, ClaimStatus.LEGEND])
        ),
    )

    assert edited.has_manual_edits is True
    assert regenerated.content.endswith("Author note: keep this eerie but unconfirmed.")
    assert regenerated.generated_markdown != ""
    assert regenerated.manual_markdown is not None


def test_bible_workspace_can_export_saved_project(temp_data_dir: Path) -> None:
    populate_bible_fixtures(temp_data_dir)
    service = build_service(temp_data_dir)
    service.save_profile(
        "project-greyport",
        BibleProjectProfileUpdateRequest(project_name="Greyport Bible"),
    )
    service.create_section(
        BibleSectionCreateRequest(
            project_id="project-greyport",
            section_type=BibleSectionType.AUTHOR_DECISIONS,
        )
    )

    exported = service.export_project("project-greyport")

    assert exported.profile.project_name == "Greyport Bible"
    assert len(exported.sections) == 1
    assert exported.sections[0].section_type == BibleSectionType.AUTHOR_DECISIONS
