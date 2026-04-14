from __future__ import annotations

from pathlib import Path

from source_aware_worldbuilding.adapters.file_backed import (
    FileEvidenceStore,
    FileSourceStore,
    FileTruthStore,
)
from source_aware_worldbuilding.domain.enums import ClaimKind, ClaimStatus
from source_aware_worldbuilding.domain.models import (
    ApprovedClaim,
    EvidenceSnippet,
    LorePacketRequest,
    QueryFilter,
    SourceRecord,
)
from source_aware_worldbuilding.services.lore_packet import LorePacketService
from source_aware_worldbuilding.storage.json_store import JsonListStore


def populate_lore_packet_fixtures(data_dir: Path) -> None:
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
                notes="Contradictory retelling",
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
                viewpoint_scope="tavern patrons",
                evidence_ids=["evi-4"],
            ),
            ApprovedClaim(
                claim_id="claim-5",
                subject="Greyport docks",
                predicate="should_be_depicted_as",
                value="crowded and wind-cut",
                claim_kind=ClaimKind.PLACE,
                status=ClaimStatus.AUTHOR_CHOICE,
                notes="Preferred atmosphere for future scenes.",
            ),
        ]
    )


def build_service(data_dir: Path) -> LorePacketService:
    return LorePacketService(
        truth_store=FileTruthStore(data_dir),
        evidence_store=FileEvidenceStore(data_dir),
        source_store=FileSourceStore(data_dir),
    )


def test_lore_packet_exports_markdown_files_with_expected_grouping(temp_data_dir: Path) -> None:
    populate_lore_packet_fixtures(temp_data_dir)
    service = build_service(temp_data_dir)

    packet = service.export(LorePacketRequest(project_name="greyport"))
    files = {item.filename: item for item in packet.files}

    assert set(files) == {"basic-lore.md", "characters.md", "timeline.md", "notes.md"}
    assert packet.metadata.claim_count == 4
    assert packet.metadata.source_count == 4
    assert packet.metadata.evidence_count == 4

    assert "# Basic Lore" in files["basic-lore.md"].content
    assert "## People" in files["basic-lore.md"].content
    assert "### Verified" in files["basic-lore.md"].content
    assert "Town Register (folio 2r)" in files["basic-lore.md"].content

    assert "# Characters" in files["characters.md"].content
    assert "## Alys" in files["characters.md"].content
    assert "## Beren" in files["characters.md"].content
    assert "Moon well" not in files["characters.md"].content

    assert "# Timeline" in files["timeline.md"].content
    assert "## Dated Events" in files["timeline.md"].content
    assert "Ember War" in files["timeline.md"].content
    assert "1201" in files["timeline.md"].content

    assert "# Notes" in files["notes.md"].content
    assert "### Rumor" in files["notes.md"].content
    assert "### Author Choices" not in files["notes.md"].content
    assert "## Export Warnings" in files["notes.md"].content
    assert "contradictory" in files["notes.md"].content.lower()


def test_lore_packet_respects_filters_and_subset_requests(temp_data_dir: Path) -> None:
    populate_lore_packet_fixtures(temp_data_dir)
    service = build_service(temp_data_dir)

    packet = service.export(
        LorePacketRequest(
            project_name="greyport",
            files=["characters.md", "notes.md"],
            filters=QueryFilter(place="Greyport"),
            include_statuses=[ClaimStatus.VERIFIED, ClaimStatus.PROBABLE],
        )
    )

    files = {item.filename: item for item in packet.files}
    assert set(files) == {"characters.md", "notes.md"}
    assert packet.metadata.claim_count == 2
    assert "Moon well" not in files["notes.md"].content
    assert "Author Choices" not in files["notes.md"].content
    assert "## Alys" in files["characters.md"].content
    assert "## Beren" in files["characters.md"].content


def test_lore_packet_returns_placeholders_when_focus_matches_nothing(temp_data_dir: Path) -> None:
    populate_lore_packet_fixtures(temp_data_dir)
    service = build_service(temp_data_dir)

    packet = service.export(LorePacketRequest(project_name="greyport", focus="sunken archive"))

    assert packet.warnings == ["No approved claims matched the export request."]
    assert packet.metadata.claim_count == 0
    for exported in packet.files:
        assert "No approved claims matched this export." in exported.content
        assert exported.claim_ids == []
