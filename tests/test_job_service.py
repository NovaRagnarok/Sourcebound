from __future__ import annotations

import threading
import time
from pathlib import Path

from source_aware_worldbuilding.adapters.file_backed import (
    FileBibleProjectProfileStore,
    FileBibleSectionStore,
    FileCandidateStore,
    FileEvidenceStore,
    FileExtractionRunStore,
    FileJobStore,
    FileResearchFindingStore,
    FileResearchProgramStore,
    FileResearchRunStore,
    FileSourceDocumentStore,
    FileSourceStore,
    FileTextUnitStore,
    FileTruthStore,
)
from source_aware_worldbuilding.adapters.qdrant_adapter import QdrantResearchSemanticAdapter
from source_aware_worldbuilding.adapters.web_research_scout import (
    CuratedInputsResearchScout,
    ResearchScoutRegistry,
)
from source_aware_worldbuilding.domain.enums import BibleSectionType, JobStatus
from source_aware_worldbuilding.domain.models import (
    ApprovedClaim,
    BibleProjectProfileUpdateRequest,
    BibleSectionCreateRequest,
    ClaimKind,
    ClaimRelationship,
    ClaimStatus,
    EvidenceSnippet,
    ResearchBrief,
    ResearchCuratedInput,
    ResearchExecutionPolicy,
    ResearchRunRequest,
    SourceRecord,
)
from source_aware_worldbuilding.services.bible import BibleWorkspaceService
from source_aware_worldbuilding.services.ingestion import IngestionService
from source_aware_worldbuilding.services.jobs import JobService
from source_aware_worldbuilding.services.normalization import NormalizationService
from source_aware_worldbuilding.services.research import ResearchService
from source_aware_worldbuilding.storage.json_store import JsonListStore


class NoopCorpus:
    def pull_sources(self):  # pragma: no cover - not used
        return []

    def discover_source_documents(self, sources):  # pragma: no cover - not used
        _ = sources
        return []

    def pull_text_units(self, sources):  # pragma: no cover - not used
        _ = sources
        return []

    def pull_sources_by_item_keys(self, item_keys):  # pragma: no cover - not used
        _ = item_keys
        return []


class NoopExtractor:
    def extract_candidates(self, run, sources, text_units):  # pragma: no cover - not used
        _ = run, sources, text_units
        raise AssertionError("Extraction should not be invoked in this test.")


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


def build_bible_service(data_dir: Path) -> BibleWorkspaceService:
    return BibleWorkspaceService(
        profile_store=FileBibleProjectProfileStore(data_dir),
        section_store=FileBibleSectionStore(data_dir),
        truth_store=FileTruthStore(data_dir),
        evidence_store=FileEvidenceStore(data_dir),
        source_store=FileSourceStore(data_dir),
    )


def build_research_service(data_dir: Path) -> ResearchService:
    return ResearchService(
        scout_registry=ResearchScoutRegistry(
            [CuratedInputsResearchScout(user_agent="test-agent")],
            default_adapter_id="curated_inputs",
        ),
        run_store=FileResearchRunStore(data_dir),
        finding_store=FileResearchFindingStore(data_dir),
        program_store=FileResearchProgramStore(data_dir),
        source_store=FileSourceStore(data_dir),
        source_document_store=FileSourceDocumentStore(data_dir),
        normalization_service=NormalizationService(
            source_document_store=FileSourceDocumentStore(data_dir),
            text_unit_store=FileTextUnitStore(data_dir),
        ),
        ingestion_service=IngestionService(
            corpus=NoopCorpus(),
            extractor=NoopExtractor(),
            source_store=FileSourceStore(data_dir),
            text_unit_store=FileTextUnitStore(data_dir),
            source_document_store=FileSourceDocumentStore(data_dir),
            run_store=FileExtractionRunStore(data_dir),
            candidate_store=FileCandidateStore(data_dir),
            evidence_store=FileEvidenceStore(data_dir),
        ),
        research_semantic=QdrantResearchSemanticAdapter(),
        default_program_markdown="default",
        default_execution_policy=ResearchExecutionPolicy(),
        default_adapter_id="curated_inputs",
        research_user_agent="test-agent",
        semantic_duplicate_threshold=0.9,
        semantic_novelty_floor=0.1,
        semantic_rerank_weight=0.05,
    )


def test_job_service_processes_bible_compose_jobs(temp_data_dir: Path) -> None:
    populate_bible_fixtures(temp_data_dir)
    bible_service = build_bible_service(temp_data_dir)
    bible_service.save_profile(
        "project-greyport",
        BibleProjectProfileUpdateRequest(project_name="Greyport Bible", geography="Greyport"),
    )
    job_service = JobService(
        job_store=FileJobStore(temp_data_dir),
        research_service=build_research_service(temp_data_dir),
        bible_service=bible_service,
    )

    job = job_service.enqueue_bible_compose(
        BibleSectionCreateRequest(
            project_id="project-greyport",
            section_type=BibleSectionType.SETTING_OVERVIEW,
        )
    )
    processed = job_service.process_pending_jobs()
    completed = job_service.get_job(job.job_id)
    section = bible_service.get_section(job.result_ref.section_id or "")

    assert processed is True
    assert completed is not None
    assert completed.status.value == "completed"
    assert section is not None
    assert section.generated_markdown
    assert section.paragraphs


def test_job_service_processes_curated_research_jobs(temp_data_dir: Path) -> None:
    job_service = JobService(
        job_store=FileJobStore(temp_data_dir),
        research_service=build_research_service(temp_data_dir),
        bible_service=build_bible_service(temp_data_dir),
    )

    job = job_service.enqueue_research_run(
        ResearchRunRequest(
            brief=ResearchBrief(
                topic="2003 DJ scene",
                adapter_id="curated_inputs",
                curated_inputs=[
                    ResearchCuratedInput(
                        input_type="text",
                        title="Flyer archive note",
                        text=(
                            "Weekly residencies, vinyl crates, and venue habits "
                            "defined the local scene."
                        ),
                    )
                ],
            )
        )
    )
    processed = job_service.process_pending_jobs()
    completed = job_service.get_job(job.job_id)

    assert processed is True
    assert completed is not None
    assert completed.status.value == "completed"
    assert completed.result_ref.run_id is not None


def test_job_service_can_cancel_queued_jobs_and_export_in_background(temp_data_dir: Path) -> None:
    populate_bible_fixtures(temp_data_dir)
    bible_service = build_bible_service(temp_data_dir)
    bible_service.save_profile(
        "project-greyport",
        BibleProjectProfileUpdateRequest(project_name="Greyport Bible", geography="Greyport"),
    )
    job_service = JobService(
        job_store=FileJobStore(temp_data_dir),
        research_service=build_research_service(temp_data_dir),
        bible_service=bible_service,
    )

    queued_export = job_service.enqueue_bible_export("project-greyport")
    cancelled = job_service.cancel_job(queued_export.job_id)
    export_job = job_service.enqueue_bible_export("project-greyport")
    processed = job_service.process_pending_jobs()
    completed = job_service.get_job(export_job.job_id)

    assert cancelled.status.value == "cancelled"
    assert cancelled.status_label == "cancelled"
    assert processed is True
    assert completed is not None
    assert completed.status.value == "completed"
    assert completed.result_payload is not None
    assert completed.result_payload["profile"]["project_name"] == "Greyport Bible"


def test_job_service_retries_failed_jobs_by_creating_a_new_attempt(temp_data_dir: Path) -> None:
    populate_bible_fixtures(temp_data_dir)
    bible_service = build_bible_service(temp_data_dir)
    bible_service.save_profile(
        "project-greyport",
        BibleProjectProfileUpdateRequest(project_name="Greyport Bible", geography="Greyport"),
    )
    job_store = FileJobStore(temp_data_dir)
    job_service = JobService(
        job_store=job_store,
        research_service=build_research_service(temp_data_dir),
        bible_service=bible_service,
    )
    failed_job = job_service.enqueue_bible_export("project-greyport")
    failed_job.status = JobStatus.FAILED
    failed_job.status_label = "failed"
    failed_job.error = "synthetic failure"
    failed_job.error_detail = "synthetic failure"
    failed_job.retryable = True
    job_store.update_job(failed_job)

    retry = job_service.retry_job(failed_job.job_id)

    assert retry.job_id != failed_job.job_id
    assert retry.retry_of_job_id == failed_job.job_id
    assert retry.attempt_count == failed_job.attempt_count + 1
    assert retry.status_label == "queued"


def test_job_service_can_cancel_running_jobs_at_checkpoints(temp_data_dir: Path) -> None:
    class SlowResearchService:
        def prepare_run(self, request):
            _ = request
            return type("PreparedRun", (), {"run_id": "run-slow"})()

        def execute_run(self, run_id, *, checkpoint=None):
            _ = run_id
            for _ in range(8):
                if checkpoint is not None:
                    checkpoint()
                time.sleep(0.01)
            return None

        def get_run_detail(self, run_id):
            _ = run_id
            return None

    populate_bible_fixtures(temp_data_dir)
    job_store = FileJobStore(temp_data_dir)
    job_service = JobService(
        job_store=job_store,
        research_service=SlowResearchService(),
        bible_service=build_bible_service(temp_data_dir),
    )

    job = job_service.enqueue_research_run(
        ResearchRunRequest(
            brief=ResearchBrief(
                topic="slow cancellation test",
                adapter_id="curated_inputs",
            )
        )
    )

    worker = threading.Thread(target=job_service.process_pending_jobs, daemon=True)
    worker.start()
    time.sleep(0.02)
    cancelled = job_service.cancel_job(job.job_id)
    worker.join(timeout=2.0)
    finished = job_service.get_job(job.job_id)

    assert cancelled.cancel_requested_at is not None
    assert finished is not None
    assert finished.status.value == "cancelled"
    assert finished.status_label == "cancelled"
