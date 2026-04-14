from __future__ import annotations

from pathlib import Path

from source_aware_worldbuilding.domain.models import (
    BibleProjectProfile,
    BibleSection,
    CandidateClaim,
    EvidenceSnippet,
    ExtractionRun,
    JobRecord,
    ResearchFinding,
    ResearchProgram,
    ResearchRun,
    ReviewEvent,
    SourceDocumentRecord,
    SourceRecord,
    TextUnit,
)
from source_aware_worldbuilding.storage.sqlite_app_state import SqliteAppStateStore


class _SqliteAdapterBase:
    def __init__(self, path: Path):
        self.store = SqliteAppStateStore(path)


class SqliteSourceStore(_SqliteAdapterBase):
    def list_sources(self) -> list[SourceRecord]:
        return self.store.list_models("sources", SourceRecord, order_by="source_id")

    def get_source(self, source_id: str) -> SourceRecord | None:
        return self.store.get_model("sources", "source_id", source_id, SourceRecord)

    def save_sources(self, sources: list[SourceRecord]) -> None:
        self.store.upsert_models("sources", "source_id", sources)


class SqliteTextUnitStore(_SqliteAdapterBase):
    def list_text_units(self, source_id: str | None = None) -> list[TextUnit]:
        where = ("source_id", source_id) if source_id else None
        return self.store.list_models(
            "text_units",
            TextUnit,
            order_by="source_id, ordinal",
            where=where,
        )

    def save_text_units(self, text_units: list[TextUnit]) -> None:
        self.store.upsert_models(
            "text_units",
            "text_unit_id",
            text_units,
            extra_columns={"source_id": "source_id", "ordinal": "ordinal"},
        )


class SqliteSourceDocumentStore(_SqliteAdapterBase):
    def list_source_documents(
        self,
        source_id: str | None = None,
        *,
        ingest_status: str | None = None,
        raw_text_status: str | None = None,
        claim_extraction_status: str | None = None,
    ) -> list[SourceDocumentRecord]:
        documents = self.store.list_models(
            "source_documents_state",
            SourceDocumentRecord,
            order_by="source_id, document_id",
        )
        if source_id is not None:
            documents = [item for item in documents if item.source_id == source_id]
        if ingest_status is not None:
            documents = [item for item in documents if item.ingest_status == ingest_status]
        if raw_text_status is not None:
            documents = [item for item in documents if item.raw_text_status == raw_text_status]
        if claim_extraction_status is not None:
            documents = [
                item
                for item in documents
                if item.claim_extraction_status == claim_extraction_status
            ]
        return documents

    def get_source_document(self, document_id: str) -> SourceDocumentRecord | None:
        return self.store.get_model(
            "source_documents_state",
            "document_id",
            document_id,
            SourceDocumentRecord,
        )

    def save_source_documents(self, source_documents: list[SourceDocumentRecord]) -> None:
        self.store.upsert_models(
            "source_documents_state",
            "document_id",
            source_documents,
            extra_columns={
                "source_id": "source_id",
                "ingest_status": "ingest_status",
                "raw_text_status": "raw_text_status",
                "claim_extraction_status": "claim_extraction_status",
            },
        )

    def update_source_document(self, source_document: SourceDocumentRecord) -> None:
        self.save_source_documents([source_document])


class SqliteExtractionRunStore(_SqliteAdapterBase):
    def list_runs(self) -> list[ExtractionRun]:
        return self.store.list_models("extraction_runs", ExtractionRun, order_by="started_at DESC")

    def get_run(self, run_id: str) -> ExtractionRun | None:
        return self.store.get_model("extraction_runs", "run_id", run_id, ExtractionRun)

    def save_run(self, run: ExtractionRun) -> None:
        self.store.upsert_models(
            "extraction_runs",
            "run_id",
            [run],
            extra_columns={"started_at": "started_at"},
        )

    def update_run(self, run: ExtractionRun) -> None:
        self.save_run(run)


class SqliteCandidateStore(_SqliteAdapterBase):
    def list_candidates(self, review_state: str | None = None) -> list[CandidateClaim]:
        where = ("review_state", review_state) if review_state else None
        return self.store.list_models(
            "candidates",
            CandidateClaim,
            order_by="candidate_id",
            where=where,
        )

    def get_candidate(self, candidate_id: str) -> CandidateClaim | None:
        return self.store.get_model("candidates", "candidate_id", candidate_id, CandidateClaim)

    def save_candidates(self, candidates: list[CandidateClaim]) -> None:
        self.store.upsert_models(
            "candidates",
            "candidate_id",
            candidates,
            extra_columns={"review_state": "review_state", "extractor_run_id": "extractor_run_id"},
        )

    def update_candidate(self, candidate: CandidateClaim) -> None:
        self.save_candidates([candidate])


class SqliteEvidenceStore(_SqliteAdapterBase):
    def list_evidence(self, source_id: str | None = None) -> list[EvidenceSnippet]:
        where = ("source_id", source_id) if source_id else None
        return self.store.list_models(
            "evidence",
            EvidenceSnippet,
            order_by="evidence_id",
            where=where,
        )

    def get_evidence(self, evidence_id: str) -> EvidenceSnippet | None:
        return self.store.get_model("evidence", "evidence_id", evidence_id, EvidenceSnippet)

    def save_evidence(self, evidence: list[EvidenceSnippet]) -> None:
        self.store.upsert_models(
            "evidence",
            "evidence_id",
            evidence,
            extra_columns={"source_id": "source_id"},
        )


class SqliteReviewStore(_SqliteAdapterBase):
    def list_reviews(self, candidate_id: str | None = None) -> list[ReviewEvent]:
        where = ("candidate_id", candidate_id) if candidate_id else None
        return self.store.list_models(
            "review_events",
            ReviewEvent,
            order_by="reviewed_at DESC",
            where=where,
        )

    def save_review(self, review: ReviewEvent) -> None:
        self.store.upsert_models(
            "review_events",
            "review_id",
            [review],
            extra_columns={"candidate_id": "candidate_id", "reviewed_at": "reviewed_at"},
        )


class SqliteResearchRunStore(_SqliteAdapterBase):
    def list_runs(self) -> list[ResearchRun]:
        return self.store.list_models("research_runs", ResearchRun, order_by="started_at DESC")

    def get_run(self, run_id: str) -> ResearchRun | None:
        return self.store.get_model("research_runs", "run_id", run_id, ResearchRun)

    def save_run(self, run: ResearchRun) -> None:
        self.store.upsert_models(
            "research_runs",
            "run_id",
            [run],
            extra_columns={
                "started_at": "started_at",
                "status": "status",
                "program_id": "program_id",
            },
        )

    def update_run(self, run: ResearchRun) -> None:
        self.save_run(run)


class SqliteResearchFindingStore(_SqliteAdapterBase):
    def list_findings(self, run_id: str | None = None) -> list[ResearchFinding]:
        where = ("run_id", run_id) if run_id else None
        return self.store.list_models(
            "research_findings",
            ResearchFinding,
            order_by="run_id, facet_id, score DESC",
            where=where,
        )

    def save_findings(self, findings: list[ResearchFinding]) -> None:
        self.store.upsert_models(
            "research_findings",
            "finding_id",
            findings,
            extra_columns={
                "run_id": "run_id",
                "facet_id": "facet_id",
                "decision": "decision",
                "score": "score",
            },
        )

    def update_finding(self, finding: ResearchFinding) -> None:
        self.save_findings([finding])


class SqliteResearchProgramStore(_SqliteAdapterBase):
    def list_programs(self) -> list[ResearchProgram]:
        return self.store.list_models(
            "research_programs",
            ResearchProgram,
            order_by="updated_at DESC",
        )

    def get_program(self, program_id: str) -> ResearchProgram | None:
        return self.store.get_model("research_programs", "program_id", program_id, ResearchProgram)

    def save_program(self, program: ResearchProgram) -> None:
        self.store.upsert_models(
            "research_programs",
            "program_id",
            [program],
            extra_columns={"updated_at": "updated_at", "built_in": "built_in"},
        )


class SqliteJobStore(_SqliteAdapterBase):
    def list_jobs(self, *, status: str | None = None) -> list[JobRecord]:
        where = ("status", status) if status else None
        return self.store.list_models(
            "jobs",
            JobRecord,
            order_by="created_at DESC",
            where=where,
        )

    def get_job(self, job_id: str) -> JobRecord | None:
        return self.store.get_model("jobs", "job_id", job_id, JobRecord)

    def save_job(self, job: JobRecord) -> None:
        self.store.upsert_models(
            "jobs",
            "job_id",
            [job],
            extra_columns={
                "status": "status",
                "job_type": "job_type",
                "created_at": "created_at",
            },
        )

    def update_job(self, job: JobRecord) -> None:
        self.save_job(job)


class SqliteBibleProjectProfileStore(_SqliteAdapterBase):
    def list_profiles(self) -> list[BibleProjectProfile]:
        return self.store.list_models(
            "bible_project_profiles",
            BibleProjectProfile,
            order_by="updated_at DESC",
        )

    def get_profile(self, project_id: str) -> BibleProjectProfile | None:
        return self.store.get_model(
            "bible_project_profiles",
            "project_id",
            project_id,
            BibleProjectProfile,
        )

    def save_profile(self, profile: BibleProjectProfile) -> None:
        self.store.upsert_models(
            "bible_project_profiles",
            "project_id",
            [profile],
            extra_columns={"updated_at": "updated_at", "project_name": "project_name"},
        )


class SqliteBibleSectionStore(_SqliteAdapterBase):
    def list_sections(self, project_id: str | None = None) -> list[BibleSection]:
        where = ("project_id", project_id) if project_id else None
        return self.store.list_models(
            "bible_sections",
            BibleSection,
            order_by="updated_at DESC",
            where=where,
        )

    def get_section(self, section_id: str) -> BibleSection | None:
        return self.store.get_model("bible_sections", "section_id", section_id, BibleSection)

    def save_section(self, section: BibleSection) -> None:
        self.store.upsert_models(
            "bible_sections",
            "section_id",
            [section],
            extra_columns={
                "project_id": "project_id",
                "section_type": "section_type",
                "updated_at": "updated_at",
            },
        )
