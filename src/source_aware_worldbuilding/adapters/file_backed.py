from __future__ import annotations

from pathlib import Path

from source_aware_worldbuilding.domain.models import (
    ApprovedClaim,
    BibleProjectProfile,
    BibleSection,
    CandidateClaim,
    ClaimRelationship,
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
from source_aware_worldbuilding.storage.json_store import JsonListStore


class FileSourceStore:
    def __init__(self, data_dir: Path):
        self.store = JsonListStore(data_dir / "sources.json")

    def list_sources(self) -> list[SourceRecord]:
        return self.store.read_models(SourceRecord)

    def get_source(self, source_id: str) -> SourceRecord | None:
        return next((item for item in self.list_sources() if item.source_id == source_id), None)

    def save_sources(self, sources: list[SourceRecord]) -> None:
        existing = {item.source_id: item for item in self.store.read_models(SourceRecord)}
        for item in sources:
            existing[item.source_id] = item
        self.store.write_models(existing.values())


class FileTextUnitStore:
    def __init__(self, data_dir: Path):
        self.store = JsonListStore(data_dir / "text_units.json")

    def list_text_units(self, source_id: str | None = None) -> list[TextUnit]:
        text_units = self.store.read_models(TextUnit)
        if source_id is None:
            return text_units
        return [item for item in text_units if item.source_id == source_id]

    def save_text_units(self, text_units: list[TextUnit]) -> None:
        existing = {item.text_unit_id: item for item in self.store.read_models(TextUnit)}
        for item in text_units:
            existing[item.text_unit_id] = item
        self.store.write_models(existing.values())


class FileSourceDocumentStore:
    def __init__(self, data_dir: Path):
        self.store = JsonListStore(data_dir / "source_documents.json")

    def list_source_documents(
        self,
        source_id: str | None = None,
        *,
        ingest_status: str | None = None,
        raw_text_status: str | None = None,
        claim_extraction_status: str | None = None,
    ) -> list[SourceDocumentRecord]:
        documents = self.store.read_models(SourceDocumentRecord)
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

    def save_source_documents(self, source_documents: list[SourceDocumentRecord]) -> None:
        existing = {
            item.document_id: item for item in self.store.read_models(SourceDocumentRecord)
        }
        for item in source_documents:
            existing[item.document_id] = item
        self.store.write_models(existing.values())

    def update_source_document(self, source_document: SourceDocumentRecord) -> None:
        self.save_source_documents([source_document])


class FileExtractionRunStore:
    def __init__(self, data_dir: Path):
        self.store = JsonListStore(data_dir / "extraction_runs.json")

    def list_runs(self) -> list[ExtractionRun]:
        return list(reversed(self.store.read_models(ExtractionRun)))

    def get_run(self, run_id: str) -> ExtractionRun | None:
        return next(
            (item for item in self.store.read_models(ExtractionRun) if item.run_id == run_id),
            None,
        )

    def save_run(self, run: ExtractionRun) -> None:
        self.update_run(run)

    def update_run(self, run: ExtractionRun) -> None:
        runs = {item.run_id: item for item in self.store.read_models(ExtractionRun)}
        runs[run.run_id] = run
        self.store.write_models(runs.values())


class FileCandidateStore:
    def __init__(self, data_dir: Path):
        self.store = JsonListStore(data_dir / "candidates.json")

    def list_candidates(self, review_state: str | None = None) -> list[CandidateClaim]:
        candidates = self.store.read_models(CandidateClaim)
        if review_state is None:
            return candidates
        return [item for item in candidates if item.review_state.value == review_state]

    def get_candidate(self, candidate_id: str) -> CandidateClaim | None:
        return next(
            (item for item in self.list_candidates() if item.candidate_id == candidate_id),
            None,
        )

    def save_candidates(self, candidates: list[CandidateClaim]) -> None:
        existing = {item.candidate_id: item for item in self.store.read_models(CandidateClaim)}
        for item in candidates:
            existing[item.candidate_id] = item
        self.store.write_models(existing.values())

    def update_candidate(self, candidate: CandidateClaim) -> None:
        candidates = self.list_candidates()
        updated = []
        found = False
        for current in candidates:
            if current.candidate_id == candidate.candidate_id:
                updated.append(candidate)
                found = True
            else:
                updated.append(current)
        if not found:
            updated.append(candidate)
        self.store.write_models(updated)


class FileEvidenceStore:
    def __init__(self, data_dir: Path):
        self.store = JsonListStore(data_dir / "evidence.json")

    def list_evidence(self, source_id: str | None = None) -> list[EvidenceSnippet]:
        evidence = self.store.read_models(EvidenceSnippet)
        if source_id is None:
            return evidence
        return [item for item in evidence if item.source_id == source_id]

    def get_evidence(self, evidence_id: str) -> EvidenceSnippet | None:
        return next(
            (item for item in self.list_evidence() if item.evidence_id == evidence_id),
            None,
        )

    def save_evidence(self, evidence: list[EvidenceSnippet]) -> None:
        existing = {item.evidence_id: item for item in self.store.read_models(EvidenceSnippet)}
        for item in evidence:
            existing[item.evidence_id] = item
        self.store.write_models(existing.values())


class FileReviewStore:
    def __init__(self, data_dir: Path):
        self.store = JsonListStore(data_dir / "review_events.json")

    def list_reviews(self, candidate_id: str | None = None) -> list[ReviewEvent]:
        reviews = list(reversed(self.store.read_models(ReviewEvent)))
        if candidate_id is None:
            return reviews
        return [item for item in reviews if item.candidate_id == candidate_id]

    def save_review(self, review: ReviewEvent) -> None:
        existing = {item.review_id: item for item in self.store.read_models(ReviewEvent)}
        existing[review.review_id] = review
        self.store.write_models(existing.values())


class FileResearchRunStore:
    def __init__(self, data_dir: Path):
        self.store = JsonListStore(data_dir / "research_runs.json")

    def list_runs(self) -> list[ResearchRun]:
        return list(reversed(self.store.read_models(ResearchRun)))

    def get_run(self, run_id: str) -> ResearchRun | None:
        return next((item for item in self.store.read_models(ResearchRun) if item.run_id == run_id), None)

    def save_run(self, run: ResearchRun) -> None:
        self.update_run(run)

    def update_run(self, run: ResearchRun) -> None:
        existing = {item.run_id: item for item in self.store.read_models(ResearchRun)}
        existing[run.run_id] = run
        self.store.write_models(existing.values())


class FileResearchFindingStore:
    def __init__(self, data_dir: Path):
        self.store = JsonListStore(data_dir / "research_findings.json")

    def list_findings(self, run_id: str | None = None) -> list[ResearchFinding]:
        findings = self.store.read_models(ResearchFinding)
        if run_id is None:
            return findings
        return [item for item in findings if item.run_id == run_id]

    def save_findings(self, findings: list[ResearchFinding]) -> None:
        existing = {item.finding_id: item for item in self.store.read_models(ResearchFinding)}
        for item in findings:
            existing[item.finding_id] = item
        self.store.write_models(existing.values())

    def update_finding(self, finding: ResearchFinding) -> None:
        self.save_findings([finding])


class FileResearchProgramStore:
    def __init__(self, data_dir: Path):
        self.store = JsonListStore(data_dir / "research_programs.json")

    def list_programs(self) -> list[ResearchProgram]:
        return self.store.read_models(ResearchProgram)

    def get_program(self, program_id: str) -> ResearchProgram | None:
        return next(
            (item for item in self.store.read_models(ResearchProgram) if item.program_id == program_id),
            None,
        )

    def save_program(self, program: ResearchProgram) -> None:
        existing = {item.program_id: item for item in self.store.read_models(ResearchProgram)}
        existing[program.program_id] = program
        self.store.write_models(existing.values())


class FileJobStore:
    def __init__(self, data_dir: Path):
        self.store = JsonListStore(data_dir / "jobs.json")

    def list_jobs(self, *, status: str | None = None) -> list[JobRecord]:
        jobs = list(reversed(self.store.read_models(JobRecord)))
        if status is None:
            return jobs
        return [item for item in jobs if item.status.value == status]

    def get_job(self, job_id: str) -> JobRecord | None:
        return next((item for item in self.store.read_models(JobRecord) if item.job_id == job_id), None)

    def save_job(self, job: JobRecord) -> None:
        self.update_job(job)

    def update_job(self, job: JobRecord) -> None:
        existing = {item.job_id: item for item in self.store.read_models(JobRecord)}
        existing[job.job_id] = job
        self.store.write_models(existing.values())


class FileBibleProjectProfileStore:
    def __init__(self, data_dir: Path):
        self.store = JsonListStore(data_dir / "bible_project_profiles.json")

    def list_profiles(self) -> list[BibleProjectProfile]:
        return self.store.read_models(BibleProjectProfile)

    def get_profile(self, project_id: str) -> BibleProjectProfile | None:
        return next(
            (item for item in self.store.read_models(BibleProjectProfile) if item.project_id == project_id),
            None,
        )

    def save_profile(self, profile: BibleProjectProfile) -> None:
        existing = {item.project_id: item for item in self.store.read_models(BibleProjectProfile)}
        existing[profile.project_id] = profile
        self.store.write_models(existing.values())


class FileBibleSectionStore:
    def __init__(self, data_dir: Path):
        self.store = JsonListStore(data_dir / "bible_sections.json")

    def list_sections(self, project_id: str | None = None) -> list[BibleSection]:
        sections = self.store.read_models(BibleSection)
        if project_id is None:
            return sections
        return [item for item in sections if item.project_id == project_id]

    def get_section(self, section_id: str) -> BibleSection | None:
        return next((item for item in self.store.read_models(BibleSection) if item.section_id == section_id), None)

    def save_section(self, section: BibleSection) -> None:
        existing = {item.section_id: item for item in self.store.read_models(BibleSection)}
        existing[section.section_id] = section
        self.store.write_models(existing.values())


class FileTruthStore:
    def __init__(self, data_dir: Path):
        self.store = JsonListStore(data_dir / "claims.json")
        self.relationship_store = JsonListStore(data_dir / "claim_relationships.json")

    def list_claims(self) -> list[ApprovedClaim]:
        return self.store.read_models(ApprovedClaim)

    def get_claim(self, claim_id: str) -> ApprovedClaim | None:
        return next((item for item in self.list_claims() if item.claim_id == claim_id), None)

    def list_relationships(self, claim_id: str | None = None) -> list[ClaimRelationship]:
        relationships = self.relationship_store.read_models(ClaimRelationship)
        if claim_id is None:
            return relationships
        return [item for item in relationships if item.claim_id == claim_id]

    def upsert_relationship(
        self,
        claim_id: str,
        related_claim_id: str,
        relationship_type: str,
        *,
        notes: str | None = None,
        source_kind: str = "manual",
    ) -> ClaimRelationship:
        relationships = self.relationship_store.read_models(ClaimRelationship)
        key = (claim_id, related_claim_id, relationship_type, source_kind)
        by_key = {
            (item.claim_id, item.related_claim_id, item.relationship_type, item.source_kind): item
            for item in relationships
        }
        relationship = ClaimRelationship(
            relationship_id=by_key.get(key, ClaimRelationship(
                relationship_id=f"rel-{len(by_key) + 1}",
                claim_id=claim_id,
                related_claim_id=related_claim_id,
                relationship_type=relationship_type,
                source_kind=source_kind,
            )).relationship_id,
            claim_id=claim_id,
            related_claim_id=related_claim_id,
            relationship_type=relationship_type,
            source_kind=source_kind,
            notes=notes,
        )
        by_key[key] = relationship
        self.relationship_store.write_models(by_key.values())
        return relationship

    def save_claim(
        self,
        claim: ApprovedClaim,
        evidence: list[EvidenceSnippet] | None = None,
        review=None,
    ) -> None:
        _ = evidence, review
        claims = {item.claim_id: item for item in self.store.read_models(ApprovedClaim)}
        claims[claim.claim_id] = claim
        self.store.write_models(claims.values())
