from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from psycopg import connect
from psycopg.rows import dict_row
from psycopg.sql import SQL, Identifier
from psycopg.types.json import Jsonb

from source_aware_worldbuilding.domain.models import (
    ApprovedClaim,
    BibleProjectProfile,
    BibleSection,
    CandidateClaim,
    ClaimRelationship,
    ClaimRelationshipSourceKind,
    ClaimRelationshipType,
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
from source_aware_worldbuilding.storage.postgres_app_state import PostgresAppStateStore


class _PostgresAdapterBase:
    def __init__(self, dsn: str, schema: str):
        self.store = PostgresAppStateStore(dsn, schema)


class PostgresSourceStore(_PostgresAdapterBase):
    def list_sources(self) -> list[SourceRecord]:
        return self.store.list_models("sources", SourceRecord, order_by="source_id")

    def get_source(self, source_id: str) -> SourceRecord | None:
        return self.store.get_model("sources", "source_id", source_id, SourceRecord)

    def save_sources(self, sources: list[SourceRecord]) -> None:
        self.store.upsert_models("sources", "source_id", sources)


class PostgresTextUnitStore(_PostgresAdapterBase):
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


class PostgresSourceDocumentStore(_PostgresAdapterBase):
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


class PostgresExtractionRunStore(_PostgresAdapterBase):
    def list_runs(self) -> list[ExtractionRun]:
        return self.store.list_models(
            "extraction_runs",
            ExtractionRun,
            order_by="started_at DESC",
        )

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


class PostgresCandidateStore(_PostgresAdapterBase):
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
            extra_columns={
                "review_state": "review_state",
                "extractor_run_id": "extractor_run_id",
            },
        )

    def update_candidate(self, candidate: CandidateClaim) -> None:
        self.save_candidates([candidate])


class PostgresEvidenceStore(_PostgresAdapterBase):
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


class PostgresReviewStore(_PostgresAdapterBase):
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


class PostgresResearchRunStore(_PostgresAdapterBase):
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


class PostgresResearchFindingStore(_PostgresAdapterBase):
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


class PostgresResearchProgramStore(_PostgresAdapterBase):
    def list_programs(self) -> list[ResearchProgram]:
        return self.store.list_models(
            "research_programs", ResearchProgram, order_by="updated_at DESC"
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


class PostgresJobStore(_PostgresAdapterBase):
    def list_jobs(self, *, status: str | None = None) -> list[JobRecord]:
        where = ("status", status) if status else None
        return self.store.list_models("jobs", JobRecord, order_by="created_at DESC", where=where)

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


class PostgresBibleProjectProfileStore(_PostgresAdapterBase):
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


class PostgresBibleSectionStore(_PostgresAdapterBase):
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


class PostgresTruthStore(_PostgresAdapterBase):
    def list_claims(self) -> list[ApprovedClaim]:
        query = SQL(
            """
            SELECT
                c.claim_id,
                c.subject,
                c.predicate,
                c.object_value,
                c.claim_kind,
                c.certainty_status,
                c.place,
                c.time_start,
                c.time_end,
                c.viewpoint_scope,
                c.author_choice,
                c.created_from_run_id,
                c.notes,
                COALESCE(
                    ARRAY_AGG(ce.evidence_id ORDER BY ce.position)
                        FILTER (WHERE ce.evidence_id IS NOT NULL),
                    ARRAY[]::TEXT[]
                ) AS evidence_ids
            FROM {}.claims c
            LEFT JOIN {}.claim_evidence ce ON ce.claim_id = c.id
            WHERE c.review_status = 'approved'
            GROUP BY
                c.id,
                c.claim_id,
                c.subject,
                c.predicate,
                c.object_value,
                c.claim_kind,
                c.certainty_status,
                c.place,
                c.time_start,
                c.time_end,
                c.viewpoint_scope,
                c.author_choice,
                c.created_from_run_id,
                c.notes
            ORDER BY c.claim_id
            """
        ).format(Identifier(self.store.schema), Identifier(self.store.schema))
        with self._connect() as connection:
            rows = connection.execute(query).fetchall()
        return [self._row_to_claim(row) for row in rows]

    def get_claim(self, claim_id: str) -> ApprovedClaim | None:
        query = SQL(
            """
            SELECT
                c.claim_id,
                c.subject,
                c.predicate,
                c.object_value,
                c.claim_kind,
                c.certainty_status,
                c.place,
                c.time_start,
                c.time_end,
                c.viewpoint_scope,
                c.author_choice,
                c.created_from_run_id,
                c.notes,
                COALESCE(
                    ARRAY_AGG(ce.evidence_id ORDER BY ce.position)
                        FILTER (WHERE ce.evidence_id IS NOT NULL),
                    ARRAY[]::TEXT[]
                ) AS evidence_ids
            FROM {}.claims c
            LEFT JOIN {}.claim_evidence ce ON ce.claim_id = c.id
            WHERE c.claim_id = %s
            GROUP BY
                c.id,
                c.claim_id,
                c.subject,
                c.predicate,
                c.object_value,
                c.claim_kind,
                c.certainty_status,
                c.place,
                c.time_start,
                c.time_end,
                c.viewpoint_scope,
                c.author_choice,
                c.created_from_run_id,
                c.notes
            """
        ).format(Identifier(self.store.schema), Identifier(self.store.schema))
        with self._connect() as connection:
            row = connection.execute(query, (claim_id,)).fetchone()
        return None if row is None else self._row_to_claim(row)

    def list_relationships(self, claim_id: str | None = None) -> list[ClaimRelationship]:
        query = SQL(
            """
            SELECT
                cr.id,
                c.claim_id,
                rc.claim_id AS related_claim_id,
                cr.relationship_type,
                cr.source_kind,
                cr.notes
            FROM {}.claim_relationships cr
            JOIN {}.claims c ON c.id = cr.claim_id
            JOIN {}.claims rc ON rc.id = cr.related_claim_id
            """
        ).format(
            Identifier(self.store.schema),
            Identifier(self.store.schema),
            Identifier(self.store.schema),
        )
        params: tuple[object, ...] = ()
        if claim_id is not None:
            query += SQL(" WHERE c.claim_id = %s")
            params = (claim_id,)
        query += SQL(" ORDER BY c.claim_id, cr.relationship_type, rc.claim_id")
        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [
            ClaimRelationship(
                relationship_id=str(row["id"]),
                claim_id=row["claim_id"],
                related_claim_id=row["related_claim_id"],
                relationship_type=row["relationship_type"],
                source_kind=row["source_kind"],
                notes=row["notes"],
            )
            for row in rows
        ]

    def upsert_relationship(
        self,
        claim_id: str,
        related_claim_id: str,
        relationship_type: ClaimRelationshipType,
        *,
        notes: str | None = None,
        source_kind: ClaimRelationshipSourceKind = "manual",
    ) -> ClaimRelationship:
        with self._connect() as connection:
            claim_row = self._require_claim_row(connection, claim_id)
            related_row = self._require_claim_row(connection, related_claim_id)
            relationship = self._insert_relationship(
                connection,
                str(claim_row["id"]),
                str(related_row["id"]),
                relationship_type,
                notes or "Manually curated relationship.",
                source_kind=source_kind,
            )
            connection.commit()
        return relationship

    def save_claim(
        self,
        claim: ApprovedClaim,
        evidence: list[EvidenceSnippet] | None = None,
        review: ReviewEvent | None = None,
    ) -> None:
        evidence = evidence or []
        with self._connect() as connection:
            claim_row_id = self._upsert_claim(connection, claim)
            self._replace_claim_evidence(connection, claim_row_id, evidence)
            self._upsert_source_records(connection, evidence)
            self._supersede_exact_matches(connection, claim_row_id, claim)
            self._insert_claim_version(connection, claim_row_id, claim, review)
            if review is not None:
                self._upsert_claim_review(connection, claim_row_id, review)
            if claim.author_choice:
                self._upsert_author_decision(connection, claim_row_id, claim, review)
            self._refresh_claim_relationships(connection, claim_row_id, claim, evidence)
            connection.commit()

    def _connect(self):
        return connect(self.store.dsn, row_factory=dict_row)

    def _row_to_claim(self, row) -> ApprovedClaim:
        return ApprovedClaim(
            claim_id=row["claim_id"],
            subject=row["subject"],
            predicate=row["predicate"],
            value=row["object_value"],
            claim_kind=row["claim_kind"],
            status=row["certainty_status"],
            place=row["place"],
            time_start=row["time_start"],
            time_end=row["time_end"],
            viewpoint_scope=row["viewpoint_scope"],
            author_choice=row["author_choice"],
            evidence_ids=list(row["evidence_ids"] or []),
            created_from_run_id=row["created_from_run_id"],
            notes=row["notes"],
        )

    def _upsert_claim(self, connection, claim: ApprovedClaim) -> str:
        now = _as_datetime(_utc_now())
        existing = connection.execute(
            SQL("SELECT id, created_at FROM {}.claims WHERE claim_id = %s").format(
                Identifier(self.store.schema)
            ),
            (claim.claim_id,),
        ).fetchone()
        claim_row_id = existing["id"] if existing else str(uuid4())
        created_at = existing["created_at"] if existing else now
        connection.execute(
            SQL(
                """
                INSERT INTO {}.claims (
                    id,
                    claim_id,
                    project_id,
                    subject,
                    predicate,
                    object_value,
                    time_start,
                    time_end,
                    place,
                    certainty_status,
                    claim_kind,
                    review_status,
                    created_from_run_id,
                    viewpoint_scope,
                    author_choice,
                    notes,
                    created_at,
                    updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (claim_id) DO UPDATE SET
                    subject = EXCLUDED.subject,
                    predicate = EXCLUDED.predicate,
                    object_value = EXCLUDED.object_value,
                    time_start = EXCLUDED.time_start,
                    time_end = EXCLUDED.time_end,
                    place = EXCLUDED.place,
                    certainty_status = EXCLUDED.certainty_status,
                    claim_kind = EXCLUDED.claim_kind,
                    review_status = EXCLUDED.review_status,
                    created_from_run_id = EXCLUDED.created_from_run_id,
                    viewpoint_scope = EXCLUDED.viewpoint_scope,
                    author_choice = EXCLUDED.author_choice,
                    notes = EXCLUDED.notes,
                    updated_at = EXCLUDED.updated_at
                """
            ).format(Identifier(self.store.schema)),
            (
                claim_row_id,
                claim.claim_id,
                "default",
                claim.subject,
                claim.predicate,
                claim.value,
                claim.time_start,
                claim.time_end,
                claim.place,
                claim.status.value,
                claim.claim_kind.value,
                "approved",
                claim.created_from_run_id,
                claim.viewpoint_scope,
                claim.author_choice,
                claim.notes,
                created_at,
                now,
            ),
        )
        return claim_row_id

    def _replace_claim_evidence(
        self,
        connection,
        claim_row_id: str,
        evidence: list[EvidenceSnippet],
    ) -> None:
        connection.execute(
            SQL("DELETE FROM {}.claim_evidence WHERE claim_id = %s").format(
                Identifier(self.store.schema)
            ),
            (claim_row_id,),
        )
        for position, snippet in enumerate(evidence):
            connection.execute(
                SQL(
                    """
                    INSERT INTO {}.claim_evidence (
                        id,
                        claim_id,
                        evidence_id,
                        source_id,
                        source_chunk_id,
                        locator,
                        evidence_text,
                        span_start,
                        span_end,
                        evidence_notes,
                        position,
                        created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                ).format(Identifier(self.store.schema)),
                (
                    str(uuid4()),
                    claim_row_id,
                    snippet.evidence_id,
                    snippet.source_id,
                    snippet.text_unit_id or snippet.evidence_id,
                    snippet.locator,
                    snippet.text,
                    snippet.span_start,
                    snippet.span_end,
                    snippet.notes,
                    position,
                    _as_datetime(_utc_now()),
                ),
            )

    def _upsert_source_records(self, connection, evidence: list[EvidenceSnippet]) -> None:
        for snippet in evidence:
            existing_document = connection.execute(
                SQL("SELECT id FROM {}.source_documents WHERE source_id = %s").format(
                    Identifier(self.store.schema)
                ),
                (snippet.source_id,),
            ).fetchone()
            document_id = existing_document["id"] if existing_document else str(uuid4())
            now = _as_datetime(_utc_now())
            connection.execute(
                SQL(
                    """
                    INSERT INTO {}.source_documents (
                        id, source_id, project_id, title, source_type, created_at,
                        updated_at, metadata_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source_id) DO UPDATE SET
                        updated_at = EXCLUDED.updated_at
                    """
                ).format(Identifier(self.store.schema)),
                (
                    document_id,
                    snippet.source_id,
                    "default",
                    None,
                    None,
                    now,
                    now,
                    Jsonb({}),
                ),
            )
            connection.execute(
                SQL(
                    """
                    INSERT INTO {}.source_chunks (
                        id,
                        chunk_id,
                        source_document_id,
                        source_id,
                        locator,
                        text_content,
                        text_unit_id,
                        checksum,
                        metadata_json,
                        created_at,
                        updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (chunk_id) DO UPDATE SET
                        locator = EXCLUDED.locator,
                        text_content = EXCLUDED.text_content,
                        text_unit_id = EXCLUDED.text_unit_id,
                        checksum = EXCLUDED.checksum,
                        metadata_json = EXCLUDED.metadata_json,
                        updated_at = EXCLUDED.updated_at
                    """
                ).format(Identifier(self.store.schema)),
                (
                    str(uuid4()),
                    snippet.text_unit_id or snippet.evidence_id,
                    document_id,
                    snippet.source_id,
                    snippet.locator,
                    snippet.text,
                    snippet.text_unit_id,
                    snippet.checksum,
                    Jsonb({"evidence_id": snippet.evidence_id}),
                    now,
                    now,
                ),
            )

    def _insert_claim_version(
        self,
        connection,
        claim_row_id: str,
        claim: ApprovedClaim,
        review: ReviewEvent | None,
    ) -> None:
        version_row = connection.execute(
            SQL(
                "SELECT COALESCE(MAX(version_number), 0) AS version_number "
                "FROM {}.claim_versions WHERE claim_id = %s"
            ).format(Identifier(self.store.schema)),
            (claim_row_id,),
        ).fetchone()
        next_version = int(version_row["version_number"]) + 1
        connection.execute(
            SQL(
                """
                INSERT INTO {}.claim_versions (
                    id,
                    claim_id,
                    version_number,
                    snapshot,
                    changed_at,
                    change_reason
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """
            ).format(Identifier(self.store.schema)),
            (
                str(uuid4()),
                claim_row_id,
                next_version,
                Jsonb(claim.model_dump(mode="json")),
                _as_datetime(_utc_now()),
                review.notes if review is not None else "Claim saved",
            ),
        )

    def _upsert_claim_review(self, connection, claim_row_id: str, review: ReviewEvent) -> None:
        connection.execute(
            SQL(
                """
                INSERT INTO {}.claim_reviews (
                    id,
                    review_id,
                    claim_id,
                    candidate_id,
                    decision,
                    override_status,
                    notes,
                    approved_claim_id,
                    reviewed_at,
                    created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (review_id) DO UPDATE SET
                    claim_id = EXCLUDED.claim_id,
                    candidate_id = EXCLUDED.candidate_id,
                    decision = EXCLUDED.decision,
                    override_status = EXCLUDED.override_status,
                    notes = EXCLUDED.notes,
                    approved_claim_id = EXCLUDED.approved_claim_id,
                    reviewed_at = EXCLUDED.reviewed_at
                """
            ).format(Identifier(self.store.schema)),
            (
                str(uuid4()),
                review.review_id,
                claim_row_id,
                review.candidate_id,
                review.decision.value,
                review.override_status.value if review.override_status is not None else None,
                review.notes,
                review.approved_claim_id,
                _as_datetime(review.reviewed_at),
                _as_datetime(_utc_now()),
            ),
        )

    def _upsert_author_decision(
        self,
        connection,
        claim_row_id: str,
        claim: ApprovedClaim,
        review: ReviewEvent | None,
    ) -> None:
        connection.execute(
            SQL("DELETE FROM {}.author_decisions WHERE claim_id = %s").format(
                Identifier(self.store.schema)
            ),
            (claim_row_id,),
        )
        connection.execute(
            SQL(
                """
                INSERT INTO {}.author_decisions (
                    id,
                    claim_id,
                    decision_type,
                    notes,
                    created_at
                ) VALUES (%s, %s, %s, %s, %s)
                """
            ).format(Identifier(self.store.schema)),
            (
                str(uuid4()),
                claim_row_id,
                "author_choice",
                review.notes if review is not None else claim.notes,
                _as_datetime(_utc_now()),
            ),
        )

    def _refresh_claim_relationships(
        self,
        connection,
        claim_row_id: str,
        claim: ApprovedClaim,
        evidence: list[EvidenceSnippet],
    ) -> None:
        connection.execute(
            SQL(
                "DELETE FROM {}.claim_relationships "
                "WHERE (claim_id = %s OR related_claim_id = %s) AND source_kind = 'derived'"
            ).format(Identifier(self.store.schema)),
            (claim_row_id, claim_row_id),
        )
        rows = connection.execute(
            SQL(
                """
                SELECT
                    id,
                    claim_id,
                    subject,
                    predicate,
                    object_value,
                    place,
                    time_start,
                    time_end,
                    viewpoint_scope,
                    review_status,
                    COALESCE(
                        ARRAY(
                            SELECT ce.source_id
                            FROM {}.claim_evidence ce
                            WHERE ce.claim_id = claims.id
                            ORDER BY ce.position
                        ),
                        ARRAY[]::TEXT[]
                    ) AS source_ids
                FROM {}.claims
                WHERE id <> %s
                """
            ).format(Identifier(self.store.schema), Identifier(self.store.schema)),
            (claim_row_id,),
        ).fetchall()
        current_source_ids = [snippet.source_id for snippet in evidence]
        for row in rows:
            relationship_type = self._classify_relationship(row, claim)
            if relationship_type is None:
                continue
            notes = self._relationship_note(
                relationship_type,
                current_source_ids=current_source_ids,
                other_source_ids=list(row["source_ids"] or []),
            )
            self._insert_relationship(
                connection, claim_row_id, str(row["id"]), relationship_type, notes
            )
            reverse_type = self._reverse_relationship_type(relationship_type)
            self._insert_relationship(
                connection,
                str(row["id"]),
                claim_row_id,
                reverse_type,
                notes,
                source_kind="derived",
            )

    def _classify_relationship(
        self,
        existing_row,
        claim: ApprovedClaim,
    ) -> ClaimRelationshipType | None:
        if existing_row["subject"] != claim.subject or existing_row["predicate"] != claim.predicate:
            return None
        same_signature = all(
            [
                existing_row["object_value"] == claim.value,
                existing_row["place"] == claim.place,
                existing_row["time_start"] == claim.time_start,
                existing_row["time_end"] == claim.time_end,
                existing_row["viewpoint_scope"] == claim.viewpoint_scope,
            ]
        )
        if same_signature:
            return "supersedes"
        if existing_row["review_status"] != "approved":
            return None
        if not self._places_compatible(existing_row["place"], claim.place):
            return None
        if existing_row["object_value"] == claim.value:
            if not self._times_compatible(
                existing_row["time_start"],
                existing_row["time_end"],
                claim.time_start,
                claim.time_end,
            ):
                return None
            return "supports"
        if not self._times_conflict_scope(
            existing_row["time_start"],
            existing_row["time_end"],
            claim.time_start,
            claim.time_end,
        ):
            return None
        return "contradicts"

    def _insert_relationship(
        self,
        connection,
        claim_row_id: str,
        related_claim_row_id: str,
        relationship_type: ClaimRelationshipType,
        notes: str,
        *,
        source_kind: ClaimRelationshipSourceKind = "derived",
    ) -> ClaimRelationship:
        relationship_id = str(uuid4())
        connection.execute(
            SQL(
                """
                INSERT INTO {}.claim_relationships (
                    id,
                    claim_id,
                    related_claim_id,
                    relationship_type,
                    source_kind,
                    notes,
                    created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (
                    claim_id, related_claim_id, relationship_type, source_kind
                ) DO UPDATE SET
                    notes = EXCLUDED.notes
                """
            ).format(Identifier(self.store.schema)),
            (
                relationship_id,
                claim_row_id,
                related_claim_row_id,
                relationship_type,
                source_kind,
                notes,
                _as_datetime(_utc_now()),
            ),
        )
        return ClaimRelationship(
            relationship_id=relationship_id,
            claim_id=self._claim_id_for_row(connection, claim_row_id),
            related_claim_id=self._claim_id_for_row(connection, related_claim_row_id),
            relationship_type=relationship_type,
            source_kind=source_kind,
            notes=notes,
        )

    def _reverse_relationship_type(
        self,
        relationship_type: ClaimRelationshipType,
    ) -> ClaimRelationshipType:
        if relationship_type == "supersedes":
            return "superseded_by"
        if relationship_type == "superseded_by":
            return "supersedes"
        return relationship_type

    def _relationship_note(
        self,
        relationship_type: str,
        *,
        current_source_ids: list[str],
        other_source_ids: list[str],
    ) -> str:
        shared_sources = sorted(set(current_source_ids) & set(other_source_ids))
        distinct_sources = len(set(current_source_ids + other_source_ids))
        provenance_note = (
            f" Shared provenance: {', '.join(shared_sources)}."
            if shared_sources
            else f" Distinct provenance across {distinct_sources} sources."
        )
        if relationship_type == "supersedes":
            return "Newer canonical claim with the same signature." + provenance_note
        if relationship_type == "supports":
            return "Canonical claims align on subject, predicate, and value." + provenance_note
        return "Canonical claims share a subject/predicate but disagree on value." + provenance_note

    def _require_claim_row(self, connection, claim_id: str):
        row = connection.execute(
            SQL("SELECT id, claim_id FROM {}.claims WHERE claim_id = %s").format(
                Identifier(self.store.schema)
            ),
            (claim_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"Claim not found: {claim_id}")
        return row

    def _claim_id_for_row(self, connection, row_id: str) -> str:
        row = connection.execute(
            SQL("SELECT claim_id FROM {}.claims WHERE id = %s").format(
                Identifier(self.store.schema)
            ),
            (row_id,),
        ).fetchone()
        return row["claim_id"]

    def _supersede_exact_matches(
        self,
        connection,
        claim_row_id: str,
        claim: ApprovedClaim,
    ) -> None:
        connection.execute(
            SQL(
                """
                UPDATE {}.claims
                SET review_status = 'superseded', updated_at = %s
                WHERE id <> %s
                  AND review_status = 'approved'
                  AND subject = %s
                  AND predicate = %s
                  AND object_value = %s
                  AND COALESCE(place, '') = COALESCE(%s, '')
                  AND COALESCE(time_start, '') = COALESCE(%s, '')
                  AND COALESCE(time_end, '') = COALESCE(%s, '')
                  AND COALESCE(viewpoint_scope, '') = COALESCE(%s, '')
                """
            ).format(Identifier(self.store.schema)),
            (
                _as_datetime(_utc_now()),
                claim_row_id,
                claim.subject,
                claim.predicate,
                claim.value,
                claim.place,
                claim.time_start,
                claim.time_end,
                claim.viewpoint_scope,
            ),
        )

    def _places_compatible(self, existing_place: str | None, new_place: str | None) -> bool:
        if not existing_place or not new_place:
            return True
        return existing_place == new_place

    def _times_compatible(
        self,
        existing_start: str | None,
        existing_end: str | None,
        new_start: str | None,
        new_end: str | None,
    ) -> bool:
        if not existing_start and not existing_end:
            return True
        if not new_start and not new_end:
            return True
        left_start = existing_start or existing_end
        left_end = existing_end or existing_start
        right_start = new_start or new_end
        right_end = new_end or new_start
        if not left_start or not left_end or not right_start or not right_end:
            return True
        return max(left_start, right_start) <= min(left_end, right_end)

    def _times_conflict_scope(
        self,
        existing_start: str | None,
        existing_end: str | None,
        new_start: str | None,
        new_end: str | None,
    ) -> bool:
        existing_has_time = bool(existing_start or existing_end)
        new_has_time = bool(new_start or new_end)
        if not existing_has_time and not new_has_time:
            return True
        if existing_has_time and new_has_time:
            return self._times_compatible(existing_start, existing_end, new_start, new_end)
        return False


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _as_datetime(value: str):
    return datetime.fromisoformat(value)
