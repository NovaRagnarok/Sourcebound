from __future__ import annotations

import json
from collections.abc import Iterable
from typing import TypeVar

from psycopg import connect
from psycopg.rows import dict_row
from psycopg.sql import SQL, Composable, Identifier
from psycopg.types.json import Jsonb
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)

TABLE_DEFINITIONS = {
    "sources": """
        source_id TEXT PRIMARY KEY,
        payload JSONB NOT NULL
    """,
    "text_units": """
        text_unit_id TEXT PRIMARY KEY,
        source_id TEXT NOT NULL,
        ordinal INTEGER NOT NULL,
        payload JSONB NOT NULL
    """,
    "source_documents_state": """
        document_id TEXT PRIMARY KEY,
        source_id TEXT NOT NULL,
        ingest_status TEXT NOT NULL,
        raw_text_status TEXT NOT NULL,
        claim_extraction_status TEXT NOT NULL,
        payload JSONB NOT NULL
    """,
    "source_documents": """
        id UUID PRIMARY KEY,
        source_id TEXT NOT NULL UNIQUE,
        project_id TEXT NOT NULL,
        title TEXT,
        source_type TEXT,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL,
        metadata_json JSONB
    """,
    "source_chunks": """
        id UUID PRIMARY KEY,
        chunk_id TEXT NOT NULL UNIQUE,
        source_document_id UUID REFERENCES source_documents(id) ON DELETE CASCADE,
        source_id TEXT NOT NULL,
        locator TEXT NOT NULL,
        text_content TEXT NOT NULL,
        text_unit_id TEXT,
        checksum TEXT,
        metadata_json JSONB,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
    """,
    "extraction_runs": """
        run_id TEXT PRIMARY KEY,
        started_at TIMESTAMPTZ NOT NULL,
        payload JSONB NOT NULL
    """,
    "candidates": """
        candidate_id TEXT PRIMARY KEY,
        review_state TEXT NOT NULL,
        extractor_run_id TEXT,
        payload JSONB NOT NULL
    """,
    "evidence": """
        evidence_id TEXT PRIMARY KEY,
        source_id TEXT NOT NULL,
        payload JSONB NOT NULL
    """,
    "review_events": """
        review_id TEXT PRIMARY KEY,
        candidate_id TEXT NOT NULL,
        reviewed_at TIMESTAMPTZ NOT NULL,
        payload JSONB NOT NULL
    """,
    "research_runs": """
        run_id TEXT PRIMARY KEY,
        started_at TIMESTAMPTZ NOT NULL,
        status TEXT NOT NULL,
        program_id TEXT NOT NULL,
        payload JSONB NOT NULL
    """,
    "research_findings": """
        finding_id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL,
        facet_id TEXT NOT NULL,
        decision TEXT NOT NULL,
        score DOUBLE PRECISION NOT NULL,
        payload JSONB NOT NULL
    """,
    "research_programs": """
        program_id TEXT PRIMARY KEY,
        updated_at TIMESTAMPTZ NOT NULL,
        built_in BOOLEAN NOT NULL,
        payload JSONB NOT NULL
    """,
    "jobs": """
        job_id TEXT PRIMARY KEY,
        status TEXT NOT NULL,
        job_type TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        payload JSONB NOT NULL
    """,
    "bible_project_profiles": """
        project_id TEXT PRIMARY KEY,
        project_name TEXT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL,
        payload JSONB NOT NULL
    """,
    "bible_sections": """
        section_id TEXT PRIMARY KEY,
        project_id TEXT NOT NULL,
        section_type TEXT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL,
        payload JSONB NOT NULL
    """,
    "claims": """
        id UUID PRIMARY KEY,
        claim_id TEXT NOT NULL UNIQUE,
        project_id TEXT NOT NULL,
        subject TEXT NOT NULL,
        predicate TEXT NOT NULL,
        object_value TEXT NOT NULL,
        time_start TEXT,
        time_end TEXT,
        place TEXT,
        certainty_status TEXT NOT NULL CHECK (
            certainty_status IN (
                'verified', 'probable', 'contested', 'rumor', 'legend', 'author_choice'
            )
        ),
        claim_kind TEXT NOT NULL CHECK (
            claim_kind IN (
                'person', 'place', 'institution', 'event', 'practice', 'belief',
                'relationship', 'object'
            )
        ),
        review_status TEXT NOT NULL CHECK (
            review_status IN (
                'pending', 'approved', 'rejected', 'needs_split', 'needs_edit',
                'superseded'
            )
        ),
        created_from_run_id TEXT,
        viewpoint_scope TEXT,
        author_choice BOOLEAN NOT NULL DEFAULT FALSE,
        notes TEXT,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL
    """,
    "claim_evidence": """
        id UUID PRIMARY KEY,
        claim_id UUID NOT NULL REFERENCES claims(id) ON DELETE CASCADE,
        evidence_id TEXT NOT NULL,
        source_id TEXT NOT NULL,
        source_chunk_id TEXT,
        locator TEXT NOT NULL,
        evidence_text TEXT NOT NULL,
        span_start INTEGER,
        span_end INTEGER,
        evidence_notes TEXT,
        position INTEGER NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        UNIQUE (claim_id, evidence_id)
    """,
    "claim_relationships": """
        id UUID PRIMARY KEY,
        claim_id UUID NOT NULL REFERENCES claims(id) ON DELETE CASCADE,
        related_claim_id UUID NOT NULL REFERENCES claims(id) ON DELETE CASCADE,
        relationship_type TEXT NOT NULL,
        source_kind TEXT NOT NULL CHECK (source_kind IN ('derived', 'manual')),
        notes TEXT,
        created_at TIMESTAMPTZ NOT NULL,
        UNIQUE (claim_id, related_claim_id, relationship_type, source_kind)
    """,
    "claim_reviews": """
        id UUID PRIMARY KEY,
        review_id TEXT NOT NULL UNIQUE,
        claim_id UUID NOT NULL REFERENCES claims(id) ON DELETE CASCADE,
        candidate_id TEXT NOT NULL,
        decision TEXT NOT NULL CHECK (decision IN ('approve', 'reject')),
        override_status TEXT CHECK (
            override_status IS NULL OR override_status IN (
                'verified', 'probable', 'contested', 'rumor', 'legend', 'author_choice'
            )
        ),
        notes TEXT,
        approved_claim_id TEXT,
        reviewed_at TIMESTAMPTZ NOT NULL,
        created_at TIMESTAMPTZ NOT NULL
    """,
    "claim_versions": """
        id UUID PRIMARY KEY,
        claim_id UUID NOT NULL REFERENCES claims(id) ON DELETE CASCADE,
        version_number INTEGER NOT NULL,
        snapshot JSONB NOT NULL,
        changed_at TIMESTAMPTZ NOT NULL,
        change_reason TEXT,
        UNIQUE (claim_id, version_number)
    """,
    "author_decisions": """
        id UUID PRIMARY KEY,
        claim_id UUID NOT NULL REFERENCES claims(id) ON DELETE CASCADE,
        decision_type TEXT NOT NULL,
        notes TEXT,
        created_at TIMESTAMPTZ NOT NULL
    """,
}

TABLE_ORDER = [
    "author_decisions",
    "claim_versions",
    "claim_reviews",
    "claim_relationships",
    "claim_evidence",
    "claims",
    "source_chunks",
    "source_documents",
    "review_events",
    "research_findings",
    "research_runs",
    "research_programs",
    "jobs",
    "bible_sections",
    "bible_project_profiles",
    "evidence",
    "candidates",
    "extraction_runs",
    "text_units",
    "source_documents_state",
    "sources",
]

POST_INIT_STATEMENTS = [
    (
        "CREATE INDEX IF NOT EXISTS idx_source_documents_source_id ON "
        "{schema}.source_documents (source_id)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_source_chunks_source_id ON "
        "{schema}.source_chunks (source_id)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_claims_project_status ON {schema}.claims "
        "(project_id, certainty_status, review_status)"
    ),
    "CREATE INDEX IF NOT EXISTS idx_claims_run_id ON {schema}.claims (created_from_run_id)",
    (
        "CREATE INDEX IF NOT EXISTS idx_claim_evidence_claim_id ON {schema}.claim_evidence "
        "(claim_id, position)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_claim_reviews_claim_id ON {schema}.claim_reviews "
        "(claim_id, reviewed_at DESC)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_claim_versions_claim_id ON {schema}.claim_versions "
        "(claim_id, version_number DESC)"
    ),
]


class PostgresAppStateStore:
    def __init__(self, dsn: str, schema: str):
        self.dsn = dsn
        self.schema = schema
        self._initialize()

    def _connect(self):
        return connect(self.dsn, autocommit=True, row_factory=dict_row)

    def _initialize(self) -> None:
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(SQL("CREATE SCHEMA IF NOT EXISTS {}").format(Identifier(self.schema)))
            cursor.execute(SQL("SET search_path TO {}").format(Identifier(self.schema)))
            for table_name, definition in TABLE_DEFINITIONS.items():
                cursor.execute(
                    SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
                        Identifier(self.schema),
                        Identifier(table_name),
                        SQL(definition),
                    )
                )
            for statement in POST_INIT_STATEMENTS:
                cursor.execute(SQL(statement.format(schema="{}")).format(Identifier(self.schema)))

    def clear_all(self) -> None:
        with self._connect() as connection:
            table_list = SQL(", ").join(
                SQL("{}.{}").format(Identifier(self.schema), Identifier(table_name))
                for table_name in TABLE_ORDER
            )
            connection.execute(SQL("TRUNCATE TABLE {} CASCADE").format(table_list))

    def list_models(
        self,
        table: str,
        model_type: type[T],
        *,
        order_by: str | None = None,
        where: tuple[str, object] | None = None,
    ) -> list[T]:
        query = SQL("SELECT payload::text AS payload FROM {}.{}").format(
            Identifier(self.schema),
            Identifier(table),
        )
        params: list[object] = []
        if where is not None:
            query += SQL(" WHERE {} = %s").format(Identifier(where[0]))
            params.append(where[1])
        if order_by is not None:
            query += SQL(" ORDER BY {}").format(self._order_by_clause(order_by))

        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [model_type.model_validate(json.loads(row["payload"])) for row in rows]

    def _order_by_clause(self, order_by: str):
        clauses: list[Composable] = []
        for fragment in order_by.split(","):
            parts = fragment.strip().split()
            if len(parts) == 1:
                clauses.append(Identifier(parts[0]))
                continue
            if len(parts) == 2 and parts[1].upper() in {"ASC", "DESC"}:
                clauses.append(SQL("{} {}").format(Identifier(parts[0]), SQL(parts[1].upper())))
                continue
            raise ValueError(f"Unsupported order_by fragment: {fragment}")
        return SQL(", ").join(clauses)

    def get_model(
        self,
        table: str,
        key_name: str,
        key_value: str,
        model_type: type[T],
    ) -> T | None:
        query = SQL("SELECT payload::text AS payload FROM {}.{} WHERE {} = %s").format(
            Identifier(self.schema),
            Identifier(table),
            Identifier(key_name),
        )
        with self._connect() as connection:
            row = connection.execute(query, (key_value,)).fetchone()
        if row is None:
            return None
        return model_type.model_validate(json.loads(row["payload"]))

    def upsert_models(
        self,
        table: str,
        key_name: str,
        items: Iterable[T],
        *,
        extra_columns: dict[str, str] | None = None,
    ) -> None:
        extra_columns = extra_columns or {}
        columns = [key_name, *extra_columns.keys(), "payload"]
        placeholders = SQL(", ").join(SQL("%s") for _ in columns)
        update_columns = SQL(", ").join(
            SQL("{} = EXCLUDED.{}").format(Identifier(column), Identifier(column))
            for column in columns[1:]
        )
        query = SQL("INSERT INTO {}.{} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}").format(
            Identifier(self.schema),
            Identifier(table),
            SQL(", ").join(Identifier(column) for column in columns),
            placeholders,
            Identifier(key_name),
            update_columns,
        )

        payloads: list[tuple[object, ...]] = []
        for item in items:
            item_payload = item.model_dump(mode="json")
            values: list[object] = [item_payload[key_name]]
            for _column, path in extra_columns.items():
                values.append(item_payload.get(path))
            values.append(Jsonb(item_payload))
            payloads.append(tuple(values))

        if not payloads:
            return

        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(query, payloads)
