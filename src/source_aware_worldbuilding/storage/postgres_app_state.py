from __future__ import annotations

import json
from collections.abc import Iterable
from typing import TypeVar

from psycopg import connect
from psycopg.rows import dict_row
from psycopg.sql import SQL, Identifier
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
    "claims": """
        claim_id TEXT PRIMARY KEY,
        payload JSONB NOT NULL
    """,
    "review_events": """
        review_id TEXT PRIMARY KEY,
        candidate_id TEXT NOT NULL,
        reviewed_at TIMESTAMPTZ NOT NULL,
        payload JSONB NOT NULL
    """,
}

TABLE_ORDER = [
    "review_events",
    "claims",
    "evidence",
    "candidates",
    "extraction_runs",
    "text_units",
    "sources",
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
            for table_name, definition in TABLE_DEFINITIONS.items():
                cursor.execute(
                    SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
                        Identifier(self.schema),
                        Identifier(table_name),
                        SQL(definition),
                    )
                )

    def clear_all(self) -> None:
        with self._connect() as connection:
            cursor = connection.cursor()
            for table_name in TABLE_ORDER:
                cursor.execute(
                    SQL("TRUNCATE TABLE {}.{}").format(
                        Identifier(self.schema),
                        Identifier(table_name),
                    )
                )

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
        clauses = []
        for fragment in order_by.split(","):
            parts = fragment.strip().split()
            if len(parts) == 1:
                clauses.append(Identifier(parts[0]))
                continue
            if len(parts) == 2 and parts[1].upper() in {"ASC", "DESC"}:
                clauses.append(
                    SQL("{} {}").format(Identifier(parts[0]), SQL(parts[1].upper()))
                )
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
