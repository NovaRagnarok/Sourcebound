from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from pathlib import Path
from typing import TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class SqliteAppStateStore:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS sources (
                    source_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS text_units (
                    text_unit_id TEXT PRIMARY KEY,
                    source_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS source_documents_state (
                    document_id TEXT PRIMARY KEY,
                    source_id TEXT NOT NULL,
                    ingest_status TEXT NOT NULL,
                    raw_text_status TEXT NOT NULL,
                    claim_extraction_status TEXT NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS extraction_runs (
                    run_id TEXT PRIMARY KEY,
                    started_at TEXT NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS candidates (
                    candidate_id TEXT PRIMARY KEY,
                    review_state TEXT NOT NULL,
                    extractor_run_id TEXT,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS evidence (
                    evidence_id TEXT PRIMARY KEY,
                    source_id TEXT NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS review_events (
                    review_id TEXT PRIMARY KEY,
                    candidate_id TEXT NOT NULL,
                    reviewed_at TEXT NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS research_runs (
                    run_id TEXT PRIMARY KEY,
                    started_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    program_id TEXT NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS research_findings (
                    finding_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    facet_id TEXT NOT NULL,
                    decision TEXT NOT NULL,
                    score REAL NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS research_programs (
                    program_id TEXT PRIMARY KEY,
                    updated_at TEXT NOT NULL,
                    built_in INTEGER NOT NULL,
                    payload TEXT NOT NULL
                );
                """
            )

    def list_models(
        self,
        table: str,
        model_type: type[T],
        *,
        order_by: str | None = None,
        where: tuple[str, object] | None = None,
    ) -> list[T]:
        query = f"SELECT payload FROM {table}"
        params: list[object] = []
        if where is not None:
            query += f" WHERE {where[0]} = ?"
            params.append(where[1])
        if order_by is not None:
            query += f" ORDER BY {order_by}"

        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [model_type.model_validate(json.loads(row["payload"])) for row in rows]

    def get_model(
        self,
        table: str,
        key_name: str,
        key_value: str,
        model_type: type[T],
    ) -> T | None:
        with self._connect() as connection:
            row = connection.execute(
                f"SELECT payload FROM {table} WHERE {key_name} = ?",
                (key_value,),
            ).fetchone()
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
        placeholders = ", ".join("?" for _ in columns)
        update_columns = ", ".join(f"{column} = excluded.{column}" for column in columns[1:])
        query = (
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) "
            f"ON CONFLICT({key_name}) DO UPDATE SET {update_columns}"
        )

        payloads: list[tuple[object, ...]] = []
        for item in items:
            item_payload = item.model_dump(mode="json")
            values: list[object] = [item_payload[key_name]]
            for _column, path in extra_columns.items():
                values.append(item_payload.get(path))
            values.append(json.dumps(item_payload))
            payloads.append(tuple(values))

        if not payloads:
            return

        with self._connect() as connection:
            connection.executemany(query, payloads)
