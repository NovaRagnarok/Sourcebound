from __future__ import annotations

import json
from pathlib import Path

import typer
import uvicorn
from rich import print

from source_aware_worldbuilding.adapters.postgres_backed import (
    PostgresCandidateStore,
    PostgresEvidenceStore,
    PostgresExtractionRunStore,
    PostgresReviewStore,
    PostgresSourceStore,
    PostgresTextUnitStore,
    PostgresTruthStore,
)
from source_aware_worldbuilding.adapters.sqlite_backed import (
    SqliteCandidateStore,
    SqliteEvidenceStore,
    SqliteExtractionRunStore,
    SqliteReviewStore,
    SqliteSourceStore,
    SqliteTextUnitStore,
    SqliteTruthStore,
)
from source_aware_worldbuilding.domain.enums import (
    ClaimKind,
    ClaimStatus,
    ExtractionRunStatus,
    ReviewState,
)
from source_aware_worldbuilding.domain.models import (
    CandidateClaim,
    EvidenceSnippet,
    ExtractionRun,
    SourceRecord,
    TextUnit,
)
from source_aware_worldbuilding.settings import settings

app = typer.Typer(help="Source-Aware Worldbuilding CLI")


def _write_json(path: Path, payload: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _seed_sources() -> list[SourceRecord]:
    return [
        SourceRecord(
            source_id="src-1",
            title="Municipal price records of Rouen",
            author="City clerk",
            year="1421",
            source_type="record",
            locator_hint="folios 10-14",
            abstract="Bread prices rose sharply during the winter shortage.",
        ),
        SourceRecord(
            source_id="src-2",
            title="Later chronicle of unrest",
            author="Anonymous chronicler",
            year="1450",
            source_type="chronicle",
            locator_hint="chapter 7",
            abstract="Townspeople whispered that merchants were withholding grain.",
        ),
    ]


def _seed_text_units() -> list[TextUnit]:
    return [
        TextUnit(
            text_unit_id="txt-1",
            source_id="src-1",
            locator="folio 12r",
            text="Bread prices rose sharply during the winter shortage.",
            ordinal=1,
            checksum="seed-1",
        ),
        TextUnit(
            text_unit_id="txt-2",
            source_id="src-2",
            locator="chapter 7",
            text="Townspeople whispered that merchants were withholding grain.",
            ordinal=1,
            checksum="seed-2",
        ),
    ]


def _seed_evidence() -> list[EvidenceSnippet]:
    return [
        EvidenceSnippet(
            evidence_id="evi-1",
            source_id="src-1",
            locator="folio 12r",
            text="Bread prices rose sharply during the winter shortage.",
            notes="Economic record",
        ),
        EvidenceSnippet(
            evidence_id="evi-2",
            source_id="src-2",
            locator="chapter 7",
            text="Townspeople whispered that merchants were withholding grain.",
            notes="Later chronicle, lower certainty",
        ),
    ]


def _seed_candidates() -> list[CandidateClaim]:
    return [
        CandidateClaim(
            candidate_id="cand-1",
            subject="Rouen bread prices",
            predicate="rose_during",
            value="winter shortage",
            claim_kind=ClaimKind.PRACTICE,
            status_suggestion=ClaimStatus.PROBABLE,
            review_state=ReviewState.PENDING,
            place="Rouen",
            time_start="1421-12-01",
            time_end="1422-02-28",
            evidence_ids=["evi-1"],
            extractor_run_id="seed-run",
            notes="Derived from municipal records.",
        ),
        CandidateClaim(
            candidate_id="cand-2",
            subject="Merchant grain hoarding",
            predicate="rumored_in",
            value="Rouen",
            claim_kind=ClaimKind.BELIEF,
            status_suggestion=ClaimStatus.RUMOR,
            review_state=ReviewState.PENDING,
            place="Rouen",
            time_start="1422-01-01",
            time_end="1422-01-31",
            viewpoint_scope="townspeople",
            evidence_ids=["evi-2"],
            extractor_run_id="seed-run",
            notes="Rumor, not a verified economic fact.",
        ),
    ]


def _seed_run() -> ExtractionRun:
    return ExtractionRun(
        run_id="seed-run",
        status=ExtractionRunStatus.COMPLETED,
        source_count=2,
        text_unit_count=2,
        candidate_count=2,
        started_at="2026-04-12T00:00:00+00:00",
        completed_at="2026-04-12T00:00:00+00:00",
        notes="Seeded development run.",
    )


@app.command()
def serve(reload: bool = False) -> None:
    uvicorn.run(
        "source_aware_worldbuilding.api.main:app",
        host=settings.app_host,
        port=settings.app_port,
        reload=reload,
        factory=False,
    )


@app.command()
def seed_dev_data() -> None:
    data_dir = settings.app_data_dir
    sources = _seed_sources()
    text_units = _seed_text_units()
    evidence = _seed_evidence()
    candidates = _seed_candidates()
    claims: list[dict] = []
    extraction_runs = [_seed_run()]
    review_events: list[dict] = []

    _write_json(data_dir / "sources.json", [item.model_dump(mode="json") for item in sources])
    _write_json(data_dir / "text_units.json", [item.model_dump(mode="json") for item in text_units])
    _write_json(data_dir / "evidence.json", [item.model_dump(mode="json") for item in evidence])
    _write_json(data_dir / "candidates.json", [item.model_dump(mode="json") for item in candidates])
    _write_json(data_dir / "claims.json", claims)
    _write_json(
        data_dir / "extraction_runs.json",
        [item.model_dump(mode="json") for item in extraction_runs],
    )
    _write_json(data_dir / "review_events.json", review_events)

    if settings.app_state_backend == "postgres" or settings.app_truth_backend == "postgres":
        source_store = PostgresSourceStore(
            settings.app_postgres_dsn,
            settings.app_postgres_schema,
        )
        source_store.store.clear_all()
        source_store.save_sources(sources)
        PostgresTextUnitStore(
            settings.app_postgres_dsn,
            settings.app_postgres_schema,
        ).save_text_units(text_units)
        PostgresEvidenceStore(
            settings.app_postgres_dsn,
            settings.app_postgres_schema,
        ).save_evidence(evidence)
        PostgresCandidateStore(
            settings.app_postgres_dsn,
            settings.app_postgres_schema,
        ).save_candidates(candidates)
        PostgresExtractionRunStore(
            settings.app_postgres_dsn,
            settings.app_postgres_schema,
        ).save_run(extraction_runs[0])
        PostgresTruthStore(settings.app_postgres_dsn, settings.app_postgres_schema)
        PostgresReviewStore(settings.app_postgres_dsn, settings.app_postgres_schema)

    if settings.app_state_backend == "sqlite" or settings.app_truth_backend == "sqlite":
        if settings.app_sqlite_path.exists():
            settings.app_sqlite_path.unlink()
        SqliteSourceStore(settings.app_sqlite_path).save_sources(sources)
        SqliteTextUnitStore(settings.app_sqlite_path).save_text_units(text_units)
        SqliteEvidenceStore(settings.app_sqlite_path).save_evidence(evidence)
        SqliteCandidateStore(settings.app_sqlite_path).save_candidates(candidates)
        SqliteExtractionRunStore(settings.app_sqlite_path).save_run(extraction_runs[0])
        SqliteTruthStore(settings.app_sqlite_path)
        SqliteReviewStore(settings.app_sqlite_path)

    print(f"[green]Seeded development data in {data_dir}[/green]")


if __name__ == "__main__":
    app()
