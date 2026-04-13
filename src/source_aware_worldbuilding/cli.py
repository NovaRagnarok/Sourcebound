from __future__ import annotations

import json
from pathlib import Path

import typer
import uvicorn
from rich import print
from rich.table import Table

from source_aware_worldbuilding.adapters.postgres_backed import (
    PostgresCandidateStore,
    PostgresEvidenceStore,
    PostgresExtractionRunStore,
    PostgresReviewStore,
    PostgresSourceStore,
    PostgresTextUnitStore,
)
from source_aware_worldbuilding.adapters.sqlite_backed import (
    SqliteCandidateStore,
    SqliteEvidenceStore,
    SqliteExtractionRunStore,
    SqliteReviewStore,
    SqliteSourceStore,
    SqliteTextUnitStore,
)
from source_aware_worldbuilding.adapters.zotero_adapter import ZoteroCorpusAdapter
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
    RuntimeStatus,
    SourceRecord,
    TextUnit,
)
from source_aware_worldbuilding.services.status import build_runtime_status
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
            text_unit_id="txt-1",
            span_start=0,
            span_end=48,
            notes="Economic record",
        ),
        EvidenceSnippet(
            evidence_id="evi-2",
            source_id="src-2",
            locator="chapter 7",
            text="Townspeople whispered that merchants were withholding grain.",
            text_unit_id="txt-2",
            span_start=0,
            span_end=59,
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
    extraction_runs = [_seed_run()]
    review_events: list[dict] = []

    _write_json(data_dir / "sources.json", [item.model_dump(mode="json") for item in sources])
    _write_json(data_dir / "text_units.json", [item.model_dump(mode="json") for item in text_units])
    _write_json(data_dir / "evidence.json", [item.model_dump(mode="json") for item in evidence])
    _write_json(data_dir / "candidates.json", [item.model_dump(mode="json") for item in candidates])
    _write_json(
        data_dir / "extraction_runs.json",
        [item.model_dump(mode="json") for item in extraction_runs],
    )
    _write_json(data_dir / "review_events.json", review_events)
    _write_json(data_dir / "claims.json", [])

    if settings.app_state_backend == "postgres":
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
        PostgresReviewStore(settings.app_postgres_dsn, settings.app_postgres_schema)

    if settings.app_state_backend == "sqlite":
        if settings.app_sqlite_path.exists():
            settings.app_sqlite_path.unlink()
        SqliteSourceStore(settings.app_sqlite_path).save_sources(sources)
        SqliteTextUnitStore(settings.app_sqlite_path).save_text_units(text_units)
        SqliteEvidenceStore(settings.app_sqlite_path).save_evidence(evidence)
        SqliteCandidateStore(settings.app_sqlite_path).save_candidates(candidates)
        SqliteExtractionRunStore(settings.app_sqlite_path).save_run(extraction_runs[0])
        SqliteReviewStore(settings.app_sqlite_path)

    print(f"[green]Seeded development data in {data_dir}[/green]")


@app.command()
def status(json_output: bool = False) -> None:
    runtime_status = build_runtime_status()
    if json_output:
        typer.echo(json.dumps(runtime_status.model_dump(mode="json"), indent=2))
        return

    _print_runtime_status(runtime_status)


def _print_runtime_status(runtime_status: RuntimeStatus) -> None:
    print(
        f"[bold]{runtime_status.app_name}[/bold] "
        f"({runtime_status.app_env}) - overall status: "
        f"[cyan]{runtime_status.overall_status}[/cyan]"
    )
    print(
        "State backend: "
        f"{runtime_status.state_backend} | Truth backend: {runtime_status.truth_backend} | "
        f"Extraction: {runtime_status.extraction_backend} | "
        f"Operator UI: {'enabled' if runtime_status.operator_ui_enabled else 'disabled'}"
    )

    table = Table(title="Runtime Services")
    table.add_column("Name")
    table.add_column("Mode")
    table.add_column("Ready")
    table.add_column("Detail")

    for service in runtime_status.services:
        ready_label = "yes" if service.ready else "no"
        table.add_row(service.name, service.mode, ready_label, service.detail)
    print(table)

    if runtime_status.next_steps:
        print("[bold]Next Steps[/bold]")
        for step in runtime_status.next_steps:
            print(f"- {step}")


@app.command("zotero-check")
def zotero_check(
    json_output: bool = False,
    source_limit: int = 3,
    include_text_units: bool = True,
) -> None:
    report = _build_zotero_report(
        source_limit=max(1, source_limit),
        include_text_units=include_text_units,
    )
    if json_output:
        typer.echo(json.dumps(report, indent=2))
        return

    _print_zotero_report(report)


def _build_zotero_report(*, source_limit: int, include_text_units: bool) -> dict:
    missing: list[str] = []
    if not settings.zotero_library_id:
        missing.append("ZOTERO_LIBRARY_ID")

    report: dict = {
        "configured": not missing,
        "library_type": settings.zotero_library_type,
        "library_id_present": bool(settings.zotero_library_id),
        "collection_key_present": bool(settings.zotero_collection_key),
        "api_key_present": bool(settings.zotero_api_key),
        "base_url": settings.zotero_base_url,
        "source_limit": source_limit,
        "include_text_units": include_text_units,
        "missing": missing,
        "success": False,
        "source_count": 0,
        "text_unit_count": 0,
        "sources_preview": [],
        "text_units_preview": [],
    }
    if missing:
        report["detail"] = "Zotero is not configured yet."
        return report

    adapter = ZoteroCorpusAdapter()
    try:
        sources = adapter.pull_sources()
        report["source_count"] = len(sources)
        report["sources_preview"] = [
            source.model_dump(mode="json") for source in sources[:source_limit]
        ]

        if include_text_units and sources:
            text_units = adapter.pull_text_units(sources[:1])
            report["text_unit_count"] = len(text_units)
            report["text_units_preview"] = [
                text_unit.model_dump(mode="json") for text_unit in text_units[:source_limit]
            ]

        report["success"] = True
        report["detail"] = "Zotero pull succeeded."
        return report
    except Exception as exc:
        report["detail"] = f"Zotero pull failed: {exc}"
        return report


def _print_zotero_report(report: dict) -> None:
    print("[bold]Zotero Check[/bold]")
    if report["configured"]:
        print(
            "Library type: "
            f"{report['library_type']} | "
            f"Collection key set: {'yes' if report['collection_key_present'] else 'no'} | "
            f"API key set: {'yes' if report['api_key_present'] else 'no'}"
        )
    else:
        missing_line = (
            "Missing: " + ", ".join(report["missing"])
            if report["missing"]
            else "Zotero config present."
        )
        print(missing_line)

    print(report["detail"])

    if report["success"]:
        print(
            f"Sources pulled: {report['source_count']} | "
            f"Text units previewed: {report['text_unit_count']}"
        )
        preview_table = Table(title="Zotero Source Preview")
        preview_table.add_column("Source ID")
        preview_table.add_column("Title")
        preview_table.add_column("Author")
        preview_table.add_column("Year")
        for source in report["sources_preview"]:
            preview_table.add_row(
                source["source_id"],
                source["title"],
                source.get("author") or "n/a",
                source.get("year") or "n/a",
            )
        print(preview_table)


if __name__ == "__main__":
    app()
