from __future__ import annotations

import json
from pathlib import Path

import typer
import uvicorn
from rich import print

from source_aware_worldbuilding.api.main import app as fastapi_app
from source_aware_worldbuilding.settings import settings

app = typer.Typer(help="Source-Aware Worldbuilding CLI")


def _write_json(path: Path, payload: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


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
    sources = [
        {
            "source_id": "src-1",
            "title": "Municipal price records of Rouen",
            "author": "City clerk",
            "year": "1421",
            "source_type": "record",
            "locator_hint": "folios 10-14",
            "zotero_item_key": None,
        },
        {
            "source_id": "src-2",
            "title": "Later chronicle of unrest",
            "author": "Anonymous chronicler",
            "year": "1450",
            "source_type": "chronicle",
            "locator_hint": "chapter 7",
            "zotero_item_key": None,
        },
    ]
    evidence = [
        {
            "evidence_id": "evi-1",
            "source_id": "src-1",
            "locator": "folio 12r",
            "text": "Bread prices rose sharply during the winter shortage.",
            "notes": "Economic record",
            "checksum": None,
        },
        {
            "evidence_id": "evi-2",
            "source_id": "src-2",
            "locator": "chapter 7",
            "text": "Townspeople whispered that merchants were withholding grain.",
            "notes": "Later chronicle, lower certainty",
            "checksum": None,
        },
    ]
    candidates = [
        {
            "candidate_id": "cand-1",
            "subject": "Rouen bread prices",
            "predicate": "rose_during",
            "value": "winter shortage",
            "claim_kind": "practice",
            "status_suggestion": "probable",
            "review_state": "pending",
            "place": "Rouen",
            "time_start": "1421-12-01",
            "time_end": "1422-02-28",
            "viewpoint_scope": None,
            "evidence_ids": ["evi-1"],
            "extractor_run_id": "seed-run",
            "notes": "Derived from municipal records.",
        },
        {
            "candidate_id": "cand-2",
            "subject": "Merchant grain hoarding",
            "predicate": "rumored_in",
            "value": "Rouen",
            "claim_kind": "belief",
            "status_suggestion": "rumor",
            "review_state": "pending",
            "place": "Rouen",
            "time_start": "1422-01-01",
            "time_end": "1422-01-31",
            "viewpoint_scope": "townspeople",
            "evidence_ids": ["evi-2"],
            "extractor_run_id": "seed-run",
            "notes": "Rumor, not a verified economic fact.",
        },
    ]
    claims = []

    _write_json(data_dir / "sources.json", sources)
    _write_json(data_dir / "evidence.json", evidence)
    _write_json(data_dir / "candidates.json", candidates)
    _write_json(data_dir / "claims.json", claims)
    print(f"[green]Seeded development data in {data_dir}[/green]")


if __name__ == "__main__":
    app()
