from __future__ import annotations

from typer.testing import CliRunner

from source_aware_worldbuilding.cli import app as cli_app
from source_aware_worldbuilding.domain.models import IntakeResult, ZoteroCreatedItem

runner = CliRunner()


def test_cli_intake_text_emits_json(monkeypatch) -> None:
    class FakeIntakeService:
        def intake_text(self, payload):
            assert payload.title == "CLI source"
            return IntakeResult(
                created_item=ZoteroCreatedItem(
                    zotero_item_key="ITEM-1",
                    title="CLI source",
                    item_type="document",
                ),
                candidate_count=1,
                evidence_count=1,
            )

    monkeypatch.setattr("source_aware_worldbuilding.cli.get_intake_service", lambda: FakeIntakeService())

    result = runner.invoke(
        cli_app,
        ["intake-text", "CLI source", "hello world", "--json-output"],
    )

    assert result.exit_code == 0
    assert '"zotero_item_key": "ITEM-1"' in result.stdout
