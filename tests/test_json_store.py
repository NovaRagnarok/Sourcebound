from __future__ import annotations

import threading
from pathlib import Path

from pydantic import BaseModel

from source_aware_worldbuilding.storage.json_store import JsonListStore


class ExampleModel(BaseModel):
    item_id: str


def test_json_list_store_reads_empty_file_as_empty_list(tmp_path: Path) -> None:
    path = tmp_path / "items.json"
    path.write_text("", encoding="utf-8")

    store = JsonListStore(path)

    assert store.read_models(ExampleModel) == []


def test_json_list_store_avoids_empty_reads_during_concurrent_access(tmp_path: Path) -> None:
    path = tmp_path / "items.json"
    store = JsonListStore(path)
    failures: list[Exception] = []
    finished = threading.Event()

    def writer() -> None:
        try:
            for index in range(200):
                JsonListStore(path).write_models(
                    [
                        ExampleModel(item_id=f"item-{index}"),
                        ExampleModel(item_id=f"item-{index + 1}"),
                    ]
                )
        except Exception as exc:  # pragma: no cover - test helper path
            failures.append(exc)
        finally:
            finished.set()

    def reader() -> None:
        try:
            while not finished.is_set():
                JsonListStore(path).read_models(ExampleModel)
        except Exception as exc:  # pragma: no cover - test helper path
            failures.append(exc)

    writer_thread = threading.Thread(target=writer)
    reader_thread = threading.Thread(target=reader)
    reader_thread.start()
    writer_thread.start()
    writer_thread.join()
    reader_thread.join()

    assert failures == []
    assert len(store.read_models(ExampleModel)) == 2
