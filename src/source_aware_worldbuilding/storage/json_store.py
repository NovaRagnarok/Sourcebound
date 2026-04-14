from __future__ import annotations

import json
import os
import threading
from collections.abc import Iterable
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class JsonListStore:
    _locks: dict[Path, threading.RLock] = {}
    _locks_guard = threading.Lock()

    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.path.write_text("[]", encoding="utf-8")

    def read_models(self, model_type: type[T]) -> list[T]:
        with self._lock():
            raw = self.path.read_text(encoding="utf-8")
        if not raw.strip():
            payload = []
        else:
            payload = json.loads(raw)
        return [model_type.model_validate(item) for item in payload]

    def write_models(self, items: Iterable[T]) -> None:
        payload = [item.model_dump(mode="json") for item in items]
        serialized = json.dumps(payload, indent=2)
        with self._lock():
            with NamedTemporaryFile(
                "w",
                encoding="utf-8",
                dir=self.path.parent,
                prefix=f".{self.path.name}.",
                suffix=".tmp",
                delete=False,
            ) as handle:
                handle.write(serialized)
                temp_path = Path(handle.name)
            os.replace(temp_path, self.path)

    def _lock(self) -> threading.RLock:
        key = self.path.resolve()
        with self._locks_guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.RLock()
                self._locks[key] = lock
        return lock
