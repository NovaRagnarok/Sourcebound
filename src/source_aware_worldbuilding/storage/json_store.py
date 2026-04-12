from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class JsonListStore:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.path.write_text("[]", encoding="utf-8")

    def read_models(self, model_type: type[T]) -> list[T]:
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        return [model_type.model_validate(item) for item in payload]

    def write_models(self, items: Iterable[T]) -> None:
        payload = [item.model_dump(mode="json") for item in items]
        self.path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
