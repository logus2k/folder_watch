"""Binding store — JSON-backed CRUD for watch bindings, the admin API's source of truth.

A **Binding** maps a watched folder/pattern to a deployed Project's runtime record uid;
when a matching file event fires, the watcher emits one bus event carrying
``event_data.record_uid`` so the farm runs that Project (Phase-05 routing).

Persistence mirrors ``http_ingress``: bindings live in a JSON file so a restart keeps
them (an in-memory dict silently lost every binding on reboot). The bus (event plane)
is the only thing that talks to Valkey — the config plane needs no live infra. When
``path`` is ``None`` the store is purely in-memory (used by unit tests).
"""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Optional

from .models import Binding, BindingCreate, BindingUpdate


class BindingStore:
    def __init__(self, path: str | Path | None = None) -> None:
        self._path: Path | None = Path(path) if path is not None else None
        self._lock = threading.RLock()
        self._bindings: dict[str, Binding] = {}
        if self._path is not None:
            self._load()

    # --- persistence ---

    def _load(self) -> None:
        assert self._path is not None
        if not self._path.exists():
            return
        raw = json.loads(self._path.read_text("utf-8") or "{}")
        with self._lock:
            self._bindings = {
                bid: Binding.model_validate(data) for bid, data in raw.items()
            }

    def _flush(self) -> None:
        if self._path is None:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        data = {bid: b.model_dump(mode="json") for bid, b in self._bindings.items()}
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, indent=2), "utf-8")
        tmp.replace(self._path)  # atomic

    # --- CRUD ---

    def create(self, data: BindingCreate) -> Binding:
        with self._lock:
            binding = Binding(**data.model_dump())
            self._bindings[binding.binding_id] = binding
            self._flush()
            return binding

    def get(self, binding_id: str) -> Optional[Binding]:
        with self._lock:
            return self._bindings.get(binding_id)

    def list(self) -> list[Binding]:
        with self._lock:
            return list(self._bindings.values())

    def update(self, binding_id: str, data: BindingUpdate) -> Optional[Binding]:
        with self._lock:
            binding = self._bindings.get(binding_id)
            if binding is None:
                return None
            patch = data.model_dump(exclude_unset=True)
            updated = binding.model_copy(update=patch)
            self._bindings[binding_id] = updated
            self._flush()
            return updated

    def delete(self, binding_id: str) -> bool:
        with self._lock:
            existed = self._bindings.pop(binding_id, None) is not None
            if existed:
                self._flush()
            return existed


from .config import settings  # noqa: E402  (import here to avoid a top-of-file cycle)

# Module-level singleton the admin API and watcher share — persisted per settings.
store = BindingStore(settings.bindings_path)
