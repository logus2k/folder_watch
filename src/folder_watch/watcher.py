"""Watcher — trigger detection.

A portable polling scanner: each tick it lists the files under every enabled
binding's folder and compares each matching file's ``(mtime, size)`` signature to
the previous scan. A file that is **new** (not seen before) or **changed** (signature
differs, only if ``on_modified``) fires exactly one bus event for that binding via
``emitter.emit_file_event``.

Polling (not inotify) is chosen to keep the service dependency-light and trivially
testable — a single ``scan_once`` call is deterministic and needs no OS event loop.
The first scan of a binding **seeds** its baseline without firing, so pre-existing
files don't stampede on startup.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Awaitable, Callable, Optional

from .config import settings
from .emitter import emit_file_event
from .models import Binding
from .store import BindingStore, store as default_store

log = logging.getLogger("folder_watch.watcher")

# Signature of one file: (mtime_ns, size). Changing either is a "change".
Signature = tuple[int, int]

# The emit hook — swappable in tests. Signature mirrors emitter.emit_file_event kwargs.
EmitFn = Callable[..., Awaitable[str]]


class Watcher:
    def __init__(
        self,
        binding_store: BindingStore = default_store,
        emit: EmitFn = emit_file_event,
    ) -> None:
        self._store = binding_store
        self._emit = emit
        # Per-binding baseline: binding_id -> {abs_path: signature}. A binding that
        # has never been scanned is absent, which triggers a no-fire seed.
        self._baseline: dict[str, dict[str, Signature]] = {}
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    def _snapshot(self, binding: Binding) -> dict[str, Signature]:
        """Signatures of all matching files currently in the binding's folder."""
        snap: dict[str, Signature] = {}
        folder = binding.path
        if not os.path.isdir(folder):
            return snap
        for name in os.listdir(folder):
            if not binding.matches(name):
                continue
            abs_path = os.path.join(folder, name)
            if not os.path.isfile(abs_path):
                continue
            st = os.stat(abs_path)
            snap[abs_path] = (st.st_mtime_ns, st.st_size)
        return snap

    async def scan_once(self) -> int:
        """One scan pass over all enabled bindings. Returns the number of emits.

        First scan of a binding seeds its baseline WITHOUT firing (pre-existing
        files are not "new"). Subsequent scans fire on new / changed files.
        """
        emitted = 0
        for binding in self._store.list():
            if not binding.enabled:
                continue
            snap = self._snapshot(binding)
            prev = self._baseline.get(binding.binding_id)
            if prev is None:
                # Seed baseline; do not fire on pre-existing files.
                self._baseline[binding.binding_id] = snap
                continue
            for abs_path, sig in snap.items():
                old = prev.get(abs_path)
                if old is None:
                    change = "created"
                elif old != sig and binding.on_modified:
                    change = "modified"
                else:
                    continue
                emitted += 1
                await self._emit(
                    record_uid=binding.record_uid,
                    binding_id=binding.binding_id,
                    file_path=abs_path,
                    change=change,
                )
            self._baseline[binding.binding_id] = snap
        return emitted

    def forget(self, binding_id: str) -> None:
        """Drop a binding's baseline (e.g. on delete) so a re-create re-seeds."""
        self._baseline.pop(binding_id, None)

    async def run(self) -> None:
        """Poll loop until ``stop()``."""
        self._stop.clear()
        log.info("watcher started (poll=%.1fs)", settings.poll_interval_s)
        while not self._stop.is_set():
            try:
                await self.scan_once()
            except Exception:
                # A scan error must be loud but must not kill the loop.
                log.error("scan pass failed", exc_info=True)
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=settings.poll_interval_s
                )
            except asyncio.TimeoutError:
                pass
        log.info("watcher stopped")

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self.run())

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            await self._task
            self._task = None


watcher = Watcher()
