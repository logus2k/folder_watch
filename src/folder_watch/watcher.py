"""Watcher — trigger detection via ``watchfiles`` (Rust ``notify`` backend).

Watches the folders named by the enabled bindings and fires exactly one bus event
(``emitter.emit_file_event``) per matching **new or modified** file. watchfiles uses
native OS notifications where they work and **auto-falls back to polling** where they
don't — importantly it auto-enables polling when **WSL is detected** or native events
are unavailable (Docker bind mounts / network FS), which is exactly our deployment.
That removes the reliability foot-gun of raw inotify while keeping sub-second latency
where the OS supports it. Set ``WATCHFILES_FORCE_POLLING=true`` to force polling.

Only files created/modified *after* the watch starts fire — pre-existing files never
stampede on startup (no baseline bookkeeping needed; watchfiles yields only changes).
Deletes are ignored (the initiator's contract is "a file arrived/changed"). Bindings
are runtime-mutable: an admin CRUD calls ``reconfigure()`` to restart the watch over the
new set of folders.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Awaitable, Callable, Optional

from watchfiles import Change, awatch

from .emitter import emit_file_event
from .store import BindingStore, store as default_store

log = logging.getLogger("folder_watch.watcher")

# The emit hook — swappable in tests. Signature mirrors emitter.emit_file_event kwargs.
EmitFn = Callable[..., Awaitable[str]]

# watchfiles Change -> our envelope's change label. Deletes are dropped (not a trigger).
_CHANGE_LABEL = {Change.added: "created", Change.modified: "modified"}


class Watcher:
    def __init__(
        self,
        binding_store: BindingStore = default_store,
        emit: EmitFn = emit_file_event,
    ) -> None:
        self._store = binding_store
        self._emit = emit
        self._task: Optional[asyncio.Task] = None
        # Set to break the current awatch: on a binding CRUD (reconfigure) or on stop.
        self._interrupt = asyncio.Event()
        self._stopping = False

    def _watch_paths(self) -> list[str]:
        """Distinct existing folders named by enabled bindings."""
        paths = {
            b.path for b in self._store.list()
            if b.enabled and b.path and os.path.isdir(b.path)
        }
        return sorted(paths)

    async def handle_changes(self, changes) -> int:
        """Fire one bus event per (binding, matching new/modified file). Pure w.r.t. the
        filesystem — driven by watchfiles change tuples, so it is deterministically
        unit-testable. Returns the number of emits."""
        emitted = 0
        for change, fpath in changes:
            label = _CHANGE_LABEL.get(change)
            if label is None:  # deleted -> not a trigger
                continue
            folder = os.path.normpath(os.path.dirname(fpath))
            name = os.path.basename(fpath)
            for b in self._store.list():
                if not b.enabled:
                    continue
                if os.path.normpath(b.path) != folder:
                    continue
                if not b.matches(name):
                    continue
                if label == "modified" and not b.on_modified:
                    continue
                await self._emit(
                    record_uid=b.record_uid,
                    binding_id=b.binding_id,
                    file_path=fpath,
                    change=label,
                )
                emitted += 1
        return emitted

    async def run(self) -> None:
        """Watch loop until ``stop()``. Restarts the underlying awatch whenever the set of
        watched folders changes (a binding was added/removed/edited)."""
        log.info("watcher started (watchfiles %s)", _wf_version())
        while not self._stopping:
            self._interrupt.clear()
            paths = self._watch_paths()
            if not paths:
                # Nothing to watch yet — wait for a reconfigure (binding added) or stop.
                await self._interrupt.wait()
                continue
            log.info("watching %d folder(s): %s", len(paths), paths)
            try:
                # recursive=False: fire only on files directly in a bound folder (the
                # binding's patterns match the basename), matching the block's contract.
                async for changes in awatch(
                    *paths, stop_event=self._interrupt, recursive=False
                ):
                    await self.handle_changes(changes)
            except Exception:
                # A watch error must be loud but must not kill the loop.
                log.error("watch loop failed; retrying", exc_info=True)
                await asyncio.sleep(1)
        log.info("watcher stopped")

    def reconfigure(self) -> None:
        """Signal that the binding set changed — restart the watch over the new folders."""
        self._interrupt.set()

    # Back-compat alias: a binding edit/delete used to "forget" a baseline; now any binding
    # change simply reconfigures the watch (watchfiles keeps no per-file baseline).
    def forget(self, binding_id: str) -> None:  # noqa: ARG002 - id unused, kept for the API
        self.reconfigure()

    def start(self) -> None:
        if self._task is None:
            self._stopping = False
            self._task = asyncio.create_task(self.run())

    async def stop(self) -> None:
        self._stopping = True
        self._interrupt.set()
        if self._task is not None:
            await self._task
            self._task = None


def _wf_version() -> str:
    try:
        import watchfiles
        return watchfiles.__version__
    except Exception:  # pragma: no cover
        return "?"


watcher = Watcher()
