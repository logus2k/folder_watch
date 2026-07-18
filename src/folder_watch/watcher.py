"""Watcher — trigger detection via ``watchfiles`` (Rust ``notify`` backend).

Watches the folders named by the enabled bindings and fires exactly one bus event
(``emitter.emit_file_event``) per matching **new or modified** file.

Backend (measured 2026-07-06, do not re-derive from folklore): watchfiles' auto-fallback
does NOT detect "native events unavailable" — ``_auto_force_polling()`` only checks for a
WSL kernel (``'microsoft-standard' in uname.release``), so it force-polls on ANY WSL host,
ext4 included. Polling costs ~250ms vs ~50ms for inotify. The fallback exists because
inotify is dead on Windows-backed ``/mnt/*`` (9p) mounts — measured: ZERO events. We watch
LINUX paths only (``./watched`` is ext4), where inotify is verified working end-to-end
through the Docker bind mount, so compose sets ``WATCHFILES_FORCE_POLLING=false``.
⚠️ If a watched folder is ever moved onto ``/mnt/c`` (or any 9p/network FS), drop that env
var or the watcher goes SILENT. Note the backends differ: an atomic replace (editor save)
reports ``modified`` under polling but ``added`` under inotify.

Only files created/modified *after* the watch starts fire — pre-existing files never
stampede on startup (no baseline bookkeeping needed; watchfiles yields only changes).
Deletes fire only for bindings that opt in via ``on_deleted`` (default False, preserving
the initiator's original "a file arrived/changed" contract). Bindings are runtime-mutable:
an admin CRUD calls ``reconfigure()`` to restart the watch over the new set of folders.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Awaitable, Callable, Optional

from watchfiles import Change, awatch

from .emitter import emit_file_event, SeedTooLarge
from .store import BindingStore, store as default_store

log = logging.getLogger("folder_watch.watcher")

# The emit hook — swappable in tests. Signature mirrors emitter.emit_file_event kwargs.
EmitFn = Callable[..., Awaitable[str]]

# watchfiles Change -> our envelope's change label. Deletes are gated per-binding by
# `on_deleted` (default False), so they only fire when a binding opts in.
_CHANGE_LABEL = {
    Change.added: "created",
    Change.modified: "modified",
    Change.deleted: "deleted",
}


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
            if label is None:  # unknown change kind -> not a trigger
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
                if label == "deleted" and not b.on_deleted:
                    continue
                try:
                    await self._emit(
                        record_uid=b.record_uid,
                        binding_id=b.binding_id,
                        file_path=fpath,
                        change=label,
                        emit=getattr(b, "emit", "content"),
                        max_content_mb=getattr(b, "max_content_mb", 64),
                    )
                except SeedTooLarge as e:
                    # Do not fire a truncated document. Half a file is not the file,
                    # and a consumer cannot tell the difference — better a loud miss
                    # than a silent corruption of the corpus.
                    log.error("binding=%s NOT fired: %s", b.binding_id, e)
                    continue
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
