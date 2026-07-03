"""Trigger detection: a matching new/modified file fires exactly one emit with the right
envelope. The bus is mocked (a fake Publisher capturing xadd); no live Valkey is needed.

The watcher now uses ``watchfiles`` (native events + auto-poll fallback), so instead of
scanning the filesystem we drive its pure ``handle_changes`` with synthetic watchfiles
change tuples — deterministic, no FS timing. (A real end-to-end file-drop is verified live
against the container over the bind mount, not here.)
"""

from __future__ import annotations

import os

import pytest
from watchfiles import Change

from folder_watch import emitter
from folder_watch.config import settings
from agent_bus_client import EventEnvelope
from folder_watch.models import BindingCreate
from folder_watch.store import BindingStore
from folder_watch.watcher import Watcher


class FakePublisher:
    """Captures what the real glide Publisher would XADD to the bus."""

    def __init__(self) -> None:
        self.sid = 0
        self.expired: list[tuple[str, int]] = []
        self.active_streams: list[str] = []
        self.published: list[tuple[str, EventEnvelope]] = []

    async def incr(self, key: str) -> int:
        self.sid += 1
        return self.sid

    async def expire(self, key: str, seconds: int) -> None:
        self.expired.append((key, seconds))

    async def sadd(self, key: str, member: str) -> None:
        self.active_streams.append(member)

    async def publish(self, stream: str, env: EventEnvelope) -> str:
        self.published.append((stream, env))
        return f"0-{len(self.published)}"


@pytest.fixture
def fake_bus():
    fake = FakePublisher()
    emitter.set_publisher(fake)
    yield fake
    emitter.set_publisher(None)


def _store_with_pdf_binding(folder: str, record_uid: str = "proj-uid-1234") -> BindingStore:
    store = BindingStore()
    store.create(BindingCreate(record_uid=record_uid, path=folder, patterns=["*.pdf"]))
    return store


async def test_new_matching_file_fires_exactly_one_emit(tmp_path, fake_bus):
    record_uid = "proj-uid-1234"
    store = _store_with_pdf_binding(str(tmp_path), record_uid)
    w = Watcher(binding_store=store, emit=emitter.emit_file_event)

    # A batch as watchfiles would yield it: one matching *.pdf added + one non-matching.
    changes = {
        (Change.added, os.path.join(str(tmp_path), "report.pdf")),
        (Change.added, os.path.join(str(tmp_path), "skip.log")),
    }
    emitted = await w.handle_changes(changes)

    assert emitted == 1                       # only the *.pdf, only once
    assert len(fake_bus.published) == 1
    stream, env = fake_bus.published[0]
    assert stream == settings.stream_key("agent-runtime")
    assert env.header.stream_id == "agent-runtime"
    assert env.header.event_type == "file.fired"
    assert env.header.sender == "folder_watch"
    # Firing contract: data carries the routing key AND the seed (file path is the task).
    assert env.payload.data == {
        "record_uid": record_uid,
        "task": os.path.join(str(tmp_path), "report.pdf"),
    }
    assert env.payload.context["file_path"] == os.path.join(str(tmp_path), "report.pdf")
    assert env.payload.context["change"] == "created"
    assert fake_bus.active_streams == ["agent-runtime"]


async def test_deleted_file_does_not_fire(tmp_path, fake_bus):
    store = _store_with_pdf_binding(str(tmp_path))
    w = Watcher(binding_store=store, emit=emitter.emit_file_event)
    n = await w.handle_changes({(Change.deleted, os.path.join(str(tmp_path), "report.pdf"))})
    assert n == 0
    assert fake_bus.published == []


async def test_modified_respects_on_modified_flag(tmp_path, fake_bus):
    store = BindingStore()
    store.create(BindingCreate(record_uid="r", path=str(tmp_path),
                               patterns=["*.pdf"], on_modified=False))
    w = Watcher(binding_store=store, emit=emitter.emit_file_event)
    # on_modified=False -> a modify must NOT fire; an add still would.
    assert await w.handle_changes({(Change.modified, os.path.join(str(tmp_path), "a.pdf"))}) == 0
    assert await w.handle_changes({(Change.added, os.path.join(str(tmp_path), "a.pdf"))}) == 1


async def test_file_in_other_folder_does_not_fire(tmp_path, fake_bus):
    store = _store_with_pdf_binding(str(tmp_path))
    w = Watcher(binding_store=store, emit=emitter.emit_file_event)
    # A matching name but under a DIFFERENT folder — not this binding's watch path.
    n = await w.handle_changes({(Change.added, "/somewhere/else/report.pdf")})
    assert n == 0
