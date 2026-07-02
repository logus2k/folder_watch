"""One pytest: dropping a matching file triggers exactly one emit with the right envelope.

The bus is mocked (a fake Publisher capturing xadd); no live Valkey is needed. We
drive the watcher's ``scan_once`` deterministically: first scan seeds the baseline,
then we drop a file and scan again — that must produce exactly one emit carrying
``event_data.record_uid`` on the farm stream with ``event_type="file.fired"``.
"""

from __future__ import annotations

import os

import pytest

from folder_watch import emitter
from folder_watch.config import settings
from folder_watch.envelope import EventEnvelope
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


async def test_new_matching_file_fires_exactly_one_emit(tmp_path, fake_bus):
    # A binding watching tmp_path for *.pdf, routed to a specific record.
    record_uid = "proj-uid-1234"
    store = BindingStore()
    store.create(
        BindingCreate(
            record_uid=record_uid,
            path=str(tmp_path),
            patterns=["*.pdf"],
        )
    )
    # Real emitter (goes through the fake publisher), real watcher over this store.
    w = Watcher(binding_store=store, emit=emitter.emit_file_event)

    # A pre-existing non-matching + matching file present at seed time must NOT fire.
    (tmp_path / "old.pdf").write_text("pre-existing")
    (tmp_path / "note.txt").write_text("ignored")
    assert await w.scan_once() == 0  # seed pass: no fires
    assert fake_bus.published == []

    # Drop a NEW matching file, and a NEW non-matching file.
    (tmp_path / "report.pdf").write_bytes(b"%PDF-1.4 hello")
    (tmp_path / "skip.log").write_text("nope")

    emitted = await w.scan_once()

    # Exactly one emit — only the *.pdf, only once.
    assert emitted == 1
    assert len(fake_bus.published) == 1

    stream, env = fake_bus.published[0]
    # Went to the farm stream (stream:agent-runtime).
    assert stream == settings.stream_key("agent-runtime")
    assert env.header.stream_id == "agent-runtime"
    assert env.header.event_type == "file.fired"
    assert env.header.sender == "folder_watch"
    # The routing key: event_data.record_uid is the deployed Project's record.
    assert env.payload.data == {"record_uid": record_uid}
    # Provenance in context; the fired file is the pdf we dropped.
    assert env.payload.context["file_path"] == os.path.join(str(tmp_path), "report.pdf")
    assert env.payload.context["change"] == "created"
    # Stream registered for bus discovery.
    assert fake_bus.active_streams == ["agent-runtime"]

    # Re-scanning with no changes fires nothing more (idempotent baseline).
    assert await w.scan_once() == 0
    assert len(fake_bus.published) == 1
