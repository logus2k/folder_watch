"""Emitter — builds and publishes the one bus event a file trigger fires.

``emit_file_event`` mirrors agent_scheduler's ``emit_scheduled_event``: each fire
is a fresh workflow with a new ``cid`` (uuid4) and ``sid`` (INCR sid:<cid>),
``sender=folder_watch``, and file provenance in ``payload.context``.

Per §9.3.1 the event goes to the farm stream (``target_stream_id="agent-runtime"``)
with ``event_type="file.fired"`` and ``event_data={"record_uid": <project uid>}``
so the farm resolves it against the deployed GraphRecord (Phase-05 routing).

The publisher is a process global, established once at startup via ``connect()``.
"""

from __future__ import annotations

import base64
import logging
import os
import uuid
from typing import Any, Optional

from .bus_client import Publisher
from .config import settings
from agent_bus_client import new_event, now_iso

log = logging.getLogger("folder_watch.emitter")

# Process-global publisher, set in connect(); read by emit_file_event.
_publisher: Optional[Publisher] = None

# Cap the file content folded into the workflow seed so a huge drop can't blow up the
# event / the downstream LLM context. Text past this is truncated with a marker.
_MAX_SEED_BYTES = 200_000


def read_seed(file_path: str, max_bytes: int = _MAX_SEED_BYTES) -> str:
    """The workflow SEED for a file trigger = the file's TEXT CONTENT (so the Agent acts on
    what's IN the file, not just its path). Binary/undecodable files fall back to a clear
    marker (never garbage); an unreadable file falls back to the path. Size-capped."""
    try:
        with open(file_path, "rb") as fh:
            raw = fh.read(max_bytes + 1)
    except OSError:
        return file_path  # can't read it → path is the best we can do (loud downstream)
    truncated = len(raw) > max_bytes
    raw = raw[:max_bytes]
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return f"[binary file {file_path}: {len(raw)} bytes, not UTF-8 text]"
    if truncated:
        text += f"\n[...truncated at {max_bytes} bytes]"
    return text


class SeedTooLarge(RuntimeError):
    """A content seed above the binding's cap. Raised rather than truncated: half a
    PDF is not a document, and silently shipping one is worse than not firing."""


def build_seed(file_path: str, change: str, emit: str = "path",
               max_content_mb: int = 64) -> str:
    """The seed, per the binding's ``emit``.

    * ``path``    → the file path. What a consumer that reads the bytes itself wants
                    (an Ingestion block: the Agent has the same mount, so base64ing
                    a PDF through the bus to it is pure overhead).
    * ``content`` → the file's text, or BASE64 for binary. Not a marker string: a
                    marker is indistinguishable from data downstream and fails
                    silently, which is exactly how a PDF drop looked like a
                    successful ingest of the literal text "[binary file …]".

    A DELETE has no content to read, so its seed is always the path regardless of
    ``emit`` — the consumer must branch on ``context.change`` rather than trust the
    seed's shape.
    """
    if change == "deleted" or emit == "path":
        return file_path

    cap = max(1, int(max_content_mb)) * 1024 * 1024
    try:
        size = os.path.getsize(file_path)
    except OSError:
        return file_path
    if size > cap:
        raise SeedTooLarge(
            f"{file_path}: {size / 1048576:.1f}MB exceeds the binding's "
            f"max_content_mb={max_content_mb}")
    try:
        with open(file_path, "rb") as fh:
            raw = fh.read()
    except OSError:
        return file_path
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        # Binary + content → the file ITSELF, base64. The bus is Valkey streams —
        # in-memory and retained — so this is ~1.33x the file on the wire, which is
        # why the cap is per-binding and configurable.
        return base64.b64encode(raw).decode("ascii")


async def connect() -> Publisher:
    """Establish the shared publisher (idempotent)."""
    global _publisher
    if _publisher is None:
        _publisher = await Publisher.create(settings)
    return _publisher


async def close() -> None:
    global _publisher
    if _publisher is not None:
        await _publisher.close()
        _publisher = None


def is_connected() -> bool:
    return _publisher is not None


async def ping() -> bool:
    return _publisher is not None and await _publisher.ping()


def set_publisher(publisher: Optional[Publisher]) -> None:
    """Inject a publisher (used by tests to mock the bus)."""
    global _publisher
    _publisher = publisher


async def emit_file_event(
    *,
    record_uid: str,
    binding_id: str,
    file_path: str,
    change: str,
    emit: str = "content",
    max_content_mb: int = 64,
) -> str:
    """Build and publish one file.fired EventEnvelope. Returns the entry id.

    ``event_data`` carries ONLY ``record_uid`` (the farm's routing key, §9.3);
    the file details are provenance and live in ``payload.context``.

    ``emit`` defaults to ``content`` so bindings created before the field existed
    keep their behaviour exactly.
    """
    if _publisher is None:
        raise RuntimeError("publisher not connected; call connect() at startup")

    stream_id = settings.target_stream_id
    stream_key = settings.stream_key(stream_id)

    try:
        cid = str(uuid.uuid4())
        sid = await _publisher.incr(f"sid:{cid}")
        # Bound the per-fire counter key; set before publish so a later failure
        # can't leave a no-TTL orphan (matches the scheduler's discipline).
        await _publisher.expire(f"sid:{cid}", settings.sid_ttl_s)

        context: dict[str, Any] = {
            "binding_id": binding_id,
            "file_path": file_path,
            "change": change,
            "fired_at": now_iso(),
        }

        # Workflow-firing contract (agent-bus-client fired_event / seed_of): `data`
        # carries the routing key AND the SEED. What the seed IS depends on the
        # binding's `emit`: the file's content (an Agent acts on what's in it) or
        # its path (an Ingestion block reads the bytes off the same mount). The
        # path and the change type ALWAYS stay in `context` — a consumer must be
        # able to tell an ingest from a removal without guessing at the seed's
        # shape. Without `task`, the graph starts from an empty message.
        env = new_event(
            stream_id=stream_id,
            cid=cid,
            sid=sid,
            sender=settings.sender_id,
            event_type=settings.event_type,
            data={"record_uid": record_uid,
                  "task": build_seed(file_path, change, emit, max_content_mb)},
            context={**context, "emit": emit},
        )

        # Register the stream so agent_bus discovery/observers/reaper see it.
        await _publisher.sadd(settings.active_streams_key, stream_id)
        entry_id = await _publisher.publish(stream_key, env)
        log.info(
            "fired binding=%s file=%s -> %s entry=%s cid=%s record_uid=%s",
            binding_id, file_path, stream_key, entry_id, cid, record_uid,
        )
        return entry_id
    except Exception:
        # Make a missed fire unmistakable rather than silent.
        log.error(
            "emit FAILED binding=%s file=%s record_uid=%s",
            binding_id, file_path, record_uid, exc_info=True,
        )
        raise
