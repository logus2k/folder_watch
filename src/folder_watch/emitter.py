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

import logging
import uuid
from typing import Any, Optional

from .bus_client import Publisher
from .config import settings
from agent_bus_client import new_event, now_iso

log = logging.getLogger("folder_watch.emitter")

# Process-global publisher, set in connect(); read by emit_file_event.
_publisher: Optional[Publisher] = None


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
) -> str:
    """Build and publish one file.fired EventEnvelope. Returns the entry id.

    ``event_data`` carries ONLY ``record_uid`` (the farm's routing key, §9.3);
    the file details are provenance and live in ``payload.context``.
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

        # Workflow-firing contract (agent-bus-client fired_event / seed_of): `data` carries
        # the routing key AND the SEED. For a file trigger the seed is the file path (the
        # workflow acts on "a file arrived at X"; reading its contents is a downstream step).
        # Provenance stays in `context`. Without `task`, the graph starts from an empty message.
        env = new_event(
            stream_id=stream_id,
            cid=cid,
            sid=sid,
            sender=settings.sender_id,
            event_type=settings.event_type,
            data={"record_uid": record_uid, "task": file_path},
            context=context,
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
