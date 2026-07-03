"""Publisher — the only module that talks to valkey-glide directly.

A deliberately thin slice of agent_bus's EventBus: a trigger actor only ever
*produces*, so we expose just what an emitter needs — publish (XADD), incr (the
sid counter), sadd (active-streams registration), plus ping/connect/close. The
wire format is the vendored ``EventEnvelope.to_fields()``, byte-identical to
agent_bus, so consumers can't tell a folder-watch event from any other.

Reuses agent_bus's client choice (valkey-glide) — the same pattern agent_scheduler
uses — so emission stays on the same wire client as the rest of the bus.
"""

from __future__ import annotations

import asyncio
import logging

from glide import (
    GlideClient,
    GlideClientConfiguration,
    NodeAddress,
    StreamAddOptions,
    TrimByMaxLen,
)

from .config import Settings, settings as default_settings
from agent_bus_client import EventEnvelope

log = logging.getLogger("folder_watch.bus")


def _s(value) -> str:
    """Decode glide's bytes results to str (pass through str/None)."""
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8")
    return value


class Publisher:
    """Async glide facade scoped to what a producer needs."""

    def __init__(self, client: GlideClient, settings: Settings):
        self._client = client
        self._settings = settings

    @classmethod
    async def create(cls, settings: Settings = default_settings) -> "Publisher":
        """Connect with a bounded retry loop so startup is forgiving when
        valkey-bus (owned by the agent_bus compose project) comes up late."""
        config = GlideClientConfiguration(
            [NodeAddress(settings.valkey_host, settings.valkey_port)]
        )
        last_exc: Exception | None = None
        for attempt in range(1, settings.connect_retries + 1):
            try:
                client = await GlideClient.create(config)
                log.info(
                    "Publisher connected to %s:%s",
                    settings.valkey_host,
                    settings.valkey_port,
                )
                return cls(client, settings)
            except Exception as exc:  # noqa: BLE001 - retry any connection failure
                last_exc = exc
                log.warning(
                    "valkey-bus not reachable (attempt %d/%d): %s",
                    attempt,
                    settings.connect_retries,
                    exc,
                )
                await asyncio.sleep(settings.connect_retry_delay_s)
        raise RuntimeError(
            f"could not connect to valkey-bus at "
            f"{settings.valkey_host}:{settings.valkey_port}"
        ) from last_exc

    async def close(self) -> None:
        await self._client.close()

    async def ping(self) -> bool:
        try:
            await self._client.ping()
            return True
        except Exception as exc:  # noqa: BLE001 - a failed ping means "not connected"
            log.warning("ping failed: %s", exc)
            return False

    async def publish(self, stream: str, env: EventEnvelope) -> str:
        """XADD an envelope; returns the generated entry id.

        The farm stream is a long-lived shared channel, so it is capped with an
        approximate ``MAXLEN ~ N`` (radix-tree-efficient) rather than a TTL. Set
        ``STREAM_MAXLEN`` above worst-case consumer lag, or ``0`` to disable trim.
        """
        trim = None
        if self._settings.stream_maxlen > 0:
            trim = TrimByMaxLen(exact=False, threshold=self._settings.stream_maxlen)
        entry_id = await self._client.xadd(
            stream, env.to_fields(), StreamAddOptions(make_stream=True, trim=trim)
        )
        return _s(entry_id)

    async def incr(self, key: str) -> int:
        return await self._client.incr(key)

    async def expire(self, key: str, seconds: int) -> None:
        await self._client.expire(key, seconds)

    async def sadd(self, key: str, member: str) -> None:
        await self._client.sadd(key, [member])
