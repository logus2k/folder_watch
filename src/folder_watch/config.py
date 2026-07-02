"""Environment-driven configuration.

A single immutable ``Settings`` instance, populated from the environment (with a
``.env`` loaded in dev). Every knob has a safe default so the app runs with an
empty environment. Mirrors agent_scheduler's config conventions.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

# Load .env if present (no-op in containers that inject real env vars).
load_dotenv()


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _str(name: str, default: str) -> str:
    raw = os.getenv(name)
    return default if raw is None or raw == "" else raw


def _bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class Settings:
    # --- Valkey connection (shared valkey-bus) ---
    valkey_host: str = _str("VALKEY_HOST", "127.0.0.1")
    valkey_port: int = _int("VALKEY_PORT", 6379)

    # --- Stream / discovery conventions (must match agent_bus) ---
    stream_prefix: str = _str("STREAM_PREFIX", "stream:")
    active_streams_key: str = _str("ACTIVE_STREAMS_KEY", "streams:active")

    # --- Binding store (JSON file; the admin API's source of truth) ---
    # Persisted so watch bindings survive a restart (mirrors http_ingress).
    bindings_path: str = _str("BINDINGS_PATH", "data/bindings.json")

    # --- Emission identity + routing ---
    sender_id: str = _str("SENDER_ID", "folder_watch")
    # The farm consumes ``stream:agent-runtime`` and routes on event_data.record_uid.
    target_stream_id: str = _str("TARGET_STREAM_ID", "agent-runtime")
    # ``<source>.fired`` per §9.3.1 — the File Initiator's source is "file".
    event_type: str = _str("EVENT_TYPE", "file.fired")

    # --- Publisher connection retry (startup resilience across compose projects) ---
    connect_retries: int = _int("CONNECT_RETRIES", 30)
    connect_retry_delay_s: int = _int("CONNECT_RETRY_DELAY_S", 2)

    # --- Resource retention (bound the keys/streams the emitter creates) ---
    sid_ttl_s: int = _int("SID_TTL_S", 3600)
    stream_maxlen: int = _int("STREAM_MAXLEN", 10000)

    # --- Watch loop ---
    poll_interval_s: float = float(_str("POLL_INTERVAL_S", "2.0"))

    # --- Admin API ---
    api_host: str = _str("API_HOST", "0.0.0.0")
    api_port: int = _int("API_PORT", 6817)

    # --- Logging ---
    log_level: str = _str("LOG_LEVEL", "INFO")

    def stream_key(self, stream_id: str) -> str:
        """The dedicated stream key for a target: ``stream:<stream_id>``."""
        return f"{self.stream_prefix}{stream_id}"


settings = Settings()
