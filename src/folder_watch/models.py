"""Pydantic models for watch bindings.

A **Binding** links a watch-spec (folder + glob patterns) to a deployed Project's
runtime record (``record_uid``). When a matching file appears/changes in the
watched folder, the service fires that record. This is the folder-watch analogue
of agent_scheduler's schedule→action binding (§9.3.1).
"""

from __future__ import annotations

import uuid
from typing import Optional

from pydantic import BaseModel, Field


def _new_uid() -> str:
    return str(uuid.uuid4())


class BindingCreate(BaseModel):
    # The runtime record the farm runs when this binding fires (§9.3 routing key).
    record_uid: str
    # Absolute path of the folder to watch.
    path: str
    # Glob patterns a filename must match to fire (e.g. ["*.pdf", "*.txt"]).
    # Empty list == match every file.
    patterns: list[str] = Field(default_factory=list)
    # Optional human label; also fire on modification (not just creation).
    name: Optional[str] = None
    on_modified: bool = True
    enabled: bool = True


class BindingUpdate(BaseModel):
    record_uid: Optional[str] = None
    path: Optional[str] = None
    patterns: Optional[list[str]] = None
    name: Optional[str] = None
    on_modified: Optional[bool] = None
    enabled: Optional[bool] = None


class Binding(BaseModel):
    binding_id: str = Field(default_factory=_new_uid)
    record_uid: str
    path: str
    patterns: list[str] = Field(default_factory=list)
    name: Optional[str] = None
    on_modified: bool = True
    enabled: bool = True

    def matches(self, filename: str) -> bool:
        """True if ``filename`` (basename) matches any configured pattern.

        No patterns == match everything.
        """
        if not self.patterns:
            return True
        from fnmatch import fnmatch

        return any(fnmatch(filename, pat) for pat in self.patterns)
