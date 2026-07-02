"""FastAPI admin API — CRUD over watch bindings + the watcher loop.

Single process: the FastAPI lifespan connects the bus publisher and runs the
watcher poll loop as an embedded asyncio task. The admin routes mutate the binding
store (the source of truth); the watcher reads it every tick.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from . import emitter
from .config import settings
from .models import Binding, BindingCreate, BindingUpdate
from .store import store
from .watcher import watcher

log = logging.getLogger("folder_watch.api")


def create_app(*, run_watcher: bool = True, connect_emitter: bool = True) -> FastAPI:
    """Build the app. ``run_watcher`` starts the poll loop in-process;
    ``connect_emitter`` connects the glide bus Publisher."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if connect_emitter:
            await emitter.connect()
        if run_watcher:
            watcher.start()
        try:
            yield
        finally:
            if run_watcher:
                await watcher.stop()
            if connect_emitter:
                await emitter.close()

    app = FastAPI(title="folder_watch admin", lifespan=lifespan)

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok", "bus_connected": emitter.is_connected()}

    @app.get("/bindings", response_model=list[Binding])
    async def list_bindings() -> list[Binding]:
        return store.list()

    @app.post("/bindings", response_model=Binding, status_code=201)
    async def create_binding(data: BindingCreate) -> Binding:
        binding = store.create(data)
        # A new binding may add a folder to watch — restart the watch over the new set.
        watcher.reconfigure()
        return binding

    @app.get("/bindings/{binding_id}", response_model=Binding)
    async def get_binding(binding_id: str) -> Binding:
        binding = store.get(binding_id)
        if binding is None:
            raise HTTPException(status_code=404, detail="binding not found")
        return binding

    @app.patch("/bindings/{binding_id}", response_model=Binding)
    async def update_binding(binding_id: str, data: BindingUpdate) -> Binding:
        updated = store.update(binding_id, data)
        if updated is None:
            raise HTTPException(status_code=404, detail="binding not found")
        # Config changed — reset baseline so the new watch-spec re-seeds cleanly.
        watcher.forget(binding_id)
        return updated

    @app.delete("/bindings/{binding_id}", status_code=204)
    async def delete_binding(binding_id: str) -> None:
        if not store.delete(binding_id):
            raise HTTPException(status_code=404, detail="binding not found")
        watcher.forget(binding_id)

    return app


app = create_app()
