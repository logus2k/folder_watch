"""folder_watch binding persistence: bindings survive a restart (JSON-backed store)."""
from __future__ import annotations

from folder_watch.models import BindingCreate, BindingUpdate
from folder_watch.store import BindingStore


def _mk(uid="p1", path="/watched/in"):
    return BindingCreate(record_uid=uid, path=path, patterns=["*.pdf"], name="n")


def test_create_persists_and_reloads(tmp_path):
    f = tmp_path / "bindings.json"
    store = BindingStore(f)
    b = store.create(_mk())
    assert f.exists()

    # Simulate a restart: a fresh store over the same file.
    store2 = BindingStore(f)
    got = store2.get(b.binding_id)
    assert got is not None
    assert got.record_uid == "p1"
    assert got.patterns == ["*.pdf"]


def test_update_and_delete_persist(tmp_path):
    f = tmp_path / "bindings.json"
    store = BindingStore(f)
    b = store.create(_mk())
    store.update(b.binding_id, BindingUpdate(record_uid="p2"))
    assert BindingStore(f).get(b.binding_id).record_uid == "p2"

    assert store.delete(b.binding_id) is True
    assert BindingStore(f).get(b.binding_id) is None


def test_no_path_is_pure_memory(tmp_path):
    store = BindingStore()  # None -> in-memory (existing unit tests rely on this)
    store.create(_mk())
    assert list(tmp_path.iterdir()) == []
    assert len(store.list()) == 1
