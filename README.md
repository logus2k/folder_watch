# folder_watch

Minimal **folder-watch service** backing the Patron **File Initiator** (block_management §8, §9.3.1).

Watches configured folders; when a **new or changed** file matches a binding's glob patterns, it
emits **one** bus event so the farm runs the deployed Project.

## What it emits

Reuses the agent_bus envelope + valkey-glide wire client exactly as `agent_scheduler` does. On a
trigger it XADDs to the farm stream:

- `target_stream_id` = `agent-runtime` → key `stream:agent-runtime`
- `event_type` = `file.fired`
- `event_data` = `{"record_uid": <project uid>}` (the farm's Phase-05 routing key)
- file provenance (`file_path`, `change`, `binding_id`) in `payload.context`

## Config (bindings)

A **Binding** = watch-spec (`path` + `patterns`) → `record_uid`. CRUD via the FastAPI admin API
(`/bindings`, default `:6817`). Empty `patterns` == match every file. First scan of a binding
**seeds** its baseline (pre-existing files don't fire).

## Run

- Local: `pip install -r requirements.txt && python -m folder_watch.app`
- Container: `docker compose up -d --build` (glibc base; joins external `logus2k_network`; does not
  redeclare `valkey-bus`). Mount host folders under `./watched`.

## Test

`python -m pytest` — mocks the bus (no live Valkey); asserts a dropped matching file emits exactly
one envelope carrying `event_data.record_uid`.
