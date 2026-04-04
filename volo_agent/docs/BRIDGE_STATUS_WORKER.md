# Bridge Status Worker

This worker polls bridge transactions stored in LangGraph state and updates
their status on every tick. It is designed to run as a **separate process**
from the main application.

## What It Does

- Reads `pending_transactions` from the latest checkpoint of each thread.
- For bridge records, calls the appropriate protocol status provider.
- Updates per‑tick fields:
  - `last_status`
  - `last_status_raw`
  - `last_checked_at`
- Marks terminal results:
  - `status = SUCCESS` when filled
  - `status = FAILED` when refunded/expired
- Updates the LangGraph execution state if `node_id` is present.

## Requirements

- MongoDB must be reachable (same DB used by the app).
- `SKIP_MONGODB_HEALTHCHECK` must **not** be set, otherwise the graph
  will use `MemorySaver` and updates won’t persist.

## How It Works (High Level)

1. Enumerate `(thread_id, checkpoint_ns)` pairs from `lg_checkpoints`.
2. Load the latest state for each thread via `graph.app.get_state(...)`.
3. Poll status for bridge records using the protocol registry in
   `core/utils/bridge_status_registry.py`.
4. Write state updates back with `graph.app.update_state(...)`.

## Usage

Run continuously:

```bash
uv run command_line_tools/bridge_status_worker.py --interval 15
```

Run once:

```bash
uv run command_line_tools/bridge_status_worker.py --once
```

## Configuration

Environment variables:

- `BRIDGE_STATUS_WORKER_INTERVAL_SECONDS` (default: `15`)
- `BRIDGE_STATUS_WORKER_LOCK_TTL_SECONDS` (default: `30`)

CLI flags override env vars.

## Locking

The worker uses a per‑thread lock in MongoDB (`bridge_status_worker_locks`)
to avoid duplicate polling across multiple worker instances. Lock acquisition
and index setup live in `core/bridge_status_worker_locks.py`; the CLI script
just consumes that logic. Locks now store `expires_at` as a MongoDB date and a
TTL index auto-deletes expired date-based lock documents. Legacy numeric
expiries are still accepted during the transition until they are overwritten by
the new format. Set `--lock-ttl-seconds 0` to disable locking.

## Protocol Support

Protocol status providers are registered in:

```
core/utils/bridge_status_registry.py
```

Currently registered:

- `across`
- `relay`

To add new protocols, implement a provider with:

- `fetch_status(tx_hash, is_testnet, meta=None)`
- `interpret_status(raw_status)`

Then register it via:

```
register_bridge_status_provider("protocol_name", Provider())
```

## Notes

- The worker does **not** execute nodes; it only updates state.
- It only processes records where `type == "bridge"`.
- Bridge records should include:
  - `protocol`
  - `tx_hash`
  - `node_id` (optional but required to update execution state)
  - `meta.request_id` for Relay status polling
- Bridge executors should return after broadcast with `status = pending`;
  the worker is responsible for finalization.
