# Bridge Status Worker: Asynchronous State Finalizer

## Purpose

The **Bridge Status Worker** is a decoupled state manager that handles the "Dark Period" of cross-chain bridging. Since bridges can take minutes or hours to finalize, the main agent process suspends execution. This worker is responsible for polling those transactions and "nudging" the agent back to life once funds arrive.

## Architecture

The worker operates as a background loop, independent of the main API or Agent processes. It follows a **Pull-Notify** model:

1.  **Poll:** It identifies active LangGraph threads with `pending_transactions` of type `bridge`.
2.  **Verify:** It calls the protocol-specific provider (Across, Relay, etc.) to check on-chain finality.
3.  **Inject:** It surgicaly updates the LangGraph checkpoint with the new status.
4.  **Resume:** It triggers a re-execution of the graph to continue the intent workflow.

## Behavioral Proof: The "Nudge" Pattern

Live simulation verifies that the worker does not just update data; it acts as a **Graph Activator**.

### Verified Execution Flow
*   **State Detection:** The worker uses `app.get_state()` to read the `pending_transactions` and `execution_state` directly from the thread's checkpoint.
*   **Atomic Injection:** Updates are written back via `app.update_state(..., as_node="execution_engine")`. This ensures that when the graph wakes up, it is positioned correctly at the execution frontier.
*   **Graph Resumption:** Upon finalization (SUCCESS or FAILED), the worker calls `app.invoke(None, config)`. This is the "Nudge" that restarts the paused LangGraph workflow.

## Distributed Coordination & Locking

To scale horizontally and prevent redundant RPC calls, the worker implements a **Distributed Locking** mechanism.

*   **Thread Locks:** Uses the `bridge_status_worker_locks` collection in MongoDB.
*   **Owner Identification:** Each worker process uses a unique `owner` ID to claim specific `thread_id:checkpoint_ns` pairs.
*   **TTL Safety:** Locks are self-expiring via MongoDB TTL indexes, ensuring that if a worker process crashes, other workers can pick up the slack within 30 seconds.

## Protocol Registry

Status providers are modular and registered in `core/utils/bridge_status_registry.py`.

*   **Across:** Polls the Across Relayer API for `fill` events.
*   **Relay:** Polls the Relay API using the `request_id` stored in the bridge metadata.

## Summary

The Bridge Status Worker transforms the agent from a synchronous "script" into a **Durable Asynchronous Workflow**. By managing the cross-chain waiting period externally, it ensures that user intents are completed reliably without tying up active compute resources or requiring the user to manually "refresh" their session.
