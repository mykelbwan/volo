# MPIE Architecture: Conversational-Level Intent Concurrency

## Purpose

This document describes the **Massively Parallel Intent Execution (MPIE)** architecture in Volo. 

Unlike traditional agents that execute linear scripts, Volo implements **Conversational-Level MPIE**. This means a single user session can manage multiple independent "Task Lanes" that plan, execute, and coordinate shared wallet resources concurrently.

## Architecture Overview

### High-Level Design: The Multi-Lane Pipeline

MPIE is implemented as a layered system designed for horizontal scalability and durable execution:

1.  **Thread Resolver & Routing:** Incoming messages are dynamically routed. In `resolve_turn_routing`, explicit actions ("swap 1 ETH") trigger a `uuid4` thread allocation to create a **New Task Lane**.
2.  **Lane Isolation:** Each lane runs its own isolated LangGraph workflow and state, managed by a unique `thread_id`.
3.  **Global Wallet Reservation Admission:** Spend-capable nodes must acquire a global lock from the **Wallet Reservation Service** before signing.
4.  **Durable Interrupts:** If funds are unavailable (e.g., Lane A is already spending the wallet's balance), the lane doesn't fail; it uses a **LangGraph Interrupt** to suspend state.
5.  **External Resume Loop:** A separate runtime (`funds_wait_runtime.py`) monitors the global queue and "thaws" paused lanes using secure resume tokens.

```text
User Conversation
  -> thread_resolver.py (Identify or Allocate Task Lane)
  -> graph.py (Per-Lane DAG Execution)
  -> runtime.py (Preflight + Spend Admission)
  -> service.py (Global Reservation Claim)
  -> wait_for_funds_node.py (interrupt() if blocked)
  -> funds_wait_runtime.py (External FIFO Wakeup)
```

## Conversational Parallelism: Task Lanes

MPIE starts at the **Routing Layer**, allowing one user to have multiple "frontiers" active at once.

*   **Dynamic Thread Allocation:** The system detects new intents vs. follow-ups. A new intent results in a brand-new execution context.
*   **Contextual Routing:** Follow-up messages ("confirm", "proceed") are pinned to the specific `thread_id` using a selection registry, ensuring independent lanes don't cross-talk.
*   **Task Handles:** Users interact with lanes via `task_number` handles (e.g., "Task 1", "Task 2").

## The Wallet Reservation "Gate" (Behavioral Proof)

To prevent the "Agent Split-Brain" problem (where two lanes see the same $100 balance and both try to spend it), Volo uses a **Distributed Semaphore**.

### Reservation Service (`core/reservations/service.py`)
Before tool execution, the `ExecutionRuntime` calls `claim()`.
*   **Atomic Locking:** Uses a MongoDB-backed lock with TTL to prevent race conditions during claim adjudication.
*   **Conflict Detection:** Behavioral simulation proves that if Lane A reserves 0.8 ETH of a 1.0 ETH balance, Lane B's request for 0.5 ETH is **rejected** with a `deferred_reason`, even if the on-chain balance is still 1.0 ETH. This ensures zero risk of overspending across parallel lanes.

## Durable Wait and Resume Flow

Volo handles the "Dark Period" of cross-chain bridging or fund contention using a suspend-and-resume model.

### 1. The Pause (Interrupt)
When `claim()` returns `acquired=False`, the execution runtime calls `enqueue_wait()` and routes the graph to the `wait_for_funds_node`.
*   **State Suspension:** The graph calls `interrupt(payload)`, which immediately halts execution and persists the full checkpoint. This saves resources by moving the task from memory to the database.
*   **Resume Token:** A unique URL-safe `resume_token` (min 32 chars) is generated and stored for secure wakeup.

### 2. The Wakeup (External Loop)
The `run_wait_poll_loop` in `funds_wait_runtime.py` operates as the system's "Heartbeat":
*   **FIFO Ordering:** It lists candidates from the `FundsWaitRecord` store, prioritizing older waits.
*   **Atomic Transition:** It marks a wait as `resuming` to ensure exactly-once resumption.
*   **Secure Resumption:** It passes the `resume_token` back to the LangGraph thread to trigger the "Thaw."

### 3. The Guarded Resume
Upon waking, the `wait_for_funds_node` uses `secrets.compare_digest` to verify the token.
*   **Fresh Preflight:** Execution flows back through `balance_check` nodes. This ensures the environment is re-validated before spending, preventing execution against stale balances.

## Summary

MPIE in Volo is a **Transactionally Safe Conversational Workflow Engine**. It combines independent conversational task lanes with global fund coordination and durable graph persistence to ensure that user intents are executed safely and predictably, with zero risk of overspending in complex, asynchronous environments.
