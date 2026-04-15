# Funds Wait Worker: Wallet-Level Concurrency Orchestrator

## Purpose

The **Funds Wait Worker** is the "State Thawer" for task lanes suspended due to wallet balance constraints. In the **MPIE (Massively Parallel Intent Execution)** model, multiple lanes may compete for the same funds. If a lane is denied a reservation, it suspends itself. This worker monitors the global fund queue and wakes up the next eligible lane as funds become available.

## Architecture

The worker runs as a background polling loop, operating as a FIFO (First-In-First-Out) scheduler for blocked intents:

1.  **Identify:** It lists `FundsWaitRecord` candidates from the database that are in the `queued` status.
2.  **Claim:** It atomically marks a candidate as `resuming` to ensure exactly-once resumption in a distributed environment.
3.  **Resume:** It sends a secure resumption command to the specific LangGraph thread.

## Behavioral Proof: LangGraph Resumption

Live simulation verifies the use of the **LangGraph Command Pattern** for durable state resumption.

### Verified Execution Flow
*   **Resume Command:** The worker resumes the interrupted graph with a LangGraph resume payload. This is the pattern used to "thaw" a graph that was paused via `interrupt()`.
*   **Secure Handshake:** The payload contains a `resume_token` (minimum 32 characters). The `wait_for_funds_node` in the graph will only proceed if this token matches the one generated during the initial suspension.
*   **Targeted Wakeup:** The payload includes the `node_id`, ensuring that the resume signal is routed to the correct execution frontier within the thread.

## Reliability & Backoff

To handle unreliable network conditions or RPC failures, the worker implements a **Robust Retry Model**:

*   **Atomic Transitions:** Resumption only proceeds if the database `mark_wait_resuming` call succeeds, preventing duplicate "thaws."
*   **Exponential Backoff:** If a `resume` call fails (e.g., timeout or graph execution error), the record is moved back to `queued` with a `resume_after` timestamp calculated using exponential backoff (`min(3600, 2^attempts)`).
*   **FIFO Fairness:** Conflicts are resolved based on the `created_at` timestamp, ensuring that user intents are processed in the order they were received.

## Summary

The Funds Wait Worker is the "Glue" that makes **Conversational Parallelism** safe and reliable. By managing the queue of blocked intents and securely resuming them, it allows the agent to handle complex, multi-lane workloads without risking wallet overspending or requiring manual user intervention to "retry" blocked steps.
