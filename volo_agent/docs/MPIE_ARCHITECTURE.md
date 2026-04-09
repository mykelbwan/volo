# MPIE Architecture

## Purpose

This document describes the current conversational-level Massively Parallel Intent Execution (MPIE) architecture in Volo.

MPIE in this repo means:

- one user conversation can hold multiple active task lanes
- those lanes can plan and progress independently
- conflicting wallet spends are centrally coordinated
- blocked tasks pause cleanly and resume automatically when funds free up

This is a runtime architecture document for the implementation currently in the repo.

## Architecture Overview

### High-Level Design

MPIE is implemented as a layered system:

1. Conversation routing creates or reuses internal task lanes.
2. Each lane runs its own LangGraph workflow and execution plan.
3. Spend-capable nodes must pass through a global wallet reservation manager.
4. Conflicting spend steps are queued instead of racing.
5. Queued tasks pause on a graph checkpoint and auto-resume later.

At a high level:

```text
User Conversation
  -> Conversation Router
  -> Thread / Task Lane Allocation
  -> Per-Lane Planning + DAG Execution
  -> Global Wallet Reservation Admission
  -> Execute or Queue
  -> Wait for Funds
  -> Auto Resume
```

### Core Components

- Conversation/task routing:
  [core/tasks/thread_resolver.py](https://github.com/mykelbwan/volo/blob/master/volo_agent/core/tasks/thread_resolver.py),
  [core/tasks/conversation_runtime.py](https://github.com/mykelbwan/volo/blob/master/volo_agent/core/tasks/conversation_runtime.py),
  [core/tasks/router.py](https://github.com/mykelbwan/volo/blob/master/volo_agent/core/tasks/router.py)
- LangGraph workflow:
  [graph/graph.py](https://github.com/mykelbwan/volo/blob/master/volo_agent/graph/graph.py)
- Planner and execution routing:
  [graph/nodes/planner_node.py](https://github.com/mykelbwan/volo/blob/master/volo_agent/graph/nodes/planner_node.py),
  [graph/nodes/routing.py](https://github.com/mykelbwan/volo/blob/master/volo_agent/graph/nodes/routing.py)
- Execution runtime:
  [core/execution/runtime.py](https://github.com/mykelbwan/volo/blob/master/volo_agent/core/execution/runtime.py)
- Global wallet reservations and funds queue:
  [core/reservations/service.py](https://github.com/mykelbwan/volo/blob/master/volo_agent/core/reservations/service.py),
  [core/reservations/store.py](https://github.com/mykelbwan/volo/blob/master/volo_agent/core/reservations/store.py),
  [core/reservations/models.py](https://github.com/mykelbwan/volo/blob/master/volo_agent/core/reservations/models.py)
- Funds wait pause/resume:
  [graph/nodes/wait_for_funds_node.py](https://github.com/mykelbwan/volo/blob/master/volo_agent/graph/nodes/wait_for_funds_node.py),
  [core/reservations/funds_wait_runtime.py](https://github.com/mykelbwan/volo/blob/master/volo_agent/core/reservations/funds_wait_runtime.py),
  [core/reservations/wait_resume_runtime.py](https://github.com/mykelbwan/volo/blob/master/volo_agent/core/reservations/wait_resume_runtime.py)

## Conversation-Level MPIE

### Task Lanes

MPIE starts at the conversation layer.

- `conversation_id` groups all work for one user/session.
- `thread_id` identifies one internal workflow lane.
- `task_number` is the user-facing handle for a lane.

When a new message arrives:

- follow-up messages stay on the relevant active lane
- new explicit actions can allocate a new lane
- each lane keeps isolated LangGraph state and execution history

This allows one conversation to contain multiple active workflows without overwriting each other.

### Why This Is Parallel

Different task lanes can:

- parse independently
- plan independently
- wait independently
- execute independently when they do not contend for the same funds

Parallelism is therefore conversation-scoped, not just DAG-scoped.

## Execution Model

### Per-Lane Planning

Inside each task lane:

- intents are resolved into an execution DAG
- balance preflight builds resource snapshots and reservation requirements
- ready nodes execute wave by wave

Independent nodes in the same lane can still run in parallel, but MPIE is broader than that. The main addition is cross-lane coordination.

### Global Spend Admission

Before a spend-capable node executes, the runtime tries to claim wallet resources globally.

This step is handled in:

- [core/execution/runtime.py](https://github.com/mykelbwan/volo/blob/master/volo_agent/core/execution/runtime.py)
- [core/reservations/service.py](https://github.com/mykelbwan/volo/blob/master/volo_agent/core/reservations/service.py)

The reservation manager:

- normalizes resource keys by wallet scope, chain, and token
- checks active reservations across all executions
- enforces overlap-aware FIFO wait ordering
- returns either:
  - claim acquired
  - claim blocked by active reservation
  - claim blocked by earlier queued wait

## Wallet Funds Queue

### Why The Queue Exists

Without a centralized queue, multiple active task lanes could observe the same balance and both try to spend it.

The queue prevents that by making conflicting spend steps wait instead of racing.

### Queue Properties

Current queue behavior:

- global across active executions
- wallet/resource scoped
- FIFO for overlapping resource claims
- overlap-aware, so unrelated waits are not serialized unnecessarily
- durable through the reservation store

The queue state is represented by `FundsWaitRecord`.

## Wait and Resume Flow

### Pause Path

When a claim is denied:

1. the executor enqueues the blocked spend step
2. the task is marked `WAITING_FUNDS`
3. state gets a `waiting_for_funds` payload
4. planner routes to `wait_for_funds`
5. the graph interrupts and checkpoint state is persisted

### Resume Path

An external host runtime later:

1. scans eligible queue heads
2. marks a wait as `resuming`
3. calls `Command(resume=...)` on the paused graph thread
4. the graph resumes at `wait_for_funds`
5. execution goes back through `balance_check`
6. if preflight still passes, execution resumes automatically

This fresh balance check is intentional. It avoids resuming against stale state.

## Security Model

### Isolation Boundaries

The architecture relies on three isolation boundaries:

1. Conversation isolation:
   task lanes do not overwrite each other because each lane uses its own `thread_id`.
2. Spend isolation:
   conflicting spend steps must acquire global wallet reservations before execution.
3. Resume isolation:
   paused tasks resume only when their specific wait token is resumed.

### Double-Spend Protection

Double-spend protection is implemented by the centralized reservation manager.

It prevents:

- two active task lanes spending the same wallet resource at the same time
- later queued tasks bypassing earlier conflicting tasks
- local in-process concurrency from oversubscribing the same resource

This protection applies at reservation-admission time, before tool execution.

### Idempotency and Reservation Separation

Idempotency and reservations solve different problems:

- idempotency prevents duplicate execution submission
- reservations prevent oversubscription of wallet funds

Both are needed.

### Checkpoint and Resume Safety

Paused tasks are checkpointed by LangGraph persistence. Resume requires an external caller, but it does not rely on in-memory thread state.

This improves crash recovery:

- if the host process dies, the paused graph state still exists
- a later host can resume the same thread from persistence

### Failure Modes and Recovery

The system aims to fail clearly:

- insufficient or reserved funds produce `WAITING_FUNDS` instead of a vague on-chain failure
- recovery path is explicit:
  wait, inspect the blocking task, or cancel it
- timed-out or failed resume attempts are re-queued

### Chain Scope

The reservation model is not EVM-only.

It is chain-agnostic as long as a chain family can emit:

- normalized resource snapshots
- normalized reservation requirements

The current design therefore supports multiple chain families at the reservation layer.

## Performance Model

MPIE is designed to stay fast while remaining safe.

Key performance properties:

- planning stays parallel per task lane
- non-conflicting task lanes can execute concurrently
- queueing is only applied to overlapping spend claims
- wait and resume are non-blocking
- worker host code is thin, with reusable runtimes in `core/`

This keeps safety centralized without collapsing the whole conversation into serial execution.

## Operational Model

The graph does not wake itself.

Paused waits require an external host to call `Command(resume=...)`.

Current implementation supports that through reusable runtimes:

- funds wait polling:
  [core/reservations/funds_wait_runtime.py](https://github.com/mykelbwan/volo/blob/master/volo_agent/core/reservations/funds_wait_runtime.py)
- funds wait resume transaction:
  [core/reservations/wait_resume_runtime.py](https://github.com/mykelbwan/volo/blob/master/volo_agent/core/reservations/wait_resume_runtime.py)
- bridge worker outer loop:
  [core/bridge_status_worker_runtime.py](https://github.com/mykelbwan/volo/blob/master/volo_agent/core/bridge_status_worker_runtime.py)
- event notifier loop:
  [core/event_notifier_runtime.py](https://github.com/mykelbwan/volo/blob/master/volo_agent/core/event_notifier_runtime.py)

This is intentional so FastAPI or another host can import the runtime directly instead of depending on script files.

## Current Limits

Current MPIE is robust, but not final:

- fairness is strict FIFO for overlapping waits
- bounded-bypass fairness is not implemented
- auto-resume still depends on an external runtime host being active
- balance preflight is advisory; final spend admission is the reservation claim

## Summary

Current conversational MPIE in Volo means:

- multiple active workflows per conversation
- independent planning and execution where safe
- centralized wallet-level coordination where funds conflict
- durable pause/resume for blocked tasks
- clear user-facing waiting state and recovery path

This provides parallel intent execution without accepting cross-task wallet overspend risk.
