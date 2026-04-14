# DAG Architecture: Frontier-Based Workflow Execution

## Purpose

This document describes the **Directed Acyclic Graph (DAG)** execution model in Volo. 

Volo does not execute intents as a fixed linear script. It compiles them into a DAG where each node is a tool invocation and each edge is a dependency. This allows for safe parallelism, strict ordering when needed, and incremental replanning after partial failures.

## Core Data Model

*   **PlanNode:** The unit of execution. Contains `tool`, `args`, and `depends_on` (a list of upstream node IDs).
*   **ExecutionState:** Tracks runtime status (`NodeState`) and artifacts separately from the plan, allowing for merge-safe state updates.

## Frontier Scheduling (Behavioral Proof)

Volo uses a **Frontier Scheduler** rather than a static topological sort. This means the runtime computes the "Ready Frontier" in every execution loop.

### Readiness Semantics
In `core/planning/execution_plan.py`, a node is considered **Ready** ONLY when:
1.  Its current status is `PENDING`.
2.  Every node in its `depends_on` list has a status of `SUCCESS`.

**Verified Behavior:** In live simulation, the system correctly blocks dependent nodes (e.g., a "Transfer") until their parents (e.g., a "Swap") have reached terminal `SUCCESS`.

## Just-In-Time (JIT) Argument Resolution

The DAG supports **Deferred Value Binding**. Arguments can contain dynamic markers that are resolved *immediately before* execution, not at plan-time.

### Resolution Logic (`resolve_dynamic_args`)
The system resolves markers from the `ExecutionState` artifacts:
*   `{{OUTPUT_OF:node_id}}`: Pulls the `output_amount` or `amount_out` from the result of a completed node.
*   `{{BALANCE_OF:node_id:SYMBOL}}`: Pulls a specific token balance from a `check_balance` tool output.
*   `{{SUM_FROM_PREVIOUS}}`: Aggregates outputs from all successful prior steps.

**Verified Behavior:** Markers are resolved using a secure FIFO lookup in the `ExecutionState`. If a swap results in `2500 USDC`, a downstream node referencing `{{OUTPUT_OF:swap_node}}` will correctly receive `2500` as its input amount.

## Deterministic "Happy Path" Short-Circuit

To maintain high performance, the system implements a **Deterministic Short-Circuit** in `graph/nodes/planner_node.py`.

*   If there are nodes in the "Ready Frontier" and NO failures have occurred, the planner returns `CONTINUE`.
*   This bypasses the LLM entirely for the "Happy Path," ensuring execution is fast and deterministic.

## Plan Mutation and Self-Healing

When a node fails, the **Planner Node** acts as the control plane. It can:
1.  **Mutate Arguments:** Reuse a node ID but update its arguments (e.g., increase slippage for a failed swap).
2.  **Reset Status:** Mutated nodes are reset to `PENDING`, effectively "re-opening" the frontier for that path.
3.  **Versioning:** Every replan incremented the `ExecutionPlan.version`, maintaining a clear lineage of how the agent adapted to runtime failures.

## Summary

Volo's DAG implementation is a robust, dependency-driven workflow engine. By combining **Frontier Scheduling** with **JIT Argument Resolution**, the system can safely navigate complex, multi-step financial intents while maintaining transactional integrity and self-healing capabilities.
