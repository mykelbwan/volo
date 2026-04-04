# DAG Architecture

## Status

Current implementation.

## Scope

This document describes the Directed Acyclic Graph (DAG) execution model used by Volo today.

It covers:

- plan representation
- dependency construction
- readiness and scheduling semantics
- execution-state model
- dynamic argument resolution
- plan mutation and replanning
- invariants and failure handling

Primary implementation references:

- [core/planning/execution_plan.py](/home/michael/dev-space/aura/volo_agent/core/planning/execution_plan.py)
- [graph/nodes/resolver_node.py](/home/michael/dev-space/aura/volo_agent/graph/nodes/resolver_node.py)
- [core/execution/runtime.py](/home/michael/dev-space/aura/volo_agent/core/execution/runtime.py)
- [graph/nodes/planner_node.py](/home/michael/dev-space/aura/volo_agent/graph/nodes/planner_node.py)

## Overview

Volo does not execute intents as a fixed linear script. It compiles them into a DAG where:

- each node is one tool invocation
- each edge is a dependency
- a node becomes runnable only when all upstream dependencies have succeeded

This allows:

- strict ordering when one step depends on another
- safe parallelism when steps are independent
- incremental replanning after partial failure

High-level flow:

```text
Parsed Intents
  -> Intent Resolver
  -> ExecutionPlan (DAG)
  -> Balance / Preflight Enrichment
  -> Execution Runtime
  -> Planner
  -> Continue / Mutate / Finish
```

## Architectural Style

The implementation follows a standard workflow-orchestration pattern:

- immutable-ish plan model with explicit node IDs
- separate execution-state model
- dependency-based readiness evaluation
- delta-based state transitions
- control-plane replanning separated from data-plane execution

This is closer to workflow engines such as Temporal/Airflow-style DAG thinking than to a single threaded task queue.

## Core Data Model

### PlanNode

`PlanNode` is the unit of execution.

Defined in [execution_plan.py](/home/michael/dev-space/aura/volo_agent/core/planning/execution_plan.py).

Fields:

- `id`: stable node identifier
- `tool`: tool name to invoke
- `args`: tool arguments
- `depends_on`: upstream node IDs that must succeed first
- `approval_required`: whether user confirmation is needed before execution
- `retry_policy`: local retry metadata
- `metadata`: non-schema execution metadata such as route-planner output

### ExecutionPlan

`ExecutionPlan` is the DAG container.

Fields:

- `goal`: human-readable goal string
- `nodes`: mapping of node ID to `PlanNode`
- `version`: plan version for mutated/replanned graphs
- `metadata`: plan-level metadata

### NodeState

`NodeState` tracks runtime status for one node.

Fields:

- `node_id`
- `status`
- `retries`
- `result`
- `error`
- `error_category`
- `user_message`
- `mutated_args`

### ExecutionState

`ExecutionState` tracks runtime state separately from the plan.

Fields:

- `node_states`
- `artifacts`
- `completed`

This separation is important:

- the plan describes intended structure
- execution state describes observed runtime outcomes

## State Machine

### StepStatus

Node lifecycle values:

- `PENDING`
- `RUNNING`
- `SUCCESS`
- `FAILED`
- `SKIPPED`

### Transition Model

Common transitions:

- `PENDING -> RUNNING`
- `RUNNING -> SUCCESS`
- `RUNNING -> FAILED`
- `FAILED -> PENDING` via planner reset or retry
- `PENDING -> SKIPPED` for explicit skip paths

Completion rule:

- a plan is complete when every node is `SUCCESS` or `SKIPPED`

Implemented in [execution_plan.py](/home/michael/dev-space/aura/volo_agent/core/planning/execution_plan.py).

## Graph Construction

### Intent Resolution

The resolver converts resolved intents into `PlanNode`s in [resolver_node.py](/home/michael/dev-space/aura/volo_agent/graph/nodes/resolver_node.py).

The process is:

1. Resolve each complete intent into normalized plan data.
2. Convert that linear intent output into graph nodes.
3. Infer dependencies.
4. Initialize `ExecutionState` with all nodes in `PENDING`.

### Dependency Sources

Dependencies are created from two sources:

1. Marker-based dependencies
2. Parallelization heuristics and barriers

#### Marker-Based Dependencies

Resolver scans arguments for marker references such as:

- `{{OUTPUT_OF:step_X}}`
- `{{BALANCE_OF:step_X:TOKEN}}`

These create explicit upstream dependencies.

This is the strongest dependency source because it reflects real data flow.

#### Heuristic Dependencies and Barriers

When there are no explicit markers, the resolver decides whether nodes may become siblings.

Parallel-safe tools currently include:

- `transfer`
- `check_balance`
- `swap`
- `bridge`

The resolver uses:

- per-tool parallel keys
- same-chain swap controls
- barrier anchors

This produces one of two outcomes:

- sibling node with shared anchor dependency
- serialized node depending on the previous node

### Parallel Swap Controls

Same-chain swap parallelism is explicitly constrained.

Controls include:

- `ENABLE_SAME_CHAIN_PARALLEL_SWAPS`
- per-chain cap
- max slippage threshold
- unique pair gating

This prevents unsafe over-parallelization while still allowing useful concurrency.

## Readiness Semantics

### Ready Node Definition

`get_ready_nodes()` defines readiness in [execution_plan.py](/home/michael/dev-space/aura/volo_agent/core/planning/execution_plan.py).

A node is ready when:

- its state is `PENDING`
- every node in `depends_on` is `SUCCESS`

Important consequences:

- `RUNNING` dependencies do not satisfy readiness
- `FAILED` dependencies block downstream progress
- downstream nodes do not run on partial upstream completion

This gives simple and deterministic scheduling semantics.

## Execution Model

### Runtime Loop

The execution runtime in [runtime.py](/home/michael/dev-space/aura/volo_agent/core/execution/runtime.py) operates as a frontier scheduler.

Each pass:

1. Merge the persisted state with current deltas.
2. Compute the ready frontier.
3. Resolve dynamic args for ready nodes.
4. Apply guardrails and reservations.
5. Schedule runnable nodes.
6. Execute runnable nodes concurrently.
7. Write node-state deltas.
8. Repeat until no further progress is possible.

### Frontier Execution

The runtime does not topologically sort the whole DAG upfront. Instead, it repeatedly asks:

- what is ready now?

This is the current industry-standard practical model for dynamic workflow execution because it works well with:

- retries
- partial failures
- replanning
- dynamic argument resolution

### State Deltas

The runtime writes execution changes as deltas rather than rebuilding full state from scratch.

Examples:

- mark node running
- mark node success
- mark node failure
- reset node to pending

`ExecutionState.merge()` merges deltas deterministically.

This keeps node updates composable and predictable.

## Dynamic Argument Resolution

Arguments can contain dynamic markers that are resolved at execution time.

Implemented in [execution_plan.py](/home/michael/dev-space/aura/volo_agent/core/planning/execution_plan.py) via `resolve_dynamic_args()`.

Resolution sources include:

- prior node results
- normalized output artifacts
- aggregate success outputs
- session/context artifacts

This matters because:

- the graph structure may be known before all values are known
- final execution arguments may depend on runtime outcomes

The DAG therefore supports deferred value binding.

## Planner and Plan Mutation

### Planner Role

The planner in [planner_node.py](/home/michael/dev-space/aura/volo_agent/graph/nodes/planner_node.py) is the control plane for the DAG.

It does not execute tools. It decides whether to:

- continue on the current happy path
- wait
- finish
- mutate or extend the plan

### Deterministic Short-Circuit

If there are ready nodes and no failures:

- planner returns `CONTINUE`
- no LLM replanning is used

This keeps the happy path fast and deterministic.

### Mutation Rules

When replanning is needed:

- existing node IDs may be reused to mutate args for a failed step
- new nodes may be appended to a new plan version
- mutated nodes are reset to `PENDING`

Hard constraints:

- do not duplicate identical nodes
- preserve dependency correctness
- reuse IDs for fixes instead of creating ghost retries

This model keeps lineage understandable while still allowing self-healing.

## Routing Metadata

Route-planner output is stored in `PlanNode.metadata["route"]`.

This is intentionally separate from `args` so:

- tool schemas remain stable
- execution-specific optimization data does not pollute intent-level arguments

Examples of route metadata:

- selected aggregator
- quote timing
- calldata fast-path
- protocol execution hints

## Invariants

The current DAG implementation relies on these invariants:

1. Node IDs are unique within a plan version.
2. `depends_on` contains only known node IDs.
3. A node executes only from `PENDING`.
4. Downstream execution requires full upstream `SUCCESS`.
5. Dynamic marker dependencies must be reflected in `depends_on`.
6. Execution state must remain merge-safe under partial updates.
7. Planner mutations must preserve DAG validity.

## Failure Model

### Node-Level Failure

A node failure does not immediately invalidate the whole plan.

The runtime records:

- error
- retry count
- error category
- optional mutated args

The planner then decides whether to:

- continue
- retry locally
- mutate the failed node
- terminate

### Non-Retryable Failure

Non-retryable failures short-circuit planning and stop the workflow.

This prevents unsafe or pointless LLM-driven recovery.

### Deadlock / No-Ready Condition

If no nodes are ready:

- planner may return `WAITING`
- or inspect failure/dependency state and decide next action

This is the main escape hatch for graph-level stalls.

## Security and Safety Boundaries

The DAG model alone does not guarantee safe execution. Safety comes from layering it with:

- guardrail validation
- idempotency
- global wallet reservations
- confirmation gates

The DAG controls ordering. It does not by itself decide economic safety.

## Observability

The runtime emits:

- reasoning logs
- node progress events
- terminal node events
- task-history updates

This gives visibility into:

- current frontier
- node outcomes
- replan decisions
- waiting states

## Tradeoffs

### Strengths

- simple readiness semantics
- safe parallelism
- clean separation of plan and state
- supports dynamic arguments
- supports mutation and self-healing
- deterministic happy path

### Current Limitations

- no explicit cycle validator is documented at plan-build time
- heuristic parallelization is intentionally conservative
- planner mutation quality depends on runtime context quality
- graph scheduling is in-process frontier scheduling, not a distributed DAG engine

## Industry-Standard Framing

In industry-standard workflow terms, the current implementation maps to:

- DAG definition layer:
  `ExecutionPlan`, `PlanNode`
- workflow state store:
  `ExecutionState`, `NodeState`
- scheduler:
  `get_ready_nodes()` plus runtime frontier loop
- executor:
  tool runtime in [runtime.py](/home/michael/dev-space/aura/volo_agent/core/execution/runtime.py)
- control plane:
  planner in [planner_node.py](/home/michael/dev-space/aura/volo_agent/graph/nodes/planner_node.py)
- admission / safety gates:
  guardrails, idempotency, reservations, approvals

That is the correct way to reason about the current DAG implementation.

## Summary

Volo’s DAG implementation is a dependency-driven workflow engine with:

- explicit node and state models
- readiness-based scheduling
- dynamic argument resolution
- controlled parallelism
- delta-based execution updates
- planner-driven mutation and recovery

It is designed for transactional intent execution, not generic batch ETL, and the surrounding safety systems are part of the architecture rather than optional add-ons.
