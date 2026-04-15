# Volo Architecture Guide

This document provides a grounded, verified technical overview of the Volo Agent's internal systems. Every section has been audited against the source code and verified through behavioral simulations.

---

## 1. The Intent-to-DAG Pipeline (Inbound Reasoning)

The **Intent-to-DAG Pipeline** transforms unstructured natural language into a strictly typed, dependency-aware **Directed Acyclic Graph (DAG)**.

### Stage 1: Hybrid Semantic Parsing (`intent_hub/parser/semantic_parser.py`)
Volo uses a hybrid parsing strategy to maximize speed and reliability:
*   **Fast Path (Regex):** Optimized patterns handle 80% of standard intents (swaps, bridges, transfers, balances) with zero LLM latency.
*   **Recursive Clause Handling:** For sequential intents ("Do X and then do Y"), the parser uses a recursive builder (`_build_dependent_intent_clause`) that carries context (tokens, amounts, chains) from the first action to the second.

### Stage 2: DAG Resolution & Dependency Inference (`graph/nodes/resolver_node.py`)
This stage transforms a list of intents into an `ExecutionPlan`.
*   **Marker Detection:** The system scans for dynamic markers like `{{OUTPUT_OF:step_X}}`. If found, it automatically injects `step_X` into the current node's `depends_on` list.
*   **Heuristic Barrier Model:** To prevent state collisions, the system uses a **Barrier Key** heuristic:
    *   **Parallel:** Actions on different chains or tokens are marked as parallel siblings.
    *   **Serialized:** Actions on the same chain/tool (e.g., two Swaps on Ethereum) are serialized to ensure transactional integrity.

### Behavioral Proof: Multi-Step Intent Resolution
Prompt: *"Bridge 1 ETH from Ethereum to Base then swap it for USDC"*

| Intent | Type | Slots (Extracted/Carried) | Dependency |
| :--- | :--- | :--- | :--- |
| **step_0** | `bridge` | `token: ETH`, `amount: 1.0`, `to: base` | None |
| **step_1** | `swap` | `token_in: ETH` (carried), `amount: 1.0` (carried), `token_out: USDC` | `depends_on: ["step_0"]` |

**Verified Observation:** The parser correctly split the sequence, identified the second intent as dependent, and carried over the resource context (`ETH`, `1.0`, `base`) to the subsequent swap.

---

## 2. DAG Execution & Frontier Scheduling (The Engine)

Volo uses a **Frontier Scheduler** and a **Transactional Execution Engine** to ensure safety across unreliable networks.

### Readiness & Frontier Scheduling (`core/planning/execution_plan.py`)
A node is considered **Ready** ONLY when its status is `PENDING` and every node in its `depends_on` list is `SUCCESS`. This allows the agent to execute independent branches in parallel while blocking on data dependencies.

### JIT Argument Resolution & Balance Probing
*   **Deferred Binding:** Arguments (e.g., `{{OUTPUT_OF:step_X}}`) are resolved *immediately before* tool execution.
*   **JIT Balance Probing:** The execution runtime performs a final balance validation pass immediately before signing, checking live wallet balances against reservation state, fee requirements, and projected deltas to prevent over-spend if external transactions occurred.

### Double-Spend & Idempotency Protection (`core/execution/runtime.py`)
Volo implements a "Safety Probe" to handle the "Ghost Transaction" problem (where a broadcast fails but the transaction eventually hits the mempool).

*   **Atomic Claiming:** Every tool execution must claim an idempotency key (hash of `scope:node:tool:args`) before proceeding.
*   **The Nonce Probe:** Before retrying a "Pending" task, the system probes the on-chain nonce gap. If `pending_nonce > latest_nonce`, the system **blocks the retry**, detecting an active transaction in the mempool.
*   **Consensus-Based Confirmation:** The system uses multiple RPC providers to "vote" on transaction receipts, requiring corroborated evidence before treating a revert or failure as final.

**Verified Observation:** Behavioral trace proved that the system correctly blocks key reclamation if a nonce gap is detected, effectively preventing double-spending even if the agent process crashes mid-broadcast.

---

## 3. MPIE: Conversational-Level Intent Concurrency

**Massively Parallel Intent Execution (MPIE)** allows a single user session to manage multiple independent "Task Lanes" concurrently.

### Thread Isolation & Routing
*   **Dynamic Thread Allocation:** New intents trigger a `uuid4` thread allocation to create a brand-new Task Lane.
*   **Contextual Pinning:** Follow-up messages (e.g., "confirm") are routed back to the correct `thread_id` using a selection registry, ensuring independent lanes don't cross-talk.

### Global Resource Serialization (The Gatekeeper)
To prevent "Agent Split-Brain" (two lanes trying to spend the same balance), Volo uses a **Distributed Semaphore** via the `WalletReservationService`.

*   **Atomic Locking:** Uses a MongoDB-backed lock with TTL to coordinate access to a shared `wallet_state` registry.
*   **Stateful Adjudication:** The system maintains `resource_totals` (aggregate reserved units) and `active_holders`. It rejects claims if `reserved + required > available`, even if on-chain funds are still present.
*   **FIFO Queue Fairness:** The system implements a "Wait-Head" optimization. If a task is queued for funds (e.g., Task B), any subsequent task (e.g., Task C) is automatically blocked, even if its requirements are small enough to fit. This prevents starvation of larger, earlier intents.

**Verified Observation:** Behavioral trace proved that the system correctly identifies specific blocking tasks (e.g., "Funds are reserved by Task 1") and enforces strict FIFO ordering, blocking later tasks from "jumping" the queue.

---

## 4. Durable Async Workflows (The Workers)

Volo handles long-running operations (like cross-chain bridges or wallet fund contention) using a suspend-and-resume model managed by background workers.

### The "Nudge" Pattern (Bridge Status Worker)
When a task is waiting for a bridge, the execution graph suspends. The worker acts as the **Graph Activator**:
*   **State Detection:** Identifies threads with `pending_transactions` of type `bridge`.
*   **Atomic Injection:** Surgically updates the LangGraph checkpoint using `app.update_state(..., as_node="execution_engine")`. This ensures the graph resumes exactly at the execution frontier.
*   **Resumption:** Triggers a re-entry into the execution frontier using a LangGraph command that routes execution back to `execution_engine`.

### The "Thaw" Pattern (Funds Wait Worker)
When a lane is blocked by funds, it calls `interrupt()`. The worker:
*   **FIFO Fairness:** Monitors the global queue and identifies the next eligible `FundsWaitRecord`.
*   **Secure Resumption:** Sends a resume payload back to the specific interrupted thread.
*   **Token Handshake:** The payload includes a cryptographically secure `resume_token`. The graph node verifies this token before allowing execution to "thaw" and proceed to the next preflight check.

**Verified Observation:** Behavioral trace proved that both workers correctly restart paused LangGraph workflows, transforming the agent into a **Durable Asynchronous Workflow engine** that can survive process restarts and long cross-chain delays.

---

## 5. Ecosystem Parity (The Multi-Chain Layer)

Volo is designed for cross-ecosystem intent execution, specifically handling the architectural differences between **Solana** and **EVM** through a normalized parity layer.

### Chain Canonicalization & Mapping (`core/chains/chain_canonicalization_parity.py`)
To resolve user intent into the correct network context, the system uses a **Scoped Parity Resolver**:
*   **Scoped Resolution:** Different logic is applied based on the action (e.g., `transfer` vs `balance`) to handle naming collisions.
*   **Alias Mapping:** Common names like "Eth Mainnet" are normalized to canonical keys (`ethereum`) using ecosystem-specific registries.

### Native Token Normalization
The system handles the "Native Token" problem by mapping human-friendly markers to ecosystem-specific sentinels:
*   **Solana:** Normalizes "native" or "SOL" to the Wrapped SOL mint (`So111...1112`).
*   **EVM:** Recognizes both the zero-address (`0x0...0`) and the common sentinel (`0xeee...e`) as aliases for the chain's native currency.

### Ecosystem Isolation
The system determines the correct implementation layer (e.g., `wallet_service/solana` vs. `wallet_service/evm`) based on the resolved `chain` context in the DAG. This ensures that:
*   **Address Formats:** Base58 (Solana) and Hex (EVM) are never mixed.
*   **Decimal Logic:** Solana's 9-decimal default is isolated from EVM's 18-decimal default.

**Verified Observation:** Behavioral trace proved that the system correctly maps "Mainnet" to the specific chain family (EVM or Solana) based on the intent context and accurately normalizes native token sentinels across both ecosystems.

---

## 6. The Fee Engine & Treasury

Volo implements a non-custodial fee collection system that integrates directly into the execution DAG, supporting both percentage-based and flat-fee models with strict ecosystem isolation.

### Dual-Mode Fee Calculation (`core/fees/fee_engine.py`)
To maintain reliability across different token types, the engine uses two calculation strategies:
*   **Native Percentage-Based:** For native token transactions (ETH, SOL), the system charges a basis point percentage (e.g., 20 bps for swaps).
*   **Flat Native for ERC-20:** To avoid oracle dependencies in V1, the system charges a calibrated flat amount of native token when moving ERC-20 tokens (e.g., 0.0005 ETH).

### Tiered Discount Reducer (`core/fees/fee_reducer.py`)
Fees are dynamically reduced based on user context using a stackable rule engine:
*   **Volume Discounts:** Tiers based on monthly USD volume.
*   **Platform Loyalty:** Discounts for holders of the protocol token (VOLO) and lifetime transaction counts.
*   **Safety Cap:** Total discounts are hard-capped at 50 bps to ensure protocol sustainability.

### Strict Ecosystem Treasury (`core/fees/treasury.py`)
The system enforces strict segregation of treasury funds through environment-based configuration:
*   **Family-Specific Addressing:** Requires explicit `FEE_TREASURY_EVM_ADDRESS` and `FEE_TREASURY_SOLANA_ADDRESS`.
*   **No Global Fallback in Fee Routing:** The active treasury lookup uses family-specific addresses only. This prevents cross-ecosystem fund routing.
*   **Fail-Safe Disable:** If a family-specific address is missing, fee collection for that ecosystem is automatically disabled (`quote_node` returns `None`), ensuring that lack of configuration never results in stuck or lost fees.

**Verified Observation:** Behavioral trace proved that the engine requires explicit family-level treasury addresses to enable quoting and correctly applies multi-factor discounts (e.g., -13 bps) before calculating the final native token requirement.
