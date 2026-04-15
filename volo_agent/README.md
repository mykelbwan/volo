# Volo: Reliable Execution For Complex Onchain Actions

**Volo** is an execution copilot for serious crypto users. It helps users execute complex, multi-step, cross-chain actions through simple natural language, with a dependency-aware workflow engine built for safety, async recovery, and dependable completion.

Unlike traditional linear agents, Volo supports **Massively Parallel Intent Execution (MPIE)**, allowing a single session to manage multiple independent transaction lanes concurrently with strict resource isolation.

---

## Key Features

### 1. Natural Language to DAG Pipeline
Volo decomposes complex, multi-step prompts into a strictly typed execution plan. It automatically infers dependencies between actions (e.g., waiting for a bridge to complete before starting a swap).
*   **Fast-Path Resolution:** Optimized regex handles 80% of standard intents with zero LLM latency.
*   **Context Carrying:** Outputs from one step (e.g., "Bridge 1 ETH") are automatically carried into the next (e.g., "then swap it for USDC").

### 2. MPIE (Massively Parallel Intent Execution)
Volo treats every new intent as an independent "Task Lane." 
*   **Thread Isolation:** Independent intents are allocated to fresh LangGraph threads, preventing cross-talk.
*   **Disambiguation:** If a user sends a follow-up (e.g., "confirm") while multiple tasks are active, Volo identifies the ambiguity and asks for clarification.

### 3. Durable Asynchronous Workflows
Volo is designed for the "Ghost Transaction" and "Slow Bridge" reality of crypto.
*   **Background Workers:** Dedicated workers (Bridge Status, Funds Wait) monitor long-running operations and resume paused execution from persisted graph state.
*   **Idempotency & Safety:** Prevents double-spending through atomic claiming and nonce-gap probing.

### 4. Cross-Chain Parity
A unified interface for both **EVM** (Ethereum, Base, Arbitrum, etc.) and **Solana**.
*   **Normalization:** Unified handling of native tokens (ETH/SOL) and addresses.
*   **Ecosystem Isolation:** Prevents cross-ecosystem fee routing or address leakage.

### 5. Built For Serious Crypto Users
Volo is strongest when execution gets messy.
*   **Complex Onchain Actions:** Bridge, swap, transfer, wait, and continue from one command.
*   **Confidence Under Complexity:** Designed to reduce manual coordination, retry anxiety, and broken multi-step flows.

---

## How it Works: The Intent-to-DAG Pipeline

When you give Volo a command, it goes through three stages:

1.  **Semantic Parsing:** Decomposes the prompt into atomic `Intents`.
2.  **DAG Resolution:** Identifies dependencies using "Marker Detection" (e.g., `{{OUTPUT_OF:step_0}}`) and "Barrier Keys" to ensure transactional integrity.
3.  **Frontier Scheduling:** The execution engine identifies "Ready" nodes and executes them, suspending the graph if a node requires an external wait (like a bridge).

### Verified Observation: Multi-Step Intent
**Prompt:** *"Bridge 1 ETH from Ethereum to Base then swap it for USDC"*

| Step | Tool | Dependency | Context |
| :--- | :--- | :--- | :--- |
| `step_0` | `bridge` | None | `1 ETH` from `ethereum` to `base` |
| `step_1` | `swap` | `step_0` | Swap `ETH` (output of `step_0`) for `USDC` |

---

## Quick Start

### Prerequisites
*   [Python 3.12+](https://www.python.org/)
*   [uv](https://github.com/astral-sh/uv) (Recommended) or `pip`
*   MongoDB (for state persistence)
*   Redis (for event notifications)

### Installation
```bash
git clone https://github.com/mykelbwan/volo.git
cd volo/volo_agent
uv sync
```

### Configuration
Create a `.env` file in the root:
```env
# Core app
MONGODB_URI=mongodb://localhost:27017
REDIS_URL=redis://localhost:6379

# Coinbase / CDP wallet execution
CDP_API_KEY_ID=your_key_id
CDP_API_KEY_SECRET=your_key_secret
CDP_WALLET_SECRET=your_wallet_secret

# Security / model providers used by the current runtime
GOPLUS_SECURITY_KEY=your_key
GEMINI_API_KEY1=...
GEMINI_API_KEY2=...
COHERE_API_KEY=your_key
HUGGINGFACE_API_KEY=your_key

# Treasury Addresses
FEE_TREASURY_EVM_ADDRESS=0x...
FEE_TREASURY_SOLANA_ADDRESS=...
```

Notes:
*   The checked-in runtime currently expects more than a minimal demo `.env`. See [config/env.py](https://github.com/mykelbwan/volo/tree/master/volo_agent/config/env.py) for the live set of required variables.
*   Fee routing is family-specific. `FEE_TREASURY_EVM_ADDRESS` and `FEE_TREASURY_SOLANA_ADDRESS` are the active treasury variables used by the fee engine.

### Running the CLI
Volo comes with a feature-rich CLI for testing.
```bash
# Run with status tables and debug logs
uv run command_line_tools/cli.py --skip-mongodb --show-status-table
```

### Running the API
Volo also exposes a turn-based API.
```bash
uv run uvicorn main:app --reload
```

Primary endpoint:
*   `POST /v1/agent/turn` - submit one user turn and receive the latest assistant reply for the selected task lane / thread.

---

## Documentation
*   [ARCHITECTURE.md](https://github.com/mykelbwan/volo/blob/master/volo_agent/ARCHITECTURE.md) - Deep dive into MPIE, DAG planning, and the safety model.
*   [docs/DAG_ARCHITECTURE.md](https://github.com/mykelbwan/volo/tree/master/volo_agent/docs/DAG_ARCHITECTURE.md) - Scheduling and execution-state model.
*   [docs/BRIDGE_STATUS_WORKER.md](https://github.com/mykelbwan/volo/tree/master/volo_agent/docs/BRIDGE_STATUS_WORKER.md) - Bridge status worker notes.
*   [docs/MPIE_ARCHITECTURE.md](https://github.com/mykelbwan/volo/tree/master/volo_agent/docs/MPIE_ARCHITECTURE.md) - Conversational-level MPIE and wait/resume flow.

---
