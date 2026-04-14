# Volo: Massively Parallel Intent-Based Crypto Agent

**Volo** is a high-performance, intent-based crypto agent that abstracts blockchain complexity. It enables users to execute multi-turn, cross-chain transactions (swaps, bridges, transfers) through simple natural language, managed by a dependency-aware Directed Acyclic Graph (DAG) execution engine.

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
*   **Background Workers:** Dedicated workers (Bridge Status, Funds Wait) monitor long-running operations and "nudge" the execution graph back to life using LangGraph's `Command(resume=...)`.
*   **Idempotency & Safety:** Prevents double-spending through atomic claiming and nonce-gap probing.

### 4. Cross-Chain Parity
A unified interface for both **EVM** (Ethereum, Base, Arbitrum, etc.) and **Solana**.
*   **Normalization:** Unified handling of native tokens (ETH/SOL) and addresses.
*   **Ecosystem Isolation:** Prevents cross-ecosystem fee routing or address leakage.

---

## 🛠 How it Works: The Intent-to-DAG Pipeline

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
# LLM Provider (Google, OpenAI, Cohere, etc.)
GOOGLE_API_KEY=your_key

# Database
MONGODB_URI=mongodb://localhost:27017
REDIS_URL=redis://localhost:6379

# Treasury Addresses
FEE_TREASURY_EVM_ADDRESS=0x...
FEE_TREASURY_SOLANA_ADDRESS=...
```

### Running the CLI
Volo comes with a feature-rich CLI for testing.
```bash
# Run with status tables and debug logs
uv run command_line_tools/cli.py --skip-mongodb --show-status-table
```

---

## Documentation
*   [ARCHITECTURE.md](https://github.com/mykelbwan/volo/blob/master/volo_agent/ARCHITECTURE.md) - Deep dive into MPIE, DAG planning, and the safety model.
*   [docs/DAG_ARCHITECTURE.md](https://github.com/mykelbwan/volo/tree/master/volo_agent/docs/DAG_ARCHITECTURE.md) - Scheduling and execution-state model.
*   [docs/BRIDGE_STATUS_WORKER.md](https://github.com/mykelbwan/volo/tree/master/volo_agent/docs/BRIDGE_STATUS_WORKER.md) - Bridge status worker notes.
*   [docs/MPIE_ARCHITECTURE.md](https://github.com/mykelbwan/volo/tree/master/volo_agent/docs/MPIE_ARCHITECTURE.md) - Conversational-level MPIE and wait/resume flow.

---

## License
MIT
