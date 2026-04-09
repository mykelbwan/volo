# Volo

**Volo** is an intent-based crypto agent that abstracts blockchain complexity, enabling users to execute multi-turn, cross-chain transactions like swapping and bridging through simple natural language.

Architecture docs

[docs/MPIE_ARCHITECTURE.md][docs/MPIE_ARCHITECTURE.md](https://github.com/mykelbwan/volo/blob/master/volo_agent/docs/MPIE_ARCHITECTURE.md) - conversational-level MPIE architecture, wait/resume flow, and security model.

[docs/DAG_ARCHITECTURE.md](https://github.com/mykelbwan/volo/blob/master/volo_agent/docs/DAG_ARCHITECTURE.md) - DAG planning, scheduling, mutation, and execution-state model.

[docs/BRIDGE_STATUS_WORKER.md](https://github.com/mykelbwan/volo/blob/master/volo_agent/docs/BRIDGE_STATUS_WORKER.md) - bridge status worker notes.
<!--
Admin scripts

- `volo_agent/scripts/update_fee_table.py` — import and validate bridge fee tables (CSV/JSON) and optionally deploy them to `volo_agent/config/bridge_fee_table.json`.

Usage:

    python volo_agent/scripts/update_fee_table.py --input path/to/table.csv --deploy

The script validates rows and writes a canonical JSON file suitable for the VWS FeeTable loader.-->
docs/MPIE_ARCHITECTURE — conversational-level MPIE architecture, wait/resume flow, and security model.
docs/DAG_ARCHITECTURE — DAG planning, scheduling, mutation, and execution-state model.
docs/DESIGN_PRINCIPLES — project design principles and implementation constraints.
docs/BRIDGE_STATUS_WORKER — bridge status worker notes.
docs/REV_GEN_PATHS — revision generation path notes.
