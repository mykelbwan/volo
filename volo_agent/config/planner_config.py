from __future__ import annotations

# Central planner configuration defaults (version-controlled).
# Adjust here for repo-wide behavior; optional env override exists in scorer.

# Hard cap on optimizer candidates to avoid combinatorial explosion.
MAX_CANDIDATE_PLANS = 5

# Per-node route alternatives considered by route_planner when building
# candidate execution plans. The baseline best route is always included,
# so this only controls runner-up variants.
MAX_ROUTE_ALTERNATIVES_PER_NODE = 2

# Deterministic scoring weights used by the multi-plan optimizer.
PLAN_SCORE_OUTPUT_WEIGHT = 1.0
PLAN_SCORE_GAS_WEIGHT = 0.05
PLAN_SCORE_RISK_WEIGHT = 25.0
PLAN_SCORE_LATENCY_WEIGHT = 0.0005

# Reliability multiplier used by the plan scorer. Float >= 0.0.
RELIABILITY_MULTIPLIER = 0.2

# Minimum total runs required to trust observed success rates.
# Below this, the scorer uses an optimistic prior to avoid noisy swings.
MIN_TOTAL_RUNS_FOR_RELIABILITY = 5
