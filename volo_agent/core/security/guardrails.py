from typing import List, Optional

from pydantic import BaseModel

from core.planning.execution_plan import ExecutionState, PlanNode, StepStatus


class GuardrailPolicy(BaseModel):
    max_retries_per_step: Optional[int] = None
    max_slippage_percent: Optional[float] = None
    max_parallel_nodes: Optional[int] = None
    blocked_chains: Optional[List[str]] = None
    min_amount_usd: Optional[float] = None  # Prevent spamming tiny transactions


class RiskViolationError(Exception):
    """Raised when a plan node violates a security guardrail."""

    pass


class GuardrailService:
    def __init__(self, policy: Optional[GuardrailPolicy] = None):
        self.policy = policy

    def validate_node(self, node: PlanNode, current_state: ExecutionState):
        if self.policy is None:
            return True

        # 1. Enforce Max Retries
        if self.policy.max_retries_per_step is not None:
            node_max_retries = node.retry_policy.get("max_retries", 0)
            if node_max_retries > self.policy.max_retries_per_step:
                raise RiskViolationError(
                    f"Node '{node.id}' violates Max Retries guardrail: "
                    f"requested {node_max_retries}, but global limit is {self.policy.max_retries_per_step}"
                )

        # 2. Enforce Max Slippage (if applicable)
        if self.policy.max_slippage_percent is not None and "slippage" in node.args:
            requested_slippage = float(node.args["slippage"])
            if requested_slippage > self.policy.max_slippage_percent:
                raise RiskViolationError(
                    f"Node '{node.id}' violates Max Slippage guardrail: "
                    f"requested {requested_slippage}%, but global limit is {self.policy.max_slippage_percent}%"
                )

        # 3. Enforce Chain Whitelist/Blacklist
        if self.policy.blocked_chains:
            chain = node.args.get("chain") or node.args.get("source_chain")
            if chain and chain.lower() in [
                c.lower() for c in self.policy.blocked_chains
            ]:
                raise RiskViolationError(
                    f"Node '{node.id}' attempts to use blocked chain: {chain}"
                )

        # 4. Enforce Parallelism Limit
        if self.policy.max_parallel_nodes is not None:
            running_nodes = [
                sid
                for sid, s in current_state.node_states.items()
                if s.status == StepStatus.RUNNING
            ]
            if len(running_nodes) >= self.policy.max_parallel_nodes:
                raise RiskViolationError(
                    f"System exceeds Max Parallel Transactions guardrail: "
                    f"limit is {self.policy.max_parallel_nodes}"
                )

        # 5. Enforce Minimum Amount (to prevent spam/dust)
        if self.policy.min_amount_usd is not None:
            try:
                min_amount = float(self.policy.min_amount_usd)
            except (TypeError, ValueError):
                min_amount = None
        else:
            min_amount = None

        if min_amount is not None and min_amount > 0:
            amount = node.args.get("amount") or node.args.get("amount_in")
            if amount is not None:
                try:
                    # Heuristic: if amount is strictly less than policy min
                    if float(amount) < min_amount:
                        raise RiskViolationError(
                            f"Node '{node.id}' violates Minimum Amount guardrail: "
                            f"amount {amount} is below the dust limit of {min_amount}"
                        )
                except (ValueError, TypeError):
                    pass  # Skip if amount isn't a number (e.g. placeholder)

        return True
