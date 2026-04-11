
import asyncio
from decimal import Decimal
from graph.nodes.confirmation_node import confirmation_node
from core.planning.execution_plan import ExecutionPlan, PlanNode

async def test_confirmation_node_output():
    # Mock FeeQuote dictionary as it would be in AgentState
    fee_quote_dict = {
        "node_id": "node_1",
        "tool": "bridge",
        "chain": "Ethereum",
        "chain_family": "evm",
        "chain_network": "Ethereum",
        "native_symbol": "STT", # <--- The bug
        "base_fee_bps": 35,
        "discount_bps": 0,
        "final_fee_bps": 35,
        "fee_amount_native": "0.000200",
        "fee_recipient": "0x123",
        "discount_reasons": [],
        "expires_at": 9999999999,
        "is_native_tx": False
    }

    state = {
        "plan_history": [
            ExecutionPlan(
                goal="bridge 10 usdc from eth to base",
                nodes={
                    "node_1": PlanNode(
                        id="node_1",
                        tool="bridge",
                        args={
                            "amount": "10.0",
                            "token_symbol": "USDC",
                            "source_chain": "eth",
                            "target_chain": "base"
                        }
                    )
                }
            )
        ],
        "fee_quotes": [fee_quote_dict],
        "preflight_estimates": {}
    }

    result = await confirmation_node(state)
    print(result["messages"][0].content)

if __name__ == "__main__":
    import os
    os.environ["SKIP_MONGODB_HEALTHCHECK"] = "1"
    asyncio.run(test_confirmation_node_output())
