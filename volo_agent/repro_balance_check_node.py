
import asyncio
from graph.nodes.balance_check_node import balance_check_node
from core.planning.execution_plan import ExecutionPlan, PlanNode

async def test_balance_check_node_fee_resolution():
    # Mock state where bridge node has both source_chain (eth) and a default chain (somnia)
    # The balance_check_node should ignore the 'chain' default if 'source_chain' is present.
    state = {
        "user_id": "cli_demo_user",
        "provider": "cli",
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
                            "target_chain": "base",
                            "chain": "somnia" # The problematic default
                        }
                    )
                }
            )
        ]
    }

    result = await balance_check_node(state)
    fee_quotes = result.get("fee_quotes", [])
    print(f"--- Fee Quotes generated ---")
    for q in fee_quotes:
        print(f"Tool: {q.get('tool')}, Chain: {q.get('chain')}, Symbol: {q.get('native_symbol')}")

if __name__ == "__main__":
    import os
    os.environ["FEE_TREASURY_ADDRESS"] = "0x1234567890123456789012345678901234567890"
    os.environ["SKIP_MONGODB_HEALTHCHECK"] = "1"
    asyncio.run(test_balance_check_node_fee_resolution())
