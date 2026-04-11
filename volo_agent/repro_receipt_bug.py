
import os
from graph.nodes.confirmation_node import confirmation_node
from core.planning.execution_plan import ExecutionPlan, PlanNode

async def test_repro_stt_bug():
    # Simulate the state that produces the STT fee currency error
    # even when the tool is a bridge.
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
        "fee_quotes": [
            {
                "node_id": "node_1",
                "tool": "bridge",
                "chain": "Somnia Testnet", # This is the bug: the quote itself has the wrong chain
                "native_symbol": "STT",   # and symbol!
                "fee_amount_native": "0.000200",
                "fee_recipient": "0x123",
                "is_native_tx": False
            }
        ]
    }
    
    result = await confirmation_node(state)
    print(result["messages"][0].content)

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_repro_stt_bug())
