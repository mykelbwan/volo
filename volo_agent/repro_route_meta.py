
import os
from decimal import Decimal
from core.fees.fee_engine import FeeEngine
from core.fees.fee_reducer import FeeContext
from core.planning.execution_plan import ExecutionPlan, PlanNode

# Mock environment variables for treasury
os.environ["FEE_TREASURY_ADDRESS"] = "0x1234567890123456789012345678901234567890"

def test_fee_quote_with_route_metadata():
    engine = FeeEngine()
    
    # Bridge 10 USDC from eth to base.
    # We include route metadata which might be influencing resolution if it's being used somewhere.
    node = PlanNode(
        id="node_1",
        tool="bridge",
        args={
            "amount": "10",
            "token_symbol": "USDC",
            "source_chain": "eth",
            "target_chain": "base",
            "source_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        },
        metadata={
            "route": {
                "aggregator": "mayan",
                "source_chain": "Ethereum", # Note: mixed case
                "target_chain": "Base",
                "native_symbol": "STT" # <--- What if this is here?
            }
        }
    )
    
    plan = ExecutionPlan(
        goal="bridge 10 usdc from eth to base",
        nodes={"node_1": node}
    )
    
    context = FeeContext(sender="0xsender")
    quotes = engine.quote_plan(plan, context)
    
    for q in quotes:
        print(f"Node: {q.node_id}")
        print(f"Tool: {q.tool}")
        print(f"Chain: {q.chain}")
        print(f"Native Symbol: {q.native_symbol}")
        print(f"Formatted Amount: {q.formatted_amount()}")

if __name__ == "__main__":
    test_fee_quote_with_route_metadata()
