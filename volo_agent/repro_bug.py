
import os
from decimal import Decimal
from core.fees.fee_engine import FeeEngine
from core.fees.fee_reducer import FeeContext
from core.planning.execution_plan import ExecutionPlan, PlanNode

# Mock environment variables for treasury
os.environ["FEE_TREASURY_ADDRESS"] = "0x1234567890123456789012345678901234567890"

def test_fee_quote():
    engine = FeeEngine()
    
    # Bridge 10 USDC from eth to base
    node = PlanNode(
        id="node_1",
        tool="bridge",
        args={
            "amount": "10",
            "token_symbol": "USDC",
            "source_chain": "eth",
            "target_chain": "base",
            "source_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", # USDC on Ethereum
        }
    )
    
    plan = ExecutionPlan(
        goal="bridge 10 usdc from eth to base",
        nodes={"node_1": node}
    )
    
    context = FeeContext(sender="0xsender")
    quotes = engine.quote_plan(plan, context)
    
    if not quotes:
        print("No quotes generated!")
        return
        
    for q in quotes:
        print(f"Node: {q.node_id}")
        print(f"Tool: {q.tool}")
        print(f"Chain: {q.chain}")
        print(f"Native Symbol: {q.native_symbol}")
        print(f"Fee Amount Native: {q.fee_amount_native}")
        print(f"Formatted Amount: {q.formatted_amount()}")

if __name__ == "__main__":
    test_fee_quote()
