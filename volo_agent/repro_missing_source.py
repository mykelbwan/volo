
import os
from decimal import Decimal
from core.fees.fee_engine import FeeEngine
from core.fees.fee_reducer import FeeContext
from core.planning.execution_plan import ExecutionPlan, PlanNode

# Mock environment variables for treasury
os.environ["FEE_TREASURY_ADDRESS"] = "0x1234567890123456789012345678901234567890"

def test_fee_quote_with_misspelled_source_chain():
    engine = FeeEngine()
    
    # Bridge 10 USDC from eth to base.
    # What if source_chain is missing or misspelled in the node args,
    # and there's a global 'chain' default set to 'somnia'?
    node = PlanNode(
        id="node_1",
        tool="bridge",
        args={
            "amount": "10",
            "token_symbol": "USDC",
            # "source_chain": "eth", # <--- Missing
            "target_chain": "base",
            "chain": "somnia" # <--- Default
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
    test_fee_quote_with_misspelled_source_chain()
