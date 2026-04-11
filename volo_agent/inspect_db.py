
import asyncio
import os
from dotenv import load_dotenv
from core.database.mongodb import MongoDB

async def inspect_db():
    load_dotenv()
    thread_id = "cli_demo_thread"
    client = MongoDB.get_client()
    db = client["volo_react_agent"]
    collection = db["checkpoints"]
    
    # Get latest checkpoint for thread
    latest = collection.find_one({"thread_id": thread_id}, sort=[("ts", -1)])
    if not latest:
        print(f"No checkpoint found for thread {thread_id}")
        return
        
    checkpoint = latest.get("checkpoint", {})
    channel_values = checkpoint.get("channel_values", {})
    
    fee_quotes = channel_values.get("fee_quotes")
    print(f"--- Fee Quotes in DB ---")
    print(fee_quotes)
    
    intents = channel_values.get("intents")
    print(f"\n--- Intents in DB ---")
    print(intents)
    
    plan_history = channel_values.get("plan_history")
    if plan_history:
        print(f"\n--- Latest Plan in DB ---")
        # plan_history is usually a list of serialized plans
        print(plan_history[-1])

if __name__ == "__main__":
    asyncio.run(inspect_db())
