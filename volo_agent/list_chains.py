
from config.chains import CHAINS, _NAME_INDEX, _CHAIN_ALIASES

def list_chains():
    print("--- CHAINS ---")
    for cid, config in CHAINS.items():
        print(f"ID: {cid}, Name: {config.name}, Symbol: {config.native_symbol}")
        
    print("\n--- NAME INDEX ---")
    for name, config in _NAME_INDEX.items():
        print(f"Name: {name}, ID: {config.chain_id}")
        
    print("\n--- ALIASES ---")
    for alias, target in _CHAIN_ALIASES.items():
        print(f"Alias: {alias} -> {target}")

if __name__ == "__main__":
    list_chains()
