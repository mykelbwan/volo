
import os
from dotenv import load_dotenv

def check_env():
    load_dotenv()
    vars_to_check = [
        "FEE_TREASURY_ADDRESS",
        "FEE_TREASURY_EVM_ADDRESS",
        "FEE_TREASURY_SOLANA_ADDRESS",
        "VOLO_CLI_PROVIDER",
        "VOLO_CLI_USER_ID",
        "VOLO_CLI_THREAD_ID",
        "DEFAULT_CHAIN",
        "CHAIN",
        "NETWORK",
        "EVM_NATIVE_TOKEN_PLACEHOLDER"
    ]
    
    print("--- Environment Variables ---")
    for var in vars_to_check:
        print(f"{var}: {os.getenv(var)}")

    # Also check for any FLAT_FEE overrides
    for key, value in os.environ.items():
        if "FEE_FLAT_NATIVE" in key:
            print(f"{key}: {value}")

if __name__ == "__main__":
    check_env()
