import os

from dotenv import load_dotenv
from pydantic import SecretStr

load_dotenv()


def _alias_env(target: str, source: str) -> None:
    if not os.getenv(target) and os.getenv(source):
        os.environ[target] = os.getenv(source)  # type: ignore[arg-type]


# Map Coinbase env vars to CDP SDK defaults when provided.
_alias_env("CDP_API_KEY_ID", "COINBASE_API_KEY_ID")
_alias_env("CDP_API_KEY_SECRET", "COINBASE_SECRET_KEY")
_alias_env("CDP_WALLET_SECRET", "COINBASE_SERVER_WALLET")

SOMNIA_CHAIN = os.getenv("SOMNIA_CHAIN")
SOMNIA_RPC_URL = os.getenv("SOMNIA_TESTNET_RPC_URL")

CDP_API_KEY_ID = os.getenv("CDP_API_KEY_ID")
if CDP_API_KEY_ID is None:
    raise ValueError("CDP_API_KEY_ID environment variable is not set")

CDP_API_KEY_SECRET = os.getenv("CDP_API_KEY_SECRET")
if CDP_API_KEY_SECRET is None:
    raise ValueError("CDP_API_KEY_SECRET environment variable is not set")

CDP_WALLET_SECRET = os.getenv("CDP_WALLET_SECRET")
if CDP_WALLET_SECRET is None:
    raise ValueError("CDP_WALLET_SECRET environment variable is not set")

MONGODB_ATLAS_PASSWORD = os.getenv("MONGODB_ATLAS_PASSWORD")

MONGODB_URI = os.getenv("MONGODB_URI")
if MONGODB_URI:
    if not (
        MONGODB_URI.startswith("mongodb://") or MONGODB_URI.startswith("mongodb+srv://")
    ):
        raise ValueError("MONGODB_URI must start with mongodb:// or mongodb+srv://")

if not MONGODB_URI and not MONGODB_ATLAS_PASSWORD:
    raise ValueError(
        "MongoDB connection is not configured. "
        "Set MONGODB_URI (full connection string) or MONGODB_ATLAS_PASSWORD "
        "in your environment / .env file."
    )


GOPLUS_SECURITY_KEY = os.getenv("GOPLUS_SECURITY_KEY")
if GOPLUS_SECURITY_KEY is None:
    raise ValueError("GOPLUS_SECURITY_KEY environment variable is not set")

GEMINI_API_KEYS = [os.getenv(f"GEMINI_API_KEY{i}") for i in range(1, 7)]
(
    GEMINI_API_KEY1,
    GEMINI_API_KEY2,
) = GEMINI_API_KEYS

if GEMINI_API_KEY1 is None or GEMINI_API_KEY2 is None:
    raise ValueError("GEMINI_API_KEY environment variable is not set")

GEMINI_API_KEYS = [SecretStr(key) for key in GEMINI_API_KEYS if key is not None]

(
    GEMINI_API_KEY1,
    GEMINI_API_KEY2,
) = GEMINI_API_KEYS

COHERE_API_KEY = os.getenv("COHERE_API_KEY")
if COHERE_API_KEY is None:
    raise ValueError("COHERE_API_KEY environment variable is not set")
COHERE_API_KEY = SecretStr(COHERE_API_KEY)
HUGGINGFACE_API_KEY = os.getenv("HUGGINGFACE_API_KEY")
if HUGGINGFACE_API_KEY is None:
    raise ValueError("HUGGINGFACE_API_KEY environment variable is not set")
HUGGINGFACE_API_KEY = SecretStr(HUGGINGFACE_API_KEY)
