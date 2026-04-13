import web3.exceptions

from core.memory.ledger import ErrorCategory


class NonRetryableError(RuntimeError):
    """Error type that should not be retried by the execution engine."""


class DeterminismViolationError(NonRetryableError):
    """Raised when execution diverges from the planned deterministic route."""


class RouteExpiredError(NonRetryableError):
    """Raised when a planned route has expired before execution."""


class SlippageExceededError(NonRetryableError):
    """Raised when actual output falls below the planned minimum output."""


def categorize_error(error: Exception) -> ErrorCategory:
    """
    Heuristically maps an exception to an ErrorCategory based on type and message.
    """
    err_str = str(error).lower()

    # Explicit non-retryable
    if isinstance(error, NonRetryableError):
        if isinstance(error, SlippageExceededError):
            return ErrorCategory.SLIPPAGE
        return ErrorCategory.NON_RETRYABLE

    # Security & Guardrails
    from core.security.guardrails import RiskViolationError

    if isinstance(error, RiskViolationError):
        return ErrorCategory.SECURITY

    # Network & RPC
    if isinstance(
        error, (web3.exceptions.Web3Exception, TimeoutError, ConnectionError)
    ):
        if "timeout" in err_str or "503" in err_str or "connection" in err_str:
            return ErrorCategory.NETWORK

    # Liquidity & Price
    if (
        "liquidity" in err_str
        or "no route" in err_str
        or "depth" in err_str
        or "insufficient liquidity" in err_str
    ):
        return ErrorCategory.LIQUIDITY

    # Slippage
    if (
        "slippage" in err_str
        or "price impact" in err_str
        or "too much" in err_str
        or "insufficient output amount" in err_str
        or "too little received" in err_str
    ):
        return ErrorCategory.SLIPPAGE

    # Gas
    if "gas" in err_str or "fee" in err_str or "underpriced" in err_str:
        return ErrorCategory.GAS

    # Contract Logic
    if isinstance(error, web3.exceptions.ContractLogicError):
        return ErrorCategory.LOGIC

    # Parameters/Validation
    if isinstance(error, (ValueError, TypeError, KeyError)):
        return ErrorCategory.LOGIC

    return ErrorCategory.UNKNOWN
