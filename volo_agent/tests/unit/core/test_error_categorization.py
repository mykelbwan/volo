from web3.exceptions import ContractLogicError

from core.memory.ledger import ErrorCategory
from core.utils.errors import NonRetryableError, categorize_error


def test_categorize_error_slippage():
    err = RuntimeError("slippage exceeded")
    assert categorize_error(err) == ErrorCategory.SLIPPAGE


def test_categorize_error_contract_logic():
    err = ContractLogicError("revert")
    assert categorize_error(err) == ErrorCategory.LOGIC


def test_categorize_error_value_error():
    err = ValueError("bad param")
    assert categorize_error(err) == ErrorCategory.LOGIC


def test_categorize_error_non_retryable():
    err = NonRetryableError("final failure")
    assert categorize_error(err) == ErrorCategory.NON_RETRYABLE
