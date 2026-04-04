from __future__ import annotations

import asyncio
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from wallet_service.evm.nonce_manager import (
    AsyncUpstashNonceManager,
    _as_int,
    _fetch_pending_async,
    get_async_nonce_manager,
    is_nonce_error,
    rollback_after_signing_error_async,
)


@pytest.mark.asyncio
async def test_as_int_garbage_inputs():
    """Rigorous check of _as_int with non-standard and failing inputs."""
    assert _as_int(None) is None
    assert _as_int("") is None
    assert _as_int("not a number") is None
    assert _as_int(b"invalid string") is None
    assert _as_int(b"\xff\xfe\xfd") is None  # Invalid UTF-8
    assert _as_int({"key": 1}) is None
    assert _as_int([1, 2]) is None
    assert _as_int(object()) is None
    # Bytes that are valid integers
    assert _as_int(b"42") == 42
    # Large numbers
    big = 10**50
    assert _as_int(big) == big
    assert _as_int(str(big)) == big


@pytest.mark.asyncio
async def test_rollback_after_signing_error_actually_awaits():
    """Verify the critical bug fix: rollback MUST be awaited."""
    mock_manager = AsyncMock()
    # We want to ensure it calls the 'rollback' method of the manager and awaits it.
    mock_manager.rollback = AsyncMock(return_value=123)
    
    with patch("wallet_service.evm.nonce_manager.get_async_nonce_manager", return_value=mock_manager):
        w3 = MagicMock()
        w3.to_checksum_address = MagicMock(side_effect=lambda x: x.lower())
        
        # If this isn't awaited, rollback won't happen.
        # But rollback_after_signing_error_async is itself async.
        success = await rollback_after_signing_error_async("0xSENDER", 1, 10, w3)
        
        assert success is True
        mock_manager.rollback.assert_awaited_once_with("0xSENDER", 1, 10, w3)


@pytest.mark.asyncio
async def test_redis_exhaustion_fails_closed():
    """Non-happy path: Redis is down or timing out."""
    mock_redis = AsyncMock()
    mock_redis.eval.side_effect = Exception("Redis Timeout")
    
    manager = AsyncUpstashNonceManager(mock_redis)
    # Reduce retries for test speed
    manager._redis_retry_attempts = 2
    
    w3 = MagicMock()
    
    with pytest.raises(RuntimeError) as excinfo:
        await manager.allocate("0xabc", 1, w3)
    
    assert "Redis nonce allocation is unavailable" in str(excinfo.value)
    assert mock_redis.eval.call_count == 2


@pytest.mark.asyncio
async def test_redis_script_returns_garbage():
    """Non-happy path: Lua script returns something unexpected (None/String)."""
    mock_redis = AsyncMock()
    mock_redis.eval.return_value = "this is not an int"
    
    manager = AsyncUpstashNonceManager(mock_redis)
    w3 = MagicMock()
    
    with pytest.raises(RuntimeError) as excinfo:
        await manager.allocate("0xabc", 1, w3)
        
    assert "Redis nonce allocation is unavailable" in str(excinfo.value)
    # The original error should be the cause
    assert "non-integer result" in str(excinfo.value.__cause__)


@pytest.mark.asyncio
async def test_idempotency_key_ttl_is_short():
    """Verify that we use the shorter _OP_IDEMPOTENCY_TTL_SECONDS for op_keys."""
    mock_redis = AsyncMock()
    mock_redis.eval.return_value = 100
    
    manager = AsyncUpstashNonceManager(mock_redis)
    # _OP_IDEMPOTENCY_TTL_SECONDS default is 60.
    assert manager._op_ttl_seconds == 60
    
    w3 = MagicMock()
    await manager.allocate("0xabc", 1, w3)
    
    # args order for allocate: now, ttl, op_ttl, reconcile_after
    args = mock_redis.eval.call_args[1]['args']
    assert args[2] == "60" # op_ttl should be 60, not 1200


@pytest.mark.asyncio
async def test_get_nonce_manager_requires_redis_by_default():
    """Security check: Should not fall back to local unless explicitly allowed."""
    with patch("wallet_service.evm.nonce_manager.upstash_configured", return_value=False):
        with patch("os.getenv", return_value=""): # ALLOW_UNSAFE_LOCAL_NONCE_MANAGER not set
            with pytest.raises(RuntimeError) as excinfo:
                await get_async_nonce_manager()
            assert "Redis-backed nonce management is required" in str(excinfo.value)


@pytest.mark.asyncio
async def test_fetch_pending_async_handles_delayed_awaitable():
    """Verify _fetch_pending_async correctly awaits the result if it's a coroutine."""
    w3 = MagicMock()
    w3.to_checksum_address.side_effect = lambda x: x
    
    # Mock result as an awaitable
    async def mock_call():
        await asyncio.sleep(0.01)
        return 55
        
    w3.eth.get_transaction_count.return_value = mock_call()
    
    res = await _fetch_pending_async(w3, "0x123")
    assert res == 55


@pytest.mark.asyncio
async def test_is_nonce_error_comprehensive():
    """Test various error strings including common RPC provider variations."""
    assert is_nonce_error(Exception("execution reverted: nonce too low")) is True
    assert is_nonce_error(Exception("nonce is too high")) is True
    assert is_nonce_error(Exception("replacement transaction underpriced")) is True
    assert is_nonce_error(Exception("Known transaction")) is True
    assert is_nonce_error(Exception("Transaction pool is full")) is False
    assert is_nonce_error(Exception("insufficient funds")) is False


@pytest.mark.asyncio
async def test_rollback_on_upstash_manager_correct_args():
    """Verify rollback correctly passes failed_nonce and rpc_pending to Lua."""
    mock_redis = AsyncMock()
    mock_redis.eval.return_value = 10
    
    manager = AsyncUpstashNonceManager(mock_redis)
    
    w3 = AsyncMock()
    w3.eth.get_transaction_count.return_value = 5 # RPC says 5
    w3.to_checksum_address.side_effect = lambda x: x
    
    # User tried nonce 9, failed.
    await manager.rollback("0xsender", 1, 9, w3)
    
    # Args for rollback: failed_nonce, rpc_pending, now, ttl, op_ttl
    args = mock_redis.eval.call_args[1]['args']
    assert args[0] == "9" # failed_nonce
    assert args[1] == "5" # rpc_pending
