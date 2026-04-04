from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class ConversationTaskRecord:
    task_id: str
    conversation_id: str
    task_number: int
    execution_id: str
    thread_id: str
    provider: str
    provider_user_id: str
    user_id: str
    title: str
    status: str
    created_at: str
    updated_at: str
    created_at_dt: Optional[datetime] = None
    updated_at_dt: Optional[datetime] = None
    terminal_at: Optional[datetime] = None
    failed_expires_at: Optional[datetime] = None
    latest_summary: Optional[str] = None
    tool: Optional[str] = None
    tx_hash: Optional[str] = None
    error_category: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None
