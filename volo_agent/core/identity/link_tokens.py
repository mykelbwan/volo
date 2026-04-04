from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Dict

from pymongo.errors import DuplicateKeyError

from core.identity.errors import (
    ExpiredLinkTokenError,
    InvalidLinkTokenError,
    RevokedLinkTokenError,
    UsedLinkTokenError,
)
from core.identity.repository import IdentityRepository


@dataclass(frozen=True)
class LinkTokenManager:
    repository: IdentityRepository
    utc_now: Callable[[], datetime]
    normalize_provider_user_id: Callable[[str], str]
    normalize_token: Callable[[str], str]

    def build_link_token_doc(
        self,
        *,
        volo_user_id: str,
        token: str,
        created_at: datetime,
        expires_at: datetime,
    ) -> Dict[str, Any]:
        return {
            "token": token,
            "volo_user_id": volo_user_id,
            "status": "issued",
            "created_at": created_at,
            "expires_at": expires_at,
            "used_at": None,
        }

    @staticmethod
    def token_is_used(token_doc: Dict[str, Any]) -> bool:
        return (
            bool(token_doc.get("used_at"))
            or str(token_doc.get("status", "")).lower() == "used"
        )

    @staticmethod
    def token_is_revoked(token_doc: Dict[str, Any]) -> bool:
        return str(token_doc.get("status", "")).lower() == "revoked"

    async def issue(self, volo_user_id: str, *, ttl_seconds: int) -> str:
        if ttl_seconds <= 0:
            raise ValueError(
                "Invalid link token TTL. Recovery path: pass ttl_seconds > 0 when requesting a token."
            )
        now = self.utc_now()
        expires_at = now + timedelta(seconds=ttl_seconds)
        max_attempts = 5
        for _ in range(max_attempts):
            token = uuid.uuid4().hex[:8].upper()
            try:
                await self.repository.revoke_issued_link_tokens(volo_user_id, now)
                await self.repository.insert_link_token(
                    self.build_link_token_doc(
                        volo_user_id=volo_user_id,
                        token=token,
                        created_at=now,
                        expires_at=expires_at,
                    )
                )
                return token
            except DuplicateKeyError:
                continue
        raise RuntimeError(
            "Could not generate a unique linking token after 5 attempts. "
            "Recovery path: retry shortly; if collisions persist, increase token length or investigate token-index integrity."
        )

    async def claim(
        self,
        token: str,
        *,
        provider: str,
        provider_user_id: str,
        now: datetime | None = None,
    ) -> Dict[str, Any]:
        claim_time = now or self.utc_now()
        normalized = self.normalize_token(token)
        claimed = await self.repository.claim_link_token(
            normalized,
            provider=provider,
            provider_user_id=self.normalize_provider_user_id(provider_user_id),
            now=claim_time,
        )
        if claimed:
            return claimed

        token_doc = await self.repository.get_link_token(normalized)
        if not token_doc:
            raise InvalidLinkTokenError()
        if self.token_is_used(token_doc):
            raise UsedLinkTokenError()
        if self.token_is_revoked(token_doc):
            raise RevokedLinkTokenError()

        expires_at = token_doc.get("expires_at")
        if expires_at and expires_at <= claim_time:
            raise ExpiredLinkTokenError()
        raise InvalidLinkTokenError()
