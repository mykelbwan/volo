from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Dict, Optional


class IdentityRecords:
    def __init__(
        self,
        *,
        utc_now: Callable[[], datetime],
        normalize_provider_user_id: Callable[[str], str],
    ) -> None:
        self._utc_now = utc_now
        self._normalize_provider_user_id = normalize_provider_user_id

    def sanitize_user(self, user: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if user and "_id" in user:
            user = dict(user)
            del user["_id"]
        return user

    def identity_selector(self, provider: str, provider_user_id: str) -> Dict[str, Any]:
        return {
            "provider": provider,
            "provider_user_id": self._normalize_provider_user_id(provider_user_id),
        }

    def find_identity(
        self, user: Optional[Dict[str, Any]], provider: str, provider_user_id: str
    ) -> Optional[Dict[str, Any]]:
        if not user:
            return None
        selector = self.identity_selector(provider, provider_user_id)
        for identity in user.get("identities", []):
            if (
                identity.get("provider") == selector["provider"]
                and identity.get("provider_user_id") == selector["provider_user_id"]
            ):
                return identity
        return None

    def build_identity(
        self,
        *,
        provider: str,
        provider_user_id: str,
        username: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        is_primary: bool = False,
    ) -> Dict[str, Any]:
        now = self._utc_now()
        return {
            "provider": provider,
            "provider_user_id": self._normalize_provider_user_id(provider_user_id),
            "username": username,
            "username_history": [],
            "linked_at": now,
            "last_seen_at": now,
            "is_primary": is_primary,
            "metadata": metadata or {},
        }

    def select_new_primary_identity(
        self,
        identities: list[Dict[str, Any]],
        *,
        provider: str,
        provider_user_id: str,
    ) -> Optional[Dict[str, Any]]:
        normalized_provider_user_id = self._normalize_provider_user_id(provider_user_id)
        for identity in identities:
            if (
                identity.get("provider") == provider
                and identity.get("provider_user_id") == normalized_provider_user_id
            ):
                continue
            return identity
        return None
