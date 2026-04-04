from __future__ import annotations

import inspect
import logging
import os
import uuid
from datetime import datetime
from typing import Any, Dict, Optional, TypeGuard

from pymongo.errors import DuplicateKeyError

from core.identity.errors import (
    IdentityConflictError,
    IdentityNotFoundError,
    LastIdentityError,
    LinkAccountError,
    LinkAttachError,
    LinkTargetMissingError,
)
from core.identity.link_tokens import LinkTokenManager
from core.identity.provisioning import (
    EvmWalletProvisioner,
    FunctionWalletProvisioner,
    ProvisioningFn,
    ProvisioningPayload,
    SolanaWalletProvisioner,
    WalletProvisioner,
)
from core.identity.records import IdentityRecords
from core.identity.repository import IdentityRepository
from core.utils.async_tools import run_blocking

LINK_TOKEN_TTL_SECONDS = 15 * 60

logger = logging.getLogger("volo.user_service")


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


_SOLANA_PROVISION_TIMEOUT_SECONDS = _float_env(
    "VOLO_SOLANA_PROVISION_TIMEOUT_SECONDS", 5.0
)
_SOLANA_PROVISION_RETRY_SECONDS = _int_env("VOLO_SOLANA_PROVISION_RETRY_SECONDS", 900)


class AsyncIdentityService:
    def __init__(
        self,
        *,
        repository: IdentityRepository | None = None,
        evm_provisioner: WalletProvisioner | ProvisioningFn | None = None,
        solana_provisioner: WalletProvisioner | ProvisioningFn | None = None,
        provision_evm: ProvisioningFn | None = None,
        provision_solana: ProvisioningFn | None = None,
    ) -> None:
        self._repo = repository or IdentityRepository()
        self._evm_provisioner = self._coerce_provisioner(
            provisioner=evm_provisioner or provision_evm,
            default=EvmWalletProvisioner(),
            provisioner_name="evm_provisioner",
        )
        self._solana_provisioner = self._coerce_provisioner(
            provisioner=solana_provisioner or provision_solana,
            default=SolanaWalletProvisioner(),
            provisioner_name="solana_provisioner",
        )
        self._records = IdentityRecords(
            utc_now=self._utc_now,
            normalize_provider_user_id=self._normalize_provider_user_id,
        )
        self._link_tokens = LinkTokenManager(
            repository=self._repo,
            utc_now=self._utc_now,
            normalize_provider_user_id=self._normalize_provider_user_id,
            normalize_token=self._normalize_token,
        )

    @staticmethod
    def _is_wallet_provisioner(value: object) -> TypeGuard[WalletProvisioner]:
        return isinstance(value, WalletProvisioner)

    @staticmethod
    def _coerce_provisioner(
        provisioner: WalletProvisioner | ProvisioningFn | None,
        *,
        default: WalletProvisioner,
        provisioner_name: str,
    ) -> WalletProvisioner:
        if provisioner is None:
            return default
        if AsyncIdentityService._is_wallet_provisioner(provisioner):
            return provisioner
        if not callable(provisioner):
            raise TypeError(
                f"Invalid {provisioner_name}: expected a WalletProvisioner or callable. "
                "Pass a synchronous function with signature (volo_user_id: str) -> dict."
            )
        if inspect.iscoroutinefunction(provisioner) or inspect.iscoroutinefunction(
            getattr(provisioner, "__call__", None)
        ):
            raise TypeError(
                f"Invalid {provisioner_name}: async callables are not supported. "
                "Use a synchronous provisioner function; the service executes it "
                "without blocking the event loop via run_blocking."
            )
        return FunctionWalletProvisioner(
            chain_label=default.chain_label,
            provision_fn=provisioner,
        )

    @staticmethod
    def _utc_now() -> datetime:
        return datetime.utcnow()

    @staticmethod
    def _normalize_provider_user_id(provider_user_id: str) -> str:
        return str(provider_user_id)

    @staticmethod
    def _normalize_token(token: str) -> str:
        return str(token or "").strip().upper()

    def _sanitize_user(
        self, user: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        return self._records.sanitize_user(user)

    def _identity_selector(
        self, provider: str, provider_user_id: str
    ) -> Dict[str, Any]:
        return self._records.identity_selector(provider, provider_user_id)

    def _find_identity(
        self, user: Optional[Dict[str, Any]], provider: str, provider_user_id: str
    ) -> Optional[Dict[str, Any]]:
        return self._records.find_identity(user, provider, provider_user_id)

    def _build_identity(
        self,
        *,
        provider: str,
        provider_user_id: str,
        username: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        is_primary: bool = False,
    ) -> Dict[str, Any]:
        return self._records.build_identity(
            provider=provider,
            provider_user_id=provider_user_id,
            username=username,
            metadata=metadata,
            is_primary=is_primary,
        )

    def _build_link_token_doc(
        self,
        *,
        volo_user_id: str,
        token: str,
        created_at: datetime,
        expires_at: datetime,
    ) -> Dict[str, Any]:
        return self._link_tokens.build_link_token_doc(
            volo_user_id=volo_user_id,
            token=token,
            created_at=created_at,
            expires_at=expires_at,
        )

    @staticmethod
    def _token_is_used(token_doc: Dict[str, Any]) -> bool:
        return LinkTokenManager.token_is_used(token_doc)

    @staticmethod
    def _token_is_revoked(token_doc: Dict[str, Any]) -> bool:
        return LinkTokenManager.token_is_revoked(token_doc)

    def _provision_wallet_bundle_sync(
        self, volo_user_id: str
    ) -> tuple[ProvisioningPayload, ProvisioningPayload]:
        return (
            self._evm_provisioner.provision(volo_user_id),
            self._solana_provisioner.provision(volo_user_id),
        )

    @staticmethod
    def _validate_wallet_payload(
        wallet_data: Any, *, chain_label: str, volo_user_id: str
    ) -> ProvisioningPayload:
        if not isinstance(wallet_data, dict):
            payload_type = type(wallet_data).__name__
            raise RuntimeError(
                f"CDP {chain_label} provisioning returned {payload_type} for {volo_user_id}. "
                "Expected a dict with keys 'sub_org_id' and 'address'. "
                "Update the provisioner output format and retry."
            )
        return wallet_data

    @staticmethod
    def _require_wallet_fields(
        wallet_data: Any, *, chain_label: str, volo_user_id: str
    ) -> tuple[str, str]:
        wallet_data = AsyncIdentityService._validate_wallet_payload(
            wallet_data, chain_label=chain_label, volo_user_id=volo_user_id
        )
        sub_org_id = wallet_data.get("sub_org_id")
        address = wallet_data.get("address")
        if not sub_org_id or not address:
            raise RuntimeError(
                f"CDP {chain_label} provisioning returned incomplete data for {volo_user_id}. "
                "Missing required keys 'sub_org_id' and/or 'address'. "
                "Update the provisioner output and retry."
            )
        return sub_org_id, address

    async def get_user_by_identity(
        self, provider: str, provider_user_id: str
    ) -> Optional[Dict[str, Any]]:
        user = await self._repo.get_user_by_identity(
            provider, self._normalize_provider_user_id(provider_user_id)
        )
        return self._sanitize_user(user)

    async def get_user_by_volo_id(self, volo_user_id: str) -> Optional[Dict[str, Any]]:
        user = await self._repo.get_user_by_volo_id(volo_user_id)
        return self._sanitize_user(user)

    async def register_user(
        self,
        provider: str,
        provider_user_id: str,
        username: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        volo_user_id = str(uuid.uuid4())
        provider_user_id = self._normalize_provider_user_id(provider_user_id)
        wallet_data, solana_wallet = await run_blocking(
            self._provision_wallet_bundle_sync, volo_user_id
        )
        evm_sub_org_id, evm_address = self._require_wallet_fields(
            wallet_data,
            chain_label="EVM",
            volo_user_id=volo_user_id,
        )
        solana_sub_org_id, solana_address = self._require_wallet_fields(
            solana_wallet,
            chain_label="Solana",
            volo_user_id=volo_user_id,
        )

        new_user = {
            "volo_user_id": volo_user_id,
            "identities": [
                self._build_identity(
                    provider=provider,
                    provider_user_id=provider_user_id,
                    username=username,
                    metadata=metadata,
                    is_primary=True,
                )
            ],
            "sub_org_id": evm_sub_org_id,
            "sender_address": evm_address,
            "evm_sub_org_id": evm_sub_org_id,
            "evm_address": evm_address,
            "solana_sub_org_id": solana_sub_org_id,
            "solana_address": solana_address,
            "created_at": self._utc_now(),
            "is_active": True,
            "metadata": metadata or {},
        }
        await self._repo.insert_user(dict(new_user))
        return self._sanitize_user(new_user) or {}

    async def reprovision_wallets(
        self,
        volo_user_id: str,
        provider: str,
        provider_user_id: str,
        username: Optional[str] = None,
    ) -> Dict[str, Any]:
        existing = await self.get_user_by_identity(provider, provider_user_id)
        if existing:
            await self.sync_username(existing, provider, provider_user_id, username)
            updated = await self.ensure_multi_chain_wallets(existing)
            updated["is_new_user"] = False
            return self._sanitize_user(updated) or {}

        new_user = await self.register_user(
            provider, provider_user_id, username=username
        )
        new_user["is_new_user"] = True
        return new_user

    async def generate_link_token(
        self, volo_user_id: str, *, ttl_seconds: int = LINK_TOKEN_TTL_SECONDS
    ) -> str:
        return await self._link_tokens.issue(volo_user_id, ttl_seconds=ttl_seconds)

    async def _claim_link_token(
        self,
        token: str,
        *,
        provider: str,
        provider_user_id: str,
        now: datetime,
    ) -> Dict[str, Any]:
        return await self._link_tokens.claim(
            token,
            provider=provider,
            provider_user_id=provider_user_id,
            now=now,
        )

    async def link_identity_by_token(
        self,
        token: str,
        provider: str,
        provider_user_id: str,
        username: Optional[str] = None,
    ) -> Dict[str, Any]:
        provider_user_id = self._normalize_provider_user_id(provider_user_id)
        normalized_token = self._normalize_token(token)
        now = self._utc_now()
        try:
            link_data = await self._claim_link_token(
                normalized_token,
                provider=provider,
                provider_user_id=provider_user_id,
                now=now,
            )
        except LinkAccountError:
            raise

        volo_user_id = str(link_data["volo_user_id"])
        existing = await self.get_user_by_identity(provider, provider_user_id)
        if existing:
            if str(existing["volo_user_id"]) == volo_user_id:
                await self.sync_username(existing, provider, provider_user_id, username)
                latest = await self.get_user_by_volo_id(volo_user_id) or existing
                return self._sanitize_user(latest) or {}
            raise IdentityConflictError(provider, provider_user_id)

        if not await self.get_user_by_volo_id(volo_user_id):
            raise LinkTargetMissingError()

        new_identity = self._build_identity(
            provider=provider,
            provider_user_id=provider_user_id,
            username=username,
            is_primary=False,
        )
        try:
            result = await self._repo.update_user(
                {
                    "volo_user_id": volo_user_id,
                    "identities": {
                        "$not": {
                            "$elemMatch": self._identity_selector(
                                provider, provider_user_id
                            )
                        }
                    },
                },
                {"$push": {"identities": new_identity}},
            )
        except DuplicateKeyError:
            owner = await self.get_user_by_identity(provider, provider_user_id)
            if owner and str(owner.get("volo_user_id")) == volo_user_id:
                return self._sanitize_user(owner) or {}
            raise IdentityConflictError(provider, provider_user_id)

        if result.modified_count == 0:
            user = await self.get_user_by_volo_id(volo_user_id)
            if not user:
                raise LinkTargetMissingError()
            if self._find_identity(user, provider, provider_user_id):
                return self._sanitize_user(user) or {}
            raise LinkAttachError()

        user = await self.get_user_by_volo_id(volo_user_id)
        if not user:
            raise LinkTargetMissingError()
        return self._sanitize_user(user) or {}

    async def sync_username(
        self,
        user: Dict[str, Any],
        provider: str,
        provider_user_id: str,
        new_username: Optional[str],
    ) -> None:
        if not new_username:
            return
        identity = self._find_identity(user, provider, provider_user_id)
        if not identity:
            return
        current_username = identity.get("username")
        if current_username == new_username:
            return
        history_entry = (
            {"username": current_username, "changed_at": self._utc_now()}
            if current_username
            else None
        )
        update_ops: Dict[str, Any] = {
            "$set": {
                "identities.$.username": new_username,
                "identities.$.last_seen_at": self._utc_now(),
            }
        }
        if history_entry:
            update_ops["$push"] = {"identities.$.username_history": history_entry}
        await self._repo.update_user(
            {
                "volo_user_id": user["volo_user_id"],
                "identities.provider": provider,
                "identities.provider_user_id": self._normalize_provider_user_id(
                    provider_user_id
                ),
            },
            update_ops,
        )

    async def ensure_multi_chain_wallets(
        self,
        user: Dict[str, Any],
        *,
        force_solana_retry: bool = False,
    ) -> Dict[str, Any]:
        updates: Dict[str, Any] = {}
        evm_sub_org_id = user.get("evm_sub_org_id") or user.get("sub_org_id")
        evm_address = user.get("evm_address") or user.get("sender_address")
        if evm_sub_org_id and user.get("evm_sub_org_id") != evm_sub_org_id:
            updates["evm_sub_org_id"] = evm_sub_org_id
        if evm_address and user.get("evm_address") != evm_address:
            updates["evm_address"] = evm_address

        if not user.get("solana_sub_org_id") or not user.get("solana_address"):
            volo_user_id = user.get("volo_user_id")
            if volo_user_id:
                metadata = user.get("metadata")
                if not isinstance(metadata, dict):
                    metadata = {}
                now = self._utc_now()
                last_failed_raw = metadata.get("solana_provision_last_failed_at")
                can_retry = True
                if isinstance(last_failed_raw, str) and last_failed_raw.strip():
                    try:
                        last_failed = datetime.fromisoformat(last_failed_raw)
                    except Exception:
                        last_failed = None
                    if last_failed is not None:
                        elapsed = (now - last_failed).total_seconds()
                        if elapsed < _SOLANA_PROVISION_RETRY_SECONDS:
                            can_retry = False
                if force_solana_retry:
                    can_retry = True
                try:
                    if can_retry:
                        solana_wallet = await run_blocking(
                            self._solana_provisioner.provision,
                            str(volo_user_id),
                            timeout=_SOLANA_PROVISION_TIMEOUT_SECONDS,
                        )
                        solana_wallet = self._validate_wallet_payload(
                            solana_wallet,
                            chain_label="Solana",
                            volo_user_id=str(volo_user_id),
                        )
                        solana_sub_org_id = solana_wallet.get("sub_org_id")
                        solana_address = solana_wallet.get("address")
                        if solana_sub_org_id and solana_address:
                            updates["solana_sub_org_id"] = solana_sub_org_id
                            updates["solana_address"] = solana_address
                            metadata.pop("solana_provision_last_failed_at", None)
                            metadata.pop("solana_provision_last_error", None)
                            updates["metadata"] = metadata
                except Exception as exc:
                    detail = (
                        f"{exc.__class__.__name__}: {exc}"
                        if str(exc)
                        else exc.__class__.__name__
                    )
                    logger.warning(
                        "Failed to provision Solana wallet for %s: %s",
                        volo_user_id,
                        detail,
                    )
                    metadata["solana_provision_last_failed_at"] = now.isoformat()
                    metadata["solana_provision_last_error"] = detail[:240]
                    updates["metadata"] = metadata
        if updates:
            await self._repo.update_user(
                {"volo_user_id": user.get("volo_user_id")},
                {"$set": updates},
            )
            user = dict(user)
            user.update(updates)
        return user

    def _select_new_primary_identity(
        self,
        identities: list[Dict[str, Any]],
        *,
        provider: str,
        provider_user_id: str,
    ) -> Optional[Dict[str, Any]]:
        return self._records.select_new_primary_identity(
            identities,
            provider=provider,
            provider_user_id=provider_user_id,
        )

    async def unlink_identity(
        self,
        provider: str,
        provider_user_id: str,
        *,
        allow_primary_unlink: bool = False,
    ) -> Dict[str, Any]:
        provider_user_id = self._normalize_provider_user_id(provider_user_id)
        user = await self.get_user_by_identity(provider, provider_user_id)
        if not user:
            raise IdentityNotFoundError()
        identities = user.get("identities", [])
        if len(identities) <= 1:
            raise LastIdentityError()
        target = self._find_identity(user, provider, provider_user_id)
        if not target:
            raise IdentityNotFoundError()

        update_ops: Dict[str, Any] = {
            "$pull": {"identities": self._identity_selector(provider, provider_user_id)}
        }
        update_kwargs: Dict[str, Any] = {}
        if target.get("is_primary"):
            replacement = self._select_new_primary_identity(
                identities,
                provider=provider,
                provider_user_id=provider_user_id,
            )
            if replacement:
                update_ops["$set"] = {"identities.$[replacement].is_primary": True}
                update_kwargs["array_filters"] = [
                    {
                        "replacement.provider": replacement.get("provider"),
                        "replacement.provider_user_id": replacement.get(
                            "provider_user_id"
                        ),
                    }
                ]
            elif not allow_primary_unlink:
                raise LastIdentityError()

        result = await self._repo.update_user(
            {"volo_user_id": user["volo_user_id"]},
            update_ops,
            **update_kwargs,
        )
        if result.modified_count == 0:
            raise IdentityNotFoundError()
        updated = await self.get_user_by_volo_id(user["volo_user_id"])
        if not updated:
            raise RuntimeError(
                "User record was not found after unlink update. "
                "Recovery path: retry once; if this repeats, verify database consistency for the affected volo_user_id."
            )
        return updated
