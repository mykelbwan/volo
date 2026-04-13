from typing import Any, Optional

from pydantic import BaseModel, Field, model_validator


class SwapArgs(BaseModel):
    token_in_symbol: str = Field(..., description="Symbol of the token to swap from.")
    token_out_symbol: str = Field(..., description="Symbol of the token to swap to.")
    token_in_address: Optional[str] = Field(
        None, description="Contract address of the input token."
    )
    token_out_address: Optional[str] = Field(
        None, description="Contract address of the output token."
    )
    amount_in: float = Field(..., description="Amount of the input token to swap.")
    amount_in_wei: Optional[str] = Field(
        None, description="Amount of the input token in wei."
    )
    chain: str = Field(..., description="Chain name where the swap occurs.")
    slippage: float = Field(
        0.5, description="Slippage tolerance percentage (default 0.5%)."
    )
    sub_org_id: str = Field(
        ..., description="CDP account name that owns the user's wallet."
    )
    sender: str = Field(
        ...,
        description="Ethereum address of the wallet that will sign and send the swap.",
    )


class BridgeArgs(BaseModel):
    token_symbol: str = Field(..., description="Symbol of the token to bridge.")
    source_chain: str = Field(..., description="Name of the source blockchain.")
    target_chain: str = Field(..., description="Name of the target blockchain.")
    source_address: Optional[str] = Field(
        None, description="Address on the source chain."
    )
    target_address: Optional[str] = Field(
        None, description="Address on the target chain."
    )
    amount: float = Field(..., description="Amount of token to bridge.")
    amount_in_wei: Optional[str] = Field(None, description="Amount in wei to bridge.")
    chain: Optional[str] = Field(None, description="Optional current chain context.")


class TransferArgs(BaseModel):
    asset_symbol: str = Field(
        ..., description="Canonical symbol of the asset to transfer."
    )
    asset_ref: Optional[str] = Field(
        None,
        description=(
            "Canonical asset reference for the transfer. Use the network's native "
            "asset reference or omit for native transfers."
        ),
    )
    amount: float = Field(..., description="Amount of token to transfer.")
    recipient: str = Field(
        ..., description="Recipient address on the selected network."
    )
    network: str = Field(..., description="Network name where the transfer occurs.")
    sub_org_id: str = Field(..., description="CDP account name.")
    sender: str = Field(..., description="Sender address for the selected network.")

    @model_validator(mode="before")
    @classmethod
    def _normalize_legacy_aliases(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        normalized = dict(data)

        asset_symbol = normalized.get("asset_symbol")
        legacy_symbol = normalized.get("token_symbol")
        if asset_symbol is not None and legacy_symbol is not None:
            if str(asset_symbol).strip().upper() != str(legacy_symbol).strip().upper():
                raise ValueError(
                    "Conflicting transfer asset symbol inputs: 'token_symbol' and "
                    "'asset_symbol' do not match"
                )
        elif asset_symbol is None and legacy_symbol is not None:
            normalized["asset_symbol"] = legacy_symbol

        asset_ref = normalized.get("asset_ref")
        legacy_asset_ref = normalized.get("token_address")
        if asset_ref is not None and legacy_asset_ref is not None:
            if str(asset_ref).strip().lower() != str(legacy_asset_ref).strip().lower():
                raise ValueError(
                    "Conflicting transfer asset inputs: 'token_address' and "
                    "'asset_ref' do not match"
                )
        elif asset_ref is None and legacy_asset_ref is not None:
            normalized["asset_ref"] = legacy_asset_ref

        network = normalized.get("network")
        legacy_network = normalized.get("chain")
        if network is not None and legacy_network is not None:
            if str(network).strip().lower() != str(legacy_network).strip().lower():
                raise ValueError(
                    "Conflicting transfer network inputs: 'chain' and 'network' "
                    "do not match"
                )
        elif network is None and legacy_network is not None:
            normalized["network"] = legacy_network

        return normalized


class UnwrapArgs(BaseModel):
    token_symbol: str = Field(
        ...,
        description=(
            "Native token symbol to unwrap to (for example 'ETH' on Base/Sepolia)."
        ),
    )
    token_address: str = Field(
        ...,
        description="Wrapped native token contract address to withdraw from.",
    )
    amount: Optional[float] = Field(
        None,
        description=(
            "Optional amount of wrapped token to unwrap. "
            "If omitted, unwrap the full wrapped balance."
        ),
    )
    chain: str = Field(..., description="Network where the unwrap occurs.")
    sub_org_id: str = Field(..., description="CDP account name.")
    sender: str = Field(..., description="Sender address on the selected network.")


class BalanceArgs(BaseModel):
    chain: str = Field(
        ...,
        description=(
            "Chain name to check balances on. Use 'all_supported' to fetch across all "
            "configured chains."
        ),
    )
    sender: str = Field(
        ..., description="Primary wallet address for EVM balance checks."
    )
    solana_sender: Optional[str] = Field(
        None,
        description=(
            "Optional Solana wallet address. Used for Solana balance checks and "
            "all-chain fanout."
        ),
    )
    scope: Optional[str] = Field(
        None,
        description="Optional scope marker (e.g. 'all_supported').",
    )


class SolanaSwapArgs(BaseModel):
    token_in_symbol: str = Field(
        ..., description="Symbol of the token to swap from (e.g. 'SOL')."
    )
    token_out_symbol: str = Field(
        ..., description="Symbol of the token to swap to (e.g. 'USDC')."
    )
    token_in_mint: str = Field(
        ...,
        description=(
            "Solana mint address of the input token. "
            "Use 'So11111111111111111111111111111111111111112' for native SOL."
        ),
    )
    token_out_mint: str = Field(
        ..., description="Solana mint address of the output token."
    )
    amount_in: float = Field(
        ..., description="Human-readable amount to swap (e.g. 1.5 for 1.5 SOL)."
    )
    network: str = Field(
        "solana",
        description=(
            "Solana cluster to use. "
            "'solana' for mainnet-beta, 'solana-devnet' for devnet."
        ),
    )
    slippage: float = Field(
        0.5,
        description="Maximum acceptable slippage as a percentage (default 0.5%).",
    )
    sub_org_id: str = Field(
        ..., description="CDP account name that owns the user's Solana wallet."
    )
    sender: str = Field(
        ...,
        description="Base-58 Solana public key of the wallet that will sign the swap.",
    )
