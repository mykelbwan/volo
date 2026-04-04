import pytest

from core.identity.wallet_bindings import (
    UnsupportedWalletFamilyError,
    wallet_markers_for_family,
)


def test_wallet_markers_for_supported_families():
    evm = wallet_markers_for_family("evm")
    solana = wallet_markers_for_family("solana")

    assert evm.sender_marker == "{{EVM_ADDRESS}}"
    assert evm.sub_org_marker == "{{EVM_SUB_ORG_ID}}"
    assert solana.sender_marker == "{{SOLANA_ADDRESS}}"
    assert solana.sub_org_marker == "{{SOLANA_SUB_ORG_ID}}"


def test_wallet_markers_for_family_rejects_unsupported_family():
    with pytest.raises(
        UnsupportedWalletFamilyError,
        match="Unsupported wallet binding family: 'cosmos'",
    ):
        wallet_markers_for_family("cosmos")
