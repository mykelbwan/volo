from tool_nodes.wallet.balance import _render_all_chain_message


def test_render_all_chain_message_skips_zero_balance_chains():
    message = _render_all_chain_message(
        [
            {
                "status": "success",
                "chain_display": "Ethereum",
                "balances": [],
                "is_testnet": False,
                "total_usd": 0.0,
            },
            {
                "status": "success",
                "chain_display": "Base Sepolia",
                "balances": [
                    {
                        "symbol": "ETH",
                        "balance_formatted": "0.01",
                        "name": "Base Sepolia Native",
                    }
                ],
                "is_testnet": True,
            },
        ]
    )

    assert "Ethereum:" not in message
    assert "Base Sepolia:" in message
    assert "0.01 ETH" in message


def test_render_all_chain_message_reports_no_assets_when_all_zero():
    message = _render_all_chain_message(
        [
            {
                "status": "success",
                "chain_display": "Ethereum",
                "balances": [],
                "is_testnet": False,
                "total_usd": 0.0,
            },
            {
                "status": "success",
                "chain_display": "Solana",
                "balances": [],
                "is_testnet": False,
                "total_usd": 0.0,
            },
        ]
    )

    assert message == "No assets found across supported chains."
