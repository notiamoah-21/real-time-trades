rules_config = {
    "equities": {
        "price": {"min": 1},  # Minimum price for equities
        "quantity": {"min": 1},  # Minimum quantity for equities
        "restricted_symbols": ["XYZ_CORP", "ILLEGAL_EQUITY"],  # Blacklisted symbols
        "allowed_exchanges": ["NYSE", "NASDAQ", "LSE"],  # Allowed exchanges
    },
    "fixed_income": {
        "price": {"min": 10},  # Minimum price for fixed-income instruments
        "quantity": {"min": 1000},  # Minimum quantity for bonds
        "restricted_symbols": ["ABC_BOND", "ILLEGAL_BOND"],  # Blacklisted symbols
        "allowed_exchanges": ["TREASURY_MARKET", "CORP_BONDS"],  # Allowed exchanges
    },
    "commodities": {
        "price": {"min": 0.1},  # Minimum price for commodities
        "quantity": {"min": 10},  # Minimum quantity for commodities
        "restricted_symbols": ["ILLEGAL_OIL", "ILLEGAL_GOLD"],  # Blacklisted symbols
        "allowed_exchanges": ["COMEX", "NYMEX", "ICE"],  # Allowed exchanges
    },
    "crypto": {
        "price": {"min": 0.0001},  # Minimum price for crypto trades
        "quantity": {"min": 0.001},  # Minimum quantity for crypto trades
        "restricted_symbols": ["FAKECOIN", "SCAMTOKEN"],  # Blacklisted symbols
        "allowed_exchanges": ["COINBASE", "BINANCE", "KRAKEN"],  # Allowed exchanges
    },
}

def get_rules_for_asset_class(asset_class):
    """
    Fetch rules for the given asset class from the rules configuration.

    Args:
        asset_class (str): Name of the asset class.

    Returns:
        dict: Validation rules for the specified asset class.
    """
    return rules_config.get(asset_class, {})
