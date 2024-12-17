rules_config = {
    "equities": {
        "price": {"min": 1},
        "quantity": {"min": 1},
        "restricted_symbols": ["XYZ_CORP", "ILLEGAL_EQUITY"],
        "allowed_exchanges": ["NYSE", "NASDAQ", "LSE"]
    },
    "fixed_income": {
        "price": {"min": 10},
        "quantity": {"min": 1000},
        "restricted_symbols": ["ABC_BOND", "ILLEGAL_BOND"],
        "allowed_exchanges": ["TREASURY_MARKET", "CORP_BONDS"]
    },
    "commodities": {
        "price": {"min": 0.1},
        "quantity": {"min": 10},
        "restricted_symbols": ["ILLEGAL_OIL", "ILLEGAL_GOLD"],
        "allowed_exchanges": ["COMEX", "NYMEX", "ICE"]
    },
    "real_estate": {
        "price": {"min": 1000},  # Minimum property price
        "quantity": {"min": 1},  # Minimum quantity (e.g., number of properties)
        "restricted_symbols": ["ILLEGAL_PROPERTY", "FAKE_LAND"],  # Blacklisted property IDs
        "allowed_exchanges": ["ZILLOW", "REDFIN", "REALTY_MLS"]  # Approved platforms
    }
}

def get_rules_for_asset_class(asset_class):
    """
    Fetches validation rules for a specific asset class.

    Args:
        asset_class (str): Name of the asset class.

    Returns:
        dict: Validation rules for the asset class.
    """
    return rules_config.get(asset_class, {})
