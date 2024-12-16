from pyspark.sql.functions import col

# Configuration for equities rules
rules_config = {
    "price": {"min": 1},  # Minimum price for equities
    "quantity": {"min": 1},  # Minimum quantity for equities
    "restricted_symbols": ["XYZ_CORP", "ILLEGAL_EQUITY"],  # Blacklisted symbols
    "allowed_exchanges": ["NYSE", "NASDAQ", "LSE"]  # Allowed exchanges
}

def validate_equities(df):
    """
    Validates trades for the equities asset class based on predefined rules.
    
    Args:
        df (DataFrame): Parsed DataFrame containing trade data.
        
    Returns:
        DataFrame: DataFrame with a new column 'is_illegal' marking illegal trades.
    """
    # Apply validation rules for equities
    df = df.withColumn(
        "is_illegal",
        (col("price") < rules_config["price"]["min"]) |  # Price below minimum
        (col("quantity") < rules_config["quantity"]["min"]) |  # Quantity below minimum
        (col("symbol").isin(rules_config["restricted_symbols"])) |  # Blacklisted symbols
        (~col("exchange").isin(rules_config["allowed_exchanges"]))  # Unauthorized exchanges
    )
    return df
