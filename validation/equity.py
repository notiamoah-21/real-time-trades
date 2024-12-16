from rules_config import get_rules_for_asset_class

def validate_equities(df):
    rules = get_rules_for_asset_class("equities")
    return df.withColumn(
        "is_illegal",
        (col("price") < rules["price"]["min"]) |
        (col("quantity") < rules["quantity"]["min"]) |
        (col("symbol").isin(rules["restricted_symbols"])) |
        (~col("exchange").isin(rules["allowed_exchanges"]))
    )
