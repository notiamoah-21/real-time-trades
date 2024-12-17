from pyspark.sql.functions import col
from validation.rules_config import get_rules_for_asset_class

def validate_equities(df):
    """
    Applies validation rules specific to equities.

    Args:
        df (DataFrame): Parsed DataFrame containing trade data.

    Returns:
        Column: Spark SQL Column representing validation logic for equities.
    """
    rules = get_rules_for_asset_class("equities")
    return (
        (col("price") < rules["price"]["min"]) | 
        (col("quantity") < rules["quantity"]["min"]) | 
        (col("symbol").isin(rules["restricted_symbols"])) | 
        (~col("exchange").isin(rules["allowed_exchanges"]))
    )
