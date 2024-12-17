from pyspark.sql.functions import col
from validation.validation_rules.rules_config import get_rules_for_asset_class

def validate_fixed_income(df):
    """
    Applies validation rules specific to fixed income.

    Args:
        df (DataFrame): Parsed DataFrame containing trade data.

    Returns:
        Column: A boolean column indicating whether the trade is illegal.
    """
    rules = get_rules_for_asset_class("fixed_income")
    return (
        (col("price") < rules["price"]["min"]) | 
        (col("quantity") < rules["quantity"]["min"]) | 
        (col("symbol").isin(rules["restricted_symbols"])) | 
        (~col("exchange").isin(rules["allowed_exchanges"]))
    )
