from pyspark.sql.functions import col
from equities_rules import validate_equities
from fixed_income_rules import validate_fixed_income
from commodities_rules import validate_commodities
from crypto_rules import validate_crypto

def apply_validation_rules(df):
    """
    Dynamically applies validation rules for each asset class.

    Args:
        df (DataFrame): Parsed DataFrame containing trade data.

    Returns:
        DataFrame: DataFrame with a new column 'is_illegal' marking illegal trades.
    """
    # Add a new column 'is_illegal' by applying asset class-specific rules
    validated_df = df.withColumn(
        "is_illegal",
        when(
            col("assetClass") == "equities", validate_equities(df)
        ).when(
            col("assetClass") == "fixed_income", validate_fixed_income(df)
        ).when(
            col("assetClass") == "commodities", validate_commodities(df)
        ).when(
            col("assetClass") == "crypto", validate_crypto(df)
        ).otherwise(False)  # Default: Valid if no rules apply
    )
    
    return validated_df
