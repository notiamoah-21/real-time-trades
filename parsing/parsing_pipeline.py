from pyspark.sql.functions import from_json, col
from parsing.trade_schema import trade_schema

def parse_raw_data(raw_stream):
    """
    Parses raw binary data from Event Hubs into a structured DataFrame.

    Args:
        raw_stream (DataFrame): DataFrame with raw data from Event Hubs.

    Returns:
        DataFrame: DataFrame with a structured schema.
    """
    # Extract the 'body' column and cast it to STRING
    parsed_stream = raw_stream.selectExpr("CAST(body AS STRING) AS body")

    # Parse the JSON string in 'body' to a structured schema
    structured_stream = parsed_stream.select(
        from_json(col("body"), trade_schema).alias("trade")
    ).select("trade.*")  # Flatten the nested structure

    return structured_stream
