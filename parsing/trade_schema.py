from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Define the schema for the trade data
trade_schema = StructType([
    StructField("tradeId", StringType(), True),           # Unique trade identifier
    StructField("assetClass", StringType(), True),        # Asset class (e.g., equities, fixed income)
    StructField("tradeCaptureSystem", StringType(), True),# Source system capturing the trade
    StructField("symbol", StringType(), True),            # Trade symbol (e.g., AAPL for Apple Inc.)
    StructField("price", FloatType(), True),              # Trade price
    StructField("quantity", IntegerType(), True),         # Quantity traded
    StructField("tradeType", StringType(), True),         # Type of trade (e.g., BUY/SELL)
    StructField("timestamp", StringType(), True),         # Trade timestamp
    StructField("traderId", StringType(), True),          # ID of the trader
    StructField("exchange", StringType(), True)           # Exchange (e.g., NYSE, NASDAQ)
])
