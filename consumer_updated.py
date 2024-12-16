from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# MongoDB connection URI base
mongodb_uri_base = ""

# Event Hubs Configuration
connectionString = ""
ehConf = {}
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Dynamic_Trades_To_MongoDB") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
    .getOrCreate()

# Define Schema
trade_schema = StructType([
    StructField("tradeId", StringType(), True),
    StructField("assetClass", StringType(), True),
    StructField("tradeCaptureSystem", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("tradeType", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("traderId", StringType(), True),
    StructField("exchange", StringType(), True)
])

# Read Data from Event Hubs
data_stream = spark.readStream \
    .format("eventhubs") \
    .options(**ehConf) \
    .load()

# Parse JSON Body
parsed_stream = data_stream.selectExpr("CAST(body AS STRING) AS body") \
    .select(from_json(col("body"), trade_schema).alias("trade")) \
    .select("trade.*")

# Function to Dynamically Write to MongoDB
def write_to_mongo(batch_df, batch_id):
    """
    Write data dynamically to MongoDB databases and collections
    based on assetClass (database) and tradeCaptureSystem (collection).
    """
    # Get distinct asset classes (databases)
    asset_classes = batch_df.select("assetClass").distinct().collect()
    
    for asset_class_row in asset_classes:
        asset_class = asset_class_row['assetClass'].replace(" ", "_").lower()  # e.g., "equities"
        
        # Filter for this asset class
        asset_class_df = batch_df.filter(col("assetClass") == asset_class_row['assetClass'])
        
        # Get distinct trade capture systems (collections)
        trade_capture_systems = asset_class_df.select("tradeCaptureSystem").distinct().collect()
        
        for system_row in trade_capture_systems:
            trade_capture_system = system_row['tradeCaptureSystem'].replace(" ", "_").lower()  # e.g., "system_a"
            
            # Filter data for this trade capture system
            system_df = asset_class_df.filter(col("tradeCaptureSystem") == system_row['tradeCaptureSystem'])
            
            # Dynamically write to MongoDB
            system_df.write \
                .format("mongodb") \
                .mode("append") \
                .option("spark.mongodb.connection.uri", mongodb_uri_base) \
                .option("spark.mongodb.database", asset_class) \
                .option("spark.mongodb.collection", trade_capture_system) \
                .save()

# Write Stream with foreachBatch
query = parsed_stream.writeStream \
    .foreachBatch(write_to_mongo) \
    .option("checkpointLocation", "") \
    .start()

query.awaitTermination()

