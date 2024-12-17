from pyspark.sql import SparkSession
from ingestion.event_hub_stream import read_stream_from_event_hub
from parsing.parsing_pipeline import parse_raw_data
from validation.validation_pipeline import apply_validation_rules
from storage.storage_pipeline import store_data_dynamically

# MongoDB connection URI base
mongodb_uri_base = ""

# Event Hubs Connection String
connection_string = ""

def main():
    """
    Main pipeline for streaming trade data from Event Hubs to MongoDB with validation.
    """
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Validated_Trades_To_MongoDB") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
        .getOrCreate()

    # Step 1: Read Data from Event Hubs
    raw_stream = read_stream_from_event_hub(spark, connection_string)

    # Step 2: Parse JSON Body into Structured Schema
    parsed_stream = parse_raw_data(raw_stream)

    # Step 3: Apply Validation Rules
    validated_stream = apply_validation_rules(parsed_stream)

    # Step 4: Dynamically Store Data
    query = validated_stream.writeStream \
        .foreachBatch(lambda batch_df, batch_id: store_data_dynamically(batch_df, mongodb_uri_base)) \
        .option("checkpointLocation", "/mnt/checkpoints/validated_trade_pipeline/") \
        .start()

    # Step 5: Await Termination
    query.awaitTermination()

if __name__ == "__main__":
    main()
