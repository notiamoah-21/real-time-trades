from pyspark.sql import SparkSession

def read_stream_from_event_hub(spark, connection_string):
    """
    Reads a stream from Azure Event Hubs and returns a Spark DataFrame.

    Args:
        spark (SparkSession): Spark session object.
        connection_string (str): Azure Event Hubs connection string.

    Returns:
        DataFrame: Spark DataFrame representing the stream.
    """
    # Configure Event Hubs connection
    eh_conf = {
        "eventhubs.connectionString": spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string)
    }

    # Read stream from Event Hubs
    event_hub_stream = spark.readStream \
        .format("eventhubs") \
        .options(**eh_conf) \
        .load()

    return event_hub_stream
