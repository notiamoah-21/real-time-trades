# Write the stream data to Cosmos DB with dynamic database and collection
def write_to_cosmos(batch_df, batch_id):
    # Collect all rows in the batch
    for row in batch_df.collect():
        database, collection = get_cosmos_database_and_collection(row)
        
        cosmos_config = {
            "spark.cosmos.accountEndpoint": COSMOS_DB_URI,
            "spark.cosmos.accountKey": COSMOS_DB_KEY,
            "spark.cosmos.database": database,
            "spark.cosmos.container": collection,
            "spark.cosmos.write.strategy": "ItemOverwrite",
            "spark.cosmos.write.bulk.enabled": "true",
            "spark.cosmos.write.maxBatchSize": "1000"  # Set to desired batch size
        }
        
        # Write to Cosmos DB with the dynamically chosen database and collection
        batch_df.write \
            .format("cosmos.oltp") \
            .options(**cosmos_config) \
            .option("checkpointLocation", "/mnt/checkpoints/eventhub-to-cosmos") \
            .mode("append") \
            .save()

# Write the parsed data to Cosmos DB
parsed_stream.writeStream \
    .foreachBatch(write_to_cosmos) \
    .start()

# Await termination
spark.streams.awaitAnyTermination()
