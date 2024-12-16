def write_to_mongo(df, mongodb_uri, database, collection):
    """
    Writes a DataFrame to MongoDB.

    Args:
        df (DataFrame): The Spark DataFrame to write.
        mongodb_uri (str): MongoDB connection URI.
        database (str): Target database name.
        collection (str): Target collection name.
    """
    df.write \
        .format("mongodb") \
        .mode("append") \
        .option("spark.mongodb.connection.uri", mongodb_uri) \
        .option("spark.mongodb.database", database) \
        .option("spark.mongodb.collection", collection) \
        .save()
