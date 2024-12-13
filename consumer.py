# Write the stream data to Cosmos DB with dynamic database and collection using pymongo
def write_to_cosmos(batch_df, batch_id):
    # Collect all rows in the batch
    for row in batch_df.collect():
        database, collection = get_cosmos_database_and_collection(row)
        
        # Connect to the appropriate Cosmos DB database and collection
        db = mongo_client[database]
        collection = db[collection]

        # Convert the row to a dictionary (MongoDB document)
        trade_data = {
            "tradeId": row['tradeId'],
            "assetClass": row['assetClass'],
            "tradeCaptureSystem": row['tradeCaptureSystem'],
            "symbol": row['symbol'],
            "price": row['price'],
            "quantity": row['quantity'],
            "tradeType": row['tradeType'],
            "timestamp": row['timestamp'],
            "traderId": row['traderId'],
            "exchange": row['exchange']
        }

        # Insert the trade data into Cosmos DB
        collection.insert_one(trade_data)
