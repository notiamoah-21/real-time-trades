from pyspark.sql.functions import col
from storage.mongo_writer import write_to_mongo

def store_data_dynamically(batch_df, mongodb_uri):
    """
    Dynamically routes data to MongoDB databases and collections based on assetClass 
    and tradeCaptureSystem. Illegal trades are also written to an alerts database.

    Args:
        batch_df (DataFrame): The DataFrame to process.
        mongodb_uri (str): MongoDB connection URI.
    """
    # Separate valid and illegal trades
    illegal_trades_df = batch_df.filter(col("is_illegal") == True)
    valid_trades_df = batch_df.filter(col("is_illegal") == False)

    # Write illegal trades to the alerts database
    if not illegal_trades_df.isEmpty():
        write_to_mongo(
            df=illegal_trades_df,
            mongodb_uri=mongodb_uri,
            database="alerts",
            collection="illegal_trades"
        )

    # Process valid trades for dynamic routing
    asset_classes = valid_trades_df.select("assetClass").distinct().collect()

    for asset_class_row in asset_classes:
        # Prepare database name
        asset_class = asset_class_row['assetClass'].replace(" ", "_").lower()  # e.g., "equities"
        
        # Filter for this asset class
        asset_class_df = valid_trades_df.filter(col("assetClass") == asset_class_row['assetClass'])
        
        # Get distinct trade capture systems (collections)
        trade_capture_systems = asset_class_df.select("tradeCaptureSystem").distinct().collect()
        
        for system_row in trade_capture_systems:
            # Prepare collection name
            trade_capture_system = system_row['tradeCaptureSystem'].replace(" ", "_").lower()  # e.g., "system_a"
            
            # Filter for this trade capture system
            system_df = asset_class_df.filter(col("tradeCaptureSystem") == system_row['tradeCaptureSystem'])
            
            # Dynamically write to MongoDB
            write_to_mongo(
                df=system_df,
                mongodb_uri=mongodb_uri,
                database=asset_class,
                collection=trade_capture_system
            )
