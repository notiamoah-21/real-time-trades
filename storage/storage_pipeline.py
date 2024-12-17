from pyspark.sql.functions import col
from storage.mongo_writer import write_to_mongo

def store_data_dynamically(batch_df, mongodb_uri):
    """
    Dynamically routes data to MongoDB databases and collections
    based on assetClass (database) and tradeCaptureSystem (collection).

    Args:
        batch_df (DataFrame): The DataFrame to process.
        mongodb_uri (str): MongoDB connection URI.
    """
    # Get distinct asset classes (databases)
    asset_classes = batch_df.select("assetClass").distinct().collect()
    
    for asset_class_row in asset_classes:
        # Prepare database name
        asset_class = asset_class_row['assetClass'].replace(" ", "_").lower()  # e.g., "equities"
        
        # Filter DataFrame for this asset class
        asset_class_df = batch_df.filter(col("assetClass") == asset_class_row['assetClass'])
        
        # Get distinct trade capture systems (collections)
        trade_capture_systems = asset_class_df.select("tradeCaptureSystem").distinct().collect()
        
        for system_row in trade_capture_systems:
            # Prepare collection name
            trade_capture_system = system_row['tradeCaptureSystem'].replace(" ", "_").lower()  # e.g., "system_a"
            
            # Filter DataFrame for this trade capture system
            system_df = asset_class_df.filter(col("tradeCaptureSystem") == system_row['tradeCaptureSystem'])
            
            # Write to MongoDB dynamically
            write_to_mongo(system_df, mongodb_uri, asset_class, trade_capture_system)
