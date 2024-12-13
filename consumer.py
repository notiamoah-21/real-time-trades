# Define a function to dynamically set the Cosmos DB collection based on the incoming data
def get_cosmos_database_and_collection(row):
    # Extract asset class and trade capture system for dynamic database/collection
    asset_class = row['assetClass']
    trade_capture_system = row['tradeCaptureSystem']
    
    # Use fixed Cosmos DB URI, dynamic database and collection
    database = asset_class  # Dynamic database
    collection = trade_capture_system  # Dynamic collection
    
    return database, collection
