import asyncio
from mongo_manager import mongo_manager

async def create_indexes():
    """Create MongoDB indexes for better query performance"""
    collection = mongo_manager.db.endpoint_responses
    
    # Create indexes
    await collection.create_index("user_email")
    await collection.create_index("endpoint")
    await collection.create_index("timestamp")
    await collection.create_index("customer_id")
    await collection.create_index("property_id")
    await collection.create_index([("user_email", 1), ("endpoint", 1), ("timestamp", -1)])
    
    print("MongoDB indexes created successfully")

if __name__ == "__main__":
    asyncio.run(create_indexes())