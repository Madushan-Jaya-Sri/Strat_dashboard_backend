"""
Quick test script to check MongoDB chat collections
Run this to verify what's in the database
"""

import asyncio
import os
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(override=True)   

async def test_chat_collections():
    """Test what's in the chat collections"""

    # Connect to MongoDB
    connection_string = os.getenv('MONGODB_CONNECTION_STRING')
    if not connection_string:
        print("❌ MONGODB_CONNECTION_STRING not found")
        return

    client = AsyncIOMotorClient(connection_string)
    db = client.internal_dashboard

    # First, list ALL collections
    print(f"\n{'='*60}")
    print(f"ALL COLLECTIONS IN DATABASE")
    print(f"{'='*60}")
    all_collections = await db.list_collection_names()

    # Filter collections that might be chat-related
    chat_collections = [c for c in all_collections if 'chat' in c.lower()]

    print(f"\n📋 All collections ({len(all_collections)} total):")
    for collection_name in sorted(all_collections):
        count = await db[collection_name].count_documents({})
        marker = "🟢" if 'chat' in collection_name.lower() else "⚪"
        print(f"   {marker} {collection_name} ({count} documents)")

    if chat_collections:
        print(f"\n💬 Chat-related collections found: {chat_collections}")
    else:
        print(f"\n⚠️  No collections with 'chat' in the name found!")

    print(f"\n{'='*60}\n")

    # Test user email (update this to match your test user)
    test_user = "madushan.jayasri@momentro.com"

    collections_to_check = chat_collections if chat_collections else [
        'chat_google_ads',
        'chat_google_analytics',
        'chat_meta_ads'
    ]

    for collection_name in collections_to_check:
        print(f"\n{'='*60}")
        print(f"Checking collection: {collection_name}")
        print(f"{'='*60}")

        try:
            collection = db[collection_name]

            # Total documents
            total = await collection.count_documents({})
            print(f"📊 Total documents: {total}")

            # Documents for test user
            user_count = await collection.count_documents({"user_email": test_user})
            print(f"📊 Documents for {test_user}: {user_count}")

            # Active documents for test user
            active_count = await collection.count_documents({
                "user_email": test_user,
                "is_active": True
            })
            print(f"📊 Active documents for {test_user}: {active_count}")

            # Get one sample document
            sample = await collection.find_one({"user_email": test_user})
            if sample:
                print(f"\n📝 Sample document:")
                print(f"   session_id: {sample.get('session_id')}")
                print(f"   user_email: {sample.get('user_email')}")
                print(f"   module_type: {sample.get('module_type')}")
                print(f"   is_active: {sample.get('is_active')}")
                print(f"   created_at: {sample.get('created_at')}")
                print(f"   messages: {len(sample.get('messages', []))} messages")

                # Show first message
                messages = sample.get('messages', [])
                if messages:
                    first_msg = messages[0]
                    print(f"\n   First message:")
                    print(f"      role: {first_msg.get('role')}")
                    print(f"      content: {first_msg.get('content', '')[:50]}...")
                    print(f"      timestamp: {first_msg.get('timestamp')}")
            else:
                print(f"❌ No documents found for {test_user}")

            # Try query with different filters
            print(f"\n🔍 Testing different queries:")

            # Query 1: Just user email
            q1_count = await collection.count_documents({"user_email": test_user})
            print(f"   Query 1 (user_email only): {q1_count} documents")

            # Query 2: User email + is_active=True
            q2_count = await collection.count_documents({
                "user_email": test_user,
                "is_active": True
            })
            print(f"   Query 2 (user_email + is_active=True): {q2_count} documents")

            # Query 3: User email + is_active=true (lowercase)
            q3_count = await collection.count_documents({
                "user_email": test_user,
                "is_active": "true"
            })
            print(f"   Query 3 (user_email + is_active='true'): {q3_count} documents")

            # Query 4: Check all documents regardless of user
            all_docs = await collection.find({}).to_list(length=5)
            print(f"\n   All documents (first 5):")
            for idx, doc in enumerate(all_docs, 1):
                print(f"      {idx}. session_id={doc.get('session_id')}, user={doc.get('user_email')}, is_active={doc.get('is_active')} (type={type(doc.get('is_active'))})")

        except Exception as e:
            print(f"❌ Error: {e}")

    client.close()
    print(f"\n{'='*60}")
    print("Test complete!")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    asyncio.run(test_chat_collections())
