"""
Script to clean up duplicate messages in chat collections
Removes empty assistant messages and duplicate user/assistant pairs
"""

import asyncio
import os
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime

async def cleanup_chat_collections():
    """Clean up duplicate messages in all chat collections"""

    # Connect to MongoDB
    connection_string = os.getenv('MONGODB_CONNECTION_STRING')
    if not connection_string:
        print("❌ MONGODB_CONNECTION_STRING not found in environment variables")
        return

    client = AsyncIOMotorClient(connection_string)
    db = client.internal_dashboard

    # List of chat collections to clean
    collections_to_clean = [
        'chat_google_ads',
        'chat_google_analytics',
        'chat_meta_ads',
        'chat_facebook_analytics',
        'chat_instagram_analytics',
        'chat_intent_insights'
    ]

    total_fixed = 0

    for collection_name in collections_to_clean:
        print(f"\n🔍 Processing collection: {collection_name}")

        try:
            collection = db[collection_name]

            # Get all sessions
            sessions = await collection.find({}).to_list(length=None)

            print(f"   Found {len(sessions)} sessions")

            for session in sessions:
                session_id = session.get('session_id')
                messages = session.get('messages', [])

                if not messages:
                    continue

                # Clean up messages
                cleaned_messages = []
                seen_pairs = set()

                i = 0
                while i < len(messages):
                    msg = messages[i]

                    # Skip empty messages
                    if not msg.get('content', '').strip():
                        print(f"   ⚠️  Removing empty message from session {session_id}")
                        i += 1
                        continue

                    # Check for duplicate user-assistant pairs
                    if msg.get('role') == 'user' and i + 1 < len(messages):
                        next_msg = messages[i + 1]

                        if next_msg.get('role') == 'assistant':
                            # Create a unique key for this pair
                            pair_key = f"{msg['content'][:50]}|{next_msg['content'][:50]}"

                            # If we've seen this pair before, skip it
                            if pair_key in seen_pairs:
                                print(f"   ⚠️  Removing duplicate pair from session {session_id}")
                                i += 2
                                continue

                            seen_pairs.add(pair_key)
                            cleaned_messages.append(msg)
                            cleaned_messages.append(next_msg)
                            i += 2
                            continue

                    # Add single message (shouldn't happen often)
                    cleaned_messages.append(msg)
                    i += 1

                # Update if messages were cleaned
                if len(cleaned_messages) != len(messages):
                    await collection.update_one(
                        {'_id': session['_id']},
                        {
                            '$set': {
                                'messages': cleaned_messages,
                                'last_activity': datetime.utcnow()
                            }
                        }
                    )
                    print(f"   ✅ Fixed session {session_id}: {len(messages)} → {len(cleaned_messages)} messages")
                    total_fixed += 1

        except Exception as e:
            print(f"   ❌ Error processing {collection_name}: {e}")

    print(f"\n✅ Cleanup complete! Fixed {total_fixed} sessions")
    client.close()


if __name__ == "__main__":
    asyncio.run(cleanup_chat_collections())
