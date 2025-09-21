import openai
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
import uuid
import json
from motor.motor_asyncio import AsyncIOMotorDatabase
from fastapi import HTTPException

from models.chat_models import *
from database.mongo_manager import mongo_manager
from auth.auth_manager import AuthManager

logger = logging.getLogger(__name__)

class ChatManager:
    def __init__(self):
        self.openai_client = openai.AsyncOpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        self.db = mongo_manager.db
        
        self.collection_mapping = {
            'google_ads': [
                'google_ads_key_stats',
                'google_ads_campaigns', 
                'google_ads_keywords_related_to_campaign',
                'google_ads_performance'
            ],
            'google_analytics': [
                'google_analytics_metrics',
                'google_analytics_conversions',
                'google_analytics_traffic_sources',
                'google_analytics_top_pages'
            ],
            'intent_insights': [
                'intent_keyword_insights'
            ]
        }

    async def select_relevant_collections(self, user_message: str, module_type: ModuleType) -> List[str]:
        """Select relevant collections based on user query"""
        
        available_collections = []
        if module_type == ModuleType.GOOGLE_ADS:
            available_collections = self.collection_mapping['google_ads']
        elif module_type == ModuleType.GOOGLE_ANALYTICS:
            available_collections = self.collection_mapping['google_analytics']
        elif module_type == ModuleType.INTENT_INSIGHTS:
            available_collections = self.collection_mapping['intent_insights']

        # Simple selection logic - in production you might use AI for this
        selected = available_collections[:2]  # Take first 2 collections
        return selected

    async def get_comprehensive_context(
        self,
        user_email: str,
        selected_collections: List[str],
        customer_id: Optional[str] = None,
        property_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get data from selected collections"""
        
        context = {
            "selected_collections": selected_collections,
            "data_summary": {},
            "full_data": {}
        }
        
        for collection_name in selected_collections:
            try:
                collection = self.db[collection_name]
                
                query_filter = {"user_email": user_email}
                
                if customer_id and "ads" in collection_name:
                    query_filter["customer_id"] = customer_id
                
                if property_id and "analytics" in collection_name:
                    query_filter["property_id"] = property_id
                
                cursor = collection.find(query_filter).sort("last_updated", -1).limit(5)
                docs = await cursor.to_list(length=5)
                
                if docs:
                    collection_data = []
                    for doc in docs:
                        response_data = doc.get("response_data", {})
                        if response_data:
                            collection_data.append({
                                "timestamp": doc.get("last_updated"),
                                "response_data": response_data
                            })
                    
                    if collection_data:
                        context["full_data"][collection_name] = collection_data
                        context["data_summary"][collection_name] = {
                            "total_documents": len(docs),
                            "data_entries": len(collection_data)
                        }
                        
            except Exception as e:
                logger.error(f"Error processing {collection_name}: {e}")
        
        return context

    async def create_or_get_simple_session(
        self,
        user_email: str,
        module_type: ModuleType,
        session_id: Optional[str] = None,
        customer_id: Optional[str] = None,
        property_id: Optional[str] = None,
        period: str = "LAST_7_DAYS"
    ) -> str:
        """Create new session or get existing one"""
        collection = self.db.chat_sessions
        
        if session_id:
            # Verify session exists and belongs to user
            existing_session = await collection.find_one({
                "session_id": session_id,
                "user_email": user_email,
                "module_type": module_type.value
            })
            if existing_session:
                await collection.update_one(
                    {"session_id": session_id},
                    {"$set": {"last_activity": datetime.utcnow()}}
                )
                logger.info(f"Using existing session: {session_id}")
                return session_id
            else:
                logger.warning(f"Session {session_id} not found, creating new one")
        
        # Create new session
        new_session_id = str(uuid.uuid4())
        session_doc = {
            "session_id": new_session_id,
            "user_email": user_email,
            "module_type": module_type.value,
            "customer_id": customer_id,
            "property_id": property_id,
            "created_at": datetime.utcnow(),
            "last_activity": datetime.utcnow(),
            "is_active": True,
            "messages": []
        }
        
        await collection.insert_one(session_doc)
        logger.info(f"Created new chat session: {new_session_id}")
        return new_session_id
    
    async def add_message_to_simple_session(self, session_id: str, message: ChatMessage):
        """Add message to session"""
        collection = self.db.chat_sessions
        
        message_dict = {
            "role": message.role.value,
            "content": message.content,
            "timestamp": message.timestamp
        }
        
        await collection.update_one(
            {"session_id": session_id},
            {
                "$push": {"messages": message_dict},
                "$set": {"last_activity": datetime.utcnow()}
            }
        )

    async def _generate_enhanced_ai_response_simple(
        self,
        message: str,
        context: Dict[str, Any],
        module_type: ModuleType,
        session_id: str
    ) -> str:
        """Generate AI response using session context"""
        
        collection = self.db.chat_sessions
        session = await collection.find_one({"session_id": session_id})
        
        conversation_history = []
        if session and session.get("messages"):
            recent_messages = session["messages"][-6:]
            for msg in recent_messages:
                conversation_history.append({
                    "role": msg["role"],
                    "content": msg["content"]
                })
        
        system_prompt = self._get_enhanced_system_prompt(module_type, context)
        
        messages = [{"role": "system", "content": system_prompt}]
        messages.extend(conversation_history)
        messages.append({"role": "user", "content": message})
        
        try:
            response = await self.openai_client.chat.completions.create(
                model="gpt-4o",
                messages=messages,
                temperature=0.7,
                max_tokens=1500
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"Error generating AI response: {e}")
            return "I apologize, but I'm having trouble analyzing your data right now. Please try again."
        
    def _get_enhanced_system_prompt(self, module_type: ModuleType, context: Dict[str, Any]) -> str:
        """Get enhanced system prompt with data context"""
        
        base_prompt = """You are an expert marketing analytics assistant. 
        
        Capabilities:
        - Analyze Google Ads performance data
        - Interpret Google Analytics data  
        - Provide keyword insights and search volume analysis
        - Give actionable recommendations based on data trends
        
        Guidelines:
        - Use actual numbers and data from the provided information
        - Highlight key insights and trends
        - Provide specific, actionable recommendations
        - Format numbers clearly
        - Be conversational but data-driven
        """
        
        module_context = {
            ModuleType.GOOGLE_ADS: "Focus on ad spend efficiency, campaign performance, keyword opportunities.",
            ModuleType.GOOGLE_ANALYTICS: "Focus on traffic patterns, user engagement, conversion funnels.",
            ModuleType.INTENT_INSIGHTS: "Focus on search trends, market demand, keyword opportunities."
        }
        
        full_data = context.get("full_data", {})
        
        if full_data:
            data_prompt = f"\n\nYOUR MARKETING DATA:\n"
            
            for collection_name, collection_entries in full_data.items():
                data_prompt += f"\n=== {collection_name.upper().replace('_', ' ')} ===\n"
                
                for i, entry in enumerate(collection_entries):
                    response_data = entry.get('response_data', {})
                    data_prompt += f"\nData Entry {i+1}:\n"
                    
                    if isinstance(response_data, dict):
                        for key, value in response_data.items():
                            if isinstance(value, dict) and 'value' in value:
                                data_prompt += f"  {key}: {value.get('formatted', value['value'])}\n"
                            elif isinstance(value, (int, float)):
                                data_prompt += f"  {key}: {value:,}\n"
                            elif isinstance(value, str):
                                data_prompt += f"  {key}: {value}\n"
                    elif isinstance(response_data, list):
                        data_prompt += f"  Contains {len(response_data)} data points\n"
                        for j, item in enumerate(response_data[:3]):
                            data_prompt += f"    {j+1}. {item}\n"
        else:
            data_prompt = f"\n\nNo marketing data found for the specified criteria.\n"
        
        context_prompt = f"""
    Current Module: {module_type.value}
    Focus: {module_context.get(module_type, "General marketing analysis")}

    {data_prompt}

    Provide specific insights and actionable recommendations based on the data above.
    """
        
        return base_prompt + context_prompt

    async def process_chat_message(self, chat_request: ChatRequest, user_email: str) -> ChatResponse:
        """Process chat message with session-based storage"""
        
        logger.info(f"Processing chat message for user: {user_email}")
        logger.info(f"Message: '{chat_request.message}'")
        logger.info(f"Module: {chat_request.module_type.value}")
        logger.info(f"Session ID: {chat_request.session_id}")
        
        # Handle session
        if chat_request.session_id:
            session_id = chat_request.session_id
            # Update last activity
            await self.db.chat_sessions.update_one(
                {"session_id": session_id, "user_email": user_email},
                {"$set": {"last_activity": datetime.utcnow()}}
            )
            logger.info(f"Using existing session: {session_id}")
        else:
            session_id = await self.create_or_get_simple_session(
                user_email=user_email,
                module_type=chat_request.module_type,
                session_id=None,
                customer_id=chat_request.customer_id,
                property_id=chat_request.property_id,
                period=chat_request.period or "LAST_7_DAYS"
            )
            logger.info(f"Created new session: {session_id}")
        
        # Add user message to session
        user_message = ChatMessage(
            role=MessageRole.USER,
            content=chat_request.message,
            timestamp=datetime.utcnow()
        )
        await self.add_message_to_simple_session(session_id, user_message)
        
        # Select relevant collections
        selected_collections = await self.select_relevant_collections(
            user_message=chat_request.message,
            module_type=chat_request.module_type
        )
        
        # Get context data
        context = await self.get_comprehensive_context(
            user_email=user_email,
            selected_collections=selected_collections,
            customer_id=chat_request.customer_id,
            property_id=chat_request.property_id
        )
        
        # Generate AI response
        ai_response = await self._generate_enhanced_ai_response_simple(
            message=chat_request.message,
            context=context,
            module_type=chat_request.module_type,
            session_id=session_id
        )
        
        # Add AI response to session
        ai_message = ChatMessage(
            role=MessageRole.ASSISTANT,
            content=ai_response,
            timestamp=datetime.utcnow()
        )
        await self.add_message_to_simple_session(session_id, ai_message)
        
        return ChatResponse(
            response=ai_response,
            session_id=session_id,
            triggered_endpoint=None,
            endpoint_data=None,
            module_type=chat_request.module_type
        )

    async def get_conversation_by_session_id(
        self,
        user_email: str,
        session_id: str,
        module_type: ModuleType
    ) -> Optional[Dict[str, Any]]:
        """Get specific conversation by session ID - FIXED"""
        collection = self.db.chat_sessions
        
        logger.info(f"Looking for session: {session_id} for user: {user_email} module: {module_type.value}")
        
        session = await collection.find_one({
            "session_id": session_id,
            "user_email": user_email,
            "module_type": module_type.value
        })
        
        if session:
            session["_id"] = str(session["_id"])
            logger.info(f"Found session with {len(session.get('messages', []))} messages")
            return session
        else:
            logger.error(f"Session not found")
            # Debug: check what sessions exist for this user
            all_sessions = await collection.find({"user_email": user_email}).to_list(length=10)
            logger.error(f"Available sessions for user: {[s['session_id'] for s in all_sessions]}")
            return None

    async def get_chat_history(self, user_email: str, module_type: ModuleType, limit: int = 20) -> ChatHistoryResponse:
        """Get chat history from sessions"""
        collection = self.db.chat_sessions
        
        cursor = collection.find({
            "user_email": user_email,
            "module_type": module_type.value,
            "is_active": True
        }).sort("last_activity", -1).limit(limit)
        
        sessions = await cursor.to_list(length=limit)
        total_sessions = len(sessions)
        
        session_objects = []
        for session in sessions:
            # Only include sessions with messages
            if session.get("messages") and len(session["messages"]) > 0:
                messages = []
                for msg in session.get("messages", []):
                    messages.append(ChatMessage(
                        role=MessageRole(msg["role"]),
                        content=msg["content"],
                        timestamp=msg["timestamp"]
                    ))
                
                session_obj = ChatSession(
                    session_id=session["session_id"],
                    user_email=session["user_email"],
                    module_type=ModuleType(session["module_type"]),
                    customer_id=session.get("customer_id"),
                    property_id=session.get("property_id"),
                    messages=messages,
                    created_at=session["created_at"],
                    last_activity=session["last_activity"],
                    is_active=session.get("is_active", True)
                )
                session_objects.append(session_obj)
        
        return ChatHistoryResponse(
            sessions=session_objects,
            total_sessions=len(session_objects),
            module_type=module_type
        )
    
    async def delete_chat_sessions(self, user_email: str, session_ids: List[str]) -> Dict[str, Any]:
        """Delete chat sessions"""
        collection = self.db.chat_sessions
        
        result = await collection.update_many(
            {
                "session_id": {"$in": session_ids},
                "user_email": user_email
            },
            {
                "$set": {
                    "is_active": False,
                    "deleted_at": datetime.utcnow()
                }
            }
        )
        
        return {
            "deleted_sessions": result.modified_count,
            "requested_sessions": len(session_ids),
            "success": result.modified_count > 0
        }

# Create singleton instance
chat_manager = ChatManager()