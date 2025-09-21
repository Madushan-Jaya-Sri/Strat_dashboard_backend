import openai
import os
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import logging
import uuid
import json
import asyncio
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
                'google_ads_customers_accounts',
                'google_ads_key_stats',
                'google_ads_campaigns', 
                'google_ads_keywords_related_to_campaign',
                'google_ads_performance',
                'google_ads_geographic_performance',
                'google_ads_device_performance',
                'google_ads_time_performance',
                'google_ads_keyword_ideas'
            ],
            'google_analytics': [
                'google_analytics_properties',
                'google_analytics_metrics',
                'google_analytics_conversions',
                'google_analytics_traffic_sources',
                'google_analytics_top_pages',
                'google_analytics_channel_performance',
                'google_analytics_audience_insights',
                'google_analytics_time_series',
                'google_analytics_trends',
                'google_analytics_roas_roi_time_series'
            ],
            'intent_insights': [
                'intent_keyword_insights'
            ],
            'revenue_analysis': [
                'ga_revenue_breakdown_by_channel',
                'ga_revenue_breakdown_by_source',
                'ga_revenue_breakdown_by_device',
                'ga_revenue_breakdown_by_location',
                'ga_revenue_breakdown_by_page',
                'ga_revenue_breakdown_by_comprehensive',
                'ga_combined_roas_roi_metrics',
            ],
            'combined_metrics': [
                'ads_ga_combined_overview_metrics',
                'ga_combined_roas_roi_metrics_legacy'
            ],
            'channel_timeseries': [
                'ga_channel_revenue_time_series',
                'ga_specific_channels_time_series',
                'ga_available_channels'
            ]
        }

    async def select_relevant_collections(self, user_message: str, module_type: ModuleType) -> List[str]:
        """Use AI agent to select relevant collections based on user query"""
        
        available_collections = []
        if module_type == ModuleType.GOOGLE_ADS:
            available_collections.extend(self.collection_mapping['google_ads'])
        elif module_type == ModuleType.GOOGLE_ANALYTICS:
            available_collections.extend(self.collection_mapping['google_analytics'])
            available_collections.extend(self.collection_mapping['revenue_analysis'])
            available_collections.extend(self.collection_mapping['channel_timeseries'])
        elif module_type == ModuleType.INTENT_INSIGHTS:
            available_collections.extend(self.collection_mapping['intent_insights'])
        
        if "keyword" in user_message.lower() or "search" in user_message.lower():
            available_collections.extend(self.collection_mapping['intent_insights'])
        
        if "revenue" in user_message.lower() or "roas" in user_message.lower() or "roi" in user_message.lower():
            available_collections.extend(self.collection_mapping['revenue_analysis'])
            available_collections.extend(self.collection_mapping['combined_metrics'])

        selection_prompt = f"""
        Analyze this user query and select the most relevant data collections to answer their question.

        User Query: "{user_message}"
        Module Type: {module_type.value}

        Available Collections:
        {chr(10).join([f"- {col}: {self._get_collection_description(col)}" for col in available_collections])}

        Instructions:
        1. Select 2-4 most relevant collections that would help answer the user's question
        2. Prioritize collections that directly relate to the user's specific query
        3. Include supporting collections that provide context
        4. Return ONLY a JSON list of collection names, nothing else

        Example response: ["google_ads_campaigns", "google_ads_key_stats"]
        """

        try:
            response = await self.openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": selection_prompt}],
                temperature=0.1,
                max_tokens=200
            )
            
            content = response.choices[0].message.content.strip()
            selected_collections = json.loads(content)
            valid_collections = [col for col in selected_collections if col in available_collections]
            return valid_collections
            
        except Exception as e:
            logger.error(f"ERROR IN COLLECTION SELECTION: {e}")
            fallback_collections = []
            if module_type == ModuleType.GOOGLE_ADS:
                fallback_collections = ['google_ads_key_stats', 'google_ads_campaigns']
            elif module_type == ModuleType.GOOGLE_ANALYTICS:
                fallback_collections = ['google_analytics_metrics', 'google_analytics_conversions']
            else:
                fallback_collections = ['intent_keyword_insights']
            
            return fallback_collections

    def _get_collection_description(self, collection_name: str) -> str:
        """Get human-readable description of what each collection contains"""
        descriptions = {
            'google_ads_customers_accounts': 'Available Google Ads customer accounts and basic info',
            'google_ads_key_stats': 'Overall campaign performance metrics (impressions, clicks, cost, conversions)',
            'google_ads_campaigns': 'Individual campaign data with performance breakdown',
            'google_ads_keywords_related_to_campaign': 'Keyword-level performance data and bidding information',
            'google_ads_performance': 'Advanced performance metrics and trends',
            'google_ads_geographic_performance': 'Performance breakdown by geographic location',
            'google_ads_device_performance': 'Performance data by device type (mobile, desktop, tablet)',
            'google_ads_time_performance': 'Daily performance trends over time',
            'google_ads_keyword_ideas': 'Keyword suggestions and search volume data',
            'google_analytics_properties': 'Available GA4 properties and account information',
            'google_analytics_metrics': 'Website traffic and user engagement metrics',
            'google_analytics_conversions': 'Conversion events and goal completions',
            'google_analytics_traffic_sources': 'Traffic source attribution and channel data',
            'google_analytics_top_pages': 'Most visited pages and page performance',
            'google_analytics_channel_performance': 'Marketing channel effectiveness',
            'google_analytics_audience_insights': 'User demographics and behavior patterns',
            'google_analytics_time_series': 'Historical trends and time-based analytics',
            'google_analytics_trends': 'User acquisition trends over time',
            'google_analytics_roas_roi_time_series': 'ROAS and ROI performance over time',
            'ga_revenue_breakdown_by_channel': 'Revenue attribution by marketing channel',
            'ga_revenue_breakdown_by_source': 'Revenue breakdown by traffic source/medium',
            'ga_revenue_breakdown_by_device': 'Revenue performance by device category',
            'ga_revenue_breakdown_by_location': 'Revenue breakdown by geographic location',
            'ga_revenue_breakdown_by_page': 'Revenue performance by landing page',
            'ga_revenue_breakdown_by_comprehensive': 'Complete revenue analysis across all dimensions',
            'ga_channel_revenue_time_series': 'Channel revenue performance over time',
            'ga_specific_channels_time_series': 'Time series for specific marketing channels',
            'ga_available_channels': 'List of available marketing channels',
            'ads_ga_combined_overview_metrics': 'Combined overview from Google Ads and Analytics',
            'ga_combined_roas_roi_metrics': 'Combined ROAS/ROI metrics from GA4 and Google Ads',
            'ga_combined_roas_roi_metrics_legacy': 'Legacy combined ROAS/ROI metrics',
            'intent_keyword_insights': 'Search volume trends and keyword opportunity analysis'
        }
        return descriptions.get(collection_name, 'Marketing data collection')

    async def get_comprehensive_context(
        self,
        user_email: str,
        selected_collections: List[str],
        customer_id: Optional[str] = None,
        property_id: Optional[str] = None,
        limit_per_collection: int = 10
    ) -> Dict[str, Any]:
        """Get comprehensive data from selected collections"""
        
        context = {
            "selected_collections": selected_collections,
            "data_summary": {},
            "full_data": {},
            "raw_response_data": {}
        }
        
        total_data_found = 0
        
        for collection_name in selected_collections:
            try:
                collection = self.db[collection_name]
                
                query_filter = {"user_email": user_email}
                
                if customer_id and ("ads" in collection_name or "google_ads" in collection_name):
                    query_filter["customer_id"] = customer_id
                
                if property_id and ("analytics" in collection_name or "ga_" in collection_name):
                    query_filter["property_id"] = property_id
                
                cursor = collection.find(query_filter).sort("last_updated", -1).limit(limit_per_collection)
                docs = await cursor.to_list(length=limit_per_collection)
                
                if docs:
                    collection_data = []
                    
                    for doc in docs:
                        response_data = doc.get("response_data", {})
                        
                        if response_data:
                            collection_data.append({
                                "timestamp": doc.get("last_updated", doc.get("created_at")),
                                "request_params": doc.get("request_params", {}),
                                "response_data": response_data
                            })
                            total_data_found += 1
                    
                    if collection_data:
                        context["full_data"][collection_name] = collection_data
                        context["raw_response_data"][collection_name] = collection_data
                        
                        context["data_summary"][collection_name] = {
                            "total_documents": len(docs),
                            "data_entries": len(collection_data),
                            "latest_update": docs[0].get("last_updated") if docs else None,
                            "description": self._get_collection_description(collection_name)
                        }
                        
            except Exception as e:
                logger.error(f"❌ ERROR PROCESSING {collection_name}: {e}")
                context["data_summary"][collection_name] = {"error": str(e)}
        
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
            existing_session = await collection.find_one({
                "session_id": session_id,
                "user_email": user_email
            })
            if existing_session:
                await collection.update_one(
                    {"session_id": session_id},
                    {"$set": {"last_activity": datetime.utcnow()}}
                )
                return session_id
        
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
        
        system_prompt = self._get_enhanced_system_prompt_v2(module_type, context)
        
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
            logger.error(f"ERROR GENERATING AI RESPONSE: {e}")
            return "I apologize, but I'm having trouble analyzing your data right now. Please try again."
        
    async def process_chat_message(self, chat_request: ChatRequest, user_email: str) -> ChatResponse:
        """Process chat message with session-based storage"""
        
        if chat_request.session_id:
            session_id = chat_request.session_id
            await self.db.chat_sessions.update_one(
                {"session_id": session_id},
                {"$set": {"last_activity": datetime.utcnow()}}
            )
        else:
            session_id = await self.create_or_get_simple_session(
                user_email=user_email,
                module_type=chat_request.module_type,
                session_id=None,
                customer_id=chat_request.customer_id,
                property_id=chat_request.property_id,
                period=chat_request.period or "LAST_7_DAYS"
            )
        
        user_message = ChatMessage(
            role=MessageRole.USER,
            content=chat_request.message,
            timestamp=datetime.utcnow()
        )
        await self.add_message_to_simple_session(session_id, user_message)
        
        selected_collections = await self.select_relevant_collections(
            user_message=chat_request.message,
            module_type=chat_request.module_type
        )
        
        context = await self.get_comprehensive_context(
            user_email=user_email,
            selected_collections=selected_collections,
            customer_id=chat_request.customer_id,
            property_id=chat_request.property_id
        )
        
        ai_response = await self._generate_enhanced_ai_response_simple(
            message=chat_request.message,
            context=context,
            module_type=chat_request.module_type,
            session_id=session_id
        )
        
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

    def _get_enhanced_system_prompt_v2(self, module_type: ModuleType, context: Dict[str, Any]) -> str:
        """Enhanced system prompt with complete data extraction"""
        
        base_prompt = """You are an expert marketing analytics assistant with access to real marketing data. 
        
        Your capabilities:
        - Analyze Google Ads performance data (campaigns, keywords, costs, conversions)
        - Interpret Google Analytics data (traffic, user behavior, conversions, revenue)
        - Provide keyword insights and search volume analysis
        - Give actionable recommendations based on data trends
        
        Guidelines:
        - Use actual numbers and data from the provided information
        - Highlight key insights and trends you can see in the data
        - Provide specific, actionable recommendations
        - Format numbers clearly (use commas, percentages, currency symbols)
        - If you see concerning trends, mention them with specific data points
        - Be conversational but data-driven
        """
        
        module_context = {
            ModuleType.GOOGLE_ADS: "Focus on ad spend efficiency, campaign performance, keyword opportunities, and conversion optimization.",
            ModuleType.GOOGLE_ANALYTICS: "Focus on traffic patterns, user engagement, conversion funnels, and revenue attribution.",
            ModuleType.INTENT_INSIGHTS: "Focus on search trends, market demand, keyword opportunities, and competitive analysis."
        }
        
        full_data = context.get("full_data", {})
        
        if full_data:
            data_prompt = f"\n\nYOUR ACTUAL MARKETING DATA:\n"
            
            for collection_name, collection_entries in full_data.items():
                data_prompt += f"\n=== {collection_name.upper().replace('_', ' ')} ===\n"
                data_prompt += f"Description: {self._get_collection_description(collection_name)}\n"
                
                for i, entry in enumerate(collection_entries):
                    response_data = entry.get('response_data', {})
                    timestamp = entry.get('timestamp', 'Unknown')
                    
                    data_prompt += f"\nData Entry {i+1} (Updated: {timestamp}):\n"
                    
                    if isinstance(response_data, dict):
                        for key, value in response_data.items():
                            if isinstance(value, dict):
                                if 'value' in value and 'formatted' in value:
                                    data_prompt += f"  {key}: {value['formatted']} (raw: {value['value']})\n"
                                elif 'label' in value:
                                    data_prompt += f"  {value.get('label', key)}: {value.get('formatted', value.get('value', 'N/A'))}\n"
                                else:
                                    data_prompt += f"  {key}: {value}\n"
                            elif isinstance(value, (int, float)):
                                data_prompt += f"  {key}: {value:,}\n"
                            elif isinstance(value, str):
                                data_prompt += f"  {key}: {value}\n"
                            elif isinstance(value, list):
                                data_prompt += f"  {key}: List with {len(value)} items\n"
                                for j, item in enumerate(value[:3]):
                                    if isinstance(item, dict):
                                        data_prompt += f"    Item {j+1}: {item}\n"
                                    else:
                                        data_prompt += f"    Item {j+1}: {item}\n"
                    
                    elif isinstance(response_data, list):
                        data_prompt += f"  Contains {len(response_data)} data points:\n"
                        for j, item in enumerate(response_data):
                            if isinstance(item, dict):
                                item_desc = ", ".join([f"{k}: {v}" for k, v in item.items()])
                                data_prompt += f"    {j+1}. {item_desc}\n"
                            else:
                                data_prompt += f"    {j+1}. {item}\n"
                    
                    data_prompt += "\n"
        else:
            data_prompt = f"\n\nNO MARKETING DATA FOUND for the specified criteria.\n"
            data_prompt += f"Available collections checked: {context.get('selected_collections', [])}\n"
        
        context_prompt = f"""
    Current Module: {module_type.value}
    Focus: {module_context.get(module_type, "General marketing analysis")}

    {data_prompt}

    Based on the above data, provide specific insights and actionable recommendations. Use the actual numbers and trends you can see in the data.
    """
        
        return base_prompt + context_prompt

    async def get_chat_history(self, user_email: str, module_type: ModuleType, limit: int = 20) -> ChatHistoryResponse:
        """Get chat history from sessions"""
        collection = self.db.chat_sessions
        
        cursor = collection.find({
            "user_email": user_email,
            "module_type": module_type.value,
            "is_active": True
        }).sort("last_activity", -1).limit(limit)
        
        sessions = await cursor.to_list(length=limit)
        total_sessions = await collection.count_documents({
            "user_email": user_email,
            "module_type": module_type.value,
            "is_active": True
        })
        
        session_objects = []
        for session in sessions:
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
            total_sessions=total_sessions,
            module_type=module_type
        )

    async def get_conversation_by_session_id(
        self,
        user_email: str,
        session_id: str,
        module_type: ModuleType
    ) -> Optional[Dict[str, Any]]:
        """Get specific conversation by session ID"""
        collection = self.db.chat_sessions
        
        session = await collection.find_one({
            "session_id": session_id,
            "user_email": user_email,
            "module_type": module_type.value
        })
        
        if session:
            session["_id"] = str(session["_id"])
        
        return session
    
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

chat_manager = ChatManager()