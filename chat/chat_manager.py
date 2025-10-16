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
        
        # Complete collection mappings to match your actual MongoDB structure
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
                'ga_audience_insights_city',
                'ga_audience_insights_userAgeBracket', 
                'ga_audience_insights_userGender',
                'ga_audience_insights_deviceCategory',
                'ga_audience_insights_browser',
                'google_analytics_time_series',
                'google_analytics_trends',
                'google_analytics_roas_roi_time_series'
            ],
            'intent_insights': [
                'intent_keyword_insights'
            ],
                'meta_ads': [
                'meta_ad_accounts',
                'meta_account_insights_summary',
                'meta_campaigns_paginated',
                'meta_campaigns_list',
                'meta_campaigns_timeseries',
                'meta_campaigns_demographics',
                'meta_campaigns_placements',
                'meta_adsets',
                'meta_adsets_timeseries',
                'meta_adsets_demographics',
                'meta_adsets_placements',
                'meta_ads',
                'meta_ads_timeseries',
                'meta_ads_demographics',
                'meta_ads_placements'
            ],
            'facebook_analytics': [
                'facebook_pages',
                'facebook_page_insights',
                'facebook_page_insights_timeseries',
                'facebook_page_posts',
                'facebook_page_posts_timeseries',
                'facebook_video_views_breakdown',
                'facebook_content_type_breakdown',
                'facebook_page_demographics',
                'facebook_follows_unfollows',
                'facebook_engagement_breakdown',
                'facebook_organic_vs_paid'
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

    async def debug_collections_and_data(
        self,
        user_email: str,
        customer_id: Optional[str] = None,
        property_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Debug function to check what collections exist and what data is available"""
        debug_info = {
            "user_email": user_email,
            "customer_id": customer_id,
            "property_id": property_id,
            "database_name": self.db.name,
            "collections_found": [],
            "collections_with_data": {},
            "total_documents": 0
        }
        
        # Get all collection names in the database
        all_collections = await self.db.list_collection_names()
        logger.info(f"🔍 All collections in database: {all_collections}")
        debug_info["all_collections_in_db"] = all_collections
        
        # Check each collection for user data
        for collection_name in all_collections:
            try:
                collection = self.db[collection_name]
                
                # Count total documents in collection
                total_docs = await collection.count_documents({})
                
                # Count documents for this user
                user_docs = await collection.count_documents({"user_email": user_email})
                
                # Count documents with customer_id if provided
                customer_docs = 0
                if customer_id:
                    customer_docs = await collection.count_documents({
                        "user_email": user_email,
                        "customer_id": customer_id
                    })
                
                # Count documents with property_id if provided
                property_docs = 0
                if property_id:
                    property_docs = await collection.count_documents({
                        "user_email": user_email,
                        "property_id": property_id
                    })
                
                if total_docs > 0:
                    debug_info["collections_found"].append(collection_name)
                    debug_info["collections_with_data"][collection_name] = {
                        "total_documents": total_docs,
                        "user_documents": user_docs,
                        "customer_documents": customer_docs,
                        "property_documents": property_docs
                    }
                    debug_info["total_documents"] += total_docs
                    
                    logger.info(f"📊 Collection '{collection_name}': {total_docs} total, {user_docs} user, {customer_docs} customer, {property_docs} property")
                
            except Exception as e:
                logger.error(f"❌ Error checking collection {collection_name}: {e}")
        
        return debug_info

    async def select_relevant_collections(
        self,
        user_message: str,
        module_type: ModuleType
    ) -> List[str]:
        """Use AI agent to select relevant collections based on user query with detailed logging"""
        
        logger.info(f"COLLECTION SELECTION - Query: '{user_message[:100]}...' Module: {module_type.value}")
        
        available_collections = []
        if module_type == ModuleType.GOOGLE_ADS:
            available_collections.extend(self.collection_mapping['google_ads'])
        elif module_type == ModuleType.GOOGLE_ANALYTICS:
            available_collections.extend(self.collection_mapping['google_analytics'])
            available_collections.extend(self.collection_mapping['revenue_analysis'])
            available_collections.extend(self.collection_mapping['channel_timeseries'])
        elif module_type == ModuleType.INTENT_INSIGHTS:
            available_collections.extend(self.collection_mapping['intent_insights'])
        elif module_type == ModuleType.META_ADS:  # Add this
            available_collections.extend(self.collection_mapping['meta_ads'])
        elif module_type == ModuleType.FACEBOOK_ANALYTICS:  # Add this
            available_collections.extend(self.collection_mapping['facebook_analytics'])
    
        
        # Cross-module collections
        if "keyword" in user_message.lower() or "search" in user_message.lower():
            available_collections.extend(self.collection_mapping['intent_insights'])
        
        if "revenue" in user_message.lower() or "roas" in user_message.lower() or "roi" in user_message.lower():
            available_collections.extend(self.collection_mapping['revenue_analysis'])
            available_collections.extend(self.collection_mapping['combined_metrics'])

        logger.info(f"AVAILABLE COLLECTIONS TO LLM: {available_collections}")

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

        logger.info(f"PROMPT SENT TO LLM FOR COLLECTION SELECTION:\n{selection_prompt}")

        try:
            response = await self.openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": selection_prompt}],
                temperature=0.1,
                max_tokens=200
            )
            
            content = response.choices[0].message.content.strip()
            logger.info(f"LLM COLLECTION SELECTION RESPONSE: {content}")
            
            selected_collections = json.loads(content)
            
            # Validate selections
            valid_collections = [col for col in selected_collections if col in available_collections]
            invalid_collections = [col for col in selected_collections if col not in available_collections]
            
            if invalid_collections:
                logger.warning(f"INVALID COLLECTIONS SELECTED BY LLM: {invalid_collections}")
            
            logger.info(f"FINAL VALID COLLECTIONS SELECTED: {valid_collections}")
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
            
            logger.info(f"USING FALLBACK COLLECTIONS: {fallback_collections}")
            return fallback_collections

    async def send_status_update(self, status: str, details: str = ""):
        """Send status update (in real implementation, this would use WebSocket)"""
        logger.info(f"STATUS: {status} - {details}")
        # For now, just log. In production, you'd send via WebSocket
        return {"status": status, "details": details, "timestamp": datetime.utcnow()}

    def _get_collection_description(self, collection_name: str) -> str:
        """Get human-readable description of what each collection contains"""
        descriptions = {
            # Google Ads collections
            'google_ads_customers_accounts': 'Available Google Ads customer accounts and basic info',
            'google_ads_key_stats': 'Overall campaign performance metrics (impressions, clicks, cost, conversions)',
            'google_ads_campaigns': 'Individual campaign data with performance breakdown',
            'google_ads_keywords_related_to_campaign': 'Keyword-level performance data and bidding information',
            'google_ads_performance': 'Advanced performance metrics and trends',
            'google_ads_geographic_performance': 'Performance breakdown by geographic location',
            'google_ads_device_performance': 'Performance data by device type (mobile, desktop, tablet)',
            'google_ads_time_performance': 'Daily performance trends over time',
            'google_ads_keyword_ideas': 'Keyword suggestions and search volume data',
            
            # Google Analytics collections
            'google_analytics_properties': 'Available GA4 properties and account information',
            'google_analytics_metrics': 'Website traffic and user engagement metrics',
            'google_analytics_conversions': 'Conversion events and goal completions',
            'google_analytics_traffic_sources': 'Traffic source attribution and channel data',
            'google_analytics_top_pages': 'Most visited pages and page performance',
            'google_analytics_channel_performance': 'Marketing channel effectiveness',
            'ga_audience_insights_city': 'User demographics/geograpic and behavior patterns by city location',
            'ga_audience_insights_userAgeBracket': 'User demographics and behavior patterns by age bracket',
            'ga_audience_insights_userGender': 'User demographics and behavior patterns by gender',
            'ga_audience_insights_deviceCategory': 'User demographics and behavior patterns by device category (mobile, desktop, tablet)',
            'ga_audience_insights_browser': 'User demographics and behavior patterns by browser type',
            'google_analytics_time_series': 'Historical trends and time-based analytics',
            'google_analytics_trends': 'User acquisition trends over time',
            'google_analytics_roas_roi_time_series': 'ROAS and ROI performance over time',
        
            
            # Revenue breakdown collections
            'ga_revenue_breakdown_by_channel': 'Revenue attribution by marketing channel',
            'ga_revenue_breakdown_by_source': 'Revenue breakdown by traffic source/medium',
            'ga_revenue_breakdown_by_device': 'Revenue performance by device category',
            'ga_revenue_breakdown_by_location': 'Revenue breakdown by geographic location',
            'ga_revenue_breakdown_by_page': 'Revenue performance by landing page',
            'ga_revenue_breakdown_by_comprehensive': 'Complete revenue analysis across all dimensions',
            
            # Channel revenue time series
            'ga_channel_revenue_time_series': 'Channel revenue performance over time',
            'ga_specific_channels_time_series': 'Time series for specific marketing channels',
            'ga_available_channels': 'List of available marketing channels',
            
            # Combined metrics
            'ads_ga_combined_overview_metrics': 'Combined overview from Google Ads and Analytics',
            'ga_combined_roas_roi_metrics': 'Combined ROAS/ROI metrics from GA4 and Google Ads',
            'ga_combined_roas_roi_metrics_legacy': 'Legacy combined ROAS/ROI metrics',
            

            # Meta Ads collections
            'meta_ad_accounts': 'Meta Ads account information and status',
            'meta_account_insights_summary': 'Account-level performance summary for Meta Ads',
            'meta_campaigns_paginated': 'Paginated Meta campaign data with insights',
            'meta_campaigns_list': 'List of all Meta campaigns',
            'meta_campaigns_timeseries': 'Time-series performance data for Meta campaigns',
            'meta_campaigns_demographics': 'Demographic breakdown for Meta campaigns',
            'meta_campaigns_placements': 'Placement performance data for Meta campaigns',
            'meta_adsets': 'Ad set information and performance for Meta',
            'meta_adsets_timeseries': 'Time-series performance data for Meta ad sets',
            'meta_adsets_demographics': 'Demographic breakdown for Meta ad sets',
            'meta_adsets_placements': 'Placement performance data for Meta ad sets',
            'meta_ads': 'Individual ad performance data for Meta',
            'meta_ads_timeseries': 'Time-series performance data for Meta ads',
            'meta_ads_demographics': 'Demographic breakdown for Meta ads',
            'meta_ads_placements': 'Placement performance data for Meta ads',
            
            # Facebook Analytics collections
            'facebook_pages': 'Facebook page information and basic stats',
            'facebook_page_insights': 'Page-level insights and engagement metrics',
            'facebook_page_insights_timeseries': 'Time-series data for Facebook page metrics',
            'facebook_page_posts': 'Facebook posts and their performance',
            'facebook_page_posts_timeseries': 'Time-series performance data for Facebook posts',
            'facebook_video_views_breakdown': 'Video views and engagement breakdown',
            'facebook_content_type_breakdown': 'Performance by content type',
            'facebook_page_demographics': 'Audience demographics for Facebook page',
            'facebook_follows_unfollows': 'Follower growth and attrition data',
            'facebook_engagement_breakdown': 'Engagement metrics breakdown',
            'facebook_organic_vs_paid': 'Organic vs paid reach and engagement',

            # Intent insights
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
        """Get comprehensive data from selected collections with complete data extraction"""
        
        logger.info(f"DATA EXTRACTION - User: {user_email}, Customer: {customer_id}, Property: {property_id}")
        logger.info(f"SELECTED COLLECTIONS FOR DATA EXTRACTION: {selected_collections}")
        
        context = {
            "selected_collections": selected_collections,
            "data_summary": {},
            "full_data": {},
            "raw_response_data": {}
        }
        
        total_data_found = 0
        
        for collection_name in selected_collections:
            logger.info(f"PROCESSING COLLECTION: {collection_name}")
            
            try:
                collection = self.db[collection_name]
                
                # Build query filter
                query_filter = {"user_email": user_email}
                
                # Add appropriate ID filters based on collection type
                if customer_id and ("ads" in collection_name or "google_ads" in collection_name):
                    query_filter["customer_id"] = customer_id
                    logger.info(f"ADDED CUSTOMER_ID FILTER: {customer_id}")
                
                if property_id and ("analytics" in collection_name or "ga_" in collection_name):
                    query_filter["property_id"] = property_id
                    logger.info(f"ADDED PROPERTY_ID FILTER: {property_id}")
                
                logger.info(f"QUERY FILTER: {query_filter}")
                
                # Get documents
                cursor = collection.find(query_filter).sort("last_updated", -1).limit(limit_per_collection)
                docs = await cursor.to_list(length=limit_per_collection)
                
                logger.info(f"FOUND {len(docs)} DOCUMENTS IN {collection_name}")
                
                if docs:
                    # Extract the complete nested response_data
                    collection_data = []
                    
                    for i, doc in enumerate(docs):
                        response_data = doc.get("response_data", {})
                        
                        if i == 0:  # Log structure of first document
                            logger.info(f"DOCUMENT STRUCTURE IN {collection_name}:")
                            logger.info(f"  - Response data type: {type(response_data)}")
                            
                            if isinstance(response_data, dict):
                                logger.info(f"  - Response data keys: {list(response_data.keys())}")
                            elif isinstance(response_data, list):
                                logger.info(f"  - Response data is list with {len(response_data)} items")
                                if response_data and isinstance(response_data[0], dict):
                                    logger.info(f"    - First item keys: {list(response_data[0].keys())}")
                        
                        # Store the complete response_data (this is the nested data)
                        if response_data:
                            collection_data.append({
                                "timestamp": doc.get("last_updated", doc.get("created_at")),
                                "request_params": doc.get("request_params", {}),
                                "response_data": response_data  # Complete nested data
                            })
                            total_data_found += 1
                            
                            # Log sample of actual data content
                            if i == 0:  # Only for first document to avoid spam
                                logger.info(f"SAMPLE DATA CONTENT:")
                                if isinstance(response_data, dict):
                                    # Show actual values for key metrics
                                    for key, value in list(response_data.items())[:5]:
                                        if isinstance(value, dict) and 'value' in value:
                                            logger.info(f"    - {key}: {value}")
                                        elif isinstance(value, (int, float, str)):
                                            logger.info(f"    - {key}: {value}")
                                elif isinstance(response_data, list) and response_data:
                                    logger.info(f"    - First list item: {response_data[0]}")
                    
                    if collection_data:
                        # Store in both places for compatibility
                        context["full_data"][collection_name] = collection_data
                        context["raw_response_data"][collection_name] = collection_data
                        
                        context["data_summary"][collection_name] = {
                            "total_documents": len(docs),
                            "data_entries": len(collection_data),
                            "latest_update": docs[0].get("last_updated") if docs else None,
                            "description": self._get_collection_description(collection_name)
                        }
                        
                        logger.info(f"✅ EXTRACTED DATA FROM {collection_name}: {len(collection_data)} entries")
                    else:
                        logger.warning(f"❌ NO VALID RESPONSE DATA IN {collection_name}")
                else:
                    logger.warning(f"❌ NO DOCUMENTS FOUND IN {collection_name} WITH FILTER: {query_filter}")
                    
                    # Debug: check what exists
                    user_only_count = await collection.count_documents({"user_email": user_email})
                    total_count = await collection.count_documents({})
                    logger.info(f"DEBUG - {collection_name}: Total docs: {total_count}, User docs: {user_only_count}")
                    
            except Exception as e:
                logger.error(f"❌ ERROR PROCESSING {collection_name}: {e}")
                context["data_summary"][collection_name] = {"error": str(e)}
        
        logger.info(f"📊 TOTAL DATA ENTRIES FOUND ACROSS ALL COLLECTIONS: {total_data_found}")
        logger.info(f"📈 COLLECTIONS WITH DATA: {list(context['full_data'].keys())}")
        
        return context

    def _should_trigger_intent_endpoint(self, message: str) -> bool:
        """Determine if message requires keyword insights"""
        intent_keywords = [
            "keyword", "search volume", "search trend", "keyword history",
            "search history", "keyword performance", "search data",
            "keyword insights", "search analytics", "keyword metrics"
        ]
        
        message_lower = message.lower()
        return any(keyword in message_lower for keyword in intent_keywords)  
    
    def _extract_keywords_from_message(self, message: str) -> Dict[str, Any]:
        """Extract keywords from user message for intent insights endpoint"""
        words = message.lower().split()
        potential_keywords = [word.strip('.,!?') for word in words if len(word) > 3]
        
        keywords = [word for word in potential_keywords if word not in [
            'what', 'how', 'when', 'where', 'why', 'show', 'give', 'tell', 'find',
            'search', 'volume', 'trend', 'data', 'keyword', 'insights'
        ]][:5]
        
        return {
            "suggested_keywords": keywords[:2] if keywords else ["marketing", "digital"],
            "original_query": message
        }

    async def create_or_get_simple_session(
        self,
        user_email: str,
        module_type: ModuleType,
        session_id: Optional[str] = None,
        customer_id: Optional[str] = None,
        property_id: Optional[str] = None,
        account_id: Optional[str] = None,  # Add this
        page_id: Optional[str] = None,  # Add this
        period: str = "LAST_7_DAYS"
    ) -> str:
        """Create new session or get existing one - simple format"""
        collection = self.db.chat_sessions
        
        if session_id:
            # Try to get existing session and verify it belongs to user
            existing_session = await collection.find_one({
                "session_id": session_id,
                "user_email": user_email
            })
            if existing_session:
                # Update last activity and return existing session
                await collection.update_one(
                    {"session_id": session_id},
                    {"$set": {"last_activity": datetime.utcnow()}}
                )
                logger.info(f"Using existing session: {session_id}")
                return session_id
            else:
                logger.warning(f"Session {session_id} not found for user {user_email}")
        
        # Create new session ONLY if no valid existing session
        new_session_id = str(uuid.uuid4())
        session_doc = {
            "session_id": new_session_id,
            "user_email": user_email,
            "module_type": module_type.value,
            "customer_id": customer_id,
            "property_id": property_id,
            "account_id": account_id,  # Add this
            "page_id": page_id,  # Add this
            "created_at": datetime.utcnow(),
            "last_activity": datetime.utcnow(),
            "is_active": True,
            "messages": []
        }
        
        await collection.insert_one(session_doc)
        logger.info(f"Created new chat session: {new_session_id}")
        return new_session_id

    async def add_message_to_simple_session(
        self,
        session_id: str,
        message: ChatMessage
    ):
        """Add message to simple session format"""
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
        """Generate AI response using simple session format"""
        
        # Get conversation history from session
        collection = self.db.chat_sessions
        session = await collection.find_one({"session_id": session_id})
        
        conversation_history = []
        if session and session.get("messages"):
            # Get last 6 messages for context
            recent_messages = session["messages"][-6:]
            for msg in recent_messages:
                conversation_history.append({
                    "role": msg["role"],
                    "content": msg["content"]
                })
        
        # Create enhanced system prompt
        system_prompt = self._get_enhanced_system_prompt_v2(module_type, context)
        
        messages = [{"role": "system", "content": system_prompt}]
        messages.extend(conversation_history)
        messages.append({"role": "user", "content": message})
        
        logger.info(f"SENDING TO AI - System prompt length: {len(system_prompt)} chars")
        logger.info(f"CONVERSATION HISTORY: {len(conversation_history)} messages")
        
        try:
            response = await self.openai_client.chat.completions.create(
                model="gpt-4o",
                messages=messages,
                temperature=0.7,
                max_tokens=1500
            )
            
            ai_response = response.choices[0].message.content
            logger.info(f"AI RESPONSE GENERATED - Length: {len(ai_response)} chars")
            return ai_response
            
        except Exception as e:
            logger.error(f"ERROR GENERATING AI RESPONSE: {e}")
            return "I apologize, but I'm having trouble analyzing your data right now. Please try again."
        
    async def process_chat_message(
        self,
        chat_request: ChatRequest,
        user_email: str
    ) -> ChatResponse:
        """Process chat message with intelligent agentic workflow"""
        
        logger.info(f"🚀 Processing chat message for user: {user_email}")
        logger.info(f"💬 Message: '{chat_request.message}'")
        logger.info(f"📱 Module: {chat_request.module_type.value}")
        
        # Status 1: Message received
        await self.send_status_update("Message received", "Processing your question...")
        
        # Handle session continuation properly
        if chat_request.session_id:
            session_id = chat_request.session_id
            await self.db.chat_sessions.update_one(
                {"session_id": session_id},
                {"$set": {"last_activity": datetime.utcnow()}}
            )
        else:
            # Extract account_id and page_id from context for session creation
            account_id = chat_request.context.get('account_id') if chat_request.context else None
            page_id = chat_request.context.get('page_id') if chat_request.context else None
            
            session_id = await self.create_or_get_simple_session(
                user_email=user_email,
                module_type=chat_request.module_type,
                session_id=None,
                customer_id=chat_request.customer_id,
                property_id=chat_request.property_id,
                account_id=account_id,  # Add this
                page_id=page_id,  # Add this
                period=chat_request.period or "LAST_7_DAYS"
            )
        
        # Add user message to session
        user_message = ChatMessage(
            role=MessageRole.USER,
            content=chat_request.message,
            timestamp=datetime.utcnow()
        )
        await self.add_message_to_simple_session(session_id, user_message)
        
        # Status 2: Agent working
        await self.send_status_update("Agent analyzing", "AI agent is examining your question...")
        
        # Intelligent collection selection
        selected_collections = await self.select_relevant_collections(
            user_message=chat_request.message,
            module_type=chat_request.module_type
        )
        
        await self.send_status_update("Collections identified", f"Found {len(selected_collections)} relevant data sources")
        
        # Build search criteria with intelligent filtering
        search_criteria = {
            "user_email": user_email,
            "customer_id": chat_request.customer_id,
            "property_id": chat_request.property_id,
            "account_id": chat_request.context.get('account_id') if chat_request.context else None,  # Add this
            "page_id": chat_request.context.get('page_id') if chat_request.context else None,  # Add this
            "period": chat_request.period or "LAST_7_DAYS",
            "selected_collections": selected_collections
        }
        
        await self.send_status_update("Searching data", "Looking for existing data in your account...")
        search_results = await self.search_documents_in_collections(search_criteria)
        
        # Handle missing data and trigger endpoints intelligently
        missing_collections = [col for col, result in search_results.items() if not result.get("found", False)]
        if missing_collections:
            await self.send_status_update("Triggering endpoints", f"Fetching fresh data from {len(missing_collections)} APIs...")
            await self.trigger_missing_endpoints(user_email, missing_collections, search_criteria)
            await asyncio.sleep(2)
            search_results = await self.search_documents_in_collections(search_criteria)
        
        await self.send_status_update("Analyzing data", "Processing and analyzing your marketing data...")
        context = await self.build_context_from_search_results(search_results, selected_collections)
        
        await self.send_status_update("Finalizing response", "AI is preparing your insights...")
        
        # Generate AI response with comprehensive context
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
        
        await self.send_status_update("Complete", "Analysis ready!")
        
        return ChatResponse(
            response=ai_response,
            session_id=session_id,
            triggered_endpoint=None,
            endpoint_data=None,
            module_type=chat_request.module_type
        )

    async def build_context_from_search_results(
        self,
        search_results: Dict[str, Any],
        selected_collections: List[str]
    ) -> Dict[str, Any]:
        """Build context from search results"""
        context = {
            "selected_collections": selected_collections,
            "data_summary": {},
            "full_data": {}
        }
        
        for collection_name, result in search_results.items():
            if result.get("found", False):
                context["full_data"][collection_name] = [{
                    "timestamp": result.get("last_updated"),
                    "request_params": result.get("request_params", {}),
                    "response_data": result.get("data", {})
                }]
                
                context["data_summary"][collection_name] = {
                    "total_documents": 1,
                    "data_entries": 1,
                    "latest_update": result.get("last_updated"),
                    "description": self._get_collection_description(collection_name)
                }
        
        return context

    def _get_enhanced_system_prompt_v2(self, module_type: ModuleType, context: Dict[str, Any]) -> str:
        """Enhanced system prompt with complete data extraction and intelligent analysis"""
        
        base_prompt = """You are an expert marketing analytics assistant with access to real marketing data. 
        
        Your capabilities:
        - Analyze Google Ads performance data (campaigns, keywords, costs, conversions)
        - Interpret Google Analytics data (traffic, user behavior, conversions, revenue)
        - Provide keyword insights and search volume analysis
        - Analyze Meta Ads campaigns (Facebook & Instagram advertising)
        - Interpret Facebook Page performance (engagement, reach, demographics)
        - Give actionable recommendations based on data trends
        
        Guidelines:
        - Use actual numbers and data from the provided information
        - Highlight key insights and trends you can see in the data
        - Provide specific, actionable recommendations
        - Format numbers clearly (use commas, percentages, currency symbols)
        - If you see concerning trends, mention them with specific data points
        - Be conversational but data-driven
        - Always provide context for your recommendations
        - Suggest specific optimization actions when possible
        """
        
        # Add module-specific context
        module_context = {
            ModuleType.GOOGLE_ADS: "Focus on ad spend efficiency, campaign performance, keyword opportunities, and conversion optimization.",
            ModuleType.GOOGLE_ANALYTICS: "Focus on traffic patterns, user engagement, conversion funnels, and revenue attribution.",
            ModuleType.INTENT_INSIGHTS: "Focus on search trends, market demand, keyword opportunities, and competitive analysis.",
            ModuleType.META_ADS: "Focus on Meta advertising performance (Facebook & Instagram), ROAS, audience targeting, creative performance, and ad placement optimization.",
            ModuleType.FACEBOOK_ANALYTICS: "Focus on Facebook page growth, post engagement, audience demographics, content performance, and organic vs paid reach."
        }
        
        # Extract the full nested data
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
                    
                    # Extract all the nested data properly
                    if isinstance(response_data, dict):
                        # Handle key stats format (nested dicts with value/formatted keys)
                        for key, value in response_data.items():
                            if isinstance(value, dict):
                                if 'value' in value and 'formatted' in value:
                                    # This is a metric with formatted display
                                    data_prompt += f"  {key}: {value['formatted']} (raw: {value['value']})\n"
                                elif 'label' in value:
                                    # This is a labeled metric
                                    data_prompt += f"  {value.get('label', key)}: {value.get('formatted', value.get('value', 'N/A'))}\n"
                                else:
                                    # Regular dict - show key contents
                                    data_prompt += f"  {key}: {value}\n"
                            elif isinstance(value, (int, float)):
                                data_prompt += f"  {key}: {value:,}\n"
                            elif isinstance(value, str):
                                data_prompt += f"  {key}: {value}\n"
                            elif isinstance(value, list):
                                data_prompt += f"  {key}: List with {len(value)} items\n"
                                # Show sample items if they're simple
                                for j, item in enumerate(value[:3]):
                                    if isinstance(item, dict):
                                        data_prompt += f"    Item {j+1}: {item}\n"
                                    else:
                                        data_prompt += f"    Item {j+1}: {item}\n"
                    
                    elif isinstance(response_data, list):
                        data_prompt += f"  Contains {len(response_data)} data points:\n"
                        # Show all items for time series data
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
        
        final_prompt = base_prompt + context_prompt
        
        # Log more details about what's being sent
        logger.info(f"SYSTEM PROMPT LENGTH: {len(final_prompt)} characters")
        logger.info(f"DATA COLLECTIONS IN PROMPT: {list(full_data.keys())}")
        
        return final_prompt

    def _normalize_period(self, period: str) -> str:
        """Normalize different period formats to a standard format"""
        period_mapping = {
            # Frontend formats
            'LAST_7_DAYS': '7d',
            'LAST_30_DAYS': '30d', 
            'LAST_90_DAYS': '90d',
            'LAST_365_DAYS': '365d',
            'LAST_3_MONTHS': '90d',
            'LAST_1_YEAR': '365d',
            
            # Analytics formats
            '7d': '7d',
            '30d': '30d',
            '90d': '90d', 
            '365d': '365d',
            '12m': '365d'
        }
        return period_mapping.get(period, period)

    async def search_documents_in_collections(
        self,
        search_criteria: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Search for documents across selected collections with intelligent filtering"""
        results = {}
        
        for collection_name in search_criteria['selected_collections']:
            try:
                collection = self.db[collection_name]
                
                # Build search query with intelligent filtering
                query = {"user_email": search_criteria['user_email']}
                
                # Add appropriate ID filters based on collection type
                if search_criteria.get('customer_id'):
                    query["customer_id"] = search_criteria['customer_id']
                if search_criteria.get('property_id'): 
                    query["property_id"] = search_criteria['property_id']
                if search_criteria.get('account_id'):  # Add this
                    query["account_id"] = search_criteria['account_id']
                if search_criteria.get('page_id'):  # Add this
                    query["page_id"] = search_criteria['page_id']
                
                # Search for documents with normalized period
                normalized_period = self._normalize_period(search_criteria['period'])
                
                # Try multiple period formats in request_params
                period_queries = [
                    {**query, "request_params.period": search_criteria['period']},
                    {**query, "request_params.period": normalized_period},
                ]
                
                document = None
                for period_query in period_queries:
                    document = await collection.find_one(period_query)
                    if document:
                        break
                
                if document:
                    results[collection_name] = {
                        "found": True,
                        "data": document.get("response_data", {}),
                        "last_updated": document.get("last_updated"),
                        "request_params": document.get("request_params", {})
                    }
                    logger.info(f"Found data in {collection_name}")
                else:
                    results[collection_name] = {
                        "found": False,
                        "needs_api_call": True
                    }
                    logger.info(f"No data found in {collection_name}, will need API call")
                    
            except Exception as e:
                logger.error(f"Error searching in {collection_name}: {e}")
                results[collection_name] = {
                    "found": False,
                    "error": str(e)
                }
        
        return results
    
    async def trigger_missing_endpoints(
        self,
        user_email: str,
        missing_collections: List[str],
        search_criteria: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Trigger API endpoints for missing data with intelligent endpoint mapping"""
        
        # Collection to endpoint mapping
        endpoint_mapping = {
            # Google Ads endpoints
            'google_ads_key_stats': {
                'endpoint': 'ads_key_stats',
                'method': 'GET',
                'url_template': '/api/ads/key-stats/{customer_id}',
                'requires': ['customer_id']
            },
            'google_ads_campaigns': {
                'endpoint': 'ads_campaigns', 
                'method': 'GET',
                'url_template': '/api/ads/campaigns/{customer_id}',
                'requires': ['customer_id']
            },
            'google_ads_keywords_related_to_campaign': {
                'endpoint': 'ads_keywords',
                'method': 'GET', 
                'url_template': '/api/ads/keywords/{customer_id}',
                'requires': ['customer_id']
            },
            'google_ads_performance': {
                'endpoint': 'ads_performance',
                'method': 'GET',
                'url_template': '/api/ads/performance/{customer_id}',
                'requires': ['customer_id']
            },
            'google_ads_geographic_performance': {
                'endpoint': 'ads_geographic_performance',
                'method': 'GET',
                'url_template': '/api/ads/geographic/{customer_id}', 
                'requires': ['customer_id']
            },
            'google_ads_device_performance': {
                'endpoint': 'ads_device_performance',
                'method': 'GET',
                'url_template': '/api/ads/device-performance/{customer_id}',
                'requires': ['customer_id']
            },
            'google_ads_time_performance': {
                'endpoint': 'ads_time_performance', 
                'method': 'GET',
                'url_template': '/api/ads/time-performance/{customer_id}',
                'requires': ['customer_id']
            },
            
            # Google Analytics endpoints
            'google_analytics_metrics': {
                'endpoint': 'ga_metrics',
                'method': 'GET',
                'url_template': '/api/analytics/metrics/{property_id}',
                'requires': ['property_id']
            },
            'google_analytics_conversions': {
                'endpoint': 'ga_conversions',
                'method': 'GET', 
                'url_template': '/api/analytics/conversions/{property_id}',
                'requires': ['property_id']
            },
            'google_analytics_traffic_sources': {
                'endpoint': 'ga_traffic_sources',
                'method': 'GET',
                'url_template': '/api/analytics/traffic-sources/{property_id}',
                'requires': ['property_id']
            },
            'google_analytics_top_pages': {
                'endpoint': 'ga_top_pages',
                'method': 'GET',
                'url_template': '/api/analytics/top-pages/{property_id}',
                'requires': ['property_id']
            },
            'google_analytics_channel_performance': {
                'endpoint': 'ga_channel_performance',
                'method': 'GET',
                'url_template': '/api/analytics/channel-performance/{property_id}',
                'requires': ['property_id']
            },
            'ga_revenue_breakdown_by_channel': {
                'endpoint': 'ga_revenue_breakdown_by_channel',
                'method': 'GET',
                'url_template': '/api/analytics/revenue-breakdown/channel/{property_id}',
                'requires': ['property_id']
            },
            'ga_revenue_breakdown_by_source': {
                'endpoint': 'ga_revenue_breakdown_by_source', 
                'method': 'GET',
                'url_template': '/api/analytics/revenue-breakdown/source/{property_id}',
                'requires': ['property_id']
            }
        }
        
        triggered_results = {}
        
        for collection_name in missing_collections:
            if collection_name not in endpoint_mapping:
                logger.warning(f"No endpoint mapping found for collection: {collection_name}")
                continue
                
            endpoint_config = endpoint_mapping[collection_name]
            
            # Check if we have required parameters
            missing_params = []
            for required_param in endpoint_config['requires']:
                if not search_criteria.get(required_param):
                    missing_params.append(required_param)
                    
            if missing_params:
                logger.warning(f"Missing required parameters {missing_params} for {collection_name}")
                triggered_results[collection_name] = {
                    "triggered": False,
                    "error": f"Missing required parameters: {missing_params}"
                }
                continue
            
            try:
                # Import the required managers based on endpoint type
                if 'ads' in collection_name:
                    from google_ads.ads_manager import GoogleAdsManager
                    auth_manager_instance = AuthManager()
                    ads_manager = GoogleAdsManager(user_email, auth_manager_instance)
                
                    # Call the appropriate method based on endpoint
                    if endpoint_config['endpoint'] == 'ads_key_stats':
                        result = ads_manager.get_overall_key_stats(
                            search_criteria['customer_id'], 
                            search_criteria['period']
                        )
                    elif endpoint_config['endpoint'] == 'ads_campaigns':
                        result = ads_manager.get_campaigns_with_period(
                            search_criteria['customer_id'],
                            search_criteria['period'] 
                        )
                    # Add more endpoint calls as needed...
                    
                elif 'analytics' in collection_name or 'ga_' in collection_name:
                    from google_analytics.ga4_manager import GA4Manager
                    ga4_manager = GA4Manager(user_email)
                    
                    # Convert period format for analytics
                    analytics_period = self._convert_period_for_analytics(search_criteria['period'])
                    
                    if endpoint_config['endpoint'] == 'ga_metrics':
                        result = ga4_manager.get_metrics(
                            search_criteria['property_id'],
                            analytics_period
                        )
                    elif endpoint_config['endpoint'] == 'ga_conversions':
                        result = ga4_manager.get_conversions(
                            search_criteria['property_id'],
                            analytics_period
                        )
                    # Add more endpoint calls as needed...
                
                triggered_results[collection_name] = {
                    "triggered": True,
                    "data": result
                }
                logger.info(f"Successfully triggered endpoint for {collection_name}")
                
            except Exception as e:
                logger.error(f"Error triggering endpoint for {collection_name}: {e}")
                triggered_results[collection_name] = {
                    "triggered": False,
                    "error": str(e)
                }
        
        return triggered_results

    def _convert_period_for_analytics(self, period: str) -> str:
        """Convert period format for analytics API"""
        conversion_map = {
            'LAST_7_DAYS': '7d',
            'LAST_30_DAYS': '30d', 
            'LAST_90_DAYS': '90d',
            'LAST_365_DAYS': '365d',
            '7d': '7d',
            '30d': '30d',
            '90d': '90d', 
            '365d': '365d'
        }
        return conversion_map.get(period, '30d')

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

    async def get_conversation_by_session_id(
        self,
        user_email: str,
        session_id: str,
        module_type: ModuleType
    ) -> Optional[Dict[str, Any]]:
        """Get specific conversation by session ID"""
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