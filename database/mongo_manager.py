import motor.motor_asyncio
import os
from datetime import datetime
from typing import Dict, Any, Optional, List
import logging
from pydantic import BaseModel
from datetime import timedelta

logger = logging.getLogger(__name__)

class MongoManager:
    def __init__(self):
        try:
            self.connection_string = os.getenv('MONGODB_CONNECTION_STRING')
            if not self.connection_string:
                raise ValueError("MONGODB_CONNECTION_STRING not found in environment variables")
            
            self.client = motor.motor_asyncio.AsyncIOMotorClient(
                self.connection_string,
                serverSelectionTimeoutMS=5000  # 5 second timeout
            )
            # Verify connection
            self.client.server_info()
            self.db = self.client.internal_dashboard
        except Exception as e:
            raise ConnectionError(f"Failed to connect to MongoDB: {str(e)}")
        
    def _serialize_response_data(self, data: Any) -> Any:
        """Convert Pydantic models and other objects to MongoDB-compatible format"""
        if isinstance(data, BaseModel):
            return data.dict()
        elif isinstance(data, list):
            return [self._serialize_response_data(item) for item in data]
        elif isinstance(data, dict):
            return {key: self._serialize_response_data(value) for key, value in data.items()}
        else:
            return data
        
    def _serialize_request_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Serialize request parameters, handling Pydantic models"""
        serialized = {}
        for key, value in params.items():
            if isinstance(value, BaseModel):
                serialized[key] = value.dict()
            elif isinstance(value, list):
                serialized[key] = [item.dict() if isinstance(item, BaseModel) else item for item in value]
            elif isinstance(value, dict):
                serialized[key] = self._serialize_request_params(value)
            else:
                serialized[key] = value
        return serialized

    async def save_endpoint_response(
        self, 
        endpoint: str, 
        user_email: str, 
        request_params: Dict[str, Any], 
        response_data: Any,
        customer_id: Optional[str] = None,
        property_id: Optional[str] = None,
        account_id: Optional[str] = None,  # Add this
        page_id: Optional[str] = None  # Add this
    ):
        """Save or update endpoint response in MongoDB based on key attributes"""
        try:
            collection_name = self._get_collection_name(endpoint, request_params)
            collection = self.db[collection_name]
            
            # Serialize both request params and response data
            serialized_request_params = self._serialize_request_params(request_params)
            serialized_data = self._serialize_response_data(response_data)
            
            # Create query filter based on the key attributes
            query_filter = {
                "endpoint": endpoint,
                "user_email": user_email,
                "customer_id": customer_id,
                "property_id": property_id,
                "account_id": account_id,  # Add this
                "page_id": page_id,  # Add this
                "request_params": serialized_request_params
            }
            
            existing_doc = await collection.find_one(query_filter)
            
            if existing_doc:
                # Update existing document
                update_data = {
                    "$set": {
                        "response_data": serialized_data,
                        "data_count": self._get_data_count(serialized_data),
                        "last_updated": datetime.utcnow(),
                        "update_count": existing_doc.get("update_count", 0) + 1
                    }
                }
                
                result = await collection.update_one(query_filter, update_data)
                logger.info(f"Updated existing document for endpoint {endpoint} in collection {collection_name}, matched: {result.matched_count}")
                return existing_doc["_id"]
            else:
                # Create new document
                document = {
                    "endpoint": endpoint,
                    "user_email": user_email,
                    "customer_id": customer_id,
                    "property_id": property_id,
                    "account_id": account_id,  # Add this
                    "page_id": page_id,  # Add this
                    "request_params": serialized_request_params,
                    "response_data": serialized_data,
                    "data_count": self._get_data_count(serialized_data),
                    "timestamp": datetime.utcnow(),
                    "created_at": datetime.utcnow(),
                    "last_updated": datetime.utcnow(),
                    "update_count": 0
                }
                
                result = await collection.insert_one(document)
                logger.info(f"Created new document for endpoint {endpoint} in collection {collection_name}, document ID: {result.inserted_id}")
                return result.inserted_id
            
        except Exception as e:
            logger.error(f"Error saving/updating MongoDB document: {e}")
            return None
    
    async def save_chat_session(
        self,
        session_id: str,
        user_email: str,
        module_type: str,
        user_message: str,
        assistant_message: str,
        customer_id: Optional[str] = None,
        property_id: Optional[str] = None,
        account_id: Optional[str] = None,
        page_id: Optional[str] = None,
        triggered_endpoints: List[Dict[str, Any]] = None,
        visualizations: Optional[Dict[str, Any]] = None
    ):
        """Save or update a chat session in MongoDB"""
        try:
            # Get the collection name - this will handle both formats
            collection_name = self._get_chat_collection_name(module_type)
            logger.info(f"💾 Saving chat session to collection: {collection_name}")
            logger.info(f"   Session ID: {session_id}")
            logger.info(f"   User: {user_email}")
            logger.info(f"   Module: {module_type}")
            logger.info(f"   User message length: {len(user_message) if user_message else 0}")
            logger.info(f"   Assistant message length: {len(assistant_message) if assistant_message else 0}")

            collection = self.db[collection_name]

            current_time = datetime.utcnow()

            # Check if session exists
            existing_session = await collection.find_one({"session_id": session_id})

            if existing_session:
                logger.info(f"📝 Found existing session {session_id}, appending messages")
                # Update existing session - append messages
                result = await collection.update_one(
                    {"session_id": session_id},
                    {
                        "$push": {
                            "messages": {
                                "$each": [
                                    {
                                        "role": "user",
                                        "content": user_message,
                                        "timestamp": current_time
                                    },
                                    {
                                        "role": "assistant",
                                        "content": assistant_message,
                                        "timestamp": current_time
                                    }
                                ]
                            }
                        },
                        "$set": {
                            "last_activity": current_time
                        }
                    }
                )
                logger.info(f"✅ Updated existing chat session: {session_id} (matched: {result.matched_count}, modified: {result.modified_count})")
            else:
                logger.info(f"🆕 Creating new session {session_id}")
                # Create new session
                session_doc = {
                    "session_id": session_id,
                    "user_email": user_email,
                    "module_type": module_type,
                    "customer_id": customer_id,
                    "property_id": property_id,
                    "account_id": account_id,
                    "page_id": page_id,
                    "created_at": current_time,
                    "last_activity": current_time,
                    "is_active": True,
                    "messages": [
                        {
                            "role": "user",
                            "content": user_message,
                            "timestamp": current_time
                        },
                        {
                            "role": "assistant",
                            "content": assistant_message,
                            "timestamp": current_time
                        }
                    ]
                }

                logger.info(f"📄 Session document structure: session_id={session_id}, user_email={user_email}, is_active=True, messages_count=2")
                result = await collection.insert_one(session_doc)
                logger.info(f"✅ Created new chat session: {session_id} (inserted_id: {result.inserted_id})")

                # Verify it was saved correctly
                verification = await collection.find_one({"session_id": session_id})
                if verification:
                    logger.info(f"✅ Verification: Session {session_id} exists in DB with {len(verification.get('messages', []))} messages")
                else:
                    logger.error(f"❌ Verification failed: Session {session_id} not found after insert!")

        except Exception as e:
            logger.error(f"❌ Error saving chat session: {e}", exc_info=True)
            raise
    
    def _get_collection_name(self, endpoint: str, request_params: Dict[str, Any] = None) -> str:
        """Get meaningful collection name based on endpoint and optional request parameters"""
        collection_mapping = {
            # Google Ads endpoints
            'ads_customers': 'google_ads_customers_accounts',
            'ads_key_stats': 'google_ads_key_stats',
            'ads_campaigns': 'google_ads_campaigns',
            'ads_keywords': 'google_ads_keywords_related_to_campaign',
            'ads_performance': 'google_ads_performance',
            'ads_geographic_performance': 'google_ads_geographic_performance',
            'ads_device_performance': 'google_ads_device_performance',
            'ads_time_performance': 'google_ads_time_performance',
            'ads_keyword_ideas': 'google_ads_keyword_ideas',
            
            # Google Analytics endpoints
            'ga_properties': 'google_analytics_properties',
            'ga_metrics': 'google_analytics_metrics',
            'ga_conversions': 'google_analytics_conversions',
            'ga_traffic_sources': 'google_analytics_traffic_sources',
            'ga_top_pages': 'google_analytics_top_pages',
            'ga_channel_performance': 'google_analytics_channel_performance',
            'ga_time_series': 'google_analytics_time_series',
            'ga_trends': 'google_analytics_trends',
            'ga_roas_roi_time_series': 'google_analytics_roas_roi_time_series',
            'ga_funnel_data': 'ga_funnel_data',

            # New mappings for Meta endpoints
            'meta_account_insights_summary': 'meta_account_insights_summary',
            'meta_campaigns_paginated': 'meta_campaigns_paginated',
            'meta_campaigns_list': 'meta_campaigns_list',
            'meta_campaigns_timeseries': 'meta_campaigns_timeseries',
            'meta_campaigns_demographics': 'meta_campaigns_demographics',
            'meta_campaigns_placements': 'meta_campaigns_placements',
            'meta_adsets': 'meta_adsets',
            'meta_adsets_timeseries': 'meta_adsets_timeseries',
            'meta_adsets_demographics': 'meta_adsets_demographics',
            'meta_adsets_placements': 'meta_adsets_placements',
            'meta_ads': 'meta_ads',
            'meta_ads_timeseries': 'meta_ads_timeseries',
            'meta_ads_demographics': 'meta_ads_demographics',
            'meta_ads_placements': 'meta_ads_placements',
            
            # Combined endpoints
            'combined_overview': 'ads_ga_combined_overview_metrics',
            'combined_roas_roi_metrics': 'ga_combined_roas_roi_metrics',
            'combined_roas_roi_metrics_legacy': 'ga_combined_roas_roi_metrics_legacy',

            # Revenue breakdown endpoints
            'ga_revenue_breakdown_by_channel': 'ga_revenue_breakdown_by_channel',
            'ga_revenue_breakdown_by_source': 'ga_revenue_breakdown_by_source',
            'ga_revenue_breakdown_by_device': 'ga_revenue_breakdown_by_device',
            'ga_revenue_breakdown_by_location': 'ga_revenue_breakdown_by_location',
            'ga_revenue_breakdown_by_page': 'ga_revenue_breakdown_by_page',
            'ga_revenue_breakdown_by_comprehensive': 'ga_revenue_breakdown_by_comprehensive',
            
            'ga_available_channels': 'ga_available_channels',
            
            # Channel revenue time series
            'ga_specific_channels_time_series': 'ga_specific_channels_time_series',
            
            # Intent insights
            'intent_keyword_insights_raw': 'intent_keyword_insights',

            # Meta Ads endpoints
            'meta_ad_accounts': 'meta_ad_accounts',
            'meta_key_stats': 'meta_key_stats', 
            'meta_campaigns': 'meta_campaigns',
            'meta_placement_performance': 'meta_placement_performance',
            'meta_demographic_performance': 'meta_demographic_performance',
            'meta_time_series': 'meta_time_series',

            # Facebook Pages endpoints
            'meta_pages': 'facebook_pages',
            'meta_page_insights': 'facebook_page_insights',
            'meta_page_insights_timeseries': 'facebook_page_insights_timeseries',
            'meta_page_posts': 'facebook_page_posts',
            'meta_page_posts_timeseries': 'facebook_page_posts_timeseries',
            'meta_video_views_breakdown': 'facebook_video_views_breakdown',
            'meta_content_type_breakdown': 'facebook_content_type_breakdown',
            'meta_page_demographics': 'facebook_page_demographics',
            'meta_follows_unfollows': 'facebook_follows_unfollows',
            'meta_engagement_breakdown': 'facebook_engagement_breakdown',
            'meta_organic_vs_paid': 'facebook_organic_vs_paid',
            'facebook_post_insights': 'facebook_post_insights',
            'facebook_audience_insights': 'facebook_audience_insights',
            'facebook_performance_summary': 'facebook_performance_summary',

            # Instagram endpoints
            
            'meta_instagram_accounts': 'instagram_accounts',
            'meta_instagram_insights': 'instagram_account_insights',
            'meta_instagram_insights_timeseries': 'instagram_insights_timeseries',
            'meta_instagram_media': 'instagram_account_media',
            'meta_instagram_media_timeseries': 'instagram_media_timeseries',
            'instagram_stories': 'instagram_stories',
            'instagram_audience_demographics': 'instagram_audience_demographics',
            'instagram_hashtag_performance': 'instagram_hashtag_performance',
            'instagram_performance_summary': 'instagram_performance_summary',

            # Combined social media endpoints
            'social_media_overview': 'social_media_overview',
            'social_insights_summary': 'social_insights_summary'
        }

        # Special handling for ga_audience_insights endpoint with dimension-based collections
        if endpoint == 'ga_audience_insights' and request_params and 'dimension' in request_params:
            dimension = request_params['dimension']
            return f'ga_audience_insights_{dimension}'

        # Special handling for revenue-timeseries endpoint
        if endpoint == 'ga_revenue_time_series' and request_params and 'breakdown_by' in request_params:
            breakdown_by = request_params['breakdown_by']
            collection_map = {
                'channel': 'ga_revenue_time_series_by_channel',
                'device': 'ga_revenue_time_series_by_device',
                'location': 'ga_revenue_time_series_by_location',
                'source': 'ga_revenue_time_series_by_source'  # Note: 'session' was likely meant to be 'source'
            }
            return collection_map.get(breakdown_by, 'ga_revenue_time_series_by_channel')  # Default to channel if invalid

        return collection_mapping.get(endpoint, 'api_responses_misc')
    
    def _get_data_count(self, data: Any) -> int:
        """Get count of items in response data"""
        if isinstance(data, list):
            return len(data)
        elif isinstance(data, dict):
            # For objects with arrays, try to find the main data array
            for key in ['campaigns', 'keywords', 'conversions', 'channels', 'pages', 'sources', 'time_series', 'breakdown', 'groups']:
                if key in data and isinstance(data[key], list):
                    return len(data[key])
            return 1
        else:
            return 1 if data is not None else 0

    def _get_chat_collection_name(self, module_type: str) -> str:
        """
        Get the correct chat collection name, handling both formats:
        - New format: "google_ads" → "chat_google_ads"
        - Old format: "ModuleType.GOOGLE_ADS" → "chat_ModuleType.GOOGLE_ADS"

        Returns the collection name that exists in the database
        """
        # Map lowercase module types to their enum equivalents (which are used in existing collections)
        module_type_map = {
            "google_ads": "ModuleType.GOOGLE_ADS",
            "google_analytics": "ModuleType.GOOGLE_ANALYTICS",
            "meta_ads": "ModuleType.META_ADS",
            "facebook_analytics": "ModuleType.FACEBOOK",
            "instagram_analytics": "ModuleType.INSTAGRAM",
            "intent_insights": "ModuleType.INTENT_INSIGHTS"
        }

        # If module_type is in lowercase format, convert to enum format for backward compatibility
        if module_type and module_type.lower() in module_type_map:
            enum_format = module_type_map[module_type.lower()]
            logger.info(f"🔄 Converted module type: {module_type} → {enum_format}")
            return f"chat_{enum_format}"

        # Otherwise, use as-is (already in enum format)
        return f"chat_{module_type}"
    
    async def get_cached_response(
        self,
        endpoint: str,
        user_email: str,
        request_params: Dict[str, Any],
        customer_id: Optional[str] = None,
        property_id: Optional[str] = None,
        max_age_minutes: int = 30
    ) -> Optional[Dict[str, Any]]:
        """Get cached response if it exists and is recent enough"""
        try:
            collection_name = self._get_collection_name(endpoint, request_params)
            collection = self.db[collection_name]
            
            query_filter = {
                "endpoint": endpoint,
                "user_email": user_email,
                "customer_id": customer_id,
                "property_id": property_id,
                "request_params": request_params
            }
            
            # Add time filter for recent data
            cutoff_time = datetime.utcnow() - timedelta(minutes=max_age_minutes)
            query_filter["last_updated"] = {"$gte": cutoff_time}
            
            cached_doc = await collection.find_one(query_filter)
            
            if cached_doc:
                logger.info(f"Found cached response for endpoint {endpoint}, last updated: {cached_doc['last_updated']}")
                return cached_doc.get("response_data")
            
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving cached response: {e}")
            return None

# Create singleton instance
mongo_manager = MongoManager()