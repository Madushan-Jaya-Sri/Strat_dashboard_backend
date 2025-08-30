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
        self.connection_string = os.getenv('MONGODB_CONNECTION_STRING')
        if not self.connection_string:
            raise ValueError("MONGODB_CONNECTION_STRING not found in environment variables")
        
        self.client = motor.motor_asyncio.AsyncIOMotorClient(self.connection_string)
        self.db = self.client.internal_dashboard
        
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
    
    async def save_endpoint_response(
        self, 
        endpoint: str, 
        user_email: str, 
        request_params: Dict[str, Any], 
        response_data: Any,
        customer_id: Optional[str] = None,
        property_id: Optional[str] = None
    ):
        """Save or update endpoint response in MongoDB based on key attributes"""
        try:
            collection_name = self._get_collection_name(endpoint)
            collection = self.db[collection_name]
            
            # Serialize response data
            serialized_data = self._serialize_response_data(response_data)
            
            # Extract period from request_params if it exists
            period = request_params.get('period')
            
            # Create query filter based on the key attributes
            query_filter = {
                "endpoint": endpoint,
                "user_email": user_email,
                "customer_id": customer_id,
                "property_id": property_id,
                "request_params": request_params
            }
            
            # If period exists, also include it in the filter
            if period:
                query_filter["request_params.period"] = period
            
            # Check if document with same key attributes exists
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
                    "request_params": request_params,
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
    
    def _get_collection_name(self, endpoint: str) -> str:
        """Get meaningful collection name based on endpoint"""
        collection_mapping = {
            # Google Ads endpoints
            'ads_key_stats': 'google_ads_key_stats',
            'ads_campaigns': 'google_ads_campaigns',
            'ads_keywords': 'google_ads_keywords',
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
            'ga_audience_insights': 'google_analytics_audience_insights',
            'ga_time_series': 'google_analytics_time_series',
            'ga_trends': 'google_analytics_trends',
            'ga_roas_roi_time_series': 'google_analytics_roas_roi_time_series',
            

            

            # Combined endpoints
            'combined_overview': 'ads_ga_combined_overview_metrics',
            'combined_roas_roi_metrics': 'combined_roas_roi_metrics',
            'combined_roas_roi_metrics_legacy': 'combined_roas_roi_metrics_legacy',
            'combined_enhanced_roas_roi': 'combined_enhanced_roas_roi_metrics',

            # Revenue breakdown endpoints

            'ga_revenue_breakdown_by_channel': 'ga_revenue_breakdown_by_channel',
            'ga_revenue_breakdown_by_source': 'ga_revenue_breakdown_by_source',
            'ga_revenue_breakdown_by_device': 'ga_revenue_breakdown_by_device',
            'ga_revenue_breakdown_by_location': 'ga_revenue_breakdown_by_location',
            'ga_revenue_breakdown_by_page': 'ga_revenue_breakdown_by_page',
            'ga_revenue_breakdown_by_comprehensive': 'ga_revenue_breakdown_by_comprehensive',
            


            'ga_available_channels': 'ga_available_channels',
            
            # Channel revenue time series

            'ga_channel_revenue_time_series': 'ga_channel_revenue_time_series',
            'ga_specific_channels_time_series': 'ga_specific_channels_time_series',

            # Intent insights
            'intent_keyword_insights_raw': 'intent_keyword_insights',
        }
        
        return collection_mapping.get(endpoint, 'api_responses_misc')
    
    def _get_data_count(self, data: Any) -> int:
        """Get count of items in response data"""
        if isinstance(data, list):
            return len(data)
        elif isinstance(data, dict):
            # For objects with arrays, try to find the main data array
            for key in ['campaigns', 'keywords', 'conversions', 'channels', 'pages', 'sources', 'time_series', 'breakdown']:
                if key in data and isinstance(data[key], list):
                    return len(data[key])
            return 1
        else:
            return 1 if data is not None else 0
    
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
            collection_name = self._get_collection_name(endpoint)
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