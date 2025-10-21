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
import aiohttp
from typing import Tuple
from models.chat_models import *
from database.mongo_manager import mongo_manager
from auth.auth_manager import AuthManager

logger = logging.getLogger(__name__)

class ChatManager:
    def __init__(self):
        self.openai_client = openai.AsyncOpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        self.db = mongo_manager.db
        
        # Endpoint mappings extracted from main.py
        self.endpoint_registry = {
            'google_ads': [
                {'name': 'get_ads_customers', 'path': '/api/ads/customers', 'params': []},
                {'name': 'get_ads_key_stats', 'path': '/api/ads/key-stats/{customer_id}', 'params': ['customer_id', 'period', 'start_date', 'end_date']},
                {'name': 'get_ads_campaigns', 'path': '/api/ads/campaigns/{customer_id}', 'params': ['customer_id', 'period', 'start_date', 'end_date']},
                {'name': 'get_ads_keywords', 'path': '/api/ads/keywords/{customer_id}', 'params': ['customer_id', 'period', 'start_date', 'end_date', 'offset', 'limit']},
                {'name': 'get_ads_performance', 'path': '/api/ads/performance/{customer_id}', 'params': ['customer_id', 'period', 'start_date', 'end_date']},
                {'name': 'get_ads_geographic', 'path': '/api/ads/geographic/{customer_id}', 'params': ['customer_id', 'period', 'start_date', 'end_date']},
                {'name': 'get_ads_device_performance', 'path': '/api/ads/device-performance/{customer_id}', 'params': ['customer_id', 'period', 'start_date', 'end_date']},
                {'name': 'get_ads_time_performance', 'path': '/api/ads/time-performance/{customer_id}', 'params': ['customer_id', 'period', 'start_date', 'end_date']},
                {'name': 'get_keyword_ideas', 'path': '/api/ads/keyword-ideas/{customer_id}', 'params': ['customer_id', 'keywords', 'location_id']},
            ],
            'google_analytics': [
                {'name': 'get_ga_properties', 'path': '/api/analytics/properties', 'params': []},
                {'name': 'get_ga_metrics', 'path': '/api/analytics/metrics/{property_id}', 'params': ['property_id', 'period', 'start_date', 'end_date']},
                {'name': 'get_ga_traffic_sources', 'path': '/api/analytics/traffic-sources/{property_id}', 'params': ['property_id', 'period', 'start_date', 'end_date']},
                {'name': 'get_ga_conversions', 'path': '/api/analytics/conversions/{property_id}', 'params': ['property_id', 'period', 'start_date', 'end_date']},
                {'name': 'get_ga_channel_performance', 'path': '/api/analytics/channel-performance/{property_id}', 'params': ['property_id', 'period', 'start_date', 'end_date']},
                {'name': 'get_ga_audience_insights', 'path': '/api/analytics/audience-insights/{property_id}', 'params': ['property_id', 'dimension', 'period', 'start_date', 'end_date']},
                {'name': 'get_ga_time_series', 'path': '/api/analytics/time-series/{property_id}', 'params': ['property_id', 'metric', 'period', 'start_date', 'end_date']},
                {'name': 'get_ga_revenue_breakdown', 'path': '/api/analytics/revenue-breakdown/channel/{property_id}', 'params': ['property_id', 'period', 'start_date', 'end_date']},
                {'name': 'get_ga_top_pages', 'path': '/api/analytics/top-pages/{property_id}', 'params': ['property_id', 'period']},
                {'name': 'generate_engagement_funnel_with_llm', 'path': '/api/analytics/funnel/{property_id}', 'params': ['property_id', 'selected_events', 'conversions_data', 'period', 'start_date', 'end_date']},
                {'name': 'get_ga_trends', 'path': '/api/analytics/trends/{property_id}', 'params': ['property_id', 'period']},
                {'name': 'get_ga_roas_roi_time_series', 'path': '/api/analytics/roas-roi-time-series/{property_id}', 'params': ['property_id', 'period']},
                {'name': 'get_combined_overview', 'path': '/api/combined/overview', 'params': ['ads_customer_id', 'ga_property_id', 'period']},
                {'name': 'get_enhanced_combined_roas_roi_metrics', 'path': '/api/combined/roas-roi-metrics', 'params': ['ga_property_id', 'ads_customer_ids', 'period', 'start_date', 'end_date']},
                {'name': 'get_combined_roas_roi_metrics_legacy', 'path': '/api/combined/roas-roi-metrics-legacy', 'params': ['ga_property_id', 'ads_customer_id', 'period']},
                {'name': 'get_revenue_breakdown_by_source', 'path': '/api/analytics/revenue-breakdown/source/{property_id}', 'params': ['property_id', 'limit', 'period', 'start_date', 'end_date']},
                {'name': 'get_revenue_breakdown_by_device', 'path': '/api/analytics/revenue-breakdown/device/{property_id}', 'params': ['property_id', 'period', 'start_date', 'end_date']},
                {'name': 'get_revenue_breakdown_by_location', 'path': '/api/analytics/revenue-breakdown/location/{property_id}', 'params': ['property_id', 'limit', 'period', 'start_date', 'end_date']},
                {'name': 'get_revenue_breakdown_by_page', 'path': '/api/analytics/revenue-breakdown/page/{property_id}', 'params': ['property_id', 'limit', 'period', 'start_date', 'end_date']},
                {'name': 'get_comprehensive_revenue_breakdown', 'path': '/api/analytics/revenue-breakdown/comprehensive/{property_id}', 'params': ['property_id', 'period', 'start_date', 'end_date']},
                {'name': 'get_revenue_breakdown_raw', 'path': '/api/analytics/revenue-breakdown/raw/{property_id}', 'params': ['property_id', 'breakdown_type', 'period', 'limit']},
                {'name': 'get_channel_revenue_time_series', 'path': '/api/analytics/channel-revenue-timeseries/{property_id}', 'params': ['property_id', 'period', 'start_date', 'end_date']},
                {'name': 'get_specific_channels_time_series', 'path': '/api/analytics/channel-revenue-timeseries/{property_id}/specific', 'params': ['property_id', 'channels', 'period']},
                {'name': 'get_available_channels', 'path': '/api/analytics/channel-revenue-timeseries/{property_id}/channels', 'params': ['property_id', 'period']},
                {'name': 'get_channel_revenue_time_series_raw', 'path': '/api/analytics/channel-revenue-timeseries/{property_id}/raw', 'params': ['property_id', 'period', 'channels']},
                {'name': 'get_revenue_time_series', 'path': '/api/analytics/revenue-timeseries/{property_id}', 'params': ['property_id', 'breakdown_by', 'period', 'start_date', 'end_date']},
            ],
            'meta_ads': [
                {'name': 'get_meta_ad_accounts', 'path': '/api/meta/ad-accounts', 'params': []},
                {'name': 'get_meta_account_insights', 'path': '/api/meta/ad-accounts/{account_id}/insights/summary', 'params': ['account_id', 'period', 'start_date', 'end_date']},
                {'name': 'get_meta_campaigns_all', 'path': '/api/meta/ad-accounts/{account_id}/campaigns/all', 'params': ['account_id', 'period', 'start_date', 'end_date']},
                {'name': 'get_meta_campaigns_list', 'path': '/api/meta/ad-accounts/{account_id}/campaigns/list', 'params': ['account_id', 'status']},
                {'name': 'get_meta_campaigns_timeseries', 'path': '/api/meta/campaigns/timeseries', 'params': ['campaign_ids', 'period', 'start_date', 'end_date']},
                {'name': 'get_meta_adsets', 'path': '/api/meta/campaigns/adsets', 'params': ['campaign_ids', 'period', 'start_date', 'end_date']},
                {'name': 'get_campaigns_paginated', 'path': '/api/meta/ad-accounts/{account_id}/campaigns/paginated', 'params': ['account_id', 'period', 'start_date', 'end_date', 'limit', 'offset']},
                {'name': 'get_campaigns_demographics', 'path': '/api/meta/campaigns/demographics', 'params': ['campaign_ids', 'period', 'start_date', 'end_date']},
                {'name': 'get_campaigns_placements', 'path': '/api/meta/campaigns/placements', 'params': ['campaign_ids', 'period', 'start_date', 'end_date']},
                {'name': 'get_adsets_timeseries', 'path': '/api/meta/adsets/timeseries', 'params': ['adset_ids', 'period', 'start_date', 'end_date']},
                {'name': 'get_adsets_demographics', 'path': '/api/meta/adsets/demographics', 'params': ['adset_ids', 'period', 'start_date', 'end_date']},
                {'name': 'get_adsets_placements', 'path': '/api/meta/adsets/placements', 'params': ['adset_ids', 'period', 'start_date', 'end_date']},
                {'name': 'get_ads_by_adsets', 'path': '/api/meta/adsets/ads', 'params': ['adset_ids']},
                {'name': 'get_ads_timeseries', 'path': '/api/meta/ads/timeseries', 'params': ['ad_ids', 'period', 'start_date', 'end_date']},
                {'name': 'get_ads_demographics', 'path': '/api/meta/ads/demographics', 'params': ['ad_ids', 'period', 'start_date', 'end_date']},
                {'name': 'get_ads_placements', 'path': '/api/meta/ads/placements', 'params': ['ad_ids', 'period', 'start_date', 'end_date']},
            ],
            'facebook_analytics': [
                {'name': 'get_facebook_pages', 'path': '/api/meta/pages', 'params': []},
                {'name': 'get_facebook_page_insights', 'path': '/api/meta/pages/{page_id}/insights', 'params': ['page_id', 'period', 'start_date', 'end_date']},
                {'name': 'get_facebook_page_posts', 'path': '/api/meta/pages/{page_id}/posts', 'params': ['page_id', 'limit', 'period', 'start_date', 'end_date']},
                {'name': 'get_facebook_demographics', 'path': '/api/meta/pages/{page_id}/demographics', 'params': ['page_id']},
                {'name': 'get_facebook_engagement', 'path': '/api/meta/pages/{page_id}/engagement-breakdown', 'params': ['page_id', 'period', 'start_date', 'end_date']},
                {'name': 'get_meta_page_insights_timeseries', 'path': '/api/meta/pages/{page_id}/insights/timeseries', 'params': ['page_id', 'period', 'start_date', 'end_date']},
                {'name': 'get_meta_page_posts_timeseries', 'path': '/api/meta/pages/{page_id}/posts/timeseries', 'params': ['page_id', 'limit', 'period', 'start_date', 'end_date']},
                {'name': 'get_meta_video_views_breakdown', 'path': '/api/meta/pages/{page_id}/video-views-breakdown', 'params': ['page_id', 'period', 'start_date', 'end_date']},
                {'name': 'get_meta_content_type_breakdown', 'path': '/api/meta/pages/{page_id}/content-type-breakdown', 'params': ['page_id', 'period', 'start_date', 'end_date']},
                {'name': 'get_meta_follows_unfollows', 'path': '/api/meta/pages/{page_id}/follows-unfollows', 'params': ['page_id', 'period', 'start_date', 'end_date']},
                {'name': 'get_meta_organic_vs_paid', 'path': '/api/meta/pages/{page_id}/organic-vs-paid', 'params': ['page_id', 'period', 'start_date', 'end_date']},
            ],
            'instagram_analytics': [
                {'name': 'get_meta_instagram_accounts', 'path': '/api/meta/instagram/accounts', 'params': []},
                {'name': 'get_meta_instagram_insights', 'path': '/api/meta/instagram/{account_id}/insights', 'params': ['account_id', 'period', 'start_date', 'end_date']},
                {'name': 'get_meta_instagram_insights_timeseries', 'path': '/api/meta/instagram/{account_id}/insights/timeseries', 'params': ['account_id', 'period', 'start_date', 'end_date']},
                {'name': 'get_meta_instagram_media', 'path': '/api/meta/instagram/{account_id}/media', 'params': ['account_id', 'limit', 'period', 'start_date', 'end_date']},
                {'name': 'get_meta_instagram_media_timeseries', 'path': '/api/meta/instagram/{account_id}/media/timeseries', 'params': ['account_id', 'limit', 'period', 'start_date', 'end_date']},
            ],
            'intent_insights': [
                {'name': 'get_keyword_insights', 'path': '/api/intent/keyword-insights/{customer_id}', 'params': ['customer_id', 'seed_keywords', 'country', 'timeframe', 'start_date', 'end_date']},
            ],
            'other': [
                {'name': 'get_meta_overview', 'path': '/api/meta/overview', 'params': ['period', 'start_date', 'end_date']},
                {'name': 'debug_meta_permissions', 'path': '/api/meta/debug/permissions', 'params': []},
                {'name': 'send_chat_message', 'path': '/api/chat/message', 'params': ['chat_request']},
                {'name': 'get_conversation', 'path': '/api/chat/conversation/{session_id}', 'params': ['session_id', 'module_type']},
                {'name': 'get_chat_sessions_list', 'path': '/api/chat/sessions/{module_type}', 'params': ['module_type']},
                {'name': 'delete_chat_sessions', 'path': '/api/chat/delete', 'params': ['delete_request']},
                {'name': 'get_chat_history', 'path': '/api/chat/history/{module_type}', 'params': ['module_type', 'limit']},
                {'name': 'debug_chat_sessions', 'path': '/api/chat/debug/{module_type}', 'params': ['module_type']},
            ]
        }

        self._organize_endpoint_registry()


    async def send_status_update(self, status: str, details: str = ""):
            """Send status update (in real implementation, this would use WebSocket)"""
            logger.info(f"STATUS: {status} - {details}")
            # For now, just log. In production, this would send via WebSocket
            return {"status": status, "details": details, "timestamp": datetime.utcnow()}


    async def create_or_get_simple_session(self, user_email: str, module_type: ModuleType, session_id: Optional[str] = None, customer_id: Optional[str] = None, property_id: Optional[str] = None, account_id: Optional[str] = None, page_id: Optional[str] = None, period: str = "LAST_7_DAYS") -> str:
        """Create new session or get existing one"""
        collection = self.db.chat_sessions
        if session_id:
            existing_session = await collection.find_one({"session_id": session_id, "user_email": user_email})
            if existing_session:
                await collection.update_one({"session_id": session_id}, {"$set": {"last_activity": datetime.utcnow()}})
                logger.info(f"Using existing session: {session_id}")
                return session_id
        
        new_session_id = str(uuid.uuid4())
        session_doc = {
            "session_id": new_session_id,
            "user_email": user_email,
            "module_type": module_type.value,
            "customer_id": customer_id,
            "property_id": property_id,
            "account_id": account_id,
            "page_id": page_id,
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
        


    # def _get_collection_description(self, collection_name: str) -> str:
    #     """Return a human-readable description of the specified collection."""

    #     collection_descriptions = {
    #         # ---------------- GOOGLE ADS ----------------
    #         'google_ads_customers': 'List of linked Google Ads customer accounts.',
    #         'google_ads_key_stats': 'Key performance indicators for Google Ads accounts, including impressions, clicks, CTR, and cost metrics.',
    #         'google_ads_campaigns': 'Detailed data for Google Ads campaigns, including campaign names, objectives, and performance results.',
    #         'google_ads_keywords': 'Performance data of individual keywords in Google Ads, including impressions, clicks, and cost.',
    #         'google_ads_performance': 'Overall performance data for Google Ads, combining clicks, conversions, spend, and ROI metrics.',
    #         'google_ads_geographic': 'Geographic distribution of Google Ads performance, showing results by region or location.',
    #         'google_ads_device_performance': 'Performance metrics for Google Ads across devices such as mobile, desktop, and tablet.',
    #         'google_ads_time_performance': 'Time-based Google Ads performance metrics showing trends by day, week, or month.',
    #         'google_ads_keyword_ideas': 'Suggested keyword ideas for Google Ads campaigns, based on seed keywords and location targeting.',

    #         # ---------------- GOOGLE ANALYTICS ----------------
    #         'google_analytics_properties': 'List of connected Google Analytics properties and configuration details.',
    #         'google_analytics_metrics': 'Core Google Analytics data, including sessions, users, bounce rates, and engagement metrics.',
    #         'google_analytics_traffic_sources': 'Breakdown of website traffic sources, including organic, paid, referral, and direct channels.',
    #         'google_analytics_conversions': 'Goal and conversion metrics tracked in Google Analytics, including conversion rate and value.',
    #         'google_analytics_channel_performance': 'Performance comparison between marketing channels such as organic search, paid ads, and social.',
    #         'google_analytics_audience_insights': 'Audience demographics and behavioral insights from Google Analytics.',
    #         'google_analytics_time_series': 'Time-series data from Google Analytics showing trends in traffic and engagement.',
    #         'google_analytics_revenue_breakdown_channel': 'Revenue breakdown by marketing channel in Google Analytics.',
    #         'google_analytics_revenue_breakdown_source': 'Revenue breakdown by traffic source in Google Analytics.',
    #         'google_analytics_revenue_breakdown_device': 'Revenue analysis segmented by user device (mobile, desktop, tablet).',
    #         'google_analytics_revenue_breakdown_location': 'Revenue distribution by user location or region.',
    #         'google_analytics_revenue_breakdown_page': 'Revenue performance by top-performing pages on the website.',
    #         'google_analytics_comprehensive_revenue_breakdown': 'A comprehensive revenue analysis combining multiple dimensions.',
    #         'google_analytics_revenue_breakdown_raw': 'Raw-level revenue breakdown data from Google Analytics.',
    #         'google_analytics_channel_revenue_timeseries': 'Time-series view of revenue by marketing channel.',
    #         'google_analytics_specific_channels_timeseries': 'Performance over time for selected Google Analytics channels.',
    #         'google_analytics_available_channels': 'List of available channels for Google Analytics revenue reporting.',
    #         'google_analytics_channel_revenue_timeseries_raw': 'Raw time-series data of channel-level revenue metrics.',
    #         'google_analytics_revenue_time_series': 'Revenue trends over time, segmented by selected dimensions.',
    #         'google_analytics_top_pages': 'Top pages on the website ranked by traffic, engagement, or conversions.',
    #         'google_analytics_trends': 'Identified performance trends from Google Analytics data.',
    #         'google_analytics_roas_roi_time_series': 'ROAS (Return on Ad Spend) and ROI trends over time.',
    #         'google_analytics_combined_overview': 'High-level overview combining Google Ads and Analytics data.',
    #         'google_analytics_enhanced_combined_roas_roi_metrics': 'Enhanced cross-platform ROI and ROAS calculations.',
    #         'google_analytics_combined_roas_roi_metrics_legacy': 'Legacy combined metrics for ROAS and ROI calculations.',
    #         'google_analytics_engagement_funnel': 'LLM-generated engagement funnel derived from Google Analytics events and conversion data.',

    #         # ---------------- META ADS ----------------
    #         'meta_ad_accounts': 'List of connected Meta (Facebook + Instagram) Ad Accounts.',
    #         'meta_account_insights': 'Account-level performance insights for Meta Ads, including reach, engagement, and spend.',
    #         'meta_campaigns_all': 'Comprehensive details of all Meta Ad Campaigns including active and inactive ones.',
    #         'meta_campaigns_list': 'List view of Meta Ad Campaigns filtered by status.',
    #         'meta_campaigns_timeseries': 'Time-series data for Meta Ad Campaigns showing daily or weekly performance.',
    #         'meta_campaigns_demographics': 'Demographic breakdown of Meta Ad Campaign performance.',
    #         'meta_campaigns_placements': 'Placement performance data for Meta Ads across different surfaces (Feed, Reels, Stories, etc.).',
    #         'meta_adsets_timeseries': 'Time-based performance data for Meta AdSets.',
    #         'meta_adsets_demographics': 'Demographic performance analysis of Meta AdSets.',
    #         'meta_adsets_placements': 'Ad placement effectiveness at the AdSet level.',
    #         'meta_ads_by_adsets': 'List of ads under each AdSet with corresponding performance data.',
    #         'meta_ads_timeseries': 'Time-series trends for individual Meta Ads.',
    #         'meta_ads_demographics': 'Demographic performance data for specific Meta Ads.',
    #         'meta_ads_placements': 'Performance insights for Meta Ads across placements such as Feed, Stories, or Audience Network.',
    #         'meta_overview': 'Overall summary of Meta Ads performance and account activity.',
    #         'meta_debug_permissions': 'Debugging information for Meta Ads API permissions and access validation.',

    #         # ---------------- FACEBOOK ANALYTICS ----------------
    #         'facebook_pages': 'List of Facebook pages connected to the account.',
    #         'facebook_page_insights': 'Analytics for Facebook Pages including engagement, reach, and page interactions.',
    #         'facebook_page_posts': 'Performance of Facebook Page posts including likes, shares, and comments.',
    #         'facebook_page_demographics': 'Audience demographic breakdown for Facebook Pages.',
    #         'facebook_page_engagement': 'Detailed engagement metrics for Facebook Pages.',
    #         'facebook_page_insights_timeseries': 'Time-based performance trends for Facebook Page insights.',
    #         'facebook_page_posts_timeseries': 'Time-series data for Facebook Page posts.',
    #         'facebook_video_views_breakdown': 'Breakdown of Facebook video views by type or duration.',
    #         'facebook_content_type_breakdown': 'Engagement analysis by content type (video, image, text, etc.) on Facebook.',
    #         'facebook_follows_unfollows': 'Daily trends in Facebook Page follows and unfollows.',
    #         'facebook_organic_vs_paid': 'Comparison of organic and paid reach for Facebook Pages.',

    #         # ---------------- INSTAGRAM ANALYTICS ----------------
    #         'instagram_accounts': 'List of connected Instagram business accounts.',
    #         'instagram_insights': 'Overall Instagram account insights including reach, impressions, and engagement.',
    #         'instagram_insights_timeseries': 'Time-series view of Instagram account performance metrics.',
    #         'instagram_media': 'Details of Instagram posts including engagement and caption data.',
    #         'instagram_media_timeseries': 'Time-based trends of Instagram media performance.',

    #         # ---------------- INTENT INSIGHTS ----------------
    #         'intent_keyword_insights': 'Search keyword insights showing search volume, CPC, and competition trends across markets.',

    #         # ---------------- CHAT & MISC ----------------
    #         'chat_sessions': 'Stored chat sessions and conversations for AI-driven analytics or support.',
    #         'chat_history': 'Historical chat messages across sessions.',
    #         'chat_debug_sessions': 'Debug data for chatbot session management.',
    #         'combined_metrics': 'Combined data metrics integrating multiple ad and analytics platforms.',
    #         'revenue_analysis': 'Comprehensive revenue analysis across campaigns, channels, and platforms.',
    #     }

    #     return collection_descriptions.get(
    #         collection_name,
    #         f'Data collection for {collection_name.replace("_", " ")}.'
    #     )

    async def get_conversation_context(self, session_id: str, limit: int = 10) -> List[Dict[str, str]]:
        """Get recent conversation history for context"""
        try:
            collection = self.db.chat_sessions
            session = await collection.find_one({"session_id": session_id})
            
            if not session or not session.get("messages"):
                return []
            
            # Get last N messages
            messages = session["messages"][-limit:]
            
            return [
                {
                    "role": msg["role"],
                    "content": msg["content"]
                }
                for msg in messages
            ]
        except Exception as e:
            logger.error(f"Error getting conversation context: {e}")
            return []

    async def _fetch_account_list(self, module_type: ModuleType, token: str, user_email: str) -> List[Dict[str, Any]]:
        """Fetch a list of available accounts for the specified module type."""
        import aiohttp
        from fastapi import HTTPException

        logger.info(f"Fetching account list for user: {user_email}, module: {module_type.value}")

        # Define endpoint mappings for account list retrieval based on module type
        endpoint_mapping = {
            ModuleType.GOOGLE_ADS: {
                'endpoint': 'get_ads_customers',
                'path': '/api/ads/customers',
                'response_key': 'customers',
                'id_field': 'customer_id',
                'name_field': 'name'
            },
            ModuleType.GOOGLE_ANALYTICS: {
                'endpoint': 'get_ga_properties',
                'path': '/api/analytics/properties',
                'response_key': 'properties',
                'id_field': 'property_id',
                'name_field': 'name'
            },
            ModuleType.META_ADS: {
                'endpoint': 'get_meta_ad_accounts',
                'path': '/api/meta/ad-accounts',
                'response_key': 'accounts',
                'id_field': 'account_id',
                'name_field': 'name'
            },
            ModuleType.FACEBOOK_ANALYTICS: {
                'endpoint': 'get_facebook_pages',
                'path': '/api/meta/pages',
                'response_key': 'pages',
                'id_field': 'page_id',
                'name_field': 'name'
            },
            ModuleType.INSTAGRAM_ANALYTICS: {
                'endpoint': 'get_meta_instagram_accounts',
                'path': '/api/meta/instagram/accounts',
                'response_key': 'accounts',
                'id_field': 'account_id',
                'name_field': 'name'
            }
        }

        # Check if module type is supported
        if module_type not in endpoint_mapping:
            logger.error(f"Unsupported module type for account list: {module_type.value}")
            raise HTTPException(status_code=400, detail=f"Module type {module_type.value} not supported for account listing")

        endpoint_config = endpoint_mapping[module_type]
        url = f"https://eyqi6vd53z.us-east-2.awsapprunner.com{endpoint_config['path']}"

        try:
            async with aiohttp.ClientSession() as session:
                headers = {'Authorization': f'Bearer {token}'}
                async with session.get(url, headers=headers) as response:
                    if response.status != 200:
                        error_msg = f"Failed to fetch accounts for {module_type.value}: Status {response.status}"
                        logger.error(error_msg)
                        raise HTTPException(status_code=response.status, detail=error_msg)

                    data = await response.json()
                    accounts = data.get(endpoint_config['response_key'], [])

                    # Normalize the account list format
                    normalized_accounts = []
                    for account in accounts:
                        normalized_account = {
                            'id': account.get(endpoint_config['id_field']),
                            'name': account.get(endpoint_config['name_field'], 'Unnamed Account'),
                            'descriptiveName': account.get('descriptiveName', account.get(endpoint_config['name_field'], 'Unnamed Account'))
                        }
                        normalized_accounts.append(normalized_account)

                    logger.info(f"Fetched {len(normalized_accounts)} accounts for {module_type.value}")
                    return normalized_accounts

        except Exception as e:
            logger.error(f"Error fetching account list for {module_type.value}: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error fetching accounts: {str(e)}")


    async def _save_endpoint_response(self, endpoint_name: str, endpoint_path: str, params: Dict[str, Any], response_data: Any, user_email: str) -> None:
        """Save API endpoint response data to MongoDB for future use."""
        try:
            collection = self.db['endpoint_responses']
            
            # Prepare document to be saved
            document = {
                "user_email": user_email,
                "endpoint_name": endpoint_name,
                "endpoint_path": endpoint_path,
                "request_params": params,
                "response_data": response_data,
                "last_updated": datetime.utcnow(),
                "module_type": self._infer_module_type(endpoint_name),
                "customer_id": params.get('customer_id'),
                "property_id": params.get('property_id'),
                "account_id": params.get('account_id'),
                "page_id": params.get('page_id'),
                "period": params.get('period')
            }

            # Insert or update document based on unique criteria
            await collection.update_one(
                {
                    "user_email": user_email,
                    "endpoint_name": endpoint_name,
                    "customer_id": document.get("customer_id"),
                    "property_id": document.get("property_id"),
                    "account_id": document.get("account_id"),
                    "page_id": document.get("page_id"),
                    "period": document.get("period")
                },
                {"$set": document},
                upsert=True
            )
            
            logger.info(f"Saved response for endpoint {endpoint_name} for user {user_email}")

        except Exception as e:
            logger.error(f"Error saving endpoint response for {endpoint_name}: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to save endpoint response: {str(e)}")

    def _infer_module_type(self, endpoint_name: str) -> str:
        """Infer the module type based on the endpoint name."""
        module_mapping = {
            'ads_': ModuleType.GOOGLE_ADS.value,
            'ga_': ModuleType.GOOGLE_ANALYTICS.value,
            'meta_': ModuleType.META_ADS.value,
            'facebook_': ModuleType.FACEBOOK_ANALYTICS.value,
            'instagram_': ModuleType.INSTAGRAM_ANALYTICS.value,
            'keyword_': ModuleType.INTENT_INSIGHTS.value
        }
        
        for prefix, module_type in module_mapping.items():
            if endpoint_name.startswith(prefix):
                return module_type
        return 'other'


    def _create_visualizations(self, data: Dict[str, Any], viz_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create visualizations (tables or charts) based on the provided data and visualization configuration."""
        visualizations = []
        
        try:
            # Handle table visualization
            if viz_config.get('needs_table', False) and viz_config.get('table_columns'):
                table_data = []
                for endpoint, endpoint_data in data.items():
                    if isinstance(endpoint_data, dict) and 'error' not in endpoint_data:
                        # Extract relevant data for table
                        rows = self._extract_table_data(endpoint_data, viz_config['table_columns'])
                        table_data.extend(rows)
                
                if table_data:
                    visualizations.append({
                        'type': 'table',
                        'columns': viz_config['table_columns'],
                        'data': table_data[:50]  # Limit to 50 rows for performance
                    })
                    logger.info(f"Created table visualization with {len(table_data)} rows")

            # Handle chart visualization
            if viz_config.get('needs_chart', False) and viz_config.get('chart_type'):
                chart_type = viz_config['chart_type']
                if chart_type not in ['bar', 'line', 'pie']:
                    logger.warning(f"Unsupported chart type: {chart_type}. Skipping chart creation.")
                    return visualizations

                chart_data = self._prepare_chart_data(data, chart_type)
                if chart_data:
                    chart_config = {
                        'type': chart_type,
                        'data': {
                            'labels': chart_data['labels'],
                            'datasets': [{
                                'label': chart_data['dataset_label'],
                                'data': chart_data['values'],
                                'backgroundColor': self._get_chart_colors(chart_type, len(chart_data['values'])),
                                'borderColor': self._get_chart_colors(chart_type, len(chart_data['values'])),
                                'borderWidth': 1
                            }]
                        },
                        'options': {
                            'responsive': True,
                            'plugins': {
                                'legend': {'position': 'top'},
                                'title': {'display': True, 'text': chart_data['title']}
                            }
                        }
                    }
                    
                    # Add specific options for different chart types
                    if chart_type == 'bar':
                        chart_config['options']['scales'] = {
                            'y': {'beginAtZero': True}
                        }
                    elif chart_type == 'line':
                        chart_config['options']['elements'] = {
                            'line': {'tension': 0.4}
                        }
                    
                    visualizations.append({
                        'type': 'chart',
                        'chartjs': chart_config
                    })
                    logger.info(f"Created {chart_type} chart visualization with {len(chart_data['values'])} data points")

        except Exception as e:
            logger.error(f"Error creating visualizations: {str(e)}")
            return visualizations

        return visualizations

    def _extract_table_data(self, endpoint_data: Dict[str, Any], columns: List[str]) -> List[Dict[str, Any]]:
        """Extract data for table visualization from endpoint response."""
        table_rows = []
        
        # Handle different data structures
        if isinstance(endpoint_data, list):
            for item in endpoint_data:
                row = {}
                for col in columns:
                    row[col] = item.get(col, 'N/A')
                table_rows.append(row)
        elif isinstance(endpoint_data, dict):
            # Handle nested metrics or key-value pairs
            row = {}
            for col in columns:
                if col in endpoint_data:
                    value = endpoint_data.get(col)
                    row[col] = value.get('formatted', value.get('value', 'N/A')) if isinstance(value, dict) else value
                else:
                    row[col] = 'N/A'
            table_rows.append(row)
        
        return table_rows

    def _prepare_chart_data(self, data: Dict[str, Any], chart_type: str) -> Dict[str, Any]:
        """Prepare data for chart visualization."""
        chart_data = {'labels': [], 'values': [], 'title': 'Data Visualization', 'dataset_label': 'Metrics'}
        
        # Simple heuristic to extract chartable data
        for endpoint, endpoint_data in data.items():
            if isinstance(endpoint_data, dict) and 'error' not in endpoint_data:
                if 'metrics' in endpoint_data or 'data' in endpoint_data:
                    data_source = endpoint_data.get('metrics', endpoint_data.get('data', []))
                    
                    if isinstance(data_source, list):
                        for item in data_source:
                            if isinstance(item, dict):
                                label = item.get('date') or item.get('name') or item.get('label') or 'Unknown'
                                value = item.get('value') or item.get('impressions') or item.get('clicks') or 0
                                if isinstance(value, (int, float)):
                                    chart_data['labels'].append(str(label))
                                    chart_data['values'].append(value)
                    
                    elif isinstance(data_source, dict):
                        for key, value in data_source.items():
                            if isinstance(value, dict) and 'value' in value:
                                chart_data['labels'].append(key)
                                chart_data['values'].append(value['value'])
                            elif isinstance(value, (int, float)):
                                chart_data['labels'].append(key)
                                chart_data['values'].append(value)
                    
                    chart_data['title'] = f"{endpoint.replace('_', ' ').title()} Trends"
                    chart_data['dataset_label'] = endpoint.replace('_', ' ').title()
                    break  # Use first valid data source
        
        return chart_data

    def _get_chart_colors(self, chart_type: str, data_length: int) -> List[str]:
        """Return appropriate colors for chart visualization."""
        base_colors = [
            'rgba(75, 192, 192, 0.6)',  # Teal
            'rgba(255, 99, 132, 0.6)',   # Red
            'rgba(54, 162, 235, 0.6)',   # Blue
            'rgba(255, 206, 86, 0.6)',   # Yellow
            'rgba(153, 102, 255, 0.6)',  # Purple
        ]
        
        if chart_type == 'pie':
            return base_colors[:min(data_length, len(base_colors))]
        else:
            # For bar and line charts, use single color or repeat first color
            return [base_colors[0]] * data_length


    # =================
    # AGENT 1: General Query Classifier
    # =================
    async def agent_classify_query(self, message: str) -> Dict[str, Any]:
        self._log_agent_step("AGENT 1: CLASSIFIER", "STARTING", {"message": message[:100]})

        """Classify if query is general or analytics-related"""
        prompt = f"""
        Classify this user query into one of two categories:
        1. GENERAL - General greetings, chit-chat, or questions not related to data/analytics
        2. ANALYTICS - Questions about marketing data, metrics, performance, campaigns, etc.

        User Query: "{message}"

        Respond in JSON format:
        {{
            "category": "GENERAL" or "ANALYTICS",
            "confidence": 0.0 to 1.0,
            "reason": "brief explanation"
        }}
        """

        try:
            response = await self.openai_client.chat.completions.create(
                model="gpt-4-turbo-preview",  # ✅ Changed from gpt-3.5-turbo
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            logger.info(f"Query classified as: {result['category']} (confidence: {result['confidence']})")
            self._log_agent_step("AGENT 1: CLASSIFIER", "COMPLETE", result)
            return result
        except Exception as e:
            logger.error(f"Error in query classification: {e}")
            return {"category": "ANALYTICS", "confidence": 0.5, "reason": "Default to analytics on error"}

    # =================
    # AGENT 2: Time Period Extractor
    # =================
    async def agent_extract_time_period(self, message: str, context: Dict[str, Any]) -> Dict[str, Any]:
        self._log_agent_step("AGENT 2: TIME - EXTRACTOR AGENT", "STARTING", {"message": message[:100]})

        """Extract time period from message or use context"""
        
        # Check if custom dates are in context first
        if context.get('custom_dates') and context['custom_dates'].get('startDate') and context['custom_dates'].get('endDate'):
            logger.info(f"Using custom dates from context: {context['custom_dates']}")
            return {
                'has_period': True,
                'period': 'CUSTOM',
                'start_date': context['custom_dates']['startDate'],
                'end_date': context['custom_dates']['endDate'],
                'needs_clarification': False
            }
        
        # Check if period is in context
        if context.get('period') and context['period'] != 'CUSTOM':
            logger.info(f"Using period from context: {context['period']}")
            return {
                'has_period': True,
                'period': context['period'],
                'start_date': None,
                'end_date': None,
                'needs_clarification': False
            }
        
        # Try to extract from message
        prompt = f"""
        Extract time period information from this message.
        Look for:
        - Specific dates (e.g., "from Jan 1 to Jan 31", "in December 2024")
        - Relative periods (e.g., "last 7 days", "past month", "this quarter", "yesterday")
        - Time keywords (e.g., "recently", "latest", "current")

        User Message: "{message}"

        If time period is found, convert to either:
        - Standard period: LAST_7_DAYS, LAST_30_DAYS, LAST_3_MONTHS, LAST_1_YEAR
        - Custom dates: specific start_date and end_date (YYYY-MM-DD format)

        Respond in JSON format:
        {{
            "has_period": true/false,
            "period": "LAST_7_DAYS" or "CUSTOM" or null,
            "start_date": "YYYY-MM-DD" or null,
            "end_date": "YYYY-MM-DD" or null,
            "extracted_text": "the time phrase found" or null,
            "needs_clarification": true/false
        }}
        """

        try:
            response = await self.openai_client.chat.completions.create(
                model="gpt-4-turbo-preview",  # ✅ Changed from gpt-3.5-turbo
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            
            # If no period found in message and not in context, use default
            if not result['has_period']:
                logger.info("No period found in message, using LAST_7_DAYS as default")
                result['has_period'] = True
                result['period'] = 'LAST_7_DAYS'
                result['needs_clarification'] = False
            
            logger.info(f"Time period extracted: {result}")
            self._log_agent_step("AGENT 2: TIME - EXTRACTOR AGENT", "COMPLETE", result)
            return result
            
        except Exception as e:
            logger.error(f"Error extracting time period: {e}")
            # Use default period on error
            return {
                'has_period': True,
                'period': 'LAST_7_DAYS',
                'needs_clarification': False
            }
        # =================
    
    # AGENT 3: Account Identifier
    # =================
    async def agent_identify_account(
        self, 
        message: str, 
        module_type: ModuleType, 
        context: Dict[str, Any]
    ) -> Dict[str, Any]:

        """Identify which account the user is referring to"""
        
        logger.info("\n" + "="*80)
        logger.info("🔍 AGENT 3: ACCOUNT IDENTIFIER - STARTING")
        logger.info(f"Module: {module_type.value}")
        logger.info(f"Context received: {json.dumps(context, default=str, indent=2)}")
        
        # Get account ID from context based on module type
        account_id = None
        if module_type == ModuleType.GOOGLE_ADS:
            account_id = context.get('customer_id')
            logger.info(f"Google Ads - customer_id: {account_id}")
        elif module_type == ModuleType.GOOGLE_ANALYTICS:
            account_id = context.get('property_id')
            logger.info(f"Google Analytics - property_id: {account_id}")
        elif module_type == ModuleType.META_ADS:
            account_id = context.get('account_id')
            logger.info(f"Meta Ads - account_id: {account_id}")
        elif module_type == ModuleType.FACEBOOK_ANALYTICS:
            account_id = context.get('page_id')
            logger.info(f"Facebook - page_id: {account_id}")
        elif module_type == ModuleType.INSTAGRAM_ANALYTICS:
            account_id = context.get('account_id')
            logger.info(f"Instagram - account_id: {account_id}")
        elif module_type == ModuleType.INTENT_INSIGHTS:
            account_id = context.get('account_id')
            logger.info(f"Intent Insights - account_id: {account_id}")
        
        result = {
            'has_specific_reference': bool(account_id),
            'reference_type': 'context' if account_id else 'none',
            'account_id': account_id,
            'use_active_account': bool(account_id),
            'needs_account_list': not bool(account_id)
        }
        
        if not account_id:
            result['clarification_message'] = f"Please specify the {module_type.value} account to analyze."
        
        logger.info(f"✅ AGENT 3 COMPLETE: {result}")
        logger.info("="*80 + "\n")
        self._log_agent_step("AGENT 3: ACCOUNT IDENTIFIER AGENT", "COMPLETE", result)

        
        return result


    # =================
    # AGENT 4: Endpoint Selector (THE PROBLEMATIC ONE)
    # =================
    async def agent_select_endpoints(
        self, 
        message: str, 
        module_type: ModuleType, 
        account_info: Dict[str, Any],
        conversation_history: List[Dict[str, str]] = None
    ) -> List[Dict[str, Any]]:
        
        logger.info("\n" + "="*80)
        logger.info("🔍 AGENT 4: ENDPOINT SELECTOR - STARTING")
        logger.info(f"Module: {module_type.value}")

        """Select relevant endpoints based on the query"""
        
        available_endpoints = self.endpoint_registry.get(module_type.value, [])
        
        # Build conversation context OUTSIDE f-string
        history_context = ""
        if conversation_history:
            recent_history = conversation_history[-4:]  # Last 2 exchanges
            history_lines = []
            for msg in recent_history:
                role = msg['role'].upper()
                content = msg['content']
                history_lines.append(f"{role}: {content}")
            history_context = "\n".join(history_lines)
        
        # Prepare endpoints info OUTSIDE f-string
        endpoints_info = []
        for e in available_endpoints:
            endpoints_info.append({
                'name': e['name'],
                'path': e['path'],
                'description': self._get_endpoint_description(e['name'])
            })
        endpoints_json = json.dumps(endpoints_info, indent=2)
        account_json = json.dumps(account_info)
        
        # Build history section
        history_section = f"CONVERSATION HISTORY:\n{history_context}\n\n" if history_context else ""
        
        prompt = f"""Select the most relevant API endpoints to answer this user query.

    {history_section}Current Query: "{message}"
    Module: {module_type.value}
    Account Info: {account_json}

    Available Endpoints:
    {endpoints_json}

    Rules:
    1. Select MINIMUM endpoints needed to answer the question
    2. For overview questions, select all the key metrics endpoints required
    3. For specific questions, select all targeted endpoints
    4. Consider follow-up context - don't repeat data already provided
    5. For comparison questions, select endpoints that provide comparative data
    6. AVOID selecting the 'get_meta_ad_accounts/campaigns/list' endpoint unless specifically asked for ALL campaigns

    Respond with JSON array:
    {{
        "endpoints": ["endpoint_name1", "endpoint_name2"],
        "reasoning": "brief explanation",
        "is_followup": true/false
    }}"""

        try:
            response = await self.openai_client.chat.completions.create(
                model="gpt-4-turbo-preview",  # ✅ FIXED - Changed from "gpt-4"
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                response_format={"type": "json_object"}  # ✅ Now this works
            )
            
            result = json.loads(response.choices[0].message.content)
            selected = []
            
            for endpoint_name in result.get('endpoints', []):
                endpoint = next((e for e in available_endpoints if e['name'] == endpoint_name), None)
                if endpoint:
                    selected.append(endpoint)
            
            logger.info(f"Selected {len(selected)} endpoints: {[e['name'] for e in selected]}")
            logger.info(f"Reasoning: {result.get('reasoning')}")
            
            # Fallback to default endpoints if none selected
            if not selected:
                selected = self._get_default_endpoints(module_type, available_endpoints)

            logger.info(f"✅ AGENT 4 COMPLETE: {selected}")
            logger.info("="*80 + "\n")
            return selected
            
        except Exception as e:
            logger.error(f"Error selecting endpoints: {e}")
            return self._get_default_endpoints(module_type, available_endpoints)

    def _get_endpoint_description(self, endpoint_name: str) -> str:
        """Get human-readable description of endpoint"""

        descriptions = {
            # ---------------- GOOGLE ADS ----------------
            'google_ads_customers': 'List of linked Google Ads customer accounts.',
            'google_ads_key_stats': 'Key performance indicators for Google Ads accounts, including impressions, clicks, CTR, and cost metrics.',
            'google_ads_campaigns': 'Detailed data for Google Ads campaigns, including campaign names, objectives, and performance results.',
            'google_ads_keywords': 'Performance data of individual keywords in Google Ads, including impressions, clicks, and cost.',
            'google_ads_performance': 'Overall performance data for Google Ads, combining clicks, conversions, spend, and ROI metrics.',
            'google_ads_geographic': 'Geographic distribution of Google Ads performance, showing results by region or location.',
            'google_ads_device_performance': 'Performance metrics for Google Ads across devices such as mobile, desktop, and tablet.',
            'google_ads_time_performance': 'Time-based Google Ads performance metrics showing trends by day, week, or month.',
            'google_ads_keyword_ideas': 'Suggested keyword ideas for Google Ads campaigns, based on seed keywords and location targeting.',

            # ---------------- GOOGLE ANALYTICS ----------------
            'google_analytics_properties': 'List of connected Google Analytics properties and configuration details.',
            'google_analytics_metrics': 'Core Google Analytics data, including sessions, users, bounce rates, and engagement metrics.',
            'google_analytics_traffic_sources': 'Breakdown of website traffic sources, including organic, paid, referral, and direct channels.',
            'google_analytics_conversions': 'Goal and conversion metrics tracked in Google Analytics, including conversion rate and value.',
            'google_analytics_channel_performance': 'Performance comparison between marketing channels such as organic search, paid ads, and social.',
            'google_analytics_audience_insights': 'Audience demographics and behavioral insights from Google Analytics.',
            'google_analytics_time_series': 'Time-series data from Google Analytics showing trends in traffic and engagement.',
            'google_analytics_revenue_breakdown_channel': 'Revenue breakdown by marketing channel in Google Analytics.',
            'google_analytics_revenue_breakdown_source': 'Revenue breakdown by traffic source in Google Analytics.',
            'google_analytics_revenue_breakdown_device': 'Revenue analysis segmented by user device (mobile, desktop, tablet).',
            'google_analytics_revenue_breakdown_location': 'Revenue distribution by user location or region.',
            'google_analytics_revenue_breakdown_page': 'Revenue performance by top-performing pages on the website.',
            'google_analytics_comprehensive_revenue_breakdown': 'A comprehensive revenue analysis combining multiple dimensions.',
            'google_analytics_revenue_breakdown_raw': 'Raw-level revenue breakdown data from Google Analytics.',
            'google_analytics_channel_revenue_timeseries': 'Time-series view of revenue by marketing channel.',
            'google_analytics_specific_channels_timeseries': 'Performance over time for selected Google Analytics channels.',
            'google_analytics_available_channels': 'List of available channels for Google Analytics revenue reporting.',
            'google_analytics_channel_revenue_timeseries_raw': 'Raw time-series data of channel-level revenue metrics.',
            'google_analytics_revenue_time_series': 'Revenue trends over time, segmented by selected dimensions.',
            'google_analytics_top_pages': 'Top pages on the website ranked by traffic, engagement, or conversions.',
            'google_analytics_trends': 'Identified performance trends from Google Analytics data.',
            'google_analytics_roas_roi_time_series': 'ROAS (Return on Ad Spend) and ROI trends over time.',
            'google_analytics_combined_overview': 'High-level overview combining Google Ads and Analytics data.',
            'google_analytics_enhanced_combined_roas_roi_metrics': 'Enhanced cross-platform ROI and ROAS calculations.',
            'google_analytics_combined_roas_roi_metrics_legacy': 'Legacy combined metrics for ROAS and ROI calculations.',
            'google_analytics_engagement_funnel': 'LLM-generated engagement funnel derived from Google Analytics events and conversion data.',

            # ---------------- META ADS ----------------
            'meta_ad_accounts': 'List of connected Meta (Facebook + Instagram) Ad Accounts.',
            'meta_account_insights': 'Account-level performance insights for Meta Ads, including reach, engagement, and spend.',
            'meta_campaigns_all': 'Comprehensive details of all Meta Ad Campaigns including active and inactive ones.',
            'meta_campaigns_list': 'List view of Meta Ad Campaigns filtered by status.',
            'meta_campaigns_timeseries': 'Time-series data for Meta Ad Campaigns showing daily or weekly performance.',
            'meta_campaigns_demographics': 'Demographic breakdown of Meta Ad Campaign performance.',
            'meta_campaigns_placements': 'Placement performance data for Meta Ads across different surfaces (Feed, Reels, Stories, etc.).',
            'meta_adsets_timeseries': 'Time-based performance data for Meta AdSets.',
            'meta_adsets_demographics': 'Demographic performance analysis of Meta AdSets.',
            'meta_adsets_placements': 'Ad placement effectiveness at the AdSet level.',
            'meta_ads_by_adsets': 'List of ads under each AdSet with corresponding performance data.',
            'meta_ads_timeseries': 'Time-series trends for individual Meta Ads.',
            'meta_ads_demographics': 'Demographic performance data for specific Meta Ads.',
            'meta_ads_placements': 'Performance insights for Meta Ads across placements such as Feed, Stories, or Audience Network.',
            'meta_overview': 'Overall summary of Meta Ads performance and account activity.',
            'meta_debug_permissions': 'Debugging information for Meta Ads API permissions and access validation.',

            # ---------------- FACEBOOK ANALYTICS ----------------
            'facebook_pages': 'List of Facebook pages connected to the account.',
            'facebook_page_insights': 'Analytics for Facebook Pages including engagement, reach, and page interactions.',
            'facebook_page_posts': 'Performance of Facebook Page posts including likes, shares, and comments.',
            'facebook_page_demographics': 'Audience demographic breakdown for Facebook Pages.',
            'facebook_page_engagement': 'Detailed engagement metrics for Facebook Pages.',
            'facebook_page_insights_timeseries': 'Time-based performance trends for Facebook Page insights.',
            'facebook_page_posts_timeseries': 'Time-series data for Facebook Page posts.',
            'facebook_video_views_breakdown': 'Breakdown of Facebook video views by type or duration.',
            'facebook_content_type_breakdown': 'Engagement analysis by content type (video, image, text, etc.) on Facebook.',
            'facebook_follows_unfollows': 'Daily trends in Facebook Page follows and unfollows.',
            'facebook_organic_vs_paid': 'Comparison of organic and paid reach for Facebook Pages.',

            # ---------------- INSTAGRAM ANALYTICS ----------------
            'instagram_accounts': 'List of connected Instagram business accounts.',
            'instagram_insights': 'Overall Instagram account insights including reach, impressions, and engagement.',
            'instagram_insights_timeseries': 'Time-series view of Instagram account performance metrics.',
            'instagram_media': 'Details of Instagram posts including engagement and caption data.',
            'instagram_media_timeseries': 'Time-based trends of Instagram media performance.',

            # ---------------- INTENT INSIGHTS ----------------
            'intent_keyword_insights': 'Search keyword insights showing search volume, CPC, and competition trends across markets.',

            # ---------------- CHAT & MISC ----------------
            'chat_sessions': 'Stored chat sessions and conversations for AI-driven analytics or support.',
            'chat_history': 'Historical chat messages across sessions.',
            'chat_debug_sessions': 'Debug data for chatbot session management.',
            'combined_metrics': 'Combined data metrics integrating multiple ad and analytics platforms.',
            'revenue_analysis': 'Comprehensive revenue analysis across campaigns, channels, and platforms.',
        }

        return descriptions.get(endpoint_name, 'Data endpoint')

    def _get_default_endpoints(self, module_type: ModuleType, available_endpoints: List[Dict]) -> List[Dict]:
        """Get default endpoints when selection fails"""
        defaults = {
            ModuleType.GOOGLE_ADS: ['get_ads_key_stats'],
            ModuleType.GOOGLE_ANALYTICS: ['get_ga_metrics'],
            ModuleType.META_ADS: ['get_meta_account_insights'],
            ModuleType.FACEBOOK_ANALYTICS: ['get_facebook_page_insights'],
        }
        
        default_names = defaults.get(module_type, [])
        return [e for e in available_endpoints if e['name'] in default_names]

    # =================
    # AGENT 5: Endpoint Executor with Special Handling
    # =================
    async def agent_execute_endpoints(
        self, 
        endpoints: List[Dict[str, Any]], 
        params: Dict[str, Any], 
        user_email: str,
        status_callback=None
    ) -> Dict[str, Any]:

        """Execute selected endpoints with special handling for slow endpoints"""
        
        logger.info("="*80)
        logger.info("🔧 AGENT 5: ENDPOINT EXECUTOR - STARTING")
        logger.info(f"📋 Endpoints to execute: {[e['name'] for e in endpoints]}")
        
        results = {}
        token = params.get('token', '')
        
        if not token:
            logger.error("❌ No token provided!")
            return {"error": "Authentication token missing"}
        
        # Check for slow endpoints that need special handling
        slow_endpoints = ['get_meta_campaigns_list', 'get_campaigns_list']
        has_slow_endpoint = any(e['name'] in slow_endpoints for e in endpoints)
        
        if has_slow_endpoint and status_callback:
            await status_callback(
                "Fetching comprehensive data",
                "This may take 30-60 seconds as we're retrieving all campaigns..."
            )
        
        for endpoint in endpoints:
            endpoint_name = endpoint['name']
            logger.info(f"\n{'='*60}")
            logger.info(f"🎯 Executing endpoint: {endpoint_name}")
            
            # Send status update for slow endpoints
            if endpoint_name in slow_endpoints and status_callback:
                await status_callback(
                    "Processing large dataset",
                    f"Fetching all campaigns from {endpoint_name}. Please wait..."
                )
            
            try:
                # Build URL with path parameters
                url = endpoint['path']
                path_params = {}
                
                # Extract path parameters
                if '{customer_id}' in url:
                    path_params['customer_id'] = params.get('customer_id')
                    url = url.replace('{customer_id}', str(params['customer_id']))
                if '{property_id}' in url:
                    path_params['property_id'] = params.get('property_id')
                    url = url.replace('{property_id}', str(params['property_id']))
                if '{account_id}' in url:
                    path_params['account_id'] = params.get('account_id')
                    url = url.replace('{account_id}', str(params['account_id']))
                if '{page_id}' in url:
                    path_params['page_id'] = params.get('page_id')
                    url = url.replace('{page_id}', str(params['page_id']))
                
                # Validate required path parameters
                missing_params = [k for k, v in path_params.items() if v is None]
                if missing_params:
                    error_msg = f"Missing required parameters: {missing_params}"
                    logger.error(f"❌ {error_msg}")
                    results[endpoint_name] = {"error": error_msg}
                    continue
                
                # Prepare query parameters
                query_params = {}
                for param_name in endpoint['params']:
                    if param_name not in path_params and param_name in params:
                        if params[param_name] is not None:
                            query_params[param_name] = params[param_name]
                
                logger.info(f"🌐 URL: {url}")
                logger.info(f"📦 Query params: {query_params}")
                
                # Make API call with extended timeout for slow endpoints
                timeout = 120 if endpoint_name in slow_endpoints else 30
                
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
                    headers = {'Authorization': f'Bearer {token}'}
                    full_url = f"https://eyqi6vd53z.us-east-2.awsapprunner.com{url}"
                    
                    async with session.get(full_url, params=query_params, headers=headers) as response:
                        response_text = await response.text()
                        
                        if response.status == 200:
                            try:
                                data = json.loads(response_text)
                                logger.info(f"✅ Successfully fetched from {endpoint_name}")
                                
                                # For large datasets, log summary
                                if endpoint_name in slow_endpoints:
                                    campaign_count = len(data.get('campaigns', []))
                                    logger.info(f"📊 Retrieved {campaign_count} campaigns")
                                
                                results[endpoint_name] = data
                                
                                # Save to MongoDB
                                await self._save_endpoint_response(
                                    endpoint_name=endpoint_name,
                                    endpoint_path=url,
                                    params=params,
                                    response_data=data,
                                    user_email=user_email
                                )
                                
                            except json.JSONDecodeError as e:
                                error_msg = f"Invalid JSON from {endpoint_name}"
                                logger.error(f"❌ {error_msg}")
                                results[endpoint_name] = {"error": error_msg}
                        else:
                            error_msg = f"API call failed: Status {response.status}"
                            logger.error(f"❌ {error_msg}")
                            results[endpoint_name] = {"error": error_msg, "status": response.status}
                            
            except asyncio.TimeoutError:
                error_msg = f"Request timeout for {endpoint_name}"
                logger.error(f"❌ {error_msg}")
                results[endpoint_name] = {"error": error_msg}
            except Exception as e:
                error_msg = f"Error executing {endpoint_name}: {str(e)}"
                logger.error(f"❌ {error_msg}")
                results[endpoint_name] = {"error": error_msg}
        
        logger.info(f"\n✅ AGENT 5 COMPLETE - Executed {len(results)} endpoints")
        logger.info("="*80 + "\n")
        
        return results

    # =================
    # AGENT 6: Data Analyzer with Context Awareness
    # =================
    async def agent_analyze_data(
        self, 
        message: str, 
        data: Dict[str, Any], 
        module_type: ModuleType,
        conversation_history: List[Dict[str, str]] = None
    ) -> str:
        
        logger.info("="*80)
        logger.info("🔧 AGENT 6 : ANALYZER - STARTING")
        """Analyze data and generate insights with conversation awareness"""
        
        # Check for errors
        errors = [result['error'] for endpoint, result in data.items() if 'error' in result]
        if errors:
            return f"I encountered an issue retrieving the data: {', '.join(errors)}. Please try rephrasing your question."
        
        # Build conversation context OUTSIDE the f-string
        history_context = ""
        if conversation_history:
            recent_history = conversation_history[-6:]  # Last 3 exchanges
            history_lines = []
            for msg in recent_history:
                role = msg['role'].upper()
                content = msg['content']
                history_lines.append(f"{role}: {content}")
            history_context = "\n".join(history_lines)
        
        # Prepare data summary (limit size)
        data_summary = {}
        for endpoint, endpoint_data in data.items():
            if isinstance(endpoint_data, dict):
                # Summarize large datasets
                if 'campaigns' in endpoint_data and len(endpoint_data['campaigns']) > 20:
                    data_summary[endpoint] = {
                        'total_campaigns': len(endpoint_data['campaigns']),
                        'sample': endpoint_data['campaigns'][:5],
                        'status_summary': endpoint_data.get('status_summary', {}),
                        'note': 'Large dataset - showing summary'
                    }
                else:
                    data_summary[endpoint] = endpoint_data
            else:
                data_summary[endpoint] = endpoint_data
        
        # Convert data to JSON string OUTSIDE f-string
        data_json = json.dumps(data_summary, indent=2, default=str)[:8000]
        
        # Build the prompt with pre-formatted strings
        history_section = f"CONVERSATION HISTORY:\n{history_context}\n\n" if history_context else ""
        
        prompt = f"""You are a {module_type.value} analytics expert. Analyze this data and answer the user's question.

    {history_section}Current Question: "{message}"

    Available Data:
    {data_json}

    Instructions:
    1. Directly answer the user's question
    2. If this is a follow-up, acknowledge previous context
    3. Use specific numbers and metrics from the data
    4. Highlight key insights and trends
    5. Provide actionable recommendations where appropriate
    6. Format numbers properly (e.g., 1,234 or $1.5K)
    7. Be concise but comprehensive
    8. If data shows ALL campaigns, mention this is comprehensive data

    Format your response in a conversational, professional tone."""

        try:
            response = await self.openai_client.chat.completions.create(
                model="gpt-4-turbo-preview",  # ✅ Changed from "gpt-4" for consistency
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
                max_tokens=1500
            )
            logger.info(f"\n✅ AGENT 6 ANALYZER : COMPLETE")
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"Error analyzing data: {e}")
            return "I encountered an error while analyzing your data. Please try rephrasing your question."
    
    # =================
    # AGENT 7: Response Formatter
    # =================
    async def agent_format_response(self, analysis: str) -> str:

        logger.info("="*80)
        logger.info("🔧 AGENT 7: RESPONSE FORMATTER - STARTING")
        """Format the final response for optimal readability"""
        
        format_prompt = f"""Format this analytics response for better readability:

    {analysis}

    Instructions:
    1. Ensure proper paragraph breaks
    2. Use bullet points for lists (use • not *)
    3. Highlight key metrics naturally in the text
    4. Keep it conversational and easy to scan
    5. Remove any redundant formatting
    6. Ensure numbers are properly formatted

    Return the formatted response only, no explanations."""
        
        try:
            response = await self.openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": format_prompt}],
                temperature=0.3,
                max_tokens=1500
            )
            
            formatted = response.choices[0].message.content
            logger.info(f"\n✅ AGENT 7 RESPONSE FORMATTER : COMPLETE")
            return formatted.strip()
            
        except Exception as e:
            logger.error(f"Error formatting response: {e}")
            return analysis  # Return original if formatting fails

    # =================
    # Main Orchestrator
    # =================

    async def process_chat_message(
        self,
        chat_request: ChatRequest,
        user_email: str
    ) -> ChatResponse:
        """Process chat message with intelligent multi-agent workflow"""
        
        logger.info(f"🚀 Processing chat message for user: {user_email}")
        logger.info(f"💬 Message: '{chat_request.message}'")
        logger.info(f"📱 Module: {chat_request.module_type.value}")
        
        # Validate module is supported
        supported_modules = [
            ModuleType.GOOGLE_ADS,
            ModuleType.GOOGLE_ANALYTICS,
            ModuleType.META_ADS,
            ModuleType.FACEBOOK_ANALYTICS,
            ModuleType.INTENT_INSIGHTS
        ]
        
        if chat_request.module_type not in supported_modules:
            error_message = f"The {chat_request.module_type.value} module is currently under development. Please use Google Ads, Google Analytics, Meta Ads, Facebook Analytics, or Intent Insights."
            
            if chat_request.session_id:
                session_id = chat_request.session_id
            else:
                session_id = await self.create_or_get_simple_session(
                    user_email=user_email,
                    module_type=chat_request.module_type,
                    session_id=None
                )
            
            user_message = ChatMessage(
                role=MessageRole.USER,
                content=chat_request.message,
                timestamp=datetime.utcnow()
            )
            await self.add_message_to_simple_session(session_id, user_message)
            
            ai_message = ChatMessage(
                role=MessageRole.ASSISTANT,
                content=error_message,
                timestamp=datetime.utcnow()
            )
            await self.add_message_to_simple_session(session_id, ai_message)
            
            return ChatResponse(
                response=error_message,
                session_id=session_id,
                triggered_endpoint=None,
                endpoint_data=None,
                module_type=chat_request.module_type
            )
        
        # Handle session
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
                account_id=chat_request.context.get('account_id') if chat_request.context else None,
                page_id=chat_request.context.get('page_id') if chat_request.context else None,
                period=chat_request.period or "LAST_7_DAYS"
            )
        
        # Add user message to session
        user_message = ChatMessage(
            role=MessageRole.USER,
            content=chat_request.message,
            timestamp=datetime.utcnow()
        )
        await self.add_message_to_simple_session(session_id, user_message)
        
        # Get conversation history for context
        collection = self.db.chat_sessions
        session = await collection.find_one({"session_id": session_id})
        conversation_history = []
        if session and session.get("messages"):
            conversation_history = [
                {"role": msg["role"], "content": msg["content"]}
                for msg in session["messages"][-10:]  # Last 5 exchanges
            ]
        
        # Initialize response variables
        endpoint_data = {}
        ai_response = ""
        
        try:
            # ===== AGENT 1: Classify Query =====
            await self.send_status_update_to_frontend("Analyzing query", "Understanding your question...")
            classification = await self.agent_classify_query(chat_request.message)
            
            if classification['category'] == 'GENERAL':
                # Handle general chat
                general_prompt = f"""You are a friendly marketing analytics assistant. The user asked: "{chat_request.message}"

            This is a general question (not analytics-related). Respond naturally and helpfully.
            Keep your response brief and conversational."""
                
                response = await self.openai_client.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=[{"role": "user", "content": general_prompt}],
                    temperature=0.7,
                    max_tokens=500
                )
                
                ai_response = response.choices[0].message.content
                
            else:
                # ===== AGENT 2: Extract Time Period =====
                await self.send_status_update_to_frontend("Extracting time period", "Determining time range...")
                
                time_context = {
                    'period': chat_request.period,
                    'custom_dates': chat_request.context.get('custom_dates') if chat_request.context else None
                }
                
                time_period = await self.agent_extract_time_period(chat_request.message, time_context)
                
                # ===== AGENT 3: Identify Account =====
                await self.send_status_update_to_frontend("Identifying account", "Finding relevant account...")
                
                account_context = {
                    'customer_id': chat_request.customer_id,
                    'property_id': chat_request.property_id,
                    'account_id': chat_request.context.get('account_id') if chat_request.context else None,
                    'page_id': chat_request.context.get('page_id') if chat_request.context else None,
                }
                
                account_info = await self.agent_identify_account(
                    chat_request.message,
                    chat_request.module_type,
                    account_context
                )
                
                if account_info.get('needs_account_list'):
                    ai_response = account_info.get('clarification_message', 'Please specify which account to analyze.')
                else:
                    # ===== AGENT 4: Select Endpoints =====
                    await self.send_status_update_to_frontend("Selecting data sources", "Determining relevant data...")
                    
                    selected_endpoints = await self.agent_select_endpoints(
                        chat_request.message,
                        chat_request.module_type,
                        account_info,
                        conversation_history
                    )
                    
                    if not selected_endpoints:
                        ai_response = "I couldn't determine which data sources to use. Please try rephrasing your question."
                    else:
                        # ===== AGENT 5: Execute Endpoints =====
                        await self.send_status_update_to_frontend("Fetching data", f"Calling {len(selected_endpoints)} data sources...")
                        
                        # Build endpoint parameters
                        endpoint_params = {
                            'token': chat_request.context.get('token', '') if chat_request.context else '',
                            'period': time_period.get('period') or chat_request.period or 'LAST_7_DAYS',
                            'start_date': time_period.get('start_date'),
                            'end_date': time_period.get('end_date'),
                        }
                        
                        # Map account_id to correct parameter
                        resolved_account_id = account_info.get('account_id')
                        
                        if chat_request.module_type == ModuleType.GOOGLE_ADS:
                            endpoint_params['customer_id'] = resolved_account_id or chat_request.customer_id
                        elif chat_request.module_type == ModuleType.GOOGLE_ANALYTICS:
                            endpoint_params['property_id'] = resolved_account_id or chat_request.property_id
                        elif chat_request.module_type == ModuleType.META_ADS:
                            endpoint_params['account_id'] = resolved_account_id or chat_request.context.get('account_id')
                        elif chat_request.module_type == ModuleType.FACEBOOK_ANALYTICS:
                            endpoint_params['page_id'] = resolved_account_id or chat_request.context.get('page_id')
                        elif chat_request.module_type == ModuleType.INTENT_INSIGHTS:
                            endpoint_params['account_id'] = resolved_account_id or chat_request.context.get('account_id')
                        
                        # Execute endpoints with status callback
                        endpoint_data = await self.agent_execute_endpoints(
                            selected_endpoints,
                            endpoint_params,
                            user_email,
                            status_callback=self.send_status_update_to_frontend
                        )
                        
                        # ===== AGENT 6: Analyze Data =====
                        await self.send_status_update_to_frontend("Analyzing data", "Generating insights...")
                        
                        analysis = await self.agent_analyze_data(
                            chat_request.message,
                            endpoint_data,
                            chat_request.module_type,
                            conversation_history
                        )
                        
                        # ===== AGENT 7: Format Response =====
                        await self.send_status_update_to_frontend("Formatting response", "Finalizing answer...")
                        
                        ai_response = await self.agent_format_response(analysis)
            
        except Exception as e:
            logger.error(f"❌ ERROR in process_chat_message: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            ai_response = f"I apologize, but I encountered an error while processing your request. Please try rephrasing your question or contact support if the issue persists."
        
        # Add AI response to session
        ai_message = ChatMessage(
            role=MessageRole.ASSISTANT,
            content=ai_response,
            timestamp=datetime.utcnow()
        )
        await self.add_message_to_simple_session(session_id, ai_message)
        
        await self.send_status_update_to_frontend("Complete", "Analysis ready!")
        
        return ChatResponse(
            response=ai_response,
            session_id=session_id,
            triggered_endpoint=None,
            endpoint_data=endpoint_data,
            module_type=chat_request.module_type
        )


    def _log_agent_step(self, agent_name: str, status: str, details: Dict = None):
        """Consistent logging for agent steps"""
        logger.info(f"\n{'='*80}")
        logger.info(f"🤖 {agent_name} - {status}")
        if details:
            logger.info(f"Details: {json.dumps(details, default=str, indent=2)}")
        logger.info(f"{'='*80}\n")

    def _organize_endpoint_registry(self):
        """Organize endpoints by category for better selection"""
        self.endpoint_categories = {
            'google_ads': {
                'overview': ['get_ads_key_stats', 'get_ads_campaigns'],
                'performance': ['get_ads_performance', 'get_ads_keywords'],
                'analysis': ['get_ads_geographic', 'get_ads_device_performance', 'get_ads_time_performance'],
                'research': ['get_keyword_ideas']
            },
            'google_analytics': {
                'overview': ['get_ga_metrics', 'get_ga_properties'],
                'traffic': ['get_ga_traffic_sources', 'get_ga_channel_performance'],
                'behavior': ['get_ga_top_pages', 'get_ga_audience_insights'],
                'conversions': ['get_ga_conversions', 'generate_engagement_funnel_with_llm'],
                'revenue': ['get_revenue_breakdown_by_channel', 'get_revenue_breakdown_by_source'],
                'trends': ['get_ga_time_series', 'get_ga_trends']
            },
            'meta_ads': {
                'overview': ['get_meta_account_insights', 'get_campaigns_paginated'],
                'campaigns': ['get_meta_campaigns_timeseries', 'get_meta_campaigns_demographics'],
                'detailed': ['get_meta_campaigns_list'],  # Slow endpoint
                'adsets': ['get_adsets_by_campaigns', 'get_adsets_timeseries'],
                'ads': ['get_ads_by_adsets', 'get_ads_timeseries']
            },
            'facebook_analytics': {
                'overview': ['get_facebook_page_insights', 'get_facebook_pages'],
                'content': ['get_facebook_page_posts', 'get_facebook_engagement'],
                'audience': ['get_facebook_demographics', 'get_meta_follows_unfollows'],
                'performance': ['get_meta_organic_vs_paid', 'get_meta_video_views_breakdown']
            }
        }
        
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
        
        # Use endpoint data directly instead of MongoDB collections
        endpoint_data = context.get("endpoint_data", {})
        
        if endpoint_data:
            data_prompt = "\n\nYOUR REAL-TIME API DATA:\n"
            for endpoint_name, data in endpoint_data.items():
                data_prompt += f"\n=== {endpoint_name.upper().replace('_', ' ')} ===\n"
                data_prompt += f"{json.dumps(data, indent=2)[:5000]}\n"  # Limit size
        else:
            data_prompt = "\n\nNo API data available yet.\n"
        
        return base_prompt + f"\nCurrent Module: {module_type.value}\n" + data_prompt

    # def _normalize_period(self, period: str) -> str:
    #     """Normalize different period formats to a standard format"""
    #     period_mapping = {
    #         # Frontend formats
    #         'LAST_7_DAYS': '7d',
    #         'LAST_30_DAYS': '30d', 
    #         'LAST_90_DAYS': '90d',
    #         'LAST_365_DAYS': '365d',
    #         'LAST_3_MONTHS': '90d',
    #         'LAST_1_YEAR': '365d',

    #         # Analytics formats
    #         '7d': '7d',
    #         '30d': '30d',
    #         '90d': '90d', 
    #         '365d': '365d',
    #         '12m': '365d'
    #     }
    #     return period_mapping.get(period, period)

   
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


    async def send_status_update_to_frontend(self, status: str, details: str = ""):
        """Send status update (placeholder for WebSocket implementation)"""
        logger.info(f"STATUS: {status} - {details}")
        # TODO: Implement WebSocket for real-time updates
        return {"status": status, "details": details, "timestamp": datetime.utcnow()}
# Create singleton instance
chat_manager = ChatManager()