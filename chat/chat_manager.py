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
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            
            return json.loads(response.choices[0].message.content)
        except Exception as e:
            logger.error(f"Error in query classification: {e}")
            return {"category": "ANALYTICS", "confidence": 0.5, "reason": "Default to analytics"}

    # =================
    # AGENT 2: Time Period Extractor
    # =================
    async def agent_extract_time_period(self, message: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Extract time period from message or request it from user"""
        
        # Check if custom dates are in context
        if context.get('custom_dates'):
            return {
                'has_period': True,
                'period': 'CUSTOM',
                'start_date': context['custom_dates']['start_date'],
                'end_date': context['custom_dates']['end_date'],
                'needs_clarification': False
            }
        
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
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            
            # If no period found and not in context, need to ask user
            if not result['has_period'] and not context.get('period'):
                result['needs_clarification'] = True
                result['clarification_message'] = "What time period would you like to analyze? (e.g., last 7 days, last month, or specific dates)"
            
            return result
            
        except Exception as e:
            logger.error(f"Error extracting time period: {e}")
            return {
                'has_period': False,
                'period': None,
                'needs_clarification': True,
                'clarification_message': "What time period would you like to analyze?"
            }

    # =================
    # AGENT 3: Account Identifier
    # =================
    
    async def agent_identify_account(self, message: str, module_type: ModuleType, context: Dict[str, Any], token: str, user_email: str) -> Dict[str, Any]:
        """Identify which account/property the user is referring to"""
        
        logger.info("\n" + "="*80)
        logger.info("🔍 AGENT 3: ACCOUNT IDENTIFIER - STARTING")
        logger.info(f"Module: {module_type.value}")
        logger.info(f"Context received: {json.dumps(context, default=str, indent=2)}")
        logger.info(f"User message: {message}")
        
        # Skip for unsupported modules
        if module_type == ModuleType.INSTAGRAM_ANALYTICS:
            logger.warning("⚠️ Instagram Analytics not supported yet")
            return {
                'has_specific_reference': False,
                'reference_type': 'none',
                'account_name': None,
                'account_id': None,
                'use_active_account': False,
                'needs_account_list': False,
                'clarification_message': "Instagram Analytics module is currently under development."
            }
        
        prompt = f"""
        Identify the account or property the user is referring to in their message.
        Module type: {module_type.value}
        Current context: {json.dumps(context, default=str)}
        User Message: "{message}"

        Look for:
        - Specific account names or IDs mentioned
        - References like "this account", "my ads account"
        - If no specific mention, use active account from context

        Respond in JSON format:
        {{
            "has_specific_reference": true/false,
            "reference_type": "explicit" or "implicit" or "none",
            "account_name": "name if mentioned" or null,
            "use_active_account": true/false
        }}
        """

        try:
            response = await self.openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            logger.info(f"🤖 LLM Result: {json.dumps(result, indent=2)}")
            
            # Use active account from context if no specific reference
            if not result.get('has_specific_reference') or result.get('use_active_account'):
                logger.info("📌 Using active account from context")
                
                if module_type == ModuleType.GOOGLE_ADS:
                    result['account_id'] = context.get('customer_id')
                    logger.info(f"  - customer_id: {result['account_id']}")
                elif module_type == ModuleType.GOOGLE_ANALYTICS:
                    result['account_id'] = context.get('property_id')
                    logger.info(f"  - property_id: {result['account_id']}")
                elif module_type == ModuleType.META_ADS:
                    result['account_id'] = context.get('account_id')
                    logger.info(f"  - account_id: {result['account_id']}")
                elif module_type == ModuleType.FACEBOOK_ANALYTICS:
                    result['account_id'] = context.get('page_id')
                    logger.info(f"  - page_id: {result['account_id']}")
                
                if result.get('account_id'):
                    result['use_active_account'] = True
                    result['needs_account_list'] = False
                    result['clarification_message'] = None
                    logger.info(f"✅ Resolved account_id: {result['account_id']}")
                else:
                    logger.warning("⚠️ No account ID found in context")
                    result['needs_account_list'] = True
                    result['clarification_message'] = f"Please specify the {module_type.value} account to analyze."
            
            logger.info(f"✅ AGENT 3 COMPLETE")
            logger.info(f"Final result: {json.dumps(result, default=str, indent=2)}")
            logger.info("="*80 + "\n")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error in agent_identify_account: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    # =================
    # AGENT 4: Endpoint Selector
    # =================
    async def agent_select_endpoints(self, message: str, module_type: ModuleType, account_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Select relevant endpoints based on the query"""
        
        available_endpoints = self.endpoint_registry.get(module_type.value, [])
        
        prompt = f"""
        Select the most relevant API endpoints to answer this user query.
        
        User Query: "{message}"
        Module: {module_type.value}
        Account Info: {json.dumps(account_info)}
        Available Endpoints: {json.dumps(available_endpoints, indent=2)}

        Consider:
        - What data is needed to answer the question
        - Dependencies between endpoints (e.g., get list first, then details)
        - Minimum set of endpoints needed

        Respond with JSON array of selected endpoint names:
        {{
            "endpoints": ["endpoint_name1", "endpoint_name2"],
            "reasoning": "brief explanation"
        }}
        """

        try:
            response = await self.openai_client.chat.completions.create(
                model="gpt-4",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            selected = []
            
            for endpoint_name in result.get('endpoints', []):
                endpoint = next((e for e in available_endpoints if e['name'] == endpoint_name), None)
                if endpoint:
                    selected.append(endpoint)
            
            # If no endpoints selected, default to a key stats endpoint for analytics queries
            if not selected and module_type == ModuleType.GOOGLE_ADS:
                selected = [e for e in available_endpoints if e['name'] == 'get_ads_key_stats']
            
            return selected
            
        except Exception as e:
            logger.error(f"Error selecting endpoints: {e}")
            # Default to key stats endpoint for Google Ads
            if module_type == ModuleType.GOOGLE_ADS:
                return [e for e in available_endpoints if e['name'] == 'get_ads_key_stats']
            return []

    # =================
    # AGENT 5: Endpoint Executor
    # =================

    async def agent_execute_endpoints(self, endpoints: List[Dict[str, Any]], params: Dict[str, Any], user_email: str) -> Dict[str, Any]:
        """Execute selected endpoints and collect data"""
        
        logger.info("=" * 80)
        logger.info("🔧 AGENT 5: ENDPOINT EXECUTOR - STARTING")
        logger.info("=" * 80)
        logger.info(f"📋 Endpoints to execute: {[e['name'] for e in endpoints]}")
        logger.info(f"📦 Received params: {json.dumps(params, default=str, indent=2)}")
        logger.info(f"👤 User email: {user_email}")
        
        results = {}
        token = params.get('token', '')
        
        if not token:
            logger.error("❌ No token provided in params!")
            return {"error": "Authentication token missing"}
        
        for endpoint in endpoints:
            endpoint_name = endpoint['name']
            logger.info(f"\n{'='*60}")
            logger.info(f"🎯 Executing endpoint: {endpoint_name}")
            logger.info(f"📍 Path template: {endpoint['path']}")
            logger.info(f"📝 Required params: {endpoint['params']}")
            
            try:
                # Validate required path parameters
                path_params_needed = []
                if '{customer_id}' in endpoint['path']:
                    path_params_needed.append('customer_id')
                if '{property_id}' in endpoint['path']:
                    path_params_needed.append('property_id')
                if '{account_id}' in endpoint['path']:
                    path_params_needed.append('account_id')
                if '{page_id}' in endpoint['path']:
                    path_params_needed.append('page_id')
                
                logger.info(f"🔍 Path parameters needed: {path_params_needed}")
                
                # Check for missing parameters
                missing_params = []
                for param in path_params_needed:
                    param_value = params.get(param)
                    logger.info(f"  - {param}: {param_value} (type: {type(param_value).__name__})")
                    if param_value is None:
                        missing_params.append(param)
                
                if missing_params:
                    error_msg = f"Missing required parameters for {endpoint_name}: {missing_params}"
                    logger.error(f"❌ {error_msg}")
                    logger.error(f"Available params keys: {list(params.keys())}")
                    results[endpoint_name] = {"error": error_msg}
                    continue
                
                # Build URL with path parameters
                url = endpoint['path']
                for param in path_params_needed:
                    placeholder = f'{{{param}}}'
                    value = str(params[param])
                    url = url.replace(placeholder, value)
                    logger.info(f"✅ Replaced {placeholder} with {value}")
                
                logger.info(f"🌐 Final URL path: {url}")
                
                # Prepare query parameters (exclude path params)
                query_params = {}
                for param_name in endpoint['params']:
                    if param_name not in path_params_needed and param_name in params:
                        if params[param_name] is not None:
                            query_params[param_name] = params[param_name]
                
                logger.info(f"🔧 Query params: {query_params}")
                
                # Make API call
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    headers = {'Authorization': f'Bearer {token}'}
                    full_url = f"https://eyqi6vd53z.us-east-2.awsapprunner.com{url}"
                    
                    logger.info(f"📡 Making request to: {full_url}")
                    logger.info(f"📋 Query params: {query_params}")
                    
                    async with session.get(full_url, params=query_params, headers=headers) as response:
                        response_text = await response.text()
                        logger.info(f"📊 Response status: {response.status}")
                        
                        if response.status == 200:
                            try:
                                data = json.loads(response_text)
                                logger.info(f"✅ Successfully fetched data from {endpoint_name}")
                                logger.info(f"📦 Data keys: {list(data.keys()) if isinstance(data, dict) else 'List data'}")
                                results[endpoint_name] = data
                                
                                # Save to MongoDB
                                await self._save_endpoint_response(
                                    endpoint_name=endpoint_name,
                                    endpoint_path=url,
                                    params=params,
                                    response_data=data,
                                    user_email=user_email
                                )
                                logger.info(f"💾 Saved response to MongoDB")
                            except json.JSONDecodeError as e:
                                error_msg = f"Invalid JSON response from {endpoint_name}"
                                logger.error(f"❌ {error_msg}: {response_text[:200]}")
                                results[endpoint_name] = {"error": error_msg}
                        else:
                            error_msg = f"API call failed for {endpoint_name}: Status {response.status}"
                            logger.error(f"❌ {error_msg}")
                            logger.error(f"Response: {response_text[:500]}")
                            results[endpoint_name] = {"error": error_msg, "status": response.status}
                            
            except Exception as e:
                error_msg = f"Error executing endpoint {endpoint_name}: {str(e)}"
                logger.error(f"❌ {error_msg}")
                logger.error(f"Exception type: {type(e).__name__}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
                results[endpoint_name] = {"error": error_msg}
        
        logger.info("\n" + "="*80)
        logger.info(f"✅ AGENT 5 COMPLETE - Executed {len(results)} endpoints")
        logger.info(f"Results summary: {list(results.keys())}")
        logger.info("="*80 + "\n")
        
        return results
    
    # =================
    # AGENT 6: Data Analyzer
    # =================
    async def agent_analyze_data(self, message: str, data: Dict[str, Any], module_type: ModuleType) -> str:
        """Analyze the collected data and generate insights"""
        
        # Check for errors in the data
        errors = [result['error'] for endpoint, result in data.items() if 'error' in result]
        if errors:
            clarification_msg = "Please specify a valid account name or ID, or I can fetch a list of available accounts. Would you like me to do that?"
            return f"I'm sorry, but there was an issue retrieving the data: {', '.join(errors)}. {clarification_msg}"
        
        prompt = f"""
        You are a {module_type.value} analytics expert. Analyze this data and answer the user's question.
        
        User Question: "{message}"
        
        Available Data:
        {json.dumps(data, indent=2)[:10000]}  # Limit to 10k chars
        
        Provide a comprehensive answer that:
        1. Directly addresses the user's question
        2. Includes specific numbers and metrics from the data
        3. Highlights key insights and trends
        4. Provides actionable recommendations where appropriate
        5. Uses clear, professional language
        
        Format numbers properly (e.g., 1,234 instead of 1234, $1.5K instead of 1500)
        """

        try:
            response = await self.openai_client.chat.completions.create(
                model="gpt-4",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
                max_tokens=1500
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"Error analyzing data: {e}")
            return "I encountered an error while analyzing your data. Please specify a valid account name or ID, or contact the technical team."

    # =================
    # AGENT 7: Response Formatter
    # =================
    async def agent_format_response(self, analysis: str, data: Dict[str, Any], needs_visualization: bool = False) -> Dict[str, Any]:
        """Format the response with proper structure and visualizations if needed"""
        
        formatted_response = {
            'text': analysis,
            'visualizations': []
        }
        
        if needs_visualization:
            prompt = f"""
            Determine if this data would benefit from visualization (tables or charts).
            
            Analysis: {analysis[:500]}
            Data structure: {list(data.keys())}
            
            Respond in JSON:
            {{
                "needs_table": true/false,
                "needs_chart": true/false,
                "chart_type": "line" or "bar" or "pie" or null,
                "table_columns": ["col1", "col2"] or null
            }}
            """
            
            try:
                response = await self.openai_client.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.1,
                    response_format={"type": "json_object"}
                )
                
                viz_config = json.loads(response.choices[0].message.content)
                
                if viz_config.get('needs_table') or viz_config.get('needs_chart'):
                    formatted_response['visualizations'] = self._create_visualizations(data, viz_config)
                    
            except Exception as e:
                logger.error(f"Error creating visualizations: {e}")
        
        return formatted_response

    # =================
    # Main Orchestrator
    # =================

    async def process_chat_message(
        self,
        chat_request: ChatRequest,
        user_email: str
    ) -> ChatResponse:
        """Process chat message with intelligent agentic workflow"""
        
        logger.info(f"🚀 Processing chat message for user: {user_email}")
        
        # Status 1: Message received
        await self.send_status_update("Message received", "Processing your question...")
        
        # Handle session
        if chat_request.session_id:
            session_id = chat_request.session_id
            await self.db.chat_sessions.update_one(
                {"session_id": session_id},
                {"$set": {"last_activity": datetime.utcnow()}}
            )
        else:
            account_id = chat_request.context.get('account_id') if chat_request.context else None
            page_id = chat_request.context.get('page_id') if chat_request.context else None
            
            session_id = await self.create_or_get_simple_session(
                user_email=user_email,
                module_type=chat_request.module_type,
                session_id=None,
                customer_id=chat_request.customer_id,
                property_id=chat_request.property_id,
                account_id=account_id,
                page_id=page_id,
                period=chat_request.period or "LAST_7_DAYS"
            )
        
        # Add user message to session
        user_message = ChatMessage(
            role=MessageRole.USER,
            content=chat_request.message,
            timestamp=datetime.utcnow()
        )
        await self.add_message_to_simple_session(session_id, user_message)
        
        # === START: NEW AGENTIC WORKFLOW ===
        
        # Agent 1: Classify query
        await self.send_status_update("Analyzing query", "Understanding your question...")
        classification = await self.agent_classify_query(chat_request.message)
        
        if classification['category'] == 'GENERAL':
            # Handle general chat without API calls
            ai_response = await self._generate_enhanced_ai_response_simple(
                message=chat_request.message,
                context={},
                module_type=chat_request.module_type,
                session_id=session_id
            )
        else:
            # Agent 2: Extract time period
            await self.send_status_update("Extracting time period", "Determining time range...")
            time_period = await self.agent_extract_time_period(
                chat_request.message,
                {
                    'period': chat_request.period,
                    'custom_dates': chat_request.context.get('custom_dates') if chat_request.context else None
                }
            )
            
            if time_period.get('needs_clarification'):
                ai_response = time_period['clarification_message']
            else:
                # Agent 3: Identify account
                await self.send_status_update("Identifying account", "Finding relevant account...")
                account_info = await self.agent_identify_account(
                    chat_request.message,
                    chat_request.module_type,
                    {
                        'customer_id': chat_request.customer_id,
                        'property_id': chat_request.property_id,
                        'account_id': account_id,
                        'page_id': page_id
                    },
                    chat_request.context.get('token', '') if chat_request.context else '',
                    user_email
                )
                
                if account_info.get('needs_account_list'):
                    # Return account list to user
                    accounts = await self._fetch_account_list(
                        chat_request.module_type,
                        chat_request.context.get('token', '') if chat_request.context else '',
                        user_email
                    )
                    ai_response = f"{account_info.get('clarification_message', 'Please select an account:')}\n\nAvailable accounts:\n"
                    for acc in accounts:
                        ai_response += f"- {acc['name']} (ID: {acc['id']})\n"
                else:
                    # Agent 4: Select endpoints
                    await self.send_status_update("Selecting endpoints", "Determining data sources...")
                    logger.info("\n" + "="*80)
                    logger.info("🎯 AGENT 4: ENDPOINT SELECTOR - STARTING")
                    logger.info(f"Account info received: {json.dumps(account_info, default=str, indent=2)}")

                    selected_endpoints = await self.agent_select_endpoints(
                        chat_request.message,
                        chat_request.module_type,
                        account_info
                    )

                    logger.info(f"Selected endpoints: {[e['name'] for e in selected_endpoints]}")

                    if not selected_endpoints:
                        ai_response = "I couldn't determine which data sources to use for your question. Please try rephrasing your question."
                    else:
                        # Agent 5: Execute endpoints
                        await self.send_status_update("Fetching data", f"Calling {len(selected_endpoints)} APIs...")
                        
                        # FIX: Build endpoint_params correctly based on module type
                        logger.info("\n" + "="*80)
                        logger.info("🔧 BUILDING ENDPOINT PARAMETERS")
                        logger.info(f"Module type: {chat_request.module_type.value}")
                        logger.info(f"Account info account_id: {account_info.get('account_id')}")
                        logger.info(f"Context customer_id: {chat_request.customer_id}")
                        logger.info(f"Context property_id: {chat_request.property_id}")
                        
                        endpoint_params = {
                            'token': chat_request.context.get('token', '') if chat_request.context else '',
                            'period': time_period.get('period') or chat_request.period or 'LAST_7_DAYS',
                            'start_date': time_period.get('start_date'),
                            'end_date': time_period.get('end_date'),
                        }
                        
                        # Map account_id to the correct parameter based on module type
                        resolved_account_id = account_info.get('account_id')
                        
                        if chat_request.module_type == ModuleType.GOOGLE_ADS:
                            endpoint_params['customer_id'] = resolved_account_id or chat_request.customer_id
                            logger.info(f"✅ Set customer_id: {endpoint_params['customer_id']}")
                            
                        elif chat_request.module_type == ModuleType.GOOGLE_ANALYTICS:
                            endpoint_params['property_id'] = resolved_account_id or chat_request.property_id
                            logger.info(f"✅ Set property_id: {endpoint_params['property_id']}")
                            
                        elif chat_request.module_type == ModuleType.META_ADS:
                            endpoint_params['account_id'] = resolved_account_id or (chat_request.context.get('account_id') if chat_request.context else None)
                            logger.info(f"✅ Set account_id: {endpoint_params['account_id']}")
                            
                        elif chat_request.module_type == ModuleType.FACEBOOK_ANALYTICS:
                            endpoint_params['page_id'] = resolved_account_id or (chat_request.context.get('page_id') if chat_request.context else None)
                            logger.info(f"✅ Set page_id: {endpoint_params['page_id']}")
                            
                        elif chat_request.module_type == ModuleType.INSTAGRAM_ANALYTICS:
                            endpoint_params['account_id'] = resolved_account_id or (chat_request.context.get('account_id') if chat_request.context else None)
                            logger.info(f"✅ Set account_id: {endpoint_params['account_id']}")
                        
                        logger.info(f"📦 Final endpoint_params: {json.dumps(endpoint_params, default=str, indent=2)}")
                        logger.info("="*80 + "\n")
                        
                        # Validate critical parameters before calling
                        critical_param = None
                        if chat_request.module_type == ModuleType.GOOGLE_ADS:
                            critical_param = endpoint_params.get('customer_id')
                            param_name = 'customer_id'
                        elif chat_request.module_type == ModuleType.GOOGLE_ANALYTICS:
                            critical_param = endpoint_params.get('property_id')
                            param_name = 'property_id'
                        elif chat_request.module_type in [ModuleType.META_ADS, ModuleType.INSTAGRAM_ANALYTICS]:
                            critical_param = endpoint_params.get('account_id')
                            param_name = 'account_id'
                        elif chat_request.module_type == ModuleType.FACEBOOK_ANALYTICS:
                            critical_param = endpoint_params.get('page_id')
                            param_name = 'page_id'
                        
                        if not critical_param:
                            logger.error(f"❌ Critical parameter '{param_name}' is None!")
                            logger.error(f"Chat request: customer_id={chat_request.customer_id}, property_id={chat_request.property_id}")
                            logger.error(f"Account info: {account_info}")
                            ai_response = f"I couldn't identify which {chat_request.module_type.value} account to analyze. Please specify the account name or ID."
                        else:
                            endpoint_data = await self.agent_execute_endpoints(
                                selected_endpoints,
                                endpoint_params,
                                user_email
                            )
                            
                            # Rest of the analysis...
                    
                    # Agent 6: Analyze data
                    await self.send_status_update("Analyzing data", "Generating insights...")
                    analysis = await self.agent_analyze_data(
                        chat_request.message,
                        endpoint_data,
                        chat_request.module_type
                    )
                    
                    # Agent 7: Format response
                    await self.send_status_update("Formatting response", "Finalizing answer...")
                    formatted_response = await self.agent_format_response(
                        analysis,
                        endpoint_data,
                        needs_visualization=True
                    )
                    
                    ai_response = formatted_response['text']
        
        # === END: NEW AGENTIC WORKFLOW ===
        
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

# Create singleton instance
chat_manager = ChatManager()