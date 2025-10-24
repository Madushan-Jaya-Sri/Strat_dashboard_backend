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
from typing import List, Dict, Any, Optional, Tuple
import traceback

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
                {'name': 'get_meta_campaigns_chat', 'path': '/api/meta/ad-accounts/{account_id}/campaigns/chat', 'params': ['account_id']},
                {'name': 'get_meta_campaigns_all', 'path': '/api/meta/ad-accounts/{account_id}/campaigns/all', 'params': ['account_id', 'period', 'start_date', 'end_date']},
                {'name': 'get_meta_campaigns_list', 'path': '/api/meta/ad-accounts/{account_id}/campaigns/list', 'params': ['account_id', 'status']},
                # For POST endpoints with body params, period should be optional and handled specially
                {'name': 'get_meta_campaigns_timeseries', 'path': '/api/meta/campaigns/timeseries', 'method': 'POST', 'params': ['start_date', 'end_date'], 'optional_params': ['period'], 'body_params': ['campaign_ids']},
                {'name': 'get_meta_adsets', 'path': '/api/meta/campaigns/adsets', 'method': 'POST', 'params': ['start_date', 'end_date'], 'optional_params': ['period'], 'body_params': ['campaign_ids']},
                {'name': 'get_campaigns_paginated', 'path': '/api/meta/ad-accounts/{account_id}/campaigns/paginated', 'params': ['account_id', 'period', 'start_date', 'end_date', 'limit', 'offset']},
                {'name': 'get_campaigns_demographics', 'path': '/api/meta/campaigns/demographics', 'method': 'POST', 'params': ['start_date', 'end_date'], 'optional_params': ['period'], 'body_params': ['campaign_ids']},
                {'name': 'get_campaigns_placements', 'path': '/api/meta/campaigns/placements', 'method': 'POST', 'params': ['start_date', 'end_date'], 'optional_params': ['period'], 'body_params': ['campaign_ids']},
                {'name': 'get_adsets_timeseries', 'path': '/api/meta/adsets/timeseries', 'method': 'POST', 'params': ['start_date', 'end_date'], 'optional_params': ['period'], 'body_params': ['adset_ids']},
                {'name': 'get_adsets_demographics', 'path': '/api/meta/adsets/demographics', 'method': 'POST', 'params': ['start_date', 'end_date'], 'optional_params': ['period'], 'body_params': ['adset_ids']},
                {'name': 'get_adsets_placements', 'path': '/api/meta/adsets/placements', 'method': 'POST', 'params': ['start_date', 'end_date'], 'optional_params': ['period'], 'body_params': ['adset_ids']},
                {'name': 'get_ads_by_adsets', 'path': '/api/meta/adsets/ads', 'method': 'POST', 'params': [], 'body_params': ['adset_ids']},
                {'name': 'get_ads_timeseries', 'path': '/api/meta/ads/timeseries', 'method': 'POST', 'params': ['start_date', 'end_date'], 'optional_params': ['period'], 'body_params': ['ad_ids']},
                {'name': 'get_ads_demographics', 'path': '/api/meta/ads/demographics', 'method': 'POST', 'params': ['start_date', 'end_date'], 'optional_params': ['period'], 'body_params': ['ad_ids']},
                {'name': 'get_ads_placements', 'path': '/api/meta/ads/placements', 'method': 'POST', 'params': ['start_date', 'end_date'], 'optional_params': ['period'], 'body_params': ['ad_ids']},
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
                {'name': 'get_intent_keyword_insights', 'path': '/api/intent/keyword-insights/{account_id}', 'method': 'POST', 'params': ['account_id'], 'body_params': ['seed_keywords', 'country', 'timeframe', 'start_date', 'end_date', 'include_zero_volume'], 'description': 'Get keyword insights and suggestions'},
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


    async def _get_token_for_module(self, user_email: str, module_type: ModuleType, auth_manager) -> Optional[str]:
        """Get authentication token based on module type"""
        try:
            if module_type in [ModuleType.META_ADS, ModuleType.FACEBOOK_ANALYTICS, ModuleType.INSTAGRAM]:
                # For Meta modules, get Facebook token
                token = auth_manager.get_facebook_token(user_email)
                if not token:
                    logger.error(f"❌ No Facebook token found for user: {user_email}")
                    return None
                logger.info(f"✅ Retrieved Facebook token for user: {user_email}")
                return token
            else:
                # For Google modules, get Google token
                token = auth_manager.get_google_token(user_email)
                if not token:
                    logger.error(f"❌ No Google token found for user: {user_email}")
                    return None
                logger.info(f"✅ Retrieved Google token for user: {user_email}")
                return token
        except Exception as e:
            logger.error(f"❌ Error getting token: {str(e)}")
            return None

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
        """Extract time period from message or use context with proper format conversion"""
        
        self._log_agent_step("AGENT 2: TIME EXTRACTOR", "STARTING", {"message": message[:100]})
        
        # Get current date for calculations
        from datetime import datetime, timedelta
        current_date = datetime.now()
        current_date_str = current_date.strftime('%Y-%m-%d')
        
        # Try to extract time period from the user's message FIRST
        prompt = f"""Extract time period information from this message and convert to standard formats.

    Current date: {current_date_str}

    User Message: "{message}"

    IMPORTANT RULES:
    1. First check if the user EXPLICITLY mentions a time period in their message
    2. If a specific date range is mentioned (like "2024-12-20 to 2025-01-13"), extract it as CUSTOM
    3. If standard period mentioned (7 days, 30 days, etc.), use predefined format
    4. If NO time period is mentioned at all, return has_period: false

    Standard periods (use these if mentioned):
    - "last 7 days", "past week" → LAST_7_DAYS
    - "last 30 days", "last month", "past month" → LAST_30_DAYS  
    - "last 3 months", "last quarter", "last 90 days" → LAST_90_DAYS
    - "last year", "last 12 months", "last 365 days" → LAST_365_DAYS

    Custom date ranges (extract dates):
    - "from 2024-12-20 to 2025-01-13" → period: CUSTOM, start_date: 2024-12-20, end_date: 2025-01-13
    - "between Jan 1 and Jan 31" → Convert to YYYY-MM-DD format
    - "last 2 months" → Calculate dates: period: CUSTOM, start_date, end_date
    - "December 2024" → period: CUSTOM, start_date: 2024-12-01, end_date: 2024-12-31

    Examples:
    Input: "What is my performance?"
    Output: {{"has_period": false, "reason": "No time period mentioned"}}

    Input: "Show me last 7 days"
    Output: {{"has_period": true, "period": "LAST_7_DAYS", "extracted_from": "message"}}

    Input: "Performance from 2024-12-20 to 2025-01-13"
    Output: {{
    "has_period": true, 
    "period": "CUSTOM",
    "start_date": "2024-12-20",
    "end_date": "2025-01-13",
    "extracted_from": "message"
    }}

    Input: "Give me data for last 2 months"
    Output: {{
    "has_period": true, 
    "period": "CUSTOM",
    "start_date": "{(current_date - timedelta(days=60)).strftime('%Y-%m-%d')}",
    "end_date": "{current_date_str}",
    "extracted_from": "message"
    }}

    Respond in JSON format:
    {{
        "has_period": true/false,
        "period": "LAST_7_DAYS" or "LAST_30_DAYS" or "LAST_90_DAYS" or "LAST_365_DAYS" or "CUSTOM" or null,
        "start_date": "YYYY-MM-DD" or null,
        "end_date": "YYYY-MM-DD" or null,
        "extracted_text": "the time phrase found" or null,
        "needs_clarification": false,
        "extracted_from": "message",
        "reason": "brief explanation"
    }}"""

        try:
            response = await self.openai_client.chat.completions.create(
                model="gpt-4-turbo-preview",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            
            # PRIORITY 1: If period found IN MESSAGE, use it (this overrides everything)
            if result.get('has_period') and result.get('extracted_text'):
                logger.info(f"✅ Time period extracted FROM MESSAGE: {result}")
                result['extracted_from'] = 'message'
                self._log_agent_step("AGENT 2: TIME EXTRACTOR", "COMPLETE", result)
                return result
            
            # PRIORITY 2: Check if custom dates are in context (from filter only when period is CUSTOM)
            if context.get('custom_dates') and context['custom_dates'].get('startDate') and context['custom_dates'].get('endDate'):
                logger.info(f"⚙️ No period in message, using custom dates from FILTER: {context['custom_dates']}")
                return {
                    'has_period': True,
                    'period': 'CUSTOM',
                    'start_date': context['custom_dates']['startDate'],
                    'end_date': context['custom_dates']['endDate'],
                    'needs_clarification': False,
                    'extracted_from': 'context_custom_dates',
                    'reason': 'Using custom date range from module filter'
                }
            
            # PRIORITY 3: If no period in message, check context for standard period
            if context.get('period') and context['period'] != 'CUSTOM':
                logger.info(f"⚙️ No time period in message, using period from FILTER: {context['period']}")
                return {
                    'has_period': True,
                    'period': context['period'],
                    'start_date': None,
                    'end_date': None,
                    'needs_clarification': False,
                    'extracted_from': 'context_filter',
                    'reason': 'Using module filter period as no period mentioned in message'
                }
            
            # PRIORITY 4: Default to LAST_7_DAYS if nothing found
            logger.info("⚙️ No period found anywhere, using LAST_7_DAYS as default")
            result = {
                'has_period': True,
                'period': 'LAST_7_DAYS',
                'start_date': None,
                'end_date': None,
                'needs_clarification': False,
                'extracted_from': 'default',
                'reason': 'Default period applied'
            }
            
            self._log_agent_step("AGENT 2: TIME EXTRACTOR", "COMPLETE", result)
            return result
            
        except Exception as e:
            logger.error(f"❌ Error extracting time period: {e}")
            # Use context period on error, not default
            if context.get('custom_dates') and context['custom_dates'].get('startDate') and context['custom_dates'].get('endDate'):
                return {
                    'has_period': True,
                    'period': 'CUSTOM',
                    'start_date': context['custom_dates']['startDate'],
                    'end_date': context['custom_dates']['endDate'],
                    'needs_clarification': False,
                    'extracted_from': 'error_fallback_custom'
                }
            if context.get('period'):
                return {
                    'has_period': True,
                    'period': context['period'],
                    'start_date': None,
                    'end_date': None,
                    'needs_clarification': False,
                    'extracted_from': 'error_fallback_context'
                }
            return {
                'has_period': True,
                'period': 'LAST_7_DAYS',
                'needs_clarification': False,
                'extracted_from': 'error_fallback_default'
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
            # ✅ For Intent Insights, check multiple possible keys
            account_id = context.get('account_id') or context.get('selectedAccount')
            
            logger.info(f"Intent Insights - account_id from context: {account_id}")
            
            if account_id:
                # User has an active account selected
                result = {
                    'has_specific_reference': False,
                    'reference_type': 'active_account',
                    'account_id': str(account_id),
                    'use_active_account': True,
                    'needs_account_list': False
                }
                logger.info(f"✅ AGENT 3 COMPLETE: {result}")
                logger.info("="*80 + "\n")
                return result
            else:
                # No account in context
                result = {
                    'has_specific_reference': False,
                    'reference_type': 'none',
                    'account_id': None,
                    'use_active_account': False,
                    'needs_account_list': False,
                    'clarification_message': 'Please select an account from the Intent Insights module to continue.'
                }
                logger.info(f"⚠️ AGENT 3 COMPLETE (No account): {result}")
                logger.info("="*80 + "\n")
                return result
        
        # ✅ Build result for other modules
        result = {
            'has_specific_reference': bool(account_id),
            'reference_type': 'context' if account_id else 'none',
            'account_id': str(account_id) if account_id else None,  # ✅ Convert to string
            'use_active_account': bool(account_id),
            'needs_account_list': not bool(account_id)
        }
        
        if not account_id:
            result['clarification_message'] = f"Please specify the {module_type.value} account to analyze."
        
        logger.info(f"✅ AGENT 3 COMPLETE: {result}")
        logger.info("="*80 + "\n")
        
        return result

    # =================
    # AGENT 3.5: country and keywords Identifier for intent insights
    # =================

    async def agent_extract_country_and_keywords(
        self,
        message: str,
        module_type: str,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Agent 3.5: Extract country and keywords for Intent Insights module
        Only runs for intent_insights module
        """
        try:
            if module_type != "intent_insights":
                return {
                    "country": None,
                    "keywords": [],
                    "needs_country_clarification": False,
                    "needs_keyword_clarification": False
                }
            
            logger.info("="*80)
            logger.info("🌍 AGENT 3.5: COUNTRY & KEYWORD EXTRACTOR - STARTING")
            logger.info(f"Module: {module_type}")
            logger.info(f"Message: {message}")
            logger.info("="*80)
            
            # List of valid countries
            valid_countries = [
                "World Wide", "Afghanistan", "Albania", "Algeria", "Andorra", "Angola",
                "Argentina", "Armenia", "Australia", "Austria", "Azerbaijan", "Bahamas",
                "Bahrain", "Bangladesh", "Barbados", "Belarus", "Belgium", "Belize",
                "Benin", "Bhutan", "Bolivia", "Bosnia and Herzegovina", "Botswana",
                "Brazil", "Brunei", "Bulgaria", "Burkina Faso", "Burundi", "Cambodia",
                "Cameroon", "Canada", "Cape Verde", "Central African Republic", "Chad",
                "Chile", "China", "Colombia", "Comoros", "Congo", "Costa Rica",
                "Croatia", "Cuba", "Cyprus", "Czech Republic", "Denmark", "Djibouti",
                "Dominica", "Dominican Republic", "Ecuador", "Egypt", "El Salvador",
                "Equatorial Guinea", "Eritrea", "Estonia", "Eswatini", "Ethiopia",
                "Fiji", "Finland", "France", "Gabon", "Gambia", "Georgia", "Germany",
                "Ghana", "Greece", "Grenada", "Guatemala", "Guinea", "Guinea-Bissau",
                "Guyana", "Haiti", "Honduras", "Hungary", "Iceland", "India",
                "Indonesia", "Iran", "Iraq", "Ireland", "Israel", "Italy", "Jamaica",
                "Japan", "Jordan", "Kazakhstan", "Kenya", "Kiribati", "Kuwait",
                "Kyrgyzstan", "Laos", "Latvia", "Lebanon", "Lesotho", "Liberia",
                "Libya", "Liechtenstein", "Lithuania", "Luxembourg", "Madagascar",
                "Malawi", "Malaysia", "Maldives", "Mali", "Malta", "Marshall Islands",
                "Mauritania", "Mauritius", "Mexico", "Micronesia", "Moldova", "Monaco",
                "Mongolia", "Montenegro", "Morocco", "Mozambique", "Myanmar", "Namibia",
                "Nauru", "Nepal", "Netherlands", "New Zealand", "Nicaragua", "Niger",
                "Nigeria", "North Korea", "North Macedonia", "Norway", "Oman",
                "Pakistan", "Palau", "Palestine", "Panama", "Papua New Guinea",
                "Paraguay", "Peru", "Philippines", "Poland", "Portugal", "Qatar",
                "Romania", "Russia", "Rwanda", "Saint Kitts and Nevis", "Saint Lucia",
                "Saint Vincent and the Grenadines", "Samoa", "San Marino",
                "Sao Tome and Principe", "Saudi Arabia", "Senegal", "Serbia",
                "Seychelles", "Sierra Leone", "Singapore", "Slovakia", "Slovenia",
                "Solomon Islands", "Somalia", "South Africa", "South Korea",
                "South Sudan", "Spain", "Sri Lanka", "Sudan", "Suriname", "Sweden",
                "Switzerland", "Syria", "Taiwan", "Tajikistan", "Tanzania", "Thailand",
                "Timor-Leste", "Togo", "Tonga", "Trinidad and Tobago", "Tunisia",
                "Turkey", "Turkmenistan", "Tuvalu", "Uganda", "Ukraine",
                "United Arab Emirates", "United Kingdom", "United States", "Uruguay",
                "Uzbekistan", "Vanuatu", "Vatican City", "Venezuela", "Vietnam",
                "Yemen", "Zambia", "Zimbabwe"
            ]
            
            prompt = f"""
            Analyze this user message and extract:
            1. Country/Region mentioned (if any)
            2. Keywords mentioned (if any)
            
            User message: "{message}"
            
            Valid countries: {', '.join(valid_countries[:20])}... (and more)
            
            Return JSON:
            {{
                "country": "extracted country name or null",
                "keywords": ["keyword1", "keyword2"],
                "needs_country_clarification": true/false,
                "needs_keyword_clarification": true/false,
                "reasoning": "explanation"
            }}
            
            Rules:
            - If country is mentioned, extract it (must match valid country list)
            - If no country mentioned, set needs_country_clarification to true
            - If specific keywords mentioned, extract them
            - If no keywords mentioned, set needs_keyword_clarification to true
            - Default country is "World Wide" only if user says "worldwide" or "global"
            """
            
            response = await self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            logger.info(f"✅ Extraction result: {result}")
            
            # Check if we have saved keywords in context
            context_keywords = context.get("keywords", [])
            if context_keywords and not result.get("keywords"):
                result["keywords"] = context_keywords
                result["needs_keyword_clarification"] = False
                logger.info(f"📋 Using keywords from context: {context_keywords}")
            
            logger.info("="*80)
            logger.info("✅ AGENT 3.5: COUNTRY & KEYWORD EXTRACTOR - COMPLETE")
            logger.info(f"Country: {result.get('country')}")
            logger.info(f"Keywords: {result.get('keywords')}")
            logger.info(f"Needs country clarification: {result.get('needs_country_clarification')}")
            logger.info(f"Needs keyword clarification: {result.get('needs_keyword_clarification')}")
            logger.info("="*80)
            
            return result
            
        except Exception as e:
            logger.error(f"❌ ERROR in agent_extract_country_and_keywords: {str(e)}")
            logger.error(traceback.format_exc())
            return {
                "country": "World Wide",
                "keywords": [],
                "needs_country_clarification": False,
                "needs_keyword_clarification": False,
                "error": str(e)
            }


    # =================
    # AGENT 4: Endpoint Selector
    # =================
    async def agent_select_endpoints(
        self, 
        message: str, 
        module_type: ModuleType, 
        account_info: Dict[str, Any],
        conversation_history: List[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Select relevant endpoints based on the query"""
        
        logger.info("\n" + "="*80)
        logger.info("🔍 AGENT 4: ENDPOINT SELECTOR - STARTING")
        logger.info(f"Module: {module_type.value}")
        logger.info(f"Message: {message}")
        
        available_endpoints = self.endpoint_registry.get(module_type.value, [])
        
        # Build conversation context
        history_context = ""
        if conversation_history:
            recent_history = conversation_history[-4:]
            history_lines = []
            for msg in recent_history:
                role = msg['role'].upper()
                content = msg['content']
                history_lines.append(f"{role}: {content}")
            history_context = "\n".join(history_lines)
        
        # ============================================
        # SPECIAL HANDLING FOR INTENT INSIGHTS MODULE
        # ============================================
        if module_type.value == 'intent_insights':
            logger.info("🔍 Processing Intent Insights module")
            
            # Intent Insights prompt - IMPROVED
            prompt = f"""You are a keyword research specialist extracting seed keywords for Intent Insights.

        Current Query: "{message}"
        Account ID: {account_info.get('account_id')}

        **Your Task:**
        Extract seed keywords that represent what the user wants to research. Be intelligent about extraction.

        **Extraction Rules:**
        1. If user says "my industry" or "our industry" → Extract general industry terms like "industry", "business", "market"
        2. If user mentions specific industry (e.g., "cosmetics industry") → Extract that industry name
        3. If user asks "trending keywords" → Use "trending", "popular", "top" as seed keywords
        4. If user mentions products/services → Extract those terms
        5. If user specifies country/region → Extract it, otherwise use "World Wide"
        6. ALWAYS try to extract at least 1-2 seed keywords from context

        **Examples:**
        - "trending keywords in my industry" → {{"extracted_keywords": ["trending", "industry"], "country": "World Wide"}}
        - "cosmetics industry keywords" → {{"extracted_keywords": ["cosmetics"], "country": "World Wide"}}
        - "beauty products in USA" → {{"extracted_keywords": ["beauty products"], "country": "United States"}}
        - "high volume keywords for skincare" → {{"extracted_keywords": ["skincare"], "country": "World Wide"}}
        - "what keywords are popular" → {{"extracted_keywords": ["popular", "keywords"], "country": "World Wide"}}

        **CRITICAL:** Even for vague queries, extract relevant seed keywords. Don't ask for clarification unless query is completely unrelated to keywords.

        Return JSON:
        {{
            "selected_endpoints": ["intent_keyword_insights"],
            "extracted_keywords": ["keyword1", "keyword2"],
            "country": "World Wide",
            "reasoning": "Extracted keywords based on context",
            "needs_clarification": false,
            "clarification_message": ""
        }}

        Only set needs_clarification to true if the query is completely unrelated to keyword research.
        """
            
            try:
                response = await self.openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.1,
                    response_format={"type": "json_object"}
                )
                
                result = json.loads(response.choices[0].message.content)
                
                logger.info(f"Intent Insights extraction result: {json.dumps(result, indent=2)}")
                
                # ✅ IMPROVED: Only ask for clarification if truly needed
                extracted_keywords = result.get('extracted_keywords', [])
                
                if result.get('needs_clarification', False) or len(extracted_keywords) == 0:
                    logger.info("⚠️ Needs clarification for keywords")
                    return {
                        'selected_endpoints': [],
                        'endpoint_configs': [],
                        'extracted_keywords': [],
                        'country': 'World Wide',
                        'needs_clarification': True,
                        'clarification_message': result.get('clarification_message', 
                            'Please specify the keywords, industry, or topic you want to research (e.g., "cosmetics", "beauty products", "skincare", "technology").'),
                        'reasoning': result.get('reasoning', 'No keywords found')
                    }
                
                # ✅ Return successful result with keywords
                endpoint_config = next((e for e in available_endpoints if e['name'] == 'intent_keyword_insights'), None)
                
                logger.info(f"✅ Successfully extracted {len(extracted_keywords)} keywords: {extracted_keywords}")
                
                return {
                    'selected_endpoints': ['intent_keyword_insights'],
                    'endpoint_configs': [endpoint_config] if endpoint_config else [],
                    'extracted_keywords': extracted_keywords,
                    'country': result.get('country', 'World Wide'),
                    'needs_clarification': False,
                    'reasoning': result.get('reasoning', 'Keywords extracted successfully')
                }
                
            except Exception as e:
                logger.error(f"❌ Error in Intent Insights endpoint selection: {str(e)}")
                return {
                    'selected_endpoints': [],
                    'endpoint_configs': [],
                    'extracted_keywords': [],
                    'country': 'World Wide',
                    'needs_clarification': True,
                    'clarification_message': 'An error occurred. Please try specifying the industry or keywords you want to research.',
                    'reasoning': f'Error: {str(e)}'
                }
        
        # ============================================
        # STANDARD HANDLING FOR OTHER MODULES
        # ============================================
        
        # Prepare endpoints info
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
    2. For overview questions, select key metrics endpoints
    3. For specific questions, select targeted endpoints
    4. Consider follow-up context - don't repeat data already provided
    5. For comparison questions, select endpoints that provide comparative data
    6. META ADS SPECIAL RULES:
    - Use 'get_meta_campaigns_chat' for listing campaigns (FAST)
    - ONLY use 'get_meta_campaigns_all' if user explicitly asks for performance metrics (SLOW, 2+ min)
    - Never select 'get_meta_campaigns_all' for simple list/overview questions

    Respond with JSON:
    {{
        "selected_endpoints": ["endpoint_name1", "endpoint_name2"],
        "reasoning": "brief explanation",
        "is_followup": true/false
    }}"""

        try:
            response = await self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            selected_configs = []
            
            for endpoint_name in result.get('selected_endpoints', []):
                endpoint = next((e for e in available_endpoints if e['name'] == endpoint_name), None)
                if endpoint:
                    selected_configs.append(endpoint)
            
            logger.info(f"✅ Selected {len(selected_configs)} endpoints: {[e['name'] for e in selected_configs]}")
            logger.info(f"Reasoning: {result.get('reasoning')}")
            
            # Fallback to default endpoints if none selected
            if not selected_configs:
                logger.warning("⚠️ No endpoints selected, using defaults")
                selected_configs = self._get_default_endpoints(module_type, available_endpoints)
            
            logger.info(f"✅ AGENT 4 COMPLETE")
            logger.info("="*80 + "\n")
            
            return {
                'selected_endpoints': [e['name'] for e in selected_configs],
                'endpoint_configs': selected_configs,
                'reasoning': result.get('reasoning', ''),
                'is_followup': result.get('is_followup', False),
                'needs_clarification': False
            }
            
        except Exception as e:
            logger.error(f"❌ Error selecting endpoints: {str(e)}")
            default_configs = self._get_default_endpoints(module_type, available_endpoints)
            return {
                'selected_endpoints': [e['name'] for e in default_configs],
                'endpoint_configs': default_configs,
                'reasoning': f'Error occurred, using defaults: {str(e)}',
                'needs_clarification': False
            }


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
            'get_meta_campaigns_chat': 'FAST: Get list of ALL campaigns (name, status, ID) for overview questions. Use this for "show campaigns", "list campaigns", "active campaigns"',
            'get_meta_campaigns_all': 'SLOW (2+ min): Get ALL campaigns WITH performance metrics. Only use if user explicitly asks for metrics/performance data',
            'get_meta_campaigns_timeseries': 'Get campaign performance over time (needs campaign_ids)',
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
            'intent_keyword_insights': 'Search keyword insights showing search volume, CPC, competition trends, and keyword suggestions for market research.',

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
            ModuleType.GOOGLE_ADS: ['google_ads_key_stats'],
            ModuleType.GOOGLE_ANALYTICS: ['google_analytics_metrics'],
            ModuleType.META_ADS: ['meta_account_insights'],
            ModuleType.FACEBOOK_ANALYTICS: ['facebook_page_insights'],
            ModuleType.INTENT_INSIGHTS: ['intent_keyword_insights'],
        }
        
        default_names = defaults.get(module_type, [])
        return [e for e in available_endpoints if e['name'] in default_names]

    def _convert_period_for_module(self, period: str, module_type: ModuleType, start_date: str = None, end_date: str = None) -> Tuple[str, str, str]:
        """
        Convert period format based on module type
        Returns: (period, start_date, end_date)
        """
        
        # If custom dates provided, always use them regardless of period
        if start_date and end_date:
            logger.info(f"📅 Using CUSTOM date range: {start_date} to {end_date}")
            if module_type == ModuleType.GOOGLE_ADS:
                return ('CUSTOM', start_date, end_date)
            else:  # GA4, Meta, Facebook, Instagram
                return ('custom', start_date, end_date)
        
        # Only convert standard periods (7, 30, 90, 365 days)
        # Any non-standard period should have been converted to custom dates by Agent 2
        standard_periods = ['LAST_7_DAYS', 'LAST_30_DAYS', 'LAST_90_DAYS', 'LAST_365_DAYS', 
                        '7d', '30d', '90d', '365d']
        
        if period not in standard_periods:
            logger.warning(f"⚠️ Non-standard period received: {period}. This should have been converted to custom dates by Agent 2.")
            # Fallback to default
            period = 'LAST_7_DAYS' if module_type == ModuleType.GOOGLE_ADS else '7d'
        
        # Convert predefined periods
        if module_type == ModuleType.GOOGLE_ADS:
            # Google Ads format: LAST_7_DAYS, LAST_30_DAYS, etc.
            period_map = {
                'LAST_7_DAYS': 'LAST_7_DAYS',
                'LAST_30_DAYS': 'LAST_30_DAYS',
                'LAST_90_DAYS': 'LAST_90_DAYS',
                'LAST_365_DAYS': 'LAST_365_DAYS',
                # Handle if they come in GA4 format
                '7d': 'LAST_7_DAYS',
                '30d': 'LAST_30_DAYS',
                '90d': 'LAST_90_DAYS',
                '365d': 'LAST_365_DAYS'
            }
            converted = period_map.get(period, 'LAST_7_DAYS')
            logger.info(f"📊 Google Ads period: {period} → {converted}")
            return (converted, None, None)
        
        else:  # Google Analytics, Meta Ads, Facebook, Instagram
            # GA4/Meta format: 7d, 30d, 90d, 365d
            period_map = {
                'LAST_7_DAYS': '7d',
                'LAST_30_DAYS': '30d',
                'LAST_90_DAYS': '90d',
                'LAST_365_DAYS': '365d',
                # Handle if already in correct format
                '7d': '7d',
                '30d': '30d',
                '90d': '90d',
                '365d': '365d'
            }
            converted = period_map.get(period, '30d')
            logger.info(f"📊 {module_type.value} period: {period} → {converted}")
            return (converted, None, None)      
    
    # =================
    # AGENT 5: Endpoint Executor with Special Handling
    # =================
   
    async def agent_execute_endpoints(
        self,
        endpoints: List[str],
        endpoint_params: Dict[str, Any],
        module_type: str,
        account_result: Dict[str, Any],
        session_id: str,
        status_callback: Optional[callable] = None  # ✅ Add this parameter
    ) -> Dict[str, Any]:
        """
        Agent 5: Execute selected endpoints and collect data
        """
        try:
            logger.info("="*80)
            logger.info("🔧 AGENT 5: ENDPOINT EXECUTOR - STARTING")
            logger.info(f"📋 Endpoints to execute: {endpoints}")
            logger.info(f"📦 Parameters: {endpoint_params}")
            logger.info(f"🎯 Module: {module_type}")
            logger.info("="*80)
            
            if status_callback:
                await status_callback("Fetching data from selected endpoints...")
            
            executed_data = {}
            execution_log = []
            
            # Handle different module types
            for idx, endpoint_name in enumerate(endpoints, 1):
                try:
                    if status_callback:
                        await status_callback(f"Executing endpoint {idx}/{len(endpoints)}: {endpoint_name}")
                    
                    logger.info(f"\n🔄 Executing endpoint {idx}/{len(endpoints)}: {endpoint_name}")
                    
                    result = None
                    
                    # ===== GOOGLE ADS MODULE =====
                    if module_type == "google_ads":
                        customer_id = endpoint_params.get("customer_id")
                        start_date = endpoint_params.get("start_date")
                        end_date = endpoint_params.get("end_date")
                        
                        if endpoint_name == "accounts_list":
                            result = await self._call_google_ads_accounts(endpoint_params.get("token"))
                        
                        elif endpoint_name == "campaigns_list":
                            campaign_id = endpoint_params.get("campaign_id")
                            result = await self._call_google_ads_campaigns(
                                customer_id, start_date, end_date, endpoint_params.get("token"), campaign_id
                            )
                        
                        elif endpoint_name == "ad_groups_list":
                            ad_group_id = endpoint_params.get("ad_group_id")
                            result = await self._call_google_ads_ad_groups(
                                customer_id, start_date, end_date, endpoint_params.get("token"), ad_group_id
                            )
                        
                        elif endpoint_name == "ads_list":
                            ad_id = endpoint_params.get("ad_id")
                            result = await self._call_google_ads_ads(
                                customer_id, start_date, end_date, endpoint_params.get("token"), ad_id
                            )
                        
                        elif endpoint_name == "keywords_list":
                            keyword_id = endpoint_params.get("keyword_id")
                            result = await self._call_google_ads_keywords(
                                customer_id, start_date, end_date, endpoint_params.get("token"), keyword_id
                            )
                        
                        elif endpoint_name == "performance_metrics":
                            result = await self._call_google_ads_performance(
                                customer_id, start_date, end_date, endpoint_params.get("token")
                            )
                    
                    # ===== GOOGLE ANALYTICS MODULE =====
                    elif module_type == "google_analytics":
                        property_id = endpoint_params.get("property_id")
                        start_date = endpoint_params.get("start_date")
                        end_date = endpoint_params.get("end_date")
                        
                        if endpoint_name == "properties_list":
                            result = await self._call_ga4_properties(endpoint_params.get("token"))
                        
                        elif endpoint_name == "traffic_overview":
                            result = await self._call_ga4_traffic_overview(
                                property_id, start_date, end_date, endpoint_params.get("token")
                            )
                        
                        elif endpoint_name == "acquisition_overview":
                            result = await self._call_ga4_acquisition(
                                property_id, start_date, end_date, endpoint_params.get("token")
                            )
                        
                        elif endpoint_name == "engagement_overview":
                            result = await self._call_ga4_engagement(
                                property_id, start_date, end_date, endpoint_params.get("token")
                            )
                        
                        elif endpoint_name == "conversion_overview":
                            result = await self._call_ga4_conversions(
                                property_id, start_date, end_date, endpoint_params.get("token")
                            )
                    
                    # ===== META ADS MODULE =====
                    elif module_type == "meta_ads":
                        account_id = endpoint_params.get("account_id")
                        start_date = endpoint_params.get("start_date")
                        end_date = endpoint_params.get("end_date")
                        
                        if endpoint_name == "ad_accounts_list":
                            result = await self._call_meta_ad_accounts(endpoint_params.get("token"))
                        
                        elif endpoint_name == "campaigns_list":
                            # Special handling for Meta campaigns - needs pagination
                            logger.info("⚠️ Meta campaigns require full pagination - this may take time")
                            if status_callback:
                                await status_callback("Loading all Meta campaigns (this may take a few minutes)...")
                            result = await self._call_meta_campaigns_full(
                                account_id, endpoint_params.get("token")
                            )
                        
                        elif endpoint_name == "campaign_insights":
                            campaign_id = endpoint_params.get("campaign_id")
                            result = await self._call_meta_campaign_insights(
                                account_id, campaign_id, start_date, end_date, endpoint_params.get("token")
                            )
                        
                        elif endpoint_name == "adsets_list":
                            result = await self._call_meta_adsets(
                                account_id, start_date, end_date, endpoint_params.get("token")
                            )
                        
                        elif endpoint_name == "ads_list":
                            result = await self._call_meta_ads(
                                account_id, start_date, end_date, endpoint_params.get("token")
                            )
                        
                        elif endpoint_name == "account_insights":
                            result = await self._call_meta_account_insights(
                                account_id, start_date, end_date, endpoint_params.get("token")
                            )
                    
                    # ===== FACEBOOK ANALYTICS MODULE =====
                    elif module_type == "facebook_analytics":
                        page_id = endpoint_params.get("page_id")
                        start_date = endpoint_params.get("start_date")
                        end_date = endpoint_params.get("end_date")
                        
                        if endpoint_name == "pages_list":
                            result = await self._call_facebook_pages(endpoint_params.get("token"))
                        
                        elif endpoint_name == "page_insights":
                            result = await self._call_facebook_page_insights(
                                page_id, start_date, end_date, endpoint_params.get("token")
                            )
                        
                        elif endpoint_name == "posts_insights":
                            result = await self._call_facebook_posts_insights(
                                page_id, start_date, end_date, endpoint_params.get("token")
                            )
                        
                        elif endpoint_name == "audience_insights":
                            result = await self._call_facebook_audience_insights(
                                page_id, start_date, end_date, endpoint_params.get("token")
                            )
                    
                    # ===== INTENT INSIGHTS MODULE =====
                    elif module_type == "intent_insights":
                        account_id = endpoint_params.get("account_id")
                        keywords = endpoint_params.get("keywords", [])
                        country = endpoint_params.get("country", "World Wide")
                        start_date = endpoint_params.get("start_date")
                        end_date = endpoint_params.get("end_date")
                        
                        if endpoint_name == "intent_keyword_insights":
                            result = await self._call_intent_keyword_insights(
                                account_id, keywords, country, start_date, end_date, endpoint_params.get("token")
                            )
                        
                        elif endpoint_name == "intent_trends":
                            result = await self._call_intent_trends(
                                account_id, keywords, country, start_date, end_date, endpoint_params.get("token")
                            )
                        
                        elif endpoint_name == "intent_demographics":
                            result = await self._call_intent_demographics(
                                account_id, keywords, country, start_date, end_date, endpoint_params.get("token")
                            )
                    
                    # Store result
                    if result:
                        executed_data[endpoint_name] = result
                        execution_log.append({
                            "endpoint": endpoint_name,
                            "status": "success",
                            "timestamp": datetime.utcnow().isoformat(),
                            "data_size": len(str(result))
                        })
                        
                        # Save to MongoDB
                        await self._save_endpoint_response(
                            session_id=session_id,
                            endpoint_name=endpoint_name,
                            response_data=result,
                            module_type=module_type
                        )
                        
                        logger.info(f"✅ Successfully executed: {endpoint_name}")
                    else:
                        execution_log.append({
                            "endpoint": endpoint_name,
                            "status": "no_data",
                            "timestamp": datetime.utcnow().isoformat()
                        })
                        logger.warning(f"⚠️ No data returned from: {endpoint_name}")
                        
                except Exception as e:
                    logger.error(f"❌ Error executing {endpoint_name}: {str(e)}")
                    logger.error(traceback.format_exc())
                    execution_log.append({
                        "endpoint": endpoint_name,
                        "status": "error",
                        "error": str(e),
                        "timestamp": datetime.utcnow().isoformat()
                    })
            
            if status_callback:
                await status_callback("Data fetching complete. Analyzing results...")
            
            logger.info("="*80)
            logger.info("✅ AGENT 5: ENDPOINT EXECUTOR - COMPLETE")
            logger.info(f"📊 Executed {len(executed_data)}/{len(endpoints)} endpoints successfully")
            logger.info("="*80)
            
            return {
                "executed_data": executed_data,
                "execution_log": execution_log,
                "total_endpoints": len(endpoints),
                "successful_executions": len(executed_data),
                "session_id": session_id
            }
            
        except Exception as e:
            logger.error(f"❌ ERROR in agent_execute_endpoints: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    # ============================================
    # Helper Method to Save Endpoint Responses
    # ============================================
    async def _save_endpoint_response(
        self,
        endpoint_name: str,
        endpoint_path: str,
        params: Dict[str, Any],
        response_data: Any,
        user_email: str,
        session_id: str
    ) -> None:
        """Save endpoint response to MongoDB for future reference"""
        try:
            collection = self.db.endpoint_responses
            
            document = {
                'user_email': user_email,
                'session_id': session_id,
                'endpoint_name': endpoint_name,
                'endpoint_path': endpoint_path,
                'request_params': params,
                'response_data': response_data,
                'timestamp': datetime.utcnow(),
                'created_at': datetime.utcnow()
            }
            
            await collection.insert_one(document)
            logger.info(f"✅ Saved endpoint response to MongoDB: {endpoint_name}")
            
        except Exception as e:
            logger.error(f"❌ Error saving endpoint response: {str(e)}")
            
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

                    module_type = chat_request.module_type.value
                    # After Agent 3 (Account Identifier)
                    if module_type == "intent_insights":
                        # Run Agent 3.5 for country and keyword extraction
                        agent_35_context = {
                            'keywords': chat_request.context.get('keywords', []) if chat_request.context else [],
                            'country': chat_request.context.get('country') if chat_request.context else None,
                        }

                        country_keyword_result = await self.agent_extract_country_and_keywords(
                            message=chat_request.message,
                            module_type=module_type,
                            context=agent_35_context
                        )
                        
                        # Check if we need clarification
                        if country_keyword_result.get("needs_country_clarification"):
                            return ChatResponse(
                                response="🌍 Which country or region would you like to analyze? Please specify a country name (e.g., 'United States', 'United Kingdom', 'World Wide').",
                                session_id=session_id,
                                module_type=ModuleType(module_type),
                                triggered_endpoint=None,
                                endpoint_data={"needs_clarification": True, "clarification_type": "country"}
                            )
                        
                        if country_keyword_result.get("needs_keyword_clarification"):
                            return ChatResponse(
                                response="🔑 Please provide the keywords you'd like to analyze. You can mention them in your message or I can use the keywords from your current filter.",
                                session_id=session_id,
                                module_type=ModuleType(module_type),
                                triggered_endpoint=None,
                                endpoint_data={"needs_clarification": True, "clarification_type": "keywords"}
                            )
                        
                        # Add country and keywords to endpoint_params
                        endpoint_params["country"] = country_keyword_result.get("country", "World Wide")
                        endpoint_params["keywords"] = country_keyword_result.get("keywords", [])
                        
                        logger.info(f"🌍 Country set to: {endpoint_params['country']}")
                        logger.info(f"🔑 Keywords set to: {endpoint_params['keywords']}")

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

                            # Build endpoint parameters with proper period conversion
                            raw_period = time_period.get('period') or chat_request.period or 'LAST_7_DAYS'
                            raw_start_date = time_period.get('start_date')
                            raw_end_date = time_period.get('end_date')

                            logger.info(f"\n{'='*60}")
                            logger.info(f"⏰ TIME PERIOD HANDLING")
                            logger.info(f"Raw period from Agent 2: {raw_period}")
                            logger.info(f"Raw start_date: {raw_start_date}")
                            logger.info(f"Raw end_date: {raw_end_date}")
                            logger.info(f"Period source: {time_period.get('extracted_from', 'unknown')}")

                            # Convert period format based on module type
                            converted_period, converted_start_date, converted_end_date = self._convert_period_for_module(
                                period=raw_period,
                                module_type=chat_request.module_type,
                                start_date=raw_start_date,
                                end_date=raw_end_date
                            )

                            logger.info(f"✅ CONVERTED - Period: {converted_period}")

                            # ✅ SPECIAL: Convert period to dates for Intent Insights
                            if chat_request.module_type.value == 'intent_insights':
                                if converted_period and not converted_start_date and not converted_end_date:
                                    end_date_obj = datetime.utcnow().date()
                                    
                                    if converted_period == '7d':
                                        start_date_obj = end_date_obj - timedelta(days=7)
                                    elif converted_period == '30d':
                                        start_date_obj = end_date_obj - timedelta(days=30)
                                    elif converted_period == '90d':
                                        start_date_obj = end_date_obj - timedelta(days=90)
                                    elif converted_period == '365d':
                                        start_date_obj = end_date_obj - timedelta(days=365)
                                    else:
                                        start_date_obj = end_date_obj - timedelta(days=30)
                                    
                                    converted_start_date = start_date_obj.strftime('%Y-%m-%d')
                                    converted_end_date = end_date_obj.strftime('%Y-%m-%d')
                                    
                                    logger.info(f"🔍 Intent - Converted {converted_period} to dates: {converted_start_date} to {converted_end_date}")

                            if converted_start_date and converted_end_date:
                                logger.info(f"✅ CONVERTED - Custom dates: {converted_start_date} to {converted_end_date}")
                            logger.info(f"{'='*60}\n")

                            endpoint_params = {
                                'token': chat_request.context.get('token', '') if chat_request.context else '',
                                'period': converted_period,
                                'start_date': converted_start_date,
                                'end_date': converted_end_date,
                                'module_type': chat_request.module_type 
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

                            logger.info(f"📦 Final endpoint_params: {json.dumps(endpoint_params, default=str, indent=2)}")
                            # Execute endpoints with status callback
                            endpoint_data = await self.agent_execute_endpoints(
                                selected_endpoints,
                                endpoint_params,
                                user_email,
                                session_id=session_id,
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