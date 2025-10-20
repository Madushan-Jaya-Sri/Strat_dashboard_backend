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
        """Identify which account/property the user is referring to by name or use active/selected account"""
        
        prompt = f"""
        Identify the account or property the user is referring to in their message.
        Module type: {module_type.value}
        Current context: {json.dumps(context)}
        User Message: "{message}"

        Look for:
        - Specific account names (e.g., "My Summer Campaign", "Main Website Analytics")
        - References like "this account", "my ads account", "current campaign"
        - Implicit references based on context (e.g., no account mentioned, use active account)

        If an account name is mentioned, return it for lookup.
        If no specific account name is mentioned, use the active/selected account from context.
        If no account can be determined, indicate that a list of accounts should be fetched.

        Respond in JSON format:
        {{
            "has_specific_reference": true/false,
            "reference_type": "explicit" or "implicit" or "none",
            "account_name": "name if mentioned" or null,
            "account_id": "specific ID if identified" or null,
            "use_active_account": true/false,
            "needs_account_list": true/false,
            "clarification_message": "message to request account clarification" or null
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
            
            # If an account name is mentioned, fetch the account list to map name to ID
            if result['has_specific_reference'] and result['account_name']:
                account_list = await self._fetch_account_list(module_type, token, user_email)
                for account in account_list:
                    # Match account name (case-insensitive)
                    if (account.get('name') or account.get('descriptiveName') or '').lower() == result['account_name'].lower():
                        result['account_id'] = account.get('id') or account.get('customerId') or \
                                             account.get('account_id') or account.get('page_id')
                        result['needs_account_list'] = False
                        result['clarification_message'] = None
                        break
                if not result.get('account_id'):
                    result['needs_account_list'] = True
                    result['clarification_message'] = f"Could not find an account named '{result['account_name']}'. Please specify a valid account name or ID, or I can provide a list of available accounts. Would you like me to do that?"
            
            # If no specific account mentioned, use active/selected account from context
            if not result['has_specific_reference'] or result.get('use_active_account'):
                if module_type == ModuleType.GOOGLE_ADS:
                    result['account_id'] = context.get('customer_id') or context.get('campaign_id')
                    result['account_name'] = context.get('campaign_name')
                elif module_type == ModuleType.GOOGLE_ANALYTICS:
                    result['account_id'] = context.get('property_id')
                    result['account_name'] = context.get('property_name')
                elif module_type == ModuleType.META_ADS:
                    result['account_id'] = context.get('account_id')
                    result['account_name'] = context.get('account_name')
                elif module_type == ModuleType.FACEBOOK_ANALYTICS:
                    result['account_id'] = context.get('page_id')
                    result['account_name'] = context.get('page_name')
                elif module_type == ModuleType.INSTAGRAM_ANALYTICS:
                    result['account_id'] = context.get('account_id')
                    result['account_name'] = context.get('account_name')
                
                if result['account_id']:
                    result['use_active_account'] = True
                    result['needs_account_list'] = False
                    result['clarification_message'] = None
                else:
                    result['needs_account_list'] = True
                    result['clarification_message'] = f"Please specify the {module_type.value} account to analyze, or I can fetch a list of available accounts. Would you like me to do that?"
            
            return result
            
        except Exception as e:
            logger.error(f"Error identifying account: {e}")
            return {
                'has_specific_reference': False,
                'reference_type': 'none',
                'account_name': None,
                'account_id': None,
                'use_active_account': True,
                'needs_account_list': True,
                'clarification_message': f"Please specify the {module_type.value} account to analyze, or I can fetch a list of available accounts."
            }

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
        
        results = {}
        token = params.get('token', '')
        
        for endpoint in endpoints:
            try:
                # Validate required parameters
                for param in ['customer_id', 'property_id', 'account_id', 'page_id']:
                    if f'{{{param}}}' in endpoint['path'] and (param not in params or params[param] is None):
                        error_msg = f"Missing or invalid {param} for endpoint {endpoint['name']}"
                        logger.error(error_msg)
                        results[endpoint['name']] = {"error": error_msg}
                        return results  # Exit early to avoid further processing
                
                # Build URL
                url = endpoint['path']
                for param in ['customer_id', 'property_id', 'account_id', 'page_id']:
                    if f'{{{param}}}' in url and param in params:
                        url = url.replace(f'{{{param}}}', str(params[param]))
                
                # For Meta campaigns list - special handling
                if endpoint['name'] == 'get_meta_campaigns_list':
                    logger.info("Special handling for Meta campaigns list endpoint")
                    # This endpoint needs pagination handling
                    # You'll need to implement the full data fetching here
                    pass
                
                # Prepare query parameters
                query_params = {}
                for param_name in endpoint['params']:
                    if param_name in params and param_name not in ['customer_id', 'property_id', 'account_id', 'page_id']:
                        query_params[param_name] = params[param_name]
                
                # Make API call
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    headers = {'Authorization': f'Bearer {token}'}
                    full_url = f"https://eyqi6vd53z.us-east-2.awsapprunner.com{url}"
                    
                    async with session.get(full_url, params=query_params, headers=headers) as response:
                        if response.status == 200:
                            data = await response.json()
                            results[endpoint['name']] = data
                            
                            # Save to MongoDB
                            await self._save_endpoint_response(
                                endpoint_name=endpoint['name'],
                                endpoint_path=url,
                                params=params,
                                response_data=data,
                                user_email=user_email
                            )
                        else:
                            error_msg = f"API call failed for {endpoint['name']}: {response.status}"
                            logger.error(error_msg)
                            results[endpoint['name']] = {"error": error_msg}
                            
            except Exception as e:
                error_msg = f"Error executing endpoint {endpoint['name']}: {str(e)}"
                logger.error(error_msg)
                results[endpoint['name']] = {"error": error_msg}
        
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
                    analytics_period = self._normalize_period(search_criteria['period'])
                    
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