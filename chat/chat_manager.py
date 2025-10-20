import openai
import os
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import logging
import uuid
import json
import asyncio
from motor.motor_asyncio import AsyncIOMotorDatabase
from fastapi import HTTPException
import re

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
    async def agent_identify_account(self, message: str, module_type: ModuleType, context: Dict[str, Any]) -> Dict[str, Any]:
        """Identify which account/property the user is referring to"""
        
        prompt = f"""
        Identify if the user is referring to a specific account/property in their message.
        Module type: {module_type.value}
        Current context: {json.dumps(context)}

        User Message: "{message}"

        Look for:
        - Specific account names or IDs
        - References like "this account", "current campaign"
        - Implicit references based on context

        Respond in JSON format:
        {{
            "has_specific_reference": true/false,
            "reference_type": "explicit" or "implicit" or "none",
            "account_name": "name if mentioned" or null,
            "use_current": true/false,
            "needs_account_list": true/false
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
            
            # Add current account/property from context
            if result['use_current'] or not result['has_specific_reference']:
                if module_type == ModuleType.GOOGLE_ADS:
                    result['customer_id'] = context.get('customer_id')
                elif module_type == ModuleType.GOOGLE_ANALYTICS:
                    result['property_id'] = context.get('property_id')
                elif module_type == ModuleType.META_ADS:
                    result['account_id'] = context.get('account_id')
                elif module_type == ModuleType.FACEBOOK_ANALYTICS:
                    result['page_id'] = context.get('page_id')
            
            return result
            
        except Exception as e:
            logger.error(f"Error identifying account: {e}")
            return {
                'use_current': True,
                'needs_account_list': False
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
            
            return selected
            
        except Exception as e:
            logger.error(f"Error selecting endpoints: {e}")
            # Default to key stats endpoint
            return [available_endpoints[1]] if len(available_endpoints) > 1 else available_endpoints

    # =================
    # AGENT 5: Endpoint Executor
    # =================
    async def agent_execute_endpoints(self, endpoints: List[Dict[str, Any]], params: Dict[str, Any], user_email: str) -> Dict[str, Any]:
        """Execute selected endpoints and collect data"""
        
        results = {}
        token = params.get('token', '')
        
        for endpoint in endpoints:
            try:
                # Build URL
                url = endpoint['path']
                for param in ['customer_id', 'property_id', 'account_id', 'page_id']:
                    if f'{{{param}}}' in url and param in params:
                        url = url.replace(f'{{{param}}}', params[param])
                
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
                            logger.error(f"API call failed for {endpoint['name']}: {response.status}")
                            
            except Exception as e:
                logger.error(f"Error executing endpoint {endpoint['name']}: {e}")
                results[endpoint['name']] = {"error": str(e)}
        
        return results

    # =================
    # AGENT 6: Data Analyzer
    # =================
    async def agent_analyze_data(self, message: str, data: Dict[str, Any], module_type: ModuleType) -> str:
        """Analyze the collected data and generate insights"""
        
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
            return "I encountered an error while analyzing your data. Please try again."

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
    async def process_chat_message(self, chat_request: ChatRequest, user_email: str) -> ChatResponse:
        """Main orchestrator that coordinates all agents"""
        
        logger.info(f"Processing chat message for {user_email}: {chat_request.message[:100]}...")
        
        # Get or create session
        session_id = chat_request.session_id or str(uuid.uuid4())
        
        # Save user message
        await self._save_message_to_session(session_id, MessageRole.USER, chat_request.message, user_email)
        
        # AGENT 1: Classify query
        classification = await self.agent_classify_query(chat_request.message)
        
        if classification['category'] == 'GENERAL':
            # Handle general queries directly
            response = await self._handle_general_query(chat_request.message)
            await self._save_message_to_session(session_id, MessageRole.ASSISTANT, response, user_email)
            
            return ChatResponse(
                response=response,
                session_id=session_id,
                module_type=chat_request.module_type
            )
        
        # AGENT 2: Extract time period
        time_info = await self.agent_extract_time_period(chat_request.message, chat_request.context or {})
        
        if time_info['needs_clarification']:
            response = time_info['clarification_message']
            await self._save_message_to_session(session_id, MessageRole.ASSISTANT, response, user_email)
            
            return ChatResponse(
                response=response,
                session_id=session_id,
                module_type=chat_request.module_type
            )
        
        # AGENT 3: Identify account
        account_info = await self.agent_identify_account(
            chat_request.message,
            chat_request.module_type,
            chat_request.context or {}
        )
        
        # Prepare parameters
        params = {
            'period': time_info.get('period', chat_request.period or 'LAST_30_DAYS'),
            'start_date': time_info.get('start_date'),
            'end_date': time_info.get('end_date'),
            'token': chat_request.context.get('token') if chat_request.context else None
        }
        
        # Add account identifiers
        if chat_request.module_type == ModuleType.GOOGLE_ADS:
            params['customer_id'] = account_info.get('customer_id', chat_request.customer_id)
        elif chat_request.module_type == ModuleType.GOOGLE_ANALYTICS:
            params['property_id'] = account_info.get('property_id', chat_request.property_id)
        elif chat_request.module_type == ModuleType.META_ADS:
            params['account_id'] = account_info.get('account_id', chat_request.context.get('account_id'))
        elif chat_request.module_type == ModuleType.FACEBOOK_ANALYTICS:
            params['page_id'] = account_info.get('page_id', chat_request.context.get('page_id'))
        
        # AGENT 4: Select endpoints
        endpoints = await self.agent_select_endpoints(
            chat_request.message,
            chat_request.module_type,
            account_info
        )
        
        # Special message for Meta campaigns
        if any(e['name'] == 'get_meta_campaigns_all' for e in endpoints):
            await self._save_message_to_session(
                session_id,
                MessageRole.SYSTEM,
                "⏳ Fetching all campaign data from Meta. This may take a moment due to API rate limits...",
                user_email
            )
        
        # AGENT 5: Execute endpoints
        data = await self.agent_execute_endpoints(endpoints, params, user_email)
        
        # AGENT 6: Analyze data
        analysis = await self.agent_analyze_data(
            chat_request.message,
            data,
            chat_request.module_type
        )
        
        # AGENT 7: Format response
        formatted_response = await self.agent_format_response(
            analysis,
            data,
            needs_visualization=True
        )
        
        # Save assistant response
        await self._save_message_to_session(
            session_id,
            MessageRole.ASSISTANT,
            formatted_response['text'],
            user_email
        )
        
        return ChatResponse(
            response=formatted_response['text'],
            session_id=session_id,
            triggered_endpoint=','.join([e['name'] for e in endpoints]),
            endpoint_data=data,
            module_type=chat_request.module_type
        )

    # =================
    # Helper Methods
    # =================
    async def _handle_general_query(self, message: str) -> str:
        """Handle general non-analytics queries"""
        try:
            response = await self.openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a helpful marketing analytics assistant."},
                    {"role": "user", "content": message}
                ],
                temperature=0.7,
                max_tokens=500
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"Error handling general query: {e}")
            return "I'm here to help with your marketing analytics questions. How can I assist you?"

    async def _save_message_to_session(self, session_id: str, role: MessageRole, content: str, user_email: str):
        """Save message to chat session in MongoDB"""
        try:
            collection = self.db.chat_sessions
            
            message = {
                'role': role.value,
                'content': content,
                'timestamp': datetime.utcnow()
            }
            
            # Update or create session
            await collection.update_one(
                {'session_id': session_id},
                {
                    '$push': {'messages': message},
                    '$set': {
                        'last_activity': datetime.utcnow(),
                        'user_email': user_email
                    },
                    '$setOnInsert': {
                        'created_at': datetime.utcnow(),
                        'is_active': True
                    }
                },
                upsert=True
            )
        except Exception as e:
            logger.error(f"Error saving message to session: {e}")

    async def _save_endpoint_response(self, endpoint_name: str, endpoint_path: str, params: Dict[str, Any], response_data: Any, user_email: str):
        """Save endpoint response to MongoDB"""
        try:
            collection = self.db.endpoint_responses
            
            document = {
                'endpoint_name': endpoint_name,
                'endpoint_path': endpoint_path,
                'params': params,
                'response_data': response_data,
                'user_email': user_email,
                'timestamp': datetime.utcnow()
            }
            
            await collection.insert_one(document)
        except Exception as e:
            logger.error(f"Error saving endpoint response: {e}")

    def _create_visualizations(self, data: Dict[str, Any], viz_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create visualization configurations"""
        visualizations = []
        
        # This would create actual chart/table configurations
        # For now, returning placeholder
        if viz_config.get('needs_chart'):
            visualizations.append({
                'type': 'chart',
                'chart_type': viz_config.get('chart_type', 'line'),
                'data': {}  # Process data for chart
            })
        
        if viz_config.get('needs_table'):
            visualizations.append({
                'type': 'table',
                'columns': viz_config.get('table_columns', []),
                'data': {}  # Process data for table
            })
        
        return visualizations

    # Add method for handling Meta campaigns all data
    async def get_meta_campaigns_all_data(self, account_id: str, token: str, period: str, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """Special handler for getting ALL Meta campaigns data"""
        try:
            # First get the campaigns list
            import aiohttp
            async with aiohttp.ClientSession() as session:
                headers = {'Authorization': f'Bearer {token}'}
                
                # Get campaigns list
                list_url = f"https://eyqi6vd53z.us-east-2.awsapprunner.com/api/meta/ad-accounts/{account_id}/campaigns/list"
                async with session.get(list_url, headers=headers) as response:
                    if response.status != 200:
                        return {"error": "Failed to fetch campaigns list"}
                    
                    campaigns_data = await response.json()
                    all_campaigns = campaigns_data.get('campaigns', [])
                    
                    # Now get insights for each campaign with rate limiting
                    campaign_insights = []
                    for i, campaign in enumerate(all_campaigns):
                        if i > 0 and i % 10 == 0:
                            # Rate limit: pause every 10 requests
                            await asyncio.sleep(2)
                        
                        # Get campaign insights
                        campaign_id = campaign['id']
                        # ... fetch individual campaign insights
                        
                    return {
                        'total_campaigns': len(all_campaigns),
                        'campaigns': campaign_insights
                    }
                    
        except Exception as e:
            logger.error(f"Error fetching all Meta campaigns: {e}")
            return {"error": str(e)}

# Create singleton instance
chat_manager = ChatManager()