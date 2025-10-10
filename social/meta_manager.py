"""
Meta Manager - Unified handler for Facebook Pages, Instagram, and Meta Ads
"""

import logging
import requests
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from fastapi import HTTPException
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional
import json
import time

logger = logging.getLogger(__name__)

class MetaManager:
    """Unified manager for all Meta platforms (Facebook, Instagram, Ads)"""
    
    GRAPH_API_VERSION = "v21.0"
    BASE_URL = f"https://graph.facebook.com/{GRAPH_API_VERSION}"

      
    # Rate limiting configuration
    RATE_LIMIT_DELAY = 0.2  # 200ms delay between requests
    MAX_RETRIES = 3
    RETRY_DELAY = 2  # Initial retry delay in seconds
    
    def __init__(self, user_email: str, auth_manager):
        self.user_email = user_email
        self.auth_manager = auth_manager
        self.access_token = self._get_access_token()
        self.last_request_time = 0
    
    def _rate_limited_request(self, endpoint: str, params: Dict = None, retry_count: int = 0) -> Dict:
        """
        Make a rate-limited request to Facebook Graph API with exponential backoff.
        """
        if params is None:
            params = {}
        
        # Only add access_token if not already provided
        if 'access_token' not in params:
            params['access_token'] = self.access_token
        
        # Rate limiting: ensure minimum delay between requests
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.RATE_LIMIT_DELAY:
            sleep_time = self.RATE_LIMIT_DELAY - time_since_last_request
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.3f}s")
            time.sleep(sleep_time)
        
        url = f"{self.BASE_URL}/{endpoint}"
        
        try:
            response = requests.get(url, params=params)
            self.last_request_time = time.time()
            
            # Check for rate limiting error
            if response.status_code == 429 or (response.status_code == 400 and 'rate limit' in response.text.lower()):
                if retry_count < self.MAX_RETRIES:
                    retry_delay = self.RETRY_DELAY * (2 ** retry_count)  # Exponential backoff
                    logger.warning(f"Rate limited! Retrying in {retry_delay}s (attempt {retry_count + 1}/{self.MAX_RETRIES})")
                    time.sleep(retry_delay)
                    return self._rate_limited_request(endpoint, params, retry_count + 1)
                else:
                    raise Exception("Rate limit exceeded and max retries reached")
            
            # Check for other errors
            if response.status_code != 200:
                error_data = response.json() if response.text else {}
                error_message = error_data.get('error', {}).get('message', 'Unknown error')
                
                # Check if it's a temporary error that should be retried
                if response.status_code >= 500 and retry_count < self.MAX_RETRIES:
                    retry_delay = self.RETRY_DELAY * (2 ** retry_count)
                    logger.warning(f"Server error {response.status_code}. Retrying in {retry_delay}s")
                    time.sleep(retry_delay)
                    return self._rate_limited_request(endpoint, params, retry_count + 1)
                
                logger.error(f"Meta API error: {response.text}")
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Meta API error: {error_message}"
                )
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            if retry_count < self.MAX_RETRIES:
                retry_delay = self.RETRY_DELAY * (2 ** retry_count)
                logger.warning(f"Request failed: {e}. Retrying in {retry_delay}s")
                time.sleep(retry_delay)
                return self._rate_limited_request(endpoint, params, retry_count + 1)
            raise

    def _get_access_token(self) -> str:
        """Get Facebook access token for user"""
        try:
            access_token = self.auth_manager.get_facebook_access_token(self.user_email)
            if not access_token:
                raise HTTPException(
                    status_code=401,
                    detail="Facebook not connected. Please authenticate with Facebook first."
                )
            return access_token
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting Facebook access token: {e}")
            raise HTTPException(
                status_code=401,
                detail="Facebook authentication required. Please connect your Facebook account."
            )
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """Legacy method - redirects to rate-limited request"""
        return self._rate_limited_request(endpoint, params)
    
    # def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
    #     """Make request to Facebook Graph API"""
    #     if params is None:
    #         params = {}
        
    #     # Only add access_token if not already provided
    #     if 'access_token' not in params:
    #         params['access_token'] = self.access_token
        
    #     url = f"{self.BASE_URL}/{endpoint}"
    #     response = requests.get(url, params=params)
        
    #     if response.status_code != 200:
    #         logger.error(f"Meta API error: {response.text}")
    #         raise HTTPException(
    #             status_code=response.status_code,
    #             detail=f"Meta API error: {response.json().get('error', {}).get('message', 'Unknown error')}"
    #         )
        
    #     return response.json()
    
    def _period_to_dates(self, period: str = None, start_date: str = None, end_date: str = None) -> tuple:
        """
        Convert period string OR custom date range to since/until dates
        
        Args:
            period: Predefined period like '7d', '30d', etc.
            start_date: Custom start date in YYYY-MM-DD format
            end_date: Custom end date in YYYY-MM-DD format
        
        Returns:
            Tuple of (since, until) in YYYY-MM-DD format
        """
        # If custom dates provided, use them
        if start_date and end_date:
            # Validate date format
            try:
                datetime.strptime(start_date, '%Y-%m-%d')
                datetime.strptime(end_date, '%Y-%m-%d')
                return start_date, end_date
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid date format. Use YYYY-MM-DD"
                )
        
        # Otherwise use predefined period
        if period:
            days_map = {'7d': 7, '30d': 30, '90d': 90, '365d': 365}
            days = days_map.get(period, 30)
            
            until = datetime.now()
            since = until - timedelta(days=days)
            
            return since.strftime('%Y-%m-%d'), until.strftime('%Y-%m-%d')
        
        # Default to last 30 days
        until = datetime.now()
        since = until - timedelta(days=30)
        return since.strftime('%Y-%m-%d'), until.strftime('%Y-%m-%d')

    def _validate_date_range(self, start_date: str, end_date: str):
        """Validate date range"""
        try:
            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')
            
            if start > end:
                raise ValueError("Start date must be before end date")
            
            # Check if date range is too large (Meta API limitation: ~37 months max)
            delta = end - start
            if delta.days > 1100:  # Approximately 3 years
                raise ValueError("Date range too large. Maximum allowed is approximately 3 years (1100 days)")
            
            # Check if end date is in the future
            if end > datetime.now():
                raise ValueError("End date cannot be in the future")
            
            # Meta API has limited historical data (typically 37 months)
            max_historical_date = datetime.now() - timedelta(days=1100)
            if start < max_historical_date:
                logger.warning(f"Start date {start_date} may be beyond Meta's historical data limit")
            
        except ValueError as e:
            logger.error(f"Date validation error: {e}")
            raise
    # =========================================================================
    # AD ACCOUNTS
    # =========================================================================
    
    def get_ad_accounts(self) -> List[Dict]:
        """Get all ad accounts accessible to user"""
        try:
            data = self._make_request("me/adaccounts", {
                'fields': 'id,account_id,name,account_status,currency,timezone_name,amount_spent,balance'
            })
            
            # Map Facebook status codes to strings
            status_map = {
                1: "ACTIVE",
                2: "DISABLED",
                3: "UNSETTLED",
                7: "PENDING_RISK_REVIEW",
                8: "PENDING_SETTLEMENT",
                9: "IN_GRACE_PERIOD",
                100: "PENDING_CLOSURE",
                101: "CLOSED",
                201: "ANY_ACTIVE",
                202: "ANY_CLOSED"
            }
            
            accounts = []
            for account in data.get('data', []):
                status_code = account.get('account_status')
                status_string = status_map.get(status_code, f"UNKNOWN_{status_code}")
                
                accounts.append({
                    'id': account.get('id'),
                    'account_id': account.get('account_id'),
                    'name': account.get('name'),
                    'status': status_string,  # Convert to string
                    'currency': account.get('currency'),
                    'timezone': account.get('timezone_name'),
                    'amount_spent': float(account.get('amount_spent', 0)) / 100,
                    'balance': float(account.get('balance', 0)) / 100
                })
            
            logger.info(f"Retrieved {len(accounts)} ad accounts")
            return accounts
            
        except Exception as e:
            logger.error(f"Error fetching ad accounts: {e}")
            return []

    def get_account_insights_summary(self, account_id: str, period: str = None, start_date: str = None, end_date: str = None) -> Dict:
        """
        Get account-level insights summary (for metric cards).
        This provides totals without needing to fetch all campaigns.
        """
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            # Get account-level insights in one request
            data = self._rate_limited_request(f"{account_id}/insights", {
                'time_range': json.dumps({"since": since, "until": until}),
                'fields': 'spend,impressions,clicks,actions,cpc,cpm,ctr,reach,frequency',
                'level': 'account',
                'action_attribution_windows': ['7d_click', '1d_view'],
                'use_account_attribution_setting': 'true'
            })
            
            if not data.get('data'):
                return {
                    'total_spend': 0,
                    'total_impressions': 0,
                    'total_clicks': 0,
                    'total_conversions': 0,
                    'total_reach': 0,
                    'avg_cpc': 0,
                    'avg_cpm': 0,
                    'avg_ctr': 0,
                    'avg_frequency': 0
                }
            
            insights = data['data'][0]
            
            # Extract conversions from actions
            conversions = 0
            actions = insights.get('actions', [])
            for action in actions:
                if action.get('action_type') in ['purchase', 'lead', 'complete_registration', 
                                                'omni_purchase', 'offsite_conversion.fb_pixel_purchase']:
                    conversions += int(action.get('value', 0))
            
            return {
                'total_spend': float(insights.get('spend', 0)),
                'total_impressions': int(insights.get('impressions', 0)),
                'total_clicks': int(insights.get('clicks', 0)),
                'total_conversions': conversions,
                'total_reach': int(insights.get('reach', 0)),
                'avg_cpc': float(insights.get('cpc', 0)),
                'avg_cpm': float(insights.get('cpm', 0)),
                'avg_ctr': float(insights.get('ctr', 0)),
                'avg_frequency': float(insights.get('frequency', 0))
            }
            
        except Exception as e:
            logger.error(f"Error fetching account insights summary: {e}")
            raise
        
    def get_ad_account_insights(self, account_id: str, period: str = None, start_date: str = None, end_date: str = None) -> Dict:
        """Get insights for specific ad account"""
        try:
            if start_date and end_date:
                self._validate_date_range(start_date, end_date)
            
            since, until = self._period_to_dates(period, start_date, end_date)
            
            # Additional validation for Meta API
            since_date = datetime.strptime(since, '%Y-%m-%d')
            until_date = datetime.strptime(until, '%Y-%m-%d')
            
            # Meta API limit check
            delta = until_date - since_date
            if delta.days > 1100:
                logger.error(f"Date range {delta.days} days exceeds Meta API limit")
                raise ValueError("Date range exceeds Meta API limit of ~3 years")
            
            data = self._make_request(f"{account_id}/insights", {
                'time_range': f'{{"since":"{since}","until":"{until}"}}',
                'fields': 'spend,impressions,clicks,actions,cpc,cpm,ctr,reach,frequency',
                'level': 'account',
                'action_attribution_windows': ['7d_click', '1d_view'],
                'use_account_attribution_setting': 'true'
            })
            
            if not data.get('data'):
                return {
                    'spend': 0,
                    'impressions': 0,
                    'clicks': 0,
                    'conversions': 0,
                    'cpc': 0,
                    'cpm': 0,
                    'ctr': 0,
                    'reach': 0,
                    'frequency': 0
                }
            
            insights = data['data'][0]
            
            # Extract conversions from actions
            conversions = 0
            actions = insights.get('actions', [])
            for action in actions:
                if action.get('action_type') in ['purchase', 'lead', 'complete_registration', 
                                                'omni_purchase', 'offsite_conversion.fb_pixel_purchase']:
                    conversions += int(action.get('value', 0))
            
            return {
                'spend': float(insights.get('spend', 0)),
                'impressions': int(insights.get('impressions', 0)),
                'clicks': int(insights.get('clicks', 0)),
                'conversions': conversions,
                'cpc': float(insights.get('cpc', 0)),
                'cpm': float(insights.get('cpm', 0)),
                'ctr': float(insights.get('ctr', 0)),
                'reach': int(insights.get('reach', 0)),
                'frequency': float(insights.get('frequency', 0))
            }
            
        except ValueError as e:
            logger.error(f"Validation error: {e}")
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error(f"Error fetching ad account insights: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch insights: {str(e)}")
    
    # Don't sum reach - it's not additive across days
    # Instead, get account-level insights without time_increment for accurate reach

    def get_ad_account_insights_timeseries(self, account_id: str, period: str = None, start_date: str = None, end_date: str = None) -> Dict:
        """Get time-series insights for specific ad account (breakdown by day)"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            # Get daily breakdown
            daily_data = self._make_request(f"{account_id}/insights", {
                'time_range': f'{{"since":"{since}","until":"{until}"}}',
                'fields': 'spend,impressions,clicks,actions,cpc,cpm,ctr,reach,frequency',
                'time_increment': '1',
                'level': 'account',
                'action_attribution_windows': ['7d_click', '1d_view'],
                'use_account_attribution_setting': 'true'
            })
            
            # Get summary totals (without time_increment for accurate reach)
            summary_data = self._make_request(f"{account_id}/insights", {
                'time_range': f'{{"since":"{since}","until":"{until}"}}',
                'fields': 'spend,impressions,clicks,actions,cpc,cpm,ctr,reach,frequency',
                'level': 'account',
                'action_attribution_windows': ['7d_click', '1d_view'],
                'use_account_attribution_setting': 'true'
            })
            
            if not daily_data.get('data'):
                return {
                    'timeseries': [],
                    'summary': {
                        'spend': 0,
                        'impressions': 0,
                        'clicks': 0,
                        'conversions': 0,
                        'cpc': 0,
                        'cpm': 0,
                        'ctr': 0,
                        'reach': 0,
                        'frequency': 0
                    }
                }
            
            timeseries = []
            
            for day_data in daily_data['data']:
                # Extract conversions from actions for this day
                conversions = 0
                actions = day_data.get('actions', [])
                for action in actions:
                    action_type = action.get('action_type')
                    if action_type in [
                        'purchase', 
                        'lead', 
                        'complete_registration',
                        'omni_purchase',
                        'offsite_conversion.fb_pixel_purchase'
                    ]:
                        conversions += int(action.get('value', 0))
                
                day_insights = {
                    'date': day_data.get('date_start'),
                    'spend': float(day_data.get('spend', 0)),
                    'impressions': int(day_data.get('impressions', 0)),
                    'clicks': int(day_data.get('clicks', 0)),
                    'conversions': conversions,
                    'cpc': float(day_data.get('cpc', 0)),
                    'cpm': float(day_data.get('cpm', 0)),
                    'ctr': float(day_data.get('ctr', 0)),
                    'reach': int(day_data.get('reach', 0)),
                    'frequency': float(day_data.get('frequency', 0))
                }
                
                timeseries.append(day_insights)
            
            # Use summary data for accurate totals
            summary_insights = summary_data['data'][0] if summary_data.get('data') else {}
            
            # Extract conversions from summary
            summary_conversions = 0
            summary_actions = summary_insights.get('actions', [])
            for action in summary_actions:
                if action.get('action_type') in [
                    'purchase', 'lead', 'complete_registration',
                    'omni_purchase', 'offsite_conversion.fb_pixel_purchase'
                ]:
                    summary_conversions += int(action.get('value', 0))
            
            return {
                'timeseries': timeseries,
                'summary': {
                    'spend': float(summary_insights.get('spend', 0)),
                    'impressions': int(summary_insights.get('impressions', 0)),
                    'clicks': int(summary_insights.get('clicks', 0)),
                    'conversions': summary_conversions,
                    'cpc': float(summary_insights.get('cpc', 0)),
                    'cpm': float(summary_insights.get('cpm', 0)),
                    'ctr': float(summary_insights.get('ctr', 0)),
                    'reach': int(summary_insights.get('reach', 0)),  # Accurate reach from summary
                    'frequency': float(summary_insights.get('frequency', 0))
                }
            }
        except Exception as e:
            logger.error(f"Error fetching ad account insights timeseries: {e}")
            raise   
 
    # meta_manager.py - Essential functions only

     
    def _get_empty_totals(self) -> Dict:
        """Return empty totals structure"""
        return {
            'total_spend': 0,
            'total_impressions': 0,
            'total_clicks': 0,
            'total_conversions': 0,
            'total_reach': 0
        }

    def get_campaigns_list(self, account_id: str, include_status: list = None) -> Dict:
        """
        Get list of all campaigns for an ad account without date filtering.
        This is faster and returns ALL campaigns regardless of activity.
        
        Args:
            account_id: The ad account ID (e.g., 'act_303894480866908')
            include_status: Optional list of statuses to filter ['ACTIVE', 'PAUSED', 'ARCHIVED']
                        If None, returns all campaigns
        
        Returns:
            Dict with campaigns list and count
        """
        try:
            campaigns = []
            params = {
                'fields': 'id,name,status,objective,created_time,updated_time,start_time,stop_time',
                'limit': 500  # Increased limit for faster fetching
            }
            
            # Add status filter if provided
            if include_status:
                params['filtering'] = json.dumps([
                    {'field': 'status', 'operator': 'IN', 'value': include_status}
                ])
            
            logger.info(f"Fetching campaigns for account: {account_id}")
            
            # Pagination
            next_url = None
            page_count = 0
            
            while True:
                page_count += 1
                logger.info(f"Fetching page {page_count}...")
                
                if next_url:
                    response = requests.get(next_url)
                    if response.status_code != 200:
                        logger.error(f"Pagination failed: {response.status_code}")
                        break
                    data = response.json()
                else:
                    data = self._make_request(f"{account_id}/campaigns", params)
                
                campaign_batch = data.get('data', [])
                logger.info(f"Retrieved {len(campaign_batch)} campaigns in page {page_count}")
                
                campaigns.extend(campaign_batch)
                
                # Check for next page
                paging = data.get('paging', {})
                next_url = paging.get('next')
                if not next_url:
                    break
            
            logger.info(f"Total campaigns retrieved: {len(campaigns)}")
            
            # Group by status for summary
            status_summary = {}
            for campaign in campaigns:
                status = campaign.get('status', 'UNKNOWN')
                status_summary[status] = status_summary.get(status, 0) + 1
            
            return {
                'account_id': account_id,
                'total_campaigns': len(campaigns),
                'status_summary': status_summary,
                'campaigns': campaigns
            }
            
        except Exception as e:
            logger.error(f"Error fetching campaigns list: {e}")
            raise
    
    def get_campaigns_paginated(
        self, 
        account_id: str, 
        period: str = None, 
        start_date: str = None, 
        end_date: str = None,
        limit: int = 5,
        offset: int = 0
    ) -> Dict:
        """
        Get campaigns with pagination and individual insights.
        Returns only the requested page of campaigns.
        
        Args:
            account_id: The ad account ID
            period: Time period
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            limit: Number of campaigns per page (default 5)
            offset: Starting position (default 0)
        """
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        logger.info(f"Fetching campaigns page: offset={offset}, limit={limit}")
        
        try:
            # Step 1: Get ALL campaign IDs first (lightweight query)
            all_campaign_ids = []
            params = {
                'fields': 'id,name,status',
                'limit': 500,
            }
            
            next_url = None
            while True:
                if next_url:
                    time.sleep(self.RATE_LIMIT_DELAY)
                    response = requests.get(next_url)
                    if response.status_code != 200:
                        break
                    data = response.json()
                else:
                    data = self._rate_limited_request(f"{account_id}/campaigns", params)
                
                campaigns_batch = data.get('data', [])
                all_campaign_ids.extend(campaigns_batch)
                
                next_url = data.get('paging', {}).get('next')
                if not next_url:
                    break
            
            total_campaigns = len(all_campaign_ids)
            logger.info(f"Total campaigns available: {total_campaigns}")
            
            # Step 2: Get only the requested page
            paginated_campaigns = all_campaign_ids[offset:offset + limit]
            
            if not paginated_campaigns:
                return {
                    'campaigns': [],
                    'pagination': {
                        'total': total_campaigns,
                        'offset': offset,
                        'limit': limit,
                        'has_more': False
                    }
                }
            
            # Step 3: Fetch insights for only this page
            campaigns_data = []
            
            for campaign in paginated_campaigns:
                campaign_id = campaign.get('id')
                
                campaign_result = {
                    'campaign_id': campaign_id,
                    'campaign_name': campaign.get('name'),
                    'status': campaign.get('status'),
                    'spend': 0.0,
                    'impressions': 0,
                    'clicks': 0,
                    'conversions': 0,
                    'cpc': 0.0,
                    'cpm': 0.0,
                    'ctr': 0.0,
                    'reach': 0,
                    'frequency': 0.0,
                    'has_data': False
                }
                
                try:
                    time.sleep(self.RATE_LIMIT_DELAY)
                    
                    insights = self._rate_limited_request(f"{campaign_id}/insights", {
                        'time_range': json.dumps({"since": since, "until": until}),
                        'fields': 'spend,impressions,clicks,actions,cpc,cpm,ctr,reach,frequency',
                        'action_attribution_windows': ['7d_click', '1d_view']
                    })
                    
                    insights_data = insights.get('data', [])
                    
                    if insights_data:
                        insights_data = insights_data[0]
                        
                        conversions = sum(
                            int(action.get('value', 0))
                            for action in insights_data.get('actions', [])
                            if action.get('action_type') in ['purchase', 'lead', 'complete_registration', 'omni_purchase']
                        )
                        
                        campaign_result.update({
                            'spend': float(insights_data.get('spend', 0)),
                            'impressions': int(insights_data.get('impressions', 0)),
                            'clicks': int(insights_data.get('clicks', 0)),
                            'conversions': conversions,
                            'cpc': float(insights_data.get('cpc', 0)),
                            'cpm': float(insights_data.get('cpm', 0)),
                            'ctr': float(insights_data.get('ctr', 0)),
                            'reach': int(insights_data.get('reach', 0)),
                            'frequency': float(insights_data.get('frequency', 0)),
                            'has_data': True
                        })
                        
                except Exception as e:
                    logger.warning(f"Error fetching insights for campaign {campaign_id}: {e}")
                
                campaigns_data.append(campaign_result)
            
            has_more = (offset + limit) < total_campaigns
            
            return {
                'campaigns': campaigns_data,
                'pagination': {
                    'total': total_campaigns,
                    'offset': offset,
                    'limit': limit,
                    'has_more': has_more,
                    'current_page': (offset // limit) + 1,
                    'total_pages': (total_campaigns + limit - 1) // limit
                }
            }
            
        except Exception as e:
            logger.error(f"Error fetching paginated campaigns: {e}")
            raise

    def get_campaigns_with_totals(self, account_id: str, period: str = None, 
                                    start_date: str = None, end_date: str = None,
                                    max_workers: int = 2) -> Dict:
            """
            Get ALL campaigns with individual metrics and grand totals for an ad account.
            Uses reduced concurrency (max 2 workers) to avoid rate limiting.
            
            Args:
                account_id: The ad account ID
                period: Time period (e.g., '7d', '30d', '90d', '365d')
                start_date: Start date in YYYY-MM-DD format
                end_date: End date in YYYY-MM-DD format
                max_workers: Number of concurrent workers (default: 2, max: 2)
            """
            if start_date and end_date:
                self._validate_date_range(start_date, end_date)
            
            since, until = self._period_to_dates(period, start_date, end_date)
            
            # FORCE max_workers to be 2 or less to avoid rate limiting
            max_workers = min(max_workers, 2)
            
            logger.info(f"Fetching campaigns for {account_id} from {since} to {until}")
            logger.info(f"Using {max_workers} concurrent workers (rate limit safe mode)")
            
            try:
                # Step 1: Get all campaigns first (fast, no insights)
                all_campaigns = []
                params = {
                    'fields': 'id,name,status,objective,created_time,updated_time',
                    'limit': 500,
                }
                
                next_url = None
                page_count = 0
                
                while True:
                    page_count += 1
                    if next_url:
                        # Add rate limiting for pagination
                        time.sleep(0.5)
                        response = requests.get(next_url)
                        if response.status_code != 200:
                            logger.warning(f"Pagination failed at page {page_count}")
                            break
                        data = response.json()
                    else:
                        data = self._rate_limited_request(f"{account_id}/campaigns", params)
                    
                    campaign_batch = data.get('data', [])
                    all_campaigns.extend(campaign_batch)
                    
                    logger.info(f"Page {page_count}: Retrieved {len(campaign_batch)} campaigns")
                    
                    paging = data.get('paging', {})
                    next_url = paging.get('next')
                    if not next_url:
                        break
                
                logger.info(f"Total campaigns found: {len(all_campaigns)}")
                
                if not all_campaigns:
                    logger.warning(f"No campaigns found for account {account_id}")
                    return {
                        'campaigns': [],
                        'totals': self._get_empty_totals(),
                        'metadata': {
                            'total_campaigns': 0,
                            'campaigns_with_data': 0,
                            'campaigns_without_data': 0,
                            'date_range': {'since': since, 'until': until}
                        }
                    }
                
                # Step 2: Fetch insights concurrently with VERY LOW concurrency
                campaigns_data = []
                campaigns_with_activity = 0
                
                def fetch_campaign_insights(campaign: Dict) -> Dict:
                    """Fetch insights for a single campaign with rate limiting"""
                    campaign_result = {
                        'campaign_id': campaign.get('id'),
                        'campaign_name': campaign.get('name'),
                        'status': campaign.get('status'),
                        'objective': campaign.get('objective'),
                        'created_time': campaign.get('created_time'),
                        'updated_time': campaign.get('updated_time'),
                        'spend': 0.0,
                        'impressions': 0,
                        'clicks': 0,
                        'conversions': 0,
                        'cpc': 0.0,
                        'cpm': 0.0,
                        'ctr': 0.0,
                        'reach': 0,
                        'frequency': 0.0,
                        'has_data': False
                    }
                    
                    try:
                        # ADD DELAY before each request
                        time.sleep(0.5)  # 500ms delay = max 2 requests/second
                        
                        insights = self._rate_limited_request(f"{campaign['id']}/insights", {
                            'time_range': json.dumps({"since": since, "until": until}),
                            'fields': 'spend,impressions,clicks,actions,cpc,cpm,ctr,reach,frequency',
                            'action_attribution_windows': ['7d_click', '1d_view']
                        })
                        
                        insights_data = insights.get('data', [])
                        
                        if insights_data:
                            insights_data = insights_data[0]
                            
                            conversions = sum(
                                int(action.get('value', 0))
                                for action in insights_data.get('actions', [])
                                if action.get('action_type') in ['purchase', 'lead', 'complete_registration', 'omni_purchase']
                            )
                            
                            campaign_result.update({
                                'spend': float(insights_data.get('spend', 0)),
                                'impressions': int(insights_data.get('impressions', 0)),
                                'clicks': int(insights_data.get('clicks', 0)),
                                'conversions': conversions,
                                'cpc': float(insights_data.get('cpc', 0)),
                                'cpm': float(insights_data.get('cpm', 0)),
                                'ctr': float(insights_data.get('ctr', 0)),
                                'reach': int(insights_data.get('reach', 0)),
                                'frequency': float(insights_data.get('frequency', 0)),
                                'has_data': True
                            })
                            
                            logger.debug(f"Campaign {campaign['id']} has data")
                            
                    except Exception as e:
                        logger.warning(f"Error fetching insights for campaign {campaign.get('id')}: {e}")
                    
                    return campaign_result
                
                # Use MINIMAL concurrency (2 workers max)
                logger.info(f"Fetching insights for {len(all_campaigns)} campaigns with {max_workers} workers...")
                
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_campaign = {
                        executor.submit(fetch_campaign_insights, campaign): campaign 
                        for campaign in all_campaigns
                    }
                    
                    completed = 0
                    for future in as_completed(future_to_campaign):
                        completed += 1
                        if completed % 10 == 0:
                            logger.info(f"Progress: {completed}/{len(all_campaigns)} campaigns processed")
                        
                        result = future.result()
                        campaigns_data.append(result)
                        
                        if result['has_data']:
                            campaigns_with_activity += 1
                
                logger.info(f"All {len(campaigns_data)} campaigns processed. {campaigns_with_activity} have data in period.")
                
                # Step 3: Calculate grand totals (only from campaigns with data)
                campaigns_with_metrics = [c for c in campaigns_data if c['has_data']]
                
                totals = {
                    'total_spend': sum(c['spend'] for c in campaigns_with_metrics),
                    'total_impressions': sum(c['impressions'] for c in campaigns_with_metrics),
                    'total_clicks': sum(c['clicks'] for c in campaigns_with_metrics),
                    'total_conversions': sum(c['conversions'] for c in campaigns_with_metrics),
                    'total_reach': 0  # Will fetch separately
                }
                
                # Step 4: Get accurate total reach from account level
                try:
                    time.sleep(0.5)  # Delay before final request
                    account_insights = self._rate_limited_request(f"{account_id}/insights", {
                        'time_range': json.dumps({"since": since, "until": until}),
                        'fields': 'reach',
                        'action_attribution_windows': ['7d_click', '1d_view']
                    })
                    totals['total_reach'] = int(account_insights.get('data', [{}])[0].get('reach', 0))
                except Exception as e:
                    logger.warning(f"Could not fetch account-level reach: {e}")
                
                return {
                    'campaigns': campaigns_data,  # ALL campaigns, including those with zero metrics
                    'totals': totals,
                    'metadata': {
                        'total_campaigns': len(all_campaigns),
                        'campaigns_with_data': campaigns_with_activity,
                        'campaigns_without_data': len(all_campaigns) - campaigns_with_activity,
                        'date_range': {'since': since, 'until': until}
                    }
                }
                
            except Exception as e:
                logger.error(f"Error fetching campaigns: {e}")
                raise

    def get_campaigns_timeseries(self, campaign_ids: List[str], period: str = None, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Get time-series data for multiple campaigns"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        results = []
        for campaign_id in campaign_ids:
            try:
                data = self._make_request(f"{campaign_id}/insights", {
                    'time_range': f'{{"since":"{since}","until":"{until}"}}',
                    'fields': 'spend,impressions,clicks,actions,cpc,cpm,ctr,reach,frequency',
                    'time_increment': '1',
                    'action_attribution_windows': ['7d_click', '1d_view']
                })
                
                timeseries = []
                for day_data in data.get('data', []):
                    conversions = sum(
                        int(action.get('value', 0))
                        for action in day_data.get('actions', [])
                        if action.get('action_type') in ['purchase', 'lead', 'complete_registration', 'omni_purchase']
                    )
                    
                    timeseries.append({
                        'date': day_data.get('date_start'),
                        'spend': float(day_data.get('spend', 0)),
                        'impressions': int(day_data.get('impressions', 0)),
                        'clicks': int(day_data.get('clicks', 0)),
                        'conversions': conversions,
                        'cpc': float(day_data.get('cpc', 0)),
                        'cpm': float(day_data.get('cpm', 0)),
                        'ctr': float(day_data.get('ctr', 0)),
                        'reach': int(day_data.get('reach', 0)),
                        'frequency': float(day_data.get('frequency', 0))
                    })
                
                results.append({
                    'campaign_id': campaign_id,
                    'timeseries': timeseries
                })
            except Exception as e:
                logger.error(f"Error fetching timeseries for campaign {campaign_id}: {e}")
        
        return results

    def get_campaigns_demographics(self, campaign_ids: List[str], period: str = None, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Get age/gender demographics for multiple campaigns"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        results = []
        for campaign_id in campaign_ids:
            try:
                data = self._make_request(f"{campaign_id}/insights", {
                    'time_range': f'{{"since":"{since}","until":"{until}"}}',
                    'fields': 'spend,impressions,reach,actions',
                    'breakdowns': 'age,gender',
                    'action_attribution_windows': ['7d_click', '1d_view']
                })
                
                demographics = []
                for item in data.get('data', []):
                    conversions = sum(
                        int(action.get('value', 0))
                        for action in item.get('actions', [])
                        if action.get('action_type') in ['purchase', 'lead', 'complete_registration', 'omni_purchase']
                    )
                    
                    demographics.append({
                        'age': item.get('age'),
                        'gender': item.get('gender'),
                        'spend': float(item.get('spend', 0)),
                        'impressions': int(item.get('impressions', 0)),
                        'reach': int(item.get('reach', 0)),
                        'results': conversions
                    })
                
                results.append({
                    'campaign_id': campaign_id,
                    'demographics': demographics
                })
            except Exception as e:
                logger.error(f"Error fetching demographics for campaign {campaign_id}: {e}")
        
        return results

    def get_campaigns_placements(self, campaign_ids: List[str], period: str = None, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Get platform placement data for multiple campaigns"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        results = []
        for campaign_id in campaign_ids:
            try:
                data = self._make_request(f"{campaign_id}/insights", {
                    'time_range': f'{{"since":"{since}","until":"{until}"}}',
                    'fields': 'spend,impressions,reach,actions',
                    'breakdowns': 'publisher_platform',
                    'action_attribution_windows': ['7d_click', '1d_view']
                })
                
                placements = []
                for item in data.get('data', []):
                    conversions = sum(
                        int(action.get('value', 0))
                        for action in item.get('actions', [])
                        if action.get('action_type') in ['purchase', 'lead', 'complete_registration', 'omni_purchase']
                    )
                    
                    placements.append({
                        'platform': item.get('publisher_platform'),
                        'spend': float(item.get('spend', 0)),
                        'impressions': int(item.get('impressions', 0)),
                        'reach': int(item.get('reach', 0)),
                        'results': conversions
                    })
                
                results.append({
                    'campaign_id': campaign_id,
                    'placements': placements
                })
            except Exception as e:
                logger.error(f"Error fetching placements for campaign {campaign_id}: {e}")
        
        return results
    
    def get_adsets_by_campaigns(self, campaign_ids: List[str], period: str = None, start_date: str = None, end_date: str = None) -> List[Dict]:
            """
            Get ad sets for multiple campaigns with proper rate limiting.
            """
            logger.info(f"Fetching ad sets for {len(campaign_ids)} campaigns")
            
            all_adsets = []
            
            for campaign_id in campaign_ids:
                try:
                    logger.info(f"Fetching ad sets for campaign: {campaign_id}")
                    
                    # Use rate-limited request
                    data = self._rate_limited_request(f"{campaign_id}/adsets", {
                        'fields': 'id,name,status,optimization_goal,billing_event,daily_budget,lifetime_budget,budget_remaining,targeting,created_time,updated_time',
                        'limit': 100
                    })
                    
                    adsets_batch = data.get('data', [])
                    logger.info(f"Campaign {campaign_id}: Found {len(adsets_batch)} ad sets")
                    
                    # Handle pagination with rate limiting
                    next_url = data.get('paging', {}).get('next')
                    page_count = 1
                    
                    while next_url:
                        logger.info(f"Fetching page {page_count + 1} for campaign {campaign_id}")
                        
                        # Rate limit for pagination
                        time.sleep(self.RATE_LIMIT_DELAY)
                        
                        response = requests.get(next_url)
                        if response.status_code != 200:
                            logger.warning(f"Pagination failed at page {page_count + 1}")
                            break
                        
                        page_data = response.json()
                        page_batch = page_data.get('data', [])
                        adsets_batch.extend(page_batch)
                        
                        next_url = page_data.get('paging', {}).get('next')
                        page_count += 1
                    
                    # Process ad sets
                    for adset in adsets_batch:
                        try:
                            targeting = adset.get('targeting', {})
                            geo_locations = targeting.get('geo_locations', {})
                            
                            locations = []
                            if geo_locations.get('countries'):
                                locations.extend(geo_locations.get('countries', []))
                            if geo_locations.get('regions'):
                                locations.extend([r.get('name', r.get('key', '')) for r in geo_locations.get('regions', [])])
                            if geo_locations.get('cities'):
                                locations.extend([c.get('name', c.get('key', '')) for c in geo_locations.get('cities', [])])
                            
                            if not locations:
                                locations = ['Not specified']
                            
                            daily_budget = adset.get('daily_budget')
                            lifetime_budget = adset.get('lifetime_budget')
                            budget_remaining = adset.get('budget_remaining')
                            
                            adset_data = {
                                'id': adset.get('id'),
                                'name': adset.get('name'),
                                'campaign_id': campaign_id,
                                'status': adset.get('status'),
                                'optimization_goal': adset.get('optimization_goal', 'N/A'),
                                'billing_event': adset.get('billing_event', 'N/A'),
                                'daily_budget': float(daily_budget) / 100 if daily_budget else 0,
                                'lifetime_budget': float(lifetime_budget) / 100 if lifetime_budget else 0,
                                'budget_remaining': float(budget_remaining) / 100 if budget_remaining else 0,
                                'locations': locations,
                                'created_time': adset.get('created_time'),
                                'updated_time': adset.get('updated_time')
                            }
                            
                            all_adsets.append(adset_data)
                            
                        except Exception as e:
                            logger.error(f"Error processing ad set {adset.get('id')}: {e}")
                            continue
                    
                except Exception as e:
                    logger.error(f"Error fetching ad sets for campaign {campaign_id}: {e}")
                    continue
            
            logger.info(f"Total ad sets retrieved: {len(all_adsets)}")
            return all_adsets

    def get_adsets_timeseries(self, adset_ids: List[str], period: str = None, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Get time-series data for multiple ad sets"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        results = []
        for adset_id in adset_ids:
            try:
                data = self._make_request(f"{adset_id}/insights", {
                    'time_range': f'{{"since":"{since}","until":"{until}"}}',
                    'fields': 'spend,impressions,clicks,actions,cpc,cpm,ctr,reach,frequency',
                    'time_increment': '1',
                    'action_attribution_windows': ['7d_click', '1d_view']
                })
                
                timeseries = []
                for day_data in data.get('data', []):
                    conversions = sum(
                        int(action.get('value', 0))
                        for action in day_data.get('actions', [])
                        if action.get('action_type') in ['purchase', 'lead', 'complete_registration', 'omni_purchase']
                    )
                    
                    timeseries.append({
                        'date': day_data.get('date_start'),
                        'spend': float(day_data.get('spend', 0)),
                        'impressions': int(day_data.get('impressions', 0)),
                        'clicks': int(day_data.get('clicks', 0)),
                        'conversions': conversions,
                        'cpc': float(day_data.get('cpc', 0)),
                        'cpm': float(day_data.get('cpm', 0)),
                        'ctr': float(day_data.get('ctr', 0)),
                        'reach': int(day_data.get('reach', 0)),
                        'frequency': float(day_data.get('frequency', 0))
                    })
                
                results.append({
                    'adset_id': adset_id,
                    'timeseries': timeseries
                })
            except Exception as e:
                logger.error(f"Error fetching timeseries for adset {adset_id}: {e}")
        
        return results

    def get_adsets_demographics(self, adset_ids: List[str], period: str = None, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Get age/gender demographics for multiple ad sets"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        results = []
        for adset_id in adset_ids:
            try:
                data = self._make_request(f"{adset_id}/insights", {
                    'time_range': f'{{"since":"{since}","until":"{until}"}}',
                    'fields': 'spend,impressions,reach,actions',
                    'breakdowns': 'age,gender',
                    'action_attribution_windows': ['7d_click', '1d_view']
                })
                
                demographics = []
                for item in data.get('data', []):
                    conversions = sum(
                        int(action.get('value', 0))
                        for action in item.get('actions', [])
                        if action.get('action_type') in ['purchase', 'lead', 'complete_registration', 'omni_purchase']
                    )
                    
                    demographics.append({
                        'age': item.get('age'),
                        'gender': item.get('gender'),
                        'spend': float(item.get('spend', 0)),
                        'impressions': int(item.get('impressions', 0)),
                        'reach': int(item.get('reach', 0)),
                        'results': conversions
                    })
                
                results.append({
                    'adset_id': adset_id,
                    'demographics': demographics
                })
            except Exception as e:
                logger.error(f"Error fetching demographics for adset {adset_id}: {e}")
        
        return results

    def get_adsets_placements(self, adset_ids: List[str], period: str = None, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Get platform placement data for multiple ad sets"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        results = []
        for adset_id in adset_ids:
            try:
                data = self._make_request(f"{adset_id}/insights", {
                    'time_range': f'{{"since":"{since}","until":"{until}"}}',
                    'fields': 'spend,impressions,reach,actions',
                    'breakdowns': 'publisher_platform',
                    'action_attribution_windows': ['7d_click', '1d_view']
                })
                
                placements = []
                for item in data.get('data', []):
                    conversions = sum(
                        int(action.get('value', 0))
                        for action in item.get('actions', [])
                        if action.get('action_type') in ['purchase', 'lead', 'complete_registration', 'omni_purchase']
                    )
                    
                    placements.append({
                        'platform': item.get('publisher_platform'),
                        'spend': float(item.get('spend', 0)),
                        'impressions': int(item.get('impressions', 0)),
                        'reach': int(item.get('reach', 0)),
                        'results': conversions
                    })
                
                results.append({
                    'adset_id': adset_id,
                    'placements': placements
                })
            except Exception as e:
                logger.error(f"Error fetching placements for adset {adset_id}: {e}")
        
        return results

    def get_ads_by_adsets(self, adset_ids: List[str]) -> List[Dict]:
        """Get ads for multiple ad sets with preview and direct links"""
        all_ads = []
        for adset_id in adset_ids:
            try:
                data = self._make_request(f"{adset_id}/ads", {
                    'fields': 'id,name,status,creative{title,body,image_url,video_id,thumbnail_url,image_hash,object_story_spec},preview_shareable_link,effective_object_story_id,created_time,updated_time'
                })
                
                for ad in data.get('data', []):
                    creative = ad.get('creative', {})
                    
                    # Build preview URL and direct link
                    ad_id = ad.get('id')
                    preview_link = ad.get('preview_shareable_link')
                    
                    # Construct Facebook Ads Manager link
                    ads_manager_link = f"https://www.facebook.com/adsmanager/manage/ads?act={adset_id.split('_')[0]}&selected_ad_ids={ad_id}"
                    
                    # Get image URL from creative
                    image_url = creative.get('image_url')
                    
                    # If no direct image_url, try to construct from image_hash
                    if not image_url and creative.get('image_hash'):
                        image_hash = creative.get('image_hash')
                        image_url = f"https://scontent.xx.fbcdn.net/v/t45.1600-4/{image_hash}"
                    
                    # Get video thumbnail or image
                    media_url = image_url or creative.get('thumbnail_url')
                    
                    # Try to get the post permalink if available
                    post_link = None
                    effective_story_id = ad.get('effective_object_story_id')
                    if effective_story_id:
                        post_link = f"https://www.facebook.com/{effective_story_id.replace('_', '/posts/')}"
                    
                    all_ads.append({
                        'id': ad_id,
                        'name': ad.get('name'),
                        'ad_set_id': adset_id,
                        'status': ad.get('status'),
                        'creative': {
                            'title': creative.get('title'),
                            'body': creative.get('body'),
                            'image_url': image_url,
                            'video_id': creative.get('video_id'),
                            'thumbnail_url': creative.get('thumbnail_url'),
                            'media_url': media_url  # Primary media URL (image or video thumbnail)
                        },
                        'preview_link': preview_link,  # Shareable preview link
                        'ads_manager_link': ads_manager_link,  # Direct link to Ads Manager
                        'post_link': post_link,  # Direct link to Facebook post (if published)
                        'created_time': ad.get('created_time'),
                        'updated_time': ad.get('updated_time')
                    })
            except Exception as e:
                logger.error(f"Error fetching ads for adset {adset_id}: {e}")
        
        return all_ads

    def get_ads_timeseries(self, ad_ids: List[str], period: str = None, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Get time-series data for multiple ads"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        results = []
        for ad_id in ad_ids:
            try:
                data = self._make_request(f"{ad_id}/insights", {
                    'time_range': f'{{"since":"{since}","until":"{until}"}}',
                    'fields': 'spend,impressions,clicks,actions,cpc,cpm,ctr,reach,frequency',
                    'time_increment': '1',
                    'action_attribution_windows': ['7d_click', '1d_view']
                })
                
                timeseries = []
                for day_data in data.get('data', []):
                    conversions = sum(
                        int(action.get('value', 0))
                        for action in day_data.get('actions', [])
                        if action.get('action_type') in ['purchase', 'lead', 'complete_registration', 'omni_purchase']
                    )
                    
                    timeseries.append({
                        'date': day_data.get('date_start'),
                        'spend': float(day_data.get('spend', 0)),
                        'impressions': int(day_data.get('impressions', 0)),
                        'clicks': int(day_data.get('clicks', 0)),
                        'conversions': conversions,
                        'cpc': float(day_data.get('cpc', 0)),
                        'cpm': float(day_data.get('cpm', 0)),
                        'ctr': float(day_data.get('ctr', 0)),
                        'reach': int(day_data.get('reach', 0)),
                        'frequency': float(day_data.get('frequency', 0))
                    })
                
                results.append({
                    'ad_id': ad_id,
                    'timeseries': timeseries
                })
            except Exception as e:
                logger.error(f"Error fetching timeseries for ad {ad_id}: {e}")
        
        return results

    def get_ads_demographics(self, ad_ids: List[str], period: str = None, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Get age/gender demographics for multiple ads"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        results = []
        for ad_id in ad_ids:
            try:
                data = self._make_request(f"{ad_id}/insights", {
                    'time_range': f'{{"since":"{since}","until":"{until}"}}',
                    'fields': 'spend,impressions,reach,actions',
                    'breakdowns': 'age,gender',
                    'action_attribution_windows': ['7d_click', '1d_view']
                })
                
                demographics = []
                for item in data.get('data', []):
                    conversions = sum(
                        int(action.get('value', 0))
                        for action in item.get('actions', [])
                        if action.get('action_type') in ['purchase', 'lead', 'complete_registration', 'omni_purchase']
                    )
                    
                    demographics.append({
                        'age': item.get('age'),
                        'gender': item.get('gender'),
                        'spend': float(item.get('spend', 0)),
                        'impressions': int(item.get('impressions', 0)),
                        'reach': int(item.get('reach', 0)),
                        'results': conversions
                    })
                
                results.append({
                    'ad_id': ad_id,
                    'demographics': demographics
                })
            except Exception as e:
                logger.error(f"Error fetching demographics for ad {ad_id}: {e}")
        
        return results

    def get_ads_placements(self, ad_ids: List[str], period: str = None, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Get platform placement data for multiple ads"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        results = []
        for ad_id in ad_ids:
            try:
                data = self._make_request(f"{ad_id}/insights", {
                    'time_range': f'{{"since":"{since}","until":"{until}"}}',
                    'fields': 'spend,impressions,reach,actions',
                    'breakdowns': 'publisher_platform',
                    'action_attribution_windows': ['7d_click', '1d_view']
                })
                
                placements = []
                for item in data.get('data', []):
                    conversions = sum(
                        int(action.get('value', 0))
                        for action in item.get('actions', [])
                        if action.get('action_type') in ['purchase', 'lead', 'complete_registration', 'omni_purchase']
                    )
                    
                    placements.append({
                        'platform': item.get('publisher_platform'),
                        'spend': float(item.get('spend', 0)),
                        'impressions': int(item.get('impressions', 0)),
                        'reach': int(item.get('reach', 0)),
                        'results': conversions
                    })
                
                results.append({
                    'ad_id': ad_id,
                    'placements': placements
                })
            except Exception as e:
                logger.error(f"Error fetching placements for ad {ad_id}: {e}")
        
        return results

    # =========================================================================
    # FACEBOOK PAGES
    # =========================================================================
        
    def get_pages(self) -> List[Dict]:
        """Get all Facebook pages with detailed information"""
        try:
            data = self._make_request("me/accounts", {
                'fields': 'id,name,category,fan_count,followers_count,link,about,description,phone,emails,website,single_line_address,location,instagram_business_account{id,username,profile_picture_url}'
            })
            
            pages = []
            for page in data.get('data', []):
                instagram_account = page.get('instagram_business_account')
                
                # Get location details
                location = page.get('location', {})
                
                pages.append({
                    'id': page.get('id'),
                    'name': page.get('name'),
                    'category': page.get('category'),
                    'fan_count': page.get('fan_count', 0),
                    'followers_count': page.get('followers_count', 0),
                    'link': page.get('link'),
                    'about': page.get('about'),
                    'description': page.get('description'),
                    'phone': page.get('phone'),
                    'emails': page.get('emails', []),
                    'website': page.get('website'),
                    'address': page.get('single_line_address'),
                    'location': {
                        'street': location.get('street'),
                        'city': location.get('city'),
                        'state': location.get('state'),
                        'country': location.get('country'),
                        'zip': location.get('zip')
                    } if location else None,
                    'has_instagram': instagram_account is not None,
                    'instagram_account': {
                        'id': instagram_account.get('id'),
                        'username': instagram_account.get('username'),
                        'profile_picture_url': instagram_account.get('profile_picture_url')
                    } if instagram_account else None
                })
            
            return pages
        except Exception as e:
            logger.error(f"Error fetching pages: {e}")
            return []
    
    def get_page_insights_timeseries(self, page_id: str, period: str = None, start_date: str = None, end_date: str = None) -> Dict:
        """Get time-series insights for specific Facebook page with improved error handling"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            # Get page access token
            page_access_token = self._get_page_access_token(page_id)
            
            # Get basic page info first
            try:
                page_info = self._make_request(page_id, {
                    'access_token': page_access_token,
                    'fields': 'followers_count,fan_count,talking_about_count,checkins'
                })
            except Exception as e:
                logger.error(f"Error fetching basic page info: {e}")
                page_info = {
                    'followers_count': 0,
                    'fan_count': 0,
                    'talking_about_count': 0,
                    'checkins': 0
                }
            
            # Define metrics to fetch - SPLIT INTO SMALLER GROUPS
            basic_metrics = {
                'page_impressions': 'impressions',
                'page_impressions_unique': 'unique_impressions',
            }
            
            engagement_metrics = {
                'page_post_engagements': 'post_engagements',
                'page_consumptions': 'engaged_users',
            }
            
            view_metrics = {
                'page_views_total': 'page_views',
            }
            
            fan_metrics = {
                'page_fan_adds': 'new_likes',
            }
            
            # Collect all daily data
            daily_data = {}
            
            # Fetch each metric group separately with error handling
            all_metric_groups = [
                ('basic', basic_metrics),
                ('engagement', engagement_metrics),
                ('view', view_metrics),
                ('fan', fan_metrics)
            ]
            
            for group_name, metrics_config in all_metric_groups:
                for metric_key, metric_name in metrics_config.items():
                    try:
                        logger.info(f"Fetching metric: {metric_key}")
                        
                        data = self._make_request(f"{page_id}/insights/{metric_key}", {
                            'access_token': page_access_token,
                            'since': since,
                            'until': until,
                            'period': 'day'
                        })
                        
                        if data.get('data') and len(data['data']) > 0:
                            values = data['data'][0].get('values', [])
                            
                            for value_entry in values:
                                date = value_entry.get('end_time', '').split('T')[0]
                                value = value_entry.get('value')
                                
                                if date:
                                    if date not in daily_data:
                                        daily_data[date] = {
                                            'date': date,
                                            'impressions': 0,
                                            'unique_impressions': 0,
                                            'post_engagements': 0,
                                            'engaged_users': 0,
                                            'page_views': 0,
                                            'new_likes': 0,
                                            'fans': 0
                                        }
                                    
                                    # Handle different value types
                                    if value is not None:
                                        if isinstance(value, dict):
                                            # For metrics that return objects, sum the values
                                            value = sum(v for v in value.values() if isinstance(v, (int, float)))
                                        daily_data[date][metric_name] = value if isinstance(value, (int, float)) else 0
                                        
                    except Exception as metric_error:
                        logger.warning(f"Metric {metric_key} not available: {metric_error}")
                        continue
            
            # Convert to sorted list
            timeseries = sorted(daily_data.values(), key=lambda x: x['date'])
            
            # Calculate summary totals
            total_impressions = sum(day['impressions'] for day in timeseries)
            total_unique_impressions = sum(day['unique_impressions'] for day in timeseries)
            total_post_engagements = sum(day['post_engagements'] for day in timeseries)
            total_engaged_users = sum(day['engaged_users'] for day in timeseries)
            total_page_views = sum(day['page_views'] for day in timeseries)
            total_new_likes = sum(day['new_likes'] for day in timeseries)
            
            # Get the latest fan count
            latest_fans = page_info.get('fan_count', 0)
            if timeseries and any(day['fans'] > 0 for day in timeseries):
                latest_fans = max((day['fans'] for day in timeseries), default=latest_fans)
            
            summary = {
                'impressions': total_impressions,
                'unique_impressions': total_unique_impressions,
                'engaged_users': total_engaged_users,
                'post_engagements': total_post_engagements,
                'fans': latest_fans,
                'followers': page_info.get('followers_count', 0),
                'page_views': total_page_views,
                'new_likes': total_new_likes,
                'talking_about_count': page_info.get('talking_about_count', 0),
                'checkins': page_info.get('checkins', 0)
            }
            
            return {
                'timeseries': timeseries,
                'summary': summary
            }
            
        except Exception as e:
            logger.error(f"Error fetching page insights timeseries: {e}", exc_info=True)
            # Fallback: return empty timeseries with basic info
            try:
                page_info = self._make_request(page_id, {
                    'fields': 'followers_count,fan_count'
                })
                return {
                    'timeseries': [],
                    'summary': {
                        'impressions': 0,
                        'unique_impressions': 0,
                        'engaged_users': 0,
                        'post_engagements': 0,
                        'fans': page_info.get('fan_count', 0),
                        'followers': page_info.get('followers_count', 0),
                        'page_views': 0,
                        'new_likes': 0,
                        'talking_about_count': 0,
                        'checkins': 0
                    }
                }
            except:
                return {
                    'timeseries': [],
                    'summary': {
                        'impressions': 0,
                        'unique_impressions': 0,
                        'engaged_users': 0,
                        'post_engagements': 0,
                        'fans': 0,
                        'followers': 0,
                        'page_views': 0,
                        'new_likes': 0,
                        'talking_about_count': 0,
                        'checkins': 0
                    }
                }

    def get_page_insights(self, page_id: str, period: str = None, start_date: str = None, end_date: str = None) -> Dict:
        """Get insights for specific Facebook page using Page Access Token with improved error handling"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            # Get page access token
            page_access_token = self._get_page_access_token(page_id)
            
            # Get basic page info with detailed fields
            try:
                page_info = self._make_request(page_id, {
                    'access_token': page_access_token,
                    'fields': 'followers_count,fan_count,new_like_count,talking_about_count,were_here_count,checkins'
                })
            except Exception as e:
                logger.warning(f"Error fetching detailed page info: {e}")
                # Try simpler fields
                try:
                    page_info = self._make_request(page_id, {
                        'access_token': page_access_token,
                        'fields': 'followers_count,fan_count'
                    })
                except:
                    page_info = {'fan_count': 0, 'followers_count': 0}
            
            # Try to get insights using page token
            insights_data = {}
            
            # Try metrics individually to identify which ones fail
            metrics_to_try = [
                'page_impressions',
                'page_impressions_unique', 
                'page_post_engagements',
                'page_consumptions',
                'page_views_total'
            ]
            
            for metric in metrics_to_try:
                try:
                    logger.info(f"Fetching metric: {metric}")
                    data = self._make_request(f"{page_id}/insights/{metric}", {
                        'access_token': page_access_token,
                        'since': since,
                        'until': until,
                        'period': 'day'
                    })
                    
                    if data.get('data'):
                        values = data['data'][0].get('values', [])
                        total = 0
                        for v in values:
                            val = v.get('value', 0)
                            if val is not None:
                                if isinstance(val, dict):
                                    # Sum dict values for metrics that return objects
                                    total += sum(x for x in val.values() if isinstance(x, (int, float)))
                                else:
                                    total += val
                        insights_data[metric] = total
                except Exception as metric_error:
                    logger.warning(f"Metric {metric} not available: {metric_error}")
                    insights_data[metric] = 0
            
            return {
                'impressions': insights_data.get('page_impressions', 0),
                'unique_impressions': insights_data.get('page_impressions_unique', 0),
                'engaged_users': insights_data.get('page_consumptions', 0),
                'post_engagements': insights_data.get('page_post_engagements', 0),
                'fans': page_info.get('fan_count', 0),
                'followers': page_info.get('followers_count', 0),
                'page_views': insights_data.get('page_views_total', 0),
                'new_likes': page_info.get('new_like_count', 0),
                'talking_about_count': page_info.get('talking_about_count', 0),
                'checkins': page_info.get('checkins', 0)
            }
            
        except Exception as e:
            logger.error(f"Error fetching page insights: {e}", exc_info=True)
            # Fallback: return basic info only
            try:
                page_info = self._make_request(page_id, {
                    'fields': 'followers_count,fan_count'
                })
                return {
                    'impressions': 0,
                    'unique_impressions': 0,
                    'engaged_users': 0,
                    'post_engagements': 0,
                    'fans': page_info.get('fan_count', 0),
                    'followers': page_info.get('followers_count', 0),
                    'page_views': 0,
                    'new_likes': 0,
                    'talking_about_count': 0,
                    'checkins': 0
                }
            except:
                return {
                    'impressions': 0,
                    'unique_impressions': 0,
                    'engaged_users': 0,
                    'post_engagements': 0,
                    'fans': 0,
                    'followers': 0,
                    'page_views': 0,
                    'new_likes': 0,
                    'talking_about_count': 0,
                    'checkins': 0
                }

    def _get_page_access_token(self, page_id: str) -> str:
        """Get page access token for a specific page with better error handling"""
        try:
            # Get page access token using the user's access token
            data = self._make_request(f"{page_id}", {
                'fields': 'access_token'
            })
            
            page_access_token = data.get('access_token')
            if not page_access_token:
                logger.warning(f"No page access token available for page {page_id}, using user token")
                return self.access_token  # Fallback to user token
            
            logger.info(f"Successfully retrieved page access token for page {page_id}")
            return page_access_token
        except Exception as e:
            logger.warning(f"Could not get page access token: {e}, using user token as fallback")
            return self.access_token
    
    def get_page_posts(self, page_id: str, limit: int = 10, period: str = None, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Get posts with comprehensive statistics"""
        
        try:
            # Get page access token
            page_data = self._make_request(f"{page_id}", {
                'fields': 'access_token,name'
            })
            
            page_access_token = page_data.get('access_token')
            
            if not page_access_token:
                logger.error("No page access token available!")
                return []
            
            # Fetch posts with all engagement fields
            posts_url = f"{self.BASE_URL}/{page_id}/posts"
            params = {
                'access_token': page_access_token,
                'fields': 'id,message,created_time,story,permalink_url,status_type,attachments{media,type,title,description},reactions.summary(total_count).limit(0),likes.summary(true).limit(0),comments.summary(true).limit(0),shares',
                'limit': limit
            }
            
            response = requests.get(posts_url, params=params)
            
            if response.status_code != 200:
                logger.error(f"Error getting posts: {response.text}")
                return []
            
            data = response.json()
            posts = []
            
            for post in data.get('data', []):
                post_id = post.get('id')
                
                # Extract attachment info
                attachments = post.get('attachments', {}).get('data', [])
                media_url = None
                attachment_type = None
                attachment_title = None
                attachment_description = None
                
                if attachments:
                    first_attachment = attachments[0]
                    attachment_type = first_attachment.get('type')
                    attachment_title = first_attachment.get('title')
                    attachment_description = first_attachment.get('description')
                    
                    media = first_attachment.get('media', {})
                    if media:
                        media_url = media.get('image', {}).get('src')
                
                # Get basic engagement counts
                reactions_count = post.get('reactions', {}).get('summary', {}).get('total_count', 0)
                likes_count = post.get('likes', {}).get('summary', {}).get('total_count', 0)
                comments_count = post.get('comments', {}).get('summary', {}).get('total_count', 0)
                shares_count = post.get('shares', {}).get('count', 0)
                
                # Get detailed reactions breakdown
                reactions_breakdown = {}
                try:
                    reactions_response = requests.get(
                        f"{self.BASE_URL}/{post_id}/reactions",
                        params={
                            'access_token': page_access_token,
                            'summary': 'total_count',
                            'limit': 0
                        }
                    )
                    if reactions_response.status_code == 200:
                        reactions_data = reactions_response.json()
                        reactions_breakdown = reactions_data.get('summary', {})
                except:
                    pass
                
                # Get comprehensive post insights
                post_insights = {
                    'impressions': 0,
                    'impressions_unique': 0,
                    'impressions_paid': 0,
                    'impressions_organic': 0,
                    'reach': 0,
                    'reach_unique': 0,
                    'engaged_users': 0,
                    'clicks': 0,
                    'clicks_unique': 0,
                    'negative_feedback': 0,
                    'video_views': 0,
                    'video_views_10s': 0,
                    'video_avg_time_watched': 0,
                    'video_complete_views': 0
                }
                
                # Fetch all available post insights metrics
                try:
                    all_metrics = [
                        'post_impressions',
                        'post_impressions_unique',
                        'post_impressions_paid',
                        'post_impressions_organic',
                        'post_impressions_viral',
                        'post_impressions_fan',
                        'post_reach',
                        'post_engaged_users',
                        'post_clicks',
                        'post_clicks_unique',
                        'post_negative_feedback',
                        'post_engaged_fan',
                        'post_reactions_by_type_total'
                    ]
                    
                    insights_response = requests.get(
                        f"{self.BASE_URL}/{post_id}/insights",
                        params={
                            'access_token': page_access_token,
                            'metric': ','.join(all_metrics)
                        }
                    )
                    
                    if insights_response.status_code == 200:
                        insights_data = insights_response.json().get('data', [])
                        
                        for metric in insights_data:
                            metric_name = metric.get('name')
                            values = metric.get('values', [])
                            
                            if values and len(values) > 0:
                                value = values[0].get('value', 0)
                                
                                if metric_name == 'post_impressions':
                                    post_insights['impressions'] = value
                                elif metric_name == 'post_impressions_unique':
                                    post_insights['impressions_unique'] = value
                                elif metric_name == 'post_impressions_paid':
                                    post_insights['impressions_paid'] = value
                                elif metric_name == 'post_impressions_organic':
                                    post_insights['impressions_organic'] = value
                                elif metric_name == 'post_reach':
                                    post_insights['reach'] = value
                                elif metric_name == 'post_engaged_users':
                                    post_insights['engaged_users'] = value
                                elif metric_name == 'post_clicks':
                                    post_insights['clicks'] = value
                                elif metric_name == 'post_clicks_unique':
                                    post_insights['clicks_unique'] = value
                                elif metric_name == 'post_negative_feedback':
                                    post_insights['negative_feedback'] = value
                                elif metric_name == 'post_reactions_by_type_total':
                                    post_insights['reactions_by_type'] = value
                    
                    # For video posts, get video insights
                    if attachment_type in ['video', 'video_inline', 'video_autoplay']:
                        video_metrics = [
                            'post_video_views',
                            'post_video_views_10s',
                            'post_video_avg_time_watched',
                            'post_video_complete_views_30s'
                        ]
                        
                        video_insights_response = requests.get(
                            f"{self.BASE_URL}/{post_id}/insights",
                            params={
                                'access_token': page_access_token,
                                'metric': ','.join(video_metrics)
                            }
                        )
                        
                        if video_insights_response.status_code == 200:
                            video_insights_data = video_insights_response.json().get('data', [])
                            
                            for metric in video_insights_data:
                                metric_name = metric.get('name')
                                values = metric.get('values', [])
                                
                                if values and len(values) > 0:
                                    value = values[0].get('value', 0)
                                    
                                    if metric_name == 'post_video_views':
                                        post_insights['video_views'] = value
                                    elif metric_name == 'post_video_views_10s':
                                        post_insights['video_views_10s'] = value
                                    elif metric_name == 'post_video_avg_time_watched':
                                        post_insights['video_avg_time_watched'] = value
                                    elif metric_name == 'post_video_complete_views_30s':
                                        post_insights['video_complete_views'] = value
                    
                except Exception as e:
                    logger.warning(f"Could not fetch insights for post {post_id}: {e}")
                
                # Calculate engagement rate
                total_engagement = reactions_count + comments_count + shares_count + post_insights['clicks']
                engagement_rate = 0
                if post_insights['reach'] > 0:
                    engagement_rate = (total_engagement / post_insights['reach']) * 100
                
                posts.append({
                    'id': post_id,
                    'message': post.get('message', ''),
                    'story': post.get('story', ''),
                    'created_time': post.get('created_time'),
                    'status_type': post.get('status_type'),
                    'type': attachment_type or 'status',
                    'full_picture': media_url,
                    'attachment_type': attachment_type,
                    'attachment_title': attachment_title,
                    'attachment_description': attachment_description,
                    'permalink_url': post.get('permalink_url'),
                    
                    # Engagement metrics
                    'reactions': reactions_count,
                    'reactions_breakdown': reactions_breakdown,
                    'likes': likes_count,
                    'comments': comments_count,
                    'shares': shares_count,
                    'total_engagement': total_engagement,
                    'engagement_rate': round(engagement_rate, 2),
                    
                    # Insights metrics
                    'impressions': post_insights['impressions'],
                    'impressions_unique': post_insights['impressions_unique'],
                    'impressions_paid': post_insights['impressions_paid'],
                    'impressions_organic': post_insights['impressions_organic'],
                    'reach': post_insights['reach'],
                    'engaged_users': post_insights['engaged_users'],
                    'clicks': post_insights['clicks'],
                    'clicks_unique': post_insights['clicks_unique'],
                    'negative_feedback': post_insights['negative_feedback'],
                    
                    # Video metrics (if applicable)
                    'video_views': post_insights.get('video_views', 0),
                    'video_views_10s': post_insights.get('video_views_10s', 0),
                    'video_avg_time_watched': post_insights.get('video_avg_time_watched', 0),
                    'video_complete_views': post_insights.get('video_complete_views', 0)
                })
            
            logger.info(f"Successfully retrieved {len(posts)} posts with comprehensive stats")
            return posts
            
        except Exception as e:
            logger.error(f"Error fetching page posts: {e}", exc_info=True)
            return []
    
    def get_page_posts_timeseries(self, page_id: str, limit: int = 10, period: str = None, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Get posts with time-series statistics"""
        
        try:
            # Get page access token
            page_data = self._make_request(f"{page_id}", {
                'fields': 'access_token,name'
            })
            
            page_access_token = page_data.get('access_token')
            
            if not page_access_token:
                logger.error("No page access token available!")
                return []
            
            # Fetch posts with all engagement fields
            posts_url = f"{self.BASE_URL}/{page_id}/posts"
            params = {
                'access_token': page_access_token,
                'fields': 'id,message,created_time,story,permalink_url,status_type,attachments{media,type,title,description},reactions.summary(total_count).limit(0),likes.summary(true).limit(0),comments.summary(true).limit(0),shares',
                'limit': limit
            }
            
            response = requests.get(posts_url, params=params)
            
            if response.status_code != 200:
                logger.error(f"Error getting posts: {response.text}")
                return []
            
            data = response.json()
            posts = []
            
            for post in data.get('data', []):
                post_id = post.get('id')
                
                # Extract attachment info
                attachments = post.get('attachments', {}).get('data', [])
                media_url = None
                attachment_type = None
                attachment_title = None
                attachment_description = None
                
                if attachments:
                    first_attachment = attachments[0]
                    attachment_type = first_attachment.get('type')
                    attachment_title = first_attachment.get('title')
                    attachment_description = first_attachment.get('description')
                    
                    media = first_attachment.get('media', {})
                    if media:
                        media_url = media.get('image', {}).get('src')
                
                # Get basic engagement counts (current totals)
                reactions_count = post.get('reactions', {}).get('summary', {}).get('total_count', 0)
                likes_count = post.get('likes', {}).get('summary', {}).get('total_count', 0)
                comments_count = post.get('comments', {}).get('summary', {}).get('total_count', 0)
                shares_count = post.get('shares', {}).get('count', 0)
                
                # Get detailed reactions breakdown
                reactions_breakdown = {}
                try:
                    reactions_response = requests.get(
                        f"{self.BASE_URL}/{post_id}/reactions",
                        params={
                            'access_token': page_access_token,
                            'summary': 'total_count',
                            'limit': 0
                        }
                    )
                    if reactions_response.status_code == 200:
                        reactions_data = reactions_response.json()
                        reactions_breakdown = reactions_data.get('summary', {})
                except:
                    pass
                
                # Get TIME-SERIES post insights
                timeseries = []
                summary = {
                    'impressions': 0,
                    'impressions_unique': 0,
                    'impressions_paid': 0,
                    'impressions_organic': 0,
                    'reach': 0,
                    'engaged_users': 0,
                    'clicks': 0,
                    'clicks_unique': 0,
                    'negative_feedback': 0,
                    'video_views': 0,
                    'video_views_10s': 0,
                    'video_avg_time_watched': 0,
                    'video_complete_views': 0
                }
                
                try:
                    # Fetch time-series metrics for the post
                    lifetime_metrics = [
                        'post_impressions',
                        'post_impressions_unique',
                        'post_impressions_paid',
                        'post_impressions_organic',
                        'post_reach',
                        'post_engaged_users',
                        'post_clicks',
                        'post_clicks_unique',
                        'post_negative_feedback'
                    ]
                    
                    insights_response = requests.get(
                        f"{self.BASE_URL}/{post_id}/insights",
                        params={
                            'access_token': page_access_token,
                            'metric': ','.join(lifetime_metrics),
                            'period': 'lifetime'  # Get cumulative data
                        }
                    )
                    
                    if insights_response.status_code == 200:
                        insights_data = insights_response.json().get('data', [])
                        
                        # Organize data by date
                        daily_data = {}
                        
                        for metric in insights_data:
                            metric_name = metric.get('name')
                            values = metric.get('values', [])
                            
                            for value_entry in values:
                                # Get the date from end_time
                                end_time = value_entry.get('end_time', '')
                                date = end_time.split('T')[0] if end_time else None
                                value = value_entry.get('value', 0)
                                
                                if date:
                                    if date not in daily_data:
                                        daily_data[date] = {
                                            'date': date,
                                            'impressions': 0,
                                            'impressions_unique': 0,
                                            'impressions_paid': 0,
                                            'impressions_organic': 0,
                                            'reach': 0,
                                            'engaged_users': 0,
                                            'clicks': 0,
                                            'clicks_unique': 0,
                                            'negative_feedback': 0
                                        }
                                    
                                    # Map metric names to our field names
                                    if metric_name == 'post_impressions':
                                        daily_data[date]['impressions'] = value
                                        summary['impressions'] = max(summary['impressions'], value)
                                    elif metric_name == 'post_impressions_unique':
                                        daily_data[date]['impressions_unique'] = value
                                        summary['impressions_unique'] = max(summary['impressions_unique'], value)
                                    elif metric_name == 'post_impressions_paid':
                                        daily_data[date]['impressions_paid'] = value
                                        summary['impressions_paid'] = max(summary['impressions_paid'], value)
                                    elif metric_name == 'post_impressions_organic':
                                        daily_data[date]['impressions_organic'] = value
                                        summary['impressions_organic'] = max(summary['impressions_organic'], value)
                                    elif metric_name == 'post_reach':
                                        daily_data[date]['reach'] = value
                                        summary['reach'] = max(summary['reach'], value)
                                    elif metric_name == 'post_engaged_users':
                                        daily_data[date]['engaged_users'] = value
                                        summary['engaged_users'] = max(summary['engaged_users'], value)
                                    elif metric_name == 'post_clicks':
                                        daily_data[date]['clicks'] = value
                                        summary['clicks'] = max(summary['clicks'], value)
                                    elif metric_name == 'post_clicks_unique':
                                        daily_data[date]['clicks_unique'] = value
                                        summary['clicks_unique'] = max(summary['clicks_unique'], value)
                                    elif metric_name == 'post_negative_feedback':
                                        daily_data[date]['negative_feedback'] = value
                                        summary['negative_feedback'] = max(summary['negative_feedback'], value)
                        
                        # Convert to sorted list
                        timeseries = sorted(daily_data.values(), key=lambda x: x['date'])
                    
                    # For video posts, get video insights
                    if attachment_type in ['video', 'video_inline', 'video_autoplay']:
                        video_metrics = [
                            'post_video_views',
                            'post_video_views_10s',
                            'post_video_avg_time_watched',
                            'post_video_complete_views_30s'
                        ]
                        
                        video_insights_response = requests.get(
                            f"{self.BASE_URL}/{post_id}/insights",
                            params={
                                'access_token': page_access_token,
                                'metric': ','.join(video_metrics),
                                'period': 'lifetime'
                            }
                        )
                        
                        if video_insights_response.status_code == 200:
                            video_insights_data = video_insights_response.json().get('data', [])
                            
                            # Add video metrics to timeseries
                            video_daily_data = {}
                            
                            for metric in video_insights_data:
                                metric_name = metric.get('name')
                                values = metric.get('values', [])
                                
                                for value_entry in values:
                                    end_time = value_entry.get('end_time', '')
                                    date = end_time.split('T')[0] if end_time else None
                                    value = value_entry.get('value', 0)
                                    
                                    if date:
                                        if date not in video_daily_data:
                                            video_daily_data[date] = {
                                                'video_views': 0,
                                                'video_views_10s': 0,
                                                'video_avg_time_watched': 0,
                                                'video_complete_views': 0
                                            }
                                        
                                        if metric_name == 'post_video_views':
                                            video_daily_data[date]['video_views'] = value
                                            summary['video_views'] = max(summary['video_views'], value)
                                        elif metric_name == 'post_video_views_10s':
                                            video_daily_data[date]['video_views_10s'] = value
                                            summary['video_views_10s'] = max(summary['video_views_10s'], value)
                                        elif metric_name == 'post_video_avg_time_watched':
                                            video_daily_data[date]['video_avg_time_watched'] = value
                                            summary['video_avg_time_watched'] = value  # Use latest value
                                        elif metric_name == 'post_video_complete_views_30s':
                                            video_daily_data[date]['video_complete_views'] = value
                                            summary['video_complete_views'] = max(summary['video_complete_views'], value)
                            
                            # Merge video data into timeseries
                            for day in timeseries:
                                date = day['date']
                                if date in video_daily_data:
                                    day.update(video_daily_data[date])
                                else:
                                    day.update({
                                        'video_views': 0,
                                        'video_views_10s': 0,
                                        'video_avg_time_watched': 0,
                                        'video_complete_views': 0
                                    })
                    
                except Exception as e:
                    logger.warning(f"Could not fetch timeseries insights for post {post_id}: {e}")
                
                # Calculate engagement rate using summary data
                total_engagement = reactions_count + comments_count + shares_count + summary['clicks']
                engagement_rate = 0
                if summary['reach'] > 0:
                    engagement_rate = (total_engagement / summary['reach']) * 100
                
                posts.append({
                    'id': post_id,
                    'message': post.get('message', ''),
                    'story': post.get('story', ''),
                    'created_time': post.get('created_time'),
                    'status_type': post.get('status_type'),
                    'type': attachment_type or 'status',
                    'full_picture': media_url,
                    'attachment_type': attachment_type,
                    'attachment_title': attachment_title,
                    'attachment_description': attachment_description,
                    'permalink_url': post.get('permalink_url'),
                    
                    # Current engagement metrics (not time-series)
                    'reactions': reactions_count,
                    'reactions_breakdown': reactions_breakdown,
                    'likes': likes_count,
                    'comments': comments_count,
                    'shares': shares_count,
                    'total_engagement': total_engagement,
                    'engagement_rate': round(engagement_rate, 2),
                    
                    # Time-series data
                    'timeseries': timeseries,
                    'summary': summary
                })
            
            logger.info(f"Successfully retrieved {len(posts)} posts with timeseries stats")
            return posts
            
        except Exception as e:
            logger.error(f"Error fetching page posts timeseries: {e}", exc_info=True)
            return []    
  
    # Add these methods to your MetaManager class


    def get_page_video_views_breakdown(self, page_id: str, period: str = None, start_date: str = None, end_date: str = None) -> Dict:
        """Get video views breakdown - 3-second views, 1-minute views"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            page_access_token = self._get_page_access_token(page_id)
            
            # Define video view metrics
            video_metrics = {
                'page_video_views': 'total_views',
                'page_video_views_3s': 'three_second_views',
                'page_video_views_60s': 'one_minute_views'
            }
            
            views_data = {}
            
            for metric_key, metric_name in video_metrics.items():
                try:
                    logger.info(f"Fetching video metric: {metric_key}")
                    
                    response = requests.get(
                        f"{self.BASE_URL}/{page_id}/insights/{metric_key}",
                        params={
                            'access_token': page_access_token,
                            'since': since,
                            'until': until,
                            'period': 'day'
                        }
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        if data.get('data') and len(data['data']) > 0:
                            values = data['data'][0].get('values', [])
                            total = sum(v.get('value', 0) for v in values if v.get('value') is not None)
                            views_data[metric_name] = total
                    else:
                        logger.warning(f"Metric {metric_key} failed: {response.text}")
                        views_data[metric_name] = 0
                        
                except Exception as metric_error:
                    logger.warning(f"Metric {metric_key} not available: {metric_error}")
                    views_data[metric_name] = 0
            
            return {
                'total_views': views_data.get('total_views', 0),
                'three_second_views': views_data.get('three_second_views', 0),
                'one_minute_views': views_data.get('one_minute_views', 0),
                'period': period or f"{start_date} to {end_date}"
            }
            
        except Exception as e:
            logger.error(f"Error fetching video views breakdown: {e}", exc_info=True)
            return {
                'total_views': 0,
                'three_second_views': 0,
                'one_minute_views': 0,
                'period': period or f"{start_date} to {end_date}"
            }


    def get_page_content_type_breakdown(self, page_id: str, period: str = None, start_date: str = None, end_date: str = None) -> Dict:
        """Get views breakdown by content type (Reels, Photos, Videos, etc.)"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            page_access_token = self._get_page_access_token(page_id)
            
            # Get posts and aggregate by type
            response = requests.get(
                f"{self.BASE_URL}/{page_id}/posts",
                params={
                    'access_token': page_access_token,
                    'fields': 'id,created_time,attachments{type},insights.metric(post_video_views,post_impressions)',
                    'limit': 100,
                    'since': since,
                    'until': until
                }
            )
            
            if response.status_code != 200:
                logger.error(f"Error getting posts: {response.text}")
                return {
                    'breakdown': [],
                    'total_views': 0,
                    'period': period or f"{start_date} to {end_date}"
                }
            
            data = response.json()
            content_stats = {
                'Reel': {'views': 0, 'count': 0},
                'Photo': {'views': 0, 'count': 0},
                'Video': {'views': 0, 'count': 0},
                'Multi-photo': {'views': 0, 'count': 0},
                'Other': {'views': 0, 'count': 0}
            }
            
            for post in data.get('data', []):
                attachments = post.get('attachments', {}).get('data', [])
                content_type = 'Other'
                
                if attachments:
                    attachment_type = attachments[0].get('type', '').lower()
                    
                    # Determine content type
                    if 'video_inline' in attachment_type or 'video_autoplay' in attachment_type:
                        content_type = 'Reel'
                    elif 'video' in attachment_type:
                        content_type = 'Video'
                    elif 'photo' in attachment_type or 'image' in attachment_type:
                        content_type = 'Photo'
                    elif 'album' in attachment_type:
                        content_type = 'Multi-photo'
                
                # Get views/impressions from insights
                insights = post.get('insights', {}).get('data', [])
                views = 0
                
                for insight in insights:
                    metric_name = insight.get('name')
                    values = insight.get('values', [])
                    
                    if values:
                        if metric_name == 'post_video_views':
                            views = values[0].get('value', 0)
                        elif metric_name == 'post_impressions' and views == 0:
                            views = values[0].get('value', 0)
                
                content_stats[content_type]['views'] += views
                content_stats[content_type]['count'] += 1
            
            # Calculate totals and percentages
            total_views = sum(stats['views'] for stats in content_stats.values())
            
            breakdown = []
            for content_type, stats in content_stats.items():
                if stats['count'] > 0:  # Only include types that have posts
                    percentage = (stats['views'] / total_views * 100) if total_views > 0 else 0
                    breakdown.append({
                        'content_type': content_type,
                        'views': stats['views'],
                        'post_count': stats['count'],
                        'percentage': round(percentage, 1)
                    })
            
            # Sort by views descending
            breakdown.sort(key=lambda x: x['views'], reverse=True)
            
            return {
                'breakdown': breakdown,
                'total_views': total_views,
                'period': period or f"{start_date} to {end_date}"
            }
            
        except Exception as e:
            logger.error(f"Error fetching content type breakdown: {e}", exc_info=True)
            return {
                'breakdown': [],
                'total_views': 0,
                'period': period or f"{start_date} to {end_date}"
            }


    def get_page_follower_demographics(self, page_id: str) -> Dict:
        """Get page audience demographics - age, gender, location"""
        try:
            page_access_token = self._get_page_access_token(page_id)
            
            demographics = {
                'age_gender': [],
                'countries': [],
                'cities': []
            }
            
            # Get age and gender breakdown
            try:
                logger.info(f"Fetching age/gender demographics for page {page_id}")
                
                response = requests.get(
                    f"{self.BASE_URL}/{page_id}/insights/page_fans_gender_age",
                    params={
                        'access_token': page_access_token,
                        'period': 'lifetime'
                    }
                )
                
                logger.info(f"Age/Gender API Response Status: {response.status_code}")
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data.get('data') and len(data['data']) > 0:
                        values = data['data'][0].get('values', [])
                        if values:
                            age_gender_data = values[-1].get('value', {})
                            demographics['age_gender'] = self._parse_age_gender(age_gender_data)
                            logger.info(f"Processed {len(demographics['age_gender'])} age/gender groups")
                else:
                    logger.error(f"Failed to fetch age/gender: {response.text}")
                    
            except Exception as e:
                logger.error(f"Could not fetch age/gender data: {e}", exc_info=True)
            
            # Get country breakdown
            try:
                logger.info(f"Fetching country demographics for page {page_id}")
                
                response = requests.get(
                    f"{self.BASE_URL}/{page_id}/insights/page_fans_country",
                    params={
                        'access_token': page_access_token,
                        'period': 'lifetime'
                    }
                )
                
                logger.info(f"Country API Response Status: {response.status_code}")
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data.get('data') and len(data['data']) > 0:
                        values = data['data'][0].get('values', [])
                        if values:
                            country_data = values[-1].get('value', {})
                            demographics['countries'] = self._parse_countries(country_data)
                            logger.info(f"Processed {len(demographics['countries'])} countries")
                else:
                    logger.error(f"Failed to fetch countries: {response.text}")
                    
            except Exception as e:
                logger.error(f"Could not fetch country data: {e}", exc_info=True)
            
            # Get city breakdown
            try:
                logger.info(f"Fetching city demographics for page {page_id}")
                
                response = requests.get(
                    f"{self.BASE_URL}/{page_id}/insights/page_fans_city",
                    params={
                        'access_token': page_access_token,
                        'period': 'lifetime'
                    }
                )
                
                logger.info(f"City API Response Status: {response.status_code}")
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data.get('data') and len(data['data']) > 0:
                        values = data['data'][0].get('values', [])
                        if values:
                            city_data = values[-1].get('value', {})
                            demographics['cities'] = self._parse_cities(city_data)
                            logger.info(f"Processed {len(demographics['cities'])} cities")
                else:
                    logger.error(f"Failed to fetch cities: {response.text}")
                    
            except Exception as e:
                logger.error(f"Could not fetch city data: {e}", exc_info=True)
            
            return demographics
            
        except Exception as e:
            logger.error(f"Error fetching follower demographics: {e}", exc_info=True)
            return {
                'age_gender': [],
                'countries': [],
                'cities': []
            }


    def _parse_age_gender(self, age_gender_data: dict) -> list:
        """Parse age and gender data"""
        age_groups = {}
        
        for key, count in age_gender_data.items():
            if '.' in key:
                gender, age_range = key.split('.')
                
                if age_range not in age_groups:
                    age_groups[age_range] = {
                        'age_range': age_range,
                        'women': 0,
                        'men': 0,
                        'total': 0
                    }
                
                if gender == 'F':
                    age_groups[age_range]['women'] = count
                elif gender == 'M':
                    age_groups[age_range]['men'] = count
                elif gender == 'U':  # Unknown gender
                    pass  # Optionally add 'unknown' field
                
                age_groups[age_range]['total'] += count
        
        # Calculate percentages
        total_audience = sum(group['total'] for group in age_groups.values())
        
        for group in age_groups.values():
            group['percentage'] = round((group['total'] / total_audience * 100), 1) if total_audience > 0 else 0
        
        return sorted(age_groups.values(), key=lambda x: x['percentage'], reverse=True)


    def _parse_countries(self, country_data: dict) -> list:
        """Parse country data"""
        total = sum(country_data.values())
        
        countries = []
        for country_code, count in country_data.items():
            percentage = (count / total * 100) if total > 0 else 0
            countries.append({
                'country': country_code,
                'count': count,
                'percentage': round(percentage, 1)
            })
        
        return sorted(countries, key=lambda x: x['count'], reverse=True)


    def _parse_cities(self, city_data: dict) -> list:
        """Parse city data"""
        total = sum(city_data.values())
        
        cities = []
        for city_name, count in city_data.items():
            percentage = (count / total * 100) if total > 0 else 0
            cities.append({
                'city': city_name,
                'count': count,
                'percentage': round(percentage, 1)
            })
        
        return sorted(cities, key=lambda x: x['count'], reverse=True)[:10]  # Top 10 cities


    def get_page_follows_unfollows(self, page_id: str, period: str = None, start_date: str = None, end_date: str = None) -> Dict:
        """Get net follows and unfollows data"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            page_access_token = self._get_page_access_token(page_id)
            
            # Get page fan adds (new follows)
            fan_adds = 0
            fan_removes = 0
            
            try:
                response = requests.get(
                    f"{self.BASE_URL}/{page_id}/insights/page_fan_adds",
                    params={
                        'access_token': page_access_token,
                        'since': since,
                        'until': until,
                        'period': 'day'
                    }
                )
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get('data'):
                        values = data['data'][0].get('values', [])
                        fan_adds = sum(v.get('value', 0) for v in values if v.get('value') is not None)
                else:
                    logger.warning(f"Could not fetch fan_adds: {response.text}")
                    
            except Exception as e:
                logger.warning(f"Could not fetch fan_adds: {e}")
            
            # Get page fan removes (unfollows)
            try:
                response = requests.get(
                    f"{self.BASE_URL}/{page_id}/insights/page_fan_removes",
                    params={
                        'access_token': page_access_token,
                        'since': since,
                        'until': until,
                        'period': 'day'
                    }
                )
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get('data'):
                        values = data['data'][0].get('values', [])
                        fan_removes = sum(v.get('value', 0) for v in values if v.get('value') is not None)
                else:
                    logger.warning(f"Could not fetch fan_removes: {response.text}")
                    
            except Exception as e:
                logger.warning(f"Could not fetch fan_removes: {e}")
            
            net_follows = fan_adds - fan_removes
            
            return {
                'new_follows': fan_adds,
                'unfollows': fan_removes,
                'net_follows': net_follows,
                'period': period or f"{start_date} to {end_date}"
            }
            
        except Exception as e:
            logger.error(f"Error fetching follows/unfollows: {e}", exc_info=True)
            return {
                'new_follows': 0,
                'unfollows': 0,
                'net_follows': 0,
                'period': period or f"{start_date} to {end_date}"
            }


    def get_page_engagement_breakdown(self, page_id: str, period: str = None, start_date: str = None, end_date: str = None) -> Dict:
        """Get engagement breakdown - comments, tags, reactions"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            page_access_token = self._get_page_access_token(page_id)
            
            # Get recent posts to aggregate engagement
            response = requests.get(
                f"{self.BASE_URL}/{page_id}/posts",
                params={
                    'access_token': page_access_token,
                    'fields': 'id,created_time,comments.summary(true).limit(0),reactions.summary(true).limit(0),shares',
                    'limit': 100,
                    'since': since,
                    'until': until
                }
            )
            
            if response.status_code != 200:
                logger.error(f"Error getting posts for engagement: {response.text}")
                return {
                    'total_engagement': 0,
                    'total_comments': 0,
                    'total_reactions': 0,
                    'total_shares': 0,
                    'recent_comments': 0,
                    'recent_tags': 0,
                    'period': period or f"{start_date} to {end_date}"
                }
            
            data = response.json()
            
            total_comments = 0
            total_reactions = 0
            total_shares = 0
            recent_comments = 0
            
            from datetime import datetime, timedelta, timezone
            seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)
            
            for post in data.get('data', []):
                comments = post.get('comments', {}).get('summary', {}).get('total_count', 0)
                reactions = post.get('reactions', {}).get('summary', {}).get('total_count', 0)
                shares = post.get('shares', {}).get('count', 0)
                
                total_comments += comments
                total_reactions += reactions
                total_shares += shares
                
                # Count as "recent" if within last 7 days
                created_time = post.get('created_time', '')
                if created_time:
                    try:
                        post_datetime = datetime.fromisoformat(created_time.replace('Z', '+00:00'))
                        if post_datetime >= seven_days_ago:
                            recent_comments += comments
                    except:
                        pass
            
            # Get tags (mentions)
            tags_count = 0
            try:
                tags_response = requests.get(
                    f"{self.BASE_URL}/{page_id}/tagged",
                    params={
                        'access_token': page_access_token,
                        'limit': 100,
                        'since': since,
                        'until': until
                    }
                )
                
                if tags_response.status_code == 200:
                    tags_data = tags_response.json()
                    tags_count = len(tags_data.get('data', []))
            except Exception as e:
                logger.warning(f"Could not fetch tags: {e}")
            
            return {
                'total_engagement': total_comments + total_reactions + total_shares,
                'total_comments': total_comments,
                'total_reactions': total_reactions,
                'total_shares': total_shares,
                'recent_comments': recent_comments,
                'recent_tags': tags_count,
                'period': period or f"{start_date} to {end_date}"
            }
            
        except Exception as e:
            logger.error(f"Error fetching engagement breakdown: {e}", exc_info=True)
            return {
                'total_engagement': 0,
                'total_comments': 0,
                'total_reactions': 0,
                'total_shares': 0,
                'recent_comments': 0,
                'recent_tags': 0,
                'period': period or f"{start_date} to {end_date}"
            }


    def get_page_organic_vs_paid(self, page_id: str, period: str = None, start_date: str = None, end_date: str = None) -> Dict:
        """Get organic vs paid impressions and reach"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            page_access_token = self._get_page_access_token(page_id)
            
            metrics = {
                'organic_impressions': 0,
                'paid_impressions': 0,
                'organic_reach': 0,
                'paid_reach': 0
            }
            
            metric_mapping = {
                'page_impressions_organic': 'organic_impressions',
                'page_impressions_paid': 'paid_impressions',
                'page_impressions_organic_unique': 'organic_reach',
                'page_impressions_paid_unique': 'paid_reach'
            }
            
            for api_metric, result_key in metric_mapping.items():
                try:
                    response = requests.get(
                        f"{self.BASE_URL}/{page_id}/insights/{api_metric}",
                        params={
                            'access_token': page_access_token,
                            'since': since,
                            'until': until,
                            'period': 'day'
                        }
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        if data.get('data'):
                            values = data['data'][0].get('values', [])
                            total = sum(v.get('value', 0) for v in values if v.get('value') is not None)
                            metrics[result_key] = total
                    else:
                        logger.warning(f"Metric {api_metric} failed: {response.text}")
                        
                except Exception as e:
                    logger.warning(f"Metric {api_metric} not available: {e}")
            
            # Calculate percentages
            total_impressions = metrics['organic_impressions'] + metrics['paid_impressions']
            total_reach = metrics['organic_reach'] + metrics['paid_reach']
            
            organic_impression_pct = (metrics['organic_impressions'] / total_impressions * 100) if total_impressions > 0 else 0
            paid_impression_pct = (metrics['paid_impressions'] / total_impressions * 100) if total_impressions > 0 else 0
            
            return {
                'organic': {
                    'impressions': metrics['organic_impressions'],
                    'reach': metrics['organic_reach'],
                    'impression_percentage': round(organic_impression_pct, 1)
                },
                'paid': {
                    'impressions': metrics['paid_impressions'],
                    'reach': metrics['paid_reach'],
                    'impression_percentage': round(paid_impression_pct, 1)
                },
                'total_impressions': total_impressions,
                'total_reach': total_reach,
                'period': period or f"{start_date} to {end_date}"
            }
            
        except Exception as e:
            logger.error(f"Error fetching organic vs paid: {e}", exc_info=True)
            return {
                'organic': {'impressions': 0, 'reach': 0, 'impression_percentage': 0},
                'paid': {'impressions': 0, 'reach': 0, 'impression_percentage': 0},
                'total_impressions': 0,
                'total_reach': 0,
                'period': period or f"{start_date} to {end_date}"
            }

  # =========================================================================
    # INSTAGRAM
    # =========================================================================
    
    def get_instagram_accounts(self) -> List[Dict]:
        """Get Instagram Business accounts connected to Facebook pages"""
        try:
            pages = self.get_pages()
            instagram_accounts = []
            
            for page in pages:
                if page.get('has_instagram') and page.get('instagram_account'):
                    ig_account = page['instagram_account']
                    ig_id = ig_account['id']
                    
                    # Get Instagram account details
                    ig_data = self._make_request(ig_id, {
                        'fields': 'id,username,name,profile_picture_url,followers_count,follows_count,media_count'
                    })
                    
                    instagram_accounts.append({
                        'id': ig_data.get('id'),
                        'username': ig_data.get('username'),
                        'name': ig_data.get('name'),
                        'profile_picture_url': ig_data.get('profile_picture_url'),
                        'followers_count': ig_data.get('followers_count', 0),
                        'follows_count': ig_data.get('follows_count', 0),
                        'media_count': ig_data.get('media_count', 0),
                        'connected_facebook_page': {
                            'id': page['id'],
                            'name': page['name']
                        }
                    })
            
            return instagram_accounts
        except Exception as e:
            logger.error(f"Error fetching Instagram accounts: {e}")
            return []
            
    def get_instagram_insights(self, account_id: str, period: str = None, start_date: str = None, end_date: str = None) -> Dict:
        """Get comprehensive Instagram Business account insights"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            insights_dict = {}
            
            # Metrics that require metric_type=total_value
            total_value_metrics = [
                'profile_views',
                'website_clicks',
                'accounts_engaged',
                'total_interactions'
            ]
            
            # Fetch metrics with total_value type
            try:
                data = self._make_request(f"{account_id}/insights", {
                    'metric': ','.join(total_value_metrics),
                    'period': 'day',
                    'metric_type': 'total_value',
                    'since': since,
                    'until': until
                })
                
                for metric_data in data.get('data', []):
                    metric_name = metric_data.get('name')
                    total_value = metric_data.get('total_value', {}).get('value', 0)
                    insights_dict[metric_name] = total_value
                    
            except Exception as e:
                logger.warning(f"Could not fetch total_value metrics: {e}")
            
            # Fetch reach separately (doesn't need metric_type)
            try:
                reach_data = self._make_request(f"{account_id}/insights", {
                    'metric': 'reach',
                    'period': 'day',
                    'since': since,
                    'until': until
                })
                
                for metric_data in reach_data.get('data', []):
                    if metric_data.get('name') == 'reach':
                        values = metric_data.get('values', [])
                        total = sum(v.get('value', 0) for v in values if v.get('value') is not None)
                        insights_dict['reach'] = total
                        
            except Exception as e:
                logger.warning(f"Could not fetch reach: {e}")
            
            # Get current account info (snapshot data)
            try:
                account_info = self._make_request(account_id, {
                    'fields': 'followers_count,media_count,follows_count'
                })
            except:
                account_info = {}
            
            return {
                'reach': insights_dict.get('reach', 0),
                'profile_views': insights_dict.get('profile_views', 0),
                'website_clicks': insights_dict.get('website_clicks', 0),
                'followers_count': account_info.get('followers_count', 0),
                'accounts_engaged': insights_dict.get('accounts_engaged', 0),
                'total_interactions': insights_dict.get('total_interactions', 0),
                'media_count': account_info.get('media_count', 0)
            }
            
        except Exception as e:
            logger.error(f"Error fetching Instagram insights: {e}")
            raise

    def get_instagram_insights_timeseries(self, account_id: str, period: str = None, start_date: str = None, end_date: str = None) -> Dict:
        """Get time-series Instagram Business account insights"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            # Metrics that require metric_type=total_value
            total_value_metrics = [
                'profile_views',
                'website_clicks',
                'accounts_engaged',
                'total_interactions'
            ]
            
            # Collect all daily data
            daily_data = {}
            
            # Fetch metrics with total_value type
            try:
                data = self._make_request(f"{account_id}/insights", {
                    'metric': ','.join(total_value_metrics),
                    'period': 'day',
                    'metric_type': 'total_value',
                    'since': since,
                    'until': until
                })
                
                for metric_data in data.get('data', []):
                    metric_name = metric_data.get('name')
                    values = metric_data.get('values', [])
                    
                    for value_entry in values:
                        end_time = value_entry.get('end_time', '')
                        date = end_time.split('T')[0] if end_time else None
                        value = value_entry.get('value', 0)
                        
                        if date:
                            if date not in daily_data:
                                daily_data[date] = {
                                    'date': date,
                                    'reach': 0,
                                    'profile_views': 0,
                                    'website_clicks': 0,
                                    'accounts_engaged': 0,
                                    'total_interactions': 0
                                }
                            
                            daily_data[date][metric_name] = value if value is not None else 0
                            
            except Exception as e:
                logger.warning(f"Could not fetch total_value metrics: {e}")
            
            # Fetch reach separately (doesn't need metric_type)
            try:
                reach_data = self._make_request(f"{account_id}/insights", {
                    'metric': 'reach',
                    'period': 'day',
                    'since': since,
                    'until': until
                })
                
                for metric_data in reach_data.get('data', []):
                    if metric_data.get('name') == 'reach':
                        values = metric_data.get('values', [])
                        
                        for value_entry in values:
                            end_time = value_entry.get('end_time', '')
                            date = end_time.split('T')[0] if end_time else None
                            value = value_entry.get('value', 0)
                            
                            if date:
                                if date not in daily_data:
                                    daily_data[date] = {
                                        'date': date,
                                        'reach': 0,
                                        'profile_views': 0,
                                        'website_clicks': 0,
                                        'accounts_engaged': 0,
                                        'total_interactions': 0
                                    }
                                
                                daily_data[date]['reach'] = value if value is not None else 0
                                
            except Exception as e:
                logger.warning(f"Could not fetch reach: {e}")
            
            # Convert to sorted list
            timeseries = sorted(daily_data.values(), key=lambda x: x['date'])
            
            # Calculate summary totals
            total_reach = sum(day['reach'] for day in timeseries)
            total_profile_views = sum(day['profile_views'] for day in timeseries)
            total_website_clicks = sum(day['website_clicks'] for day in timeseries)
            total_accounts_engaged = sum(day['accounts_engaged'] for day in timeseries)
            total_interactions = sum(day['total_interactions'] for day in timeseries)
            
            # Get current account info (snapshot data)
            try:
                account_info = self._make_request(account_id, {
                    'fields': 'followers_count,media_count,follows_count'
                })
            except:
                account_info = {}
            
            summary = {
                'reach': total_reach,
                'profile_views': total_profile_views,
                'website_clicks': total_website_clicks,
                'followers_count': account_info.get('followers_count', 0),
                'accounts_engaged': total_accounts_engaged,
                'total_interactions': total_interactions,
                'media_count': account_info.get('media_count', 0)
            }
            
            return {
                'timeseries': timeseries,
                'summary': summary
            }
            
        except Exception as e:
            logger.error(f"Error fetching Instagram insights timeseries: {e}")
            return {
                'timeseries': [],
                'summary': {
                    'reach': 0,
                    'profile_views': 0,
                    'website_clicks': 0,
                    'followers_count': 0,
                    'accounts_engaged': 0,
                    'total_interactions': 0,
                    'media_count': 0
                }
            }
        
    def get_instagram_media(self, account_id: str, limit: int = 10, period: str = None, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Get recent media from Instagram account with comprehensive insights"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
            since_timestamp = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
        else:
            days = int(period[:-1]) if period else 30
            since_timestamp = int((datetime.now() - timedelta(days=days)).timestamp())
        
        try:
            data = self._make_request(f"{account_id}/media", {
                'fields': 'id,caption,media_type,media_url,thumbnail_url,permalink,timestamp,like_count,comments_count,media_product_type',
                'limit': limit
            })
            
            media_items = []
            now = datetime.now()
            
            for media in data.get('data', []):
                # Parse timestamp
                timestamp_str = media.get('timestamp', '')
                try:
                    if '+0000' in timestamp_str:
                        timestamp_str = timestamp_str.replace('+0000', '+00:00')
                    elif 'Z' in timestamp_str:
                        timestamp_str = timestamp_str.replace('Z', '+00:00')
                    
                    media_datetime = datetime.fromisoformat(timestamp_str)
                    media_timestamp = media_datetime.timestamp()
                    
                    if media_timestamp < since_timestamp:
                        continue
                    
                    # Check if media is at least 24 hours old
                    hours_old = (now - media_datetime.replace(tzinfo=None)).total_seconds() / 3600
                    is_old_enough = hours_old >= 24
                    
                except Exception as ts_error:
                    logger.warning(f"Could not parse timestamp {media.get('timestamp')}: {ts_error}")
                    is_old_enough = True  # Assume it's old enough if we can't parse
                
                # Get media insights (only if media is old enough)
                insights_dict = {
                    'impressions': 0,
                    'reach': 0,
                    'engagement': 0,
                    'saved': 0
                }
                
                if is_old_enough:
                    media_id = media['id']
                    media_type = media.get('media_type')
                    media_product_type = media.get('media_product_type', 'FEED')
                    
                    # Different metrics for different media types
                    if media_product_type == 'STORY' or media_product_type == 'REELS':
                        # Reels and Stories have different metrics
                        try:
                            if media_product_type == 'REELS':
                                insights = self._make_request(f"{media_id}/insights", {
                                    'metric': 'plays,reach,total_interactions,saved'
                                })
                            else:  # STORY
                                insights = self._make_request(f"{media_id}/insights", {
                                    'metric': 'impressions,reach,exits,replies'
                                })
                            
                            for insight in insights.get('data', []):
                                metric_name = insight.get('name')
                                values = insight.get('values', [])
                                if values:
                                    value = values[0].get('value', 0)
                                    
                                    if metric_name in ['impressions', 'plays']:
                                        insights_dict['impressions'] = value
                                    elif metric_name == 'reach':
                                        insights_dict['reach'] = value
                                    elif metric_name in ['total_interactions', 'replies']:
                                        insights_dict['engagement'] = value
                                    elif metric_name == 'saved':
                                        insights_dict['saved'] = value
                                        
                        except Exception as e:
                            logger.debug(f"Could not fetch {media_product_type} insights for {media_id}: {e}")
                    
                    else:  # Regular FEED posts
                        try:
                            insights = self._make_request(f"{media_id}/insights", {
                                'metric': 'impressions,reach,engagement,saved'
                            })
                            
                            for insight in insights.get('data', []):
                                metric_name = insight.get('name')
                                values = insight.get('values', [])
                                if values:
                                    insights_dict[metric_name] = values[0].get('value', 0)
                                    
                        except Exception as e:
                            logger.debug(f"Could not fetch feed insights for {media_id}: {e}")
                else:
                    logger.debug(f"Media {media['id']} is less than 24 hours old, insights not yet available")
                
                media_items.append({
                    'id': media.get('id'),
                    'caption': media.get('caption', ''),
                    'media_type': media.get('media_type'),
                    'media_product_type': media.get('media_product_type', 'FEED'),
                    'media_url': media.get('media_url'),
                    'thumbnail_url': media.get('thumbnail_url'),
                    'permalink': media.get('permalink'),
                    'timestamp': media.get('timestamp'),
                    'like_count': media.get('like_count', 0),
                    'comments_count': media.get('comments_count', 0),
                    'impressions': insights_dict['impressions'],
                    'reach': insights_dict['reach'],
                    'engagement': insights_dict['engagement'],
                    'saved': insights_dict['saved'],
                    'insights_available': is_old_enough
                })
            
            logger.info(f"Retrieved {len(media_items)} Instagram media items")
            return media_items
            
        except Exception as e:
            logger.error(f"Error fetching Instagram media: {e}")
            return []   
    
    def get_instagram_media_timeseries(self, account_id: str, limit: int = 10, period: str = None, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Get recent media from Instagram account with time-series insights"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
            since_timestamp = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
        else:
            days = int(period[:-1]) if period else 30
            since_timestamp = int((datetime.now() - timedelta(days=days)).timestamp())
        
        try:
            data = self._make_request(f"{account_id}/media", {
                'fields': 'id,caption,media_type,media_url,thumbnail_url,permalink,timestamp,like_count,comments_count,media_product_type',
                'limit': limit
            })
            
            media_items = []
            now = datetime.now()
            
            for media in data.get('data', []):
                # Parse timestamp
                timestamp_str = media.get('timestamp', '')
                try:
                    if '+0000' in timestamp_str:
                        timestamp_str = timestamp_str.replace('+0000', '+00:00')
                    elif 'Z' in timestamp_str:
                        timestamp_str = timestamp_str.replace('Z', '+00:00')
                    
                    media_datetime = datetime.fromisoformat(timestamp_str)
                    media_timestamp = media_datetime.timestamp()
                    
                    if media_timestamp < since_timestamp:
                        continue
                    
                    # Check if media is at least 24 hours old
                    hours_old = (now - media_datetime.replace(tzinfo=None)).total_seconds() / 3600
                    is_old_enough = hours_old >= 24
                    
                except Exception as ts_error:
                    logger.warning(f"Could not parse timestamp {media.get('timestamp')}: {ts_error}")
                    is_old_enough = True
                
                media_id = media['id']
                media_type = media.get('media_type')
                media_product_type = media.get('media_product_type', 'FEED')
                
                # Get time-series insights (only if media is old enough)
                timeseries = []
                summary = {
                    'impressions': 0,
                    'reach': 0,
                    'engagement': 0,
                    'saved': 0
                }
                
                if is_old_enough:
                    try:
                        # Different metrics for different media types
                        if media_product_type == 'REELS':
                            # For Reels, get lifetime metrics (Instagram doesn't provide daily breakdown for reels)
                            insights = self._make_request(f"{media_id}/insights", {
                                'metric': 'plays,reach,total_interactions,saved',
                                'period': 'lifetime'
                            })
                            
                            # Reels typically only have lifetime data, not daily breakdown
                            for insight in insights.get('data', []):
                                metric_name = insight.get('name')
                                values = insight.get('values', [])
                                
                                if values:
                                    for value_entry in values:
                                        value = value_entry.get('value', 0)
                                        
                                        if metric_name in ['plays']:
                                            summary['impressions'] = value
                                        elif metric_name == 'reach':
                                            summary['reach'] = value
                                        elif metric_name == 'total_interactions':
                                            summary['engagement'] = value
                                        elif metric_name == 'saved':
                                            summary['saved'] = value
                            
                            # For reels, create a single data point since daily breakdown isn't available
                            if summary['impressions'] > 0 or summary['reach'] > 0:
                                timeseries.append({
                                    'date': media.get('timestamp', '').split('T')[0],
                                    'impressions': summary['impressions'],
                                    'reach': summary['reach'],
                                    'engagement': summary['engagement'],
                                    'saved': summary['saved']
                                })
                        
                        elif media_product_type == 'STORY':
                            # Stories also typically only have lifetime data
                            insights = self._make_request(f"{media_id}/insights", {
                                'metric': 'impressions,reach,exits,replies'
                            })
                            
                            for insight in insights.get('data', []):
                                metric_name = insight.get('name')
                                values = insight.get('values', [])
                                
                                if values:
                                    value = values[0].get('value', 0)
                                    
                                    if metric_name == 'impressions':
                                        summary['impressions'] = value
                                    elif metric_name == 'reach':
                                        summary['reach'] = value
                                    elif metric_name == 'replies':
                                        summary['engagement'] = value
                            
                            # Single data point for stories
                            if summary['impressions'] > 0 or summary['reach'] > 0:
                                timeseries.append({
                                    'date': media.get('timestamp', '').split('T')[0],
                                    'impressions': summary['impressions'],
                                    'reach': summary['reach'],
                                    'engagement': summary['engagement'],
                                    'saved': summary['saved']
                                })
                        
                        else:  # Regular FEED posts - try to get lifetime data with breakdown
                            insights = self._make_request(f"{media_id}/insights", {
                                'metric': 'impressions,reach,engagement,saved',
                                'period': 'lifetime'
                            })
                            
                            # Organize data by date
                            daily_data = {}
                            
                            for insight in insights.get('data', []):
                                metric_name = insight.get('name')
                                values = insight.get('values', [])
                                
                                for value_entry in values:
                                    # Try to get end_time for daily breakdown
                                    end_time = value_entry.get('end_time', '')
                                    date = end_time.split('T')[0] if end_time else media.get('timestamp', '').split('T')[0]
                                    value = value_entry.get('value', 0)
                                    
                                    if date:
                                        if date not in daily_data:
                                            daily_data[date] = {
                                                'date': date,
                                                'impressions': 0,
                                                'reach': 0,
                                                'engagement': 0,
                                                'saved': 0
                                            }
                                        
                                        daily_data[date][metric_name] = value if value is not None else 0
                                        
                                        # Update summary with max values (lifetime cumulative)
                                        summary[metric_name] = max(summary[metric_name], value)
                            
                            # Convert to sorted list
                            timeseries = sorted(daily_data.values(), key=lambda x: x['date'])
                            
                            # If no daily breakdown available, create single summary point
                            if not timeseries and (summary['impressions'] > 0 or summary['reach'] > 0):
                                timeseries.append({
                                    'date': media.get('timestamp', '').split('T')[0],
                                    'impressions': summary['impressions'],
                                    'reach': summary['reach'],
                                    'engagement': summary['engagement'],
                                    'saved': summary['saved']
                                })
                    
                    except Exception as e:
                        logger.debug(f"Could not fetch insights for {media_id}: {e}")
                else:
                    logger.debug(f"Media {media_id} is less than 24 hours old, insights not yet available")
                
                media_items.append({
                    'id': media.get('id'),
                    'caption': media.get('caption', ''),
                    'media_type': media.get('media_type'),
                    'media_product_type': media_product_type,
                    'media_url': media.get('media_url'),
                    'thumbnail_url': media.get('thumbnail_url'),
                    'permalink': media.get('permalink'),
                    'timestamp': media.get('timestamp'),
                    'like_count': media.get('like_count', 0),
                    'comments_count': media.get('comments_count', 0),
                    'insights_available': is_old_enough,
                    'timeseries': timeseries,
                    'summary': summary
                })
            
            logger.info(f"Retrieved {len(media_items)} Instagram media items with timeseries")
            return media_items
            
        except Exception as e:
            logger.error(f"Error fetching Instagram media timeseries: {e}")
            return []

    # =========================================================================
    # COMBINED OVERVIEW
    # =========================================================================
    
    def get_meta_overview(self, period: str = None, start_date: str = None, end_date: str = None) -> Dict:
        """Get combined overview of all Meta assets"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        try:
            ad_accounts = self.get_ad_accounts()
            pages = self.get_pages()
            instagram_accounts = self.get_instagram_accounts()
            
            # Calculate totals
            total_ad_spend = sum(acc.get('amount_spent', 0) for acc in ad_accounts)
            total_page_followers = sum(page.get('followers_count', 0) for page in pages)
            total_instagram_followers = sum(acc.get('followers_count', 0) for acc in instagram_accounts)
            
            return {
                'ad_accounts_count': len(ad_accounts),
                'pages_count': len(pages),
                'instagram_accounts_count': len(instagram_accounts),
                'total_ad_spend': total_ad_spend,
                'total_page_followers': total_page_followers,
                'total_instagram_followers': total_instagram_followers,
                'total_social_followers': total_page_followers + total_instagram_followers,
                'ad_accounts': ad_accounts,
                'pages': pages,
                'instagram_accounts': instagram_accounts
            }
        except Exception as e:
            logger.error(f"Error fetching Meta overview: {e}")
            raise