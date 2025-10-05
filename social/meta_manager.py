"""
Meta Manager - Unified handler for Facebook Pages, Instagram, and Meta Ads
"""

import logging
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from fastapi import HTTPException

logger = logging.getLogger(__name__)

class MetaManager:
    """Unified manager for all Meta platforms (Facebook, Instagram, Ads)"""
    
    GRAPH_API_VERSION = "v21.0"
    BASE_URL = f"https://graph.facebook.com/{GRAPH_API_VERSION}"
    
    def __init__(self, user_email: str, auth_manager):
        self.user_email = user_email
        self.auth_manager = auth_manager
        self.access_token = self._get_access_token()
    
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
        """Make request to Facebook Graph API"""
        if params is None:
            params = {}
        
        # Only add access_token if not already provided
        if 'access_token' not in params:
            params['access_token'] = self.access_token
        
        url = f"{self.BASE_URL}/{endpoint}"
        response = requests.get(url, params=params)
        
        if response.status_code != 200:
            logger.error(f"Meta API error: {response.text}")
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Meta API error: {response.json().get('error', {}).get('message', 'Unknown error')}"
            )
        
        return response.json()
    
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
    
    def get_campaigns(self, account_id: str, period: str = None, start_date: str = None, end_date: str = None, limit: int = None) -> List[Dict]:
        """Get campaigns for ad account with pagination support"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            campaigns = []
            params = {
                'fields': 'id,name,objective,status,created_time,updated_time',
                'limit': 100  # Increase page size to reduce API calls
            }
            
            # Fetch all campaigns with pagination
            next_url = None
            while True:
                if next_url:
                    # Use the full next URL from pagination
                    response = requests.get(next_url)
                    if response.status_code != 200:
                        logger.error(f"Error fetching paginated campaigns: {response.text}")
                        break
                    data = response.json()
                else:
                    # First request
                    data = self._make_request(f"{account_id}/campaigns", params)
                
                campaign_batch = data.get('data', [])
                
                # Process this batch of campaigns
                for campaign in campaign_batch:
                    # Get insights for each campaign
                    try:
                        insights = self._make_request(f"{campaign['id']}/insights", {
                            'time_range': f'{{"since":"{since}","until":"{until}"}}',
                            'fields': 'spend,impressions,clicks,cpc,cpm,ctr,actions'
                        })
                        
                        insights_data = insights.get('data', [])
                        if insights_data:
                            campaign_insights = insights_data[0]
                        else:
                            campaign_insights = {}
                            logger.debug(f"No insights data for campaign {campaign.get('name')} in period {since} to {until}")
                        
                    except Exception as insight_error:
                        logger.warning(f"Could not fetch insights for campaign {campaign.get('id')}: {insight_error}")
                        campaign_insights = {}
                    
                    # Extract conversions
                    conversions = 0
                    actions = campaign_insights.get('actions', [])
                    for action in actions:
                        if action.get('action_type') in ['purchase', 'lead', 'complete_registration', 'omni_purchase']:
                            conversions += int(action.get('value', 0))
                    
                    campaigns.append({
                        'id': campaign.get('id'),
                        'name': campaign.get('name'),
                        'objective': campaign.get('objective'),
                        'status': campaign.get('status'),
                        'spend': float(campaign_insights.get('spend', 0)),
                        'impressions': int(campaign_insights.get('impressions', 0)),
                        'clicks': int(campaign_insights.get('clicks', 0)),
                        'conversions': conversions,
                        'cpc': float(campaign_insights.get('cpc', 0)),
                        'cpm': float(campaign_insights.get('cpm', 0)),
                        'ctr': float(campaign_insights.get('ctr', 0)),
                        'created_time': campaign.get('created_time'),
                        'updated_time': campaign.get('updated_time')
                    })
                
                # Check if there's a next page
                paging = data.get('paging', {})
                next_url = paging.get('next')
                
                # Stop if no more pages or reached limit
                if not next_url or (limit and len(campaigns) >= limit):
                    break
            
            logger.info(f"Retrieved {len(campaigns)} campaigns for account {account_id}")
            return campaigns[:limit] if limit else campaigns
            
        except Exception as e:
            logger.error(f"Error fetching campaigns: {e}")
            return [] 
    
    def get_campaign_demographics(self, campaign_id: str, period: str = None, start_date: str = None, end_date: str = None) -> Dict:
        """Get demographic breakdowns for a campaign"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            # Age and Gender breakdown
            age_gender_data = self._make_request(f"{campaign_id}/insights", {
                'time_range': f'{{"since":"{since}","until":"{until}"}}',
                'fields': 'impressions,clicks,spend,actions',
                'breakdowns': 'age,gender',
                'level': 'campaign',
                'action_attribution_windows': ['7d_click', '1d_view']
            })
            
            # Country breakdown
            country_data = self._make_request(f"{campaign_id}/insights", {
                'time_range': f'{{"since":"{since}","until":"{until}"}}',
                'fields': 'impressions,clicks,spend',
                'breakdowns': 'country',
                'level': 'campaign',
                'action_attribution_windows': ['7d_click', '1d_view']
            })
            
            # Region breakdown
            region_data = self._make_request(f"{campaign_id}/insights", {
                'time_range': f'{{"since":"{since}","until":"{until}"}}',
                'fields': 'impressions,clicks,spend',
                'breakdowns': 'region',
                'level': 'campaign',
                'action_attribution_windows': ['7d_click', '1d_view']
            })
            
            # Process age and gender data
            age_gender = []
            for item in age_gender_data.get('data', []):
                conversions = 0
                actions = item.get('actions', [])
                for action in actions:
                    if action.get('action_type') in ['purchase', 'lead', 'complete_registration', 'omni_purchase']:
                        conversions += int(action.get('value', 0))
                
                age_gender.append({
                    'age': item.get('age'),
                    'gender': item.get('gender'),
                    'impressions': int(item.get('impressions', 0)),
                    'clicks': int(item.get('clicks', 0)),
                    'spend': float(item.get('spend', 0)),
                    'conversions': conversions
                })
            
            # Process country data
            countries = []
            for item in country_data.get('data', []):
                countries.append({
                    'country': item.get('country'),
                    'impressions': int(item.get('impressions', 0)),
                    'clicks': int(item.get('clicks', 0)),
                    'spend': float(item.get('spend', 0))
                })
            
            # Process region data
            regions = []
            for item in region_data.get('data', []):
                regions.append({
                    'region': item.get('region'),
                    'impressions': int(item.get('impressions', 0)),
                    'clicks': int(item.get('clicks', 0)),
                    'spend': float(item.get('spend', 0))
                })
            
            return {
                'age_gender': age_gender,
                'countries': countries,
                'regions': regions
            }
            
        except Exception as e:
            logger.error(f"Error fetching campaign demographics: {e}")
            return {
                'age_gender': [],
                'countries': [],
                'regions': []
            }


    def get_campaign_placements(self, campaign_id: str, period: str = None, start_date: str = None, end_date: str = None) -> Dict:
        """Get placement/platform breakdowns for a campaign"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            # Platform breakdown (Facebook, Instagram, Audience Network, Messenger)
            platform_data = self._make_request(f"{campaign_id}/insights", {
                'time_range': f'{{"since":"{since}","until":"{until}"}}',
                'fields': 'impressions,clicks,spend,actions',
                'breakdowns': 'publisher_platform',
                'level': 'campaign',
                'action_attribution_windows': ['7d_click', '1d_view']
            })
            
            # Platform position breakdown (Feed, Stories, Reels, etc.)
            platform_position_data = self._make_request(f"{campaign_id}/insights", {
                'time_range': f'{{"since":"{since}","until":"{until}"}}',
                'fields': 'impressions,clicks,spend,actions',
                'breakdowns': 'platform_position',
                'level': 'campaign',
                'action_attribution_windows': ['7d_click', '1d_view']
            })
            
            # Device platform breakdown (mobile, desktop)
            device_data = self._make_request(f"{campaign_id}/insights", {
                'time_range': f'{{"since":"{since}","until":"{until}"}}',
                'fields': 'impressions,clicks,spend,actions',
                'breakdowns': 'device_platform',
                'level': 'campaign',
                'action_attribution_windows': ['7d_click', '1d_view']
            })
            
            # Process platform data
            platforms = []
            for item in platform_data.get('data', []):
                conversions = 0
                actions = item.get('actions', [])
                for action in actions:
                    if action.get('action_type') in ['purchase', 'lead', 'complete_registration', 'omni_purchase']:
                        conversions += int(action.get('value', 0))
                
                platforms.append({
                    'platform': item.get('publisher_platform'),
                    'impressions': int(item.get('impressions', 0)),
                    'clicks': int(item.get('clicks', 0)),
                    'spend': float(item.get('spend', 0)),
                    'conversions': conversions
                })
            
            # Process platform position data
            positions = []
            for item in platform_position_data.get('data', []):
                conversions = 0
                actions = item.get('actions', [])
                for action in actions:
                    if action.get('action_type') in ['purchase', 'lead', 'complete_registration', 'omni_purchase']:
                        conversions += int(action.get('value', 0))
                
                positions.append({
                    'position': item.get('platform_position'),
                    'impressions': int(item.get('impressions', 0)),
                    'clicks': int(item.get('clicks', 0)),
                    'spend': float(item.get('spend', 0)),
                    'conversions': conversions
                })
            
            # Process device data
            devices = []
            for item in device_data.get('data', []):
                conversions = 0
                actions = item.get('actions', [])
                for action in actions:
                    if action.get('action_type') in ['purchase', 'lead', 'complete_registration', 'omni_purchase']:
                        conversions += int(action.get('value', 0))
                
                devices.append({
                    'device': item.get('device_platform'),
                    'impressions': int(item.get('impressions', 0)),
                    'clicks': int(item.get('clicks', 0)),
                    'spend': float(item.get('spend', 0)),
                    'conversions': conversions
                })
            
            return {
                'platforms': platforms,
                'positions': positions,
                'devices': devices
            }
            
        except Exception as e:
            logger.error(f"Error fetching campaign placements: {e}")
            return {
                'platforms': [],
                'positions': [],
                'devices': []
            }


    # Similar functions for Ad Sets
    def get_adset_demographics(self, adset_id: str, period: str = None, start_date: str = None, end_date: str = None) -> Dict:
        """Get demographic breakdowns for an ad set"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            age_gender_data = self._make_request(f"{adset_id}/insights", {
                'time_range': f'{{"since":"{since}","until":"{until}"}}',
                'fields': 'impressions,clicks,spend,actions',
                'breakdowns': 'age,gender',
                'level': 'adset',
                'action_attribution_windows': ['7d_click', '1d_view']
            })
            
            country_data = self._make_request(f"{adset_id}/insights", {
                'time_range': f'{{"since":"{since}","until":"{until}"}}',
                'fields': 'impressions,clicks,spend',
                'breakdowns': 'country',
                'level': 'adset',
                'action_attribution_windows': ['7d_click', '1d_view']
            })
            
            age_gender = []
            for item in age_gender_data.get('data', []):
                conversions = 0
                actions = item.get('actions', [])
                for action in actions:
                    if action.get('action_type') in ['purchase', 'lead', 'complete_registration', 'omni_purchase']:
                        conversions += int(action.get('value', 0))
                
                age_gender.append({
                    'age': item.get('age'),
                    'gender': item.get('gender'),
                    'impressions': int(item.get('impressions', 0)),
                    'clicks': int(item.get('clicks', 0)),
                    'spend': float(item.get('spend', 0)),
                    'conversions': conversions
                })
            
            countries = []
            for item in country_data.get('data', []):
                countries.append({
                    'country': item.get('country'),
                    'impressions': int(item.get('impressions', 0)),
                    'clicks': int(item.get('clicks', 0)),
                    'spend': float(item.get('spend', 0))
                })
            
            return {
                'age_gender': age_gender,
                'countries': countries
            }
            
        except Exception as e:
            logger.error(f"Error fetching adset demographics: {e}")
            return {'age_gender': [], 'countries': []}


    def get_adset_placements(self, adset_id: str, period: str = None, start_date: str = None, end_date: str = None) -> Dict:
        """Get placement breakdowns for an ad set"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            platform_data = self._make_request(f"{adset_id}/insights", {
                'time_range': f'{{"since":"{since}","until":"{until}"}}',
                'fields': 'impressions,clicks,spend,actions',
                'breakdowns': 'publisher_platform',
                'level': 'adset',
                'action_attribution_windows': ['7d_click', '1d_view']
            })
            
            device_data = self._make_request(f"{adset_id}/insights", {
                'time_range': f'{{"since":"{since}","until":"{until}"}}',
                'fields': 'impressions,clicks,spend,actions',
                'breakdowns': 'device_platform',
                'level': 'adset',
                'action_attribution_windows': ['7d_click', '1d_view']
            })
            
            platforms = []
            for item in platform_data.get('data', []):
                conversions = 0
                actions = item.get('actions', [])
                for action in actions:
                    if action.get('action_type') in ['purchase', 'lead', 'complete_registration', 'omni_purchase']:
                        conversions += int(action.get('value', 0))
                
                platforms.append({
                    'platform': item.get('publisher_platform'),
                    'impressions': int(item.get('impressions', 0)),
                    'clicks': int(item.get('clicks', 0)),
                    'spend': float(item.get('spend', 0)),
                    'conversions': conversions
                })
            
            devices = []
            for item in device_data.get('data', []):
                conversions = 0
                actions = item.get('actions', [])
                for action in actions:
                    if action.get('action_type') in ['purchase', 'lead', 'complete_registration', 'omni_purchase']:
                        conversions += int(action.get('value', 0))
                
                devices.append({
                    'device': item.get('device_platform'),
                    'impressions': int(item.get('impressions', 0)),
                    'clicks': int(item.get('clicks', 0)),
                    'spend': float(item.get('spend', 0)),
                    'conversions': conversions
                })
            
            return {
                'platforms': platforms,
                'devices': devices
            }
            
        except Exception as e:
            logger.error(f"Error fetching adset placements: {e}")
            return {'platforms': [], 'devices': []}


    # Similar functions for Ads (individual ads)
    def get_ad_demographics(self, ad_id: str, period: str = None, start_date: str = None, end_date: str = None) -> Dict:
        """Get demographic breakdowns for an individual ad"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            age_gender_data = self._make_request(f"{ad_id}/insights", {
                'time_range': f'{{"since":"{since}","until":"{until}"}}',
                'fields': 'impressions,clicks,spend,actions',
                'breakdowns': 'age,gender',
                'level': 'ad',
                'action_attribution_windows': ['7d_click', '1d_view']
            })
            
            age_gender = []
            for item in age_gender_data.get('data', []):
                conversions = 0
                actions = item.get('actions', [])
                for action in actions:
                    if action.get('action_type') in ['purchase', 'lead', 'complete_registration', 'omni_purchase']:
                        conversions += int(action.get('value', 0))
                
                age_gender.append({
                    'age': item.get('age'),
                    'gender': item.get('gender'),
                    'impressions': int(item.get('impressions', 0)),
                    'clicks': int(item.get('clicks', 0)),
                    'spend': float(item.get('spend', 0)),
                    'conversions': conversions
                })
            
            return {'age_gender': age_gender}
            
        except Exception as e:
            logger.error(f"Error fetching ad demographics: {e}")
            return {'age_gender': []}


    def get_ad_placements(self, ad_id: str, period: str = None, start_date: str = None, end_date: str = None) -> Dict:
        """Get placement breakdowns for an individual ad"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            platform_data = self._make_request(f"{ad_id}/insights", {
                'time_range': f'{{"since":"{since}","until":"{until}"}}',
                'fields': 'impressions,clicks,spend,actions',
                'breakdowns': 'publisher_platform',
                'level': 'ad',
                'action_attribution_windows': ['7d_click', '1d_view']
            })
            
            platforms = []
            for item in platform_data.get('data', []):
                conversions = 0
                actions = item.get('actions', [])
                for action in actions:
                    if action.get('action_type') in ['purchase', 'lead', 'complete_registration', 'omni_purchase']:
                        conversions += int(action.get('value', 0))
                
                platforms.append({
                    'platform': item.get('publisher_platform'),
                    'impressions': int(item.get('impressions', 0)),
                    'clicks': int(item.get('clicks', 0)),
                    'spend': float(item.get('spend', 0)),
                    'conversions': conversions
                })
            
            return {'platforms': platforms}
            
        except Exception as e:
            logger.error(f"Error fetching ad placements: {e}")
            return {'platforms': []}

    def get_ad_sets(self, campaign_id: str, period: str = None, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Get ad sets for a campaign with insights"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            data = self._make_request(f"{campaign_id}/adsets", {
                'fields': 'id,name,status,targeting,optimization_goal,billing_event,budget_remaining,daily_budget,lifetime_budget,created_time,updated_time'
            })
            
            ad_sets = []
            for ad_set in data.get('data', []):
                # Get insights for each ad set
                try:
                    insights = self._make_request(f"{ad_set['id']}/insights", {
                        'time_range': f'{{"since":"{since}","until":"{until}"}}',
                        'fields': 'spend,impressions,clicks,cpc,cpm,ctr,reach,frequency,actions,conversions'
                    })
                    
                    insights_data = insights.get('data', [])
                    if insights_data:
                        ad_set_insights = insights_data[0]
                    else:
                        ad_set_insights = {}
                        logger.warning(f"No insights data for ad set {ad_set.get('id')}")
                    
                except Exception as insight_error:
                    logger.warning(f"Could not fetch insights for ad set {ad_set.get('id')}: {insight_error}")
                    ad_set_insights = {}
                
                # Extract conversions from actions
                conversions = 0
                actions = ad_set_insights.get('actions', [])
                for action in actions:
                    if action.get('action_type') in ['purchase', 'lead', 'complete_registration', 'offsite_conversion.fb_pixel_purchase']:
                        conversions += int(action.get('value', 0))
                
                # Parse targeting (simplified)
                targeting = ad_set.get('targeting', {})
                targeting_summary = {
                    'locations': targeting.get('geo_locations', {}),
                    'age_min': targeting.get('age_min'),
                    'age_max': targeting.get('age_max'),
                    'genders': targeting.get('genders', [])
                }
                
                ad_sets.append({
                    'id': ad_set.get('id'),
                    'name': ad_set.get('name'),
                    'campaign_id': campaign_id,
                    'status': ad_set.get('status'),
                    'optimization_goal': ad_set.get('optimization_goal'),
                    'billing_event': ad_set.get('billing_event'),
                    'daily_budget': float(ad_set.get('daily_budget', 0)) / 100 if ad_set.get('daily_budget') else None,
                    'lifetime_budget': float(ad_set.get('lifetime_budget', 0)) / 100 if ad_set.get('lifetime_budget') else None,
                    'budget_remaining': float(ad_set.get('budget_remaining', 0)) / 100 if ad_set.get('budget_remaining') else None,
                    'targeting_summary': targeting_summary,
                    'spend': float(ad_set_insights.get('spend', 0)),
                    'impressions': int(ad_set_insights.get('impressions', 0)),
                    'clicks': int(ad_set_insights.get('clicks', 0)),
                    'conversions': conversions,
                    'cpc': float(ad_set_insights.get('cpc', 0)),
                    'cpm': float(ad_set_insights.get('cpm', 0)),
                    'ctr': float(ad_set_insights.get('ctr', 0)),
                    'reach': int(ad_set_insights.get('reach', 0)),
                    'frequency': float(ad_set_insights.get('frequency', 0)),
                    'created_time': ad_set.get('created_time'),
                    'updated_time': ad_set.get('updated_time')
                })
            
            logger.info(f"Retrieved {len(ad_sets)} ad sets for campaign {campaign_id}")
            return ad_sets
            
        except Exception as e:
            logger.error(f"Error fetching ad sets: {e}")
            return []

    def get_ad_sets_timeseries(self, campaign_id: str, period: str = None, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Get ad sets for a campaign with time-series insights"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            data = self._make_request(f"{campaign_id}/adsets", {
                'fields': 'id,name,status,targeting,optimization_goal,billing_event,budget_remaining,daily_budget,lifetime_budget,created_time,updated_time'
            })
            
            ad_sets = []
            for ad_set in data.get('data', []):
                # Get time-series insights for each ad set
                try:
                    insights = self._make_request(f"{ad_set['id']}/insights", {
                        'time_range': f'{{"since":"{since}","until":"{until}"}}',
                        'fields': 'spend,impressions,clicks,cpc,cpm,ctr,reach,frequency,actions,conversions',
                        'time_increment': '1',  # Break down by day
                        'level': 'adset'
                    })
                    
                    insights_data = insights.get('data', [])
                    
                    if not insights_data:
                        logger.warning(f"No insights data for ad set {ad_set.get('id')}")
                        timeseries = []
                        summary = self._get_empty_insights()
                    else:
                        # Process daily data
                        timeseries = []
                        total_spend = 0
                        total_impressions = 0
                        total_clicks = 0
                        total_conversions = 0
                        total_reach = 0
                        
                        for day_data in insights_data:
                            # Extract conversions from actions for this day
                            conversions = 0
                            actions = day_data.get('actions', [])
                            for action in actions:
                                if action.get('action_type') in ['purchase', 'lead', 'complete_registration', 'offsite_conversion.fb_pixel_purchase']:
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
                            
                            # Accumulate totals
                            total_spend += day_insights['spend']
                            total_impressions += day_insights['impressions']
                            total_clicks += day_insights['clicks']
                            total_conversions += conversions
                            total_reach += day_insights['reach']
                        
                        # Calculate summary metrics
                        avg_cpc = total_spend / total_clicks if total_clicks > 0 else 0
                        avg_cpm = (total_spend / total_impressions * 1000) if total_impressions > 0 else 0
                        avg_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
                        avg_frequency = total_impressions / total_reach if total_reach > 0 else 0
                        
                        summary = {
                            'spend': total_spend,
                            'impressions': total_impressions,
                            'clicks': total_clicks,
                            'conversions': total_conversions,
                            'cpc': avg_cpc,
                            'cpm': avg_cpm,
                            'ctr': avg_ctr,
                            'reach': total_reach,
                            'frequency': avg_frequency
                        }
                    
                except Exception as insight_error:
                    logger.warning(f"Could not fetch insights for ad set {ad_set.get('id')}: {insight_error}")
                    timeseries = []
                    summary = self._get_empty_insights()
                
                # Parse targeting (simplified)
                targeting = ad_set.get('targeting', {})
                targeting_summary = {
                    'locations': targeting.get('geo_locations', {}),
                    'age_min': targeting.get('age_min'),
                    'age_max': targeting.get('age_max'),
                    'genders': targeting.get('genders', [])
                }
                
                ad_sets.append({
                    'id': ad_set.get('id'),
                    'name': ad_set.get('name'),
                    'campaign_id': campaign_id,
                    'status': ad_set.get('status'),
                    'optimization_goal': ad_set.get('optimization_goal'),
                    'billing_event': ad_set.get('billing_event'),
                    'daily_budget': float(ad_set.get('daily_budget', 0)) / 100 if ad_set.get('daily_budget') else None,
                    'lifetime_budget': float(ad_set.get('lifetime_budget', 0)) / 100 if ad_set.get('lifetime_budget') else None,
                    'budget_remaining': float(ad_set.get('budget_remaining', 0)) / 100 if ad_set.get('budget_remaining') else None,
                    'targeting_summary': targeting_summary,
                    'created_time': ad_set.get('created_time'),
                    'updated_time': ad_set.get('updated_time'),
                    'timeseries': timeseries,
                    'summary': summary
                })
            
            logger.info(f"Retrieved {len(ad_sets)} ad sets with timeseries for campaign {campaign_id}")
            return ad_sets
            
        except Exception as e:
            logger.error(f"Error fetching ad sets timeseries: {e}")
            return []

    def _get_empty_insights(self) -> Dict:
        """Helper method to return empty insights structure"""
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

    def get_ads(self, ad_set_id: str, period: str = None, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Get ads for an ad set with insights"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            data = self._make_request(f"{ad_set_id}/ads", {
                'fields': 'id,name,status,creative{title,body,image_url,video_id,thumbnail_url,object_story_spec},created_time,updated_time'
            })
            
            ads = []
            for ad in data.get('data', []):
                # Get insights for each ad
                try:
                    insights = self._make_request(f"{ad['id']}/insights", {
                        'time_range': f'{{"since":"{since}","until":"{until}"}}',
                        'fields': 'spend,impressions,clicks,cpc,cpm,ctr,reach,frequency,actions,inline_link_clicks,cost_per_inline_link_click'
                    })
                    
                    insights_data = insights.get('data', [])
                    if insights_data:
                        ad_insights = insights_data[0]
                    else:
                        ad_insights = {}
                        logger.warning(f"No insights data for ad {ad.get('id')}")
                    
                except Exception as insight_error:
                    logger.warning(f"Could not fetch insights for ad {ad.get('id')}: {insight_error}")
                    ad_insights = {}
                
                # Extract conversions from actions
                conversions = 0
                link_clicks = 0
                actions = ad_insights.get('actions', [])
                for action in actions:
                    action_type = action.get('action_type')
                    value = int(action.get('value', 0))
                    
                    if action_type in ['purchase', 'lead', 'complete_registration', 'offsite_conversion.fb_pixel_purchase']:
                        conversions += value
                    elif action_type == 'link_click':
                        link_clicks += value
                
                # Extract creative details
                creative = ad.get('creative', {})
                creative_details = {
                    'title': creative.get('title'),
                    'body': creative.get('body'),
                    'image_url': creative.get('image_url'),
                    'video_id': creative.get('video_id'),
                    'thumbnail_url': creative.get('thumbnail_url')
                }
                
                ads.append({
                    'id': ad.get('id'),
                    'name': ad.get('name'),
                    'ad_set_id': ad_set_id,
                    'status': ad.get('status'),
                    'creative': creative_details,
                    'spend': float(ad_insights.get('spend', 0)),
                    'impressions': int(ad_insights.get('impressions', 0)),
                    'clicks': int(ad_insights.get('clicks', 0)),
                    'link_clicks': link_clicks,
                    'conversions': conversions,
                    'cpc': float(ad_insights.get('cpc', 0)),
                    'cpm': float(ad_insights.get('cpm', 0)),
                    'ctr': float(ad_insights.get('ctr', 0)),
                    'reach': int(ad_insights.get('reach', 0)),
                    'frequency': float(ad_insights.get('frequency', 0)),
                    'cost_per_link_click': float(ad_insights.get('cost_per_inline_link_click', 0)),
                    'created_time': ad.get('created_time'),
                    'updated_time': ad.get('updated_time')
                })
            
            logger.info(f"Retrieved {len(ads)} ads for ad set {ad_set_id}")
            return ads
            
        except Exception as e:
            logger.error(f"Error fetching ads: {e}")
            return []

    def get_ads_timeseries(self, ad_set_id: str, period: str = None, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Get ads for an ad set with time-series insights"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            data = self._make_request(f"{ad_set_id}/ads", {
                'fields': 'id,name,status,creative{title,body,image_url,video_id,thumbnail_url,object_story_spec},created_time,updated_time'
            })
            
            ads = []
            for ad in data.get('data', []):
                # Get time-series insights for each ad
                try:
                    insights = self._make_request(f"{ad['id']}/insights", {
                        'time_range': f'{{"since":"{since}","until":"{until}"}}',
                        'fields': 'spend,impressions,clicks,cpc,cpm,ctr,reach,frequency,actions,inline_link_clicks,cost_per_inline_link_click',
                        'time_increment': '1',  # Break down by day
                        'level': 'ad'
                    })
                    
                    insights_data = insights.get('data', [])
                    
                    if not insights_data:
                        logger.warning(f"No insights data for ad {ad.get('id')}")
                        timeseries = []
                        summary = self._get_empty_ad_insights()
                    else:
                        # Process daily data
                        timeseries = []
                        total_spend = 0
                        total_impressions = 0
                        total_clicks = 0
                        total_conversions = 0
                        total_link_clicks = 0
                        total_reach = 0
                        total_cost_per_link_click = 0
                        days_with_link_clicks = 0
                        
                        for day_data in insights_data:
                            # Extract conversions and link clicks from actions for this day
                            conversions = 0
                            link_clicks = 0
                            actions = day_data.get('actions', [])
                            for action in actions:
                                action_type = action.get('action_type')
                                value = int(action.get('value', 0))
                                
                                if action_type in ['purchase', 'lead', 'complete_registration', 'offsite_conversion.fb_pixel_purchase']:
                                    conversions += value
                                elif action_type == 'link_click':
                                    link_clicks += value
                            
                            cost_per_link_click = float(day_data.get('cost_per_inline_link_click', 0))
                            
                            day_insights = {
                                'date': day_data.get('date_start'),
                                'spend': float(day_data.get('spend', 0)),
                                'impressions': int(day_data.get('impressions', 0)),
                                'clicks': int(day_data.get('clicks', 0)),
                                'link_clicks': link_clicks,
                                'conversions': conversions,
                                'cpc': float(day_data.get('cpc', 0)),
                                'cpm': float(day_data.get('cpm', 0)),
                                'ctr': float(day_data.get('ctr', 0)),
                                'reach': int(day_data.get('reach', 0)),
                                'frequency': float(day_data.get('frequency', 0)),
                                'cost_per_link_click': cost_per_link_click
                            }
                            
                            timeseries.append(day_insights)
                            
                            # Accumulate totals
                            total_spend += day_insights['spend']
                            total_impressions += day_insights['impressions']
                            total_clicks += day_insights['clicks']
                            total_conversions += conversions
                            total_link_clicks += link_clicks
                            total_reach += day_insights['reach']
                            
                            if cost_per_link_click > 0:
                                total_cost_per_link_click += cost_per_link_click
                                days_with_link_clicks += 1
                        
                        # Calculate summary metrics
                        avg_cpc = total_spend / total_clicks if total_clicks > 0 else 0
                        avg_cpm = (total_spend / total_impressions * 1000) if total_impressions > 0 else 0
                        avg_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
                        avg_frequency = total_impressions / total_reach if total_reach > 0 else 0
                        avg_cost_per_link_click = total_cost_per_link_click / days_with_link_clicks if days_with_link_clicks > 0 else 0
                        
                        summary = {
                            'spend': total_spend,
                            'impressions': total_impressions,
                            'clicks': total_clicks,
                            'link_clicks': total_link_clicks,
                            'conversions': total_conversions,
                            'cpc': avg_cpc,
                            'cpm': avg_cpm,
                            'ctr': avg_ctr,
                            'reach': total_reach,
                            'frequency': avg_frequency,
                            'cost_per_link_click': avg_cost_per_link_click
                        }
                    
                except Exception as insight_error:
                    logger.warning(f"Could not fetch insights for ad {ad.get('id')}: {insight_error}")
                    timeseries = []
                    summary = self._get_empty_ad_insights()
                
                # Extract creative details
                creative = ad.get('creative', {})
                creative_details = {
                    'title': creative.get('title'),
                    'body': creative.get('body'),
                    'image_url': creative.get('image_url'),
                    'video_id': creative.get('video_id'),
                    'thumbnail_url': creative.get('thumbnail_url')
                }
                
                ads.append({
                    'id': ad.get('id'),
                    'name': ad.get('name'),
                    'ad_set_id': ad_set_id,
                    'status': ad.get('status'),
                    'creative': creative_details,
                    'created_time': ad.get('created_time'),
                    'updated_time': ad.get('updated_time'),
                    'timeseries': timeseries,
                    'summary': summary
                })
            
            logger.info(f"Retrieved {len(ads)} ads with timeseries for ad set {ad_set_id}")
            return ads
            
        except Exception as e:
            logger.error(f"Error fetching ads timeseries: {e}")
            return []

    def _get_empty_ad_insights(self) -> Dict:
        """Helper method to return empty ad insights structure"""
        return {
            'spend': 0,
            'impressions': 0,
            'clicks': 0,
            'link_clicks': 0,
            'conversions': 0,
            'cpc': 0,
            'cpm': 0,
            'ctr': 0,
            'reach': 0,
            'frequency': 0,
            'cost_per_link_click': 0
        }

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
    
    def get_page_insights(self, page_id: str, period: str = None, start_date: str = None, end_date: str = None) -> Dict:
        """Get insights for specific Facebook page using Page Access Token"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            # Get page access token
            page_access_token = self._get_page_access_token(page_id)
            
            # Get basic page info with detailed fields
            page_info = self._make_request(page_id, {
                'access_token': page_access_token,
                'fields': 'followers_count,fan_count,new_like_count,talking_about_count,were_here_count,checkins'
            })
            
            # Try to get insights using page token
            insights_data = {}
            
            metrics_to_try = [
                'page_impressions',
                'page_impressions_unique', 
                'page_post_engagements',
                'page_total_actions',
                'page_views_total'
            ]
            
            for metric in metrics_to_try:
                try:
                    data = self._make_request(f"{page_id}/insights/{metric}", {
                        'access_token': page_access_token,
                        'since': since,
                        'until': until,
                        'period': 'day'
                    })
                    
                    if data.get('data'):
                        values = data['data'][0].get('values', [])
                        total = sum(v.get('value', 0) for v in values if v.get('value') is not None)
                        insights_data[metric] = total
                except Exception as metric_error:
                    logger.debug(f"Metric {metric} not available: {metric_error}")
                    insights_data[metric] = 0
            
            return {
                'impressions': insights_data.get('page_impressions', 0),
                'unique_impressions': insights_data.get('page_impressions_unique', 0),
                'engaged_users': insights_data.get('page_total_actions', 0),
                'post_engagements': insights_data.get('page_post_engagements', 0),
                'fans': page_info.get('fan_count', 0),
                'followers': page_info.get('followers_count', 0),
                'page_views': insights_data.get('page_views_total', 0),
                'new_likes': page_info.get('new_like_count', 0),
                'talking_about_count': page_info.get('talking_about_count', 0),
                'checkins': page_info.get('checkins', 0)
            }
            
        except Exception as e:
            logger.error(f"Error fetching page insights: {e}")
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
        
    def get_page_insights_timeseries(self, page_id: str, period: str = None, start_date: str = None, end_date: str = None) -> Dict:
        """Get time-series insights for specific Facebook page"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            # Get page access token
            page_access_token = self._get_page_access_token(page_id)
            
            # Get basic page info (these are current values, not time-series)
            page_info = self._make_request(page_id, {
                'access_token': page_access_token,
                'fields': 'followers_count,fan_count,talking_about_count,checkins'
            })
            
            # Define metrics to fetch with their time-series data
            metrics_config = {
                'page_impressions': 'impressions',
                'page_impressions_unique': 'unique_impressions',
                'page_post_engagements': 'post_engagements',
                'page_total_actions': 'engaged_users',
                'page_views_total': 'page_views',
                'page_fan_adds': 'new_likes',
                'page_fans': 'fans'  # Total fans over time
            }
            
            # Collect all daily data
            daily_data = {}
            
            for metric_key, metric_name in metrics_config.items():
                try:
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
                            value = value_entry.get('value', 0)
                            
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
                                
                                daily_data[date][metric_name] = value if value is not None else 0
                                
                except Exception as metric_error:
                    logger.debug(f"Metric {metric_key} not available: {metric_error}")
            
            # Convert to sorted list
            timeseries = sorted(daily_data.values(), key=lambda x: x['date'])
            
            # Calculate summary totals
            total_impressions = sum(day['impressions'] for day in timeseries)
            total_unique_impressions = sum(day['unique_impressions'] for day in timeseries)
            total_post_engagements = sum(day['post_engagements'] for day in timeseries)
            total_engaged_users = sum(day['engaged_users'] for day in timeseries)
            total_page_views = sum(day['page_views'] for day in timeseries)
            total_new_likes = sum(day['new_likes'] for day in timeseries)
            
            # Get the latest fan count (or use current from page_info)
            latest_fans = timeseries[-1]['fans'] if timeseries and timeseries[-1]['fans'] > 0 else page_info.get('fan_count', 0)
            
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
            logger.error(f"Error fetching page insights timeseries: {e}")
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

    def _get_page_access_token(self, page_id: str) -> str:
        """Get page access token for a specific page"""
        try:
            # Get page access token using the user's access token
            data = self._make_request(f"{page_id}", {
                'fields': 'access_token'
            })
            
            page_access_token = data.get('access_token')
            if not page_access_token:
                logger.warning(f"No page access token available for page {page_id}")
                return self.access_token  # Fallback to user token
            
            return page_access_token
        except Exception as e:
            logger.warning(f"Could not get page access token: {e}")
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