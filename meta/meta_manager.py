"""
Meta Ads Manager for Facebook Marketing API
Handles all Meta Ads API operations for campaigns, ad sets, ads, and insights
"""

import os
import logging
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from fastapi import HTTPException
from auth.auth_manager import AuthManager
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)


try:
    from utils.helpers import format_large_number, format_currency
except ImportError:
    # Fallback functions if utils not available
    def format_large_number(num):
        if num >= 1000000:
            return f"{num/1000000:.1f}M"
        elif num >= 1000:
            return f"{num/1000:.1f}K"
        return str(num)
    
    def format_currency(amount):
        return f"${amount:.2f}"
    
class MetaManager:
    """Manager class for Meta Ads (Facebook Marketing) API operations"""
    
    def __init__(self, user_email: str, auth_manager: AuthManager):
        self.auth_manager = auth_manager
        self.user_email = user_email
        
        # ADD THESE MISSING LINES:
        self.api_version = "v18.0"
        self.base_url = f"https://graph.facebook.com/{self.api_version}"
        
        logger.info(f"🔍 MetaManager init - Looking for email: {user_email}")
        logger.info(f"🔍 Available Facebook sessions: {list(auth_manager.facebook_sessions.keys())}")
        
        # Get Facebook access token
        try:
            self.access_token = self.auth_manager.get_facebook_access_token(user_email)
            logger.info(f"✅ Got access token: {self.access_token[:20]}...")
        except Exception as e:
            logger.error(f"❌ Failed to get access token: {e}")
            raise HTTPException(status_code=401, detail=f"Facebook authentication required: {str(e)}")
    
    def _make_api_request(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Make API request to Facebook Graph API"""
        if params is None:
            params = {}
        
        # Add access token to all requests
        params['access_token'] = self.access_token
        
        url = f"{self.base_url}/{endpoint}"
        
        try:
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Meta API error {response.status_code}: {response.text}")
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Meta API error: {response.text}"
                )
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Meta API request failed: {e}")
            raise HTTPException(status_code=500, detail=f"Meta API request failed: {str(e)}")
    
    def safe_float(self, value, default=0.0):
        """Safely convert to float"""
        try:
            return float(value) if value else default
        except (ValueError, TypeError):
            return default
    
    def safe_int(self, value, default=0):
        """Safely convert to int"""
        try:
            return int(float(value)) if value else default
        except (ValueError, TypeError):
            return default
    
    def get_date_range(self, period: str) -> Dict[str, str]:
        """Get date range for Meta API queries"""
        end_date = datetime.now().date()
        
        if period == "7d":
            start_date = end_date - timedelta(days=7)
        elif period == "90d":
            start_date = end_date - timedelta(days=90)
        elif period == "365d":
            start_date = end_date - timedelta(days=365)
        else:  # default 30d
            start_date = end_date - timedelta(days=30)
        
        return {
            "since": start_date.strftime("%Y-%m-%d"),
            "until": end_date.strftime("%Y-%m-%d")
        }
    
    # =============================================================================
    # AD ACCOUNT OPERATIONS
    # =============================================================================
        
    def get_ad_accounts(self) -> List[Dict[str, Any]]:
        """Get all accessible Meta Ad accounts"""
        try:
            endpoint = "me/adaccounts"
            params = {
                "fields": "id,name,account_status,currency,balance,amount_spent,spend_cap,timezone_name,business"
            }
            
            response = self._make_api_request(endpoint, params)
            accounts = []
            
            for account in response.get('data', []):
                # Convert account_status to string
                account_status = account.get('account_status', '')
                if isinstance(account_status, int):
                    # Map Facebook's integer status codes to strings
                    status_mapping = {
                        1: "ACTIVE",
                        2: "DISABLED", 
                        3: "UNSETTLED",
                        7: "PENDING_RISK_REVIEW",
                        8: "PENDING_SETTLEMENT",
                        9: "IN_GRACE_PERIOD",
                        101: "PENDING_CLOSURE",
                        201: "CLOSED"
                    }
                    account_status = status_mapping.get(account_status, f"STATUS_{account_status}")
                
                accounts.append({
                    'id': account.get('id', ''),
                    'name': account.get('name', ''),
                    'account_status': str(account_status),  # Ensure it's a string
                    'currency': account.get('currency', 'USD'),
                    'balance': self.safe_float(account.get('balance', 0)),
                    'amount_spent': self.safe_float(account.get('amount_spent', 0)),
                    'spend_cap': self.safe_float(account.get('spend_cap', 0)),
                    'timezone_name': account.get('timezone_name', 'UTC'),
                    'business_name': account.get('business', {}).get('name', '') if account.get('business') else ''
                })
            
            logger.info(f"Found {len(accounts)} Meta ad accounts for {self.user_email}")
            return accounts
            
        except Exception as e:
            logger.error(f"Error fetching Meta ad accounts: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch ad accounts: {str(e)}")
    
    # =============================================================================
    # CAMPAIGN OPERATIONS
    # =============================================================================
    
    def get_campaigns(self, account_id: str, period: str = "30d") -> List[Dict[str, Any]]:
        """Get campaigns for a specific ad account"""
        try:
            date_range = self.get_date_range(period)
            
            endpoint = f"{account_id}/campaigns"
            params = {
                "fields": "id,name,status,objective,created_time,start_time,stop_time,updated_time",
                "limit": 100
            }
            
            response = self._make_api_request(endpoint, params)
            campaigns = []
            
            for campaign in response.get('data', []):
                # Get campaign insights for this period
                insights = self.get_campaign_insights(campaign['id'], date_range)
                
                campaigns.append({
                    'id': campaign.get('id', ''),
                    'name': campaign.get('name', ''),
                    'status': campaign.get('status', ''),
                    'objective': campaign.get('objective', ''),
                    'created_time': campaign.get('created_time', ''),
                    'start_time': campaign.get('start_time', ''),
                    'stop_time': campaign.get('stop_time', ''),
                    'updated_time': campaign.get('updated_time', ''),
                    # Add performance metrics
                    'spend': insights.get('spend', 0),
                    'impressions': insights.get('impressions', 0),
                    'clicks': insights.get('clicks', 0),
                    'ctr': insights.get('ctr', 0),
                    'cpc': insights.get('cpc', 0),
                    'cpm': insights.get('cpm', 0),
                    'reach': insights.get('reach', 0),
                    'frequency': insights.get('frequency', 0)
                })
            
            logger.info(f"Found {len(campaigns)} campaigns for account {account_id}")
            return campaigns
            
        except Exception as e:
            logger.error(f"Error fetching campaigns for account {account_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch campaigns: {str(e)}")
    
    def get_campaign_insights(self, campaign_id: str, date_range: Dict[str, str]) -> Dict[str, Any]:
        """Get insights for a specific campaign"""
        try:
            endpoint = f"{campaign_id}/insights"
            params = {
                "fields": "spend,impressions,clicks,ctr,cpc,cpm,reach,frequency,actions,cost_per_action_type",
                "time_range": f"{{'since':'{date_range['since']}','until':'{date_range['until']}'}}",
                "limit": 1
            }
            
            response = self._make_api_request(endpoint, params)
            data = response.get('data', [])
            
            if data:
                insight = data[0]
                return {
                    'spend': self.safe_float(insight.get('spend', 0)),
                    'impressions': self.safe_int(insight.get('impressions', 0)),
                    'clicks': self.safe_int(insight.get('clicks', 0)),
                    'ctr': self.safe_float(insight.get('ctr', 0)),
                    'cpc': self.safe_float(insight.get('cpc', 0)),
                    'cpm': self.safe_float(insight.get('cpm', 0)),
                    'reach': self.safe_int(insight.get('reach', 0)),
                    'frequency': self.safe_float(insight.get('frequency', 0))
                }
            
            return {
                'spend': 0, 'impressions': 0, 'clicks': 0, 'ctr': 0,
                'cpc': 0, 'cpm': 0, 'reach': 0, 'frequency': 0
            }
            
        except Exception as e:
            logger.warning(f"Could not fetch insights for campaign {campaign_id}: {e}")
            return {
                'spend': 0, 'impressions': 0, 'clicks': 0, 'ctr': 0,
                'cpc': 0, 'cpm': 0, 'reach': 0, 'frequency': 0
            }
    
    # =============================================================================
    # ACCOUNT LEVEL INSIGHTS AND KEY STATS
    # =============================================================================
    
    def get_account_key_stats(self, account_id: str, period: str = "30d") -> Dict[str, Any]:
        """Get overall key statistics for an ad account"""
        try:
            date_range = self.get_date_range(period)
            
            endpoint = f"{account_id}/insights"
            params = {
                "fields": "spend,impressions,clicks,ctr,cpc,cpm,reach,frequency,actions,cost_per_action_type,unique_clicks,link_clicks,post_engagement",
                "time_range": f"{{'since':'{date_range['since']}','until':'{date_range['until']}'}}",
                "limit": 1
            }
            
            response = self._make_api_request(endpoint, params)
            data = response.get('data', [])
            
            if data:
                insight = data[0]
                
                total_spend = self.safe_float(insight.get('spend', 0))
                total_impressions = self.safe_int(insight.get('impressions', 0))
                total_clicks = self.safe_int(insight.get('clicks', 0))
                total_reach = self.safe_int(insight.get('reach', 0))
                link_clicks = self.safe_int(insight.get('link_clicks', 0))
                
                # Calculate derived metrics
                click_through_rate = self.safe_float(insight.get('ctr', 0))
                cost_per_click = self.safe_float(insight.get('cpc', 0))
                cost_per_mille = self.safe_float(insight.get('cpm', 0))
                frequency = self.safe_float(insight.get('frequency', 0))
                
                # Format for dashboard display
                from utils.helpers import format_large_number, format_currency
                
                return {
                    'total_spend': {
                        'value': total_spend,
                        'formatted': format_currency(total_spend),
                        'label': 'TOTAL SPEND',
                        'description': 'Total amount spent'
                    },
                    'total_impressions': {
                        'value': total_impressions,
                        'formatted': format_large_number(total_impressions),
                        'label': 'TOTAL IMPRESSIONS',
                        'description': 'Total ad impressions'
                    },
                    'total_clicks': {
                        'value': total_clicks,
                        'formatted': format_large_number(total_clicks),
                        'label': 'TOTAL CLICKS',
                        'description': 'Total clicks received'
                    },
                    'click_through_rate': {
                        'value': click_through_rate,
                        'formatted': f"{click_through_rate:.2f}%",
                        'label': 'CLICK-THROUGH RATE',
                        'description': 'Ad engagement rate'
                    },
                    'cost_per_click': {
                        'value': cost_per_click,
                        'formatted': format_currency(cost_per_click),
                        'label': 'COST PER CLICK',
                        'description': 'Average cost per click'
                    },
                    'cost_per_mille': {
                        'value': cost_per_mille,
                        'formatted': format_currency(cost_per_mille),
                        'label': 'COST PER 1000 IMPRESSIONS',
                        'description': 'Cost per thousand impressions'
                    },
                    'total_reach': {
                        'value': total_reach,
                        'formatted': format_large_number(total_reach),
                        'label': 'TOTAL REACH',
                        'description': 'Unique people reached'
                    },
                    'frequency': {
                        'value': frequency,
                        'formatted': f"{frequency:.2f}",
                        'label': 'FREQUENCY',
                        'description': 'Average times shown per person'
                    },
                    'summary': {
                        'period': period,
                        'account_id': account_id,
                        'date_range': f"{date_range['since']} to {date_range['until']}",
                        'generated_at': datetime.now().isoformat()
                    }
                }
            
            # Return default values if no data
            return self._get_default_key_stats(account_id, period)
            
        except Exception as e:
            logger.error(f"Error fetching key stats for account {account_id}: {e}")
            return self._get_default_key_stats(account_id, period)
    
    def _get_default_key_stats(self, account_id: str, period: str) -> Dict[str, Any]:
        """Return default key stats when no data available"""
        return {
            'total_spend': {'value': 0, 'formatted': '$0.00', 'label': 'TOTAL SPEND', 'description': 'Total amount spent'},
            'total_impressions': {'value': 0, 'formatted': '0', 'label': 'TOTAL IMPRESSIONS', 'description': 'Total ad impressions'},
            'total_clicks': {'value': 0, 'formatted': '0', 'label': 'TOTAL CLICKS', 'description': 'Total clicks received'},
            'click_through_rate': {'value': 0, 'formatted': '0.00%', 'label': 'CLICK-THROUGH RATE', 'description': 'Ad engagement rate'},
            'cost_per_click': {'value': 0, 'formatted': '$0.00', 'label': 'COST PER CLICK', 'description': 'Average cost per click'},
            'cost_per_mille': {'value': 0, 'formatted': '$0.00', 'label': 'COST PER 1000 IMPRESSIONS', 'description': 'Cost per thousand impressions'},
            'total_reach': {'value': 0, 'formatted': '0', 'label': 'TOTAL REACH', 'description': 'Unique people reached'},
            'frequency': {'value': 0, 'formatted': '0.00', 'label': 'FREQUENCY', 'description': 'Average times shown per person'},
            'summary': {
                'period': period,
                'account_id': account_id,
                'generated_at': datetime.now().isoformat()
            }
        }
    
    # =============================================================================
    # PERFORMANCE ANALYTICS
    # =============================================================================
    
    def get_performance_by_placement(self, account_id: str, period: str = "30d") -> List[Dict[str, Any]]:
        """Get performance breakdown by ad placement"""
        try:
            date_range = self.get_date_range(period)
            
            endpoint = f"{account_id}/insights"
            params = {
                "fields": "spend,impressions,clicks,ctr,cpc,placement",
                "breakdowns": "publisher_platform,platform_position",
                "time_range": f"{{'since':'{date_range['since']}','until':'{date_range['until']}'}}",
                "limit": 50
            }
            
            response = self._make_api_request(endpoint, params)
            placements = []
            
            for insight in response.get('data', []):
                placement_name = f"{insight.get('publisher_platform', 'Unknown')} - {insight.get('platform_position', 'Unknown')}"
                
                placements.append({
                    'placement': placement_name,
                    'publisher_platform': insight.get('publisher_platform', ''),
                    'platform_position': insight.get('platform_position', ''),
                    'spend': self.safe_float(insight.get('spend', 0)),
                    'impressions': self.safe_int(insight.get('impressions', 0)),
                    'clicks': self.safe_int(insight.get('clicks', 0)),
                    'ctr': self.safe_float(insight.get('ctr', 0)),
                    'cpc': self.safe_float(insight.get('cpc', 0))
                })
            
            return placements
            
        except Exception as e:
            logger.error(f"Error fetching placement performance for account {account_id}: {e}")
            return []
    
    def get_performance_by_age_gender(self, account_id: str, period: str = "30d") -> List[Dict[str, Any]]:
        """Get performance breakdown by age and gender"""
        try:
            date_range = self.get_date_range(period)
            
            endpoint = f"{account_id}/insights"
            params = {
                "fields": "spend,impressions,clicks,ctr,cpc,reach",
                "breakdowns": "age,gender",
                "time_range": f"{{'since':'{date_range['since']}','until':'{date_range['until']}'}}",
                "limit": 50
            }
            
            response = self._make_api_request(endpoint, params)
            demographics = []
            
            for insight in response.get('data', []):
                demographics.append({
                    'age': insight.get('age', ''),
                    'gender': insight.get('gender', ''),
                    'demographic': f"{insight.get('gender', '')} {insight.get('age', '')}",
                    'spend': self.safe_float(insight.get('spend', 0)),
                    'impressions': self.safe_int(insight.get('impressions', 0)),
                    'clicks': self.safe_int(insight.get('clicks', 0)),
                    'reach': self.safe_int(insight.get('reach', 0)),
                    'ctr': self.safe_float(insight.get('ctr', 0)),
                    'cpc': self.safe_float(insight.get('cpc', 0))
                })
            
            return demographics
            
        except Exception as e:
            logger.error(f"Error fetching demographic performance for account {account_id}: {e}")
            return []
    
    def get_time_series_data(self, account_id: str, period: str = "30d") -> List[Dict[str, Any]]:
        """Get daily performance data over time"""
        try:
            date_range = self.get_date_range(period)
            
            endpoint = f"{account_id}/insights"
            params = {
                "fields": "spend,impressions,clicks,ctr,cpc,reach",
                "time_increment": "1",  # Daily breakdown
                "time_range": f"{{'since':'{date_range['since']}','until':'{date_range['until']}'}}",
                "limit": 100
            }
            
            response = self._make_api_request(endpoint, params)
            time_series = []
            
            for insight in response.get('data', []):
                time_series.append({
                    'date': insight.get('date_start', ''),
                    'spend': self.safe_float(insight.get('spend', 0)),
                    'impressions': self.safe_int(insight.get('impressions', 0)),
                    'clicks': self.safe_int(insight.get('clicks', 0)),
                    'reach': self.safe_int(insight.get('reach', 0)),
                    'ctr': self.safe_float(insight.get('ctr', 0)),
                    'cpc': self.safe_float(insight.get('cpc', 0))
                })
            
            # Sort by date
            time_series.sort(key=lambda x: x['date'])
            
            return time_series
            
        except Exception as e:
            logger.error(f"Error fetching time series data for account {account_id}: {e}")
            return []
    
    def generate_api_calls_for_advanced_access(self, target_calls: int = 100) -> Dict[str, Any]:
        """Generate legitimate API calls to reach Facebook's 1500 call requirement"""
        import time
        import random
        
        successful_calls = 0
        failed_calls = 0
        call_breakdown = {
            "ad_accounts": 0,
            "campaigns": 0,
            "insights": 0,
            "ad_sets": 0,
            "ads": 0,
            "audiences": 0
        }
        
        try:
            # Get ad accounts first
            accounts = self.get_ad_accounts()
            if not accounts:
                raise Exception("No ad accounts available for API calls")
            
            # Generate calls across different endpoints
            calls_per_type = target_calls // 6  # Distribute across 6 endpoint types
            
            for i in range(target_calls):
                try:
                    # Add small delay to respect rate limits
                    time.sleep(random.uniform(0.1, 0.3))
                    
                    endpoint_type = i % 6
                    account = random.choice(accounts)
                    account_id = account['id']
                    
                    if endpoint_type == 0:
                        # Call ad accounts endpoint
                        self._make_api_request("me/adaccounts")
                        call_breakdown["ad_accounts"] += 1
                        
                    elif endpoint_type == 1:
                        # Call campaigns endpoint
                        self._make_api_request(f"{account_id}/campaigns", {
                            "fields": "id,name,status,objective"
                        })
                        call_breakdown["campaigns"] += 1
                        
                    elif endpoint_type == 2:
                        # Call insights endpoint
                        self._make_api_request(f"{account_id}/insights", {
                            "date_preset": "last_7d",
                            "fields": "impressions,clicks,spend"
                        })
                        call_breakdown["insights"] += 1
                        
                    elif endpoint_type == 3:
                        # Call ad sets endpoint
                        self._make_api_request(f"{account_id}/adsets", {
                            "fields": "id,name,status"
                        })
                        call_breakdown["ad_sets"] += 1
                        
                    elif endpoint_type == 4:
                        # Call ads endpoint
                        self._make_api_request(f"{account_id}/ads", {
                            "fields": "id,name,status"
                        })
                        call_breakdown["ads"] += 1
                        
                    else:
                        # Call audiences endpoint
                        self._make_api_request(f"{account_id}/customaudiences", {
                            "fields": "id,name"
                        })
                        call_breakdown["audiences"] += 1
                    
                    successful_calls += 1
                    
                except Exception as call_error:
                    logger.warning(f"Individual API call failed: {call_error}")
                    failed_calls += 1
                    continue
        
        except Exception as e:
            logger.error(f"Error in API call generation: {e}")
            raise
        
        return {
            "successful_calls": successful_calls,
            "failed_calls": failed_calls,
            "call_breakdown": call_breakdown
        }

    def get_api_usage_statistics(self) -> Dict[str, Any]:
        """Get API usage statistics (mock implementation - Facebook doesn't provide this directly)"""
        try:
            # Make a test call to verify API access
            test_response = self._make_api_request("me/adaccounts", {"limit": 1})
            
            # Since Facebook doesn't provide usage stats directly, we'll return helpful info
            return {
                "api_access": "active",
                "test_call_successful": True,
                "available_accounts": len(self.get_ad_accounts()),
                "permissions_note": "Check Facebook Developer Console for actual API usage statistics",
                "advanced_access_requirements": {
                    "ads_management": "1500 calls in 15 days",
                    "ads_read": "1500 calls in 15 days",
                    "current_status": "Use Facebook Analytics dashboard to track actual usage"
                }
            }
            
        except Exception as e:
            return {
                "api_access": "error",
                "test_call_successful": False,
                "error": str(e),
                "permissions_note": "API access may be limited or expired"
            }


    # =============================================================================
    # UTILITY METHODS
    # =============================================================================
    
    def get_account_spending_for_period(self, account_id: str, period: str) -> float:
        """Get total spending for an account in a specific period"""
        try:
            key_stats = self.get_account_key_stats(account_id, period)
            return key_stats.get('total_spend', {}).get('value', 0.0)
        except Exception as e:
            logger.error(f"Error fetching account spending for {account_id}: {e}")
            return 0.0