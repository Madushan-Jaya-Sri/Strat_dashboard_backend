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

    def _validate_date_range(self, start_date: str, end_date: str) -> None:
        """Validate that date range is reasonable"""
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        # Check that start is before end
        if start > end:
            raise HTTPException(
                status_code=400,
                detail="Start date must be before end date"
            )
        
        # Check that range isn't too large (e.g., max 2 years)
        days_diff = (end - start).days
        if days_diff > 730:  # 2 years
            raise HTTPException(
                status_code=400,
                detail="Date range cannot exceed 2 years (730 days)"
            )
        
        # Check that dates aren't in the future
        if end > datetime.now():
            raise HTTPException(
                status_code=400,
                detail="End date cannot be in the future"
            )
        
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
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            data = self._make_request(f"{account_id}/insights", {
                'time_range': f'{{"since":"{since}","until":"{until}"}}',
                'fields': 'spend,impressions,clicks,actions,cpc,cpm,ctr,reach,frequency'
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
                if action.get('action_type') in ['purchase', 'lead', 'complete_registration']:
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
        except Exception as e:
            logger.error(f"Error fetching ad account insights: {e}")
            raise
    
    def get_campaigns(self, account_id: str, period: str = None, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Get campaigns for ad account"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            data = self._make_request(f"{account_id}/campaigns", {
                'fields': 'id,name,objective,status,created_time,updated_time'
            })
            
            campaigns = []
            for campaign in data.get('data', []):
                # Get insights for each campaign
                try:
                    insights = self._make_request(f"{campaign['id']}/insights", {
                        'time_range': f'{{"since":"{since}","until":"{until}"}}',
                        'fields': 'spend,impressions,clicks,cpc,cpm,ctr'
                    })
                    
                    # Handle empty insights data
                    insights_data = insights.get('data', [])
                    if insights_data:
                        campaign_insights = insights_data[0]
                    else:
                        # No data for this time period - use defaults
                        campaign_insights = {}
                        logger.warning(f"No insights data for campaign {campaign.get('id')} in period {since} to {until}")
                    
                except Exception as insight_error:
                    logger.warning(f"Could not fetch insights for campaign {campaign.get('id')}: {insight_error}")
                    campaign_insights = {}
                
                campaigns.append({
                    'id': campaign.get('id'),
                    'name': campaign.get('name'),
                    'objective': campaign.get('objective'),
                    'status': campaign.get('status'),
                    'spend': float(campaign_insights.get('spend', 0)),
                    'impressions': int(campaign_insights.get('impressions', 0)),
                    'clicks': int(campaign_insights.get('clicks', 0)),
                    'cpc': float(campaign_insights.get('cpc', 0)),
                    'cpm': float(campaign_insights.get('cpm', 0)),
                    'ctr': float(campaign_insights.get('ctr', 0)),
                    'created_time': campaign.get('created_time'),
                    'updated_time': campaign.get('updated_time')
                })
            
            logger.info(f"Retrieved {len(campaigns)} campaigns for account {account_id}")
            return campaigns
            
        except Exception as e:
            logger.error(f"Error fetching campaigns: {e}")
            return []
   
    # =========================================================================
    # FACEBOOK PAGES
    # =========================================================================
    
    def get_pages(self) -> List[Dict]:
        """Get all Facebook pages"""
        try:
            data = self._make_request("me/accounts", {
                'fields': 'id,name,category,fan_count,followers_count,link,instagram_business_account{id,username,profile_picture_url}'
            })
            
            pages = []
            for page in data.get('data', []):
                instagram_account = page.get('instagram_business_account')
                
                pages.append({
                    'id': page.get('id'),
                    'name': page.get('name'),
                    'category': page.get('category'),
                    'fan_count': page.get('fan_count', 0),
                    'followers_count': page.get('followers_count', 0),
                    'link': page.get('link'),
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
        """Get insights for specific Facebook page"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            metrics = [
                'page_impressions',
                'page_impressions_unique',
                'page_engaged_users',
                'page_post_engagements',
                'page_fans',
                'page_views_total'
            ]
            
            data = self._make_request(f"{page_id}/insights", {
                'metric': ','.join(metrics),
                'since': since,
                'until': until,
                'period': 'day'
            })
            
            insights_dict = {}
            for metric_data in data.get('data', []):
                metric_name = metric_data.get('name')
                values = metric_data.get('values', [])
                
                # Sum up values across the period
                total = sum(v.get('value', 0) for v in values)
                insights_dict[metric_name] = total
            
            return {
                'impressions': insights_dict.get('page_impressions', 0),
                'unique_impressions': insights_dict.get('page_impressions_unique', 0),
                'engaged_users': insights_dict.get('page_engaged_users', 0),
                'post_engagements': insights_dict.get('page_post_engagements', 0),
                'fans': insights_dict.get('page_fans', 0),
                'page_views': insights_dict.get('page_views_total', 0)
            }
        except Exception as e:
            logger.error(f"Error fetching page insights: {e}")
            raise
    
    def get_page_posts(self, page_id: str, limit: int = 10, period: str = None, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Get recent posts from Facebook page"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            data = self._make_request(f"{page_id}/posts", {
                'fields': 'id,message,created_time,type,attachments,likes.summary(true),comments.summary(true),shares',
                'limit': limit,
                'since': since,
                'until': until
            })
            
            posts = []
            for post in data.get('data', []):
                # Get post insights
                try:
                    insights = self._make_request(f"{post['id']}/insights", {
                        'metric': 'post_impressions,post_engaged_users,post_clicks'
                    })
                    
                    insights_dict = {i['name']: i['values'][0]['value'] for i in insights.get('data', [])}
                except:
                    insights_dict = {}
                
                posts.append({
                    'id': post.get('id'),
                    'message': post.get('message', ''),
                    'created_time': post.get('created_time'),
                    'type': post.get('type'),
                    'likes': post.get('likes', {}).get('summary', {}).get('total_count', 0),
                    'comments': post.get('comments', {}).get('summary', {}).get('total_count', 0),
                    'shares': post.get('shares', {}).get('count', 0),
                    'impressions': insights_dict.get('post_impressions', 0),
                    'engaged_users': insights_dict.get('post_engaged_users', 0),
                    'clicks': insights_dict.get('post_clicks', 0)
                })
            
            return posts
        except Exception as e:
            logger.error(f"Error fetching page posts: {e}")
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
        """Get insights for Instagram Business account"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
        
        since, until = self._period_to_dates(period, start_date, end_date)
        
        try:
            metrics = [
                'impressions',
                'reach',
                'profile_views',
                'website_clicks'
            ]
            
            data = self._make_request(f"{account_id}/insights", {
                'metric': ','.join(metrics),
                'period': 'day',
                'since': since,
                'until': until
            })
            
            insights_dict = {}
            for metric_data in data.get('data', []):
                metric_name = metric_data.get('name')
                values = metric_data.get('values', [])
                total = sum(v.get('value', 0) for v in values)
                insights_dict[metric_name] = total
            
            return {
                'impressions': insights_dict.get('impressions', 0),
                'reach': insights_dict.get('reach', 0),
                'profile_views': insights_dict.get('profile_views', 0),
                'website_clicks': insights_dict.get('website_clicks', 0)
            }
        except Exception as e:
            logger.error(f"Error fetching Instagram insights: {e}")
            raise
    
    def get_instagram_media(self, account_id: str, limit: int = 10, period: str = None, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Get recent media from Instagram account"""
        if start_date and end_date:
            self._validate_date_range(start_date, end_date)
            since_timestamp = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
        else:
            days = int(period[:-1]) if period else 30
            since_timestamp = int((datetime.now() - timedelta(days=days)).timestamp())
        
        try:
            data = self._make_request(f"{account_id}/media", {
                'fields': 'id,caption,media_type,media_url,thumbnail_url,permalink,timestamp,like_count,comments_count',
                'limit': limit
            })
            
            media_items = []
            for media in data.get('data', []):
                # Filter by timestamp
                media_timestamp = datetime.fromisoformat(media.get('timestamp').replace('Z', '+00:00')).timestamp()
                if media_timestamp < since_timestamp:
                    continue
                
                # Get media insights
                try:
                    insights = self._make_request(f"{media['id']}/insights", {
                        'metric': 'impressions,reach,engagement,saved'
                    })
                    insights_dict = {i['name']: i['values'][0]['value'] for i in insights.get('data', [])}
                except:
                    insights_dict = {}
                
                media_items.append({
                    'id': media.get('id'),
                    'caption': media.get('caption', ''),
                    'media_type': media.get('media_type'),
                    'media_url': media.get('media_url'),
                    'thumbnail_url': media.get('thumbnail_url'),
                    'permalink': media.get('permalink'),
                    'timestamp': media.get('timestamp'),
                    'like_count': media.get('like_count', 0),
                    'comments_count': media.get('comments_count', 0),
                    'impressions': insights_dict.get('impressions', 0),
                    'reach': insights_dict.get('reach', 0),
                    'engagement': insights_dict.get('engagement', 0),
                    'saved': insights_dict.get('saved', 0)
                })
            
            return media_items
        except Exception as e:
            logger.error(f"Error fetching Instagram media: {e}")
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