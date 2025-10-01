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