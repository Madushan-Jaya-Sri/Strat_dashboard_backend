"""
Instagram Manager for Instagram Business API
Handles Instagram Business account operations, insights, and content analytics
"""

import os
import logging
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from fastapi import HTTPException
from auth.auth_manager import AuthManager

logger = logging.getLogger(__name__)

class InstagramManager:
    """Manager class for Instagram Business API operations"""
    
    def __init__(self, user_email: str, auth_manager: AuthManager):
        self.auth_manager = auth_manager
        self.user_email = user_email
        self.api_version = "v18.0"
        self.base_url = f"https://graph.facebook.com/{self.api_version}"
        
        # Get Facebook access token (Instagram Business API uses Facebook tokens)
        try:
            self.access_token = self.auth_manager.get_facebook_access_token(user_email)
            logger.info(f"Instagram Manager initialized for {user_email}")
        except Exception as e:
            logger.error(f"Failed to initialize Instagram Manager: {e}")
            raise HTTPException(status_code=401, detail=f"Facebook authentication required: {str(e)}")
    
    def _make_api_request(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Make API request to Facebook Graph API for Instagram"""
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
                logger.error(f"Instagram API error {response.status_code}: {response.text}")
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Instagram API error: {response.text}"
                )
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Instagram API request failed: {e}")
            raise HTTPException(status_code=500, detail=f"Instagram API request failed: {str(e)}")
    
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
        """Get date range for Instagram API queries"""
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
    # INSTAGRAM BUSINESS ACCOUNTS
    # =============================================================================
    
    def get_instagram_business_accounts(self) -> List[Dict[str, Any]]:
        """Get Instagram Business accounts connected to Facebook Pages"""
        try:
            # First get Facebook Pages
            endpoint = "me/accounts"
            params = {
                "fields": "id,name,instagram_business_account"
            }
            
            response = self._make_api_request(endpoint, params)
            instagram_accounts = []
            
            for page in response.get('data', []):
                ig_account = page.get('instagram_business_account')
                if ig_account:
                    ig_account_id = ig_account.get('id')
                    
                    # Get Instagram account details
                    ig_details = self.get_instagram_account_details(ig_account_id)
                    if ig_details:
                        ig_details['connected_facebook_page'] = {
                            'id': page.get('id'),
                            'name': page.get('name')
                        }
                        instagram_accounts.append(ig_details)
            
            logger.info(f"Found {len(instagram_accounts)} Instagram Business accounts for {self.user_email}")
            return instagram_accounts
            
        except Exception as e:
            logger.error(f"Error fetching Instagram Business accounts: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch Instagram accounts: {str(e)}")
    
    def get_instagram_account_details(self, account_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed information for an Instagram Business account"""
        try:
            endpoint = f"{account_id}"
            params = {
                "fields": "id,username,name,biography,website,followers_count,follows_count,media_count,profile_picture_url"
            }
            
            response = self._make_api_request(endpoint, params)
            
            return {
                'id': response.get('id', ''),
                'username': response.get('username', ''),
                'name': response.get('name', ''),
                'biography': response.get('biography', ''),
                'website': response.get('website', ''),
                'followers_count': self.safe_int(response.get('followers_count', 0)),
                'follows_count': self.safe_int(response.get('follows_count', 0)),
                'media_count': self.safe_int(response.get('media_count', 0)),
                'profile_picture_url': response.get('profile_picture_url', ''),
                'account_type': 'business'
            }
            
        except Exception as e:
            logger.warning(f"Could not fetch details for Instagram account {account_id}: {e}")
            return None
    
    # =============================================================================
    # INSTAGRAM INSIGHTS
    # =============================================================================
    
    def get_account_insights(self, account_id: str, period: str = "30d") -> Dict[str, Any]:
        """Get Instagram Business account insights"""
        try:
            date_range = self.get_date_range(period)
            
            # Instagram account insights metrics
            metrics = [
                'impressions',
                'reach', 
                'profile_views',
                'website_clicks',
                'follower_count'
            ]
            
            endpoint = f"{account_id}/insights"
            params = {
                'metric': ','.join(metrics),
                'period': 'day',
                'since': date_range['since'],
                'until': date_range['until']
            }
            
            response = self._make_api_request(endpoint, params)
            
            # Process insights data
            insights_data = {}
            for metric_data in response.get('data', []):
                metric_name = metric_data.get('name', '')
                values = metric_data.get('values', [])
                
                if metric_name == 'follower_count':
                    # Follower count is a lifetime metric, get the latest value
                    if values:
                        insights_data[metric_name] = self.safe_int(values[-1].get('value', 0))
                else:
                    # Sum up daily values for period metrics
                    total_value = sum(self.safe_int(value.get('value', 0)) for value in values)
                    insights_data[metric_name] = total_value
            
            # Get account details for additional info
            account_details = self.get_instagram_account_details(account_id)
            
            return {
                'account_id': account_id,
                'username': account_details.get('username', '') if account_details else '',
                'period': period,
                'date_range': f"{date_range['since']} to {date_range['until']}",
                
                # Core metrics
                'impressions': insights_data.get('impressions', 0),
                'reach': insights_data.get('reach', 0),
                'profile_views': insights_data.get('profile_views', 0),
                'website_clicks': insights_data.get('website_clicks', 0),
                'follower_count': insights_data.get('follower_count', 0),
                
                # Calculated metrics
                'engagement_rate': 0,  # Will be calculated with media insights
                'reach_rate': (insights_data.get('reach', 0) / insights_data.get('follower_count', 1) * 100) if insights_data.get('follower_count', 0) > 0 else 0,
                
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error fetching Instagram account insights for {account_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch account insights: {str(e)}")
    
    # =============================================================================
    # INSTAGRAM MEDIA CONTENT
    # =============================================================================
    
    def get_account_media(self, account_id: str, limit: int = 20, period: str = "30d") -> List[Dict[str, Any]]:
        """Get recent media posts from Instagram Business account"""
        try:
            date_range = self.get_date_range(period)
            since_timestamp = datetime.strptime(date_range['since'], "%Y-%m-%d").timestamp()
            
            endpoint = f"{account_id}/media"
            params = {
                'fields': 'id,caption,media_type,media_url,permalink,thumbnail_url,timestamp,username,like_count,comments_count',
                'since': since_timestamp,
                'limit': limit
            }
            
            response = self._make_api_request(endpoint, params)
            media_posts = []
            
            for media in response.get('data', []):
                # Get media insights if available
                media_insights = self.get_media_insights(media.get('id', ''))
                
                media_posts.append({
                    'id': media.get('id', ''),
                    'caption': media.get('caption', ''),
                    'media_type': media.get('media_type', ''),
                    'media_url': media.get('media_url', ''),
                    'permalink': media.get('permalink', ''),
                    'thumbnail_url': media.get('thumbnail_url', ''),
                    'timestamp': media.get('timestamp', ''),
                    'username': media.get('username', ''),
                    
                    # Engagement metrics
                    'like_count': self.safe_int(media.get('like_count', 0)),
                    'comments_count': self.safe_int(media.get('comments_count', 0)),
                    
                    # Insights metrics
                    'impressions': media_insights.get('impressions', 0),
                    'reach': media_insights.get('reach', 0),
                    'engagement': media_insights.get('engagement', 0),
                    'saved': media_insights.get('saved', 0),
                    'video_views': media_insights.get('video_views', 0) if media.get('media_type') == 'VIDEO' else 0,
                    
                    # Calculated metrics
                    'total_engagement': self.safe_int(media.get('like_count', 0)) + self.safe_int(media.get('comments_count', 0)),
                    'engagement_rate': (media_insights.get('engagement', 0) / media_insights.get('reach', 1) * 100) if media_insights.get('reach', 0) > 0 else 0
                })
            
            # Sort by timestamp (most recent first)
            media_posts.sort(key=lambda x: x['timestamp'], reverse=True)
            
            logger.info(f"Found {len(media_posts)} media posts for Instagram account {account_id}")
            return media_posts
            
        except Exception as e:
            logger.error(f"Error fetching Instagram media for account {account_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch Instagram media: {str(e)}")
    
    def get_media_insights(self, media_id: str) -> Dict[str, Any]:
        """Get insights for a specific Instagram media post"""
        try:
            # Different metrics for different media types
            photo_metrics = ['impressions', 'reach', 'engagement', 'saved', 'profile_visits', 'website_clicks']
            video_metrics = ['impressions', 'reach', 'engagement', 'saved', 'video_views', 'profile_visits', 'website_clicks']
            reel_metrics = ['impressions', 'reach', 'engagement', 'saved', 'video_views', 'plays', 'profile_visits', 'website_clicks']
            
            # First get media info to determine type
            media_info_response = self._make_api_request(f"{media_id}", {'fields': 'media_type'})
            media_type = media_info_response.get('media_type', 'IMAGE')
            
            # Select appropriate metrics
            if media_type == 'VIDEO':
                metrics = video_metrics
            elif media_type == 'REELS':
                metrics = reel_metrics
            else:
                metrics = photo_metrics
            
            endpoint = f"{media_id}/insights"
            params = {
                'metric': ','.join(metrics)
            }
            
            response = self._make_api_request(endpoint, params)
            
            # Process insights
            insights = {}
            for metric_data in response.get('data', []):
                metric_name = metric_data.get('name', '')
                value = metric_data.get('values', [{}])[0].get('value', 0)
                insights[metric_name] = self.safe_int(value)
            
            return insights
            
        except Exception as e:
            logger.warning(f"Could not fetch insights for media {media_id}: {e}")
            return {}
    
    # =============================================================================
    # INSTAGRAM STORIES
    # =============================================================================
    
    def get_story_insights(self, account_id: str, period: str = "7d") -> List[Dict[str, Any]]:
        """Get Instagram Stories insights (stories are only available for 24 hours)"""
        try:
            # Stories have a very short lifespan, so we limit the period
            if period not in ["1d", "7d"]:
                period = "7d"
            
            date_range = self.get_date_range(period)
            since_timestamp = datetime.strptime(date_range['since'], "%Y-%m-%d").timestamp()
            
            endpoint = f"{account_id}/stories"
            params = {
                'fields': 'id,media_type,media_url,permalink,thumbnail_url,timestamp',
                'since': since_timestamp,
                'limit': 50
            }
            
            response = self._make_api_request(endpoint, params)
            stories = []
            
            for story in response.get('data', []):
                # Get story insights
                story_insights = self.get_story_media_insights(story.get('id', ''))
                
                stories.append({
                    'id': story.get('id', ''),
                    'media_type': story.get('media_type', ''),
                    'media_url': story.get('media_url', ''),
                    'permalink': story.get('permalink', ''),
                    'thumbnail_url': story.get('thumbnail_url', ''),
                    'timestamp': story.get('timestamp', ''),
                    
                    # Story insights
                    'impressions': story_insights.get('impressions', 0),
                    'reach': story_insights.get('reach', 0),
                    'replies': story_insights.get('replies', 0),
                    'exits': story_insights.get('exits', 0),
                    'taps_forward': story_insights.get('taps_forward', 0),
                    'taps_back': story_insights.get('taps_back', 0),
                    
                    # Calculated metrics
                    'completion_rate': ((story_insights.get('impressions', 0) - story_insights.get('exits', 0)) / story_insights.get('impressions', 1) * 100) if story_insights.get('impressions', 0) > 0 else 0
                })
            
            logger.info(f"Found {len(stories)} stories for Instagram account {account_id}")
            return stories
            
        except Exception as e:
            logger.error(f"Error fetching Instagram stories for account {account_id}: {e}")
            return []
    
    def get_story_media_insights(self, story_id: str) -> Dict[str, Any]:
        """Get insights for a specific Instagram story"""
        try:
            story_metrics = ['impressions', 'reach', 'replies', 'exits', 'taps_forward', 'taps_back']
            
            endpoint = f"{story_id}/insights"
            params = {
                'metric': ','.join(story_metrics)
            }
            
            response = self._make_api_request(endpoint, params)
            
            # Process insights
            insights = {}
            for metric_data in response.get('data', []):
                metric_name = metric_data.get('name', '')
                value = metric_data.get('values', [{}])[0].get('value', 0)
                insights[metric_name] = self.safe_int(value)
            
            return insights
            
        except Exception as e:
            logger.warning(f"Could not fetch insights for story {story_id}: {e}")
            return {}
    
    # =============================================================================
    # AUDIENCE INSIGHTS
    # =============================================================================
    
    def get_audience_demographics(self, account_id: str, period: str = "30d") -> Dict[str, Any]:
        """Get audience demographics for Instagram Business account"""
        try:
            date_range = self.get_date_range(period)
            
            # Audience demographic metrics
            demographic_metrics = [
                'audience_gender_age',
                'audience_country',
                'audience_city'
            ]
            
            endpoint = f"{account_id}/insights"
            params = {
                'metric': ','.join(demographic_metrics),
                'period': 'lifetime'  # Audience demographics are lifetime metrics
            }
            
            response = self._make_api_request(endpoint, params)
            
            # Process demographic data
            demographics = {}
            for metric_data in response.get('data', []):
                metric_name = metric_data.get('name', '')
                values = metric_data.get('values', [])
                
                if values:
                    demographics[metric_name] = values[0].get('value', {})
            
            return {
                'account_id': account_id,
                'period': period,
                
                # Demographic breakdowns
                'audience_by_gender_age': demographics.get('audience_gender_age', {}),
                'audience_by_country': demographics.get('audience_country', {}),
                'audience_by_city': demographics.get('audience_city', {}),
                
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error fetching audience demographics for account {account_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch audience demographics: {str(e)}")
    
    # =============================================================================
    # PERFORMANCE SUMMARY
    # =============================================================================
    
    def get_account_performance_summary(self, account_id: str, period: str = "30d") -> Dict[str, Any]:
        """Get comprehensive performance summary for Instagram Business account"""
        try:
            # Get account details and insights
            account_details = self.get_instagram_account_details(account_id)
            account_insights = self.get_account_insights(account_id, period)
            recent_media = self.get_account_media(account_id, limit=10, period=period)
            
            # Calculate additional metrics
            total_media_engagement = sum(post['total_engagement'] for post in recent_media)
            avg_engagement_per_post = total_media_engagement / len(recent_media) if recent_media else 0
            
            # Calculate overall engagement rate
            total_likes = sum(post['like_count'] for post in recent_media)
            total_comments = sum(post['comments_count'] for post in recent_media)
            total_reach = sum(post['reach'] for post in recent_media)
            overall_engagement_rate = ((total_likes + total_comments) / total_reach * 100) if total_reach > 0 else 0
            
            # Find best performing post
            best_post = max(recent_media, key=lambda x: x['total_engagement']) if recent_media else None
            
            return {
                'account_id': account_id,
                'username': account_details.get('username', '') if account_details else '',
                'period': period,
                
                # Account metrics
                'followers_count': account_details.get('followers_count', 0) if account_details else 0,
                'following_count': account_details.get('follows_count', 0) if account_details else 0,
                'media_count': account_details.get('media_count', 0) if account_details else 0,
                
                # Performance metrics
                'total_impressions': account_insights.get('impressions', 0),
                'total_reach': account_insights.get('reach', 0),
                'profile_views': account_insights.get('profile_views', 0),
                'website_clicks': account_insights.get('website_clicks', 0),
                
                # Content performance
                'posts_in_period': len(recent_media),
                'total_media_engagement': total_media_engagement,
                'avg_engagement_per_post': round(avg_engagement_per_post, 2),
                'overall_engagement_rate': round(overall_engagement_rate, 2),
                'best_performing_post': best_post,
                
                # Calculated ratios
                'follower_growth_rate': 0,  # Would need historical data
                'reach_rate': account_insights.get('reach_rate', 0),
                
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error fetching performance summary for account {account_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch account performance summary: {str(e)}")
    
    # =============================================================================
    # HASHTAG PERFORMANCE (if available)
    # =============================================================================
    
    def get_hashtag_performance(self, account_id: str, hashtags: List[str], period: str = "30d") -> List[Dict[str, Any]]:
        """Get performance data for specific hashtags (limited availability)"""
        try:
            # Note: Hashtag insights are limited and may require special permissions
            # This is a basic implementation
            
            hashtag_performance = []
            recent_media = self.get_account_media(account_id, limit=50, period=period)
            
            for hashtag in hashtags:
                hashtag_clean = hashtag.replace('#', '').lower()
                
                # Find posts that contain this hashtag
                matching_posts = []
                for post in recent_media:
                    caption = post.get('caption', '').lower()
                    if f"#{hashtag_clean}" in caption or hashtag_clean in caption:
                        matching_posts.append(post)
                
                if matching_posts:
                    total_engagement = sum(post['total_engagement'] for post in matching_posts)
                    total_reach = sum(post['reach'] for post in matching_posts)
                    avg_engagement = total_engagement / len(matching_posts)
                    
                    hashtag_performance.append({
                        'hashtag': f"#{hashtag_clean}",
                        'posts_count': len(matching_posts),
                        'total_engagement': total_engagement,
                        'avg_engagement_per_post': round(avg_engagement, 2),
                        'total_reach': total_reach,
                        'engagement_rate': (total_engagement / total_reach * 100) if total_reach > 0 else 0
                    })
            
            # Sort by total engagement
            hashtag_performance.sort(key=lambda x: x['total_engagement'], reverse=True)
            
            return hashtag_performance
            
        except Exception as e:
            logger.error(f"Error analyzing hashtag performance for account {account_id}: {e}")
            return []