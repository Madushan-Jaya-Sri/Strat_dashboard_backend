"""
Clean Instagram Manager for Instagram Business API
Handles Instagram Business account operations, insights, and basic analytics
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
    """Clean manager class for Instagram Business API operations"""
    
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
        
        params['access_token'] = self.access_token
        url = f"{self.base_url}/{endpoint}"
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            response_data = response.json()
            
            # Check for Facebook API errors
            if 'error' in response_data:
                error_info = response_data['error']
                error_message = f"Instagram API Error {error_info.get('code', 'unknown')}: {error_info.get('message', 'Unknown error')}"
                logger.error(f"Instagram API error: {error_message}")
                raise HTTPException(status_code=400, detail=error_message)
                    
            return response_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {e}")
            raise HTTPException(status_code=500, detail=f"Network error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
    
    def safe_int(self, value, default=0):
        """Safely convert to int"""
        try:
            return int(float(value)) if value else default
        except (ValueError, TypeError):
            return default
    
    def safe_float(self, value, default=0.0):
        """Safely convert to float"""
        try:
            return float(value) if value else default
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
    
    def get_instagram_accounts(self) -> List[Dict[str, Any]]:
        """Get Instagram Business accounts connected to Facebook Pages"""
        try:
            # Get Facebook Pages first
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
                    ig_details = self.get_account_details(ig_account_id)
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
    
    def get_account_details(self, account_id: str) -> Optional[Dict[str, Any]]:
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
    # ACCOUNT INSIGHTS (BASIC STATS)
    # =============================================================================
    
    def get_account_basic_stats(self, account_id: str, period: str = "30d") -> Dict[str, Any]:
        """Get basic insights for Instagram Business account"""
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
            
            try:
                response = self._make_api_request(endpoint, params)
            except HTTPException as api_error:
                if api_error.status_code == 400:
                    # Get account details as fallback
                    account_details = self.get_account_details(account_id)
                    return {
                        'account_id': account_id,
                        'username': account_details.get('username', '') if account_details else '',
                        'error': 'Insufficient permissions or account requirements not met',
                        'message': 'Instagram Business account may need additional permissions or follower threshold',
                        'followers_count': account_details.get('followers_count', 0) if account_details else 0,
                        'media_count': account_details.get('media_count', 0) if account_details else 0,
                        'generated_at': datetime.now().isoformat()
                    }
                raise
            
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
            account_details = self.get_account_details(account_id)
            
            # Calculate derived metrics
            impressions = insights_data.get('impressions', 0)
            reach = insights_data.get('reach', 0)
            followers = insights_data.get('follower_count', account_details.get('followers_count', 0) if account_details else 0)
            
            reach_rate = (reach / followers * 100) if followers > 0 else 0
            
            return {
                'account_id': account_id,
                'username': account_details.get('username', '') if account_details else '',
                'name': account_details.get('name', '') if account_details else '',
                'period': period,
                'date_range': f"{date_range['since']} to {date_range['until']}",
                
                # Core metrics
                'impressions': impressions,
                'reach': reach,
                'profile_views': insights_data.get('profile_views', 0),
                'website_clicks': insights_data.get('website_clicks', 0),
                'follower_count': followers,
                
                # Calculated metrics
                'reach_rate': round(reach_rate, 2),
                
                # Account info
                'followers_count': account_details.get('followers_count', 0) if account_details else 0,
                'follows_count': account_details.get('follows_count', 0) if account_details else 0,
                'media_count': account_details.get('media_count', 0) if account_details else 0,
                'biography': account_details.get('biography', '') if account_details else '',
                'website': account_details.get('website', '') if account_details else '',
                
                'generated_at': datetime.now().isoformat()
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error fetching Instagram account stats for {account_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch account stats: {str(e)}")
    
    # =============================================================================
    # RECENT MEDIA (BASIC)
    # =============================================================================
    
    def get_recent_media(self, account_id: str, limit: int = 10, period: str = "30d") -> List[Dict[str, Any]]:
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
                # Get basic media insights
                media_insights = self.get_media_basic_insights(media.get('id', ''))
                
                # Calculate engagement
                likes = self.safe_int(media.get('like_count', 0))
                comments = self.safe_int(media.get('comments_count', 0))
                total_engagement = likes + comments
                
                # Truncate caption for display
                caption = media.get('caption', '')
                if caption and len(caption) > 100:
                    caption = caption[:100] + '...'
                
                media_posts.append({
                    'id': media.get('id', ''),
                    'caption': caption,
                    'media_type': media.get('media_type', ''),
                    'media_url': media.get('media_url', ''),
                    'permalink': media.get('permalink', ''),
                    'thumbnail_url': media.get('thumbnail_url', ''),
                    'timestamp': media.get('timestamp', ''),
                    'username': media.get('username', ''),
                    
                    # Engagement metrics
                    'like_count': likes,
                    'comments_count': comments,
                    'total_engagement': total_engagement,
                    
                    # Basic insights metrics
                    'impressions': media_insights.get('impressions', 0),
                    'reach': media_insights.get('reach', 0),
                    
                    # Calculated metrics
                    'engagement_rate': (total_engagement / media_insights.get('reach', 1) * 100) if media_insights.get('reach', 0) > 0 else 0
                })
            
            # Sort by engagement
            media_posts.sort(key=lambda x: x['total_engagement'], reverse=True)
            
            logger.info(f"Found {len(media_posts)} media posts for Instagram account {account_id}")
            return media_posts
            
        except Exception as e:
            logger.error(f"Error fetching Instagram media for account {account_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch Instagram media: {str(e)}")
    
    def get_media_basic_insights(self, media_id: str) -> Dict[str, Any]:
        """Get basic insights for a specific Instagram media post"""
        try:
            # Basic metrics that work for most media types
            metrics = ['impressions', 'reach']
            
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
    # ACCOUNT SUMMARY
    # =============================================================================
    
    def get_account_summary(self, account_id: str, period: str = "30d") -> Dict[str, Any]:
        """Get comprehensive summary for an Instagram Business account"""
        try:
            # Get basic stats and recent media
            basic_stats = self.get_account_basic_stats(account_id, period)
            recent_media = self.get_recent_media(account_id, limit=5, period=period)
            
            # Calculate media performance
            avg_engagement_per_post = 0
            total_media_engagement = 0
            overall_engagement_rate = 0
            
            if recent_media:
                total_media_engagement = sum(post['total_engagement'] for post in recent_media)
                avg_engagement_per_post = total_media_engagement / len(recent_media)
                
                # Calculate overall engagement rate from reach
                total_reach = sum(post['reach'] for post in recent_media if post['reach'] > 0)
                if total_reach > 0:
                    overall_engagement_rate = (total_media_engagement / total_reach) * 100
            
            # Find best performing post
            best_post = max(recent_media, key=lambda x: x['total_engagement']) if recent_media else None
            
            return {
                'account_id': account_id,
                'username': basic_stats.get('username', ''),
                'name': basic_stats.get('name', ''),
                'period': period,
                
                # Account metrics
                'followers_count': basic_stats.get('followers_count', 0),
                'follows_count': basic_stats.get('follows_count', 0),
                'media_count': basic_stats.get('media_count', 0),
                
                # Performance metrics
                'total_impressions': basic_stats.get('impressions', 0),
                'total_reach': basic_stats.get('reach', 0),
                'profile_views': basic_stats.get('profile_views', 0),
                'website_clicks': basic_stats.get('website_clicks', 0),
                'reach_rate': basic_stats.get('reach_rate', 0),
                
                # Content performance
                'posts_in_period': len(recent_media),
                'total_media_engagement': total_media_engagement,
                'avg_engagement_per_post': round(avg_engagement_per_post, 2),
                'overall_engagement_rate': round(overall_engagement_rate, 2),
                'best_performing_post': best_post,
                
                # Account info
                'biography': basic_stats.get('biography', ''),
                'website': basic_stats.get('website', ''),
                
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error fetching Instagram account summary for {account_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch account summary: {str(e)}")
    
    # =============================================================================
    # BASIC ANALYTICS (TOP HASHTAGS FROM POSTS)
    # =============================================================================
    
    def get_popular_hashtags(self, account_id: str, period: str = "30d", limit: int = 10) -> List[Dict[str, Any]]:
        """Get popular hashtags from recent posts"""
        try:
            recent_media = self.get_recent_media(account_id, limit=20, period=period)
            hashtag_performance = {}
            
            for post in recent_media:
                caption = post.get('caption', '')
                if caption:
                    # Extract hashtags
                    words = caption.split()
                    hashtags = [word for word in words if word.startswith('#') and len(word) > 1]
                    
                    for hashtag in hashtags:
                        hashtag_clean = hashtag.lower()
                        if hashtag_clean not in hashtag_performance:
                            hashtag_performance[hashtag_clean] = {
                                'hashtag': hashtag_clean,
                                'posts_count': 0,
                                'total_engagement': 0,
                                'total_reach': 0
                            }
                        
                        hashtag_performance[hashtag_clean]['posts_count'] += 1
                        hashtag_performance[hashtag_clean]['total_engagement'] += post.get('total_engagement', 0)
                        hashtag_performance[hashtag_clean]['total_reach'] += post.get('reach', 0)
            
            # Calculate averages and sort
            hashtag_list = []
            for hashtag_data in hashtag_performance.values():
                if hashtag_data['posts_count'] > 0:
                    avg_engagement = hashtag_data['total_engagement'] / hashtag_data['posts_count']
                    avg_reach = hashtag_data['total_reach'] / hashtag_data['posts_count']
                    
                    hashtag_list.append({
                        'hashtag': hashtag_data['hashtag'],
                        'posts_count': hashtag_data['posts_count'],
                        'avg_engagement_per_post': round(avg_engagement, 2),
                        'avg_reach_per_post': round(avg_reach, 2),
                        'total_engagement': hashtag_data['total_engagement']
                    })
            
            # Sort by total engagement and limit results
            hashtag_list.sort(key=lambda x: x['total_engagement'], reverse=True)
            
            return hashtag_list[:limit]
            
        except Exception as e:
            logger.error(f"Error analyzing hashtag performance for account {account_id}: {e}")
            return []