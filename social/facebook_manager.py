"""
Clean Facebook Manager for Facebook Pages API
Handles Facebook Pages operations, insights, and basic analytics
"""

import os
import logging
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from fastapi import HTTPException
from auth.auth_manager import AuthManager

logger = logging.getLogger(__name__)

class FacebookManager:
    """Clean manager class for Facebook Pages API operations"""
    
    def __init__(self, user_email: str, auth_manager: AuthManager):
        self.auth_manager = auth_manager
        self.user_email = user_email
        self.api_version = "v18.0"
        self.base_url = f"https://graph.facebook.com/{self.api_version}"
        
        # Get Facebook access token
        try:
            self.access_token = self.auth_manager.get_facebook_access_token(user_email)
            logger.info(f"Facebook Manager initialized for {user_email}")
        except Exception as e:
            logger.error(f"Failed to initialize Facebook Manager: {e}")
            raise HTTPException(status_code=401, detail=f"Facebook authentication required: {str(e)}")
    
    def _make_api_request(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Make API request to Facebook Graph API"""
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
                error_message = f"Facebook API Error {error_info.get('code', 'unknown')}: {error_info.get('message', 'Unknown error')}"
                logger.error(f"Facebook API error: {error_message}")
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
        """Get date range for Facebook API queries"""
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
    # FACEBOOK PAGES OPERATIONS
    # =============================================================================
    
    def get_user_pages(self) -> List[Dict[str, Any]]:
        """Get all Facebook Pages accessible to the user"""
        try:
            endpoint = "me/accounts"
            params = {
                "fields": "id,name,category,about,website,fan_count,followers_count,picture,access_token"
            }
            
            response = self._make_api_request(endpoint, params)
            pages = []
            
            for page in response.get('data', []):
                pages.append({
                    'id': page.get('id', ''),
                    'name': page.get('name', ''),
                    'category': page.get('category', ''),
                    'about': page.get('about', ''),
                    'website': page.get('website', ''),
                    'fan_count': self.safe_int(page.get('fan_count', 0)),
                    'followers_count': self.safe_int(page.get('followers_count', 0)),
                    'picture_url': page.get('picture', {}).get('data', {}).get('url', ''),
                    'access_token': page.get('access_token', ''),
                    'has_insights_access': bool(page.get('access_token', ''))
                })
            
            logger.info(f"Found {len(pages)} Facebook pages for {self.user_email}")
            return pages
            
        except Exception as e:
            logger.error(f"Error fetching Facebook pages: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch Facebook pages: {str(e)}")
    
    # =============================================================================
    # PAGE INSIGHTS (BASIC STATS)
    # =============================================================================
    
    def get_page_basic_stats(self, page_id: str, period: str = "30d") -> Dict[str, Any]:
        """Get basic statistics for a Facebook Page"""
        try:
            date_range = self.get_date_range(period)
            
            # Get page access token
            pages = self.get_user_pages()
            page_access_token = None
            page_info = None
            
            for page in pages:
                if page['id'] == page_id:
                    page_access_token = page.get('access_token')
                    page_info = page
                    break
            
            if not page_info:
                raise HTTPException(status_code=404, detail=f"Page {page_id} not found in user's pages")
            
            if not page_access_token:
                return {
                    'page_id': page_id,
                    'page_name': page_info.get('name', ''),
                    'error': 'No access token available for this page',
                    'message': 'Ensure you have admin rights to the page',
                    'fan_count': page_info.get('fan_count', 0),
                    'followers_count': page_info.get('followers_count', 0),
                    'generated_at': datetime.now().isoformat()
                }
            
            # Define basic metrics to fetch
            metrics = [
                'page_fans',
                'page_fan_adds', 
                'page_impressions',
                'page_impressions_unique',
                'page_engaged_users'
            ]
            
            endpoint = f"{page_id}/insights"
            params = {
                'metric': ','.join(metrics),
                'since': date_range['since'],
                'until': date_range['until'],
                'period': 'day',
                'access_token': page_access_token
            }
            
            try:
                response = self._make_api_request(endpoint, params)
            except HTTPException as api_error:
                if api_error.status_code == 400:
                    return {
                        'page_id': page_id,
                        'page_name': page_info.get('name', ''),
                        'error': 'Insufficient permissions or page requirements not met',
                        'message': 'Page needs 30+ followers or additional permissions',
                        'fan_count': page_info.get('fan_count', 0),
                        'followers_count': page_info.get('followers_count', 0),
                        'generated_at': datetime.now().isoformat()
                    }
                raise
            
            # Process insights data
            insights_data = {}
            for metric_data in response.get('data', []):
                metric_name = metric_data.get('name', '')
                values = metric_data.get('values', [])
                
                if metric_name in ['page_fans']:
                    # Get latest value for cumulative metrics
                    latest_value = values[-1].get('value', 0) if values else 0
                    insights_data[metric_name] = self.safe_int(latest_value)
                else:
                    # Sum daily values for period metrics
                    total_value = sum(self.safe_int(value.get('value', 0)) for value in values)
                    insights_data[metric_name] = total_value
            
            # Calculate derived metrics
            total_fans = insights_data.get('page_fans', page_info.get('fan_count', 0))
            new_fans = insights_data.get('page_fan_adds', 0)
            total_reach = insights_data.get('page_impressions_unique', 0)
            total_impressions = insights_data.get('page_impressions', 0)
            engaged_users = insights_data.get('page_engaged_users', 0)
            
            engagement_rate = (engaged_users / total_reach * 100) if total_reach > 0 else 0
            fan_growth_rate = (new_fans / total_fans * 100) if total_fans > 0 else 0
            
            return {
                'page_id': page_id,
                'page_name': page_info.get('name', ''),
                'period': period,
                'date_range': f"{date_range['since']} to {date_range['until']}",
                
                # Basic metrics
                'total_fans': total_fans,
                'new_fans': new_fans,
                'fan_growth_rate': round(fan_growth_rate, 2),
                'total_reach': total_reach,
                'total_impressions': total_impressions,
                'engaged_users': engaged_users,
                'engagement_rate': round(engagement_rate, 2),
                
                # Additional info
                'category': page_info.get('category', ''),
                'website': page_info.get('website', ''),
                'has_insights_access': True,
                'generated_at': datetime.now().isoformat()
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error fetching page stats for {page_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch page stats: {str(e)}")
    
    # =============================================================================
    # RECENT POSTS (BASIC)
    # =============================================================================
    
    def get_recent_posts(self, page_id: str, limit: int = 10, period: str = "30d") -> List[Dict[str, Any]]:
        """Get recent posts from a Facebook Page with basic engagement data"""
        try:
            date_range = self.get_date_range(period)
            since_timestamp = datetime.strptime(date_range['since'], "%Y-%m-%d").timestamp()
            
            # Get page access token
            pages = self.get_user_pages()
            page_access_token = None
            
            for page in pages:
                if page['id'] == page_id:
                    page_access_token = page.get('access_token')
                    break
            
            if not page_access_token:
                raise HTTPException(status_code=404, detail="Page not found or no access token available")
            
            endpoint = f"{page_id}/posts"
            params = {
                'fields': 'id,message,created_time,type,permalink_url,likes.summary(true),comments.summary(true),shares',
                'since': since_timestamp,
                'limit': limit,
                'access_token': page_access_token
            }
            
            response = self._make_api_request(endpoint, params)
            posts = []
            
            for post in response.get('data', []):
                # Extract basic engagement metrics
                likes_count = post.get('likes', {}).get('summary', {}).get('total_count', 0)
                comments_count = post.get('comments', {}).get('summary', {}).get('total_count', 0)
                shares_count = post.get('shares', {}).get('count', 0)
                
                total_engagement = likes_count + comments_count + shares_count
                
                posts.append({
                    'id': post.get('id', ''),
                    'message': post.get('message', '')[:200] + '...' if len(post.get('message', '')) > 200 else post.get('message', ''),
                    'created_time': post.get('created_time', ''),
                    'type': post.get('type', ''),
                    'permalink_url': post.get('permalink_url', ''),
                    'likes_count': likes_count,
                    'comments_count': comments_count,
                    'shares_count': shares_count,
                    'total_engagement': total_engagement
                })
            
            # Sort by engagement
            posts.sort(key=lambda x: x['total_engagement'], reverse=True)
            
            logger.info(f"Found {len(posts)} posts for page {page_id}")
            return posts
            
        except Exception as e:
            logger.error(f"Error fetching posts for page {page_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch page posts: {str(e)}")
    
    # =============================================================================
    # PAGE SUMMARY
    # =============================================================================
    
    def get_page_summary(self, page_id: str, period: str = "30d") -> Dict[str, Any]:
        """Get comprehensive summary for a Facebook Page"""
        try:
            # Get basic stats and recent posts
            basic_stats = self.get_page_basic_stats(page_id, period)
            recent_posts = self.get_recent_posts(page_id, limit=5, period=period)
            
            # Calculate post performance
            avg_engagement_per_post = 0
            total_post_engagement = 0
            
            if recent_posts:
                total_post_engagement = sum(post['total_engagement'] for post in recent_posts)
                avg_engagement_per_post = total_post_engagement / len(recent_posts)
            
            return {
                'page_id': page_id,
                'page_name': basic_stats.get('page_name', ''),
                'period': period,
                
                # Page metrics
                'total_fans': basic_stats.get('total_fans', 0),
                'new_fans': basic_stats.get('new_fans', 0),
                'fan_growth_rate': basic_stats.get('fan_growth_rate', 0),
                'total_reach': basic_stats.get('total_reach', 0),
                'engaged_users': basic_stats.get('engaged_users', 0),
                'engagement_rate': basic_stats.get('engagement_rate', 0),
                
                # Post performance
                'total_posts': len(recent_posts),
                'total_post_engagement': total_post_engagement,
                'avg_engagement_per_post': round(avg_engagement_per_post, 2),
                'top_performing_post': recent_posts[0] if recent_posts else None,
                
                # Page info
                'category': basic_stats.get('category', ''),
                'website': basic_stats.get('website', ''),
                'has_insights_access': basic_stats.get('has_insights_access', False),
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error fetching page summary for {page_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch page summary: {str(e)}")