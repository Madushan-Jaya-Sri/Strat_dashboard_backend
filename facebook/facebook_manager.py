"""
Facebook Manager for Facebook Pages API
Handles all Facebook Pages operations, insights, and content analytics
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
    """Manager class for Facebook Pages API operations"""
    
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
        
        # Add access token to all requests
        params['access_token'] = self.access_token
        
        url = f"{self.base_url}/{endpoint}"
        
        try:
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Facebook API error {response.status_code}: {response.text}")
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Facebook API error: {response.text}"
                )
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Facebook API request failed: {e}")
            raise HTTPException(status_code=500, detail=f"Facebook API request failed: {str(e)}")
    
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
                "fields": "id,name,category,category_list,about,description,website,phone,location,fan_count,followers_count,engagement,picture"
            }
            
            response = self._make_api_request(endpoint, params)
            pages = []
            
            for page in response.get('data', []):
                # Get page access token for insights
                page_access_token = page.get('access_token', '')
                
                pages.append({
                    'id': page.get('id', ''),
                    'name': page.get('name', ''),
                    'category': page.get('category', ''),
                    'category_list': page.get('category_list', []),
                    'about': page.get('about', ''),
                    'description': page.get('description', ''),
                    'website': page.get('website', ''),
                    'phone': page.get('phone', ''),
                    'location': page.get('location', {}),
                    'fan_count': self.safe_int(page.get('fan_count', 0)),
                    'followers_count': self.safe_int(page.get('followers_count', 0)),
                    'engagement': page.get('engagement', {}),
                    'picture_url': page.get('picture', {}).get('data', {}).get('url', ''),
                    'access_token': page_access_token,
                    'has_insights_access': bool(page_access_token)
                })
            
            logger.info(f"Found {len(pages)} Facebook pages for {self.user_email}")
            return pages
            
        except Exception as e:
            logger.error(f"Error fetching Facebook pages: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch Facebook pages: {str(e)}")
    
    # =============================================================================
    # PAGE INSIGHTS AND ANALYTICS
    # =============================================================================
    
    def get_page_insights(self, page_id: str, period: str = "30d") -> Dict[str, Any]:
        """Get comprehensive insights for a Facebook Page"""
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
            
            if not page_access_token:
                raise HTTPException(status_code=404, detail="Page not found or no access token available")
            
            # Define metrics to fetch
            metrics = [
                'page_fans',  # Total page likes
                'page_fan_adds',  # New page likes
                'page_fan_removes',  # Page unlikes
                'page_impressions',  # Total reach
                'page_impressions_unique',  # Unique reach
                'page_engaged_users',  # People engaged
                'page_post_engagements',  # Post engagements
                'page_posts_impressions',  # Post reach
                'page_video_views',  # Video views
                'page_views_total'  # Page views
            ]
            
            endpoint = f"{page_id}/insights"
            params = {
                'metric': ','.join(metrics),
                'since': date_range['since'],
                'until': date_range['until'],
                'period': 'day',
                'access_token': page_access_token
            }
            
            response = self._make_api_request(endpoint, params)
            
            # Process insights data
            insights_data = {}
            for metric_data in response.get('data', []):
                metric_name = metric_data.get('name', '')
                values = metric_data.get('values', [])
                
                # Sum up daily values
                total_value = sum(self.safe_int(value.get('value', 0)) for value in values)
                insights_data[metric_name] = total_value
            
            # Calculate derived metrics
            total_fans = insights_data.get('page_fans', 0)
            new_fans = insights_data.get('page_fan_adds', 0)
            lost_fans = insights_data.get('page_fan_removes', 0)
            net_fan_change = new_fans - lost_fans
            
            total_reach = insights_data.get('page_impressions_unique', 0)
            total_impressions = insights_data.get('page_impressions', 0)
            engaged_users = insights_data.get('page_engaged_users', 0)
            post_engagements = insights_data.get('page_post_engagements', 0)
            
            engagement_rate = (engaged_users / total_reach * 100) if total_reach > 0 else 0
            frequency = (total_impressions / total_reach) if total_reach > 0 else 0
            
            return {
                'page_id': page_id,
                'page_name': page_info.get('name', ''),
                'period': period,
                'date_range': f"{date_range['since']} to {date_range['until']}",
                
                # Fan metrics
                'total_fans': total_fans,
                'new_fans': new_fans,
                'lost_fans': lost_fans,
                'net_fan_change': net_fan_change,
                'fan_growth_rate': (net_fan_change / total_fans * 100) if total_fans > 0 else 0,
                
                # Reach and impressions
                'total_reach': total_reach,
                'total_impressions': total_impressions,
                'frequency': round(frequency, 2),
                
                # Engagement
                'engaged_users': engaged_users,
                'post_engagements': post_engagements,
                'engagement_rate': round(engagement_rate, 2),
                
                # Content performance
                'video_views': insights_data.get('page_video_views', 0),
                'page_views': insights_data.get('page_views_total', 0),
                'post_reach': insights_data.get('page_posts_impressions', 0),
                
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error fetching page insights for {page_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch page insights: {str(e)}")
    
    # =============================================================================
    # POST ANALYTICS
    # =============================================================================
    
    def get_page_posts(self, page_id: str, limit: int = 20, period: str = "30d") -> List[Dict[str, Any]]:
        """Get recent posts from a Facebook Page with engagement data"""
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
                'fields': 'id,message,story,created_time,type,status_type,link,picture,full_picture,permalink_url,shares,likes.summary(true),comments.summary(true),reactions.summary(true)',
                'since': since_timestamp,
                'limit': limit,
                'access_token': page_access_token
            }
            
            response = self._make_api_request(endpoint, params)
            posts = []
            
            for post in response.get('data', []):
                # Extract engagement metrics
                likes_count = post.get('likes', {}).get('summary', {}).get('total_count', 0)
                comments_count = post.get('comments', {}).get('summary', {}).get('total_count', 0)
                reactions_count = post.get('reactions', {}).get('summary', {}).get('total_count', 0)
                shares_count = post.get('shares', {}).get('count', 0)
                
                total_engagement = likes_count + comments_count + shares_count
                
                posts.append({
                    'id': post.get('id', ''),
                    'message': post.get('message', ''),
                    'story': post.get('story', ''),
                    'created_time': post.get('created_time', ''),
                    'type': post.get('type', ''),
                    'status_type': post.get('status_type', ''),
                    'link': post.get('link', ''),
                    'picture': post.get('picture', ''),
                    'full_picture': post.get('full_picture', ''),
                    'permalink_url': post.get('permalink_url', ''),
                    
                    # Engagement metrics
                    'likes_count': likes_count,
                    'comments_count': comments_count,
                    'reactions_count': reactions_count,
                    'shares_count': shares_count,
                    'total_engagement': total_engagement
                })
            
            # Sort by total engagement
            posts.sort(key=lambda x: x['total_engagement'], reverse=True)
            
            logger.info(f"Found {len(posts)} posts for page {page_id}")
            return posts
            
        except Exception as e:
            logger.error(f"Error fetching posts for page {page_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch page posts: {str(e)}")
    
    def get_post_insights(self, post_id: str) -> Dict[str, Any]:
        """Get detailed insights for a specific post"""
        try:
            # Get page access token (you may need to determine which page owns the post)
            pages = self.get_user_pages()
            
            # Try to get insights using page access tokens
            for page in pages:
                page_access_token = page.get('access_token')
                if not page_access_token:
                    continue
                
                try:
                    endpoint = f"{post_id}/insights"
                    params = {
                        'metric': 'post_impressions,post_reach,post_engaged_users,post_clicks,post_reactions_by_type_total',
                        'access_token': page_access_token
                    }
                    
                    response = self._make_api_request(endpoint, params)
                    
                    # Process insights
                    insights = {}
                    for metric_data in response.get('data', []):
                        metric_name = metric_data.get('name', '')
                        value = metric_data.get('values', [{}])[0].get('value', 0)
                        insights[metric_name] = value
                    
                    return {
                        'post_id': post_id,
                        'impressions': self.safe_int(insights.get('post_impressions', 0)),
                        'reach': self.safe_int(insights.get('post_reach', 0)),
                        'engaged_users': self.safe_int(insights.get('post_engaged_users', 0)),
                        'clicks': self.safe_int(insights.get('post_clicks', 0)),
                        'reactions_breakdown': insights.get('post_reactions_by_type_total', {}),
                        'generated_at': datetime.now().isoformat()
                    }
                    
                except Exception:
                    continue  # Try next page
            
            raise HTTPException(status_code=404, detail="Post insights not accessible")
            
        except Exception as e:
            logger.error(f"Error fetching post insights for {post_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch post insights: {str(e)}")
    
    # =============================================================================
    # AUDIENCE INSIGHTS
    # =============================================================================
    
    def get_audience_insights(self, page_id: str, period: str = "30d") -> Dict[str, Any]:
        """Get audience demographics and insights for a Facebook Page"""
        try:
            date_range = self.get_date_range(period)
            
            # Get page access token
            pages = self.get_user_pages()
            page_access_token = None
            
            for page in pages:
                if page['id'] == page_id:
                    page_access_token = page.get('access_token')
                    break
            
            if not page_access_token:
                raise HTTPException(status_code=404, detail="Page not found or no access token available")
            
            # Get demographic insights
            demographic_metrics = [
                'page_fans_gender_age',
                'page_fans_country',
                'page_fans_city',
                'page_impressions_by_age_gender_unique',
                'page_impressions_by_country_unique'
            ]
            
            endpoint = f"{page_id}/insights"
            params = {
                'metric': ','.join(demographic_metrics),
                'since': date_range['since'],
                'until': date_range['until'],
                'period': 'day',
                'access_token': page_access_token
            }
            
            response = self._make_api_request(endpoint, params)
            
            # Process demographic data
            demographics = {}
            for metric_data in response.get('data', []):
                metric_name = metric_data.get('name', '')
                values = metric_data.get('values', [])
                
                # Get the latest value (most recent day)
                if values:
                    latest_value = values[-1].get('value', {})
                    demographics[metric_name] = latest_value
            
            return {
                'page_id': page_id,
                'period': period,
                'date_range': f"{date_range['since']} to {date_range['until']}",
                
                # Fan demographics
                'fans_by_age_gender': demographics.get('page_fans_gender_age', {}),
                'fans_by_country': demographics.get('page_fans_country', {}),
                'fans_by_city': demographics.get('page_fans_city', {}),
                
                # Reach demographics
                'reach_by_age_gender': demographics.get('page_impressions_by_age_gender_unique', {}),
                'reach_by_country': demographics.get('page_impressions_by_country_unique', {}),
                
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error fetching audience insights for page {page_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch audience insights: {str(e)}")
    
    # =============================================================================
    # PAGE PERFORMANCE SUMMARY
    # =============================================================================
    
    def get_page_performance_summary(self, page_id: str, period: str = "30d") -> Dict[str, Any]:
        """Get comprehensive performance summary for a Facebook Page"""
        try:
            # Get basic page info
            page_info = None
            pages = self.get_user_pages()
            for page in pages:
                if page['id'] == page_id:
                    page_info = page
                    break
            
            if not page_info:
                raise HTTPException(status_code=404, detail="Page not found")
            
            # Get insights and posts data
            insights = self.get_page_insights(page_id, period)
            recent_posts = self.get_page_posts(page_id, limit=10, period=period)
            
            # Calculate additional metrics
            avg_engagement_per_post = 0
            total_post_engagement = 0
            
            if recent_posts:
                total_post_engagement = sum(post['total_engagement'] for post in recent_posts)
                avg_engagement_per_post = total_post_engagement / len(recent_posts)
            
            return {
                'page_id': page_id,
                'page_name': page_info.get('name', ''),
                'page_category': page_info.get('category', ''),
                'followers_count': page_info.get('followers_count', 0),
                'period': period,
                
                # Key metrics from insights
                'total_reach': insights.get('total_reach', 0),
                'total_impressions': insights.get('total_impressions', 0),
                'engaged_users': insights.get('engaged_users', 0),
                'engagement_rate': insights.get('engagement_rate', 0),
                'new_fans': insights.get('new_fans', 0),
                'net_fan_change': insights.get('net_fan_change', 0),
                
                # Post performance
                'total_posts': len(recent_posts),
                'total_post_engagement': total_post_engagement,
                'avg_engagement_per_post': round(avg_engagement_per_post, 2),
                'top_performing_post': recent_posts[0] if recent_posts else None,
                
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error fetching performance summary for page {page_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch page performance summary: {str(e)}")