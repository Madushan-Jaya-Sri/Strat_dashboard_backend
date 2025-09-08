"""
Response Models for Meta/Facebook/Instagram APIs
Pydantic models for all Meta platform responses
"""

from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime

# =============================================================================
# META ADS MODELS
# =============================================================================

class MetaAdAccount(BaseModel):
    id: str
    name: str
    account_status: str
    currency: str
    balance: float
    amount_spent: float
    spend_cap: float
    timezone_name: str
    business_name: str

class MetaCampaign(BaseModel):
    id: str
    name: str
    status: str
    objective: str
    created_time: str
    start_time: Optional[str] = None
    stop_time: Optional[str] = None
    updated_time: str
    spend: float
    impressions: int
    clicks: int
    ctr: float
    cpc: float
    cpm: float
    reach: int
    frequency: float

class MetaKeyStatMetric(BaseModel):
    value: float
    formatted: str
    label: str
    description: str

class MetaKeyStatsSummary(BaseModel):
    period: str
    account_id: str
    date_range: str
    generated_at: str

class MetaAdAccountKeyStats(BaseModel):
    total_spend: MetaKeyStatMetric
    total_impressions: MetaKeyStatMetric
    total_clicks: MetaKeyStatMetric
    click_through_rate: MetaKeyStatMetric
    cost_per_click: MetaKeyStatMetric
    cost_per_mille: MetaKeyStatMetric
    total_reach: MetaKeyStatMetric
    frequency: MetaKeyStatMetric
    summary: MetaKeyStatsSummary

class MetaPlacementPerformance(BaseModel):
    placement: str
    publisher_platform: str
    platform_position: str
    spend: float
    impressions: int
    clicks: int
    ctr: float
    cpc: float

class MetaDemographicPerformance(BaseModel):
    age: str
    gender: str
    demographic: str
    spend: float
    impressions: int
    clicks: int
    reach: int
    ctr: float
    cpc: float

class MetaTimeSeriesData(BaseModel):
    date: str
    spend: float
    impressions: int
    clicks: int
    reach: int
    ctr: float
    cpc: float

# =============================================================================
# FACEBOOK PAGES MODELS
# =============================================================================

class FacebookPageLocation(BaseModel):
    street: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    zip_code: Optional[str] = None

class FacebookPageCategory(BaseModel):
    id: str
    name: str

class FacebookPage(BaseModel):
    id: str
    name: str
    category: str
    category_list: List[FacebookPageCategory]
    about: str
    description: str
    website: str
    phone: str
    location: FacebookPageLocation
    fan_count: int
    followers_count: int
    engagement: Dict[str, Any]
    picture_url: str
    access_token: str
    has_insights_access: bool

class FacebookPageInsights(BaseModel):
    page_id: str
    page_name: str
    period: str
    date_range: str
    # Fan metrics
    total_fans: int
    new_fans: int
    lost_fans: int
    net_fan_change: int
    fan_growth_rate: float
    # Reach and impressions
    total_reach: int
    total_impressions: int
    frequency: float
    # Engagement
    engaged_users: int
    post_engagements: int
    engagement_rate: float
    # Content performance
    video_views: int
    page_views: int
    post_reach: int
    generated_at: str

class FacebookPost(BaseModel):
    id: str
    message: str
    story: str
    created_time: str
    type: str
    status_type: str
    link: str
    picture: str
    full_picture: str
    permalink_url: str
    # Engagement metrics
    likes_count: int
    comments_count: int
    reactions_count: int
    shares_count: int
    total_engagement: int

class FacebookPostInsights(BaseModel):
    post_id: str
    impressions: int
    reach: int
    engaged_users: int
    clicks: int
    reactions_breakdown: Dict[str, int]
    generated_at: str

class FacebookAudienceInsights(BaseModel):
    page_id: str
    period: str
    date_range: str
    # Fan demographics
    fans_by_age_gender: Dict[str, Any]
    fans_by_country: Dict[str, Any]
    fans_by_city: Dict[str, Any]
    # Reach demographics
    reach_by_age_gender: Dict[str, Any]
    reach_by_country: Dict[str, Any]
    generated_at: str

class FacebookPagePerformanceSummary(BaseModel):
    page_id: str
    page_name: str
    page_category: str
    followers_count: int
    period: str
    # Key metrics from insights
    total_reach: int
    total_impressions: int
    engaged_users: int
    engagement_rate: float
    new_fans: int
    net_fan_change: int
    # Post performance
    total_posts: int
    total_post_engagement: int
    avg_engagement_per_post: float
    top_performing_post: Optional[FacebookPost] = None
    generated_at: str

# =============================================================================
# INSTAGRAM MODELS
# =============================================================================

class ConnectedFacebookPage(BaseModel):
    id: str
    name: str

class InstagramAccount(BaseModel):
    id: str
    username: str
    name: str
    biography: str
    website: str
    followers_count: int
    follows_count: int
    media_count: int
    profile_picture_url: str
    account_type: str
    connected_facebook_page: Optional[ConnectedFacebookPage] = None

class InstagramAccountInsights(BaseModel):
    account_id: str
    username: str
    period: str
    date_range: str
    # Core metrics
    impressions: int
    reach: int
    profile_views: int
    website_clicks: int
    follower_count: int
    # Calculated metrics
    engagement_rate: float
    reach_rate: float
    generated_at: str

class InstagramMedia(BaseModel):
    id: str
    caption: str
    media_type: str
    media_url: str
    permalink: str
    thumbnail_url: str
    timestamp: str
    username: str
    # Engagement metrics
    like_count: int
    comments_count: int
    # Insights metrics
    impressions: int
    reach: int
    engagement: int
    saved: int
    video_views: int
    # Calculated metrics
    total_engagement: int
    engagement_rate: float

class InstagramStory(BaseModel):
    id: str
    media_type: str
    media_url: str
    permalink: str
    thumbnail_url: str
    timestamp: str
    # Story insights
    impressions: int
    reach: int
    replies: int
    exits: int
    taps_forward: int
    taps_back: int
    # Calculated metrics
    completion_rate: float

class InstagramAudienceDemographics(BaseModel):
    account_id: str
    period: str
    # Demographic breakdowns
    audience_by_gender_age: Dict[str, Any]
    audience_by_country: Dict[str, Any]
    audience_by_city: Dict[str, Any]
    generated_at: str

class InstagramHashtagPerformance(BaseModel):
    hashtag: str
    posts_count: int
    total_engagement: int
    avg_engagement_per_post: float
    total_reach: int
    engagement_rate: float

class InstagramAccountPerformanceSummary(BaseModel):
    account_id: str
    username: str
    period: str
    # Account metrics
    followers_count: int
    following_count: int
    media_count: int
    # Performance metrics
    total_impressions: int
    total_reach: int
    profile_views: int
    website_clicks: int
    # Content performance
    posts_in_period: int
    total_media_engagement: int
    avg_engagement_per_post: float
    overall_engagement_rate: float
    best_performing_post: Optional[InstagramMedia] = None
    # Calculated ratios
    follower_growth_rate: float
    reach_rate: float
    generated_at: str

# =============================================================================
# COMBINED SOCIAL MEDIA MODELS
# =============================================================================

class SocialMediaOverview(BaseModel):
    facebook_pages: Optional[List[FacebookPage]] = None
    instagram_accounts: Optional[List[InstagramAccount]] = None
    meta_ad_accounts: Optional[List[MetaAdAccount]] = None
    total_followers: int
    total_reach: int
    total_engagement: int
    generated_at: str

class CrossPlatformEngagement(BaseModel):
    platform: str
    followers: int
    engagement_rate: float
    total_engagement: int
    reach: int
    impressions: int

class SocialMediaInsightsSummary(BaseModel):
    period: str
    platforms: List[CrossPlatformEngagement]
    top_performing_platform: str
    overall_engagement_rate: float
    total_social_reach: int
    total_social_engagement: int
    generated_at: str

# =============================================================================
# AUTH AND USER MODELS
# =============================================================================

class FacebookUserInfo(BaseModel):
    id: str
    email: str
    name: str
    picture: Optional[str] = None

class FacebookAuthResponse(BaseModel):
    token: str
    user: FacebookUserInfo
    auth_provider: str

# =============================================================================
# ERROR MODELS
# =============================================================================

class MetaAPIError(BaseModel):
    error: str
    error_code: Optional[int] = None
    error_subcode: Optional[int] = None
    error_user_title: Optional[str] = None
    error_user_msg: Optional[str] = None
    fbtrace_id: Optional[str] = None

class SocialMediaError(BaseModel):
    platform: str
    error: str
    detail: Optional[str] = None
    status_code: int