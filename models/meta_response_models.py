"""
Response models for Meta Insights endpoints
"""

from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
from typing import Union

# =============================================================================
# AD ACCOUNTS
# =============================================================================

class MetaAdAccount(BaseModel):
    id: str
    account_id: str
    name: str
    status: Union[int, str]  # Allow both int and str
    currency: str
    timezone: Optional[str] = None
    amount_spent: float
    balance: float

class MetaAdAccountInsights(BaseModel):
    spend: float
    impressions: int
    clicks: int
    conversions: int
    cpc: float
    cpm: float
    ctr: float
    reach: int
    frequency: float

class MetaCampaign(BaseModel):
    id: str
    name: str
    objective: str
    status: str
    spend: float
    impressions: int
    clicks: int
    cpc: float
    cpm: float
    ctr: float
    created_time: str
    updated_time: str


class MetaAdSet(BaseModel):
    id: str
    name: str
    campaign_id: str
    status: str
    optimization_goal: Optional[str] = None
    billing_event: Optional[str] = None
    daily_budget: Optional[float] = None
    lifetime_budget: Optional[float] = None
    budget_remaining: Optional[float] = None
    targeting_summary: Optional[dict] = None
    spend: float
    impressions: int
    clicks: int
    conversions: int
    cpc: float
    cpm: float
    ctr: float
    reach: int
    frequency: float
    created_time: str
    updated_time: str

class MetaAdCreative(BaseModel):
    title: Optional[str] = None
    body: Optional[str] = None
    image_url: Optional[str] = None
    video_id: Optional[str] = None
    thumbnail_url: Optional[str] = None

class MetaAd(BaseModel):
    id: str
    name: str
    ad_set_id: str
    status: str
    creative: Optional[MetaAdCreative] = None
    spend: float
    impressions: int
    clicks: int
    link_clicks: int
    conversions: int
    cpc: float
    cpm: float
    ctr: float
    reach: int
    frequency: float
    cost_per_link_click: float
    created_time: str
    updated_time: str

# =============================================================================
# FACEBOOK PAGES
# =============================================================================

class FacebookPageBasic(BaseModel):
    id: str
    name: str
    category: Optional[str] = None
    fan_count: int = 0
    followers_count: int = 0
    link: Optional[str] = None
    about: Optional[str] = None
    description: Optional[str] = None
    phone: Optional[str] = None
    emails: Optional[List[str]] = []
    website: Optional[str] = None
    address: Optional[str] = None
    location: Optional[dict] = None
    has_instagram: bool = False
    instagram_account: Optional[dict] = None


class FacebookPageInsights(BaseModel):
    impressions: int
    unique_impressions: int
    engaged_users: int
    post_engagements: int
    fans: int
    followers: int
    page_views: int
    new_likes: int = 0
    talking_about_count: int = 0
    checkins: int = 0

class FacebookPostDetail(BaseModel):
    id: str
    message: str
    story: Optional[str] = None
    created_time: str
    status_type: Optional[str] = None
    type: str
    full_picture: Optional[str] = None
    attachment_type: Optional[str] = None
    attachment_title: Optional[str] = None
    attachment_description: Optional[str] = None
    permalink_url: Optional[str] = None
    
    # Engagement metrics
    reactions: int
    reactions_breakdown: Optional[dict] = {}
    likes: int
    comments: int
    shares: int
    total_engagement: int
    engagement_rate: float
    
    # Insights metrics
    impressions: int
    impressions_unique: int
    impressions_paid: int
    impressions_organic: int
    reach: int
    engaged_users: int
    clicks: int
    clicks_unique: int
    negative_feedback: int
    
    # Video metrics
    video_views: int = 0
    video_views_10s: int = 0
    video_avg_time_watched: float = 0
    video_complete_views: int = 0

# =============================================================================
# INSTAGRAM
# =============================================================================

class InstagramAccountBasic(BaseModel):
    id: str
    username: str
    name: Optional[str] = None
    profile_picture_url: Optional[str] = None
    followers_count: int = 0
    follows_count: int = 0
    media_count: int = 0
    connected_facebook_page: Optional[dict] = None

class InstagramAccountInsights(BaseModel):
    reach: int
    profile_views: int
    website_clicks: int
    followers_count: int
    accounts_engaged: int
    total_interactions: int
    media_count: int = 0

class InstagramMediaDetail(BaseModel):
    id: str
    caption: str
    media_type: str
    media_url: Optional[str] = None
    thumbnail_url: Optional[str] = None
    permalink: Optional[str] = None
    timestamp: str
    like_count: int = 0
    comments_count: int = 0
    impressions: int = 0
    reach: int = 0
    engagement: int = 0
    saved: int = 0

# =============================================================================
# COMBINED OVERVIEW
# =============================================================================

class MetaOverview(BaseModel):
    ad_accounts_count: int
    pages_count: int
    instagram_accounts_count: int
    total_ad_spend: float
    total_page_followers: int
    total_instagram_followers: int
    total_social_followers: int
    ad_accounts: List[MetaAdAccount]
    pages: List[FacebookPageBasic]
    instagram_accounts: List[InstagramAccountBasic]