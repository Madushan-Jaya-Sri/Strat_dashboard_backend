"""
Response models for Meta Insights endpoints
"""

from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime

# =============================================================================
# AD ACCOUNTS
# =============================================================================

class MetaAdAccount(BaseModel):
    id: str
    account_id: str
    name: str
    status: str
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
    has_instagram: bool = False
    instagram_account: Optional[dict] = None

class FacebookPageInsights(BaseModel):
    impressions: int
    unique_impressions: int
    engaged_users: int
    post_engagements: int
    fans: int
    page_views: int

class FacebookPostDetail(BaseModel):
    id: str
    message: str
    created_time: str
    type: str
    likes: int
    comments: int
    shares: int
    impressions: int = 0
    engaged_users: int = 0
    clicks: int = 0

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
    impressions: int
    reach: int
    profile_views: int
    website_clicks: int

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