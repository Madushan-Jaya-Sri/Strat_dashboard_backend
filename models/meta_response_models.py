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

class MetaAdInsightsDay(BaseModel):
    date: str
    spend: float
    impressions: int
    clicks: int
    conversions: int
    cpc: float
    cpm: float
    ctr: float
    reach: int
    frequency: float

class MetaAdAccountInsightsTimeseries(BaseModel):
    timeseries: List[MetaAdInsightsDay]
    summary: MetaAdAccountInsights
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

from pydantic import BaseModel
from typing import List, Optional, Dict, Any

class MetricDay(BaseModel):
    date: str
    spend: float
    impressions: int
    clicks: int
    conversions: int
    cpc: float
    cpm: float
    ctr: float
    reach: int
    frequency: float

class MetricSummary(BaseModel):
    spend: float
    impressions: int
    clicks: int
    conversions: int
    cpc: float
    cpm: float
    ctr: float
    reach: int
    frequency: float

class TargetingSummary(BaseModel):
    locations: Dict[str, Any]
    age_min: Optional[int]
    age_max: Optional[int]
    genders: List[int]

class MetaAdSetTimeseries(BaseModel):
    id: str
    name: str
    campaign_id: str
    status: str
    optimization_goal: Optional[str]
    billing_event: Optional[str]
    daily_budget: Optional[float]
    lifetime_budget: Optional[float]
    budget_remaining: Optional[float]
    targeting_summary: TargetingSummary
    created_time: str
    updated_time: str
    timeseries: List[MetricDay]
    summary: MetricSummary

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


class AdMetricDay(BaseModel):
    date: str
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

class AdMetricSummary(BaseModel):
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

class CreativeDetails(BaseModel):
    title: Optional[str]
    body: Optional[str]
    image_url: Optional[str]
    video_id: Optional[str]
    thumbnail_url: Optional[str]

class MetaAdTimeseries(BaseModel):
    id: str
    name: str
    ad_set_id: str
    status: str
    creative: CreativeDetails
    created_time: str
    updated_time: str
    timeseries: List[AdMetricDay]
    summary: AdMetricSummary

# models/meta_response_models.py

from pydantic import BaseModel
from typing import List, Optional

# Campaign Models
class CampaignMetrics(BaseModel):
    campaign_id: str
    campaign_name: str
    status: str
    spend: float
    impressions: int
    clicks: int
    conversions: int
    cpc: float
    cpm: float
    ctr: float
    reach: int
    frequency: float

class CampaignTotals(BaseModel):
    total_spend: float
    total_impressions: int
    total_clicks: int
    total_conversions: int
    total_reach: int

class CampaignsWithTotals(BaseModel):
    campaigns: List[CampaignMetrics]
    totals: CampaignTotals

class DailyMetrics(BaseModel):
    date: str
    spend: float
    impressions: int
    clicks: int
    conversions: int
    cpc: float
    cpm: float
    ctr: float
    reach: int
    frequency: float

class CampaignTimeseries(BaseModel):
    campaign_id: str
    timeseries: List[DailyMetrics]

class DemographicItem(BaseModel):
    age: str
    gender: str
    spend: float
    impressions: int
    reach: int
    results: int

class CampaignDemographics(BaseModel):
    campaign_id: str
    demographics: List[DemographicItem]

class PlacementItem(BaseModel):
    platform: str
    spend: float
    impressions: int
    reach: int
    results: int

class CampaignPlacements(BaseModel):
    campaign_id: str
    placements: List[PlacementItem]

# Ad Set Models
class AdSetInfo(BaseModel):
    id: str
    name: str
    campaign_id: str
    status: str
    optimization_goal: Optional[str]
    billing_event: Optional[str]
    daily_budget: Optional[float]
    lifetime_budget: Optional[float]
    budget_remaining: Optional[float]
    locations: List[str]
    created_time: str
    updated_time: str

class AdSetTimeseries(BaseModel):
    adset_id: str
    timeseries: List[DailyMetrics]

class AdSetDemographics(BaseModel):
    adset_id: str
    demographics: List[DemographicItem]

class AdSetPlacements(BaseModel):
    adset_id: str
    placements: List[PlacementItem]

# Ad Models
class Creative(BaseModel):
    title: Optional[str]
    body: Optional[str]
    image_url: Optional[str]
    video_id: Optional[str]
    thumbnail_url: Optional[str]
    media_url: Optional[str] 

class AdInfo(BaseModel):
    id: str
    name: str
    ad_set_id: str
    status: str
    creative: Creative
    preview_link: Optional[str]  # Shareable preview link
    ads_manager_link: str  # Direct link to Ads Manager
    post_link: Optional[str]  # Direct link to Facebook post
    created_time: str
    updated_time: str

class AdTimeseries(BaseModel):
    ad_id: str
    timeseries: List[DailyMetrics]

class AdDemographics(BaseModel):
    ad_id: str
    demographics: List[DemographicItem]

class AdPlacements(BaseModel):
    ad_id: str
    placements: List[PlacementItem]


class CampaignBasic(BaseModel):
    id: str
    name: str
    status: str
    objective: Optional[str] = None
    created_time: Optional[str] = None
    updated_time: Optional[str] = None
    start_time: Optional[str] = None
    stop_time: Optional[str] = None

class CampaignsList(BaseModel):
    account_id: str
    total_campaigns: int
    status_summary: dict
    campaigns: List[CampaignBasic]

class CampaignMetadata(BaseModel):
    total_campaigns: int
    campaigns_with_data: int
    campaigns_without_data: int
    date_range: dict

class CampaignsWithTotalsOptimized(BaseModel):
    campaigns: List[dict]
    totals: dict
    metadata: CampaignMetadata


class BatchCampaignsRequest(BaseModel):
    account_ids: List[str]
    period: Optional[str] = "30d"
    max_workers: Optional[int] = 10
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


from pydantic import BaseModel
from typing import List

class PageInsightsDay(BaseModel):
    date: str
    impressions: int
    unique_impressions: int
    post_engagements: int
    engaged_users: int
    page_views: int
    new_likes: int
    fans: int

class PageInsightsSummary(BaseModel):
    impressions: int
    unique_impressions: int
    engaged_users: int
    post_engagements: int
    fans: int
    followers: int
    page_views: int
    new_likes: int
    talking_about_count: int
    checkins: int

class FacebookPageInsightsTimeseries(BaseModel):
    timeseries: List[PageInsightsDay]
    summary: PageInsightsSummary

from pydantic import BaseModel
from typing import List, Optional, Dict, Any

class PostInsightsDay(BaseModel):
    date: str
    impressions: int
    impressions_unique: int
    impressions_paid: int
    impressions_organic: int
    reach: int
    engaged_users: int
    clicks: int
    clicks_unique: int
    negative_feedback: int
    video_views: Optional[int] = 0
    video_views_10s: Optional[int] = 0
    video_avg_time_watched: Optional[float] = 0
    video_complete_views: Optional[int] = 0

class PostInsightsSummary(BaseModel):
    impressions: int
    impressions_unique: int
    impressions_paid: int
    impressions_organic: int
    reach: int
    engaged_users: int
    clicks: int
    clicks_unique: int
    negative_feedback: int
    video_views: int
    video_views_10s: int
    video_avg_time_watched: float
    video_complete_views: int

class FacebookPostTimeseries(BaseModel):
    id: str
    message: str
    story: str
    created_time: str
    status_type: Optional[str]
    type: str
    full_picture: Optional[str]
    attachment_type: Optional[str]
    attachment_title: Optional[str]
    attachment_description: Optional[str]
    permalink_url: str
    
    # Current engagement totals
    reactions: int
    reactions_breakdown: Dict[str, Any]
    likes: int
    comments: int
    shares: int
    total_engagement: int
    engagement_rate: float
    
    # Time-series data
    timeseries: List[PostInsightsDay]
    summary: PostInsightsSummary
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
    media_product_type: str = "FEED"
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
    insights_available: bool = False

from pydantic import BaseModel
from typing import List

class InstagramInsightsDay(BaseModel):
    date: str
    reach: int
    profile_views: int
    website_clicks: int
    accounts_engaged: int
    total_interactions: int

class InstagramInsightsSummary(BaseModel):
    reach: int
    profile_views: int
    website_clicks: int
    followers_count: int
    accounts_engaged: int
    total_interactions: int
    media_count: int

class InstagramAccountInsightsTimeseries(BaseModel):
    timeseries: List[InstagramInsightsDay]
    summary: InstagramInsightsSummary


from pydantic import BaseModel
from typing import List, Optional

class InstagramMediaInsightsDay(BaseModel):
    date: str
    impressions: int
    reach: int
    engagement: int
    saved: int

class InstagramMediaInsightsSummary(BaseModel):
    impressions: int
    reach: int
    engagement: int
    saved: int

class InstagramMediaTimeseries(BaseModel):
    id: str
    caption: str
    media_type: str
    media_product_type: str
    media_url: Optional[str]
    thumbnail_url: Optional[str]
    permalink: str
    timestamp: str
    like_count: int
    comments_count: int
    insights_available: bool
    timeseries: List[InstagramMediaInsightsDay]
    summary: InstagramMediaInsightsSummary
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