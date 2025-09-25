"""
Response Models for the Unified Marketing Dashboard API
"""

from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime

# Authentication Models
class UserInfo(BaseModel):
    email: str
    name: str
    picture: str

# Google Ads Models
class AdCustomer(BaseModel):
    id: str
    name: str
    currency: str
    time_zone: str
    is_manager: bool
    resource_name: str

class KeyStatMetric(BaseModel):
    value: float
    formatted: str
    label: str
    description: str

class KeyStatsSummary(BaseModel):
    period: str
    customer_id: str
    campaigns_count: int
    generated_at: str

class StatusInfo(BaseModel):
    name: str
    label: str
    color: str

class TypeInfo(BaseModel):
    name: str
    label: str
    icon: str

class EnhancedAdCampaign(BaseModel):
    id: str
    name: str
    status: str
    status_code: str
    status_info: StatusInfo
    type: str
    type_code: str
    type_info: TypeInfo
    start_date: str
    end_date: Optional[str] = None
    impressions: int
    clicks: int
    cost: float
    conversions: float
    ctr: float
class AdKeyStats(BaseModel):
    total_impressions: KeyStatMetric
    total_cost: KeyStatMetric
    total_clicks: KeyStatMetric
    conversion_rate: KeyStatMetric
    total_conversions: KeyStatMetric
    avg_cost_per_click: KeyStatMetric
    cost_per_conversion: KeyStatMetric
    click_through_rate: KeyStatMetric
    summary: KeyStatsSummary

class AdCampaign(BaseModel):
    id: str
    name: str
    status: str
    type: str
    start_date: str
    end_date: Optional[str] = None
    impressions: int
    clicks: int
    cost: float
    conversions: float
    ctr: float

class AdKeyword(BaseModel):
    text: str
    clicks: int
    impressions: int
    cost: float
    ctr: float
    cpc: float

class KeywordResponse(BaseModel):
    keywords: List[AdKeyword]
    has_more: bool
    total: int
    offset: int
    limit: int

class PerformanceMetric(BaseModel):
    name: str
    value: str
    performance: str

class GeographicPerformance(BaseModel):
    location_name: str
    clicks: int
    impressions: int
    cost: float

class DevicePerformance(BaseModel):
    device: str
    device_info: dict
    clicks: int
    impressions: int
    cost: float

class TimePerformance(BaseModel):
    date: str
    clicks: int
    impressions: int
    cost: float

class KeywordIdea(BaseModel):
    keyword: str
    avg_monthly_searches: int
    competition: str
    competition_index: float
    low_top_of_page_bid: float
    high_top_of_page_bid: float

class KeywordIdeasRequest(BaseModel):
    keywords: List[str]
    location_id: int = 2840

class KeywordIdeasResponse(BaseModel):
    keyword_ideas: List[KeywordIdea]
    total: int
    keywords_searched: List[str]
    location_id: int

# Google Analytics Models
class GAProperty(BaseModel):
    propertyId: str
    displayName: str
    websiteUrl: Optional[str] = None







class AdSpendBreakdown(BaseModel):
    customer_id: str
    cost_original: float
    currency: str
    cost_usd: float

class CurrencyInfo(BaseModel):
    property_currency: str
    calculation_currency: str
    exchange_rates: Dict[str, float]
    ad_spend_breakdown: List[AdSpendBreakdown]

class GAEnhancedCombinedROASROIMetrics(BaseModel):
    propertyId: str
    propertyName: str
    adsCustomerIds: List[str]
    currency_info: CurrencyInfo
    
    # Original metrics (all in USD)
    totalRevenue: float
    totalRevenueOriginal: float  # Revenue in original currency
    adSpend: float  # Total ad spend in USD
    roas: float
    roi: float
    conversionValue: float
    conversionValueOriginal: float  # Conversion value in original currency
    costPerConversion: float
    revenuePerUser: float
    profitMargin: float
    roasStatus: str
    roiStatus: str
    conversions: int
    sessions: int
    totalUsers: int
    
    # New ecommerce metrics
    totalAdRevenue: float
    totalAdRevenueOriginal: float  # Ad revenue in original currency
    totalPurchasers: int
    firstTimePurchasers: int
    averagePurchaseRevenuePerActiveUser: float
    activeUsers: int











class GAMetrics(BaseModel):
    propertyId: str
    propertyName: str
    # Original 7 GA4 metrics (main card values)
    totalUsers: int
    sessions: int
    engagedSessions: int
    engagementRate: float
    averageSessionDuration: float
    bounceRate: float
    pagesPerSession: float
    # Additional 9 calculated insights (secondary card values)
    totalUsersChange: str
    sessionsPerUser: float
    engagedSessionsPercentage: str
    engagementRateStatus: str
    sessionDurationQuality: str
    bounceRateStatus: str
    contentDepthStatus: str
    # Extra metrics for 8th card
    viewsPerSession: float
    sessionQualityScore: str

class GATrafficSource(BaseModel):
    channel: str
    sessions: int
    users: int
    percentage: float

class GAPageData(BaseModel):
    title: str
    path: str
    pageViews: int
    uniquePageViews: int
    avgTimeOnPage: float
    bounceRate: float

class GAConversionData(BaseModel):
    eventName: str
    conversions: int
    conversionRate: float
    conversionValue: float
    eventCount: int
    eventCountRate: float

class GAChannelPerformance(BaseModel):
    channel: str
    users: int
    sessions: int
    bounceRate: float
    avgSessionDuration: float
    conversionRate: float
    revenue: float

class GAAudienceInsight(BaseModel):
    dimension: str
    value: str
    users: int
    percentage: float
    engagementRate: float

class GATimeSeriesData(BaseModel):
    date: str
    metric: str
    value: float

class GATrendData(BaseModel):
    date: str
    newUsers: int
    returningUsers: int
    sessions: int

# Geographic Models (shared between Ads and Analytics)
class GeographicData(BaseModel):
    country: str
    users: int
    sessions: int
    engagementRate: float
    avgSessionDuration: float

class DemographicData(BaseModel):
    ageGroup: str
    users: int
    percentage: float

class DeviceData(BaseModel):
    deviceCategory: str
    users: int
    sessions: int
    percentage: float

# Combined Dashboard Models
class CombinedOverview(BaseModel):
    ads: Optional[dict] = None
    analytics: Optional[dict] = None

class GACombinedROASROIMetrics(BaseModel):
    propertyId: str
    propertyName: str
    adsCustomerId: str
    # Original metrics
    totalRevenue: float
    adSpend: float  # Real ad spend from Google Ads
    roas: float
    roi: float
    conversionValue: float
    costPerConversion: float
    revenuePerUser: float
    profitMargin: float
    roasStatus: str
    roiStatus: str
    conversions: int
    sessions: int
    totalUsers: int
    # New ecommerce metrics
    totalAdRevenue: float
    totalPurchasers: int
    firstTimePurchasers: int
    averagePurchaseRevenuePerActiveUser: float
    activeUsers: int

    
class GAROASROITimeSeriesData(BaseModel):
    date: str
    revenue: float
    adSpend: float
    roas: float
    roi: float
    conversions: float
    sessions: int

# Intent Insights Models - UPDATED
class KeywordInsightRequest(BaseModel):
    seed_keywords: List[str]  # Max 10 keywords
    country: str = "Sri Lanka"
    timeframe: str = "12_months"  # "1_month", "3_months", "12_months", "custom"
    start_date: Optional[str] = None  # Required for custom timeframe
    end_date: Optional[str] = None    # Required for custom timeframe

class KeywordMetrics(BaseModel):
    """Model for individual keyword metrics with simplified structure"""
    keyword: str
    avg_monthly_searches: int
    competition: str
    competition_index: float
    low_top_of_page_bid: float
    high_top_of_page_bid: float
    yoy_change: float
    three_month_change: float
    monthly_volumes: Dict[str, int]
    seasonality: Dict[str, Any]  # Changed to Any to handle mixed types from calculate_seasonality_index

class KeywordInsightsResponse(BaseModel):
    """Response model for keyword insights with separated seed and related keywords"""
    seed_keywords: List[KeywordMetrics]
    related_keywords: List[KeywordMetrics]
    country: str
    location_id: str
    timeframe: str
    date_range: str
    month_labels: List[str]
    search_volumes: Dict[str, Dict[str, int]]
    total_seed_keywords: int
    total_related_keywords: int
    generated_at: str

class IntentAnalysisResponse(BaseModel):
    informational: List[str]
    commercial: List[str]
    transactional: List[str]
    navigational: List[str]

# Error Models
class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None
    status_code: int

# Add this to your models/response_models.py

class RawKeywordMetrics(BaseModel):
    avg_monthly_searches: int
    competition: str
    competition_index: float
    low_top_of_page_bid_micros: int
    high_top_of_page_bid_micros: int
    monthly_search_volumes: Optional[List[dict]] = []

class RawKeywordResult(BaseModel):
    keyword_text: str
    metrics: Optional[RawKeywordMetrics] = None

class RawKeywordIdeas(BaseModel):
    total_results: int
    results: List[RawKeywordResult]

class RawHistoricalResult(BaseModel):
    keyword_text: str
    keyword_metrics: Optional[RawKeywordMetrics] = None

class RawHistoricalMetrics(BaseModel):
    date_range: str
    total_results: int
    results: List[RawHistoricalResult]

class RequestInfo(BaseModel):
    customer_id: str
    seed_keywords: List[str]
    country: str
    location_id: str
    timeframe: str
    date_range: str
    include_zero_volume: bool
    generated_at: str

class RawKeywordInsightsResponse(BaseModel):
    request_info: RequestInfo
    keyword_ideas_raw: RawKeywordIdeas
    historical_metrics_raw: RawHistoricalMetrics



class ChannelRevenue(BaseModel):
    channel: str
    totalRevenue: float
    totalRevenueUSD: Optional[float] = None
    purchaseRevenue: float
    purchaseRevenueUSD: Optional[float] = None
    sessions: int
    users: int
    conversions: int
    purchasers: int
    revenuePerSession: float
    conversionRate: float
    revenuePercentage: float

class SourceRevenue(BaseModel):
    source: str
    medium: str
    sourceMedium: str
    totalRevenue: float
    totalRevenueUSD: Optional[float] = None
    purchaseRevenue: float
    purchaseRevenueUSD: Optional[float] = None
    sessions: int
    conversions: int
    revenuePercentage: float

class DeviceRevenue(BaseModel):
    device: str
    totalRevenue: float
    totalRevenueUSD: Optional[float] = None
    purchaseRevenue: float
    purchaseRevenueUSD: Optional[float] = None
    sessions: int
    conversions: int
    users: int
    revenuePercentage: float

class LocationRevenue(BaseModel):
    country: str
    city: str
    location: str
    totalRevenue: float
    totalRevenueUSD: Optional[float] = None
    purchaseRevenue: float
    purchaseRevenueUSD: Optional[float] = None
    sessions: int
    users: int
    revenuePercentage: float

class PageRevenue(BaseModel):
    landingPage: str
    pageTitle: str
    totalRevenue: float
    totalRevenueUSD: Optional[float] = None
    purchaseRevenue: float
    purchaseRevenueUSD: Optional[float] = None
    sessions: int
    conversions: int
    revenuePercentage: float

class ChannelRevenueBreakdown(BaseModel):
    channels: List[ChannelRevenue]
    totalRevenue: float
    totalRevenueUSD: Optional[float] = None
    totalChannels: int

class SourceRevenueBreakdown(BaseModel):
    sources: List[SourceRevenue]
    totalRevenue: float
    totalRevenueUSD: Optional[float] = None
    totalSources: int

class DeviceRevenueBreakdown(BaseModel):
    devices: List[DeviceRevenue]
    totalRevenue: float
    totalRevenueUSD: Optional[float] = None
    totalDevices: int

class LocationRevenueBreakdown(BaseModel):
    locations: List[LocationRevenue]
    totalRevenue: float
    totalRevenueUSD: Optional[float] = None
    totalLocations: int

class PageRevenueBreakdown(BaseModel):
    pages: List[PageRevenue]
    totalRevenue: float
    totalRevenueUSD: Optional[float] = None
    totalPages: int

class RevenueBreakdownSummary(BaseModel):
    total_channels: int
    total_sources: int
    total_devices: int
    total_locations: int
    total_pages: int

class CurrencyInfo(BaseModel):
    original_currency: str
    exchange_rates: Dict[str, float]

class ComprehensiveRevenueBreakdown(BaseModel):
    propertyId: str
    period: str
    currency_info: CurrencyInfo
    breakdown_by_channel: ChannelRevenueBreakdown
    breakdown_by_source: SourceRevenueBreakdown
    breakdown_by_device: DeviceRevenueBreakdown
    breakdown_by_location: LocationRevenueBreakdown
    breakdown_by_page: PageRevenueBreakdown
    summary: RevenueBreakdownSummary


class ChannelDayData(BaseModel):
    channel: str
    totalRevenue: float
    totalRevenueUSD: float
    purchaseRevenue: float
    purchaseRevenueUSD: float
    sessions: int
    users: int
    conversions: int
    revenuePercentage: float

class DayTimeSeriesData(BaseModel):
    date: str
    channels: Dict[str, ChannelDayData]
    total_revenue: float
    total_revenue_usd: float
    total_sessions: int
    total_users: int
    total_conversions: int

class ChannelSummary(BaseModel):
    channel: str
    totalRevenue: float
    totalRevenueUSD: float
    totalSessions: int
    totalUsers: int
    totalConversions: int
    days_active: int
    avgDailyRevenue: float
    avgDailyRevenueUSD: float
    avgDailySessions: float

class DateRangeInfo(BaseModel):
    start_date: str
    end_date: str
    total_days: int

class TimeSeriesTotal(BaseModel):
    total_revenue: float
    total_revenue_usd: float
    total_sessions: int
    total_users: int
    total_conversions: int

class ChannelRevenueTimeSeries(BaseModel):
    propertyId: str
    period: str
    currency_info: CurrencyInfo
    time_series: List[DayTimeSeriesData]
    channel_summary: List[ChannelSummary]
    channels_found: List[str]
    date_range: DateRangeInfo
    totals: TimeSeriesTotal

class SpecificChannelsTimeSeries(ChannelRevenueTimeSeries):
    channels_requested: List[str]
    channels_not_found: Optional[List[str]] = []

class FunnelRequest(BaseModel):
    selected_events: List[str]
    conversions_data: List[Dict[str, Any]]  # Raw conversion data from the initial endpoint


class GAAudienceInsight(BaseModel):
    dimension: str
    value: str
    latitude: float
    longitude: float
    users: int
    percentage: float
    engagementRate: float

class RevenueTimeSeries(BaseModel):
    propertyId: str
    period: str
    breakdown_by: str
    currency_info: Dict[str, Any]
    time_series: list
    group_summary: list
    groups_found: list
    date_range: Dict[str, Any]
    totals: Dict[str, Any]
    # error: str | None = None
    error: Optional[str] = None



"""
Response Models for Facebook and Instagram APIs
Clean Pydantic models for social media platform responses
"""

from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime

# =============================================================================
# FACEBOOK MODELS
# =============================================================================

class FacebookPage(BaseModel):
    id: str
    name: str
    category: str
    about: str
    website: str
    fan_count: int
    followers_count: int
    picture_url: str
    access_token: str
    has_insights_access: bool

class FacebookPageBasicStats(BaseModel):
    page_id: str
    page_name: str
    period: str
    date_range: str
    total_fans: int
    new_fans: int
    fan_growth_rate: float
    total_reach: int
    total_impressions: int
    engaged_users: int
    engagement_rate: float
    category: str
    website: str
    has_insights_access: bool
    generated_at: str
    error: Optional[str] = None
    message: Optional[str] = None

class FacebookPost(BaseModel):
    id: str
    message: str
    created_time: str
    type: str
    permalink_url: str
    likes_count: int
    comments_count: int
    shares_count: int
    total_engagement: int

class FacebookPageSummary(BaseModel):
    page_id: str
    page_name: str
    period: str
    total_fans: int
    new_fans: int
    fan_growth_rate: float
    total_reach: int
    engaged_users: int
    engagement_rate: float
    total_posts: int
    total_post_engagement: int
    avg_engagement_per_post: float
    top_performing_post: Optional[FacebookPost]
    category: str
    website: str
    has_insights_access: bool
    generated_at: str

# =============================================================================
# INSTAGRAM MODELS
# =============================================================================

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
    connected_facebook_page: Optional[Dict[str, str]] = None

class InstagramAccountBasicStats(BaseModel):
    account_id: str
    username: str
    name: str
    period: str
    date_range: str
    impressions: int
    reach: int
    profile_views: int
    website_clicks: int
    follower_count: int
    reach_rate: float
    followers_count: int
    follows_count: int
    media_count: int
    biography: str
    website: str
    generated_at: str
    error: Optional[str] = None
    message: Optional[str] = None

class InstagramMedia(BaseModel):
    id: str
    caption: str
    media_type: str
    media_url: str
    permalink: str
    thumbnail_url: str
    timestamp: str
    username: str
    like_count: int
    comments_count: int
    total_engagement: int
    impressions: int
    reach: int
    engagement_rate: float

class InstagramHashtag(BaseModel):
    hashtag: str
    posts_count: int
    avg_engagement_per_post: float
    avg_reach_per_post: float
    total_engagement: int

class InstagramAccountSummary(BaseModel):
    account_id: str
    username: str
    name: str
    period: str
    followers_count: int
    follows_count: int
    media_count: int
    total_impressions: int
    total_reach: int
    profile_views: int
    website_clicks: int
    reach_rate: float
    posts_in_period: int
    total_media_engagement: int
    avg_engagement_per_post: float
    overall_engagement_rate: float
    best_performing_post: Optional[InstagramMedia]
    biography: str
    website: str
    generated_at: str

# =============================================================================
# COMBINED SOCIAL MEDIA MODELS
# =============================================================================

class SocialMediaOverview(BaseModel):
    facebook_pages: Optional[List[FacebookPage]] = None
    instagram_accounts: Optional[List[InstagramAccount]] = None
    total_social_followers: int
    total_social_engagement: int
    generated_at: str

# =============================================================================
# ERROR MODELS
# =============================================================================

class SocialMediaError(BaseModel):
    platform: str
    error: str
    detail: Optional[str] = None
    status_code: int