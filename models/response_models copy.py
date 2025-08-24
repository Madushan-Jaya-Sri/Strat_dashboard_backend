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

class GAROASROIMetrics(BaseModel):
    propertyId: str
    propertyName: str
    # Original metrics
    totalRevenue: float
    adSpend: float
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

# Combined Dashboard Models
# Combined Dashboard Models
class CombinedOverview(BaseModel):
    ads: Optional[dict] = None
    analytics: Optional[dict] = None

# Intent Insights Models
class KeywordInsightRequest(BaseModel):
    seed_keywords: List[str]  # Max 10 keywords
    country: str
    timeframe: str  # "1_month", "3_months", "12_months", "custom"
    start_date: Optional[str] = None  # Required for custom timeframe
    end_date: Optional[str] = None    # Required for custom timeframe

class EnhancedKeywordInsight(BaseModel):
    keyword: str
    avg_monthly_searches: int
    competition: str
    competition_index: float
    low_top_of_page_bid: float
    high_top_of_page_bid: float
    yoy_change: str
    three_month_change: str
    trend_direction: str
    monthly_volumes: Dict[str, int]
    opportunity_score: int
    recommendation: str
    seasonality: Dict[str, Any]


class IntentAnalysisResponse(BaseModel):
    informational: List[str]
    commercial: List[str]
    transactional: List[str]
    navigational: List[str]

# Search Volume Segmentation Models - Summary Focused
class ComprehensiveDeviceSegmentation(BaseModel):
    summary: Dict[str, Any]  # Contains total_mobile, total_desktop, total_tablet, percentages
    data_source: str
    keywords_analyzed: int

class ComprehensiveLocationSegmentation(BaseModel):
    summary: Dict[str, Any]  # Contains region_totals, percentages, top_region, geographic_concentration
    country: str
    data_source: str
    keywords_analyzed: int

class ComprehensiveLanguageSegmentation(BaseModel):
    summary: Dict[str, Any]  # Contains language_totals, percentages, dominant_language, localization_opportunities
    data_source: str
    keywords_analyzed: int

class ComprehensiveNetworkSegmentation(BaseModel):
    summary: Dict[str, Any]  # Contains network_totals, google_search_share, partners_share, optimization_recommendation
    data_source: str
    keywords_analyzed: int
class SegmentationInsights(BaseModel):
    device_insights: Dict[str, Any]
    location_insights: Dict[str, Any]
    language_insights: Dict[str, Any]
    network_insights: Dict[str, Any]
    overall_insights: Dict[str, Any]

class SearchVolumeSegmentationResponse(BaseModel):
    seed_keywords: List[str]
    total_keywords_analyzed: int
    total_suggested_keywords: int
    country: str
    location_id: str
    timeframe: str
    segmentation: Dict[str, Any]  # Contains device, location, language, network summaries
    insights: SegmentationInsights
    generated_at: str
    analysis_scope: str

# Error Models
class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None
    status_code: int


class KeywordMetrics(BaseModel):
    """Model for individual keyword metrics"""
    keyword: str
    avg_monthly_searches: int
    competition: str
    competition_index: float
    low_top_of_page_bid: float
    high_top_of_page_bid: float
    yoy_change: float
    three_month_change: float
    monthly_volumes: Dict[str, int]
    # seasonality: Dict[str, float]
    seasonality: Dict[str, float]  

class KeywordInsightsResponse(BaseModel):
    """Updated response model matching the new structure"""
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

# If you have a request model, here it is:
class KeywordInsightsRequest(BaseModel):
    """Request model for keyword insights"""
    seed_keywords: List[str]
    country: str = "US"
    timeframe: str = "12_months"
    start_date: Optional[str] = None
    end_date: Optional[str] = None


    