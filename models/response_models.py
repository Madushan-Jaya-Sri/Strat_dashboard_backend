"""
Response Models for the Unified Marketing Dashboard API
"""

from pydantic import BaseModel
from typing import Optional, List
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

# class GAMetrics(BaseModel):
#     propertyId: str
#     propertyName: str
#     totalUsers: int
#     sessions: int
#     engagedSessions: int
#     engagementRate: float
#     averageSessionDuration: float
#     bounceRate: float
#     pagesPerSession: float

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

# Error Models
class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None
    status_code: int