#!/usr/bin/env python3
"""
Unified Marketing Dashboard Backend
Combines Google Ads and Google Analytics data in a single FastAPI application
"""

from fastapi import FastAPI, HTTPException, Query, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, JSONResponse, HTMLResponse, FileResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import uvicorn
import requests
import json
import logging
import asyncio
from datetime import datetime
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv

# Import our custom modules
from auth.auth_manager import AuthManager
from google_ads.ads_manager import GoogleAdsManager
from google_analytics.ga4_manager import GA4Manager
from intent_insights.intent_manager import IntentManager
from models.response_models import *
from models.meta_response_models import *
from models.response_models import AdKeyStats
from models.response_models import EnhancedAdCampaign, FunnelRequest
from utils.charts_helper import ChartsDataTransformer
from database.mongo_manager import MongoManager


from models.meta_response_models import (
    AccountInsightsSummary,
    PaginatedCampaignsResponse
)


from chat.chat_manager import chat_manager
from models.chat_models import *

mongo_manager = MongoManager()

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Load environment variables
load_dotenv(override=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Unified Marketing Dashboard API",
    description="Backend API combining Google Ads, Google Analytics, and Intent Insights",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize managers
auth_manager = AuthManager()
security = HTTPBearer()


from functools import wraps

def save_response(endpoint_name: str, cache_minutes: int = 0):
    """
    Decorator to save endpoint responses and optionally cache them
    
    Args:
        endpoint_name: Name of the endpoint for logging/collection naming
        cache_minutes: If > 0, try to return cached response instead of making API call
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            current_user = kwargs.get('current_user', {})
            user_email = current_user.get('email', 'unknown')
            customer_id = kwargs.get('customer_id')
            property_id = kwargs.get('property_id')
            request_params = {k: v for k, v in kwargs.items() if k != 'current_user'}
            
            # Try to get cached response if caching is enabled
            if cache_minutes > 0:
                try:
                    cached_response = await mongo_manager.get_cached_response(
                        endpoint=endpoint_name,
                        user_email=user_email,
                        request_params=request_params,
                        customer_id=customer_id,
                        property_id=property_id,
                        max_age_minutes=cache_minutes
                    )
                    
                    if cached_response:
                        logger.info(f"Returning cached response for {endpoint_name}")
                        return cached_response
                except Exception as e:
                    logger.warning(f"Cache lookup failed for {endpoint_name}: {e}")
            
            # Execute the original function
            response_data = await func(*args, **kwargs)
            
            # Save/update in MongoDB
            try:
                await mongo_manager.save_endpoint_response(
                    endpoint=endpoint_name,
                    user_email=user_email,
                    request_params=request_params,
                    response_data=response_data,
                    customer_id=customer_id,
                    property_id=property_id
                )
            except Exception as e:
                logger.warning(f"Failed to save response for {endpoint_name}: {e}")
            
            return response_data
        return wrapper
    return decorator

# Dependency to get current user
async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    """Get current authenticated user"""
    return auth_manager.verify_jwt_token(credentials.credentials)




# Authentication Routes
@app.get("/auth/login")
async def login():
    """Initiate Google OAuth login"""
    return await auth_manager.initiate_login()

@app.get("/auth/callback")
async def auth_callback(code: str, state: Optional[str] = None):
    try:
        result = await auth_manager.handle_callback(code, state)
        
        # Get frontend URL based on environment
        frontend_url = os.getenv("FRONTEND_URL", "http://localhost:5173")  # Default to dev URL
        
        # Redirect to frontend with token as URL parameter
        return RedirectResponse(url=f"{frontend_url}/?token={result['token']}")
        
    except Exception as e:
        # On error, redirect to frontend with error parameter
        frontend_url = os.getenv("FRONTEND_URL", "http://localhost:5173")
        error_message = str(e).replace(" ", "%20")  # URL encode spaces
        return RedirectResponse(url=f"{frontend_url}/?error={error_message}")

@app.get("/auth/user")
async def get_user_info(current_user: dict = Depends(get_current_user)):
    """Get current user information"""
    return UserInfo(
        email=current_user["email"],
        name=current_user["name"],
        picture=current_user["picture"]
    )

@app.post("/auth/logout")
async def logout(current_user: dict = Depends(get_current_user)):
    """Logout user"""
    return await auth_manager.logout_user(current_user["email"])


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    # Test MongoDB connection
    mongodb_status = "healthy"
    try:
        await mongo_manager.client.admin.command('ping')
    except Exception as e:
        mongodb_status = f"unhealthy: {str(e)}"
    
    return {
        "status": "healthy" if mongodb_status == "healthy" else "partial",
        "timestamp": datetime.now().isoformat(),
        "services": {
            "google_ads": "healthy",
            "google_analytics": "healthy", 
            "intent_insights": "healthy",
            "mongodb": mongodb_status
        },
        "auth_providers": {
            "google": "configured" if auth_manager.GOOGLE_CLIENT_ID else "not_configured"
        }
    }

# Add these endpoint functions to your main.py file
from fastapi.responses import HTMLResponse

@app.get("/privacy")
async def privacy_page():
    """Serve the privacy policy HTML page"""
    try:
        with open("privacy.html", "r", encoding="utf-8") as file:
            html_content = file.read()
        return HTMLResponse(content=html_content, status_code=200)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Privacy policy page not found")
    except Exception as e:
        logger.error(f"Error serving privacy page: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/terms")
async def terms_page():
    """Serve the terms of service HTML page"""
    try:
        with open("terms.html", "r", encoding="utf-8") as file:
            html_content = file.read()
        return HTMLResponse(content=html_content, status_code=200)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Terms of service page not found")
    except Exception as e:
        logger.error(f"Error serving terms page: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# Google Ads Routes
# Google Ads Routes with Custom Date Support
@app.get("/api/ads/customers", response_model=List[AdCustomer])
@save_response("ads_customers")
async def get_ads_customers(current_user: dict = Depends(get_current_user)):
    """Get accessible Google Ads customer accounts"""
    try:
        ads_manager = GoogleAdsManager(current_user["email"], auth_manager)
        customers = ads_manager.get_accessible_customers()
        return [AdCustomer(**customer) for customer in customers]
    except Exception as e:
        logger.error(f"Error fetching ads customers: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ads/key-stats/{customer_id}", response_model=AdKeyStats)
@save_response("ads_key_stats")
async def get_ads_key_stats(
    customer_id: str,
    period: str = Query("LAST_30_DAYS", pattern="^(LAST_7_DAYS|LAST_30_DAYS|LAST_90_DAYS|LAST_365_DAYS|CUSTOM)$"),
    start_date: Optional[str] = Query(None, pattern="^\d{4}-\d{2}-\d{2}$"),
    end_date: Optional[str] = Query(None, pattern="^\d{4}-\d{2}-\d{2}$"),
    current_user: dict = Depends(get_current_user)
):
    """Get Google Ads key statistics for dashboard cards"""
    try:
        if period == "CUSTOM" and (not start_date or not end_date):
            raise HTTPException(status_code=400, detail="start_date and end_date are required for CUSTOM period")
        
        ads_manager = GoogleAdsManager(current_user["email"], auth_manager)
        key_stats = ads_manager.get_overall_key_stats(customer_id, period, start_date, end_date)
        return AdKeyStats(**key_stats)
    except Exception as e:
        logger.error(f"Error fetching key stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ads/campaigns/{customer_id}", response_model=List[EnhancedAdCampaign])
@save_response("ads_campaigns")
async def get_ads_campaigns(
    customer_id: str,
    period: str = Query("LAST_30_DAYS", pattern="^(LAST_7_DAYS|LAST_30_DAYS|LAST_90_DAYS|LAST_365_DAYS|CUSTOM)$"),
    start_date: Optional[str] = Query(None, pattern="^\d{4}-\d{2}-\d{2}$"),
    end_date: Optional[str] = Query(None, pattern="^\d{4}-\d{2}-\d{2}$"),
    current_user: dict = Depends(get_current_user)
):
    """Get Google Ads campaigns for a customer"""
    try:
        if period == "CUSTOM" and (not start_date or not end_date):
            raise HTTPException(status_code=400, detail="start_date and end_date are required for CUSTOM period")
        
        ads_manager = GoogleAdsManager(current_user["email"], auth_manager)
        campaigns = ads_manager.get_campaigns_with_period(customer_id, period, start_date, end_date)
        return [EnhancedAdCampaign(**campaign) for campaign in campaigns]
    except Exception as e:
        logger.error(f"Error fetching campaigns: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ads/keywords/{customer_id}", response_model=KeywordResponse)
@save_response("ads_keywords")    
async def get_ads_keywords(
    customer_id: str,
    period: str = Query("LAST_30_DAYS", pattern="^(LAST_7_DAYS|LAST_30_DAYS|LAST_90_DAYS|LAST_365_DAYS|CUSTOM)$"),
    start_date: Optional[str] = Query(None, pattern="^\d{4}-\d{2}-\d{2}$"),
    end_date: Optional[str] = Query(None, pattern="^\d{4}-\d{2}-\d{2}$"),
    offset: int = Query(0, ge=0),
    limit: int = Query(10, ge=1, le=100),
    current_user: dict = Depends(get_current_user)
):
    """Get Google Ads keywords data with pagination"""
    try:
        if period == "CUSTOM" and (not start_date or not end_date):
            raise HTTPException(status_code=400, detail="start_date and end_date are required for CUSTOM period")
        
        ads_manager = GoogleAdsManager(current_user["email"], auth_manager)
        result = ads_manager.get_keywords_data(customer_id, period, start_date, end_date, offset, limit)
        return KeywordResponse(
            keywords=[AdKeyword(**kw) for kw in result["keywords"]],
            has_more=result["has_more"],
            total=result["total"],
            offset=offset,
            limit=limit
        )
    except Exception as e:
        logger.error(f"Error fetching keywords: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ads/performance/{customer_id}", response_model=List[PerformanceMetric])
@save_response("ads_performance")
async def get_ads_performance(
    customer_id: str,
    period: str = Query("LAST_30_DAYS", pattern="^(LAST_7_DAYS|LAST_30_DAYS|LAST_90_DAYS|LAST_365_DAYS|CUSTOM)$"),
    start_date: Optional[str] = Query(None, pattern="^\d{4}-\d{2}-\d{2}$"),
    end_date: Optional[str] = Query(None, pattern="^\d{4}-\d{2}-\d{2}$"),
    current_user: dict = Depends(get_current_user)
):
    """Get Google Ads performance metrics"""
    try:
        if period == "CUSTOM" and (not start_date or not end_date):
            raise HTTPException(status_code=400, detail="start_date and end_date are required for CUSTOM period")
        
        ads_manager = GoogleAdsManager(current_user["email"], auth_manager)
        metrics = ads_manager.get_advanced_metrics(customer_id, period, start_date, end_date)
        return [PerformanceMetric(**metric) for metric in metrics]
    except Exception as e:
        logger.error(f"Error fetching ads performance: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ads/geographic/{customer_id}", response_model=List[GeographicPerformance])
@save_response("ads_geographic_performance")
async def get_ads_geographic(
    customer_id: str,
    period: str = Query("LAST_30_DAYS", pattern="^(LAST_7_DAYS|LAST_30_DAYS|LAST_90_DAYS|LAST_365_DAYS|CUSTOM)$"),
    start_date: Optional[str] = Query(None, pattern="^\d{4}-\d{2}-\d{2}$"),
    end_date: Optional[str] = Query(None, pattern="^\d{4}-\d{2}-\d{2}$"),
    current_user: dict = Depends(get_current_user)
):
    """Get Google Ads geographic performance"""
    try:
        if period == "CUSTOM" and (not start_date or not end_date):
            raise HTTPException(status_code=400, detail="start_date and end_date are required for CUSTOM period")
        
        ads_manager = GoogleAdsManager(current_user["email"], auth_manager)
        geo_data = ads_manager.get_geographic_data(customer_id, period, start_date, end_date)
        return [GeographicPerformance(**geo) for geo in geo_data]
    except Exception as e:
        logger.error(f"Error fetching ads geographic data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ads/device-performance/{customer_id}", response_model=List[DevicePerformance])
@save_response("ads_device_performance")
async def get_ads_device_performance(
    customer_id: str,
    period: str = Query("LAST_30_DAYS", pattern="^(LAST_7_DAYS|LAST_30_DAYS|LAST_90_DAYS|LAST_365_DAYS|CUSTOM)$"),
    start_date: Optional[str] = Query(None, pattern="^\d{4}-\d{2}-\d{2}$"),
    end_date: Optional[str] = Query(None, pattern="^\d{4}-\d{2}-\d{2}$"),
    current_user: dict = Depends(get_current_user)
):
    """Get Google Ads device performance"""
    try:
        if period == "CUSTOM" and (not start_date or not end_date):
            raise HTTPException(status_code=400, detail="start_date and end_date are required for CUSTOM period")
        
        ads_manager = GoogleAdsManager(current_user["email"], auth_manager)
        device_data = ads_manager.get_device_performance_data(customer_id, period, start_date, end_date)
        return [DevicePerformance(**device) for device in device_data]
    except Exception as e:
        logger.error(f"Error fetching device performance: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ads/time-performance/{customer_id}", response_model=List[TimePerformance])
@save_response("ads_time_performance")
async def get_ads_time_performance(
    customer_id: str,
    period: str = Query("LAST_30_DAYS", pattern="^(LAST_7_DAYS|LAST_30_DAYS|LAST_90_DAYS|LAST_365_DAYS|CUSTOM)$"),
    start_date: Optional[str] = Query(None, pattern="^\d{4}-\d{2}-\d{2}$"),
    end_date: Optional[str] = Query(None, pattern="^\d{4}-\d{2}-\d{2}$"),
    current_user: dict = Depends(get_current_user)
):
    """Get Google Ads time-based performance"""
    try:
        if period == "CUSTOM" and (not start_date or not end_date):
            raise HTTPException(status_code=400, detail="start_date and end_date are required for CUSTOM period")
        
        ads_manager = GoogleAdsManager(current_user["email"], auth_manager)
        time_data = ads_manager.get_time_performance_data(customer_id, period, start_date, end_date)
        return [TimePerformance(**time) for time in time_data]
    except Exception as e:
        logger.error(f"Error fetching time performance: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/ads/keyword-ideas/{customer_id}", response_model=KeywordIdeasResponse)
@save_response("ads_keyword_ideas")
async def get_keyword_ideas(
    customer_id: str,
    request_data: KeywordIdeasRequest,
    current_user: dict = Depends(get_current_user)
):
    """Get keyword ideas and metrics"""
    try:
        ads_manager = GoogleAdsManager(current_user["email"], auth_manager)
        ideas = ads_manager.get_keyword_ideas(
            customer_id, 
            request_data.keywords, 
            str(request_data.location_id)
        )
        return KeywordIdeasResponse(
            keyword_ideas=[KeywordIdea(**idea) for idea in ideas],
            total=len(ideas),
            keywords_searched=request_data.keywords,
            location_id=request_data.location_id
        )
    except Exception as e:
        logger.error(f"Error fetching keyword ideas: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    

# Google Analytics Routes
@app.get("/api/analytics/properties", response_model=List[GAProperty])
@save_response("ga_properties")
async def get_ga_properties(current_user: dict = Depends(get_current_user)):
    """Get accessible GA4 properties"""
    try:
        ga4_manager = GA4Manager(current_user["email"])
        properties = ga4_manager.get_user_properties()
        return [GAProperty(**prop) for prop in properties]
    except Exception as e:
        logger.error(f"Error fetching GA properties: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/metrics/{property_id}", response_model=GAMetrics)
@save_response("ga_metrics")
async def get_ga_metrics(
    property_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get GA4 metrics for a property"""
    try:
        ga4_manager = GA4Manager(current_user["email"])
        metrics = ga4_manager.get_metrics(property_id, period)
        return GAMetrics(**metrics)
    except Exception as e:
        logger.error(f"Error fetching GA metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/traffic-sources/{property_id}", response_model=List[GATrafficSource])
@save_response("ga_traffic_sources", cache_minutes=10)
async def get_ga_traffic_sources(
    property_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get GA4 traffic sources"""
    try:
        ga4_manager = GA4Manager(current_user["email"])
        sources = ga4_manager.get_traffic_sources(property_id, period)
        return [GATrafficSource(**source) for source in sources]
    except Exception as e:
        logger.error(f"Error fetching traffic sources: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/top-pages/{property_id}", response_model=List[GAPageData])
@save_response("ga_top_pages")
async def get_ga_top_pages(
    property_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get GA4 top pages"""
    try:
        ga4_manager = GA4Manager(current_user["email"])
        pages = ga4_manager.get_top_pages(property_id, period)
        return [GAPageData(**page) for page in pages]
    except Exception as e:
        logger.error(f"Error fetching top pages: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/conversions/{property_id}", response_model=List[GAConversionData])
@save_response("ga_conversions", cache_minutes=15)
async def get_ga_conversions(
    property_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get GA4 conversion data"""
    try:
        ga4_manager = GA4Manager(current_user["email"])
        conversions = ga4_manager.get_conversions(property_id, period)
        return [GAConversionData(**conv) for conv in conversions]
    except Exception as e:
        logger.error(f"Error fetching conversions: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/analytics/funnel/{property_id}")
async def generate_engagement_funnel_with_llm(
    property_id: str,
    request: FunnelRequest,
    period: str = Query("30d", description="Time period"),
    current_user: dict = Depends(get_current_user)
):
    """Generate engagement funnel from selected event names"""
    try:
        ga4_manager = GA4Manager(current_user['email'])
        
        funnel_data = ga4_manager.generate_engagement_funnel_with_llm(
            property_id=property_id,
            selected_event_names=request.selected_events,
            conversions_raw_data=request.conversions_data,  # Fixed parameter name
            period=period
        )
        
        return {
            "success": True,
            "data": funnel_data
        }
        
    except Exception as e:
        logger.error(f"Error in funnel generation endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/channel-performance/{property_id}", response_model=List[GAChannelPerformance])
@save_response("ga_channel_performance")
async def get_ga_channel_performance(
    property_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get GA4 channel performance"""
    try:
        ga4_manager = GA4Manager(current_user["email"])
        channels = ga4_manager.get_channel_performance(property_id, period)
        return [GAChannelPerformance(**channel) for channel in channels]
    except Exception as e:
        logger.error(f"Error fetching channel performance: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/audience-insights/{property_id}", response_model=List[GAAudienceInsight])
@save_response("ga_audience_insights")
async def get_ga_audience_insights(
    property_id: str,
    dimension: str = Query("city", pattern="^(city|userAgeBracket|userGender|deviceCategory|browser)$"),
    period: str = Query("30d", pattern="^(7d|30d|90d|365d|12m)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get GA4 audience insights"""
    try:
        ga4_manager = GA4Manager(current_user["email"])
        insights = ga4_manager.get_audience_insights(property_id, dimension, period)
        return [GAAudienceInsight(**insight) for insight in insights]
    except Exception as e:
        logger.error(f"Error fetching audience insights: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/time-series/{property_id}", response_model=List[GATimeSeriesData])
@save_response("ga_time_series")
async def get_ga_time_series(
    property_id: str,
    metric: str = Query("totalUsers", pattern="^(totalUsers|sessions|conversions|totalRevenue)$"),
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get GA4 time series data"""
    try:
        ga4_manager = GA4Manager(current_user["email"])
        time_series = ga4_manager.get_time_series(property_id, metric, period)
        return [GATimeSeriesData(**ts) for ts in time_series]
    except Exception as e:
        logger.error(f"Error fetching time series: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/trends/{property_id}", response_model=List[GATrendData])
@save_response("ga_trends")
async def get_ga_trends(
    property_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get GA4 trend data"""
    try:
        ga4_manager = GA4Manager(current_user["email"])
        trends = ga4_manager.get_trends(property_id, period)
        return [GATrendData(**trend) for trend in trends]
    except Exception as e:
        logger.error(f"Error fetching trends: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/roas-roi-time-series/{property_id}", response_model=List[GAROASROITimeSeriesData])
@save_response("ga_roas_roi_time_series")
async def get_ga_roas_roi_time_series(
    property_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get GA4 ROAS and ROI time series data"""
    try:
        ga4_manager = GA4Manager(current_user["email"])
        time_series = ga4_manager.get_roas_roi_time_series(property_id, period)
        return [GAROASROITimeSeriesData(**ts) for ts in time_series]
    except Exception as e:
        logger.error(f"Error fetching ROAS/ROI time series: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/combined/overview")
@save_response("combined_overview")
async def get_combined_overview(
    ads_customer_id: Optional[str] = Query(None),
    ga_property_id: Optional[str] = Query(None),
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get combined overview from both Google Ads and Analytics"""
    try:
        overview = {}
        
        if ads_customer_id:
            ads_manager = GoogleAdsManager(current_user["email"], auth_manager)
            ads_period = "LAST_30_DAYS" if period == "30d" else f"LAST_{period[:-1]}_DAYS"
            ads_campaigns = ads_manager.get_campaigns_with_period(ads_customer_id, ads_period)
            overview["ads"] = {
                "total_campaigns": len(ads_campaigns),
                "total_cost": sum(c.get("cost", 0) for c in ads_campaigns),
                "total_clicks": sum(c.get("clicks", 0) for c in ads_campaigns),
                "total_impressions": sum(c.get("impressions", 0) for c in ads_campaigns)
            }
        
        if ga_property_id:
            ga4_manager = GA4Manager(current_user["email"])
            ga_metrics = ga4_manager.get_metrics(ga_property_id, period)
            overview["analytics"] = {
                "total_users": ga_metrics.get("totalUsers", 0),
                "sessions": ga_metrics.get("sessions", 0),
                "engagement_rate": ga_metrics.get("engagementRate", 0),
                "bounce_rate": ga_metrics.get("bounceRate", 0)
            }
        
        return overview
    except Exception as e:
        logger.error(f"Error fetching combined overview: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/combined/roas-roi-metrics", response_model=GAEnhancedCombinedROASROIMetrics)
@save_response("combined_roas_roi_metrics")
async def get_enhanced_combined_roas_roi_metrics(
    ga_property_id: str = Query(..., description="GA4 Property ID"),
    ads_customer_ids: str = Query(..., description="Comma-separated Google Ads Customer IDs"),
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get enhanced combined ROAS and ROI metrics from GA4 and multiple Google Ads accounts with proper currency handling"""
    try:
        # Parse comma-separated customer IDs
        customer_ids_list = [cid.strip() for cid in ads_customer_ids.split(",") if cid.strip()]
        
        if not customer_ids_list:
            raise HTTPException(status_code=400, detail="At least one Google Ads customer ID is required")
        
        if len(customer_ids_list) > 10:  # Reasonable limit
            raise HTTPException(status_code=400, detail="Maximum 10 Google Ads customer IDs allowed")
        
        ga4_manager = GA4Manager(current_user["email"])
        metrics = ga4_manager.get_enhanced_combined_roas_roi_metrics(
            ga_property_id, 
            customer_ids_list, 
            period
        )
        return GAEnhancedCombinedROASROIMetrics(**metrics)
        
    except Exception as e:
        logger.error(f"Error fetching enhanced combined ROAS/ROI metrics: {e}")
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))

# Optional: Keep the old endpoint for backward compatibility
@app.get("/api/combined/roas-roi-metrics-legacy", response_model=GACombinedROASROIMetrics)
@save_response("combined_roas_roi_metrics_legacy")
async def get_combined_roas_roi_metrics_legacy(
    ga_property_id: str = Query(..., description="GA4 Property ID"),
    ads_customer_id: str = Query(..., description="Google Ads Customer ID"),
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_current_user)
):
    """Legacy endpoint - Get combined ROAS and ROI metrics from GA4 and Google Ads (single customer)"""
    try:
        ga4_manager = GA4Manager(current_user["email"])
        metrics = ga4_manager.get_combined_roas_roi_metrics(ga_property_id, ads_customer_id, period)
        return GACombinedROASROIMetrics(**metrics)
    except Exception as e:
        logger.error(f"Error fetching combined ROAS/ROI metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/revenue-breakdown/channel/{property_id}", response_model=ChannelRevenueBreakdown)
@save_response("ga_revenue_breakdown_by_channel")
async def get_revenue_breakdown_by_channel(
    property_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get revenue breakdown by channel"""
    try:
        ga4_manager = GA4Manager(current_user["email"])
        breakdown = ga4_manager.get_revenue_breakdown_by_channel(property_id, period)
        return ChannelRevenueBreakdown(**breakdown)
    except Exception as e:
        logger.error(f"Error fetching channel revenue breakdown: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/revenue-breakdown/source/{property_id}", response_model=SourceRevenueBreakdown)
@save_response("ga_revenue_breakdown_by_source")
async def get_revenue_breakdown_by_source(
    property_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    limit: int = Query(20, ge=5, le=50),
    current_user: dict = Depends(get_current_user)
):
    """Get revenue breakdown by source/medium"""
    try:
        ga4_manager = GA4Manager(current_user["email"])
        breakdown = ga4_manager.get_revenue_breakdown_by_source_medium(property_id, period, limit)
        return SourceRevenueBreakdown(**breakdown)
    except Exception as e:
        logger.error(f"Error fetching source revenue breakdown: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/revenue-breakdown/device/{property_id}", response_model=DeviceRevenueBreakdown)
@save_response("ga_revenue_breakdown_by_device")
async def get_revenue_breakdown_by_device(
    property_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get revenue breakdown by device category"""
    try:
        ga4_manager = GA4Manager(current_user["email"])
        breakdown = ga4_manager.get_revenue_breakdown_by_device(property_id, period)
        return DeviceRevenueBreakdown(**breakdown)
    except Exception as e:
        logger.error(f"Error fetching device revenue breakdown: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/revenue-breakdown/location/{property_id}", response_model=LocationRevenueBreakdown)
@save_response("ga_revenue_breakdown_by_location")
async def get_revenue_breakdown_by_location(
    property_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    limit: int = Query(15, ge=5, le=30),
    current_user: dict = Depends(get_current_user)
):
    """Get revenue breakdown by geographic location"""
    try:
        ga4_manager = GA4Manager(current_user["email"])
        breakdown = ga4_manager.get_revenue_breakdown_by_location(property_id, period, limit)
        return LocationRevenueBreakdown(**breakdown)
    except Exception as e:
        logger.error(f"Error fetching location revenue breakdown: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/revenue-breakdown/page/{property_id}", response_model=PageRevenueBreakdown)
@save_response("ga_revenue_breakdown_by_page")
async def get_revenue_breakdown_by_page(
    property_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    limit: int = Query(20, ge=5, le=50),
    current_user: dict = Depends(get_current_user)
):
    """Get revenue breakdown by landing page"""
    try:
        ga4_manager = GA4Manager(current_user["email"])
        breakdown = ga4_manager.get_revenue_breakdown_by_page(property_id, period, limit)
        return PageRevenueBreakdown(**breakdown)
    except Exception as e:
        logger.error(f"Error fetching page revenue breakdown: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/revenue-breakdown/comprehensive/{property_id}", response_model=ComprehensiveRevenueBreakdown)
@save_response("ga_revenue_breakdown_by_comprehensive")
async def get_comprehensive_revenue_breakdown(
    property_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get comprehensive revenue breakdown across all dimensions"""
    try:
        ga4_manager = GA4Manager(current_user["email"])
        breakdown = ga4_manager.get_comprehensive_revenue_breakdown(property_id, period)
        return ComprehensiveRevenueBreakdown(**breakdown)
    except Exception as e:
        logger.error(f"Error fetching comprehensive revenue breakdown: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Alternative endpoint that returns raw JSON (useful for debugging)
@app.get("/api/analytics/revenue-breakdown/raw/{property_id}")
async def get_revenue_breakdown_raw(
    property_id: str,
    breakdown_type: str = Query("comprehensive", pattern="^(channel|source|device|location|page|comprehensive)$"),
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    limit: int = Query(20, ge=5, le=50),
    current_user: dict = Depends(get_current_user)
):
    """Get revenue breakdown data in raw JSON format"""
    try:
        ga4_manager = GA4Manager(current_user["email"])
        
        if breakdown_type == "channel":
            breakdown = ga4_manager.get_revenue_breakdown_by_channel(property_id, period)
        elif breakdown_type == "source":
            breakdown = ga4_manager.get_revenue_breakdown_by_source_medium(property_id, period, limit)
        elif breakdown_type == "device":
            breakdown = ga4_manager.get_revenue_breakdown_by_device(property_id, period)
        elif breakdown_type == "location":
            breakdown = ga4_manager.get_revenue_breakdown_by_location(property_id, period, limit)
        elif breakdown_type == "page":
            breakdown = ga4_manager.get_revenue_breakdown_by_page(property_id, period, limit)
        else:  # comprehensive
            breakdown = ga4_manager.get_comprehensive_revenue_breakdown(property_id, period)
        
        return breakdown
        
    except Exception as e:
        logger.error(f"Error fetching raw revenue breakdown: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/channel-revenue-timeseries/{property_id}", response_model=ChannelRevenueTimeSeries)
@save_response("ga_channel_revenue_time_series")
async def get_channel_revenue_time_series(
    property_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get revenue breakdown by channel as time series data for the given period"""
    try:
        ga4_manager = GA4Manager(current_user["email"])
        time_series = ga4_manager.get_channel_revenue_time_series(property_id, period)
        
        if 'error' in time_series:
            raise HTTPException(status_code=500, detail=time_series['error'])
            
        return ChannelRevenueTimeSeries(**time_series)
    except Exception as e:
        logger.error(f"Error fetching channel revenue time series: {e}")
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/analytics/channel-revenue-timeseries/{property_id}/specific", response_model=SpecificChannelsTimeSeries)
@save_response("ga_specific_channels_time_series")
async def get_specific_channels_time_series(
    property_id: str,
    channels: List[str],  # Request body with list of channels
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get time series data for specific channels only"""
    try:
        if not channels or len(channels) == 0:
            raise HTTPException(status_code=400, detail="At least one channel must be specified")
        
        if len(channels) > 20:
            raise HTTPException(status_code=400, detail="Maximum 20 channels allowed")
        
        ga4_manager = GA4Manager(current_user["email"])
        time_series = ga4_manager.get_specific_channels_time_series(property_id, channels, period)
        
        if 'error' in time_series:
            raise HTTPException(status_code=500, detail=time_series['error'])
            
        return SpecificChannelsTimeSeries(**time_series)
    except Exception as e:
        logger.error(f"Error fetching specific channels time series: {e}")
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/channel-revenue-timeseries/{property_id}/channels")
@save_response("ga_available_channels")
async def get_available_channels(
    property_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get list of available channels for the property (useful for frontend dropdowns)"""
    try:
        ga4_manager = GA4Manager(current_user["email"])
        
        # Get channel breakdown to find available channels
        breakdown = ga4_manager.get_revenue_breakdown_by_channel(property_id, period)
        
        channels = [
            {
                'channel': channel['channel'],
                'totalRevenue': channel['totalRevenue'],
                'revenuePercentage': channel['revenuePercentage']
            }
            for channel in breakdown.get('channels', [])
        ]
        
        return {
            'propertyId': property_id,
            'period': period,
            'channels': channels,
            'total_channels': len(channels)
        }
        
    except Exception as e:
        logger.error(f"Error fetching available channels: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Raw endpoint for debugging or custom frontend handling
@app.get("/api/analytics/channel-revenue-timeseries/{property_id}/raw")
async def get_channel_revenue_time_series_raw(
    property_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    channels: Optional[str] = Query(None, description="Comma-separated list of specific channels"),
    current_user: dict = Depends(get_current_user)
):
    """Get channel revenue time series in raw JSON format"""
    try:
        ga4_manager = GA4Manager(current_user["email"])
        
        if channels:
            # Parse comma-separated channels
            channel_list = [ch.strip() for ch in channels.split(",") if ch.strip()]
            if len(channel_list) > 20:
                raise HTTPException(status_code=400, detail="Maximum 20 channels allowed")
            time_series = ga4_manager.get_specific_channels_time_series(property_id, channel_list, period)
        else:
            time_series = ga4_manager.get_channel_revenue_time_series(property_id, period)
        
        return time_series
        
    except Exception as e:
        logger.error(f"Error fetching raw channel revenue time series: {e}")
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/revenue-timeseries/{property_id}", response_model=RevenueTimeSeries)
@save_response("ga_revenue_time_series")
async def get_revenue_time_series(
    property_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    breakdown_by: str = Query("channel", pattern="^(channel|device|location|source)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get revenue breakdown by specified dimension (channel, device, location, source) as time series data for the given period"""
    try:
        ga4_manager = GA4Manager(current_user["email"])
        time_series = ga4_manager.get_revenue_time_series(property_id, period, breakdown_by)
        
        if 'error' in time_series:
            raise HTTPException(status_code=500, detail=time_series['error'])
            
        return RevenueTimeSeries(**time_series)
    except Exception as e:
        logger.error(f"Error fetching revenue time series for {breakdown_by}: {e}")
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))


# Intent Insights Routes

@app.post("/api/intent/keyword-insights/{customer_id}")
@save_response("intent_keyword_insights_raw")
async def get_keyword_insights(
    customer_id: str,
    request_data: KeywordInsightRequest,
    current_user: dict = Depends(get_current_user)
):
    """Get raw keyword insights from Google Ads API - returns raw JSON"""
    try:
        # Validate seed keywords limit
        if len(request_data.seed_keywords) > 10:
            raise HTTPException(status_code=400, detail="Maximum 10 seed keywords allowed")
        
        intent_manager = IntentManager(current_user["email"], auth_manager)
        
        insights = intent_manager.get_keyword_insights(
            customer_id,
            request_data.seed_keywords,
            request_data.country,
            request_data.timeframe,
            request_data.start_date,
            request_data.end_date
        )
        
        # Return raw JSON without Pydantic validation
        return insights
        
    except Exception as e:
        logger.error(f"Error fetching keyword insights: {e}")
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))


# meta Integration Routes

# =============================================================================
# FACEBOOK AUTHENTICATION ROUTES
# =============================================================================

@app.get("/auth/facebook/login")
async def facebook_login():
    """Initiate Facebook OAuth login"""
    return await auth_manager.initiate_facebook_login()

# @app.get("/auth/facebook/callback")
# async def facebook_auth_callback(code: str, state: Optional[str] = None):
#     """Handle Facebook OAuth callback"""
#     result = await auth_manager.handle_facebook_callback(code, state)
    
#     # Return HTML with JavaScript to display token (similar to Google auth)
#     html_content = f"""
#     <!DOCTYPE html>
#     <html>
#     <head><title>Facebook Authentication Success</title></head>
#     <body>
#         <h2>Facebook Authentication Successful!</h2>
#         <p>Welcome, {result['user']['name']}!</p>
#         <p>Copy your JWT token:</p>
#         <textarea rows="10" cols="80">{result['token']}</textarea>
#         <script>
#             console.log('Facebook JWT Token:', '{result['token']}');
#             console.log('User Info:', {json.dumps(result['user'], indent=2)});
#         </script>
#     </body>
#     </html>
#     """
#     return HTMLResponse(content=html_content)

@app.get("/auth/facebook/callback")
async def facebook_auth_callback(code: str, state: Optional[str] = None):
    """Handle Facebook OAuth callback and redirect to frontend Meta Ads tab"""
    try:
        result = await auth_manager.handle_facebook_callback(code, state)
        frontend_url = os.getenv("FRONTEND_URL", "http://localhost:5173")
        
        # Pass Facebook token and flag to switch to Meta Ads tab
        import urllib.parse
        encoded_token = urllib.parse.quote(result['token'])
        return RedirectResponse(
            url=f"{frontend_url}/dashboard?facebook_token={encoded_token}&switch_to_meta_ads=true"
        )
        
    except Exception as e:
        frontend_url = os.getenv("FRONTEND_URL", "http://localhost:5173")
        error_message = str(e).replace(" ", "%20")
        return RedirectResponse(url=f"{frontend_url}/?error={error_message}")

# =============================================================================
# META INSIGHTS ENDPOINTS (UNIFIED WITH CUSTOM DATE RANGE)
# =============================================================================

def parse_date_params(
    period: Optional[str] = Query(None, pattern="^(7d|30d|90d|365d)$"),
    start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"),
    end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format")
) -> dict:
    """Helper to parse and validate date parameters"""
    if start_date and end_date:
        # Custom date range
        return {"period": None, "start_date": start_date, "end_date": end_date}
    elif period:
        # Predefined period
        return {"period": period, "start_date": None, "end_date": None}
    else:
        # Default to last 30 days
        return {"period": "30d", "start_date": None, "end_date": None}

@app.get("/api/meta/overview", response_model=MetaOverview)
@save_response("meta_overview")
async def get_meta_overview(
    period: Optional[str] = Query(None, pattern="^(7d|30d|90d|365d)$"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    current_user: dict = Depends(get_current_user)
):
    """Get unified overview of all Meta assets (Ads, Pages, Instagram)"""
    try:
        from social.meta_manager import MetaManager
        from models.meta_response_models import MetaOverview
        
        meta_manager = MetaManager(current_user["email"], auth_manager)
        overview = meta_manager.get_meta_overview(period, start_date, end_date)
        return MetaOverview(**overview)
    except Exception as e:
        logger.error(f"Error fetching Meta overview: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# 1. Get ad accounts
@app.get("/api/meta/ad-accounts", response_model=List[MetaAdAccount])
@save_response("meta_ad_accounts")
async def get_meta_ad_accounts(current_user: dict = Depends(get_current_user)):
    """Get Meta ad accounts (no date filter needed for account list)"""
    try:
        from social.meta_manager import MetaManager
        from models.meta_response_models import MetaAdAccount
        
        meta_manager = MetaManager(current_user["email"], auth_manager)
        accounts = meta_manager.get_ad_accounts()
        return [MetaAdAccount(**acc) for acc in accounts]
    except Exception as e:
        logger.error(f"Error fetching Meta ad accounts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/meta/ad-accounts/{account_id}/insights/summary", response_model=AccountInsightsSummary)
async def get_account_insights_summary(
    account_id: str,
    period: Optional[str] = Query(None, pattern="^(7d|30d|90d|365d)$"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    """
    Get account-level insights summary (for metric cards).
    This is fast and doesn't require fetching all campaigns.
    """
    try:
        from social.meta_manager import MetaManager
        meta_manager = MetaManager(current_user["email"], auth_manager)
        
        summary = meta_manager.get_account_insights_summary(
            account_id, period, start_date, end_date
        )
        
        return summary
    except Exception as e:
        logger.error(f"Error fetching account insights summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/meta/ad-accounts/{account_id}/campaigns/paginated", response_model=PaginatedCampaignsResponse)
async def get_campaigns_paginated(
    account_id: str,
    period: Optional[str] = Query(None, pattern="^(7d|30d|90d|365d)$"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    limit: int = Query(5, ge=1, le=20, description="Campaigns per page"),
    offset: int = Query(0, ge=0, description="Starting position"),
    current_user: dict = Depends(get_current_user)
):
    """
    Get campaigns with pagination. Returns only the requested page.
    
    Example:
        GET /api/meta/ad-accounts/act_123/campaigns/paginated?limit=5&offset=0  # First 5
        GET /api/meta/ad-accounts/act_123/campaigns/paginated?limit=5&offset=5  # Next 5
    """
    try:
        from social.meta_manager import MetaManager
        meta_manager = MetaManager(current_user["email"], auth_manager)
        
        result = meta_manager.get_campaigns_paginated(
            account_id, period, start_date, end_date, limit, offset
        )
        
        return result
    except Exception as e:
        logger.error(f"Error fetching paginated campaigns: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    

# 2. Get campaigns list (no insights, very fast)
@app.get("/api/meta/ad-accounts/{account_id}/campaigns/list", response_model=CampaignsList)
async def get_campaigns_list(
    account_id: str,
    status: Optional[str] = Query(None, description="Comma-separated status values: ACTIVE,PAUSED,ARCHIVED"),
    current_user: dict = Depends(get_current_user)
):
    """
    Get list of all campaigns for an ad account without insights (very fast).
    This returns ALL campaigns regardless of activity in any time period.
    
    Args:
        account_id: Ad account ID (e.g., act_303894480866908)
        status: Optional filter by status (e.g., "ACTIVE" or "ACTIVE,PAUSED")
    
    Example:
        GET /api/meta/ad-accounts/act_303894480866908/campaigns/list
        GET /api/meta/ad-accounts/act_303894480866908/campaigns/list?status=ACTIVE
        GET /api/meta/ad-accounts/act_303894480866908/campaigns/list?status=ACTIVE,PAUSED
    """
    try:
        from social.meta_manager import MetaManager
        meta_manager = MetaManager(current_user["email"], auth_manager)
        
        # Parse status filter
        include_status = None
        if status:
            include_status = [s.strip().upper() for s in status.split(',')]
        
        result = meta_manager.get_campaigns_list(account_id, include_status)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# 3. Get campaign timeseries
@app.post("/api/meta/campaigns/timeseries", response_model=List[CampaignTimeseries])
async def get_campaigns_timeseries(
    campaign_ids: List[str],
    period: Optional[str] = Query(None, pattern="^(7d|30d|90d|365d)$"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    """Get time-series data for campaigns"""
    try:
        from social.meta_manager import MetaManager
        meta_manager = MetaManager(current_user["email"], auth_manager)
        return meta_manager.get_campaigns_timeseries(campaign_ids, period, start_date, end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# 4. Get campaign demographics
@app.post("/api/meta/campaigns/demographics", response_model=List[CampaignDemographics])
async def get_campaigns_demographics(
    campaign_ids: List[str],
    period: Optional[str] = Query(None, pattern="^(7d|30d|90d|365d)$"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    """Get age/gender demographics for campaigns"""
    try:
        from social.meta_manager import MetaManager
        meta_manager = MetaManager(current_user["email"], auth_manager)
        return meta_manager.get_campaigns_demographics(campaign_ids, period, start_date, end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# 5. Get campaign placements
@app.post("/api/meta/campaigns/placements", response_model=List[CampaignPlacements])
async def get_campaigns_placements(
    campaign_ids: List[str],
    period: Optional[str] = Query(None, pattern="^(7d|30d|90d|365d)$"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    """Get platform placement data for campaigns"""
    try:
        from social.meta_manager import MetaManager
        meta_manager = MetaManager(current_user["email"], auth_manager)
        return meta_manager.get_campaigns_placements(campaign_ids, period, start_date, end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# 6. Get ad sets by campaigns
@app.post("/api/meta/campaigns/adsets", response_model=List[AdSetInfo])
async def get_adsets_by_campaigns(
    campaign_ids: List[str],
    period: Optional[str] = Query(None, pattern="^(7d|30d|90d|365d)$"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    """
    Get ad sets for multiple campaigns.
    Note: Date parameters are accepted but not used - returns ALL ad sets for the campaigns.
    
    Body example:
    ["120212345678901234", "120212345678901235"]
    """
    try:
        from social.meta_manager import MetaManager
        
        logger.info(f"Fetching ad sets for {len(campaign_ids)} campaigns")
        logger.info(f"Campaign IDs: {campaign_ids}")
        
        if not campaign_ids:
            raise HTTPException(status_code=400, detail="No campaign IDs provided")
        
        meta_manager = MetaManager(current_user["email"], auth_manager)
        adsets = meta_manager.get_adsets_by_campaigns(campaign_ids, period, start_date, end_date)
        
        logger.info(f"Successfully retrieved {len(adsets)} ad sets")
        
        if not adsets:
            logger.warning(f"No ad sets found for campaigns: {campaign_ids}")
        
        return adsets
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching ad sets: {e}")
        logger.error(f"Campaign IDs: {campaign_ids}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch ad sets: {str(e)}")

# 7. Get ad set timeseries
@app.post("/api/meta/adsets/timeseries", response_model=List[AdSetTimeseries])
async def get_adsets_timeseries(
    adset_ids: List[str],
    period: Optional[str] = Query(None, pattern="^(7d|30d|90d|365d)$"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    """Get time-series data for ad sets"""
    try:
        from social.meta_manager import MetaManager
        meta_manager = MetaManager(current_user["email"], auth_manager)
        return meta_manager.get_adsets_timeseries(adset_ids, period, start_date, end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# 8. Get ad set demographics
@app.post("/api/meta/adsets/demographics", response_model=List[AdSetDemographics])
async def get_adsets_demographics(
    adset_ids: List[str],
    period: Optional[str] = Query(None, pattern="^(7d|30d|90d|365d)$"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    """Get age/gender demographics for ad sets"""
    try:
        from social.meta_manager import MetaManager
        meta_manager = MetaManager(current_user["email"], auth_manager)
        return meta_manager.get_adsets_demographics(adset_ids, period, start_date, end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# 9. Get ad set placements
@app.post("/api/meta/adsets/placements", response_model=List[AdSetPlacements])
async def get_adsets_placements(
    adset_ids: List[str],
    period: Optional[str] = Query(None, pattern="^(7d|30d|90d|365d)$"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    """Get platform placement data for ad sets"""
    try:
        from social.meta_manager import MetaManager
        meta_manager = MetaManager(current_user["email"], auth_manager)
        return meta_manager.get_adsets_placements(adset_ids, period, start_date, end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# 10. Get ads by ad sets
@app.post("/api/meta/adsets/ads", response_model=List[AdInfo])
async def get_ads_by_adsets(
    adset_ids: List[str],
    current_user: dict = Depends(get_current_user)
):
    """Get ads for multiple ad sets"""
    try:
        from social.meta_manager import MetaManager
        meta_manager = MetaManager(current_user["email"], auth_manager)
        return meta_manager.get_ads_by_adsets(adset_ids)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# 11. Get ad timeseries
@app.post("/api/meta/ads/timeseries", response_model=List[AdTimeseries])
async def get_ads_timeseries(
    ad_ids: List[str],
    period: Optional[str] = Query(None, pattern="^(7d|30d|90d|365d)$"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    """Get time-series data for ads"""
    try:
        from social.meta_manager import MetaManager
        meta_manager = MetaManager(current_user["email"], auth_manager)
        return meta_manager.get_ads_timeseries(ad_ids, period, start_date, end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# 12. Get ad demographics
@app.post("/api/meta/ads/demographics", response_model=List[AdDemographics])
async def get_ads_demographics(
    ad_ids: List[str],
    period: Optional[str] = Query(None, pattern="^(7d|30d|90d|365d)$"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    """Get age/gender demographics for ads"""
    try:
        from social.meta_manager import MetaManager
        meta_manager = MetaManager(current_user["email"], auth_manager)
        return meta_manager.get_ads_demographics(ad_ids, period, start_date, end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# 13. Get ad placements
@app.post("/api/meta/ads/placements", response_model=List[AdPlacements])
async def get_ads_placements(
    ad_ids: List[str],
    period: Optional[str] = Query(None, pattern="^(7d|30d|90d|365d)$"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    """Get platform placement data for ads"""
    try:
        from social.meta_manager import MetaManager
        meta_manager = MetaManager(current_user["email"], auth_manager)
        return meta_manager.get_ads_placements(ad_ids, period, start_date, end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/api/meta/pages", response_model=List[FacebookPageBasic])
@save_response("meta_pages")
async def get_meta_pages(current_user: dict = Depends(get_current_user)):
    """Get Facebook pages (no date filter needed for page list)"""
    try:
        from social.meta_manager import MetaManager
        from models.meta_response_models import FacebookPageBasic
        
        meta_manager = MetaManager(current_user["email"], auth_manager)
        pages = meta_manager.get_pages()
        return [FacebookPageBasic(**page) for page in pages]
    except Exception as e:
        logger.error(f"Error fetching Meta pages: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/meta/pages/{page_id}/insights", response_model=FacebookPageInsights)
@save_response("meta_page_insights")
async def get_meta_page_insights(
    page_id: str,
    period: Optional[str] = Query(None, pattern="^(7d|30d|90d|365d)$"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    current_user: dict = Depends(get_current_user)
):
    """Get insights for Facebook page with custom date range support"""
    try:
        from social.meta_manager import MetaManager
        from models.meta_response_models import FacebookPageInsights
        
        meta_manager = MetaManager(current_user["email"], auth_manager)
        insights = meta_manager.get_page_insights(page_id, period, start_date, end_date)
        return FacebookPageInsights(**insights)
    except Exception as e:
        logger.error(f"Error fetching Meta page insights: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/meta/pages/{page_id}/insights/timeseries")
@save_response("meta_page_insights_timeseries")
async def get_meta_page_insights_timeseries(
    page_id: str,
    period: Optional[str] = Query(None, pattern="^(7d|30d|90d|365d)$"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    current_user: dict = Depends(get_current_user)
):
    """Get time-series insights for Facebook page (for line charts)"""
    try:
        from social.meta_manager import MetaManager
        
        meta_manager = MetaManager(current_user["email"], auth_manager)
        insights = meta_manager.get_page_insights_timeseries(page_id, period, start_date, end_date)
        return insights
    except Exception as e:
        logger.error(f"Error fetching Meta page insights timeseries: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/api/meta/pages/{page_id}/posts", response_model=List[FacebookPostDetail])
@save_response("meta_page_posts")
async def get_meta_page_posts(
    page_id: str,
    limit: int = Query(10, ge=1, le=50),
    period: Optional[str] = Query(None, pattern="^(7d|30d|90d|365d)$"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    current_user: dict = Depends(get_current_user)
):
    """Get posts from Facebook page with custom date range support"""
    try:
        from social.meta_manager import MetaManager
        from models.meta_response_models import FacebookPostDetail
        
        meta_manager = MetaManager(current_user["email"], auth_manager)
        posts = meta_manager.get_page_posts(page_id, limit, period, start_date, end_date)
        return [FacebookPostDetail(**post) for post in posts]
    except Exception as e:
        logger.error(f"Error fetching Meta page posts: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/meta/pages/{page_id}/posts/timeseries")
@save_response("meta_page_posts_timeseries")
async def get_meta_page_posts_timeseries(
    page_id: str,
    limit: int = Query(10, ge=1, le=100),
    period: Optional[str] = Query(None, pattern="^(7d|30d|90d|365d)$"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    current_user: dict = Depends(get_current_user)
):
    """Get posts with time-series insights (for tracking post performance over time)"""
    try:
        from social.meta_manager import MetaManager
        
        meta_manager = MetaManager(current_user["email"], auth_manager)
        posts = meta_manager.get_page_posts_timeseries(page_id, limit, period, start_date, end_date)
        return posts
    except Exception as e:
        logger.error(f"Error fetching Meta page posts timeseries: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/meta/instagram/accounts", response_model=List[InstagramAccountBasic])
@save_response("meta_instagram_accounts")
async def get_meta_instagram_accounts(current_user: dict = Depends(get_current_user)):
    """Get Instagram Business accounts (no date filter needed for account list)"""
    try:
        from social.meta_manager import MetaManager
        from models.meta_response_models import InstagramAccountBasic
        
        meta_manager = MetaManager(current_user["email"], auth_manager)
        accounts = meta_manager.get_instagram_accounts()
        return [InstagramAccountBasic(**acc) for acc in accounts]
    except Exception as e:
        logger.error(f"Error fetching Instagram accounts: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/meta/instagram/{account_id}/insights", response_model=InstagramAccountInsights)
@save_response("meta_instagram_insights")
async def get_meta_instagram_insights(
    account_id: str,
    period: Optional[str] = Query(None, pattern="^(7d|30d|90d|365d)$"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    current_user: dict = Depends(get_current_user)
):
    """Get insights for Instagram account with custom date range support"""
    try:
        from social.meta_manager import MetaManager
        from models.meta_response_models import InstagramAccountInsights
        
        meta_manager = MetaManager(current_user["email"], auth_manager)
        insights = meta_manager.get_instagram_insights(account_id, period, start_date, end_date)
        return InstagramAccountInsights(**insights)
    except Exception as e:
        logger.error(f"Error fetching Instagram insights: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/meta/instagram/{account_id}/insights/timeseries")
@save_response("meta_instagram_insights_timeseries")
async def get_meta_instagram_insights_timeseries(
    account_id: str,
    period: Optional[str] = Query(None, pattern="^(7d|30d|90d|365d)$"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    current_user: dict = Depends(get_current_user)
):
    """Get time-series insights for Instagram account (for line charts)"""
    try:
        from social.meta_manager import MetaManager
        
        meta_manager = MetaManager(current_user["email"], auth_manager)
        insights = meta_manager.get_instagram_insights_timeseries(account_id, period, start_date, end_date)
        return insights
    except Exception as e:
        logger.error(f"Error fetching Instagram insights timeseries: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/api/meta/instagram/{account_id}/media", response_model=List[InstagramMediaDetail])
@save_response("meta_instagram_media")
async def get_meta_instagram_media(
    account_id: str,
    limit: int = Query(10, ge=1, le=50),
    period: Optional[str] = Query(None, pattern="^(7d|30d|90d|365d)$"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    current_user: dict = Depends(get_current_user)
):
    """Get media from Instagram account with custom date range support"""
    try:
        from social.meta_manager import MetaManager
        from models.meta_response_models import InstagramMediaDetail
        
        meta_manager = MetaManager(current_user["email"], auth_manager)
        media = meta_manager.get_instagram_media(account_id, limit, period, start_date, end_date)
        return [InstagramMediaDetail(**media_item) for media_item in media]
    except Exception as e:
        logger.error(f"Error fetching Instagram media: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/api/meta/debug/permissions")

@app.get("/api/meta/instagram/{account_id}/media/timeseries")
@save_response("meta_instagram_media_timeseries")
async def get_meta_instagram_media_timeseries(
    account_id: str,
    limit: int = Query(10, ge=1, le=100),
    period: Optional[str] = Query(None, pattern="^(7d|30d|90d|365d)$"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    current_user: dict = Depends(get_current_user)
):
    """Get Instagram media with time-series insights (for tracking post performance over time)"""
    try:
        from social.meta_manager import MetaManager
        
        meta_manager = MetaManager(current_user["email"], auth_manager)
        media = meta_manager.get_instagram_media_timeseries(account_id, limit, period, start_date, end_date)
        return media
    except Exception as e:
        logger.error(f"Error fetching Instagram media timeseries: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    

async def debug_meta_permissions(current_user: dict = Depends(get_current_user)):
    """Debug endpoint to check what permissions we have"""
    try:
        from social.meta_manager import MetaManager
        meta_manager = MetaManager(current_user["email"], auth_manager)
        
        # Check user token permissions
        user_perms = meta_manager._make_request("me/permissions")
        
        # Check pages without perms field
        pages = meta_manager._make_request("me/accounts", {
            'fields': 'id,name,access_token,tasks'
        })
        
        # For each page, try to get posts count
        pages_debug = []
        for page in pages.get('data', []):
            page_id = page['id']
            page_token = page.get('access_token')
            
            # Try to get posts without time filter
            try:
                posts_response = requests.get(
                    f"https://graph.facebook.com/v21.0/{page_id}/posts",
                    params={
                        'access_token': page_token,
                        'fields': 'id',
                        'limit': 5
                    }
                )
                posts_data = posts_response.json()
                
                pages_debug.append({
                    'id': page_id,
                    'name': page['name'],
                    'has_token': page_token is not None,
                    'tasks': page.get('tasks', []),
                    'posts_count': len(posts_data.get('data', [])),
                    'posts_error': posts_data.get('error')
                })
            except Exception as e:
                pages_debug.append({
                    'id': page_id,
                    'name': page['name'],
                    'error': str(e)
                })
        
        return {
            "user_permissions": user_perms,
            "pages_debug": pages_debug
        }
    except Exception as e:
        return {"error": str(e)}


# Chat endpoints
@app.post("/api/chat/message", response_model=ChatResponse)
async def send_chat_message(
    chat_request: ChatRequest,
    current_user: dict = Depends(get_current_user)
):
    """Chat message endpoint"""
    try:
        response = await chat_manager.process_chat_message(
            chat_request=chat_request,
            user_email=current_user["email"]
        )
        
        return response
        
    except Exception as e:
        logger.error(f"Error processing chat message: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/chat/conversation/{session_id}")
async def get_conversation(
    session_id: str,
    module_type: ModuleType = Query(...),
    current_user: dict = Depends(get_current_user)
):
    """Get specific conversation by session ID"""
    try:
        logger.info(f"Getting conversation: session_id={session_id}, user={current_user['email']}, module={module_type.value}")
        
        # Use the chat_manager method
        conversation = await chat_manager.get_conversation_by_session_id(
            user_email=current_user["email"],
            session_id=session_id,
            module_type=module_type
        )
        
        if not conversation:
            logger.error(f"Conversation not found in database")
            # Let's also check what sessions exist for debugging
            collection = chat_manager.db.chat_sessions
            existing_sessions = await collection.find({
                "user_email": current_user["email"],
                "module_type": module_type.value
            }).to_list(length=10)
            
            logger.error(f"Available sessions for user: {[s['session_id'] for s in existing_sessions]}")
            raise HTTPException(status_code=404, detail=f"Conversation {session_id} not found")
        
        logger.info(f"Found conversation with {len(conversation.get('messages', []))} messages")
        
        return conversation
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting conversation: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/chat/sessions/{module_type}")
async def get_chat_sessions_list(
    module_type: ModuleType,
    current_user: dict = Depends(get_current_user)
):
    """Get list of chat sessions for a module"""
    try:
        collection = chat_manager.db.chat_sessions
        
        # Get sessions with messages
        cursor = collection.find({
            "user_email": current_user["email"],
            "module_type": module_type.value,
            "is_active": True
        }).sort("last_activity", -1)
        
        sessions = await cursor.to_list(length=50)
        
        logger.info(f"Found {len(sessions)} sessions for user {current_user['email']} and module {module_type.value}")
        
        # Format sessions and include debug info
        formatted_sessions = []
        for session in sessions:
            messages = session.get("messages", [])
            
            # Only include sessions that have messages
            if len(messages) > 0:
                formatted_sessions.append({
                    "session_id": session["session_id"],
                    "user_email": session["user_email"],
                    "module_type": session["module_type"],
                    "customer_id": session.get("customer_id"),
                    "property_id": session.get("property_id"),
                    "created_at": session["created_at"],
                    "last_activity": session["last_activity"],
                    "is_active": session.get("is_active", True),
                    "messages": messages
                })
        
        logger.info(f"Returning {len(formatted_sessions)} sessions with messages")
        
        return {
            "sessions": formatted_sessions,
            "total_sessions": len(formatted_sessions),
            "module_type": module_type.value
        }
        
    except Exception as e:
        logger.error(f"Error getting chat sessions list: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/chat/delete")
async def delete_chat_sessions(
    delete_request: DeleteChatRequest,
    current_user: dict = Depends(get_current_user)
):
    """Delete chat sessions"""
    try:
        result = await chat_manager.delete_chat_sessions(
            user_email=current_user["email"],
            session_ids=delete_request.session_ids
        )
        return result
        
    except Exception as e:
        logger.error(f"Error deleting chat sessions: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/chat/history/{module_type}", response_model=ChatHistoryResponse)
async def get_chat_history(
    module_type: ModuleType,
    limit: int = Query(20, ge=1, le=100),
    current_user: dict = Depends(get_current_user)
):
    """Get chat history for a module"""
    try:
        history = await chat_manager.get_chat_history(
            user_email=current_user["email"],
            module_type=module_type,
            limit=limit
        )
        return history
        
    except Exception as e:
        logger.error(f"Error getting chat history: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Debug endpoint to help troubleshoot
@app.get("/api/chat/debug/{module_type}")
async def debug_chat_sessions(
    module_type: ModuleType,
    current_user: dict = Depends(get_current_user)
):
    """Debug endpoint to check chat sessions"""
    try:
        collection = chat_manager.db.chat_sessions
        
        # Get all sessions for this user and module
        sessions = await collection.find({
            "user_email": current_user["email"],
            "module_type": module_type.value
        }).to_list(length=100)
        
        debug_info = {
            "user_email": current_user["email"],
            "module_type": module_type.value,
            "total_sessions": len(sessions),
            "sessions_summary": []
        }
        
        for session in sessions:
            debug_info["sessions_summary"].append({
                "session_id": session["session_id"],
                "message_count": len(session.get("messages", [])),
                "is_active": session.get("is_active", True),
                "created_at": session["created_at"],
                "last_activity": session["last_activity"],
                "first_message": session["messages"][0]["content"][:50] + "..." if session.get("messages") else "No messages"
            })
        
        return debug_info
        
    except Exception as e:
        logger.error(f"Error in debug endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))   

if __name__ == "__main__":
    logger.info("🚀 Starting Unified Marketing Dashboard API...")
    logger.info("📊 Available services: Google Ads + Google Analytics + Intent Insights")
    logger.info("🌐 Server will be available at: http://localhost:8000")
    logger.info("📚 API docs available at: http://localhost:8000/docs")
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )