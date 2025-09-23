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
from meta.meta_manager import MetaManager
from models.response_models import *
from models.response_models import AdKeyStats
from models.response_models import EnhancedAdCampaign, FunnelRequest
from database.mongo_manager import MongoManager


from models.meta_response_models import (
    MetaAdAccount, MetaCampaign, MetaAdAccountKeyStats,
    MetaPlacementPerformance, MetaDemographicPerformance, MetaTimeSeriesData,
    FacebookPage, FacebookPageInsights, FacebookPost, FacebookPostInsights,
    FacebookAudienceInsights, FacebookPagePerformanceSummary,
    InstagramAccount, InstagramAccountInsights, InstagramMedia, InstagramStory,
    InstagramAudienceDemographics, InstagramHashtagPerformance, InstagramAccountPerformanceSummary,
    SocialMediaOverview, SocialMediaInsightsSummary, CrossPlatformEngagement,
    FacebookUserInfo, FacebookAuthResponse
)
from models.chat_models import ModuleType

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

# @app.get("/auth/callback")
# async def auth_callback(code: str, state: Optional[str] = None):
#     result = await auth_manager.handle_callback(code, state)
    
#     # Return HTML with JavaScript to display token
#     html_content = f"""
#     <!DOCTYPE html>
#     <html>
#     <head><title>Authentication Success</title></head>
#     <body>
#         <h2>Authentication Successful!</h2>
#         <p>Copy your JWT token:</p>
#         <textarea rows="10" cols="80">{result['token']}</textarea>
#         <script>
#             console.log('JWT Token:', '{result['token']}');
#         </script>
#     </body>
#     </html>
#     """
#     return HTMLResponse(content=html_content)


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


# =============================================================================
# FACEBOOK AUTHENTICATION ROUTES
# =============================================================================

@app.get("/auth/facebook/login")
async def facebook_login():
    """Initiate Facebook OAuth login"""
    return await auth_manager.initiate_facebook_login()

@app.get("/auth/facebook/callback")
async def facebook_auth_callback(code: str, state: Optional[str] = None):
    """Handle Facebook OAuth callback"""
    result = await auth_manager.handle_facebook_callback(code, state)
    
    # Return HTML with JavaScript to display token (similar to Google auth)
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head><title>Facebook Authentication Success</title></head>
    <body>
        <h2>Facebook Authentication Successful!</h2>
        <p>Welcome, {result['user']['name']}!</p>
        <p>Copy your JWT token:</p>
        <textarea rows="10" cols="80">{result['token']}</textarea>
        <script>
            console.log('Facebook JWT Token:', '{result['token']}');
            console.log('User Info:', {json.dumps(result['user'], indent=2)});
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@app.get("/auth/facebook/user")
async def get_facebook_user_info(current_user: dict = Depends(get_current_user)):
    """Get current Facebook user information"""
    # Check if this is a Facebook authenticated user
    if current_user.get("auth_provider") != "facebook":
        raise HTTPException(status_code=400, detail="Not a Facebook authenticated user")
    
    return {
        "id": current_user.get("id", ""),
        "email": current_user["email"],
        "name": current_user["name"],
        "picture": current_user.get("picture", ""),
        "auth_provider": "facebook"
    }

@app.post("/auth/facebook/logout")
async def facebook_logout(current_user: dict = Depends(get_current_user)):
    """Logout Facebook user"""
    if current_user.get("auth_provider") != "facebook":
        raise HTTPException(status_code=400, detail="Not a Facebook authenticated user")
    
    return await auth_manager.logout_user(current_user["email"], "facebook")

@app.get("/auth/facebook/deauthorize")
@app.post("/auth/facebook/deauthorize")
async def facebook_deauthorize(request: Request):
    """Handle Facebook app deauthorization (required by Facebook)"""
    try:
        # Get signed_request parameter
        if request.method == "GET":
            signed_request = request.query_params.get("signed_request", "")
        else:
            form_data = await request.form()
            signed_request = form_data.get("signed_request", "")
        
        result = await auth_manager.handle_facebook_deauthorization(signed_request)
        return result
        
    except Exception as e:
        logger.error(f"Facebook deauthorization error: {e}")
        return {"status": "error", "message": "Deauthorization failed"}

@app.get("/auth/facebook/data-deletion")
@app.post("/auth/facebook/data-deletion") 
async def facebook_data_deletion(request: Request):
    """Handle Facebook data deletion request (required by Facebook)"""
    try:
        # Get signed_request parameter
        if request.method == "GET":
            signed_request = request.query_params.get("signed_request", "")
        else:
            form_data = await request.form()
            signed_request = form_data.get("signed_request", "")
        
        # In production, you should:
        # 1. Parse the signed_request to get user ID
        # 2. Delete all user data from your systems
        # 3. Return a confirmation URL
        
        logger.info("Facebook data deletion request received")
        
        return {
            "url": f"{request.base_url}privacy",  # URL to your data deletion confirmation page
            "confirmation_code": f"deletion_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        }
        
    except Exception as e:
        logger.error(f"Facebook data deletion error: {e}")
        return {"status": "error", "message": "Data deletion request failed"}

# =============================================================================
# ENHANCED CURRENT USER DEPENDENCY (Supporting both Google and Facebook)
# =============================================================================

async def get_current_user_enhanced(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    """Get current authenticated user (Google or Facebook)"""
    user_data = auth_manager.verify_jwt_token(credentials.credentials)
    
    # Add auth provider information for easier handling
    auth_provider = user_data.get("auth_provider", "google")
    user_data["auth_provider"] = auth_provider
    
    return user_data

# =============================================================================
# FACEBOOK SPECIFIC USER DEPENDENCY
# =============================================================================

async def get_facebook_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    """Get current Facebook authenticated user only"""
    user_data = auth_manager.verify_jwt_token(credentials.credentials)
    
    if user_data.get("auth_provider") != "facebook":
        raise HTTPException(status_code=403, detail="Facebook authentication required")
    
    return user_data

# =============================================================================
# GOOGLE SPECIFIC USER DEPENDENCY  
# =============================================================================

async def get_google_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    """Get current Google authenticated user only"""
    user_data = auth_manager.verify_jwt_token(credentials.credentials)
    
    if user_data.get("auth_provider") != "google":
        raise HTTPException(status_code=403, detail="Google authentication required")
    
    return user_data

# =============================================================================
# UPDATE HEALTH CHECK TO INCLUDE SOCIAL MEDIA APIS
# =============================================================================

@app.get("/health")
async def health_check():
    """Enhanced health check endpoint with social media connectivity"""
    # Test MongoDB connection
    mongodb_status = "healthy"
    try:
        await mongo_manager.client.admin.command('ping')
    except Exception as e:
        mongodb_status = f"unhealthy: {str(e)}"
    
    # Test Facebook API connectivity
    facebook_status = "not_configured"
    if auth_manager.FACEBOOK_APP_ID:
        try:
            # Simple Facebook API test
            test_url = f"https://graph.facebook.com/v18.0/me?access_token=test"
            response = requests.get(test_url, timeout=5)
            facebook_status = "configured" if response.status_code in [400, 401] else "connection_error"
        except Exception:
            facebook_status = "connection_error"
    
    return {
        "status": "healthy" if mongodb_status == "healthy" else "partial",
        "timestamp": datetime.now().isoformat(),
        "services": {
            "google_ads": "healthy",
            "google_analytics": "healthy", 
            "intent_insights": "healthy",
            "mongodb": mongodb_status,
            "facebook_auth": facebook_status,
            "meta_ads_api": "depends_on_auth",
            "facebook_pages_api": "depends_on_auth",
            "instagram_business_api": "depends_on_auth"
        },
        "auth_providers": {
            "google": "configured" if auth_manager.GOOGLE_CLIENT_ID else "not_configured",
            "facebook": "configured" if auth_manager.FACEBOOK_APP_ID else "not_configured"
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
    
# =============================================================================
# TESTING AND DEBUG ENDPOINTS
# =============================================================================

@app.get("/api/meta/test-connection")
async def test_meta_connection(current_user: dict = Depends(get_facebook_user)):
    """Test Meta API connection and permissions"""
    try:
        from meta.meta_manager import MetaManager
        meta_manager = MetaManager(current_user["email"], auth_manager)
        
        # Test basic API call
        accounts = meta_manager.get_ad_accounts()
        
        return {
            "status": "success",
            "message": "Meta API connection successful",
            "accounts_found": len(accounts),
            "user_email": current_user["email"],
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Meta API connection test failed: {e}")
        return {
            "status": "error",
            "message": str(e),
            "user_email": current_user["email"],
            "timestamp": datetime.now().isoformat()
        }

@app.get("/api/facebook/test-connection")
async def test_facebook_connection(current_user: dict = Depends(get_facebook_user)):
    """Test Facebook API connection and permissions"""
    try:
        from facebook.facebook_manager import FacebookManager
        facebook_manager = FacebookManager(current_user["email"], auth_manager)
        
        # Test basic API call
        pages = facebook_manager.get_user_pages()
        
        return {
            "status": "success",
            "message": "Facebook API connection successful",
            "pages_found": len(pages),
            "user_email": current_user["email"],
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Facebook API connection test failed: {e}")
        return {
            "status": "error",
            "message": str(e),
            "user_email": current_user["email"],
            "timestamp": datetime.now().isoformat()
        }

# =============================================================================
# META ADS API ENDPOINTS
# =============================================================================

@app.get("/api/debug/clear-sessions")
async def clear_sessions():
    """Clear all sessions for testing"""
    auth_manager.facebook_sessions.clear()
    auth_manager.user_sessions.clear()
    auth_manager.facebook_states.clear()
    auth_manager.oauth_states.clear()
    return {"message": "All sessions cleared"}


@app.get("/api/meta/accounts", response_model=List[MetaAdAccount])
@save_response("meta_ad_accounts")
async def get_meta_ad_accounts(current_user: dict = Depends(get_facebook_user)):
    """Get accessible Meta Ad accounts"""
    try:
        from meta.meta_manager import MetaManager
        meta_manager = MetaManager(current_user["email"], auth_manager)
        accounts = meta_manager.get_ad_accounts()
        return [MetaAdAccount(**account) for account in accounts]
    except Exception as e:
        logger.error(f"Error fetching Meta ad accounts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/debug/facebook-raw")
async def debug_facebook_raw(current_user: dict = Depends(get_facebook_user)):
    """Debug raw Facebook API responses"""
    try:
        access_token = auth_manager.get_facebook_access_token(current_user["email"])
        
        # Test different Facebook API endpoints
        me_url = f"https://graph.facebook.com/v18.0/me?access_token={access_token}"
        accounts_url = f"https://graph.facebook.com/v18.0/me/accounts?access_token={access_token}"
        permissions_url = f"https://graph.facebook.com/v18.0/me/permissions?access_token={access_token}"
        
        me_response = requests.get(me_url)
        accounts_response = requests.get(accounts_url)
        permissions_response = requests.get(permissions_url)
        
        return {
            "me_status": me_response.status_code,
            "me_data": me_response.json() if me_response.status_code == 200 else me_response.text,
            
            "accounts_status": accounts_response.status_code,
            "accounts_data": accounts_response.json() if accounts_response.status_code == 200 else accounts_response.text,
            
            "permissions_status": permissions_response.status_code,
            "permissions_data": permissions_response.json() if permissions_response.status_code == 200 else permissions_response.text,
            
            "access_token_preview": access_token[:30] + "..."
        }
    except Exception as e:
        return {"error": str(e)}
    

@app.get("/api/meta/key-stats/{account_id}", response_model=MetaAdAccountKeyStats)
@save_response("meta_key_stats")
async def get_meta_key_stats(
    account_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_facebook_user)
):
    """Get Meta Ads key statistics for dashboard cards"""
    try:
        from meta.meta_manager import MetaManager
        meta_manager = MetaManager(current_user["email"], auth_manager)
        key_stats = meta_manager.get_account_key_stats(account_id, period)
        return MetaAdAccountKeyStats(**key_stats)
    except Exception as e:
        logger.error(f"Error fetching Meta key stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/meta/campaigns/{account_id}", response_model=List[MetaCampaign])
@save_response("meta_campaigns")
async def get_meta_campaigns(
    account_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_facebook_user)
):
    """Get Meta Ads campaigns for an account"""
    try:
        from meta.meta_manager import MetaManager
        meta_manager = MetaManager(current_user["email"], auth_manager)
        campaigns = meta_manager.get_campaigns(account_id, period)
        return [MetaCampaign(**campaign) for campaign in campaigns]
    except Exception as e:
        logger.error(f"Error fetching Meta campaigns: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/meta/performance/placement/{account_id}", response_model=List[MetaPlacementPerformance])
@save_response("meta_placement_performance")
async def get_meta_placement_performance(
    account_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_facebook_user)
):
    """Get Meta Ads performance by placement"""
    try:
        from meta.meta_manager import MetaManager
        meta_manager = MetaManager(current_user["email"], auth_manager)
        placements = meta_manager.get_performance_by_placement(account_id, period)
        return [MetaPlacementPerformance(**placement) for placement in placements]
    except Exception as e:
        logger.error(f"Error fetching Meta placement performance: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/meta/performance/demographics/{account_id}", response_model=List[MetaDemographicPerformance])
@save_response("meta_demographic_performance")
async def get_meta_demographic_performance(
    account_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_facebook_user)
):
    """Get Meta Ads performance by demographics"""
    try:
        from meta.meta_manager import MetaManager
        meta_manager = MetaManager(current_user["email"], auth_manager)
        demographics = meta_manager.get_performance_by_age_gender(account_id, period)
        return [MetaDemographicPerformance(**demo) for demo in demographics]
    except Exception as e:
        logger.error(f"Error fetching Meta demographic performance: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/meta/performance/timeseries/{account_id}", response_model=List[MetaTimeSeriesData])
@save_response("meta_time_series")
async def get_meta_time_series(
    account_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_facebook_user)
):
    """Get Meta Ads time series performance data"""
    try:
        from meta.meta_manager import MetaManager
        meta_manager = MetaManager(current_user["email"], auth_manager)
        time_series = meta_manager.get_time_series_data(account_id, period)
        return [MetaTimeSeriesData(**ts) for ts in time_series]
    except Exception as e:
        logger.error(f"Error fetching Meta time series: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# FACEBOOK PAGES API ENDPOINTS
# =============================================================================

@app.get("/api/facebook/pages", response_model=List[FacebookPage])
@save_response("facebook_pages")
async def get_facebook_pages(current_user: dict = Depends(get_facebook_user)):
    """Get accessible Facebook Pages"""
    try:
        from facebook.facebook_manager import FacebookManager
        facebook_manager = FacebookManager(current_user["email"], auth_manager)
        pages = facebook_manager.get_user_pages()
        return [FacebookPage(**page) for page in pages]
    except Exception as e:
        logger.error(f"Error fetching Facebook pages: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/facebook/insights/{page_id}", response_model=FacebookPageInsights)
@save_response("facebook_page_insights")
async def get_facebook_page_insights(
    page_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_facebook_user)
):
    """Get Facebook Page insights"""
    try:
        from facebook.facebook_manager import FacebookManager
        facebook_manager = FacebookManager(current_user["email"], auth_manager)
        insights = facebook_manager.get_page_insights(page_id, period)
        return FacebookPageInsights(**insights)
    except Exception as e:
        logger.error(f"Error fetching Facebook page insights: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/facebook/posts/{page_id}", response_model=List[FacebookPost])
@save_response("facebook_page_posts")
async def get_facebook_page_posts(
    page_id: str,
    limit: int = Query(20, ge=1, le=100),
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_facebook_user)
):
    """Get Facebook Page posts with engagement data"""
    try:
        from facebook.facebook_manager import FacebookManager
        facebook_manager = FacebookManager(current_user["email"], auth_manager)
        posts = facebook_manager.get_page_posts(page_id, limit, period)
        return [FacebookPost(**post) for post in posts]
    except Exception as e:
        logger.error(f"Error fetching Facebook page posts: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/facebook/post-insights/{post_id}", response_model=FacebookPostInsights)
@save_response("facebook_post_insights")
async def get_facebook_post_insights(
    post_id: str,
    current_user: dict = Depends(get_facebook_user)
):
    """Get detailed insights for a specific Facebook post"""
    try:
        from facebook.facebook_manager import FacebookManager
        facebook_manager = FacebookManager(current_user["email"], auth_manager)
        insights = facebook_manager.get_post_insights(post_id)
        return FacebookPostInsights(**insights)
    except Exception as e:
        logger.error(f"Error fetching Facebook post insights: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/facebook/audience/{page_id}", response_model=FacebookAudienceInsights)
@save_response("facebook_audience_insights")
async def get_facebook_audience_insights(
    page_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_facebook_user)
):
    """Get Facebook Page audience demographics"""
    try:
        from facebook.facebook_manager import FacebookManager
        facebook_manager = FacebookManager(current_user["email"], auth_manager)
        audience = facebook_manager.get_audience_insights(page_id, period)
        return FacebookAudienceInsights(**audience)
    except Exception as e:
        logger.error(f"Error fetching Facebook audience insights: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/facebook/performance-summary/{page_id}", response_model=FacebookPagePerformanceSummary)
@save_response("facebook_performance_summary")
async def get_facebook_performance_summary(
    page_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_facebook_user)
):
    """Get comprehensive Facebook Page performance summary"""
    try:
        from facebook.facebook_manager import FacebookManager
        facebook_manager = FacebookManager(current_user["email"], auth_manager)
        summary = facebook_manager.get_page_performance_summary(page_id, period)
        return FacebookPagePerformanceSummary(**summary)
    except Exception as e:
        logger.error(f"Error fetching Facebook performance summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# INSTAGRAM API ENDPOINTS
# =============================================================================

@app.get("/api/instagram/accounts", response_model=List[InstagramAccount])
@save_response("instagram_accounts")
async def get_instagram_accounts(current_user: dict = Depends(get_facebook_user)):
    """Get Instagram Business accounts"""
    try:
        from instagram.instagram_manager import InstagramManager
        instagram_manager = InstagramManager(current_user["email"], auth_manager)
        accounts = instagram_manager.get_instagram_business_accounts()
        return [InstagramAccount(**account) for account in accounts]
    except Exception as e:
        logger.error(f"Error fetching Instagram accounts: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/instagram/insights/{account_id}", response_model=InstagramAccountInsights)
@save_response("instagram_account_insights")
async def get_instagram_account_insights(
    account_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_facebook_user)
):
    """Get Instagram Business account insights"""
    try:
        from instagram.instagram_manager import InstagramManager
        instagram_manager = InstagramManager(current_user["email"], auth_manager)
        insights = instagram_manager.get_account_insights(account_id, period)
        return InstagramAccountInsights(**insights)
    except Exception as e:
        logger.error(f"Error fetching Instagram account insights: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/instagram/media/{account_id}", response_model=List[InstagramMedia])
@save_response("instagram_account_media")
async def get_instagram_account_media(
    account_id: str,
    limit: int = Query(20, ge=1, le=100),
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_facebook_user)
):
    """Get Instagram account media with insights"""
    try:
        from instagram.instagram_manager import InstagramManager
        instagram_manager = InstagramManager(current_user["email"], auth_manager)
        media = instagram_manager.get_account_media(account_id, limit, period)
        return [InstagramMedia(**post) for post in media]
    except Exception as e:
        logger.error(f"Error fetching Instagram media: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/instagram/stories/{account_id}", response_model=List[InstagramStory])
@save_response("instagram_stories")
async def get_instagram_stories(
    account_id: str,
    period: str = Query("7d", pattern="^(1d|7d)$"),
    current_user: dict = Depends(get_facebook_user)
):
    """Get Instagram Stories insights (limited to 7 days)"""
    try:
        from instagram.instagram_manager import InstagramManager
        instagram_manager = InstagramManager(current_user["email"], auth_manager)
        stories = instagram_manager.get_story_insights(account_id, period)
        return [InstagramStory(**story) for story in stories]
    except Exception as e:
        logger.error(f"Error fetching Instagram stories: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/instagram/audience/{account_id}", response_model=InstagramAudienceDemographics)
@save_response("instagram_audience_demographics")
async def get_instagram_audience_demographics(
    account_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_facebook_user)
):
    """Get Instagram audience demographics"""
    try:
        from instagram.instagram_manager import InstagramManager
        instagram_manager = InstagramManager(current_user["email"], auth_manager)
        demographics = instagram_manager.get_audience_demographics(account_id, period)
        return InstagramAudienceDemographics(**demographics)
    except Exception as e:
        logger.error(f"Error fetching Instagram audience demographics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/instagram/hashtag-performance/{account_id}", response_model=List[InstagramHashtagPerformance])
@save_response("instagram_hashtag_performance")
async def get_instagram_hashtag_performance(
    account_id: str,
    hashtags: List[str],
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_facebook_user)
):
    """Get hashtag performance analysis"""
    try:
        if len(hashtags) > 20:
            raise HTTPException(status_code=400, detail="Maximum 20 hashtags allowed")
        
        from instagram.instagram_manager import InstagramManager
        instagram_manager = InstagramManager(current_user["email"], auth_manager)
        performance = instagram_manager.get_hashtag_performance(account_id, hashtags, period)
        return [InstagramHashtagPerformance(**hashtag) for hashtag in performance]
    except Exception as e:
        logger.error(f"Error fetching Instagram hashtag performance: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/instagram/performance-summary/{account_id}", response_model=InstagramAccountPerformanceSummary)
@save_response("instagram_performance_summary")
async def get_instagram_performance_summary(
    account_id: str,
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_facebook_user)
):
    """Get comprehensive Instagram account performance summary"""
    try:
        from instagram.instagram_manager import InstagramManager
        instagram_manager = InstagramManager(current_user["email"], auth_manager)
        summary = instagram_manager.get_account_performance_summary(account_id, period)
        return InstagramAccountPerformanceSummary(**summary)
    except Exception as e:
        logger.error(f"Error fetching Instagram performance summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# COMBINED SOCIAL MEDIA ENDPOINTS
# =============================================================================

@app.get("/api/social/overview", response_model=SocialMediaOverview)
@save_response("social_media_overview")
async def get_social_media_overview(current_user: dict = Depends(get_facebook_user)):
    """Get combined overview of all social media platforms"""
    try:
        from meta.meta_manager import MetaManager
        from facebook.facebook_manager import FacebookManager
        from instagram.instagram_manager import InstagramManager
        
        # Get data from all platforms
        meta_manager = MetaManager(current_user["email"], auth_manager)
        facebook_manager = FacebookManager(current_user["email"], auth_manager)
        instagram_manager = InstagramManager(current_user["email"], auth_manager)
        
        meta_accounts = meta_manager.get_ad_accounts()
        facebook_pages = facebook_manager.get_user_pages()
        instagram_accounts = instagram_manager.get_instagram_business_accounts()
        
        # Calculate totals
        total_followers = sum(page.get('followers_count', 0) for page in facebook_pages)
        total_followers += sum(account.get('followers_count', 0) for account in instagram_accounts)
        
        return SocialMediaOverview(
            facebook_pages=[FacebookPage(**page) for page in facebook_pages],
            instagram_accounts=[InstagramAccount(**account) for account in instagram_accounts],
            meta_ad_accounts=[MetaAdAccount(**account) for account in meta_accounts],
            total_followers=total_followers,
            total_reach=0,  # Would need to aggregate from insights
            total_engagement=0,  # Would need to aggregate from insights
            generated_at=datetime.now().isoformat()
        )
    except Exception as e:
        logger.error(f"Error fetching social media overview: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/social/insights-summary", response_model=SocialMediaInsightsSummary)
@save_response("social_insights_summary")
async def get_social_insights_summary(
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_facebook_user)
):
    """Get cross-platform social media insights summary"""
    try:
        from facebook.facebook_manager import FacebookManager
        from instagram.instagram_manager import InstagramManager
        
        facebook_manager = FacebookManager(current_user["email"], auth_manager)
        instagram_manager = InstagramManager(current_user["email"], auth_manager)
        
        platforms = []
        total_reach = 0
        total_engagement = 0
        
        # Get Facebook data
        try:
            facebook_pages = facebook_manager.get_user_pages()
            for page in facebook_pages:
                insights = facebook_manager.get_page_insights(page['id'], period)
                platform_data = CrossPlatformEngagement(
                    platform=f"Facebook - {page['name']}",
                    followers=page.get('followers_count', 0),
                    engagement_rate=insights.get('engagement_rate', 0),
                    total_engagement=insights.get('post_engagements', 0),
                    reach=insights.get('total_reach', 0),
                    impressions=insights.get('total_impressions', 0)
                )
                platforms.append(platform_data)
                total_reach += insights.get('total_reach', 0)
                total_engagement += insights.get('post_engagements', 0)
        except Exception as e:
            logger.warning(f"Could not fetch Facebook data: {e}")
        
        # Get Instagram data
        try:
            instagram_accounts = instagram_manager.get_instagram_business_accounts()
            for account in instagram_accounts:
                insights = instagram_manager.get_account_insights(account['id'], period)
                summary = instagram_manager.get_account_performance_summary(account['id'], period)
                
                platform_data = CrossPlatformEngagement(
                    platform=f"Instagram - {account['username']}",
                    followers=account.get('followers_count', 0),
                    engagement_rate=summary.get('overall_engagement_rate', 0),
                    total_engagement=summary.get('total_media_engagement', 0),
                    reach=insights.get('reach', 0),
                    impressions=insights.get('impressions', 0)
                )
                platforms.append(platform_data)
                total_reach += insights.get('reach', 0)
                total_engagement += summary.get('total_media_engagement', 0)
        except Exception as e:
            logger.warning(f"Could not fetch Instagram data: {e}")
        
        # Calculate overall metrics
        overall_engagement_rate = 0
        top_platform = ""
        
        if platforms:
            # Find top performing platform by engagement rate
            top_platform_data = max(platforms, key=lambda x: x.engagement_rate)
            top_platform = top_platform_data.platform
            
            # Calculate weighted average engagement rate
            total_followers = sum(p.followers for p in platforms)
            if total_followers > 0:
                weighted_engagement = sum(p.engagement_rate * p.followers for p in platforms)
                overall_engagement_rate = weighted_engagement / total_followers
        
        return SocialMediaInsightsSummary(
            period=period,
            platforms=platforms,
            top_performing_platform=top_platform,
            overall_engagement_rate=round(overall_engagement_rate, 2),
            total_social_reach=total_reach,
            total_social_engagement=total_engagement,
            generated_at=datetime.now().isoformat()
        )
    except Exception as e:
        logger.error(f"Error fetching social insights summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/api/instagram/test-connection")
async def test_instagram_connection(current_user: dict = Depends(get_facebook_user)):
    """Test Instagram API connection and permissions"""
    try:
        from instagram.instagram_manager import InstagramManager
        instagram_manager = InstagramManager(current_user["email"], auth_manager)
        
        # Test basic API call
        accounts = instagram_manager.get_instagram_business_accounts()
        
        return {
            "status": "success",
            "message": "Instagram API connection successful",
            "accounts_found": len(accounts),
            "user_email": current_user["email"],
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Instagram API connection test failed: {e}")
        return {
            "status": "error",
            "message": str(e),
            "user_email": current_user["email"],
            "timestamp": datetime.now().isoformat()
        }


# Google Ads Routes
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
    period: str = Query("LAST_30_DAYS", pattern="^(LAST_7_DAYS|LAST_30_DAYS|LAST_90_DAYS|LAST_365_DAYS)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get Google Ads key statistics for dashboard cards"""
    try:
        ads_manager = GoogleAdsManager(current_user["email"], auth_manager)
        key_stats = ads_manager.get_overall_key_stats(customer_id, period)
        return AdKeyStats(**key_stats)
    except Exception as e:
        logger.error(f"Error fetching key stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ads/campaigns/{customer_id}", response_model=List[EnhancedAdCampaign])
@save_response("ads_campaigns")
async def get_ads_campaigns(
    customer_id: str,
    period: str = Query("LAST_30_DAYS", pattern="^(LAST_7_DAYS|LAST_30_DAYS|LAST_90_DAYS|LAST_365_DAYS)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get Google Ads campaigns for a customer"""
    try:
        ads_manager = GoogleAdsManager(current_user["email"], auth_manager)
        campaigns = ads_manager.get_campaigns_with_period(customer_id, period)
        return [EnhancedAdCampaign(**campaign) for campaign in campaigns]
    except Exception as e:
        logger.error(f"Error fetching campaigns: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ads/keywords/{customer_id}", response_model=KeywordResponse)
@save_response("ads_keywords")    
async def get_ads_keywords(
    customer_id: str,
    period: str = Query("LAST_30_DAYS", pattern="^(LAST_7_DAYS|LAST_30_DAYS|LAST_90_DAYS|LAST_365_DAYS)$"),
    offset: int = Query(0, ge=0),
    limit: int = Query(10, ge=1, le=100),
    current_user: dict = Depends(get_current_user)
):
    """Get Google Ads keywords data with pagination"""
    try:
        ads_manager = GoogleAdsManager(current_user["email"], auth_manager)
        result = ads_manager.get_keywords_data(customer_id, period, offset, limit)
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
    period: str = Query("LAST_30_DAYS", pattern="^(LAST_7_DAYS|LAST_30_DAYS|LAST_90_DAYS|LAST_365_DAYS)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get Google Ads performance metrics"""
    try:
        ads_manager = GoogleAdsManager(current_user["email"], auth_manager)
        metrics = ads_manager.get_advanced_metrics(customer_id, period)
        return [PerformanceMetric(**metric) for metric in metrics]
    except Exception as e:
        logger.error(f"Error fetching ads performance: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ads/geographic/{customer_id}", response_model=List[GeographicPerformance])
@save_response("ads_geographic_performance")
async def get_ads_geographic(
    customer_id: str,
    period: str = Query("LAST_30_DAYS", pattern="^(LAST_7_DAYS|LAST_30_DAYS|LAST_90_DAYS|LAST_365_DAYS)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get Google Ads geographic performance"""
    try:
        ads_manager = GoogleAdsManager(current_user["email"], auth_manager)
        geo_data = ads_manager.get_geographic_data(customer_id, period)
        return [GeographicPerformance(**geo) for geo in geo_data]
    except Exception as e:
        logger.error(f"Error fetching ads geographic data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ads/device-performance/{customer_id}", response_model=List[DevicePerformance])
@save_response("ads_device_performance")
async def get_ads_device_performance(
    customer_id: str,
    period: str = Query("LAST_30_DAYS", pattern="^(LAST_7_DAYS|LAST_30_DAYS|LAST_90_DAYS|LAST_365_DAYS)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get Google Ads device performance"""
    try:
        ads_manager = GoogleAdsManager(current_user["email"], auth_manager)
        device_data = ads_manager.get_device_performance_data(customer_id, period)
        return [DevicePerformance(**device) for device in device_data]
    except Exception as e:
        logger.error(f"Error fetching device performance: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ads/time-performance/{customer_id}", response_model=List[TimePerformance])
@save_response("ads_time_performance")
async def get_ads_time_performance(
    customer_id: str,
    period: str = Query("LAST_30_DAYS", pattern="^(LAST_7_DAYS|LAST_30_DAYS|LAST_90_DAYS|LAST_365_DAYS)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get Google Ads time-based performance"""
    try:
        ads_manager = GoogleAdsManager(current_user["email"], auth_manager)
        time_data = ads_manager.get_time_performance_data(customer_id, period)
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





# Error handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.detail, "status_code": exc.status_code}
    )

@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    logger.exception(f"Unhandled error: {exc}")
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "detail": str(exc)}
    )



# Chat endpoints
@app.post("/api/chat/message", response_model=ChatResponse)
async def send_chat_message(
    chat_request: ChatRequest,
    current_user: dict = Depends(get_current_user_enhanced)
):
    """Enhanced chat message endpoint"""
    try:
        # Validate auth provider for module type
        auth_provider = current_user.get("auth_provider", "google")
        
        if chat_request.module_type == ModuleType.META and auth_provider != "facebook":
            raise HTTPException(status_code=403, detail="Facebook authentication required for Meta module")
        
        if chat_request.module_type in [ModuleType.GOOGLE_ADS, ModuleType.GOOGLE_ANALYTICS] and auth_provider != "google":
            raise HTTPException(status_code=403, detail="Google authentication required for this module")
        
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
    """Get specific conversation by session ID - FIXED VERSION"""
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
    """Get list of chat sessions for a module - IMPROVED VERSION"""
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



