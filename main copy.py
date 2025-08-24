#!/usr/bin/env python3
"""
Unified Marketing Dashboard Backend
Combines Google Ads and Google Analytics data in a single FastAPI application
"""

from fastapi import FastAPI, HTTPException, Query, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, JSONResponse, HTMLResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import uvicorn
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv

# Import our custom modules
from auth.auth_manager import AuthManager
from google_ads.ads_manager import GoogleAdsManager
from google_analytics.ga4_manager import GA4Manager
from intent_insights.intent_manager import IntentManager
from models.response_models import *
from utils.helpers import get_date_range, get_country_location_id, validate_timeframe

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

# Dependency to get current user
async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    """Get current authenticated user"""
    return auth_manager.verify_jwt_token(credentials.credentials)

# Add this endpoint to your main.py file, just before the health check endpoint

@app.get("/", response_class=HTMLResponse)
async def auth_frontend():
    """Serve the automated authentication frontend"""
    html_content = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Marketing Dashboard - Auto Auth</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        
        .container {
            background: white;
            border-radius: 16px;
            padding: 2rem;
            box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.1);
            max-width: 500px;
            width: 90%;
            text-align: center;
        }
        
        .logo {
            font-size: 2rem;
            font-weight: 700;
            color: #4f46e5;
            margin-bottom: 0.5rem;
        }
        
        .subtitle {
            color: #6b7280;
            margin-bottom: 2rem;
        }
        
        .status {
            padding: 1rem;
            border-radius: 8px;
            margin: 1rem 0;
            font-weight: 500;
        }
        
        .status.loading {
            background: #fef3c7;
            color: #92400e;
            border: 1px solid #fbbf24;
        }
        
        .status.success {
            background: #d1fae5;
            color: #065f46;
            border: 1px solid #10b981;
        }
        
        .status.error {
            background: #fee2e2;
            color: #991b1b;
            border: 1px solid #ef4444;
        }
        
        .btn {
            background: #4f46e5;
            color: white;
            border: none;
            padding: 12px 24px;
            border-radius: 8px;
            font-size: 1rem;
            font-weight: 500;
            cursor: pointer;
            transition: all 0.3s ease;
            text-decoration: none;
            display: inline-block;
            margin: 0.5rem;
        }
        
        .btn:hover {
            background: #4338ca;
            transform: translateY(-1px);
        }
        
        .btn:disabled {
            background: #9ca3af;
            cursor: not-allowed;
            transform: none;
        }
        
        .token-display {
            background: #f9fafb;
            border: 1px solid #d1d5db;
            border-radius: 8px;
            padding: 1rem;
            margin: 1rem 0;
            font-family: 'Monaco', 'Consolas', monospace;
            font-size: 0.8rem;
            word-break: break-all;
            max-height: 200px;
            overflow-y: auto;
            text-align: left;
        }
        
        .copy-btn {
            background: #059669;
            font-size: 0.9rem;
            padding: 8px 16px;
        }
        
        .copy-btn:hover {
            background: #047857;
        }
        
        .endpoints {
            text-align: left;
            background: #f8fafc;
            border-radius: 8px;
            padding: 1rem;
            margin-top: 1rem;
        }
        
        .endpoint {
            margin: 0.5rem 0;
            font-family: 'Monaco', 'Consolas', monospace;
            font-size: 0.85rem;
            color: #374151;
        }
        
        .spinner {
            border: 3px solid #f3f3f3;
            border-top: 3px solid #4f46e5;
            border-radius: 50%;
            width: 24px;
            height: 24px;
            animation: spin 1s linear infinite;
            display: inline-block;
            margin-right: 8px;
        }
        
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        
        .test-results {
            background: #f9fafb;
            border: 1px solid #d1d5db;
            border-radius: 8px;
            padding: 1rem;
            margin: 1rem 0;
            max-height: 300px;
            overflow-y: auto;
            text-align: left;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="logo">🚀 Marketing Dashboard</div>
        <div class="subtitle">Automated Authentication System</div>
        
        <div id="status" class="status success">
            Ready to authenticate with Google
        </div>
        
        <div id="auth-section">
            <button id="login-btn" class="btn" onclick="startAuth()">
                🔑 Sign in with Google
            </button>
        </div>
        
        <div id="token-section" style="display: none;">
            <h3>✅ Authentication Successful!</h3>
            <p>Your Bearer Token (auto-copied to clipboard):</p>
            <div id="token-display" class="token-display"></div>
            <button class="btn copy-btn" onclick="copyToken()">📋 Copy Token Again</button>
            
            <div class="endpoints">
                <h4>🔌 Available API Endpoints:</h4>
                <div class="endpoint">GET /api/ads/customers</div>
                <div class="endpoint">GET /api/analytics/properties</div>
                <div class="endpoint">GET /api/ads/campaigns/{customer_id}</div>
                <div class="endpoint">GET /api/analytics/metrics/{property_id}</div>
                <div class="endpoint">POST /api/intent/keyword-insights/{customer_id}</div>
            </div>
            
            <div style="margin-top: 1rem;">
                <button class="btn" onclick="testCustomers()">🧪 Test Get Customers</button>
                <button class="btn" onclick="testProperties()">📊 Test Get Properties</button>
            </div>
            <div id="test-results"></div>
        </div>
    </div>

    <script>
        const API_BASE_URL = window.location.origin;
        let currentToken = null;
        
        // Check URL parameters on page load
        window.addEventListener('load', function() {
            checkForCallback();
        });
        
        function checkForCallback() {
            const urlParams = new URLSearchParams(window.location.search);
            const code = urlParams.get('code');
            const state = urlParams.get('state');
            
            if (code && state) {
                // We're in the callback, exchange code for token
                exchangeCodeForToken(code, state);
            }
        }
        
        async function startAuth() {
            try {
                updateStatus('Contacting authentication server...', 'loading');
                
                const response = await fetch(`${API_BASE_URL}/auth/login`);
                const data = await response.json();
                
                if (data.auth_url) {
                    updateStatus('Redirecting to Google...', 'loading');
                    // Redirect to Google OAuth
                    window.location.href = data.auth_url;
                } else {
                    throw new Error('No auth URL received');
                }
            } catch (error) {
                updateStatus(`Authentication error: ${error.message}`, 'error');
            }
        }
        
        async function exchangeCodeForToken(code, state) {
            try {
                updateStatus('Processing authentication...', 'loading');
                
                const response = await fetch(`${API_BASE_URL}/auth/callback?code=${code}&state=${state}`);
                
                if (response.headers.get('content-type')?.includes('text/html')) {
                    // Handle HTML response (original callback)
                    const html = await response.text();
                    const tokenMatch = html.match(/<textarea[^>]*>(.*?)<\/textarea>/s);
                    
                    if (tokenMatch) {
                        const token = tokenMatch[1].trim();
                        showToken(token);
                    } else {
                        throw new Error('Token not found in response');
                    }
                } else {
                    // Handle JSON response
                    const data = await response.json();
                    if (data.token) {
                        showToken(data.token);
                    } else {
                        throw new Error('No token in response');
                    }
                }
            } catch (error) {
                updateStatus(`Token exchange failed: ${error.message}`, 'error');
            }
        }
        
        function showToken(token) {
            currentToken = token;
            
            // Update status
            updateStatus('🎉 Authentication completed successfully!', 'success');
            
            // Show token section
            document.getElementById('token-section').style.display = 'block';
            document.getElementById('token-display').textContent = token;
            
            // Hide auth section
            document.getElementById('auth-section').style.display = 'none';
            
            // Auto-copy token to clipboard
            navigator.clipboard.writeText(token).then(() => {
                console.log('✅ Token automatically copied to clipboard');
            }).catch(() => {
                console.log('❌ Could not auto-copy token');
            });
            
            // Clear URL parameters
            window.history.replaceState({}, document.title, window.location.pathname);
        }
        
        function copyToken() {
            if (currentToken) {
                navigator.clipboard.writeText(currentToken).then(() => {
                    alert('✅ Token copied to clipboard!');
                }).catch(() => {
                    alert('❌ Could not copy token');
                });
            }
        }
        
        function updateStatus(message, type) {
            const statusEl = document.getElementById('status');
            statusEl.className = `status ${type}`;
            
            if (type === 'loading') {
                statusEl.innerHTML = `<div class="spinner"></div>${message}`;
            } else {
                statusEl.innerHTML = message;
            }
        }
        
        // Test functions
        async function testCustomers() {
            if (!currentToken) {
                alert('Please authenticate first');
                return;
            }
            
            try {
                updateTestResults('Testing /api/ads/customers...', 'loading');
                
                const response = await fetch(`${API_BASE_URL}/api/ads/customers`, {
                    headers: {
                        'Authorization': `Bearer ${currentToken}`,
                        'Content-Type': 'application/json'
                    }
                });
                
                const data = await response.json();
                
                if (response.ok) {
                    updateTestResults(`✅ Customers API Test Successful:\\n${JSON.stringify(data, null, 2)}`, 'success');
                } else {
                    updateTestResults(`❌ Customers API Error:\\n${JSON.stringify(data, null, 2)}`, 'error');
                }
                
            } catch (error) {
                updateTestResults(`❌ Network Error: ${error.message}`, 'error');
            }
        }
        
        async function testProperties() {
            if (!currentToken) {
                alert('Please authenticate first');
                return;
            }
            
            try {
                updateTestResults('Testing /api/analytics/properties...', 'loading');
                
                const response = await fetch(`${API_BASE_URL}/api/analytics/properties`, {
                    headers: {
                        'Authorization': `Bearer ${currentToken}`,
                        'Content-Type': 'application/json'
                    }
                });
                
                const data = await response.json();
                
                if (response.ok) {
                    updateTestResults(`✅ Properties API Test Successful:\\n${JSON.stringify(data, null, 2)}`, 'success');
                } else {
                    updateTestResults(`❌ Properties API Error:\\n${JSON.stringify(data, null, 2)}`, 'error');
                }
                
            } catch (error) {
                updateTestResults(`❌ Network Error: ${error.message}`, 'error');
            }
        }
        
        function updateTestResults(message, type) {
            const resultsEl = document.getElementById('test-results');
            resultsEl.className = `test-results status ${type}`;
            resultsEl.innerHTML = `<pre>${message}</pre>`;
        }
    </script>
</body>
</html>"""
    return HTMLResponse(content=html_content)

# Health check
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": ["google_ads", "google_analytics", "intent_insights"]
    }

# Authentication Routes
@app.get("/auth/login")
async def login():
    """Initiate Google OAuth login"""
    return await auth_manager.initiate_login()

@app.get("/auth/callback")
async def auth_callback(code: str, state: Optional[str] = None):
    result = await auth_manager.handle_callback(code, state)
    
    # Return HTML with JavaScript to display token
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head><title>Authentication Success</title></head>
    <body>
        <h2>Authentication Successful!</h2>
        <p>Copy your JWT token:</p>
        <textarea rows="10" cols="80">{result['token']}</textarea>
        <script>
            console.log('JWT Token:', '{result['token']}');
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

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

# Google Ads Routes
@app.get("/api/ads/customers", response_model=List[AdCustomer])
async def get_ads_customers(current_user: dict = Depends(get_current_user)):
    """Get accessible Google Ads customer accounts"""
    try:
        ads_manager = GoogleAdsManager(current_user["email"], auth_manager)
        customers = ads_manager.get_accessible_customers()
        return [AdCustomer(**customer) for customer in customers]
    except Exception as e:
        logger.error(f"Error fetching ads customers: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ads/campaigns/{customer_id}", response_model=List[AdCampaign])
async def get_ads_campaigns(
    customer_id: str,
    period: str = Query("LAST_30_DAYS", pattern="^(LAST_7_DAYS|LAST_30_DAYS|LAST_90_DAYS|LAST_365_DAYS)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get Google Ads campaigns for a customer"""
    try:
        ads_manager = GoogleAdsManager(current_user["email"], auth_manager)
        campaigns = ads_manager.get_campaigns_with_period(customer_id, period)
        return [AdCampaign(**campaign) for campaign in campaigns]
    except Exception as e:
        logger.error(f"Error fetching campaigns: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ads/keywords/{customer_id}", response_model=KeywordResponse)
async def get_ads_keywords(
    customer_id: str,
    period: str = Query("LAST_30_DAYS", pattern="^(LAST_7_DAYS|LAST_30_DAYS|LAST_90_DAYS|LAST_365_DAYS)$"),
    offset: int = Query(0, ge=0),
    limit: int = Query(10, ge=1, le=50),
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

@app.get("/api/analytics/channel-performance/{property_id}", response_model=List[GAChannelPerformance])
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

@app.get("/api/combined/roas-roi-metrics", response_model=GACombinedROASROIMetrics)
async def get_combined_roas_roi_metrics(
    ga_property_id: str = Query(..., description="GA4 Property ID"),
    ads_customer_id: str = Query(..., description="Google Ads Customer ID"),
    period: str = Query("30d", pattern="^(7d|30d|90d|365d)$"),
    current_user: dict = Depends(get_current_user)
):
    """Get combined ROAS and ROI metrics from GA4 and Google Ads"""
    try:
        ga4_manager = GA4Manager(current_user["email"])
        metrics = ga4_manager.get_combined_roas_roi_metrics(ga_property_id, ads_customer_id, period)
        return GACombinedROASROIMetrics(**metrics)
    except Exception as e:
        logger.error(f"Error fetching combined ROAS/ROI metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# # Intent Insights Routes with Enhanced Timeframe Validation
# @app.post("/api/intent/keyword-insights/{customer_id}", response_model=KeywordInsightsResponse)
# async def get_keyword_insights(
#     customer_id: str,
#     request_data: KeywordInsightRequest,
#     current_user: dict = Depends(get_current_user)
# ):
#     """Get comprehensive keyword insights with search volumes and trends"""
#     try:
#         # Validate seed keywords limit
#         if len(request_data.seed_keywords) > 10:
#             raise HTTPException(status_code=400, detail="Maximum 10 seed keywords allowed")
        
#         # Validate timeframe with enhanced validation
#         if not validate_timeframe(request_data.timeframe, request_data.start_date, request_data.end_date):
#             if request_data.timeframe == "custom":
#                 current_month = datetime.now().strftime("%B %Y")
#                 raise HTTPException(
#                     status_code=400, 
#                     detail=f"Invalid timeframe. Data for {current_month} is not yet complete. Please ensure your end date is before the current month."
#                 )
#             else:
#                 raise HTTPException(status_code=400, detail="Invalid timeframe parameters")
        
#         intent_manager = IntentManager(current_user["email"], auth_manager)
        
#         insights = intent_manager.get_keyword_insights(
#             customer_id,
#             request_data.seed_keywords,
#             request_data.country,
#             request_data.timeframe,
#             request_data.start_date,
#             request_data.end_date
#         )
        
#         return KeywordInsightsResponse(**insights)
        
#     except Exception as e:
#         logger.error(f"Error fetching keyword insights: {e}")
#         if isinstance(e, HTTPException):
#             raise e
#         raise HTTPException(status_code=500, detail=str(e))  

# Alternative: Return raw JSON without Pydantic validation

@app.post("/api/intent/keyword-insights/{customer_id}")
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
