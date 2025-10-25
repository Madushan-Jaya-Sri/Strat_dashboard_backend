"""
Chat State Models for LangGraph Multi-Module Chat System
This file defines the state structures used across all chat modules
"""

from typing import TypedDict, List, Optional, Dict, Any, Literal
from datetime import datetime
from enum import Enum


class ModuleType(str, Enum):
    """Supported module types"""
    GOOGLE_ADS = "google_ads"
    GOOGLE_ANALYTICS = "google_analytics"
    INTENT_INSIGHTS = "intent_insights"
    META_ADS = "meta_ads"
    FACEBOOK = "facebook"
    INSTAGRAM = "instagram"


class IntentType(str, Enum):
    """User intent classification"""
    CHITCHAT = "chitchat"
    ANALYTICAL = "analytical"


class GranularityLevel(str, Enum):
    """Meta Ads granularity levels"""
    ACCOUNT = "account"
    CAMPAIGN = "campaign"
    ADSET = "adset"
    AD = "ad"
    UNKNOWN = "unknown"


# ============================================================================
# BASE CHAT STATE (Used by all modules)
# ============================================================================

class BaseChatState(TypedDict):
    """
    Base state structure shared across all modules
    Contains common fields needed by all chat workflows
    """
    # ===== USER INPUT =====
    user_question: str
    module_type: str  # ModuleType enum value
    session_id: str
    user_email: str
    
    # ===== AUTHENTICATION =====
    auth_token: str  # Google or Meta auth token depending on module
    
    # ===== INTENT CLASSIFICATION =====
    intent_type: Optional[str]  # IntentType enum value
    
    # ===== TIME PARAMETERS =====
    start_date: Optional[str]  # Format: YYYY-MM-DD
    end_date: Optional[str]    # Format: YYYY-MM-DD
    period: Optional[str]      # e.g., "LAST_7_DAYS", "LAST_30_DAYS", "day", "week", "month"
    
    # ===== ENDPOINT MANAGEMENT =====
    selected_endpoints: List[str]  # List of endpoint paths to call
    endpoint_responses: List[Dict[str, Any]]  # Responses from API calls
    triggered_endpoints: List[Dict[str, Any]]  # Detailed log of all API calls made
    
    # ===== LLM OUTPUTS =====
    llm_insights: Optional[str]  # Raw insights from LLM
    formatted_response: Optional[str]  # Final formatted response for frontend
    visualizations: Optional[Dict[str, Any]]  # Chart/table data for frontend
    
    # ===== ERROR HANDLING =====
    errors: List[str]  # List of errors encountered
    warnings: List[str]  # List of warnings
    
    # ===== METADATA =====
    current_agent: Optional[str]  # Track which agent is processing
    processing_start_time: Optional[datetime]
    processing_end_time: Optional[datetime]
    
    # ===== FLAGS =====
    needs_user_input: bool  # If True, wait for user clarification
    user_clarification_prompt: Optional[str]  # Question to ask user
    is_complete: bool  # If True, workflow is done


# ============================================================================
# GOOGLE ADS STATE
# ============================================================================

class GoogleAdsState(BaseChatState):
    """State for Google Ads module"""
    # Google Ads specific fields
    customer_id: Optional[str]  # Google Ads customer ID
    campaign_ids: Optional[List[str]]
    ad_group_ids: Optional[List[str]]
    keyword_ids: Optional[List[str]]
    
    # Extracted parameters
    location: Optional[str]
    device: Optional[str]
    
    # Available endpoints for this module
    available_endpoints: List[Dict[str, Any]]


# ============================================================================
# GOOGLE ANALYTICS STATE
# ============================================================================

class GoogleAnalyticsState(BaseChatState):
    """State for Google Analytics (GA4) module"""
    # GA4 specific fields
    property_id: Optional[str]  # GA4 property ID
    
    # Extracted parameters
    dimension: Optional[str]  # e.g., 'country', 'city', 'deviceCategory'
    metric: Optional[str]     # e.g., 'sessions', 'users', 'conversions'
    
    # Available endpoints for this module
    available_endpoints: List[Dict[str, Any]]


# ============================================================================
# INTENT INSIGHTS STATE
# ============================================================================

class IntentInsightsState(BaseChatState):
    """State for Intent Insights module"""
    # Intent specific fields
    account_id: Optional[str]  # Google Ads account ID used for intent
    seed_keywords: Optional[List[str]]
    country: Optional[str]
    include_zero_volume: bool
    
    # Timeframe specific to intent
    timeframe: Optional[str]  # e.g., 'LAST_7_DAYS', 'LAST_30_DAYS'
    
    # Flags
    needs_api_call: bool  # Whether to call keyword insights endpoint
    
    # Available endpoint (single endpoint for this module)
    available_endpoints: List[Dict[str, Any]]


# ============================================================================
# META ADS STATE (Most Complex)
# ============================================================================

class MetaAdsState(BaseChatState):
    """State for Meta Ads module with hierarchical structure"""
    # Meta Ads specific fields
    account_id: Optional[str]  # Meta ad account ID (e.g., 'act_123456')
    
    # Hierarchical selections
    granularity_level: Optional[str]  # GranularityLevel enum value
    campaign_ids: Optional[List[str]]
    campaign_names: Optional[List[str]]
    adset_ids: Optional[List[str]]
    adset_names: Optional[List[str]]
    ad_ids: Optional[List[str]]
    ad_names: Optional[List[str]]
    
    # Lists for dropdown selections
    available_campaigns: Optional[List[Dict[str, Any]]]
    available_adsets: Optional[List[Dict[str, Any]]]
    available_ads: Optional[List[Dict[str, Any]]]
    
    # Flags
    is_account_level: bool
    is_campaign_level: bool
    is_adset_level: bool
    is_ad_level: bool
    campaigns_loading: bool  # Special flag for long campaign list loading
    needs_campaign_selection: bool
    needs_adset_selection: bool
    needs_ad_selection: bool
    
    # Status filter for campaigns
    status_filter: Optional[List[str]]  # ['ACTIVE', 'PAUSED', 'ARCHIVED']
    
    # Available endpoints for this module
    available_endpoints: List[Dict[str, Any]]


# ============================================================================
# FACEBOOK PAGE STATE
# ============================================================================

class FacebookPageState(BaseChatState):
    """State for Facebook Page Analytics module"""
    # Facebook specific fields
    page_id: Optional[str]  # Facebook page ID
    
    # Extracted parameters
    metric_type: Optional[str]  # e.g., 'engagement', 'reach', 'impressions'
    post_limit: Optional[int]   # Number of posts to analyze
    
    # Available endpoints for this module
    available_endpoints: List[Dict[str, Any]]


# ============================================================================
# INSTAGRAM STATE
# ============================================================================

class InstagramState(BaseChatState):
    """State for Instagram Analytics module"""
    # Instagram specific fields
    account_id: Optional[str]  # Instagram business account ID
    
    # Extracted parameters
    media_type: Optional[str]  # e.g., 'IMAGE', 'VIDEO', 'CAROUSEL_ALBUM'
    media_limit: Optional[int]  # Number of media to analyze
    
    # Available endpoints for this module
    available_endpoints: List[Dict[str, Any]]


# ============================================================================
# HELPER FUNCTIONS FOR STATE INITIALIZATION
# ============================================================================

def create_initial_state(
    user_question: str,
    module_type: str,
    session_id: str,
    user_email: str,
    auth_token: str,
    context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Create initial state for a chat workflow based on module type
    
    Args:
        user_question: The user's question
        module_type: Type of module (google_ads, ga4, etc.)
        session_id: Unique session identifier
        user_email: User's email address
        auth_token: Authentication token
        context: Additional context from frontend
    
    Returns:
        Initial state dictionary for the specified module
    """
    context = context or {}
    
    # Base state common to all modules
    base_state = {
        "user_question": user_question,
        "module_type": module_type,
        "session_id": session_id,
        "user_email": user_email,
        "auth_token": auth_token,
        "intent_type": None,
        "start_date": context.get("start_date"),
        "end_date": context.get("end_date"),
        "period": context.get("period"),
        "selected_endpoints": [],
        "endpoint_responses": [],
        "triggered_endpoints": [],
        "llm_insights": None,
        "formatted_response": None,
        "visualizations": None,
        "errors": [],
        "warnings": [],
        "current_agent": None,
        "processing_start_time": datetime.utcnow(),
        "processing_end_time": None,
        "needs_user_input": False,
        "user_clarification_prompt": None,
        "is_complete": False,
    }
    
    # Module-specific initialization
    if module_type == ModuleType.GOOGLE_ADS.value:
        base_state.update({
            "customer_id": context.get("customer_id"),
            "campaign_ids": None,
            "ad_group_ids": None,
            "keyword_ids": None,
            "location": None,
            "device": None,
            "available_endpoints": get_google_ads_endpoints()
        })
    
    elif module_type == ModuleType.GOOGLE_ANALYTICS.value:
        base_state.update({
            "property_id": context.get("property_id"),
            "dimension": None,
            "metric": None,
            "available_endpoints": get_ga4_endpoints()
        })
    
    elif module_type == ModuleType.INTENT_INSIGHTS.value:
        base_state.update({
            "account_id": context.get("account_id"),
            "seed_keywords": context.get("seed_keywords", []),
            "country": context.get("country"),
            "include_zero_volume": context.get("include_zero_volume", True),
            "timeframe": None,
            "needs_api_call": False,
            "available_endpoints": get_intent_endpoints()
        })
    
    elif module_type == ModuleType.META_ADS.value:
        base_state.update({
            "account_id": context.get("account_id"),
            "granularity_level": None,
            "campaign_ids": None,
            "campaign_names": None,
            "adset_ids": None,
            "adset_names": None,
            "ad_ids": None,
            "ad_names": None,
            "available_campaigns": None,
            "available_adsets": None,
            "available_ads": None,
            "is_account_level": False,
            "is_campaign_level": False,
            "is_adset_level": False,
            "is_ad_level": False,
            "campaigns_loading": False,
            "needs_campaign_selection": False,
            "needs_adset_selection": False,
            "needs_ad_selection": False,
            "status_filter": None,
            "available_endpoints": get_meta_ads_endpoints()
        })
    
    elif module_type == ModuleType.FACEBOOK.value:
        base_state.update({
            "page_id": context.get("page_id"),
            "metric_type": None,
            "post_limit": 10,
            "available_endpoints": get_facebook_endpoints()
        })
    
    elif module_type == ModuleType.INSTAGRAM.value:
        base_state.update({
            "account_id": context.get("account_id"),
            "media_type": None,
            "media_limit": 10,
            "available_endpoints": get_instagram_endpoints()
        })
    
    return base_state


# ============================================================================
# ENDPOINT DEFINITIONS (Will be populated from main.py)
# ============================================================================

def get_google_ads_endpoints() -> List[Dict[str, Any]]:
    """Get available Google Ads endpoints"""
    return [
        {'name': 'get_customers', 'path': '/api/ads/customers', 'method': 'GET', 'params': []},
        {'name': 'get_key_stats', 'path': '/api/ads/key-stats/{customer_id}', 'method': 'GET', 'params': ['customer_id', 'period', 'start_date', 'end_date']},
        {'name': 'get_campaigns', 'path': '/api/ads/campaigns/{customer_id}', 'method': 'GET', 'params': ['customer_id', 'period', 'start_date', 'end_date']},
        {'name': 'get_keywords', 'path': '/api/ads/keywords/{customer_id}', 'method': 'GET', 'params': ['customer_id', 'period', 'start_date', 'end_date']},
        {'name': 'get_performance', 'path': '/api/ads/performance/{customer_id}', 'method': 'GET', 'params': ['customer_id', 'period', 'start_date', 'end_date']},
        {'name': 'get_geographic', 'path': '/api/ads/geographic/{customer_id}', 'method': 'GET', 'params': ['customer_id', 'period', 'start_date', 'end_date']},
        {'name': 'get_device_performance', 'path': '/api/ads/device-performance/{customer_id}', 'method': 'GET', 'params': ['customer_id', 'period', 'start_date', 'end_date']},
        {'name': 'get_time_performance', 'path': '/api/ads/time-performance/{customer_id}', 'method': 'GET', 'params': ['customer_id', 'period', 'start_date', 'end_date']},
        {'name': 'get_keyword_ideas', 'path': '/api/ads/keyword-ideas/{customer_id}', 'method': 'POST', 'params': ['customer_id'], 'body_params': ['keywords', 'location', 'language']},
    ]


def get_ga4_endpoints() -> List[Dict[str, Any]]:
    """Get available Google Analytics endpoints"""
    return [
        {'name': 'get_properties', 'path': '/api/analytics/properties', 'method': 'GET', 'params': []},
        {'name': 'get_metrics', 'path': '/api/analytics/metrics/{property_id}', 'method': 'GET', 'params': ['property_id', 'period', 'start_date', 'end_date']},
        {'name': 'get_traffic_sources', 'path': '/api/analytics/traffic-sources/{property_id}', 'method': 'GET', 'params': ['property_id', 'period', 'start_date', 'end_date']},
        {'name': 'get_top_pages', 'path': '/api/analytics/top-pages/{property_id}', 'method': 'GET', 'params': ['property_id', 'period', 'start_date', 'end_date']},
        {'name': 'get_conversions', 'path': '/api/analytics/conversions/{property_id}', 'method': 'GET', 'params': ['property_id', 'period', 'start_date', 'end_date']},
        {'name': 'get_channel_performance', 'path': '/api/analytics/channel-performance/{property_id}', 'method': 'GET', 'params': ['property_id', 'period', 'start_date', 'end_date']},
        {'name': 'get_audience_insights', 'path': '/api/analytics/audience-insights/{property_id}', 'method': 'GET', 'params': ['property_id', 'period', 'start_date', 'end_date']},
        {'name': 'get_time_series', 'path': '/api/analytics/time-series/{property_id}', 'method': 'GET', 'params': ['property_id', 'period', 'start_date', 'end_date']},
        {'name': 'get_trends', 'path': '/api/analytics/trends/{property_id}', 'method': 'GET', 'params': ['property_id', 'period', 'start_date', 'end_date']},
        {'name': 'get_roas_roi_time_series', 'path': '/api/analytics/roas-roi-time-series/{property_id}', 'method': 'GET', 'params': ['property_id', 'period', 'start_date', 'end_date']},
        {'name': 'get_revenue_breakdown_channel', 'path': '/api/analytics/revenue-breakdown/channel/{property_id}', 'method': 'GET', 'params': ['property_id', 'period', 'start_date', 'end_date']},
        {'name': 'get_revenue_breakdown_source', 'path': '/api/analytics/revenue-breakdown/source/{property_id}', 'method': 'GET', 'params': ['property_id', 'period', 'start_date', 'end_date']},
        {'name': 'get_revenue_breakdown_device', 'path': '/api/analytics/revenue-breakdown/device/{property_id}', 'method': 'GET', 'params': ['property_id', 'period', 'start_date', 'end_date']},
        {'name': 'get_channel_revenue_timeseries', 'path': '/api/analytics/channel-revenue-timeseries/{property_id}', 'method': 'GET', 'params': ['property_id', 'period', 'start_date', 'end_date']},
        {'name': 'post_funnel_analysis', 'path': '/api/analytics/funnel/{property_id}', 'method': 'POST', 'params': ['property_id'], 'body_params': ['steps', 'start_date', 'end_date']},
    ]


def get_intent_endpoints() -> List[Dict[str, Any]]:
    """Get available Intent Insights endpoints"""
    return [
        {'name': 'get_intent_keyword_insights', 'path': '/api/intent/keyword-insights/{account_id}', 'method': 'POST', 'params': ['account_id'], 'body_params': ['seed_keywords', 'country', 'timeframe', 'start_date', 'end_date', 'include_zero_volume'], 'description': 'Get keyword insights and suggestions'},
    ]


def get_meta_ads_endpoints() -> List[Dict[str, Any]]:
    """Get available Meta Ads endpoints"""
    return [
        # Account level
        {'name': 'get_meta_ad_accounts', 'path': '/api/meta/ad-accounts', 'method': 'GET', 'params': []},
        {'name': 'get_meta_account_insights', 'path': '/api/meta/ad-accounts/{account_id}/insights/summary', 'method': 'GET', 'params': ['account_id', 'period', 'start_date', 'end_date']},
        
        # Campaign level
        {'name': 'get_meta_campaigns_list', 'path': '/api/meta/ad-accounts/{account_id}/campaigns/list', 'method': 'GET', 'params': ['account_id'], 'optional_params': ['status']},
        {'name': 'get_meta_campaigns_timeseries', 'path': '/api/meta/campaigns/timeseries', 'method': 'POST', 'params': ['start_date', 'end_date'], 'optional_params': ['period'], 'body_params': ['campaign_ids']},
        {'name': 'get_campaigns_demographics', 'path': '/api/meta/campaigns/demographics', 'method': 'POST', 'params': ['start_date', 'end_date'], 'optional_params': ['period'], 'body_params': ['campaign_ids']},
        {'name': 'get_campaigns_placements', 'path': '/api/meta/campaigns/placements', 'method': 'POST', 'params': ['start_date', 'end_date'], 'optional_params': ['period'], 'body_params': ['campaign_ids']},
        
        # AdSet level
        {'name': 'get_adsets_by_campaigns', 'path': '/api/meta/campaigns/adsets', 'method': 'POST', 'params': [], 'body_params': ['campaign_ids']},
        {'name': 'get_adsets_timeseries', 'path': '/api/meta/adsets/timeseries', 'method': 'POST', 'params': ['start_date', 'end_date'], 'optional_params': ['period'], 'body_params': ['adset_ids']},
        {'name': 'get_adsets_demographics', 'path': '/api/meta/adsets/demographics', 'method': 'POST', 'params': ['start_date', 'end_date'], 'optional_params': ['period'], 'body_params': ['adset_ids']},
        {'name': 'get_adsets_placements', 'path': '/api/meta/adsets/placements', 'method': 'POST', 'params': ['start_date', 'end_date'], 'optional_params': ['period'], 'body_params': ['adset_ids']},
        
        # Ad level
        {'name': 'get_ads_by_adsets', 'path': '/api/meta/adsets/ads', 'method': 'POST', 'params': [], 'body_params': ['adset_ids']},
        {'name': 'get_ads_timeseries', 'path': '/api/meta/ads/timeseries', 'method': 'POST', 'params': ['start_date', 'end_date'], 'optional_params': ['period'], 'body_params': ['ad_ids']},
        {'name': 'get_ads_demographics', 'path': '/api/meta/ads/demographics', 'method': 'POST', 'params': ['start_date', 'end_date'], 'optional_params': ['period'], 'body_params': ['ad_ids']},
        {'name': 'get_ads_placements', 'path': '/api/meta/ads/placements', 'method': 'POST', 'params': ['start_date', 'end_date'], 'optional_params': ['period'], 'body_params': ['ad_ids']},
    ]


def get_facebook_endpoints() -> List[Dict[str, Any]]:
    """Get available Facebook Page endpoints"""
    return [
        {'name': 'get_facebook_pages', 'path': '/api/meta/pages', 'method': 'GET', 'params': []},
        {'name': 'get_facebook_page_insights', 'path': '/api/meta/pages/{page_id}/insights', 'method': 'GET', 'params': ['page_id', 'period', 'start_date', 'end_date']},
        {'name': 'get_facebook_page_posts', 'path': '/api/meta/pages/{page_id}/posts', 'method': 'GET', 'params': ['page_id', 'limit', 'period', 'start_date', 'end_date']},
        {'name': 'get_facebook_demographics', 'path': '/api/meta/pages/{page_id}/demographics', 'method': 'GET', 'params': ['page_id']},
        {'name': 'get_facebook_engagement', 'path': '/api/meta/pages/{page_id}/engagement-breakdown', 'method': 'GET', 'params': ['page_id', 'period', 'start_date', 'end_date']},
        {'name': 'get_meta_page_insights_timeseries', 'path': '/api/meta/pages/{page_id}/insights/timeseries', 'method': 'GET', 'params': ['page_id', 'period', 'start_date', 'end_date']},
        {'name': 'get_meta_page_posts_timeseries', 'path': '/api/meta/pages/{page_id}/posts/timeseries', 'method': 'GET', 'params': ['page_id', 'limit', 'period', 'start_date', 'end_date']},
        {'name': 'get_meta_video_views_breakdown', 'path': '/api/meta/pages/{page_id}/video-views-breakdown', 'method': 'GET', 'params': ['page_id', 'period', 'start_date', 'end_date']},
        {'name': 'get_meta_content_type_breakdown', 'path': '/api/meta/pages/{page_id}/content-type-breakdown', 'method': 'GET', 'params': ['page_id', 'period', 'start_date', 'end_date']},
        {'name': 'get_meta_follows_unfollows', 'path': '/api/meta/pages/{page_id}/follows-unfollows', 'method': 'GET', 'params': ['page_id', 'period', 'start_date', 'end_date']},
        {'name': 'get_meta_organic_vs_paid', 'path': '/api/meta/pages/{page_id}/organic-vs-paid', 'method': 'GET', 'params': ['page_id', 'period', 'start_date', 'end_date']},
    ]


def get_instagram_endpoints() -> List[Dict[str, Any]]:
    """Get available Instagram endpoints"""
    return [
        {'name': 'get_meta_instagram_accounts', 'path': '/api/meta/instagram/accounts', 'method': 'GET', 'params': []},
        {'name': 'get_meta_instagram_insights', 'path': '/api/meta/instagram/{account_id}/insights', 'method': 'GET', 'params': ['account_id', 'period', 'start_date', 'end_date']},
        {'name': 'get_meta_instagram_insights_timeseries', 'path': '/api/meta/instagram/{account_id}/insights/timeseries', 'method': 'GET', 'params': ['account_id', 'period', 'start_date', 'end_date']},
        {'name': 'get_meta_instagram_media', 'path': '/api/meta/instagram/{account_id}/media', 'method': 'GET', 'params': ['account_id', 'limit', 'period', 'start_date', 'end_date']},
        {'name': 'get_meta_instagram_media_timeseries', 'path': '/api/meta/instagram/{account_id}/media/timeseries', 'method': 'GET', 'params': ['account_id', 'limit', 'period', 'start_date', 'end_date']},
    ]


# ============================================================================
# STATE VALIDATION HELPERS
# ============================================================================

def validate_state(state: Dict[str, Any], module_type: str) -> tuple[bool, List[str]]:
    """
    Validate state has required fields for the module
    
    Args:
        state: Current state dictionary
        module_type: Module type to validate against
    
    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []
    
    # Common validations
    if not state.get("user_question"):
        errors.append("Missing user_question")
    
    if not state.get("session_id"):
        errors.append("Missing session_id")
    
    if not state.get("auth_token"):
        errors.append("Missing auth_token")
    
    # Module-specific validations
    if module_type == ModuleType.GOOGLE_ADS.value:
        if not state.get("customer_id"):
            errors.append("Missing customer_id for Google Ads")
    
    elif module_type == ModuleType.GOOGLE_ANALYTICS.value:
        if not state.get("property_id"):
            errors.append("Missing property_id for Google Analytics")
    
    elif module_type == ModuleType.META_ADS.value:
        if not state.get("account_id"):
            errors.append("Missing account_id for Meta Ads")
    
    elif module_type == ModuleType.FACEBOOK.value:
        if not state.get("page_id"):
            errors.append("Missing page_id for Facebook")
    
    elif module_type == ModuleType.INSTAGRAM.value:
        if not state.get("account_id"):
            errors.append("Missing account_id for Instagram")
    
    return len(errors) == 0, errors


def should_ask_for_time_period(state: Dict[str, Any]) -> bool:
    """
    Check if we need to ask user for time period
    
    Args:
        state: Current state
    
    Returns:
        True if time period is missing and needed
    """
    # If intent is chitchat, we don't need time period
    if state.get("intent_type") == IntentType.CHITCHAT.value:
        return False
    
    # Check if we have time parameters
    has_dates = state.get("start_date") and state.get("end_date")
    has_period = state.get("period")
    
    return not (has_dates or has_period)