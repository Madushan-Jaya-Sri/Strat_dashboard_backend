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
    FACEBOOK_ANALYTICS = "facebook_analytics"


class IntentType(str, Enum):
    """User intent classification"""
    CHITCHAT = "chitchat"
    ANALYTICAL = "analytical"


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
    customer_id: Optional[str]  # Google Ads customer ID used for intent (matches API endpoint parameter)
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
# META ADS STATE
# ============================================================================

class MetaAdsState(BaseChatState):
    """State for Meta Ads module"""
    # Meta Ads specific fields
    account_id: Optional[str]  # Meta ad account ID (e.g., act_303894480866908)

    # Hierarchical entity IDs for selection workflow
    campaign_ids: Optional[List[str]]
    adset_ids: Optional[List[str]]
    ad_ids: Optional[List[str]]

    # Granularity level detected from query
    granularity_level: Optional[str]  # "account", "campaign", "adset", "ad"

    # Extracted parameters from query
    extracted_entities: Optional[List[str]]  # Campaign names, adset names mentioned
    extracted_metrics: Optional[List[str]]   # Metrics requested (spend, clicks, etc.)
    extracted_filters: Optional[Dict[str, Any]]

    # Frontend selections metadata
    awaiting_campaign_selection: bool
    awaiting_adset_selection: bool
    awaiting_ad_selection: bool

    # Dropdown options to send to frontend
    campaign_options: Optional[List[Dict[str, Any]]]
    adset_options: Optional[List[Dict[str, Any]]]
    ad_options: Optional[List[Dict[str, Any]]]

    # Available endpoints for this module
    available_endpoints: List[Dict[str, Any]]


# ============================================================================
# FACEBOOK STATE
# ============================================================================

class FacebookState(BaseChatState):
    """State for Facebook Pages module"""
    # Facebook specific fields
    page_id: Optional[str]  # Facebook page ID

    # Extracted parameters from query
    limit: Optional[int]  # For posts limit

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
            "customer_id": context.get("customer_id") or context.get("account_id"),  # Support both for backward compatibility
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
            "campaign_ids": context.get("campaign_ids"),
            "adset_ids": context.get("adset_ids"),
            "ad_ids": context.get("ad_ids"),
            "granularity_level": context.get("granularity_level"),  # Preserve from context (for multi-step flows)
            "extracted_entities": context.get("extracted_entities"),
            "extracted_metrics": context.get("extracted_metrics"),
            "extracted_filters": context.get("extracted_filters"),
            "awaiting_campaign_selection": context.get("awaiting_campaign_selection", False),
            "awaiting_adset_selection": context.get("awaiting_adset_selection", False),
            "awaiting_ad_selection": context.get("awaiting_ad_selection", False),
            "campaign_options": context.get("campaign_options"),
            "adset_options": context.get("adset_options"),
            "ad_options": context.get("ad_options"),
            "available_endpoints": get_meta_ads_endpoints()
        })

    elif module_type == ModuleType.FACEBOOK_ANALYTICS.value:
        base_state.update({
            "page_id": context.get("page_id"),
            "limit": context.get("limit", 10),  # Default limit for posts
            "available_endpoints": get_facebook_endpoints()
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
        {'name': 'get_intent_keyword_insights', 'path': '/api/intent/keyword-insights/{customer_id}', 'method': 'POST', 'params': ['customer_id'], 'body_params': ['seed_keywords', 'country', 'timeframe', 'start_date', 'end_date', 'include_zero_volume'], 'description': 'Get keyword insights and suggestions'},
    ]


def get_meta_ads_endpoints() -> List[Dict[str, Any]]:
    """Get available Meta Ads endpoints"""
    return [
        # Account level
        {'name': 'get_meta_account_insights', 'path': '/api/meta/ad-accounts/{account_id}/insights/summary', 'method': 'GET', 'params': ['account_id', 'period', 'start_date', 'end_date'], 'description': 'Get account-level insights summary'},

        # Campaign level - list endpoints (no time params)
        {'name': 'get_meta_campaigns_list', 'path': '/api/meta/ad-accounts/{account_id}/campaigns/list', 'method': 'GET', 'params': ['account_id', 'status'], 'description': 'Get list of all campaigns without insights'},

        # Campaign level - analytics endpoints (with time params)
        {'name': 'get_campaigns_timeseries', 'path': '/api/meta/campaigns/timeseries', 'method': 'POST', 'params': ['period', 'start_date', 'end_date'], 'body_params': ['campaign_ids'], 'description': 'Get campaign timeseries data'},
        {'name': 'get_campaigns_demographics', 'path': '/api/meta/campaigns/demographics', 'method': 'POST', 'params': ['period', 'start_date', 'end_date'], 'body_params': ['campaign_ids'], 'description': 'Get campaign demographics'},
        {'name': 'get_campaigns_placements', 'path': '/api/meta/campaigns/placements', 'method': 'POST', 'params': ['period', 'start_date', 'end_date'], 'body_params': ['campaign_ids'], 'description': 'Get campaign placements'},

        # Adset level - list endpoints (no time params)
        {'name': 'get_campaigns_adsets', 'path': '/api/meta/campaigns/adsets', 'method': 'POST', 'params': [], 'body_params': ['campaign_ids'], 'description': 'Get adsets for campaigns'},

        # Adset level - analytics endpoints (with time params)
        {'name': 'get_adsets_timeseries', 'path': '/api/meta/adsets/timeseries', 'method': 'POST', 'params': ['period', 'start_date', 'end_date'], 'body_params': ['adset_ids'], 'description': 'Get adset timeseries data'},
        {'name': 'get_adsets_demographics', 'path': '/api/meta/adsets/demographics', 'method': 'POST', 'params': ['period', 'start_date', 'end_date'], 'body_params': ['adset_ids'], 'description': 'Get adset demographics'},
        {'name': 'get_adsets_placements', 'path': '/api/meta/adsets/placements', 'method': 'POST', 'params': ['period', 'start_date', 'end_date'], 'body_params': ['adset_ids'], 'description': 'Get adset placements'},

        # Ad level - list endpoints (no time params)
        {'name': 'get_adsets_ads', 'path': '/api/meta/adsets/ads', 'method': 'POST', 'params': [], 'body_params': ['adset_ids'], 'description': 'Get ads for adsets'},

        # Ad level - analytics endpoints (with time params)
        {'name': 'get_ads_timeseries', 'path': '/api/meta/ads/timeseries', 'method': 'POST', 'params': ['period', 'start_date', 'end_date'], 'body_params': ['ad_ids'], 'description': 'Get ad timeseries data'},
        {'name': 'get_ads_demographics', 'path': '/api/meta/ads/demographics', 'method': 'POST', 'params': ['period', 'start_date', 'end_date'], 'body_params': ['ad_ids'], 'description': 'Get ad demographics'},
        {'name': 'get_ads_placements', 'path': '/api/meta/ads/placements', 'method': 'POST', 'params': ['period', 'start_date', 'end_date'], 'body_params': ['ad_ids'], 'description': 'Get ad placements'},
    ]


def get_facebook_endpoints() -> List[Dict[str, Any]]:
    """Get available Facebook Pages endpoints"""
    return [
        {'name': 'get_facebook_pages', 'path': '/api/meta/pages', 'method': 'GET', 'params': [], 'description': 'Get list of Facebook pages'},
        {'name': 'get_facebook_page_insights', 'path': '/api/meta/pages/{page_id}/insights', 'method': 'GET', 'params': ['page_id', 'period', 'start_date', 'end_date'], 'description': 'Get Facebook page insights and metrics'},
        {'name': 'get_facebook_page_posts', 'path': '/api/meta/pages/{page_id}/posts', 'method': 'GET', 'params': ['page_id', 'limit', 'period', 'start_date', 'end_date'], 'description': 'Get posts from Facebook page'},
        {'name': 'get_facebook_demographics', 'path': '/api/meta/pages/{page_id}/demographics', 'method': 'GET', 'params': ['page_id'], 'description': 'Get page audience demographics'},
        {'name': 'get_facebook_engagement', 'path': '/api/meta/pages/{page_id}/engagement-breakdown', 'method': 'GET', 'params': ['page_id', 'period', 'start_date', 'end_date'], 'description': 'Get engagement breakdown'},
        {'name': 'get_meta_page_insights_timeseries', 'path': '/api/meta/pages/{page_id}/insights/timeseries', 'method': 'GET', 'params': ['page_id', 'period', 'start_date', 'end_date'], 'description': 'Get time-series insights for Facebook page'},
        {'name': 'get_meta_page_posts_timeseries', 'path': '/api/meta/pages/{page_id}/posts/timeseries', 'method': 'GET', 'params': ['page_id', 'limit', 'period', 'start_date', 'end_date'], 'description': 'Get posts with time-series insights'},
        {'name': 'get_meta_video_views_breakdown', 'path': '/api/meta/pages/{page_id}/video-views-breakdown', 'method': 'GET', 'params': ['page_id', 'period', 'start_date', 'end_date'], 'description': 'Get video views breakdown'},
        {'name': 'get_meta_content_type_breakdown', 'path': '/api/meta/pages/{page_id}/content-type-breakdown', 'method': 'GET', 'params': ['page_id', 'period', 'start_date', 'end_date'], 'description': 'Get content type breakdown'},
        {'name': 'get_meta_follows_unfollows', 'path': '/api/meta/pages/{page_id}/follows-unfollows', 'method': 'GET', 'params': ['page_id', 'period', 'start_date', 'end_date'], 'description': 'Get follows and unfollows data'},
        {'name': 'get_meta_organic_vs_paid', 'path': '/api/meta/pages/{page_id}/organic-vs-paid', 'method': 'GET', 'params': ['page_id', 'period', 'start_date', 'end_date'], 'description': 'Get organic vs paid content breakdown'},
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