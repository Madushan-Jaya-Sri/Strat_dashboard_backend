"""
Meta Ads Module Agents for LangGraph Chat System
Implements the hierarchical granularity workflow for Meta Ads
"""

import logging
import json
import re
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import os
from openai import OpenAI

# Initialize logger
logger = logging.getLogger(__name__)

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Default model
DEFAULT_MODEL = "gpt-4-turbo-preview"


# ============================================================================
# AGENT 2: GRANULARITY CHECK AGENT (META ADS SPECIFIC)
# ============================================================================

def agent_2_meta_granularity_check(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Agent 2 for Meta Ads: Determine granularity level (account, campaign, adset, ad)

    This agent analyzes the query ONCE to categorize into exactly one level of depth.
    Looks for mentions (explicit or implied) of campaigns/adsets/ads.
    Prioritizes deepest match; if ambiguous, defaults to account level.

    Args:
        state: Current chat state

    Returns:
        Updated state with granularity_level set
    """
    logger.info("=== AGENT 2: Meta Ads Granularity Check ===")
    state["current_agent"] = "agent_2_meta_granularity_check"

    user_question = state.get("user_question", "")

    # If granularity_level is already set (from previous run in multi-step flow), preserve it
    if state.get("granularity_level"):
        logger.info(f"Granularity already set: {state['granularity_level']} (preserving from previous state)")
        return state

    # If we already have selections from context (initial request), use them to determine granularity
    # This only applies when IDs are provided in the initial context, not during selection flow
    if state.get("ad_ids"):
        state["granularity_level"] = "ad"
        logger.info("Granularity determined from context: ad level")
        return state
    elif state.get("adset_ids"):
        state["granularity_level"] = "adset"
        logger.info("Granularity determined from context: adset level")
        return state
    elif state.get("campaign_ids"):
        state["granularity_level"] = "campaign"
        logger.info("Granularity determined from context: campaign level")
        return state

    try:
        system_prompt = """You are a granularity detection agent for Meta Ads analytics.

Your job is to determine the DEPTH LEVEL of the user's query based on what entities they mention.

Granularity Levels (from shallow to deep):
1. ACCOUNT - No mention of campaigns, adsets, or ads. Overall account metrics.
   Examples: "total spend", "overall performance", "account ROAS", "all my ads performance"

2. CAMPAIGN - Mentions campaigns but NOT adsets or ads.
   Examples: "campaign performance", "Merdeka campaign", "how's my campaign doing", "campaign spend"

3. ADSET - Mentions adsets (or ad sets) but NOT individual ads.
   Examples: "adset performance", "IG traffic adsets", "adset reach", "budget left in adsets"

4. AD - Mentions specific ads or ad creatives.
   Examples: "ad clicks", "Datuk Seri ad", "ad creative performance", "this ad's CTR"

IMPORTANT RULES:
- Prioritize the DEEPEST level mentioned
- If "ad" is mentioned → return "ad"
- If "adset" is mentioned but not "ad" → return "adset"
- If "campaign" is mentioned but not "adset" or "ad" → return "campaign"
- If nothing specific mentioned → return "account"
- If ambiguous, default to "account" and suggest clarification

Return ONLY a JSON object with this structure:
{
    "granularity_level": "account|campaign|adset|ad",
    "confidence": "high|medium|low",
    "reasoning": "brief explanation",
    "needs_clarification": true/false,
    "suggested_clarification": "question to ask user if needs_clarification is true"
}"""

        user_prompt = f"Analyze this query and determine the granularity level: {user_question}"

        response = client.chat.completions.create(
            model=DEFAULT_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.1,
            max_tokens=300,
            response_format={"type": "json_object"}
        )

        result = json.loads(response.choices[0].message.content)

        granularity = result.get("granularity_level", "account")
        confidence = result.get("confidence", "low")
        reasoning = result.get("reasoning", "")
        needs_clarification = result.get("needs_clarification", False)

        logger.info(f"Granularity detected: {granularity} (confidence: {confidence})")
        logger.info(f"Reasoning: {reasoning}")

        # If low confidence or needs clarification, ask user
        if needs_clarification or confidence == "low":
            state["needs_user_input"] = True
            clarification = result.get("suggested_clarification",
                "Please specify what level you'd like to analyze: campaigns, adsets, or individual ads?")
            state["user_clarification_prompt"] = clarification
            logger.info(f"Requesting clarification: {clarification}")
            return state

        state["granularity_level"] = granularity

        # Extract time period and metrics as well
        state = extract_time_and_metrics(state, user_question)

        return state

    except Exception as e:
        logger.error(f"Error in granularity check: {e}")
        state["errors"].append(f"Granularity check failed: {str(e)}")
        # Default to account level
        state["granularity_level"] = "account"
        return state


def extract_time_and_metrics(state: Dict[str, Any], user_question: str) -> Dict[str, Any]:
    """
    Extract time period and metrics from the user question

    Args:
        state: Current state
        user_question: User's question

    Returns:
        Updated state with time and metrics
    """
    try:
        system_prompt = f"""Extract time period and metrics from the user's question.

Return a JSON object with:
{{
    "has_time_period": true/false,
    "time_period_text": "extracted text" or null,
    "period_keyword": "last 7 days|last 30 days|last week|last month|yesterday" or null,
    "start_date": "YYYY-MM-DD" or null,
    "end_date": "YYYY-MM-DD" or null,
    "metrics_requested": ["spend", "clicks", "reach", etc.] or []
}}

Current date is: {datetime.now().strftime('%Y-%m-%d')}

For Meta Ads, common time periods are:
- last 7 days, last 30 days, last 90 days, last 365 days
- yesterday, this week, this month
- If no period mentioned, set has_time_period to false"""

        response = client.chat.completions.create(
            model=DEFAULT_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Extract from: {user_question}"}
            ],
            temperature=0.1,
            max_tokens=300,
            response_format={"type": "json_object"}
        )

        result = json.loads(response.choices[0].message.content)

        # Process time period
        if result.get("has_time_period"):
            period_keyword = result.get("period_keyword")

            if period_keyword:
                # Convert to Meta Ads period format (7d, 30d, 90d, 365d)
                period_map = {
                    "last 7 days": "7d",
                    "last week": "7d",
                    "last 30 days": "30d",
                    "last month": "30d",
                    "last 90 days": "90d",
                    "last 365 days": "365d",
                    "last year": "365d"
                }

                normalized_keyword = period_keyword.lower()
                if normalized_keyword in period_map:
                    state["period"] = period_map[normalized_keyword]
                    logger.info(f"Time period set to: {state['period']}")

            # Use explicit dates if provided
            if result.get("start_date") and result.get("end_date"):
                state["start_date"] = result["start_date"]
                state["end_date"] = result["end_date"]
        else:
            # Default to last 30 days if no period specified
            if not state.get("period") and not state.get("start_date"):
                state["period"] = "30d"
                logger.info("No time period specified, defaulting to last 30 days")

        # Store extracted metrics
        if result.get("metrics_requested"):
            state["extracted_metrics"] = result["metrics_requested"]
            logger.info(f"Metrics requested: {state['extracted_metrics']}")

        return state

    except Exception as e:
        logger.error(f"Error extracting time and metrics: {e}")
        # Set defaults
        if not state.get("period") and not state.get("start_date"):
            state["period"] = "30d"
        return state


# ============================================================================
# AGENT 3: META ADS DATA FETCH & ANALYSIS
# ============================================================================

def agent_3_meta_data_fetch_and_analysis(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Agent 3 for Meta Ads: Fetch data based on granularity level

    Workflow:
    - Account level: Call account insights directly
    - Campaign level: Get campaigns list → wait for selection → fetch analytics
    - Adset level: Get campaigns → adsets → wait for selection → fetch analytics
    - Ad level: Get campaigns → adsets → ads → wait for selection → fetch analytics

    Args:
        state: Current chat state

    Returns:
        Updated state with API calls made or dropdown options prepared
    """
    logger.info("=== AGENT 3: Meta Ads Data Fetch & Analysis ===")
    state["current_agent"] = "agent_3_meta_data_fetch_and_analysis"

    granularity = state.get("granularity_level", "account")
    logger.info(f"Processing granularity level: {granularity}")

    # Route to appropriate handler based on granularity
    if granularity == "account":
        return handle_account_level(state)
    elif granularity == "campaign":
        return handle_campaign_level(state)
    elif granularity == "adset":
        return handle_adset_level(state)
    elif granularity == "ad":
        return handle_ad_level(state)
    else:
        logger.error(f"Unknown granularity level: {granularity}")
        state["errors"].append(f"Unknown granularity level: {granularity}")
        return state


def handle_account_level(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle account-level queries
    Directly call account insights endpoint
    """
    logger.info("Handling account-level query")

    # Select account insights endpoint
    account_endpoint = next(
        (ep for ep in state.get("available_endpoints", [])
         if ep["name"] == "get_meta_account_insights"),
        None
    )

    if not account_endpoint:
        state["errors"].append("Account insights endpoint not found")
        return state

    state["selected_endpoints"] = [account_endpoint]
    logger.info("Selected endpoint: get_meta_account_insights")

    return state


def handle_campaign_level(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle campaign-level queries

    Flow:
    1. If no campaign_ids: Fetch campaigns list → send dropdown → wait
    2. If campaign_ids present: Select analytics endpoints and fetch
    """
    logger.info("Handling campaign-level query")

    # Check if we already have campaign_ids selected
    if state.get("campaign_ids"):
        logger.info(f"Campaign IDs already selected: {state['campaign_ids']}")
        return select_campaign_analytics_endpoints(state)

    # Need to fetch campaigns list for selection
    logger.info("No campaign IDs - fetching campaigns list")

    campaigns_list_endpoint = next(
        (ep for ep in state.get("available_endpoints", [])
         if ep["name"] == "get_meta_campaigns_list"),
        None
    )

    if not campaigns_list_endpoint:
        state["errors"].append("Campaigns list endpoint not found")
        return state

    state["selected_endpoints"] = [campaigns_list_endpoint]
    state["awaiting_campaign_selection"] = True
    logger.info("Will fetch campaigns list and wait for user selection")

    return state


def handle_adset_level(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle adset-level queries

    Flow:
    1. If no campaign_ids: Fetch campaigns → wait
    2. If campaign_ids but no adset_ids: Fetch adsets → wait
    3. If adset_ids: Select analytics endpoints and fetch
    """
    logger.info("=" * 80)
    logger.info("📊 HANDLE ADSET LEVEL")
    logger.info(f"   Adset IDs: {state.get('adset_ids')}")
    logger.info(f"   Campaign IDs: {state.get('campaign_ids')}")
    logger.info("=" * 80)

    # Check if we have adset_ids
    if state.get("adset_ids"):
        logger.info(f"✅ Adset IDs already selected: {state['adset_ids']}")
        logger.info("→ Selecting adset analytics endpoints")
        return select_adset_analytics_endpoints(state)

    # Check if we have campaign_ids to fetch adsets
    if state.get("campaign_ids"):
        logger.info(f"✅ Have campaign IDs: {state['campaign_ids']}")
        logger.info("→ Fetching adsets for selected campaigns")

        adsets_endpoint = next(
            (ep for ep in state.get("available_endpoints", [])
             if ep["name"] == "get_campaigns_adsets"),
            None
        )

        if not adsets_endpoint:
            state["errors"].append("Adsets endpoint not found")
            return state

        state["selected_endpoints"] = [adsets_endpoint]
        state["awaiting_adset_selection"] = True
        logger.info("✅ Selected endpoint: get_campaigns_adsets")
        logger.info("✅ Set awaiting_adset_selection = True")
        logger.info("→ Will fetch adsets and wait for user selection")
        logger.info("=" * 80)

        return state

    # Need to fetch campaigns first
    logger.info("⚠️ No campaign IDs - need to fetch campaigns list first")

    campaigns_list_endpoint = next(
        (ep for ep in state.get("available_endpoints", [])
         if ep["name"] == "get_meta_campaigns_list"),
        None
    )

    if not campaigns_list_endpoint:
        state["errors"].append("Campaigns list endpoint not found")
        return state

    state["selected_endpoints"] = [campaigns_list_endpoint]
    state["awaiting_campaign_selection"] = True
    logger.info("Will fetch campaigns list and wait for selection before fetching adsets")

    return state


def handle_ad_level(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle ad-level queries

    Flow:
    1. If no campaign_ids: Fetch campaigns → wait
    2. If campaign_ids but no adset_ids: Fetch adsets → wait
    3. If adset_ids but no ad_ids: Fetch ads → wait
    4. If ad_ids: Select analytics endpoints and fetch
    """
    logger.info("Handling ad-level query")

    # Check if we have ad_ids
    if state.get("ad_ids"):
        logger.info(f"Ad IDs already selected: {state['ad_ids']}")
        return select_ad_analytics_endpoints(state)

    # Check if we have adset_ids to fetch ads
    if state.get("adset_ids"):
        logger.info(f"Have adset IDs, fetching ads: {state['adset_ids']}")

        ads_endpoint = next(
            (ep for ep in state.get("available_endpoints", [])
             if ep["name"] == "get_adsets_ads"),
            None
        )

        if not ads_endpoint:
            state["errors"].append("Ads endpoint not found")
            return state

        state["selected_endpoints"] = [ads_endpoint]
        state["awaiting_ad_selection"] = True
        logger.info("Will fetch ads and wait for user selection")

        return state

    # Check if we have campaign_ids to fetch adsets
    if state.get("campaign_ids"):
        logger.info(f"Have campaign IDs, fetching adsets: {state['campaign_ids']}")

        adsets_endpoint = next(
            (ep for ep in state.get("available_endpoints", [])
             if ep["name"] == "get_campaigns_adsets"),
            None
        )

        if not adsets_endpoint:
            state["errors"].append("Adsets endpoint not found")
            return state

        state["selected_endpoints"] = [adsets_endpoint]
        state["awaiting_adset_selection"] = True
        logger.info("Will fetch adsets and wait for selection before fetching ads")

        return state

    # Need to fetch campaigns first
    logger.info("No campaign IDs - fetching campaigns list first")

    campaigns_list_endpoint = next(
        (ep for ep in state.get("available_endpoints", [])
         if ep["name"] == "get_meta_campaigns_list"),
        None
    )

    if not campaigns_list_endpoint:
        state["errors"].append("Campaigns list endpoint not found")
        return state

    state["selected_endpoints"] = [campaigns_list_endpoint]
    state["awaiting_campaign_selection"] = True
    logger.info("Will fetch campaigns list first")

    return state


def select_campaign_analytics_endpoints(state: Dict[str, Any]) -> Dict[str, Any]:
    """Select appropriate analytics endpoints for campaigns based on query"""
    logger.info("Selecting campaign analytics endpoints")

    user_question = state.get("user_question", "").lower()
    extracted_metrics = state.get("extracted_metrics", [])

    selected_endpoints = []

    # Use LLM to select appropriate endpoints
    try:
        available_analytics = [
            ep for ep in state.get("available_endpoints", [])
            if ep["name"] in ["get_campaigns_timeseries", "get_campaigns_demographics", "get_campaigns_placements"]
        ]

        endpoint_descriptions = "\n".join([
            f"- {ep['name']}: {ep['description']}" for ep in available_analytics
        ])

        system_prompt = f"""You are an endpoint selector for Meta Ads campaign analytics.

Available analytics endpoints:
{endpoint_descriptions}

Based on the user's question and requested metrics, select which endpoint(s) to call.
- timeseries: For trends over time, daily/weekly data
- demographics: For age/gender breakdown
- placements: For platform/placement performance (Facebook, Instagram, etc.)

Return JSON:
{{
    "selected_endpoints": ["endpoint_name1", "endpoint_name2"],
    "reasoning": "brief explanation"
}}

If unclear, select timeseries as default."""

        response = client.chat.completions.create(
            model=DEFAULT_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Question: {user_question}\nMetrics: {extracted_metrics}"}
            ],
            temperature=0.2,
            max_tokens=200,
            response_format={"type": "json_object"}
        )

        result = json.loads(response.choices[0].message.content)
        endpoint_names = result.get("selected_endpoints", ["get_campaigns_timeseries"])

        logger.info(f"LLM selected endpoints: {endpoint_names}")
        logger.info(f"Reasoning: {result.get('reasoning', '')}")

        for name in endpoint_names:
            endpoint = next((ep for ep in available_analytics if ep["name"] == name), None)
            if endpoint:
                selected_endpoints.append(endpoint)

    except Exception as e:
        logger.error(f"Error selecting endpoints: {e}")
        # Fallback to timeseries
        timeseries_ep = next(
            (ep for ep in state.get("available_endpoints", [])
             if ep["name"] == "get_campaigns_timeseries"),
            None
        )
        if timeseries_ep:
            selected_endpoints.append(timeseries_ep)

    if not selected_endpoints:
        state["errors"].append("No analytics endpoints selected")

    state["selected_endpoints"] = selected_endpoints
    logger.info(f"Selected {len(selected_endpoints)} analytics endpoint(s)")

    return state


def select_adset_analytics_endpoints(state: Dict[str, Any]) -> Dict[str, Any]:
    """Select appropriate analytics endpoints for adsets based on query"""
    logger.info("=" * 80)
    logger.info("📊 SELECTING ADSET ANALYTICS ENDPOINTS")
    logger.info(f"   Adset IDs: {state.get('adset_ids')}")
    logger.info("=" * 80)

    user_question = state.get("user_question", "").lower()
    extracted_metrics = state.get("extracted_metrics", [])

    selected_endpoints = []

    try:
        available_analytics = [
            ep for ep in state.get("available_endpoints", [])
            if ep["name"] in ["get_adsets_timeseries", "get_adsets_demographics", "get_adsets_placements"]
        ]

        endpoint_descriptions = "\n".join([
            f"- {ep['name']}: {ep['description']}" for ep in available_analytics
        ])

        system_prompt = f"""You are an endpoint selector for Meta Ads adset analytics.

Available analytics endpoints:
{endpoint_descriptions}

Select which endpoint(s) to call based on the query.

Return JSON:
{{
    "selected_endpoints": ["endpoint_name1", "endpoint_name2"],
    "reasoning": "brief explanation"
}}"""

        response = client.chat.completions.create(
            model=DEFAULT_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Question: {user_question}\nMetrics: {extracted_metrics}"}
            ],
            temperature=0.2,
            max_tokens=200,
            response_format={"type": "json_object"}
        )

        result = json.loads(response.choices[0].message.content)
        endpoint_names = result.get("selected_endpoints", ["get_adsets_timeseries"])

        logger.info(f"🧠 LLM selected adset endpoints: {endpoint_names}")
        logger.info(f"   Reasoning: {result.get('reasoning', '')}")

        for name in endpoint_names:
            endpoint = next((ep for ep in available_analytics if ep["name"] == name), None)
            if endpoint:
                selected_endpoints.append(endpoint)

    except Exception as e:
        logger.error(f"❌ Error selecting endpoints: {e}")
        # Fallback
        timeseries_ep = next(
            (ep for ep in state.get("available_endpoints", [])
             if ep["name"] == "get_adsets_timeseries"),
            None
        )
        if timeseries_ep:
            selected_endpoints.append(timeseries_ep)
            logger.info("⚠️ Using fallback: get_adsets_timeseries")

    state["selected_endpoints"] = selected_endpoints
    logger.info(f"✅ Selected {len(selected_endpoints)} adset analytics endpoint(s):")
    for ep in selected_endpoints:
        logger.info(f"   - {ep['name']}")
    logger.info("=" * 80)

    return state


def select_ad_analytics_endpoints(state: Dict[str, Any]) -> Dict[str, Any]:
    """Select appropriate analytics endpoints for ads based on query"""
    logger.info("Selecting ad analytics endpoints")

    user_question = state.get("user_question", "").lower()
    extracted_metrics = state.get("extracted_metrics", [])

    selected_endpoints = []

    try:
        available_analytics = [
            ep for ep in state.get("available_endpoints", [])
            if ep["name"] in ["get_ads_timeseries", "get_ads_demographics", "get_ads_placements"]
        ]

        endpoint_descriptions = "\n".join([
            f"- {ep['name']}: {ep['description']}" for ep in available_analytics
        ])

        system_prompt = f"""You are an endpoint selector for Meta Ads individual ad analytics.

Available analytics endpoints:
{endpoint_descriptions}

Select which endpoint(s) to call based on the query.

Return JSON:
{{
    "selected_endpoints": ["endpoint_name1", "endpoint_name2"],
    "reasoning": "brief explanation"
}}"""

        response = client.chat.completions.create(
            model=DEFAULT_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Question: {user_question}\nMetrics: {extracted_metrics}"}
            ],
            temperature=0.2,
            max_tokens=200,
            response_format={"type": "json_object"}
        )

        result = json.loads(response.choices[0].message.content)
        endpoint_names = result.get("selected_endpoints", ["get_ads_timeseries"])

        for name in endpoint_names:
            endpoint = next((ep for ep in available_analytics if ep["name"] == name), None)
            if endpoint:
                selected_endpoints.append(endpoint)

    except Exception as e:
        logger.error(f"Error selecting endpoints: {e}")
        # Fallback
        timeseries_ep = next(
            (ep for ep in state.get("available_endpoints", [])
             if ep["name"] == "get_ads_timeseries"),
            None
        )
        if timeseries_ep:
            selected_endpoints.append(timeseries_ep)

    state["selected_endpoints"] = selected_endpoints
    return state
