"""
Meta Ads LangGraph Workflow
Implements the hierarchical granularity workflow for Meta Ads module
"""

import logging
from typing import Dict, Any, Literal
from langgraph.graph import StateGraph, END

from chat.states.chat_states import create_initial_state, ModuleType
from chat.agents.shared_agents import (
    agent_1_intent_classification,
    direct_llm_response,
    agent_5_data_processing_and_analysis,
    agent_6_response_formatting
)
from chat.agents.meta_agents import (
    agent_2_meta_granularity_check,
    agent_3_meta_data_fetch_and_analysis
)
from chat.utils.api_client import agent_4_api_execution

# Initialize logger
logger = logging.getLogger(__name__)


# ============================================================================
# CONDITIONAL ROUTING FUNCTIONS
# ============================================================================

def route_after_intent_classification(state: Dict[str, Any]) -> Literal["direct_llm_response", "granularity_check"]:
    """Route based on intent type"""
    intent = state.get("intent_type", "analytical")

    if intent == "chitchat":
        logger.info("→ Routing to direct_llm_response (chitchat)")
        return "direct_llm_response"
    else:
        logger.info("→ Routing to granularity_check (analytical)")
        return "granularity_check"


def route_after_granularity_check(state: Dict[str, Any]) -> Literal["data_fetch", "end"]:
    """Route after granularity check - check if clarification needed"""
    if state.get("needs_user_input"):
        logger.info("→ Routing to END (needs user clarification)")
        return "end"
    else:
        logger.info("→ Routing to data_fetch")
        return "data_fetch"


def route_after_data_fetch(state: Dict[str, Any]) -> Literal["api_execution", "end"]:
    """Route after data fetch planning"""
    # Check if we're awaiting user selection
    if (state.get("awaiting_campaign_selection") or
        state.get("awaiting_adset_selection") or
        state.get("awaiting_ad_selection")):
        logger.info("→ Routing to api_execution (will fetch list data)")
        return "api_execution"

    # Check if we have endpoints selected for analytics
    if state.get("selected_endpoints") and len(state.get("selected_endpoints", [])) > 0:
        logger.info("→ Routing to api_execution")
        return "api_execution"

    logger.warning("→ No endpoints selected, routing to END")
    return "end"


def route_after_api_execution(state: Dict[str, Any]) -> Literal["process_selection_response", "data_analysis", "end"]:
    """Route after API execution"""

    # Check if we just fetched campaigns list and need to prepare dropdown
    if state.get("awaiting_campaign_selection"):
        logger.info("→ Routing to process_selection_response (campaigns)")
        return "process_selection_response"

    # Check if we just fetched adsets and need to prepare dropdown
    if state.get("awaiting_adset_selection"):
        logger.info("→ Routing to process_selection_response (adsets)")
        return "process_selection_response"

    # Check if we just fetched ads and need to prepare dropdown
    if state.get("awaiting_ad_selection"):
        logger.info("→ Routing to process_selection_response (ads)")
        return "process_selection_response"

    # Otherwise, we have analytics data to process
    if state.get("endpoint_responses") and len(state.get("endpoint_responses", [])) > 0:
        logger.info("→ Routing to data_analysis")
        return "data_analysis"

    logger.warning("→ No data to process, routing to END")
    return "end"


def route_after_data_analysis(state: Dict[str, Any]) -> Literal["response_formatting", "end"]:
    """Route after data analysis"""
    if state.get("llm_insights"):
        logger.info("→ Routing to response_formatting")
        return "response_formatting"
    else:
        logger.warning("→ No insights generated, routing to END")
        return "end"


# ============================================================================
# SELECTION RESPONSE PROCESSOR
# ============================================================================

def process_selection_response(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process API responses that return selection lists (campaigns, adsets, ads)
    and prepare dropdown options for frontend
    """
    logger.info("=== Processing Selection Response ===")
    state["current_agent"] = "process_selection_response"

    endpoint_responses = state.get("endpoint_responses", [])

    if not endpoint_responses:
        logger.error("No endpoint responses to process")
        state["errors"].append("No data received for selection")
        state["needs_user_input"] = False
        return state

    # Get the last response (most recent API call)
    last_response = endpoint_responses[-1]
    endpoint_name = last_response.get("endpoint", "")
    data = last_response.get("data", {})

    logger.info(f"Processing response from: {endpoint_name}")

    # Handle campaigns list response
    if state.get("awaiting_campaign_selection"):
        campaigns = data.get("campaigns", [])
        logger.info(f"Found {len(campaigns)} campaigns")

        if not campaigns:
            state["formatted_response"] = "No campaigns found in your account. Please check your Meta Ads account."
            state["is_complete"] = True
            state["awaiting_campaign_selection"] = False
            return state

        # Prepare dropdown options
        campaign_options = [
            {
                "id": campaign.get("id"),
                "name": campaign.get("name"),
                "status": campaign.get("status")
            }
            for campaign in campaigns
        ]

        state["campaign_options"] = campaign_options
        state["needs_user_input"] = True
        state["awaiting_campaign_selection"] = True

        # Prepare user prompt
        state["user_clarification_prompt"] = {
            "type": "campaign_selection",
            "message": "Please select one or more campaigns to analyze:",
            "options": campaign_options,
            "selection_type": "multi"  # Can select multiple campaigns
        }

        logger.info(f"Prepared {len(campaign_options)} campaign options for selection")
        return state

    # Handle adsets list response
    elif state.get("awaiting_adset_selection"):
        # Response is a list of adsets
        adsets = data if isinstance(data, list) else []
        logger.info(f"Found {len(adsets)} adsets")

        if not adsets:
            state["formatted_response"] = "No adsets found for the selected campaigns."
            state["is_complete"] = True
            state["awaiting_adset_selection"] = False
            return state

        # Prepare dropdown options
        adset_options = [
            {
                "id": adset.get("id"),
                "name": adset.get("name"),
                "campaign_id": adset.get("campaign_id"),
                "status": adset.get("status")
            }
            for adset in adsets
        ]

        state["adset_options"] = adset_options
        state["needs_user_input"] = True
        state["awaiting_adset_selection"] = True

        # Prepare user prompt
        state["user_clarification_prompt"] = {
            "type": "adset_selection",
            "message": "Please select one or more adsets to analyze:",
            "options": adset_options,
            "selection_type": "multi"
        }

        logger.info(f"Prepared {len(adset_options)} adset options for selection")
        return state

    # Handle ads list response
    elif state.get("awaiting_ad_selection"):
        # Response is a list of ads
        ads = data if isinstance(data, list) else []
        logger.info(f"Found {len(ads)} ads")

        if not ads:
            state["formatted_response"] = "No ads found for the selected adsets."
            state["is_complete"] = True
            state["awaiting_ad_selection"] = False
            return state

        # Prepare dropdown options with creative info
        ad_options = [
            {
                "id": ad.get("id"),
                "name": ad.get("name"),
                "adset_id": ad.get("ad_set_id"),
                "status": ad.get("status"),
                "creative": ad.get("creative", {}),
                "preview_link": ad.get("preview_link")
            }
            for ad in ads
        ]

        state["ad_options"] = ad_options
        state["needs_user_input"] = True
        state["awaiting_ad_selection"] = True

        # Prepare user prompt
        state["user_clarification_prompt"] = {
            "type": "ad_selection",
            "message": "Please select one or more ads to analyze:",
            "options": ad_options,
            "selection_type": "multi"
        }

        logger.info(f"Prepared {len(ad_options)} ad options for selection")
        return state

    # Should not reach here
    logger.warning("Unknown selection state")
    return state


# ============================================================================
# BUILD THE GRAPH
# ============================================================================

def build_meta_ads_graph() -> StateGraph:
    """
    Build the Meta Ads workflow graph

    Flow:
    1. Intent Classification (chitchat vs analytical)
    2. If chitchat → Direct LLM response
    3. If analytical → Granularity Check (account/campaign/adset/ad)
    4. Data Fetch Planning (determine what to fetch)
    5. API Execution
    6. Process Selection (if awaiting user input) OR Data Analysis
    7. Response Formatting
    """
    logger.info("Building Meta Ads LangGraph workflow")

    # Create graph
    workflow = StateGraph(Dict[str, Any])

    # Add nodes
    workflow.add_node("intent_classification", agent_1_intent_classification)
    workflow.add_node("direct_llm_response", direct_llm_response)
    workflow.add_node("granularity_check", agent_2_meta_granularity_check)
    workflow.add_node("data_fetch", agent_3_meta_data_fetch_and_analysis)
    workflow.add_node("api_execution", agent_4_api_execution)
    workflow.add_node("process_selection_response", process_selection_response)
    workflow.add_node("data_analysis", agent_5_data_processing_and_analysis)
    workflow.add_node("response_formatting", agent_6_response_formatting)

    # Set entry point
    workflow.set_entry_point("intent_classification")

    # Add conditional edges
    workflow.add_conditional_edges(
        "intent_classification",
        route_after_intent_classification,
        {
            "direct_llm_response": "direct_llm_response",
            "granularity_check": "granularity_check"
        }
    )

    workflow.add_conditional_edges(
        "granularity_check",
        route_after_granularity_check,
        {
            "data_fetch": "data_fetch",
            "end": END
        }
    )

    workflow.add_conditional_edges(
        "data_fetch",
        route_after_data_fetch,
        {
            "api_execution": "api_execution",
            "end": END
        }
    )

    workflow.add_conditional_edges(
        "api_execution",
        route_after_api_execution,
        {
            "process_selection_response": "process_selection_response",
            "data_analysis": "data_analysis",
            "end": END
        }
    )

    workflow.add_conditional_edges(
        "data_analysis",
        route_after_data_analysis,
        {
            "response_formatting": "response_formatting",
            "end": END
        }
    )

    # Add edges to END
    workflow.add_edge("direct_llm_response", END)
    workflow.add_edge("process_selection_response", END)
    workflow.add_edge("response_formatting", END)

    logger.info("Meta Ads graph built successfully")

    return workflow.compile()


# ============================================================================
# MAIN EXECUTION FUNCTION
# ============================================================================

async def run_meta_ads_chat(
    user_question: str,
    session_id: str,
    user_email: str,
    auth_token: str,
    context: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Execute the Meta Ads chat workflow

    Args:
        user_question: User's question
        session_id: Session identifier
        user_email: User's email
        auth_token: Meta auth token
        context: Additional context (account_id, period, campaign_ids, etc.)

    Returns:
        Final state with response
    """
    logger.info("=" * 80)
    logger.info("Starting Meta Ads Chat Workflow")
    logger.info(f"Question: {user_question}")
    logger.info(f"Session: {session_id}")
    logger.info(f"Context: {context}")
    logger.info("=" * 80)

    try:
        # Create initial state
        initial_state = create_initial_state(
            user_question=user_question,
            module_type=ModuleType.META_ADS.value,
            session_id=session_id,
            user_email=user_email,
            auth_token=auth_token,
            context=context
        )

        # Build and run graph
        graph = build_meta_ads_graph()

        # Execute workflow
        final_state = await graph.ainvoke(initial_state)

        logger.info("=" * 80)
        logger.info("Meta Ads Chat Workflow Completed")
        logger.info(f"Is Complete: {final_state.get('is_complete')}")
        logger.info(f"Needs User Input: {final_state.get('needs_user_input')}")
        logger.info(f"Errors: {final_state.get('errors', [])}")
        logger.info("=" * 80)

        return final_state

    except Exception as e:
        logger.error(f"Error in Meta Ads chat workflow: {e}", exc_info=True)
        return {
            "formatted_response": f"I apologize, but I encountered an error: {str(e)}",
            "errors": [str(e)],
            "is_complete": True,
            "module_type": ModuleType.META_ADS.value,
            "session_id": session_id,
            "user_question": user_question,
            "triggered_endpoints": [],
            "warnings": []
        }
