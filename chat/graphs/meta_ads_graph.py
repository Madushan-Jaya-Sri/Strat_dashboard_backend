"""
Meta Ads LangGraph Implementation
Complete workflow for Meta Ads module with hierarchical flow
Handles: Account → Campaign → AdSet → Ad selection
"""

import logging
from typing import Dict, Any, Literal
from langgraph.graph import StateGraph, END
from chat.states.chat_states import MetaAdsState, create_initial_state
from chat.agents.shared_agents import (
    agent_1_intent_classification,
    direct_llm_response,
    agent_2_parameter_extraction,
    agent_3_endpoint_selection,
    agent_5_data_processing_and_analysis,
    agent_6_response_formatting
)
from chat.agents.meta_agents import (
    meta_agent_3_granularity_detection,
    meta_agent_4_campaign_selection,
    meta_agent_5_campaign_level_decision,
    meta_agent_6_adset_selection,
    meta_agent_7_adset_level_decision,
    meta_agent_8_ad_selection,
    meta_agent_9_ad_level_analysis
)
from chat.utils.api_client import agent_4_api_execution, handle_meta_campaigns_loading

# Initialize logger
logger = logging.getLogger(__name__)


# ============================================================================
# META-SPECIFIC NODE FUNCTIONS
# ============================================================================

def get_account_insights(state: MetaAdsState) -> MetaAdsState:
    """
    Get account-level insights directly
    """
    logger.info("=== GET ACCOUNT INSIGHTS ===")
    state["current_agent"] = "get_account_insights"
    
    # Set endpoint for account insights
    state["selected_endpoints"] = [
        {
            'name': 'get_meta_account_insights',
            'path': '/api/meta/ad-accounts/{account_id}/insights/summary',
            'method': 'GET',
            'params': ['account_id', 'period', 'start_date', 'end_date']
        }
    ]
    
    # Execute API
    state = agent_4_api_execution(state)
    
    return state


# ============================================================================
# ROUTING FUNCTIONS
# ============================================================================

def route_after_intent_classification(
    state: MetaAdsState
) -> Literal["direct_llm", "extract_params"]:
    """Route based on intent type"""
    return "direct_llm" if state.get("intent_type") == "chitchat" else "extract_params"


def route_after_parameter_extraction(
    state: MetaAdsState
) -> Literal["detect_granularity", "wait_for_user"]:
    """Route based on whether we need user input"""
    return "wait_for_user" if state.get("needs_user_input") else "detect_granularity"


def route_after_granularity_detection(
    state: MetaAdsState
) -> Literal["account_insights", "load_campaigns"]:
    """Route based on granularity level"""
    if state.get("is_account_level"):
        logger.info("Route: account_insights (account-level question)")
        return "account_insights"
    else:
        logger.info("Route: load_campaigns (needs campaign data)")
        return "load_campaigns"


def route_after_account_insights(
    state: MetaAdsState
) -> Literal["analyze_data", "error_response"]:
    """Route after getting account insights"""
    return "analyze_data" if state.get("endpoint_responses") else "error_response"


def route_after_campaign_selection(
    state: MetaAdsState
) -> Literal["wait_for_campaign_selection", "campaign_decision"]:
    """Route after loading campaigns"""
    if state.get("needs_campaign_selection"):
        logger.info("Route: wait_for_campaign_selection")
        return "wait_for_campaign_selection"
    elif state.get("campaign_ids"):
        logger.info("Route: campaign_decision")
        return "campaign_decision"
    else:
        logger.info("Route: wait_for_campaign_selection (no campaigns selected)")
        return "wait_for_campaign_selection"


def route_after_campaign_decision(
    state: MetaAdsState
) -> Literal["execute_campaign_apis", "load_adsets"]:
    """Route based on campaign-level decision"""
    if state.get("stop_at_campaign_level"):
        logger.info("Route: execute_campaign_apis (stop at campaign level)")
        return "execute_campaign_apis"
    else:
        logger.info("Route: load_adsets (go deeper)")
        return "load_adsets"


def route_after_adset_selection(
    state: MetaAdsState
) -> Literal["wait_for_adset_selection", "adset_decision"]:
    """Route after loading adsets"""
    if state.get("needs_adset_selection"):
        logger.info("Route: wait_for_adset_selection")
        return "wait_for_adset_selection"
    elif state.get("adset_ids"):
        logger.info("Route: adset_decision")
        return "adset_decision"
    else:
        logger.info("Route: wait_for_adset_selection (no adsets selected)")
        return "wait_for_adset_selection"


def route_after_adset_decision(
    state: MetaAdsState
) -> Literal["execute_adset_apis", "load_ads"]:
    """Route based on adset-level decision"""
    if state.get("stop_at_adset_level"):
        logger.info("Route: execute_adset_apis (stop at adset level)")
        return "execute_adset_apis"
    else:
        logger.info("Route: load_ads (go deeper)")
        return "load_ads"


def route_after_ad_selection(
    state: MetaAdsState
) -> Literal["wait_for_ad_selection", "ad_analysis"]:
    """Route after loading ads"""
    if state.get("needs_ad_selection"):
        logger.info("Route: wait_for_ad_selection")
        return "wait_for_ad_selection"
    elif state.get("ad_ids"):
        logger.info("Route: ad_analysis")
        return "ad_analysis"
    else:
        logger.info("Route: wait_for_ad_selection (no ads selected)")
        return "wait_for_ad_selection"


def route_after_ad_analysis(
    state: MetaAdsState
) -> Literal["execute_ad_apis", "ask_clarification"]:
    """Route based on ad-level analysis"""
    if state.get("can_answer_with_ads"):
        logger.info("Route: execute_ad_apis")
        return "execute_ad_apis"
    else:
        logger.info("Route: ask_clarification")
        return "ask_clarification"


def route_after_api_execution(
    state: MetaAdsState
) -> Literal["analyze_data", "error_response"]:
    """Route based on API execution success"""
    return "analyze_data" if state.get("endpoint_responses") else "error_response"


# ============================================================================
# WRAPPER FUNCTIONS FOR ENDPOINT EXECUTION
# ============================================================================

def execute_campaign_level_apis(state: MetaAdsState) -> MetaAdsState:
    """Execute campaign-level endpoints"""
    logger.info("=== EXECUTE CAMPAIGN-LEVEL APIS ===")
    
    # Select campaign endpoints based on question
    state = agent_3_endpoint_selection(state)
    
    # Filter to only campaign endpoints
    available = [
        {'name': 'get_meta_campaigns_timeseries', 'path': '/api/meta/campaigns/timeseries', 'method': 'POST'},
        {'name': 'get_campaigns_demographics', 'path': '/api/meta/campaigns/demographics', 'method': 'POST'},
        {'name': 'get_campaigns_placements', 'path': '/api/meta/campaigns/placements', 'method': 'POST'},
    ]
    
    selected = state.get("selected_endpoints", [])
    campaign_endpoints = [ep for ep in selected if any(ep['name'] == avail['name'] for avail in available)]
    
    if not campaign_endpoints:
        campaign_endpoints = [available[0]]  # Default to timeseries
    
    state["selected_endpoints"] = campaign_endpoints
    
    # Execute
    state = agent_4_api_execution(state)
    
    return state


def execute_adset_level_apis(state: MetaAdsState) -> MetaAdsState:
    """Execute adset-level endpoints"""
    logger.info("=== EXECUTE ADSET-LEVEL APIS ===")
    
    available = [
        {'name': 'get_adsets_timeseries', 'path': '/api/meta/adsets/timeseries', 'method': 'POST'},
        {'name': 'get_adsets_demographics', 'path': '/api/meta/adsets/demographics', 'method': 'POST'},
        {'name': 'get_adsets_placements', 'path': '/api/meta/adsets/placements', 'method': 'POST'},
    ]
    
    state["available_endpoints"] = available
    state = agent_3_endpoint_selection(state)
    
    # Execute
    state = agent_4_api_execution(state)
    
    return state


def execute_ad_level_apis(state: MetaAdsState) -> MetaAdsState:
    """Execute ad-level endpoints"""
    logger.info("=== EXECUTE AD-LEVEL APIS ===")
    
    available = [
        {'name': 'get_ads_timeseries', 'path': '/api/meta/ads/timeseries', 'method': 'POST'},
        {'name': 'get_ads_demographics', 'path': '/api/meta/ads/demographics', 'method': 'POST'},
        {'name': 'get_ads_placements', 'path': '/api/meta/ads/placements', 'method': 'POST'},
    ]
    
    state["available_endpoints"] = available
    state = agent_3_endpoint_selection(state)
    
    # Execute
    state = agent_4_api_execution(state)
    
    return state


# ============================================================================
# WAIT NODE FUNCTIONS
# ============================================================================

def wait_for_user_input(state: MetaAdsState) -> MetaAdsState:
    """Wait for general user input"""
    logger.info("=== WAIT FOR USER INPUT ===")
    state["current_agent"] = "wait_for_user_input"
    return state


def wait_for_campaign_selection(state: MetaAdsState) -> MetaAdsState:
    """Wait for user to select campaigns"""
    logger.info("=== WAIT FOR CAMPAIGN SELECTION ===")

    campaign_opts = state.get("campaign_selection_options")
    logger.info(f"📥 Received state with campaign_selection_options: {campaign_opts is not None}")
    if campaign_opts:
        logger.info(f"📥 campaign_selection_options has {len(campaign_opts)} items")

    state["current_agent"] = "wait_for_campaign_selection"
    state["needs_user_input"] = True
    # Don't set is_complete=True - conversation is incomplete until user selects campaigns

    logger.info(f"📤 Returning state - needs_user_input: True, needs_campaign_selection: {state.get('needs_campaign_selection')}")
    return state


def wait_for_adset_selection(state: MetaAdsState) -> MetaAdsState:
    """Wait for user to select adsets"""
    logger.info("=== WAIT FOR ADSET SELECTION ===")
    state["current_agent"] = "wait_for_adset_selection"
    state["needs_user_input"] = True
    return state


def wait_for_ad_selection(state: MetaAdsState) -> MetaAdsState:
    """Wait for user to select ads"""
    logger.info("=== WAIT FOR AD SELECTION ===")
    state["current_agent"] = "wait_for_ad_selection"
    state["needs_user_input"] = True
    return state


def ask_for_clarification(state: MetaAdsState) -> MetaAdsState:
    """Ask user for clarification"""
    logger.info("=== ASK FOR CLARIFICATION ===")
    state["current_agent"] = "ask_for_clarification"
    state["needs_user_input"] = True
    state["formatted_response"] = state.get("user_clarification_prompt", "Please clarify your question.")
    state["is_complete"] = True
    return state


def error_response_handler(state: MetaAdsState) -> MetaAdsState:
    """Handle errors"""
    logger.info("=== ERROR RESPONSE HANDLER ===")
    state["current_agent"] = "error_response_handler"
    
    errors = state.get("errors", [])
    error_message = "I apologize, but I encountered issues:\n\n"
    error_message += "\n".join(f"- {e}" for e in errors)
    
    state["formatted_response"] = error_message
    state["is_complete"] = True
    
    return state


# ============================================================================
# BUILD META ADS GRAPH
# ============================================================================

def create_meta_ads_graph() -> StateGraph:
    """Create the LangGraph workflow for Meta Ads module"""
    logger.info("Creating Meta Ads LangGraph workflow")
    
    workflow = StateGraph(MetaAdsState)
    
    # Add all nodes
    workflow.add_node("classify_intent", agent_1_intent_classification)
    workflow.add_node("direct_llm", direct_llm_response)
    workflow.add_node("extract_params", agent_2_parameter_extraction)
    workflow.add_node("wait_for_user", wait_for_user_input)
    workflow.add_node("detect_granularity", meta_agent_3_granularity_detection)
    workflow.add_node("account_insights", get_account_insights)
    workflow.add_node("load_campaigns", meta_agent_4_campaign_selection)
    workflow.add_node("wait_campaigns", wait_for_campaign_selection)
    workflow.add_node("campaign_decision", meta_agent_5_campaign_level_decision)
    workflow.add_node("execute_campaigns", execute_campaign_level_apis)
    workflow.add_node("load_adsets", meta_agent_6_adset_selection)
    workflow.add_node("wait_adsets", wait_for_adset_selection)
    workflow.add_node("adset_decision", meta_agent_7_adset_level_decision)
    workflow.add_node("execute_adsets", execute_adset_level_apis)
    workflow.add_node("load_ads", meta_agent_8_ad_selection)
    workflow.add_node("wait_ads", wait_for_ad_selection)
    workflow.add_node("ad_analysis", meta_agent_9_ad_level_analysis)
    workflow.add_node("execute_ads", execute_ad_level_apis)
    workflow.add_node("ask_clarification", ask_for_clarification)
    workflow.add_node("analyze_data", agent_5_data_processing_and_analysis)
    workflow.add_node("format_response", agent_6_response_formatting)
    workflow.add_node("error_response", error_response_handler)
    
    # Set entry point
    workflow.set_entry_point("classify_intent")
    
    # Add edges
    workflow.add_conditional_edges("classify_intent", route_after_intent_classification, 
                                   {"direct_llm": "direct_llm", "extract_params": "extract_params"})
    workflow.add_edge("direct_llm", END)
    
    workflow.add_conditional_edges("extract_params", route_after_parameter_extraction,
                                   {"detect_granularity": "detect_granularity", "wait_for_user": "wait_for_user"})
    workflow.add_edge("wait_for_user", END)
    
    workflow.add_conditional_edges("detect_granularity", route_after_granularity_detection,
                                   {"account_insights": "account_insights", "load_campaigns": "load_campaigns"})
    
    workflow.add_conditional_edges("account_insights", route_after_account_insights,
                                   {"analyze_data": "analyze_data", "error_response": "error_response"})
    
    workflow.add_conditional_edges("load_campaigns", route_after_campaign_selection,
                                   {"wait_for_campaign_selection": "wait_campaigns", "campaign_decision": "campaign_decision"})
    workflow.add_edge("wait_campaigns", END)
    
    workflow.add_conditional_edges("campaign_decision", route_after_campaign_decision,
                                   {"execute_campaign_apis": "execute_campaigns", "load_adsets": "load_adsets"})
    
    workflow.add_conditional_edges("execute_campaigns", route_after_api_execution,
                                   {"analyze_data": "analyze_data", "error_response": "error_response"})
    
    workflow.add_conditional_edges("load_adsets", route_after_adset_selection,
                                   {"wait_for_adset_selection": "wait_adsets", "adset_decision": "adset_decision"})
    workflow.add_edge("wait_adsets", END)
    
    workflow.add_conditional_edges("adset_decision", route_after_adset_decision,
                                   {"execute_adset_apis": "execute_adsets", "load_ads": "load_ads"})
    
    workflow.add_conditional_edges("execute_adsets", route_after_api_execution,
                                   {"analyze_data": "analyze_data", "error_response": "error_response"})
    
    workflow.add_conditional_edges("load_ads", route_after_ad_selection,
                                   {"wait_for_ad_selection": "wait_ads", "ad_analysis": "ad_analysis"})
    workflow.add_edge("wait_ads", END)
    
    workflow.add_conditional_edges("ad_analysis", route_after_ad_analysis,
                                   {"execute_ad_apis": "execute_ads", "ask_clarification": "ask_clarification"})
    
    workflow.add_conditional_edges("execute_ads", route_after_api_execution,
                                   {"analyze_data": "analyze_data", "error_response": "error_response"})
    
    workflow.add_edge("ask_clarification", END)
    workflow.add_edge("analyze_data", "format_response")
    workflow.add_edge("format_response", END)
    workflow.add_edge("error_response", END)
    
    app = workflow.compile()
    logger.info("Meta Ads graph compiled successfully")
    
    return app


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
    """Run the complete Meta Ads chat workflow"""
    logger.info(f"Starting Meta Ads chat for user: {user_email}")
    
    try:
        initial_state = create_initial_state(
            user_question=user_question,
            module_type="meta_ads",
            session_id=session_id,
            user_email=user_email,
            auth_token=auth_token,
            context=context
        )
        
        app = create_meta_ads_graph()
        final_state = app.invoke(initial_state)
        
        logger.info("Meta Ads chat completed successfully")
        return final_state
        
    except Exception as e:
        logger.error(f"Error running Meta Ads chat: {e}")
        return {
            "formatted_response": f"An error occurred: {str(e)}",
            "errors": [str(e)],
            "is_complete": True
        }