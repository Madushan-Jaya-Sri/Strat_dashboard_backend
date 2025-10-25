"""
Google Ads LangGraph Implementation
Complete workflow for Google Ads module chat
"""

import logging
from typing import Dict, Any, Literal
from langgraph.graph import StateGraph, END
from chat.states.chat_states import GoogleAdsState, create_initial_state
from chat.agents.shared_agents import (
    agent_1_intent_classification,
    direct_llm_response,
    agent_2_parameter_extraction,
    agent_3_endpoint_selection,
    agent_5_data_processing_and_analysis,
    agent_6_response_formatting
)
from chat.utils.api_client import agent_4_api_execution

# Initialize logger
logger = logging.getLogger(__name__)


# ============================================================================
# ROUTING FUNCTIONS
# ============================================================================

def route_after_intent_classification(
    state: GoogleAdsState
) -> Literal["direct_llm", "extract_params"]:
    """
    Route based on intent type
    
    Args:
        state: Current state
        
    Returns:
        Next node name
    """
    intent = state.get("intent_type")
    
    if intent == "chitchat":
        logger.info("Routing to direct_llm (chitchat)")
        return "direct_llm"
    else:
        logger.info("Routing to extract_params (analytical)")
        return "extract_params"


def route_after_parameter_extraction(
    state: GoogleAdsState
) -> Literal["select_endpoints", "wait_for_user"]:
    """
    Route based on whether we need user input
    
    Args:
        state: Current state
        
    Returns:
        Next node name
    """
    if state.get("needs_user_input"):
        logger.info("Routing to wait_for_user (missing parameters)")
        return "wait_for_user"
    else:
        logger.info("Routing to select_endpoints")
        return "select_endpoints"


def route_after_api_execution(
    state: GoogleAdsState
) -> Literal["analyze_data", "error_response"]:
    """
    Route based on whether we got successful responses
    
    Args:
        state: Current state
        
    Returns:
        Next node name
    """
    endpoint_responses = state.get("endpoint_responses", [])
    
    if endpoint_responses:
        logger.info(f"Routing to analyze_data ({len(endpoint_responses)} responses)")
        return "analyze_data"
    else:
        logger.info("Routing to error_response (no data)")
        return "error_response"


# ============================================================================
# SPECIAL NODE FUNCTIONS
# ============================================================================

def wait_for_user_input(state: GoogleAdsState) -> GoogleAdsState:
    """
    Wait for user to provide missing information
    This node sets a flag that frontend can check
    
    Args:
        state: Current state
        
    Returns:
        Updated state
    """
    logger.info("=== WAIT FOR USER INPUT ===")
    state["current_agent"] = "wait_for_user_input"
    
    # The user_clarification_prompt is already set by previous agent
    # Frontend will display this and wait for user response
    logger.info(f"Waiting for user input: {state.get('user_clarification_prompt')}")
    
    return state


def error_response_handler(state: GoogleAdsState) -> GoogleAdsState:
    """
    Handle error case when no data was retrieved
    
    Args:
        state: Current state
        
    Returns:
        Updated state with error response
    """
    logger.info("=== ERROR RESPONSE HANDLER ===")
    state["current_agent"] = "error_response_handler"
    
    errors = state.get("errors", [])
    warnings = state.get("warnings", [])
    
    error_message = "I apologize, but I encountered issues retrieving the data:\n\n"
    
    if errors:
        error_message += "**Errors:**\n"
        for error in errors:
            error_message += f"- {error}\n"
    
    if warnings:
        error_message += "\n**Warnings:**\n"
        for warning in warnings:
            error_message += f"- {warning}\n"
    
    error_message += "\nPlease try again or rephrase your question."
    
    state["formatted_response"] = error_message
    state["is_complete"] = True
    
    return state


# ============================================================================
# BUILD GOOGLE ADS GRAPH
# ============================================================================

def create_google_ads_graph() -> StateGraph:
    """
    Create the LangGraph workflow for Google Ads module
    
    Returns:
        Compiled StateGraph
    """
    logger.info("Creating Google Ads LangGraph workflow")
    
    # Initialize graph with GoogleAdsState
    workflow = StateGraph(GoogleAdsState)
    
    # Add all nodes
    workflow.add_node("classify_intent", agent_1_intent_classification)
    workflow.add_node("direct_llm", direct_llm_response)
    workflow.add_node("extract_params", agent_2_parameter_extraction)
    workflow.add_node("wait_for_user", wait_for_user_input)
    workflow.add_node("select_endpoints", agent_3_endpoint_selection)
    workflow.add_node("execute_apis", agent_4_api_execution)
    workflow.add_node("analyze_data", agent_5_data_processing_and_analysis)
    workflow.add_node("format_response", agent_6_response_formatting)
    workflow.add_node("error_response", error_response_handler)
    
    # Set entry point
    workflow.set_entry_point("classify_intent")
    
    # Add edges
    # After intent classification
    workflow.add_conditional_edges(
        "classify_intent",
        route_after_intent_classification,
        {
            "direct_llm": "direct_llm",
            "extract_params": "extract_params"
        }
    )
    
    # Direct LLM goes to END
    workflow.add_edge("direct_llm", END)
    
    # After parameter extraction
    workflow.add_conditional_edges(
        "extract_params",
        route_after_parameter_extraction,
        {
            "select_endpoints": "select_endpoints",
            "wait_for_user": "wait_for_user"
        }
    )
    
    # Wait for user goes to END (frontend will handle re-entry)
    workflow.add_edge("wait_for_user", END)
    
    # After endpoint selection → execute APIs
    workflow.add_edge("select_endpoints", "execute_apis")
    
    # After API execution
    workflow.add_conditional_edges(
        "execute_apis",
        route_after_api_execution,
        {
            "analyze_data": "analyze_data",
            "error_response": "error_response"
        }
    )
    
    # After data analysis → format response
    workflow.add_edge("analyze_data", "format_response")
    
    # Format response and error response go to END
    workflow.add_edge("format_response", END)
    workflow.add_edge("error_response", END)
    
    # Compile the graph
    app = workflow.compile()
    
    logger.info("Google Ads graph compiled successfully")
    
    return app


# ============================================================================
# MAIN EXECUTION FUNCTION
# ============================================================================

async def run_google_ads_chat(
    user_question: str,
    session_id: str,
    user_email: str,
    auth_token: str,
    context: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Run the complete Google Ads chat workflow
    
    Args:
        user_question: User's question
        session_id: Session identifier
        user_email: User's email
        auth_token: Google auth token
        context: Additional context from frontend
        
    Returns:
        Final state with response
    """
    logger.info(f"Starting Google Ads chat for user: {user_email}")
    logger.info(f"Question: {user_question}")
    
    try:
        # Create initial state
        initial_state = create_initial_state(
            user_question=user_question,
            module_type="google_ads",
            session_id=session_id,
            user_email=user_email,
            auth_token=auth_token,
            context=context
        )
        
        # Get compiled graph
        app = create_google_ads_graph()
        
        # Run the graph
        final_state = app.invoke(initial_state)
        
        logger.info("Google Ads chat completed successfully")
        
        return final_state
        
    except Exception as e:
        logger.error(f"Error running Google Ads chat: {e}")
        return {
            "formatted_response": f"An error occurred: {str(e)}",
            "errors": [str(e)],
            "is_complete": True
        }


# ============================================================================
# HELPER FUNCTION FOR CONTINUATION
# ============================================================================

def continue_google_ads_chat(
    previous_state: Dict[str, Any],
    user_response: str
) -> Dict[str, Any]:
    """
    Continue a Google Ads chat after user provides additional input
    
    Args:
        previous_state: Previous state that was waiting for input
        user_response: User's response
        
    Returns:
        Updated state
    """
    logger.info("Continuing Google Ads chat with user response")
    
    try:
        # Update state with user response
        state = previous_state.copy()
        state["user_question"] = user_response
        state["needs_user_input"] = False
        state["user_clarification_prompt"] = None
        
        # Determine where to continue from
        current_agent = state.get("current_agent")
        
        if current_agent == "wait_for_user_input":
            # If we were waiting after parameter extraction, re-run extraction
            state = agent_2_parameter_extraction(state)
            
            if not state.get("needs_user_input"):
                # Parameters extracted successfully, continue to endpoint selection
                state = agent_3_endpoint_selection(state)
                state = agent_4_api_execution(state)
                state = agent_5_data_processing_and_analysis(state)
                state = agent_6_response_formatting(state)
        
        return state
        
    except Exception as e:
        logger.error(f"Error continuing Google Ads chat: {e}")
        return {
            "formatted_response": f"An error occurred: {str(e)}",
            "errors": [str(e)],
            "is_complete": True
        }