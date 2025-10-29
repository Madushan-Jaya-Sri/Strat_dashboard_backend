"""
Facebook Pages and Instagram LangGraph Implementations
Both follow similar patterns to Google Analytics
"""

import logging
from typing import Dict, Any, Literal
from langgraph.graph import StateGraph, END
from chat.states.chat_states import FacebookPageState, InstagramState, create_initial_state
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
# SHARED ROUTING FUNCTIONS
# ============================================================================

def route_after_intent(state: Dict[str, Any]) -> Literal["direct_llm", "extract_params"]:
    """Route based on intent type"""
    return "direct_llm" if state.get("intent_type") == "chitchat" else "extract_params"


def route_after_params(state: Dict[str, Any]) -> Literal["select_endpoints", "wait_for_user"]:
    """Route based on whether we need user input"""
    return "wait_for_user" if state.get("needs_user_input") else "select_endpoints"


def route_after_api(state: Dict[str, Any]) -> Literal["analyze_data", "error_response"]:
    """Route based on API execution success"""
    return "analyze_data" if state.get("endpoint_responses") else "error_response"


# ============================================================================
# SHARED NODE FUNCTIONS
# ============================================================================

def wait_for_user_input(state: Dict[str, Any]) -> Dict[str, Any]:
    """Wait for user input"""
    logger.info("=== WAIT FOR USER INPUT ===")
    state["current_agent"] = "wait_for_user_input"
    return state


def error_response_handler(state: Dict[str, Any]) -> Dict[str, Any]:
    """Handle errors"""
    logger.info("=== ERROR RESPONSE HANDLER ===")
    state["current_agent"] = "error_response_handler"
    
    errors = state.get("errors", [])
    error_message = "I apologize, but I encountered issues:\n\n"
    error_message += "\n".join(f"- {e}" for e in errors)
    error_message += "\n\nPlease try again."
    
    state["formatted_response"] = error_message
    state["is_complete"] = True
    
    return state


# ============================================================================
# FACEBOOK PAGES GRAPH
# ============================================================================

def create_facebook_graph() -> StateGraph:
    """Create the LangGraph workflow for Facebook Pages module"""
    logger.info("Creating Facebook Pages LangGraph workflow")
    
    workflow = StateGraph(FacebookPageState)
    
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
    workflow.add_conditional_edges("classify_intent", route_after_intent,
                                   {"direct_llm": "direct_llm", "extract_params": "extract_params"})
    workflow.add_edge("direct_llm", END)
    
    workflow.add_conditional_edges("extract_params", route_after_params,
                                   {"select_endpoints": "select_endpoints", "wait_for_user": "wait_for_user"})
    workflow.add_edge("wait_for_user", END)
    workflow.add_edge("select_endpoints", "execute_apis")
    
    workflow.add_conditional_edges("execute_apis", route_after_api,
                                   {"analyze_data": "analyze_data", "error_response": "error_response"})
    
    workflow.add_edge("analyze_data", "format_response")
    workflow.add_edge("format_response", END)
    workflow.add_edge("error_response", END)
    
    app = workflow.compile()
    logger.info("Facebook Pages graph compiled successfully")
    
    return app


async def run_facebook_chat(
    user_question: str,
    session_id: str,
    user_email: str,
    auth_token: str,
    context: Dict[str, Any]
) -> Dict[str, Any]:
    """Run the complete Facebook Pages chat workflow"""
    logger.info(f"Starting Facebook chat for user: {user_email}")
    
    try:
        initial_state = create_initial_state(
            user_question=user_question,
            module_type="facebook",
            session_id=session_id,
            user_email=user_email,
            auth_token=auth_token,
            context=context
        )
        
        app = create_facebook_graph()
        final_state = app.invoke(initial_state)
        
        logger.info("Facebook chat completed successfully")
        return final_state
        
    except Exception as e:
        logger.error(f"Error running Facebook chat: {e}")
        return {
            "formatted_response": f"An error occurred: {str(e)}",
            "errors": [str(e)],
            "is_complete": True
        }


# ============================================================================
# INSTAGRAM GRAPH
# ============================================================================

def create_instagram_graph() -> StateGraph:
    """Create the LangGraph workflow for Instagram module"""
    logger.info("Creating Instagram LangGraph workflow")
    
    workflow = StateGraph(InstagramState)
    
    # Add all nodes (same as Facebook)
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
    
    # Add edges (same as Facebook)
    workflow.add_conditional_edges("classify_intent", route_after_intent,
                                   {"direct_llm": "direct_llm", "extract_params": "extract_params"})
    workflow.add_edge("direct_llm", END)
    
    workflow.add_conditional_edges("extract_params", route_after_params,
                                   {"select_endpoints": "select_endpoints", "wait_for_user": "wait_for_user"})
    workflow.add_edge("wait_for_user", END)
    workflow.add_edge("select_endpoints", "execute_apis")
    
    workflow.add_conditional_edges("execute_apis", route_after_api,
                                   {"analyze_data": "analyze_data", "error_response": "error_response"})
    
    workflow.add_edge("analyze_data", "format_response")
    workflow.add_edge("format_response", END)
    workflow.add_edge("error_response", END)
    
    app = workflow.compile()
    logger.info("Instagram graph compiled successfully")
    
    return app


async def run_instagram_chat(
    user_question: str,
    session_id: str,
    user_email: str,
    auth_token: str,
    context: Dict[str, Any]
) -> Dict[str, Any]:
    """Run the complete Instagram chat workflow"""
    logger.info(f"Starting Instagram chat for user: {user_email}")
    
    try:
        initial_state = create_initial_state(
            user_question=user_question,
            module_type="instagram",
            session_id=session_id,
            user_email=user_email,
            auth_token=auth_token,
            context=context
        )
        
        app = create_instagram_graph()
        final_state = app.invoke(initial_state)
        
        logger.info("Instagram chat completed successfully")
        return final_state
        
    except Exception as e:
        logger.error(f"Error running Instagram chat: {e}")
        return {
            "formatted_response": f"An error occurred: {str(e)}",
            "errors": [str(e)],
            "is_complete": True
        }