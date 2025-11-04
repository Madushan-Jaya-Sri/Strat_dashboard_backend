"""
Facebook Pages LangGraph Implementation
Complete workflow for Facebook Pages module chat
"""

import logging
from typing import Dict, Any, Literal
from langgraph.graph import StateGraph, END
from chat.states.chat_states import FacebookState, create_initial_state
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
    state: FacebookState
) -> Literal["direct_llm", "extract_params"]:
    """Route based on intent type"""
    return "direct_llm" if state.get("intent_type") == "chitchat" else "extract_params"


def route_after_parameter_extraction(
    state: FacebookState
) -> Literal["select_endpoints", "wait_for_user"]:
    """Route based on whether we need user input"""
    return "wait_for_user" if state.get("needs_user_input") else "select_endpoints"


def route_after_endpoint_selection(
    state: FacebookState
) -> Literal["execute_api", "error_response"]:
    """Route based on whether endpoints were selected"""
    has_endpoints = bool(state.get("selected_endpoints"))
    return "execute_api" if has_endpoints else "error_response"


def route_after_api_execution(
    state: FacebookState
) -> Literal["process_data", "error_response"]:
    """Route based on whether we got successful responses"""
    return "process_data" if state.get("endpoint_responses") else "error_response"


# ============================================================================
# SPECIAL NODE FUNCTIONS
# ============================================================================

def wait_for_user_input(state: FacebookState) -> FacebookState:
    """Wait for user to provide missing information"""
    logger.info("=== WAIT FOR USER INPUT ===")
    state["current_agent"] = "wait_for_user_input"
    return state


def error_response_handler(state: FacebookState) -> FacebookState:
    """Handle error case"""
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
# BUILD FACEBOOK GRAPH
# ============================================================================

def create_facebook_graph() -> StateGraph:
    """Create the LangGraph workflow for Facebook Pages module"""
    logger.info("Creating Facebook Pages LangGraph workflow")

    workflow = StateGraph(FacebookState)

    # Add all nodes (6 agents + helpers)
    workflow.add_node("classify_intent", agent_1_intent_classification)  # Agent 1
    workflow.add_node("direct_llm", direct_llm_response)
    workflow.add_node("extract_params", agent_2_parameter_extraction)  # Agent 2
    workflow.add_node("wait_for_user", wait_for_user_input)
    workflow.add_node("select_endpoints", agent_3_endpoint_selection)  # Agent 3
    workflow.add_node("execute_api", agent_4_api_execution)  # Agent 4
    workflow.add_node("process_data", agent_5_data_processing_and_analysis)  # Agent 5
    workflow.add_node("format_response", agent_6_response_formatting)  # Agent 6
    workflow.add_node("error_response", error_response_handler)

    # Set entry point
    workflow.set_entry_point("classify_intent")

    # Add edges
    # After Agent 1 (Intent Classification)
    workflow.add_conditional_edges(
        "classify_intent",
        route_after_intent_classification,
        {"direct_llm": "direct_llm", "extract_params": "extract_params"}
    )

    workflow.add_edge("direct_llm", END)

    # After Agent 2 (Parameter Extraction)
    workflow.add_conditional_edges(
        "extract_params",
        route_after_parameter_extraction,
        {"select_endpoints": "select_endpoints", "wait_for_user": "wait_for_user"}
    )

    workflow.add_edge("wait_for_user", END)

    # After Agent 3 (Endpoint Selection)
    workflow.add_conditional_edges(
        "select_endpoints",
        route_after_endpoint_selection,
        {"execute_api": "execute_api", "error_response": "error_response"}
    )

    # After Agent 4 (API Execution)
    workflow.add_conditional_edges(
        "execute_api",
        route_after_api_execution,
        {"process_data": "process_data", "error_response": "error_response"}
    )

    # After Agent 5 (Data Processing) -> Agent 6 (Response Formatting)
    workflow.add_edge("process_data", "format_response")
    workflow.add_edge("format_response", END)
    workflow.add_edge("error_response", END)

    app = workflow.compile()
    logger.info("Facebook Pages graph compiled successfully")

    return app


# ============================================================================
# MAIN EXECUTION FUNCTION
# ============================================================================

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
            module_type="facebook_analytics",
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
