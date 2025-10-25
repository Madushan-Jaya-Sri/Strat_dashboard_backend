# """
# Google Analytics (GA4) LangGraph Implementation
# Complete workflow for Google Analytics module chat
# """

# import logging
# from typing import Dict, Any, Literal
# from langgraph.graph import StateGraph, END
# from chat.states.chat_states import GoogleAnalyticsState, create_initial_state
# from chat.agents.shared_agents import (
#     agent_1_intent_classification,
#     direct_llm_response,
#     agent_2_parameter_extraction,
#     agent_3_endpoint_selection,
#     agent_5_data_processing_and_analysis,
#     agent_6_response_formatting
# )
# from chat.utils.api_client import agent_4_api_execution

# # Initialize logger
# logger = logging.getLogger(__name__)


# # ============================================================================
# # ROUTING FUNCTIONS (Same as Google Ads)
# # ============================================================================

# def route_after_intent_classification(
#     state: GoogleAnalyticsState
# ) -> Literal["direct_llm", "extract_params"]:
#     """Route based on intent type"""
#     intent = state.get("intent_type")
#     return "direct_llm" if intent == "chitchat" else "extract_params"


# def route_after_parameter_extraction(
#     state: GoogleAnalyticsState
# ) -> Literal["select_endpoints", "wait_for_user"]:
#     """Route based on whether we need user input"""
#     return "wait_for_user" if state.get("needs_user_input") else "select_endpoints"


# def route_after_api_execution(
#     state: GoogleAnalyticsState
# ) -> Literal["analyze_data", "error_response"]:
#     """Route based on whether we got successful responses"""
#     return "analyze_data" if state.get("endpoint_responses") else "error_response"


# # ============================================================================
# # SPECIAL NODE FUNCTIONS
# # ============================================================================

# def wait_for_user_input(state: GoogleAnalyticsState) -> GoogleAnalyticsState:
#     """Wait for user to provide missing information"""
#     logger.info("=== WAIT FOR USER INPUT ===")
#     state["current_agent"] = "wait_for_user_input"
#     return state


# def error_response_handler(state: GoogleAnalyticsState) -> GoogleAnalyticsState:
#     """Handle error case when no data was retrieved"""
#     logger.info("=== ERROR RESPONSE HANDLER ===")
#     state["current_agent"] = "error_response_handler"
    
#     errors = state.get("errors", [])
#     warnings = state.get("warnings", [])
    
#     error_message = "I apologize, but I encountered issues retrieving the data:\n\n"
    
#     if errors:
#         error_message += "**Errors:**\n" + "\n".join(f"- {e}" for e in errors) + "\n"
#     if warnings:
#         error_message += "\n**Warnings:**\n" + "\n".join(f"- {w}" for w in warnings) + "\n"
    
#     error_message += "\nPlease try again or rephrase your question."
    
#     state["formatted_response"] = error_message
#     state["is_complete"] = True
    
#     return state


# # ============================================================================
# # BUILD GOOGLE ANALYTICS GRAPH
# # ============================================================================

# def create_ga4_graph() -> StateGraph:
#     """Create the LangGraph workflow for Google Analytics module"""
#     logger.info("Creating Google Analytics LangGraph workflow")
    
#     workflow = StateGraph(GoogleAnalyticsState)
    
#     # Add all nodes
#     workflow.add_node("classify_intent", agent_1_intent_classification)
#     workflow.add_node("direct_llm", direct_llm_response)
#     workflow.add_node("extract_params", agent_2_parameter_extraction)
#     workflow.add_node("wait_for_user", wait_for_user_input)
#     workflow.add_node("select_endpoints", agent_3_endpoint_selection)
#     workflow.add_node("execute_apis", agent_4_api_execution)
#     workflow.add_node("analyze_data", agent_5_data_processing_and_analysis)
#     workflow.add_node("format_response", agent_6_response_formatting)
#     workflow.add_node("error_response", error_response_handler)
    
#     # Set entry point
#     workflow.set_entry_point("classify_intent")
    
#     # Add edges
#     workflow.add_conditional_edges(
#         "classify_intent",
#         route_after_intent_classification,
#         {"direct_llm": "direct_llm", "extract_params": "extract_params"}
#     )
    
#     workflow.add_edge("direct_llm", END)
    
#     workflow.add_conditional_edges(
#         "extract_params",
#         route_after_parameter_extraction,
#         {"select_endpoints": "select_endpoints", "wait_for_user": "wait_for_user"}
#     )
    
#     workflow.add_edge("wait_for_user", END)
#     workflow.add_edge("select_endpoints", "execute_apis")
    
#     workflow.add_conditional_edges(
#         "execute_apis",
#         route_after_api_execution,
#         {"analyze_data": "analyze_data", "error_response": "error_response"}
#     )
    
#     workflow.add_edge("analyze_data", "format_response")
#     workflow.add_edge("format_response", END)
#     workflow.add_edge("error_response", END)
    
#     app = workflow.compile()
#     logger.info("Google Analytics graph compiled successfully")
    
#     return app


# # ============================================================================
# # MAIN EXECUTION FUNCTION
# # ============================================================================

# async def run_ga4_chat(
#     user_question: str,
#     session_id: str,
#     user_email: str,
#     auth_token: str,
#     context: Dict[str, Any]
# ) -> Dict[str, Any]:
#     """Run the complete Google Analytics chat workflow"""
#     logger.info(f"Starting GA4 chat for user: {user_email}")
    
#     try:
#         initial_state = create_initial_state(
#             user_question=user_question,
#             module_type="google_analytics",
#             session_id=session_id,
#             user_email=user_email,
#             auth_token=auth_token,
#             context=context
#         )
        
#         app = create_ga4_graph()
#         final_state = app.invoke(initial_state)
        
#         logger.info("GA4 chat completed successfully")
#         return final_state
        
#     except Exception as e:
#         logger.error(f"Error running GA4 chat: {e}")
#         return {
#             "formatted_response": f"An error occurred: {str(e)}",
#             "errors": [str(e)],
#             "is_complete": True
#         }