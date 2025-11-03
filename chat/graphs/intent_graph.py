"""
Intent Insights LangGraph Implementation
Complete workflow for Intent Insights module chat
"""

import logging
from typing import Dict, Any, Literal
from langgraph.graph import StateGraph, END
from chat.states.chat_states import IntentInsightsState, create_initial_state
from chat.agents.shared_agents import (
    agent_1_intent_classification,
    direct_llm_response,
    agent_2_parameter_extraction,
    agent_5_data_processing_and_analysis,
    agent_6_response_formatting
)
from chat.utils.api_client import agent_4_api_execution

# Initialize logger
logger = logging.getLogger(__name__)


# ============================================================================
# INTENT-SPECIFIC AGENTS
# ============================================================================

def intent_agent_3_endpoint_decision(state: IntentInsightsState) -> IntentInsightsState:
    """
    Intent Agent 3: Decide if we need to call the keyword insights API
    or can answer directly with LLM
    """
    logger.info("=== INTENT AGENT 3: Endpoint Decision ===")
    state["current_agent"] = "intent_agent_3_endpoint_decision"
    
    user_question = state.get("user_question", "")
    
    try:
        from openai import OpenAI
        import os
        
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        
        system_prompt = """You are analyzing whether a question about keyword research requires calling the keyword insights API.

Questions that NEED API (return "api"):
- "find keywords for..."
- "keyword suggestions for..."
- "search volume for..."
- "keyword ideas about..."
- "trending keywords..."
- Any question requiring actual keyword data, search volumes, or suggestions

Questions that DON'T need API (return "direct"):
- "what is keyword research?"
- "how to do keyword research?"
- "explain keyword metrics"
- "what does search volume mean?"
- General knowledge questions about SEO/keywords

Return ONLY one word: "api" or "direct"
"""

        response = client.chat.completions.create(
            model="gpt-4-turbo-preview",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Question: {user_question}"}
            ],
            temperature=0.1,
            max_tokens=10
        )
        
        decision = response.choices[0].message.content.strip().lower()
        
        if decision == "api":
            state["needs_api_call"] = True
            
            # Set up the intent endpoint
            state["selected_endpoints"] = [
                {
                    'name': 'get_intent_keyword_insights',
                    'path': '/api/intent/keyword-insights/{customer_id}',
                    'method': 'POST',
                    'params': ['customer_id'],
                    'body_params': ['seed_keywords', 'country', 'timeframe', 'start_date', 'end_date', 'include_zero_volume']
                }
            ]
            logger.info("Decision: Will call keyword insights API")
        else:
            state["needs_api_call"] = False
            logger.info("Decision: Will answer directly with LLM")
        
        return state
        
    except Exception as e:
        logger.error(f"Error in endpoint decision: {e}")
        state["errors"].append(f"Endpoint decision failed: {str(e)}")
        # Default to API call to be safe
        state["needs_api_call"] = True
        return state


def intent_agent_4_keyword_analyzer(state: IntentInsightsState) -> IntentInsightsState:
    """
    Intent Agent 4: Act as keyword research analyzer expert
    Provides strategic recommendations and insights
    """
    logger.info("=== INTENT AGENT 4: Keyword Research Analyzer ===")
    state["current_agent"] = "intent_agent_4_keyword_analyzer"
    
    user_question = state.get("user_question", "")
    llm_insights = state.get("llm_insights", "")
    
    try:
        from openai import OpenAI
        import os
        
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        
        system_prompt = """You are an expert keyword research analyst and SEO strategist.

Your job is to provide strategic, actionable insights about keywords:

1. **Opportunity Identification**: Highlight high-value keywords (high volume, low competition)
2. **Trend Analysis**: Identify trending keywords and seasonal patterns
3. **Strategic Recommendations**: Suggest which keywords to target first
4. **Content Strategy**: Recommend content topics based on keyword clusters
5. **Competitive Insights**: Point out keyword gaps and opportunities

Format your response professionally with:
- Clear section headers
- Bullet points for recommendations
- Bold key numbers and keywords
- Actionable next steps

Be specific, data-driven, and strategic."""

        user_prompt = f"""Original Question: {user_question}

Initial Analysis:
{llm_insights}

Provide expert keyword research analysis with strategic recommendations."""

        response = client.chat.completions.create(
            model="gpt-4-turbo-preview",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.4,
            max_tokens=2000
        )
        
        expert_analysis = response.choices[0].message.content.strip()
        state["llm_insights"] = expert_analysis
        
        logger.info("Keyword research analysis completed")
        return state
        
    except Exception as e:
        logger.error(f"Error in keyword analyzer: {e}")
        state["errors"].append(f"Keyword analysis failed: {str(e)}")
        # Keep original insights
        return state


# ============================================================================
# ROUTING FUNCTIONS
# ============================================================================

def route_after_intent_classification(
    state: IntentInsightsState
) -> Literal["direct_llm", "extract_params"]:
    """Route based on intent type"""
    return "direct_llm" if state.get("intent_type") == "chitchat" else "extract_params"


def route_after_parameter_extraction(
    state: IntentInsightsState
) -> Literal["endpoint_decision", "wait_for_user"]:
    """Route based on whether we need user input"""
    return "wait_for_user" if state.get("needs_user_input") else "endpoint_decision"


def route_after_endpoint_decision(
    state: IntentInsightsState
) -> Literal["execute_api", "direct_answer"]:
    """Route based on whether we need to call API"""
    return "execute_api" if state.get("needs_api_call") else "direct_answer"


def route_after_api_execution(
    state: IntentInsightsState
) -> Literal["analyze_data", "error_response"]:
    """Route based on whether we got successful responses"""
    return "analyze_data" if state.get("endpoint_responses") else "error_response"


# ============================================================================
# SPECIAL NODE FUNCTIONS
# ============================================================================

def direct_answer_with_llm(state: IntentInsightsState) -> IntentInsightsState:
    """
    Provide direct answer using LLM without calling API
    For general knowledge questions
    """
    logger.info("=== DIRECT ANSWER (No API Call) ===")
    state["current_agent"] = "direct_answer_with_llm"
    
    # Use the direct_llm_response function
    return direct_llm_response(state)


def wait_for_user_input(state: IntentInsightsState) -> IntentInsightsState:
    """Wait for user to provide missing information"""
    logger.info("=== WAIT FOR USER INPUT ===")
    state["current_agent"] = "wait_for_user_input"
    return state


def error_response_handler(state: IntentInsightsState) -> IntentInsightsState:
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
# BUILD INTENT INSIGHTS GRAPH
# ============================================================================

def create_intent_graph() -> StateGraph:
    """Create the LangGraph workflow for Intent Insights module"""
    logger.info("Creating Intent Insights LangGraph workflow")
    
    workflow = StateGraph(IntentInsightsState)
    
    # Add all nodes
    workflow.add_node("classify_intent", agent_1_intent_classification)
    workflow.add_node("direct_llm", direct_llm_response)
    workflow.add_node("extract_params", agent_2_parameter_extraction)
    workflow.add_node("wait_for_user", wait_for_user_input)
    workflow.add_node("endpoint_decision", intent_agent_3_endpoint_decision)
    workflow.add_node("direct_answer", direct_answer_with_llm)
    workflow.add_node("execute_api", agent_4_api_execution)
    workflow.add_node("analyze_data", agent_5_data_processing_and_analysis)
    workflow.add_node("keyword_analyzer", intent_agent_4_keyword_analyzer)
    workflow.add_node("format_response", agent_6_response_formatting)
    workflow.add_node("error_response", error_response_handler)
    
    # Set entry point
    workflow.set_entry_point("classify_intent")
    
    # Add edges
    workflow.add_conditional_edges(
        "classify_intent",
        route_after_intent_classification,
        {"direct_llm": "direct_llm", "extract_params": "extract_params"}
    )
    
    workflow.add_edge("direct_llm", END)
    
    workflow.add_conditional_edges(
        "extract_params",
        route_after_parameter_extraction,
        {"endpoint_decision": "endpoint_decision", "wait_for_user": "wait_for_user"}
    )
    
    workflow.add_edge("wait_for_user", END)
    
    workflow.add_conditional_edges(
        "endpoint_decision",
        route_after_endpoint_decision,
        {"execute_api": "execute_api", "direct_answer": "direct_answer"}
    )
    
    workflow.add_edge("direct_answer", END)
    
    workflow.add_conditional_edges(
        "execute_api",
        route_after_api_execution,
        {"analyze_data": "analyze_data", "error_response": "error_response"}
    )
    
    workflow.add_edge("analyze_data", "keyword_analyzer")
    workflow.add_edge("keyword_analyzer", "format_response")
    workflow.add_edge("format_response", END)
    workflow.add_edge("error_response", END)
    
    app = workflow.compile()
    logger.info("Intent Insights graph compiled successfully")
    
    return app


# ============================================================================
# MAIN EXECUTION FUNCTION
# ============================================================================

async def run_intent_chat(
    user_question: str,
    session_id: str,
    user_email: str,
    auth_token: str,
    context: Dict[str, Any]
) -> Dict[str, Any]:
    """Run the complete Intent Insights chat workflow"""
    logger.info(f"Starting Intent chat for user: {user_email}")
    
    try:
        initial_state = create_initial_state(
            user_question=user_question,
            module_type="intent_insights",
            session_id=session_id,
            user_email=user_email,
            auth_token=auth_token,
            context=context
        )
        
        app = create_intent_graph()
        final_state = app.invoke(initial_state)
        
        logger.info("Intent chat completed successfully")
        return final_state
        
    except Exception as e:
        logger.error(f"Error running Intent chat: {e}")
        return {
            "formatted_response": f"An error occurred: {str(e)}",
            "errors": [str(e)],
            "is_complete": True
        }