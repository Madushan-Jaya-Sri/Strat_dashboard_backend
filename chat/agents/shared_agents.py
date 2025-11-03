"""
Shared Agent Functions for LangGraph Multi-Module Chat System
These agents are reusable across all chat modules
"""

import logging
import json
import re
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import os
from openai import OpenAI

# Initialize logger
logger = logging.getLogger(__name__)

# Initialize OpenAI client (or use your preferred LLM)
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Default model
DEFAULT_MODEL = "gpt-4-turbo-preview"  # or "gpt-3.5-turbo" for faster/cheaper


# ============================================================================
# AGENT 1: INTENT CLASSIFICATION AGENT
# ============================================================================

def agent_1_intent_classification(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Agent 1: Classify user intent as chitchat or analytical

    Args:
        state: Current chat state

    Returns:
        Updated state with intent_type set
    """
    logger.info("=" * 80)
    logger.info("🤖 AGENT 1: Intent Classification")
    logger.info(f"   Session ID: {state.get('session_id')}")
    logger.info(f"   User Question: {state.get('user_question', '')[:100]}...")
    logger.info(f"   Module Type: {state.get('module_type')}")
    state["current_agent"] = "agent_1_intent_classification"

    # If continuing from a selection (campaign/adset/ad IDs already set), skip classification
    if state.get("campaign_ids") or state.get("adset_ids") or state.get("ad_ids"):
        logger.info("🔄 AGENT 1: Continuing from selection - skipping intent classification")
        state["intent_type"] = "analytical"
        return state

    user_question = state.get("user_question", "")

    try:
        logger.info(f"🧠 AGENT 1: Calling LLM for intent classification")
        # LLM prompt for intent classification
        system_prompt = """You are an intent classifier for a marketing analytics chat system.
Your job is to determine if the user's message is:
1. CHITCHAT - General conversation, greetings, questions about you, casual talk
2. ANALYTICAL - Questions about data, metrics, campaigns, performance, insights

Respond with ONLY one word: either "chitchat" or "analytical"

Examples:
- "hi" → chitchat
- "hello, how are you?" → chitchat
- "who are you?" → chitchat
- "what can you do?" → chitchat
- "what's the weather?" → chitchat
- "show me campaign performance" → analytical
- "how many clicks did we get?" → analytical
- "what's our conversion rate?" → analytical
- "analyze last month's data" → analytical
"""

        user_prompt = f"Classify this message: {user_question}"

        response = client.chat.completions.create(
            model=DEFAULT_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.1,
            max_tokens=10
        )

        intent = response.choices[0].message.content.strip().lower()
        logger.info(f"✅ AGENT 1: LLM response received: '{intent}'")

        # Validate response
        if intent not in ["chitchat", "analytical"]:
            # Default to analytical if unclear
            intent = "analytical"
            logger.warning(f"⚠️ AGENT 1: Unclear intent classification '{intent}', defaulting to: analytical")

        state["intent_type"] = intent
        logger.info(f"✅ AGENT 1: Intent classified as: {intent}")

        # If chitchat, we can prepare a direct response
        if intent == "chitchat":
            state["needs_user_input"] = False
            logger.info("💬 AGENT 1: Chitchat detected - will route to direct LLM response")
        else:
            logger.info("📊 AGENT 1: Analytical intent detected - will proceed to parameter extraction")

        logger.info("=" * 80)
        return state

    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"❌ AGENT 1: Error in intent classification")
        logger.error(f"   Error type: {type(e).__name__}")
        logger.error(f"   Error message: {str(e)}")
        logger.error("=" * 80)
        logger.error(f"   Full traceback:", exc_info=True)
        state["errors"].append(f"Intent classification failed: {str(e)}")
        # Default to analytical to be safe
        state["intent_type"] = "analytical"
        return state


# ============================================================================
# DIRECT LLM RESPONSE (For Chitchat)
# ============================================================================

def direct_llm_response(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate direct LLM response for chitchat without calling any APIs
    
    Args:
        state: Current chat state
        
    Returns:
        Updated state with formatted_response
    """
    logger.info("=== DIRECT LLM RESPONSE (Chitchat) ===")
    state["current_agent"] = "direct_llm_response"
    
    user_question = state.get("user_question", "")
    module_type = state.get("module_type", "")
    
    try:
        system_prompt = f"""You are a helpful AI assistant for a marketing analytics dashboard.
You are currently in the {module_type} module.

You can help with:
- Google Ads: Campaign performance, ad metrics, keyword insights
- Google Analytics: Website traffic, user behavior, conversions
- Intent Insights: Keyword research and search trends

Be friendly, concise, and helpful. If the user asks what you can do, briefly explain the capabilities of the current module."""

        response = client.chat.completions.create(
            model=DEFAULT_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_question}
            ],
            temperature=0.7,
            max_tokens=300
        )
        
        answer = response.choices[0].message.content.strip()
        state["formatted_response"] = answer
        state["is_complete"] = True
        
        logger.info("Direct response generated successfully")
        return state
        
    except Exception as e:
        logger.error(f"Error generating direct response: {e}")
        state["errors"].append(f"Failed to generate response: {str(e)}")
        state["formatted_response"] = "I apologize, but I encountered an error processing your message. Please try again."
        state["is_complete"] = True
        return state


# ============================================================================
# AGENT 2: PARAMETER EXTRACTION AGENT
# ============================================================================

def agent_2_parameter_extraction(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Agent 2: Extract parameters from user question and context
    Extracts: time periods, IDs, filters, etc.

    Args:
        state: Current chat state

    Returns:
        Updated state with extracted parameters
    """
    logger.info("=" * 80)
    logger.info("🤖 AGENT 2: Parameter Extraction")
    logger.info(f"   Session ID: {state.get('session_id')}")
    logger.info(f"   Module Type: {state.get('module_type')}")
    logger.info(f"   User Question: {state.get('user_question', '')[:100]}...")
    state["current_agent"] = "agent_2_parameter_extraction"

    user_question = state.get("user_question", "")
    module_type = state.get("module_type", "")
    
    try:
        # Build context-specific prompt
        current_date = datetime.now()
        current_year = current_date.year
        system_prompt = f"""You are a parameter extraction agent for a {module_type} analytics system.

Extract the following information from the user's question:
1. TIME PERIOD - Any mention of dates, periods, timeframes
2. SPECIFIC ENTITIES - Campaign names, keywords, locations, devices, etc.
3. METRICS - What metrics they're asking about (clicks, conversions, spend, etc.)
4. FILTERS - Any filtering criteria mentioned

Return your response as a JSON object with these keys:
{{
    "has_time_period": true/false,
    "time_period_text": "extracted text about time period",
    "start_date": "YYYY-MM-DD" or null,
    "end_date": "YYYY-MM-DD" or null,
    "period_keyword": "last 7 days" or "last month" or "yesterday" etc. or null,
    "entities_mentioned": ["entity1", "entity2"],
    "metrics_requested": ["metric1", "metric2"],
    "filters": {{"filter_type": "value"}}
}}

IMPORTANT DATE HANDLING RULES:
- Current date is: {current_date.strftime('%Y-%m-%d')} (Year: {current_year})
- If no time period is mentioned, set has_time_period to false
- Be generous with date parsing - "last week", "past month", "yesterday" etc.
- When month names or ranges are mentioned WITHOUT a year (e.g., "Oct - Dec", "October to December", "Nov"):
  * If the month(s) have already passed this year, use the current year ({current_year})
  * If the month(s) are in the future this year, use the PREVIOUS year ({current_year - 1})
  * Example: If today is November 2024 and user says "Oct - Dec", interpret as October 2024 - December 2024 (use current year since Oct has passed)
  * Example: If today is March 2024 and user says "Nov - Dec", interpret as November 2023 - December 2023 (use previous year)
- NEVER return dates in the future unless explicitly specified with a year
- For relative periods like "last 7 days", prefer using period_keyword instead of explicit dates
"""

        user_prompt = f"Extract parameters from: {user_question}"

        logger.info(f"🧠 AGENT 2: Calling LLM for parameter extraction")
        response = client.chat.completions.create(
            model=DEFAULT_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.1,
            max_tokens=500,
            response_format={"type": "json_object"}
        )

        extracted = json.loads(response.choices[0].message.content)
        logger.info(f"✅ AGENT 2: LLM response received")
        logger.info(f"📊 AGENT 2: Extracted parameters: {extracted}")

        # Process time period
        if extracted.get("has_time_period"):
            # If keyword period provided, parse it first (this takes priority)
            if extracted.get("period_keyword"):
                start_date, end_date, period = parse_period_keyword(
                    extracted["period_keyword"],
                    module_type
                )
                # Only set non-empty values
                if start_date:
                    state["start_date"] = start_date
                if end_date:
                    state["end_date"] = end_date
                if period:
                    state["period"] = period

            # If explicit dates provided by LLM (and not already set from keyword parsing)
            if extracted.get("start_date") and extracted.get("end_date"):
                # If we didn't get dates from keyword parsing, use LLM-extracted dates
                if not state.get("start_date"):
                    state["start_date"] = extracted["start_date"]
                if not state.get("end_date"):
                    state["end_date"] = extracted["end_date"]
                # If we have dates but no period, it's a custom range
                if state.get("start_date") and state.get("end_date") and not state.get("period"):
                    if module_type in ["google_ads", "intent_insights"]:
                        state["period"] = "CUSTOM"

            # Validate and correct future dates (inside has_time_period block)
            if state.get("start_date") and state.get("end_date"):
                logger.info(f"📅 AGENT 2: Validating extracted dates")
                logger.info(f"   Start: {state['start_date']}, End: {state['end_date']}")

                try:
                    start_dt = datetime.strptime(state["start_date"], '%Y-%m-%d')
                    end_dt = datetime.strptime(state["end_date"], '%Y-%m-%d')
                    now = datetime.now()

                    # If end date is in the future, adjust both dates by moving them back one year
                    if end_dt > now:
                        logger.warning(f"⚠️ AGENT 2: End date {state['end_date']} is in the future, adjusting to previous year")

                        # Move both dates back by one year
                        start_dt = start_dt.replace(year=start_dt.year - 1)
                        end_dt = end_dt.replace(year=end_dt.year - 1)

                        state["start_date"] = start_dt.strftime('%Y-%m-%d')
                        state["end_date"] = end_dt.strftime('%Y-%m-%d')

                        logger.info(f"✅ AGENT 2: Adjusted dates - Start: {state['start_date']}, End: {state['end_date']}")
                    else:
                        logger.info(f"✅ AGENT 2: Dates are valid (not in future)")

                except Exception as date_err:
                    logger.error(f"❌ AGENT 2: Error validating dates: {date_err}")
        else:
            # No time period mentioned - check if we already have one from context
            if not (state.get("start_date") or state.get("period")):
                # Need to ask user for time period
                state["needs_user_input"] = True
                state["user_clarification_prompt"] = "I'd be happy to help! What time period would you like to analyze? (e.g., 'last 7 days', 'last month', 'yesterday', or specific dates)"
                logger.info("No time period found - requesting clarification")
                return state
        
        # Store other extracted info
        if extracted.get("entities_mentioned"):
            state["extracted_entities"] = extracted["entities_mentioned"]
        
        if extracted.get("metrics_requested"):
            state["extracted_metrics"] = extracted["metrics_requested"]
        
        if extracted.get("filters"):
            state["extracted_filters"] = extracted["filters"]

        logger.info(f"✅ AGENT 2: Parameters extracted successfully")
        logger.info(f"   Start Date: {state.get('start_date')}")
        logger.info(f"   End Date: {state.get('end_date')}")
        logger.info(f"   Period: {state.get('period')}")
        logger.info(f"   Extracted Entities: {state.get('extracted_entities', [])}")
        logger.info(f"   Extracted Metrics: {state.get('extracted_metrics', [])}")
        logger.info("=" * 80)

        return state

    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"❌ AGENT 2: Error in parameter extraction")
        logger.error(f"   Error type: {type(e).__name__}")
        logger.error(f"   Error message: {str(e)}")
        logger.error("=" * 80)
        logger.error(f"   Full traceback:", exc_info=True)
        state["errors"].append(f"Parameter extraction failed: {str(e)}")
        
        # If extraction fails but we have defaults from context, use those
        if not (state.get("start_date") or state.get("period")):
            state["needs_user_input"] = True
            state["user_clarification_prompt"] = "I'd be happy to help! What time period would you like to analyze?"
        
        return state


# ============================================================================
# HELPER: PARSE PERIOD KEYWORDS
# ============================================================================

def parse_period_keyword(keyword: str, module_type: str) -> Tuple[str, str, str]:
    """
    Parse natural language period keywords into start_date, end_date, and period
    """
    keyword = keyword.lower().strip()
    today = datetime.now().date()
    
    # For Google Ads, use predefined periods without dates
    if module_type in ["google_ads", "intent_insights"]:
        # Last X days - return NO dates, only period
        if "last 7 days" in keyword or "past 7 days" in keyword or "last week" in keyword or "past week" in keyword:
            return ("", "", "LAST_7_DAYS")
        
        if "last 30 days" in keyword or "past 30 days" in keyword or "last month" in keyword or "past month" in keyword:
            return ("", "", "LAST_30_DAYS")
        
        if "last 90 days" in keyword or "past 90 days" in keyword:
            return ("", "", "LAST_90_DAYS")
        
        if "last 365 days" in keyword or "past year" in keyword or "last year" in keyword:
            return ("", "", "LAST_365_DAYS")
        
        # This month / custom periods - return dates with CUSTOM
        if "this month" in keyword:
            start = today.replace(day=1)
            return (
                start.strftime("%Y-%m-%d"),
                today.strftime("%Y-%m-%d"),
                "CUSTOM"
            )
        
        if "yesterday" in keyword:
            yesterday = today - timedelta(days=1)
            return (
                yesterday.strftime("%Y-%m-%d"),
                yesterday.strftime("%Y-%m-%d"),
                "CUSTOM"
            )
    
    # For other modules or unmatched patterns, return dates
    # Yesterday
    if "yesterday" in keyword:
        yesterday = today - timedelta(days=1)
        return (
            yesterday.strftime("%Y-%m-%d"),
            yesterday.strftime("%Y-%m-%d"),
            "day"
        )
    
    # Last X days
    if "last" in keyword and "day" in keyword:
        match = re.search(r'(\d+)\s*day', keyword)
        if match:
            days = int(match.group(1))
            start = today - timedelta(days=days)
            period = f"LAST_{days}_DAYS" if module_type in ["google_ads", "intent_insights"] else "day"
            return (
                start.strftime("%Y-%m-%d"),
                today.strftime("%Y-%m-%d"),
                period
            )
    
    # This month
    if "this month" in keyword:
        start = today.replace(day=1)
        return (
            start.strftime("%Y-%m-%d"),
            today.strftime("%Y-%m-%d"),
            "month"
        )
    
    # Default
    logger.warning(f"Could not parse period keyword '{keyword}', defaulting to last 7 days")
    if module_type in ["google_ads", "intent_insights"]:
        return ("", "", "LAST_7_DAYS")
    else:
        start = today - timedelta(days=7)
        return (
            start.strftime("%Y-%m-%d"),
            today.strftime("%Y-%m-%d"),
            "day"
        )

# ============================================================================
# AGENT 3: ENDPOINT SELECTION AGENT
# ============================================================================

def agent_3_endpoint_selection(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Agent 3: Select appropriate endpoints based on user question

    Args:
        state: Current chat state

    Returns:
        Updated state with selected_endpoints
    """
    logger.info("=" * 80)
    logger.info("🤖 AGENT 3: Endpoint Selection")
    logger.info(f"   Session ID: {state.get('session_id')}")
    logger.info(f"   Module Type: {state.get('module_type')}")
    logger.info(f"   User Question: {state.get('user_question', '')[:100]}...")
    state["current_agent"] = "agent_3_endpoint_selection"

    user_question = state.get("user_question", "")
    available_endpoints = state.get("available_endpoints", [])
    extracted_metrics = state.get("extracted_metrics", [])
    extracted_entities = state.get("extracted_entities", [])

    logger.info(f"   Available endpoints count: {len(available_endpoints)}")
    logger.info(f"   Extracted metrics: {extracted_metrics}")
    logger.info(f"   Extracted entities: {extracted_entities}")

    if not available_endpoints:
        logger.error("❌ AGENT 3: No available endpoints in state")
        state["errors"].append("No endpoints available for this module")
        return state
    
    try:
        # Create endpoint descriptions for LLM
        endpoint_descriptions = []
        for i, ep in enumerate(available_endpoints):
            desc = f"{i}. {ep['name']} - {ep['path']} ({ep['method']})"
            if ep.get('description'):
                desc += f" - {ep['description']}"
            endpoint_descriptions.append(desc)
        
        endpoints_text = "\n".join(endpoint_descriptions)
        
        system_prompt = f"""You are an endpoint selection agent for a marketing analytics API.

Given a user question, select the most appropriate endpoint(s) to call.

Available endpoints:
{endpoints_text}

Rules:
1. Select endpoints that are most relevant to the user's question
2. You can select multiple endpoints if needed (but prefer fewer)
3. Consider the metrics and entities mentioned in the question
4. Return the endpoint names as a JSON array

Return format:
{{
    "selected_endpoints": ["endpoint_name1", "endpoint_name2"],
    "reasoning": "brief explanation"
}}
"""

        user_prompt = f"""User question: {user_question}

Mentioned metrics: {', '.join(extracted_metrics) if extracted_metrics else 'none'}
Mentioned entities: {', '.join(extracted_entities) if extracted_entities else 'none'}

Select the best endpoint(s) to answer this question."""

        logger.info(f"🧠 AGENT 3: Calling LLM for endpoint selection")
        response = client.chat.completions.create(
            model=DEFAULT_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.2,
            max_tokens=500,
            response_format={"type": "json_object"}
        )

        result = json.loads(response.choices[0].message.content)
        selected_names = result.get("selected_endpoints", [])
        reasoning = result.get("reasoning", "")

        logger.info(f"✅ AGENT 3: LLM response received")
        logger.info(f"💡 AGENT 3: Selection reasoning: {reasoning}")
        logger.info(f"📝 AGENT 3: Selected endpoint names: {selected_names}")

        # Convert endpoint names to full endpoint objects
        selected_endpoints = []
        for name in selected_names:
            endpoint = next((ep for ep in available_endpoints if ep['name'] == name), None)
            if endpoint:
                selected_endpoints.append(endpoint)
                logger.info(f"   ✅ Found endpoint: {name}")
            else:
                logger.warning(f"   ⚠️ AGENT 3: Selected endpoint '{name}' not found in available endpoints")

        if not selected_endpoints:
            # Fallback: select a default endpoint based on module
            logger.warning("⚠️ AGENT 3: No endpoints selected by LLM, using default")
            selected_endpoints = [available_endpoints[0]] if available_endpoints else []

        state["selected_endpoints"] = selected_endpoints
        logger.info(f"✅ AGENT 3: Final selection - {len(selected_endpoints)} endpoint(s)")
        for ep in selected_endpoints:
            logger.info(f"   - {ep['name']} ({ep['path']})")
        logger.info("=" * 80)

        return state

    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"❌ AGENT 3: Error in endpoint selection")
        logger.error(f"   Error type: {type(e).__name__}")
        logger.error(f"   Error message: {str(e)}")
        logger.error("=" * 80)
        logger.error(f"   Full traceback:", exc_info=True)
        state["errors"].append(f"Endpoint selection failed: {str(e)}")
        # Fallback to first endpoint
        if available_endpoints:
            state["selected_endpoints"] = [available_endpoints[0]]
        return state


# ============================================================================
# AGENT 5: DATA PROCESSING & LLM ANALYSIS AGENT
# ============================================================================

def agent_5_data_processing_and_analysis(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Agent 5: Process endpoint responses and generate insights with LLM
    Handles chunking if response is too large

    Args:
        state: Current chat state

    Returns:
        Updated state with llm_insights
    """
    logger.info("=" * 80)
    logger.info("🤖 AGENT 5: Data Processing & LLM Analysis")
    logger.info(f"   Session ID: {state.get('session_id')}")
    logger.info(f"   Module Type: {state.get('module_type')}")
    logger.info(f"   User Question: {state.get('user_question', '')[:100]}...")
    state["current_agent"] = "agent_5_data_processing_and_analysis"

    user_question = state.get("user_question", "")
    endpoint_responses = state.get("endpoint_responses", [])

    logger.info(f"   Endpoint responses count: {len(endpoint_responses)}")
    for i, resp in enumerate(endpoint_responses):
        logger.info(f"   Response {i+1}: endpoint={resp.get('endpoint', 'unknown')}, success={resp.get('success', False)}, data_size={len(str(resp.get('data', '')))}")

    if not endpoint_responses:
        logger.warning("⚠️ AGENT 5: No endpoint responses to analyze")
        state["llm_insights"] = "I didn't receive any data to analyze. Please try again."
        return state

    try:
        # Combine all endpoint data
        combined_data = {
            "question": user_question,
            "data": endpoint_responses
        }

        data_json = json.dumps(combined_data, indent=2)

        # Check token size (rough estimate: 1 token ≈ 4 characters)
        estimated_tokens = len(data_json) / 4
        max_context_tokens = 8000  # Leave room for system prompt and response

        logger.info(f"📊 AGENT 5: Data size: {len(data_json)} chars, ~{estimated_tokens:.0f} tokens")

        if estimated_tokens > max_context_tokens:
            logger.info(f"🧩 AGENT 5: Data too large, using chunking strategy")
            insights = process_large_data_with_chunking(user_question, endpoint_responses)
        else:
            logger.info(f"🧠 AGENT 5: Data size acceptable, processing directly with LLM")
            insights = process_data_directly(user_question, endpoint_responses)

        state["llm_insights"] = insights
        logger.info(f"✅ AGENT 5: Data analysis completed successfully")
        logger.info(f"   Insights length: {len(insights)} chars")
        logger.info("=" * 80)

        return state

    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"❌ AGENT 5: Error in data processing and analysis")
        logger.error(f"   Error type: {type(e).__name__}")
        logger.error(f"   Error message: {str(e)}")
        logger.error("=" * 80)
        logger.error(f"   Full traceback:", exc_info=True)
        state["errors"].append(f"Data analysis failed: {str(e)}")
        state["llm_insights"] = "I encountered an error while analyzing the data. Please try again."
        return state


def process_data_directly(question: str, endpoint_responses: List[Dict[str, Any]]) -> str:
    """
    Process data directly with LLM (data is small enough)
    
    Args:
        question: User's question
        endpoint_responses: List of endpoint response data
        
    Returns:
        Analysis insights as string
    """
    system_prompt = """You are a marketing analytics expert analyzing data from various APIs.

Your job:
1. Analyze the provided data thoroughly
2. Answer the user's question directly
3. Provide key insights and patterns
4. Include specific numbers and metrics
5. Be concise but comprehensive

Format your response clearly with:
- Direct answer to the question first
- Key metrics and numbers
- Important insights or trends
- Actionable recommendations if applicable

Do not mention technical details like API endpoints or JSON structure."""

    data_text = json.dumps(endpoint_responses, indent=2)
    
    user_prompt = f"""Question: {question}

Data:
{data_text}

Analyze this data and provide insights."""

    response = client.chat.completions.create(
        model=DEFAULT_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0.3,
        max_tokens=1500
    )
    
    return response.choices[0].message.content.strip()


def process_large_data_with_chunking(
    question: str,
    endpoint_responses: List[Dict[str, Any]]
) -> str:
    """
    Process large data by chunking and aggregating insights
    
    Args:
        question: User's question
        endpoint_responses: List of endpoint response data
        
    Returns:
        Aggregated insights as string
    """
    logger.info("Processing large dataset with chunking")
    
    # Split endpoint responses into chunks
    chunk_size = 3  # Process 3 endpoints at a time
    chunks = [endpoint_responses[i:i + chunk_size] for i in range(0, len(endpoint_responses), chunk_size)]
    
    partial_insights = []
    
    for i, chunk in enumerate(chunks):
        logger.info(f"Processing chunk {i+1}/{len(chunks)}")
        
        system_prompt = """You are a marketing analytics expert analyzing a portion of data.

Provide a concise summary of the key metrics and insights from this data chunk.
Focus on the most important numbers and trends."""

        data_text = json.dumps(chunk, indent=2)
        
        user_prompt = f"""Question: {question}

Data chunk {i+1}:
{data_text}

Summarize the key insights from this chunk."""

        response = client.chat.completions.create(
            model=DEFAULT_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.3,
            max_tokens=800
        )
        
        partial_insights.append(response.choices[0].message.content.strip())
    
    # Now aggregate all partial insights
    logger.info("Aggregating partial insights")
    
    system_prompt = """You are a marketing analytics expert creating a final comprehensive report.

You've analyzed the data in chunks. Now synthesize all the partial insights into one cohesive answer.

Provide:
1. Direct answer to the user's question
2. Key metrics and numbers
3. Important insights and trends
4. Actionable recommendations

Be clear and well-structured."""

    insights_text = "\n\n".join([f"Chunk {i+1} insights:\n{ins}" for i, ins in enumerate(partial_insights)])
    
    user_prompt = f"""Question: {question}

Partial insights from data analysis:
{insights_text}

Create a comprehensive final answer."""

    response = client.chat.completions.create(
        model=DEFAULT_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0.3,
        max_tokens=2000
    )
    
    return response.choices[0].message.content.strip()


# ============================================================================
# AGENT 6: RESPONSE FORMATTING & ENHANCEMENT AGENT
# ============================================================================

def agent_6_response_formatting(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Agent 6: Format the final response for frontend with tables/charts if needed
    
    Args:
        state: Current chat state
        
    Returns:
        Updated state with formatted_response and visualizations
    """
    logger.info("=== AGENT 6: Response Formatting & Enhancement ===")
    state["current_agent"] = "agent_6_response_formatting"
    
    user_question = state.get("user_question", "")
    llm_insights = state.get("llm_insights", "")
    endpoint_responses = state.get("endpoint_responses", [])
    
    try:
        system_prompt = """You are a response formatting expert for a marketing analytics dashboard.

Your job:
1. Take the analysis insights and make them user-friendly
2. Structure the response with markdown formatting
3. Use **bold** for key metrics
4. Use bullet points for lists
5. Add section headers where appropriate
6. Make it scannable and easy to read
7. Determine if data should be shown as a table or chart

IMPORTANT: If the data would benefit from visualization (tables or charts), add a special marker:
- For tables: Add {{TABLE: description}}
- For charts: Add {{CHART: chart_type, description}}

Chart types: line, bar, pie, area

Example response:
## Performance Summary

Your campaigns achieved **1,234 clicks** with a **2.5% CTR** in the last 7 days.

### Key Metrics
- Total spend: **$456.78**
- Conversions: **45**
- Cost per conversion: **$10.15**

{{TABLE: Campaign performance breakdown}}

### Trends
{{CHART: line, Daily clicks and conversions over time}}

The top performing campaign was "Summer Sale" with 500 clicks.
"""

        user_prompt = f"""User question: {user_question}

Analysis insights:
{llm_insights}

Format this into a user-friendly response. Determine if tables or charts would help visualize the data."""

        response = client.chat.completions.create(
            model=DEFAULT_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.3,
            max_tokens=2000
        )
        
        formatted_response = response.choices[0].message.content.strip()
        
        # Extract visualization markers
        visualizations = extract_visualization_markers(formatted_response, endpoint_responses)
        
        # Remove markers from final response
        formatted_response = re.sub(r'\{\{TABLE:.*?\}\}', '', formatted_response)
        formatted_response = re.sub(r'\{\{CHART:.*?\}\}', '', formatted_response)
        
        state["formatted_response"] = formatted_response
        state["visualizations"] = visualizations
        state["is_complete"] = True
        state["processing_end_time"] = datetime.utcnow()
        
        logger.info("Response formatted successfully")
        return state
        
    except Exception as e:
        logger.error(f"Error in response formatting: {e}")
        state["errors"].append(f"Response formatting failed: {str(e)}")
        # Use raw insights as fallback
        state["formatted_response"] = llm_insights or "I encountered an error formatting the response."
        state["is_complete"] = True
        return state


def extract_visualization_markers(
    response: str,
    endpoint_responses: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Extract visualization markers from formatted response
    
    Args:
        response: Formatted response text
        endpoint_responses: Raw endpoint data
        
    Returns:
        Dictionary with visualization data
    """
    visualizations = {
        "has_table": False,
        "has_chart": False,
        "tables": [],
        "charts": []
    }
    
    # Find table markers
    table_matches = re.findall(r'\{\{TABLE:\s*(.*?)\}\}', response)
    if table_matches:
        visualizations["has_table"] = True
        for desc in table_matches:
            visualizations["tables"].append({
                "description": desc.strip(),
                "data": prepare_table_data(endpoint_responses)
            })
    
    # Find chart markers
    chart_matches = re.findall(r'\{\{CHART:\s*(\w+),\s*(.*?)\}\}', response)
    if chart_matches:
        visualizations["has_chart"] = True
        for chart_type, desc in chart_matches:
            visualizations["charts"].append({
                "type": chart_type.strip(),
                "description": desc.strip(),
                "data": prepare_chart_data(endpoint_responses, chart_type)
            })
    
    return visualizations


def prepare_table_data(endpoint_responses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Prepare data for table visualization
    
    Args:
        endpoint_responses: Raw endpoint data
        
    Returns:
        List of rows for table
    """
    # This is a simple implementation - you can enhance based on your data structure
    table_data = []
    
    for response in endpoint_responses:
        data = response.get("data", {})
        
        # Handle different data structures
        if isinstance(data, list):
            table_data.extend(data[:10])  # Limit to 10 rows
        elif isinstance(data, dict):
            table_data.append(data)
    
    return table_data[:10]  # Maximum 10 rows


def prepare_chart_data(
    endpoint_responses: List[Dict[str, Any]],
    chart_type: str
) -> Dict[str, Any]:
    """
    Prepare data for chart visualization
    
    Args:
        endpoint_responses: Raw endpoint data
        chart_type: Type of chart (line, bar, pie, area)
        
    Returns:
        Chart data in format suitable for frontend charting library
    """
    # Simple implementation - enhance based on your charting library
    chart_data = {
        "labels": [],
        "datasets": []
    }
    
    # Extract time series or categorical data
    for response in endpoint_responses:
        data = response.get("data", {})
        
        # This is a placeholder - customize based on your actual data structure
        if isinstance(data, list) and len(data) > 0:
            if "date" in str(data[0]).lower():
                # Time series data
                chart_data["labels"] = [item.get("date", f"Item {i}") for i, item in enumerate(data)]
                chart_data["datasets"].append({
                    "label": response.get("endpoint", "Data"),
                    "data": [item.get("value", 0) for item in data]
                })
    
    return chart_data


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def log_agent_transition(from_agent: str, to_agent: str, reason: str = ""):
    """Log transition between agents"""
    logger.info(f"Agent transition: {from_agent} → {to_agent}" + (f" ({reason})" if reason else ""))


def estimate_token_count(text: str) -> int:
    """Estimate token count for text (rough approximation)"""
    return len(text) // 4


def truncate_for_logging(data: Any, max_length: int = 200) -> str:
    """Truncate data for logging purposes"""
    data_str = str(data)
    if len(data_str) > max_length:
        return data_str[:max_length] + "..."
    return data_str