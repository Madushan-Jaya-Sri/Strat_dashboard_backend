"""
API Client for LangGraph Multi-Module Chat System
Handles HTTP requests to FastAPI endpoints and MongoDB storage
"""

import logging
import requests
import json
import time
from typing import Dict, Any, List, Optional
from datetime import datetime
from urllib.parse import urljoin
import os


# Initialize logger
logger = logging.getLogger(__name__)

# Configuration
# Use environment variable if set, otherwise detect if running locally or on AWS
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
# For AWS deployment, set environment variable: API_BASE_URL=https://3ixmj4hf2a.us-east-2.awsapprunner.com
logger.info(f"🌐 API Client using base URL: {API_BASE_URL}")

REQUEST_TIMEOUT = 30  # seconds (reduced from 240s = 4 minutes!)
MAX_RETRIES = 2  # retries (reduced from 3)
RETRY_DELAY = 1  # seconds (reduced from 2s)


# ============================================================================
# API CLIENT CLASS
# ============================================================================

class APIClient:
    """
    HTTP client for calling FastAPI endpoints and managing responses
    """
    
    def __init__(self, base_url: str = API_BASE_URL, auth_token: Optional[str] = None):
        """
        Initialize API client
        
        Args:
            base_url: Base URL of the FastAPI server
            auth_token: Authentication token
        """
        self.base_url = base_url.rstrip('/')
        self.auth_token = auth_token
        self.session = requests.Session()
        
        # Set default headers
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json"
        })
        
        if self.auth_token:
            self.session.headers.update({
                "Authorization": f"Bearer {self.auth_token}"
            })
    
    def call_endpoint(
        self,
        endpoint: Dict[str, Any],
        params: Dict[str, Any],
        retry: bool = True,
        timeout: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Call a single endpoint with parameters

        Args:
            endpoint: Endpoint definition dict with 'path', 'method', 'params', etc.
            params: Parameters to pass (path params, query params, body)
            retry: Whether to retry on failure
            timeout: Custom timeout in seconds (defaults to REQUEST_TIMEOUT)

        Returns:
            Dict with endpoint response data and metadata
        """
        endpoint_name = endpoint.get('name', 'unknown')
        endpoint_path = endpoint.get('path', '')
        method = endpoint.get('method', 'GET').upper()
        
        logger.info(f"Calling endpoint: {method} {endpoint_path}")
        
        # Build full URL with path parameters
        url = self._build_url(endpoint_path, params)
        
        # Prepare query parameters
        query_params = self._prepare_query_params(endpoint, params)
        
        # Prepare request body for POST requests
        body = self._prepare_body(endpoint, params)
        
        # Make request with retry logic
        start_time = time.time()
        attempts = 0
        last_error = None
        
        while attempts < (MAX_RETRIES if retry else 1):
            attempts += 1
            
            try:
               
                logger.info(f"🌐 Base URL: {url}")
                logger.info(f"🔍 Query params: {query_params}")

                # Build full URL with params for logging
                if query_params:
                    from urllib.parse import urlencode
                    full_url = f"{url}?{urlencode(query_params)}"
                    logger.info(f"🔗 Complete URL with params: {full_url}")

                request_timeout = timeout if timeout is not None else REQUEST_TIMEOUT

                if method == 'GET':
                    response = self.session.get(url, params=query_params, timeout=request_timeout)
                    logger.info(f"✅ Actual request URL: {response.url}")

                elif method == 'POST':
                    response = self.session.post(
                        url,
                        params=query_params,
                        json=body,
                        timeout=request_timeout
                    )
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
                
                # Check response status
                response.raise_for_status()
                
                # Parse JSON response
                response_data = response.json()
                response_time = time.time() - start_time
                
                logger.info(f"Endpoint {endpoint_name} completed successfully in {response_time:.2f}s")
                
                return {
                    "success": True,
                    "endpoint": endpoint_name,
                    "path": endpoint_path,
                    "method": method,
                    "params": params,
                    "query_params": query_params,
                    "body": body,
                    "data": response_data,
                    "status_code": response.status_code,
                    "response_time": response_time,
                    "timestamp": datetime.utcnow().isoformat(),
                    "attempts": attempts
                }
                
            except requests.exceptions.HTTPError as e:
                last_error = e
                status_code = e.response.status_code if e.response else None
                logger.error(f"HTTP error calling {endpoint_name}: {status_code} - {str(e)}")
                
                # Don't retry on client errors (4xx)
                if status_code and 400 <= status_code < 500:
                    break
                
                if attempts < MAX_RETRIES:
                    logger.info(f"Retrying in {RETRY_DELAY}s... (attempt {attempts}/{MAX_RETRIES})")
                    time.sleep(RETRY_DELAY)
                    
            except requests.exceptions.RequestException as e:
                last_error = e
                logger.error(f"Request error calling {endpoint_name}: {str(e)}")
                
                if attempts < MAX_RETRIES:
                    logger.info(f"Retrying in {RETRY_DELAY}s... (attempt {attempts}/{MAX_RETRIES})")
                    time.sleep(RETRY_DELAY)
            
            except Exception as e:
                last_error = e
                logger.error(f"Unexpected error calling {endpoint_name}: {str(e)}")
                break
        
        # All attempts failed
        response_time = time.time() - start_time
        error_message = str(last_error) if last_error else "Unknown error"
        
        return {
            "success": False,
            "endpoint": endpoint_name,
            "path": endpoint_path,
            "method": method,
            "params": params,
            "query_params": query_params,
            "body": body,
            "error": error_message,
            "response_time": response_time,
            "timestamp": datetime.utcnow().isoformat(),
            "attempts": attempts
        }
    
    def call_multiple_endpoints(
        self,
        endpoints: List[Dict[str, Any]],
        params: Dict[str, Any],
        parallel: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Call multiple endpoints sequentially or in parallel
        
        Args:
            endpoints: List of endpoint definitions
            params: Parameters to pass to all endpoints
            parallel: Whether to call in parallel (currently not implemented)
            
        Returns:
            List of endpoint responses
        """
        logger.info(f"Calling {len(endpoints)} endpoints")
        
        responses = []
        
        for endpoint in endpoints:
            response = self.call_endpoint(endpoint, params)
            responses.append(response)
            
            # Add small delay between requests to avoid rate limiting
            if not parallel:
                time.sleep(0.5)
        
        successful = sum(1 for r in responses if r.get('success'))
        logger.info(f"Completed {successful}/{len(endpoints)} endpoint calls successfully")
        
        return responses
    
    def _build_url(self, endpoint_path: str, params: Dict[str, Any]) -> str:
        """
        Build full URL with path parameters replaced
        
        Args:
            endpoint_path: Path template like '/api/ads/campaigns/{customer_id}'
            params: Parameters dict
            
        Returns:
            Full URL with path params replaced
        """
        # Replace path parameters
        url_path = endpoint_path
        
        # Find all {param} placeholders
        import re
        placeholders = re.findall(r'\{(\w+)\}', endpoint_path)
        
        for placeholder in placeholders:
            if placeholder in params and params[placeholder]:
                url_path = url_path.replace(f'{{{placeholder}}}', str(params[placeholder]))
            else:
                logger.warning(f"Missing path parameter: {placeholder}")
        
        return urljoin(self.base_url, url_path)
    
    def _prepare_query_params(
        self,
        endpoint: Dict[str, Any],
        params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Prepare query parameters for GET requests
        
        Args:
            endpoint: Endpoint definition
            params: All parameters
            
        Returns:
            Dict of query parameters
        """
        query_params = {}
        
        # Get expected params from endpoint definition
        expected_params = endpoint.get('params', [])
        optional_params = endpoint.get('optional_params', [])
        
        # Add all expected and optional params that are not path params
        for param_name in expected_params + optional_params:
            # Skip path parameters (they have {} in endpoint path)
            if f'{{{param_name}}}' not in endpoint.get('path', ''):
                if param_name in params and params[param_name] is not None:
                    query_params[param_name] = params[param_name]
        
        return query_params
    
    def _prepare_body(
        self,
        endpoint: Dict[str, Any],
        params: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Prepare request body for POST requests
        
        Args:
            endpoint: Endpoint definition
            params: All parameters
            
        Returns:
            Dict for request body or None
        """
        if endpoint.get('method', 'GET').upper() != 'POST':
            return None
        
        body_params = endpoint.get('body_params', [])
        
        if not body_params:
            return None
        
        body = {}
        for param_name in body_params:
            if param_name in params and params[param_name] is not None:
                body[param_name] = params[param_name]
        
        return body if body else None


# ============================================================================
# AGENT 4: API EXECUTION AGENT
# ============================================================================

"""
Replace the agent_4_api_execution function in your api_client.py
Starting at line 317
"""

import asyncio

def agent_4_api_execution(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Agent 4: Execute selected API endpoints using INTERNAL calls
    Saves all responses to MongoDB via the triggered_endpoints list
    SYNCHRONOUS wrapper around async endpoint calls

    Args:
        state: Current chat state

    Returns:
        Updated state with endpoint_responses
    """
    logger.info("=" * 80)
    logger.info("🤖 AGENT 4: API Execution")
    logger.info(f"   Session ID: {state.get('session_id')}")
    logger.info(f"   Module Type: {state.get('module_type')}")
    state["current_agent"] = "agent_4_api_execution"

    selected_endpoints = state.get("selected_endpoints", [])

    logger.info(f"   Selected endpoints count: {len(selected_endpoints)}")
    for i, ep in enumerate(selected_endpoints):
        logger.info(f"   Endpoint {i+1}: {ep.get('name')} ({ep.get('method')} {ep.get('path')})")

    if not selected_endpoints:
        logger.warning("⚠️ AGENT 4: No endpoints selected for execution")
        state["warnings"].append("No endpoints were selected to call")
        return state

    # Build parameters dict from state
    params = build_params_from_state(state)

    logger.info(f"📊 AGENT 4: Executing {len(selected_endpoints)} endpoints")
    logger.info(f"   Parameters: {params}")
    
    try:
        # Build current_user dict from state
        current_user = {
            "email": state.get("user_email"),
            "name": state.get("user_name", ""),
            "picture": state.get("user_picture", ""),
            "auth_provider": "google"
        }
        
        logger.info(f"🔧 Using internal calls for user: {current_user['email']}")
        
        # Import internal caller
        from chat.utils.internal_api_caller import call_internal_endpoint
        
        # Call endpoints directly (no HTTP)
        responses = []
        for idx, endpoint in enumerate(selected_endpoints, 1):
            logger.info(f"🔧 AGENT 4: Calling endpoint {idx}/{len(selected_endpoints)}: {endpoint.get('name')}")
            logger.info(f"   Method: {endpoint.get('method')}")
            logger.info(f"   Path: {endpoint.get('path')}")

            # Prepare params for this endpoint
            endpoint_params = params.copy()

            # Handle body_params for POST requests
            body_params_list = endpoint.get('body_params', [])
            if body_params_list and endpoint.get('method') == 'POST':
                # Move specified params into body
                body = {}
                for body_param in body_params_list:
                    if body_param in endpoint_params:
                        body[body_param] = endpoint_params.pop(body_param)

                # Wrap in body key for internal API caller
                endpoint_params['body'] = body
                logger.info(f"📦 AGENT 4: Moved {body_params_list} to request body")
                logger.info(f"   Body: {body}")

            # Run async function synchronously
            try:
                # Check if there's a running event loop
                try:
                    loop = asyncio.get_running_loop()
                    # If we're already in an async context, create a task
                    # This shouldn't happen in LangGraph sync execution, but just in case
                    import nest_asyncio
                    nest_asyncio.apply()
                    response = loop.run_until_complete(call_internal_endpoint(
                        endpoint_name=endpoint.get("name"),
                        endpoint_path=endpoint.get("path"),
                        method=endpoint.get("method", "GET"),
                        params=endpoint_params,
                        current_user=current_user
                    ))
                except RuntimeError:
                    # No running loop, use asyncio.run()
                    response = asyncio.run(call_internal_endpoint(
                        endpoint_name=endpoint.get("name"),
                        endpoint_path=endpoint.get("path"),
                        method=endpoint.get("method", "GET"),
                        params=endpoint_params,
                        current_user=current_user
                    ))
            except Exception as call_error:
                logger.error(f"❌ AGENT 4: Error calling {endpoint.get('name')}")
                logger.error(f"   Error type: {type(call_error).__name__}")
                logger.error(f"   Error message: {str(call_error)}")
                logger.error(f"   Full traceback:", exc_info=True)
                response = {
                    "success": False,
                    "endpoint": endpoint.get("name"),
                    "error": str(call_error)
                }

            # Log response
            if response.get('success'):
                logger.info(f"✅ AGENT 4: Endpoint {endpoint.get('name')} succeeded")
                logger.info(f"   Response data size: {len(str(response.get('data', '')))} chars")
            else:
                logger.error(f"❌ AGENT 4: Endpoint {endpoint.get('name')} failed")
                logger.error(f"   Error: {response.get('error', 'Unknown error')}")

            responses.append(response)
        
        # Separate successful and failed responses
        successful_responses = []
        failed_responses = []
        
        for response in responses:
            if response.get('success'):
                successful_responses.append(response)
            else:
                failed_responses.append(response)
                state["errors"].append(
                    f"Endpoint {response['endpoint']} failed: {response.get('error', 'Unknown error')}"
                )
        
        # Store all responses for MongoDB
        state["triggered_endpoints"] = responses

        # Store only successful response data for LLM analysis
        state["endpoint_responses"] = successful_responses

        logger.info(f"✅ AGENT 4: API execution completed")
        logger.info(f"   Total endpoints: {len(responses)}")
        logger.info(f"   Successful: {len(successful_responses)}")
        logger.info(f"   Failed: {len(failed_responses)}")

        if successful_responses:
            logger.info(f"   Successful endpoints:")
            for resp in successful_responses:
                logger.info(f"     - {resp.get('endpoint')}")

        if failed_responses:
            logger.info(f"   Failed endpoints:")
            for resp in failed_responses:
                logger.info(f"     - {resp.get('endpoint')}: {resp.get('error', 'Unknown error')}")

        if not successful_responses:
            logger.warning("⚠️ AGENT 4: All API calls failed. Unable to retrieve data.")
            state["warnings"].append("All API calls failed. Unable to retrieve data.")

        logger.info("=" * 80)

        return state

    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"❌ AGENT 4: Error executing endpoints")
        logger.error(f"   Error type: {type(e).__name__}")
        logger.error(f"   Error message: {str(e)}")
        logger.error("=" * 80)
        logger.error(f"   Full traceback:", exc_info=True)
        state["errors"].append(f"Endpoint execution failed: {str(e)}")
        return state

def build_params_from_state(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build parameters dict from state for API calls
    
    Args:
        state: Current chat state
        
    Returns:
        Parameters dict for API calls
    """
    module_type = state.get("module_type")
    params = {}
    
    # Common parameters - Handle Google Ads period logic
    period = state.get("period")
    start_date = state.get("start_date")
    end_date = state.get("end_date")
    
    # Google Ads modules: LAST_X_DAYS periods should NOT include dates
    if module_type in ["google_ads", "intent_insights"]:
        predefined_periods = ["LAST_7_DAYS", "LAST_30_DAYS", "LAST_90_DAYS", "LAST_365_DAYS"]
        
        if period and period in predefined_periods:
            # Only include period, exclude dates
            params["period"] = period
        elif start_date and end_date:
            # Custom date range - include dates and set period to CUSTOM
            params["period"] = "CUSTOM"
            params["start_date"] = start_date
            params["end_date"] = end_date
        elif period:
            # Some other period value, include it
            params["period"] = period
    else:
        # Other modules (GA4, etc.) - keep existing behavior
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if period:
            params["period"] = period
    
    # Module-specific parameters
    if module_type == "google_ads":
        if state.get("customer_id"):
            params["customer_id"] = state["customer_id"]
        if state.get("campaign_ids"):
            params["campaign_ids"] = state["campaign_ids"]
        if state.get("location"):
            params["location"] = state["location"]
        if state.get("device"):
            params["device"] = state["device"]
    
    elif module_type == "google_analytics":
        if state.get("property_id"):
            params["property_id"] = state["property_id"]
        if state.get("dimension"):
            params["dimension"] = state["dimension"]
        if state.get("metric"):
            params["metric"] = state["metric"]
    
    elif module_type == "intent_insights":
        # Intent insights uses customer_id (same as Google Ads account ID)
        if state.get("customer_id"):
            params["customer_id"] = state["customer_id"]
        if state.get("seed_keywords"):
            params["seed_keywords"] = state["seed_keywords"]
        if state.get("country"):
            params["country"] = state["country"]
        if state.get("timeframe"):
            params["timeframe"] = state["timeframe"]
        params["include_zero_volume"] = state.get("include_zero_volume", True)

    elif module_type == "meta_ads":
        if state.get("account_id"):
            params["account_id"] = state["account_id"]
        if state.get("campaign_ids"):
            params["campaign_ids"] = state["campaign_ids"]
        if state.get("adset_ids"):
            params["adset_ids"] = state["adset_ids"]
        if state.get("ad_ids"):
            params["ad_ids"] = state["ad_ids"]
        # Meta Ads uses period format like "7d", "30d", "90d"
        # Keep the period and date params as is for Meta Ads

    # ========================================================================
    # FINAL DATE VALIDATION - Ensure no future dates are passed to endpoints
    # ========================================================================
    if params.get("start_date") and params.get("end_date"):
        from datetime import datetime

        logger.info("=" * 80)
        logger.info("📅 BUILD_PARAMS: Final date validation before endpoint execution")
        logger.info(f"   Original Start Date: {params['start_date']}")
        logger.info(f"   Original End Date: {params['end_date']}")

        try:
            start_dt = datetime.strptime(params["start_date"], '%Y-%m-%d')
            end_dt = datetime.strptime(params["end_date"], '%Y-%m-%d')
            today = datetime.now()

            logger.info(f"   Current Date: {today.strftime('%Y-%m-%d')}")

            # If end date is in the future, replace it with today's date
            if end_dt > today:
                logger.warning(f"⚠️ BUILD_PARAMS: End date {params['end_date']} is in the future!")
                logger.warning(f"   Replacing with current date: {today.strftime('%Y-%m-%d')}")
                params["end_date"] = today.strftime('%Y-%m-%d')

            # If start date is in the future, replace it with today as well
            if start_dt > today:
                logger.warning(f"⚠️ BUILD_PARAMS: Start date {params['start_date']} is in the future!")
                logger.warning(f"   Replacing with current date: {today.strftime('%Y-%m-%d')}")
                params["start_date"] = today.strftime('%Y-%m-%d')

            logger.info(f"✅ BUILD_PARAMS: Final validated dates:")
            logger.info(f"   Start Date: {params['start_date']}")
            logger.info(f"   End Date: {params['end_date']}")
            logger.info("=" * 80)

        except Exception as date_err:
            logger.error(f"❌ BUILD_PARAMS: Error validating dates: {date_err}")
            logger.error("=" * 80)

    return params


# ============================================================================
# MONGODB STORAGE FUNCTIONS
# ============================================================================

def save_conversation_to_mongodb(
    session_id: str,
    user_email: str,
    module_type: str,
    state: Dict[str, Any],
    mongo_manager
) -> bool:
    """
    Save complete conversation state to MongoDB
    
    Args:
        session_id: Unique session identifier
        user_email: User's email
        module_type: Module type
        state: Final state with all data
        mongo_manager: MongoManager instance
        
    Returns:
        True if saved successfully
    """
    try:
        from datetime import datetime
        
        # Prepare conversation document
        conversation_doc = {
            "session_id": session_id,
            "user_email": user_email,
            "module_type": module_type,
            "timestamp": datetime.utcnow(),
            
            # User interaction
            "user_question": state.get("user_question"),
            "formatted_response": state.get("formatted_response"),
            "visualizations": state.get("visualizations"),
            
            # Parameters used
            "parameters": {
                "start_date": state.get("start_date"),
                "end_date": state.get("end_date"),
                "period": state.get("period"),
                "customer_id": state.get("customer_id"),
                "property_id": state.get("property_id"),
                "account_id": state.get("account_id"),
                "page_id": state.get("page_id"),
            },
            
            # All triggered endpoints with full details
            "triggered_endpoints": state.get("triggered_endpoints", []),
            
            # Processing metadata
            "processing_metadata": {
                "intent_type": state.get("intent_type"),
                "selected_endpoints": [ep.get('name') for ep in state.get("selected_endpoints", [])],
                "processing_start_time": state.get("processing_start_time"),
                "processing_end_time": state.get("processing_end_time"),
                "total_processing_time": calculate_processing_time(state),
                "errors": state.get("errors", []),
                "warnings": state.get("warnings", [])
            },
            
            # Conversation context
            "is_active": True,
            "last_activity": datetime.utcnow()
        }
        
        # Save to MongoDB using the chat collection
        collection_name = f"chat_{module_type}"
        result = mongo_manager.db[collection_name].insert_one(conversation_doc)
        
        logger.info(f"Saved conversation to MongoDB: {result.inserted_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving conversation to MongoDB: {e}")
        return False


def calculate_processing_time(state: Dict[str, Any]) -> Optional[float]:
    """
    Calculate total processing time from state
    
    Args:
        state: Chat state
        
    Returns:
        Processing time in seconds or None
    """
    start = state.get("processing_start_time")
    end = state.get("processing_end_time")
    
    if start and end:
        if isinstance(start, datetime) and isinstance(end, datetime):
            return (end - start).total_seconds()
    
    return None


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def validate_response_data(response: Dict[str, Any]) -> bool:
    """
    Validate that response contains usable data
    
    Args:
        response: API response
        
    Returns:
        True if response has valid data
    """
    if not response.get('success'):
        return False
    
    data = response.get('data')
    
    if data is None:
        return False
    
    # Check if data is empty
    if isinstance(data, (list, dict)):
        return len(data) > 0
    
    return True


def format_response_for_logging(response: Dict[str, Any], max_length: int = 500) -> str:
    """
    Format response for logging (truncate large data)
    
    Args:
        response: API response
        max_length: Maximum length for logging
        
    Returns:
        Formatted string for logging
    """
    log_data = {
        "endpoint": response.get("endpoint"),
        "success": response.get("success"),
        "status_code": response.get("status_code"),
        "response_time": f"{response.get('response_time', 0):.2f}s"
    }
    
    if not response.get('success'):
        log_data["error"] = response.get("error")
    else:
        data = response.get("data")
        if isinstance(data, list):
            log_data["data_count"] = len(data)
        elif isinstance(data, dict):
            log_data["data_keys"] = list(data.keys())
    
    result = json.dumps(log_data, indent=2)
    
    if len(result) > max_length:
        return result[:max_length] + "\n... (truncated)"
    
    return result