# """
# API Client for LangGraph Multi-Module Chat System
# Handles HTTP requests to FastAPI endpoints and MongoDB storage
# """

# import logging
# import requests
# import json
# import time
# from typing import Dict, Any, List, Optional
# from datetime import datetime
# from urllib.parse import urljoin
# import os

# # Initialize logger
# logger = logging.getLogger(__name__)

# # Configuration
# API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
# REQUEST_TIMEOUT = 30  # seconds
# MAX_RETRIES = 3
# RETRY_DELAY = 2  # seconds


# # ============================================================================
# # API CLIENT CLASS
# # ============================================================================

# class APIClient:
#     """
#     HTTP client for calling FastAPI endpoints and managing responses
#     """
    
#     def __init__(self, base_url: str = API_BASE_URL, auth_token: Optional[str] = None):
#         """
#         Initialize API client
        
#         Args:
#             base_url: Base URL of the FastAPI server
#             auth_token: Authentication token (Google or Meta)
#         """
#         self.base_url = base_url.rstrip('/')
#         self.auth_token = auth_token
#         self.session = requests.Session()
        
#         # Set default headers
#         self.session.headers.update({
#             "Content-Type": "application/json",
#             "Accept": "application/json"
#         })
        
#         if self.auth_token:
#             self.session.headers.update({
#                 "Authorization": f"Bearer {self.auth_token}"
#             })
    
#     def call_endpoint(
#         self,
#         endpoint: Dict[str, Any],
#         params: Dict[str, Any],
#         retry: bool = True
#     ) -> Dict[str, Any]:
#         """
#         Call a single endpoint with parameters
        
#         Args:
#             endpoint: Endpoint definition dict with 'path', 'method', 'params', etc.
#             params: Parameters to pass (path params, query params, body)
#             retry: Whether to retry on failure
            
#         Returns:
#             Dict with endpoint response data and metadata
#         """
#         endpoint_name = endpoint.get('name', 'unknown')
#         endpoint_path = endpoint.get('path', '')
#         method = endpoint.get('method', 'GET').upper()
        
#         logger.info(f"Calling endpoint: {method} {endpoint_path}")
        
#         # Build full URL with path parameters
#         url = self._build_url(endpoint_path, params)
        
#         # Prepare query parameters
#         query_params = self._prepare_query_params(endpoint, params)
        
#         # Prepare request body for POST requests
#         body = self._prepare_body(endpoint, params)
        
#         # Make request with retry logic
#         start_time = time.time()
#         attempts = 0
#         last_error = None
        
#         while attempts < (MAX_RETRIES if retry else 1):
#             attempts += 1
            
#             try:
#                 if method == 'GET':
#                     response = self.session.get(
#                         url,
#                         params=query_params,
#                         timeout=REQUEST_TIMEOUT
#                     )
#                 elif method == 'POST':
#                     response = self.session.post(
#                         url,
#                         params=query_params,
#                         json=body,
#                         timeout=REQUEST_TIMEOUT
#                     )
#                 else:
#                     raise ValueError(f"Unsupported HTTP method: {method}")
                
#                 # Check response status
#                 response.raise_for_status()
                
#                 # Parse JSON response
#                 response_data = response.json()
#                 response_time = time.time() - start_time
                
#                 logger.info(f"Endpoint {endpoint_name} completed successfully in {response_time:.2f}s")
                
#                 return {
#                     "success": True,
#                     "endpoint": endpoint_name,
#                     "path": endpoint_path,
#                     "method": method,
#                     "params": params,
#                     "query_params": query_params,
#                     "body": body,
#                     "data": response_data,
#                     "status_code": response.status_code,
#                     "response_time": response_time,
#                     "timestamp": datetime.utcnow().isoformat(),
#                     "attempts": attempts
#                 }
                
#             except requests.exceptions.HTTPError as e:
#                 last_error = e
#                 status_code = e.response.status_code if e.response else None
#                 logger.error(f"HTTP error calling {endpoint_name}: {status_code} - {str(e)}")
                
#                 # Don't retry on client errors (4xx)
#                 if status_code and 400 <= status_code < 500:
#                     break
                
#                 if attempts < MAX_RETRIES:
#                     logger.info(f"Retrying in {RETRY_DELAY}s... (attempt {attempts}/{MAX_RETRIES})")
#                     time.sleep(RETRY_DELAY)
                    
#             except requests.exceptions.RequestException as e:
#                 last_error = e
#                 logger.error(f"Request error calling {endpoint_name}: {str(e)}")
                
#                 if attempts < MAX_RETRIES:
#                     logger.info(f"Retrying in {RETRY_DELAY}s... (attempt {attempts}/{MAX_RETRIES})")
#                     time.sleep(RETRY_DELAY)
            
#             except Exception as e:
#                 last_error = e
#                 logger.error(f"Unexpected error calling {endpoint_name}: {str(e)}")
#                 break
        
#         # All attempts failed
#         response_time = time.time() - start_time
#         error_message = str(last_error) if last_error else "Unknown error"
        
#         return {
#             "success": False,
#             "endpoint": endpoint_name,
#             "path": endpoint_path,
#             "method": method,
#             "params": params,
#             "query_params": query_params,
#             "body": body,
#             "error": error_message,
#             "response_time": response_time,
#             "timestamp": datetime.utcnow().isoformat(),
#             "attempts": attempts
#         }
    
#     def call_multiple_endpoints(
#         self,
#         endpoints: List[Dict[str, Any]],
#         params: Dict[str, Any],
#         parallel: bool = False
#     ) -> List[Dict[str, Any]]:
#         """
#         Call multiple endpoints sequentially or in parallel
        
#         Args:
#             endpoints: List of endpoint definitions
#             params: Parameters to pass to all endpoints
#             parallel: Whether to call in parallel (currently not implemented)
            
#         Returns:
#             List of endpoint responses
#         """
#         logger.info(f"Calling {len(endpoints)} endpoints")
        
#         responses = []
        
#         for endpoint in endpoints:
#             response = self.call_endpoint(endpoint, params)
#             responses.append(response)
            
#             # Add small delay between requests to avoid rate limiting
#             if not parallel:
#                 time.sleep(0.5)
        
#         successful = sum(1 for r in responses if r.get('success'))
#         logger.info(f"Completed {successful}/{len(endpoints)} endpoint calls successfully")
        
#         return responses
    
#     def _build_url(self, endpoint_path: str, params: Dict[str, Any]) -> str:
#         """
#         Build full URL with path parameters replaced
        
#         Args:
#             endpoint_path: Path template like '/api/ads/campaigns/{customer_id}'
#             params: Parameters dict
            
#         Returns:
#             Full URL with path params replaced
#         """
#         # Replace path parameters
#         url_path = endpoint_path
        
#         # Find all {param} placeholders
#         import re
#         placeholders = re.findall(r'\{(\w+)\}', endpoint_path)
        
#         for placeholder in placeholders:
#             if placeholder in params and params[placeholder]:
#                 url_path = url_path.replace(f'{{{placeholder}}}', str(params[placeholder]))
#             else:
#                 logger.warning(f"Missing path parameter: {placeholder}")
        
#         return urljoin(self.base_url, url_path)
    
#     def _prepare_query_params(
#         self,
#         endpoint: Dict[str, Any],
#         params: Dict[str, Any]
#     ) -> Dict[str, Any]:
#         """
#         Prepare query parameters for GET requests
        
#         Args:
#             endpoint: Endpoint definition
#             params: All parameters
            
#         Returns:
#             Dict of query parameters
#         """
#         query_params = {}
        
#         # Get expected params from endpoint definition
#         expected_params = endpoint.get('params', [])
#         optional_params = endpoint.get('optional_params', [])
        
#         # Add all expected and optional params that are not path params
#         for param_name in expected_params + optional_params:
#             # Skip path parameters (they have {} in endpoint path)
#             if f'{{{param_name}}}' not in endpoint.get('path', ''):
#                 if param_name in params and params[param_name] is not None:
#                     query_params[param_name] = params[param_name]
        
#         return query_params
    
#     def _prepare_body(
#         self,
#         endpoint: Dict[str, Any],
#         params: Dict[str, Any]
#     ) -> Optional[Dict[str, Any]]:
#         """
#         Prepare request body for POST requests
        
#         Args:
#             endpoint: Endpoint definition
#             params: All parameters
            
#         Returns:
#             Dict for request body or None
#         """
#         if endpoint.get('method', 'GET').upper() != 'POST':
#             return None
        
#         body_params = endpoint.get('body_params', [])
        
#         if not body_params:
#             return None
        
#         body = {}
#         for param_name in body_params:
#             if param_name in params and params[param_name] is not None:
#                 body[param_name] = params[param_name]
        
#         return body if body else None


# # ============================================================================
# # AGENT 4: API EXECUTION AGENT
# # ============================================================================

# def agent_4_api_execution(state: Dict[str, Any]) -> Dict[str, Any]:
#     """
#     Agent 4: Execute selected endpoints and collect responses
#     Saves all responses to MongoDB via the triggered_endpoints list
    
#     Args:
#         state: Current chat state
        
#     Returns:
#         Updated state with endpoint_responses
#     """
#     logger.info("=== AGENT 4: API Execution ===")
#     state["current_agent"] = "agent_4_api_execution"
    
#     selected_endpoints = state.get("selected_endpoints", [])
#     auth_token = state.get("auth_token")
    
#     if not selected_endpoints:
#         logger.warning("No endpoints selected for execution")
#         state["warnings"].append("No endpoints were selected to call")
#         return state
    
#     # Build parameters dict from state
#     params = build_params_from_state(state)
    
#     logger.info(f"Executing {len(selected_endpoints)} endpoints with params: {params}")
    
#     try:
#         # Initialize API client
#         client = APIClient(base_url=API_BASE_URL, auth_token=auth_token)
        
#         # Call all selected endpoints
#         responses = client.call_multiple_endpoints(selected_endpoints, params)
        
#         # Separate successful and failed responses
#         successful_responses = []
#         failed_responses = []
        
#         for response in responses:
#             if response.get('success'):
#                 successful_responses.append(response)
#             else:
#                 failed_responses.append(response)
#                 state["errors"].append(
#                     f"Endpoint {response['endpoint']} failed: {response.get('error', 'Unknown error')}"
#                 )
        
#         # Store all responses for MongoDB
#         state["triggered_endpoints"] = responses
        
#         # Store only successful response data for LLM analysis
#         state["endpoint_responses"] = successful_responses
        
#         logger.info(
#             f"API execution completed: {len(successful_responses)} successful, "
#             f"{len(failed_responses)} failed"
#         )
        
#         if not successful_responses:
#             state["warnings"].append("All API calls failed. Unable to retrieve data.")
        
#         return state
        
#     except Exception as e:
#         logger.error(f"Error in API execution: {e}")
#         state["errors"].append(f"API execution failed: {str(e)}")
#         return state


# def build_params_from_state(state: Dict[str, Any]) -> Dict[str, Any]:
#     """
#     Build parameters dict from state for API calls
    
#     Args:
#         state: Current chat state
        
#     Returns:
#         Parameters dict for API calls
#     """
#     module_type = state.get("module_type")
#     params = {}
    
#     # Common parameters
#     if state.get("start_date"):
#         params["start_date"] = state["start_date"]
#     if state.get("end_date"):
#         params["end_date"] = state["end_date"]
#     if state.get("period"):
#         params["period"] = state["period"]
    
#     # Module-specific parameters
#     if module_type == "google_ads":
#         if state.get("customer_id"):
#             params["customer_id"] = state["customer_id"]
#         if state.get("campaign_ids"):
#             params["campaign_ids"] = state["campaign_ids"]
#         if state.get("location"):
#             params["location"] = state["location"]
#         if state.get("device"):
#             params["device"] = state["device"]
    
#     elif module_type == "google_analytics":
#         if state.get("property_id"):
#             params["property_id"] = state["property_id"]
#         if state.get("dimension"):
#             params["dimension"] = state["dimension"]
#         if state.get("metric"):
#             params["metric"] = state["metric"]
    
#     elif module_type == "intent_insights":
#         if state.get("account_id"):
#             params["account_id"] = state["account_id"]
#         if state.get("seed_keywords"):
#             params["seed_keywords"] = state["seed_keywords"]
#         if state.get("country"):
#             params["country"] = state["country"]
#         if state.get("timeframe"):
#             params["timeframe"] = state["timeframe"]
#         params["include_zero_volume"] = state.get("include_zero_volume", True)
    
#     elif module_type == "meta_ads":
#         if state.get("account_id"):
#             params["account_id"] = state["account_id"]
#         if state.get("campaign_ids"):
#             params["campaign_ids"] = state["campaign_ids"]
#         if state.get("adset_ids"):
#             params["adset_ids"] = state["adset_ids"]
#         if state.get("ad_ids"):
#             params["ad_ids"] = state["ad_ids"]
#         if state.get("status_filter"):
#             params["status"] = state["status_filter"]
    
#     elif module_type == "facebook":
#         if state.get("page_id"):
#             params["page_id"] = state["page_id"]
#         if state.get("post_limit"):
#             params["limit"] = state["post_limit"]
    
#     elif module_type == "instagram":
#         if state.get("account_id"):
#             params["account_id"] = state["account_id"]
#         if state.get("media_limit"):
#             params["limit"] = state["media_limit"]
#         if state.get("media_type"):
#             params["media_type"] = state["media_type"]
    
#     return params


# # ============================================================================
# # MONGODB STORAGE FUNCTIONS
# # ============================================================================

# def save_conversation_to_mongodb(
#     session_id: str,
#     user_email: str,
#     module_type: str,
#     state: Dict[str, Any],
#     mongo_manager
# ) -> bool:
#     """
#     Save complete conversation state to MongoDB
    
#     Args:
#         session_id: Unique session identifier
#         user_email: User's email
#         module_type: Module type
#         state: Final state with all data
#         mongo_manager: MongoManager instance
        
#     Returns:
#         True if saved successfully
#     """
#     try:
#         from datetime import datetime
        
#         # Prepare conversation document
#         conversation_doc = {
#             "session_id": session_id,
#             "user_email": user_email,
#             "module_type": module_type,
#             "timestamp": datetime.utcnow(),
            
#             # User interaction
#             "user_question": state.get("user_question"),
#             "formatted_response": state.get("formatted_response"),
#             "visualizations": state.get("visualizations"),
            
#             # Parameters used
#             "parameters": {
#                 "start_date": state.get("start_date"),
#                 "end_date": state.get("end_date"),
#                 "period": state.get("period"),
#                 "customer_id": state.get("customer_id"),
#                 "property_id": state.get("property_id"),
#                 "account_id": state.get("account_id"),
#                 "page_id": state.get("page_id"),
#             },
            
#             # All triggered endpoints with full details
#             "triggered_endpoints": state.get("triggered_endpoints", []),
            
#             # Processing metadata
#             "processing_metadata": {
#                 "intent_type": state.get("intent_type"),
#                 "selected_endpoints": [ep.get('name') for ep in state.get("selected_endpoints", [])],
#                 "processing_start_time": state.get("processing_start_time"),
#                 "processing_end_time": state.get("processing_end_time"),
#                 "total_processing_time": calculate_processing_time(state),
#                 "errors": state.get("errors", []),
#                 "warnings": state.get("warnings", [])
#             },
            
#             # Conversation context
#             "is_active": True,
#             "last_activity": datetime.utcnow()
#         }
        
#         # Save to MongoDB using the chat collection
#         collection_name = f"chat_{module_type}"
#         result = mongo_manager.db[collection_name].insert_one(conversation_doc)
        
#         logger.info(f"Saved conversation to MongoDB: {result.inserted_id}")
#         return True
        
#     except Exception as e:
#         logger.error(f"Error saving conversation to MongoDB: {e}")
#         return False


# def calculate_processing_time(state: Dict[str, Any]) -> Optional[float]:
#     """
#     Calculate total processing time from state
    
#     Args:
#         state: Chat state
        
#     Returns:
#         Processing time in seconds or None
#     """
#     start = state.get("processing_start_time")
#     end = state.get("processing_end_time")
    
#     if start and end:
#         if isinstance(start, datetime) and isinstance(end, datetime):
#             return (end - start).total_seconds()
    
#     return None


# # ============================================================================
# # SPECIAL HANDLERS FOR SPECIFIC MODULES
# # ============================================================================

# def handle_meta_campaigns_loading(
#     account_id: str,
#     auth_token: str,
#     status_filter: Optional[List[str]] = None
# ) -> Dict[str, Any]:
#     """
#     Special handler for Meta campaigns list endpoint
#     This endpoint can take several minutes due to pagination
    
#     Args:
#         account_id: Meta ad account ID
#         auth_token: Meta auth token
#         status_filter: Optional status filter
        
#     Returns:
#         Response with all campaigns
#     """
#     logger.info("=== LOADING ALL META CAMPAIGNS (This may take several minutes) ===")
    
#     endpoint = {
#         'name': 'get_meta_campaigns_list',
#         'path': f'/api/meta/ad-accounts/{account_id}/campaigns/list',
#         'method': 'GET',
#         'params': ['account_id']
#     }
    
#     params = {
#         'account_id': account_id
#     }
    
#     if status_filter:
#         params['status'] = ','.join(status_filter)
    
#     client = APIClient(base_url=API_BASE_URL, auth_token=auth_token)
    
#     # This call may take 3-5 minutes
#     response = client.call_endpoint(endpoint, params, retry=True)
    
#     if response.get('success'):
#         campaigns = response.get('data', {}).get('campaigns', [])
#         logger.info(f"Loaded {len(campaigns)} campaigns successfully")
#     else:
#         logger.error(f"Failed to load campaigns: {response.get('error')}")
    
#     return response


# def handle_meta_adsets_loading(
#     campaign_ids: List[str],
#     auth_token: str
# ) -> Dict[str, Any]:
#     """
#     Special handler for Meta adsets by campaigns endpoint
    
#     Args:
#         campaign_ids: List of campaign IDs
#         auth_token: Meta auth token
        
#     Returns:
#         Response with all adsets
#     """
#     logger.info(f"=== LOADING ADSETS FOR {len(campaign_ids)} CAMPAIGNS ===")
    
#     endpoint = {
#         'name': 'get_adsets_by_campaigns',
#         'path': '/api/meta/campaigns/adsets',
#         'method': 'POST',
#         'body_params': ['campaign_ids']
#     }
    
#     params = {
#         'campaign_ids': campaign_ids
#     }
    
#     client = APIClient(base_url=API_BASE_URL, auth_token=auth_token)
#     response = client.call_endpoint(endpoint, params)
    
#     if response.get('success'):
#         adsets = response.get('data', [])
#         logger.info(f"Loaded {len(adsets)} adsets successfully")
#     else:
#         logger.error(f"Failed to load adsets: {response.get('error')}")
    
#     return response


# def handle_meta_ads_loading(
#     adset_ids: List[str],
#     auth_token: str
# ) -> Dict[str, Any]:
#     """
#     Special handler for Meta ads by adsets endpoint
    
#     Args:
#         adset_ids: List of adset IDs
#         auth_token: Meta auth token
        
#     Returns:
#         Response with all ads
#     """
#     logger.info(f"=== LOADING ADS FOR {len(adset_ids)} ADSETS ===")
    
#     endpoint = {
#         'name': 'get_ads_by_adsets',
#         'path': '/api/meta/adsets/ads',
#         'method': 'POST',
#         'body_params': ['adset_ids']
#     }
    
#     params = {
#         'adset_ids': adset_ids
#     }
    
#     client = APIClient(base_url=API_BASE_URL, auth_token=auth_token)
#     response = client.call_endpoint(endpoint, params)
    
#     if response.get('success'):
#         ads = response.get('data', [])
#         logger.info(f"Loaded {len(ads)} ads successfully")
#     else:
#         logger.error(f"Failed to load ads: {response.get('error')}")
    
#     return response


# # ============================================================================
# # UTILITY FUNCTIONS
# # ============================================================================

# def validate_response_data(response: Dict[str, Any]) -> bool:
#     """
#     Validate that response contains usable data
    
#     Args:
#         response: API response
        
#     Returns:
#         True if response has valid data
#     """
#     if not response.get('success'):
#         return False
    
#     data = response.get('data')
    
#     if data is None:
#         return False
    
#     # Check if data is empty
#     if isinstance(data, (list, dict)):
#         return len(data) > 0
    
#     return True


# def format_response_for_logging(response: Dict[str, Any], max_length: int = 500) -> str:
#     """
#     Format response for logging (truncate large data)
    
#     Args:
#         response: API response
#         max_length: Maximum length for logging
        
#     Returns:
#         Formatted string for logging
#     """
#     log_data = {
#         "endpoint": response.get("endpoint"),
#         "success": response.get("success"),
#         "status_code": response.get("status_code"),
#         "response_time": f"{response.get('response_time', 0):.2f}s"
#     }
    
#     if not response.get('success'):
#         log_data["error"] = response.get("error")
#     else:
#         data = response.get("data")
#         if isinstance(data, list):
#             log_data["data_count"] = len(data)
#         elif isinstance(data, dict):
#             log_data["data_keys"] = list(data.keys())
    
#     result = json.dumps(log_data, indent=2)
    
#     if len(result) > max_length:
#         return result[:max_length] + "\n... (truncated)"
    
#     return result