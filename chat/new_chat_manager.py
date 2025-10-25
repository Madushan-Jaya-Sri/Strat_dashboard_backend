# """
# Updated Chat Manager using LangGraph
# Integrates LangGraph orchestrator with FastAPI endpoints
# """

# import logging
# from typing import Dict, Any, Optional, List
# from datetime import datetime
# import uuid
# from fastapi import HTTPException

# from chat.graphs.graph_orchestrator import get_orchestrator, process_message, continue_message
# from chat.states.chat_states import ModuleType
# from models.chat_models import (
#     ChatRequest,
#     ChatResponse,
#     ChatSession,
#     ChatMessage,
#     MessageRole
# )

# # Initialize logger
# logger = logging.getLogger(__name__)


# # ============================================================================
# # CHAT MANAGER CLASS
# # ============================================================================

# class ChatManager:
#     """
#     Manager for handling chat operations with LangGraph integration
#     """
    
#     def __init__(self, mongo_manager=None):
#         """
#         Initialize chat manager
        
#         Args:
#             mongo_manager: MongoManager instance for database operations
#         """
#         self.mongo_manager = mongo_manager
#         self.orchestrator = get_orchestrator(mongo_manager)
#         logger.info("ChatManager initialized with LangGraph")
    
#     async def process_chat_request(
#         self,
#         request: ChatRequest,
#         current_user: Dict[str, Any]
#     ) -> ChatResponse:
#         """
#         Process a chat request
        
#         Args:
#             request: ChatRequest object
#             current_user: Current authenticated user
            
#         Returns:
#             ChatResponse object
#         """
#         logger.info(f"Processing chat request for module: {request.module_type}")
        
#         try:
#             # Generate session ID if not provided
#             session_id = request.session_id or str(uuid.uuid4())
#             user_email = current_user.get("email", "unknown")
            
#             # Get auth token based on module
#             auth_token = self._get_auth_token(request.module_type, current_user)
            
#             # Prepare context
#             context = self._prepare_context(request, current_user)
            
#             # Process message using orchestrator
#             final_state = await process_message(
#                 user_question=request.message,
#                 module_type=request.module_type,
#                 session_id=session_id,
#                 user_email=user_email,
#                 auth_token=auth_token,
#                 context=context,
#                 mongo_manager=self.mongo_manager
#             )
            
#             # Convert state to ChatResponse
#             response = self._state_to_response(final_state, session_id, request.module_type)
            
#             logger.info(f"Chat request processed successfully. Session: {session_id}")
            
#             return response
            
#         except Exception as e:
#             logger.error(f"Error processing chat request: {e}", exc_info=True)
            
#             # Return error response
#             return ChatResponse(
#                 response=f"I apologize, but I encountered an error: {str(e)}",
#                 session_id=session_id if 'session_id' in locals() else str(uuid.uuid4()),
#                 module_type=request.module_type,
#                 timestamp=datetime.utcnow()
#             )
    
#     async def continue_chat_session(
#         self,
#         session_id: str,
#         user_response: str,
#         module_type: str,
#         current_user: Dict[str, Any]
#     ) -> ChatResponse:
#         """
#         Continue an existing chat session after user provides input
        
#         Args:
#             session_id: Session identifier
#             user_response: User's response
#             module_type: Module type
#             current_user: Current authenticated user
            
#         Returns:
#             ChatResponse object
#         """
#         logger.info(f"Continuing chat session: {session_id}")
        
#         try:
#             # Get previous state from MongoDB
#             previous_state = self._get_session_state(session_id, module_type)
            
#             if not previous_state:
#                 raise ValueError(f"Session {session_id} not found")
            
#             # Get auth token
#             auth_token = self._get_auth_token(module_type, current_user)
#             previous_state["auth_token"] = auth_token
            
#             # Continue conversation
#             final_state = await continue_message(
#                 previous_state=previous_state,
#                 user_response=user_response,
#                 module_type=module_type,
#                 mongo_manager=self.mongo_manager
#             )
            
#             # Convert to response
#             response = self._state_to_response(final_state, session_id, module_type)
            
#             logger.info(f"Chat session continued successfully")
            
#             return response
            
#         except Exception as e:
#             logger.error(f"Error continuing chat session: {e}", exc_info=True)
            
#             return ChatResponse(
#                 response=f"Error continuing conversation: {str(e)}",
#                 session_id=session_id,
#                 module_type=module_type,
#                 timestamp=datetime.utcnow()
#             )
    
#     def get_chat_history(
#         self,
#         module_type: str,
#         user_email: str,
#         limit: int = 50
#     ) -> List[ChatSession]:
#         """
#         Get chat history for a user in a specific module
        
#         Args:
#             module_type: Module type
#             user_email: User's email
#             limit: Maximum number of sessions to return
            
#         Returns:
#             List of ChatSession objects
#         """
#         logger.info(f"Fetching chat history for {user_email} in {module_type}")
        
#         try:
#             if not self.mongo_manager:
#                 return []
            
#             collection_name = f"chat_{module_type}"
#             collection = self.mongo_manager.db[collection_name]
            
#             # Query sessions
#             sessions = collection.find(
#                 {"user_email": user_email},
#                 sort=[("timestamp", -1)],
#                 limit=limit
#             )
            
#             chat_sessions = []
#             for session_doc in sessions:
#                 chat_session = ChatSession(
#                     session_id=session_doc["session_id"],
#                     user_email=session_doc["user_email"],
#                     module_type=session_doc["module_type"],
#                     messages=[
#                         ChatMessage(
#                             role=MessageRole.USER,
#                             content=session_doc["user_question"],
#                             timestamp=session_doc["timestamp"]
#                         ),
#                         ChatMessage(
#                             role=MessageRole.ASSISTANT,
#                             content=session_doc["formatted_response"],
#                             timestamp=session_doc["timestamp"]
#                         )
#                     ],
#                     created_at=session_doc["timestamp"],
#                     last_activity=session_doc.get("last_activity", session_doc["timestamp"]),
#                     is_active=session_doc.get("is_active", True)
#                 )
#                 chat_sessions.append(chat_session)
            
#             logger.info(f"Found {len(chat_sessions)} chat sessions")
            
#             return chat_sessions
            
#         except Exception as e:
#             logger.error(f"Error fetching chat history: {e}")
#             return []
    
#     def delete_chat_sessions(
#         self,
#         session_ids: List[str],
#         module_type: str,
#         user_email: str
#     ) -> bool:
#         """
#         Delete chat sessions
        
#         Args:
#             session_ids: List of session IDs to delete
#             module_type: Module type
#             user_email: User's email
            
#         Returns:
#             True if successful
#         """
#         logger.info(f"Deleting {len(session_ids)} chat sessions")
        
#         try:
#             if not self.mongo_manager:
#                 return False
            
#             collection_name = f"chat_{module_type}"
#             collection = self.mongo_manager.db[collection_name]
            
#             # Delete sessions
#             result = collection.delete_many({
#                 "session_id": {"$in": session_ids},
#                 "user_email": user_email
#             })
            
#             logger.info(f"Deleted {result.deleted_count} sessions")
            
#             return True
            
#         except Exception as e:
#             logger.error(f"Error deleting chat sessions: {e}")
#             return False
    
#     # ========================================================================
#     # HELPER METHODS
#     # ========================================================================
        
#     def _get_auth_token(self, module_type: str, current_user: Dict[str, Any]) -> str:
#         """Get appropriate auth token based on module type"""
#         user_email = current_user.get("email", "")
        
#         # Meta modules use Facebook token
#         if module_type in [ModuleType.META_ADS.value, ModuleType.FACEBOOK.value, ModuleType.INSTAGRAM.value]:
#             try:
#                 # Try to get Facebook token from auth manager
#                 from auth.auth_manager import get_auth_manager
#                 auth_manager = get_auth_manager()
#                 fb_token = auth_manager.get_facebook_access_token(user_email)
#                 logger.info(f"✅ Retrieved Facebook token for {user_email}")
#                 return fb_token
#             except Exception as e:
#                 logger.error(f"❌ Failed to get Facebook token: {e}")
#                 raise HTTPException(
#                     status_code=401,
#                     detail="Facebook authentication required. Please reconnect your Facebook account."
#                 )
        
#         # Google modules use Google credentials token
#         else:
#             try:
#                 from auth.auth_manager import get_auth_manager
#                 auth_manager = get_auth_manager()
#                 creds = auth_manager.get_user_credentials(user_email)
#                 return creds.token
#             except Exception as e:
#                 logger.error(f"❌ Failed to get Google token: {e}")
#                 raise HTTPException(
#                     status_code=401,
#                     detail="Google authentication required. Please reconnect your Google account."
#                 ) 
#     def _prepare_context(
#         self,
#         request: ChatRequest,
#         current_user: Dict[str, Any]
#     ) -> Dict[str, Any]:
#         """
#         Prepare context dictionary from request
        
#         Args:
#             request: ChatRequest object
#             current_user: Current user
            
#         Returns:
#             Context dictionary
#         """
#         context = request.context or {}
        
#         # Add IDs based on module
#         if request.module_type == ModuleType.GOOGLE_ADS.value:
#             context["customer_id"] = request.customer_id or context.get("customer_id")
        
#         elif request.module_type == ModuleType.GOOGLE_ANALYTICS.value:
#             context["property_id"] = request.property_id or context.get("property_id")
        
#         elif request.module_type == ModuleType.INTENT_INSIGHTS.value:
#             context["account_id"] = request.customer_id or context.get("account_id")
        
#         elif request.module_type == ModuleType.META_ADS.value:
#             context["account_id"] = context.get("account_id") or context.get("ad_account_id")
        
#         elif request.module_type == ModuleType.FACEBOOK.value:
#             context["page_id"] = context.get("page_id")
        
#         elif request.module_type == ModuleType.INSTAGRAM.value:
#             context["account_id"] = context.get("account_id") or context.get("instagram_account_id")
        
#         # Add time period
#         if request.period:
#             context["period"] = request.period
        
#         return context
    
#     def _state_to_response(
#         self,
#         state: Dict[str, Any],
#         session_id: str,
#         module_type: str
#     ) -> ChatResponse:
#         """
#         Convert final state to ChatResponse
        
#         Args:
#             state: Final state from graph
#             session_id: Session ID
#             module_type: Module type
            
#         Returns:
#             ChatResponse object
#         """
#         # Extract response text
#         response_text = state.get("formatted_response", "No response generated")
        
#         # Extract triggered endpoints
#         triggered_endpoints = state.get("triggered_endpoints", [])
#         endpoint_names = [ep.get("endpoint") for ep in triggered_endpoints if ep.get("success")]
        
#         # Build endpoint_data for response
#         endpoint_data = {
#             "triggered_endpoints": endpoint_names,
#             "total_endpoints": len(triggered_endpoints),
#             "successful_endpoints": sum(1 for ep in triggered_endpoints if ep.get("success")),
#             "processing_time": state.get("processing_metadata", {}).get("total_processing_time"),
#             "visualizations": state.get("visualizations")
#         }
        
#         # Handle cases where user input is needed
#         if state.get("needs_user_input"):
#             # For Meta Ads dropdown selections
#             if state.get("campaign_selection_options"):
#                 endpoint_data["requires_selection"] = {
#                     "type": "campaigns",
#                     "options": state["campaign_selection_options"],
#                     "prompt": state.get("user_clarification_prompt")
#                 }
#             elif state.get("adset_selection_options"):
#                 endpoint_data["requires_selection"] = {
#                     "type": "adsets",
#                     "options": state["adset_selection_options"],
#                     "prompt": state.get("user_clarification_prompt")
#                 }
#             elif state.get("ad_selection_options"):
#                 endpoint_data["requires_selection"] = {
#                     "type": "ads",
#                     "options": state["ad_selection_options"],
#                     "prompt": state.get("user_clarification_prompt")
#                 }
#             else:
#                 # General clarification needed
#                 response_text = state.get("user_clarification_prompt", response_text)
        
#         return ChatResponse(
#             response=response_text,
#             session_id=session_id,
#             triggered_endpoint=endpoint_names[0] if endpoint_names else None,
#             endpoint_data=endpoint_data,
#             timestamp=datetime.utcnow(),
#             module_type=module_type
#         )
    
#     def _get_session_state(
#         self,
#         session_id: str,
#         module_type: str
#     ) -> Optional[Dict[str, Any]]:
#         """
#         Get previous state for a session from MongoDB
        
#         Args:
#             session_id: Session ID
#             module_type: Module type
            
#         Returns:
#             Previous state or None
#         """
#         try:
#             if not self.mongo_manager:
#                 return None
            
#             collection_name = f"chat_{module_type}"
#             collection = self.mongo_manager.db[collection_name]
            
#             # Find latest session document
#             session_doc = collection.find_one(
#                 {"session_id": session_id},
#                 sort=[("timestamp", -1)]
#             )
            
#             if not session_doc:
#                 return None
            
#             # Reconstruct state from session document
#             # This is simplified - in production you'd want to store and retrieve full state
#             state = {
#                 "session_id": session_id,
#                 "user_email": session_doc.get("user_email"),
#                 "module_type": module_type,
#                 "parameters": session_doc.get("parameters", {})
#             }
            
#             return state
            
#         except Exception as e:
#             logger.error(f"Error getting session state: {e}")
#             return None


# # ============================================================================
# # SINGLETON INSTANCE
# # ============================================================================

# _chat_manager_instance = None


# def get_chat_manager(mongo_manager=None) -> ChatManager:
#     """
#     Get singleton instance of ChatManager
    
#     Args:
#         mongo_manager: MongoManager instance
        
#     Returns:
#         ChatManager instance
#     """
#     global _chat_manager_instance
    
#     if _chat_manager_instance is None:
#         _chat_manager_instance = ChatManager(mongo_manager)
    
#     return _chat_manager_instance


# # Create default instance
# chat_manager = get_chat_manager()