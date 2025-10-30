"""
Updated Chat Manager using LangGraph
Integrates LangGraph orchestrator with FastAPI endpoints
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import uuid

from chat.graphs.graph_orchestrator import get_orchestrator, process_message, continue_message
from chat.states.chat_states import ModuleType
from models.chat_models import (
    ChatRequest,
    ChatResponse,
    ChatSession,
    ChatMessage,
    MessageRole
)

# Initialize logger
logger = logging.getLogger(__name__)


# ============================================================================
# CHAT MANAGER CLASS
# ============================================================================

class ChatManager:
    """
    Manager for handling chat operations with LangGraph integration
    """
    
    def __init__(self, mongo_manager=None):
        """
        Initialize chat manager
        
        Args:
            mongo_manager: MongoManager instance for database operations
        """
        self.mongo_manager = mongo_manager
        self.orchestrator = get_orchestrator(mongo_manager)
        logger.info("ChatManager initialized with LangGraph")
    
    async def process_chat_request(
        self,
        request: ChatRequest,
        current_user: Dict[str, Any]
    ) -> ChatResponse:
        """
        Process a chat request
        
        Args:
            request: ChatRequest object
            current_user: Current authenticated user with auth_token from frontend
            
        Returns:
            ChatResponse object
        """
        logger.info(f"Processing chat request for module: {request.module_type}")
        
        try:
            # Generate session ID if not provided
            session_id = request.session_id or str(uuid.uuid4())
            user_email = current_user.get("email", "unknown")
            
            # Get auth token from current_user (passed from main.py via Authorization header)
            auth_token = current_user.get('auth_token', '')
            
            if not auth_token:
                logger.warning(f"⚠️ No auth token provided for {request.module_type}")
            
            # Prepare context
            context = self._prepare_context(request, current_user)
            
            # Process message using orchestrator
            final_state = await process_message(
                user_question=request.message,
                module_type=request.module_type,
                session_id=session_id,
                user_email=user_email,
                auth_token=auth_token,
                context=context,
                mongo_manager=self.mongo_manager
            )
            
            # Convert state to ChatResponse
            response = self._state_to_response(final_state, session_id, request.module_type)
            
            logger.info(f"Chat request processed successfully. Session: {session_id}")
            
            return response
            
        except Exception as e:
            logger.error(f"Error processing chat request: {e}", exc_info=True)
            
            # Return error response
            return ChatResponse(
                response=f"I apologize, but I encountered an error: {str(e)}",
                session_id=session_id if 'session_id' in locals() else str(uuid.uuid4()),
                module_type=request.module_type,
                timestamp=datetime.utcnow()
            )
    
    async def continue_chat_session(
        self,
        session_id: str,
        user_response: str,
        module_type: str,
        current_user: Dict[str, Any]
    ) -> ChatResponse:
        """
        Continue an existing chat session after user provides input
        
        Args:
            session_id: Session identifier
            user_response: User's response
            module_type: Module type
            current_user: Current authenticated user with auth_token
            
        Returns:
            ChatResponse object
        """
        logger.info(f"Continuing chat session: {session_id}")
        
        try:
            # Get previous state from MongoDB
            previous_state = self._get_session_state(session_id, module_type)
            
            if not previous_state:
                raise ValueError(f"Session {session_id} not found")
            
            # Get auth token from current_user
            auth_token = current_user.get('auth_token', '')
            previous_state["auth_token"] = auth_token
            
            # Continue conversation
            final_state = await continue_message(
                previous_state=previous_state,
                user_response=user_response,
                module_type=module_type,
                mongo_manager=self.mongo_manager
            )
            
            # Convert to response
            response = self._state_to_response(final_state, session_id, module_type)
            
            logger.info(f"Chat session continued successfully")
            
            return response
            
        except Exception as e:
            logger.error(f"Error continuing chat session: {e}", exc_info=True)
            
            return ChatResponse(
                response=f"Error continuing conversation: {str(e)}",
                session_id=session_id,
                module_type=module_type,
                timestamp=datetime.utcnow()
            )
        
    async def get_chat_history(
        self,
        module_type: str,
        user_email: str,
        limit: int = 50
    ) -> List[ChatSession]:
        """Get chat history for a user in a specific module"""
        logger.info(f"📚 Fetching chat history for {user_email} in {module_type}")

        try:
            if not self.mongo_manager:
                logger.warning("⚠️ MongoDB manager is None")
                return []

            collection_name = self.mongo_manager._get_chat_collection_name(module_type)
            logger.info(f"🔍 Querying collection: {collection_name}")
            logger.info(f"   Module type: {module_type}")

            collection = self.mongo_manager.db[collection_name]

            # First, check if collection exists and has any documents
            total_count = await collection.count_documents({})
            logger.info(f"📊 Total documents in {collection_name}: {total_count}")

            # Check documents for this user
            user_count = await collection.count_documents({"user_email": user_email})
            logger.info(f"📊 Documents for user {user_email}: {user_count}")

            # Check active documents for this user
            active_count = await collection.count_documents({
                "user_email": user_email,
                "is_active": True
            })
            logger.info(f"📊 Active documents for user {user_email}: {active_count}")

            # Query sessions with updated fields matching new schema
            query_filter = {
                "user_email": user_email,
                "is_active": True
            }
            logger.info(f"🔍 Query filter: {query_filter}")

            sessions = await collection.find(query_filter).sort("last_activity", -1).limit(limit).to_list(length=limit)

            logger.info(f"✅ Found {len(sessions)} sessions from MongoDB")

            chat_sessions = []
            for session_doc in sessions:
                logger.info(f"📝 Processing session: {session_doc.get('session_id')}")
                messages = []
                for msg in session_doc.get("messages", []):
                    messages.append(ChatMessage(
                        role=MessageRole(msg["role"]),
                        content=msg["content"],
                        timestamp=msg["timestamp"]
                    ))

                chat_session = ChatSession(
                    session_id=session_doc["session_id"],
                    user_email=session_doc["user_email"],
                    module_type=session_doc["module_type"],
                    customer_id=session_doc.get("customer_id"),
                    property_id=session_doc.get("property_id"),
                    messages=messages,
                    created_at=session_doc["created_at"],
                    last_activity=session_doc.get("last_activity", session_doc["created_at"]),
                    is_active=session_doc.get("is_active", True)
                )
                chat_sessions.append(chat_session)
                logger.info(f"✅ Converted session {session_doc.get('session_id')} with {len(messages)} messages")

            logger.info(f"✅ Returning {len(chat_sessions)} chat sessions")
            return chat_sessions

        except Exception as e:
            logger.error(f"❌ Error fetching chat history: {e}", exc_info=True)
            return []
        
    async def delete_chat_sessions(
        self,
        session_ids: List[str],
        module_type: str,
        user_email: str
    ) -> bool:
        """
        Delete chat sessions

        Args:
            session_ids: List of session IDs to delete
            module_type: Module type
            user_email: User's email

        Returns:
            True if successful
        """
        logger.info(f"Deleting {len(session_ids)} chat sessions")

        try:
            if not self.mongo_manager:
                return False

            collection_name = self.mongo_manager._get_chat_collection_name(module_type)
            collection = self.mongo_manager.db[collection_name]

            # Delete sessions
            result = await collection.delete_many({
                "session_id": {"$in": session_ids},
                "user_email": user_email
            })

            logger.info(f"Deleted {result.deleted_count} sessions")

            return True

        except Exception as e:
            logger.error(f"Error deleting chat sessions: {e}")
            return False
    
    # ========================================================================
    # HELPER METHODS
    # ========================================================================
        
    def _prepare_context(
        self,
        request: ChatRequest,
        current_user: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Prepare context dictionary from request"""
        context = request.context or {}
        
        # Add IDs based on module
        if request.module_type == ModuleType.GOOGLE_ADS.value:
            context["customer_id"] = request.customer_id or context.get("customer_id")
        
        elif request.module_type == ModuleType.GOOGLE_ANALYTICS.value:
            context["property_id"] = request.property_id or context.get("property_id")
        
        elif request.module_type == ModuleType.INTENT_INSIGHTS.value:
            context["account_id"] = request.customer_id or context.get("account_id")
        
        elif request.module_type == ModuleType.META_ADS.value:
            context["account_id"] = request.account_id or context.get("account_id") or context.get("ad_account_id")
        
        elif request.module_type == "facebook_analytics":
            context["page_id"] = request.page_id or context.get("page_id")
        
        elif request.module_type == "instagram_analytics":
            context["account_id"] = request.account_id or context.get("account_id") or context.get("instagram_account_id")
        
        # Add time period with proper handling
        if request.period:
            context["period"] = request.period
        
        # Add custom dates if CUSTOM period
        if request.period == "CUSTOM":
            if hasattr(request, 'start_date') and request.start_date:
                context["start_date"] = request.start_date
            if hasattr(request, 'end_date') and request.end_date:
                context["end_date"] = request.end_date
        
        logger.info(f"📦 Prepared context: {context}")
        
        return context
    
    def _state_to_response(
        self,
        state: Dict[str, Any],
        session_id: str,
        module_type: str
    ) -> ChatResponse:
        """
        Convert final state to ChatResponse

        Args:
            state: Final state from graph
            session_id: Session ID
            module_type: Module type

        Returns:
            ChatResponse object
        """
        logger.info(f"🔍 Converting state to response")
        logger.info(f"🔍 needs_user_input: {state.get('needs_user_input')}")
        logger.info(f"🔍 needs_campaign_selection: {state.get('needs_campaign_selection')}")
        logger.info(f"🔍 is_complete: {state.get('is_complete')}")

        campaign_opts = state.get('campaign_selection_options')
        if campaign_opts is not None:
            logger.info(f"✅ campaign_selection_options has {len(campaign_opts)} options")
        else:
            logger.warning(f"⚠️ campaign_selection_options is None")

        # Extract response text
        response_text = state.get("formatted_response", "No response generated")
        
        # Extract triggered endpoints
        triggered_endpoints = state.get("triggered_endpoints", [])
        endpoint_names = [ep.get("endpoint") for ep in triggered_endpoints if ep.get("success")]
        
        # Build endpoint_data for response
        endpoint_data = {
            "triggered_endpoints": endpoint_names,
            "total_endpoints": len(triggered_endpoints),
            "successful_endpoints": sum(1 for ep in triggered_endpoints if ep.get("success")),
            "processing_time": state.get("processing_metadata", {}).get("total_processing_time"),
            "visualizations": state.get("visualizations")
        }
        
        # Handle cases where user input is needed
        # Check both needs_user_input AND specific selection flags for Meta Ads
        needs_selection = (
            state.get("needs_user_input") or
            state.get("needs_campaign_selection") or
            state.get("needs_adset_selection") or
            state.get("needs_ad_selection")
        )

        if needs_selection:
            logger.info(f"🔍 User input/selection needed")
            # For Meta Ads dropdown selections
            campaign_opts = state.get("campaign_selection_options")
            if campaign_opts and len(campaign_opts) > 0:
                logger.info(f"✅ Setting requires_selection for campaigns with {len(campaign_opts)} options")
                endpoint_data["requires_selection"] = {
                    "type": "campaigns",
                    "options": campaign_opts,
                    "prompt": state.get("user_clarification_prompt", "Please select campaigns")
                }
                logger.info(f"📤 requires_selection set: {endpoint_data['requires_selection']['type']}")
            elif state.get("adset_selection_options") and len(state.get("adset_selection_options", [])) > 0:
                adset_opts = state["adset_selection_options"]
                logger.info(f"✅ Setting requires_selection for adsets with {len(adset_opts)} options")
                endpoint_data["requires_selection"] = {
                    "type": "adsets",
                    "options": adset_opts,
                    "prompt": state.get("user_clarification_prompt", "Please select adsets")
                }
            elif state.get("ad_selection_options") and len(state.get("ad_selection_options", [])) > 0:
                ad_opts = state["ad_selection_options"]
                logger.info(f"✅ Setting requires_selection for ads with {len(ad_opts)} options")
                endpoint_data["requires_selection"] = {
                    "type": "ads",
                    "options": state["ad_selection_options"],
                    "prompt": state.get("user_clarification_prompt")
                }
            else:
                # General clarification needed
                logger.info(f"⚠️ needs_user_input=True but no selection_options found")
                response_text = state.get("user_clarification_prompt", response_text)
        
        return ChatResponse(
            response=response_text,
            session_id=session_id,
            triggered_endpoint=endpoint_names[0] if endpoint_names else None,
            endpoint_data=endpoint_data,
            timestamp=datetime.utcnow(),
            module_type=module_type
        )
    
    async def _get_session_state(
        self,
        session_id: str,
        module_type: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get previous state for a session from MongoDB

        Args:
            session_id: Session ID
            module_type: Module type

        Returns:
            Previous state or None
        """
        try:
            if not self.mongo_manager:
                return None

            collection_name = self.mongo_manager._get_chat_collection_name(module_type)
            collection = self.mongo_manager.db[collection_name]

            # Find latest session document
            session_doc = await collection.find_one(
                {"session_id": session_id},
                sort=[("last_activity", -1)]
            )

            if not session_doc:
                return None

            # Reconstruct state from session document
            state = {
                "session_id": session_id,
                "user_email": session_doc.get("user_email"),
                "module_type": module_type,
                "parameters": session_doc.get("parameters", {})
            }

            return state

        except Exception as e:
            logger.error(f"Error getting session state: {e}")
            return None


# ============================================================================
# SINGLETON INSTANCE
# ============================================================================

_chat_manager_instance = None


def get_chat_manager(mongo_manager=None) -> ChatManager:
    """
    Get singleton instance of ChatManager

    Args:
        mongo_manager: MongoManager instance

    Returns:
        ChatManager instance
    """
    global _chat_manager_instance

    if _chat_manager_instance is None:
        _chat_manager_instance = ChatManager(mongo_manager)
    elif mongo_manager and _chat_manager_instance.mongo_manager is None:
        # Update with mongo_manager if it was None before
        _chat_manager_instance.mongo_manager = mongo_manager
        _chat_manager_instance.orchestrator.mongo_manager = mongo_manager

    return _chat_manager_instance


# Create default instance (will be updated with mongo_manager from main.py)
chat_manager = get_chat_manager()