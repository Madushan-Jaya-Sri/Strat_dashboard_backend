"""
Graph Orchestrator - Main entry point for LangGraph chat system
Routes chat requests to appropriate module-specific graphs
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

from chat.graphs.google_ads_graph import run_google_ads_chat
from chat.graphs.ga4_graph import run_ga4_chat
from chat.graphs.intent_graph import run_intent_chat
from chat.graphs.meta_ads_graph import run_meta_ads_chat
from chat.graphs.facebook_instagram_graphs import run_facebook_chat, run_instagram_chat
from chat.states.chat_states import ModuleType

# Initialize logger
logger = logging.getLogger(__name__)


# ============================================================================
# MAIN ORCHESTRATOR CLASS
# ============================================================================

class GraphOrchestrator:
    """
    Main orchestrator for routing chat requests to appropriate module graphs
    """
    
    def __init__(self, mongo_manager=None):
        """
        Initialize orchestrator
        
        Args:
            mongo_manager: MongoManager instance for database operations
        """
        self.mongo_manager = mongo_manager
        logger.info("Graph Orchestrator initialized")
    
    async def process_chat_message(
        self,
        user_question: str,
        module_type: str,
        session_id: str,
        user_email: str,
        auth_token: str,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Process a chat message by routing to appropriate module graph
        
        Args:
            user_question: User's question
            module_type: Module type (google_ads, ga4, intent_insights, etc.)
            session_id: Session identifier
            user_email: User's email
            auth_token: Authentication token (Google or Meta depending on module)
            context: Additional context from frontend (includes IDs, period, etc.)
            
        Returns:
            Final state with response
        """
        logger.info(f"🚀 Processing chat message for module: {module_type}")
        logger.info(f"👤 User: {user_email}, Session: {session_id}")
        logger.info(f"📦 Context: {context}")
        
        try:
            # Validate module type
            if module_type not in [e.value for e in ModuleType]:
                raise ValueError(f"Invalid module type: {module_type}")
            
            # Prepare context with module-specific parameters
            prepared_context = self._prepare_module_context(module_type, context)
            
            # Route to appropriate graph
            if module_type == ModuleType.GOOGLE_ADS.value:
                final_state = await run_google_ads_chat(
                    user_question=user_question,
                    session_id=session_id,
                    user_email=user_email,
                    auth_token=auth_token,
                    context=prepared_context
                )
            
            elif module_type == ModuleType.GOOGLE_ANALYTICS.value:
                final_state = await run_ga4_chat(
                    user_question=user_question,
                    session_id=session_id,
                    user_email=user_email,
                    auth_token=auth_token,
                    context=prepared_context
                )
            
            elif module_type == ModuleType.INTENT_INSIGHTS.value:
                final_state = await run_intent_chat(
                    user_question=user_question,
                    session_id=session_id,
                    user_email=user_email,
                    auth_token=auth_token,
                    context=prepared_context
                )
            
            elif module_type == ModuleType.META_ADS.value:
                final_state = await run_meta_ads_chat(
                    user_question=user_question,
                    session_id=session_id,
                    user_email=user_email,
                    auth_token=auth_token,
                    context=prepared_context
                )
            
            elif module_type == ModuleType.FACEBOOK.value:
                final_state = await run_facebook_chat(
                    user_question=user_question,
                    session_id=session_id,
                    user_email=user_email,
                    auth_token=auth_token,
                    context=prepared_context
                )
            
            elif module_type == ModuleType.INSTAGRAM.value:
                final_state = await run_instagram_chat(
                    user_question=user_question,
                    session_id=session_id,
                    user_email=user_email,
                    auth_token=auth_token,
                    context=prepared_context
                )
            
            else:
                raise ValueError(f"Unsupported module type: {module_type}")
            
            # Save conversation to MongoDB if completed OR if waiting for user input (for resumption)
            logger.info(f"🔍 Checking MongoDB save conditions - mongo_manager exists: {self.mongo_manager is not None}, is_complete: {final_state.get('is_complete')}, needs_user_input: {final_state.get('needs_user_input')}")
            should_save = final_state.get("is_complete") or final_state.get("needs_user_input")

            if self.mongo_manager and should_save:
                try:
                    await self._save_conversation_to_mongodb(
                        session_id=session_id,
                        user_email=user_email,
                        module_type=module_type,
                        user_question=user_question,
                        final_state=final_state,
                        context=prepared_context
                    )
                    logger.info(f"✅ Conversation saved to MongoDB: {session_id} (is_complete: {final_state.get('is_complete')}, needs_user_input: {final_state.get('needs_user_input')})")
                except Exception as e:
                    logger.error(f"❌ Failed to save to MongoDB: {e}", exc_info=True)
                    # Don't fail the whole request if MongoDB save fails
            else:
                if not self.mongo_manager:
                    logger.warning("⚠️ MongoDB manager is None - conversation not saved")
                if not should_save:
                    logger.warning("⚠️ Neither is_complete nor needs_user_input is True - conversation not saved")
            
            logger.info(f"✅ Chat processing completed for module: {module_type}")
            
            return final_state
            
        except Exception as e:
            logger.error(f"❌ Error in graph orchestrator: {e}", exc_info=True)
            return {
                "formatted_response": f"I apologize, but I encountered an error while processing your request: {str(e)}",
                "errors": [str(e)],
                "is_complete": True,
                "module_type": module_type,
                "session_id": session_id,
                "user_question": user_question,
                "triggered_endpoints": [],
                "warnings": []
            }
    
    async def continue_conversation(
        self,
        previous_state: Dict[str, Any],
        user_response: str,
        module_type: str
    ) -> Dict[str, Any]:
        """
        Continue a conversation after user provides additional input
        (Used for Meta Ads campaign/adset/ad selection flow)
        
        Args:
            previous_state: Previous state that was waiting for input
            user_response: User's response (can be selection IDs or text)
            module_type: Module type
            
        Returns:
            Updated state
        """
        logger.info(f"🔄 Continuing conversation for module: {module_type}")
        logger.info(f"💬 User response: {user_response}")
        
        try:
            # Update state with new user response
            state = previous_state.copy()
            state["user_question"] = user_response
            state["needs_user_input"] = False
            state["user_clarification_prompt"] = None
            
            # For Meta Ads, handle selection responses
            if module_type == ModuleType.META_ADS.value:
                current_agent = state.get("current_agent")
                
                # Parse user response as JSON array of IDs if it's a selection
                try:
                    import json
                    selected_ids = json.loads(user_response)
                    
                    if current_agent == "wait_for_campaign_selection":
                        state["selected_campaign_ids"] = selected_ids
                        state["campaign_selection_options"] = None
                        logger.info(f"✅ Campaign selection received: {len(selected_ids)} campaigns")
                    
                    elif current_agent == "wait_for_adset_selection":
                        state["selected_adset_ids"] = selected_ids
                        state["adset_selection_options"] = None
                        logger.info(f"✅ AdSet selection received: {len(selected_ids)} adsets")
                    
                    elif current_agent == "wait_for_ad_selection":
                        state["selected_ad_ids"] = selected_ids
                        state["ad_selection_options"] = None
                        logger.info(f"✅ Ad selection received: {len(selected_ids)} ads")
                    
                except json.JSONDecodeError:
                    # Not a JSON array, treat as regular text question
                    logger.info("User response is text, not selection")
                    pass
            
            # Re-run the appropriate graph from continuation point
            if module_type == ModuleType.META_ADS.value:
                final_state = await run_meta_ads_chat(
                    user_question=state.get("user_question"),
                    session_id=state.get("session_id"),
                    user_email=state.get("user_email"),
                    auth_token=state.get("auth_token"),
                    context=state,
                    continue_from_state=state  # Pass previous state for continuation
                )
            else:
                # For other modules, just restart with the new question
                final_state = await self.process_chat_message(
                    user_question=user_response,
                    module_type=module_type,
                    session_id=state.get("session_id"),
                    user_email=state.get("user_email"),
                    auth_token=state.get("auth_token"),
                    context=state
                )
            
            # Save continuation to MongoDB
            if self.mongo_manager and final_state.get("is_complete"):
                try:
                    await self._save_conversation_to_mongodb(
                        session_id=state.get("session_id"),
                        user_email=state.get("user_email"),
                        module_type=module_type,
                        user_question=user_response,
                        final_state=final_state,
                        context=state
                    )
                except Exception as e:
                    logger.error(f"Failed to save continuation: {e}")
            
            return final_state
            
        except Exception as e:
            logger.error(f"❌ Error continuing conversation: {e}", exc_info=True)
            return {
                "formatted_response": f"An error occurred while processing your response: {str(e)}",
                "errors": [str(e)],
                "is_complete": True,
                "module_type": module_type
            }
    
    def _prepare_module_context(
        self,
        module_type: str,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Prepare and validate context for specific module
        
        Args:
            module_type: Module type
            context: Raw context from request
            
        Returns:
            Prepared context with correct fields
        """
        prepared = context.copy()
        
        # Ensure required fields based on module
        if module_type == ModuleType.GOOGLE_ADS.value:
            prepared.setdefault("customer_id", None)
            
        elif module_type == ModuleType.GOOGLE_ANALYTICS.value:
            prepared.setdefault("property_id", None)
            
        elif module_type == ModuleType.INTENT_INSIGHTS.value:
            prepared.setdefault("account_id", None)
            prepared.setdefault("seed_keywords", [])
            prepared.setdefault("country", None)
            prepared.setdefault("include_zero_volume", False)
            
        elif module_type == ModuleType.META_ADS.value:
            prepared.setdefault("account_id", None)
            
        elif module_type == ModuleType.FACEBOOK.value:
            prepared.setdefault("page_id", None)
            
        elif module_type == ModuleType.INSTAGRAM.value:
            prepared.setdefault("account_id", None)
        
        # Ensure time period fields exist
        prepared.setdefault("period", None)
        prepared.setdefault("start_date", None)
        prepared.setdefault("end_date", None)
        
        return prepared
    
    async def _save_conversation_to_mongodb(
        self,
        session_id: str,
        user_email: str,
        module_type: str,
        user_question: str,
        final_state: Dict[str, Any],
        context: Dict[str, Any]
    ):
        """Save conversation to MongoDB in the correct format"""
        try:
            # Extract response and metadata
            # Use user_clarification_prompt if formatted_response is None (waiting for user input)
            assistant_message = final_state.get("formatted_response") or final_state.get("user_clarification_prompt") or "Processing your request..."
            triggered_endpoints = final_state.get("triggered_endpoints", [])
            visualizations = final_state.get("visualizations")
            
            # Get module-specific IDs
            customer_id = context.get("customer_id")
            property_id = context.get("property_id")
            account_id = context.get("account_id")
            page_id = context.get("page_id")
            
            logger.info(f"💾 Saving to MongoDB - Session: {session_id}, Module: {module_type}")
            logger.info(f"📝 User: {user_question[:50]}...")
            if assistant_message:
                logger.info(f"🤖 Assistant: {assistant_message[:50]}...")
            else:
                logger.info(f"🤖 Assistant: [Waiting for user input]")
            
            # Use the mongo_manager's save method
            await self.mongo_manager.save_chat_session(
                session_id=session_id,
                user_email=user_email,
                module_type=module_type,
                user_message=user_question,
                assistant_message=assistant_message,
                customer_id=customer_id,
                property_id=property_id,
                account_id=account_id,
                page_id=page_id,
                triggered_endpoints=triggered_endpoints,
                visualizations=visualizations,
                state=final_state  # Save full state for resumption
            )
            
            logger.info(f"✅ Successfully saved conversation to MongoDB")
            
        except Exception as e:
            logger.error(f"❌ Error saving to MongoDB: {e}", exc_info=True)
            raise

    def get_module_info(self, module_type: str) -> Dict[str, Any]:
        """
        Get information about a module
        
        Args:
            module_type: Module type
            
        Returns:
            Module information
        """
        module_info = {
            ModuleType.GOOGLE_ADS.value: {
                "name": "Google Ads",
                "description": "Campaign performance, ad metrics, keyword insights",
                "required_context": ["customer_id"],
                "optional_context": ["period", "start_date", "end_date"],
                "capabilities": [
                    "Campaign analysis",
                    "Keyword performance",
                    "Geographic insights",
                    "Device performance",
                    "Time-based trends"
                ]
            },
            ModuleType.GOOGLE_ANALYTICS.value: {
                "name": "Google Analytics (GA4)",
                "description": "Website traffic, user behavior, conversions",
                "required_context": ["property_id"],
                "optional_context": ["period", "start_date", "end_date"],
                "capabilities": [
                    "Traffic source analysis",
                    "Page performance",
                    "Conversion tracking",
                    "Audience insights",
                    "Channel analysis"
                ]
            },
            ModuleType.INTENT_INSIGHTS.value: {
                "name": "Intent Insights",
                "description": "Keyword research and search trends",
                "required_context": ["account_id"],
                "optional_context": ["seed_keywords", "country", "timeframe", "start_date", "end_date"],
                "capabilities": [
                    "Keyword suggestions",
                    "Search volume data",
                    "Trend analysis",
                    "Competitive insights",
                    "Content opportunities"
                ]
            },
            ModuleType.META_ADS.value: {
                "name": "Meta Ads",
                "description": "Facebook/Instagram ad campaigns",
                "required_context": ["account_id"],
                "optional_context": ["period", "start_date", "end_date"],
                "capabilities": [
                    "Campaign performance",
                    "Ad set analysis",
                    "Ad creative insights",
                    "Demographic breakdown",
                    "Placement performance",
                    "Hierarchical drilling (Campaign → AdSet → Ad)"
                ]
            },
            ModuleType.FACEBOOK.value: {
                "name": "Facebook Pages",
                "description": "Page insights and engagement",
                "required_context": ["page_id"],
                "optional_context": ["period", "start_date", "end_date"],
                "capabilities": [
                    "Page metrics",
                    "Post performance",
                    "Audience demographics",
                    "Engagement analysis",
                    "Video insights"
                ]
            },
            ModuleType.INSTAGRAM.value: {
                "name": "Instagram",
                "description": "Profile insights and content performance",
                "required_context": ["account_id"],
                "optional_context": ["period", "start_date", "end_date"],
                "capabilities": [
                    "Profile metrics",
                    "Media performance",
                    "Story insights",
                    "Engagement tracking",
                    "Audience growth"
                ]
            }
        }
        
        return module_info.get(module_type, {
            "name": "Unknown Module",
            "description": "No information available",
            "required_context": [],
            "optional_context": [],
            "capabilities": []
        })


# ============================================================================
# SINGLETON INSTANCE
# ============================================================================

_orchestrator_instance = None


def get_orchestrator(mongo_manager=None) -> GraphOrchestrator:
    """
    Get singleton instance of GraphOrchestrator
    
    Args:
        mongo_manager: MongoManager instance
        
    Returns:
        GraphOrchestrator instance
    """
    global _orchestrator_instance
    
    if _orchestrator_instance is None:
        _orchestrator_instance = GraphOrchestrator(mongo_manager)
    elif mongo_manager and _orchestrator_instance.mongo_manager is None:
        # Update with mongo_manager if it was None before
        _orchestrator_instance.mongo_manager = mongo_manager
    
    return _orchestrator_instance


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

async def process_message(
    user_question: str,
    module_type: str,
    session_id: str,
    user_email: str,
    auth_token: str,
    context: Dict[str, Any],
    mongo_manager=None
) -> Dict[str, Any]:
    """
    Convenience function to process a chat message
    
    Args:
        user_question: User's question
        module_type: Module type
        session_id: Session identifier
        user_email: User's email
        auth_token: Authentication token (Google or Meta)
        context: Additional context (customer_id, property_id, period, etc.)
        mongo_manager: MongoManager instance
        
    Returns:
        Final state with response
    """
    orchestrator = get_orchestrator(mongo_manager)
    
    return await orchestrator.process_chat_message(
        user_question=user_question,
        module_type=module_type,
        session_id=session_id,
        user_email=user_email,
        auth_token=auth_token,
        context=context
    )


async def continue_message(
    previous_state: Dict[str, Any],
    user_response: str,
    module_type: str,
    mongo_manager=None
) -> Dict[str, Any]:
    """
    Convenience function to continue a conversation
    (Used for Meta Ads multi-step selection flow)
    
    Args:
        previous_state: Previous state that was waiting for user input
        user_response: User's response (selection or text)
        module_type: Module type
        mongo_manager: MongoManager instance
        
    Returns:
        Updated state
    """
    orchestrator = get_orchestrator(mongo_manager)
    
    return await orchestrator.continue_conversation(
        previous_state=previous_state,
        user_response=user_response,
        module_type=module_type
    )


def get_available_modules() -> List[Dict[str, Any]]:
    """
    Get list of all available modules with their capabilities
    
    Returns:
        List of module information dictionaries
    """
    orchestrator = get_orchestrator()
    
    return [
        {
            "module_type": module.value,
            **orchestrator.get_module_info(module.value)
        }
        for module in ModuleType
        if module.value != "combined"  # Exclude combined module
    ]


def validate_module_context(module_type: str, context: Dict[str, Any]) -> tuple[bool, List[str]]:
    """
    Validate that context has required fields for a module
    
    Args:
        module_type: Module type
        context: Context dictionary
        
    Returns:
        Tuple of (is_valid, list_of_missing_fields)
    """
    orchestrator = get_orchestrator()
    module_info = orchestrator.get_module_info(module_type)
    
    required_fields = module_info.get("required_context", [])
    missing_fields = [
        field for field in required_fields
        if not context.get(field)
    ]
    
    return len(missing_fields) == 0, missing_fields