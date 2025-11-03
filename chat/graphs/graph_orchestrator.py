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
        logger.info("=" * 80)
        logger.info(f"🚀 GRAPH ORCHESTRATOR: Processing chat message")
        logger.info(f"   Module: {module_type}")
        logger.info(f"   User: {user_email}")
        logger.info(f"   Session: {session_id}")
        logger.info(f"   Question length: {len(user_question)} chars")
        logger.info(f"   Question preview: {user_question[:100]}...")
        logger.info(f"   Context: {context}")
        logger.info(f"   Auth token present: {'Yes' if auth_token else 'No'}")

        try:
            # Validate module type
            logger.info(f"🔍 GRAPH ORCHESTRATOR: Validating module type: {module_type}")
            if module_type not in [e.value for e in ModuleType]:
                logger.error(f"❌ GRAPH ORCHESTRATOR: Invalid module type: {module_type}")
                logger.error(f"   Valid module types: {[e.value for e in ModuleType]}")
                raise ValueError(f"Invalid module type: {module_type}")
            logger.info(f"✅ GRAPH ORCHESTRATOR: Module type is valid")

            # Prepare context with module-specific parameters
            logger.info(f"📦 GRAPH ORCHESTRATOR: Preparing module-specific context")
            prepared_context = self._prepare_module_context(module_type, context)
            logger.info(f"📦 GRAPH ORCHESTRATOR: Context prepared: {prepared_context}")
            
            # Route to appropriate graph
            logger.info(f"🎯 GRAPH ORCHESTRATOR: Routing to {module_type} graph")

            if module_type == ModuleType.GOOGLE_ADS.value:
                logger.info(f"📞 GRAPH ORCHESTRATOR: Calling run_google_ads_chat()")
                final_state = await run_google_ads_chat(
                    user_question=user_question,
                    session_id=session_id,
                    user_email=user_email,
                    auth_token=auth_token,
                    context=prepared_context
                )
                logger.info(f"✅ GRAPH ORCHESTRATOR: Google Ads chat completed")

            elif module_type == ModuleType.GOOGLE_ANALYTICS.value:
                logger.info(f"📞 GRAPH ORCHESTRATOR: Calling run_ga4_chat()")
                final_state = await run_ga4_chat(
                    user_question=user_question,
                    session_id=session_id,
                    user_email=user_email,
                    auth_token=auth_token,
                    context=prepared_context
                )
                logger.info(f"✅ GRAPH ORCHESTRATOR: Google Analytics chat completed")

            elif module_type == ModuleType.INTENT_INSIGHTS.value:
                logger.info(f"📞 GRAPH ORCHESTRATOR: Calling run_intent_chat()")
                final_state = await run_intent_chat(
                    user_question=user_question,
                    session_id=session_id,
                    user_email=user_email,
                    auth_token=auth_token,
                    context=prepared_context
                )
                logger.info(f"✅ GRAPH ORCHESTRATOR: Intent Insights chat completed")

            elif module_type == ModuleType.META_ADS.value:
                logger.info(f"📞 GRAPH ORCHESTRATOR: Calling run_meta_ads_chat()")
                final_state = await run_meta_ads_chat(
                    user_question=user_question,
                    session_id=session_id,
                    user_email=user_email,
                    auth_token=auth_token,
                    context=prepared_context
                )
                logger.info(f"✅ GRAPH ORCHESTRATOR: Meta Ads chat completed")

            else:
                logger.error(f"❌ GRAPH ORCHESTRATOR: Unsupported module type: {module_type}")
                raise ValueError(f"Unsupported module type: {module_type}")

            logger.info(f"📊 GRAPH ORCHESTRATOR: Final state keys: {list(final_state.keys())}")
            logger.info(f"📊 GRAPH ORCHESTRATOR: Is complete: {final_state.get('is_complete')}")
            logger.info(f"📊 GRAPH ORCHESTRATOR: Needs user input: {final_state.get('needs_user_input')}")
            logger.info(f"📊 GRAPH ORCHESTRATOR: Triggered endpoints count: {len(final_state.get('triggered_endpoints', []))}")
            
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
        logger.info("=" * 80)
        logger.info(f"🔄 CONTINUE CONVERSATION")
        logger.info(f"   Module: {module_type}")
        logger.info(f"   User response: {user_response}")
        logger.info(f"   Previous state has {len(previous_state)} keys")

        try:
            import json

            # Update state with new user response
            state = previous_state.copy()
            state["needs_user_input"] = False
            state["user_clarification_prompt"] = None

            # For Meta Ads: Parse selection IDs from JSON
            if module_type == ModuleType.META_ADS.value:
                logger.info("📋 Meta Ads module - parsing selection")

                try:
                    # Parse JSON array of selected IDs
                    selected_ids = json.loads(user_response)
                    logger.info(f"✅ Parsed {len(selected_ids)} selected IDs: {selected_ids}")

                    # Log all awaiting flags for debugging
                    logger.info("=" * 80)
                    logger.info("🔍 CHECKING SELECTION FLAGS:")
                    logger.info(f"   awaiting_campaign_selection: {state.get('awaiting_campaign_selection')}")
                    logger.info(f"   awaiting_adset_selection: {state.get('awaiting_adset_selection')}")
                    logger.info(f"   awaiting_ad_selection: {state.get('awaiting_ad_selection')}")
                    logger.info(f"   Current campaign_ids: {state.get('campaign_ids')}")
                    logger.info(f"   Current adset_ids: {state.get('adset_ids')}")
                    logger.info(f"   Current ad_ids: {state.get('ad_ids')}")
                    logger.info(f"   Current granularity_level: {state.get('granularity_level')}")
                    logger.info("=" * 80)

                    # Determine what type of selection this is
                    if state.get("awaiting_campaign_selection"):
                        logger.info("📊 Storing selected campaign IDs")
                        state["campaign_ids"] = selected_ids
                        state["awaiting_campaign_selection"] = False
                        logger.info(f"✅ Stored campaign_ids: {state['campaign_ids']}")

                    elif state.get("awaiting_adset_selection"):
                        logger.info("📊 Storing selected adset IDs")
                        state["adset_ids"] = selected_ids
                        state["awaiting_adset_selection"] = False
                        # Keep campaign_ids for context (needed to know which campaigns these adsets belong to)
                        logger.info(f"✅ Stored adset_ids: {state['adset_ids']}")
                        logger.info(f"   Campaign IDs (preserved): {state.get('campaign_ids')}")

                    elif state.get("awaiting_ad_selection"):
                        logger.info("📊 Storing selected ad IDs")
                        state["ad_ids"] = selected_ids
                        state["awaiting_ad_selection"] = False
                        # Keep campaign_ids and adset_ids for context
                        logger.info(f"✅ Stored ad_ids: {state['ad_ids']}")
                        logger.info(f"   Campaign IDs (preserved): {state.get('campaign_ids')}")
                        logger.info(f"   Adset IDs (preserved): {state.get('adset_ids')}")

                    else:
                        logger.warning("⚠️ No selection flag set, treating as campaign selection")
                        logger.warning(f"   This might indicate the awaiting flag was not preserved from MongoDB")
                        state["campaign_ids"] = selected_ids

                except json.JSONDecodeError as e:
                    logger.error(f"❌ Failed to parse user_response as JSON: {e}")
                    logger.error(f"   Raw user_response: {user_response}")
                    # Fall back to treating it as text
                    pass

                # Update user_question to reflect the selection for logging
                original_question = state.get("user_question", "")
                state["user_question"] = f"Selected: {user_response}"

                # Continue with Meta Ads graph
                logger.info("🚀 Re-running Meta Ads graph with selected IDs")
                from chat.graphs.meta_ads_graph import run_meta_ads_chat

                final_state = await run_meta_ads_chat(
                    user_question=original_question,  # Use original question
                    session_id=state.get("session_id"),
                    user_email=state.get("user_email"),
                    auth_token=state.get("auth_token"),
                    context=state  # Pass full state with campaign_ids
                )

            else:
                # For other modules: treat as regular text continuation
                logger.info("📝 Other module - treating as text question")
                final_state = await self.process_chat_message(
                    user_question=user_response,
                    module_type=module_type,
                    session_id=state.get("session_id"),
                    user_email=state.get("user_email"),
                    auth_token=state.get("auth_token"),
                    context=state
                )
            
            # Save continuation to MongoDB (save when complete OR when waiting for user input)
            # This ensures multi-step selection states (e.g., awaiting_adset_selection) are persisted
            if self.mongo_manager and (final_state.get("is_complete") or final_state.get("needs_user_input")):
                try:
                    logger.info("=" * 80)
                    logger.info("💾 SAVING STATE TO MONGODB")
                    logger.info(f"   is_complete: {final_state.get('is_complete')}")
                    logger.info(f"   needs_user_input: {final_state.get('needs_user_input')}")
                    if module_type == "meta_ads":
                        logger.info(f"   awaiting_campaign_selection: {final_state.get('awaiting_campaign_selection')}")
                        logger.info(f"   awaiting_adset_selection: {final_state.get('awaiting_adset_selection')}")
                        logger.info(f"   awaiting_ad_selection: {final_state.get('awaiting_ad_selection')}")
                        logger.info(f"   granularity_level: {final_state.get('granularity_level')}")
                    logger.info("=" * 80)

                    await self._save_conversation_to_mongodb(
                        session_id=state.get("session_id"),
                        user_email=state.get("user_email"),
                        module_type=module_type,
                        user_question=user_response,
                        final_state=final_state,
                        context=state
                    )

                    logger.info("✅ State saved successfully to MongoDB")
                except Exception as e:
                    logger.error(f"❌ Failed to save continuation: {e}")
            
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
            prepared.setdefault("campaign_ids", None)
            prepared.setdefault("adset_ids", None)
            prepared.setdefault("ad_ids", None)

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

            # Handle case where assistant_message is a dict (Meta Ads selection)
            if isinstance(assistant_message, dict):
                # Extract the message string from the selection dict
                assistant_message = assistant_message.get("message", "Please make a selection to continue.")

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
                logger.info(f"🤖 Assistant: {assistant_message[:50] if isinstance(assistant_message, str) else str(assistant_message)[:50]}...")
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
                "description": "Facebook and Instagram ads performance and analytics",
                "required_context": ["account_id"],
                "optional_context": ["period", "start_date", "end_date", "campaign_ids", "adset_ids", "ad_ids"],
                "capabilities": [
                    "Account-level insights",
                    "Campaign performance analysis",
                    "Adset performance tracking",
                    "Individual ad analytics",
                    "Demographics breakdown",
                    "Placement analysis (Facebook, Instagram)",
                    "Time-series trends",
                    "Multi-level hierarchical analysis"
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