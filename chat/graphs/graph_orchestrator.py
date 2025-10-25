"""
Graph Orchestrator - Main entry point for LangGraph chat system
Routes chat requests to appropriate module-specific graphs
"""

import logging
from typing import Dict, Any, List
from datetime import datetime

from chat.graphs.google_ads_graph import run_google_ads_chat
from chat.graphs.ga4_graph import run_ga4_chat
from chat.graphs.intent_graph import run_intent_chat
from chat.graphs.meta_ads_graph import run_meta_ads_chat
from chat.graphs.facebook_instagram_graphs import run_facebook_chat, run_instagram_chat
from chat.utils.api_client import save_conversation_to_mongodb
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
            auth_token: Authentication token
            context: Additional context from frontend
            
        Returns:
            Final state with response
        """
        logger.info(f"Processing chat message for module: {module_type}")
        logger.info(f"User: {user_email}, Session: {session_id}")
        
        try:
            # Validate module type
            if module_type not in [e.value for e in ModuleType]:
                raise ValueError(f"Invalid module type: {module_type}")
            
            # Route to appropriate graph
            if module_type == ModuleType.GOOGLE_ADS.value:
                final_state = await run_google_ads_chat(
                    user_question, session_id, user_email, auth_token, context
                )
            
            elif module_type == ModuleType.GOOGLE_ANALYTICS.value:
                final_state = await run_ga4_chat(
                    user_question, session_id, user_email, auth_token, context
                )
            
            elif module_type == ModuleType.INTENT_INSIGHTS.value:
                final_state = await run_intent_chat(
                    user_question, session_id, user_email, auth_token, context
                )
            
            elif module_type == ModuleType.META_ADS.value:
                final_state = await run_meta_ads_chat(
                    user_question, session_id, user_email, auth_token, context
                )
            
            elif module_type == ModuleType.FACEBOOK.value:
                final_state = await run_facebook_chat(
                    user_question, session_id, user_email, auth_token, context
                )
            
            elif module_type == ModuleType.INSTAGRAM.value:
                final_state = await run_instagram_chat(
                    user_question, session_id, user_email, auth_token, context
                )
            
            else:
                raise ValueError(f"Unsupported module type: {module_type}")
            
            # Save conversation to MongoDB if manager available
            if self.mongo_manager and final_state.get("is_complete"):
                try:
                    save_conversation_to_mongodb(
                        session_id=session_id,
                        user_email=user_email,
                        module_type=module_type,
                        state=final_state,
                        mongo_manager=self.mongo_manager
                    )
                except Exception as e:
                    logger.error(f"Failed to save to MongoDB: {e}")
                    # Don't fail the whole request if MongoDB save fails
            
            logger.info(f"Chat processing completed for module: {module_type}")
            
            return final_state
            
        except Exception as e:
            logger.error(f"Error in graph orchestrator: {e}", exc_info=True)
            return {
                "formatted_response": f"An error occurred while processing your request: {str(e)}",
                "errors": [str(e)],
                "is_complete": True,
                "module_type": module_type,
                "session_id": session_id
            }
    
    async def continue_conversation(
        self,
        previous_state: Dict[str, Any],
        user_response: str,
        module_type: str
    ) -> Dict[str, Any]:
        """
        Continue a conversation after user provides additional input
        
        Args:
            previous_state: Previous state that was waiting for input
            user_response: User's response
            module_type: Module type
            
        Returns:
            Updated state
        """
        logger.info(f"Continuing conversation for module: {module_type}")
        
        try:
            # Update state with new user response
            state = previous_state.copy()
            state["user_question"] = user_response
            state["needs_user_input"] = False
            state["user_clarification_prompt"] = None
            
            # For Meta Ads, handle special cases
            if module_type == ModuleType.META_ADS.value:
                current_agent = state.get("current_agent")
                
                # If waiting for campaign selection
                if current_agent == "wait_for_campaign_selection":
                    # User response should contain campaign IDs
                    # Parse and set them in state
                    # Then continue from campaign_decision
                    pass
                
                # If waiting for adset selection
                elif current_agent == "wait_for_adset_selection":
                    # Parse adset IDs and continue
                    pass
                
                # If waiting for ad selection
                elif current_agent == "wait_for_ad_selection":
                    # Parse ad IDs and continue
                    pass
            
            # Re-run the appropriate graph
            # This is a simplified version - in production you'd want to
            # continue from the exact point where it left off
            return await self.process_chat_message(
                user_question=user_response,
                module_type=module_type,
                session_id=state.get("session_id"),
                user_email=state.get("user_email"),
                auth_token=state.get("auth_token"),
                context=state
            )
            
        except Exception as e:
            logger.error(f"Error continuing conversation: {e}")
            return {
                "formatted_response": f"An error occurred: {str(e)}",
                "errors": [str(e)],
                "is_complete": True
            }
    
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
                "capabilities": [
                    "Campaign performance",
                    "Ad set analysis",
                    "Ad creative insights",
                    "Demographic breakdown",
                    "Placement performance"
                ]
            },
            ModuleType.FACEBOOK.value: {
                "name": "Facebook Pages",
                "description": "Page insights and engagement",
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
        auth_token: Authentication token
        context: Additional context
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
    
    Args:
        previous_state: Previous state
        user_response: User's response
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
    Get list of all available modules
    
    Returns:
        List of module information
    """
    orchestrator = get_orchestrator()
    
    return [
        orchestrator.get_module_info(module.value)
        for module in ModuleType
    ]