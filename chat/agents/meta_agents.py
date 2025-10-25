# """
# Meta-Specific Agents for LangGraph Multi-Module Chat System
# Handles the hierarchical Meta Ads flow: Account → Campaign → AdSet → Ad
# """

# import logging
# import json
# from typing import Dict, Any, List, Optional
# from openai import OpenAI
# import os

# # Initialize logger
# logger = logging.getLogger(__name__)

# # Initialize OpenAI client
# client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
# DEFAULT_MODEL = "gpt-4-turbo-preview"


# # ============================================================================
# # META AGENT 3: GRANULARITY DETECTION AGENT
# # ============================================================================

# def meta_agent_3_granularity_detection(state: Dict[str, Any]) -> Dict[str, Any]:
#     """
#     Meta Agent 3: Detect granularity level (account, campaign, adset, or ad)
    
#     Args:
#         state: Current chat state
        
#     Returns:
#         Updated state with granularity_level set
#     """
#     logger.info("=== META AGENT 3: Granularity Detection ===")
#     state["current_agent"] = "meta_agent_3_granularity_detection"
    
#     user_question = state.get("user_question", "")
    
#     try:
#         system_prompt = """You are analyzing a user's question about Meta/Facebook advertising data.

# Determine the GRANULARITY LEVEL of their question:

# 1. ACCOUNT - Overall account insights, summary metrics, no specific campaigns mentioned
#    Examples: "overall performance", "account summary", "total spend", "how's my account doing"

# 2. CAMPAIGN - Asking about specific campaigns or campaign-level data
#    Examples: "show campaign performance", "which campaigns", "campaign metrics", "my campaigns"

# 3. ADSET - Asking about specific adsets or adset-level data
#    Examples: "show adsets", "adset performance", "which adsets", "my adsets"

# 4. AD - Asking about specific ads or ad-level data
#    Examples: "show ads", "ad performance", "which ads", "my ads", "ad creatives"

# Return ONLY one word: "account", "campaign", "adset", or "ad"

# If unclear or asking for general help, return "account".
# """

#         user_prompt = f"Analyze this question: {user_question}"
        
#         response = client.chat.completions.create(
#             model=DEFAULT_MODEL,
#             messages=[
#                 {"role": "system", "content": system_prompt},
#                 {"role": "user", "content": user_prompt}
#             ],
#             temperature=0.1,
#             max_tokens=10
#         )
        
#         granularity = response.choices[0].message.content.strip().lower()
        
#         # Validate response
#         valid_levels = ["account", "campaign", "adset", "ad"]
#         if granularity not in valid_levels:
#             granularity = "account"
#             logger.warning(f"Invalid granularity detected, defaulting to: account")
        
#         state["granularity_level"] = granularity
        
#         # Set flags for routing
#         state["is_account_level"] = (granularity == "account")
#         state["is_campaign_level"] = (granularity == "campaign")
#         state["is_adset_level"] = (granularity == "adset")
#         state["is_ad_level"] = (granularity == "ad")
        
#         logger.info(f"Granularity level detected: {granularity}")
        
#         return state
        
#     except Exception as e:
#         logger.error(f"Error in granularity detection: {e}")
#         state["errors"].append(f"Granularity detection failed: {str(e)}")
#         # Default to account level
#         state["granularity_level"] = "account"
#         state["is_account_level"] = True
#         return state


# # ============================================================================
# # META AGENT 4: CAMPAIGN SELECTION AGENT
# # ============================================================================

# def meta_agent_4_campaign_selection(state: Dict[str, Any]) -> Dict[str, Any]:
#     """
#     Meta Agent 4: Load and prepare campaign selection
#     This may take several minutes due to API limitations
    
#     Args:
#         state: Current chat state
        
#     Returns:
#         Updated state with available_campaigns
#     """
#     logger.info("=== META AGENT 4: Campaign Selection ===")
#     state["current_agent"] = "meta_agent_4_campaign_selection"
    
#     account_id = state.get("account_id")
#     auth_token = state.get("auth_token")
    
#     if not account_id:
#         logger.error("No account_id provided for campaign selection")
#         state["errors"].append("Missing Meta ad account ID")
#         return state
    
#     try:
#         # Import the special handler
#         from chat.utils.api_client import handle_meta_campaigns_loading
        
#         # Set loading flag
#         state["campaigns_loading"] = True
        
#         # Show loading message to user
#         state["needs_user_input"] = True
#         state["user_clarification_prompt"] = (
#             "⏳ Loading all campaigns from your Meta ad account. "
#             "This may take several minutes due to API rate limits. Please wait..."
#         )
        
#         logger.info(f"Loading campaigns for account: {account_id}")
        
#         # Load all campaigns (this can take 3-5 minutes)
#         response = handle_meta_campaigns_loading(
#             account_id=account_id,
#             auth_token=auth_token,
#             status_filter=state.get("status_filter")
#         )
        
#         state["campaigns_loading"] = False
        
#         if not response.get("success"):
#             logger.error(f"Failed to load campaigns: {response.get('error')}")
#             state["errors"].append("Failed to load campaigns from Meta")
#             return state
        
#         # Extract campaigns data
#         data = response.get("data", {})
#         campaigns = data.get("campaigns", [])
        
#         if not campaigns:
#             logger.warning("No campaigns found for this account")
#             state["warnings"].append("No campaigns found in your Meta ad account")
#             return state
        
#         # Store campaigns for selection
#         state["available_campaigns"] = campaigns
        
#         # Prepare campaign selection dropdown data
#         campaign_options = [
#             {
#                 "id": campaign["id"],
#                 "name": campaign["name"],
#                 "status": campaign.get("status", "UNKNOWN"),
#                 "objective": campaign.get("objective", "UNKNOWN")
#             }
#             for campaign in campaigns
#         ]
        
#         state["campaign_selection_options"] = campaign_options
#         state["needs_campaign_selection"] = True
#         state["needs_user_input"] = True
#         state["user_clarification_prompt"] = (
#             f"I found {len(campaigns)} campaigns in your account. "
#             "Please select the campaign(s) you'd like to analyze."
#         )
        
#         logger.info(f"Loaded {len(campaigns)} campaigns successfully")
        
#         # Add to triggered endpoints for MongoDB
#         if "triggered_endpoints" not in state:
#             state["triggered_endpoints"] = []
#         state["triggered_endpoints"].append(response)
        
#         return state
        
#     except Exception as e:
#         logger.error(f"Error in campaign selection: {e}")
#         state["errors"].append(f"Campaign selection failed: {str(e)}")
#         state["campaigns_loading"] = False
#         return state


# # ============================================================================
# # META AGENT 5: CAMPAIGN-LEVEL DECISION AGENT
# # ============================================================================

# def meta_agent_5_campaign_level_decision(state: Dict[str, Any]) -> Dict[str, Any]:
#     """
#     Meta Agent 5: Determine if question can be answered at campaign level
#     or needs to go deeper to adset level
    
#     Args:
#         state: Current chat state
        
#     Returns:
#         Updated state with decision flags
#     """
#     logger.info("=== META AGENT 5: Campaign-Level Decision ===")
#     state["current_agent"] = "meta_agent_5_campaign_level_decision"
    
#     user_question = state.get("user_question", "")
#     campaign_ids = state.get("campaign_ids", [])
    
#     if not campaign_ids:
#         logger.warning("No campaign IDs selected")
#         state["warnings"].append("No campaigns selected")
#         return state
    
#     try:
#         system_prompt = """You are analyzing whether a user's question about Meta ads can be answered at the CAMPAIGN level, or if it requires ADSET-level or AD-level data.

# Determine if the question is ONLY about campaigns, or if it mentions adsets/ads:

# CAMPAIGN-LEVEL questions (return "campaign"):
# - Campaign performance, metrics, insights
# - Campaign comparisons
# - Campaign trends over time
# - Campaign demographics
# - Campaign placements
# - NO mention of adsets or ads

# NEEDS DEEPER LEVEL (return "deeper"):
# - Mentions "adset", "ad set", or "ad-set"
# - Mentions "ad", "ads", "ad creative", "creative"
# - Asks about specific ads or adsets within campaigns
# - Asks for ad-level or adset-level breakdown

# Return ONLY one word: "campaign" or "deeper"
# """

#         user_prompt = f"Question: {user_question}\n\nCan this be answered at campaign level only?"
        
#         response = client.chat.completions.create(
#             model=DEFAULT_MODEL,
#             messages=[
#                 {"role": "system", "content": system_prompt},
#                 {"role": "user", "content": user_prompt}
#             ],
#             temperature=0.1,
#             max_tokens=10
#         )
        
#         decision = response.choices[0].message.content.strip().lower()
        
#         if decision == "campaign":
#             state["stop_at_campaign_level"] = True
#             state["needs_adset_selection"] = False
#             logger.info("Decision: Can answer at campaign level")
#         else:
#             state["stop_at_campaign_level"] = False
#             state["needs_adset_selection"] = True
#             logger.info("Decision: Needs to go deeper to adset level")
        
#         return state
        
#     except Exception as e:
#         logger.error(f"Error in campaign-level decision: {e}")
#         state["errors"].append(f"Campaign-level decision failed: {str(e)}")
#         # Default to campaign level to be safe
#         state["stop_at_campaign_level"] = True
#         return state


# # ============================================================================
# # META AGENT 6: ADSET SELECTION AGENT
# # ============================================================================

# def meta_agent_6_adset_selection(state: Dict[str, Any]) -> Dict[str, Any]:
#     """
#     Meta Agent 6: Load adsets for selected campaigns
    
#     Args:
#         state: Current chat state
        
#     Returns:
#         Updated state with available_adsets
#     """
#     logger.info("=== META AGENT 6: AdSet Selection ===")
#     state["current_agent"] = "meta_agent_6_adset_selection"
    
#     campaign_ids = state.get("campaign_ids", [])
#     auth_token = state.get("auth_token")
    
#     if not campaign_ids:
#         logger.error("No campaign IDs provided for adset selection")
#         state["errors"].append("Missing campaign IDs")
#         return state
    
#     try:
#         from chat.utils.api_client import handle_meta_adsets_loading
        
#         logger.info(f"Loading adsets for {len(campaign_ids)} campaigns")
        
#         # Load adsets
#         response = handle_meta_adsets_loading(
#             campaign_ids=campaign_ids,
#             auth_token=auth_token
#         )
        
#         if not response.get("success"):
#             logger.error(f"Failed to load adsets: {response.get('error')}")
#             state["errors"].append("Failed to load adsets from Meta")
#             return state
        
#         # Extract adsets data
#         adsets = response.get("data", [])
        
#         if not adsets:
#             logger.warning("No adsets found for selected campaigns")
#             state["warnings"].append("No adsets found for the selected campaigns")
#             return state
        
#         # Store adsets for selection
#         state["available_adsets"] = adsets
        
#         # Prepare adset selection dropdown data
#         adset_options = [
#             {
#                 "id": adset["id"],
#                 "name": adset["name"],
#                 "campaign_id": adset.get("campaign_id"),
#                 "status": adset.get("status", "UNKNOWN")
#             }
#             for adset in adsets
#         ]
        
#         state["adset_selection_options"] = adset_options
#         state["needs_adset_selection"] = True
#         state["needs_user_input"] = True
#         state["user_clarification_prompt"] = (
#             f"I found {len(adsets)} adsets in the selected campaigns. "
#             "Please select the adset(s) you'd like to analyze."
#         )
        
#         logger.info(f"Loaded {len(adsets)} adsets successfully")
        
#         # Add to triggered endpoints
#         if "triggered_endpoints" not in state:
#             state["triggered_endpoints"] = []
#         state["triggered_endpoints"].append(response)
        
#         return state
        
#     except Exception as e:
#         logger.error(f"Error in adset selection: {e}")
#         state["errors"].append(f"AdSet selection failed: {str(e)}")
#         return state


# # ============================================================================
# # META AGENT 7: ADSET-LEVEL DECISION AGENT
# # ============================================================================

# def meta_agent_7_adset_level_decision(state: Dict[str, Any]) -> Dict[str, Any]:
#     """
#     Meta Agent 7: Determine if question can be answered at adset level
#     or needs to go deeper to ad level
    
#     Args:
#         state: Current chat state
        
#     Returns:
#         Updated state with decision flags
#     """
#     logger.info("=== META AGENT 7: AdSet-Level Decision ===")
#     state["current_agent"] = "meta_agent_7_adset_level_decision"
    
#     user_question = state.get("user_question", "")
#     adset_ids = state.get("adset_ids", [])
    
#     if not adset_ids:
#         logger.warning("No adset IDs selected")
#         state["warnings"].append("No adsets selected")
#         return state
    
#     try:
#         system_prompt = """You are analyzing whether a user's question about Meta ads can be answered at the ADSET level, or if it requires AD-level data.

# Determine if the question is ONLY about adsets, or if it mentions ads:

# ADSET-LEVEL questions (return "adset"):
# - AdSet performance, metrics, insights
# - AdSet comparisons
# - AdSet trends over time
# - AdSet demographics
# - AdSet placements
# - NO specific mention of individual ads or ad creatives

# NEEDS AD LEVEL (return "deeper"):
# - Mentions "ad", "ads", "advertisement"
# - Mentions "ad creative", "creative", "ad copy"
# - Asks about specific ads within adsets
# - Asks for ad-level breakdown or individual ad performance

# Return ONLY one word: "adset" or "deeper"
# """

#         user_prompt = f"Question: {user_question}\n\nCan this be answered at adset level only?"
        
#         response = client.chat.completions.create(
#             model=DEFAULT_MODEL,
#             messages=[
#                 {"role": "system", "content": system_prompt},
#                 {"role": "user", "content": user_prompt}
#             ],
#             temperature=0.1,
#             max_tokens=10
#         )
        
#         decision = response.choices[0].message.content.strip().lower()
        
#         if decision == "adset":
#             state["stop_at_adset_level"] = True
#             state["needs_ad_selection"] = False
#             logger.info("Decision: Can answer at adset level")
#         else:
#             state["stop_at_adset_level"] = False
#             state["needs_ad_selection"] = True
#             logger.info("Decision: Needs to go deeper to ad level")
        
#         return state
        
#     except Exception as e:
#         logger.error(f"Error in adset-level decision: {e}")
#         state["errors"].append(f"AdSet-level decision failed: {str(e)}")
#         # Default to adset level
#         state["stop_at_adset_level"] = True
#         return state


# # ============================================================================
# # META AGENT 8: AD SELECTION AGENT
# # ============================================================================

# def meta_agent_8_ad_selection(state: Dict[str, Any]) -> Dict[str, Any]:
#     """
#     Meta Agent 8: Load ads for selected adsets
    
#     Args:
#         state: Current chat state
        
#     Returns:
#         Updated state with available_ads
#     """
#     logger.info("=== META AGENT 8: Ad Selection ===")
#     state["current_agent"] = "meta_agent_8_ad_selection"
    
#     adset_ids = state.get("adset_ids", [])
#     auth_token = state.get("auth_token")
    
#     if not adset_ids:
#         logger.error("No adset IDs provided for ad selection")
#         state["errors"].append("Missing adset IDs")
#         return state
    
#     try:
#         from chat.utils.api_client import handle_meta_ads_loading
        
#         logger.info(f"Loading ads for {len(adset_ids)} adsets")
        
#         # Load ads
#         response = handle_meta_ads_loading(
#             adset_ids=adset_ids,
#             auth_token=auth_token
#         )
        
#         if not response.get("success"):
#             logger.error(f"Failed to load ads: {response.get('error')}")
#             state["errors"].append("Failed to load ads from Meta")
#             return state
        
#         # Extract ads data
#         ads = response.get("data", [])
        
#         if not ads:
#             logger.warning("No ads found for selected adsets")
#             state["warnings"].append("No ads found for the selected adsets")
#             return state
        
#         # Store ads for selection
#         state["available_ads"] = ads
        
#         # Prepare ad selection dropdown data
#         ad_options = [
#             {
#                 "id": ad["id"],
#                 "name": ad["name"],
#                 "ad_set_id": ad.get("ad_set_id"),
#                 "status": ad.get("status", "UNKNOWN")
#             }
#             for ad in ads
#         ]
        
#         state["ad_selection_options"] = ad_options
#         state["needs_ad_selection"] = True
#         state["needs_user_input"] = True
#         state["user_clarification_prompt"] = (
#             f"I found {len(ads)} ads in the selected adsets. "
#             "Please select the ad(s) you'd like to analyze."
#         )
        
#         logger.info(f"Loaded {len(ads)} ads successfully")
        
#         # Add to triggered endpoints
#         if "triggered_endpoints" not in state:
#             state["triggered_endpoints"] = []
#         state["triggered_endpoints"].append(response)
        
#         return state
        
#     except Exception as e:
#         logger.error(f"Error in ad selection: {e}")
#         state["errors"].append(f"Ad selection failed: {str(e)}")
#         return state


# # ============================================================================
# # META AGENT 9: AD-LEVEL ANALYSIS AGENT
# # ============================================================================

# def meta_agent_9_ad_level_analysis(state: Dict[str, Any]) -> Dict[str, Any]:
#     """
#     Meta Agent 9: Determine if question can be answered with selected ads
#     or needs clarification
    
#     Args:
#         state: Current chat state
        
#     Returns:
#         Updated state with can_answer_with_ads flag
#     """
#     logger.info("=== META AGENT 9: Ad-Level Analysis ===")
#     state["current_agent"] = "meta_agent_9_ad_level_analysis"
    
#     user_question = state.get("user_question", "")
#     ad_ids = state.get("ad_ids", [])
    
#     if not ad_ids:
#         logger.warning("No ad IDs selected")
#         state["can_answer_with_ads"] = False
#         state["needs_user_input"] = True
#         state["user_clarification_prompt"] = (
#             "Could you please elaborate on your question by specifying which "
#             "campaigns, adsets, or ads you're interested in?"
#         )
#         return state
    
#     try:
#         system_prompt = """You are analyzing whether a user's question about Meta ads can be answered with the selected ad(s).

# Determine if the question is answerable:

# ANSWERABLE (return "yes"):
# - Questions about ad performance, metrics, insights
# - Questions about ad creatives, copy, images
# - Questions about specific ads
# - Questions with clear metrics like clicks, impressions, CTR for ads

# NOT CLEAR (return "no"):
# - Very vague questions with no clear metrics
# - Questions that don't specify what they want to know
# - Questions that seem to ask about something else entirely

# Return ONLY one word: "yes" or "no"
# """

#         user_prompt = f"Question: {user_question}\n\nCan this be answered with ad-level data?"
        
#         response = client.chat.completions.create(
#             model=DEFAULT_MODEL,
#             messages=[
#                 {"role": "system", "content": system_prompt},
#                 {"role": "user", "content": user_prompt}
#             ],
#             temperature=0.1,
#             max_tokens=10
#         )
        
#         decision = response.choices[0].message.content.strip().lower()
        
#         if decision == "yes":
#             state["can_answer_with_ads"] = True
#             logger.info("Decision: Can answer with ad-level data")
#         else:
#             state["can_answer_with_ads"] = False
#             state["needs_user_input"] = True
#             state["user_clarification_prompt"] = (
#                 "Could you please provide more details about what specific metrics or "
#                 "insights you'd like to see for these ads?"
#             )
#             logger.info("Decision: Need clarification from user")
        
#         return state
        
#     except Exception as e:
#         logger.error(f"Error in ad-level analysis: {e}")
#         state["errors"].append(f"Ad-level analysis failed: {str(e)}")
#         state["can_answer_with_ads"] = True  # Default to try answering
#         return state


# # ============================================================================
# # UTILITY FUNCTIONS FOR META MODULE
# # ============================================================================

# def extract_campaign_ids_from_names(
#     campaign_names: List[str],
#     available_campaigns: List[Dict[str, Any]]
# ) -> List[str]:
#     """
#     Extract campaign IDs from campaign names
    
#     Args:
#         campaign_names: List of campaign names mentioned by user
#         available_campaigns: List of available campaigns
        
#     Returns:
#         List of campaign IDs
#     """
#     campaign_ids = []
    
#     for name in campaign_names:
#         name_lower = name.lower()
#         for campaign in available_campaigns:
#             if name_lower in campaign.get("name", "").lower():
#                 campaign_ids.append(campaign["id"])
#                 break
    
#     return campaign_ids


# def format_campaign_summary(campaigns: List[Dict[str, Any]]) -> str:
#     """
#     Format campaign list for display
    
#     Args:
#         campaigns: List of campaigns
        
#     Returns:
#         Formatted string
#     """
#     if not campaigns:
#         return "No campaigns found"
    
#     summary = f"Found {len(campaigns)} campaigns:\n\n"
    
#     for i, campaign in enumerate(campaigns[:10], 1):  # Show max 10
#         summary += f"{i}. **{campaign.get('name')}** ({campaign.get('status')})\n"
#         summary += f"   - Objective: {campaign.get('objective', 'N/A')}\n"
    
#     if len(campaigns) > 10:
#         summary += f"\n... and {len(campaigns) - 10} more campaigns"
    
#     return summary


# def format_adset_summary(adsets: List[Dict[str, Any]]) -> str:
#     """
#     Format adset list for display
    
#     Args:
#         adsets: List of adsets
        
#     Returns:
#         Formatted string
#     """
#     if not adsets:
#         return "No adsets found"
    
#     summary = f"Found {len(adsets)} adsets:\n\n"
    
#     for i, adset in enumerate(adsets[:10], 1):  # Show max 10
#         summary += f"{i}. **{adset.get('name')}** ({adset.get('status')})\n"
#         summary += f"   - Optimization: {adset.get('optimization_goal', 'N/A')}\n"
    
#     if len(adsets) > 10:
#         summary += f"\n... and {len(adsets) - 10} more adsets"
    
#     return summary


# def format_ad_summary(ads: List[Dict[str, Any]]) -> str:
#     """
#     Format ad list for display
    
#     Args:
#         ads: List of ads
        
#     Returns:
#         Formatted string
#     """
#     if not ads:
#         return "No ads found"
    
#     summary = f"Found {len(ads)} ads:\n\n"
    
#     for i, ad in enumerate(ads[:10], 1):  # Show max 10
#         summary += f"{i}. **{ad.get('name')}** ({ad.get('status')})\n"
#         if ad.get('creative'):
#             summary += f"   - Preview: {ad['creative'].get('body', 'N/A')[:50]}...\n"
    
#     if len(ads) > 10:
#         summary += f"\n... and {len(ads) - 10} more ads"
    
#     return summary