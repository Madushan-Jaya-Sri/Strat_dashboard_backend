# file: internal_api_caller.py
"""
Internal API Caller – zero-overhead direct call to main.py endpoint functions.
Used by chat/agent workers running in the same process.
"""

import logging
from datetime import datetime
from typing import Any, Dict

logger = logging.getLogger(__name__)


class InternalAPICaller:
    """
    Wrapper class for making internal API calls within the same process.
    Used by agents to call endpoints without HTTP overhead.
    """
    
    def __init__(self, auth_token: str, user_email: str):
        self.auth_token = auth_token
        self.user_email = user_email
        self.current_user = {
            "email": user_email,
            "token": auth_token
        }
    
    async def call_get_endpoint(
        self,
        endpoint_path: str,
        params: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Call a GET endpoint
        
        Args:
            endpoint_path: The API path (e.g., "/api/ads/customers")
            params: Query parameters
            
        Returns:
            API response data
        """
        if params is None:
            params = {}
        
        # Extract path parameters (e.g., {account_id})
        import re
        path_params = re.findall(r'\{(\w+)\}', endpoint_path)
        
        # Replace path parameters
        for param in path_params:
            if param in params:
                endpoint_path = endpoint_path.replace(f"{{{param}}}", str(params[param]))
        
        result = await call_internal_endpoint(
            endpoint_name=endpoint_path.split('/')[-1],
            endpoint_path=endpoint_path,
            method="GET",
            params=params,
            current_user=self.current_user
        )
        
        if result.get("success"):
            return result.get("data")
        else:
            raise Exception(result.get("error", "API call failed"))
    
    async def call_post_endpoint(
        self,
        endpoint_path: str,
        body: Any = None,
        params: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Call a POST endpoint
        
        Args:
            endpoint_path: The API path
            body: Request body (list or dict)
            params: Query parameters
            
        Returns:
            API response data
        """
        if params is None:
            params = {}
        
        # Add body to params
        params["body"] = body if body is not None else {}
        
        result = await call_internal_endpoint(
            endpoint_name=endpoint_path.split('/')[-1],
            endpoint_path=endpoint_path,
            method="POST",
            params=params,
            current_user=self.current_user
        )
        
        if result.get("success"):
            return result.get("data")
        else:
            raise Exception(result.get("error", "API call failed"))
    
    def call_get_endpoint_sync(self, endpoint_path: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Synchronous version of call_get_endpoint (for non-async contexts)"""
        import asyncio
        try:
            # Try to get the current event loop
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # We're in an async context but need sync behavior
                # This shouldn't happen in production, but handle it gracefully
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    future = pool.submit(asyncio.run, self.call_get_endpoint(endpoint_path, params))
                    return future.result()
            else:
                # No loop running, safe to use asyncio.run
                return asyncio.run(self.call_get_endpoint(endpoint_path, params))
        except RuntimeError:
            # No event loop exists, create one
            return asyncio.run(self.call_get_endpoint(endpoint_path, params))
    
    def call_post_endpoint_sync(self, endpoint_path: str, body: Any = None, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Synchronous version of call_post_endpoint (for non-async contexts)"""
        import asyncio
        try:
            # Try to get the current event loop
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # We're in an async context but need sync behavior
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    future = pool.submit(asyncio.run, self.call_post_endpoint(endpoint_path, body, params))
                    return future.result()
            else:
                # No loop running, safe to use asyncio.run
                return asyncio.run(self.call_post_endpoint(endpoint_path, body, params))
        except RuntimeError:
            # No event loop exists, create one
            return asyncio.run(self.call_post_endpoint(endpoint_path, body, params))


async def call_internal_endpoint(
    endpoint_name: str,
    endpoint_path: str,
    method: str,
    params: Dict[str, Any],
    current_user: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Dispatch to the exact function defined in main.py based on the FastAPI path.
    """
    start = datetime.utcnow()

    try:
        logger.info("Internal call: %s → %s", endpoint_name, endpoint_path)

        # ------------------------------------------------------------------ #
        # Late import – only real functions from main.py
        # ------------------------------------------------------------------ #
        from main import (
            # Google Ads
            get_ads_customers,
            get_ads_key_stats,
            get_ads_campaigns,
            get_ads_keywords,
            get_keyword_ideas,
            get_ads_performance,
            get_ads_geographic,
            get_ads_device_performance,
            get_ads_time_performance,

            # GA4
            get_ga_properties,
            get_ga_metrics,               # renamed from get_metrics
            get_ga_traffic_sources,       # renamed
            get_ga_top_pages,             # renamed
            get_ga_conversions,           # renamed
            generate_engagement_funnel_with_llm,  # the POST funnel endpoint
            get_ga_channel_performance,
            get_ga_audience_insights,
            get_ga_time_series,
            get_ga_trends,
            get_ga_roas_roi_time_series,
            get_revenue_breakdown_by_channel,
            get_revenue_breakdown_by_source,
            get_revenue_breakdown_by_device,

            # Intent
            get_keyword_insights,         # the POST endpoint

            # Meta Ads
            get_account_insights_summary,
            get_campaigns_list,
            get_campaigns_timeseries,
            get_campaigns_demographics,
            get_campaigns_placements,
            get_adsets_by_campaigns,
            get_adsets_timeseries,
            get_adsets_demographics,
            get_adsets_placements,
            get_ads_by_adsets,
            get_ads_timeseries,
            get_ads_demographics,
            get_ads_placements,
        )

        result = None
        body = params.get("body", {})

        # ================================================================== #
        # GOOGLE ADS
        # ================================================================== #
        if endpoint_path == "/api/ads/customers":
            result = await get_ads_customers(current_user=current_user)

        elif endpoint_path.startswith("/api/ads/key-stats/"):
            result = await get_ads_key_stats(
                customer_id=params["customer_id"],
                period=params.get("period", "LAST_30_DAYS"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif endpoint_path.startswith("/api/ads/campaigns/"):
            result = await get_ads_campaigns(
                customer_id=params["customer_id"],
                period=params.get("period", "LAST_30_DAYS"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif endpoint_path.startswith("/api/ads/keywords/"):
            result = await get_ads_keywords(
                customer_id=params["customer_id"],
                period=params.get("period", "LAST_30_DAYS"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                offset=params.get("offset", 0),
                limit=params.get("limit", 10),
                current_user=current_user,
            )

        elif endpoint_path.startswith("/api/ads/keyword-ideas/"):
            result = await get_keyword_ideas(
                customer_id=params["customer_id"],
                request_data=body,          # whole request body
                current_user=current_user,
            )

        elif endpoint_path.startswith("/api/ads/performance/"):
            result = await get_ads_performance(
                customer_id=params["customer_id"],
                period=params.get("period", "LAST_30_DAYS"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif endpoint_path.startswith("/api/ads/geographic/"):
            result = await get_ads_geographic(
                customer_id=params["customer_id"],
                period=params.get("period", "LAST_30_DAYS"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif endpoint_path.startswith("/api/ads/device-performance/"):
            result = await get_ads_device_performance(
                customer_id=params["customer_id"],
                period=params.get("period", "LAST_30_DAYS"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif endpoint_path.startswith("/api/ads/time-performance/"):
            result = await get_ads_time_performance(
                customer_id=params["customer_id"],
                period=params.get("period", "LAST_30_DAYS"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        # ================================================================== #
        # GA4
        # ================================================================== #
        elif endpoint_path == "/api/analytics/properties":
            result = await get_ga_properties(current_user=current_user)

        elif endpoint_path.startswith("/api/analytics/metrics/"):
            result = await get_ga_metrics(
                property_id=params["property_id"],
                period=params.get("period", "30d"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif endpoint_path.startswith("/api/analytics/traffic-sources/"):
            result = await get_ga_traffic_sources(
                property_id=params["property_id"],
                period=params.get("period", "30d"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif endpoint_path.startswith("/api/analytics/top-pages/"):
            result = await get_ga_top_pages(
                property_id=params["property_id"],
                period=params.get("period", "30d"),
                current_user=current_user,
            )

        elif endpoint_path.startswith("/api/analytics/conversions/"):
            result = await get_ga_conversions(
                property_id=params["property_id"],
                period=params.get("period", "30d"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif endpoint_path.startswith("/api/analytics/funnel/"):
            result = await generate_engagement_funnel_with_llm(
                property_id=params["property_id"],
                request=body,                     # FunnelRequest
                period=params.get("period", "30d"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif endpoint_path.startswith("/api/analytics/channel-performance/"):
            result = await get_ga_channel_performance(
                property_id=params["property_id"],
                period=params.get("period", "30d"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif endpoint_path.startswith("/api/analytics/audience-insights/"):
            result = await get_ga_audience_insights(
                property_id=params["property_id"],
                dimension=params.get("dimension", "city"),
                period=params.get("period", "30d"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif endpoint_path.startswith("/api/analytics/time-series/"):
            result = await get_ga_time_series(
                property_id=params["property_id"],
                metric=params.get("metric", "totalUsers"),
                period=params.get("period", "30d"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif endpoint_path.startswith("/api/analytics/trends/"):
            result = await get_ga_trends(
                property_id=params["property_id"],
                period=params.get("period", "30d"),
                current_user=current_user,
            )

        elif endpoint_path.startswith("/api/analytics/roas-roi-time-series/"):
            result = await get_ga_roas_roi_time_series(
                property_id=params["property_id"],
                period=params.get("period", "30d"),
                current_user=current_user,
            )

        elif endpoint_path.startswith("/api/analytics/revenue-breakdown/channel/"):
            result = await get_revenue_breakdown_by_channel(
                property_id=params["property_id"],
                period=params.get("period", "30d"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif endpoint_path.startswith("/api/analytics/revenue-breakdown/source/"):
            result = await get_revenue_breakdown_by_source(
                property_id=params["property_id"],
                limit=params.get("limit", 20),
                period=params.get("period", "30d"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif endpoint_path.startswith("/api/analytics/revenue-breakdown/device/"):
            result = await get_revenue_breakdown_by_device(
                property_id=params["property_id"],
                period=params.get("period", "30d"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        # ================================================================== #
        # INTENT
        # ================================================================== #
        elif endpoint_path.startswith("/api/intent/keyword-insights/"):
            # Convert body dict to KeywordInsightRequest model
            from models.response_models import KeywordInsightRequest
            request_data = KeywordInsightRequest(**body)

            result = await get_keyword_insights(
                customer_id=params["customer_id"],
                request_data=request_data,
                current_user=current_user,
            )

        # ================================================================== #
        # META ADS
        # ================================================================== #
        elif endpoint_path.startswith("/api/meta/ad-accounts/") and "/insights/summary" in endpoint_path:
            result = await get_account_insights_summary(
                account_id=params["account_id"],
                period=params.get("period"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif endpoint_path.startswith("/api/meta/ad-accounts/") and "/campaigns/list" in endpoint_path:
            result = await get_campaigns_list(
                account_id=params["account_id"],
                status=params.get("status"),
                current_user=current_user,
            )

        elif endpoint_path == "/api/meta/campaigns/timeseries":
            logger.info("=" * 80)
            logger.info("📊 INTERNAL CALLER: Calling get_campaigns_timeseries")
            logger.info(f"   Campaign IDs: {body.get('campaign_ids', [])}")
            logger.info(f"   Period: {params.get('period')}")
            logger.info(f"   Start Date: {params.get('start_date')}")
            logger.info(f"   End Date: {params.get('end_date')}")
            logger.info(f"   User: {current_user.get('email')}")
            logger.info("=" * 80)

            result = await get_campaigns_timeseries(
                campaign_ids=body.get("campaign_ids", []),
                period=params.get("period"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

            logger.info("=" * 80)
            logger.info("✅ INTERNAL CALLER: get_campaigns_timeseries completed")
            logger.info(f"   Result type: {type(result).__name__}")
            logger.info(f"   Result size: {len(str(result))} chars")
            logger.info("=" * 80)

        elif endpoint_path == "/api/meta/campaigns/demographics":
            result = await get_campaigns_demographics(
                campaign_ids=body.get("campaign_ids", []),
                period=params.get("period"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif endpoint_path == "/api/meta/campaigns/placements":
            result = await get_campaigns_placements(
                campaign_ids=body.get("campaign_ids", []),
                period=params.get("period"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif endpoint_path == "/api/meta/campaigns/adsets":
            logger.info("=" * 80)
            logger.info("📊 INTERNAL CALLER: Calling get_adsets_by_campaigns")
            logger.info(f"   Campaign IDs: {body.get('campaign_ids', [])}")
            logger.info(f"   User: {current_user.get('email')}")
            logger.info("=" * 80)

            result = await get_adsets_by_campaigns(
                campaign_ids=body.get("campaign_ids", []),
                period=params.get("period"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

            logger.info("=" * 80)
            logger.info("✅ INTERNAL CALLER: get_adsets_by_campaigns completed")
            logger.info(f"   Result type: {type(result).__name__}")
            if isinstance(result, list):
                logger.info(f"   Adsets returned: {len(result)}")
            logger.info("=" * 80)

        elif endpoint_path == "/api/meta/adsets/timeseries":
            logger.info("=" * 80)
            logger.info("📊 INTERNAL CALLER: Calling get_adsets_timeseries")
            logger.info(f"   Adset IDs: {body.get('adset_ids', [])}")
            logger.info(f"   Period: {params.get('period')}")
            logger.info(f"   Start Date: {params.get('start_date')}")
            logger.info(f"   End Date: {params.get('end_date')}")
            logger.info(f"   User: {current_user.get('email')}")
            logger.info("=" * 80)

            result = await get_adsets_timeseries(
                adset_ids=body.get("adset_ids", []),
                period=params.get("period"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

            logger.info("=" * 80)
            logger.info("✅ INTERNAL CALLER: get_adsets_timeseries completed")
            logger.info(f"   Result type: {type(result).__name__}")
            logger.info(f"   Result size: {len(str(result))} chars")
            logger.info("=" * 80)

        elif endpoint_path == "/api/meta/adsets/demographics":
            result = await get_adsets_demographics(
                adset_ids=body.get("adset_ids", []),
                period=params.get("period"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif endpoint_path == "/api/meta/adsets/placements":
            result = await get_adsets_placements(
                adset_ids=body.get("adset_ids", []),
                period=params.get("period"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif endpoint_path == "/api/meta/adsets/ads":
            result = await get_ads_by_adsets(
                adset_ids=body.get("adset_ids", []),
                current_user=current_user,
            )

        elif endpoint_path == "/api/meta/ads/timeseries":
            result = await get_ads_timeseries(
                ad_ids=body.get("ad_ids", []),
                period=params.get("period"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif endpoint_path == "/api/meta/ads/demographics":
            result = await get_ads_demographics(
                ad_ids=body.get("ad_ids", []),
                period=params.get("period"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif endpoint_path == "/api/meta/ads/placements":
            result = await get_ads_placements(
                ad_ids=body.get("ad_ids", []),
                period=params.get("period"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        # ================================================================== #
        # FALLBACK
        # ================================================================== #
        else:
            raise ValueError(f"Unsupported endpoint_path: {endpoint_path}")

        # ------------------------------------------------------------------ #
        # Normalize result
        # ------------------------------------------------------------------ #
        if result is None:
            data = None
        elif hasattr(result, "model_dump"):
            data = result.model_dump()
        elif hasattr(result, "__dict__"):
            data = result.__dict__
        elif isinstance(result, (list, tuple)):
            data = [
                r.model_dump() if hasattr(r, "model_dump")
                else r.__dict__ if hasattr(r, "__dict__")
                else r
                for r in result
            ]
        else:
            data = result

        elapsed = (datetime.utcnow() - start).total_seconds()
        logger.info("Internal call %s completed in %.2fs", endpoint_name, elapsed)

        return {
            "success": True,
            "endpoint": endpoint_name,
            "path": endpoint_path,
            "method": method,
            "params": params,
            "data": data,
            "status_code": 200,
            "response_time": elapsed,
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as exc:
        elapsed = (datetime.utcnow() - start).total_seconds()

        logger.error("=" * 80)
        logger.error(f"❌ INTERNAL CALLER: Call to {endpoint_name} failed")
        logger.error(f"   Endpoint path: {endpoint_path}")
        logger.error(f"   Method: {method}")
        logger.error(f"   Error type: {type(exc).__name__}")
        logger.error(f"   Error message: {str(exc)}")
        logger.error(f"   Params: {params}")
        logger.error(f"   Response time: {elapsed:.2f}s")
        logger.error("=" * 80)
        logger.error("   Full traceback:", exc_info=True)

        return {
            "success": False,
            "endpoint": endpoint_name,
            "path": endpoint_path,
            "method": method,
            "params": params,
            "error": str(exc),
            "status_code": 500,
            "response_time": elapsed,
            "timestamp": datetime.utcnow().isoformat(),
        }