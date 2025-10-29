# file: internal_api_caller.py
"""
Internal API Caller – zero-overhead direct call to main.py endpoint functions.
Used by chat/agent workers running in the same process.
"""

import logging
from datetime import datetime
from typing import Any, Dict

logger = logging.getLogger(__name__)


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
            get_meta_ad_accounts,
            get_account_insights_summary,   # renamed
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

            # Facebook Page
            get_meta_pages,
            get_meta_page_insights,
            get_meta_page_insights_timeseries,
            get_meta_page_posts,
            get_meta_page_posts_timeseries,
            get_meta_video_views_breakdown,
            get_meta_content_type_breakdown,
            get_meta_follows_unfollows,
            get_meta_organic_vs_paid,
            get_meta_page_demographics,
            get_meta_engagement_breakdown,

            # Instagram
            get_meta_instagram_accounts,
            get_meta_instagram_insights,
            get_meta_instagram_insights_timeseries,
            get_meta_instagram_media,
            get_meta_instagram_media_timeseries,
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
            result = await get_keyword_insights(
                customer_id=params["customer_id"],
                request_data=body,                # KeywordInsightRequest
                current_user=current_user,
            )

        # ================================================================== #
        # META ADS
        # ================================================================== #
        elif endpoint_path == "/api/meta/ad-accounts":
            result = await get_meta_ad_accounts(current_user=current_user)

        elif "/ad-accounts/" in endpoint_path and "/insights/summary" in endpoint_path:
            result = await get_account_insights_summary(
                account_id=params["account_id"],
                period=params.get("period"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif "/ad-accounts/" in endpoint_path and "/campaigns/list" in endpoint_path:
            result = await get_campaigns_list(
                account_id=params["account_id"],
                status=params.get("status"),
                current_user=current_user,
            )

        elif endpoint_path == "/api/meta/campaigns/timeseries":
            result = await get_campaigns_timeseries(
                campaign_ids=body.get("campaign_ids", []),
                period=params.get("period"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

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
            result = await get_adsets_by_campaigns(
                campaign_ids=body.get("campaign_ids", []),
                period=params.get("period"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif endpoint_path == "/api/meta/adsets/timeseries":
            result = await get_adsets_timeseries(
                adset_ids=body.get("adset_ids", []),
                period=params.get("period"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

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
        # FACEBOOK PAGE
        # ================================================================== #
        elif endpoint_path == "/api/meta/pages":
            result = await get_meta_pages(current_user=current_user)

        elif "/pages/" in endpoint_path and "/insights/timeseries" in endpoint_path:
            result = await get_meta_page_insights_timeseries(
                page_id=params["page_id"],
                period=params.get("period"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif "/pages/" in endpoint_path and "/insights" in endpoint_path and "/timeseries" not in endpoint_path:
            result = await get_meta_page_insights(
                page_id=params["page_id"],
                period=params.get("period"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif "/pages/" in endpoint_path and "/posts/timeseries" in endpoint_path:
            result = await get_meta_page_posts_timeseries(
                page_id=params["page_id"],
                limit=params.get("limit", 10),
                period=params.get("period"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif "/pages/" in endpoint_path and "/posts" in endpoint_path and "/timeseries" not in endpoint_path:
            result = await get_meta_page_posts(
                page_id=params["page_id"],
                limit=params.get("limit", 10),
                period=params.get("period"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif "/video-views-breakdown" in endpoint_path:
            result = await get_meta_video_views_breakdown(
                page_id=params["page_id"],
                period=params.get("period"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif "/content-type-breakdown" in endpoint_path:
            result = await get_meta_content_type_breakdown(
                page_id=params["page_id"],
                period=params.get("period"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif "/follows-unfollows" in endpoint_path:
            result = await get_meta_follows_unfollows(
                page_id=params["page_id"],
                period=params.get("period"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif "/organic-vs-paid" in endpoint_path:
            result = await get_meta_organic_vs_paid(
                page_id=params["page_id"],
                period=params.get("period"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif "/pages/" in endpoint_path and "/demographics" in endpoint_path:
            result = await get_meta_page_demographics(
                page_id=params["page_id"],
                current_user=current_user,
            )

        elif "/pages/" in endpoint_path and "/engagement-breakdown" in endpoint_path:
            result = await get_meta_engagement_breakdown(
                page_id=params["page_id"],
                period=params.get("period"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        # ================================================================== #
        # INSTAGRAM
        # ================================================================== #
        elif endpoint_path == "/api/meta/instagram/accounts":
            result = await get_meta_instagram_accounts(current_user=current_user)

        elif "/instagram/" in endpoint_path and "/insights/timeseries" in endpoint_path:
            result = await get_meta_instagram_insights_timeseries(
                account_id=params["account_id"],
                period=params.get("period"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif "/instagram/" in endpoint_path and "/insights" in endpoint_path and "/timeseries" not in endpoint_path:
            result = await get_meta_instagram_insights(
                account_id=params["account_id"],
                period=params.get("period"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif "/instagram/" in endpoint_path and "/media/timeseries" in endpoint_path:
            result = await get_meta_instagram_media_timeseries(
                account_id=params["account_id"],
                limit=params.get("limit", 10),
                period=params.get("period"),
                start_date=params.get("start_date"),
                end_date=params.get("end_date"),
                current_user=current_user,
            )

        elif "/instagram/" in endpoint_path and "/media" in endpoint_path and "/timeseries" not in endpoint_path:
            result = await get_meta_instagram_media(
                account_id=params["account_id"],
                limit=params.get("limit", 10),
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
        logger.error("Internal call % %s failed: %s", endpoint_name, exc, exc_info=True)

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