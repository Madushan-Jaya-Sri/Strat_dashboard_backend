"""
Google Analytics 4 Manager
Handles all GA4 API operations
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from fastapi import HTTPException
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest,
    DateRange,
    Dimension,
    Metric,
    OrderBy
)
from googleapiclient.discovery import build

from google_ads.ads_manager import GoogleAdsManager

logger = logging.getLogger(__name__)

class GA4Manager:
    """Manager class for Google Analytics 4 API operations"""
    
    def __init__(self, user_email: str):
        self.user_email = user_email
        
        # Get auth manager instance
        from main import auth_manager
        self.auth_manager = auth_manager
        self._client = None
    
    @property
    def client(self) -> BetaAnalyticsDataClient:
        """Get or create GA4 client"""
        if not self._client:
            try:
                credentials = self.auth_manager.get_user_credentials(self.user_email)
                self._client = BetaAnalyticsDataClient(credentials=credentials)
                logger.info(f"GA4 client created for {self.user_email}")
            except Exception as e:
                logger.error(f"Failed to create GA4 client: {e}")
                raise HTTPException(status_code=500, detail=f"GA4 API client initialization error: {str(e)}")
        
        return self._client
    
    def get_date_range(self, period: str):
        """Get date range based on period"""
        end_date = datetime.now()
        
        if period == "7d":
            start_date = end_date - timedelta(days=7)
        elif period == "90d":
            start_date = end_date - timedelta(days=90)
        elif period in ["365d", "12m"]:  # Support both formats
            start_date = end_date - timedelta(days=365)
        else:  # default 30d
            start_date = end_date - timedelta(days=30)
        
        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
    
    def safe_float(self, value, default=0.0):
        """Safely convert to float"""
        try:
            return float(value) if value else default
        except (ValueError, TypeError):
            return default
    
    def safe_int(self, value, default=0):
        """Safely convert to int"""
        try:
            return int(float(value)) if value else default
        except (ValueError, TypeError):
            return default
    
    def get_user_properties(self) -> List[Dict[str, Any]]:
        """Get all GA4 properties the user has access to"""
        try:
            credentials = self.auth_manager.get_user_credentials(self.user_email)
            admin_service = build('analyticsadmin', 'v1alpha', credentials=credentials)
            
            all_properties = []
            
            try:
                # Direct properties list
                properties_response = admin_service.properties().list().execute()
                properties = properties_response.get('properties', [])
                
                for prop in properties:
                    property_id = prop['name'].split('/')[-1]
                    property_display_name = prop.get('displayName', 'Unknown Property')
                    
                    all_properties.append({
                        'propertyId': property_id,
                        'displayName': property_display_name,
                        'websiteUrl': prop.get('websiteUrl', '')
                    })
                    
            except Exception as direct_error:
                logger.warning(f"Direct properties list failed: {direct_error}")
                
                # Fallback: account-based approach
                try:
                    accounts_response = admin_service.accounts().list().execute()
                    accounts = accounts_response.get('accounts', [])
                    
                    for account in accounts:
                        account_name = account['name']
                        
                        try:
                            properties_response = admin_service.properties().list(
                                filter=f"parent:{account_name}"
                            ).execute()
                            
                            properties = properties_response.get('properties', [])
                            
                            for prop in properties:
                                property_id = prop['name'].split('/')[-1]
                                property_display_name = prop.get('displayName', 'Unknown Property')
                                
                                all_properties.append({
                                    'propertyId': property_id,
                                    'displayName': property_display_name,
                                    'websiteUrl': prop.get('websiteUrl', '')
                                })
                                
                        except Exception as account_error:
                            logger.warning(f"Could not access properties for account: {account_error}")
                            continue
                            
                except Exception as fallback_error:
                    logger.error(f"Account-based approach also failed: {fallback_error}")
            
            logger.info(f"Found {len(all_properties)} accessible properties for {self.user_email}")
            return all_properties
            
        except Exception as e:
            logger.error(f"Error fetching properties: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch properties: {str(e)}")
    
    def get_metrics(self, property_id: str, period: str = "30d") -> Dict[str, Any]:
        """Get GA4 metrics for a specific property with dashboard insights"""
        try:
            start_date, end_date = self.get_date_range(period)
            
            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                metrics=[
                    Metric(name="totalUsers"),
                    Metric(name="sessions"),
                    Metric(name="engagedSessions"),
                    Metric(name="engagementRate"),
                    Metric(name="userEngagementDuration"),
                    Metric(name="bounceRate"),
                    Metric(name="screenPageViewsPerSession"),
                ],
            )
            
            response = self.client.run_report(request=request)
            
            if response.rows:
                row = response.rows[0]
                metrics = row.metric_values
                
                # Basic calculations
                total_users = self.safe_int(metrics[0].value)
                sessions = self.safe_int(metrics[1].value)
                engaged_sessions = self.safe_int(metrics[2].value)
                engagement_rate = self.safe_float(metrics[3].value) * 100
                total_duration = self.safe_float(metrics[4].value)
                avg_session_duration = total_duration / sessions if sessions > 0 else 0
                bounce_rate = self.safe_float(metrics[5].value) * 100
                pages_per_session = self.safe_float(metrics[6].value)
                
                # Get previous period data for comparison
                previous_metrics = self.get_previous_period_metrics(property_id, period)
                
                # Calculate additional insights
                user_change = self.calculate_percentage_change(total_users, previous_metrics.get('totalUsers', 0))
                sessions_per_user = round(sessions / total_users, 1) if total_users > 0 else 0
                engaged_percentage = round((engaged_sessions / sessions) * 100) if sessions > 0 else 0
                engagement_status = self.get_engagement_status(engagement_rate)
                duration_quality = self.get_duration_quality(avg_session_duration)
                bounce_status = self.get_bounce_status(bounce_rate)
                content_depth_status = self.get_content_depth_status(pages_per_session)
                views_per_session = pages_per_session  # Same as pagesPerSession
                session_quality_score = self.calculate_session_quality_score(engagement_rate, avg_session_duration, bounce_rate, pages_per_session)
                
                return {
                    'propertyId': property_id,
                    'propertyName': f"Property {property_id}",
                    # Original 7 GA4 metrics
                    'totalUsers': total_users,
                    'sessions': sessions,
                    'engagedSessions': engaged_sessions,
                    'engagementRate': round(engagement_rate, 2),
                    'averageSessionDuration': round(avg_session_duration, 2),
                    'bounceRate': round(bounce_rate, 2),
                    'pagesPerSession': round(pages_per_session, 2),
                    # Additional 9 calculated insights
                    'totalUsersChange': f"{'+' if user_change > 0 else ''}{user_change:.1f}%",
                    'sessionsPerUser': sessions_per_user,
                    'engagedSessionsPercentage': f"{engaged_percentage}%",
                    'engagementRateStatus': engagement_status,
                    'sessionDurationQuality': duration_quality,
                    'bounceRateStatus': bounce_status,
                    'contentDepthStatus': content_depth_status,
                    'viewsPerSession': round(views_per_session, 2),
                    'sessionQualityScore': session_quality_score
                }
            
            # Return default values if no data
            return {
                'propertyId': property_id,
                'propertyName': f"Property {property_id}",
                # Original 7 GA4 metrics
                'totalUsers': 0,
                'sessions': 0,
                'engagedSessions': 0,
                'engagementRate': 0.0,
                'averageSessionDuration': 0.0,
                'bounceRate': 0.0,
                'pagesPerSession': 0.0,
                # Additional 9 calculated insights
                'totalUsersChange': "0%",
                'sessionsPerUser': 0.0,
                'engagedSessionsPercentage': "0%",
                'engagementRateStatus': "No Data",
                'sessionDurationQuality': "No Data",
                'bounceRateStatus': "No Data",
                'contentDepthStatus': "No Data",
                'viewsPerSession': 0.0,
                'sessionQualityScore': "0/100"
            }
            
        except Exception as e:
            logger.error(f"Error fetching metrics for property {property_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch metrics: {str(e)}")

    def get_previous_period_metrics(self, property_id: str, period: str) -> Dict[str, Any]:
        """Get metrics from previous period for comparison"""
        try:
            # Calculate previous period dates
            if period == "7d":
                previous_period = "14d"  # Get 14 days ago to 7 days ago
                end_date = datetime.now() - timedelta(days=7)
                start_date = end_date - timedelta(days=7)
            elif period == "90d":
                previous_period = "180d"  # Get 180 days ago to 90 days ago
                end_date = datetime.now() - timedelta(days=90)
                start_date = end_date - timedelta(days=90)
            elif period == "365d":
                previous_period = "730d"  # Get 730 days ago to 365 days ago
                end_date = datetime.now() - timedelta(days=365)
                start_date = end_date - timedelta(days=365)
            else:  # 30d default
                previous_period = "60d"  # Get 60 days ago to 30 days ago
                end_date = datetime.now() - timedelta(days=30)
                start_date = end_date - timedelta(days=30)
            
            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(
                    start_date=start_date.strftime("%Y-%m-%d"), 
                    end_date=end_date.strftime("%Y-%m-%d")
                )],
                metrics=[Metric(name="totalUsers")],
            )
            
            response = self.client.run_report(request=request)
            
            if response.rows:
                return {'totalUsers': self.safe_int(response.rows[0].metric_values[0].value)}
            return {'totalUsers': 0}
            
        except Exception:
            return {'totalUsers': 0}  # Fallback if comparison fails

    def calculate_percentage_change(self, current: float, previous: float) -> float:
        """Calculate percentage change between current and previous values"""
        if previous == 0:
            return 0.0
        return ((current - previous) / previous) * 100

    def get_engagement_status(self, engagement_rate: float) -> str:
        """Get engagement rate status"""
        if engagement_rate >= 50:
            return "Excellent"
        elif engagement_rate >= 35:
            return "Above Average"
        elif engagement_rate >= 20:
            return "Average"
        else:
            return "Below Average"

    def get_duration_quality(self, duration: float) -> str:
        """Get session duration quality assessment"""
        if duration >= 120:  # 2 minutes
            return "Excellent"
        elif duration >= 60:  # 1 minute
            return "Good"
        elif duration >= 30:  # 30 seconds
            return "Average"
        else:
            return "Needs Improvement"

    def get_bounce_status(self, bounce_rate: float) -> str:
        """Get bounce rate status"""
        if bounce_rate <= 25:
            return "Excellent"
        elif bounce_rate <= 40:
            return "Good"
        elif bounce_rate <= 55:
            return "Average"
        elif bounce_rate <= 70:
            return "High"
        else:
            return "Very High"

    def get_content_depth_status(self, pages_per_session: float) -> str:
        """Get content exploration depth status"""
        if pages_per_session >= 4:
            return "Deep"
        elif pages_per_session >= 2.5:
            return "Good"
        elif pages_per_session >= 1.5:
            return "Moderate"
        else:
            return "Shallow"

    def calculate_session_quality_score(self, engagement_rate: float, duration: float, bounce_rate: float, pages_per_session: float) -> str:
        """Calculate overall session quality score (0-100)"""
        # Weighted scoring system
        engagement_score = min(engagement_rate * 0.8, 30)  # Max 30 points
        duration_score = min(duration / 4, 25)              # Max 25 points (100 seconds = 25 points)
        depth_score = min(pages_per_session * 10, 25)       # Max 25 points
        bounce_penalty = max(0, 20 - (bounce_rate / 5))     # Max 20 points (less bounce = more points)
        
        total_score = engagement_score + duration_score + depth_score + bounce_penalty
        final_score = min(int(total_score), 100)
        
        return f"{final_score}/100"


    def get_traffic_sources(self, property_id: str, period: str = "30d") -> List[Dict[str, Any]]:
        """Get traffic source data for a specific property"""
        try:
            start_date, end_date = self.get_date_range(period)
            
            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimensions=[Dimension(name="sessionDefaultChannelGrouping")],
                metrics=[
                    Metric(name="sessions"),
                    Metric(name="totalUsers"),
                ],
                order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="sessions"), desc=True)],
                limit=10
            )
            
            response = self.client.run_report(request=request)
            
            total_sessions = sum(self.safe_int(row.metric_values[0].value) for row in response.rows)
            
            sources = []
            for row in response.rows:
                sessions = self.safe_int(row.metric_values[0].value)
                users = self.safe_int(row.metric_values[1].value)
                percentage = (sessions / total_sessions * 100) if total_sessions > 0 else 0
                
                sources.append({
                    'channel': row.dimension_values[0].value,
                    'sessions': sessions,
                    'users': users,
                    'percentage': round(percentage, 2)
                })
            
            return sources
            
        except Exception as e:
            logger.error(f"Error fetching traffic sources for property {property_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch traffic sources: {str(e)}")
    
    def get_top_pages(self, property_id: str, period: str = "30d") -> List[Dict[str, Any]]:
        """Get top pages data for a specific property"""
        try:
            start_date, end_date = self.get_date_range(period)
            
            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimensions=[
                    Dimension(name="pageTitle"),
                    Dimension(name="pagePath")
                ],
                metrics=[
                    Metric(name="screenPageViews"),
                    Metric(name="userEngagementDuration"),
                    Metric(name="bounceRate"),
                ],
                order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="screenPageViews"), desc=True)],
                limit=10
            )
            
            response = self.client.run_report(request=request)
            
            pages = []
            for row in response.rows:
                page_views = self.safe_int(row.metric_values[0].value)
                engagement_duration = self.safe_float(row.metric_values[1].value)
                avg_time = engagement_duration / page_views if page_views > 0 else 0
                
                pages.append({
                    'title': row.dimension_values[0].value,
                    'path': row.dimension_values[1].value,
                    'pageViews': page_views,
                    'uniquePageViews': page_views,
                    'avgTimeOnPage': avg_time,
                    'bounceRate': self.safe_float(row.metric_values[2].value) * 100
                })
            
            return pages
            
        except Exception as e:
            logger.error(f"Error fetching top pages for property {property_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch top pages: {str(e)}")
    
    def get_conversions(self, property_id: str, period: str = "30d") -> List[Dict[str, Any]]:
        """Get conversion data for a specific property"""
        try:
            start_date, end_date = self.get_date_range(period)
            
            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimensions=[Dimension(name="eventName")],
                metrics=[
                    Metric(name="eventCount"),
                    Metric(name="conversions"),
                    Metric(name="totalRevenue"),
                ],
                order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="eventCount"), desc=True)],
                limit=15
            )
            
            response = self.client.run_report(request=request)
            
            # Get total sessions for rate calculation
            total_sessions_request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                metrics=[Metric(name="sessions")]
            )
            
            total_sessions_response = self.client.run_report(total_sessions_request)
            total_sessions = 1
            if total_sessions_response.rows:
                total_sessions = self.safe_int(total_sessions_response.rows[0].metric_values[0].value)
            
            conversions_list = []
            for row in response.rows:
                event_name = row.dimension_values[0].value
                event_count = self.safe_int(row.metric_values[0].value)
                conversions_count = self.safe_int(row.metric_values[1].value)
                revenue = self.safe_float(row.metric_values[2].value)
                
                conversion_rate = (conversions_count / total_sessions * 100) if total_sessions > 0 else 0
                event_count_rate = (event_count / total_sessions) if total_sessions > 0 else 0
                
                conversions_list.append({
                    'eventName': event_name,
                    'conversions': conversions_count,
                    'conversionRate': conversion_rate,
                    'conversionValue': revenue,
                    'eventCount': event_count,
                    'eventCountRate': event_count_rate
                })
            
            return conversions_list
            
        except Exception as e:
            logger.error(f"Error fetching conversions for property {property_id}: {e}")
            return []
    # def get_roas_roi_metrics(self, property_id: str, period: str = "30d") -> Dict[str, Any]:
    #     """Get ROAS and ROI metrics for a specific property with additional ecommerce metrics"""
    #     try:
    #         start_date, end_date = self.get_date_range(period)
            
    #         # First request: Get basic revenue and user data
    #         request = RunReportRequest(
    #             property=f"properties/{property_id}",
    #             date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
    #             metrics=[
    #                 Metric(name="totalRevenue"),
    #                 Metric(name="purchaseRevenue"),
    #                 Metric(name="totalAdRevenue"),
    #                 Metric(name="conversions"),
    #                 Metric(name="sessions"),
    #                 Metric(name="totalUsers"),
    #                 Metric(name="activeUsers"),
    #                 Metric(name="totalPurchasers"),
    #                 Metric(name="eventCount")
    #             ],
    #         )
            
    #         response = self.client.run_report(request=request)
            
    #         # Second request: Get first-time purchasers data
    #         first_time_request = RunReportRequest(
    #             property=f"properties/{property_id}",
    #             date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
    #             dimensions=[Dimension(name="newVsReturning")],
    #             metrics=[
    #                 Metric(name="totalPurchasers"),
    #                 Metric(name="purchaseRevenue")
    #             ],
    #         )
            
    #         first_time_response = self.client.run_report(first_time_request)
            
    #         if response.rows:
    #             row = response.rows[0]
    #             metrics = row.metric_values
                
    #             total_revenue = self.safe_float(metrics[0].value)
    #             purchase_revenue = self.safe_float(metrics[1].value)
    #             total_ad_revenue = self.safe_float(metrics[2].value)
    #             conversions = self.safe_float(metrics[3].value)
    #             sessions = self.safe_int(metrics[4].value)
    #             total_users = self.safe_int(metrics[5].value)
    #             active_users = self.safe_int(metrics[6].value)
    #             total_purchasers = self.safe_int(metrics[7].value)
                
    #             # Process first-time purchasers data
    #             first_time_purchasers = 0
    #             if first_time_response.rows:
    #                 for row in first_time_response.rows:
    #                     user_type = row.dimension_values[0].value
    #                     purchasers = self.safe_int(row.metric_values[0].value)
    #                     if user_type == "new":
    #                         first_time_purchasers = purchasers
                
    #             # Calculate metrics
    #             estimated_ad_cost = total_revenue * 0.25  # Assuming 25% ad spend ratio
    #             roas = (total_revenue / estimated_ad_cost) if estimated_ad_cost > 0 else 0
    #             roi = ((total_revenue - estimated_ad_cost) / estimated_ad_cost * 100) if estimated_ad_cost > 0 else 0
                
    #             cost_per_conversion = estimated_ad_cost / conversions if conversions > 0 else 0
    #             revenue_per_user = total_revenue / total_users if total_users > 0 else 0
    #             profit_margin = ((total_revenue - estimated_ad_cost) / total_revenue * 100) if total_revenue > 0 else 0
                
    #             # New calculations
    #             average_purchase_revenue_per_active_user = purchase_revenue / active_users if active_users > 0 else 0
                
    #             return {
    #                 'propertyId': property_id,
    #                 'propertyName': f"Property {property_id}",
    #                 # Original metrics
    #                 'totalRevenue': round(total_revenue, 2),
    #                 'adSpend': round(estimated_ad_cost, 2),
    #                 'roas': round(roas, 2),
    #                 'roi': round(roi, 2),
    #                 'conversionValue': round(purchase_revenue, 2),
    #                 'costPerConversion': round(cost_per_conversion, 2),
    #                 'revenuePerUser': round(revenue_per_user, 2),
    #                 'profitMargin': round(profit_margin, 2),
    #                 'roasStatus': self.get_roas_status(roas),
    #                 'roiStatus': self.get_roi_status(roi),
    #                 'conversions': int(conversions),
    #                 'sessions': sessions,
    #                 'totalUsers': total_users,
    #                 # New ecommerce metrics
    #                 'totalAdRevenue': round(total_ad_revenue, 2),
    #                 'totalPurchasers': total_purchasers,
    #                 'firstTimePurchasers': first_time_purchasers,
    #                 'averagePurchaseRevenuePerActiveUser': round(average_purchase_revenue_per_active_user, 2),
    #                 'activeUsers': active_users
    #             }
            
    #         return self.get_default_roas_roi_metrics(property_id)
            
    #     except Exception as e:
    #         logger.error(f"Error fetching ROAS/ROI metrics for property {property_id}: {e}")
    #         # Return default data instead of raising exception
    #         return self.get_default_roas_roi_metrics(property_id)


    def get_combined_roas_roi_metrics(self, property_id: str, ads_customer_id: str, period: str = "30d") -> Dict[str, Any]:
        """Get ROAS and ROI metrics combining GA4 data with real Google Ads spend"""
        try:
            start_date, end_date = self.get_date_range(period)
            
            # Get GA4 data
            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                metrics=[
                    Metric(name="totalRevenue"),
                    Metric(name="purchaseRevenue"),
                    Metric(name="totalAdRevenue"),
                    Metric(name="conversions"),
                    Metric(name="sessions"),
                    Metric(name="totalUsers"),
                    Metric(name="activeUsers"),
                    Metric(name="totalPurchasers"),
                    Metric(name="eventCount")
                ],
            )
            
            response = self.client.run_report(request=request)
            
            # Get first-time purchasers data
            first_time_request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimensions=[Dimension(name="newVsReturning")],
                metrics=[
                    Metric(name="totalPurchasers"),
                    Metric(name="purchaseRevenue")
                ],
            )
            
            first_time_response = self.client.run_report(first_time_request)
            
            # Get real ad spend from Google Ads
            try:
                ads_manager = GoogleAdsManager(self.user_email, self.auth_manager)
                # Convert GA4 period format to Google Ads format
                ads_period = self.convert_ga_period_to_ads_period(period)
                actual_ad_cost = ads_manager.get_total_cost_for_period(ads_customer_id, ads_period)
            except Exception as ads_error:
                logger.warning(f"Could not fetch Google Ads data: {ads_error}")
                actual_ad_cost = 0.0
            
            if response.rows:
                row = response.rows[0]
                metrics = row.metric_values
                
                total_revenue = self.safe_float(metrics[0].value)
                purchase_revenue = self.safe_float(metrics[1].value)
                total_ad_revenue = self.safe_float(metrics[2].value)
                conversions = self.safe_float(metrics[3].value)
                sessions = self.safe_int(metrics[4].value)
                total_users = self.safe_int(metrics[5].value)
                active_users = self.safe_int(metrics[6].value)
                total_purchasers = self.safe_int(metrics[7].value)
                
                # Process first-time purchasers data
                first_time_purchasers = 0
                if first_time_response.rows:
                    for row in first_time_response.rows:
                        user_type = row.dimension_values[0].value
                        purchasers = self.safe_int(row.metric_values[0].value)
                        if user_type == "new":
                            first_time_purchasers = purchasers
                
                # Calculate metrics with real ad spend
                roas = (total_revenue / actual_ad_cost) if actual_ad_cost > 0 else 0
                roi = ((total_revenue - actual_ad_cost) / actual_ad_cost * 100) if actual_ad_cost > 0 else 0
                
                cost_per_conversion = actual_ad_cost / conversions if conversions > 0 else 0
                revenue_per_user = total_revenue / total_users if total_users > 0 else 0
                profit_margin = ((total_revenue - actual_ad_cost) / total_revenue * 100) if total_revenue > 0 else 0
                
                # New calculations
                average_purchase_revenue_per_active_user = purchase_revenue / active_users if active_users > 0 else 0
                
                return {
                    'propertyId': property_id,
                    'propertyName': f"Property {property_id}",
                    'adsCustomerId': ads_customer_id,
                    # Original metrics with real ad spend
                    'totalRevenue': round(total_revenue, 2),
                    'adSpend': round(actual_ad_cost, 2),
                    'roas': round(roas, 2),
                    'roi': round(roi, 2),
                    'conversionValue': round(purchase_revenue, 2),
                    'costPerConversion': round(cost_per_conversion, 2),
                    'revenuePerUser': round(revenue_per_user, 2),
                    'profitMargin': round(profit_margin, 2),
                    'roasStatus': self.get_roas_status(roas),
                    'roiStatus': self.get_roi_status(roi),
                    'conversions': int(conversions),
                    'sessions': sessions,
                    'totalUsers': total_users,
                    # New ecommerce metrics
                    'totalAdRevenue': round(total_ad_revenue, 2),
                    'totalPurchasers': total_purchasers,
                    'firstTimePurchasers': first_time_purchasers,
                    'averagePurchaseRevenuePerActiveUser': round(average_purchase_revenue_per_active_user, 2),
                    'activeUsers': active_users
                }
            
            return self.get_default_combined_metrics(property_id, ads_customer_id)
            
        except Exception as e:
            logger.error(f"Error fetching combined ROAS/ROI metrics: {e}")
            return self.get_default_combined_metrics(property_id, ads_customer_id)

    def convert_ga_period_to_ads_period(self, ga_period: str) -> str:
        """Convert GA4 period format to Google Ads period format"""
        mapping = {
            "7d": "LAST_7_DAYS",
            "30d": "LAST_30_DAYS", 
            "90d": "LAST_90_DAYS",
            "365d": "LAST_365_DAYS"
        }
        return mapping.get(ga_period, "LAST_30_DAYS")

    def get_default_combined_metrics(self, property_id: str, ads_customer_id: str) -> Dict[str, Any]:
        """Return default combined metrics when no data available"""
        return {
            'propertyId': property_id,
            'propertyName': f"Property {property_id}",
            'adsCustomerId': ads_customer_id,
            'totalRevenue': 0.0,
            'adSpend': 0.0,
            'roas': 0.0,
            'roi': 0.0,
            'conversionValue': 0.0,
            'costPerConversion': 0.0,
            'revenuePerUser': 0.0,
            'profitMargin': 0.0,
            'roasStatus': "No Data",
            'roiStatus': "No Data",
            'conversions': 0,
            'sessions': 0,
            'totalUsers': 0,
            'totalAdRevenue': 0.0,
            'totalPurchasers': 0,
            'firstTimePurchasers': 0,
            'averagePurchaseRevenuePerActiveUser': 0.0,
            'activeUsers': 0
        }

    def get_default_roas_roi_metrics(self, property_id: str) -> Dict[str, Any]:
        """Return default ROAS/ROI metrics when no data available"""
        return {
            'propertyId': property_id,
            'propertyName': f"Property {property_id}",
            # Original metrics
            'totalRevenue': 0.0,
            'adSpend': 0.0,
            'roas': 0.0,
            'roi': 0.0,
            'conversionValue': 0.0,
            'costPerConversion': 0.0,
            'revenuePerUser': 0.0,
            'profitMargin': 0.0,
            'roasStatus': "No Data",
            'roiStatus': "No Data",
            'conversions': 0,
            'sessions': 0,
            'totalUsers': 0,
            # New ecommerce metrics
            'totalAdRevenue': 0.0,
            'totalPurchasers': 0,
            'firstTimePurchasers': 0,
            'averagePurchaseRevenuePerActiveUser': 0.0,
            'activeUsers': 0
        }
    def get_roas_roi_time_series(self, property_id: str, period: str = "30d") -> List[Dict[str, Any]]:
        """Get ROAS and ROI time series data"""
        try:
            start_date, end_date = self.get_date_range(period)
            
            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimensions=[Dimension(name="date")],
                metrics=[
                    Metric(name="totalRevenue"),
                    Metric(name="conversions"),
                    Metric(name="sessions"),
                    Metric(name="totalUsers")
                ],
                order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"))]
            )
            
            response = self.client.run_report(request=request)
            
            time_series = []
            for row in response.rows:
                date_val = row.dimension_values[0].value
                revenue = self.safe_float(row.metric_values[0].value)
                conversions = self.safe_float(row.metric_values[1].value)
                sessions = self.safe_int(row.metric_values[2].value)
                users = self.safe_int(row.metric_values[3].value)
                
                # Estimate ad cost (in real implementation, get from Google Ads API)
                estimated_ad_cost = revenue * 0.25
                
                roas = (revenue / estimated_ad_cost) if estimated_ad_cost > 0 else 0
                roi = ((revenue - estimated_ad_cost) / estimated_ad_cost * 100) if estimated_ad_cost > 0 else 0
                
                time_series.append({
                    'date': date_val,
                    'revenue': round(revenue, 2),
                    'adSpend': round(estimated_ad_cost, 2),
                    'roas': round(roas, 2),
                    'roi': round(roi, 2),
                    'conversions': conversions,
                    'sessions': sessions
                })
            
            return time_series
            
        except Exception as e:
            logger.error(f"Error fetching ROAS/ROI time series for property {property_id}: {e}")
            return []

    def get_roas_status(self, roas: float) -> str:
        """Get ROAS performance status"""
        if roas >= 4.0:
            return "Excellent"
        elif roas >= 3.0:
            return "Good"
        elif roas >= 2.0:
            return "Average"
        elif roas >= 1.0:
            return "Below Average"
        else:
            return "Poor"

    def get_roi_status(self, roi: float) -> str:
        """Get ROI performance status"""
        if roi >= 300:
            return "Excellent"
        elif roi >= 200:
            return "Good"
        elif roi >= 100:
            return "Average"
        elif roi >= 50:
            return "Below Average"
        else:
            return "Poor"


    def get_channel_performance(self, property_id: str, period: str = "30d") -> List[Dict[str, Any]]:
        """Get detailed channel performance data"""
        try:
            start_date, end_date = self.get_date_range(period)
            
            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimensions=[Dimension(name="sessionDefaultChannelGrouping")],
                metrics=[
                    Metric(name="totalUsers"),
                    Metric(name="sessions"),
                    Metric(name="bounceRate"),
                    Metric(name="userEngagementDuration"),
                    Metric(name="conversions"),
                    Metric(name="totalRevenue"),
                ],
                order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="totalUsers"), desc=True)],
                limit=10
            )
            
            response = self.client.run_report(request=request)
            
            channels = []
            for row in response.rows:
                channel_name = row.dimension_values[0].value
                users = self.safe_int(row.metric_values[0].value)
                sessions = self.safe_int(row.metric_values[1].value)
                bounce_rate = self.safe_float(row.metric_values[2].value)
                duration = self.safe_float(row.metric_values[3].value)
                conversions = self.safe_int(row.metric_values[4].value)
                revenue = self.safe_float(row.metric_values[5].value)
                
                avg_session_duration = duration / sessions if sessions > 0 else 0
                conversion_rate = (conversions / sessions * 100) if sessions > 0 else 0
                
                channels.append({
                    'channel': channel_name,
                    'users': users,
                    'sessions': sessions,
                    'bounceRate': bounce_rate * 100,
                    'avgSessionDuration': avg_session_duration,
                    'conversionRate': conversion_rate,
                    'revenue': revenue
                })
            
            return channels
            
        except Exception as e:
            logger.error(f"Error fetching channel performance for property {property_id}: {e}")
            return []
    
    def get_audience_insights(self, property_id: str, dimension: str = "city", period: str = "30d") -> List[Dict[str, Any]]:
        """Get audience insights for a specific dimension"""
        try:
            start_date, end_date = self.get_date_range(period)
            
            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimensions=[Dimension(name=dimension)],
                metrics=[
                    Metric(name="totalUsers"),
                    Metric(name="engagementRate"),
                ],
                order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="totalUsers"), desc=True)],
                limit=15
            )
            
            response = self.client.run_report(request=request)
            total_users = sum(self.safe_int(row.metric_values[0].value) for row in response.rows)
            
            insights = []
            for row in response.rows:
                users = self.safe_int(row.metric_values[0].value)
                percentage = (users / total_users * 100) if total_users > 0 else 0
                
                insights.append({
                    'dimension': dimension,
                    'value': row.dimension_values[0].value,
                    'users': users,
                    'percentage': round(percentage, 2),
                    'engagementRate': self.safe_float(row.metric_values[1].value) * 100
                })
            
            return insights
            
        except Exception as e:
            logger.error(f"Error fetching audience insights for property {property_id}: {e}")
            return []
    
    def get_time_series(self, property_id: str, metric: str = "totalUsers", period: str = "30d") -> List[Dict[str, Any]]:
        """Get time series data for strategic analysis"""
        try:
            start_date, end_date = self.get_date_range(period)
            
            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimensions=[Dimension(name="date")],
                metrics=[Metric(name=metric)],
                order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"))]
            )
            
            response = self.client.run_report(request=request)
            
            time_series = []
            for row in response.rows:
                date_val = row.dimension_values[0].value
                metric_val = self.safe_float(row.metric_values[0].value)
                
                time_series.append({
                    'date': date_val,
                    'metric': metric,
                    'value': metric_val
                })
            
            return time_series
            
        except Exception as e:
            logger.error(f"Error fetching time series for property {property_id}: {e}")
            return []
    
    def get_trends(self, property_id: str, period: str = "30d") -> List[Dict[str, Any]]:
        """Get user acquisition trends over time for a specific property"""
        try:
            start_date, end_date = self.get_date_range(period)
            
            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimensions=[
                    Dimension(name="date"),
                    Dimension(name="newVsReturning")
                ],
                metrics=[Metric(name="totalUsers"), Metric(name="sessions")],
                order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"))]
            )
            
            response = self.client.run_report(request=request)
            
            # Process data by date
            trends_data = {}
            for row in response.rows:
                date = row.dimension_values[0].value
                user_type = row.dimension_values[1].value
                users = self.safe_int(row.metric_values[0].value)
                sessions = self.safe_int(row.metric_values[1].value)
                
                if date not in trends_data:
                    trends_data[date] = {
                        "date": date,
                        "newUsers": 0,
                        "returningUsers": 0,
                        "sessions": 0
                    }
                
                if user_type == "new":
                    trends_data[date]["newUsers"] = users
                else:
                    trends_data[date]["returningUsers"] = users
                
                trends_data[date]["sessions"] += sessions
            
            # Convert to list and sort by date
            trends = list(trends_data.values())
            trends.sort(key=lambda x: x["date"])
            
            return trends
            
        except Exception as e:
            logger.error(f"Error fetching trends for property {property_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch trends: {str(e)}")