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
        """Get GA4 metrics for a specific property"""
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
                
                sessions = self.safe_int(metrics[1].value)
                total_duration = self.safe_float(metrics[4].value)
                avg_session_duration = total_duration / sessions if sessions > 0 else 0
                
                return {
                    'propertyId': property_id,
                    'propertyName': f"Property {property_id}",
                    'totalUsers': self.safe_int(metrics[0].value),
                    'sessions': sessions,
                    'engagedSessions': self.safe_int(metrics[2].value),
                    'engagementRate': self.safe_float(metrics[3].value) * 100,
                    'averageSessionDuration': avg_session_duration,
                    'bounceRate': self.safe_float(metrics[5].value) * 100,
                    'pagesPerSession': self.safe_float(metrics[6].value)
                }
            
            return {
                'propertyId': property_id,
                'propertyName': f"Property {property_id}",
                'totalUsers': 0, 'sessions': 0, 'engagedSessions': 0,
                'engagementRate': 0.0, 'averageSessionDuration': 0.0,
                'bounceRate': 0.0, 'pagesPerSession': 0.0
            }
            
        except Exception as e:
            logger.error(f"Error fetching metrics for property {property_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch metrics: {str(e)}")
    
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