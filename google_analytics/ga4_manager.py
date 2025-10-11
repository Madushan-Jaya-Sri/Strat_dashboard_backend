"""
Google Analytics 4 Manager
Handles all GA4 API operations
"""
import os
import logging
import requests
import openai
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
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

load_dotenv(override=True)
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
    
    def get_date_range(self, period: str, start_date: str = None, end_date: str = None):
        """Get date range based on period or custom dates"""
        # Handle custom date range
        if period == "custom" and start_date and end_date:
            # Validate date format
            try:
                datetime.strptime(start_date, '%Y-%m-%d')
                datetime.strptime(end_date, '%Y-%m-%d')
                return start_date, end_date
            except ValueError:
                raise ValueError("Dates must be in YYYY-MM-DD format")
        
        # Handle predefined periods
        end_date_obj = datetime.now()
        
        if period == "7d":
            start_date_obj = end_date_obj - timedelta(days=7)
        elif period == "90d":
            start_date_obj = end_date_obj - timedelta(days=90)
        elif period in ["365d", "12m"]:
            start_date_obj = end_date_obj - timedelta(days=365)
        else:  # default 30d
            start_date_obj = end_date_obj - timedelta(days=30)
        
        return start_date_obj.strftime("%Y-%m-%d"), end_date_obj.strftime("%Y-%m-%d")
    
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
    
    def get_metrics(self, property_id: str, period: str = "30d", start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """Get GA4 metrics for a specific property with dashboard insights"""
        try:
            start_date_str, end_date_str = self.get_date_range(period, start_date, end_date)
            
            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date_str, end_date=end_date_str)],
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


    def get_traffic_sources(self, property_id: str, period: str = "30d", start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        """Get traffic source data for a specific property"""
        try:
            start_date_str, end_date_str = self.get_date_range(period, start_date, end_date)
            
            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date_str, end_date=end_date_str)],
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
    
    def get_conversions(self, property_id: str, period: str = "30d", start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        """Get conversion data for a specific property"""
        try:
            start_date_str, end_date_str = self.get_date_range(period, start_date, end_date)
            
            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date_str, end_date=end_date_str)],
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


    def call_openai_api(self, prompt: str) -> str:
        """Call OpenAI API"""
        try:
            os
            client = openai.OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
            
            response = client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {
                        "role": "system", 
                        "content": "You are a Google Analytics expert. Always respond with valid JSON only, no additional text."
                    },
                    {
                        "role": "user", 
                        "content": prompt
                    }
                ],
                temperature=0.1,
                max_tokens=500
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"OpenAI API call failed: {e}")
            raise Exception(f"Failed to get LLM analysis: {str(e)}")


    def generate_engagement_funnel_with_llm(self, property_id: str, selected_event_names: List[str], conversions_raw_data: List[Dict[str, Any]], period: str = "30d") -> Dict[str, Any]:
        """Generate engagement funnel using OpenAI LLM with selected events and raw conversion data"""
        try:
            # Filter raw data for selected events only
            selected_events_data = [
                event for event in conversions_raw_data 
                if event['eventName'] in selected_event_names
            ]
            
            if not selected_events_data:
                return {
                    'propertyId': property_id,
                    'period': period,
                    'error': 'No data found for selected events',
                    'funnel_stages': []
                }
            
            # Prepare LLM prompt
            llm_prompt = f"""
            You are a Google Analytics expert creating a user engagement funnel from GA4 event data.

            Selected Events: {selected_event_names}

            Raw GA4 Conversion Data:
            {json.dumps(selected_events_data, indent=2)}

            Task: Create a logical user engagement funnel by:
            1. Order these events from top of funnel (earliest user interaction) to bottom (final conversion) so that the eventCount values form a perfect funnel: each stage's count must be less than or equal to the previous stage (no negative drop-off percentages).
            2. Calculate funnel metrics for each stage.

            Consider typical user journey patterns:
            - Entry events usually come first (session_start, page_view, first_visit)
            - Engagement events in middle (scroll, user_engagement, view_item)
            - Action/conversion events at bottom (form_submit, purchase, sign_up, click)
            - If eventCount values do not follow a decreasing order, reorder the events so that the funnel is perfectly decreasing (no stage should have a higher count than the previous).
            - If needed, drop events that break the funnel shape.

            Return ONLY a JSON response in this exact format:
            {{
                "funnel_stages": [
                    {{
                        "stage_name": "event_name_here",
                        "count": event_count_from_data,
                        "percentage_of_total": percentage_relative_to_first_stage,
                        "drop_off_percentage": percentage_dropped_from_previous_stage,
                        "conversions": conversions_from_data,
                        "conversion_rate": conversion_rate_from_data,
                        "revenue": revenue_from_data
                    }}
                ],
                "ordered_events": ["event1", "event2", "event3"]
            }}

            Notes:
            - First stage should have percentage_of_total: 100.0 and drop_off_percentage: 0.0
            - Use actual eventCount values from the data for 'count'
            - Calculate drop_off_percentage as: ((previous_count - current_count) / previous_count) * 100
            - Calculate percentage_of_total as: (current_count / first_stage_count) * 100
            - Order events so that eventCount values strictly decrease from stage to stage (no negative drop-off).
            - If any event breaks the funnel shape, exclude it from the funnel.
            """
            
            # Call OpenAI API
            openai_response = self.call_openai_api(llm_prompt)
            
            # Parse LLM response
            llm_data = json.loads(openai_response.strip())
            
            funnel_stages = llm_data.get('funnel_stages', [])
            ordered_events = llm_data.get('ordered_events', [])
            
            # Calculate overall metrics
            if funnel_stages:
                first_stage = funnel_stages[0]
                last_stage = funnel_stages[-1]
                overall_conversion_rate = (last_stage['count'] / first_stage['count']) * 100 if first_stage['count'] > 0 else 0
                total_drop_off = first_stage['count'] - last_stage['count']
                total_users_entered = first_stage['count']
                total_users_completed = last_stage['count']
            else:
                overall_conversion_rate = 0
                total_drop_off = 0
                total_users_entered = 0
                total_users_completed = 0
            
            return {
                'propertyId': property_id,
                'period': period,
                'funnel_stages': funnel_stages,
                'total_stages': len(funnel_stages),
                'overall_conversion_rate': round(overall_conversion_rate, 2),
                'total_users_entered': total_users_entered,
                'total_users_completed': total_users_completed,
                'total_drop_off': total_drop_off,
                'selected_events': selected_event_names,
                'ordered_events': ordered_events
            }
            
        except Exception as e:
            logger.error(f"Error generating engagement funnel with LLM: {e}")
            return {
                'propertyId': property_id,
                'period': period,
                'error': str(e),
                'funnel_stages': []
            }










    # Enhanced methods for your GA4Manager class

    def get_currency_rates(self) -> Dict[str, float]:
            """Get current USD exchange rates from a free API"""
            try:
                # Using exchangerate-api.com (free tier)
                response = requests.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    return data.get('rates', {})
                else:
                    # Fallback rates if API fails
                    return self._get_fallback_rates()
            except Exception as e:
                logger.warning(f"Failed to fetch currency rates: {e}")
                return self._get_fallback_rates()

    def _get_fallback_rates(self) -> Dict[str, float]:
        """Fallback exchange rates when API is unavailable"""
        return {
            'LKR': 325.0,   # 1 USD = 325 LKR (approximate)
            'EUR': 0.92,    # 1 USD = 0.92 EUR
            'GBP': 0.79,    # 1 USD = 0.79 GBP
            'INR': 83.0,    # 1 USD = 83 INR
            'AUD': 1.52,    # 1 USD = 1.52 AUD
            'CAD': 1.37,    # 1 USD = 1.37 CAD
            'JPY': 149.0,   # 1 USD = 149 JPY
            'USD': 1.0      # 1 USD = 1 USD
        }

    def convert_to_usd(self, amount: float, from_currency: str, rates: Dict[str, float]) -> float:
        """Convert amount from specified currency to USD"""
        if from_currency == 'USD' or amount == 0:
            return amount
        
        if from_currency in rates:
            # For exchange rate APIs, rates are typically given as 1 USD = X foreign currency
            # So to convert from foreign currency to USD: amount / rate
            rate = rates[from_currency]
            usd_amount = amount / rate
            logger.debug(f"Converted {amount} {from_currency} to {usd_amount:.2f} USD (rate: {rate})")
            return usd_amount
        else:
            logger.warning(f"Currency {from_currency} not found in rates, assuming USD")
            return amount

    def get_property_currency_from_api(self, property_id: str) -> str:
        """Get currency code from GA4 property using Admin API"""
        try:
            credentials = self.auth_manager.get_user_credentials(self.user_email)
            admin_service = build('analyticsadmin', 'v1alpha', credentials=credentials)
            
            # Get property details including currency
            property_name = f"properties/{property_id}"
            property_response = admin_service.properties().get(name=property_name).execute()
            
            # The currency is in the property details
            currency_code = property_response.get('currencyCode', 'USD')
            logger.info(f"Retrieved currency {currency_code} for property {property_id}")
            return currency_code
            
        except Exception as e:
            logger.warning(f"Could not get currency from property API for {property_id}: {e}")
            return 'USD'

    def get_property_currency_enhanced(self, property_id: str) -> str:
        """Enhanced currency detection with caching"""
        try:
            # First, check cache
            cached_info = self.get_cached_property_info(property_id)
            if cached_info:
                return cached_info.get('currency_code', 'USD')
            
            # If not cached, fetch from API
            property_info = self.get_property_details_with_currency(property_id)
            return property_info.get('currency_code', 'USD')
            
        except Exception as e:
            logger.error(f"Enhanced currency detection failed for {property_id}: {e}")
            return 'USD'

    def get_property_details_with_currency(self, property_id: str) -> Dict[str, Any]:
        """Get comprehensive property details including currency from GA4"""
        try:
            credentials = self.auth_manager.get_user_credentials(self.user_email)
            admin_service = build('analyticsadmin', 'v1alpha', credentials=credentials)
            
            # Get property details
            property_name = f"properties/{property_id}"
            property_response = admin_service.properties().get(name=property_name).execute()
            
            # Extract currency and other useful info
            property_info = {
                'property_id': property_id,
                'display_name': property_response.get('displayName', f'Property {property_id}'),
                'currency_code': property_response.get('currencyCode', 'USD'),
                'time_zone': property_response.get('timeZone', 'UTC'),
                'industry_category': property_response.get('industryCategory', ''),
                'website_url': property_response.get('websiteUrl', ''),
                'create_time': property_response.get('createTime', ''),
                'update_time': property_response.get('updateTime', '')
            }
            
            # Cache this information for future use
            self.cache_property_info(property_id, property_info)
            
            return property_info
            
        except Exception as e:
            logger.error(f"Failed to get property details for {property_id}: {e}")
            return {
                'property_id': property_id,
                'currency_code': 'USD',  # Safe default
                'display_name': f'Property {property_id}'
            }

    def cache_property_info(self, property_id: str, property_info: Dict[str, Any]):
        """Cache property information to avoid repeated API calls"""
        # You can implement this with Redis, database, or in-memory cache
        # For now, using a simple class-level cache
        if not hasattr(self, '_property_cache'):
            self._property_cache = {}
        
        self._property_cache[property_id] = {
            'data': property_info,
            'cached_at': datetime.now(),
            'expires_at': datetime.now() + timedelta(hours=24)  # Cache for 24 hours
        }

    def get_cached_property_info(self, property_id: str) -> Optional[Dict[str, Any]]:
        """Get cached property information if still valid"""
        if not hasattr(self, '_property_cache'):
            return None
        
        cached = self._property_cache.get(property_id)
        if cached and datetime.now() < cached['expires_at']:
            return cached['data']
        
        # Remove expired cache
        if cached:
            del self._property_cache[property_id]
        
        return None

    def get_ads_customer_currency(self, customer_id: str) -> str:
        """Get currency for Google Ads customer"""
        try:
            from google_ads.ads_manager import GoogleAdsManager
            ads_manager = GoogleAdsManager(self.user_email, self.auth_manager)
            customer_info = ads_manager.get_customer_info(customer_id)
            return customer_info.get('currency', 'USD')
        except Exception as e:
            logger.warning(f"Could not get currency for customer {customer_id}: {e}")
            return "USD"

    def get_multi_customer_ad_spend(self, ads_customer_ids: List[str], period: str) -> Dict[str, Any]:
        """Get total ad spend from multiple Google Ads customers"""
        try:
            from google_ads.ads_manager import GoogleAdsManager
            ads_manager = GoogleAdsManager(self.user_email, self.auth_manager)
            
            total_cost_usd = 0.0
            currency_rates = self.get_currency_rates()
            customer_costs = []
            
            for customer_id in ads_customer_ids:
                try:
                    # Get ad spend for this customer
                    ads_period = self.convert_ga_period_to_ads_period(period)
                    customer_cost = ads_manager.get_total_cost_for_period(customer_id, ads_period)
                    
                    # Get customer currency
                    customer_currency = self.get_ads_customer_currency(customer_id)
                    
                    # Convert to USD
                    customer_cost_usd = self.convert_to_usd(customer_cost, customer_currency, currency_rates)
                    
                    total_cost_usd += customer_cost_usd
                    
                    customer_costs.append({
                        'customer_id': customer_id,
                        'cost_original': customer_cost,
                        'currency': customer_currency,
                        'cost_usd': customer_cost_usd
                    })
                    
                    logger.info(f"Customer {customer_id}: {customer_cost} {customer_currency} = {customer_cost_usd:.2f} USD")
                    
                except Exception as customer_error:
                    logger.warning(f"Could not fetch costs for customer {customer_id}: {customer_error}")
                    # Add zero-cost entry for failed customers
                    customer_costs.append({
                        'customer_id': customer_id,
                        'cost_original': 0.0,
                        'currency': 'USD',
                        'cost_usd': 0.0,
                        'error': str(customer_error)
                    })
                    continue
            
            logger.info(f"Total ad spend across {len(ads_customer_ids)} customers: ${total_cost_usd:.2f} USD")
            
            return {
                'total_cost_usd': total_cost_usd,
                'customer_breakdown': customer_costs,
                'exchange_rates_used': currency_rates,
                'customers_processed': len(customer_costs),
                'customers_successful': len([c for c in customer_costs if 'error' not in c])
            }
            
        except Exception as e:
            logger.error(f"Error fetching multi-customer ad spend: {e}")
            return {
                'total_cost_usd': 0.0,
                'customer_breakdown': [],
                'exchange_rates_used': {},
                'customers_processed': 0,
                'customers_successful': 0,
                'error': str(e)
            }
        
    def get_enhanced_combined_roas_roi_metrics(self, property_id: str, ads_customer_ids: List[str], period: str = "30d") -> Dict[str, Any]:
        """Get ROAS and ROI metrics with proper currency handling and multiple ads accounts"""
        try:
            start_date, end_date = self.get_date_range(period)
            currency_rates = self.get_currency_rates()
            
            # Get property currency using enhanced method
            property_currency = self.get_property_currency_enhanced(property_id)
            property_info = self.get_cached_property_info(property_id) or {'display_name': f'Property {property_id}'}
            
            logger.info(f"Processing property {property_id} with currency {property_currency}")
            
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
            
            response = self.client.run_report(request)
            
            # Get PAID SEARCH revenue specifically
            paid_search_request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimensions=[Dimension(name="sessionDefaultChannelGrouping")],
                metrics=[
                    Metric(name="totalRevenue"),
                    Metric(name="purchaseRevenue")
                ],
            )
            
            paid_search_response = self.client.run_report(paid_search_request)
            
            # Extract paid search revenue
            paid_search_total_revenue = 0.0
            paid_search_purchase_revenue = 0.0
            
            for row in paid_search_response.rows:
                channel = row.dimension_values[0].value
                if channel.lower() in ['paid search', 'google ads', 'cpc']:
                    paid_search_total_revenue += self.safe_float(row.metric_values[0].value)
                    paid_search_purchase_revenue += self.safe_float(row.metric_values[1].value)
            
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
            
            # Get multi-customer ad spend in USD
            ad_spend_data = self.get_multi_customer_ad_spend(ads_customer_ids, period)
            actual_ad_cost_usd = ad_spend_data['total_cost_usd']
            
            if response.rows:
                row = response.rows[0]
                metrics = row.metric_values
                
                # Get GA4 metrics in original currency (keeping original total revenue for other calculations)
                total_revenue_original = self.safe_float(metrics[0].value)
                purchase_revenue_original = self.safe_float(metrics[1].value)
                total_ad_revenue_original = self.safe_float(metrics[2].value)
                conversions = self.safe_float(metrics[3].value)
                sessions = self.safe_int(metrics[4].value)
                total_users = self.safe_int(metrics[5].value)
                active_users = self.safe_int(metrics[6].value)
                total_purchasers = self.safe_int(metrics[7].value)
                
                logger.info(f"GA4 Total Revenue: {total_revenue_original} {property_currency}")
                logger.info(f"GA4 Paid Search Revenue: {paid_search_total_revenue} {property_currency}")
                logger.info(f"Ad Spend: {actual_ad_cost_usd} USD")
                
                # Convert GA4 revenue metrics to USD
                total_revenue_usd = self.convert_to_usd(total_revenue_original, property_currency, currency_rates)
                purchase_revenue_usd = self.convert_to_usd(purchase_revenue_original, property_currency, currency_rates)
                total_ad_revenue_usd = self.convert_to_usd(total_ad_revenue_original, property_currency, currency_rates)
                
                # Convert PAID SEARCH revenue to USD for ROAS/ROI calculations
                paid_search_total_revenue_usd = self.convert_to_usd(paid_search_total_revenue, property_currency, currency_rates)
                
                # Process first-time purchasers data
                first_time_purchasers = 0
                if first_time_response.rows:
                    for row in first_time_response.rows:
                        user_type = row.dimension_values[0].value
                        purchasers = self.safe_int(row.metric_values[0].value)
                        if user_type == "new":
                            first_time_purchasers = purchasers
                
                # Calculate ROAS/ROI using PAID SEARCH revenue instead of total revenue
                roas = (paid_search_total_revenue_usd / actual_ad_cost_usd) if actual_ad_cost_usd > 0 else 0
                roi = (((paid_search_total_revenue_usd - actual_ad_cost_usd) / actual_ad_cost_usd) * 100) if actual_ad_cost_usd > 0 else 0
                
                cost_per_conversion = actual_ad_cost_usd / conversions if conversions > 0 else 0
                revenue_per_user = total_revenue_usd / total_users if total_users > 0 else 0
                profit_margin = ((paid_search_total_revenue_usd - actual_ad_cost_usd) / paid_search_total_revenue_usd * 100) if paid_search_total_revenue_usd > 0 else 0
                
                # New calculations
                average_purchase_revenue_per_active_user = purchase_revenue_usd / active_users if active_users > 0 else 0
                
                logger.info(f"ROAS (Paid Search): {roas:.2f}, ROI (Paid Search): {roi:.2f}%")
                
                return {
                    'propertyId': property_id,
                    'propertyName': property_info.get('display_name', f"Property {property_id}"),
                    'adsCustomerIds': ads_customer_ids,
                    'currency_info': {
                        'property_currency': property_currency,
                        'calculation_currency': 'USD',
                        'exchange_rates': currency_rates,
                        'ad_spend_breakdown': ad_spend_data['customer_breakdown'],
                        'property_info': property_info,
                        'customers_processed': ad_spend_data.get('customers_processed', 0),
                        'customers_successful': ad_spend_data.get('customers_successful', 0)
                    },
                    # Original metrics in USD (totalRevenue still shows total, but ROAS/ROI calculated from paid search)
                    'totalRevenue': round(total_revenue_usd, 2),
                    'totalRevenueOriginal': round(total_revenue_original, 2),
                    'adSpend': round(actual_ad_cost_usd, 2),
                    'roas': round(roas, 2),
                    'roi': round(roi, 2),
                    'conversionValue': round(purchase_revenue_usd, 2),
                    'conversionValueOriginal': round(purchase_revenue_original, 2),
                    'costPerConversion': round(cost_per_conversion, 2),
                    'revenuePerUser': round(revenue_per_user, 2),
                    'profitMargin': round(profit_margin, 2),
                    'roasStatus': self.get_roas_status(roas),
                    'roiStatus': self.get_roi_status(roi),
                    'conversions': int(conversions),
                    'sessions': sessions,
                    'totalUsers': total_users,
                    # New ecommerce metrics
                    'totalAdRevenue': round(total_ad_revenue_usd, 2),
                    'totalAdRevenueOriginal': round(total_ad_revenue_original, 2),
                    'totalPurchasers': total_purchasers,
                    'firstTimePurchasers': first_time_purchasers,
                    'averagePurchaseRevenuePerActiveUser': round(average_purchase_revenue_per_active_user, 2),
                    'activeUsers': active_users
                }
            
            return self.get_default_enhanced_combined_metrics(property_id, ads_customer_ids)
            
        except Exception as e:
            logger.error(f"Error fetching enhanced combined ROAS/ROI metrics: {e}")
            return self.get_default_enhanced_combined_metrics(property_id, ads_customer_ids)


    def get_default_enhanced_combined_metrics(self, property_id: str, ads_customer_ids: List[str]) -> Dict[str, Any]:
        """Return default enhanced combined metrics when no data available"""
        return {
            'propertyId': property_id,
            'propertyName': f"Property {property_id}",
            'adsCustomerIds': ads_customer_ids,
            'currency_info': {
                'property_currency': 'USD',
                'calculation_currency': 'USD',
                'exchange_rates': {},
                'ad_spend_breakdown': [],
                'property_info': {'display_name': f'Property {property_id}'},
                'customers_processed': 0,
                'customers_successful': 0
            },
            'totalRevenue': 0.0,
            'totalRevenueOriginal': 0.0,
            'adSpend': 0.0,
            'roas': 0.0,
            'roi': 0.0,
            'conversionValue': 0.0,
            'conversionValueOriginal': 0.0,
            'costPerConversion': 0.0,
            'revenuePerUser': 0.0,
            'profitMargin': 0.0,
            'roasStatus': "No Data",
            'roiStatus': "No Data",
            'conversions': 0,
            'sessions': 0,
            'totalUsers': 0,
            'totalAdRevenue': 0.0,
            'totalAdRevenueOriginal': 0.0,
            'totalPurchasers': 0,
            'firstTimePurchasers': 0,
            'averagePurchaseRevenuePerActiveUser': 0.0,
            'activeUsers': 0
        }



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
                roi = (((total_revenue - actual_ad_cost) / actual_ad_cost) * 100) if actual_ad_cost > 0 else 0
                
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


    def get_channel_performance(self, property_id: str, period: str = "30d", start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        """Get detailed channel performance data"""
        try:
            start_date_str, end_date_str = self.get_date_range(period, start_date, end_date)
            
            
            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date_str, end_date=end_date_str)],
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
        
    def get_coordinates(self, city: str, country: str) -> tuple:
        """Get latitude and longitude for a city/country"""
        try:
            # Handle cases where city is actually a country name
            if city in ["(not set)", "unknown"]:
                return 0.0, 0.0
            
            # If city appears to be a country name (no comma in the original value), use just the city as the query
            if country in ["(not set)", "unknown"] or city == country:
                query = city  # Use just the location name
            else:
                query = f"{city}, {country}"  # Use city, country format
            
            # Use free Nominatim API
            url = "https://nominatim.openstreetmap.org/search"
            params = {
                'q': query,
                'format': 'json',
                'limit': 1
            }
            headers = {
                'User-Agent': 'Marketing-Dashboard/1.0'
            }
            
            response = requests.get(url, params=params, headers=headers, timeout=5)
            if response.status_code == 200:
                data = response.json()
                if data:
                    return float(data[0]['lat']), float(data[0]['lon'])
            
            return 0.0, 0.0
            
        except Exception as e:
            logger.warning(f"Geocoding failed for {city}, {country}: {e}")
            return 0.0, 0.0
    
    def get_audience_insights(self, property_id: str, dimension: str = "city", period: str = "30d", start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        """Get audience insights for a specific dimension"""
        try:
            start_date_str, end_date_str = self.get_date_range(period, start_date, end_date)
            
            # For geographic dimensions, get both city and country
            if dimension in ["city", "country"]:
                dimensions = [
                    Dimension(name="city"),
                    Dimension(name="country")
                ]
            else:
                dimensions = [Dimension(name=dimension)]
            
            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date_str, end_date=end_date_str)],
                dimensions=dimensions,
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
                engagement_rate = self.safe_float(row.metric_values[1].value) * 100
                
                if dimension in ["city", "country"]:
                    city = row.dimension_values[0].value
                    country = row.dimension_values[1].value
                    
                    # Create clean display value
                    if city == "(not set)" or city == country:
                        display_value = country
                    else:
                        display_value = f"{city}, {country}"
                    
                    latitude, longitude = self.get_coordinates(city, country)
                    
                    insights.append({
                        'dimension': dimension,
                        'value': display_value,
                        'latitude': latitude,
                        'longitude': longitude,
                        'users': users,
                        'percentage': round(percentage, 2),
                        'engagementRate': round(engagement_rate, 2)
                    })
                else:
                    insights.append({
                        'dimension': dimension,
                        'value': row.dimension_values[0].value,
                        'latitude': 0.0,
                        'longitude': 0.0,
                        'users': users,
                        'percentage': round(percentage, 2),
                        'engagementRate': round(engagement_rate, 2)
                    })
            
            return insights
            
        except Exception as e:
            logger.error(f"Error fetching audience insights for property {property_id}: {e}")
            return []
    
    def get_time_series(self, property_id: str, metric: str = "totalUsers", period: str = "30d", start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        """Get time series data for strategic analysis"""
        try:
            start_date_str, end_date_str = self.get_date_range(period, start_date, end_date)
            
            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date_str, end_date=end_date_str)],
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
        

    # Add these methods to your GA4Manager class
    def get_revenue_breakdown_by_channel(self, property_id: str, period: str = "30d", start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """Get detailed revenue breakdown by channel"""
        try:
            start_date_str, end_date_str = self.get_date_range(period, start_date, end_date)
            
            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date_str, end_date=end_date_str)],
                dimensions=[Dimension(name="sessionDefaultChannelGrouping")],
                metrics=[
                    Metric(name="totalRevenue"),
                    Metric(name="purchaseRevenue"),
                    Metric(name="sessions"),
                    Metric(name="totalUsers"),
                    Metric(name="conversions"),
                    Metric(name="totalPurchasers")
                ],
                order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="totalRevenue"), desc=True)]
            )
            
            response = self.client.run_report(request)
            
            channels = []
            total_revenue_sum = 0
            
            for row in response.rows:
                channel = row.dimension_values[0].value
                total_revenue = self.safe_float(row.metric_values[0].value)
                purchase_revenue = self.safe_float(row.metric_values[1].value)
                sessions = self.safe_int(row.metric_values[2].value)
                users = self.safe_int(row.metric_values[3].value)
                conversions = self.safe_float(row.metric_values[4].value)
                purchasers = self.safe_int(row.metric_values[5].value)
                
                total_revenue_sum += total_revenue
                
                revenue_per_session = total_revenue / sessions if sessions > 0 else 0
                conversion_rate = (conversions / sessions * 100) if sessions > 0 else 0
                
                channels.append({
                    'channel': channel,
                    'totalRevenue': total_revenue,
                    'purchaseRevenue': purchase_revenue,
                    'sessions': sessions,
                    'users': users,
                    'conversions': int(conversions),
                    'purchasers': purchasers,
                    'revenuePerSession': revenue_per_session,
                    'conversionRate': conversion_rate
                })
            
            # Calculate percentages
            for channel in channels:
                channel['revenuePercentage'] = (channel['totalRevenue'] / total_revenue_sum * 100) if total_revenue_sum > 0 else 0
            
            return {
                'channels': channels,
                'totalRevenue': total_revenue_sum,
                'totalChannels': len(channels)
            }
            
        except Exception as e:
            logger.error(f"Error getting revenue breakdown by channel: {e}")
            return {'channels': [], 'totalRevenue': 0, 'totalChannels': 0}


    def get_revenue_breakdown_by_source_medium(self, property_id: str, period: str = "30d", start_date: str = None, end_date: str = None,limit: int = 20) -> Dict[str, Any]:
        """Get detailed revenue breakdown by channel"""
        try:
            start_date_str, end_date_str = self.get_date_range(period, start_date, end_date)
            
            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date_str, end_date=end_date_str)],
                dimensions=[
                    Dimension(name="sessionSource"),
                    Dimension(name="sessionMedium")
                ],
                metrics=[
                    Metric(name="totalRevenue"),
                    Metric(name="purchaseRevenue"),
                    Metric(name="sessions"),
                    Metric(name="conversions")
                ],
                order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="totalRevenue"), desc=True)],
                limit=limit
            )
            
            response = self.client.run_report(request)
            
            sources = []
            total_revenue_sum = 0
            
            for row in response.rows:
                source = row.dimension_values[0].value
                medium = row.dimension_values[1].value
                total_revenue = self.safe_float(row.metric_values[0].value)
                purchase_revenue = self.safe_float(row.metric_values[1].value)
                sessions = self.safe_int(row.metric_values[2].value)
                conversions = self.safe_float(row.metric_values[3].value)
                
                total_revenue_sum += total_revenue
                
                sources.append({
                    'source': source,
                    'medium': medium,
                    'sourceMedium': f"{source} / {medium}",
                    'totalRevenue': total_revenue,
                    'purchaseRevenue': purchase_revenue,
                    'sessions': sessions,
                    'conversions': int(conversions)
                })
            
            # Calculate percentages
            for source in sources:
                source['revenuePercentage'] = (source['totalRevenue'] / total_revenue_sum * 100) if total_revenue_sum > 0 else 0
            
            return {
                'sources': sources,
                'totalRevenue': total_revenue_sum,
                'totalSources': len(sources)
            }
            
        except Exception as e:
            logger.error(f"Error getting revenue breakdown by source/medium: {e}")
            return {'sources': [], 'totalRevenue': 0, 'totalSources': 0}



    def get_revenue_breakdown_by_device(self, property_id: str, period: str = "30d", start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """Get detailed revenue breakdown by channel"""
        try:
            start_date_str, end_date_str = self.get_date_range(period, start_date, end_date)
            
            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date_str, end_date=end_date_str)],
                dimensions=[Dimension(name="deviceCategory")],
                metrics=[
                    Metric(name="totalRevenue"),
                    Metric(name="purchaseRevenue"),
                    Metric(name="sessions"),
                    Metric(name="conversions"),
                    Metric(name="totalUsers")
                ],
                order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="totalRevenue"), desc=True)]
            )
            
            response = self.client.run_report(request)
            
            devices = []
            total_revenue_sum = 0
            
            for row in response.rows:
                device = row.dimension_values[0].value
                total_revenue = self.safe_float(row.metric_values[0].value)
                purchase_revenue = self.safe_float(row.metric_values[1].value)
                sessions = self.safe_int(row.metric_values[2].value)
                conversions = self.safe_float(row.metric_values[3].value)
                users = self.safe_int(row.metric_values[4].value)
                
                total_revenue_sum += total_revenue
                
                devices.append({
                    'device': device,
                    'totalRevenue': total_revenue,
                    'purchaseRevenue': purchase_revenue,
                    'sessions': sessions,
                    'conversions': int(conversions),
                    'users': users
                })
            
            # Calculate percentages
            for device in devices:
                device['revenuePercentage'] = (device['totalRevenue'] / total_revenue_sum * 100) if total_revenue_sum > 0 else 0
            
            return {
                'devices': devices,
                'totalRevenue': total_revenue_sum,
                'totalDevices': len(devices)
            }
            
        except Exception as e:
            logger.error(f"Error getting revenue breakdown by device: {e}")
            return {'devices': [], 'totalRevenue': 0, 'totalDevices': 0}


    def get_revenue_breakdown_by_location(self, property_id: str, period: str = "30d", start_date: str = None, end_date: str = None,limit: int = 15) -> Dict[str, Any]:
        """Get detailed revenue breakdown by channel"""
        try:
            start_date_str, end_date_str = self.get_date_range(period, start_date, end_date)
            
            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date_str, end_date=end_date_str)],
                dimensions=[
                    Dimension(name="country"),
                    Dimension(name="city")
                ],
                metrics=[
                    Metric(name="totalRevenue"),
                    Metric(name="purchaseRevenue"),
                    Metric(name="sessions"),
                    Metric(name="totalUsers")
                ],
                order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="totalRevenue"), desc=True)],
                limit=limit
            )
            
            response = self.client.run_report(request)
            
            locations = []
            total_revenue_sum = 0
            
            for row in response.rows:
                country = row.dimension_values[0].value
                city = row.dimension_values[1].value
                total_revenue = self.safe_float(row.metric_values[0].value)
                purchase_revenue = self.safe_float(row.metric_values[1].value)
                sessions = self.safe_int(row.metric_values[2].value)
                users = self.safe_int(row.metric_values[3].value)
                
                total_revenue_sum += total_revenue
                
                locations.append({
                    'country': country,
                    'city': city,
                    'location': f"{city}, {country}" if city != "(not set)" else country,
                    'totalRevenue': total_revenue,
                    'purchaseRevenue': purchase_revenue,
                    'sessions': sessions,
                    'users': users
                })
            
            # Calculate percentages
            for location in locations:
                location['revenuePercentage'] = (location['totalRevenue'] / total_revenue_sum * 100) if total_revenue_sum > 0 else 0
            
            return {
                'locations': locations,
                'totalRevenue': total_revenue_sum,
                'totalLocations': len(locations)
            }
            
        except Exception as e:
            logger.error(f"Error getting revenue breakdown by location: {e}")
            return {'locations': [], 'totalRevenue': 0, 'totalLocations': 0}


    def get_revenue_breakdown_by_page(self, property_id: str, period: str = "30d", start_date: str = None, end_date: str = None,limit: int = 20) -> Dict[str, Any]:
        """Get detailed revenue breakdown by channel"""
        try:
            start_date_str, end_date_str = self.get_date_range(period, start_date, end_date)
            
            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date_str, end_date=end_date_str)],
                dimensions=[
                    Dimension(name="landingPage"),
                    Dimension(name="pageTitle")
                ],
                metrics=[
                    Metric(name="totalRevenue"),
                    Metric(name="purchaseRevenue"),
                    Metric(name="sessions"),
                    Metric(name="conversions")
                ],
                order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="totalRevenue"), desc=True)],
                limit=limit
            )
            
            response = self.client.run_report(request)
            
            pages = []
            total_revenue_sum = 0
            
            for row in response.rows:
                landing_page = row.dimension_values[0].value
                page_title = row.dimension_values[1].value
                total_revenue = self.safe_float(row.metric_values[0].value)
                purchase_revenue = self.safe_float(row.metric_values[1].value)
                sessions = self.safe_int(row.metric_values[2].value)
                conversions = self.safe_float(row.metric_values[3].value)
                
                total_revenue_sum += total_revenue
                
                pages.append({
                    'landingPage': landing_page,
                    'pageTitle': page_title,
                    'totalRevenue': total_revenue,
                    'purchaseRevenue': purchase_revenue,
                    'sessions': sessions,
                    'conversions': int(conversions)
                })
            
            # Calculate percentages
            for page in pages:
                page['revenuePercentage'] = (page['totalRevenue'] / total_revenue_sum * 100) if total_revenue_sum > 0 else 0
            
            return {
                'pages': pages,
                'totalRevenue': total_revenue_sum,
                'totalPages': len(pages)
            }
            
        except Exception as e:
            logger.error(f"Error getting revenue breakdown by page: {e}")
            return {'pages': [], 'totalRevenue': 0, 'totalPages': 0}


    def get_comprehensive_revenue_breakdown(self, property_id: str, period: str = "30d", start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """Get detailed revenue breakdown by channel"""

        try:
            property_currency = self.get_property_currency_enhanced(property_id)
            currency_rates = self.get_currency_rates()
            
            # Get all breakdowns
            channel_breakdown = self.get_revenue_breakdown_by_channel(property_id, period)
            source_breakdown = self.get_revenue_breakdown_by_source_medium(property_id, period)
            device_breakdown = self.get_revenue_breakdown_by_device(property_id, period)
            location_breakdown = self.get_revenue_breakdown_by_location(property_id, period)
            page_breakdown = self.get_revenue_breakdown_by_page(property_id, period)
            
            # Convert all revenue to USD
            def convert_breakdown_to_usd(breakdown_data, revenue_fields):
                if 'totalRevenue' in breakdown_data:
                    breakdown_data['totalRevenueUSD'] = self.convert_to_usd(
                        breakdown_data['totalRevenue'], property_currency, currency_rates
                    )
                
                for item in breakdown_data.get('channels', []) + breakdown_data.get('sources', []) + \
                        breakdown_data.get('devices', []) + breakdown_data.get('locations', []) + \
                        breakdown_data.get('pages', []):
                    for field in revenue_fields:
                        if field in item:
                            item[f"{field}USD"] = self.convert_to_usd(
                                item[field], property_currency, currency_rates
                            )
            
            revenue_fields = ['totalRevenue', 'purchaseRevenue']
            convert_breakdown_to_usd(channel_breakdown, revenue_fields)
            convert_breakdown_to_usd(source_breakdown, revenue_fields)
            convert_breakdown_to_usd(device_breakdown, revenue_fields)
            convert_breakdown_to_usd(location_breakdown, revenue_fields)
            convert_breakdown_to_usd(page_breakdown, revenue_fields)
            
            return {
                'propertyId': property_id,
                'period': period,
                'currency_info': {
                    'original_currency': property_currency,
                    'exchange_rates': currency_rates
                },
                'breakdown_by_channel': channel_breakdown,
                'breakdown_by_source': source_breakdown,
                'breakdown_by_device': device_breakdown,
                'breakdown_by_location': location_breakdown,
                'breakdown_by_page': page_breakdown,
                'summary': {
                    'total_channels': channel_breakdown['totalChannels'],
                    'total_sources': source_breakdown['totalSources'],
                    'total_devices': device_breakdown['totalDevices'],
                    'total_locations': location_breakdown['totalLocations'],
                    'total_pages': page_breakdown['totalPages']
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting comprehensive revenue breakdown: {e}")
            return {
                'propertyId': property_id,
                'period': period,
                'error': str(e)
            }
        

    # Add these methods to your GA4Manager class

    def get_channel_revenue_time_series(self, property_id: str, period: str = "30d", start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """Get revenue breakdown by channel over time"""
        try:
            start_date_str, end_date_str = self.get_date_range(period, start_date, end_date)
            
        
            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date_str, end_date=end_date_str)],
                dimensions=[
                    Dimension(name="date"),
                    Dimension(name="sessionDefaultChannelGrouping")
                ],
                metrics=[
                    Metric(name="totalRevenue"),
                    Metric(name="purchaseRevenue"),
                    Metric(name="sessions"),
                    Metric(name="totalUsers"),
                    Metric(name="conversions")
                ],
                order_bys=[
                    OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date")),
                    OrderBy(metric=OrderBy.MetricOrderBy(metric_name="totalRevenue"), desc=True)
                ]
            )
            
            response = self.client.run_report(request)
            
            # Get property currency for conversion
            property_currency = self.get_property_currency_enhanced(property_id)
            currency_rates = self.get_currency_rates()
            
            # Organize data by date and channel
            time_series_data = {}
            channel_totals = {}
            date_totals = {}
            all_channels = set()
            
            for row in response.rows:
                date = row.dimension_values[0].value
                channel = row.dimension_values[1].value
                total_revenue = self.safe_float(row.metric_values[0].value)
                purchase_revenue = self.safe_float(row.metric_values[1].value)
                sessions = self.safe_int(row.metric_values[2].value)
                users = self.safe_int(row.metric_values[3].value)
                conversions = self.safe_float(row.metric_values[4].value)
                
                # Convert revenue to USD
                total_revenue_usd = self.convert_to_usd(total_revenue, property_currency, currency_rates)
                purchase_revenue_usd = self.convert_to_usd(purchase_revenue, property_currency, currency_rates)
                
                # Track all channels
                all_channels.add(channel)
                
                # Initialize date if not exists
                if date not in time_series_data:
                    time_series_data[date] = {
                        'date': date,
                        'channels': {},
                        'total_revenue': 0,
                        'total_revenue_usd': 0,
                        'total_sessions': 0,
                        'total_users': 0,
                        'total_conversions': 0
                    }
                
                # Add channel data for this date
                time_series_data[date]['channels'][channel] = {
                    'channel': channel,
                    'totalRevenue': total_revenue,
                    'totalRevenueUSD': total_revenue_usd,
                    'purchaseRevenue': purchase_revenue,
                    'purchaseRevenueUSD': purchase_revenue_usd,
                    'sessions': sessions,
                    'users': users,
                    'conversions': int(conversions)
                }
                
                # Update date totals
                time_series_data[date]['total_revenue'] += total_revenue
                time_series_data[date]['total_revenue_usd'] += total_revenue_usd
                time_series_data[date]['total_sessions'] += sessions
                time_series_data[date]['total_users'] += users
                time_series_data[date]['total_conversions'] += conversions
                
                # Update channel totals
                if channel not in channel_totals:
                    channel_totals[channel] = {
                        'channel': channel,
                        'totalRevenue': 0,
                        'totalRevenueUSD': 0,
                        'totalSessions': 0,
                        'totalUsers': 0,
                        'totalConversions': 0,
                        'days_active': 0
                    }
                
                channel_totals[channel]['totalRevenue'] += total_revenue
                channel_totals[channel]['totalRevenueUSD'] += total_revenue_usd
                channel_totals[channel]['totalSessions'] += sessions
                channel_totals[channel]['totalUsers'] += users
                channel_totals[channel]['totalConversions'] += conversions
                channel_totals[channel]['days_active'] += 1
            
            # Calculate percentages and fill missing channel data for each date
            for date_data in time_series_data.values():
                # Fill missing channels with zero values
                for channel in all_channels:
                    if channel not in date_data['channels']:
                        date_data['channels'][channel] = {
                            'channel': channel,
                            'totalRevenue': 0,
                            'totalRevenueUSD': 0,
                            'purchaseRevenue': 0,
                            'purchaseRevenueUSD': 0,
                            'sessions': 0,
                            'users': 0,
                            'conversions': 0
                        }
                
                # Calculate percentages for each channel on this date
                for channel_data in date_data['channels'].values():
                    channel_data['revenuePercentage'] = (
                        (channel_data['totalRevenue'] / date_data['total_revenue'] * 100)
                        if date_data['total_revenue'] > 0 else 0
                    )
            
            # Convert to list format sorted by date
            time_series_list = sorted(time_series_data.values(), key=lambda x: x['date'])
            
            # Calculate channel averages
            for channel_data in channel_totals.values():
                days_active = channel_data['days_active']
                if days_active > 0:
                    channel_data['avgDailyRevenue'] = channel_data['totalRevenue'] / days_active
                    channel_data['avgDailyRevenueUSD'] = channel_data['totalRevenueUSD'] / days_active
                    channel_data['avgDailySessions'] = channel_data['totalSessions'] / days_active
                else:
                    channel_data['avgDailyRevenue'] = 0
                    channel_data['avgDailyRevenueUSD'] = 0
                    channel_data['avgDailySessions'] = 0
            
            # Sort channels by total revenue
            sorted_channels = sorted(channel_totals.values(), key=lambda x: x['totalRevenueUSD'], reverse=True)
            
            return {
                'propertyId': property_id,
                'period': period,
                'currency_info': {
                    'original_currency': property_currency,
                    'exchange_rates': currency_rates
                },
                'time_series': time_series_list,
                'channel_summary': sorted_channels,
                'channels_found': list(all_channels),
                'date_range': {
                    'start_date': start_date,
                    'end_date': end_date,
                    'total_days': len(time_series_list)
                },
                'totals': {
                    'total_revenue': sum(day['total_revenue'] for day in time_series_list),
                    'total_revenue_usd': sum(day['total_revenue_usd'] for day in time_series_list),
                    'total_sessions': sum(day['total_sessions'] for day in time_series_list),
                    'total_users': sum(day['total_users'] for day in time_series_list),
                    'total_conversions': sum(day['total_conversions'] for day in time_series_list)
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting channel revenue time series: {e}")
            return {
                'propertyId': property_id,
                'period': period,
                'error': str(e),
                'time_series': [],
                'channel_summary': [],
                'channels_found': []
            }

    def get_specific_channels_time_series(self, property_id: str, channels: List[str], period: str = "30d") -> Dict[str, Any]:
        """Get time series data for specific channels only"""
        try:
            # Get full time series data
            full_data = self.get_channel_revenue_time_series(property_id, period)
            
            if 'error' in full_data:
                return full_data
            
            # Filter for specific channels
            filtered_time_series = []
            for day_data in full_data['time_series']:
                filtered_day = {
                    'date': day_data['date'],
                    'channels': {},
                    'total_revenue': 0,
                    'total_revenue_usd': 0,
                    'total_sessions': 0,
                    'total_users': 0,
                    'total_conversions': 0
                }
                
                for channel in channels:
                    if channel in day_data['channels']:
                        channel_data = day_data['channels'][channel]
                        filtered_day['channels'][channel] = channel_data
                        filtered_day['total_revenue'] += channel_data['totalRevenue']
                        filtered_day['total_revenue_usd'] += channel_data['totalRevenueUSD']
                        filtered_day['total_sessions'] += channel_data['sessions']
                        filtered_day['total_users'] += channel_data['users']
                        filtered_day['total_conversions'] += channel_data['conversions']
                
                # Recalculate percentages based on filtered data
                for channel_data in filtered_day['channels'].values():
                    channel_data['revenuePercentage'] = (
                        (channel_data['totalRevenue'] / filtered_day['total_revenue'] * 100)
                        if filtered_day['total_revenue'] > 0 else 0
                    )
                
                filtered_time_series.append(filtered_day)
            
            # Filter channel summary
            filtered_summary = [
                channel for channel in full_data['channel_summary']
                if channel['channel'] in channels
            ]
            
            return {
                **full_data,
                'time_series': filtered_time_series,
                'channel_summary': filtered_summary,
                'channels_found': [ch for ch in channels if ch in full_data['channels_found']],
                'channels_requested': channels,
                'channels_not_found': [ch for ch in channels if ch not in full_data['channels_found']]
            }
            
        except Exception as e:
            logger.error(f"Error getting specific channels time series: {e}")
            return {
                'propertyId': property_id,
                'period': period,
                'error': str(e),
                'channels_requested': channels
            }
        
    def get_revenue_time_series(self, property_id: str, period: str = "30d", breakdown_by: str = "channel") -> Dict[str, Any]:
        """Get revenue breakdown by specified dimension over time (e.g., channel, source, device, location)"""
        try:
            # Map breakdown_by to GA4 dimension name
            dimension_map = {
                "channel": "sessionDefaultChannelGrouping",
                "source": "sessionSource",  # Or "sessionSourceMedium" if preferred
                "device": "deviceCategory",
                "location": "country"  # Or "region" or "city" for more granularity
            }
            if breakdown_by not in dimension_map:
                raise ValueError(f"Invalid breakdown_by: {breakdown_by}. Supported: {list(dimension_map.keys())}")
            
            group_dimension = dimension_map[breakdown_by]
            
            start_date, end_date = self.get_date_range(period)
            
            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimensions=[
                    Dimension(name="date"),
                    Dimension(name=group_dimension)
                ],
                metrics=[
                    Metric(name="totalRevenue"),
                    Metric(name="purchaseRevenue"),
                    Metric(name="sessions"),
                    Metric(name="totalUsers"),
                    Metric(name="conversions")
                ],
                order_bys=[
                    OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date")),
                    OrderBy(metric=OrderBy.MetricOrderBy(metric_name="totalRevenue"), desc=True)
                ]
            )
            
            response = self.client.run_report(request)
            
            # Get property currency for conversion
            property_currency = self.get_property_currency_enhanced(property_id)
            currency_rates = self.get_currency_rates()
            
            # Organize data by date and group (e.g., source/device/location)
            time_series_data = {}
            group_totals = {}
            date_totals = {}
            all_groups = set()
            
            for row in response.rows:
                date = row.dimension_values[0].value
                group = row.dimension_values[1].value
                total_revenue = self.safe_float(row.metric_values[0].value)
                purchase_revenue = self.safe_float(row.metric_values[1].value)
                sessions = self.safe_int(row.metric_values[2].value)
                users = self.safe_int(row.metric_values[3].value)
                conversions = self.safe_float(row.metric_values[4].value)
                
                # Convert revenue to USD
                total_revenue_usd = self.convert_to_usd(total_revenue, property_currency, currency_rates)
                purchase_revenue_usd = self.convert_to_usd(purchase_revenue, property_currency, currency_rates)
                
                # Track all groups
                all_groups.add(group)
                
                # Initialize date if not exists
                if date not in time_series_data:
                    time_series_data[date] = {
                        'date': date,
                        'groups': {},
                        'total_revenue': 0,
                        'total_revenue_usd': 0,
                        'total_sessions': 0,
                        'total_users': 0,
                        'total_conversions': 0
                    }
                
                # Add group data for this date
                time_series_data[date]['groups'][group] = {
                    'group': group,
                    'totalRevenue': total_revenue,
                    'totalRevenueUSD': total_revenue_usd,
                    'purchaseRevenue': purchase_revenue,
                    'purchaseRevenueUSD': purchase_revenue_usd,
                    'sessions': sessions,
                    'users': users,
                    'conversions': int(conversions)
                }
                
                # Update date totals
                time_series_data[date]['total_revenue'] += total_revenue
                time_series_data[date]['total_revenue_usd'] += total_revenue_usd
                time_series_data[date]['total_sessions'] += sessions
                time_series_data[date]['total_users'] += users
                time_series_data[date]['total_conversions'] += conversions
                
                # Update group totals
                if group not in group_totals:
                    group_totals[group] = {
                        'group': group,
                        'totalRevenue': 0,
                        'totalRevenueUSD': 0,
                        'totalSessions': 0,
                        'totalUsers': 0,
                        'totalConversions': 0,
                        'days_active': 0
                    }
                
                group_totals[group]['totalRevenue'] += total_revenue
                group_totals[group]['totalRevenueUSD'] += total_revenue_usd
                group_totals[group]['totalSessions'] += sessions
                group_totals[group]['totalUsers'] += users
                group_totals[group]['totalConversions'] += conversions
                group_totals[group]['days_active'] += 1
            
            # Calculate percentages and fill missing group data for each date
            for date_data in time_series_data.values():
                # Fill missing groups with zero values
                for group in all_groups:
                    if group not in date_data['groups']:
                        date_data['groups'][group] = {
                            'group': group,
                            'totalRevenue': 0,
                            'totalRevenueUSD': 0,
                            'purchaseRevenue': 0,
                            'purchaseRevenueUSD': 0,
                            'sessions': 0,
                            'users': 0,
                            'conversions': 0
                        }
                
                # Calculate percentages for each group on this date
                for group_data in date_data['groups'].values():
                    group_data['revenuePercentage'] = (
                        (group_data['totalRevenue'] / date_data['total_revenue'] * 100)
                        if date_data['total_revenue'] > 0 else 0
                    )
            
            # Convert to list format sorted by date
            time_series_list = sorted(time_series_data.values(), key=lambda x: x['date'])
            
            # Calculate group averages
            for group_data in group_totals.values():
                days_active = group_data['days_active']
                if days_active > 0:
                    group_data['avgDailyRevenue'] = group_data['totalRevenue'] / days_active
                    group_data['avgDailyRevenueUSD'] = group_data['totalRevenueUSD'] / days_active
                    group_data['avgDailySessions'] = group_data['totalSessions'] / days_active
                else:
                    group_data['avgDailyRevenue'] = 0
                    group_data['avgDailyRevenueUSD'] = 0
                    group_data['avgDailySessions'] = 0
            
            # Sort groups by total revenue
            sorted_groups = sorted(group_totals.values(), key=lambda x: x['totalRevenueUSD'], reverse=True)
            
            return {
                'propertyId': property_id,
                'period': period,
                'breakdown_by': breakdown_by,
                'currency_info': {
                    'original_currency': property_currency,
                    'exchange_rates': currency_rates
                },
                'time_series': time_series_list,
                'group_summary': sorted_groups,
                'groups_found': list(all_groups),
                'date_range': {
                    'start_date': start_date,
                    'end_date': end_date,
                    'total_days': len(time_series_list)
                },
                'totals': {
                    'total_revenue': sum(day['total_revenue'] for day in time_series_list),
                    'total_revenue_usd': sum(day['total_revenue_usd'] for day in time_series_list),
                    'total_sessions': sum(day['total_sessions'] for day in time_series_list),
                    'total_users': sum(day['total_users'] for day in time_series_list),
                    'total_conversions': sum(day['total_conversions'] for day in time_series_list)
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting revenue time series by {breakdown_by}: {e}")
            return {
                'propertyId': property_id,
                'period': period,
                'breakdown_by': breakdown_by,
                'error': str(e),
                'time_series': [],
                'group_summary': [],
                'groups_found': []
            }