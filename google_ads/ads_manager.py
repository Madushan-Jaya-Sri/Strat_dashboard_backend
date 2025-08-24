"""
Google Ads Manager
Handles all Google Ads API operations
"""

import os
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from fastapi import HTTPException
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.oauth2.credentials import Credentials
from auth.auth_manager import AuthManager

logger = logging.getLogger(__name__)

class GoogleAdsManager:
    """Manager class for Google Ads API operations"""
    
    def __init__(self, user_email: str,auth_manager):
        self.auth_manager = auth_manager

        self.user_email = user_email
        self.developer_token = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN")
        
        if not self.developer_token:
            raise ValueError("GOOGLE_ADS_DEVELOPER_TOKEN must be set")
        
        self.auth_manager = auth_manager
        self._client = None
    
    @property
    def client(self) -> GoogleAdsClient:
        """Get or create Google Ads client"""
        if not self._client:
            try:
                credentials = self.auth_manager.get_user_credentials(self.user_email)
                
                self._client = GoogleAdsClient(
                    credentials=credentials,
                    developer_token=self.developer_token
                )
                
                logger.info(f"Google Ads client created for {self.user_email}")
                
            except Exception as e:
                logger.error(f"Failed to create Google Ads client: {e}")
                raise HTTPException(status_code=500, detail=f"Google Ads API client initialization error: {str(e)}")
        
        return self._client
    
    def _get_date_filter(self, period: str) -> str:
        """Get the appropriate date filter for the given period"""
        if period in ["LAST_90_DAYS", "LAST_365_DAYS"]:  # UPDATE THIS LINE
            end_date = datetime.now().date()
            
            if period == "LAST_90_DAYS":
                start_date = end_date - timedelta(days=90)
            elif period == "LAST_365_DAYS":  # ADD THIS
                start_date = end_date - timedelta(days=365)
                
            return f"segments.date >= '{start_date}' AND segments.date <= '{end_date}'"
        else:
            return f"segments.date DURING {period}"
    
    def get_accessible_customers(self) -> List[Dict[str, Any]]:
        """Get list of accessible customer accounts"""
        try:
            customer_service = self.client.get_service("CustomerService")
            accessible_customers = customer_service.list_accessible_customers()
            
            customers = []
            for resource_name in accessible_customers.resource_names:
                customer_id = resource_name.split('/')[-1]
                try:
                    customer_info = self.get_customer_info(customer_id)
                    customers.append({
                        'id': customer_id,
                        'resource_name': resource_name,
                        'name': customer_info.get('name', f'Customer {customer_id}'),
                        'currency': customer_info.get('currency', 'USD'),
                        'time_zone': customer_info.get('time_zone', 'UTC'),
                        'is_manager': customer_info.get('is_manager', False)
                    })
                except Exception as e:
                    logger.warning(f"Could not get details for customer {customer_id}: {e}")
                    customers.append({
                        'id': customer_id,
                        'resource_name': resource_name,
                        'name': f'Customer {customer_id}',
                        'currency': 'USD',
                        'time_zone': 'UTC',
                        'is_manager': False
                    })
            
            logger.info(f"Found {len(customers)} accessible customers for {self.user_email}")
            return customers
            
        except GoogleAdsException as ex:
            logger.error(f"Google Ads API error getting customers: {ex}")
            if "CUSTOMER_NOT_FOUND" in str(ex):
                raise HTTPException(status_code=404, detail="No accessible Google Ads accounts found")
            elif "AUTHENTICATION_ERROR" in str(ex):
                raise HTTPException(status_code=401, detail="Authentication failed. Please sign in again")
            else:
                raise HTTPException(status_code=400, detail=f"Failed to get customers: {ex.error.message}")
        except Exception as e:
            logger.error(f"Unexpected error getting customers: {e}")
            raise HTTPException(status_code=500, detail="An unexpected error occurred while fetching customers")
    
    def get_customer_info(self, customer_id: str) -> Dict[str, Any]:
        """Get detailed customer information"""
        try:
            googleads_service = self.client.get_service("GoogleAdsService")
            query = """
                SELECT
                    customer.id,
                    customer.descriptive_name,
                    customer.currency_code,
                    customer.time_zone,
                    customer.manager
                FROM customer
                LIMIT 1
            """
            
            response = googleads_service.search(customer_id=customer_id, query=query)
            
            for row in response:
                customer = row.customer
                return {
                    'id': str(customer.id),
                    'name': customer.descriptive_name or f'Customer {customer.id}',
                    'currency': customer.currency_code,
                    'time_zone': customer.time_zone,
                    'is_manager': customer.manager
                }
            
            return {
                'id': customer_id,
                'name': f'Customer {customer_id}',
                'currency': 'USD',
                'time_zone': 'UTC',
                'is_manager': False
            }
            
        except GoogleAdsException as ex:
            logger.warning(f"Google Ads API error getting customer info for {customer_id}: {ex}")
            raise
        except Exception as e:
            logger.warning(f"Error getting customer info for {customer_id}: {e}")
            raise

    def get_campaigns_with_period(self, customer_id: str, period: str = "LAST_30_DAYS") -> List[Dict[str, Any]]:
        """Get campaigns for a customer with specified time period"""
        try:
            # Handle custom date ranges for 90 days and 1 year
            if period in ["LAST_90_DAYS", "LAST_365_DAYS"]:
                end_date = datetime.now().date()
                
                if period == "LAST_90_DAYS":
                    start_date = end_date - timedelta(days=90)
                elif period == "LAST_365_DAYS":  # ADD 1 YEAR SUPPORT
                    start_date = end_date - timedelta(days=365)
                
                query = f"""
                    SELECT
                        campaign.id,
                        campaign.name,
                        campaign.status,
                        campaign.advertising_channel_type,
                        campaign.start_date,
                        campaign.end_date,
                        metrics.impressions,
                        metrics.clicks,
                        metrics.cost_micros,
                        metrics.conversions,
                        metrics.ctr
                    FROM campaign
                    WHERE segments.date >= '{start_date}' AND segments.date <= '{end_date}'
                    ORDER BY metrics.impressions DESC
                """
            else:
                # Handle standard Google Ads periods (LAST_7_DAYS, LAST_30_DAYS)
                query = f"""
                    SELECT
                        campaign.id,
                        campaign.name,
                        campaign.status,
                        campaign.advertising_channel_type,
                        campaign.start_date,
                        campaign.end_date,
                        metrics.impressions,
                        metrics.clicks,
                        metrics.cost_micros,
                        metrics.conversions,
                        metrics.ctr
                    FROM campaign
                    WHERE segments.date DURING {period}
                    ORDER BY metrics.impressions DESC
                """
            
            googleads_service = self.client.get_service("GoogleAdsService")
            response = googleads_service.search(customer_id=customer_id, query=query)
            
            campaigns = []
            for row in response:
                campaign = row.campaign
                metrics = row.metrics
                
                campaigns.append({
                    'id': str(campaign.id),
                    'name': campaign.name,
                    'status': campaign.status.name if hasattr(campaign.status, 'name') else str(campaign.status),
                    'type': campaign.advertising_channel_type.name if hasattr(campaign.advertising_channel_type, 'name') else str(campaign.advertising_channel_type),
                    'start_date': campaign.start_date,
                    'end_date': campaign.end_date,
                    'impressions': metrics.impressions,
                    'clicks': metrics.clicks,
                    'cost': metrics.cost_micros / 1_000_000,
                    'conversions': metrics.conversions,
                    'ctr': round(metrics.ctr * 100, 2) if metrics.ctr else 0
                })
            
            logger.info(f"Found {len(campaigns)} campaigns for customer {customer_id} ({period})")
            return campaigns
            
        except GoogleAdsException as ex:
            logger.error(f"Google Ads API error getting campaigns: {ex}")
            raise HTTPException(status_code=400, detail=f"Failed to get campaigns: {ex.error.message}")
        except Exception as e:
            logger.error(f"Unexpected error getting campaigns: {e}")
            raise HTTPException(status_code=500, detail="An unexpected error occurred while fetching campaigns")
           
    def get_keywords_data(self, customer_id: str, period: str = "LAST_30_DAYS", offset: int = 0, limit: int = 10) -> Dict[str, Any]:
        """Get top performing keywords with pagination"""
        try:
            googleads_service = self.client.get_service("GoogleAdsService")
            date_filter = self._get_date_filter(period)
            
            query = f"""
                SELECT
                    ad_group_criterion.keyword.text,
                    ad_group_criterion.criterion_id,
                    metrics.clicks,
                    metrics.impressions,
                    metrics.cost_micros,
                    metrics.ctr,
                    metrics.average_cpc
                FROM keyword_view
                WHERE {date_filter}
                AND ad_group_criterion.status = 'ENABLED'
                AND ad_group_criterion.type = 'KEYWORD'
                ORDER BY metrics.clicks DESC
                LIMIT {limit + 1}
            """
            
            response = googleads_service.search(customer_id=customer_id, query=query)
            
            keywords = []
            all_results = list(response)
            
            start_idx = offset
            end_idx = offset + limit
            
            for i, row in enumerate(all_results):
                if i < start_idx:
                    continue
                if i >= end_idx:
                    break
                    
                metrics = row.metrics
                keyword = row.ad_group_criterion.keyword
                
                keywords.append({
                    'text': keyword.text,
                    'clicks': metrics.clicks,
                    'impressions': metrics.impressions,
                    'cost': metrics.cost_micros / 1_000_000,
                    'ctr': round(metrics.ctr * 100, 2) if metrics.ctr else 0,
                    'cpc': metrics.average_cpc / 1_000_000 if metrics.average_cpc else 0
                })
            
            has_more = len(all_results) > end_idx
            
            return {
                'keywords': keywords,
                'has_more': has_more,
                'total': len(all_results)
            }
            
        except GoogleAdsException as ex:
            logger.error(f"Google Ads API error getting keywords: {ex}")
            return {'keywords': [], 'has_more': False, 'total': 0}
        except Exception as e:
            logger.error(f"Error getting keywords for {customer_id}: {e}")
            return {'keywords': [], 'has_more': False, 'total': 0}
    
    def get_advanced_metrics(self, customer_id: str, period: str = "LAST_30_DAYS") -> List[Dict[str, Any]]:
        """Get advanced metrics like Quality Score, Impression Share, etc."""
        try:
            googleads_service = self.client.get_service("GoogleAdsService")
            date_filter = self._get_date_filter(period)
            
            query = f"""
                SELECT
                    campaign.id,
                    campaign.name,
                    metrics.impressions,
                    metrics.search_impression_share,
                    metrics.average_cpc,
                    metrics.average_cpm,
                    metrics.ctr,
                    metrics.cost_per_conversion
                FROM campaign
                WHERE {date_filter}
                AND campaign.status = 'ENABLED'
                ORDER BY metrics.impressions DESC
                LIMIT 10
            """
            
            response = googleads_service.search(customer_id=customer_id, query=query)
            
            metrics = []
            total_impression_share = 0
            total_ctr = 0
            total_cpc = 0
            count = 0
            
            for row in response:
                campaign_metrics = row.metrics
                if campaign_metrics.search_impression_share:
                    total_impression_share += campaign_metrics.search_impression_share
                if campaign_metrics.ctr:
                    total_ctr += campaign_metrics.ctr
                if campaign_metrics.average_cpc:
                    total_cpc += campaign_metrics.average_cpc / 1_000_000
                count += 1
            
            # Calculate averages
            avg_impression_share = (total_impression_share / count * 100) if count > 0 else 0
            avg_ctr = (total_ctr / count * 100) if count > 0 else 0
            avg_cpc = (total_cpc / count) if count > 0 else 0
            
            metrics.extend([
                {
                    'name': 'Search Impression Share',
                    'value': f"{avg_impression_share:.1f}%",
                    'performance': 'High' if avg_impression_share >= 70 else 'Medium' if avg_impression_share >= 50 else 'Low'
                },
                {
                    'name': 'Average CTR',
                    'value': f"{avg_ctr:.2f}%",
                    'performance': 'High' if avg_ctr >= 3 else 'Medium' if avg_ctr >= 1 else 'Low'
                },
                {
                    'name': 'Average CPC',
                    'value': f"${avg_cpc:.2f}",
                    'performance': 'Low' if avg_cpc <= 1 else 'Medium' if avg_cpc <= 3 else 'High'
                }
            ])
            
            return metrics
            
        except GoogleAdsException as ex:
            logger.error(f"Google Ads API error getting advanced metrics: {ex}")
            return []
        except Exception as e:
            logger.error(f"Error getting advanced metrics for {customer_id}: {e}")
            return []
    
    def get_geographic_data(self, customer_id: str, period: str = "LAST_30_DAYS") -> List[Dict[str, Any]]:
        """Get campaign performance by geographic location"""
        try:
            googleads_service = self.client.get_service("GoogleAdsService")
            date_filter = self._get_date_filter(period)
            
            query = f"""
                SELECT
                    geographic_view.country_criterion_id,
                    geographic_view.location_type,
                    metrics.clicks,
                    metrics.impressions,
                    metrics.cost_micros
                FROM geographic_view
                WHERE {date_filter}
                ORDER BY metrics.clicks DESC
                LIMIT 20
            """
            
            response = googleads_service.search(customer_id=customer_id, query=query)
            
            geo_data = []
            for row in response:
                metrics = row.metrics
                geographic = row.geographic_view
                
                location_name = f"Location {geographic.country_criterion_id}"
                
                geo_data.append({
                    'location_name': location_name,
                    'clicks': metrics.clicks,
                    'impressions': metrics.impressions,
                    'cost': metrics.cost_micros / 1_000_000
                })
            
            return geo_data
            
        except GoogleAdsException as ex:
            logger.error(f"Google Ads API error getting geographic data: {ex}")
            return []
        except Exception as e:
            logger.error(f"Error getting geographic data for {customer_id}: {e}")
            return []
    
    def get_device_performance_data(self, customer_id: str, period: str = "LAST_30_DAYS") -> List[Dict[str, Any]]:
        """Get campaign performance by device type"""
        try:
            googleads_service = self.client.get_service("GoogleAdsService")
            date_filter = self._get_date_filter(period)
            
            query = f"""
                SELECT
                    segments.device,
                    metrics.clicks,
                    metrics.impressions,
                    metrics.cost_micros
                FROM campaign
                WHERE {date_filter}
                ORDER BY metrics.clicks DESC
            """
            
            response = googleads_service.search(customer_id=customer_id, query=query)
            
            device_data = {}
            for row in response:
                device = row.segments.device.name if hasattr(row.segments.device, 'name') else str(row.segments.device)
                metrics = row.metrics
                
                if device not in device_data:
                    device_data[device] = {
                        'device': device,
                        'clicks': 0,
                        'impressions': 0,
                        'cost': 0
                    }
                
                device_data[device]['clicks'] += metrics.clicks
                device_data[device]['impressions'] += metrics.impressions
                device_data[device]['cost'] += metrics.cost_micros / 1_000_000
            
            return list(device_data.values())
            
        except Exception as e:
            logger.error(f"Error getting device performance for {customer_id}: {e}")
            return []
    
    def get_time_performance_data(self, customer_id: str, period: str = "LAST_30_DAYS") -> List[Dict[str, Any]]:
        """Get daily campaign performance over time"""
        try:
            googleads_service = self.client.get_service("GoogleAdsService")
            date_filter = self._get_date_filter(period)
            
            query = f"""
                SELECT
                    campaign.id,
                    campaign.name,
                    segments.date,
                    metrics.clicks,
                    metrics.impressions,
                    metrics.cost_micros
                FROM campaign
                WHERE {date_filter}
                ORDER BY segments.date DESC
            """
            
            response = googleads_service.search(customer_id=customer_id, query=query)
            
            daily_data = {}
            for row in response:
                metrics = row.metrics
                date = row.segments.date
                
                if date not in daily_data:
                    daily_data[date] = {
                        'date': date,
                        'clicks': 0,
                        'impressions': 0,
                        'cost': 0
                    }
                
                daily_data[date]['clicks'] += metrics.clicks
                daily_data[date]['impressions'] += metrics.impressions
                daily_data[date]['cost'] += metrics.cost_micros / 1_000_000
            
            result = sorted(list(daily_data.values()), key=lambda x: x['date'])
            logger.info(f"Found {len(result)} days of performance data for customer {customer_id} ({period})")
            return result
            
        except GoogleAdsException as ex:
            logger.error(f"Google Ads API error getting time performance: {ex}")
            return []
        except Exception as e:
            logger.error(f"Error getting time performance for {customer_id}: {e}")
            return []
    
    def get_keyword_ideas(self, customer_id: str, keywords: List[str], location_id: str = "2840") -> List[Dict[str, Any]]:
        """Get keyword ideas and metrics"""
        try:
            keyword_plan_idea_service = self.client.get_service("KeywordPlanIdeaService")
            googleads_service = self.client.get_service("GoogleAdsService")
            
            request = self.client.get_type("GenerateKeywordIdeasRequest")
            request.customer_id = customer_id
            request.language = googleads_service.language_constant_path("1000")  # English
            
            if location_id:
                request.geo_target_constants.append(googleads_service.geo_target_constant_path(location_id))
            
            request.include_adult_keywords = False
            request.keyword_plan_network = self.client.enums.KeywordPlanNetworkEnum.GOOGLE_SEARCH_AND_PARTNERS
            
            if keywords:
                request.keyword_seed.keywords.extend(keywords)
            else:
                raise ValueError("Keywords must be provided for keyword idea generation")
            
            keyword_ideas_response = keyword_plan_idea_service.generate_keyword_ideas(request=request)
            
            ideas = []
            for idea in keyword_ideas_response:
                metrics = idea.keyword_idea_metrics
                ideas.append({
                    'keyword': idea.text,
                    'avg_monthly_searches': metrics.avg_monthly_searches,
                    'competition': metrics.competition.name if hasattr(metrics.competition, 'name') else str(metrics.competition),
                    'competition_index': metrics.competition_index,
                    'low_top_of_page_bid': metrics.low_top_of_page_bid_micros / 1_000_000 if metrics.low_top_of_page_bid_micros else 0,
                    'high_top_of_page_bid': metrics.high_top_of_page_bid_micros / 1_000_000 if metrics.high_top_of_page_bid_micros else 0
                })
            
            logger.info(f"Found {len(ideas)} keyword ideas for customer {customer_id}")
            return ideas
            
        except ValueError as ve:
            logger.error(f"Validation error for keyword ideas: {ve}")
            raise HTTPException(status_code=400, detail=str(ve))
        except GoogleAdsException as ex:
            logger.error(f"Google Ads API error getting keyword ideas: {ex}")
            raise HTTPException(status_code=400, detail=f"Failed to get keyword ideas: {ex.error.message}")
        except Exception as e:
            logger.error(f"Unexpected error getting keyword ideas: {e}")
            raise HTTPException(status_code=500, detail="An unexpected error occurred while fetching keyword ideas")
        
    def get_total_cost_for_period(self, customer_id: str, period: str) -> float:
        """Get total ad spend for a specific period"""
        try:
            # Use existing method to get campaigns with costs
            campaigns = self.get_campaigns_with_period(customer_id, period)
            
            # Sum up all campaign costs
            total_cost = sum(campaign.get('cost', 0) for campaign in campaigns)
            
            return total_cost
            
        except Exception as e:
            logger.error(f"Error fetching total ad cost for customer {customer_id}: {e}")
            return 0.0
            
    def get_overall_key_stats(self, customer_id: str, period: str = "LAST_30_DAYS") -> Dict[str, Any]:
            """Get overall key statistics for dashboard cards"""
            try:
                googleads_service = self.client.get_service("GoogleAdsService")
                date_filter = self._get_date_filter(period)
                
                # Get overall campaign metrics
                query = f"""
                    SELECT
                        metrics.impressions,
                        metrics.clicks,
                        metrics.cost_micros,
                        metrics.conversions,
                        metrics.ctr,
                        metrics.average_cpc
                    FROM campaign
                    WHERE {date_filter}
                    AND campaign.status != 'REMOVED'
                """
                
                response = googleads_service.search(customer_id=customer_id, query=query)
                
                # Aggregate all metrics
                total_impressions = 0
                total_clicks = 0
                total_cost = 0.0
                total_conversions = 0.0
                total_ctr_sum = 0.0
                total_cpc_sum = 0.0
                row_count = 0
                
                for row in response:
                    metrics = row.metrics
                    total_impressions += metrics.impressions
                    total_clicks += metrics.clicks
                    total_cost += metrics.cost_micros / 1_000_000
                    total_conversions += metrics.conversions
                    total_ctr_sum += metrics.ctr
                    total_cpc_sum += metrics.average_cpc / 1_000_000 if metrics.average_cpc else 0
                    row_count += 1
                
                # Calculate derived metrics
                overall_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
                conversion_rate = (total_conversions / total_clicks * 100) if total_clicks > 0 else 0
                avg_cpc = total_cpc_sum / row_count if row_count > 0 else 0
                cost_per_conversion = total_cost / total_conversions if total_conversions > 0 else 0
                
                # Format numbers for display
                from utils.helpers import format_large_number, format_currency
                
                key_stats = {
                    'total_impressions': {
                        'value': total_impressions,
                        'formatted': format_large_number(total_impressions),
                        'label': 'TOTAL IMPRESSIONS',
                        'description': 'Reach and Visibility'
                    },
                    'total_cost': {
                        'value': total_cost,
                        'formatted': format_currency(total_cost),
                        'label': 'TOTAL COST',
                        'description': 'Budget utilization'
                    },
                    'total_clicks': {
                        'value': total_clicks,
                        'formatted': format_large_number(total_clicks),
                        'label': 'TOTAL CLICKS',
                        'description': 'User engagement'
                    },
                    'conversion_rate': {
                        'value': conversion_rate,
                        'formatted': f"{conversion_rate:.2f}%",
                        'label': 'CONVERSION RATE',
                        'description': 'Campaign effectiveness'
                    },
                    'total_conversions': {
                        'value': total_conversions,
                        'formatted': f"{total_conversions:.1f}",
                        'label': 'TOTAL CONVERSIONS',
                        'description': 'Goal achievements'
                    },
                    'avg_cost_per_click': {
                        'value': avg_cpc,
                        'formatted': format_currency(avg_cpc),
                        'label': 'AVG. COST PER CLICK',
                        'description': 'Bidding efficiency'
                    },
                    'cost_per_conversion': {
                        'value': cost_per_conversion,
                        'formatted': format_currency(cost_per_conversion),
                        'label': 'COST PER CONV.',
                        'description': 'ROI efficiency'
                    },
                    'click_through_rate': {
                        'value': overall_ctr,
                        'formatted': f"{overall_ctr:.2f}%",
                        'label': 'CLICK-THROUGH RATE',
                        'description': 'Ad relevance'
                    }
                }
                
                # Add summary info
                key_stats['summary'] = {
                    'period': period,
                    'customer_id': customer_id,
                    'campaigns_count': row_count,
                    'generated_at': datetime.now().isoformat()
                }
                
                logger.info(f"Generated key stats for customer {customer_id} ({period})")
                return key_stats
                
            except GoogleAdsException as ex:
                logger.error(f"Google Ads API error getting key stats: {ex}")
                raise HTTPException(status_code=400, detail=f"Failed to get key stats: {ex.error.message}")
            except Exception as e:
                logger.error(f"Unexpected error getting key stats: {e}")
                raise HTTPException(status_code=500, detail="An unexpected error occurred while fetching key stats")