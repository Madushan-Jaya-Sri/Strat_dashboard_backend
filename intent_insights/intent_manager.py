import os
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
from fastapi import HTTPException
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from auth.auth_manager import AuthManager
from utils.helpers import get_country_location_id

logger = logging.getLogger(__name__)

class IntentManager:
    """Simplified manager that returns raw Google Ads API responses"""
    
    def __init__(self, user_email: str, auth_manager):
        self.auth_manager = auth_manager
        self.user_email = user_email
        self.developer_token = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN")
        
        if not self.developer_token:
            raise ValueError("GOOGLE_ADS_DEVELOPER_TOKEN must be set")
        
        self._client = None
        self.country_codes = get_country_location_id()
    
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
                raise HTTPException(status_code=500, detail=f"Google Ads API error: {str(e)}")
        
        return self._client
    
    def get_location_id(self, country: str) -> str:
        """Get location ID for country, return None for worldwide"""
        if country.lower() in ["world wide", "worldwide", "global", "all countries"]:
            return None
        return self.country_codes.get(country, "2840")
    
    def _validate_date_range(self, start_date: str, end_date: str) -> None:
        """Basic date validation"""
        try:
            start_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
            
            if start_obj > end_obj:
                raise HTTPException(status_code=400, detail="Start date must be before end date")
            
            if (end_obj - start_obj).days > 730:
                raise HTTPException(status_code=400, detail="Date range cannot exceed 2 years")
                
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    
    def _calculate_date_range(self, timeframe: str, start_date: str = None, end_date: str = None) -> Tuple[str, str]:
        """Calculate date range based on timeframe"""
        today = datetime.now().date()
        
        if timeframe == "custom" and start_date and end_date:
            self._validate_date_range(start_date, end_date)
            return start_date, end_date
        
        elif timeframe == "1_month":
            end_date = (today.replace(day=1) - timedelta(days=1))
            start_date = end_date.replace(day=1)
            
        elif timeframe == "3_months":
            end_date = (today.replace(day=1) - timedelta(days=1))
            start_date = (end_date.replace(day=1) - timedelta(days=90)).replace(day=1)
            
        elif timeframe == "12_months":
            end_date = (today.replace(day=1) - timedelta(days=1))
            start_date = (end_date.replace(day=1) - timedelta(days=365)).replace(day=1)
        
        else:
            # Default to 12 months
            end_date = (today.replace(day=1) - timedelta(days=1))
            start_date = (end_date.replace(day=1) - timedelta(days=365)).replace(day=1)
        
        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
    
    def _convert_competition_to_text(self, competition_value):
        """Convert competition enum to readable text"""
        try:
            if hasattr(competition_value, 'name'):
                return competition_value.name
            else:
                comp_int = int(competition_value)
                if comp_int == 1: return "LOW"
                elif comp_int == 2: return "MEDIUM"
                elif comp_int == 3: return "HIGH"
                else: return "UNKNOWN"
        except:
            return "UNKNOWN"
    
    def get_keyword_insights(self, customer_id: str, seed_keywords: List[str], 
                            country: str, timeframe: str, start_date: str = None, 
                            end_date: str = None, include_zero_volume: bool = False) -> Dict[str, Any]:
        """Get raw keyword insights from Google Ads API"""
        
        location_id = self.get_location_id(country)
        calculated_start, calculated_end = self._calculate_date_range(timeframe, start_date, end_date)
        
        try:
            # Get keyword ideas
            keyword_ideas_response = self._get_keyword_ideas_raw(customer_id, seed_keywords, location_id)
            
            # Get historical metrics ONLY for seed keywords
            historical_response = self._get_historical_metrics_raw(
                customer_id, seed_keywords, location_id, calculated_start, calculated_end
            )
            
            # Return raw API responses
            return {
                "request_info": {
                    "customer_id": customer_id,
                    "seed_keywords": seed_keywords,
                    "country": country,
                    "location_id": location_id,
                    "timeframe": timeframe,
                    "date_range": f"{calculated_start} to {calculated_end}",
                    "include_zero_volume": include_zero_volume,
                    "generated_at": datetime.now().isoformat()
                },
                "keyword_ideas_raw": keyword_ideas_response,
                "historical_metrics_raw": historical_response
            }
            
        except Exception as e:
            logger.error(f"Error getting keyword insights: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    def _get_keyword_ideas_raw(self, customer_id: str, keywords: List[str], location_id: str) -> Dict[str, Any]:
        """Get raw keyword ideas response from API"""
        try:
            keyword_plan_idea_service = self.client.get_service("KeywordPlanIdeaService")
            googleads_service = self.client.get_service("GoogleAdsService")
            
            request = self.client.get_type("GenerateKeywordIdeasRequest")
            request.customer_id = customer_id
            request.language = googleads_service.language_constant_path("1000")
            
            # Only add geo targeting if location_id is provided (not worldwide)
            if location_id:
                request.geo_target_constants.append(googleads_service.geo_target_constant_path(location_id))
            
            request.include_adult_keywords = False
            request.keyword_plan_network = self.client.enums.KeywordPlanNetworkEnum.GOOGLE_SEARCH_AND_PARTNERS
            
            request.keyword_seed.keywords.extend(keywords)
            request.page_size = 10000
            
            response = keyword_plan_idea_service.generate_keyword_ideas(request=request)
            
            # Convert response to dict format
            raw_data = {
                "total_results": len(response.results),
                "results": []
            }
            
            for idea in response.results:
                result_data = {
                    "keyword_text": idea.text if hasattr(idea, 'text') else "",
                }
                
                if hasattr(idea, 'keyword_idea_metrics') and idea.keyword_idea_metrics:
                    metrics = idea.keyword_idea_metrics
                    
                    # Convert micros to dollars
                    low_bid_dollars = round(int(metrics.low_top_of_page_bid_micros) / 1_000_000, 2) if metrics.low_top_of_page_bid_micros else 0.0
                    high_bid_dollars = round(int(metrics.high_top_of_page_bid_micros) / 1_000_000, 2) if metrics.high_top_of_page_bid_micros else 0.0
                    
                    result_data["metrics"] = {
                        "avg_monthly_searches": int(metrics.avg_monthly_searches) if metrics.avg_monthly_searches else 0,
                        "competition": self._convert_competition_to_text(metrics.competition),
                        "competition_index": float(metrics.competition_index) if metrics.competition_index else 0.0,
                        "low_top_of_page_bid_dollars": low_bid_dollars,
                        "high_top_of_page_bid_dollars": high_bid_dollars
                    }
                
                raw_data["results"].append(result_data)
            
            return raw_data
            
        except Exception as e:
            logger.error(f"Error getting keyword ideas: {e}")
            return {"error": str(e), "results": []}
    
    def _get_historical_metrics_raw(self, customer_id: str, keywords: List[str], 
                                   location_id: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """Get raw historical metrics response from API"""
        try:
            keyword_plan_idea_service = self.client.get_service("KeywordPlanIdeaService")
            googleads_service = self.client.get_service("GoogleAdsService")
            
            request = self.client.get_type("GenerateKeywordHistoricalMetricsRequest")
            request.customer_id = customer_id
            request.keywords.extend(keywords)
            request.language = googleads_service.language_constant_path("1000")
            
            # Only add geo targeting if location_id is provided (not worldwide)
            if location_id:
                request.geo_target_constants.append(googleads_service.geo_target_constant_path(location_id))
            
            request.keyword_plan_network = self.client.enums.KeywordPlanNetworkEnum.GOOGLE_SEARCH
            request.include_adult_keywords = False
            
            # Set date range
            start_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
            
            request.historical_metrics_options.year_month_range.start.year = start_obj.year
            request.historical_metrics_options.year_month_range.start.month = start_obj.month
            request.historical_metrics_options.year_month_range.end.year = end_obj.year
            request.historical_metrics_options.year_month_range.end.month = end_obj.month
            
            response = keyword_plan_idea_service.generate_keyword_historical_metrics(request=request)
            
            # Convert response to dict format
            raw_data = {
                "date_range": f"{start_date} to {end_date}",
                "total_results": len(response.results),
                "results": []
            }
            
            for result in response.results:
                result_data = {
                    "keyword_text": result.text if hasattr(result, 'text') else ""
                }
                
                if hasattr(result, 'keyword_metrics') and result.keyword_metrics:
                    metrics = result.keyword_metrics
                    
                    # Convert micros to dollars
                    low_bid_dollars = round(int(metrics.low_top_of_page_bid_micros) / 1_000_000, 2) if metrics.low_top_of_page_bid_micros else 0.0
                    high_bid_dollars = round(int(metrics.high_top_of_page_bid_micros) / 1_000_000, 2) if metrics.high_top_of_page_bid_micros else 0.0
                    
                    result_data["keyword_metrics"] = {
                        "avg_monthly_searches": int(metrics.avg_monthly_searches) if metrics.avg_monthly_searches else 0,
                        "competition": self._convert_competition_to_text(metrics.competition),
                        "competition_index": float(metrics.competition_index) if metrics.competition_index else 0.0,
                        "low_top_of_page_bid_dollars": low_bid_dollars,
                        "high_top_of_page_bid_dollars": high_bid_dollars,
                        "monthly_search_volumes": []
                    }
                    
                    # Extract monthly search volumes
                    if hasattr(metrics, 'monthly_search_volumes'):
                        for monthly_volume in metrics.monthly_search_volumes:
                            if hasattr(monthly_volume, 'year') and hasattr(monthly_volume, 'month'):
                                monthly_data = {
                                    "year": int(monthly_volume.year),
                                    "month": int(monthly_volume.month),
                                    "monthly_searches": int(monthly_volume.monthly_searches) if monthly_volume.monthly_searches else 0
                                }
                                result_data["keyword_metrics"]["monthly_search_volumes"].append(monthly_data)
                
                raw_data["results"].append(result_data)
            
            return raw_data
            
        except Exception as e:
            logger.error(f"Error getting historical metrics: {e}")
            return {"error": str(e), "results": []}