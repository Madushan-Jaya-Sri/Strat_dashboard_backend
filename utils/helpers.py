"""
Utility helper functions for the Unified Marketing Dashboard
"""

from datetime import datetime, timedelta
from typing import Tuple, Dict, Any, List

def get_date_range(period: str) -> Tuple[str, str]:
    """Get date range based on period string"""
    end_date = datetime.now()
    
    if period == "7d":
        start_date = end_date - timedelta(days=7)
    elif period == "90d":
        start_date = end_date - timedelta(days=90)
    elif period == "365d":
        start_date = end_date - timedelta(days=365)
    else:  # default 30d
        start_date = end_date - timedelta(days=30)
    
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

def safe_float(value, default=0.0):
    """Safely convert to float"""
    try:
        return float(value) if value else default
    except (ValueError, TypeError):
        return default

def safe_int(value, default=0):
    """Safely convert to int"""
    try:
        return int(float(value)) if value else default
    except (ValueError, TypeError):
        return default

def format_currency(amount: float, currency: str = "USD") -> str:
    """Format currency amount"""
    if currency == "USD":
        return f"${amount:,.2f}"
    elif currency == "EUR":
        return f"€{amount:,.2f}"
    elif currency == "GBP":
        return f"£{amount:,.2f}"
    else:
        return f"{amount:,.2f} {currency}"

def format_percentage(value: float) -> str:
    """Format percentage value"""
    return f"{value:.2f}%"

def calculate_percentage_change(current: float, previous: float) -> float:
    """Calculate percentage change between two values"""
    if previous == 0:
        return 0.0
    return ((current - previous) / previous) * 100

def format_large_number(number: int) -> str:
    """Format large numbers with K, M, B suffixes"""
    if number >= 1_000_000_000:
        return f"{number/1_000_000_000:.1f}B"
    elif number >= 1_000_000:
        return f"{number/1_000_000:.1f}M"
    elif number >= 1_000:
        return f"{number/1_000:.1f}K"
    else:
        return str(number)

def convert_ads_period_to_ga_period(ads_period: str) -> str:
    """Convert Google Ads period format to GA4 period format"""
    mapping = {
        "LAST_7_DAYS": "7d",
        "LAST_30_DAYS": "30d",
        "LAST_90_DAYS": "90d",
        "LAST_365_DAYS": "365d"
    }
    return mapping.get(ads_period, "30d")

def convert_ga_period_to_ads_period(ga_period: str) -> str:
    """Convert GA4 period format to Google Ads period format"""
    mapping = {
        "7d": "LAST_7_DAYS",
        "30d": "LAST_30_DAYS",
        "90d": "LAST_90_DAYS",
        "365d": "LAST_365_DAYS"
    }
    return mapping.get(ga_period, "LAST_30_DAYS")

def calculate_roas(revenue: float, ad_spend: float) -> float:
    """Calculate Return on Ad Spend (Revenue / Ad Spend)"""
    return (revenue / ad_spend) if ad_spend > 0 else 0.0

def calculate_roi(revenue: float, ad_spend: float) -> float:
    """Calculate Return on Investment ((Revenue - Cost) / Cost * 100)"""
    return ((revenue - ad_spend) / ad_spend * 100) if ad_spend > 0 else 0.0

def format_roas(roas: float) -> str:
    """Format ROAS value"""
    return f"{roas:.2f}:1"

def format_roi(roi: float) -> str:
    """Format ROI value as percentage"""
    return f"{roi:.1f}%"

def get_roas_benchmark(roas: float) -> str:
    """Get ROAS benchmark status"""
    if roas >= 4.0:
        return "Above Industry Average"
    elif roas >= 2.0:
        return "Industry Average"
    else:
        return "Below Industry Average"

def get_roi_benchmark(roi: float) -> str:
    """Get ROI benchmark status"""
    if roi >= 200:
        return "Above Industry Average"
    elif roi >= 100:
        return "Industry Average"
    else:
        return "Below Industry Average"

def validate_timeframe(timeframe: str, start_date: str = None, end_date: str = None) -> bool:
    """Validate timeframe parameters"""
    valid_timeframes = ["1_month", "3_months", "12_months", "custom"]
    
    if timeframe not in valid_timeframes:
        return False
    
    if timeframe == "custom":
        if not start_date or not end_date:
            return False
        
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
            
            if start >= end:
                return False
            
            # Check if date range is not too large (max 24 months)
            if (end - start).days > 730:
                return False
                
        except ValueError:
            return False
    
    return True

def get_country_location_id(country_name: str) -> str:
    """Get Google Ads location ID for country"""
    country_mapping = {
        "Sri Lanka": "2144",
        "United States": "2840", 
        "United Kingdom": "2826",
        "Canada": "2124",
        "Australia": "2036",
        "India": "2356",
        "Singapore": "2702",
        "Malaysia": "2458",
        "Thailand": "2764",
        "Philippines": "2608",
        "Germany": "2276",
        "France": "2250",
        "Japan": "2392",
        "South Korea": "2410",
        "Brazil": "2076",
        "Mexico": "2484",
        "Netherlands": "2528",
        "Spain": "2724",
        "Italy": "2380",
        "Indonesia": "2360",
        "Vietnam": "2704",
        "Bangladesh": "2050",
        "Pakistan": "2586",
        "Myanmar": "2104",
        "Cambodia": "2116"
    }
    
    return country_mapping.get(country_name, "2144")  # Default to Sri Lanka

def format_search_volume(volume: int) -> str:
    """Format search volume with appropriate suffixes"""
    if volume >= 1_000_000:
        return f"{volume/1_000_000:.1f}M"
    elif volume >= 1_000:
        return f"{volume/1_000:.1f}K"
    else:
        return str(volume)

def calculate_trend_strength(change_percentage: float) -> str:
    """Calculate trend strength based on percentage change"""
    abs_change = abs(change_percentage)
    
    if abs_change >= 50:
        return "Very Strong"
    elif abs_change >= 25:
        return "Strong"
    elif abs_change >= 10:
        return "Moderate"
    elif abs_change >= 5:
        return "Weak"
    else:
        return "Minimal"

def get_competition_level_description(competition: str, competition_index: float) -> str:
    """Get detailed competition description"""
    base_descriptions = {
        "LOW": "Low competition - easier to rank",
        "MEDIUM": "Medium competition - moderate difficulty",
        "HIGH": "High competition - challenging to rank"
    }
    
    base = base_descriptions.get(competition.upper(), "Unknown competition level")
    
    if competition_index:
        if competition_index <= 33:
            detail = " (relatively accessible)"
        elif competition_index <= 66:
            detail = " (moderately competitive)"
        else:
            detail = " (highly competitive)"
        
        return f"{base}{detail}"
    
    return base

def calculate_keyword_opportunity_score(avg_searches: int, competition_index: float, 
                                      trend_direction: str) -> int:
    """Calculate opportunity score for keywords (0-100)"""
    # Base score from search volume (max 40 points)
    if avg_searches >= 100000:
        volume_score = 40
    elif avg_searches >= 10000:
        volume_score = 30
    elif avg_searches >= 1000:
        volume_score = 20
    elif avg_searches >= 100:
        volume_score = 10
    else:
        volume_score = 5
    
    # Competition score (max 30 points, lower competition = higher score)
    competition_score = max(0, 30 - competition_index * 0.3)
    
    # Trend score (max 30 points)
    trend_scores = {
        "Strong Upward": 30,
        "Upward": 20,
        "Stable": 15,
        "Downward": 10,
        "Strong Downward": 5
    }
    trend_score = trend_scores.get(trend_direction, 15)
    
    total_score = min(100, int(volume_score + competition_score + trend_score))
    return total_score

def get_keyword_recommendation(opportunity_score: int, competition: str, avg_searches: int) -> str:
    """Get keyword recommendation based on metrics"""
    if opportunity_score >= 80:
        return "Highly Recommended - Great opportunity with good search volume and manageable competition"
    elif opportunity_score >= 60:
        return "Recommended - Good balance of opportunity and competition"
    elif opportunity_score >= 40:
        return "Consider - Moderate opportunity, evaluate based on your specific goals"
    elif opportunity_score >= 20:
        return "Low Priority - High competition or low search volume"
    else:
        return "Not Recommended - Very challenging keyword with limited opportunity"

def parse_monthly_volumes_to_chart_data(monthly_volumes: Dict[str, int]) -> List[Dict[str, Any]]:
    """Convert monthly volumes to chart-ready format"""
    chart_data = []
    for month, volume in monthly_volumes.items():
        chart_data.append({
            "month": month,
            "volume": volume,
            "formatted_volume": format_search_volume(volume)
        })
    return chart_data

def calculate_seasonality_index(monthly_volumes: Dict[str, int]) -> Dict[str, Any]:
    """Calculate seasonality patterns for keywords"""
    if not monthly_volumes:
        return {"has_seasonality": False, "peak_months": [], "low_months": []}
    
    volumes = list(monthly_volumes.values())
    months = list(monthly_volumes.keys())
    
    if len(volumes) < 3:
        return {"has_seasonality": False, "peak_months": [], "low_months": []}
    
    avg_volume = sum(volumes) / len(volumes)
    max_volume = max(volumes)
    min_volume = min(volumes)
    
    # Check if there's significant variation (>30% from average)
    variation_threshold = avg_volume * 0.3
    has_seasonality = (max_volume - min_volume) > variation_threshold
    
    peak_months = [months[i] for i, v in enumerate(volumes) if v >= avg_volume + variation_threshold]
    low_months = [months[i] for i, v in enumerate(volumes) if v <= avg_volume - variation_threshold]
    
    return {
        "has_seasonality": has_seasonality,
        "peak_months": peak_months,
        "low_months": low_months,
        "average_volume": int(avg_volume),
        "peak_volume": max_volume,
        "low_volume": min_volume,
        "variation_coefficient": round((max_volume - min_volume) / avg_volume * 100, 2) if avg_volume > 0 else 0
    }

def get_keyword_difficulty_level(competition_index: float) -> str:
    """Get keyword difficulty level based on competition index"""
    if competition_index <= 20:
        return "Very Easy"
    elif competition_index <= 40:
        return "Easy"
    elif competition_index <= 60:
        return "Medium"
    elif competition_index <= 80:
        return "Hard"
    else:
        return "Very Hard"

def calculate_cost_per_click_range(low_bid: float, high_bid: float) -> Dict[str, str]:
    """Calculate CPC range information"""
    avg_cpc = (low_bid + high_bid) / 2 if high_bid > 0 else low_bid
    
    return {
        "low_cpc": format_currency(low_bid),
        "high_cpc": format_currency(high_bid),
        "avg_cpc": format_currency(avg_cpc),
        "range_width": format_currency(high_bid - low_bid) if high_bid > low_bid else "$0.00"
    }

def get_search_volume_category(volume: int) -> str:
    """Categorize search volume"""
    if volume >= 100000:
        return "Very High Volume"
    elif volume >= 10000:
        return "High Volume"
    elif volume >= 1000:
        return "Medium Volume"
    elif volume >= 100:
        return "Low Volume"
    else:
        return "Very Low Volume"

def calculate_market_share_potential(keyword_volume: int, total_market_volume: int) -> float:
    """Calculate potential market share for a keyword"""
    if total_market_volume == 0:
        return 0.0
    return (keyword_volume / total_market_volume) * 100

def get_bid_strategy_recommendation(competition_index: float, avg_searches: int) -> str:
    """Get bidding strategy recommendation"""
    if competition_index <= 30 and avg_searches >= 1000:
        return "Aggressive bidding recommended - Low competition, high volume opportunity"
    elif competition_index <= 50 and avg_searches >= 500:
        return "Moderate bidding - Balanced competition and volume"
    elif competition_index >= 70:
        return "Conservative bidding - High competition market"
    elif avg_searches < 100:
        return "Low bid strategy - Limited search volume"
    else:
        return "Standard bidding approach"

def format_trend_indicator(yoy_change: str, three_month_change: str) -> Dict[str, str]:
    """Format trend indicators for display"""
    def get_trend_icon(change_str: str) -> str:
        if change_str == "N/A":
            return "➖"
        
        # Extract numeric value
        try:
            value = float(change_str.replace('%', '').replace('+', ''))
            if value > 10:
                return "📈"  # Strong upward
            elif value > 0:
                return "↗️"  # Upward
            elif value < -10:
                return "📉"  # Strong downward
            elif value < 0:
                return "↘️"  # Downward
            else:
                return "➖"  # Stable
        except:
            return "❓"
    
    return {
        "yoy_display": f"{get_trend_icon(yoy_change)} {yoy_change}",
        "three_month_display": f"{get_trend_icon(three_month_change)} {three_month_change}",
        "yoy_icon": get_trend_icon(yoy_change),
        "three_month_icon": get_trend_icon(three_month_change)
    }

def calculate_keyword_priority_score(opportunity_score: int, search_volume: int, 
                                   relevance_score: float = 1.0) -> int:
    """Calculate overall keyword priority score"""
    # Base score from opportunity
    base_score = opportunity_score * 0.6
    
    # Volume bonus (max 25 points)
    if search_volume >= 50000:
        volume_bonus = 25
    elif search_volume >= 10000:
        volume_bonus = 20
    elif search_volume >= 1000:
        volume_bonus = 15
    elif search_volume >= 100:
        volume_bonus = 10
    else:
        volume_bonus = 5
    
    # Relevance multiplier (0.5 to 1.5)
    relevance_multiplier = max(0.5, min(1.5, relevance_score))
    
    priority_score = int((base_score + volume_bonus) * relevance_multiplier)
    return min(100, priority_score)

def get_monthly_budget_estimate(avg_cpc: float, target_clicks: int) -> Dict[str, Any]:
    """Estimate monthly budget based on CPC and target clicks"""
    daily_clicks = target_clicks / 30
    daily_budget = daily_clicks * avg_cpc
    monthly_budget = daily_budget * 30
    
    return {
        "daily_budget": format_currency(daily_budget),
        "monthly_budget": format_currency(monthly_budget),
        "daily_clicks": int(daily_clicks),
        "monthly_clicks": target_clicks,
        "avg_cpc": format_currency(avg_cpc)
    }

def validate_keyword_list(keywords: List[str], max_length: int = 10) -> Dict[str, Any]:
    """Validate keyword list and provide recommendations"""
    issues = []
    recommendations = []
    
    if len(keywords) > max_length:
        issues.append(f"Too many keywords ({len(keywords)}). Maximum allowed: {max_length}")
    
    if len(keywords) == 0:
        issues.append("No keywords provided")
    
    # Check for duplicates
    duplicates = [kw for kw in set(keywords) if keywords.count(kw) > 1]
    if duplicates:
        issues.append(f"Duplicate keywords found: {', '.join(duplicates)}")
    
    # Check keyword length
    long_keywords = [kw for kw in keywords if len(kw) > 50]
    if long_keywords:
        recommendations.append("Consider shorter, more focused keywords")
    
    # Check for very short keywords
    short_keywords = [kw for kw in keywords if len(kw.strip()) < 2]
    if short_keywords:
        issues.append("Keywords must be at least 2 characters long")
    
    return {
        "is_valid": len(issues) == 0,
        "issues": issues,
        "recommendations": recommendations,
        "keyword_count": len(keywords),
        "unique_keywords": len(set(keywords))
    }