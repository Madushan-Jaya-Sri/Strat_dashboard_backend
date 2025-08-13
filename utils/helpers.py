"""
Utility helper functions for the Unified Marketing Dashboard
"""

from datetime import datetime, timedelta
from typing import Tuple

def get_date_range(period: str) -> Tuple[str, str]:
    """Get date range based on period string"""
    end_date = datetime.now()
    
    if period == "7d":
        start_date = end_date - timedelta(days=7)
    elif period == "90d":
        start_date = end_date - timedelta(days=90)
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
        "LAST_90_DAYS": "90d"
    }
    return mapping.get(ads_period, "30d")

def convert_ga_period_to_ads_period(ga_period: str) -> str:
    """Convert GA4 period format to Google Ads period format"""
    mapping = {
        "7d": "LAST_7_DAYS",
        "30d": "LAST_30_DAYS",
        "90d": "LAST_90_DAYS"
    }
    return mapping.get(ga_period, "LAST_30_DAYS")