"""
Utility helper functions for the Unified Marketing Dashboard
"""

from datetime import datetime, timedelta
from typing import Tuple, Dict, Any, List, Optional
import logging
logger = logging.getLogger(__name__)


def get_date_range(period: str) -> Tuple[str, str]:
    """Get date range for a given period"""
    end_date = datetime.now().date()
    
    if period == "7d":
        start_date = end_date - timedelta(days=7)
    elif period == "30d":
        start_date = end_date - timedelta(days=30)
    elif period == "90d":
        start_date = end_date - timedelta(days=90)
    elif period == "365d":
        start_date = end_date - timedelta(days=365)
    else:
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


def convert_ga_period_to_ads_period(ga_period: str) -> str:
    """Convert GA4 period format to Google Ads period format"""
    mapping = {
        "7d": "LAST_7_DAYS",
        "30d": "LAST_30_DAYS",
        "90d": "LAST_90_DAYS",
        "365d": "LAST_365_DAYS"
    }
    return mapping.get(ga_period, "LAST_30_DAYS")

    
def get_country_location_id()-> Dict[str, str]:
    """Get comprehensive mapping of countries to Google Ads location IDs"""
    return {
        # Asia Pacific
        "Sri Lanka": "2144",
        "India": "2356",
        "Pakistan": "2586", 
        "Bangladesh": "2050",
        "Nepal": "2524",
        "Bhutan": "2064",
        "Maldives": "2462",
        "Afghanistan": "2004",
        
        "China": "2156",
        "Japan": "2392",
        "South Korea": "2410",
        "North Korea": "2408",
        "Mongolia": "2496",
        "Taiwan": "2158",
        "Hong Kong": "2344",
        "Macau": "2446",
        
        "Thailand": "2764",
        "Vietnam": "2704",
        "Cambodia": "2116",
        "Laos": "2418",
        "Myanmar": "2104",
        "Malaysia": "2458",
        "Singapore": "2702",
        "Indonesia": "2360",
        "Philippines": "2608",
        "Brunei": "2096",
        "Timor-Leste": "2626",
        
        "Australia": "2036",
        "New Zealand": "2554",
        "Papua New Guinea": "2598",
        "Fiji": "2242",
        "Solomon Islands": "2090",
        "Vanuatu": "2548",
        "Samoa": "2882",
        "Tonga": "2776",
        "Palau": "2585",
        "Marshall Islands": "2584",
        "Micronesia": "2583",
        "Kiribati": "2296",
        "Tuvalu": "2798",
        "Nauru": "2520",
        
        # Europe
        "United Kingdom": "2826",
        "Ireland": "2372",
        "Iceland": "2352",
        "Norway": "2578",
        "Sweden": "2752",
        "Denmark": "2208",
        "Finland": "2246",
        
        "Germany": "2276",
        "France": "2250",
        "Italy": "2380",
        "Spain": "2724",
        "Portugal": "2620",
        "Netherlands": "2528",
        "Belgium": "2056",
        "Luxembourg": "2442",
        "Switzerland": "2756",
        "Austria": "2040",
        "Liechtenstein": "2438",
        "Monaco": "2492",
        "San Marino": "2674",
        "Vatican City": "2336",
        "Andorra": "2020",
        "Malta": "2470",
        "Cyprus": "2196",
        
        "Poland": "2616",
        "Czech Republic": "2203",
        "Slovakia": "2703",
        "Hungary": "2348",
        "Slovenia": "2705",
        "Croatia": "2191",
        "Bosnia and Herzegovina": "2070",
        "Serbia": "2688",
        "Montenegro": "2499",
        "North Macedonia": "2807",
        "Albania": "2008",
        "Kosovo": "2383",
        
        "Romania": "2642",
        "Bulgaria": "2100",
        "Moldova": "2498",
        "Ukraine": "2804",
        "Belarus": "2112",
        "Lithuania": "2440",
        "Latvia": "2428",
        "Estonia": "2233",
        
        "Russia": "2643",
        "Georgia": "2268",
        "Armenia": "2051",
        "Azerbaijan": "2031",
        
        "Turkey": "2792",
        "Greece": "2300",
        
        # North America
        "United States": "2840",
        "Canada": "2124",
        "Mexico": "2484",
        "Guatemala": "2320",
        "Belize": "2084",
        "El Salvador": "2222",
        "Honduras": "2340",
        "Nicaragua": "2558",
        "Costa Rica": "2188",
        "Panama": "2591",
        
        # Caribbean
        "Cuba": "2192",
        "Jamaica": "2388",
        "Haiti": "2332",
        "Dominican Republic": "2214",
        "Puerto Rico": "2630",
        "Trinidad and Tobago": "2780",
        "Barbados": "2052",
        "Bahamas": "2044",
        "Antigua and Barbuda": "2028",
        "Saint Kitts and Nevis": "2659",
        "Dominica": "2212",
        "Saint Lucia": "2662",
        "Saint Vincent and the Grenadines": "2670",
        "Grenada": "2308",
        
        # South America
        "Brazil": "2076",
        "Argentina": "2032",
        "Chile": "2152",
        "Peru": "2604",
        "Colombia": "2170",
        "Venezuela": "2862",
        "Ecuador": "2218",
        "Bolivia": "2068",
        "Paraguay": "2600",
        "Uruguay": "2858",
        "Guyana": "2328",
        "Suriname": "2740",
        "French Guiana": "2254",
        
        # Africa
        "South Africa": "2710",
        "Nigeria": "2566",
        "Kenya": "2404",
        "Ethiopia": "2231",
        "Egypt": "2818",
        "Morocco": "2504",
        "Algeria": "2012",
        "Tunisia": "2788",
        "Libya": "2434",
        "Sudan": "2729",
        "South Sudan": "2728",
        "Chad": "2148",
        "Niger": "2562",
        "Mali": "2466",
        "Burkina Faso": "2854",
        "Senegal": "2686",
        "Gambia": "2270",
        "Guinea-Bissau": "2624",
        "Guinea": "2324",
        "Sierra Leone": "2694",
        "Liberia": "2430",
        "Ivory Coast": "2384",
        "Ghana": "2288",
        "Togo": "2768",
        "Benin": "2204",
        "Cameroon": "2120",
        "Central African Republic": "2140",
        "Equatorial Guinea": "2226",
        "Gabon": "2266",
        "Republic of the Congo": "2178",
        "Democratic Republic of the Congo": "2180",
        "Angola": "2024",
        "Zambia": "2894",
        "Malawi": "2454",
        "Mozambique": "2508",
        "Zimbabwe": "2716",
        "Botswana": "2072",
        "Namibia": "2516",
        "Swaziland": "2748",
        "Lesotho": "2426",
        "Madagascar": "2450",
        "Mauritius": "2480",
        "Seychelles": "2690",
        "Comoros": "2174",
        "Mayotte": "2175",
        "Reunion": "2638",
        
        "Uganda": "2800",
        "Tanzania": "2834",
        "Rwanda": "2646",
        "Burundi": "2108",
        "Djibouti": "2262",
        "Somalia": "2706",
        "Eritrea": "2232",
        
        # Middle East
        "Saudi Arabia": "2682",
        "United Arab Emirates": "2784",
        "Qatar": "2634",
        "Kuwait": "2414",
        "Bahrain": "2048",
        "Oman": "2512",
        "Yemen": "2887",
        "Iran": "2364",
        "Iraq": "2368",
        "Syria": "2760",
        "Lebanon": "2422",
        "Jordan": "2400",
        "Palestine": "2275",
        "Israel": "2376"
    }




def convert_device_type(device_code: str) -> Dict[str, str]:
    """Convert device type code to readable format"""
    device_map = {
        "0": {"name": "UNSPECIFIED", "label": "Not Specified", "icon": "❓"},
        "1": {"name": "UNKNOWN", "label": "Unknown", "icon": "❓"},
        "2": {"name": "MOBILE", "label": "Mobile", "icon": "📱"},
        "3": {"name": "TABLET", "label": "Tablet", "icon": "� tablet"},
        "4": {"name": "DESKTOP", "label": "Desktop", "icon": "💻"},
        "5": {"name": "CONNECTED_TV", "label": "Connected TV", "icon": "📺"},
        "6": {"name": "OTHER", "label": "Other", "icon": "❓"}
    }
    
    return device_map.get(str(device_code), {
        "name": "UNKNOWN",
        "label": "Unknown Device",
        "icon": "❓"
    })

def convert_campaign_status(status_code: str) -> Dict[str, str]:
    """Convert campaign status code to readable format"""
    status_map = {
        "0": {"name": "UNSPECIFIED", "label": "Not Specified", "color": "gray"},
        "1": {"name": "UNKNOWN", "label": "Unknown", "color": "gray"},
        "2": {"name": "ENABLED", "label": "Active", "color": "green"},
        "3": {"name": "PAUSED", "label": "Paused", "color": "yellow"},
        "4": {"name": "REMOVED", "label": "Removed", "color": "red"}
    }
    
    return status_map.get(str(status_code), {
        "name": "UNKNOWN",
        "label": "Unknown",
        "color": "gray"
    })

def convert_campaign_type(type_code: str) -> Dict[str, str]:
    """Convert campaign type code to readable format"""
    type_map = {
        "0": {"name": "UNSPECIFIED", "label": "Not Specified", "icon": "❓"},
        "1": {"name": "UNKNOWN", "label": "Unknown", "icon": "❓"},
        "2": {"name": "SEARCH", "label": "Search Network", "icon": "🔍"},
        "3": {"name": "DISPLAY", "label": "Display Network", "icon": "🖼️"},
        "4": {"name": "SHOPPING", "label": "Shopping", "icon": "🛒"},
        "5": {"name": "HOTEL", "label": "Hotel", "icon": "🏨"},
        "6": {"name": "VIDEO", "label": "YouTube/Video", "icon": "📹"},
        "7": {"name": "MULTI_CHANNEL", "label": "Multi-Channel", "icon": "📡"},
        "8": {"name": "LOCAL", "label": "Local", "icon": "📍"},
        "9": {"name": "SMART", "label": "Smart Campaign", "icon": "🤖"},
        "10": {"name": "PERFORMANCE_MAX", "label": "Performance Max", "icon": "⚡"},
        "11": {"name": "LOCAL_SERVICES", "label": "Local Services", "icon": "🔧"},
        "12": {"name": "DISCOVERY", "label": "Discovery", "icon": "🎯"},
        "13": {"name": "TRAVEL", "label": "Travel", "icon": "✈️"}
    }
    
    return type_map.get(str(type_code), {
        "name": "UNKNOWN",
        "label": "Unknown Type",
        "icon": "❓"
    })

