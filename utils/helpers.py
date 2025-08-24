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

def validate_timeframe(timeframe: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> bool:
    """Enhanced timeframe validation that ensures current month is not included"""
    try:
        if timeframe == "custom":
            if not start_date or not end_date:
                return False
            
            start_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
            current_date = datetime.now().date()
            
            # Get the start of current month
            current_month_start = current_date.replace(day=1)
            
            # Check if end date is in current month or future
            if end_obj >= current_month_start:
                return False
            
            # Check if start date is after end date
            if start_obj > end_obj:
                return False
            
            # Check if date range is reasonable (not more than 2 years)
            if (end_obj - start_obj).days > 730:
                return False
            
            return True
            
        elif timeframe in ["1_month", "3_months", "12_months"]:
            # These are always valid as they automatically exclude current month
            return True
        else:
            return False
            
    except (ValueError, TypeError):
        return False
    
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

def get_comprehensive_region_mappings() -> Dict[str, Dict[str, str]]:
    """Get comprehensive regional mappings for major countries"""
    return {
        # Asia Pacific
        "Sri Lanka": {
            "Western Province": "20275",
            "Central Province": "20276", 
            "Southern Province": "20277",
            "Northern Province": "20278",
            "Eastern Province": "20279",
            "North Western Province": "20280",
            "North Central Province": "20281",
            "Uva Province": "20282",
            "Sabaragamuwa Province": "20283"
        },
        
        "India": {
            "Maharashtra": "21147",
            "Karnataka": "21145",
            "Tamil Nadu": "21175",
            "Delhi": "1007785",
            "Gujarat": "21135",
            "Rajasthan": "21165",
            "Uttar Pradesh": "21185",
            "West Bengal": "21195",
            "Andhra Pradesh": "21105",
            "Telangana": "21180",
            "Kerala": "21150",
            "Punjab": "21160",
            "Haryana": "21140",
            "Bihar": "21115",
            "Odisha": "21155",
            "Madhya Pradesh": "21148",
            "Assam": "21110",
            "Jharkhand": "21142",
            "Chhattisgarh": "21120"
        },
        
        "Australia": {
            "New South Wales": "21001",
            "Victoria": "21002",
            "Queensland": "21003",
            "Western Australia": "21004",
            "South Australia": "21005",
            "Tasmania": "21006",
            "Australian Capital Territory": "21007",
            "Northern Territory": "21008"
        },
        
        "Japan": {
            "Tokyo": "1009309",
            "Osaka": "1009310", 
            "Kanagawa": "1009311",
            "Aichi": "1009312",
            "Saitama": "1009313",
            "Chiba": "1009314",
            "Hyogo": "1009315",
            "Hokkaido": "1009316",
            "Fukuoka": "1009317",
            "Kyoto": "1009318"
        },
        
        "South Korea": {
            "Seoul": "1003181",
            "Busan": "1003182",
            "Daegu": "1003183",
            "Incheon": "1003184",
            "Gwangju": "1003185",
            "Daejeon": "1003186",
            "Ulsan": "1003187",
            "Gyeonggi": "1003188",
            "Gangwon": "1003189",
            "North Chungcheong": "1003190"
        },
        
        "China": {
            "Beijing": "1009231",
            "Shanghai": "1009232",
            "Guangdong": "1009233",
            "Jiangsu": "1009234",
            "Zhejiang": "1009235",
            "Shandong": "1009236",
            "Henan": "1009237",
            "Sichuan": "1009238",
            "Hubei": "1009239",
            "Hunan": "1009240"
        },
        
        "Thailand": {
            "Bangkok": "1012150",
            "Chiang Mai": "1012151",
            "Nonthaburi": "1012152",
            "Pak Kret": "1012153",
            "Hat Yai": "1012154",
            "Khon Kaen": "1012155",
            "Udon Thani": "1012156",
            "Nakhon Ratchasima": "1012157",
            "Chonburi": "1012158",
            "Rayong": "1012159"
        },
        
        "Singapore": {
            "Central Region": "20501",
            "East Region": "20502",
            "North Region": "20503",
            "Northeast Region": "20504",
            "West Region": "20505"
        },
        
        "Malaysia": {
            "Kuala Lumpur": "1001493",
            "Selangor": "1001494",
            "Johor": "1001495",
            "Perak": "1001496",
            "Penang": "1001497",
            "Sarawak": "1001498",
            "Sabah": "1001499",
            "Kedah": "1001500",
            "Kelantan": "1001501",
            "Terengganu": "1001502"
        },
        
        "Indonesia": {
            "Jakarta": "1009345",
            "Surabaya": "1009346",
            "Bandung": "1009347",
            "Bekasi": "1009348",
            "Medan": "1009349",
            "Tangerang": "1009350",
            "Depok": "1009351",
            "Semarang": "1009352",
            "Palembang": "1009353",
            "Makassar": "1009354"
        },
        
                "Philippines": {
            "Metro Manila": "1014833",
            "Cebu": "1014834",
            "Davao": "1014835",
            "Cagayan de Oro": "1014836",
            "Zamboanga": "1014837",
            "Antipolo": "1014838",
            "Pasig": "1014839",
            "Taguig": "1014840",
            "Quezon City": "1014841",
            "Manila": "1014842"
        },
        
        "Vietnam": {
            "Ho Chi Minh City": "1012047",
            "Hanoi": "1012048",
            "Da Nang": "1012049",
            "Hai Phong": "1012050",
            "Can Tho": "1012051",
            "Hue": "1012052",
            "Nha Trang": "1012053",
            "Buon Ma Thuot": "1012054",
            "Nam Dinh": "1012055",
            "Qui Nhon": "1012056"
        },
        
        "Pakistan": {
            "Punjab": "21301",
            "Sindh": "21302",
            "Khyber Pakhtunkhwa": "21303",
            "Balochistan": "21304",
            "Islamabad Capital Territory": "21305",
            "Azad Kashmir": "21306",
            "Gilgit-Baltistan": "21307"
        },
        
        "Bangladesh": {
            "Dhaka": "21401",
            "Chittagong": "21402",
            "Rajshahi": "21403",
            "Khulna": "21404",
            "Barisal": "21405",
            "Sylhet": "21406",
            "Rangpur": "21407",
            "Mymensingh": "21408"
        },
        
        # Europe
        "United Kingdom": {
            "London": "1006886",
            "England": "20339",
            "Scotland": "20362",
            "Wales": "20341",
            "Northern Ireland": "20340",
            "Greater Manchester": "20350",
            "West Midlands": "20360",
            "West Yorkshire": "20370",
            "Merseyside": "20355",
            "South Yorkshire": "20365"
        },
        
        "Germany": {
            "North Rhine-Westphalia": "20401",
            "Bavaria": "20402",
            "Baden-Württemberg": "20403",
            "Lower Saxony": "20404",
            "Hesse": "20405",
            "Saxony": "20406",
            "Rhineland-Palatinate": "20407",
            "Schleswig-Holstein": "20408",
            "Brandenburg": "20409",
            "Saxony-Anhalt": "20410",
            "Thuringia": "20411",
            "Hamburg": "20412",
            "Mecklenburg-Vorpommern": "20413",
            "Saarland": "20414",
            "Berlin": "20415",
            "Bremen": "20416"
        },
        
        "France": {
            "Île-de-France": "20451",
            "Auvergne-Rhône-Alpes": "20452",
            "Hauts-de-France": "20453",
            "Occitanie": "20454",
            "Nouvelle-Aquitaine": "20455",
            "Grand Est": "20456",
            "Provence-Alpes-Côte d'Azur": "20457",
            "Pays de la Loire": "20458",
            "Normandy": "20459",
            "Brittany": "20460",
            "Bourgogne-Franche-Comté": "20461",
            "Centre-Val de Loire": "20462",
            "Corsica": "20463"
        },
        
        "Italy": {
            "Lombardy": "20501",
            "Lazio": "20502",
            "Campania": "20503",
            "Sicily": "20504",
            "Veneto": "20505",
            "Emilia-Romagna": "20506",
            "Piedmont": "20507",
            "Apulia": "20508",
            "Tuscany": "20509",
            "Calabria": "20510",
            "Sardinia": "20511",
            "Liguria": "20512",
            "Marche": "20513",
            "Abruzzo": "20514",
            "Friuli-Venezia Giulia": "20515",
            "Trentino-Alto Adige": "20516",
            "Umbria": "20517",
            "Basilicata": "20518",
            "Molise": "20519",
            "Aosta Valley": "20520"
        },
        
        "Spain": {
            "Andalusia": "20601",
            "Catalonia": "20602",
            "Community of Madrid": "20603",
            "Valencian Community": "20604",
            "Galicia": "20605",
            "Castile and León": "20606",
            "Basque Country": "20607",
            "Canary Islands": "20608",
            "Castile-La Mancha": "20609",
            "Region of Murcia": "20610",
            "Aragon": "20611",
            "Extremadura": "20612",
            "Balearic Islands": "20613",
            "Asturias": "20614",
            "Navarre": "20615",
            "Cantabria": "20616",
            "La Rioja": "20617"
        },
        
        "Netherlands": {
            "North Holland": "20701",
            "South Holland": "20702",
            "North Brabant": "20703",
            "Gelderland": "20704",
            "Utrecht": "20705",
            "Overijssel": "20706",
            "Limburg": "20707",
            "Friesland": "20708",
            "Groningen": "20709",
            "Drenthe": "20710",
            "Flevoland": "20711",
            "Zeeland": "20712"
        },
        
        "Poland": {
            "Masovian": "20801",
            "Lesser Poland": "20802",
            "Greater Poland": "20803",
            "Silesian": "20804",
            "Lower Silesian": "20805",
            "Łódź": "20806",
            "Pomeranian": "20807",
            "Lublin": "20808",
            "West Pomeranian": "20809",
            "Warmian-Masurian": "20810",
            "Kuyavian-Pomeranian": "20811",
            "Podlaskie": "20812",
            "Lubusz": "20813",
            "Subcarpathian": "20814",
            "Świętokrzyskie": "20815",
            "Opole": "20816"
        },
        
        "Russia": {
            "Moscow": "1009467",
            "Saint Petersburg": "1009468",
            "Novosibirsk Oblast": "1009469",
            "Yekaterinburg": "1009470",
            "Nizhny Novgorod": "1009471",
            "Kazan": "1009472",
            "Chelyabinsk": "1009473",
            "Omsk": "1009474",
            "Samara": "1009475",
            "Rostov-on-Don": "1009476"
        },
        
        # North America
        "United States": {
            "California": "21137",
            "New York": "21167",
            "Texas": "21176",
            "Florida": "21149",
            "Pennsylvania": "21170",
            "Illinois": "21151",
            "Ohio": "21168",
            "Georgia": "21150",
            "North Carolina": "21166",
            "Michigan": "21158",
            "New Jersey": "21165",
            "Virginia": "21179",
            "Washington": "21180",
            "Arizona": "21140",
            "Massachusetts": "21157",
            "Tennessee": "21175",
            "Indiana": "21152",
            "Maryland": "21156",
            "Missouri": "21161",
            "Wisconsin": "21182",
            "Colorado": "21143",
            "Minnesota": "21159",
            "South Carolina": "21173",
            "Alabama": "21139",
            "Louisiana": "21155",
            "Kentucky": "21153",
            "Oregon": "21169",
            "Oklahoma": "21169",
            "Connecticut": "21144",
            "Utah": "21178",
            "Iowa": "21152",
            "Nevada": "21164",
            "Arkansas": "21141",
            "Mississippi": "21160",
            "Kansas": "21153",
            "New Mexico": "21165",
            "Nebraska": "21163",
            "West Virginia": "21181",
            "Idaho": "21151",
            "Hawaii": "21150",
            "New Hampshire": "21164",
            "Maine": "21156",
            "Montana": "21162",
            "Rhode Island": "21172",
            "Delaware": "21146",
            "South Dakota": "21174",
            "North Dakota": "21167",
            "Alaska": "21139",
            "Vermont": "21178",
            "Wyoming": "21183"
        },
        
        "Canada": {
            "Ontario": "20801",
            "Quebec": "20802",
            "British Columbia": "20803",
            "Alberta": "20804",
            "Manitoba": "20805",
            "Saskatchewan": "20806",
            "Nova Scotia": "20807",
            "New Brunswick": "20808",
            "Newfoundland and Labrador": "20809",
            "Prince Edward Island": "20810",
            "Northwest Territories": "20811",
            "Yukon": "20812",
            "Nunavut": "20813"
        },
        
        "Mexico": {
            "Mexico City": "1010043",
            "State of Mexico": "1010044",
            "Jalisco": "1010045",
            "Nuevo León": "1010046",
            "Puebla": "1010047",
            "Guanajuato": "1010048",
            "Chihuahua": "1010049",
            "Veracruz": "1010050",
            "Michoacán": "1010051",
            "Oaxaca": "1010052",
            "Chiapas": "1010053",
            "Sonora": "1010054",
            "Coahuila": "1010055",
            "Tamaulipas": "1010056",
            "Baja California": "1010057",
            "Guerrero": "1010058",
            "San Luis Potosí": "1010059",
            "Sinaloa": "1010060",
            "Hidalgo": "1010061",
            "Tabasco": "1010062"
        },
        
        # South America
        "Brazil": {
            "São Paulo": "1010721",
            "Rio de Janeiro": "1010722",
            "Minas Gerais": "1010723",
            "Bahia": "1010724",
            "Paraná": "1010725",
            "Rio Grande do Sul": "1010726",
            "Pernambuco": "1010727",
            "Ceará": "1010728",
            "Pará": "1010729",
            "Santa Catarina": "1010730",
            "Goiás": "1010731",
            "Maranhão": "1010732",
            "Espírito Santo": "1010733",
            "Paraíba": "1010734",
            "Mato Grosso": "1010735",
            "Rio Grande do Norte": "1010736",
            "Alagoas": "1010737",
            "Piauí": "1010738",
            "Mato Grosso do Sul": "1010739",
            "Sergipe": "1010740",
            "Rondônia": "1010741",
            "Acre": "1010742",
            "Amazonas": "1010743",
            "Roraima": "1010744",
            "Amapá": "1010745",
            "Tocantins": "1010746",
            "Distrito Federal": "1010747"
        },
        
        "Argentina": {
            "Buenos Aires": "1011001",
            "Córdoba": "1011002",
            "Santa Fe": "1011003",
            "Mendoza": "1011004",
            "Tucumán": "1011005",
            "Entre Ríos": "1011006",
            "Salta": "1011007",
            "Misiones": "1011008",
            "Chaco": "1011009",
            "Corrientes": "1011010",
            "Santiago del Estero": "1011011",
            "San Juan": "1011012",
            "Jujuy": "1011013",
            "Río Negro": "1011014",
            "Formosa": "1011015",
            "Neuquén": "1011016",
            "Chubut": "1011017",
            "San Luis": "1011018",
            "Catamarca": "1011019",
            "La Rioja": "1011020",
            "La Pampa": "1011021",
            "Santa Cruz": "1011022",
            "Tierra del Fuego": "1011023"
        },
        
        # Africa
        "South Africa": {
            "Gauteng": "21201",
            "Western Cape": "21202",
            "KwaZulu-Natal": "21203",
            "Eastern Cape": "21204",
            "Limpopo": "21205",
            "Mpumalanga": "21206",
            "North West": "21207",
            "Free State": "21208",
            "Northern Cape": "21209"
        },
        
        "Nigeria": {
            "Lagos": "21251",
            "Kano": "21252",
            "Ibadan": "21253",
            "Kaduna": "21254",
            "Port Harcourt": "21255",
            "Benin City": "21256",
            "Maiduguri": "21257",
            "Zaria": "21258",
            "Aba": "21259",
            "Jos": "21260"
        },
        
        "Egypt": {
            "Cairo": "1012570",
            "Alexandria": "1012571",
            "Giza": "1012572",
            "Shubra El Kheima": "1012573",
            "Port Said": "1012574",
            "Suez": "1012575",
            "Luxor": "1012576",
            "Mansoura": "1012577",
            "El Mahalla El Kubra": "1012578",
            "Tanta": "1012579"
        },
        
        # Middle East
        "Saudi Arabia": {
            "Riyadh": "1012916",
            "Jeddah": "1012917",
            "Mecca": "1012918",
            "Medina": "1012919",
            "Dammam": "1012920",
            "Khobar": "1012921",
            "Dhahran": "1012922",
            "Taif": "1012923",
            "Buraidah": "1012924",
            "Tabuk": "1012925"
        },
        
        "United Arab Emirates": {
            "Dubai": "1013448",
            "Abu Dhabi": "1013449",
            "Sharjah": "1013450",
            "Al Ain": "1013451",
            "Ajman": "1013452",
            "Ras Al Khaimah": "1013453",
            "Fujairah": "1013454",
            "Umm Al Quwain": "1013455"
        },
        
        "Israel": {
            "Tel Aviv": "1011577",
            "Jerusalem": "1011578",
            "Haifa": "1011579",
            "Rishon LeZion": "1011580",
            "Petah Tikva": "1011581",
            "Ashdod": "1011582",
            "Netanya": "1011583",
            "Beer Sheva": "1011584",
            "Holon": "1011585",
            "Bnei Brak": "1011586"
        }
    }

def get_comprehensive_language_mappings() -> Dict[str, Dict[str, float]]:
    """Get comprehensive language distribution mappings by location ID"""
    return {
        # Asia Pacific
        "2144": {  # Sri Lanka
            "English": 0.65,
            "Sinhala": 0.25,
            "Tamil": 0.10
        },
        "2356": {  # India
            "English": 0.40,
            "Hindi": 0.35,
            "Bengali": 0.08,
            "Telugu": 0.05,
            "Marathi": 0.04,
            "Tamil": 0.03,
            "Gujarati": 0.02,
            "Kannada": 0.02,
            "Malayalam": 0.01
        },
        "2586": {  # Pakistan
            "Urdu": 0.45,
            "English": 0.30,
            "Punjabi": 0.15,
            "Sindhi": 0.05,
            "Pashto": 0.03,
            "Balochi": 0.02
        },
        "2050": {  # Bangladesh
            "Bengali": 0.85,
            "English": 0.12,
            "Chittagonian": 0.02,
            "Sylheti": 0.01
        },
        "2156": {  # China
            "Chinese (Simplified)": 0.85,
            "Chinese (Traditional)": 0.08,
            "English": 0.05,
            "Cantonese": 0.02
        },
        "2392": {  # Japan
            "Japanese": 0.95,
            "English": 0.04,
            "Chinese": 0.01
        },
        "2410": {  # South Korea
            "Korean": 0.92,
            "English": 0.06,
            "Chinese": 0.02
        },
        "2764": {  # Thailand
            "Thai": 0.90,
            "English": 0.08,
            "Chinese": 0.02
        },
        "2704": {  # Vietnam
            "Vietnamese": 0.92,
            "English": 0.06,
            "Chinese": 0.02
        },
        "2458": {  # Malaysia
            "Malay": 0.45,
            "English": 0.30,
            "Chinese": 0.20,
            "Tamil": 0.05
        },
        "2702": {  # Singapore
            "English": 0.70,
            "Chinese": 0.20,
            "Malay": 0.07,
            "Tamil": 0.03
        },
        "2360": {  # Indonesia
            "Indonesian": 0.85,
            "English": 0.10,
            "Javanese": 0.03,
            "Chinese": 0.02
        },
        "2608": {  # Philippines
            "Filipino": 0.45,
            "English": 0.40,
            "Cebuano": 0.10,
            "Tagalog": 0.05
        },
        "2036": {  # Australia
            "English": 0.85,
            "Chinese": 0.05,
            "Arabic": 0.03,
            "Vietnamese": 0.02,
            "Italian": 0.02,
            "Greek": 0.01,
            "Hindi": 0.01,
            "Spanish": 0.01
        },
        
        # Europe
        "2826": {  # United Kingdom
            "English": 0.95,
            "Polish": 0.015,
            "Urdu": 0.01,
            "Bengali": 0.008,
            "Gujarati": 0.007,
            "Arabic": 0.005,
            "French": 0.003,
            "Chinese": 0.002
        },
        "2276": {  # Germany
            "German": 0.85,
            "English": 0.08,
            "Turkish": 0.03,
            "Russian": 0.02,
            "Arabic": 0.01,
            "Polish": 0.01
        },
        "2250": {  # France
            "French": 0.88,
            "English": 0.06,
            "Arabic": 0.03,
            "Spanish": 0.01,
            "Portuguese": 0.01,
            "German": 0.01
        },
        "2380": {  # Italy
            "Italian": 0.92,
            "English": 0.04,
            "Spanish": 0.02,
            "French": 0.01,
            "German": 0.01
        },
        "2724": {  # Spain
            "Spanish": 0.88,
            "Catalan": 0.05,
            "English": 0.04,
            "Galician": 0.02,
            "Basque": 0.01
        },
        "2528": {  # Netherlands
            "Dutch": 0.85,
            "English": 0.10,
            "German": 0.02,
            "Turkish": 0.01,
            "Arabic": 0.01,
            "French": 0.01
        },
        "2616": {  # Poland
            "Polish": 0.95,
            "English": 0.03,
            "German": 0.01,
            "Russian": 0.01
        },
        "2643": {  # Russia
            "Russian": 0.88,
            "English": 0.05,
            "Tatar": 0.02,
            "Ukrainian": 0.02,
            "Bashkir": 0.01,
            "Chuvash": 0.01,
            "Chechen": 0.01
        },
        
        # North America
        "2840": {  # United States
            "English": 0.78,
            "Spanish": 0.15,
            "Chinese": 0.02,
            "French": 0.015,
            "German": 0.01,
            "Arabic": 0.008,
            "Russian": 0.007,
            "Korean": 0.005,
            "Vietnamese": 0.005,
            "Italian": 0.003
        },
        "2124": {  # Canada
            "English": 0.75,
            "French": 0.20,
            "Chinese": 0.02,
            "Spanish": 0.01,
            "Arabic": 0.008,
            "Italian": 0.007,
            "German": 0.005
        },
        "2484": {  # Mexico
            "Spanish": 0.92,
            "English": 0.05,
            "Nahuatl": 0.015,
            "Maya": 0.01,
            "Zapotec": 0.005
        },
        
        # South America
        "2076": {  # Brazil
            "Portuguese": 0.95,
            "English": 0.03,
            "Spanish": 0.015,
            "German": 0.003,
            "Italian": 0.002
        },
        "2032": {  # Argentina
            "Spanish": 0.95,
            "English": 0.03,
            "Italian": 0.015,
            "German": 0.003,
            "French": 0.002
        },
        "2152": {  # Chile
            "Spanish": 0.96,
            "English": 0.025,
            "Mapuche": 0.01,
            "German": 0.003,
            "French": 0.002
        },
        
        # Africa
        "2710": {  # South Africa
            "English": 0.35,
            "Afrikaans": 0.25,
            "Zulu": 0.15,
            "Xhosa": 0.10,
            "Sotho": 0.05,
            "Tswana": 0.03,
            "Tsonga": 0.02,
            "Swazi": 0.02,
            "Venda": 0.02,
            "Ndebele": 0.01
        },
        "2566": {  # Nigeria
            "English": 0.60,
            "Hausa": 0.15,
            "Yoruba": 0.12,
            "Igbo": 0.10,
            "Fulani": 0.02,
            "Kanuri": 0.01
        },
        "2818": {  # Egypt
            "Arabic": 0.92,
            "English": 0.06,
            "French": 0.015,
            "Coptic": 0.005
        },
        
        # Middle East
        "2682": {  # Saudi Arabia
            "Arabic": 0.88,
            "English": 0.10,
            "Urdu": 0.015,
            "Filipino": 0.003,
            "Bengali": 0.002
        },
        "2784": {  # United Arab Emirates
            "Arabic": 0.45,
            "English": 0.35,
            "Hindi": 0.08,
            "Urdu": 0.05,
            "Filipino": 0.03,
            "Bengali": 0.02,
            "Malayalam": 0.015,
            "Tamil": 0.005
        },
        "2376": {  # Israel
            "Hebrew": 0.70,
            "Arabic": 0.18,
            "English": 0.08,
            "Russian": 0.03,
            "French": 0.01
        }
    }

def get_region_population_weights() -> Dict[str, Dict[str, float]]:
    """Get population weights for regions within countries"""
    return get_comprehensive_region_mappings()



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

def calculate_keyword_opportunity_score(avg_monthly_searches: int, competition_index: float, 
                                      trend_direction: str) -> int:
    """Calculate opportunity score for a keyword (0-100)"""
    score = 0
    
    # Search volume score (40% weight)
    if avg_monthly_searches >= 10000:
        score += 40
    elif avg_monthly_searches >= 5000:
        score += 35
    elif avg_monthly_searches >= 1000:
        score += 30
    elif avg_monthly_searches >= 500:
        score += 25
    elif avg_monthly_searches >= 100:
        score += 20
    else:
        score += 10
    
    # Competition score (30% weight) - lower competition = higher score
    if competition_index <= 0.3:
        score += 30
    elif competition_index <= 0.5:
        score += 25
    elif competition_index <= 0.7:
        score += 20
    else:
        score += 10
    
    # Trend score (30% weight)
    if trend_direction == "Strong Upward":
        score += 30
    elif trend_direction == "Upward":
        score += 25
    elif trend_direction == "Stable":
        score += 20
    elif trend_direction == "Downward":
        score += 10
    else:  # Strong Downward
        score += 5
    
    return min(score, 100)


def get_keyword_recommendation(opportunity_score: int, competition: str, avg_monthly_searches: int) -> str:
    """Get recommendation for a keyword based on its metrics"""
    if opportunity_score >= 80:
        return "Excellent opportunity - High priority target"
    elif opportunity_score >= 70:
        return "Very good opportunity - Consider targeting"
    elif opportunity_score >= 60:
        return "Good opportunity - Worth considering"
    elif opportunity_score >= 50:
        if competition == "LOW" and avg_monthly_searches >= 1000:
            return "Moderate opportunity - Low competition advantage"
        else:
            return "Moderate opportunity - Research further"
    elif opportunity_score >= 40:
        return "Limited opportunity - Consider long-tail variations"
    else:
        return "Poor opportunity - Focus on alternatives"

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
    """Calculate seasonality metrics for keyword volumes - returns mixed types for backward compatibility"""
    if not monthly_volumes or len(monthly_volumes) < 3:
        return {
            "seasonality": "insufficient_data",
            "coefficient_of_variation": 0.0,
            "peak_month": "N/A", 
            "low_month": "N/A",
            "seasonal_factor": 0.0
        }
    
    volumes = list(monthly_volumes.values())
    avg_volume = sum(volumes) / len(volumes)
    
    if avg_volume == 0:
        return {
            "seasonality": "no_data",
            "coefficient_of_variation": 0.0,
            "peak_month": "N/A",
            "low_month": "N/A", 
            "seasonal_factor": 0.0
        }
    
    # Calculate coefficient of variation
    variance = sum((x - avg_volume) ** 2 for x in volumes) / len(volumes)
    std_dev = variance ** 0.5
    cv = (std_dev / avg_volume) * 100
    
    # Find peak and low months
    sorted_months = sorted(monthly_volumes.items(), key=lambda x: x[1], reverse=True)
    peak_month = sorted_months[0][0] if sorted_months else "N/A"
    low_month = sorted_months[-1][0] if sorted_months else "N/A"
    
    # Determine seasonality level
    if cv > 50:
        seasonality_level = "highly_seasonal"
    elif cv > 30:
        seasonality_level = "moderately_seasonal"
    elif cv > 15:
        seasonality_level = "slightly_seasonal"
    else:
        seasonality_level = "stable"
    
    return {
        "seasonality": seasonality_level,
        "coefficient_of_variation": round(cv, 1),
        "peak_month": peak_month,
        "low_month": low_month,
        "seasonal_factor": round(cv / 100, 2)
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



def format_metric_value(value: float, metric_type: str) -> str:
    """Format metric values based on type"""
    if metric_type in ["impressions", "clicks", "conversions"]:
        return format_large_number(int(value))
    elif metric_type in ["cost", "cpc", "cost_per_conversion"]:
        return format_currency(value)
    elif metric_type in ["ctr", "conversion_rate"]:
        return f"{value:.2f}%"
    else:
        return str(value)

def get_metric_performance_status(metric_name: str, value: float) -> str:
    """Get performance status for different metrics"""
    benchmarks = {
        "ctr": {"excellent": 5.0, "good": 3.0, "average": 1.5},
        "conversion_rate": {"excellent": 3.0, "good": 2.0, "average": 1.0},
        "cost_per_conversion": {"excellent": 20.0, "good": 50.0, "average": 100.0},  # Lower is better
        "avg_cpc": {"excellent": 1.0, "good": 2.0, "average": 5.0}  # Lower is better
    }
    
    if metric_name.lower() not in benchmarks:
        return "N/A"
    
    bench = benchmarks[metric_name.lower()]
    
    # For cost metrics, lower is better
    if metric_name.lower() in ["cost_per_conversion", "avg_cpc"]:
        if value <= bench["excellent"]:
            return "Excellent"
        elif value <= bench["good"]:
            return "Good"
        elif value <= bench["average"]:
            return "Average"
        else:
            return "Needs Improvement"
    else:
        # For rate metrics, higher is better
        if value >= bench["excellent"]:
            return "Excellent"
        elif value >= bench["good"]:
            return "Good"
        elif value >= bench["average"]:
            return "Average"
        else:
            return "Needs Improvement"

def calculate_metric_change(current_value: float, previous_value: float) -> Dict[str, Any]:
    """Calculate metric change and trend"""
    if previous_value == 0:
        return {
            "change_value": 0.0,
            "change_percentage": 0.0,
            "trend": "stable",
            "formatted_change": "N/A"
        }
    
    change_value = current_value - previous_value
    change_percentage = (change_value / previous_value) * 100
    
    if change_percentage > 5:
        trend = "increasing"
        icon = "↗"
    elif change_percentage < -5:
        trend = "decreasing"
        icon = "↘"
    else:
        trend = "stable"
        icon = "→"
    
    return {
        "change_value": change_value,
        "change_percentage": change_percentage,
        "trend": trend,
        "formatted_change": f"{icon} {change_percentage:+.1f}%"
    }

def get_metric_description(metric_name: str) -> str:
    """Get user-friendly descriptions for metrics"""
    descriptions = {
        "total_impressions": "Number of times your ads were shown",
        "total_cost": "Total amount spent on advertising",
        "total_clicks": "Number of times users clicked on your ads",
        "conversion_rate": "Percentage of clicks that resulted in conversions",
        "total_conversions": "Total number of desired actions completed",
        "avg_cost_per_click": "Average amount paid for each click",
        "cost_per_conversion": "Average cost to acquire one conversion",
        "click_through_rate": "Percentage of impressions that resulted in clicks"
    }
    
    return descriptions.get(metric_name.lower(), "Performance metric")