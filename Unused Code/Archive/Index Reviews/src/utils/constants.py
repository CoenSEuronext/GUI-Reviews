from typing import List, Dict, Set
from datetime import datetime

# File and path constants
REQUIRED_COLUMNS: Dict[str, List[str]] = {
    'developed_market': [
        'Name', 'ISIN', 'MIC', 'Currency (Local)', 'NOSH',
        '3 months ADTV', 'index'
    ],
    'oekom': [
        'ISIN', 'NBR Overall Flag', 'CRStaffRatingNum',
        'Coal Mining and Power Gen - Maximum Percentage of Revenues (%)',
        'FossilFuelProdMaxRev', 'FossilFuelDistMaxRev',
        'Power Generation - Thermal Maximum Percentage of Revenues (%)',
        'Tobacco - Production Maximum Percentage of Revenues (%)',
        'Tobacco - Distribution Maximum Percentage of Revenues (%)'
    ],
    'sesamm': ['ISIN', 'layoff_score_6m', 'Job_score_3Y'],
    'icb': ['ISIN Code', 'Supersector Code', 'Industry']
}

# Market data constants
ALLOWED_CURRENCIES: Set[str] = {'EUR', 'JPY', 'USD', 'CAD', 'GBP'}
MIN_MARKET_CAP: float = 100_000_000  # 100M in index currency
MAX_WEIGHT: float = 0.20  # 20% maximum weight
ALLOWED_AREAS: Set[str] = {'EU', 'US', 'AS', 'NA'}

# Review parameters
REVIEW_FREQUENCIES: Set[str] = {'monthly', 'quarterly', 'semi-annual', 'annual'}
DEFAULT_NOTICE_DAYS: int = 2
REBALANCE_DAY: int = 23

# Screening thresholds
TURNOVER_THRESHOLD: float = 10_000_000  # 10M EUR minimum ADTV
STAFF_RATING_EXCLUSION_PERCENTAGE: float = 0.20  # Bottom 20%
COAL_REVENUE_THRESHOLD: float = 0.01  # 1%
FOSSIL_FUEL_THRESHOLD: float = 0.10  # 10%
THERMAL_POWER_THRESHOLD: float = 0.50  # 50%
TOBACCO_PRODUCTION_THRESHOLD: float = 0  # 0%
TOBACCO_DISTRIBUTION_THRESHOLD: float = 0.15  # 15%

# NACE codes
HIGH_IMPACT_NACE_CODES: Set[str] = {
    'A',  # Agriculture, forestry and fishing
    'B',  # Mining and quarrying
    'C',  # Manufacturing
    'D',  # Electricity, gas, steam and air conditioning supply
    'E',  # Water supply; sewerage, waste management
    'F',  # Construction
    'G',  # Wholesale and retail trade
    'H',  # Transportation and storage
    'L'   # Real estate activities
}

# Flag values
FLAG_VALUES: Dict[str, str] = {
    'RED': 'RED',
    'AMBER': 'AMBER',
    'GREEN': 'GREEN'
}

# Date formats
DATE_FORMATS: Dict[str, str] = {
    'filename': '%Y%m%d',
    'effective': '%d-%b-%y',
    'display': '%Y-%m-%d'
}

# System constants
ROUNDING_DECIMALS: int = 14
CURRENT_YEAR: str = str(datetime.now().year)
CURRENT_MONTH: str = datetime.now().strftime("%Y%m")