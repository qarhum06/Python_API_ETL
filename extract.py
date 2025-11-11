import requests
import pandas as pd
import pyodbc
from logger_config import setup_logger

logger = setup_logger()

def extract_data(api_url):
    logger.info("Starting data extraction from API.")
    response = requests.get(api_url)
    response.raise_for_status()
    
    data = response.json().get('carts', [])
    if not data:
        logger.warning("No data found in API response.")
        return pd.DataFrame()
    
    df = pd.json_normalize(data, 'products', ['id', 'userId', 'total', 'discountedTotal', 'totalProducts', 'totalQuantity'])
    logger.info(f"Extracted {len(df)} records from API.")
    return df


