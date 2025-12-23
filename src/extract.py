import requests
import pandas as pd
import yaml
from datetime import datetime
import os
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

def load_api_config():
    """Load API configuration from YAML"""
    with open('config/api_config.yaml', 'r') as file:
        return yaml.safe_load(file)['api']

def fetch_data_from_api():
    """Extract data from COVID-19 API"""
    api_config = load_api_config()
    url = api_config['base_url'] + api_config['endpoints']['countries']
    
    logger.info(f"Fetching data from API: {url}")
    
    try:
        response = requests.get(url, timeout=api_config['timeout'])
        response.raise_for_status()  # Raises an HTTPError for bad responses
        
        data = response.json()
        logger.info(f"Successfully fetched data for {len(data)} countries")
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        raise

def save_raw_data(data):
    """Save raw data as CSV with timestamp"""
    os.makedirs('data/raw', exist_ok=True)
    
    df_raw = pd.json_normalize(data)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"data/raw/covid_raw_{timestamp}.csv"
    
    df_raw.to_csv(filename, index=False)
    logger.info(f"Raw data saved to: {filename}")
    
    return df_raw

def extract():
    """Extracting data and returning DataFrame(not saving to file)"""
    logger.info("Starting extraction process")
    
    data = fetch_data_from_api()
    df_raw = pd.json_normalize(data)

    #Airflow will handle this
    logger.info(f"Extracted {len(df_raw)} records from API")
    return df_raw # returning the Dataframe for next task

if __name__ == "__main__":
    extract()