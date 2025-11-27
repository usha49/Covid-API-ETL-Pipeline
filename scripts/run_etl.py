import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.extract import extract
from src.transform import transform
from src.load import load
from src.utils.logger import setup_logger

logger = setup_logger('main_etl')

def run_etl_pipeline():
    """Run the complete ETL pipeline"""
    try:
        logger.info("=== Starting COVID-19 ETL Pipeline ===")
        
        # Extract
        raw_data = extract()
        
        # Transform
        countries_data, covid_data = transform(raw_data)
        
        # Load
        load(countries_data, covid_data)
        
        logger.info("=== ETL Pipeline Completed Successfully ===")
        return True
        
    except Exception as e:
        logger.error(f"ETL Pipeline Failed: {str(e)}")
        raise

if __name__ == "__main__":
    run_etl_pipeline()