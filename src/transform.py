import pandas as pd
from datetime import datetime
import os
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

def validate_data(df):
    """Perform basic data validation"""
    logger.info("Validating raw data...")
    
    # Check required columns exist
    required_columns = ['country', 'countryInfo.iso3', 'population', 'cases', 'deaths', 'recovered']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Check for null values in critical columns
    critical_nulls = df[['country']].isnull().sum()
    if critical_nulls.any():
        logger.warning(f"Null values found in critical columns: {critical_nulls.to_dict()}")
    
    logger.info("Data validation completed")
    return True

def create_countries_dimension(df):
    """Create countries dimension table"""
    logger.info("Creating countries dimension...")
    
    countries_df = df[['country', 'countryInfo.iso3', 'population']].copy()
    countries_df.columns = ['country_name', 'iso_code', 'population']
    
    # Data cleaning
    countries_df['country_name'] = countries_df['country_name'].str.strip()
    countries_df['iso_code'] = countries_df['iso_code'].str.upper()
    countries_df['population'] = pd.to_numeric(countries_df['population'], errors='coerce')
    
    # Remove duplicates
    countries_df = countries_df.drop_duplicates(subset=['country_name']).reset_index(drop=True)
    
    logger.info(f"Created dimension for {len(countries_df)} countries")
    return countries_df

def create_covid_facts(df):
    """Create COVID-19 facts table"""
    logger.info("Creating COVID-19 facts...")
    
    covid_df = df[['country', 'cases', 'deaths', 'recovered', 'tests']].copy()
    covid_df.columns = ['country_name', 'cases', 'deaths', 'recovered', 'tests']
    
    # Data cleaning and type conversion
    numeric_columns = ['cases', 'deaths', 'recovered', 'tests']
    for col in numeric_columns:
        covid_df[col] = pd.to_numeric(covid_df[col], errors='coerce').fillna(0).astype('Int64')
    
    # Add processing date
    covid_df['date'] = datetime.now().date()
    covid_df['country_name'] = covid_df['country_name'].str.strip()
    
    logger.info(f"Created facts for {len(covid_df)} country records")
    return covid_df

def save_processed_data(countries_df, covid_df):
    """Save processed data as CSV"""
    os.makedirs('data/processed', exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    countries_file = f"data/processed/countries_{timestamp}.csv"
    covid_file = f"data/processed/covid_facts_{timestamp}.csv"
    
    countries_df.to_csv(countries_file, index=False)
    covid_df.to_csv(covid_file, index=False)
    
    logger.info(f"Processed data saved: {countries_file}, {covid_file}")

def transform(df_raw):
    """Transforming data and returning both DataFrames"""
    logger.info("Starting transformation process")
    
    # Validate incoming data
    validate_data(df_raw)
    
    # Create dimension and fact tables
    countries_df = create_countries_dimension(df_raw)
    covid_df = create_covid_facts(df_raw)
    
    # Save processed data
    save_processed_data(countries_df, covid_df)
    
    logger.info("Transformation process completed successfully")
    return countries_df, covid_df  # returning dataframes for next task

if __name__ == "__main__":
    # For testing - you would need to pass a DataFrame
    pass