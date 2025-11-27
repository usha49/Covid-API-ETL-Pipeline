import pandas as pd
from sqlalchemy import text
from src.utils.db_connection import create_db_engine
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

def upsert_countries(connection, countries_df):
    """Upsert countries into dimension table"""
    logger.info("Loading countries dimension...")
    
    # Create temporary table
    temp_table_name = "temp_countries"
    countries_df.to_sql(
        temp_table_name, 
        connection, 
        if_exists='replace', 
        index=False
    )
    
    # Upsert from temporary table
    upsert_sql = """
        INSERT INTO dim_countries (country_name, iso_code, population)
        SELECT country_name, iso_code, population FROM temp_countries
        ON CONFLICT (country_name) 
        DO UPDATE SET 
            iso_code = EXCLUDED.iso_code,
            population = EXCLUDED.population
    """
    
    result = connection.execute(text(upsert_sql))
    connection.execute(text(f"DROP TABLE {temp_table_name}"))
    
    logger.info(f"Countries dimension updated: {result.rowcount} records affected")

def load_covid_facts(connection, covid_df):
    """Load COVID-19 facts into fact table"""
    logger.info("Loading COVID-19 facts...")
    
    # Get country_id mapping
    country_map_query = "SELECT country_id, country_name FROM dim_countries"
    country_map = pd.read_sql(country_map_query, connection)
    
    # Merge with country IDs
    fact_final_df = covid_df.merge(
        country_map,
        on='country_name',
        how='left'
    )
    
    # Check for countries that didn't match
    unmatched = fact_final_df[fact_final_df['country_id'].isnull()]
    if not unmatched.empty:
        logger.warning(f"Countries not found in dimension table: {unmatched['country_name'].tolist()}")
    
    # Prepare final fact data
    fact_final_df = fact_final_df[['date', 'country_id', 'cases', 'deaths', 'recovered', 'tests']]
    fact_final_df = fact_final_df.dropna(subset=['country_id'])  # Remove unmatched countries
    
    # Load to database
    fact_final_df.to_sql(
        'fact_covid_daily',
        connection,
        if_exists='append',
        index=False
    )
    
    logger.info(f"Loaded {len(fact_final_df)} fact records")

def load(countries_df, covid_df):
    """Main load function"""
    logger.info("Starting load process")
    
    engine = create_db_engine()
    
    with engine.connect() as connection:
        with connection.begin():  # Start transaction
            upsert_countries(connection, countries_df)
            load_covid_facts(connection, covid_df)
    
    logger.info("Load process completed successfully")

if __name__ == "__main__":
    # For testing
    pass