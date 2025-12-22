import sys
import os

# Add paths for Docker environment
sys.path.insert(0, '/opt/airflow/src')
sys.path.insert(0, '/opt/airflow')

def run_etl_pipeline():
    """Run the complete ETL pipeline"""
    try:
        # Import inside function
        from src.extract import extract
        from src.transform import transform
        from src.load import load
        
        # Extract
        print("📥 Extracting data from API...")
        raw_data = extract()
        
        # Transform
        print("🔄 Transforming data...")
        countries_data, covid_data = transform(raw_data)
        
        # Load
        print("💾 Loading data to database...")
        load(countries_data, covid_data)
        
        print("✅ ETL Pipeline completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ ETL Pipeline failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_etl_pipeline()
    sys.exit(0 if success else 1)