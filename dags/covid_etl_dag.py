from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import sys
import os

# Add our source code to Python path
sys.path.insert(0, '/opt/airflow/src')
sys.path.insert(0, '/opt/airflow/scripts')
sys.path.insert(0, '/opt/airflow')

def extract_task():
    """Extract data from COVID API"""
    try:
        from src.extract import extract
        print("📥 Starting extraction from API...")
        df_raw = extract()
        print(f"✅ Extraction complete: {len(df_raw)} records")
        
        # Return data to XCom (Airflow's cross-communication)
        return df_raw.to_json()  # Convert DataFrame to JSON for XCom
        
    except Exception as e:
        print(f"❌ Extraction failed: {str(e)}")
        raise

def transform_task(**context):
    """Transform the extracted data"""
    try:
        import pandas as pd
        from src.transform import transform
        
        # Get data from previous task via XCom
        ti = context['ti']
        json_data = ti.xcom_pull(task_ids='extract_data')
        df_raw = pd.read_json(json_data)
        
        print("🔄 Starting transformation...")
        countries_df, covid_df = transform(df_raw)
        print(f"✅ Transformation complete")
        print(f"   - Countries: {len(countries_df)} records")
        print(f"   - COVID stats: {len(covid_df)} records")
        
        # Return both DataFrames as JSON
        return {
            'countries': countries_df.to_json(),
            'covid': covid_df.to_json()
        }
        
    except Exception as e:
        print(f"❌ Transformation failed: {str(e)}")
        raise

def load_task(**context):
    """Load transformed data to database"""
    try:
        import pandas as pd
        from src.load import load
        
        # Get data from previous task via XCom
        ti = context['ti']
        data_dict = ti.xcom_pull(task_ids='transform_data')
        
        countries_df = pd.read_json(data_dict['countries'])
        covid_df = pd.read_json(data_dict['covid'])
        
        print("💾 Starting load to database...")
        load(countries_df, covid_df)
        print("✅ Load complete")
        
    except Exception as e:
        print(f"❌ Load failed: {str(e)}")
        raise


# Default arguments for the DAG          
default_args = {
    'owner': 'Usha Chalise',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'covid_etl_pipeline',
    default_args=default_args,
    description='Daily COVID-19 Data ETL Pipeline',
    schedule_interval='0 7 * * *',  # Daily at 7 AM (cron syntax)
    catchup=False,
    tags=['covid', 'etl', 'data-pipeline'],
)

# Define tasks
with dag:
    # Start marker
    start = EmptyOperator(task_id='start')
    
    # ETL tasks
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_task,
    )
    
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_task,
    )
    
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_task,
    )
    
    # End marker
    end = EmptyOperator(task_id='end')
    
    # Define workflow
    start >> extract_data >> transform_data >> load_data >> end