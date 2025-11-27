from sqlalchemy import create_engine
import yaml
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env

def get_db_config():
    """Loading database configuration from YAML and environment variables"""
    with open('config/db_config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    
    # Override password from environment variable for security
    config['database']['password'] = os.getenv('DB_PASSWORD')
    return config['database']

def create_db_engine():
    """Create SQLAlchemy engine with connection pooling"""
    db_config = get_db_config()
    connection_string = (
        f"postgresql://{db_config['username']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['database_name']}"
    )
    
    engine = create_engine(
        connection_string,
        pool_size=5,
        max_overflow=10,
        echo=False  # Set to True for SQL query logging
    )
    return engine