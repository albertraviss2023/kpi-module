import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv(dotenv_path='/opt/etl_scripts/.env')

def get_env_var(name, default=None):
    value = os.getenv(name, default)
    if value is None:
        logger.error(f"Required environment variable '{name}' is not set and no default provided.")
        raise EnvironmentError(f"Required environment variable '{name}' is not set.")
    logger.info(f"Retrieved environment variable '{name}' with value: {value}")
    return value

def extract_ct_logs():
    try:
        logger.info("Starting extraction from ct_logs table")
        user = get_env_var('MYSQL_USER')
        password = get_env_var('MYSQL_PASSWORD')
        host = get_env_var('MYSQL_HOST')
        port = get_env_var('MYSQL_PORT')
        database = get_env_var('MYSQL_DATABASE')
        
        connection_string = (
            f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"
        )
        logger.info(f"Connecting to database with connection string: {connection_string}")
        
        engine = create_engine(connection_string)
        query = "SELECT * FROM ct_logs"
        df = pd.read_sql(query, engine)
        logger.info(f"Extracted {len(df)} rows from ct_logs")
        logger.info(f"ct_logs columns: {df.columns.tolist()}")
        engine.dispose()
        return df
    except Exception as e:
        logger.error(f"Failed to extract data: {str(e)}")
        raise
    