import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv(dotenv_path='/opt/etl_scripts/.env')  # Mount this in Docker

def get_env_var(name, default=None):
    value = os.getenv(name, default)
    if value is None:
        logger.error(f"Required environment variable '{name}' not set")
        raise EnvironmentError(f"Required environment variable '{name}' not set")
    logger.info(f"Retrieved environment variable '{name}' with value: {value}")
    return value

def load_ma_kpis(kpi_df: pd.DataFrame):
    try:
        logger.info("Starting load of KPI data into kpi_ma table")
        engine = create_engine(
            f"mysql+mysqlconnector://{get_env_var('MYSQL_USER')}:{get_env_var('MYSQL_PASSWORD')}@"
            f"{get_env_var('MYSQL_HOST')}:{get_env_var('MYSQL_PORT', '3306')}/{get_env_var('MYSQL_DATABASE')}"
        )
        connection = engine.connect()

        # Create KPI table matching the original schema
        create_table_query = """
        CREATE TABLE IF NOT EXISTS kpi_ma (
            id INT AUTO_INCREMENT PRIMARY KEY,
            run_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            quarter VARCHAR(10),
            pct_new_apps_evaluated_on_time FLOAT,
            pct_renewal_apps_evaluated_on_time FLOAT,
            pct_variation_apps_evaluated_on_time FLOAT,
            pct_fir_responses_on_time FLOAT,
            pct_query_responses_evaluated_on_time FLOAT,
            pct_granted_within_90_days FLOAT,
            median_duration_continental FLOAT
        )
        """
        connection.execute(create_table_query)
        logger.info("Ensured kpi_ma table exists")

        insert_query = """
        INSERT INTO kpi_ma (
            quarter,
            pct_new_apps_evaluated_on_time,
            pct_renewal_apps_evaluated_on_time,
            pct_variation_apps_evaluated_on_time,
            pct_fir_responses_on_time,
            pct_query_responses_evaluated_on_time,
            pct_granted_within_90_days,
            median_duration_continental
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        for _, row in kpi_df.iterrows():
            # Convert NaN to None for MySQL compatibility
            row_data = (
                row['quarter'],
                None if pd.isna(row['pct_new_apps_evaluated_on_time']) else row['pct_new_apps_evaluated_on_time'],
                None if pd.isna(row['pct_renewal_apps_evaluated_on_time']) else row['pct_renewal_apps_evaluated_on_time'],
                None if pd.isna(row['pct_variation_apps_evaluated_on_time']) else row['pct_variation_apps_evaluated_on_time'],
                None if pd.isna(row['pct_fir_responses_on_time']) else row['pct_fir_responses_on_time'],
                None if pd.isna(row['pct_query_responses_evaluated_on_time']) else row['pct_query_responses_evaluated_on_time'],
                None if pd.isna(row['pct_granted_within_90_days']) else row['pct_granted_within_90_days'],
                None if pd.isna(row['median_duration_continental']) else row['median_duration_continental']
            )
            connection.execute(insert_query, row_data)
            logger.info(f"Inserted KPI row for quarter {row['quarter']}")

        connection.execute("COMMIT")
        logger.info("Committed KPI data to database")
        connection.close()
        engine.dispose()
        logger.info("Database connection closed")
    except Exception as e:
        logger.error(f"Failed to load KPI data: {str(e)}")
        raise
    