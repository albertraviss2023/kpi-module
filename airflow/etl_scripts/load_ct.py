import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv(dotenv_path='/opt/etl_scripts/.env')

def get_env_var(name, default=None):
    value = os.getenv(name, default)
    if value is None:
        logger.error(f"Required environment variable '{name}' not set")
        raise EnvironmentError(f"Required environment variable '{name}' not set")
    logger.info(f"Retrieved environment variable '{name}' with value: {value}")
    return value

def load_ct_kpis(kpi_df: pd.DataFrame):
    try:
        logger.info("Starting load of KPI data into kpi_ct table")
        engine = create_engine(
            f"mysql+mysqlconnector://{get_env_var('MYSQL_USER')}:{get_env_var('MYSQL_PASSWORD')}@"
            f"{get_env_var('MYSQL_HOST')}:{get_env_var('MYSQL_PORT')}/{get_env_var('MYSQL_DATABASE')}"
        )
        connection = engine.connect()

        # Create KPI table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS kpi_ct (
            id INT AUTO_INCREMENT PRIMARY KEY,
            run_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            quarter VARCHAR(10),
            pct_new_apps_evaluated_on_time FLOAT,
            pct_amendment_apps_evaluated_on_time FLOAT,
            pct_gcp_inspections_on_time FLOAT,
            pct_safety_reports_assessed_on_time FLOAT,
            pct_gcp_compliant FLOAT,
            pct_registry_submissions_on_time FLOAT,
            pct_capa_evaluated_on_time FLOAT,
            avg_turnaround_time FLOAT
        )
        """
        connection.execute(create_table_query)
        logger.info("Ensured kpi_ct table exists")

        insert_query = """
        INSERT INTO kpi_ct (
            quarter,
            pct_new_apps_evaluated_on_time,
            pct_amendment_apps_evaluated_on_time,
            pct_gcp_inspections_on_time,
            pct_safety_reports_assessed_on_time,
            pct_gcp_compliant,
            pct_registry_submissions_on_time,
            pct_capa_evaluated_on_time,
            avg_turnaround_time
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for _, row in kpi_df.iterrows():
            # Convert NaN to None for MySQL compatibility
            row_data = (
                row['quarter'],
                None if pd.isna(row['pct_new_apps_evaluated_on_time']) else row['pct_new_apps_evaluated_on_time'],
                None if pd.isna(row['pct_amendment_apps_evaluated_on_time']) else row['pct_amendment_apps_evaluated_on_time'],
                None if pd.isna(row['pct_gcp_inspections_on_time']) else row['pct_gcp_inspections_on_time'],
                None if pd.isna(row['pct_safety_reports_assessed_on_time']) else row['pct_safety_reports_assessed_on_time'],
                None if pd.isna(row['pct_gcp_compliant']) else row['pct_gcp_compliant'],
                None if pd.isna(row['pct_registry_submissions_on_time']) else row['pct_registry_submissions_on_time'],
                None if pd.isna(row['pct_capa_evaluated_on_time']) else row['pct_capa_evaluated_on_time'],
                None if pd.isna(row['avg_turnaround_time']) else row['avg_turnaround_time']
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
