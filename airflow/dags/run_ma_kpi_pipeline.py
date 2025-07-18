from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get the absolute path to the airflow directory
airflow_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Add both the airflow directory and etl_scripts to sys.path
sys.path.insert(0, airflow_dir)
sys.path.insert(0, '/opt/etl_scripts')  # Docker-mounted ETL scripts

from extract_ma import extract_ma_logs
from transform_ma import transform_ma_kpis
from load_ma import load_ma_kpis

def run_ma_etl():
    try:
        logger.info("Starting ETL pipeline")
        df = extract_ma_logs()
        logger.info("Extraction completed")
        kpi_df = transform_ma_kpis(df)
        logger.info("Transformation completed")
        load_ma_kpis(kpi_df)
        logger.info("Load completed")
    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}")
        raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'etl_ma_pipeline',
    default_args=default_args,
    description='ETL pipeline for Market Authorization KPIs',
    schedule_interval='@quarterly',
    catchup=False
)

task = PythonOperator(
    task_id='run_ma_etl_task',
    python_callable=run_ma_etl,
    dag=dag
)
