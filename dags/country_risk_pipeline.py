"""
Airflow DAG for Country Risk BI Pipeline.

This DAG orchestrates the complete data pipeline:
1. Ingest data from World Bank and Alpha Vantage APIs
2. Clean and transform the data
3. Calculate risk scores
4. Load into data warehouse
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import sys
import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE_DIR))

# Add project root to path
sys.path.insert(0, '/opt/airflow')

# Import African countries list
from ingestion.african_countries import AFRICAN_COUNTRIES

# Default arguments
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1)
}

dag = DAG(
    'country_risk_pipeline',
    default_args=default_args,
    description='Country Risk BI Data Pipeline',
    schedule_interval='@daily',  # Run daily
    catchup=False,
    tags=['bi', 'country_risk', 'macroeconomic']
)


def ingest_world_bank_data(**context):
    """Ingest data from World Bank API."""
    import logging
    from ingestion.world_bank import WorldBankIngestion
    
    logger = logging.getLogger(__name__)
    
    # List of all African countries to process
    countries = AFRICAN_COUNTRIES
    
    # Initialize World Bank client
    wb = WorldBankIngestion()
    
    # Fetch data for last 10 years
    current_year = datetime.now().year
    df = wb.fetch_multiple_countries(
        countries,
        start_date=current_year - 10,
        end_date=current_year
    )
    
    if not df.empty:
        # Save to data lake
        filepath = wb.save_to_data_lake(df)
        logger.info(f"World Bank data saved to: {filepath}")
        
        # Push filepath to XCom for downstream tasks
        context['ti'].xcom_push(key='world_bank_filepath', value=filepath)
        return filepath
    else:
        raise ValueError("No data fetched from World Bank API")



task_ingest_world_bank = PythonOperator(
    task_id='ingest_world_bank',
    python_callable=ingest_world_bank_data,
    dag=dag
)



task_ingest_world_bank