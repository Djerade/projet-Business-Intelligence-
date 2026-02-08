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


def transform_data(**context):
    """Clean and transform the ingested data."""
    import logging
    import pandas as pd
    from transformations.clean_macro import (
        clean_world_bank_data,
        clean_alpha_vantage_data,
        clean_imf_data,
        merge_macro_data,
        handle_missing_values,
        save_cleaned_data
    )
    
    logger = logging.getLogger(__name__)
    
    # Pull filepaths from XCom
    ti = context['ti']
    world_bank_filepath = ti.xcom_pull(key='world_bank_filepath', task_ids='ingest_world_bank')
    alpha_vantage_filepath = ti.xcom_pull(key='alpha_vantage_filepath', task_ids='ingest_alpha_vantage')
    imf_filepath = ti.xcom_pull(key='imf_filepath', task_ids='ingest_imf')
    
    # Load World Bank data
    if world_bank_filepath and os.path.exists(world_bank_filepath):
        wb_df = pd.read_parquet(world_bank_filepath)
        logger.info(f"Loaded World Bank data: {wb_df.shape}")
        
        # Clean World Bank data
        wb_clean = clean_world_bank_data(wb_df)
    else:
        raise ValueError("World Bank data file not found")
    

    

    
    # Handle missing values
    merged_df = handle_missing_values(merged_df, method='forward_fill')
    
    # Save cleaned data
    cleaned_filepath = save_cleaned_data(merged_df)
    logger.info(f"Cleaned data saved to: {cleaned_filepath}")
    
    # Push to XCom
    context['ti'].xcom_push(key='cleaned_data_filepath', value=cleaned_filepath)
    
    return cleaned_filepath



task_ingest_world_bank = PythonOperator(
    task_id='ingest_world_bank',
    python_callable=ingest_world_bank_data,
    dag=dag
)

task_transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)


task_ingest_world_bank >> task_transform