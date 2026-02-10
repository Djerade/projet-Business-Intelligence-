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
    'retries': 5,
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
      
        merge_macro_data,
        handle_missing_values,
        save_cleaned_data
    )
    
    logger = logging.getLogger(__name__)
    
    # Pull filepaths from XCom
    ti = context['ti']
    world_bank_filepath = ti.xcom_pull(key='world_bank_filepath', task_ids='ingest_world_bank')

    # Load World Bank data
    if world_bank_filepath and os.path.exists(world_bank_filepath):
        # Support both CSV and Parquet input formats
        if world_bank_filepath.endswith(".csv"):
            wb_df = pd.read_csv(world_bank_filepath)
        else:
            wb_df = pd.read_parquet(world_bank_filepath)
        logger.info(f"Loaded World Bank data: {wb_df.shape}")
        
        # Clean World Bank data
        wb_clean = clean_world_bank_data(wb_df)
    else:
        raise ValueError("World Bank data file not found")
    

    merged_df = merge_macro_data(wb_clean)

    
    # Handle missing values
    merged_df = handle_missing_values(merged_df, method='forward_fill')
    
    # Save cleaned data
    cleaned_filepath = save_cleaned_data(merged_df)
    logger.info(f"Cleaned data saved to: {cleaned_filepath}")
    
    # Push to XCom
    context['ti'].xcom_push(key='cleaned_data_filepath', value=cleaned_filepath)
    
    return cleaned_filepath


def compute_risk_scores(**context):
    """Compute risk scores for countries."""
    import logging
    import pandas as pd
    from transformations.compute_indicators import (
        compute_trade_balance_absolute,
        compute_debt_absolute,
        prepare_indicators_for_scoring
    )
    from transformations.risk_score import compute_comprehensive_risk
    
    logger = logging.getLogger(__name__)
    
    # Pull cleaned data filepath from XCom
    ti = context['ti']
    cleaned_filepath = ti.xcom_pull(key='cleaned_data_filepath', task_ids='transform_data')
    
    if not cleaned_filepath or not os.path.exists(cleaned_filepath):
        raise ValueError("Cleaned data file not found")
    
    # Load cleaned data
    df = pd.read_parquet(cleaned_filepath)
    logger.info(f"Loaded cleaned data: {df.shape}")
    
    # Compute derived indicators
    df = compute_trade_balance_absolute(df)
    df = compute_debt_absolute(df)
    
    # Prepare for scoring
    df_scoring = prepare_indicators_for_scoring(df)
    
    # Compute risk scores
    df_risk = compute_comprehensive_risk(df_scoring)
    
    # Save risk-scored data
    from transformations.clean_macro import save_cleaned_data
    risk_filepath = save_cleaned_data(df_risk, filename=f"risk_scored_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet")
    logger.info(f"Risk-scored data saved to: {risk_filepath}")
    
    # Push to XCom
    context['ti'].xcom_push(key='risk_data_filepath', value=risk_filepath)
    
    return risk_filepath

def load_dimensions(**context):
    """Load dimension tables into data warehouse."""
    import logging
    from warehouse.load_dims import DimensionLoader, create_sample_dimensions
    
    logger = logging.getLogger(__name__)
    
    # Initialize dimension loader
    loader = DimensionLoader()
    
    # Create sample dimensions (in production, this would load from a source)
    create_sample_dimensions(loader)
    
    logger.info("Dimensions loaded successfully")



def load_facts(**context):
    """Load fact table into data warehouse."""
    import logging
    import pandas as pd
    from warehouse.load_facts import FactLoader
    
    logger = logging.getLogger(__name__)
    
    # Pull risk data filepath from XCom
    ti = context['ti']
    risk_filepath = ti.xcom_pull(key='risk_data_filepath', task_ids='compute_risk_scores')
    
    if not risk_filepath or not os.path.exists(risk_filepath):
        raise ValueError("Risk data file not found")
    
    # Load risk data
    df = pd.read_parquet(risk_filepath)
    logger.info(f"Loading {df.shape[0]} records into fact table")
    
    # Initialize fact loader
    loader = FactLoader()
    
    # Load facts
    loader.load_fact_country_risk(df, data_source='airflow_pipeline')
    
    logger.info("Fact table loaded successfully")





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

task_compute_risk = PythonOperator(
    task_id='compute_risk_scores',
    python_callable=compute_risk_scores,
    dag=dag
)


task_load_dims = PythonOperator(
    task_id='load_dimensions',
    python_callable=load_dimensions,
    dag=dag
)

task_load_facts = PythonOperator(
    task_id='load_facts',
    python_callable=load_facts,
    dag=dag
)


task_ingest_world_bank >> task_transform >> task_compute_risk >> task_load_dims >> task_load_facts 