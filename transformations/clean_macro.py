"""
Data cleaning and preprocessing for macroeconomic data.
"""

import pandas as pd
import numpy as np
import logging
from typing import Optional, Dict
from datetime import datetime
import os

logger = logging.getLogger(__name__)


def clean_world_bank_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and pivot World Bank data.
    
    Args:
        df: Raw World Bank DataFrame
        
    Returns:
        Cleaned DataFrame with indicators as columns
    """
    if df.empty:
        return df
    
    df = df.copy()
    
    # Filter out null values
    df = df[df['value'].notna()]
    
    # Pivot table: indicators as columns
    pivot_df = df.pivot_table(
        index=['country_code', 'year'],
        columns='indicator',
        values='value',
        aggfunc='first'
    ).reset_index()
    
    # Flatten column names
    pivot_df.columns.name = None
    
    # Convert year to integer
    pivot_df['year'] = pd.to_numeric(pivot_df['year'], errors='coerce')
    
    # Rename columns for consistency
    column_mapping = {
        'gdp': 'gdp_usd',
        'gdp_growth': 'gdp_growth_pct',
        'inflation': 'inflation_pct',
        'debt_gdp': 'debt_gdp_pct',
        'trade_balance': 'trade_balance_pct',
        'current_account': 'current_account_pct'
    }
    
    for old_name, new_name in column_mapping.items():
        if old_name in pivot_df.columns:
            pivot_df = pivot_df.rename(columns={old_name: new_name})
    
    # Sort by country and year
    pivot_df = pivot_df.sort_values(['country_code', 'year']).reset_index(drop=True)
    
    logger.info(f"Cleaned World Bank data: {pivot_df.shape[0]} records")
    
    return pivot_df




def merge_macro_data(
    world_bank_df: pd.DataFrame,

) -> pd.DataFrame:
    """
    Merge World Bank an
    
    Args:
        world_bank_df: Cleaned World Bank DataFrame
        alpha_vantage_df: Cleaned Alpha Vantage DataFrame
        
    Returns:
        Merged DataFrame
    """
    if world_bank_df.empty:
        logger.warning("World Bank DataFrame is empty")

    merged_df = world_bank_df.sort_values(['country_code', 'year']).reset_index(drop=True)

    logger.info(f"Merged data: {merged_df.shape[0]} records")
    
    return merged_df


def handle_missing_values(df: pd.DataFrame, method: str = 'forward_fill') -> pd.DataFrame:
    """
    Handle missing values in the dataset.
    
    Args:
        df: DataFrame with potential missing values
        method: Method to use ('forward_fill', 'backward_fill', 'interpolate', 'drop')
        
    Returns:
        DataFrame with handled missing values
    """
    df = df.copy()
    
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    
    if method == 'forward_fill':
        df[numeric_cols] = df.groupby('country_code')[numeric_cols].ffill()
    elif method == 'backward_fill':
        df[numeric_cols] = df.groupby('country_code')[numeric_cols].bfill()
    elif method == 'interpolate':
        df[numeric_cols] = df.groupby('country_code')[numeric_cols].interpolate()
    elif method == 'drop':
        df = df.dropna(subset=numeric_cols)
    else:
        logger.warning(f"Unknown method: {method}. Using forward_fill.")
        df[numeric_cols] = df.groupby('country_code')[numeric_cols].ffill()
    
    logger.info(f"Handled missing values using method: {method}")
    
    return df


def save_cleaned_data(df: pd.DataFrame, filename: Optional[str] = None) -> str:
    """
    Save cleaned data to processed zone.
    
    Args:
        df: Cleaned DataFrame
        filename: Optional filename
        
    Returns:
        Path to saved file
    """
    if df.empty:
        raise ValueError("Cannot save empty DataFrame")
    
    if filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"cleaned_macro_data_{timestamp}.parquet"
    
    base_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'processed')
    os.makedirs(base_path, exist_ok=True)
    filepath = os.path.join(base_path, filename)
    
    df.to_parquet(filepath, index=False, engine='pyarrow')
    logger.info(f"Cleaned data saved to: {filepath}")
    
    return filepath
