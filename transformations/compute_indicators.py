"""
Compute derived indicators and metrics.
"""

import pandas as pd
import numpy as np
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def compute_trade_balance_absolute(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute absolute trade balance from percentage of GDP.
    
    Args:
        df: DataFrame with trade_balance_pct and gdp_usd
        
    Returns:
        DataFrame with added trade_balance_usd column
    """
    df = df.copy()
    
    if 'trade_balance_pct' in df.columns and 'gdp_usd' in df.columns:
        df['trade_balance_usd'] = (df['trade_balance_pct'] / 100) * df['gdp_usd']
    else:
        logger.warning("Missing columns for trade balance calculation")
        df['trade_balance_usd'] = np.nan
    
    return df


def compute_debt_absolute(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute absolute debt from percentage of GDP.
    
    Args:
        df: DataFrame with debt_gdp_pct and gdp_usd
        
    Returns:
        DataFrame with added debt_usd column
    """
    df = df.copy()
    
    if 'debt_gdp_pct' in df.columns and 'gdp_usd' in df.columns:
        df['debt_usd'] = (df['debt_gdp_pct'] / 100) * df['gdp_usd']
    else:
        logger.warning("Missing columns for debt calculation")
        df['debt_usd'] = np.nan
    
    return df


def compute_gdp_per_capita(df: pd.DataFrame, population_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Compute GDP per capita (if population data available).
    
    Args:
        df: DataFrame with gdp_usd
        population_df: Optional DataFrame with population data
        
    Returns:
        DataFrame with added gdp_per_capita column
    """
    df = df.copy()
    
    if population_df is not None:
        # Merge population data
        df = df.merge(
            population_df[['country_code', 'year', 'population']],
            on=['country_code', 'year'],
            how='left'
        )
        
        if 'population' in df.columns:
            df['gdp_per_capita'] = df['gdp_usd'] / df['population']
        else:
            df['gdp_per_capita'] = np.nan
    else:
        logger.warning("No population data provided, skipping GDP per capita calculation")
        df['gdp_per_capita'] = np.nan
    
    return df


def compute_rolling_averages(df: pd.DataFrame, window: int = 3) -> pd.DataFrame:
    """
    Compute rolling averages for key indicators.
    
    Args:
        df: DataFrame with time series data
        window: Rolling window size in years
        
    Returns:
        DataFrame with added rolling average columns
    """
    df = df.copy()
    
    numeric_cols = [
        'gdp_growth_pct',
        'inflation_pct',
        'debt_gdp_pct',
        'fx_volatility',
        'trade_balance_pct'
    ]
    
    for col in numeric_cols:
        if col in df.columns:
            rolling_col = f'{col}_rolling_{window}y'
            df[rolling_col] = df.groupby('country_code')[col].transform(
                lambda x: x.rolling(window=window, min_periods=1).mean()
            )
    
    return df


def compute_year_over_year_changes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute year-over-year changes for key indicators.
    
    Args:
        df: DataFrame with time series data
        
    Returns:
        DataFrame with added YoY change columns
    """
    df = df.copy()
    
    numeric_cols = [
        'gdp_growth_pct',
        'inflation_pct',
        'debt_gdp_pct',
        'fx_volatility'
    ]
    
    for col in numeric_cols:
        if col in df.columns:
            yoy_col = f'{col}_yoy_change'
            df[yoy_col] = df.groupby('country_code')[col].pct_change() * 100
    
    return df


def compute_derived_risk_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute additional derived features to support deeper risk analysis:
    - growth_volatility: rolling std of GDP growth (e.g. 5 ans)
    - negative_growth_streak: nombre d'années consécutives de croissance négative
    - prolonged_negative_growth_flag: True si streak >= 2
    - gdp_growth_lag1: croissance de l'année N-1
    - early_warning_flag: signaux précoces de détérioration (chute de croissance ou hausse forte de dette / inflation)
    """
    df = df.copy()
    if 'country_code' not in df.columns or 'year' not in df.columns:
        return df

    # Assurer l'unicité des colonnes avant les opérations groupby/sort
    df = df.loc[:, ~df.columns.duplicated()]

    df = df.sort_values(['country_code', 'year'])

    # Lag de croissance (indicateur avancé)
    if 'gdp_growth_pct' in df.columns:
        df['gdp_growth_lag1'] = df.groupby('country_code')['gdp_growth_pct'].shift(1)

        # Volatilité de la croissance sur 5 ans
        df['growth_volatility'] = (
            df.groupby('country_code')['gdp_growth_pct']
            .transform(lambda x: x.rolling(window=5, min_periods=2).std())
        )

        # Streak de croissance négative
        def _negative_streak(x):
            streak = 0
            res = []
            for val in x:
                if pd.notna(val) and val < 0:
                    streak += 1
                else:
                    streak = 0
                res.append(streak)
            return pd.Series(res, index=x.index)

        df['negative_growth_streak'] = (
            df.groupby('country_code')['gdp_growth_pct'].apply(_negative_streak).reset_index(level=0, drop=True)
        )
        df['prolonged_negative_growth_flag'] = df['negative_growth_streak'] >= 2

    # Signaux précoces (early warning) par pays (diff année à année)
    conds = []
    if 'gdp_growth_pct' in df.columns:
        conds.append(
            df.groupby('country_code')['gdp_growth_pct']
            .diff()
            .le(-2)
        )  # chute >= 2 points
    if 'debt_gdp_pct' in df.columns:
        conds.append(
            df.groupby('country_code')['debt_gdp_pct']
            .diff()
            .ge(5)
        )  # +5 pts de dette/PIB
    if 'inflation_pct' in df.columns:
        conds.append(
            df.groupby('country_code')['inflation_pct']
            .diff()
            .ge(3)
        )  # +3 pts d'inflation

    if conds:
        combined = conds[0]
        for c in conds[1:]:
            combined = combined | c
        df['early_warning_flag'] = combined.fillna(False)
    else:
        df['early_warning_flag'] = False

    return df


def prepare_indicators_for_scoring(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare indicators for risk score calculation.
    Ensures all required columns are present and handles missing values.
    
    Args:
        df: DataFrame with macroeconomic indicators
        
    Returns:
        DataFrame ready for risk scoring
    """
    df = df.copy()
    
    # Required columns for risk scoring
    required_cols = {
        'country_code': 'country_code',
        'year': 'year',
        'inflation_pct': 'inflation',
        'debt_gdp_pct': 'debt_gdp',
        'gdp_growth_pct': 'gdp_growth',
        'fx_volatility': 'fx_volatility',
        'trade_balance_pct': 'trade_balance'
    }
    
    # Rename columns if needed
    for old_name, new_name in required_cols.items():
        if old_name in df.columns and new_name not in df.columns:
            df = df.rename(columns={old_name: new_name})
    
    # Ensure all required columns exist
    missing_cols = set(required_cols.values()) - set(df.columns)
    if missing_cols:
        logger.warning(f"Missing columns for risk scoring: {missing_cols}")
        for col in missing_cols:
            df[col] = np.nan
    
    # Select only relevant columns, en évitant les doublons
    base_cols = ['country_code', 'year']
    indicator_cols = list(required_cols.values())
    scoring_cols = base_cols + [c for c in indicator_cols if c not in base_cols]
    available_cols = [col for col in scoring_cols if col in df.columns]
    
    df_scoring = df[available_cols].copy()
    
    # Remove rows with all missing indicator values
    indicator_cols = [col for col in available_cols if col not in ['country_code', 'year']]
    df_scoring = df_scoring[df_scoring[indicator_cols].notna().any(axis=1)]
    
    logger.info(f"Prepared {df_scoring.shape[0]} records for risk scoring")

    # Enrichir avec les features dérivées pour l'analyse avancée
    df_scoring = compute_derived_risk_features(df_scoring)

    return df_scoring
