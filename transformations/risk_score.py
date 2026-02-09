"""
Calculate economic risk score for countries.
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# Default weights for risk score calculation
DEFAULT_WEIGHTS = {
    'inflation': 0.25,
    'debt_gdp': 0.25,
    'gdp_growth': 0.20,
    'fx_volatility': 0.15,
    'trade_balance': 0.15
}

# Risk direction: higher values = higher risk (1) or lower risk (-1)
RISK_DIRECTION = {
    'inflation': 1,  # Higher inflation = higher risk
    'debt_gdp': 1,   # Higher debt = higher risk
    'gdp_growth': -1,  # Higher growth = lower risk
    'fx_volatility': 1,  # Higher volatility = higher risk
    'trade_balance': -1  # Higher trade balance = lower risk
}


def normalize_indicator(
    values: pd.Series,
    method: str = 'min_max',
    inverse: bool = False
) -> pd.Series:
    """
    Normalize indicator values to 0-100 scale.
    
    Args:
        values: Series of indicator values
        method: Normalization method ('min_max' or 'z_score')
        inverse: If True, invert the scale (for indicators where lower is better)
        
    Returns:
        Normalized Series (0-100 scale)
    """
    if values.empty or values.isna().all():
        return pd.Series(index=values.index, dtype=float)
    
    valid_values = values.dropna()
    
    if len(valid_values) == 0:
        return pd.Series(index=values.index, dtype=float)
    
    if method == 'min_max':
        min_val = valid_values.min()
        max_val = valid_values.max()
        
        if max_val == min_val:
            # All values are the same
            normalized = pd.Series(50.0, index=values.index)
        else:
            normalized = ((values - min_val) / (max_val - min_val)) * 100
        
    elif method == 'z_score':
        mean_val = valid_values.mean()
        std_val = valid_values.std()
        
        if std_val == 0:
            normalized = pd.Series(50.0, index=values.index)
        else:
            # Convert z-score to 0-100 scale
            z_scores = (values - mean_val) / std_val
            # Map z-scores to 0-100 (assuming normal distribution)
            normalized = 50 + (z_scores * 20)  # Scale so ±2.5 std = 0-100
            normalized = normalized.clip(0, 100)
    else:
        raise ValueError(f"Unknown normalization method: {method}")
    
    # Invert if needed (for indicators where lower is better)
    if inverse:
        normalized = 100 - normalized
    
    return normalized


def calculate_risk_score(
    df: pd.DataFrame,
    weights: Optional[Dict[str, float]] = None,
    normalization_method: str = 'min_max'
) -> pd.DataFrame:
    """
    Calculate economic risk score for each country-year.
    
    Risk score ranges from 0 (low risk) to 100 (high risk).
    
    Args:
        df: DataFrame with macroeconomic indicators
        weights: Dictionary of indicator weights (must sum to 1.0)
        normalization_method: Method for normalizing indicators
        
    Returns:
        DataFrame with added risk_score column
    """
    if df.empty:
        logger.warning("Empty DataFrame provided for risk score calculation")
        return df
    
    df = df.copy()
    weights = weights or DEFAULT_WEIGHTS
    
    # Validate weights sum to 1.0
    total_weight = sum(weights.values())
    if abs(total_weight - 1.0) > 0.01:
        logger.warning(f"Weights sum to {total_weight}, normalizing to 1.0")
        weights = {k: v / total_weight for k, v in weights.items()}
    
    # Normalize each indicator
    normalized_scores = {}
    
    for indicator, weight in weights.items():
        if indicator not in df.columns:
            logger.warning(f"Indicator {indicator} not found in DataFrame")
            continue
        
        # Get risk direction
        inverse = RISK_DIRECTION.get(indicator, 1) == -1
        
        # Normalize indicator
        normalized = normalize_indicator(
            df[indicator],
            method=normalization_method,
            inverse=inverse
        )
        
        normalized_scores[indicator] = normalized * weight
    
    # Calculate weighted risk score
    if normalized_scores:
        df['risk_score'] = sum(normalized_scores.values())
        df['risk_score'] = df['risk_score'].clip(0, 100)
        
        # Round to 2 decimal places
        df['risk_score'] = df['risk_score'].round(2)
    else:
        logger.error("No indicators available for risk score calculation")
        df['risk_score'] = np.nan
    
    return df


def calculate_risk_category(risk_score: float) -> str:
    """
    Categorize risk score into risk levels.
    
    Args:
        risk_score: Risk score value (0-100)
        
    Returns:
        Risk category string
    """
    if pd.isna(risk_score):
        return 'Unknown'
    
    if risk_score < 25:
        return 'Low'
    elif risk_score < 50:
        return 'Moderate'
    elif risk_score < 75:
        return 'High'
    else:
        return 'Very High'


def add_risk_categories(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add risk category column to DataFrame.
    
    Args:
        df: DataFrame with risk_score column
        
    Returns:
        DataFrame with added risk_category column
    """
    df = df.copy()
    
    if 'risk_score' not in df.columns:
        logger.warning("risk_score column not found")
        return df
    
    df['risk_category'] = df['risk_score'].apply(calculate_risk_category)
    
    return df


def calculate_risk_trend(df: pd.DataFrame, window: int = 3) -> pd.DataFrame:
    """
    Calculate risk trend (improving, stable, deteriorating).
    
    Args:
        df: DataFrame with risk_score column
        window: Number of years to consider for trend
        
    Returns:
        DataFrame with added risk_trend column
    """
    df = df.copy()
    
    if 'risk_score' not in df.columns:
        logger.warning("risk_score column not found")
        return df
        
    df = df.loc[:, ~df.columns.duplicated()]
    # Calculate rolling average and compare with current value
    df = df.sort_values(['country_code', 'year'])
    
    df['risk_score_rolling'] = df.groupby('country_code')['risk_score'].transform(
        lambda x: x.rolling(window=window, min_periods=2).mean()
    )
    
    # Calculate trend
    def get_trend(row):
        if pd.isna(row['risk_score']) or pd.isna(row['risk_score_rolling']):
            return 'Unknown'
        
        diff = row['risk_score'] - row['risk_score_rolling']
        
        if abs(diff) < 2:
            return 'Stable'
        elif diff > 0:
            return 'Deteriorating'
        else:
            return 'Improving'
    
    df['risk_trend'] = df.apply(get_trend, axis=1)
    
    # Drop temporary column
    df = df.drop(columns=['risk_score_rolling'])
    
    return df


def compute_comprehensive_risk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute comprehensive risk score with all components.
    
    Args:
        df: DataFrame with macroeconomic indicators
        
    Returns:
        DataFrame with risk score, category, and trend
    """
    # Calculate risk score
    df = calculate_risk_score(df)
    
    # Add risk category
    df = add_risk_categories(df)
    
    # Calculate risk trend
    df = calculate_risk_trend(df)
    
    logger.info(f"Computed risk scores for {df.shape[0]} records")
    
    return df
