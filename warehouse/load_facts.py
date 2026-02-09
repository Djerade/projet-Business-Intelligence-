"""
Load fact table into the data warehouse.
"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class FactLoader:
    """
    Class for loading fact table.
    """
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize fact loader.
        
        Args:
            connection_string: PostgreSQL connection string
        """
        if connection_string is None:
            # Build connection string from environment variables
            db_host = os.getenv('DB_HOST', 'localhost')
            db_port = os.getenv('DB_PORT', '5432')
            db_name = os.getenv('DB_NAME', 'country_risk_dw')
            db_user = os.getenv('DB_USER', 'parfait')
            db_password = os.getenv('DB_PASSWORD', 'parfait')
            
            connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        
        self.engine = create_engine(connection_string)
        self.connection_string = connection_string
    
    def get_date_key(self, date: datetime) -> int:
        """
        Get or create date key for a given date.
        
        Args:
            date: Datetime object
            
        Returns:
            Date key (YYYYMMDD format)
        """
        date_key = int(date.strftime('%Y%m%d'))
        
        # Ensure date exists in dim_date
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT date_key FROM dim_date WHERE date_key = :date_key
            """), {'date_key': date_key})
            
            if result.fetchone() is None:
                # Insert date into dim_date using the function
                conn.execute(text("SELECT get_date_key(:date)"), {'date': date.date()})
                conn.commit()
        
        return date_key
    
    def load_fact_country_risk(self, df: pd.DataFrame, data_source: str = 'pipeline') -> None:
        """
        Load country risk facts into fact_country_risk.
        
        Args:
            df: DataFrame with risk data
            data_source: Source identifier for the data
        """
        if df.empty:
            logger.warning("Empty DataFrame provided for fact loading")
            return
        
        # Required columns
        required_cols = ['country_code', 'year']
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"DataFrame must contain columns: {required_cols}")
        
        df = df.copy()
        
        # Ensure date column exists
        if 'date' not in df.columns:
            # Create date from year (first day of year)
            df['date'] = pd.to_datetime(df['year'].astype(str) + '-01-01')
        else:
            df['date'] = pd.to_datetime(df['date'])
        
        # Get date keys
        df['date_key'] = df['date'].apply(lambda x: self.get_date_key(x))
        
        # Map columns to fact table columns
        column_mapping = {
            'country_code': 'country_code',
            'date_key': 'date_key',
            'year': 'year',
            'month': 'month',
            'gdp_usd': 'gdp_usd',
            'gdp_growth': 'gdp_growth',
            'gdp_growth_pct': 'gdp_growth',
            'inflation': 'inflation',
            'inflation_pct': 'inflation',
            'debt_gdp': 'debt_gdp',
            'debt_gdp_pct': 'debt_gdp',
            'trade_balance': 'trade_balance',
            'trade_balance_pct': 'trade_balance',
            'current_account': 'current_account',
            'current_account_pct': 'current_account',
            'fx_volatility': 'fx_volatility',
            'trade_balance_usd': 'trade_balance_usd',
            'debt_usd': 'debt_usd',
            'risk_score': 'risk_score',
            'risk_category': 'risk_category',
            'risk_trend': 'risk_trend'
        }
        
        # Select and rename columns
        fact_cols = [
            'country_code', 'date_key', 'year', 'month',
            'gdp_usd', 'gdp_growth', 'inflation', 'debt_gdp',
            'trade_balance', 'current_account', 'fx_volatility',
            'trade_balance_usd', 'debt_usd',
            'risk_score', 'risk_category', 'risk_trend', 'data_source'
        ]
        
        fact_df = pd.DataFrame()
        for col in fact_cols:
            if col == 'data_source':
                fact_df[col] = data_source
            elif col == 'month':
                # Extract month from date if not present
                if 'month' in df.columns:
                    fact_df[col] = df['month']
                else:
                    fact_df[col] = df['date'].dt.month
            else:
                # Try to find the column in the source DataFrame
                source_col = None
                for src_col, target_col in column_mapping.items():
                    if target_col == col and src_col in df.columns:
                        source_col = src_col
                        break
                
                if source_col:
                    fact_df[col] = df[source_col]
                else:
                    fact_df[col] = None
        
        # Ensure month is integer
        if 'month' in fact_df.columns:
            fact_df['month'] = fact_df['month'].fillna(1).astype(int)
        
        # Load to database (upsert)
        # Use ON CONFLICT to handle duplicates
        with self.engine.connect() as conn:
            for _, row in fact_df.iterrows():
                conn.execute(text("""
                    INSERT INTO fact_country_risk (
                        country_code, date_key, year, month,
                        gdp_usd, gdp_growth, inflation, debt_gdp,
                        trade_balance, current_account, fx_volatility,
                        trade_balance_usd, debt_usd,
                        risk_score, risk_category, risk_trend, data_source
                    )
                    VALUES (
                        :country_code, :date_key, :year, :month,
                        :gdp_usd, :gdp_growth, :inflation, :debt_gdp,
                        :trade_balance, :current_account, :fx_volatility,
                        :trade_balance_usd, :debt_usd,
                        :risk_score, :risk_category, :risk_trend, :data_source
                    )
                    ON CONFLICT (country_code, date_key, year) DO UPDATE
                    SET
                        month = EXCLUDED.month,
                        gdp_usd = EXCLUDED.gdp_usd,
                        gdp_growth = EXCLUDED.gdp_growth,
                        inflation = EXCLUDED.inflation,
                        debt_gdp = EXCLUDED.debt_gdp,
                        trade_balance = EXCLUDED.trade_balance,
                        current_account = EXCLUDED.current_account,
                        fx_volatility = EXCLUDED.fx_volatility,
                        trade_balance_usd = EXCLUDED.trade_balance_usd,
                        debt_usd = EXCLUDED.debt_usd,
                        risk_score = EXCLUDED.risk_score,
                        risk_category = EXCLUDED.risk_category,
                        risk_trend = EXCLUDED.risk_trend,
                        data_source = EXCLUDED.data_source,
                        loaded_at = CURRENT_TIMESTAMP
                """), {
                    'country_code': row['country_code'],
                    'date_key': int(row['date_key']),
                    'year': int(row['year']),
                    'month': int(row['month']) if pd.notna(row['month']) else None,
                    'gdp_usd': float(row['gdp_usd']) if pd.notna(row['gdp_usd']) else None,
                    'gdp_growth': float(row['gdp_growth']) if pd.notna(row['gdp_growth']) else None,
                    'inflation': float(row['inflation']) if pd.notna(row['inflation']) else None,
                    'debt_gdp': float(row['debt_gdp']) if pd.notna(row['debt_gdp']) else None,
                    'trade_balance': float(row['trade_balance']) if pd.notna(row['trade_balance']) else None,
                    'current_account': float(row['current_account']) if pd.notna(row['current_account']) else None,
                    'fx_volatility': float(row['fx_volatility']) if pd.notna(row['fx_volatility']) else None,
                    'trade_balance_usd': float(row['trade_balance_usd']) if pd.notna(row['trade_balance_usd']) else None,
                    'debt_usd': float(row['debt_usd']) if pd.notna(row['debt_usd']) else None,
                    'risk_score': float(row['risk_score']) if pd.notna(row['risk_score']) else None,
                    'risk_category': row['risk_category'] if pd.notna(row['risk_category']) else None,
                    'risk_trend': row['risk_trend'] if pd.notna(row['risk_trend']) else None,
                    'data_source': row['data_source']
                })
            conn.commit()
        
        logger.info(f"Loaded {len(fact_df)} records into fact_country_risk")


if __name__ == "__main__":
    # Example usage
    sample_data = pd.DataFrame({
        'country_code': ['US', 'FR', 'DE'],
        'year': [2023, 2023, 2023],
        'gdp_growth': [2.1, 1.5, 1.8],
        'inflation': [3.2, 2.5, 2.8],
        'debt_gdp': [120.5, 110.2, 65.8],
        'risk_score': [45.2, 38.5, 32.1],
        'risk_category': ['Moderate', 'Moderate', 'Low']
    })
    
    loader = FactLoader()
    loader.load_fact_country_risk(sample_data)
    print("Fact data loaded successfully!")
