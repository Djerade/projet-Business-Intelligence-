"""
World Bank API data ingestion module.
Fetches macroeconomic indicators for countries.
"""
import sys
import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE_DIR))

import os
import logging
import pandas as pd
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from ingestion.utils import (
    make_api_request,
    get_data_path,
    get_env_var,
    format_date_for_api
)

logger = logging.getLogger(__name__)

# World Bank API base URL
WORLD_BANK_BASE_URL = "https://api.worldbank.org/v2"

# Indicator codes for World Bank API
INDICATORS = {
    'gdp': 'NY.GDP.MKTP.CD',  # PIB
    'gdp_growth': 'NY.GDP.MKTP.KD.ZG',  #Croissance du PIB
    'inflation': 'FP.CPI.TOTL.ZG',  # Inflation
    'debt_gdp': 'GC.DOD.TOTL.GD.ZS',  # Dette publique (% du PIB)
    'trade_balance': 'NE.TRD.GNFS.ZS',  # Commerce extérieur (% du PIB)
    'current_account': 'BN.CAB.XOKA.GD.ZS'  # Compte courant (% du PIB)
}


class WorldBankIngestion:
    """
    Class for ingesting data from World Bank API.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize World Bank ingestion client.
        
        Args:
            api_key: Optional API key (World Bank API is mostly public)
        """
        self.api_key = api_key or get_env_var('WORLD_BANK_API_KEY', '')
        self.base_url = WORLD_BANK_BASE_URL
        
    def fetch_indicator(
        self,
        country_code: str,
        indicator_code: str,
        start_date: Optional[int] = None,
        end_date: Optional[int] = None,
        format: str = 'json'
    ) -> List[Dict]:
        """
        Fetch indicator data for a country.
        
        Args:
            country_code: ISO country code (e.g., 'US', 'FR')
            indicator_code: World Bank indicator code
            start_date: Start year (optional)
            end_date: End year (optional)
            format: Response format (json or xml)
            
        Returns:
            List of data records
        """
        url = f"{self.base_url}/country/{country_code}/indicator/{indicator_code}"
        
        params = {
            'format': format,
            'per_page': 1000,  # Max per page
            'date': None
        }
        
        if start_date and end_date:
            params['date'] = f"{start_date}:{end_date}"
        elif start_date:
            params['date'] = f"{start_date}:{datetime.now().year}"
        elif end_date:
            params['date'] = f"1960:{end_date}"
        else:
            # Default: last 10 years
            current_year = datetime.now().year
            params['date'] = f"{current_year - 10}:{current_year}"
        
        if self.api_key:
            params['api_key'] = self.api_key
        
        try:
            response = make_api_request(url, params=params)
            data = response.json()
            
            if len(data) >= 2 and isinstance(data[1], list):
                return data[1]
            else:
                logger.warning(f"No data returned for {country_code} - {indicator_code}")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching indicator {indicator_code} for {country_code}: {str(e)}")
            return []
    
    def fetch_all_indicators(
        self,
        country_code: str,
        start_date: Optional[int] = None,
        end_date: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Fetch all macroeconomic indicators for a country.
        
        Args:
            country_code: ISO country code
            start_date: Start year
            end_date: End year
            
        Returns:
            DataFrame with all indicators
        """
        all_data = []
        
        for indicator_name, indicator_code in INDICATORS.items():
            logger.info(f"Fetching {indicator_name} for {country_code}")
            records = self.fetch_indicator(country_code, indicator_code, start_date, end_date)
            print("records", records)
            for record in records:
                if record.get('value') is not None:
                    all_data.append({
                        'country_code': country_code,
                        'indicator': indicator_name,
                        'indicator_code': indicator_code,
                        'year': record.get('date'),
                        'value': record.get('value'),
                        'country_name': record.get('country', {}).get('value', ''),
                        'fetched_at': datetime.now().isoformat()
                    })
            
            # Rate limiting: be respectful to the API
            import time
            time.sleep(0.5)
        
        if not all_data:
            logger.warning(f"No data fetched for country {country_code}")
            return pd.DataFrame()
        
        df = pd.DataFrame(all_data)
        return df
    
    def fetch_multiple_countries(
        self,
        country_codes: List[str],
        start_date: Optional[int] = None,
        end_date: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Fetch indicators for multiple countries.
        
        Args:
            country_codes: List of ISO country codes
            start_date: Start year
            end_date: End year
            
        Returns:
            Combined DataFrame for all countries
        """
        all_dfs = []
        
        for country_code in country_codes:
            logger.info(f"Processing country: {country_code}")
            df = self.fetch_all_indicators(country_code, start_date, end_date)
            print("df", df)
            if not df.empty:
                all_dfs.append(df)
        
        if not all_dfs:
            return pd.DataFrame()
        
        return pd.concat(all_dfs, ignore_index=True)



    def save_to_data_lake(
        self,
        df: pd.DataFrame,
        country_code: Optional[str] = None,
        timestamp: Optional[datetime] = None
    ) -> str:
        """
        Save fetched data to data lake (raw zone).
        
        Args:
            df: DataFrame to save
            country_code: Optional country code for filename
            timestamp: Optional timestamp for filename
            
        Returns:
            Path to saved file
        """
        if df.empty:
            raise ValueError("Cannot save empty DataFrame")
        
        timestamp = timestamp or datetime.now()
        timestamp_str = timestamp.strftime('%Y%m%d_%H%M%S')
        
        # Save as CSV in raw data lake
        if country_code:
            filename = f"world_bank_{country_code}_{timestamp_str}.csv"
        else:
            filename = f"world_bank_all_{timestamp_str}.csv"
        
        filepath = get_data_path('raw', filename)
        df.to_csv(filepath, index=False)
        logger.info(f"World Bank data saved to CSV at: {filepath}")
        
        return filepath
    


def main():
    """
    Example usage of World Bank ingestion.
    """
    # Example: Fetch data for African countries
    from ingestion.african_countries import AFRICAN_COUNTRIES
    countries = AFRICAN_COUNTRIES[:10]  # First 10 for example
    
    wb = WorldBankIngestion()
    
    # Fetch data for last 10 years
    current_year = datetime.now().year
    df = wb.fetch_multiple_countries(countries, start_date=current_year - 10)
    
    if not df.empty:
        filepath = wb.save_to_data_lake(df)
        print(f"Data saved to: {filepath}")
        print(f"\nData shape: {df.shape}")
        print(f"\nSample data:\n{df.head()}")
    else:
        print("No data fetched")


if __name__ == "__main__":
    main()
