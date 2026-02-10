"""
Load dimension tables into the data warehouse.
"""
import logging
import os
import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import create_engine,  text
from typing import Optional, Dict, List
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import MetaData



logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  

class DimensionLoader:
    """
    Class for loading dimension tables.
    """
 

    def __init__(self, connection_string: Optional[str] = None):

        if connection_string is None:
            db_host = os.getenv('DB_HOST', 'localhost')
            db_port = os.getenv('DB_PORT', '5432')
            db_name = os.getenv('DB_NAME', 'country_risk_dw')
            db_user = os.getenv('DB_USER', 'parfait')
            db_password = os.getenv('DB_PASSWORD', 'parfait')

            connection_string = (
                f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            )

        self.engine = create_engine(connection_string)
        self.connection_string = connection_string
    

        # ✅ LIGNE MANQUANTE
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)
    
    def load_countries(self, countries_df: pd.DataFrame) -> None:
        required_cols = ['country_code', 'country_name']
        if not all(col in countries_df.columns for col in required_cols):
            raise ValueError(f"DataFrame must contain columns: {required_cols}")

        countries_df = countries_df.copy()
        countries_df['created_at'] = pd.Timestamp.now()
        countries_df['updated_at'] = pd.Timestamp.now()

        dim_cols = [
            'country_code', 'country_name', 'iso2_code', 'iso3_code',
            'region_code', 'income_level', 'currency_code',
            'created_at', 'updated_at'
        ]

        for col in dim_cols:
            if col not in countries_df.columns:
                countries_df[col] = None

        countries_df = countries_df[dim_cols]

        # ✅ TRANSACTION PROPRE
        with self.engine.begin() as conn:
            for _, row in countries_df.iterrows():
                conn.execute(
                    text("""
                        INSERT INTO dim_country (
                            country_code, country_name, iso2_code, iso3_code,
                            region_code, income_level, currency_code,
                            created_at, updated_at
                        )
                        VALUES (
                            :country_code, :country_name, :iso2_code, :iso3_code,
                            :region_code, :income_level, :currency_code,
                            :created_at, :updated_at
                        )
                        ON CONFLICT (country_code) DO UPDATE SET
                            country_name = EXCLUDED.country_name,
                            iso2_code = EXCLUDED.iso2_code,
                            iso3_code = EXCLUDED.iso3_code,
                            region_code = EXCLUDED.region_code,
                            income_level = EXCLUDED.income_level,
                            currency_code = EXCLUDED.currency_code,
                            updated_at = EXCLUDED.updated_at
                    """),
                    row.to_dict()
                )

        logger.info(f"Loaded {len(countries_df)} countries into dim_country")



        logger = logging.getLogger(__name__)

    def load_regions(self, regions_df: pd.DataFrame) -> None:
            """
            Load regions into dim_region (idempotent).
            """

            required_cols = ['region_code', 'region_name']
            if not all(col in regions_df.columns for col in required_cols):
                raise ValueError(f"DataFrame must contain columns: {required_cols}")

            df = regions_df.copy()
            df['created_at'] = pd.Timestamp.now()

            if 'subregion_name' not in df.columns:
                df['subregion_name'] = None

            records = df[
                ['region_code', 'region_name', 'subregion_name', 'created_at']
            ].to_dict(orient='records')

            dim_region = self.metadata.tables['dim_region']

            stmt = insert(dim_region).values(records)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=['region_code']
            )

            with self.engine.begin() as conn:
                result = conn.execute(stmt)

            logger.info(
                f"Loaded regions into dim_region (ignored duplicates safely)"
            )

        
    def ensure_date_dimension(self, start_date: str, end_date: str) -> None:
            """
            Ensure date dimension is populated for date range.
            
            Args:
                start_date: Start date (YYYY-MM-DD)
                end_date: End date (YYYY-MM-DD)
            """
            start = pd.to_datetime(start_date)
            end = pd.to_datetime(end_date)
            
            date_range = pd.date_range(start=start, end=end, freq='D')
            
            dates_df = pd.DataFrame({
                'date': date_range,
                'year': date_range.year,
                'quarter': date_range.quarter,
                'month': date_range.month,
                'month_name': date_range.strftime('%B'),
                'day_of_month': date_range.day,
                'day_of_week': date_range.dayofweek,
                'day_name': date_range.strftime('%A'),
                'is_weekend': date_range.dayofweek.isin([5, 6]),
                'is_month_end': date_range.is_month_end,
                'is_quarter_end': date_range.is_quarter_end,
                'is_year_end': date_range.is_year_end
            })
            
            # Generate date_key (YYYYMMDD)
            dates_df['date_key'] = (
                dates_df['year'] * 10000 +
                dates_df['month'] * 100 +
                dates_df['day_of_month']
            ).astype(int)
            
            # Reorder columns
            dates_df = dates_df[[
                'date_key', 'date', 'year', 'quarter', 'month', 'month_name',
                'day_of_month', 'day_of_week', 'day_name',
                'is_weekend', 'is_month_end', 'is_quarter_end', 'is_year_end'
            ]]
            
            # Load to database (ignore duplicates)
            dates_df.to_sql(
                'dim_date',
                self.engine,
                if_exists='append',
                index=False
            )
            
            logger.info(f"Ensured date dimension for {len(dates_df)} dates")


def create_sample_dimensions(loader: DimensionLoader) -> None:
        """
        Create sample dimension data.
        
        Args:
            loader: DimensionLoader instance
        """
        # Sample regions
        regions_df = pd.DataFrame([
            {'region_code': 'NA', 'region_name': 'North America', 'subregion_name': None},
            {'region_code': 'SA', 'region_name': 'South America', 'subregion_name': None},
            {'region_code': 'EU', 'region_name': 'Europe', 'subregion_name': None},
            {'region_code': 'AS', 'region_name': 'Asia', 'subregion_name': None},
            {'region_code': 'AF', 'region_name': 'Africa', 'subregion_name': None},
            {'region_code': 'OC', 'region_name': 'Oceania', 'subregion_name': None},
            {'region_code': 'ME', 'region_name': 'Middle East', 'subregion_name': None}
        ])
        
        loader.load_regions(regions_df)
        
        # All African countries (54 countries)
        from ingestion.african_countries import AFRICAN_COUNTRIES, AFRICAN_CURRENCIES
        
        # Country names mapping
        country_names = {
            'DZ': 'Algeria', 'AO': 'Angola', 'BJ': 'Benin', 'BW': 'Botswana', 'BF': 'Burkina Faso',
            'BI': 'Burundi', 'CV': 'Cabo Verde', 'CM': 'Cameroon', 'CF': 'Central African Republic',
            'TD': 'Chad', 'KM': 'Comoros', 'CG': 'Congo', 'CD': 'Congo, Democratic Republic of the',
            'CI': 'Côte d\'Ivoire', 'DJ': 'Djibouti', 'EG': 'Egypt', 'GQ': 'Equatorial Guinea',
            'ER': 'Eritrea', 'SZ': 'Eswatini', 'ET': 'Ethiopia', 'GA': 'Gabon', 'GM': 'Gambia',
            'GH': 'Ghana', 'GN': 'Guinea', 'GW': 'Guinea-Bissau', 'KE': 'Kenya', 'LS': 'Lesotho',
            'LR': 'Liberia', 'LY': 'Libya', 'MG': 'Madagascar', 'MW': 'Malawi', 'ML': 'Mali',
            'MR': 'Mauritania', 'MU': 'Mauritius', 'MA': 'Morocco', 'MZ': 'Mozambique', 'NA': 'Namibia',
            'NE': 'Niger', 'NG': 'Nigeria', 'RW': 'Rwanda', 'ST': 'São Tomé and Príncipe',
            'SN': 'Senegal', 'SC': 'Seychelles', 'SL': 'Sierra Leone', 'SO': 'Somalia',
            'ZA': 'South Africa', 'SS': 'South Sudan', 'SD': 'Sudan', 'TZ': 'Tanzania', 'TG': 'Togo',
            'TN': 'Tunisia', 'UG': 'Uganda', 'ZM': 'Zambia', 'ZW': 'Zimbabwe'
        }
        
        # ISO3 codes mapping
        iso3_codes = {
            'DZ': 'DZA', 'AO': 'AGO', 'BJ': 'BEN', 'BW': 'BWA', 'BF': 'BFA', 'BI': 'BDI',
            'CV': 'CPV', 'CM': 'CMR', 'CF': 'CAF', 'TD': 'TCD', 'KM': 'COM', 'CG': 'COG',
            'CD': 'COD', 'CI': 'CIV', 'DJ': 'DJI', 'EG': 'EGY', 'GQ': 'GNQ', 'ER': 'ERI',
            'SZ': 'SWZ', 'ET': 'ETH', 'GA': 'GAB', 'GM': 'GMB', 'GH': 'GHA', 'GN': 'GIN',
            'GW': 'GNB', 'KE': 'KEN', 'LS': 'LSO', 'LR': 'LBR', 'LY': 'LBY', 'MG': 'MDG',
            'MW': 'MWI', 'ML': 'MLI', 'MR': 'MRT', 'MU': 'MUS', 'MA': 'MAR', 'MZ': 'MOZ',
            'NA': 'NAM', 'NE': 'NER', 'NG': 'NGA', 'RW': 'RWA', 'ST': 'STP', 'SN': 'SEN',
            'SC': 'SYC', 'SL': 'SLE', 'SO': 'SOM', 'ZA': 'ZAF', 'SS': 'SSD', 'SD': 'SDN',
            'TZ': 'TZA', 'TG': 'TGO', 'TN': 'TUN', 'UG': 'UGA', 'ZM': 'ZMB', 'ZW': 'ZWE'
        }
        
        countries_data = []
        for country_code in AFRICAN_COUNTRIES:
            countries_data.append({
                'country_code': country_code,
                'country_name': country_names.get(country_code, country_code),
                'iso2_code': country_code,
                'iso3_code': iso3_codes.get(country_code, country_code),
                'region_code': 'AF',
                'currency_code': AFRICAN_CURRENCIES.get(country_code, 'USD')
            })
        
        countries_df = pd.DataFrame(countries_data)
        
        loader.load_countries(countries_df)
        
        # Ensure date dimension for last 15 years
        from datetime import datetime, timedelta
        end_date = datetime.now()
        start_date = end_date - timedelta(days=15*365)
        
        loader.ensure_date_dimension(
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )


if __name__ == "__main__":
    loader = DimensionLoader()
    create_sample_dimensions(loader)
    print("Dimensions loaded successfully!")
