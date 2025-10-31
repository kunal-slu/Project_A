import os
import pandas as pd
from simple_salesforce import Salesforce
import logging

logger = logging.getLogger(__name__)

def fetch_salesforce_products(sf: Salesforce) -> pd.DataFrame:
    """
    Fetches Salesforce Product data.
    """
    logger.info("Fetching Salesforce Products...")
    query = """
        SELECT Id,
               Name,
               ProductCode,
               Description,
               Family,
               IsActive,
               CreatedDate,
               LastModifiedDate
        FROM Product2
    """
    records = sf.query_all(query)['records']
    for r in records:
        r.pop('attributes', None)
    df = pd.DataFrame(records)
    logger.info(f"Fetched {len(df)} Salesforce Products.")
    return df

def extract_salesforce_products(spark_session, config: dict, table_name: str) -> pd.DataFrame:
    """
    Extracts Salesforce products data. For local dev, uses sample data.
    For AWS, connects to Salesforce API.
    """
    logger.info(f"Extracting Salesforce products for table: {table_name}")
    if config.get('environment') == 'local':
        sample_path = "aws/data/salesforce/salesforce_products.csv"
        df = pd.read_csv(sample_path)
    else:
        # In AWS, use actual Salesforce API connection
        sf = Salesforce(
            username=os.environ["SF_USERNAME"],
            password=os.environ["SF_PASSWORD"],
            security_token=os.environ["SF_SECURITY_TOKEN"],
            instance_url=os.environ.get("SF_INSTANCE_URL")
        )
        df = fetch_salesforce_products(sf)
    return df
