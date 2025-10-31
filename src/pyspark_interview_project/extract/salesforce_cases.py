import os
import pandas as pd
from simple_salesforce import Salesforce
import logging

logger = logging.getLogger(__name__)

def fetch_salesforce_cases(sf: Salesforce) -> pd.DataFrame:
    """
    Fetches Salesforce Case data.
    """
    logger.info("Fetching Salesforce Cases...")
    query = """
        SELECT Id,
               AccountId,
               ContactId,
               Subject,
               Description,
               Priority,
               Status,
               CreatedDate,
               LastModifiedDate,
               OwnerId
        FROM Case
    """
    records = sf.query_all(query)['records']
    for r in records:
        r.pop('attributes', None)
    df = pd.DataFrame(records)
    logger.info(f"Fetched {len(df)} Salesforce Cases.")
    return df

def extract_salesforce_cases(spark_session, config: dict, table_name: str) -> pd.DataFrame:
    """
    Extracts Salesforce cases data. For local dev, uses sample data.
    For AWS, connects to Salesforce API.
    """
    logger.info(f"Extracting Salesforce cases for table: {table_name}")
    if config.get('environment') == 'local':
        sample_path = "aws/data/salesforce/salesforce_cases.csv"
        df = pd.read_csv(sample_path)
    else:
        # In AWS, use actual Salesforce API connection
        sf = Salesforce(
            username=os.environ["SF_USERNAME"],
            password=os.environ["SF_PASSWORD"],
            security_token=os.environ["SF_SECURITY_TOKEN"],
            instance_url=os.environ.get("SF_INSTANCE_URL")
        )
        df = fetch_salesforce_cases(sf)
    return df
