"""
REST API Producer Lambda.
Fetches data from external REST API and sends messages to SQS for processing.
"""

import json
import logging
import os
import boto3
import requests
from datetime import datetime
from typing import Dict, Any, List

logger = logging.getLogger(__name__)
sqs = boto3.client('sqs')
secrets_manager = boto3.client('secretsmanager')


def get_api_credentials() -> Dict[str, str]:
    """Get API credentials from Secrets Manager."""
    secret_name = os.environ.get('API_CREDENTIALS_SECRET')
    if not secret_name:
        raise ValueError("API_CREDENTIALS_SECRET environment variable not set")
    
    try:
        response = secrets_manager.get_secret_value(SecretId=secret_name)
        credentials = json.loads(response['SecretString'])
        return credentials
    except Exception as e:
        logger.error(f"Failed to retrieve API credentials: {e}")
        raise


def fetch_data_from_api(api_url: str, credentials: Dict[str, str]) -> List[Dict[str, Any]]:
    """Fetch data from external REST API."""
    headers = {
        'Authorization': f"Bearer {credentials.get('api_token')}",
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.get(api_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"Fetched {len(data)} records from API")
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        raise


def send_to_sqs(queue_url: str, messages: List[Dict[str, Any]]) -> None:
    """Send messages to SQS queue."""
    try:
        # SQS has a limit of 10 messages per batch
        for i in range(0, len(messages), 10):
            batch = messages[i:i+10]
            
            entries = []
            for j, message in enumerate(batch):
                entries.append({
                    'Id': str(i + j),
                    'MessageBody': json.dumps(message),
                    'MessageAttributes': {
                        'Source': {
                            'StringValue': 'rest-api-producer',
                            'DataType': 'String'
                        },
                        'Timestamp': {
                            'StringValue': datetime.utcnow().isoformat(),
                            'DataType': 'String'
                        }
                    }
                })
            
            response = sqs.send_message_batch(
                QueueUrl=queue_url,
                Entries=entries
            )
            
            if response.get('Failed'):
                logger.error(f"Failed to send {len(response['Failed'])} messages")
                for failure in response['Failed']:
                    logger.error(f"Message {failure['Id']} failed: {failure['Message']}")
            else:
                logger.info(f"Successfully sent {len(entries)} messages to SQS")
                
    except Exception as e:
        logger.error(f"Failed to send messages to SQS: {e}")
        raise


def lambda_handler(event, context):
    """Lambda handler for REST API producer."""
    logger.info(f"Starting REST API producer. Event: {event}")
    
    try:
        # Get configuration from environment
        api_url = os.environ.get('API_URL')
        queue_url = os.environ.get('SQS_QUEUE_URL')
        
        if not api_url or not queue_url:
            raise ValueError("API_URL and SQS_QUEUE_URL environment variables must be set")
        
        # Get API credentials
        credentials = get_api_credentials()
        
        # Fetch data from API
        data = fetch_data_from_api(api_url, credentials)
        
        if not data:
            logger.warning("No data received from API")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No data to process',
                    'records_processed': 0
                })
            }
        
        # Send to SQS
        send_to_sqs(queue_url, data)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Data successfully sent to SQS',
                'records_processed': len(data)
            })
        }
        
    except Exception as e:
        logger.error(f"REST API producer failed: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'REST API producer failed'
            })
        }
