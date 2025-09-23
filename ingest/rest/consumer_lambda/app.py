"""
REST API Consumer Lambda.
Reads messages from SQS and writes JSON data to S3 bronze layer.
"""

import json
import logging
import os
import boto3
from datetime import datetime
from typing import Dict, Any, List

logger = logging.getLogger(__name__)
sqs = boto3.client('sqs')
s3 = boto3.client('s3')


def process_sqs_messages(queue_url: str) -> List[Dict[str, Any]]:
    """Process messages from SQS queue."""
    messages = []
    
    try:
        # Receive messages from SQS (max 10 at a time)
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,  # Long polling
            MessageAttributeNames=['All']
        )
        
        received_messages = response.get('Messages', [])
        logger.info(f"Received {len(received_messages)} messages from SQS")
        
        for message in received_messages:
            try:
                # Parse message body
                body = json.loads(message['Body'])
                messages.append(body)
                
                # Delete message from queue after successful processing
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse message body: {e}")
                # Move to DLQ or handle error appropriately
                continue
            except Exception as e:
                logger.error(f"Failed to process message: {e}")
                continue
        
        return messages
        
    except Exception as e:
        logger.error(f"Failed to process SQS messages: {e}")
        raise


def write_to_s3_bronze(messages: List[Dict[str, Any]], bucket: str, prefix: str) -> str:
    """Write messages to S3 bronze layer."""
    if not messages:
        logger.warning("No messages to write to S3")
        return None
    
    # Create S3 key with partitioning by date
    current_date = datetime.utcnow().strftime('%Y-%m-%d')
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    s3_key = f"{prefix}/rest_api/users/ingestion_date={current_date}/data_{timestamp}.json"
    
    try:
        # Convert messages to JSON lines format
        json_lines = '\n'.join(json.dumps(msg) for msg in messages)
        
        # Upload to S3
        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json_lines.encode('utf-8'),
            ContentType='application/json'
        )
        
        logger.info(f"Successfully wrote {len(messages)} records to s3://{bucket}/{s3_key}")
        return s3_key
        
    except Exception as e:
        logger.error(f"Failed to write to S3: {e}")
        raise


def lambda_handler(event, context):
    """Lambda handler for REST API consumer."""
    logger.info(f"Starting REST API consumer. Event: {event}")
    
    try:
        # Get configuration from environment
        queue_url = os.environ.get('SQS_QUEUE_URL')
        bronze_bucket = os.environ.get('BRONZE_BUCKET')
        bronze_prefix = os.environ.get('BRONZE_PREFIX', 'bronze')
        
        if not queue_url or not bronze_bucket:
            raise ValueError("SQS_QUEUE_URL and BRONZE_BUCKET environment variables must be set")
        
        # Process messages from SQS
        messages = process_sqs_messages(queue_url)
        
        if not messages:
            logger.info("No messages to process")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No messages to process',
                    'records_processed': 0
                })
            }
        
        # Write to S3 bronze layer
        s3_key = write_to_s3_bronze(messages, bronze_bucket, bronze_prefix)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Messages successfully processed',
                'records_processed': len(messages),
                's3_location': f"s3://{bronze_bucket}/{s3_key}"
            })
        }
        
    except Exception as e:
        logger.error(f"REST API consumer failed: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'REST API consumer failed'
            })
        }
