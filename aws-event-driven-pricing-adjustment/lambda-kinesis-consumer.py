import json
import base64
import logging
import re
import boto3
from decimal import Context, setcontext, ROUND_HALF_EVEN

logger = logging.getLogger()
logger.setLevel(logging.INFO)

setcontext(Context(rounding=ROUND_HALF_EVEN))
region_name = 'us-east-1'

def lambda_handler(event, context):
    kinesis_client = boto3.client('kinesis', region_name=region_name)
    dynamodb = boto3.resource('dynamodb', region_name=region_name)
    purchase_history_table = dynamodb.Table('recent_purchase_history')
    adjusted_pricing_stream_name = 'user_product_adjusted_pricing'

    record_count = 0
    error_count = 0

    for record in event['Records']:
        try:
            payload = base64.b64decode(record['kinesis']['data'])
            data_item = json.loads(payload)

            user_id = data_item.get('user_id')
            event_type = data_item.get('event_type')

            if user_id and event_type == 'product':
                # Extract product_id from URI if event type is 'product'
                match = re.search(r'/product/(\d+)', data_item.get('uri', ''))
                if match:
                    product_id = match.group(1)
                    # Lookup recent purchase history for the user
                    response = purchase_history_table.get_item(Key={'user_id': str(user_id)})
                    if 'Item' in response:
                        total_orders = int(response['Item']['total_orders'])
                        total_items_purchased = int(response['Item']['total_items_purchased'])

                        if total_orders > 2 and total_items_purchased > 2:
                            # Prepare new payload with a discount
                            adjusted_pricing_data = {
                                'user_id': user_id,
                                'product_id': product_id,
                                'discount': '20%'
                            }

                            # Send to another Kinesis stream
                            kinesis_client.put_record(
                                StreamName=adjusted_pricing_stream_name,
                                Data=json.dumps(adjusted_pricing_data),
                                PartitionKey=str(user_id)  # Using user_id as the partition key
                            )
            record_count += 1
        except Exception as e:
            logger.error(f"Error processing record: {e}")
            error_count += 1

    logger.info(f'Processed {record_count} records with {error_count} errors.')
    return {
        'statusCode': 200,
        'body': json.dumps(f'Processed {record_count} records with {error_count} errors.')
    }

