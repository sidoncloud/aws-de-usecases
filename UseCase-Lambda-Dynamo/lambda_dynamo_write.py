import json
from decimal import Decimal
import boto3

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb', region_name='eu-central-1')
    table = dynamodb.Table('transactions')

    # Check if the event comes from API Gateway
    if 'body' in event:
        event = json.loads(event['body'])
    try:
        transaction_id = event['transaction_id']
        user_id = event['user_id']
        creation_date = event['creation_date']
        product_id = event['product_id']
        num_items = event['num_items']
        transaction_status = event['transaction_status']
    except KeyError as e:
        return {
            'statusCode': 400,
            'body': json.dumps(f'Missing required input: {str(e)}')
        }

    try:
        response = table.put_item(
           Item={
                'transaction_id': transaction_id,
                'user_id': user_id,
                'creation_date': creation_date,
                'num_items' : num_items,
                'product_id': product_id,
                'transaction_status': transaction_status
            }
        )
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error putting item to DynamoDB: {str(e)}')
        }

    return {
        'statusCode': 200,
        'body': json.dumps('Transaction added to the database!')
    }
