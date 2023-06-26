import json
import boto3
import decimal
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)
        
def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb', region_name='eu-west-1')
    table = dynamodb.Table('transactions')
    
    if 'body' in event:
        event = json.loads(event['body'])
        
    transaction_id = event['transaction_id']
    
    try:
        response = table.get_item(
            Key={
                'transaction_id': str(transaction_id)
            }
        )
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error querying DynamoDB: {str(e)}')
        }

    if 'Item' not in response:
        return {
            'statusCode': 404,
            'body': json.dumps(f'Transaction with transaction_id {transaction_id} not found')
        }

    item = response['Item']

    return {
        'statusCode': 200,
        'body': json.dumps(item, cls=DecimalEncoder)
    }
