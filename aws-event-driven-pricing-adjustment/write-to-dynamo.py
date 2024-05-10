import boto3
import pandas as pd

# Load CSV data
data = pd.read_csv('data/user_purchase_history.csv')

# Connect to DynamoDB using boto3
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('recent_purchase_history')

# Helper function to convert data to a suitable format and insert into DynamoDB
def write_to_dynamodb(row):
    try:
        table.put_item(
           Item={
                'user_id': str(row['user_id']),
                'total_orders': int(row['total_orders']),
                'total_items_purchased': int(row['total_items_purchased'])
            }
        )
        print(f"Inserted user_id {row['user_id']}")
    except Exception as e:
        print(f"Error inserting user_id {row['user_id']}: {str(e)}")

# Apply the function to each row in the dataframe
data.apply(write_to_dynamodb, axis=1)