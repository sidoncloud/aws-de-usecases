import json
import pymysql
import boto3
from datetime import datetime

# Database configuration
db_endpoint = "endpoint-uri"
db_username = "admin"
db_password = ""
db_name = "ecom_db"

# S3 configuration
s3_bucket = "{bucket-name}"
s3_key = f"raw_landing_zone/orders/data.json"

# DynamoDB configuration
dynamo_table_name = "etl_logs"
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(dynamo_table_name)

def default_serializer(obj):
    if isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%d %H:%M:%S')

def insert_dynamo_log(last_value, last_run_ts):
    # Insert a new row in DynamoDB after successful extraction
    table.put_item(Item={
        'table_name': 'orders',
        'filter_column': 'last_updated_ts',
        'last_extracted_value': last_value,
        'last_run_ts': last_run_ts
    })

def main():
    try:
        # Connect to the RDS database
        connection = pymysql.connect(
            host=db_endpoint,
            user=db_username,
            password=db_password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor
        )

        # Fetch settings from DynamoDB to get the latest filter value and filter column
        response = table.get_item(Key={'table_name': 'orders'})
        filter_value = response['Item']['last_extracted_value'] if 'Item' in response else '2024-04-18 00:00:00.000000'
        filter_column = response['Item']['filter_column'] if 'Item' in response else 'last_updated_ts'

        # Execute SQL query to fetch data
        with connection.cursor() as cursor:
            sql = f"SELECT * FROM orders WHERE {filter_column} >= '{filter_value}'"
            cursor.execute(sql)
            result = cursor.fetchall()

        # Process result if not empty
        if result:
            serialized_result = json.dumps(result, default=default_serializer)
            max_created_date = max([row[filter_column] for row in result])
            current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Write data to S3
            s3 = boto3.client('s3')
            s3.put_object(Body=serialized_result, Bucket=s3_bucket, Key=s3_key)

            # Insert new log entry into DynamoDB
            insert_dynamo_log(max_created_date.strftime('%Y-%m-%d %H:%M:%S'), current_timestamp)

            print('Data extracted and written to S3 successfully')

    except Exception as e:
        print(f'Error: {str(e)}')
    finally:
        if 'connection' in locals() and connection:
            connection.close()

if __name__ == '__main__':
    main()