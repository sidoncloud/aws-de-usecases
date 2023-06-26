import json
import pymysql
import boto3
from datetime import datetime

db_endpoint = ""
db_username = ""
db_password = ""
db_name = ""

# S3 configuration
s3_bucket = "aws-de-use-cases"
s3_key = 'posts.json'

def default_serializer(obj):
    if isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%d %H:%M:%S')

def lambda_handler(event, context):
    try:
        # Connect to the RDS database
        connection = pymysql.connect(
            host=db_endpoint,
            user=db_username,
            password=db_password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor
        )

        # Execute SQL query to fetch data
        with connection.cursor() as cursor:
            sql = "SELECT * FROM stackoverflow_posts"
            cursor.execute(sql)
            result = cursor.fetchall()

        serialized_result = json.dumps(result, default=default_serializer)

        # Write data to S3
        s3 = boto3.client('s3')
        s3.put_object(Body=serialized_result, Bucket=s3_bucket, Key=s3_key)

        return {
            'statusCode': 200,
            'body': 'Data extracted and written to S3 successfully'
        }
    except Exception as e:
        raise Exception(f'Error: {str(e)}')
    finally:
        connection.close()