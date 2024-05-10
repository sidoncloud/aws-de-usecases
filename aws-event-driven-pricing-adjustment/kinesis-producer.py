import json
import time
import random
import csv
import boto3
import logging

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_file(file_path, region_name='us-east-1'):
    """Process local CSV file and send data to Kinesis."""
    kinesis_client = boto3.client('kinesis', region_name=region_name)

    try:
        # Open the local CSV file
        with open(file_path, 'r') as file_content:
            csv_reader = csv.DictReader(file_content)

            counter = 0
            for row in csv_reader:
                try:
                    # Convert row to JSON string
                    data_json = json.dumps(row)
                    # Send record to Kinesis Data Stream
                    response = kinesis_client.put_record(
                        StreamName="user_click_streams",
                        Data=data_json,
                        PartitionKey=str(row['session_id'])  # Using session_id as the partition key
                    )
                    counter += 1

                    print(f"Record {counter}: {row['session_id']} sent successfully.")
                    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                        logger.error('Error sending message to Kinesis:', response)
                except Exception as e:
                    logger.error(f"Error processing record {row}: {e}")
                time.sleep(random.randint(0,1))  # Random sleep between 10 to 20 seconds

            logger.info(f"Finished processing. Total records sent: {counter}")

    except Exception as e:
        logger.error(f"Error opening file {file_path}: {e}")
        raise e

def main():
    file_path = 'data/events.csv'  # Ensure this path correctly points to your CSV file
    process_file(file_path)

if __name__ == "__main__":
    main()