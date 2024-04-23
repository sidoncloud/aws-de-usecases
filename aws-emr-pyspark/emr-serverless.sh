
aws emr-serverless start-job-run \
    --application-id 00fiopgg0s0guc09 \
    --execution-role-arn arn:aws:iam::127489365181:role/custom-emr-runtime-role \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://nl-labs-aws/spark-agg.py",
            "entryPointArguments": ["1"],
            "sparkSubmitParameters": "--class org.apache.spark.examples.SparkPi --conf spark.executor.cores=4 --conf spark.executor.memory=20g --conf spark.driver.cores=4 --conf spark.driver.memory=8g --conf spark.executor.instances=1"
        }
    }'

# # IAM ROLE Trust relationship 
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ServerlessTrustPolicy",
            "Effect": "Allow",
            "Principal": {
                "Service": "emr-serverless.amazonaws.com"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringLike": {
                    "aws:SourceArn": "arn:aws:emr-serverless:us-east-1:127489365181:/applications/{application-id}",
                    "aws:SourceAccount": "{account-number}"
                }
            }
        }
    ]
}