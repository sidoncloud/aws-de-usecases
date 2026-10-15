"""Create the fenced demo infrastructure. Creates ONLY these five resources.

Nothing existing in the account is read, modified or deleted. Every name is
prefixed so teardown can target them exactly.
"""
import json
import time

import boto3

REGION = "us-east-1"
ACCOUNT = boto3.client("sts").get_caller_identity()["Account"]

BUCKET = f"agent-etl-demo-{ACCOUNT}"
DATABASE = "agent_etl_demo"
WORKGROUP = "agent-etl-demo-wg"
ROLE_NAME = "AgentEtlDemoGlueRole"

s3 = boto3.client("s3", region_name=REGION)
iam = boto3.client("iam")
glue = boto3.client("glue", region_name=REGION)
athena = boto3.client("athena", region_name=REGION)

TRUST = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "glue.amazonaws.com"},
        "Action": "sts:AssumeRole",
    }],
}


def scoped_policy(bucket, database):
    """Least privilege: this bucket and this Glue database only."""
    return {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow",
             "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
             "Resource": [f"arn:aws:s3:::{bucket}/*"]},
            {"Effect": "Allow",
             "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
             "Resource": [f"arn:aws:s3:::{bucket}"]},
            {"Effect": "Allow",
             "Action": ["glue:GetDatabase", "glue:GetDatabases", "glue:CreateTable",
                        "glue:GetTable", "glue:GetTables", "glue:UpdateTable",
                        "glue:DeleteTable", "glue:GetPartition", "glue:GetPartitions",
                        "glue:BatchCreatePartition", "glue:CreatePartition",
                        "glue:UpdatePartition", "glue:BatchGetPartition"],
             "Resource": [
                 f"arn:aws:glue:{REGION}:{ACCOUNT}:catalog",
                 f"arn:aws:glue:{REGION}:{ACCOUNT}:database/{database}",
                 f"arn:aws:glue:{REGION}:{ACCOUNT}:table/{database}/*",
             ]},
            {"Effect": "Allow",
             "Action": ["logs:CreateLogGroup", "logs:CreateLogStream",
                        "logs:PutLogEvents", "logs:AssociateKmsKey"],
             "Resource": [f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:/aws-glue/*"]},
        ],
    }


def create_bucket():
    try:
        s3.head_bucket(Bucket=BUCKET)
        print(f"  bucket exists: {BUCKET}")
        return
    except s3.exceptions.ClientError:
        pass
    s3.create_bucket(Bucket=BUCKET)  # us-east-1 takes no LocationConstraint
    s3.put_public_access_block(
        Bucket=BUCKET,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True, "IgnorePublicAcls": True,
            "BlockPublicPolicy": True, "RestrictPublicBuckets": True})
    print(f"  created bucket: {BUCKET} (public access blocked)")


def create_role():
    try:
        existing = iam.get_role(RoleName=ROLE_NAME)["Role"]["Arn"]
        print(f"  role exists: {existing}")
        return existing
    except iam.exceptions.NoSuchEntityException:
        pass
    arn = iam.create_role(
        RoleName=ROLE_NAME,
        AssumeRolePolicyDocument=json.dumps(TRUST),
        Description="Glue job role for the agent ETL demo. Scoped to one bucket.",
    )["Role"]["Arn"]
    iam.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName="AgentEtlDemoScopedAccess",
        PolicyDocument=json.dumps(scoped_policy(BUCKET, DATABASE)),
    )
    print(f"  created role: {arn}")
    print("  waiting 15s for IAM propagation...")
    time.sleep(15)
    return arn


def create_database():
    try:
        glue.get_database(Name=DATABASE)
        print(f"  database exists: {DATABASE}")
        return
    except glue.exceptions.EntityNotFoundException:
        pass
    glue.create_database(DatabaseInput={
        "Name": DATABASE,
        "Description": "Agent ETL demo. Safe to delete.",
        "LocationUri": f"s3://{BUCKET}/warehouse/",
    })
    print(f"  created database: {DATABASE}")


def create_workgroup():
    """Its own workgroup so query results never land in an existing bucket."""
    try:
        athena.get_work_group(WorkGroup=WORKGROUP)
        print(f"  workgroup exists: {WORKGROUP}")
        return
    except athena.exceptions.InvalidRequestException:
        pass
    athena.create_work_group(
        Name=WORKGROUP,
        Description="Agent ETL demo. Safe to delete.",
        Configuration={
            "ResultConfiguration": {
                "OutputLocation": f"s3://{BUCKET}/athena-results/"},
            "EnforceWorkGroupConfiguration": True,
            "PublishCloudWatchMetricsEnabled": False,
        },
    )
    print(f"  created workgroup: {WORKGROUP}")


if __name__ == "__main__":
    print(f"Account {ACCOUNT}, region {REGION}\n")
    print("Creating fenced demo infrastructure:")
    create_bucket()
    role_arn = create_role()
    create_database()
    create_workgroup()
    print("\nDone. Resources created:")
    print(f"  s3://{BUCKET}/")
    print(f"  glue database   {DATABASE}")
    print(f"  athena workgroup {WORKGROUP}")
    print(f"  iam role        {role_arn}")
