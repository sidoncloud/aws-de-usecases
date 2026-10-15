"""Delete every resource this demo created. Nothing else.

Each target is matched by exact name. If a name does not match, it is skipped
and reported, never guessed at. Run with --commit to actually delete.
"""
import sys

import boto3

REGION = "us-east-1"
ACCOUNT = boto3.client("sts").get_caller_identity()["Account"]

BUCKET = f"agent-etl-demo-{ACCOUNT}"
DATABASE = "agent_etl_demo"
WORKGROUP = "agent-etl-demo-wg"
JOB_NAME = "agent-etl-demo-job"
ROLE_NAME = "AgentEtlDemoGlueRole"
POLICY_NAME = "AgentEtlDemoScopedAccess"

# Anything whose name is not in this set is never touched.
ALLOWED = {BUCKET, DATABASE, WORKGROUP, JOB_NAME, ROLE_NAME}

COMMIT = "--commit" in sys.argv

s3 = boto3.resource("s3", region_name=REGION)
s3c = boto3.client("s3", region_name=REGION)
glue = boto3.client("glue", region_name=REGION)
athena = boto3.client("athena", region_name=REGION)
iam = boto3.client("iam")


def guard(name):
    if name not in ALLOWED:
        raise RuntimeError(f"refusing to touch unlisted resource: {name}")


def step(label, fn):
    try:
        fn()
        print(f"  {'deleted' if COMMIT else 'would delete'}: {label}")
    except Exception as e:
        print(f"  skipped: {label}  ({type(e).__name__}: {str(e)[:90]})")


def main():
    print(f"Account {ACCOUNT}, region {REGION}")
    print("DRY RUN, pass --commit to delete\n" if not COMMIT else "COMMITTING\n")

    guard(JOB_NAME)
    step(f"glue job {JOB_NAME}",
         lambda: COMMIT and glue.delete_job(JobName=JOB_NAME))

    guard(DATABASE)
    def drop_tables():
        tables = glue.get_tables(DatabaseName=DATABASE).get("TableList", [])
        for t in tables:
            if COMMIT:
                glue.delete_table(DatabaseName=DATABASE, Name=t["Name"])
        print(f"    ({len(tables)} table(s) in {DATABASE})")
    step(f"tables in {DATABASE}", drop_tables)
    step(f"glue database {DATABASE}",
         lambda: COMMIT and glue.delete_database(Name=DATABASE))

    guard(WORKGROUP)
    step(f"athena workgroup {WORKGROUP}",
         lambda: COMMIT and athena.delete_work_group(WorkGroup=WORKGROUP,
                                                     RecursiveDeleteOption=True))

    guard(BUCKET)
    def empty_and_delete():
        b = s3.Bucket(BUCKET)
        n = sum(1 for _ in b.objects.all())
        if COMMIT:
            b.object_versions.delete()
            b.objects.all().delete()
            s3c.delete_bucket(Bucket=BUCKET)
        print(f"    ({n} object(s) in {BUCKET})")
    step(f"s3 bucket {BUCKET}", empty_and_delete)

    guard(ROLE_NAME)
    def drop_role():
        if COMMIT:
            iam.delete_role_policy(RoleName=ROLE_NAME, PolicyName=POLICY_NAME)
            iam.delete_role(RoleName=ROLE_NAME)
    step(f"iam role {ROLE_NAME}", drop_role)

    print("\nNothing outside those five names was touched.")


if __name__ == "__main__":
    main()
