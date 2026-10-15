"""An agent that builds a Glue + Iceberg pipeline, then reports on it.

The agent writes the PySpark itself, creates the Glue job, runs it, reads its
own failures out of CloudWatch, fixes them, and re-runs until the job is green.
Then it registers the table and queries it with Athena.

Everything reaches AWS through the managed AWS MCP Server, so there is no
boto3 client code and no per-service MCP server here.
"""
import os
import sys

import boto3

from mcp import StdioServerParameters, stdio_client
from strands import Agent
from strands.models import BedrockModel
from strands.tools.mcp import MCPClient

AWS_MCP_URL = "https://aws-mcp.us-east-1.api.aws/mcp"
REGION = "us-east-1"
MODEL_ID = "us.anthropic.claude-haiku-4-5-20251001-v1:0"

ACCOUNT = boto3.client("sts").get_caller_identity()["Account"]
BUCKET = f"agent-etl-demo-{ACCOUNT}"
DATABASE = "agent_etl_demo"
TABLE = "orders_iceberg"
WORKGROUP = "agent-etl-demo-wg"
JOB_NAME = "agent-etl-demo-job"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/AgentEtlDemoGlueRole"

SYSTEM_PROMPT = f"""You are a data engineer building a production pipeline on AWS.

HARD BOUNDARY. You may only touch these resources. Never create, modify, read or
delete anything else in this account. There is other production data here.
  s3 bucket        {BUCKET}
  glue database    {DATABASE}
  glue job         {JOB_NAME}
  athena workgroup {WORKGROUP}
  iam role         {ROLE_ARN}   (already exists, use it, never modify IAM)

HOW TO CALL AWS. Use `aws___run_script`. Inside it:
    await call_boto3(service_name="glue", operation_name="StartJobRun",
                     region_name="{REGION}", params={{...}})
Keyword arguments only. `operation_name` is PascalCase. Assign your answer to
`result`, then put `result` alone on the final line.

GLUE + ICEBERG REQUIREMENTS, these are not optional:
  GlueVersion "6.0", WorkerType "G.1X", NumberOfWorkers 2, Command Name
  "glueetl", PythonVersion "3".
  DefaultArguments must include:
    "--datalake-formats": "iceberg"
    "--enable-glue-datacatalog": "true"
    "--conf": the five Iceberg settings joined by " --conf ", namely
      spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
      spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
      spark.sql.catalog.glue_catalog.warehouse=s3://{BUCKET}/warehouse/
      spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
      spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO

WHEN A JOB RUN FAILS, and it probably will:
  1. GetJobRun gives you ErrorMessage. Read it.
  2. For the real stack trace, read CloudWatch Logs. The error stream is
     log group /aws-glue/jobs/error, log stream = the JobRunId.
     Use logs GetLogEvents.
  3. Fix the script, PutObject it again, StartJobRun again.
  4. Do not give up after one failure. Iterate until the run state is SUCCEEDED.
     Stop and report if you have failed 5 times.

POLLING. After StartJobRun, poll GetJobRun every 30 seconds. Glue cold start is
1 to 3 minutes. Do not conclude the job hung before 8 minutes have passed.
Inside the sandbox you may `await asyncio.sleep(30)` between polls.

Be terse in your narration. Show the SQL and the numbers.
"""

PHASE_1 = f"""Build the initial load of the pipeline.

1. Read the source layout first. The raw CSVs are at
   s3://{BUCKET}/raw/orders/dt=2026-10-13/orders.csv and a second day at
   dt=2026-10-14. Look at the header and a few rows before you write any code.
   Use only the 2026-10-13 drop for this phase.

2. Write a PySpark Glue script that reads the 2026-10-13 CSV, casts the columns
   to sensible types (order_total is a double, items is an int, order_ts and
   updated_at are timestamps), and writes it as an Iceberg table
   glue_catalog.{DATABASE}.{TABLE} partitioned by country.
   Upload the script to s3://{BUCKET}/scripts/orders_etl.py

3. Create the Glue job {JOB_NAME} pointing at that script with the role and the
   Iceberg configuration from your instructions. Run it. Fix it until it is green.

4. Confirm the table is registered in the Glue Data Catalog.

5. Using Athena workgroup {WORKGROUP} and database {DATABASE}, report:
   total orders, distinct customers, total revenue, and a breakdown of orders
   and revenue by status. Report bytes scanned.
"""

PHASE_2 = f"""Now run the incremental load, and be careful here.

The s3://{BUCKET}/raw/orders/dt=2026-10-14/orders.csv drop contains BOTH brand
new orders AND late corrections to orders that already exist in the table from
the 2026-10-13 load. Corrections reuse the same order_id with a later
updated_at and a changed status. Some became refunds with order_total 0.

If you simply append this file you will double count the corrected orders and
your revenue will be wrong.

1. First, prove the problem. Query how many order_ids in the day-2 file already
   exist in {TABLE}.

2. Write a second PySpark script that performs an Iceberg MERGE INTO on
   order_id: update the existing row when the incoming updated_at is newer,
   insert when the order_id is new. Upload it to
   s3://{BUCKET}/scripts/orders_merge.py and run it through the same job.

3. Re-run the report from phase 1 and show what changed: the row count should
   reflect distinct orders, not appended duplicates, and revenue should drop
   because of the refunds.

4. Demonstrate one thing you can only do with Iceberg: query the table as of the
   snapshot before the merge, and show the two snapshots side by side.
"""

PHASE_2B = f"""The merge has not actually happened yet, and you need to understand why.

You started the Glue job passing "--script-location" as a run-level argument.
Glue ignores that. The script a job runs is a property of the JOB, not the run:
it lives at Command.ScriptLocation and the only way to change it is UpdateJob.

So all three of your runs re-executed the old {TABLE} ETL script, which does
mode("overwrite") on the day-1 CSV. Each run went green and rewrote the same
20,000 rows. The run state told you SUCCEEDED and the data never moved.

Fix it:
1. Call glue UpdateJob to point Command.ScriptLocation at
   s3://{BUCKET}/scripts/orders_merge.py, keeping the role, GlueVersion 6.0,
   worker config and all the Iceberg DefaultArguments exactly as they are.
2. Run the job and wait for SUCCEEDED.
3. Then VERIFY ON THE DATA, not on the run state. Query the table and confirm
   the row count actually changed from 20000 and that refunded orders now exist.
   If the numbers did not move, the merge did not happen, no matter what the
   job state says.
4. Report final row count, distinct order count, total revenue and the status
   breakdown, using Athena workgroup {WORKGROUP}.
5. Finally, list the Iceberg snapshots and query the table AS OF the snapshot
   before the merge, so you can show the before and after side by side.
"""


def run(mission):
    aws = MCPClient(lambda: stdio_client(StdioServerParameters(
        command="mcp-proxy-for-aws",
        args=[AWS_MCP_URL, "--metadata", f"AWS_REGION={REGION}"],
        env={**os.environ},
    )))
    with aws:
        agent = Agent(
            model=BedrockModel(model_id=MODEL_ID, region_name=REGION,
                               temperature=0, max_tokens=8000),
            tools=aws.list_tools_sync(),
            system_prompt=SYSTEM_PROMPT,
        )
        agent(mission)


if __name__ == "__main__":
    phase = sys.argv[1] if len(sys.argv) > 1 else "1"
    run({"1": PHASE_1, "2": PHASE_2, "2b": PHASE_2B}[phase])
