# An agent that builds a Glue + Iceberg pipeline, then reports on it

The agent writes the PySpark itself, creates the AWS Glue job, runs it, reads
its own failures out of CloudWatch, fixes them, and re-runs until the job is
green. Then it registers the Iceberg table in the Glue Data Catalog and queries
it with Athena.

There is no boto3 client code in the agent and no per-service MCP server. Every
AWS call goes through the managed AWS MCP Server, which exposes 8 tools for 300+
services and hands the model a Python sandbox instead of a tool catalog.

## What it builds

```
s3://.../raw/orders/dt=2026-10-13/orders.csv   20,000 orders
s3://.../raw/orders/dt=2026-10-14/orders.csv    2,800 rows
                                                  |
                                   Glue 6.0 job (PySpark, Iceberg)
                                                  |
                            s3://.../warehouse/   Iceberg table, partitioned
                                                  |
                                   AWS Glue Data Catalog
                                                  |
                                   Athena   ->   the report
```

The day-2 file is the interesting part. It contains 2,000 genuinely new orders
plus 800 late corrections that reuse day-1 `order_id`s, with a newer
`updated_at` and a changed status. 208 of them became refunds.

Append that file and you get 22,800 rows and inflated revenue. The correct
answer is 22,000 distinct orders with the refunds applied. An Iceberg
`MERGE INTO` on `order_id` is what gets you there, which is the point of phase 2.

## Layout

| Path | What it is |
| --- | --- |
| `setup/01_create_infrastructure.py` | Creates the bucket, IAM role, Glue database, Athena workgroup |
| `setup/02_generate_and_upload_data.py` | Generates the two synthetic daily drops and uploads them |
| `agent/glue_pipeline_agent.py` | The agent. Phase 1 initial load, phase 2 merge |
| `teardown.py` | Deletes exactly what was created, nothing else |

## Run it

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

On Windows, activate with:

```bash
venv\Scripts\activate
```

Then:

```bash
python setup/01_create_infrastructure.py
python setup/02_generate_and_upload_data.py
python agent/glue_pipeline_agent.py 1    # initial load
python agent/glue_pipeline_agent.py 2    # incremental merge
python teardown.py --commit              # delete everything
```

## Prerequisites

1. AWS credentials on your machine. The official `mcp-proxy-for-aws` signs every
   request with SigV4 using them.
2. Amazon Bedrock access to Claude Haiku 4.5 in `us-east-1`.
3. Permission to create an IAM role, an S3 bucket, a Glue database and job, and
   an Athena workgroup.

## The IAM position

The agent never gets IAM write access. `setup/01` creates the Glue service role
once, as a human step, and scopes it to the one demo bucket and the one demo
database. The agent is handed the role ARN and told to use it.

Letting an agent mint its own IAM roles is the one step worth keeping human, and
the role here is deliberately narrow enough to read in one screen.

## The fence

`teardown.py` carries an explicit allowlist of the five resource names it may
delete and raises if asked to touch anything else. The agent's system prompt
carries the same list as a hard boundary. Both exist because this was built in
an account with unrelated production data in it.

## Cost

Glue 6.0 bills per DPU-hour, cut about 30 percent in August 2026. Two G.1X
workers for a few minutes per run is cents. Athena scans here are a couple of MB
per query. The Bedrock spend on Haiku 4.5 is the largest line and it is still
small. Expect well under a dollar for a full run including failed attempts.
