from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# Initialize the Spark session
spark = SparkSession.builder \
    .appName("Delta Lake Upsert Data Aggregations") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .getOrCreate()

# Define S3 bucket paths
s3_bucket_path = "s3://nl-labs-aws/"
input_path = f"{s3_bucket_path}/raw_landing_zone/orders/"

output_path = f"{s3_bucket_path}/delta_lake/orders"

df = spark.read.format("json").load(input_path)

delta_table_exists = DeltaTable.isDeltaTable(spark, output_path)

if delta_table_exists:
    # Load the Delta table
    delta_table = DeltaTable.forPath(spark, output_path)
    
    # Perform UPSERT (MERGE)
    delta_table.alias("target").merge(
        df.alias("source"),
        "target.order_id = source.order_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    # If the Delta table does not exist, create one by writing out the current aggregation
    df.write.format("delta").mode("overwrite").save(output_path)

# Stop the Spark session
spark.stop()