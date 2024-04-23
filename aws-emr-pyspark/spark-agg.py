from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as sum_, avg

# Initialize the Spark session
spark = SparkSession.builder \
    .appName("Date Level Aggregations") \
    .getOrCreate()

# Define S3 bucket paths
s3_bucket_path = "s3://bucket-name"
orders_path = f"{s3_bucket_path}/input_data/orders.csv"
order_items_path = f"{s3_bucket_path}/input_data/order_items.csv"
users_path = f"{s3_bucket_path}/input_data/users.csv"


# Read CSV files
orders_df = spark.read.csv(orders_path, header=True, inferSchema=True).alias('orders')
order_items_df = spark.read.csv(order_items_path, header=True, inferSchema=True).alias('items')
users_df = spark.read.csv(users_path, header=True, inferSchema=True).alias('users')

# Perform transformations
# Join orders with order items
orders_items_joined = orders_df.join(
    order_items_df,
    (orders_df['orders.order_id'] == order_items_df['items.order_id']),
    "inner"
)

# Join with users
complete_df = orders_items_joined.join(
    users_df,
    (orders_items_joined['orders.user_id'] == users_df['users.id']),
    "inner"
)

# Aggregate data on a daily level
daily_aggregations = complete_df.groupBy(to_date(col("orders.created_at")).alias("date")).agg(
    sum_("sale_price").alias("total_sales"),
    avg("num_of_item").alias("average_items_per_order")
)

# Define the output path
output_path = f"{s3_bucket_path}/output/daily_aggregations"

# Write the DataFrame back to S3 in parquet format
daily_aggregations.write.parquet(output_path, mode="overwrite")

# Stop the Spark session
spark.stop()