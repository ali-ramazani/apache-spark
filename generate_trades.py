from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, round, expr
import os

# Configure output path
output_path = "data/trades"
os.makedirs(output_path, exist_ok=True)

# Initialize Spark
spark = SparkSession.builder \
    .appName("Generate Trade Data") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# Generate synthetic trade data
num_rows = 10_000_000
trades_df = spark.range(0, num_rows).withColumn("ticker", expr("CASE WHEN id % 2 = 0 THEN 'AAPL' ELSE 'TSLA' END")) \
    .withColumn("trade_volume", round(rand() * 1000)) \
    .withColumn("price", round(rand() * 1000, 2)) \
    .withColumn("trade_type", expr("CASE WHEN id % 3 = 0 THEN 'BUY' ELSE 'SELL' END")) \
    .withColumn("timestamp", expr("current_timestamp()"))

# Save as Parquet (swap to Delta if needed)
trades_df.write.mode("overwrite").parquet(output_path)

# For Delta, use:
# trades_df.write.format("delta").mode("overwrite").save(output_path)

spark.stop()
print(f"✔️ Successfully generated {num_rows} records at {output_path}")
