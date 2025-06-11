from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("data/trades")
result = df.filter(col("trade_volume") > 500).groupBy("ticker").count()
result.collect()
