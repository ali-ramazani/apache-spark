import time
import psutil
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd

# Setup Spark
spark = SparkSession.builder \
    .appName("PySpark Benchmark") \
    .master("local[*]") \
    .getOrCreate()

# Measure resource usage
process = psutil.Process(os.getpid())
cpu_before = psutil.cpu_percent(interval=None)
mem_before = process.memory_info().rss / 1024**2

start_time = time.time()

# Load and transform data
df = spark.read.parquet("data/trades")
result = df.filter(col("trade_volume") > 500) \
           .groupBy("ticker") \
           .count()

result.collect()  # Trigger execution

end_time = time.time()
cpu_after = psutil.cpu_percent(interval=None)
mem_after = process.memory_info().rss / 1024**2

# Results
duration = end_time - start_time
mem_used = mem_after - mem_before
cpu_used = cpu_after

print(f"Duration: {duration:.2f}s | Memory: {mem_used:.2f}MB | CPU: {cpu_used:.2f}%")

# Optional: Log to CSV
metrics = pd.DataFrame([{
    "method": "pyspark",
    "duration_sec": round(duration, 2),
    "memory_mb": round(mem_used, 2),
    "cpu_percent": round(cpu_used, 2),
    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
}])

os.makedirs("results", exist_ok=True)
metrics.to_csv("results/pyspark_results.csv", mode='a', header=not os.path.exists("results/pyspark_results.csv"), index=False)

spark.stop()
