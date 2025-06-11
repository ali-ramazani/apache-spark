import pandas as pd
import matplotlib.pyplot as plt

# Load the CSV
df = pd.read_csv("results/pyspark_results.csv")

# Optional: convert timestamp to datetime if you want to sort chronologically
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Plot: Job Duration
plt.figure()
plt.bar(df['timestamp'], df['duration_sec'])
plt.title("PySpark Job Duration")
plt.ylabel("Seconds")
plt.xlabel("Run Timestamp")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("results/pyspark_duration.png")
plt.close()

# Plot: Memory Usage
plt.figure()
plt.bar(df['timestamp'], df['memory_mb'])
plt.title("PySpark Memory Usage")
plt.ylabel("MB")
plt.xlabel("Run Timestamp")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("results/pyspark_memory.png")
plt.close()

# Plot: CPU Usage
plt.figure()
plt.bar(df['timestamp'], df['cpu_percent'])
plt.title("PySpark CPU Utilization")
plt.ylabel("CPU %")
plt.xlabel("Run Timestamp")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("results/pyspark_cpu.png")
plt.close()

print("✅ Graphs saved in: results/")
