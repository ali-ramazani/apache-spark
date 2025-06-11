import pandas as pd
import matplotlib.pyplot as plt

# Load both datasets
pyspark_df = pd.read_csv("results/pyspark_results.csv")
rest_df = pd.read_csv("results/rest_results.csv")

# Aggregate mean for each
pyspark_avg = pyspark_df[['duration_sec', 'memory_mb', 'cpu_percent']].mean()
rest_avg = rest_df[['duration_sec', 'memory_mb', 'cpu_percent']].mean()

comparison_df = pd.DataFrame({
    "PySpark": pyspark_avg,
    "REST-API": rest_avg
})

# Transpose for plotting
comparison_df = comparison_df.T

# Plot each metric
for metric in ['duration_sec', 'memory_mb', 'cpu_percent']:
    plt.figure()
    comparison_df[metric].plot(kind='bar')
    plt.title(f"Average {metric.replace('_', ' ').title()}")
    plt.ylabel(metric.replace('_', ' ').title())
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig(f"results/comparison_{metric}.png")
    plt.close()

print("✅ Comparison plots saved in /results")
