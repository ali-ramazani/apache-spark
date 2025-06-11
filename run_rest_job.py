import requests
import time
import psutil
import os
import pandas as pd

start_time = time.time()
cpu_before = psutil.cpu_percent(interval=None)
mem_before = psutil.Process(os.getpid()).memory_info().rss / 1024**2

response = requests.post("http://localhost:5050/submit")
duration = time.time() - start_time

cpu_after = psutil.cpu_percent(interval=None)
mem_after = psutil.Process(os.getpid()).memory_info().rss / 1024**2

mem_used = mem_after - mem_before
cpu_used = cpu_after

metrics = pd.DataFrame([{
    "method": "rest-api",
    "duration_sec": round(duration, 2),
    "memory_mb": round(mem_used, 2),
    "cpu_percent": round(cpu_used, 2),
    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
}])

os.makedirs("results", exist_ok=True)
metrics.to_csv("results/rest_results.csv", mode='a', header=not os.path.exists("results/rest_results.csv"), index=False)

print(f"✅ REST job finished: {response.json()}")
