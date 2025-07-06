import pandas as pd
import matplotlib.pyplot as plt
import re
import os

data = "./challenger_metrics.csv"

df = pd.read_csv(data)
df.columns = df.columns.str.strip()

def parse_latency(latency_str):
    match = re.match(r'(?:(\d+)ms)?(?:(\d+)µs)?(?:(\d+)ns)?', str(latency_str))
    if not match:
        return 0
    ms = int(match.group(1)) if match.group(1) else 0
    us = int(match.group(2)) if match.group(2) else 0
    ns = int(match.group(3)) if match.group(3) else 0
    return ms + us / 1000 + ns / 1_000_000

df['latency_mean_ms'] = df['latency_mean'].apply(parse_latency)
df['latency_max_ms'] = df['latency_max'].apply(parse_latency)

os.makedirs("challenger_metrics_charts", exist_ok=True)

color_map = {
    1: 'red',
    2: 'blue',
    3: 'green',
    4: 'orange',
    5: 'purple',
    6: 'brown',
    7: 'cyan',
    8: 'magenta',
}

def plot_throughput(df_subset, name_suffix):
    df_sorted = df_subset.sort_values(by='parallelism')
    plt.figure(figsize=(10, 5))
    
    plt.plot(df_sorted['parallelism'], df_sorted['throughput'], linestyle='-', color='blue', linewidth=1.5)
    plt.scatter(df_sorted['parallelism'], df_sorted['throughput'], color='blue', s=80)

    plt.xlabel('Task Manager (parallelism)')
    plt.ylabel('Throughput')

    title = "Throughput Variance Normal" if name_suffix == "a" else "Throughput Variance Optimized"
    plt.title(title)

    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f"challenger_metrics_charts/throughput_{name_suffix}.png")
    plt.close()

def plot_latency(df_subset, name_suffix):
    df_sorted = df_subset.sort_values(by='parallelism')

    plt.figure(figsize=(10, 5))

    plt.plot(df_sorted['parallelism'], df_sorted['latency_mean_ms'], marker='o', color='blue', label='Latency Mean (ms)')
    plt.plot(df_sorted['parallelism'], df_sorted['latency_max_ms'], marker='o', color='orange', label='Latency Max (ms)')

    plt.xlabel('Task Manager (parallelism)')
    plt.ylabel('Latency (ms)')

    title = "Latency Variance Normal" if name_suffix == "a" else "Latency Variance Optimized"
    plt.title(title)

    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f"challenger_metrics_charts/latency_vs_parallelism_{name_suffix}.png")
    plt.close()

df_a = df.iloc[1:9]   
df_b = df.iloc[8:17]  

plot_throughput(df_a, "a")
plot_latency(df_a, "a")
plot_throughput(df_b, "b")
plot_latency(df_b, "b")