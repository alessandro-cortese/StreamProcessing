import pandas as pd
import matplotlib.pyplot as plt
import os

file_path = "./query_metrics.csv"
output_dir = "query_performance_charts_kafka"
os.makedirs(output_dir, exist_ok=True)

df = pd.read_csv(file_path)
df.columns = df.columns.str.strip()

def plot_metrics(df_subset, query_name):
    df_sorted = df_subset.sort_values(by='parallelism')

    plt.figure(figsize=(10, 5))
    plt.plot(df_sorted['parallelism'], df_sorted['throughput_events_per_sec'],
             marker='o', color='blue', linewidth=2)
    plt.xlabel('Consumer Number (parallelism)')
    plt.ylabel('Throughput (events/sec)')
    plt.title(f'{query_name} Throughput')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f'{query_name}_throughput.png'), dpi=300)
    plt.close()

    plt.figure(figsize=(10, 5))
    plt.plot(df_sorted['parallelism'], df_sorted['avg_latency_ms'],
             marker='o', color='blue', linewidth=2)
    plt.xlabel('Consumer Number (parallelism)')
    plt.ylabel('Latency (ms)')
    plt.title(f'{query_name} Latency')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f'{query_name}_latency.png'), dpi=300)
    plt.close()

for query in df['query'].unique():
    subset = df[df['query'] == query]
    if not subset.empty:
        plot_metrics(subset, query)
