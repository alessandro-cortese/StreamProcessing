import pandas as pd
import matplotlib.pyplot as plt
import os

file_path = "./metrics_summary.csv"
df = pd.read_csv(file_path)
df.columns = df.columns.str.strip()

output_dir = "query_performance_charts"
os.makedirs(output_dir, exist_ok=True)

def plot_metrics(df_subset, query_name, optimization_flag):
    df_sorted = df_subset.sort_values(by='parallelism')
    opt_label = "Optimized" if optimization_flag == "opt" else "Normal"

    # Throughput plot
    plt.figure(figsize=(10, 5))
    plt.plot(df_sorted['parallelism'], df_sorted['throughput_events_per_sec'], marker='o', color='blue')
    plt.xlabel('Task Manager (parallelism)')
    plt.ylabel('Throughput (events/sec)')
    plt.title(f'{query_name} Throughput - {opt_label}')
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f'{query_name}_throughput_{optimization_flag}.png'))
    plt.close()

    # Latency plot
    plt.figure(figsize=(10, 5))
    plt.plot(df_sorted['parallelism'], df_sorted['avg_latency_ms'], marker='o', color='blue')
    plt.xlabel('Task Manager (parallelism)')
    plt.ylabel('Latency (ms)')
    plt.title(f'{query_name} Latency - {opt_label}')
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f'{query_name}_latency_{optimization_flag}.png'))
    plt.close()

queries = ['Q1', 'Q2', 'Q3']
for query in queries:
    for opt in ['noopt', 'opt']:
        subset = df[(df['query'] == query) & (df['optimization'] == opt)]
        if not subset.empty:
            plot_metrics(subset, query, opt)
