import pandas as pd
import matplotlib.pyplot as plt
import re
import os

# === CONFIG ===
data = "./challenger_metrics_kafka.csv"
output_dir = "challenger_metrics_charts_kafka"

# === PARSE LATENCY STRING ===
def parse_latency(latency_str):
    """
    Converte stringhe di latenza in millisecondi.
    Esempi supportati: '12s775ms733µs853ns', '39s107ms178µs730ns', ecc.
    """
    if pd.isna(latency_str) or latency_str == '':
        return 0.0

    latency_str = str(latency_str).strip()
    total_ms = 0.0

    # Secondi
    s_match = re.search(r'(\d+(?:\.\d+)?)s', latency_str)
    if s_match:
        total_ms += float(s_match.group(1)) * 1000

    # Millisecondi
    ms_match = re.search(r'(\d+(?:\.\d+)?)ms', latency_str)
    if ms_match:
        total_ms += float(ms_match.group(1))

    # Microsecondi
    us_match = re.search(r'(\d+(?:\.\d+)?)µs', latency_str)
    if us_match:
        total_ms += float(us_match.group(1)) / 1000

    # Nanosecondi
    ns_match = re.search(r'(\d+(?:\.\d+)?)ns', latency_str)
    if ns_match:
        total_ms += float(ns_match.group(1)) / 1_000_000

    return total_ms

# === CARICA CSV E PREPARA DATAFRAME ===
df = pd.read_csv(data)
df.columns = df.columns.str.strip()

# Converti le latenze in millisecondi (con arrotondamento opzionale)
df['latency_mean'] = df['latency_mean'].apply(parse_latency).round(2)
df['latency_max'] = df['latency_max'].apply(parse_latency).round(2)

# === CREA CARTELLA GRAFICI ===
os.makedirs(output_dir, exist_ok=True)

# === GRAFICO LATENZA ===
def plot_latency(df_subset):
    df_sorted = df_subset.sort_values(by='parallelism')

    plt.figure(figsize=(10, 6))

    plt.plot(df_sorted['parallelism'], df_sorted['latency_mean'], marker='o',
             color='blue', label='Latency Mean (ms)', linewidth=2, markersize=6)

    plt.plot(df_sorted['parallelism'], df_sorted['latency_max'], marker='s',
             color='orange', label='Latency Max (ms)', linewidth=2, markersize=6)

    plt.xlabel('Consumer Number (parallelism)')
    plt.ylabel('Latency (ms)')
    plt.title(f"Latency Variance")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{output_dir}/latency_vs_parallelism.png", dpi=300)
    plt.close()

# === GRAFICO THROUGHPUT ===
def plot_throughput(df_subset):
    df_sorted = df_subset.sort_values(by='parallelism')

    plt.figure(figsize=(10, 6))

    plt.plot(df_sorted['parallelism'], df_sorted['throughput'], linestyle='-', color='blue', linewidth=2)
    plt.scatter(df_sorted['parallelism'], df_sorted['throughput'], color='blue', s=80)

    plt.xlabel('Consumer Number (parallelism)')
    plt.ylabel('Throughput')
    plt.title(f"Throughput Variance")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{output_dir}/throughput.png", dpi=300)
    plt.close()

# === SUDDIVISIONE DATI ===
df_a = df.iloc[0:8]   # Normal
df_b = df.iloc[8:16]  # Optimized (se presente)

# === GENERAZIONE GRAFICI ===
plot_throughput(df_a)
plot_latency(df_a)

print("Grafici generati con successo nella cartella 'challenger_metrics_charts_kafka'")
