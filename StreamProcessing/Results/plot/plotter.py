import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import re

# === Caricamento CSV ===
query2_df = pd.read_csv("../query2.csv")
query3_df = pd.read_csv("../query3.csv", sep=",", engine="python", quotechar='"', dtype=str)


# === Estrai outlier da Query 2 ===
outliers = []
for _, row in query2_df.iterrows():
    for i in range(1, 6):
        x = row.get(f"P{i}_x", 0)
        y = row.get(f"P{i}_y", 0)
        delta = row.get(f"δP{i}", 0)
        if (x, y, delta) != (0, 0, 0):
            outliers.append((x, y, delta))

outliers_np = np.array(outliers)
x_out, y_out, dev_out = outliers_np[:, 0], outliers_np[:, 1], outliers_np[:, 2]

# === Estrai centroidi da Query 3 ===
def parse_centroids(row):
    centroids = []
    matches = re.findall(r"\((\d+),(\d+),(\d+)\)", str(row))
    for m in matches:
        x, y, count = map(int, m)
        centroids.append((x, y, count))
    return centroids

query3_df["parsed_centroids"] = query3_df["centroids"].apply(parse_centroids)

# === Plot ===
plt.figure(figsize=(10, 8))
sc = plt.scatter(x_out, y_out, c=dev_out.astype(float), cmap='viridis', s=10)
plt.colorbar(sc, label="Distance from Neighbors")

for _, row in query3_df.iterrows():
    for (x, y, count) in row["parsed_centroids"]:
        plt.scatter(x, y, color='red', marker='x', s=80)
        plt.text(x + 5, y + 5, str(count), color='red', fontsize=8)

plt.title("Outliers and Cluster Centroids")
plt.xlabel("Column")
plt.ylabel("Row")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.savefig("outliers_with_centroids.png")
plt.show()
