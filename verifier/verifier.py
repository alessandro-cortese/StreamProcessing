import argparse
import logging
import requests
import umsgpack
import numpy as np
from sklearn.cluster import DBSCAN
from PIL import Image
import io
import csv
import os
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("demo_client")

# Create CSV files with headers if not present
for filename, headers in {
    "query1.csv": ["seq_id", "print_id", "tile_id", "saturated"],
    "query2.csv": ["seq_id", "print_id", "tile_id", "P1", "δP1", "P2", "δP2", "P3", "δP3", "P4", "δP4", "P5", "δP5"],
    "query3.csv": ["seq_id", "print_id", "tile_id", "saturated", "centroids"]
}.items():
    if not os.path.exists(filename):
        with open(filename, mode="w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)

def main():
    parser = argparse.ArgumentParser(description="Demo Client")
    parser.add_argument("endpoint", type=str, help="Endpoint URL")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of batches to process")
    args = parser.parse_args()

    url = args.endpoint
    limit = args.limit
    session = requests.Session()

    logger.info("Starting demo client")
    
    create_response = session.post(
        f"{url}/api/create",
        json={"apitoken": "polimi-deib", "name":"unoptimized", "test": True, "max_batches": limit},
    )
    create_response.raise_for_status()
    bench_id = create_response.json()
    # logger.info(f"Created bench {bench_id}")

    start_response = session.post(f"{url}/api/start/{bench_id}")
    assert start_response.status_code == 200
    # logger.info(f"Started bench {bench_id}")

    i = 0
    while not limit or i < limit:
        # logger.info(f"Getting batch {i}")
        next_batch_response = session.get(f"{url}/api/next_batch/{bench_id}")
        if next_batch_response.status_code == 404:
            break
        next_batch_response.raise_for_status()

        batch_input = umsgpack.unpackb(next_batch_response.content)
        result = process(batch_input)

        # logger.info(f"Sending batch result {i}")
        result_serialized = umsgpack.packb(result)
        result_response = session.post(
            f"{url}/api/result/0/{bench_id}/{i}",
            data=result_serialized
        )
        assert result_response.status_code == 200
        print(result_response.content)
        i += 1

    end_response = session.post(f"{url}/api/end/{bench_id}")
    end_response.raise_for_status()
    result = end_response.text
    # logger.info(f"Completed bench {bench_id}")
    print(f"Result: {result}")

def compute_outliers(image3d, empty_threshold, saturation_threshold, distance_threshold, outlier_threshold):
    image3d = image3d.astype(np.float64)
    depth, width, height = image3d.shape

    def get_padded(image, d, x, y, pad=0.0):
        if d < 0 or d >= image.shape[0]:
            return pad
        if x < 0 or x >= image.shape[1]:
            return pad
        if y < 0 or y >= image.shape[2]:
            return pad
        return image[d, x, y]

    outliers = []
    for y in range(height):
        for x in range(width):
            if image3d[-1, x, y] <= empty_threshold or image3d[-1, x, y] >= saturation_threshold:
                continue
            cn_sum = 0
            cn_count = 0
            for j in range(-distance_threshold, distance_threshold + 1):
                for i in range(-distance_threshold, distance_threshold + 1):
                    for d in range(depth):
                        distance = abs(i) + abs(j) + abs(depth - 1 - d)
                        if distance <= distance_threshold:
                            cn_sum += get_padded(image3d, d, x+i, y+j)
                            cn_count += 1
            on_sum = 0
            on_count = 0
            for j in range(-2 * distance_threshold, 2 * distance_threshold + 1):
                for i in range(-2 * distance_threshold, 2 * distance_threshold + 1):
                    for d in range(depth):
                        distance = abs(i) + abs(j) + abs(depth - 1 - d)
                        if distance > distance_threshold and distance <= 2 * distance_threshold:
                            on_sum += get_padded(image3d, d, x+i, y+j)
                            on_count += 1
            close_mean = cn_sum / cn_count
            outer_mean = on_sum / on_count
            dev = abs(close_mean - outer_mean)

            if image3d[-1, x, y] > empty_threshold and image3d[-1, x, y] < saturation_threshold and dev > outlier_threshold:
                outliers.append((x, y, dev))

    return outliers

def cluster_outliers_2d(outliers, eps=20, min_samples=5):
    if len(outliers) == 0:
        return []
    
    positions = np.array([(outlier[0], outlier[1]) for outlier in outliers])
    clustering = DBSCAN(eps=eps, min_samples=min_samples).fit(positions)
    labels = clustering.labels_

    centroids = []
    for label in set(labels):
        if label == -1:
            continue
        cluster_points = positions[labels == label]
        centroid = cluster_points.mean(axis=0)
        centroids.append({
            'x': float(centroid[0]),
            'y': float(centroid[1]),
            'count': len(cluster_points)
        })
    
    return centroids

tile_map = dict()

def process(batch):
    EMPTY_THRESH = 5000
    SATURATION_THRESH = 65000
    DISTANCE_FACTOR = 2
    OUTLIER_THRESH = 6000
    DBSCAN_EPS = 20
    DBSCAN_MIN = 5

    print_id = batch["print_id"]
    tile_id = batch["tile_id"]
    batch_id = batch["batch_id"]
    layer = batch["layer"]
    image = Image.open(io.BytesIO(batch["tif"]))
    # logger.info(f"Processing layer {layer} of print {print_id}, tile {tile_id}")

    if not (print_id, tile_id) in tile_map:
        tile_map[(print_id, tile_id)] = []

    window = tile_map[(print_id, tile_id)]
    if len(window) == 3:
        window.pop(0)
    window.append(image)

    saturated = np.count_nonzero(np.array(image) > SATURATION_THRESH)

    if len(window) == 3:
        image3d = np.stack(window, axis=0)
        outliers = compute_outliers(image3d, EMPTY_THRESH, SATURATION_THRESH, DISTANCE_FACTOR, OUTLIER_THRESH)
        centroids = cluster_outliers_2d(outliers, DBSCAN_EPS, DBSCAN_MIN)
        sorted_outliers = sorted(outliers, key=lambda o: -o[2])[:5]
    else:
        outliers = []
        centroids = []
        sorted_outliers = []

    # Query 1 output
    with open("query1.csv", mode="a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([batch_id, print_id, tile_id, saturated])

    # Query 2 output
    row = [batch_id, print_id, tile_id]
    for p in sorted_outliers:
        row.extend([f"({p[0]},{p[1]})", int(p[2])])
    while len(row) < 3 + 10:
        row.extend(["", ""])
    with open("query2.csv", mode="a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(row)

    # Query 3 output
    with open("query3.csv", mode="a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            batch_id,
            print_id,
            tile_id,
            saturated,
            json.dumps(centroids)
        ])

    result = {
        "batch_id": batch_id,
        "print_id": print_id,
        "tile_id": tile_id,
        "saturated": saturated,
        "centroids": centroids
    }

    #print(result)
    return result

if __name__ == "__main__":
    main()

