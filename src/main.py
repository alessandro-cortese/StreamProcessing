import logging
import datetime
import numpy as np
from collections import defaultdict
from engineering.api import ChallengerAPI
from query.query1 import analyze_saturation
#from query.query2 import analyze_outliers
from query.query2_prova import analyze_outliers
from utils.utils import tiff_to_array, now_ms

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Main")

results_path = "/Results/"
query1_file = "query1.csv"
query2_file = "query2.csv"
query3_file = "query3.csv"


def main():
    # URL of the challenger (container name or localhost if outside Docker)
    base_url = "http://gc25-challenger:8866"
    api = ChallengerAPI(base_url)

    query1_results_file = results_path + query1_file
    query2_results_file = results_path + query2_file
    query3_results_file = results_path + query3_file

    with open(query1_results_file, "w") as f1:
        f1.write("batch_id,tile_id,print_id,saturated,latency_ms,timestamp\n")

    with open(query2_results_file, "w") as f2:
        f2.write("batch_id,tile_id,print_id," +
            ",".join([f"P{i}_x,P{i}_y,δP{i}" for i in range(1, 6)]) +
            ",latency_ms,timestamp\n")

    # with open(query3_results_file, "w") as f3:
    #     f3.write("batch_id,tile_id,print_id,saturated,latency_ms,timestamp\n")

    # Create and launch benchmarks
    bench_id = api.create_benchmark(name="q1-analysis", test_mode=True)
    api.start_benchmark(bench_id)

    tile_map = defaultdict(list)

    batch_count = 0

    while batch_count < 1000:
        # Request next batch
        batch = api.get_next_batch(bench_id)
        if batch is None:
            break

        logger.info(f"Batch {batch['batch_id']} - Print {batch['print_id']} - Tile {batch['tile_id']}")

        # Extract image and measure latency
        
        start_ts = now_ms()
        image_array = tiff_to_array(batch["tif"])
        
        # Query 1 
        
        saturated = analyze_saturation(image_array)
        end_ts = now_ms()

        with open(query1_results_file, "a") as f1:
            timestamp = datetime.datetime.utcnow().isoformat()
            f1.write(f"{batch['batch_id']},{batch['tile_id']},{batch['print_id']},{saturated},{end_ts - start_ts},{timestamp}\n")

        # Salta il batch se l'immagine non ha saturazione
        if saturated == 0:
            result = {
                "batch_id": batch["batch_id"],
                "print_id": batch["print_id"],
                "tile_id": batch["tile_id"],
                "saturated": saturated,
                "centroids": []  # Placeholder, will be added from Q3
            }
            api.submit_result(bench_id, batch["batch_id"], result)
            batch_count += 1
            continue

         # Mantieni finestra per tile
        key = (batch["print_id"], batch["tile_id"])
        tile_map[key].append(image_array)
        if len(tile_map[key]) > 3:
            tile_map[key].pop(0)

        if len(tile_map[key]) == 3:
            # Esegui Q2
            image3d = np.stack(tile_map[key], axis=0)
            outliers_result = analyze_outliers(image3d)

            # Scrivi risultato Q2
            with open(query2_results_file, "a") as f2:
                f2.write(f"{batch['batch_id']},{batch['tile_id']},{batch['print_id']}")
                for i in range(1, 6):
                    p = outliers_result.get(f"P{i}", ["-", "-"])
                    delta = outliers_result.get(f"δP{i}", "-")
                    f2.write(f",{p[0]},{p[1]},{delta}")
                f2.write(f",{end_ts - start_ts},{timestamp}\n")


        result = {
            "batch_id": batch["batch_id"],
            "print_id": batch["print_id"],
            "tile_id": batch["tile_id"],
            "saturated": saturated,
            "centroids": []  # Q3 output
        }

        # logger.info(f"Batch {batch['batch_id']} → Saturated points: {saturated} | Latency: {end_ts - start_ts} ms")

        # Send result to challenger
        api.submit_result(bench_id, batch["batch_id"], result)
        batch_count += 1


    # End benchmark
    final_report = api.end_benchmark(bench_id)
    logger.info("Benchmark Completed.")
    print("Final Result:\n", final_report)
    print(f"Processed {batch_count} batches.")

if __name__ == "__main__":
    main()
