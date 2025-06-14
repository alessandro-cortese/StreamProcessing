import logging
from engineering.api import ChallengerAPI
from query.query1 import analyze_saturation
from utils.utils import tiff_to_array, now_ms

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Main")

def main():
    # URL del challenger (nome del container o localhost se fuori da Docker)
    base_url = "http://gc25-challenger:8866"
    api = ChallengerAPI(base_url)

    # 1. Crea e avvia benchmark
    bench_id = api.create_benchmark(name="q1-analysis", test_mode=True)
    api.start_benchmark(bench_id)

    batch_count = 0

    while True:
        # 2. Richiedi batch successivo
        batch = api.get_next_batch(bench_id)
        if batch is None:
            break

        logger.info(f"Elaborazione batch {batch['batch_id']} - Print {batch['print_id']} - Tile {batch['tile_id']}")

        # 3. Estrai immagine e misura latenza
        start_ts = now_ms()
        image_array = tiff_to_array(batch["tif"])
        saturated = analyze_saturation(image_array)
        end_ts = now_ms()

        # 4. Costruisci risultato
        result = {
            "batch_id": batch["batch_id"],
            "print_id": batch["print_id"],
            "tile_id": batch["tile_id"],
            "saturated": saturated,
            "centroids": []  # Placeholder, verranno aggiunti da Q3
        }

        logger.info(f"Batch {batch['batch_id']} → Saturated points: {saturated} | Latency: {end_ts - start_ts} ms")

        # 5. Invia risultato al challenger
        api.submit_result(bench_id, batch["batch_id"], result)
        batch_count += 1

    # 6. Termina benchmark
    final_report = api.end_benchmark(bench_id)
    logger.info("Benchmark completato.")
    print("Final Result:\n", final_report)
    print(f"Processed {batch_count} batches.")

if __name__ == "__main__":
    main()
