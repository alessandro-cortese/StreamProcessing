import requests
import msgpack
import logging

logger = logging.getLogger("ChallengerAPI")
logging.basicConfig(level=logging.INFO)

class ChallengerAPI:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.bench_id = None

    # Initialise a new benchmark session with name and batch limit
    def create_benchmark(self, name="default-benchmark", test_mode=True, max_batches=None):
        payload = {
            "apitoken": "polimi-deib",
            "name": name,
            "test": test_mode,
        }
        if max_batches:
            payload["max_batches"] = max_batches

        logger.info("Creating benchmark...")
        response = self.session.post(f"{self.base_url}/api/create", json=payload)
        response.raise_for_status()
        self.bench_id = response.json()
        logger.info(f"Benchmark created: {self.bench_id}")
        return self.bench_id

    # Starts the benchmark
    def start_benchmark(self, bench_id=None):
        bench_id = bench_id or self.bench_id
        logger.info("Starting benchmark...")
        response = self.session.post(f"{self.base_url}/api/start/{bench_id}")
        response.raise_for_status()
        logger.info(f"Benchmark started: {bench_id}")

    # Requires the next batch of data from the challenger
    def get_next_batch(self, bench_id=None):
        bench_id = bench_id or self.bench_id
        logger.debug("Requesting next batch...")
        response = self.session.get(f"{self.base_url}/api/next_batch/{bench_id}")
        if response.status_code == 404:
            logger.info("No more batches.")
            return None
        response.raise_for_status()
        return msgpack.unpackb(response.content, strict_map_key=False)

    # Send processed result (Q1-Q3)
    def submit_result(self, bench_id, batch_id, result_dict):
        logger.info(f"Submitting result for batch {batch_id}...")
        result_serialized = msgpack.packb(result_dict)
        response = self.session.post(
            f"{self.base_url}/api/result/0/{bench_id}/{batch_id}",
            data=result_serialized
        )
        response.raise_for_status()
        logger.info(f"Result submitted: {response.content.decode()}")

    # Terminates the benchmark and returns the final report
    def end_benchmark(self, bench_id=None):
        bench_id = bench_id or self.bench_id
        logger.info("Ending benchmark...")
        response = self.session.post(f"{self.base_url}/api/end/{bench_id}")
        response.raise_for_status()
        logger.info("Benchmark completed.")
        return response.text
