from pyflink.datastream.functions import SourceFunction
from engineering.api import ChallengerAPI

class ChallengerBatchSource(SourceFunction):

    def __init__(self, base_url="http://gc25-challenger:8866"):
        self.base_url = base_url
        self.running = True

    def run(self, ctx):
        api = ChallengerAPI(self.base_url)
        bench_id = api.create_benchmark(name="flink-analysis", test_mode=True)
        api.start_benchmark(bench_id)

        while self.running:
            batch = api.get_next_batch(bench_id)
            if batch is None:
                break
            ctx.collect(batch)

        final_report = api.end_benchmark(bench_id)
        print("Final report:", final_report)

    def cancel(self):
        self.running = False
