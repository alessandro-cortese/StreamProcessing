from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import SourceFunction

from engineering.challenger_source import ChallengerBatchSource
from query.query1 import Q1Saturation
from query.query2 import Q2SlidingWindow
from utils.utils import now_ms


def format_q2_output(batch):
    outliers = batch.get("q2_outliers", {})
    line = f"{batch['batch_id']},{batch['tile_id']},{batch['print_id']}"
    for i in range(1, 6):
        p = outliers.get(f"P{i}", ["-", "-"])
        delta = outliers.get(f"\u03b4P{i}", "-")
        line += f",{p[0]},{p[1]},{delta}"
    line += f",{now_ms()},{batch['timestamp']}"
    return line


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    #source_func = SourceFunction(challenger_source)
    ds = env.add_source(ChallengerBatchSource(), type_info=Types.PICKLED_BYTE_ARRAY())

    # Query 1 pipeline
    ds = ds.map(Q1Saturation(), output_type=Types.PICKLED_BYTE_ARRAY())
    ds.map(lambda b: f"{b['batch_id']},{b['tile_id']},{b['print_id']},{b['saturated']},{now_ms()},{b['timestamp']}",
           output_type=Types.STRING()) \
      .write_as_text("/Results/query1.csv").name("Write Q1")

    # Query 2 pipeline
    ds.filter(lambda b: b["saturated"] > 0) \
      .key_by(lambda b: (b["print_id"], b["tile_id"])) \
      .process(Q2SlidingWindow(), output_type=Types.PICKLED_BYTE_ARRAY()) \
      .map(format_q2_output, output_type=Types.STRING()) \
      .write_as_text("/Results/query2.csv").name("Write Q2")

    env.execute("Thermal Defect Analysis Pipeline")


if __name__ == "__main__":
    main()
