package it.uniroma2.sabd;

import it.uniroma2.sabd.engineering.ChallengerSource;
import it.uniroma2.sabd.model.Batch;
//import it.uniroma2.sabd.query.Q1SaturationMapFunction;
//import it.uniroma2.sabd.query.Q2SlidingWindowProcessFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class MainJob {

    public static void main(String[] args) throws Exception {

        System.out.println("PROVA 2");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Add source
        DataStream<Batch> source = env.addSource(new ChallengerSource());

        // Query 1: Saturation detection
        // DataStream<Batch> q1Processed = source.map(new Q1SaturationMapFunction());

//        q1Processed
//                .map(new MapFunction<Batch, String>() {
//                    @Override
//                    public String map(Batch batch) {
//                        return String.format("%d,%s,%s,%d,%d,%s",
//                                batch.batch_id,
//                                batch.tile_id,
//                                batch.print_id,
//                                batch.saturated,
//                                batch.latency_ms,
//                                batch.timestamp);
//                    }
//                })
//                .writeAsText("/Results/query1.csv").name("Write Q1");

        // Query 2: Sliding window deviation analysis
//        q1Processed
//                .filter(new FilterFunction<Batch>() {
//                    @Override
//                    public boolean filter(Batch batch) {
//                        return batch.saturated > 0;
//                    }
//                })
//                .keyBy(batch -> Tuple2.of(batch.print_id, batch.tile_id))
//                .process(new Q2SlidingWindowProcessFunction())
//                .map(new MapFunction<Batch, String>() {
//                    @Override
//                    public String map(Batch batch) {
//                        StringBuilder line = new StringBuilder();
//                        line.append(String.format("%d,%s,%s",
//                                batch.batch_id,
//                                batch.tile_id,
//                                batch.print_id));
//                        for (int i = 1; i <= 5; i++) {
//                            Object pos = batch.q2_outliers.getOrDefault("P" + i, new int[]{-1, -1});
//                            Object delta = batch.q2_outliers.getOrDefault("\u03b4P" + i, "-");
//                            int[] point = (int[]) pos;
//                            line.append(String.format(",%d,%d,%s", point[0], point[1], delta));
//                        }
//                        line.append(String.format(",%d,%s", batch.latency_ms, batch.timestamp));
//                        return line.toString();
//                    }
//                })
//                .writeAsText("/Results/query2.csv").name("Write Q2");

        env.execute("Thermal Defect Analysis Pipeline");
    }
}