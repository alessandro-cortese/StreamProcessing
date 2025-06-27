package it.uniroma2.sabd;

import it.uniroma2.sabd.engineering.*;
import it.uniroma2.sabd.model.Batch;
import it.uniroma2.sabd.query.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class MainJob {

    private static final int PARALLELISM_LEVEL = 8;
    private static final String RESULTS_DIR = "/Results/";

    public static void main(String[] args) throws Exception {

        System.out.println("Start Pipeline: Q1 - Saturation + Q2 - Outlier Detection + Q3 - Clustering");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Batch> source = env.addSource(new ChallengerSource());

        DataStream<Batch> q1Result = source
                .map(new Q1SaturationMapFunction())
                .setParallelism(PARALLELISM_LEVEL);

        q1Result.addSink(new UploadResultSinkQ3()).setParallelism(1);

        writeCsv(env, q1Result.map(toCsvQ1()), "query1.csv", getHeaderQ1());

//        DataStream<Batch> q2Result = q1Result
//                .rebalance()
//                .keyBy(batch -> batch.print_id + "_" + batch.tile_id)
//                .process(new Q2OutlierDetectionOpt())
//                .setParallelism(PARALLELISM_LEVEL);

        DataStream<Batch> q2Result = q1Result
                .rebalance()
                .keyBy(batch -> batch.print_id + "_" + batch.tile_id)
                .process(new Q2OutlierDetectionOpt())
                .setParallelism(PARALLELISM_LEVEL);



        writeCsv(env, q2Result.map(toCsvQ2()), "query2.csv", getHeaderQ2());

        DataStream<Batch> q3Result = q2Result
                .map(new Q3ClusteringMapFunction())
                .setParallelism(PARALLELISM_LEVEL);

        //q3Result.addSink(new UploadResultSinkQ3()).setParallelism(1);
        writeCsv(env, q3Result.map(toCsvQ3()), "query3.csv", getHeaderQ3());

        q3Result
                .map(toTextClusters())
                .writeAsText(RESULTS_DIR + "query3_clusters.txt", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute("Thermal Defect Analysis Pipeline - Q1 + Q2 + Q3");

        String benchId = Files.readString(Path.of(RESULTS_DIR + "current_bench_id.txt")).trim();
        System.out.println("Bench Id readed from file: " + benchId);

        ChallengerMetricsFetcher.fetchAndSaveLatestMetrics(PARALLELISM_LEVEL, benchId);
        System.out.println("Metrics collected by the challenger.");
    }

    private static MapFunction<Batch, String> toCsvQ1() {
        return batch -> String.format("%d,%d,%s,%d,%d,%s",
                batch.batch_id, batch.tile_id, batch.print_id,
                batch.saturated, batch.latency_ms, batch.timestamp);
    }

    private static MapFunction<Batch, String> toCsvQ2() {
        return batch -> {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%d,%s,%d", batch.batch_id, batch.print_id, batch.tile_id));
            for (int i = 1; i <= 5; i++) {
                Object pos = batch.q2_top5_outliers.getOrDefault("P" + i, List.of(0, 0));
                Object delta = batch.q2_top5_outliers.getOrDefault("\u03b4P" + i, 0);
                if (pos instanceof List<?> l && l.size() == 2) {
                    sb.append(String.format(",%s,%s,%s", l.get(0), l.get(1), delta));
                } else {
                    sb.append(",0,0,0");
                }
            }
            sb.append(String.format(",%d,%s", batch.latency_ms, batch.timestamp));
            return sb.toString();
        };
    }

    private static MapFunction<Batch, String> toCsvQ3() {
        return batch -> String.format("%d,%s,%d,%d,%s",
                batch.batch_id, batch.print_id, batch.tile_id,
                batch.saturated, String.join(";", batch.q3_clusters));
    }

    private static MapFunction<Batch, String> toTextClusters() {
        return batch -> {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("batch_id: %d, print_id: %s, tile_id: %d\n",
                    batch.batch_id, batch.print_id, batch.tile_id));
            for (String cluster : batch.q3_clusters) {
                sb.append("Cluster: ").append(cluster).append("\n");
            }
            sb.append("\n");
            return sb.toString();
        };
    }

    private static void writeCsv(StreamExecutionEnvironment env, DataStream<String> dataStream, String filename, String header) {
        DataStream<String> headerStream = env.fromData(header);
        headerStream.union(dataStream)
                .writeAsText(RESULTS_DIR + filename, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1) // <--- soluzione 1: forzare scrittura singola
                .name("Write " + filename);
    }

    private static String getHeaderQ1() {
        return "batch_id,tile_id,print_id,saturated,latency_ms,timestamp";
    }

    private static String getHeaderQ2() {
        return "batch_id,print_id,tile_id," +
                "P1_x,P1_y,\u03b4P1," +
                "P2_x,P2_y,\u03b4P2," +
                "P3_x,P3_y,\u03b4P3," +
                "P4_x,P4_y,\u03b4P4," +
                "P5_x,P5_y,\u03b4P5," +
                "latency_ms,timestamp";
    }

    private static String getHeaderQ3() {
        return "batch_id,print_id,tile_id,saturated,centroids";
    }
}
