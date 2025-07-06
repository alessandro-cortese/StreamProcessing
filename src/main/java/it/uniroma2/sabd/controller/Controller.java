package it.uniroma2.sabd.controller;

import it.uniroma2.sabd.engineering.ChallengerMetricsFetcher;
import it.uniroma2.sabd.engineering.UploadResultSink;
import it.uniroma2.sabd.model.Batch;
import it.uniroma2.sabd.query.Q1SaturationMapFunction;
import it.uniroma2.sabd.query.Q2OutlierDetection;
import it.uniroma2.sabd.query.Q2OutlierDetectionOpt;
import it.uniroma2.sabd.query.Q3ClusteringMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import it.uniroma2.sabd.engineering.QueryMetricsSink;


import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class Controller {

    private static final int MAX_PARALLELISM_LEVEL = 8;
    private static final String RESULTS_DIR = "/Results/";

    public static void execute_computation(StreamExecutionEnvironment env, DataStream<Batch> source, boolean optimization) throws Exception {

        for (int parallelism_level = 1; parallelism_level <= MAX_PARALLELISM_LEVEL; parallelism_level++) {

            System.out.println("Start Pipeline with " + parallelism_level + " task manager.");

            // Q1
            DataStream<Batch> q1Result = source
                    .map(new Q1SaturationMapFunction())
                    .setParallelism(parallelism_level);

            String outputPathQ = RESULTS_DIR + "/task_manager_number_" + parallelism_level + (optimization ? "_opt" : "");

            q1Result.addSink(new QueryMetricsSink("Q1", parallelism_level, optimization, outputPathQ)).setParallelism(1);

            if (!optimization)
                writeCsv(env, q1Result.map(toCsvQ1()), "query1.csv", getHeaderQ1(), parallelism_level, false);
            else
                writeCsv(env, q1Result.map(toCsvQ1()), "query1.csv", getHeaderQ1(), parallelism_level, true);

            // Q2
            DataStream<Batch> q2Result;

            if (!optimization)
                q2Result = q1Result.rebalance()
                        .keyBy(batch -> batch.print_id + "_" + batch.tile_id)
                        .process(new Q2OutlierDetection())
                        .setParallelism(parallelism_level);
            else
                q2Result = q1Result.rebalance()
                        .keyBy(batch -> batch.print_id + "_" + batch.tile_id)
                        .process(new Q2OutlierDetectionOpt())
                        .setParallelism(parallelism_level);

            q2Result.addSink(new QueryMetricsSink("Q2", parallelism_level, optimization, outputPathQ)).setParallelism(1);

            if (!optimization)
                writeCsv(env, q2Result.map(toCsvQ2()), "query2.csv", getHeaderQ2(), parallelism_level, false);
            else
                writeCsv(env, q2Result.map(toCsvQ2()), "query2.csv", getHeaderQ2(), parallelism_level, true);

            // Q3
            DataStream<Batch> q3Result = q2Result
                    .map(new Q3ClusteringMapFunction())
                    .setParallelism(parallelism_level);

            q3Result.addSink(new UploadResultSink()).setParallelism(1);
            q3Result.addSink(new QueryMetricsSink("Q3", parallelism_level, optimization, outputPathQ)).setParallelism(1);

            if (!optimization)
                writeCsv(env, q3Result.map(toCsvQ3()), "query3.csv", getHeaderQ3(), parallelism_level, false);
            else
                writeCsv(env, q3Result.map(toCsvQ3()), "query3.csv", getHeaderQ3(), parallelism_level, true);

            if (!optimization)
                q3Result
                        .map(toTextClusters())
                        .writeAsText(RESULTS_DIR + "/task_manager_number_" + parallelism_level + "/" + "query3_clusters.txt", FileSystem.WriteMode.OVERWRITE)
                        .setParallelism(1);
            else
                q3Result
                        .map(toTextClusters())
                        .writeAsText(RESULTS_DIR + "/task_manager_number_" + parallelism_level + "_opt/" + "query3_clusters.txt", FileSystem.WriteMode.OVERWRITE)
                        .setParallelism(1);

            env.execute("Thermal Defect Analysis Pipeline - Q1 + Q2 + Q3");

            String benchId = Files.readString(Path.of(RESULTS_DIR + "current_bench_id.txt")).trim();
            System.out.println("Bench Id readed from file: " + benchId);

            ChallengerMetricsFetcher.fetchAndSaveLatestMetrics(parallelism_level, benchId);
            System.out.println("Metrics collected by the challenger with " + parallelism_level + " task manager.");
        }
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

    private static void writeCsv(StreamExecutionEnvironment env, DataStream<String> dataStream, String filename, String header, int parallelism_level, boolean optimization){
        DataStream<String> headerStream = env.fromData(header);

        if(!optimization)
            headerStream.union(dataStream)
                    .writeAsText(RESULTS_DIR + "/task_manager_number_" + parallelism_level + "/" + filename, FileSystem.WriteMode.OVERWRITE)
                    .setParallelism(1)
                    .name("Write " + filename);
        else
            headerStream.union(dataStream)
                    .writeAsText(RESULTS_DIR + "/task_manager_number_" + parallelism_level + "_opt/" + filename, FileSystem.WriteMode.OVERWRITE)
                    .setParallelism(1)
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
