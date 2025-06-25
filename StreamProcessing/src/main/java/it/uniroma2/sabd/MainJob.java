package it.uniroma2.sabd;

import it.uniroma2.sabd.engineering.ChallengerSource;
import it.uniroma2.sabd.engineering.UploadResultSinkQ0;
import it.uniroma2.sabd.engineering.UploadResultSinkQ3;
import it.uniroma2.sabd.model.Batch;
import it.uniroma2.sabd.query.Q1SaturationMapFunction;
import it.uniroma2.sabd.query.Q2OutlierDetection;
import it.uniroma2.sabd.query.Q3ClusteringMapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import java.util.List;

/*
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* Q1 - Count the number of saturated pixels in each image (tif) by marking them in the saturated field                *
* Q2 -  It analyses saturated batches to find local thermal outliers on 3D time windows.                              *
* Q3 -  Clustering with DB SCAN                                                                                       *
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* */



public class MainJob {

    private static final int PARALLELISM_LEVEL = 8;

    public static void main(String[] args) throws Exception {

        System.out.println("Avvio pipeline: Query 1 - Saturation Detection + Query 2 - Outlier Detection");

        // Create Flink environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM_LEVEL);

        DataStream<Batch> source = env.addSource(new ChallengerSource());

        source.map((MapFunction<Batch, String>) batch -> {
            System.out.println(">>> Batch ricevuto: " + batch.batch_id); // stampa lato JobManager/TaskManager
            return ">>> Batch ricevuto: " + batch.batch_id; // ritorna una stringa vuota, ignora
        });

        // Query 1 - Transforms each batch by calculating how many pixels are saturated
        DataStream<Batch> q1Result = source.map(new Q1SaturationMapFunction()).setParallelism(PARALLELISM_LEVEL);

        q1Result.addSink(new UploadResultSinkQ0());
        // Saving Q1 results
        String q1Header = "batch_id,tile_id,print_id,saturated,latency_ms,timestamp";
        DataStream<String> q1HeaderStream = env.fromData(q1Header);
        DataStream<String> q1DataStream = q1Result.map((MapFunction<Batch, String>) batch ->
                String.format("%d,%d,%s,%d,%d,%s",
                        batch.batch_id,
                        batch.tile_id,
                        batch.print_id,
                        batch.saturated,
                        batch.latency_ms,
                        batch.timestamp));

        // Only batches with at least one saturated pixel are analysed by Query 2.
        q1HeaderStream.union(q1DataStream)
                .writeAsText("/Results/query1.csv", FileSystem.WriteMode.OVERWRITE)
                .name("Write Q1 Results");

        // Query 2 - Outlier Detection
        DataStream<Batch> filteredQ1 = q1Result.filter((FilterFunction<Batch>) batch -> batch.saturated > 0);

        DataStream<Batch> q2Result = filteredQ1
                .keyBy(batch -> batch.print_id + "_" + batch.tile_id)
                .process(new Q2OutlierDetection()).setParallelism(PARALLELISM_LEVEL);

        // Saving Q2 Results
        String q2Header = "batch_id,print_id,tile_id," +
                "P1_x,P1_y,δP1," +
                "P2_x,P2_y,δP2," +
                "P3_x,P3_y,δP3," +
                "P4_x,P4_y,δP4," +
                "P5_x,P5_y,δP5," +
                "latency_ms,timestamp";
        DataStream<String> q2HeaderStream = env.fromData(q2Header);
        DataStream<String> q2DataStream = q2Result.map((MapFunction<Batch, String>) batch -> {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%d,%s,%d", batch.batch_id, batch.print_id, batch.tile_id));

            for (int i = 1; i <= 5; i++) {
                Object pos = batch.q2_top5_outliers.getOrDefault("P" + i, List.of(0, 0));
                Object delta = batch.q2_top5_outliers.getOrDefault("δP" + i, 0);

                if (pos instanceof List<?> l && l.size() == 2) {
                    sb.append(String.format(",%s,%s,%s", l.get(0), l.get(1), delta));
                } else {
                    sb.append(",0,0,0");
                }
            }

            sb.append(String.format(",%d,%s", batch.latency_ms, batch.timestamp));
            return sb.toString();
        });

        q2HeaderStream.union(q2DataStream)
                .writeAsText("/Results/query2.csv", FileSystem.WriteMode.OVERWRITE)
                .name("Write Q2 Results");

        // Q3
        DataStream<Batch> q3Result = q2Result
                .map(new Q3ClusteringMapFunction())
                .map((MapFunction<Batch, Batch>) batch -> {
                    String benchId = ChallengerSource.BENCH_ID;
                    //ChallengerSource.uploadResult(batch, benchId);
                    return batch;
                });

        q3Result = q2Result.map(new Q3ClusteringMapFunction()).setParallelism(PARALLELISM_LEVEL);

        q3Result.addSink(new UploadResultSinkQ3());
        String q3Header = "batch_id,print_id,tile_id,saturated,centroids";
        DataStream<String> q3HeaderStream = env.fromData(q3Header);
        DataStream<String> q3DataStream = q3Result.map((MapFunction<Batch, String>) batch -> {
            String centroids = String.join(";", batch.q3_clusters);
            return String.format("%d,%s,%d,%d,%s",
                    batch.batch_id,
                    batch.print_id,
                    batch.tile_id,
                    batch.saturated,
                    centroids);
        });
        q3HeaderStream.union(q3DataStream)
                .writeAsText("/Results/query3.csv", FileSystem.WriteMode.OVERWRITE)
                .name("Write Q3 Results");
        DataStream<String> q3TextClusters = q3Result.map((MapFunction<Batch, String>) batch -> {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("batch_id: %d, print_id: %s, tile_id: %d\n", batch.batch_id, batch.print_id, batch.tile_id));
            for (String cluster : batch.q3_clusters) {
                sb.append("Cluster: ").append(cluster).append("\n");
            }
            sb.append("\n");
            return sb.toString();
        });

        q3TextClusters.writeAsText("/Results/query3_clusters.txt", FileSystem.WriteMode.OVERWRITE)
                .name("Write Q3 Clusters as TXT");

        env.execute("Thermal Defect Analysis Pipeline - Q1 + Q2 + Q3");
    }
}
