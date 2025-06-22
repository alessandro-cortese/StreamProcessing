package it.uniroma2.sabd;

import it.uniroma2.sabd.engineering.ChallengerSource;
import it.uniroma2.sabd.model.Batch;
import it.uniroma2.sabd.query.Q1SaturationMapFunction;
import it.uniroma2.sabd.query.Q2OutlierDetection;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class MainJob {
    public static void main(String[] args) throws Exception {
        System.out.println("Avvio pipeline: Query 1 - Saturation Detection + Query 2 - Outlier Detection");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Batch> source = env.addSource(new ChallengerSource());

        // Query 1
        DataStream<Batch> q1Result = source.map(new Q1SaturationMapFunction());

        // Salvataggio risultati Q1
        String q1Header = "batch_id,tile_id,print_id,saturated,latency_ms,timestamp";
        DataStream<String> q1HeaderStream = env.fromElements(q1Header);
        DataStream<String> q1DataStream = q1Result.map((MapFunction<Batch, String>) batch ->
                String.format("%d,%d,%s,%d,%d,%s",
                        batch.batch_id,
                        batch.tile_id,
                        batch.print_id,
                        batch.saturated,
                        batch.latency_ms,
                        batch.timestamp));

        q1HeaderStream.union(q1DataStream)
                .writeAsText("/Results/query1.csv", FileSystem.WriteMode.OVERWRITE)
                .name("Write Q1 Results");

        // Query 2
        DataStream<Batch> filteredQ1 = q1Result.filter((FilterFunction<Batch>) batch -> batch.saturated > 0);

        DataStream<Batch> q2Result = filteredQ1
                .keyBy(batch -> batch.print_id + "_" + batch.tile_id)
                .process(new Q2OutlierDetection());

        // Salvataggio risultati Q2
        String q2Header = "batch_id,print_id,tile_id," +
                "P1_x,P1_y,δP1," +
                "P2_x,P2_y,δP2," +
                "P3_x,P3_y,δP3," +
                "P4_x,P4_y,δP4," +
                "P5_x,P5_y,δP5," +
                "latency_ms,timestamp";

        DataStream<String> q2HeaderStream = env.fromElements(q2Header);
        DataStream<String> q2DataStream = q2Result.map((MapFunction<Batch, String>) batch -> {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%d,%s,%d", batch.batch_id, batch.print_id, batch.tile_id));

            for (int i = 1; i <= 5; i++) {
                Object pos = batch.q2_outliers.getOrDefault("P" + i, List.of(0, 0));
                Object delta = batch.q2_outliers.getOrDefault("δP" + i, 0);

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

        env.execute("Thermal Defect Analysis Pipeline - Q1 + Q2");
    }
}
