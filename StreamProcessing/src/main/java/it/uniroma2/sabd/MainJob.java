package it.uniroma2.sabd;

import it.uniroma2.sabd.engineering.ChallengerSource;
import it.uniroma2.sabd.model.Batch;
import it.uniroma2.sabd.query.Q1SaturationMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class MainJob {
    public static void main(String[] args) throws Exception {

        System.out.println("Avvio pipeline: Query 1 - Saturation Detection");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Batch> source = env.addSource(new ChallengerSource()).disableChaining();

        source.map((MapFunction<Batch, String>) batch -> {
            System.out.println(">>> Trigger batch: " + batch.batch_id);
            return "";
        }).print();

        DataStream<Batch> q1 = source.map(new Q1SaturationMapFunction());

        // CSV con intestazione simulata (aggiunta a inizio file)
        String header = "batch_id,tile_id,print_id,saturated,latency_ms,timestamp";
        String path = "/Results/query1.csv";

        DataStream<String> csvHeader = env.fromElements(header);
        DataStream<String> csvData = q1.map((MapFunction<Batch, String>) batch ->
                String.format("%d,%d,%s,%d,%d,%s",
                        batch.batch_id,
                        batch.tile_id,
                        batch.print_id,
                        batch.saturated,
                        batch.latency_ms,
                        batch.timestamp));

        csvHeader.union(csvData)
                .writeAsText(path, FileSystem.WriteMode.OVERWRITE)
                .name("Write Q1 CSV");

        env.execute("Thermal Defect Analysis Pipeline - Query 1");
    }
}
