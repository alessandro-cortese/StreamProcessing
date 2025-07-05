package it.uniroma2.sabd.engineering;

import it.uniroma2.sabd.model.Batch;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.FileWriter;
import java.io.IOException;

public class QueryMetricsSink extends RichSinkFunction<Batch> {

    private static final String SUMMARY_FILE = "/Results/metrics_summary.csv";


    private final String query;
    private final int parallelismLevel;
    private final boolean optimization;
    private final String outputDir;

    private long count = 0;
    private long sumLatency = 0;
    private long maxLatency = Long.MIN_VALUE;

    private long startTime = -1;
    private long endTime = -1;

    public QueryMetricsSink(String query, int parallelismLevel, boolean optimization, String outputDir) {
        this.query = query;
        this.parallelismLevel = parallelismLevel;
        this.optimization = optimization;
        this.outputDir = outputDir;
    }

    @Override
    public void invoke(Batch batch, Context context) {
        long now = System.currentTimeMillis();

        if (startTime == -1) {
            startTime = now;
        }

        endTime = now;
        count++;
        sumLatency += (System.currentTimeMillis() - batch.ingestion_time);
        maxLatency = Math.max(maxLatency, System.currentTimeMillis() - batch.ingestion_time);
    }

    @Override
    public void close() throws IOException {
        double avgLatency = count > 0 ? (double) sumLatency / count : 0;
        long elapsedMillis = endTime - startTime;
        double throughput = (elapsedMillis > 0) ? count / (elapsedMillis / 1000.0) : 0;

        boolean writeHeader = false;

        synchronized (QueryMetricsSink.class) {
            // controlla se il file esiste
            java.io.File file = new java.io.File(SUMMARY_FILE);
            writeHeader = !file.exists();

            try (FileWriter writer = new FileWriter(file, true)) {
                if (writeHeader) {
                    writer.write("query,parallelism,optimization,count,avg_latency_ms,max_latency_ms,elapsed_ms,throughput_events_per_sec\n");
                }

                writer.write(String.format("%s,%d,%s,%d,%.2f,%d,%d,%.2f\n",
                        query, parallelismLevel, optimization ? "opt" : "noopt",
                        count, avgLatency, maxLatency, elapsedMillis, throughput));
            }
        }
    }

}
