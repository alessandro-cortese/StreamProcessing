package it.uniroma2.sabd.engineering;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MetricsCollector {

    private static final String CSV_FILE = "Results/query_metrics.csv";
    private static final Map<String, Stats> metrics = new ConcurrentHashMap<>();

    // Track when each query starts processing its first batch
    private static final Map<String, Long> queryFirstBatchStart = new ConcurrentHashMap<>();
    // Track when each query finishes processing its last batch
    private static final Map<String, Long> queryLastBatchEnd = new ConcurrentHashMap<>();

    /**
     * Alternative method that accepts start and end times
     * @param query Query name
     * @param startNano Start time in nanoseconds
     * @param endNano End time in nanoseconds
     */
    public static synchronized void recordWithTiming(String query, long startNano, long endNano) {
        long latencyMs = (endNano - startNano) / 1_000_000;
        metrics.computeIfAbsent(query, k -> new Stats()).add(latencyMs);

        queryFirstBatchStart.putIfAbsent(query, startNano);
        queryLastBatchEnd.put(query, endNano);
    }

    public static synchronized void export(int parallelism) {
        boolean writeHeader = false;
        File file = new File(CSV_FILE);
        if (!file.exists() || file.length() == 0) {
            writeHeader = true;
        }

        try (PrintWriter out = new PrintWriter(new FileWriter(CSV_FILE, true))) {
            if (writeHeader) {
                out.println("query,parallelism,count,avg_latency_ms,max_latency_ms,throughput_events_per_sec");
            }

            for (Map.Entry<String, Stats> entry : metrics.entrySet()) {
                String query = entry.getKey();
                Stats s = entry.getValue();

                // Calculate throughput based on actual processing time for this query
                long firstBatchStartNano = queryFirstBatchStart.getOrDefault(query, 0L);
                long lastBatchEndNano = queryLastBatchEnd.getOrDefault(query, 0L);

                double throughput = 0.0;
                if (firstBatchStartNano > 0 && lastBatchEndNano > firstBatchStartNano) {
                    long totalProcessingTimeMs = (lastBatchEndNano - firstBatchStartNano) / 1_000_000;
                    if (totalProcessingTimeMs > 0) {
                        throughput = (s.count * 1000.0) / totalProcessingTimeMs; // events per second
                    }
                }

                out.printf("%s,%d,%d,%.2f,%.2f,%.2f%n",
                        query,
                        parallelism,
                        s.count,
                        s.getAverage(),
                        (double) s.max,
                        throughput
                );
            }
            System.out.println("[MetricsCollector] Exported metrics to " + CSV_FILE);
        } catch (IOException e) {
            System.err.println("[MetricsCollector] Failed to export metrics: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Get current statistics for a query (useful for debugging)
     */
    public static synchronized Stats getStats(String query) {
        return metrics.get(query);
    }

    /**
     * Clear all collected metrics (useful for testing)
     */
    public static synchronized void clear() {
        metrics.clear();
        queryFirstBatchStart.clear();
        queryLastBatchEnd.clear();
    }

    private static class Stats {
        int count = 0;
        long totalLatency = 0;
        long max = 0;

        synchronized void add(long latency) {
            count++;
            totalLatency += latency;
            if (latency > max) max = latency;
        }

        double getAverage() {
            return count == 0 ? 0 : (double) totalLatency / count;
        }

        @Override
        public String toString() {
            return String.format("Stats{count=%d, avg=%.2f, max=%d}",
                    count, getAverage(), max);
        }
    }
}