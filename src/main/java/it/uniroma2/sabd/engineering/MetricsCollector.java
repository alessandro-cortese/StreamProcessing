package it.uniroma2.sabd.engineering;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MetricsCollector {

    private static final String CSV_FILE = "Results/query_metrics.csv";
    private static final Map<String, Stats> metrics = new ConcurrentHashMap<>();

    // Track per-query cumulative processing time across all consumers
    private static final Map<String, AtomicLong> queryTotalProcessingTime = new ConcurrentHashMap<>();

    /**
     * Record method that accepts start and end times
     */
    public static synchronized void recordWithTiming(String query, long startNano, long endNano) {
        long latencyMs = (endNano - startNano) / 1_000_000;

        // Record individual query stats
        metrics.computeIfAbsent(query, k -> new Stats()).add(latencyMs);

        // Accumulate total processing time for this query across all consumers
        queryTotalProcessingTime.computeIfAbsent(query, k -> new AtomicLong(0))
                .addAndGet(latencyMs);
    }

    public static synchronized void export(int parallelism) {
        boolean writeHeader = false;
        File file = new File(CSV_FILE);
        if (!file.exists() || file.length() == 0) {
            writeHeader = true;
        }

        try (PrintWriter out = new PrintWriter(new FileWriter(CSV_FILE, true))) {
            if (writeHeader) {
                out.println("query,parallelism,batches_processed,avg_latency_ms,max_latency_ms,throughput_events_per_sec");
            }

            for (Map.Entry<String, Stats> entry : metrics.entrySet()) {
                String query = entry.getKey();
                Stats s = entry.getValue();

                // Calculate per-query throughput
                final int TOTAL_BATCHES = 3600; // Total batches processed by all consumers
                double queryThroughput = 0.0;

                // Get total processing time for this query across all consumers
                AtomicLong totalProcessingTimeAtomic = queryTotalProcessingTime.get(query);
                if (totalProcessingTimeAtomic != null) {
                    long totalProcessingTimeMs = totalProcessingTimeAtomic.get();
                    if (totalProcessingTimeMs > 0) {
                        // Throughput = total batches processed / total time spent processing this query
                        queryThroughput = (TOTAL_BATCHES * 1000.0) / totalProcessingTimeMs;
                    }
                }

                out.printf("%s,%d,%d,%.2f,%.2f,%.2f%n",
                        query,
                        parallelism,
                        TOTAL_BATCHES,
                        s.getAverage(),
                        (double) s.max,
                        queryThroughput
                );

                System.out.printf("[MetricsCollector] %s: %d batches, %.2f avg latency, %.2f throughput%n",
                        query, TOTAL_BATCHES, s.getAverage(), queryThroughput);
            }

            System.out.println("[MetricsCollector] Exported metrics to " + CSV_FILE);

        } catch (IOException e) {
            System.err.println("[MetricsCollector] Failed to export metrics: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Get current statistics for a query
     */
    public static synchronized Stats getStats(String query) {
        return metrics.get(query);
    }

    /**
     * Get total processing time for a query (for debugging)
     */
    public static long getTotalProcessingTime(String query) {
        AtomicLong total = queryTotalProcessingTime.get(query);
        return total != null ? total.get() : 0;
    }

    /**
     * Clear all collected metrics
     */
    public static synchronized void clear() {
        metrics.clear();
        queryTotalProcessingTime.clear();
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