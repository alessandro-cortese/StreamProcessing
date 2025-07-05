package it.uniroma2.sabd.engineering;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDateTime;

public class ChallengerMetricsFetcher {

    private static final String HISTORY_URL = "http://gc25-challenger:8866/api/history";
    private static final String OUTPUT_FILE = "/Results/challenger_metrics.csv";
    private static final String OUTPUT_FILE_KAFKA_COMPARISON = "/Results/challenger_metrics_kafka_streams_comparison.csv";
    private static final String ERROR_LOG = "/Results/errors.log";

    public static void fetchAndSaveLatestMetrics(int parallelismLevel, String benchId, boolean kafka_comparison) {
        try {
            System.out.println("Requesting Challenger metrics from: " + HISTORY_URL);

            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(HISTORY_URL))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            int status = response.statusCode();

            if (status != 200) {
                logError("HTTP error from Challenger: " + status + "\nBody:\n" + response.body());
                return;
            }

            String body = response.body();
            ObjectMapper mapper = new ObjectMapper();
            JsonNode data = mapper.readTree(body);

            if (!data.isArray() || data.size() == 0) {
                logError("No benchmark found in the JSON.");
                return;
            }

            JsonNode target = null;
            for (JsonNode entry : data) {
                if (entry.has("bench_id") && entry.get("bench_id").asText().equals(benchId)) {
                    target = entry;
                }
            }

            if (target == null) {
                logError("No matches found for bench_id=" + benchId);
                return;
            }

            File file;
            if(kafka_comparison)
                 file = new File(OUTPUT_FILE_KAFKA_COMPARISON);
            else
                file = new File(OUTPUT_FILE);

            boolean isNew = !file.exists() || file.length() == 0;

            try (FileWriter writer = new FileWriter(file, true)) {
                if (isNew) {
                    writer.write("parallelism,bench_id,throughput,latency_mean,latency_max\n");
                }
                writer.write(String.format("%d,%s,%.2f,%s,%s\n",
                        parallelismLevel,
                        benchId,
                        target.get("throughput").asDouble(),
                        target.get("latency_mean").asText(),
                        target.get("latency_max").asText()
                ));
            }

            System.out.println("Metrics saved for bench_id=" + benchId);

        } catch (Exception e) {
            logError("Metrics Recovery Error: " + e);
            e.printStackTrace();
        }
    }

    private static void logError(String msg) {
        try (PrintWriter err = new PrintWriter(new FileWriter(ERROR_LOG, true))) {
            err.println("[" + LocalDateTime.now() + "] " + msg);
        } catch (Exception ignored) {}
        System.err.println(msg);
    }
}
