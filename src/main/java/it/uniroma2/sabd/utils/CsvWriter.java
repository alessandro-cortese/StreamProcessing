package it.uniroma2.sabd.utils;

import it.uniroma2.sabd.model.Batch;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class CsvWriter {

    private static boolean q1HeaderWritten = false;
    private static boolean q2HeaderWritten = false;

    public static synchronized void writeQ1(Batch batch) {
        try (OutputStreamWriter writer = new OutputStreamWriter(
                new FileOutputStream("Results/q1_results.csv", true),
                StandardCharsets.UTF_8)) {
            if (!q1HeaderWritten) {
                writer.write(getHeaderQ1() + "\n");
                q1HeaderWritten = true;
            }
            writer.write(String.format("%d\t%d\t%s\t%d\t%d\t%s\n",
                    batch.getBatch_id(),
                    batch.getTile_id(),
                    batch.getPrint_id(),
                    batch.getSaturated(),
                    batch.getLatency_ms(),
                    batch.getTimestamp()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static synchronized void writeQ2(Batch batch) {
        Map<String, Object> top5 = batch.getQ2_top5_outliers();
        if (top5 == null || top5.isEmpty()) {
            return;
        }

        try (OutputStreamWriter writer = new OutputStreamWriter(
                new FileOutputStream("Results/q2_results.csv", true),
                StandardCharsets.UTF_8)) {
            if (!q2HeaderWritten) {
                writer.write(getHeaderQ2() + "\n");
                q2HeaderWritten = true;
            }

            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%d,%s,%d", batch.getBatch_id(), batch.getPrint_id(), batch.getTile_id()));

            for (int i = 1; i <= 5; i++) {
                Object pos = top5.getOrDefault("P" + i, List.of(0, 0));
                Object delta = top5.getOrDefault("\u03b4P" + i, 0);  // Usa escape Unicode come in Flink
                if (pos instanceof List<?> l && l.size() == 2) {
                    sb.append(String.format(",%s,%s,%s", l.get(0), l.get(1), delta));
                } else {
                    sb.append(",0,0,0");
                }
            }

            sb.append(String.format(",%d,%s\n", batch.getLatency_ms(), batch.getTimestamp()));
            writer.write(sb.toString());

        } catch (IOException e) {
            e.printStackTrace();
        }
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
}