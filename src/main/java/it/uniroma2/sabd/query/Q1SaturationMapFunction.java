package it.uniroma2.sabd.query;

import it.uniroma2.sabd.model.Batch;
import org.apache.flink.api.common.functions.MapFunction;

public class Q1SaturationMapFunction implements MapFunction<Batch, Batch> {

    private static final int UPPER_THRESHOLD = 65000;

    @Override
    public Batch map(Batch batch) {
        batch.decodeTIFF();  // Assicurati che il TIFF venga decodificato in batch.pixels

        int count = 0;
        if (batch.pixels != null) {
            for (int[] row : batch.pixels) {
                for (int value : row) {
                    if (value > UPPER_THRESHOLD) count++;
                }
            }
        }

        batch.saturated = count;
        batch.latency_ms = System.currentTimeMillis() - batch.ingestion_time;
        return batch;
    }
}
