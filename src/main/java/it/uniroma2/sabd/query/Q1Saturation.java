package it.uniroma2.sabd.query;

import it.uniroma2.sabd.model.Batch;

public class Q1Saturation{

    private static final int UPPER_THRESHOLD = 65000;

    public Batch apply(Batch batch) {
        batch.decodeTIFF();

        int count = 0;
        if (batch.getPixels() != null) {
            for (int[] row : batch.getPixels()) {
                for (int value : row) {
                    if (value > UPPER_THRESHOLD) count++;
                }
            }
        }

        batch.setSaturated(count);
        batch.setLatency_ms(System.currentTimeMillis() - batch.getIngestion_time());
        return batch;
    }
}