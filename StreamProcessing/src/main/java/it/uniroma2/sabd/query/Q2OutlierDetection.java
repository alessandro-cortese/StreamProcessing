package it.uniroma2.sabd.query;

import it.uniroma2.sabd.model.Batch;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class Q2OutlierDetection extends KeyedProcessFunction<String, Batch, Batch> {

    private transient ListState<int[][]> windowState;

    private static final int EMPTY_THRESHOLD = 5000;
    private static final int SATURATION_THRESHOLD = 65000;
    private static final int OUTLIER_THRESHOLD = 6000;
    private static final int DISTANCE_THRESHOLD = 2;

    @Override
    public void open(Configuration parameters) {
        ListStateDescriptor<int[][]> descriptor = new ListStateDescriptor<>(
                "window_state", int[][].class);
        windowState = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void processElement(Batch batch, Context ctx, Collector<Batch> out) throws Exception {
        batch.decodeTIFF();

        List<int[][]> window = new ArrayList<>();
        for (int[][] img : windowState.get()) {
            window.add(img);
        }

        window.add(batch.pixels);
        if (window.size() > 3) window.remove(0);
        windowState.update(window);

        if (window.size() == 3) {
            batch.q2_top5_outliers = analyzeOutliers(window, batch);
        }

        out.collect(batch);
    }

    private Map<String, Object> analyzeOutliers(List<int[][]> window, Batch batch) {
        int height = window.get(0).length;
        int width = window.get(0)[0].length;
        int depth = window.size();

        List<Outlier> candidates = new ArrayList<>();
        List<List<Number>> allOutliers = new ArrayList<>();

        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                // Check on the current pixel (last layer)
                int val = window.get(depth - 1)[y][x];
                if (val <= EMPTY_THRESHOLD || val >= SATURATION_THRESHOLD) continue;

                // Compute Close Neighbors (CN) - distance <= DISTANCE_THRESHOLD
                double cnSum = 0.0;
                int cnCount = 0;

                for (int d = 0; d < depth; d++) {
                    for (int dy = -DISTANCE_THRESHOLD * 2; dy <= DISTANCE_THRESHOLD * 2; dy++) {
                        for (int dx = -DISTANCE_THRESHOLD * 2; dx <= DISTANCE_THRESHOLD * 2; dx++) {
                            // Compute distance as abs(dx) + abs(dy) + abs(depth - 1 - d)
                            int distance = Math.abs(dx) + Math.abs(dy) + Math.abs(depth - 1 - d);

                            if (distance <= DISTANCE_THRESHOLD) {
                                cnSum += getPaddedValue(window, d, x + dx, y + dy);
                                cnCount++;
                            }
                        }
                    }
                }

                // Compute Outer Neighbors (ON) - distance > DISTANCE_THRESHOLD && <= 2*DISTANCE_THRESHOLD
                double onSum = 0.0;
                int onCount = 0;

                for (int d = 0; d < depth; d++) {
                    for (int dy = -DISTANCE_THRESHOLD * 2; dy <= DISTANCE_THRESHOLD * 2; dy++) {
                        for (int dx = -DISTANCE_THRESHOLD * 2; dx <= DISTANCE_THRESHOLD * 2; dx++) {
                            int distance = Math.abs(dx) + Math.abs(dy) + Math.abs(depth - 1 - d);

                            if (distance > DISTANCE_THRESHOLD && distance <= 2 * DISTANCE_THRESHOLD) {
                                onSum += getPaddedValue(window, d, x + dx, y + dy);
                                onCount++;
                            }
                        }
                    }
                }

                double closeMean = cnCount > 0 ? cnSum / cnCount : 0.0;
                double outerMean = onCount > 0 ? onSum / onCount : 0.0;
                double dev = Math.abs(closeMean - outerMean);


                if (val > EMPTY_THRESHOLD && val < SATURATION_THRESHOLD && dev > OUTLIER_THRESHOLD) {
                    candidates.add(new Outlier(x, y, dev));
                    allOutliers.add(Arrays.asList(x, y));
                }
            }
        }

        batch.q2_all_outliers = allOutliers;

        candidates.sort(Comparator.comparingDouble(o -> -o.delta));

        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < Math.min(5, candidates.size()); i++) {
            Outlier o = candidates.get(i);
            result.put("P" + (i + 1), Arrays.asList(o.x, o.y));
            result.put("δP" + (i + 1), Math.round(o.delta));
        }

        return result;
    }

    /**
     * Obtain value with zero-padding
     */
    private double getPaddedValue(List<int[][]> window, int d, int x, int y) {
        // Check temporal bounds
        if (d < 0 || d >= window.size()) {
            return 0.0;
        }

        int[][] img = window.get(d);

        // Check bounds spatial
        if (x < 0 || x >= img[0].length || y < 0 || y >= img.length) {
            return 0.0;
        }


        return (double) img[y][x];
    }

    private static class Outlier {
        int x, y;
        double delta;

        Outlier(int x, int y, double delta) {
            this.x = x;
            this.y = y;
            this.delta = delta;
        }
    }
}