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
    private static final int MAX_DIST = 4;

    private static final List<int[]> INTERNAL_OFFSETS = computeOffsets(2);
    private static final List<int[]> EXTERNAL_OFFSETS = computeOffsets(MAX_DIST);

    @Override
    public void open(Configuration parameters) {
        ListStateDescriptor<int[][]> descriptor =
                new ListStateDescriptor<>(
                        "window_state",
                        (Class<int[][]>) int[][].class
                );
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
        if (window.size() > 3) {
            window.remove(0);
        }
        windowState.update(window);

        if (window.size() == 3) {
            batch.q2_outliers = analyzeOutliers(window);
        }
        out.collect(batch);
    }

    private Map<String, Object> analyzeOutliers(List<int[][]> window) {
        int height = window.get(0).length;
        int width = window.get(0)[0].length;

        float[][][] padded = new float[3][height + 2 * MAX_DIST][width + 2 * MAX_DIST];
        for (int z = 0; z < 3; z++) {
            for (int y = 0; y < height; y++) {
                for (int x = 0; x < width; x++) {
                    padded[z][y + MAX_DIST][x + MAX_DIST] = window.get(z)[y][x];
                }
            }
        }

        List<Outlier> candidates = new ArrayList<>();

        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                float val = padded[2][y + MAX_DIST][x + MAX_DIST];
                if (val <= EMPTY_THRESHOLD || val >= SATURATION_THRESHOLD) continue;

                List<Float> internal = getInternal(INTERNAL_OFFSETS, padded, y, x);

                List<Float> external = getInternal(EXTERNAL_OFFSETS, padded, y, x);

                if (internal.isEmpty() || external.isEmpty()) continue;

                float dev = Math.abs(mean(internal) - mean(external));
                if (dev > OUTLIER_THRESHOLD) {
                    candidates.add(new Outlier(x, y, dev));
                }
            }
        }

        candidates.sort(Comparator.comparingDouble(o -> -o.delta));
        Map<String, Object> result = new HashMap<>();
        for (int i = 0; i < Math.min(5, candidates.size()); i++) {
            Outlier o = candidates.get(i);
            result.put("P" + (i + 1), Arrays.asList(o.x, o.y));  // <-- questa è mutabile
            result.put("δP" + (i + 1), o.delta);                // float è già OK
        }

        return result;
    }

    private static List<Float> getInternal(List<int[]> internalOffsets, float[][][] padded, int y, int x) {
        List<Float> internal = new ArrayList<>();
        for (int[] off : internalOffsets) {
            int dz = off[0], dx = off[1], dy = off[2];
            float v = padded[dz][y + dy + MAX_DIST][x + dx + MAX_DIST];
            if (v > EMPTY_THRESHOLD && v < SATURATION_THRESHOLD) internal.add(v);
        }
        return internal;
    }

    private static float mean(List<Float> values) {
        float sum = 0;
        for (float v : values) sum += v;
        return sum / values.size();
    }

    private static List<int[]> computeOffsets(int maxDist) {
        List<int[]> offsets = new ArrayList<>();
        for (int dz = 0; dz < 3; dz++) {
            for (int dx = -MAX_DIST; dx <= MAX_DIST; dx++) {
                for (int dy = -MAX_DIST; dy <= MAX_DIST; dy++) {
                    int dist = Math.abs(dx) + Math.abs(dy) + Math.abs(2 - dz);
                    if (maxDist == 2 && dist <= 2 && dist >= 1) {
                        offsets.add(new int[]{dz, dx, dy});
                    } else if (maxDist == 4 && dist > 2 && dist <= MAX_DIST) {
                        offsets.add(new int[]{dz, dx, dy});
                    }
                }
            }
        }
        return offsets;
    }

    private static class Outlier {
        int x, y;
        float delta;

        Outlier(int x, int y, float delta) {
            this.x = x;
            this.y = y;
            this.delta = delta;
        }
    }
}
