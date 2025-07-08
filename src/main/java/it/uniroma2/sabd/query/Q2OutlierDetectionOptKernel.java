package it.uniroma2.sabd.query;

import it.uniroma2.sabd.model.Batch;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class Q2OutlierDetectionOptKernel extends KeyedProcessFunction<String, Batch, Batch> {

    private transient ListState<int[][]> windowState;

    private static final int EMPTY_THRESHOLD = 5000;
    private static final int SATURATION_THRESHOLD = 65000;
    private static final int OUTLIER_THRESHOLD = 6000;
    private static final int DISTANCE_THRESHOLD = 2;
    private static final int MAX_COORD_OFFSET = DISTANCE_THRESHOLD * 2;

    @Override
    public void open(Configuration parameters) {
        ListStateDescriptor<int[][]> descriptor = new ListStateDescriptor<>(
                "window_state", int[][].class);
        windowState = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void processElement(Batch batch, Context ctx, Collector<Batch> out) throws Exception {
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
        int depth = window.size(); // 3
        int height = window.get(0).length;
        int width = window.get(0)[0].length;

        // Convert to 3D tensor: [d][y][x]
        double[][][] tensor = new double[depth][height][width];
        for (int d = 0; d < depth; d++) {
            for (int y = 0; y < height; y++) {
                for (int x = 0; x < width; x++) {
                    tensor[d][y][x] = window.get(d)[y][x];
                }
            }
        }

        double[][][] kernel = buildKernel();
        double[][] deviationMap = convolve(tensor, kernel);

        List<Outlier> candidates = new ArrayList<>();
        List<List<Number>> allOutliers = new ArrayList<>();
        int[][] lastLayer = window.get(depth - 1);

        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                int val = lastLayer[y][x];
                if (val <= EMPTY_THRESHOLD || val >= SATURATION_THRESHOLD) continue;

                double delta = Math.abs(deviationMap[y][x]);

                if (delta > OUTLIER_THRESHOLD) {
                    candidates.add(new Outlier(x, y, delta));
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

    private double[][][] buildKernel() {
        int size = MAX_COORD_OFFSET * 2 + 1;  // = 5
        double[][][] kernel = new double[3][size][size];

        int cpCount = 0, opCount = 0;
        for (int d = 0; d < 3; d++) {
            for (int dy = -MAX_COORD_OFFSET; dy <= MAX_COORD_OFFSET; dy++) {
                for (int dx = -MAX_COORD_OFFSET; dx <= MAX_COORD_OFFSET; dx++) {
                    int distance = Math.abs(dx) + Math.abs(dy) + Math.abs(2 - d); // focus on layer 2
                    if (distance <= DISTANCE_THRESHOLD) cpCount++;
                    else if (distance <= 2 * DISTANCE_THRESHOLD) opCount++;
                }
            }
        }

        for (int d = 0; d < 3; d++) {
            for (int dy = -MAX_COORD_OFFSET; dy <= MAX_COORD_OFFSET; dy++) {
                for (int dx = -MAX_COORD_OFFSET; dx <= MAX_COORD_OFFSET; dx++) {
                    int distance = Math.abs(dx) + Math.abs(dy) + Math.abs(2 - d);
                    int yd = dy + MAX_COORD_OFFSET;
                    int xd = dx + MAX_COORD_OFFSET;

                    if (distance <= DISTANCE_THRESHOLD) {
                        kernel[d][yd][xd] = 1.0 / cpCount;
                    } else if (distance <= 2 * DISTANCE_THRESHOLD) {
                        kernel[d][yd][xd] = -1.0 / opCount;
                    }
                }
            }
        }

        return kernel;
    }

    private double[][] convolve(double[][][] tensor, double[][][] kernel) {
        int depth = tensor.length;
        int height = tensor[0].length;
        int width = tensor[0][0].length;
        int offset = MAX_COORD_OFFSET;

        double[][] result = new double[height][width];

        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                double sum = 0.0;
                for (int d = 0; d < 3; d++) {
                    for (int dy = -offset; dy <= offset; dy++) {
                        for (int dx = -offset; dx <= offset; dx++) {
                            int yd = y + dy;
                            int xd = x + dx;
                            if (yd < 0 || yd >= height || xd < 0 || xd >= width) continue;
                            sum += tensor[d][yd][xd] * kernel[d][dy + offset][dx + offset];
                        }
                    }
                }
                result[y][x] = sum;
            }
        }

        return result;
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