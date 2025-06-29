package it.uniroma2.sabd.query;

import it.uniroma2.sabd.model.Batch;

import java.util.*;

public class Q2OutlierDetection {

    private static final int EMPTY_THRESHOLD = 5000;
    private static final int SATURATION_THRESHOLD = 65000;
    private static final int OUTLIER_THRESHOLD = 6000;
    private static final int DISTANCE_THRESHOLD = 2;

    // This is our sliding window
    private final Map<String, Deque<int[][]>> windowMap = new HashMap<>();

    public Batch apply(Batch batch) {
        batch.decodeTIFF();

        String key = batch.getPrint_id() + "_" + batch.getTile_id();
        windowMap.putIfAbsent(key, new LinkedList<>());
        Deque<int[][]> window = windowMap.get(key);

        window.addLast(batch.getPixels());
        if (window.size() > 3) window.removeFirst();

        if (window.size() == 3) {
            batch.setQ2_top5_outliers(analyzeOutliers(new ArrayList<>(window), batch));
        }

        return batch;
    }

    private Map<String, Object> analyzeOutliers(List<int[][]> window, Batch batch) {
        int height = window.get(0).length;
        int width = window.get(0)[0].length;
        int depth = window.size();

        List<Outlier> candidates = new ArrayList<>();
        List<List<Number>> allOutliers = new ArrayList<>();

        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                int val = window.get(depth - 1)[y][x];
                if (val <= EMPTY_THRESHOLD || val >= SATURATION_THRESHOLD) continue;

                double cnSum = 0.0, onSum = 0.0;
                int cnCount = 0, onCount = 0;

                for (int d = 0; d < depth; d++) {
                    for (int dy = -DISTANCE_THRESHOLD * 2; dy <= DISTANCE_THRESHOLD * 2; dy++) {
                        for (int dx = -DISTANCE_THRESHOLD * 2; dx <= DISTANCE_THRESHOLD * 2; dx++) {
                            int distance = Math.abs(dx) + Math.abs(dy) + Math.abs(depth - 1 - d);
                            double valXY = getPaddedValue(window, d, x + dx, y + dy);
                            if (distance <= DISTANCE_THRESHOLD) {
                                cnSum += valXY;
                                cnCount++;
                            } else if (distance <= 2 * DISTANCE_THRESHOLD) {
                                onSum += valXY;
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

        batch.setQ2_all_outliers(allOutliers);

        candidates.sort(Comparator.comparingDouble(o -> -o.delta));
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < Math.min(5, candidates.size()); i++) {
            Outlier o = candidates.get(i);
            result.put("P" + (i + 1), Arrays.asList(o.x, o.y));
            result.put("δP" + (i + 1), Math.round(o.delta));
        }

        return result;
    }

    private double getPaddedValue(List<int[][]> window, int d, int x, int y) {
        if (d < 0 || d >= window.size()) return 0.0;
        int[][] img = window.get(d);
        if (x < 0 || x >= img[0].length || y < 0 || y >= img.length) return 0.0;
        return img[y][x];
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
