package it.uniroma2.sabd.query;

import it.uniroma2.sabd.model.Batch;
import org.apache.flink.api.common.functions.MapFunction;
import smile.clustering.DBSCAN;

import java.util.*;

public class Q3ClusteringMapFunction implements MapFunction<Batch, Batch> {

    private static final double EPSILON = 20;
    private static final int MIN_POINTS = 5;

    @Override
    public Batch map(Batch batch) throws Exception {
        if (batch.q2_outliers == null || batch.q2_outliers.isEmpty()) {
            batch.q3_clusters = new ArrayList<>();
            return batch;
        }

        // Extract P1..P5 from q2_outliers
        List<double[]> pointsList = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            Object pos = batch.q2_outliers.get("P" + i);
            if (pos instanceof List<?> l && l.size() == 2) {
                double x = ((Number) l.get(0)).doubleValue();
                double y = ((Number) l.get(1)).doubleValue();
                pointsList.add(new double[]{x, y});
            }
        }

        if (pointsList.isEmpty()) {
            batch.q3_clusters = new ArrayList<>();
            return batch;
        }


        double[][] points = pointsList.toArray(new double[0][]);

        DBSCAN<double[]> dbscan = DBSCAN.fit(points, MIN_POINTS, EPSILON);

        // Group points for cluster
        int[] labels = dbscan.y;
        Map<Integer, List<double[]>> clusters = new HashMap<>();

        for (int i = 0; i < labels.length; i++) {
            if (labels[i] == -1) continue; // noise
            clusters.computeIfAbsent(labels[i], k -> new ArrayList<>()).add(points[i]);
        }

        List<String> centroids = new ArrayList<>();
        for (List<double[]> cluster : clusters.values()) {
            double sumX = 0, sumY = 0;
            for (double[] p : cluster) {
                sumX += p[0];
                sumY += p[1];
            }
            int size = cluster.size();
            int cx = (int) Math.round(sumX / size);
            int cy = (int) Math.round(sumY / size);
            centroids.add(String.format("(%d,%d,%d)", cx, cy, size));
        }

        batch.q3_clusters = centroids;
        return batch;
    }
}
