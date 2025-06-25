package it.uniroma2.sabd.query;

import it.uniroma2.sabd.model.Batch;
import org.apache.flink.api.common.functions.MapFunction;
import smile.clustering.DBSCAN;

import java.util.*;

public class Q3ClusteringMapFunction implements MapFunction<Batch, Batch> {

    private static final double EPSILON = 20;
    private static final int MIN_POINTS = 5;

    @Override
    public Batch map(Batch batch) {
        if (batch.q2_outliers == null || batch.q2_outliers.isEmpty()) {
            batch.q3_clusters = new ArrayList<>();
            return batch;
        }

        List<double[]> points = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            Object p = batch.q2_outliers.get("P" + i);
            if (p instanceof List<?> l && l.size() == 2) {
                double x = ((Number) l.get(0)).doubleValue();
                double y = ((Number) l.get(1)).doubleValue();
                points.add(new double[]{x, y});
            }
        }

        if (points.size() < MIN_POINTS) {
            batch.q3_clusters = new ArrayList<>();
            return batch;
        }

        DBSCAN<double[]> dbscan = DBSCAN.fit(points.toArray(new double[0][]), MIN_POINTS, EPSILON);
        int[] labels = dbscan.y;

        Map<Integer, List<double[]>> clusters = new HashMap<>();
        for (int i = 0; i < labels.length; i++) {
            if (labels[i] == -1) continue;
            clusters.computeIfAbsent(labels[i], k -> new ArrayList<>()).add(points.get(i));
        }

        List<String> centroids = new ArrayList<>();
        for (List<double[]> cluster : clusters.values()) {
            double sumX = 0, sumY = 0;
            for (double[] p : cluster) {
                sumX += p[0];
                sumY += p[1];
            }
            int cx = (int) Math.round(sumX / cluster.size());
            int cy = (int) Math.round(sumY / cluster.size());
            centroids.add(String.format("(%d,%d,%d)", cx, cy, cluster.size()));
        }

        batch.q3_clusters = centroids;
        return batch;
    }
}