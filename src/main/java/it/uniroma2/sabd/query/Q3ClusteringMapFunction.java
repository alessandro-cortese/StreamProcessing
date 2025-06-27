//package it.uniroma2.sabd.query;
//
//import it.uniroma2.sabd.model.Batch;
//import org.apache.flink.api.common.functions.MapFunction;
//import smile.clustering.DBSCAN;
//
//import java.util.*;
//
//public class Q3ClusteringMapFunction implements MapFunction<Batch, Batch> {
//
//    private static final double EPSILON = 20;
//    private static final int MIN_POINTS = 5;
//
//    @Override
//    public Batch map(Batch batch) {
//
//        List<List<Number>> rawPoints = batch.q2_all_outliers;
//
//        if (rawPoints == null || rawPoints.isEmpty()) {
//            batch.q3_clusters = new ArrayList<>();
//            return batch;
//        }
//
//
//        List<double[]> pointsList = new ArrayList<>();
//        for (List<Number> point : rawPoints) {
//            if (point.size() == 2) {
//                double x = point.get(0).doubleValue();
//                double y = point.get(1).doubleValue();
//                pointsList.add(new double[]{x, y});
//            }
//        }
//
//        if (pointsList.size() < MIN_POINTS) {
//            batch.q3_clusters = new ArrayList<>();
//            return batch;
//        }
//
//        double[][] points = pointsList.toArray(new double[0][]);
//
//        // Esegui DBSCAN
//        DBSCAN<double[]> dbscan = DBSCAN.fit(points, MIN_POINTS, EPSILON);
//        int[] labels = dbscan.y;
//
//        // Raggruppa per cluster
//        Map<Integer, List<double[]>> clusters = new HashMap<>();
//
//        for (int i = 0; i < labels.length; i++) {
//            if (labels[i] == -1) continue; // noise
//            clusters.computeIfAbsent(labels[i], k -> new ArrayList<>()).add(points[i]);
//        }
//
//        // Costruisci centroidi in formato JSON-compatibile
//        List<String> centroids = new ArrayList<>();
//        for (List<double[]> cluster : clusters.values()) {
//            double sumX = 0.0, sumY = 0.0;
//            for (double[] p : cluster) {
//                sumX += p[0];
//                sumY += p[1];
//            }
//            int size = cluster.size();
//            double cx = sumX / size;
//            double cy = sumY / size;
//
//            String centroidJson = String.format(
//                    "{\"x\": %.3f, \"y\": %.3f, \"count\": %d}",
//                    cx, cy, size
//            );
//            centroids.add(centroidJson);
//        }
//
//        batch.q3_clusters = centroids;
//        return batch;
//    }
//}
