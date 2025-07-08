package it.uniroma2.sabd.query;

import it.uniroma2.sabd.model.Batch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smile.clustering.DBSCAN;
import java.util.*;
import java.util.function.Function;

/**
 * Query Q3: Clustering of the outliers using DBSCAN
 */
public class Q3Clustering implements Function<Batch, Batch> {

    private static final Logger LOG = LoggerFactory.getLogger(Q3Clustering.class);
    private static final double EPSILON = 20.0;
    private static final int MIN_POINTS = 5;

    @Override
    public Batch apply(Batch batch) {
        try {
            List<List<Number>> rawPoints = batch.getQ2_all_outliers();

            if (rawPoints == null || rawPoints.isEmpty()) {
                batch.setQ3_clusters(new ArrayList<>());
                LOG.debug("Batch {}: No outliers found for clustering", batch.getBatch_id());
                return batch;
            }

            List<double[]> pointsList = new ArrayList<>();
            for (List<Number> point : rawPoints) {
                if (point != null && point.size() >= 2) {
                    double x = point.get(0).doubleValue();
                    double y = point.get(1).doubleValue();
                    pointsList.add(new double[]{x, y});
                }
            }

            if (pointsList.size() < MIN_POINTS) {
                batch.setQ3_clusters(new ArrayList<>());
                LOG.debug("Batch {}: Insufficient points for clustering ({})",
                        batch.getBatch_id(), pointsList.size());
                return batch;
            }


            double[][] points = pointsList.toArray(new double[0][]);

            // Apply DBSCAN
            DBSCAN<double[]> dbscan = DBSCAN.fit(points, MIN_POINTS, EPSILON);
            int[] labels = dbscan.y;

            // Compute the centroids
            List<String> centroids = calculateCentroids(points, labels);

            batch.setQ3_clusters(centroids);
            LOG.debug("Batch {}: Found {} clusters from {} outliers",
                    batch.getBatch_id(), centroids.size(), pointsList.size());

            return batch;

        } catch (Exception e) {
            LOG.error("Error processing Q3 for batch {}: {}", batch.getBatch_id(), e.getMessage(), e);
            return batch;
        }
    }

    /**
     * Compute the centroid identified from DBSCAN
     */
    private List<String> calculateCentroids(double[][] points, int[] labels) {
        Map<Integer, List<double[]>> clusters = new HashMap<>();


        for (int i = 0; i < labels.length; i++) {
            if (labels[i] != -1) {
                clusters.computeIfAbsent(labels[i], k -> new ArrayList<>()).add(points[i]);
            }
        }

        List<String> centroids = new ArrayList<>();
        for (Map.Entry<Integer, List<double[]>> entry : clusters.entrySet()) {
            List<double[]> cluster = entry.getValue();

            double sumX = 0.0, sumY = 0.0;
            for (double[] point : cluster) {
                sumX += point[0];
                sumY += point[1];
            }

            int size = cluster.size();
            double centroidX = sumX / size;
            double centroidY = sumY / size;

            String centroidJson = String.format(
                    "{\"x\": %.3f, \"y\": %.3f, \"count\": %d}",
                    centroidX, centroidY, size
            );
            centroids.add(centroidJson);
        }

        return centroids;
    }
}