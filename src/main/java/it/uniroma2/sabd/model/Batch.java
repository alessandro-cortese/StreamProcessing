package it.uniroma2.sabd.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Iterator;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.MemoryCacheImageInputStream;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;

/**
 * Represents a data batch coming from the Challenger.
 * Includes core metadata (batch ID, tile ID, etc.), the TIFF image,
 * and fields for the outputs of queries Q1, Q2, and Q3.
 */
public class Batch implements Serializable {
    private Integer batch_id;
    private Integer tile_id;
    private String print_id;
    private byte[] tif; // TIFF image data
    private Integer saturated = 0; // Q1 result: number of saturated pixels
    private long latency_ms = 0; // optional field, used to track processing delay
    private String timestamp = ""; // ingestion timestamp in ISO format
    private int[][] pixels; // 2D pixel matrix extracted from the TIFF image
    private long ingestion_time = 0; // ingestion time in epoch milliseconds
    private Map<String, Object> q2_top5_outliers = new HashMap<>();
    private List<List<Number>> q2_all_outliers = new ArrayList<>(); // full list of Q2 outliers
    private List<String> q3_clusters = new ArrayList<>(); // result of Q3 clustering

    /** Default constructor required for JSON deserialization. */
    public Batch() {}

    /**
     * Creates a Batch object from a map, mimicking the behavior of the Flink source.
     * @param map The raw map of values received from the Challenger.
     * @return A populated Batch object.
     */
    public static Batch fromMap(Map<String, Object> map) {
        Batch b = new Batch();
        b.setBatch_id((Integer) map.getOrDefault("batch_id", 0));
        b.setTile_id((Integer) map.getOrDefault("tile_id", 0));
        b.setPrint_id((String) map.getOrDefault("print_id", ""));
        b.setTif((byte[]) map.get("tif"));

        b.setTimestamp(java.time.Instant.now().toString());   // capture system timestamp
        b.setIngestion_time(System.currentTimeMillis());      // capture system time in ms

        return b;
    }

    /**
     * Decodes the TIFF image data into a 2D pixel matrix.
     * The matrix is transposed to match expected row-major format.
     */
    public void decodeTIFF() {
        try {
            Iterator<ImageReader> readers = ImageIO.getImageReadersByFormatName("tiff");
            ImageReader reader = readers.next();
            reader.setInput(new MemoryCacheImageInputStream(new ByteArrayInputStream(tif)));

            BufferedImage image = reader.read(0);

            int width = image.getWidth();
            int height = image.getHeight();
            int[][] temp = new int[height][width];

            for (int y = 0; y < height; y++) {
                for (int x = 0; x < width; x++) {
                    temp[y][x] = image.getRaster().getSample(x, y, 0); // 16-bit grayscale
                }
            }

            // Transpose the matrix: [height][width] → [width][height]
            pixels = new int[width][height];
            for (int y = 0; y < height; y++) {
                for (int x = 0; x < width; x++) {
                    pixels[x][y] = temp[y][x];
                }
            }
            reader.dispose();
        } catch (Exception e) {
            throw new RuntimeException("TIFF decoding error.", e);
        }
    }

    // Getters and setters for all fields

    public Integer getBatch_id() {
        return batch_id;
    }

    public void setBatch_id(Integer batch_id) {
        this.batch_id = batch_id;
    }

    public Integer getTile_id() {
        return tile_id;
    }

    public void setTile_id(Integer tile_id) {
        this.tile_id = tile_id;
    }

    public String getPrint_id() {
        return print_id;
    }

    public void setPrint_id(String print_id) {
        this.print_id = print_id;
    }

    public byte[] getTif() {
        return tif;
    }

    public void setTif(byte[] tif) {
        this.tif = tif;
    }

    public Integer getSaturated() {
        return saturated;
    }

    public void setSaturated(Integer saturated) {
        this.saturated = saturated;
    }

    public long getLatency_ms() {
        return latency_ms;
    }

    public void setLatency_ms(long latency_ms) {
        this.latency_ms = latency_ms;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public int[][] getPixels() {
        return pixels;
    }

    public void setPixels(int[][] pixels) {
        this.pixels = pixels;
    }

    public long getIngestion_time() {
        return ingestion_time;
    }

    public void setIngestion_time(long ingestion_time) {
        this.ingestion_time = ingestion_time;
    }

    public Map<String, Object> getQ2_top5_outliers() {
        return q2_top5_outliers;
    }

    public void setQ2_top5_outliers(Map<String, Object> q2_top5_outliers) {
        this.q2_top5_outliers = q2_top5_outliers;
    }

    public List<List<Number>> getQ2_all_outliers() {
        return q2_all_outliers;
    }

    public void setQ2_all_outliers(List<List<Number>> q2_all_outliers) {
        this.q2_all_outliers = q2_all_outliers;
    }

    public List<String> getQ3_clusters() {
        return q3_clusters;
    }

    public void setQ3_clusters(List<String> q3_clusters) {
        this.q3_clusters = q3_clusters;
    }

    @Override
    public String toString() {
        return "Batch{" +
                "batch_id=" + batch_id +
                ", tile_id=" + tile_id +
                ", print_id='" + print_id + '\'' +
                ", tif.length=" + (tif != null ? tif.length : 0) +
                ", saturated=" + saturated +
                ", latency_ms=" + latency_ms +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
}
