package it.uniroma2.sabd.model;

import java.util.*;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.MemoryCacheImageInputStream;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;

public class Batch {
    private int batch_id;
    private int tile_id;
    private String print_id;
    private byte[] tif;
    private int saturated = 0;
    private long latency_ms = 0;
    private String timestamp = "";
    private int[][] pixels;
    private long ingestion_time = 0;
    private Map<String, Object> q2_top5_outliers = new HashMap<>();
    private List<List<Number>> q2_all_outliers;
    private List<String> q3_clusters = new ArrayList<>();

    public static Batch fromMap(Map<String, Object> map) {
        Batch b = new Batch();
        b.batch_id = (int) map.get("batch_id");
        b.tile_id = (int) map.get("tile_id");
        b.print_id = (String) map.get("print_id");
        b.tif = (byte[]) map.get("tif");

        b.timestamp = java.time.Instant.now().toString();   // timestamp
        b.ingestion_time = System.currentTimeMillis();      // save instant reception
        return b;
    }

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
                    temp[y][x] = image.getRaster().getSample(x, y, 0); // 16-bit value
                }
            }
            // transpose the matrix
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

    public int getBatch_id() {
        return batch_id;
    }

    public void setBatch_id(int batch_id) {
        this.batch_id = batch_id;
    }

    public int getTile_id() {
        return tile_id;
    }

    public void setTile_id(int tile_id) {
        this.tile_id = tile_id;
    }

    public String getPrint_id() {
        return print_id;
    }

    public void setPrint_id(String print_id) {
        this.print_id = print_id;
    }

    public int getSaturated() {
        return saturated;
    }

    public void setSaturated(int saturated) {
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

    public void setPixels(int[][] pixels) {
        this.pixels = pixels;
    }

    public int[][] getPixels() {
        return pixels;
    }

}