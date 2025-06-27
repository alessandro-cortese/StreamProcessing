package it.uniroma2.sabd.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList; // Aggiunto per q2_all_outliers e q3_clusters
import java.util.Iterator; // Aggiunto per decodeTIFF
import javax.imageio.ImageIO; // Aggiunto per decodeTIFF
import javax.imageio.ImageReader; // Aggiunto per decodeTIFF
import javax.imageio.stream.MemoryCacheImageInputStream; // Aggiunto per decodeTIFF
import java.awt.image.BufferedImage; // Aggiunto per decodeTIFF
import java.io.ByteArrayInputStream; // Aggiunto per decodeTIFF

/**
 * Rappresenta un batch di dati proveniente dal Challenger.
 * Include i campi base per l'identificazione e il TIFF,
 * oltre a placeholder per i risultati delle query Q1, Q2 e Q3.
 */
public class Batch implements Serializable {
    private Integer batch_id;
    private Integer tile_id;
    private String print_id;
    private byte[] tif; // Dati immagine TIFF
    private Integer saturated = 0; // Inizializzato a 0 come nella tua versione precedente
    private long latency_ms = 0; // Inizializzato a 0 come nella tua versione precedente
    private String timestamp = ""; // Inizializzato a "" come nella tua versione precedente
    private int[][] pixels; // Nuovo campo dalla tua versione precedente
    private long ingestion_time = 0; // Nuovo campo dalla tua versione precedente
    private Map<String, Object> q2_top5_outliers = new HashMap<>();
    private List<List<Number>> q2_all_outliers = new ArrayList<>(); // Nuovo campo dalla tua versione precedente
    private List<String> q3_clusters = new ArrayList<>();

    /**
     * Costruttore di default necessario per la deserializzazione JSON.
     */
    public Batch() {}

    /**
     * Costruttore per creare un oggetto Batch da una Map, mimando il comportamento
     * della sorgente Flink.
     * @param map La mappa di dati ricevuta dal Challenger.
     * @return Un nuovo oggetto Batch.
     */
    public static Batch fromMap(Map<String, Object> map) {
        Batch b = new Batch();
        // Usiamo i setter per popolare l'oggetto, come fatto precedentemente
        b.setBatch_id((Integer) map.getOrDefault("batch_id", 0));
        b.setTile_id((Integer) map.getOrDefault("tile_id", 0));
        b.setPrint_id((String) map.getOrDefault("print_id", ""));
        b.setTif((byte[]) map.get("tif"));

        // Logica aggiunta dalla tua versione precedente
        b.setTimestamp(java.time.Instant.now().toString());   // timestamp
        b.setIngestion_time(System.currentTimeMillis());      // save instant reception

        return b;
    }

    /**
     * Decodifica i dati TIFF in un array di pixel.
     * Questo metodo era presente nella tua versione precedente e viene ora incluso.
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


    // Metodi Getter e Setter per tutti i campi

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
