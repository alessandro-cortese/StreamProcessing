package it.uniroma2.sabd.model;

import java.util.*;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.MemoryCacheImageInputStream;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;

public class Batch {
    public int batch_id;
    public int tile_id;
    public String print_id;
    public byte[] tif;
    public int saturated = 0;
    public long latency_ms = 0;
    public String timestamp = "";
    public int[][] pixels;
    public long ingestion_time = 0;
    public Map<String, Object> q2_top5_outliers = new HashMap<>();
    public List<List<Number>> q2_all_outliers;
    public List<String> q3_clusters = new ArrayList<>();


    public static Batch fromMap(Map<String, Object> map) {
        Batch b = new Batch();
        b.batch_id = (int) map.get("batch_id");
        b.tile_id = (int) map.get("tile_id");
        b.print_id = (String) map.get("print_id");
        b.tif = (byte[]) map.get("tif");

        b.timestamp = java.time.Instant.now().toString(); // ⬅️ timestamp corrente
        b.ingestion_time = System.currentTimeMillis();     // ⬅️ salva istante ricezione
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
            pixels = new int[height][width];

            for (int y = 0; y < height; y++) {
                for (int x = 0; x < width; x++) {
                    pixels[y][x] = image.getRaster().getSample(x, y, 0); // 16-bit value
                }
            }

            reader.dispose();
        } catch (Exception e) {
            throw new RuntimeException("Errore nella decodifica TIFF", e);
        }
    }

}