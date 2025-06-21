package it.uniroma2.sabd.model;

import java.util.Map;

public class Batch {
    public int batch_id;
    public int tile_id;
    public String print_id;
    public byte[] tif;
    public int saturated = 0;
    public long latency_ms = 0;
    public String timestamp = "";

    public static Batch fromMap(Map<String, Object> map) {
        Batch b = new Batch();
        b.batch_id = (int) map.get("batch_id");
        b.tile_id = (int) map.get("tile_id");
        b.print_id = (String) map.get("print_id");
        b.tif = (byte[]) map.get("tif");
        return b;
    }
}