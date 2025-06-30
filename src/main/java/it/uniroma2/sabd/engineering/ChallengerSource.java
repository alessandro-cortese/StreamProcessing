package it.uniroma2.sabd.engineering;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.sabd.model.Batch;
import it.uniroma2.sabd.utils.HTTPClient;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/*
* * * * * * * * * * * * * * * * * * * * * *
* Create a source for Flink               *
* * * * * * * * * * * * * * * * * * * * * *
*/

public class ChallengerSource implements SourceFunction<Batch> {

    private volatile boolean running = true;                        // <- used to interrupt the source in thread safe mode
    private final static String API_URL = "http://gc25-challenger:8866";   // <- url of challenger
    public static String BENCH_ID = null;
    private static final Logger LOG = LoggerFactory.getLogger(ChallengerSource.class);
    // Flink Source
    @Override
    public void run(SourceContext<Batch> ctx) throws Exception {
        CloseableHttpClient http = HttpClients.createDefault();
        String benchId;

        try {
            // start bench
            benchId = createAndStartBench(http);
            BENCH_ID = benchId;
            LOG.info("Created Bench: " + benchId);
            try (PrintWriter out = new PrintWriter("/Results/current_bench_id.txt")) {
                out.println(benchId);
            } catch (IOException e) {
                LOG.info("Impossible write bench ID on file: " + e.getMessage());
            }

        } catch (Exception e) {
            LOG.info("Error in source: " + e.getMessage());
            e.printStackTrace();
            return;
        }

        int received = 0;
        while (running) {
            byte[] blob = fetchNextBatch(http, benchId);
            if (blob == null) break;

            Map<String, Object> record = unpack(blob);
            System.out.println("Record received: " + record);
            System.out.printf("tif.length = %d%n", ((byte[]) record.get("tif")).length);
            if (!record.containsKey("tif") || record.get("tif") == null || ((byte[]) record.get("tif")).length == 0) {
                System.err.println("Batch ignored: TIFF missing or empty.");
                continue;
            }
            Batch batch = Batch.fromMap(record);
            ctx.collect(batch);
            received++;
        }

        if (received > 0) {
            try {
                System.out.println("Bench finalized: " + benchId);
                endBench(http, benchId);
            } catch (Exception e) {
                System.err.println("Error in /api/end: " + e.getMessage());
            }
        } else {
            System.err.println("No batch received. Skip Call /api/end");
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    private String createAndStartBench(CloseableHttpClient http) throws IOException {
        System.out.println("Invoked createAndStartBench()");
        HttpPost create = new HttpPost(API_URL + "/api/create");
        create.setHeader("Content-Type", "application/json");

        Map<String, Object> body = new HashMap<>();
        body.put("name", "Flink-Analysis");
        body.put("test", false);
        body.put("apitoken", "uniroma2");

        String json = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(body);
        create.setEntity(new StringEntity(json, ContentType.APPLICATION_JSON));

        String raw = http.execute(create, HTTPClient.toStringResponseHandler());
        String id = raw.replace("\"", "");

        HttpPost start = new HttpPost(API_URL + "/api/start/" + id);
        start.setHeader("Content-Type", "application/json");
        http.execute(start, HTTPClient.toStringResponseHandler());

        return id;
    }

    private byte[] fetchNextBatch(CloseableHttpClient http, String benchId) {
        try {
            HttpGet req = new HttpGet(API_URL + "/api/next_batch/" + benchId);
            req.setHeader("Accept", "application/msgpack");
            return http.execute(req, HTTPClient.toByteResponseHandler());
        } catch (RuntimeException re) {
            if (re.getMessage().contains("API Error: 404"))
                return null;
            throw re;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    private void endBench(CloseableHttpClient http, String benchId) throws IOException {
        HttpPost end = new HttpPost(API_URL + "/api/end/" + benchId);
        http.execute(end, HTTPClient.toStringResponseHandler());
    }

    private static Map<String, Object> unpack(byte[] bytes) throws IOException {
        MessageUnpacker up = MessagePack.newDefaultUnpacker(bytes);
        int n = up.unpackMapHeader();
        Map<String, Object> m = new HashMap<>(n);
        for (int i = 0; i < n; i++) {
            String k = up.unpackString();
            Value v = up.unpackValue();
            if (v.isIntegerValue()) m.put(k, v.asIntegerValue().toInt());
            else if (v.isStringValue()) m.put(k, v.asStringValue().asString());
            else if (v.isBinaryValue()) m.put(k, v.asBinaryValue().asByteArray());
            else if (v.isNilValue()) m.put(k, null);
            else if (v.isFloatValue()) m.put(k, v.asFloatValue().toFloat());
            else if (v.isBooleanValue()) m.put(k, v.asBooleanValue().getBoolean());
            else m.put(k, v.toString());
        }
        up.close();
        return m;
    }

    public static void uploadResult(Batch batch, String benchId) {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            String url = String.format("%s/api/result/0/%s/%d", API_URL, benchId, batch.batch_id);

            ObjectMapper mapper = new ObjectMapper(new MessagePackFactory());

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("batch_id", batch.batch_id);
            resultMap.put("print_id", batch.print_id);
            resultMap.put("tile_id", batch.tile_id);
            resultMap.put("saturated", batch.saturated); // Dummy value per Q0
            resultMap.put("centroids", new ArrayList<>()); // Nessun cluster per Q0

            writeResults(false, batch, httpClient, url, mapper, resultMap);
            LOG.info("Q0: Result uploaded for batch_id={}, tile_id={}", batch.batch_id, batch.tile_id);
        } catch (Exception e) {
            LOG.error("Q0: Failed to upload result for batch_id={}: {}", batch.batch_id, e.getMessage(), e);
        }
    }

    private static void writeResults(Boolean flag, Batch batch, CloseableHttpClient httpClient, String url, ObjectMapper mapper, Map<String, Object> resultMap) throws IOException {
        byte[] payload = mapper.writeValueAsBytes(resultMap);

        HttpPost post = new HttpPost(url);
        post.setHeader("Content-Type", "application/msgpack");
        post.setEntity(new ByteArrayEntity(payload, ContentType.create("application/msgpack")));

        String response = httpClient.execute(post, response1 ->
                new String(response1.getEntity().getContent().readAllBytes())
        );
        if(flag){
            LOG.debug("Uploaded result for tile_id={}, response={}", batch.tile_id, response);
        }
    }

}
