package it.uniroma2.sabd.engineering;

import it.uniroma2.sabd.model.Batch;
import it.uniroma2.sabd.utils.HTTPClient;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ChallengerSource implements SourceFunction<Batch> {

    private volatile boolean running = true;
    private String API_URL = "http://gc25-challenger:8866";

    @Override
    public void run(SourceContext<Batch> ctx) throws Exception {
        System.out.println("run() di ChallengerSource avviato");
        CloseableHttpClient http = HttpClients.createDefault();
        String benchId = createAndStartBench(http);
        int received = 0;

        while (running) {
            byte[] blob = fetchNextBatch(http, benchId);
            if (blob == null) break;

            Map<String, Object> record = unpack(blob);
            Batch batch = Batch.fromMap(record);
            ctx.collect(batch);
            received++;
        }

        // Solo se abbiamo ricevuto almeno un batch, chiamiamo end
        if (received > 0) {
            try {
                endBench(http, benchId);
            } catch (Exception e) {
                System.err.println("Errore chiamando /api/end: " + e.getMessage());
            }
        } else {
            System.err.println("Nessun batch ricevuto. Salto chiamata /api/end");
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    private String createAndStartBench(CloseableHttpClient http) throws IOException {
        HttpPost create = new HttpPost(API_URL + "/api/create");
        create.setHeader("Content-Type", "application/json");

        Map<String, Object> body = new HashMap<>();
        body.put("name", "Flink_1");
        body.put("test", false);
        body.put("apitoken", "token");

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
}

