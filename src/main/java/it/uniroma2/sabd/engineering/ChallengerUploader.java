package it.uniroma2.sabd.engineering;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.sabd.utils.HTTPClient;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.uniroma2.sabd.model.Batch;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ChallengerUploader {

    public static String BENCH_ID = null;

    private static final Logger LOG = LoggerFactory.getLogger(ChallengerUploader.class);
    public static final String API_URL =
            System.getenv().getOrDefault("CHALLENGER_API_URL", "http://gc25-challenger:8866");

    public static void uploadQ2(Batch batch, String benchId) {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {

            String url = String.format("%s/api/result/0/%s/%d", API_URL, benchId, batch.getBatch_id());
            ObjectMapper mapper = new ObjectMapper(new MessagePackFactory());

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("batch_id", batch.getBatch_id());
            resultMap.put("print_id", batch.getPrint_id());
            resultMap.put("tile_id", batch.getTile_id());
            List<List<Number>> outliers = batch.getQ2_all_outliers();
            resultMap.put("outliers", outliers != null ? outliers : List.of());

            byte[] msgpackData = mapper.writeValueAsBytes(resultMap);
            HttpPost post = new HttpPost(url);
            post.setEntity(new ByteArrayEntity(msgpackData, ContentType.create("application/msgpack")));

            httpClient.execute(post, HTTPClient.toStringResponseHandler());

            LOG.info("[Q2] Inviato batch {} con {} outlier",
                    batch.getBatch_id(),
                    outliers != null ? outliers.size() : 0);

        } catch (Exception e) {
            LOG.error("Errore invio Q2 per batch {}: {}", batch.getBatch_id(), e.getMessage(), e);
        }
    }


    public static void endBenchmark(String benchId) {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            String url = String.format("%s/api/end/%s", API_URL, benchId);
            HttpPost post = new HttpPost(url);
            httpClient.execute(post, HTTPClient.toStringResponseHandler());

            LOG.info("Benchmark {} terminata", benchId);
        } catch (Exception e) {
            LOG.error("Errore chiusura benchmark {}: {}", benchId, e.getMessage(), e);
        }
    }

    public static String getBenchId() {
        return BENCH_ID;
    }

    public static void setBenchId(String benchId) {
        BENCH_ID = benchId;
    }

}
