package it.uniroma2.sabd.engineering;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.sabd.model.Batch;
import it.uniroma2.sabd.utils.HTTPClient;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka Producer application that fetches batches from the Challenger HTTP API,
 * serializes them, and sends them to a Kafka topic for downstream processing.
 */
public class KafkaProducerApp {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerApp.class);
    private static final String KAFKA_TOPIC = "challenger-batches";
    private static final String KAFKA_BOOTSTRAP_SERVERS =
            System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
    private static final String API_URL =
            System.getenv().getOrDefault("CHALLENGER_API_URL", "http://gc25-challenger:8866");

    private KafkaProducer<String, Batch> producer;
    private ObjectMapper msgpackMapper;
    private ObjectMapper jsonMapper;

    public KafkaProducerApp() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "it.uniroma2.sabd.model.BatchSerializer");

        producer = new KafkaProducer<>(props);


        msgpackMapper = new ObjectMapper(new MessagePackFactory());
        jsonMapper = new ObjectMapper();
        try(AdminClient admin = AdminClient.create(props)){
            int partitions = Integer.parseInt(System.getenv().getOrDefault("KAFKA_TOPIC_PARTITIONS", "2"));
            NewTopic newTopic = new NewTopic(KAFKA_TOPIC, partitions, (short) 1);
            admin.createTopics(Collections.singletonList(newTopic)).all().get();
            LOG.info("Created topic {} with {} partitions", KAFKA_TOPIC, partitions);

        } catch (Exception e) {
            LOG.error("Failed to setup topic", e);
            throw new RuntimeException("Topic setup failed", e);
        }
    }

    public void run() {
        CloseableHttpClient http = HttpClients.createDefault();
        String benchId;

        try {
            benchId = createAndStartBench(http);

            LOG.info("Benchmark created and started: {}", benchId);
        } catch (Exception e) {
            LOG.error("Error starting the benchmark: {}", e.getMessage(), e);
            try {
                http.close();
            } catch (IOException ioException) {
                LOG.error("Error closing the HTTP client", ioException);
            }
            producer.close();
            return;
        }

        int received = 0;
        final int MAX_BATCHES = 3600;

        try {
            while (received < MAX_BATCHES) {
                byte[] blob = fetchNextBatch(http, benchId);
                if (blob == null) {
                    LOG.info("No more batches available or received 404 from API. Stopping fetch.");
                    break;
                }

                Map<String, Object> record = unpack(blob);
                LOG.debug("Record received: {}", record);

                if (!record.containsKey("tif") || record.get("tif") == null || ((byte[]) record.get("tif")).length == 0) {
                    LOG.warn("Batch skipped: missing or empty TIFF data.");
                    continue;
                }

                Batch batch = Batch.fromMap(record);
                batch.setBench_id(benchId);
                LOG.info("Batch parsed: batch_id={} tile_id={} print_id={}",
                        batch.getBatch_id(), batch.getTile_id(), batch.getPrint_id());
                int partitions = Integer.parseInt(System.getenv().getOrDefault("KAFKA_TOPIC_PARTITIONS", "2"));
                int partition = (batch.getBatch_id() == 3599) ? 0 : (batch.getBatch_id() % partitions); // <-- Qua dici a quale partizione inviarlo giusto?
                producer.send(new ProducerRecord<>(KAFKA_TOPIC, partition, String.valueOf(batch.getBatch_id()), batch),
                        (metadata, exception) -> {
                            if (exception == null) {
                                LOG.debug("Batch ID {} sent to topic {} at offset {}",
                                        batch.getBatch_id(), metadata.topic(), metadata.offset());
                            } else {
                                LOG.error("Failed to send batch ID {}: {}", batch.getBatch_id(), exception.getMessage(), exception);
                            }
                        });
                received++;
            }
        } catch (Exception e) {
            LOG.error("Error while fetching or sending batches: {}", e.getMessage(), e);
        }

        producer.flush();
        producer.close();
        LOG.info("Challenger producer finished.");
    }

    private String createAndStartBench(CloseableHttpClient http) throws IOException {
        LOG.info("Sending createAndStartBench() request...");
        HttpPost create = new HttpPost(API_URL + "/api/create");
        create.setHeader("Content-Type", "application/json");

        Map<String, Object> body = new HashMap<>();
        body.put("name", "KafkaStreams-Analysis");
        body.put("test", false);
        body.put("apitoken", "uniroma2");

        String json = new ObjectMapper().writeValueAsString(body);
        create.setEntity(new StringEntity(json, ContentType.APPLICATION_JSON));

        String raw = http.execute(create, HTTPClient.toStringResponseHandler());
        String id = raw.replace("\"", "");

        HttpPost start = new HttpPost(API_URL + "/api/start/" + id);
        start.setHeader("Content-Type", "application/json");
        String startJson = jsonMapper.writeValueAsString(new HashMap<>());
        start.setEntity(new StringEntity(startJson, ContentType.APPLICATION_JSON));
        http.execute(start, HTTPClient.toStringResponseHandler());

        return id;
    }

    private byte[] fetchNextBatch(CloseableHttpClient http, String benchId) {
        try {
            HttpGet req = new HttpGet(API_URL + "/api/next_batch/" + benchId);
            req.setHeader("Accept", "application/msgpack");
            return http.execute(req, HTTPClient.toByteResponseHandler());
        } catch (RuntimeException re) {
            if (re.getMessage().contains("API Error: 404")) {
                return null;
            }
            throw re;
        } catch (IOException ioe) {
            throw new RuntimeException("I/O error while fetching batch: " + ioe.getMessage(), ioe);
        }
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

    public static void main(String[] args) {
        LOG.info("Starting Challenger Producer...");
        KafkaProducerApp producer = new KafkaProducerApp();
        producer.run();
    }
}
