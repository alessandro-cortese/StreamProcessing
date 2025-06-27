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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Il Produttore Challenger si connette all'API REST del Challenger,
 * recupera i batch di dati e li pubblica su un topic Kafka.
 */
public class ChallengerProducer {

    private final static String API_URL = "http://gc25-challenger:8866"; // URL del Challenger
    private static final Logger LOG = LoggerFactory.getLogger(ChallengerProducer.class);
    private static final String KAFKA_TOPIC = "challenger-batches"; // Topic Kafka di destinazione
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"; // Indirizzo del broker Kafka

    private KafkaProducer<String, Batch> producer;
    private ObjectMapper msgpackMapper; // ObjectMapper per MessagePack

    public ChallengerProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Utilizziamo un serializzatore personalizzato per l'oggetto Batch
        //props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.json.JacksonJsonSerializer.class.getName());
        // Se non usi Confluent o vuoi un Serde più semplice, potresti usare un serializzatore custom
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "it.uniroma2.sabd.serializers.BatchSerializer");

        producer = new KafkaProducer<>(props);
        msgpackMapper = new ObjectMapper(new MessagePackFactory());
    }

    /**
     * Avvia il processo di fetching dei batch dal Challenger e pubblicazione su Kafka.
     */
    public void run() {
        CloseableHttpClient http = HttpClients.createDefault();
        String benchId;

        try {
            // Avvia il benchmark
            benchId = createAndStartBench(http);
            LOG.info("Bench creato e avviato: " + benchId);

        } catch (Exception e) {
            LOG.error("Errore durante l'avvio del bench: " + e.getMessage(), e);
            try {
                http.close();
            } catch (IOException ioException) {
                LOG.error("Errore durante la chiusura dell'HTTP client", ioException);
            }
            producer.close();
            return;
        }

        int received = 0;
        try {
            while (true) { // Loop infinito fino a quando non ci sono più batch
                byte[] blob = fetchNextBatch(http, benchId);
                if (blob == null) {
                    LOG.info("Nessun altro batch disponibile o API error 404. Termino il fetching.");
                    break; // Nessun altro batch o fine del benchmark
                }

                Map<String, Object> record = unpack(blob);
                LOG.debug("Record ricevuto: " + record);

                if (!record.containsKey("tif") || record.get("tif") == null || ((byte[]) record.get("tif")).length == 0) {
                    LOG.warn("Batch ignorato: TIFF mancante o vuoto.");
                    continue;
                }

                Batch batch = Batch.fromMap(record);
                // Invia il batch al topic Kafka
                producer.send(new ProducerRecord<>(KAFKA_TOPIC, String.valueOf(batch.getBatch_id()), batch), (metadata, exception) -> {
                    if (exception == null) {
                        LOG.debug("Batch ID {} inviato al topic {} offset {}", batch.getBatch_id(), metadata.topic(), metadata.offset());
                    } else {
                        LOG.error("Errore nell'invio del batch ID {}: {}", batch.getBatch_id(), exception.getMessage(), exception);
                    }
                });
                received++;
            }
        } catch (Exception e) {
            LOG.error("Errore durante il fetching o l'invio dei batch: " + e.getMessage(), e);
        } finally {
            if (received > 0) {
                try {
                    LOG.info("Finalizzazione del Bench: " + benchId);
                    endBench(http, benchId);
                } catch (Exception e) {
                    LOG.error("Errore nella chiamata /api/end: " + e.getMessage(), e);
                }
            } else {
                LOG.warn("Nessun batch ricevuto. Salto la chiamata /api/end.");
            }
            try {
                http.close();
            } catch (IOException ioException) {
                LOG.error("Errore durante la chiusura dell'HTTP client", ioException);
            }
            LOG.info("Produttore Challenger terminato.");
        }
    }

    /**
     * Crea e avvia un nuovo benchmark sul Challenger.
     * @param http L'HTTP client.
     * @return L'ID del benchmark avviato.
     * @throws IOException In caso di errori di I/O.
     */
    private String createAndStartBench(CloseableHttpClient http) throws IOException {
        LOG.info("Richiesta createAndStartBench()");
        HttpPost create = new HttpPost(API_URL + "/api/create");
        create.setHeader("Content-Type", "application/json");

        Map<String, Object> body = new HashMap<>();
        body.put("name", "KafkaStreams-Analysis");
        body.put("test", false);
        body.put("apitoken", "uniroma2");

        String json = msgpackMapper.writeValueAsString(body);
        create.setEntity(new StringEntity(json, ContentType.APPLICATION_JSON));

        String raw = http.execute(create, HTTPClient.toStringResponseHandler());
        String id = raw.replace("\"", "");

        HttpPost start = new HttpPost(API_URL + "/api/start/" + id);
        start.setHeader("Content-Type", "application/json");
        http.execute(start, HTTPClient.toStringResponseHandler());

        return id;
    }

    /**
     * Fetches the next batch from Challenger.
     * @param http HTTP client.
     * @param benchId ID of current benchmark.
     * @return The array of bytes of the batch, or null if 404 error response.
     */
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
            throw new RuntimeException("Errore I/O durante il fetching del batch: " + ioe.getMessage(), ioe);
        }
    }

    /**
     * Ends the benchmark.
     * @param http HTTP client.
     * @param benchId ID of the benchmark.
     * @throws IOException In case of I/O errors.
     */
    private void endBench(CloseableHttpClient http, String benchId) throws IOException {
        HttpPost end = new HttpPost(API_URL + "/api/end/" + benchId);
        http.execute(end, HTTPClient.toStringResponseHandler());
    }

    /**
     * Unpack data MessagePack in una Map<String, Object>.
     * @param bytes L'array di byte MessagePack.
     * @return La mappa risultante.
     * @throws IOException In caso di errori di I/O.
     */
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
        // Verifica se i server Kafka sono specificati come argomenti
        String bootstrapServers = args.length > 0 ? args[0] : KAFKA_BOOTSTRAP_SERVERS;
        System.setProperty("KAFKA_BOOTSTRAP_SERVERS", bootstrapServers); // Imposta per il produttore e il consumer

        LOG.info("Avvio del Produttore Challenger...");
        ChallengerProducer producer = new ChallengerProducer();
        producer.run();
    }
}
