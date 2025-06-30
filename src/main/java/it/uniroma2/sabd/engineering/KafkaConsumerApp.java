package it.uniroma2.sabd.engineering;

import it.uniroma2.sabd.model.Batch;
import it.uniroma2.sabd.query.Q1Saturation;
import it.uniroma2.sabd.query.Q2OutlierDetection;
import it.uniroma2.sabd.utils.CsvWriter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * L'applicazione Kafka Streams per l'elaborazione dei batch dal Challenger.
 * Per questa demo, legge i batch da un topic Kafka e li stampa.
 */
public class KafkaConsumerApp {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerApp.class);
    private static final String KAFKA_TOPIC = "challenger-batches";
    private static final String KAFKA_BOOTSTRAP_SERVERS = "kafka:9092";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "thermal-defect-analysis-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // Serde personalizzato per l'oggetto Batch
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, BatchSerde.class.getName());
        // Gestione degli errori di deserializzazione, in modo da non bloccare la pipeline
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        props.put("auto.offset.reset", "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        // Stream dei batch dal topic Kafka
        KStream<String, Batch> stream = builder.stream(KAFKA_TOPIC, Consumed.with(Serdes.String(), new KafkaConsumerApp.BatchSerde()));
//        stream.peek((key, batch) -> {
//            if (batch != null) {
//                LOG.info("Batch ricevuto da Kafka Streams: Key={} BatchID={}", key, batch.getBatch_id());
//            } else {
//                LOG.warn("Batch nullo per key={}", key);
//            }
//        });
        // Q1
        Q1Saturation q1 = new Q1Saturation();
        Q2OutlierDetection q2 = new Q2OutlierDetection();

        long MAX_BATCHES = 3599;
        stream
                .mapValues(q1::apply)
                .mapValues(q2::apply)
                .peek((key, batch) -> {
                    //LOG.info("Q2 - Batch {}: {} outlier totali", batch.getBatch_id(),
                            //batch.getQ2_all_outliers() != null ? batch.getQ2_all_outliers().size() : 0);

                    CsvWriter.writeQ1(batch);
                    CsvWriter.writeQ2(batch);

                    ChallengerUploader.uploadQ2(batch, batch.getBench_id());

                    if (batch.getBatch_id() == MAX_BATCHES) {
                        LOG.info("Ultimo batch ricevuto: {}. Termino il benchmark.", batch.getBatch_id());
                        ChallengerUploader.endBenchmark(batch.getBench_id());
                        ChallengerMetricsFetcher.fetchAndSaveLatestMetrics(1, batch.getBench_id(), true);
                    }
                });

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        // Add a shutdown hook to close gracefully Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        LOG.info("Avvio dell'applicazione Kafka Streams...");
        streams.start();
    }

    /**
     * Serde personalizzato per la classe Batch utilizzando Jackson.
     * Questo Serde permette a Kafka Streams di serializzare e deserializzare
     * gli oggetti Batch da/verso JSON.
     */
    public static class BatchSerde implements Serde<Batch> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public org.apache.kafka.common.serialization.Serializer<Batch> serializer() {
            return new org.apache.kafka.common.serialization.Serializer<Batch>() {
                @Override
                public byte[] serialize(String topic, Batch data) {
                    if (data == null) {
                        return null;
                    }
                    try {
                        return objectMapper.writeValueAsBytes(data);
                    } catch (IOException e) {
                        LOG.error("Errore durante la serializzazione di Batch: {}", e.getMessage(), e);
                        throw new RuntimeException("Errore di serializzazione di Batch", e);
                    }
                }
            };
        }

        @Override
        public org.apache.kafka.common.serialization.Deserializer<Batch> deserializer() {
            return new org.apache.kafka.common.serialization.Deserializer<Batch>() {
                @Override
                public Batch deserialize(String topic, byte[] data) {
                    if (data == null) {
                        return null;
                    }
                    try {
                        return objectMapper.readValue(data, Batch.class);
                    } catch (IOException e) {
                        LOG.error("Errore durante la deserializzazione di Batch: {}", e.getMessage(), e);
                        return null; // Ritorna null e lascia che l'ExceptionHandler gestisca l'errore
                    }
                }
            };
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            // Nessuna configurazione specifica necessaria per ObjectMapper
        }

    }
}

