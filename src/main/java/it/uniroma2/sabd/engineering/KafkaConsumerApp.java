package it.uniroma2.sabd.engineering;

import it.uniroma2.sabd.model.Batch;
import it.uniroma2.sabd.query.Q1Saturation;
import it.uniroma2.sabd.query.Q2OutlierDetection;
import it.uniroma2.sabd.query.Q3Clustering;
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

public class KafkaConsumerApp {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerApp.class);
    private static final String KAFKA_TOPIC = "challenger-batches";
    private static final String KAFKA_BOOTSTRAP_SERVERS = "kafka:9092";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "thermal-defect-analysis-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, BatchSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        props.put("auto.offset.reset", "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Batch> stream = builder.stream(KAFKA_TOPIC, Consumed.with(Serdes.String(), new KafkaConsumerApp.BatchSerde()));
        stream.peek((key, batch) -> {
            if (batch != null) {
                LOG.info("Batch received from Kafka Streams: Key={} BatchID={}", key, batch.getBatch_id());
            } else {
                LOG.warn("Null batch for key={}", key);
            }
        });

        Q1Saturation q1 = new Q1Saturation();
        Q2OutlierDetection q2 = new Q2OutlierDetection();
        Q3Clustering q3 = new Q3Clustering();
        long MAX_BATCHES = 3599;
        stream
                .mapValues(batch -> {
                    // Q1 Processing
                    long startQ1 = System.nanoTime();
                    Batch afterQ1 = q1.apply(batch);
                    long endQ1 = System.nanoTime();
                    MetricsCollector.recordWithTiming("Q1", startQ1, endQ1);

                    // Q2 Processing
                    long startQ2 = System.nanoTime();
                    Batch afterQ2 = q2.apply(afterQ1);
                    long endQ2 = System.nanoTime();
                    MetricsCollector.recordWithTiming("Q2", startQ2, endQ2);

                    // Q3 Processing
                    long startQ3 = System.nanoTime();
                    Batch afterQ3 = q3.apply(afterQ2);
                    long endQ3 = System.nanoTime();
                    MetricsCollector.recordWithTiming("Q3", startQ3, endQ3);

                    return afterQ3;
                })
                .peek((key, batch) -> {
                    CsvWriter.writeQ1(batch);
                    CsvWriter.writeQ2(batch);
                    CsvWriter.writeQ3(batch);

                    //ChallengerUploader.uploadQ2(batch, batch.getBench_id());
                    ChallengerUploader.uploadQ3(batch, batch.getBench_id());
                    if (batch.getBatch_id() == MAX_BATCHES) {
                        LOG.info("Last batch received: {}. Waiting 10s before closing the benchmark...", batch.getBatch_id());


                        new Thread(() -> {
                            try {
                                Thread.sleep(10000); // Wait to ensure all consumers have finished
                                ChallengerUploader.endBenchmark(batch.getBench_id());
                                int partitions = Integer.parseInt(System.getenv().getOrDefault("KAFKA_TOPIC_PARTITIONS", "2"));

                                // Export metrics with per-query throughput
                                MetricsCollector.export(partitions);

                                // Debug info
                                LOG.info("Q1 Stats: {}", MetricsCollector.getStats("Q1"));
                                LOG.info("Q2 Stats: {}", MetricsCollector.getStats("Q2"));
                                LOG.info("Q1 Total Processing Time: {}ms", MetricsCollector.getTotalProcessingTime("Q1"));
                                LOG.info("Q2 Total Processing Time: {}ms", MetricsCollector.getTotalProcessingTime("Q2"));

                            } catch (InterruptedException e) {
                                LOG.error("Error during delay before closing benchmark", e);
                            }
                        }).start();
                    }
                });

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        LOG.info("Starting Kafka Streams application...");
        streams.start();
    }

    public static class BatchSerde implements Serde<Batch> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public org.apache.kafka.common.serialization.Serializer<Batch> serializer() {
            return (topic, data) -> {
                if (data == null) return null;
                try {
                    return objectMapper.writeValueAsBytes(data);
                } catch (IOException e) {
                    LOG.error("Error serializing Batch: {}", e.getMessage(), e);
                    throw new RuntimeException("Batch serialization error", e);
                }
            };
        }

        @Override
        public org.apache.kafka.common.serialization.Deserializer<Batch> deserializer() {
            return (topic, data) -> {
                if (data == null) return null;
                try {
                    return objectMapper.readValue(data, Batch.class);
                } catch (IOException e) {
                    LOG.error("Error deserializing Batch: {}", e.getMessage(), e);
                    return null;
                }
            };
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {}
    }
}
