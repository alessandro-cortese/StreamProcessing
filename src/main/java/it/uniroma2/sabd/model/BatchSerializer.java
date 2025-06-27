package it.uniroma2.sabd.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Serializzatore personalizzato per la classe Batch.
 * Converte un oggetto Batch in un array di byte JSON.
 */
public class BatchSerializer implements Serializer<Batch> {

    private static final Logger LOG = LoggerFactory.getLogger(BatchSerializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Nessuna configurazione specifica necessaria per ObjectMapper
    }

    @Override
    public byte[] serialize(String topic, Batch data) {
        if (data == null) {
            return null;
        }
        try {
            // ObjectMapper converte l'oggetto Batch in un array di byte JSON
            return objectMapper.writeValueAsBytes(data);
        } catch (IOException e) {
            LOG.error("Errore durante la serializzazione di Batch per il topic {}: {}", topic, e.getMessage(), e);
            throw new RuntimeException("Errore di serializzazione di Batch", e);
        }
    }

    @Override
    public void close() {
        // Nessuna risorsa da chiudere per ObjectMapper
    }
}