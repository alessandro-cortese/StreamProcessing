package it.uniroma2.sabd.utils;

import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;

import java.io.IOException;

/**
 * Classe di utilità per la gestione delle risposte HTTP.
 * Fornisce handler per convertire l'entity della risposta in String o byte[].
 */
public class HTTPClient {

    /**
     * Restituisce un HttpClientResponseHandler che converte l'entity della risposta in una String.
     * Gestisce anche gli errori HTTP.
     * @return Un HttpClientResponseHandler per String.
     */
    public static HttpClientResponseHandler<String> toStringResponseHandler() {
        return response -> {
            int status = response.getCode();
            HttpEntity entity = response.getEntity();
            if (status >= 200 && status < 300) {
                try {
                    return entity != null ? EntityUtils.toString(entity) : null;
                } catch (ParseException e) {
                    throw new RuntimeException("Errore durante il parsing dell'entity della risposta a String", e);
                }
            } else {
                String errorBody = "";
                try {
                    if (entity != null) {
                        errorBody = EntityUtils.toString(entity);
                    }
                } catch (ParseException | IOException e) {
                    // Ignora, prova solo a recuperare il corpo dell'errore
                }
                throw new RuntimeException("Errore API: " + status + " - " + response.getReasonPhrase() + " Corpo: " + errorBody);
            }
        };
    }

    /**
     * Restituisce un HttpClientResponseHandler che converte l'entity della risposta in un array di byte.
     * Gestisce anche gli errori HTTP.
     * @return Un HttpClientResponseHandler per byte[].
     */
    public static HttpClientResponseHandler<byte[]> toByteResponseHandler() {
        return response -> {
            int status = response.getCode();
            HttpEntity entity = response.getEntity();
            if (status >= 200 && status < 300) {
                return entity != null ? EntityUtils.toByteArray(entity) : null;
            } else {
                String errorBody = "";
                try {
                    if (entity != null) {
                        errorBody = EntityUtils.toString(entity);
                    }
                } catch (ParseException | IOException e) {
                    // Ignora, prova solo a recuperare il corpo dell'errore
                }
                throw new RuntimeException("Errore API: " + status + " - " + response.getReasonPhrase() + " Corpo: " + errorBody);
            }
        };
    }
}
