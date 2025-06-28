package it.uniroma2.sabd.utils;

import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;

import java.io.IOException;

/**
 * Utility class for handling HTTP responses.
 * Provides response handlers to convert response entities to String or byte[].
 */
public class HTTPClient {

    /**
     * Returns an HttpClientResponseHandler that converts the response entity to a String.
     * Also handles HTTP error responses.
     * @return An HttpClientResponseHandler for String.
     */
    public static HttpClientResponseHandler<String> toStringResponseHandler() {
        return response -> {
            int status = response.getCode();
            HttpEntity entity = response.getEntity();
            if (status >= 200 && status < 300) {
                try {
                    return entity != null ? EntityUtils.toString(entity) : null;
                } catch (ParseException e) {
                    throw new RuntimeException("Error while parsing response entity to String", e);
                }
            } else {
                String errorBody = "";
                try {
                    if (entity != null) {
                        errorBody = EntityUtils.toString(entity);
                    }
                } catch (ParseException | IOException e) {
                    // Ignore, just attempting to extract error body
                }
                throw new RuntimeException("API Error: " + status + " - " + response.getReasonPhrase() + " Body: " + errorBody);
            }
        };
    }

    /**
     * Returns an HttpClientResponseHandler that converts the response entity to a byte array.
     * Also handles HTTP error responses.
     * @return An HttpClientResponseHandler for byte[].
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
                    // Ignore, just attempting to extract error body
                }
                throw new RuntimeException("API Error: " + status + " - " + response.getReasonPhrase() + " Body: " + errorBody);
            }
        };
    }
}
