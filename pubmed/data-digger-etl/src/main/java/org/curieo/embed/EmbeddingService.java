package org.curieo.embed;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Generated;
import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Generated
@Value
public class EmbeddingService {
  private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddingService.class);
  private static final ObjectWriter OBJECT_WRITER;
  private static final ObjectReader OBJECT_READER;
  String serviceUrl;
  int embeddingsLength;

  static {
    TypeReference<HashMap<String, List<String>>> typeRef = new TypeReference<>() {};
    TypeReference<List<List<Double>>> embeddingType = new TypeReference<>() {};
    OBJECT_WRITER = new ObjectMapper().writerFor(typeRef);
    OBJECT_READER = new ObjectMapper().readerFor(embeddingType);
  }

  public double[] embed(String text) {
    try (HttpClient client = HttpClient.newHttpClient()) {
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(serviceUrl))
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(serializeTextEmbeddingsRouter(text)))
              .build();

      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() == 200) {
        try {
          int i = 0;
          double[] e = new double[embeddingsLength];
          for (JsonNode value : deserializeTextEmbeddingsRouter(response.body())) {
            if (i == embeddingsLength) {
              throw new RuntimeException("Embeddings are too long");
            }
            e[i++] = value.asDouble();
          }
          return e;
        } catch (JsonProcessingException e) {
          LOGGER.error(String.format("Cannot read embedding %s", response.body()), e);
          Thread.currentThread().interrupt();
        }
      } else {
        String error = String.format("Server not available: %d", response.statusCode());
        LOGGER.error(error);
        throw new RuntimeException(error);
      }
    } catch (IOException | InterruptedException e) {
      LOGGER.error(String.format("Not connected to service %s", serviceUrl), e);
      Thread.currentThread().interrupt();
    }
    LOGGER.warn("Failure to embed text.");
    return null;
  }

  private static String serializeTextEmbeddingsRouter(String text) throws JsonProcessingException {
    Map<String, List<String>> map = new HashMap<>();
    map.put("inputs", Collections.singletonList(text));
    return OBJECT_WRITER.writeValueAsString(map);
  }

  private static JsonNode deserializeTextEmbeddingsRouter(String response)
      throws JsonProcessingException {
    return OBJECT_READER.readTree(response).get(0);
  }
}
