package org.curieo.embed;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

import lombok.Generated;
import lombok.Value;

@Generated @Value
public class EmbeddingService {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddingService.class);
	private static final ObjectWriter OBJECT_WRITER;
	private static final ObjectReader OBJECT_READER;
	String serviceUrl;
	int embeddingsLength;

	static {
		TypeReference<HashMap<String, String>> typeRef 
		  	= new TypeReference<HashMap<String, String>>() {};
		TypeReference<HashMap<String, List<Double>>> embeddingType 
		  	= new TypeReference<HashMap<String, List<Double>>>() {};
	  	OBJECT_WRITER = new ObjectMapper().writerFor(typeRef);
	  	OBJECT_READER = new ObjectMapper().readerFor(embeddingType);
	}
	
	public double[] embed(String text) {
		
		try {
			HttpClient client = HttpClient.newHttpClient();
			Map<String, String> map = new HashMap<>();
			map.put("text", text);
			HttpRequest request = HttpRequest.newBuilder()
					  .uri(URI.create(serviceUrl))
					  .header("Content-Type", "application/json")
					  .POST(HttpRequest.BodyPublishers.ofString(OBJECT_WRITER.writeValueAsString(map)))
					  .build();
			
			HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
			if (response.statusCode() == 200) {
				try {
					int i =0;
					double[] e = new double[embeddingsLength];
					for (JsonNode value : OBJECT_READER.readTree(response.body()).get("data")) {
						if (i == embeddingsLength) {
							throw new RuntimeException("Embeddings are too long");
						}
						e[i++] = value.asDouble();
					}
					return e;
				}
				catch (JsonProcessingException e) {
					LOGGER.error(String.format("Cannot read embedding %s", response.body()), e);
					Thread.currentThread().interrupt();
				}
			}
			else {
				String error = String.format("Server not available: %d", response.statusCode());
				LOGGER.error(error);
				throw new RuntimeException(error);
			}
		} catch (IOException e) {
			LOGGER.error(String.format("Not connected to service %s", serviceUrl), e);
			Thread.currentThread().interrupt();
		} catch (InterruptedException e) {
			LOGGER.error(String.format("Not connected to service %s", serviceUrl), e);
			Thread.currentThread().interrupt();
		}

		LOGGER.warn("Failure to embed text.");
		return null;
	}
}
